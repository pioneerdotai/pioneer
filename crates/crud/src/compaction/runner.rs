use super::statement;
use super::{
    CHECKPOINT_SOURCE_LIMIT, SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS, checkpoint_identity,
    sqlite_specific_sql,
};
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::runner::{FailureKind, RunnerPhase, RunnerState};
use pioneer_compaction::{Checkpoint, ModelBudget, SourceRef};
use sea_orm::sea_query::{
    Alias, Asterisk, BinOper, Expr, ExprTrait, Func, JoinType, OnConflict, Order, Query,
};
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

#[derive(Clone, Debug)]
pub struct ManifestEntry {
    pub ordinal: u64,
    pub unit: u64,
    pub reference_only: bool,
    pub thread_id: String,
    pub source: SourceRef,
}
#[derive(Clone, Debug)]
pub struct RunnerPlanRecord {
    pub source_count: u64,
    pub reference_count: u64,
    pub budget: ModelBudget,
    pub ready: bool,
}

impl CrudStore {
    /// Manifest admission contains metadata only and resumes in bounded batches.
    /// No provider may run until activate_runner has checked the whole manifest.
    /// The execution turn is an immutable admission boundary. Its terminal
    /// interruption fences checkpoint publication even before service cleanup.
    pub async fn compaction_execution_cancelled(&self, operation: &str) -> Result<bool> {
        Ok(self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("o", "id")))
                    .from_as("compaction_operation", "o")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_context",
                        "c",
                        Expr::col(("c", "owner")).eq(Expr::col(("o", "owner"))),
                    )
                    .and_where(
                        Expr::col(("o", "id"))
                            .eq(Expr::Value(operation.into()))
                            .and(
                                Expr::col(("o", "execution_turn"))
                                    .binary(BinOper::Is, Expr::val(Option::<String>::None))
                                    .not(),
                            )
                            .and(
                                Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as("compaction_execution_stop", "stop")
                                        .and_where(
                                            Expr::col(("stop", "owner"))
                                                .eq(Expr::col(("c", "owner")))
                                                .and(
                                                    Expr::col(("stop", "turn_id"))
                                                        .eq(Expr::col(("o", "execution_turn"))),
                                                ),
                                        )
                                        .to_owned(),
                                )
                                .or(Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as("turn", "t")
                                        .and_where(
                                            Expr::col(("t", "id"))
                                                .eq(Expr::col(("o", "execution_turn")))
                                                .and(
                                                    Expr::col(("t", "status"))
                                                        .is_in(["interrupted", "cancelled"])
                                                        .not(),
                                                ),
                                        )
                                        .to_owned(),
                                )
                                .not()),
                            ),
                    )
                    .to_owned(),
            ))
            .await?
            .is_some())
    }

    pub async fn compaction_bind_execution_turn(&self, operation: &str, turn: &str) -> Result<()> {
        self.connection
            .execute_raw(statement(
                &Query::update()
                    .table("compaction_operation")
                    .value("execution_turn", Expr::Value(turn.into()))
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(operation.into()))
                            .and(
                                Expr::col("execution_turn")
                                    .binary(BinOper::Is, Expr::val(Option::<String>::None)),
                            )
                            .and(Expr::col("status").eq(Expr::val("running")))
                            .and(Expr::exists(
                                Query::select()
                                    .expr(Expr::val(1_i64))
                                    .from_as("compaction_context", "c")
                                    .join_as(
                                        JoinType::InnerJoin,
                                        "turn",
                                        "t",
                                        Expr::col(("t", "thread_id"))
                                            .eq(Expr::col(("c", "thread_id"))),
                                    )
                                    .and_where(
                                        Expr::col(("c", "owner"))
                                            .eq(Expr::col(("compaction_operation", "owner")))
                                            .and(
                                                Expr::col(("t", "id")).eq(Expr::Value(turn.into())),
                                            )
                                            .and(
                                                Expr::exists(
                                                    Query::select()
                                                        .expr(Expr::val(1_i64))
                                                        .from_as(
                                                            "compaction_execution_stop",
                                                            "stop",
                                                        )
                                                        .and_where(
                                                            Expr::col(("stop", "owner"))
                                                                .eq(Expr::col(("c", "owner")))
                                                                .and(
                                                                    Expr::col(("stop", "turn_id"))
                                                                        .eq(Expr::col(("t", "id"))),
                                                                ),
                                                        )
                                                        .to_owned(),
                                                )
                                                .not(),
                                            ),
                                    )
                                    .to_owned(),
                            )),
                    )
                    .to_owned(),
            ))
            .await?;
        ensure!(
            self.connection
                .query_one_raw(statement(
                    &Query::select()
                        .expr(Expr::col("id"))
                        .from("compaction_operation")
                        .and_where(
                            Expr::col("id")
                                .eq(Expr::Value(operation.into()))
                                .and(Expr::col("execution_turn").eq(Expr::Value(turn.into())))
                        )
                        .to_owned()
                ))
                .await?
                .is_some(),
            "execution turn binding conflicts with operation scope"
        );
        Ok(())
    }

    pub async fn compaction_prepare_runner(
        &self,
        operation: &str,
        budget: &ModelBudget,
        source_count: u64,
        reference_count: u64,
    ) -> Result<()> {
        ensure!(
            source_count > 0,
            "compaction needs a selected source or previous checkpoint"
        );
        let descriptor = serde_json::to_string(budget)?;
        ensure!(
            descriptor.len() <= SOURCE_PAGE_BYTES,
            "runner descriptor exceeds quantum"
        );
        let source_count = i64::try_from(source_count)?;
        let reference_count = i64::try_from(reference_count)?;
        self.connection
            .execute_raw(statement(
                &Query::insert()
                    .into_table("compaction_runner_plan")
                    .columns([
                        "operation_id",
                        "source_count",
                        "reference_count",
                        "descriptor",
                    ])
                    .select_from(
                        Query::select()
                            .expr(Expr::col("id"))
                            .expr(Expr::Value(source_count.into()))
                            .expr(Expr::Value(reference_count.into()))
                            .expr(Expr::Value(descriptor.clone().into()))
                            .from("compaction_operation")
                            .and_where(
                                Expr::col("id")
                                    .eq(Expr::Value(operation.into()))
                                    .and(Expr::col("status").eq(Expr::val("running"))),
                            )
                            .to_owned(),
                    )?
                    .on_conflict(
                        OnConflict::columns(["operation_id"])
                            .do_nothing()
                            .to_owned(),
                    )
                    .to_owned(),
            ))
            .await?;
        let exact = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("operation_id"))
                    .from("compaction_runner_plan")
                    .and_where(
                        Expr::col("operation_id")
                            .eq(Expr::Value(operation.into()))
                            .and(Expr::col("source_count").eq(Expr::Value(source_count.into())))
                            .and(
                                Expr::col("reference_count")
                                    .eq(Expr::Value(reference_count.into())),
                            )
                            .and(Expr::col("descriptor").eq(Expr::Value(descriptor.into()))),
                    )
                    .to_owned(),
            ))
            .await?;
        ensure!(exact.is_some(), "runner plan idempotency conflict");
        Ok(())
    }
    pub async fn compaction_runner_plan(
        &self,
        operation: &str,
    ) -> Result<Option<RunnerPlanRecord>> {
        let Some(row) = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("source_count"))
                    .expr(Expr::col("reference_count"))
                    .expr(Expr::col("descriptor"))
                    .expr(Expr::col("ready"))
                    .from("compaction_runner_plan")
                    .and_where(Expr::col("operation_id").eq(Expr::Value(operation.into())))
                    .to_owned(),
            ))
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(RunnerPlanRecord {
            source_count: u64::try_from(row.try_get::<i64>("", "source_count")?)?,
            reference_count: u64::try_from(row.try_get::<i64>("", "reference_count")?)?,
            budget: serde_json::from_str(&row.try_get::<String>("", "descriptor")?)?,
            ready: row.try_get::<i64>("", "ready")? != 0,
        }))
    }
    pub async fn compaction_append_manifest(
        &self,
        operation: &str,
        entries: &[ManifestEntry],
    ) -> Result<()> {
        ensure!(
            entries.len() <= SOURCE_PAGE_ROWS as usize,
            "manifest batch exceeds row quantum"
        );
        let mut bytes = 0;
        let prepared: Vec<(Statement, Statement)> = entries
            .iter()
            .map(|entry| {
                bytes += entry.thread_id.len()
                    + entry.source.scope.len()
                    + entry.source.id.len()
                    + entry.source.version.len();
                let values: Vec<sea_orm::Value> = vec![
                    operation.into(),
                    i64::try_from(entry.ordinal)?.into(),
                    i64::try_from(entry.unit)?.into(),
                    (entry.reference_only as i64).into(),
                    entry.thread_id.clone().into(),
                    entry.source.scope.clone().into(),
                    entry.source.id.clone().into(),
                    entry.source.version.clone().into(),
                ];
                Ok((
                    statement(
                        &Query::insert()
                            .into_table("compaction_manifest")
                            .columns([
                                "operation_id",
                                "ordinal",
                                "unit_ordinal",
                                "reference_only",
                                "source_thread",
                                "source_scope",
                                "source_id",
                                "source_version",
                            ])
                            .values_panic([
                                Expr::Value(values[0].clone()),
                                Expr::Value(values[1].clone()),
                                Expr::Value(values[2].clone()),
                                Expr::Value(values[3].clone()),
                                Expr::Value(values[4].clone()),
                                Expr::Value(values[5].clone()),
                                Expr::Value(values[6].clone()),
                                Expr::Value(values[7].clone()),
                            ])
                            .on_conflict(
                                OnConflict::columns(["operation_id", "ordinal"])
                                    .do_nothing()
                                    .to_owned(),
                            )
                            .to_owned(),
                    ),
                    statement(
                        &Query::select()
                            .expr(Expr::col("ordinal"))
                            .from("compaction_manifest")
                            .and_where(
                                Expr::col("operation_id")
                                    .eq(Expr::Value(values[0].clone()))
                                    .and(Expr::col("ordinal").eq(Expr::Value(values[1].clone())))
                                    .and(
                                        Expr::col("unit_ordinal")
                                            .eq(Expr::Value(values[2].clone())),
                                    )
                                    .and(
                                        Expr::col("reference_only")
                                            .eq(Expr::Value(values[3].clone())),
                                    )
                                    .and(
                                        Expr::col("source_thread")
                                            .eq(Expr::Value(values[4].clone())),
                                    )
                                    .and(
                                        Expr::col("source_scope")
                                            .eq(Expr::Value(values[5].clone())),
                                    )
                                    .and(Expr::col("source_id").eq(Expr::Value(values[6].clone())))
                                    .and(
                                        Expr::col("source_version")
                                            .eq(Expr::Value(values[7].clone())),
                                    ),
                            )
                            .to_owned(),
                    ),
                ))
            })
            .collect::<Result<_>>()?;
        ensure!(
            bytes <= SOURCE_PAGE_BYTES,
            "manifest batch exceeds byte quantum"
        );
        // Prepare only metadata statements outside writer capacity. The insert
        // rechecks live ownership and never overwrites an existing revision;
        // edits racing admission therefore remain detectable at final CAS.
        let seeds = entries
            .iter()
            .map(|entry| -> Result<Option<Statement>> {
                let (prefix, turn) = entry
                    .source
                    .scope
                    .split_once(':')
                    .ok_or_else(|| anyhow::anyhow!("invalid manifest source scope"))?;
                let kind = match prefix {
                    "input" => super::CanonicalSource::Input,
                    "event" => super::CanonicalSource::Event,
                    "context" => super::CanonicalSource::ProviderContext,
                    "item" => super::CanonicalSource::ToolItem,
                    "checkpoint" | "task-basis" => return Ok(None),
                    _ => anyhow::bail!("unsupported manifest source"),
                };
                let revision = entry
                    .source
                    .version
                    .strip_prefix(&format!("{}:", kind.version_prefix()))
                    .ok_or_else(|| anyhow::anyhow!("manifest source needs a revision"))?
                    .parse::<u64>()?;
                ensure!(revision > 0, "invalid source revision");
                Ok(Some(statement(
                    &Query::insert()
                        .into_table(Alias::new(kind.revisions()))
                        .columns(["source_id", "turn_id", "revision", "present"])
                        .select_from(
                            Query::select()
                                .expr(Expr::col(("s", "id")))
                                .expr(Expr::col(("s", "turn_id")))
                                .expr(Expr::val(1_i64))
                                .expr(Expr::val(1_i64))
                                .from_as(Alias::new(kind.table()), "s")
                                .join_as(
                                    JoinType::InnerJoin,
                                    "turn",
                                    "t",
                                    Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "thread",
                                    "th",
                                    Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_context",
                                    "c",
                                    Expr::col(("c", "workspace_id"))
                                        .eq(Expr::col(("th", "workspace_id"))),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_operation",
                                    "o",
                                    Expr::col(("o", "owner")).eq(Expr::col(("c", "owner"))),
                                )
                                .and_where(
                                    Expr::col(("o", "id"))
                                        .eq(Expr::Value(operation.into()))
                                        .and(
                                            Expr::col(("s", "id"))
                                                .eq(Expr::Value(entry.source.id.clone().into())),
                                        )
                                        .and(
                                            Expr::col(("s", "turn_id"))
                                                .eq(Expr::Value(turn.into())),
                                        )
                                        .and(
                                            Expr::col(("th", "id"))
                                                .eq(Expr::Value(entry.thread_id.clone().into())),
                                        ),
                                )
                                .to_owned(),
                        )?
                        .on_conflict(OnConflict::columns(["source_id"]).do_nothing().to_owned())
                        .to_owned(),
                )))
            })
            .collect::<Result<Vec<_>>>()?;
        self.run_serialized_write(|| async {
            let txn = self.connection.begin().await?;
            let open = txn
                .query_one_raw(statement(
                    &Query::select()
                        .expr(Expr::col(("p", "operation_id")))
                        .from_as("compaction_runner_plan", "p")
                        .join_as(
                            JoinType::InnerJoin,
                            "compaction_operation",
                            "o",
                            Expr::col(("o", "id")).eq(Expr::col(("p", "operation_id"))),
                        )
                        .and_where(
                            Expr::col(("p", "operation_id"))
                                .eq(Expr::Value(operation.into()))
                                .and(Expr::col(("p", "ready")).eq(Expr::val(0_i64)))
                                .and(Expr::col(("o", "status")).eq(Expr::val("running"))),
                        )
                        .to_owned(),
                ))
                .await?;
            ensure!(open.is_some(), "manifest is immutable or operation stopped");
            for seed in seeds.iter().flatten() {
                txn.execute_raw(seed.clone()).await?;
            }
            for (insert, exact) in &prepared {
                txn.execute_raw(insert.clone()).await?;
                ensure!(
                    txn.query_one_raw(exact.clone()).await?.is_some(),
                    "manifest idempotency conflict"
                );
            }
            txn.commit().await?;
            Ok(())
        })
        .await
    }
    pub async fn compaction_activate_runner(
        &self,
        operation: &str,
        initial: &RunnerState,
    ) -> Result<()> {
        ensure!(
            initial.generation == 0
                && initial.attempts == 0
                && initial.retries == 0
                && initial.corrections == 0,
            "invalid initial runner state"
        );
        let encoded = serde_json::to_string(initial)?;
        let deadline = i64::try_from(initial.deadline_ms)?;
        self.run_serialized_write(|| async {
            let txn = self.connection.begin().await?;
            let valid = txn
                .query_one_raw(statement(
                    &Query::select()
                        .expr(Expr::col(("p", "operation_id")))
                        .from_as("compaction_runner_plan", "p")
                        .join_as(
                            JoinType::InnerJoin,
                            "compaction_operation",
                            "o",
                            Expr::col(("o", "id")).eq(Expr::col(("p", "operation_id"))),
                        )
                        .and_where(
                            Expr::col(("p", "operation_id"))
                                .eq(Expr::Value(operation.into()))
                                .and(Expr::col(("o", "status")).eq(Expr::val("running")))
                                .and(
                                    Expr::col(("o", "deadline_ms"))
                                        .eq(Expr::Value(deadline.into())),
                                )
                                .and(
                                    Expr::col(("o", "expected_head"))
                                        .binary(
                                            BinOper::Is,
                                            Expr::Value(initial.previous_checkpoint.clone().into()),
                                        )
                                        .or(Expr::Value(
                                            initial.previous_checkpoint.clone().into(),
                                        )
                                        .binary(BinOper::Is, Expr::val(Option::<String>::None))
                                        .and(
                                            Expr::exists(
                                                Query::select()
                                                    .expr(Expr::val(1_i64))
                                                    .from_as("compaction_live_sources", "s")
                                                    .and_where(
                                                        Expr::col(("s", "source_scope"))
                                                            .eq(Expr::val("checkpoint:").binary(
                                                                BinOper::Custom("||"),
                                                                Expr::col(("o", "owner")),
                                                            ))
                                                            .and(Expr::col(("s", "source_id")).eq(
                                                                Expr::col(("o", "expected_head")),
                                                            )),
                                                    )
                                                    .to_owned(),
                                            )
                                            .not(),
                                        )),
                                )
                                .and(
                                    Expr::SubQuery(
                                        None,
                                        Box::new(
                                            Query::select()
                                                .expr(Expr::expr(
                                                    Func::cust(Alias::new("count"))
                                                        .args([Expr::col(Asterisk)]),
                                                ))
                                                .from_as("compaction_manifest", "m")
                                                .and_where(
                                                    Expr::col(("m", "operation_id"))
                                                        .eq(Expr::col(("p", "operation_id")))
                                                        .and(
                                                            Expr::col(("m", "reference_only"))
                                                                .eq(Expr::val(0_i64)),
                                                        ),
                                                )
                                                .to_owned()
                                                .into(),
                                        ),
                                    )
                                    .eq(Expr::col(("p", "source_count"))),
                                )
                                .and(
                                    Expr::SubQuery(
                                        None,
                                        Box::new(
                                            Query::select()
                                                .expr(Expr::expr(
                                                    Func::cust(Alias::new("count"))
                                                        .args([Expr::col(Asterisk)]),
                                                ))
                                                .from_as("compaction_manifest", "m")
                                                .and_where(
                                                    Expr::col(("m", "operation_id"))
                                                        .eq(Expr::col(("p", "operation_id")))
                                                        .and(
                                                            Expr::col(("m", "reference_only"))
                                                                .eq(Expr::val(1_i64)),
                                                        ),
                                                )
                                                .to_owned()
                                                .into(),
                                        ),
                                    )
                                    .eq(Expr::col(("p", "reference_count"))),
                                )
                                .and(
                                    Expr::exists(
                                        Query::select()
                                            .expr(Expr::val(1_i64))
                                            .from_as("compaction_manifest", "m")
                                            .and_where(
                                                Expr::col(("m", "operation_id"))
                                                    .eq(Expr::col(("p", "operation_id")))
                                                    .and(
                                                        Expr::col(("m", "ordinal"))
                                                            .lt(Expr::val(0_i64))
                                                            .or(Expr::col(("m", "ordinal")).gte(
                                                                Expr::col(("p", "source_count"))
                                                                    .add(Expr::col((
                                                                        "p",
                                                                        "reference_count",
                                                                    ))),
                                                            )),
                                                    ),
                                            )
                                            .to_owned(),
                                    )
                                    .not(),
                                ),
                        )
                        .to_owned(),
                ))
                .await?;
            ensure!(
                valid.is_some(),
                "manifest is incomplete or admission expired"
            );
            txn.execute_raw(statement(
                &Query::insert()
                    .into_table("compaction_runner_state")
                    .columns(["operation_id", "generation", "state"])
                    .values_panic([
                        Expr::Value(operation.into()),
                        Expr::val(0_i64),
                        Expr::Value(encoded.clone().into()),
                    ])
                    .on_conflict(
                        OnConflict::columns(["operation_id"])
                            .do_nothing()
                            .to_owned(),
                    )
                    .to_owned(),
            ))
            .await?;
            txn.execute_raw(statement(
                &Query::update()
                    .table("compaction_runner_plan")
                    .value("ready", Expr::val(1_i64))
                    .and_where(Expr::col("operation_id").eq(Expr::Value(operation.into())))
                    .to_owned(),
            ))
            .await?;
            txn.commit().await?;
            Ok(())
        })
        .await
    }
    pub async fn compaction_runner_state(&self, operation: &str) -> Result<Option<RunnerState>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("state"))
                    .from("compaction_runner_state")
                    .and_where(Expr::col("operation_id").eq(Expr::Value(operation.into())))
                    .to_owned(),
            ))
            .await?;
        row.map(|row| Ok(serde_json::from_str(&row.try_get::<String>("", "state")?)?))
            .transpose()
    }
    /// Complete the state record after a control-plane terminal fence. No
    /// attempt can advance past that fence. Preparation reads one bounded row;
    /// the write revalidates its generation and durable terminal classification.
    pub async fn compaction_reconcile_runner_state(
        &self,
        operation: &str,
    ) -> Result<Option<RunnerState>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "state")))
                    .expr(Expr::col(("o", "status")))
                    .expr(Expr::col(("o", "outcome")))
                    .from_as("compaction_runner_state", "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_operation",
                        "o",
                        Expr::col(("o", "id")).eq(Expr::col(("s", "operation_id"))),
                    )
                    .and_where(Expr::col(("s", "operation_id")).eq(Expr::Value(operation.into())))
                    .to_owned(),
            ))
            .await?;
        let Some(row) = row else { return Ok(None) };
        let state: RunnerState = serde_json::from_str(&row.try_get::<String>("", "state")?)?;
        let status: String = row.try_get("", "status")?;
        let outcome: Option<String> = row.try_get("", "outcome")?;
        if status == "running"
            || status == "completed"
            || matches!(
                state.phase,
                RunnerPhase::Failed { .. } | RunnerPhase::Applied { .. }
            )
        {
            return Ok(Some(state));
        }
        let kind = if status == "cancelled" {
            FailureKind::Cancelled
        } else if outcome.as_deref() == Some("deadline") {
            FailureKind::Deadline
        } else {
            FailureKind::Permanent
        };
        let terminal = state.terminate(kind)?;
        let encoded = serde_json::to_string(&terminal)?;
        ensure!(
            encoded.len() <= SOURCE_PAGE_BYTES,
            "runner state exceeds quantum"
        );
        let changed =
            self.connection
                .execute_raw(statement(
                    &Query::update()
                        .table("compaction_runner_state")
                        .value(
                            "generation",
                            Expr::Value(i64::try_from(terminal.generation)?.into()),
                        )
                        .value("state", Expr::Value(encoded.into()))
                        .and_where(
                            Expr::col("operation_id")
                                .eq(Expr::Value(operation.into()))
                                .and(
                                    Expr::col("generation")
                                        .eq(Expr::Value(i64::try_from(state.generation)?.into())),
                                )
                                .and(Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as("compaction_operation", "o")
                                        .and_where(
                                            Expr::col(("o", "id"))
                                                .eq(Expr::Value(operation.into()))
                                                .and(
                                                    Expr::col(("o", "status"))
                                                        .eq(Expr::Value(status.into())),
                                                )
                                                .and(Expr::col(("o", "outcome")).binary(
                                                    BinOper::Is,
                                                    Expr::Value(outcome.into()),
                                                ))
                                                .and(Expr::col(("o", "status")).is_in([
                                                    "cancelled",
                                                    "failed",
                                                    "stale",
                                                ])),
                                        )
                                        .to_owned(),
                                )),
                        )
                        .to_owned(),
                ))
                .await?;
        if changed.rows_affected() == 1 {
            return Ok(Some(terminal));
        }
        // A concurrent reconciler may have won the same idempotent transition.
        self.compaction_runner_state(operation).await
    }

    pub async fn compaction_manifest_page(
        &self,
        operation: &str,
        reference_only: bool,
        unit: u64,
        source_offset: u32,
    ) -> Result<Vec<ManifestEntry>> {
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col("ordinal"))
                    .expr(Expr::col("unit_ordinal"))
                    .expr(Expr::col("reference_only"))
                    .expr(Expr::col("source_thread"))
                    .expr(Expr::col("source_scope"))
                    .expr(Expr::col("source_id"))
                    .expr(Expr::col("source_version"))
                    .from("compaction_manifest")
                    .and_where(
                        Expr::col("operation_id")
                            .eq(Expr::Value(operation.into()))
                            .and(
                                Expr::col("reference_only")
                                    .eq(Expr::Value((reference_only as i64).into())),
                            )
                            .and(
                                Expr::col("unit_ordinal")
                                    .gte(Expr::Value(i64::try_from(unit)?.into())),
                            ),
                    )
                    .order_by_expr(Expr::col("unit_ordinal"), Order::Asc)
                    .order_by_expr(Expr::col("ordinal"), Order::Asc)
                    .limit(u64::try_from(SOURCE_PAGE_ROWS)?)
                    .offset(u64::try_from(i64::from(source_offset))?)
                    .to_owned(),
            ))
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ManifestEntry {
                    ordinal: u64::try_from(row.try_get::<i64>("", "ordinal")?)?,
                    unit: u64::try_from(row.try_get::<i64>("", "unit_ordinal")?)?,
                    reference_only: row.try_get::<i64>("", "reference_only")? != 0,
                    thread_id: row.try_get("", "source_thread")?,
                    source: SourceRef {
                        scope: row.try_get("", "source_scope")?,
                        id: row.try_get("", "source_id")?,
                        version: row.try_get("", "source_version")?,
                    },
                })
            })
            .collect()
    }
    /// Preparation uses immutable values only. Generation, counters, ownership,
    /// deadline and running status are revalidated atomically with candidate save.
    pub async fn compaction_runner_transition(
        &self,
        operation: &str,
        expected: u64,
        next: &RunnerState,
        candidate: Option<&Checkpoint>,
    ) -> Result<bool> {
        ensure!(
            next.generation
                == expected
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("generation overflow"))?,
            "invalid state generation"
        );
        ensure!(
            !matches!(next.phase, RunnerPhase::Applied { .. })
                && next.retries <= 2
                && next.corrections <= 1,
            "invalid state transition"
        );
        let state = serde_json::to_string(next)?;
        ensure!(
            state.len() <= SOURCE_PAGE_BYTES,
            "runner state exceeds quantum"
        );
        let mut candidate_sql = Vec::new();
        let mut identity_query = None;
        if let Some(cp) = candidate {
            ensure!(
                cp.operation_id == operation
                    && cp.coverage.len() <= CHECKPOINT_SOURCE_LIMIT
                    && cp.format_version == 1,
                "invalid candidate scope or quantum"
            );
            ensure!(
                matches!(&next.phase,RunnerPhase::Candidate{checkpoint,..} if checkpoint == &cp.id),
                "candidate/state identity mismatch"
            );
            // Token cap bounds the answer; accommodate even long whitespace tokens.
            ensure!(
                cp.summary.len() <= 13_107 * 128 && !cp.summary.trim().is_empty(),
                "invalid candidate text"
            );
            let identity = checkpoint_identity(cp)?;
            let values: Vec<sea_orm::Value> = vec![
                cp.id.clone().into(),
                operation.into(),
                cp.owner.clone().into(),
                cp.previous.clone().into(),
                i64::try_from(next.attempts)?.into(),
                cp.summary.clone().into(),
                serde_json::to_string(&cp.selection)?.into(),
                i64::try_from(cp.projection_version)?.into(),
                identity.clone().into(),
            ];
            candidate_sql.push(statement(
                &Query::insert()
                    .into_table("compaction_checkpoint")
                    .columns([
                        "id",
                        "operation_id",
                        "owner",
                        "previous",
                        "portion",
                        "summary",
                        "selection",
                        "projection_version",
                        "format_version",
                        "identity_sha256",
                        "status",
                    ])
                    .values_panic([
                        Expr::Value(values[0].clone()),
                        Expr::Value(values[1].clone()),
                        Expr::Value(values[2].clone()),
                        Expr::Value(values[3].clone()),
                        Expr::Value(values[4].clone()),
                        Expr::Value(values[5].clone()),
                        Expr::Value(values[6].clone()),
                        Expr::Value(values[7].clone()),
                        Expr::val(1_i64),
                        Expr::Value(values[8].clone()),
                        Expr::val("candidate"),
                    ])
                    .on_conflict(
                        OnConflict::columns(["operation_id", "portion"])
                            .do_nothing()
                            .to_owned(),
                    )
                    .to_owned(),
            ));
            identity_query = Some(statement(
                &Query::select()
                    .expr(Expr::col(("p", "id")))
                    .from_as("compaction_checkpoint", "p")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_operation",
                        "o",
                        Expr::col(("o", "id")).eq(Expr::col(("p", "operation_id"))),
                    )
                    .and_where(
                        Expr::col(("p", "id"))
                            .eq(Expr::Value(cp.id.clone().into()))
                            .and(
                                Expr::col(("p", "identity_sha256"))
                                    .eq(Expr::Value(identity.into())),
                            )
                            .and(Expr::col(("p", "operation_id")).eq(Expr::Value(operation.into())))
                            .and(Expr::col(("p", "owner")).eq(Expr::col(("o", "owner"))))
                            .and(Expr::col(("p", "projection_version")).eq(Expr::expr(
                                Func::cust(Alias::new("json_extract")).args([
                                    Expr::col(("o", "snapshot")),
                                    Expr::val("$.projection_version"),
                                ]),
                            )))
                            .and(
                                Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                    Expr::col(("p", "selection")),
                                    Expr::val("$.transport"),
                                ]))
                                .eq(Expr::expr(
                                    Func::cust(Alias::new("json_extract")).args([
                                        Expr::col(("o", "snapshot")),
                                        Expr::val("$.admission.selection.transport"),
                                    ]),
                                )),
                            )
                            .and(
                                Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                    Expr::col(("p", "selection")),
                                    Expr::val("$.instance"),
                                ]))
                                .eq(Expr::expr(
                                    Func::cust(Alias::new("json_extract")).args([
                                        Expr::col(("o", "snapshot")),
                                        Expr::val("$.admission.selection.instance"),
                                    ]),
                                )),
                            )
                            .and(
                                Expr::expr(
                                    Func::cust(Alias::new("json_extract")).args([
                                        Expr::col(("p", "selection")),
                                        Expr::val("$.model"),
                                    ]),
                                )
                                .eq(Expr::expr(
                                    Func::cust(Alias::new("json_extract")).args([
                                        Expr::col(("o", "snapshot")),
                                        Expr::val("$.admission.selection.model"),
                                    ]),
                                )),
                            )
                            .and(
                                Expr::expr(
                                    Func::cust(Alias::new("json_extract")).args([
                                        Expr::col(("p", "selection")),
                                        Expr::val("$.effort"),
                                    ]),
                                )
                                .binary(
                                    BinOper::Is,
                                    Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                        Expr::col(("o", "snapshot")),
                                        Expr::val("$.admission.selection.effort"),
                                    ])),
                                ),
                            ),
                    )
                    .to_owned(),
            ));
            for source in &cp.coverage {
                candidate_sql.push(statement(
                    &Query::insert()
                        .into_table("compaction_coverage")
                        .columns([
                            "checkpoint_id",
                            "source_scope",
                            "source_id",
                            "source_version",
                        ])
                        .values_panic([
                            Expr::Value(cp.id.clone().into()),
                            Expr::Value(source.scope.clone().into()),
                            Expr::Value(source.id.clone().into()),
                            Expr::Value(source.version.clone().into()),
                        ])
                        .on_conflict(OnConflict::new().do_nothing().to_owned())
                        .to_owned(),
                ));
            }
        }
        let observation_sql = next
            .observation
            .as_ref()
            .map(|observation| -> Result<Statement> {
                ensure!(
                    observation.number == next.attempts,
                    "attempt observation identity mismatch"
                );
                Ok(statement(
                    &Query::insert()
                        .into_table("compaction_attempt_observation")
                        .columns(["operation_id", "attempt", "observation"])
                        .values_panic([
                            Expr::Value(operation.into()),
                            Expr::Value(i64::try_from(observation.number)?.into()),
                            Expr::Value(serde_json::to_string(observation)?.into()),
                        ])
                        .on_conflict(
                            OnConflict::columns(["operation_id", "attempt"])
                                .update_column("observation")
                                .to_owned(),
                        )
                        .to_owned(),
                ))
            })
            .transpose()?;
        let basis_query = candidate.map(|cp| {
            statement(
                &Query::select()
                    .expr(Expr::col("operation_id"))
                    .from("compaction_runner_state")
                    .and_where(
                        Expr::col("operation_id")
                            .eq(Expr::Value(operation.into()))
                            .and(Expr::col("generation").eq(Expr::Value(
                                i64::try_from(expected).unwrap_or(i64::MAX).into(),
                            )))
                            .and(
                                Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                    Expr::col("state"),
                                    Expr::val("$.previous_checkpoint"),
                                ]))
                                .binary(BinOper::Is, Expr::Value(cp.previous.clone().into())),
                            ),
                    )
                    .to_owned(),
            )
        });
        let (status, outcome) = match &next.phase {
            RunnerPhase::Failed {
                kind: FailureKind::Cancelled,
            } => ("cancelled", Some("cancelled")),
            RunnerPhase::Failed { .. } => ("failed", Some("runner_failed")),
            _ => ("running", None),
        };
        self.run_serialized_write(|| async {
            let txn = self.connection.begin().await?;
            if let Some(query) = &basis_query {
                if txn.query_one_raw(query.clone()).await?.is_none() {
                    txn.rollback().await?;
                    return Ok(false);
                }
            }
            let updated = txn
                .execute_raw(statement(
                    &Query::update()
                        .table("compaction_runner_state")
                        .value(
                            "generation",
                            Expr::Value(i64::try_from(next.generation)?.into()),
                        )
                        .value("state", Expr::Value(state.clone().into()))
                        .and_where(
                            Expr::col("operation_id")
                                .eq(Expr::Value(operation.into()))
                                .and(
                                    Expr::col("generation")
                                        .eq(Expr::Value(i64::try_from(expected)?.into())),
                                )
                                .and(Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as("compaction_operation", "o")
                                        .join_as(
                                            JoinType::InnerJoin,
                                            "compaction_runner_plan",
                                            "p",
                                            Expr::col(("p", "operation_id"))
                                                .eq(Expr::col(("o", "id"))),
                                        )
                                        .and_where(
                                            Expr::col(("o", "id"))
                                                .eq(Expr::Value(operation.into()))
                                                .and(
                                                    Expr::col(("o", "status"))
                                                        .eq(Expr::val("running")),
                                                )
                                                .and(Expr::col(("p", "ready")).eq(Expr::val(1_i64)))
                                                .and(Expr::col(("o", "deadline_ms")).eq(
                                                    Expr::Value(
                                                        i64::try_from(next.deadline_ms)?.into(),
                                                    ),
                                                ))
                                                .and(Expr::col(("o", "attempts")).lte(Expr::Value(
                                                    i64::try_from(next.attempts)?.into(),
                                                )))
                                                .and(
                                                    Expr::col(("o", "attempts"))
                                                        .add(Expr::val(1_i64))
                                                        .gte(Expr::Value(
                                                            i64::try_from(next.attempts)?.into(),
                                                        )),
                                                )
                                                .and(Expr::col(("o", "transient_retries")).lte(
                                                    Expr::Value(i64::from(next.retries).into()),
                                                ))
                                                .and(Expr::col(("o", "correction")).lte(
                                                    Expr::Value(i64::from(next.corrections).into()),
                                                )),
                                        )
                                        .to_owned(),
                                )),
                        )
                        .to_owned(),
                ))
                .await?;
            if updated.rows_affected() != 1 {
                txn.rollback().await?;
                return Ok(false);
            }
            if let Some(statement) = &observation_sql {
                txn.execute_raw(statement.clone()).await?;
            }
            for statement in &candidate_sql {
                txn.execute_raw(statement.clone()).await?;
            }
            if let Some(query) = &identity_query {
                ensure!(
                    txn.query_one_raw(query.clone()).await?.is_some(),
                    "candidate identity conflict"
                );
            }
            txn.execute_raw(statement(
                &Query::update()
                    .table("compaction_operation")
                    .value("status", Expr::Value(status.into()))
                    .value("outcome", Expr::Value(outcome.map(str::to_owned).into()))
                    .value(
                        "attempts",
                        Expr::Value(i64::try_from(next.attempts)?.into()),
                    )
                    .value(
                        "transient_retries",
                        Expr::Value(i64::from(next.retries).into()),
                    )
                    .value(
                        "correction",
                        Expr::Value(i64::from(next.corrections).into()),
                    )
                    .and_where(Expr::col("id").eq(Expr::Value(operation.into())))
                    .to_owned(),
            ))
            .await?;
            txn.commit().await?;
            Ok(true)
        })
        .await
    }
}

impl CrudStore {
    /// Prepare only the exact predecessor chain in bounded one-row quanta.
    /// `retained` rows are invisible in live_sources while their operation is
    /// running. The final operation/head CAS makes the whole prepared chain
    /// visible atomically. Generation checks prevent a stale preparer from
    /// publishing; cancellation/restart may safely leave invisible markers.
    async fn prepare_checkpoint_ancestry(
        &self,
        operation: &str,
        checkpoint: &str,
        generation: u64,
    ) -> Result<()> {
        let mut cursor = Some(checkpoint.to_owned());
        let mut visited = std::collections::BTreeSet::new();
        while let Some(id) = cursor.take() {
            ensure!(visited.insert(id.clone()), "cyclic candidate ancestry");
            let row = self
                .connection
                .query_one_raw(statement(
                    &Query::select()
                        .expr(Expr::col("operation_id"))
                        .expr(Expr::col("previous"))
                        .from("compaction_checkpoint")
                        .and_where(Expr::col("id").eq(Expr::Value(id.clone().into())))
                        .to_owned(),
                ))
                .await?
                .ok_or_else(|| anyhow::anyhow!("candidate ancestry is missing"))?;
            let source_operation: String = row.try_get("", "operation_id")?;
            if source_operation != operation {
                break;
            }
            cursor = row.try_get("", "previous")?;
            self.connection
                .execute_raw(statement(
                    &Query::update()
                        .table("compaction_checkpoint")
                        .value("status", Expr::val("retained"))
                        .and_where(
                            Expr::col("id")
                                .eq(Expr::Value(id.into()))
                                .and(Expr::col("operation_id").eq(Expr::Value(operation.into())))
                                .and(Expr::col("status").is_in(["candidate", "retained"]))
                                .and(Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as("compaction_operation", "o")
                                        .join_as(
                                            JoinType::InnerJoin,
                                            "compaction_runner_state",
                                            "s",
                                            Expr::col(("s", "operation_id"))
                                                .eq(Expr::col(("o", "id"))),
                                        )
                                        .and_where(
                                            Expr::col(("o", "id"))
                                                .eq(Expr::Value(operation.into()))
                                                .and(
                                                    Expr::col(("o", "status"))
                                                        .eq(Expr::val("running")),
                                                )
                                                .and(Expr::col(("s", "generation")).eq(
                                                    Expr::Value(i64::try_from(generation)?.into()),
                                                ))
                                                .and(
                                                    Expr::expr(
                                                        Func::cust(Alias::new("json_extract"))
                                                            .args([
                                                                Expr::col(("s", "state")),
                                                                Expr::val(
                                                                    "$.phase.Commit.checkpoint",
                                                                ),
                                                            ]),
                                                    )
                                                    .eq(Expr::Value(checkpoint.into())),
                                                ),
                                        )
                                        .to_owned(),
                                )),
                        )
                        .to_owned(),
                ))
                .await?;
        }
        Ok(())
    }

    /// One atomic domain transition. The immutable, admitted manifest bounds
    /// validation to this operation's selected sources; it never scans transcript
    /// payloads or the complete history. Every source version and the owner head
    /// are checked inside the same transaction which publishes the candidate.
    pub async fn compaction_apply_runner(
        &self,
        operation: &str,
        state: &RunnerState,
        expected_head: Option<&str>,
    ) -> Result<super::CommitOutcome> {
        let RunnerPhase::Commit { checkpoint } = &state.phase else {
            anyhow::bail!("runner has no commit candidate")
        };
        let applied = state.applied(checkpoint)?;
        let encoded = serde_json::to_string(&applied)?;
        self.prepare_checkpoint_ancestry(operation, checkpoint, state.generation)
            .await?;
        self.run_serialized_write(|| async {
            let txn = self.connection.begin().await?;
            let row = txn.query_one_raw(statement(&Query::select().expr(Expr::col(("o", "status"))).from_as("compaction_operation", "o").join_as(JoinType::InnerJoin, "compaction_checkpoint", "p", Expr::col(("p", "operation_id")).eq(Expr::col(("o", "id"))).and(Expr::col(("p", "owner")).eq(Expr::col(("o", "owner"))))).and_where(Expr::col(("o", "id")).eq(Expr::Value(operation.into())).and(Expr::col(("p", "id")).eq(Expr::Value(checkpoint.clone().into()))).and(Expr::col(("o", "expected_head")).binary(BinOper::Is, Expr::Value(expected_head.map(str::to_owned).into())))).to_owned())).await?;
            let Some(row) = row else { txn.rollback().await?; return Ok(super::CommitOutcome::Stale) };
            let status: String = row.try_get("","status")?;
            if status == "completed" {
                let exact = txn.query_one_raw(statement(&Query::select().expr(Expr::col("id")).from("compaction_checkpoint").and_where(Expr::col("id").eq(Expr::Value(checkpoint.clone().into())).and(Expr::col("status").eq(Expr::val("applied")))).to_owned())).await?;
                txn.rollback().await?;
                return Ok(if exact.is_some() { super::CommitOutcome::AlreadyApplied } else { super::CommitOutcome::Stale });
            }
            if status != "running" { txn.rollback().await?; return Ok(super::CommitOutcome::Cancelled) }
            let interrupted = txn.query_one_raw(statement(&Query::select().expr(Expr::col(("o", "id"))).from_as("compaction_operation", "o").join_as(JoinType::InnerJoin, "compaction_context", "c", Expr::col(("c", "owner")).eq(Expr::col(("o", "owner")))).and_where(Expr::col(("o", "id")).eq(Expr::Value(operation.into())).and(Expr::col(("o", "execution_turn")).binary(BinOper::Is, Expr::val(Option::<String>::None)).not()).and(Expr::exists(Query::select().expr(Expr::val(1_i64)).from_as("compaction_execution_stop", "stop").and_where(Expr::col(("stop", "owner")).eq(Expr::col(("c", "owner"))).and(Expr::col(("stop", "turn_id")).eq(Expr::col(("o", "execution_turn"))))).to_owned()).or(Expr::exists(Query::select().expr(Expr::val(1_i64)).from_as("turn", "t").and_where(Expr::col(("t", "id")).eq(Expr::col(("o", "execution_turn"))).and(Expr::col(("t", "status")).is_in(["interrupted", "cancelled"]).not())).to_owned()).not()))).to_owned())).await?;
            if interrupted.is_some() { txn.rollback().await?; return Ok(super::CommitOutcome::Cancelled) }

            let generation = txn.query_one_raw(statement(&Query::select().expr(Expr::col("operation_id")).from("compaction_runner_state").and_where(Expr::col("operation_id").eq(Expr::Value(operation.into())).and(Expr::col("generation").eq(Expr::Value(i64::try_from(state.generation)?.into()))).and(Expr::expr(Func::cust(Alias::new("json_extract")).args([Expr::col("state"), Expr::val("$.phase.Commit.checkpoint")])).eq(Expr::Value(checkpoint.clone().into())))).to_owned())).await?;
            if generation.is_none() { txn.rollback().await?; return Ok(super::CommitOutcome::Stale) }
            // Exact coverage: every selected record has a fully read version,
            // and no reference-only or unselected source has acquired coverage.
            let missing = txn.query_one_raw(statement(&Query::select().expr(Expr::col(("m", "ordinal"))).from_as("compaction_manifest", "m").and_where(Expr::col(("m", "operation_id")).eq(Expr::Value(operation.into())).and(Expr::col(("m", "reference_only")).eq(Expr::val(0_i64))).and(Expr::exists(Query::select().expr(Expr::val(1_i64)).from_as("compaction_coverage", "c").join_as(JoinType::InnerJoin, "compaction_checkpoint", "p", Expr::col(("p", "id")).eq(Expr::col(("c", "checkpoint_id")))).and_where(Expr::col(("p", "operation_id")).eq(Expr::col(("m", "operation_id"))).and(Expr::col(("c", "source_scope")).eq(Expr::col(("m", "source_scope")))).and(Expr::col(("c", "source_id")).eq(Expr::col(("m", "source_id")))).and(Expr::col(("c", "source_version")).eq(Expr::col(("m", "source_version"))))).to_owned()).not())).limit(1).to_owned())).await?;
            let extra = txn.query_one_raw(statement(&Query::select().expr(Expr::col(("c", "source_id"))).from_as("compaction_coverage", "c").join_as(JoinType::InnerJoin, "compaction_checkpoint", "p", Expr::col(("p", "id")).eq(Expr::col(("c", "checkpoint_id")))).and_where(Expr::col(("p", "operation_id")).eq(Expr::Value(operation.into())).and(Expr::exists(Query::select().expr(Expr::val(1_i64)).from_as("compaction_manifest", "m").and_where(Expr::col(("m", "operation_id")).eq(Expr::col(("p", "operation_id"))).and(Expr::col(("m", "reference_only")).eq(Expr::val(0_i64))).and(Expr::col(("c", "source_scope")).eq(Expr::col(("m", "source_scope")))).and(Expr::col(("c", "source_id")).eq(Expr::col(("m", "source_id")))).and(Expr::col(("c", "source_version")).eq(Expr::col(("m", "source_version"))))).to_owned()).not())).limit(1).to_owned())).await?;
            let stale = txn.query_one_raw(sqlite_specific_sql("SELECT m.ordinal FROM compaction_manifest m JOIN compaction_operation o ON o.id=m.operation_id JOIN compaction_context owner ON owner.owner=o.owner WHERE m.operation_id=? AND (EXISTS (SELECT 1 FROM json_each(o.snapshot,'$.source_epochs') wanted WHERE COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id=wanted.key),0)<>wanted.value OR NOT EXISTS (SELECT 1 FROM thread t WHERE t.id=wanted.key AND t.workspace_id=owner.workspace_id)) OR COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id=owner.thread_id),0)<>json_extract(o.snapshot,'$.projection_version') OR NOT EXISTS (SELECT 1 FROM compaction_live_sources s WHERE s.source_scope=m.source_scope AND s.source_id=m.source_id AND s.source_version=m.source_version AND s.thread_id=m.source_thread AND (m.reference_only=1 OR s.thread_id=owner.thread_id OR EXISTS (SELECT 1 FROM compaction_operation_projection accepted JOIN compaction_frozen_history h ON h.id=accepted.manifest_id AND h.workspace_id=owner.workspace_id AND h.ready=1 AND h.identity_sha256=accepted.identity_sha256 AND h.imports_sha256=accepted.imports_sha256 AND h.import_count=accepted.import_count AND h.next_import=accepted.import_count JOIN compaction_frozen_import imported ON imported.manifest_id=h.id WHERE accepted.operation_id=o.id AND imported.source_scope=s.source_scope AND imported.source_id=s.source_id AND imported.source_version=s.source_version AND imported.source_thread=s.thread_id)) AND s.workspace_id=owner.workspace_id)) LIMIT 1",[operation.into()])).await?;
            if missing.is_some() || extra.is_some() || stale.is_some() { txn.rollback().await?; return Ok(super::CommitOutcome::Stale) }
            let changed = txn.execute_raw(statement(&Query::update().table("compaction_context").value("head", Expr::Value(checkpoint.clone().into())).and_where(Expr::col("owner").eq(Expr::SubQuery(None, Box::new(Query::select().expr(Expr::col("owner")).from("compaction_operation").and_where(Expr::col("id").eq(Expr::Value(operation.into()))).to_owned().into()))).and(Expr::col("head").binary(BinOper::Is, Expr::Value(expected_head.map(str::to_owned).into()))).and(Expr::col("format_version").eq(Expr::val(1_i64)))).to_owned())).await?;
            if changed.rows_affected() != 1 { txn.rollback().await?; return Ok(super::CommitOutcome::Stale) }
            txn.execute_raw(statement(&Query::update().table("compaction_checkpoint").value("status", Expr::val("applied")).and_where(Expr::col("id").eq(Expr::Value(checkpoint.clone().into()))).to_owned())).await?;
            txn.execute_raw(statement(&Query::update().table("compaction_operation").value("status", Expr::val("completed")).value("outcome", Expr::val("applied")).and_where(Expr::col("id").eq(Expr::Value(operation.into()))).to_owned())).await?;
            txn.execute_raw(statement(&Query::update().table("compaction_runner_state").value("generation", Expr::Value(i64::try_from(applied.generation)?.into())).value("state", Expr::Value(encoded.clone().into())).and_where(Expr::col("operation_id").eq(Expr::Value(operation.into())).and(Expr::col("generation").eq(Expr::Value(i64::try_from(state.generation)?.into())))).to_owned())).await?;
            txn.commit().await?;
            Ok(super::CommitOutcome::Applied)
        }).await
    }
}
