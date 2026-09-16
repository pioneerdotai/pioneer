use super::compaction::{
    CHECKPOINT_SOURCE_LIMIT, SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS, checkpoint_identity,
    sqlite_specific_sql,
};
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::runner::{FailureKind, RunnerPhase, RunnerState};
use pioneer_compaction::{Checkpoint, ModelBudget, SourceRef};
use pioneer_entity::{
    compaction_attempt_observation, compaction_checkpoint, compaction_context, compaction_coverage,
    compaction_execution_stop, compaction_manifest, compaction_operation, compaction_runner_plan,
    compaction_runner_state, thread, turn,
};
use sea_orm::sea_query::{
    Alias, Asterisk, BinOper, Expr, ExprTrait, Func, JoinType, OnConflict, Query,
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sea_orm::{ConnectionTrait, TransactionTrait};

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

/// Manifest admission contains metadata only and resumes in bounded batches.
/// No provider may run until activate_runner has checked the whole manifest.
/// The execution turn is an immutable admission boundary. Its terminal
/// interruption fences checkpoint publication even before service cleanup.
pub(crate) async fn compaction_execution_cancelled<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    Ok(compaction_operation::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_operation::Entity::belongs_to(compaction_context::Entity)
                .from(compaction_operation::Column::Owner)
                .to(compaction_context::Column::Owner)
                .into(),
        )
        .expr(Expr::col((
            compaction_operation::Entity,
            compaction_operation::Column::Id,
        )))
        .filter(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Id,
            ))
            .eq(Expr::Value(operation.into()))
            .and(
                Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::ExecutionTurn,
                ))
                .binary(BinOper::Is, Expr::val(Option::<String>::None))
                .not(),
            )
            .and(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_execution_stop::Entity, "stop")
                        .and_where(
                            Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                .eq(Expr::col((
                                    compaction_context::Entity,
                                    compaction_context::Column::Owner,
                                )))
                                .and(
                                    Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                        .eq(Expr::col((
                                            compaction_operation::Entity,
                                            compaction_operation::Column::ExecutionTurn,
                                        ))),
                                ),
                        )
                        .to_owned(),
                )
                .or(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(turn::Entity, "t")
                        .and_where(
                            Expr::col(("t", turn::Column::Id))
                                .eq(Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::ExecutionTurn,
                                )))
                                .and(
                                    Expr::col(("t", turn::Column::Status))
                                        .is_in(["interrupted", "cancelled"])
                                        .not(),
                                ),
                        )
                        .to_owned(),
                )
                .not()),
            ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}

pub(crate) async fn compaction_bind_execution_turn<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    turn: &str,
) -> Result<()> {
    compaction_operation::Entity::update_many()
        .col_expr(
            compaction_operation::Column::ExecutionTurn,
            Expr::Value(turn.into()),
        )
        .filter(
            Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(operation.into()))
                .and(
                    Expr::col(compaction_operation::Column::ExecutionTurn)
                        .binary(BinOper::Is, Expr::val(Option::<String>::None)),
                )
                .and(Expr::col(compaction_operation::Column::Status).eq(Expr::val("running")))
                .and(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_context::Entity, "c")
                        .join_as(
                            JoinType::InnerJoin,
                            turn::Entity,
                            "t",
                            Expr::col(("t", turn::Column::ThreadId))
                                .eq(Expr::col(("c", compaction_context::Column::ThreadId))),
                        )
                        .and_where(
                            Expr::col(("c", compaction_context::Column::Owner))
                                .eq(Expr::col(("compaction_operation", "owner")))
                                .and(
                                    Expr::col(("t", turn::Column::Id)).eq(Expr::Value(turn.into())),
                                )
                                .and(
                                    Expr::exists(
                                        Query::select()
                                            .expr(Expr::val(1_i64))
                                            .from_as(compaction_execution_stop::Entity, "stop")
                                            .and_where(
                                                Expr::col((
                                                    "stop",
                                                    compaction_execution_stop::Column::Owner,
                                                ))
                                                .eq(Expr::col((
                                                    "c",
                                                    compaction_context::Column::Owner,
                                                )))
                                                .and(
                                                    Expr::col((
                                                        "stop",
                                                        compaction_execution_stop::Column::TurnId,
                                                    ))
                                                    .eq(Expr::col(("t", turn::Column::Id))),
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
        .exec(db)
        .await?;
    ensure!(
        compaction_operation::Entity::find()
            .select_only()
            .expr(Expr::val(1_i64))
            .filter(
                Expr::col(compaction_operation::Column::Id)
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col(compaction_operation::Column::ExecutionTurn)
                            .eq(Expr::Value(turn.into()))
                    )
            )
            .into_tuple::<i64>()
            .one(db)
            .await?
            .is_some(),
        "execution turn binding conflicts with operation scope"
    );
    Ok(())
}

pub(crate) async fn compaction_prepare_runner<C: ConnectionTrait>(
    db: &C,
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
    db.execute(
        &Query::insert()
            .into_table(compaction_runner_plan::Entity)
            .columns([
                compaction_runner_plan::Column::OperationId,
                compaction_runner_plan::Column::SourceCount,
                compaction_runner_plan::Column::ReferenceCount,
                compaction_runner_plan::Column::Descriptor,
            ])
            .select_from(
                compaction_operation::Entity::find()
                    .select_only()
                    .expr(Expr::col(compaction_operation::Column::Id))
                    .expr(Expr::Value(source_count.into()))
                    .expr(Expr::Value(reference_count.into()))
                    .expr(Expr::Value(descriptor.clone().into()))
                    .filter(
                        Expr::col(compaction_operation::Column::Id)
                            .eq(Expr::Value(operation.into()))
                            .and(
                                Expr::col(compaction_operation::Column::Status)
                                    .eq(Expr::val("running")),
                            ),
                    )
                    .into_query(),
            )?
            .on_conflict(
                OnConflict::columns(["operation_id"])
                    .do_nothing()
                    .to_owned(),
            )
            .to_owned(),
    )
    .await?;
    let exact = compaction_runner_plan::Entity::find()
        .select_only()
        .expr(Expr::col(compaction_runner_plan::Column::OperationId))
        .filter(
            Expr::col(compaction_runner_plan::Column::OperationId)
                .eq(Expr::Value(operation.into()))
                .and(
                    Expr::col(compaction_runner_plan::Column::SourceCount)
                        .eq(Expr::Value(source_count.into())),
                )
                .and(
                    Expr::col(compaction_runner_plan::Column::ReferenceCount)
                        .eq(Expr::Value(reference_count.into())),
                )
                .and(
                    Expr::col(compaction_runner_plan::Column::Descriptor)
                        .eq(Expr::Value(descriptor.into())),
                ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(exact.is_some(), "runner plan idempotency conflict");
    Ok(())
}
pub(crate) async fn compaction_runner_plan<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<Option<RunnerPlanRecord>> {
    use pioneer_entity::compaction_runner_plan as plan;
    use sea_orm::EntityTrait;
    let Some(row) = plan::Entity::find_by_id(operation).one(db).await? else {
        return Ok(None);
    };
    Ok(Some(RunnerPlanRecord {
        source_count: u64::try_from(row.source_count)?,
        reference_count: u64::try_from(row.reference_count)?,
        budget: serde_json::from_str(&row.descriptor)?,
        ready: row.ready != 0,
    }))
}
pub(crate) async fn compaction_append_manifest(
    store: &CrudStore,
    operation: &str,
    entries: &[ManifestEntry],
) -> Result<()> {
    ensure!(
        entries.len() <= SOURCE_PAGE_ROWS as usize,
        "manifest batch exceeds row quantum"
    );
    let mut bytes = 0;
    let prepared = entries
        .iter()
        .map(|entry| -> Result<_> {
            bytes += entry.thread_id.len()
                + entry.source.scope.len()
                + entry.source.id.len()
                + entry.source.version.len();
            let ordinal = i64::try_from(entry.ordinal)?;
            let unit = i64::try_from(entry.unit)?;
            let reference_only = i64::from(entry.reference_only);
            let model = compaction_manifest::ActiveModel {
                operation_id: sea_orm::Set(operation.to_owned()),
                ordinal: sea_orm::Set(ordinal),
                unit_ordinal: sea_orm::Set(unit),
                reference_only: sea_orm::Set(reference_only),
                source_thread: sea_orm::Set(entry.thread_id.clone()),
                source_scope: sea_orm::Set(entry.source.scope.clone()),
                source_id: sea_orm::Set(entry.source.id.clone()),
                source_version: sea_orm::Set(entry.source.version.clone()),
            };
            let exact = compaction_manifest::Entity::find_by_id((operation.to_owned(), ordinal))
                .select_only()
                .column(compaction_manifest::Column::Ordinal)
                .filter(compaction_manifest::Column::UnitOrdinal.eq(unit))
                .filter(compaction_manifest::Column::ReferenceOnly.eq(reference_only))
                .filter(compaction_manifest::Column::SourceThread.eq(entry.thread_id.clone()))
                .filter(compaction_manifest::Column::SourceScope.eq(entry.source.scope.clone()))
                .filter(compaction_manifest::Column::SourceId.eq(entry.source.id.clone()))
                .filter(
                    compaction_manifest::Column::SourceVersion.eq(entry.source.version.clone()),
                );
            Ok((model, exact))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        bytes <= SOURCE_PAGE_BYTES,
        "manifest batch exceeds byte quantum"
    );
    // Prepare only metadata statements outside writer capacity. The insert
    // rechecks live ownership and never overwrites an existing revision;
    // edits racing admission therefore remain detectable at final CAS.
    let seeds = entries
        .iter()
        .map(|entry| -> Result<_> {
            let (prefix, turn) = entry
                .source
                .scope
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("invalid manifest source scope"))?;
            let kind = match prefix {
                "input" => super::compaction::CanonicalSource::Input,
                "event" => super::compaction::CanonicalSource::Event,
                "context" => super::compaction::CanonicalSource::ProviderContext,
                "item" => super::compaction::CanonicalSource::ToolItem,
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
            Ok(Some((
                kind,
                turn.to_owned(),
                entry.source.id.clone(),
                entry.thread_id.clone(),
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            let open = compaction_runner_plan::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_runner_plan::Entity::belongs_to(compaction_operation::Entity)
                        .from(compaction_runner_plan::Column::OperationId)
                        .to(compaction_operation::Column::Id)
                        .into(),
                )
                .expr(Expr::col((
                    compaction_runner_plan::Entity,
                    compaction_runner_plan::Column::OperationId,
                )))
                .filter(
                    Expr::col((
                        compaction_runner_plan::Entity,
                        compaction_runner_plan::Column::OperationId,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_runner_plan::Entity,
                            compaction_runner_plan::Column::Ready,
                        ))
                        .eq(Expr::val(0_i64)),
                    )
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::Status,
                        ))
                        .eq(Expr::val("running")),
                    ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            ensure!(open.is_some(), "manifest is immutable or operation stopped");
            for (kind, turn_id, source_id, thread_id) in seeds.iter().flatten() {
                match kind {
                    super::compaction::CanonicalSource::Input => {
                        if manifest_source_exists::<_, pioneer_entity::turn_input::Entity>(
                            &txn,
                            operation,
                            source_id,
                            turn_id,
                            thread_id,
                            (
                                pioneer_entity::turn_input::Column::Id,
                                pioneer_entity::turn_input::Column::TurnId,
                            ),
                        )
                        .await?
                        {
                            pioneer_entity::compaction_input_revision::Entity::insert(
                                pioneer_entity::compaction_input_revision::ActiveModel {
                                    source_id: sea_orm::Set(source_id.clone()),
                                    turn_id: sea_orm::Set(turn_id.clone()),
                                    revision: sea_orm::Set(1),
                                    present: sea_orm::Set(1),
                                    ..Default::default()
                                },
                            )
                            .on_conflict(
                                OnConflict::columns([
                                    pioneer_entity::compaction_input_revision::Column::SourceId,
                                ])
                                .do_nothing()
                                .to_owned(),
                            )
                            .exec_without_returning(&txn)
                            .await?;
                        }
                    }
                    super::compaction::CanonicalSource::Event => {
                        if manifest_source_exists::<_, pioneer_entity::turn_event::Entity>(
                            &txn,
                            operation,
                            source_id,
                            turn_id,
                            thread_id,
                            (
                                pioneer_entity::turn_event::Column::Id,
                                pioneer_entity::turn_event::Column::TurnId,
                            ),
                        )
                        .await?
                        {
                            pioneer_entity::compaction_event_revision::Entity::insert(
                                pioneer_entity::compaction_event_revision::ActiveModel {
                                    source_id: sea_orm::Set(source_id.clone()),
                                    turn_id: sea_orm::Set(turn_id.clone()),
                                    revision: sea_orm::Set(1),
                                    present: sea_orm::Set(1),
                                    ..Default::default()
                                },
                            )
                            .on_conflict(
                                OnConflict::columns([
                                    pioneer_entity::compaction_event_revision::Column::SourceId,
                                ])
                                .do_nothing()
                                .to_owned(),
                            )
                            .exec_without_returning(&txn)
                            .await?;
                        }
                    }
                    super::compaction::CanonicalSource::ProviderContext => {
                        if manifest_source_exists::<_, pioneer_entity::turn_llm_context::Entity>(
                            &txn,
                            operation,
                            source_id,
                            turn_id,
                            thread_id,
                            (
                                pioneer_entity::turn_llm_context::Column::Id,
                                pioneer_entity::turn_llm_context::Column::TurnId,
                            ),
                        )
                        .await?
                        {
                            pioneer_entity::compaction_source_revision::Entity::insert(
                                pioneer_entity::compaction_source_revision::ActiveModel {
                                    source_id: sea_orm::Set(source_id.clone()),
                                    turn_id: sea_orm::Set(turn_id.clone()),
                                    revision: sea_orm::Set(1),
                                    present: sea_orm::Set(1),
                                    ..Default::default()
                                },
                            )
                            .on_conflict(
                                OnConflict::columns([
                                    pioneer_entity::compaction_source_revision::Column::SourceId,
                                ])
                                .do_nothing()
                                .to_owned(),
                            )
                            .exec_without_returning(&txn)
                            .await?;
                        }
                    }
                    super::compaction::CanonicalSource::ToolItem => {
                        if manifest_source_exists::<_, pioneer_entity::turn_item::Entity>(
                            &txn,
                            operation,
                            source_id,
                            turn_id,
                            thread_id,
                            (
                                pioneer_entity::turn_item::Column::Id,
                                pioneer_entity::turn_item::Column::TurnId,
                            ),
                        )
                        .await?
                        {
                            pioneer_entity::compaction_item_revision::Entity::insert(
                                pioneer_entity::compaction_item_revision::ActiveModel {
                                    source_id: sea_orm::Set(source_id.clone()),
                                    turn_id: sea_orm::Set(turn_id.clone()),
                                    revision: sea_orm::Set(1),
                                    present: sea_orm::Set(1),
                                    ..Default::default()
                                },
                            )
                            .on_conflict(
                                OnConflict::columns([
                                    pioneer_entity::compaction_item_revision::Column::SourceId,
                                ])
                                .do_nothing()
                                .to_owned(),
                            )
                            .exec_without_returning(&txn)
                            .await?;
                        }
                    }
                }
            }
            for (insert, exact) in &prepared {
                compaction_manifest::Entity::insert(insert.clone())
                    .on_conflict(
                        OnConflict::columns([
                            compaction_manifest::Column::OperationId,
                            compaction_manifest::Column::Ordinal,
                        ])
                        .do_nothing()
                        .to_owned(),
                    )
                    .exec_without_returning(&txn)
                    .await?;
                ensure!(
                    exact.clone().into_tuple::<i64>().one(&txn).await?.is_some(),
                    "manifest idempotency conflict"
                );
            }
            txn.commit().await?;
            Ok(())
        })
        .await
}
pub(crate) async fn compaction_activate_runner(
    store: &CrudStore,
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
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            let valid = compaction_runner_plan::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_runner_plan::Entity::belongs_to(compaction_operation::Entity)
                        .from(compaction_runner_plan::Column::OperationId)
                        .to(compaction_operation::Column::Id)
                        .into(),
                )
                .expr(Expr::col((
                    compaction_runner_plan::Entity,
                    compaction_runner_plan::Column::OperationId,
                )))
                .filter(
                    Expr::col((
                        compaction_runner_plan::Entity,
                        compaction_runner_plan::Column::OperationId,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::Status,
                        ))
                        .eq(Expr::val("running")),
                    )
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::DeadlineMs,
                        ))
                        .eq(Expr::Value(deadline.into())),
                    )
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::ExpectedHead,
                        ))
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
                                    .from_as(super::compaction_live_sources::Column::Table, "s")
                                    .and_where(
                                        Expr::col(("s", super::compaction_live_sources::Column::SourceScope))
                                            .eq(Expr::val("checkpoint:").binary(
                                                BinOper::Custom("||"),
                                                Expr::col((
                                                    compaction_operation::Entity,
                                                    compaction_operation::Column::Owner,
                                                )),
                                            ))
                                            .and(Expr::col(("s", super::compaction_live_sources::Column::SourceId)).eq(Expr::col((
                                                compaction_operation::Entity,
                                                compaction_operation::Column::ExpectedHead,
                                            )))),
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
                                        Func::cust(Alias::new("count")).args([Expr::col(Asterisk)]),
                                    ))
                                    .from_as(compaction_manifest::Entity, "m")
                                    .and_where(
                                        Expr::col(("m", compaction_manifest::Column::OperationId))
                                            .eq(Expr::col((
                                                compaction_runner_plan::Entity,
                                                compaction_runner_plan::Column::OperationId,
                                            )))
                                            .and(
                                                Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::ReferenceOnly,
                                                ))
                                                .eq(Expr::val(0_i64)),
                                            ),
                                    )
                                    .to_owned()
                                    .into(),
                            ),
                        )
                        .eq(Expr::col((
                            compaction_runner_plan::Entity,
                            compaction_runner_plan::Column::SourceCount,
                        ))),
                    )
                    .and(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(
                                        Func::cust(Alias::new("count")).args([Expr::col(Asterisk)]),
                                    ))
                                    .from_as(compaction_manifest::Entity, "m")
                                    .and_where(
                                        Expr::col(("m", compaction_manifest::Column::OperationId))
                                            .eq(Expr::col((
                                                compaction_runner_plan::Entity,
                                                compaction_runner_plan::Column::OperationId,
                                            )))
                                            .and(
                                                Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::ReferenceOnly,
                                                ))
                                                .eq(Expr::val(1_i64)),
                                            ),
                                    )
                                    .to_owned()
                                    .into(),
                            ),
                        )
                        .eq(Expr::col((
                            compaction_runner_plan::Entity,
                            compaction_runner_plan::Column::ReferenceCount,
                        ))),
                    )
                    .and(
                        Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_manifest::Entity, "m")
                                .and_where(
                                    Expr::col(("m", compaction_manifest::Column::OperationId))
                                        .eq(Expr::col((
                                            compaction_runner_plan::Entity,
                                            compaction_runner_plan::Column::OperationId,
                                        )))
                                        .and(
                                            Expr::col(("m", compaction_manifest::Column::Ordinal))
                                                .lt(Expr::val(0_i64))
                                                .or(Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::Ordinal,
                                                ))
                                                .gte(
                                                    Expr::col((
                                                        compaction_runner_plan::Entity,
                                                        compaction_runner_plan::Column::SourceCount,
                                                    ))
                                                    .add(Expr::col((compaction_runner_plan::Entity, compaction_runner_plan::Column::ReferenceCount))),
                                                )),
                                        ),
                                )
                                .to_owned(),
                        )
                        .not(),
                    ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            ensure!(
                valid.is_some(),
                "manifest is incomplete or admission expired"
            );
            compaction_runner_state::Entity::insert(compaction_runner_state::ActiveModel {
                operation_id: sea_orm::Set((operation).to_owned()),
                generation: sea_orm::Set(0_i64),
                state: sea_orm::Set((encoded.clone()).to_owned()),
            })
            .on_conflict(
                OnConflict::columns([compaction_runner_state::Column::OperationId])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&txn)
            .await?;
            compaction_runner_plan::Entity::update_many()
                .col_expr(compaction_runner_plan::Column::Ready, Expr::val(1_i64))
                .filter(
                    Expr::col(compaction_runner_plan::Column::OperationId)
                        .eq(Expr::Value(operation.into())),
                )
                .exec(&txn)
                .await?;
            txn.commit().await?;
            Ok(())
        })
        .await
}
pub(crate) async fn compaction_runner_state<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<Option<RunnerState>> {
    use pioneer_entity::compaction_runner_state as state;
    use sea_orm::EntityTrait;
    let row = state::Entity::find_by_id(operation).one(db).await?;
    row.map(|row| Ok(serde_json::from_str(&row.state)?))
        .transpose()
}
/// Complete the state record after a control-plane terminal fence. No
/// attempt can advance past that fence. Preparation reads one bounded row;
/// the write revalidates its generation and durable terminal classification.
pub(crate) async fn compaction_reconcile_runner_state(
    store: &CrudStore,
    operation: &str,
) -> Result<Option<RunnerState>> {
    use sea_orm::QuerySelect;
    let row = compaction_runner_state::Entity::find_by_id(operation)
        .inner_join(compaction_operation::Entity)
        .select_only()
        .column(compaction_runner_state::Column::State)
        .column_as(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Status,
            )),
            "status",
        )
        .column_as(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Outcome,
            )),
            "outcome",
        )
        .into_tuple::<(String, String, Option<String>)>()
        .one(&store.connection)
        .await?;
    let Some((state, status, outcome)) = row else {
        return Ok(None);
    };
    let state: RunnerState = serde_json::from_str(&state)?;
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
        compaction_runner_state::Entity::update_many()
            .col_expr(
                compaction_runner_state::Column::Generation,
                Expr::Value(i64::try_from(terminal.generation)?.into()),
            )
            .col_expr(
                compaction_runner_state::Column::State,
                Expr::Value(encoded.into()),
            )
            .filter(
                Expr::col(compaction_runner_state::Column::OperationId)
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col(compaction_runner_state::Column::Generation)
                            .eq(Expr::Value(i64::try_from(state.generation)?.into())),
                    )
                    .and(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(compaction_operation::Entity, "o")
                            .and_where(
                                Expr::col(("o", compaction_operation::Column::Id))
                                    .eq(Expr::Value(operation.into()))
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Status))
                                            .eq(Expr::Value(status.into())),
                                    )
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Outcome))
                                            .binary(BinOper::Is, Expr::Value(outcome.into())),
                                    )
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Status))
                                            .is_in(["cancelled", "failed", "stale"]),
                                    ),
                            )
                            .to_owned(),
                    )),
            )
            .exec(&store.connection)
            .await?;
    if changed.rows_affected == 1 {
        return Ok(Some(terminal));
    }
    // A concurrent reconciler may have won the same idempotent transition.
    store.compaction_runner_state(operation).await
}

pub(crate) async fn compaction_manifest_page<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    reference_only: bool,
    unit: u64,
    source_offset: u32,
) -> Result<Vec<ManifestEntry>> {
    use sea_orm::{ColumnTrait, QueryOrder, QuerySelect};
    #[derive(sea_orm::FromQueryResult)]
    struct ManifestSourceRow {
        ordinal: i64,
        unit_ordinal: i64,
        reference_only: i64,
        source_thread: String,
        source_scope: String,
        source_id: String,
        source_version: String,
    }
    let rows = compaction_manifest::Entity::find()
        .select_only()
        .columns([
            compaction_manifest::Column::Ordinal,
            compaction_manifest::Column::UnitOrdinal,
            compaction_manifest::Column::ReferenceOnly,
            compaction_manifest::Column::SourceThread,
            compaction_manifest::Column::SourceScope,
            compaction_manifest::Column::SourceId,
            compaction_manifest::Column::SourceVersion,
        ])
        .filter(compaction_manifest::Column::OperationId.eq(operation))
        .filter(compaction_manifest::Column::ReferenceOnly.eq(reference_only as i64))
        .filter(compaction_manifest::Column::UnitOrdinal.gte(i64::try_from(unit)?))
        .order_by_asc(compaction_manifest::Column::UnitOrdinal)
        .order_by_asc(compaction_manifest::Column::Ordinal)
        .limit(SOURCE_PAGE_ROWS)
        .offset(u64::from(source_offset))
        .into_model::<ManifestSourceRow>()
        .all(db)
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(ManifestEntry {
                ordinal: u64::try_from(row.ordinal)?,
                unit: u64::try_from(row.unit_ordinal)?,
                reference_only: row.reference_only != 0,
                thread_id: row.source_thread,
                source: SourceRef {
                    scope: row.source_scope,
                    id: row.source_id,
                    version: row.source_version,
                },
            })
        })
        .collect()
}
/// Preparation uses immutable values only. Generation, counters, ownership,
/// deadline and running status are revalidated atomically with candidate save.
pub(crate) async fn compaction_runner_transition(
    store: &CrudStore,
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
    let mut candidate_model = None;
    let mut coverage_models = Vec::new();
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
        candidate_model = Some(compaction_checkpoint::ActiveModel {
            id: sea_orm::Set(cp.id.clone()),
            operation_id: sea_orm::Set(operation.to_owned()),
            owner: sea_orm::Set(cp.owner.clone()),
            previous: sea_orm::Set(cp.previous.clone()),
            portion: sea_orm::Set(i64::try_from(next.attempts)?),
            summary: sea_orm::Set(cp.summary.clone()),
            selection: sea_orm::Set(serde_json::to_string(&cp.selection)?),
            projection_version: sea_orm::Set(i64::try_from(cp.projection_version)?),
            format_version: sea_orm::Set(1),
            identity_sha256: sea_orm::Set(identity.clone()),
            status: sea_orm::Set("candidate".to_owned()),
        });
        identity_query = Some(
            compaction_checkpoint::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_checkpoint::Entity::belongs_to(compaction_operation::Entity)
                        .from(compaction_checkpoint::Column::OperationId)
                        .to(compaction_operation::Column::Id)
                        .into(),
                )
                .expr(Expr::col((
                    compaction_checkpoint::Entity,
                    compaction_checkpoint::Column::Id,
                )))
                .filter(
                    Expr::col((
                        compaction_checkpoint::Entity,
                        compaction_checkpoint::Column::Id,
                    ))
                    .eq(Expr::Value(cp.id.clone().into()))
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::IdentitySha256,
                        ))
                        .eq(Expr::Value(identity.into())),
                    )
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::OperationId,
                        ))
                        .eq(Expr::Value(operation.into())),
                    )
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::Owner,
                        ))
                        .eq(Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::Owner,
                        ))),
                    )
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::ProjectionVersion,
                        ))
                        .eq(Expr::expr(
                            Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.projection_version"),
                            ]),
                        )),
                    )
                    .and(
                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            Expr::val("$.transport"),
                        ]))
                        .eq(Expr::expr(
                            Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.admission.selection.transport"),
                            ]),
                        )),
                    )
                    .and(
                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            Expr::val("$.instance"),
                        ]))
                        .eq(Expr::expr(
                            Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.admission.selection.instance"),
                            ]),
                        )),
                    )
                    .and(
                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            Expr::val("$.model"),
                        ]))
                        .eq(Expr::expr(
                            Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.admission.selection.model"),
                            ]),
                        )),
                    )
                    .and(
                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            Expr::val("$.effort"),
                        ]))
                        .binary(
                            BinOper::Is,
                            Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.admission.selection.effort"),
                            ])),
                        ),
                    ),
                ),
        );
        coverage_models = cp
            .coverage
            .iter()
            .map(|source| compaction_coverage::ActiveModel {
                checkpoint_id: sea_orm::Set(cp.id.clone()),
                source_scope: sea_orm::Set(source.scope.clone()),
                source_id: sea_orm::Set(source.id.clone()),
                source_version: sea_orm::Set(source.version.clone()),
            })
            .collect::<Vec<_>>();
    }
    let observation_model = next
        .observation
        .as_ref()
        .map(|observation| -> Result<_> {
            ensure!(
                observation.number == next.attempts,
                "attempt observation identity mismatch"
            );
            Ok(compaction_attempt_observation::ActiveModel {
                operation_id: sea_orm::Set(operation.to_owned()),
                attempt: sea_orm::Set(i64::try_from(observation.number)?),
                observation: sea_orm::Set(serde_json::to_string(observation)?),
            })
        })
        .transpose()?;
    let basis_query =
        candidate.map(|cp| {
            compaction_runner_state::Entity::find()
                .select_only()
                .expr(Expr::col(compaction_runner_state::Column::OperationId))
                .filter(
                    Expr::col(compaction_runner_state::Column::OperationId)
                        .eq(Expr::Value(operation.into()))
                        .and(Expr::col(compaction_runner_state::Column::Generation).eq(
                            Expr::Value(i64::try_from(expected).unwrap_or(i64::MAX).into()),
                        ))
                        .and(
                            Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                Expr::col(compaction_runner_state::Column::State),
                                Expr::val("$.previous_checkpoint"),
                            ]))
                            .binary(BinOper::Is, Expr::Value(cp.previous.clone().into())),
                        ),
                )
        });
    let (status, outcome) = match &next.phase {
        RunnerPhase::Failed {
            kind: FailureKind::Cancelled,
        } => ("cancelled", Some("cancelled")),
        RunnerPhase::Failed { .. } => ("failed", Some("runner_failed")),
        _ => ("running", None),
    };
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            if let Some(query) = &basis_query {
                if query
                    .clone()
                    .into_tuple::<String>()
                    .one(&txn)
                    .await?
                    .is_none()
                {
                    txn.rollback().await?;
                    return Ok(false);
                }
            }
            let updated = compaction_runner_state::Entity::update_many()
                .col_expr(
                    compaction_runner_state::Column::Generation,
                    Expr::Value(i64::try_from(next.generation)?.into()),
                )
                .col_expr(
                    compaction_runner_state::Column::State,
                    Expr::Value(state.clone().into()),
                )
                .filter(
                    Expr::col(compaction_runner_state::Column::OperationId)
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::col(compaction_runner_state::Column::Generation)
                                .eq(Expr::Value(i64::try_from(expected)?.into())),
                        )
                        .and(Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_operation::Entity, "o")
                                .join_as(
                                    JoinType::InnerJoin,
                                    compaction_runner_plan::Entity,
                                    "p",
                                    Expr::col(("p", compaction_runner_plan::Column::OperationId))
                                        .eq(Expr::col(("o", compaction_operation::Column::Id))),
                                )
                                .and_where(
                                    Expr::col(("o", compaction_operation::Column::Id))
                                        .eq(Expr::Value(operation.into()))
                                        .and(
                                            Expr::col(("o", compaction_operation::Column::Status))
                                                .eq(Expr::val("running")),
                                        )
                                        .and(
                                            Expr::col(("p", compaction_runner_plan::Column::Ready))
                                                .eq(Expr::val(1_i64)),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::DeadlineMs,
                                            ))
                                            .eq(
                                                Expr::Value(
                                                    i64::try_from(next.deadline_ms)?.into(),
                                                ),
                                            ),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::Attempts,
                                            ))
                                            .lte(Expr::Value(i64::try_from(next.attempts)?.into())),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::Attempts,
                                            ))
                                            .add(Expr::val(1_i64))
                                            .gte(Expr::Value(i64::try_from(next.attempts)?.into())),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::TransientRetries,
                                            ))
                                            .lte(Expr::Value(i64::from(next.retries).into())),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::Correction,
                                            ))
                                            .lte(Expr::Value(i64::from(next.corrections).into())),
                                        ),
                                )
                                .to_owned(),
                        )),
                )
                .exec(&txn)
                .await?;
            if updated.rows_affected != 1 {
                txn.rollback().await?;
                return Ok(false);
            }
            if let Some(model) = &observation_model {
                compaction_attempt_observation::Entity::insert(model.clone())
                    .on_conflict(
                        OnConflict::columns([
                            compaction_attempt_observation::Column::OperationId,
                            compaction_attempt_observation::Column::Attempt,
                        ])
                        .update_column(compaction_attempt_observation::Column::Observation)
                        .to_owned(),
                    )
                    .exec_without_returning(&txn)
                    .await?;
            }
            if let Some(model) = &candidate_model {
                compaction_checkpoint::Entity::insert(model.clone())
                    .on_conflict(
                        OnConflict::columns([
                            compaction_checkpoint::Column::OperationId,
                            compaction_checkpoint::Column::Portion,
                        ])
                        .do_nothing()
                        .to_owned(),
                    )
                    .exec_without_returning(&txn)
                    .await?;
            }
            for model in &coverage_models {
                compaction_coverage::Entity::insert(model.clone())
                    .on_conflict(OnConflict::new().do_nothing().to_owned())
                    .exec_without_returning(&txn)
                    .await?;
            }
            if let Some(query) = &identity_query {
                ensure!(
                    query
                        .clone()
                        .into_tuple::<String>()
                        .one(&txn)
                        .await?
                        .is_some(),
                    "candidate identity conflict"
                );
            }
            compaction_operation::Entity::update_many()
                .col_expr(
                    compaction_operation::Column::Status,
                    Expr::Value(status.into()),
                )
                .col_expr(
                    compaction_operation::Column::Outcome,
                    Expr::Value(outcome.map(str::to_owned).into()),
                )
                .col_expr(
                    compaction_operation::Column::Attempts,
                    Expr::Value(i64::try_from(next.attempts)?.into()),
                )
                .col_expr(
                    compaction_operation::Column::TransientRetries,
                    Expr::Value(i64::from(next.retries).into()),
                )
                .col_expr(
                    compaction_operation::Column::Correction,
                    Expr::Value(i64::from(next.corrections).into()),
                )
                .filter(
                    Expr::col(compaction_operation::Column::Id).eq(Expr::Value(operation.into())),
                )
                .exec(&txn)
                .await?;
            txn.commit().await?;
            Ok(true)
        })
        .await
}

/// Prepare only the exact predecessor chain in bounded one-row quanta.
/// `retained` rows are invisible in live_sources while their operation is
/// running. The final operation/head CAS makes the whole prepared chain
/// visible atomically. Generation checks prevent a stale preparer from
/// publishing; cancellation/restart may safely leave invisible markers.
pub(crate) async fn prepare_checkpoint_ancestry<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    checkpoint: &str,
    generation: u64,
) -> Result<()> {
    let mut cursor = Some(checkpoint.to_owned());
    let mut visited = std::collections::BTreeSet::new();
    while let Some(id) = cursor.take() {
        ensure!(visited.insert(id.clone()), "cyclic candidate ancestry");
        let (source_operation, previous) = compaction_checkpoint::Entity::find_by_id(id.clone())
            .select_only()
            .column(compaction_checkpoint::Column::OperationId)
            .column(compaction_checkpoint::Column::Previous)
            .into_tuple::<(String, Option<String>)>()
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("candidate ancestry is missing"))?;
        if source_operation != operation {
            break;
        }
        cursor = previous;
        compaction_checkpoint::Entity::update_many()
            .col_expr(compaction_checkpoint::Column::Status, Expr::val("retained"))
            .filter(
                Expr::col(compaction_checkpoint::Column::Id)
                    .eq(Expr::Value(id.into()))
                    .and(
                        Expr::col(compaction_checkpoint::Column::OperationId)
                            .eq(Expr::Value(operation.into())),
                    )
                    .and(
                        Expr::col(compaction_checkpoint::Column::Status)
                            .is_in(["candidate", "retained"]),
                    )
                    .and(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(compaction_operation::Entity, "o")
                            .join_as(
                                JoinType::InnerJoin,
                                compaction_runner_state::Entity,
                                "s",
                                Expr::col(("s", compaction_runner_state::Column::OperationId))
                                    .eq(Expr::col(("o", compaction_operation::Column::Id))),
                            )
                            .and_where(
                                Expr::col(("o", compaction_operation::Column::Id))
                                    .eq(Expr::Value(operation.into()))
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Status))
                                            .eq(Expr::val("running")),
                                    )
                                    .and(
                                        Expr::col((
                                            "s",
                                            compaction_runner_state::Column::Generation,
                                        ))
                                        .eq(Expr::Value(i64::try_from(generation)?.into())),
                                    )
                                    .and(
                                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                            Expr::col((
                                                "s",
                                                compaction_runner_state::Column::State,
                                            )),
                                            Expr::val("$.phase.Commit.checkpoint"),
                                        ]))
                                        .eq(Expr::Value(checkpoint.into())),
                                    ),
                            )
                            .to_owned(),
                    )),
            )
            .exec(db)
            .await?;
    }
    Ok(())
}

/// One atomic domain transition. The immutable, admitted manifest bounds
/// validation to this operation's selected sources; it never scans transcript
/// payloads or the complete history. Every source version and the owner head
/// are checked inside the same transaction which publishes the candidate.
pub(crate) async fn compaction_apply_runner(
    store: &CrudStore,
    operation: &str,
    state: &RunnerState,
    expected_head: Option<&str>,
) -> Result<super::compaction::CommitOutcome> {
    let RunnerPhase::Commit { checkpoint } = &state.phase else {
        anyhow::bail!("runner has no commit candidate")
    };
    let applied = state.applied(checkpoint)?;
    let encoded = serde_json::to_string(&applied)?;
    store
        .prepare_checkpoint_ancestry(operation, checkpoint, state.generation)
        .await?;
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            let row = compaction_operation::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_operation::Entity::belongs_to(compaction_checkpoint::Entity)
                        .from(compaction_operation::Column::Id)
                        .to(compaction_checkpoint::Column::OperationId)
                        .on_condition(|_, _| {
                            sea_orm::Condition::all().add(
                                Expr::col((
                                    compaction_checkpoint::Entity,
                                    compaction_checkpoint::Column::Owner,
                                ))
                                .eq(Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Owner,
                                ))),
                            )
                        })
                        .into(),
                )
                .expr(Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::Status,
                )))
                .filter(
                    Expr::col((
                        compaction_operation::Entity,
                        compaction_operation::Column::Id,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::Id,
                        ))
                        .eq(Expr::Value(checkpoint.clone().into())),
                    )
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::ExpectedHead,
                        ))
                        .binary(
                            BinOper::Is,
                            Expr::Value(expected_head.map(str::to_owned).into()),
                        ),
                    ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            let Some(row) = row else {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            };
            let status = row;
            if status == "completed" {
                let exact = compaction_checkpoint::Entity::find()
                    .select_only()
                    .expr(Expr::col(compaction_checkpoint::Column::Id))
                    .filter(
                        Expr::col(compaction_checkpoint::Column::Id)
                            .eq(Expr::Value(checkpoint.clone().into()))
                            .and(
                                Expr::col(compaction_checkpoint::Column::Status)
                                    .eq(Expr::val("applied")),
                            ),
                    )
                    .into_tuple::<String>()
                    .one(&txn)
                    .await?;
                txn.rollback().await?;
                return Ok(if exact.is_some() {
                    super::compaction::CommitOutcome::AlreadyApplied
                } else {
                    super::compaction::CommitOutcome::Stale
                });
            }
            if status != "running" {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Cancelled);
            }
            let interrupted = compaction_operation::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_operation::Entity::belongs_to(compaction_context::Entity)
                        .from(compaction_operation::Column::Owner)
                        .to(compaction_context::Column::Owner)
                        .into(),
                )
                .expr(Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::Id,
                )))
                .filter(
                    Expr::col((
                        compaction_operation::Entity,
                        compaction_operation::Column::Id,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::ExecutionTurn,
                        ))
                        .binary(BinOper::Is, Expr::val(Option::<String>::None))
                        .not(),
                    )
                    .and(
                        Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_execution_stop::Entity, "stop")
                                .and_where(
                                    Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                        .eq(Expr::col((
                                            compaction_context::Entity,
                                            compaction_context::Column::Owner,
                                        )))
                                        .and(
                                            Expr::col((
                                                "stop",
                                                compaction_execution_stop::Column::TurnId,
                                            ))
                                            .eq(
                                                Expr::col((
                                                    compaction_operation::Entity,
                                                    compaction_operation::Column::ExecutionTurn,
                                                )),
                                            ),
                                        ),
                                )
                                .to_owned(),
                        )
                        .or(Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(turn::Entity, "t")
                                .and_where(
                                    Expr::col(("t", turn::Column::Id))
                                        .eq(Expr::col((
                                            compaction_operation::Entity,
                                            compaction_operation::Column::ExecutionTurn,
                                        )))
                                        .and(
                                            Expr::col(("t", turn::Column::Status))
                                                .is_in(["interrupted", "cancelled"])
                                                .not(),
                                        ),
                                )
                                .to_owned(),
                        )
                        .not()),
                    ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            if interrupted.is_some() {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Cancelled);
            }

            let generation = compaction_runner_state::Entity::find()
                .select_only()
                .expr(Expr::col(compaction_runner_state::Column::OperationId))
                .filter(
                    Expr::col(compaction_runner_state::Column::OperationId)
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::col(compaction_runner_state::Column::Generation)
                                .eq(Expr::Value(i64::try_from(state.generation)?.into())),
                        )
                        .and(
                            Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                Expr::col(compaction_runner_state::Column::State),
                                Expr::val("$.phase.Commit.checkpoint"),
                            ]))
                            .eq(Expr::Value(checkpoint.clone().into())),
                        ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            if generation.is_none() {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            }
            // Exact coverage: every selected record has a fully read version,
            // and no reference-only or unselected source has acquired coverage.
            let missing = compaction_manifest::Entity::find()
                .select_only()
                .expr(Expr::col((
                    compaction_manifest::Entity,
                    compaction_manifest::Column::Ordinal,
                )))
                .filter(
                    Expr::col((
                        compaction_manifest::Entity,
                        compaction_manifest::Column::OperationId,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_manifest::Entity,
                            compaction_manifest::Column::ReferenceOnly,
                        ))
                        .eq(Expr::val(0_i64)),
                    )
                    .and(
                        Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_coverage::Entity, "c")
                                .join_as(
                                    JoinType::InnerJoin,
                                    compaction_checkpoint::Entity,
                                    "p",
                                    Expr::col(("p", compaction_checkpoint::Column::Id)).eq(
                                        Expr::col(("c", compaction_coverage::Column::CheckpointId)),
                                    ),
                                )
                                .and_where(
                                    Expr::col(("p", compaction_checkpoint::Column::OperationId))
                                        .eq(Expr::col((
                                            compaction_manifest::Entity,
                                            compaction_manifest::Column::OperationId,
                                        )))
                                        .and(
                                            Expr::col((
                                                "c",
                                                compaction_coverage::Column::SourceScope,
                                            ))
                                            .eq(
                                                Expr::col((
                                                    compaction_manifest::Entity,
                                                    compaction_manifest::Column::SourceScope,
                                                )),
                                            ),
                                        )
                                        .and(
                                            Expr::col(("c", compaction_coverage::Column::SourceId))
                                                .eq(Expr::col((
                                                    compaction_manifest::Entity,
                                                    compaction_manifest::Column::SourceId,
                                                ))),
                                        )
                                        .and(
                                            Expr::col((
                                                "c",
                                                compaction_coverage::Column::SourceVersion,
                                            ))
                                            .eq(
                                                Expr::col((
                                                    compaction_manifest::Entity,
                                                    compaction_manifest::Column::SourceVersion,
                                                )),
                                            ),
                                        ),
                                )
                                .to_owned(),
                        )
                        .not(),
                    ),
                )
                .limit(1)
                .into_tuple::<i64>()
                .one(&txn)
                .await?;
            let extra =
                compaction_coverage::Entity::find()
                    .select_only()
                    .join(
                        JoinType::InnerJoin,
                        compaction_coverage::Entity::belongs_to(compaction_checkpoint::Entity)
                            .from(compaction_coverage::Column::CheckpointId)
                            .to(compaction_checkpoint::Column::Id)
                            .into(),
                    )
                    .expr(Expr::col((
                        compaction_coverage::Entity,
                        compaction_coverage::Column::SourceId,
                    )))
                    .filter(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::OperationId,
                        ))
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::exists(
                                Query::select()
                                    .expr(Expr::val(1_i64))
                                    .from_as(compaction_manifest::Entity, "m")
                                    .and_where(
                                        Expr::col(("m", compaction_manifest::Column::OperationId))
                                            .eq(Expr::col((
                                                compaction_checkpoint::Entity,
                                                compaction_checkpoint::Column::OperationId,
                                            )))
                                            .and(
                                                Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::ReferenceOnly,
                                                ))
                                                .eq(Expr::val(0_i64)),
                                            )
                                            .and(
                                                Expr::col((
                                                    compaction_coverage::Entity,
                                                    compaction_coverage::Column::SourceScope,
                                                ))
                                                .eq(Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::SourceScope,
                                                ))),
                                            )
                                            .and(
                                                Expr::col((
                                                    compaction_coverage::Entity,
                                                    compaction_coverage::Column::SourceId,
                                                ))
                                                .eq(Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::SourceId,
                                                ))),
                                            )
                                            .and(
                                                Expr::col((
                                                    compaction_coverage::Entity,
                                                    compaction_coverage::Column::SourceVersion,
                                                ))
                                                .eq(Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::SourceVersion,
                                                ))),
                                            ),
                                    )
                                    .to_owned(),
                            )
                            .not(),
                        ),
                    )
                    .limit(1)
                    .into_tuple::<String>()
                    .one(&txn)
                    .await?;
            let sources_current = compaction_manifest_sources_current(&txn, operation).await?;
            if missing.is_some() || extra.is_some() || !sources_current {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            }
            let changed = compaction_context::Entity::update_many()
                .col_expr(
                    compaction_context::Column::Head,
                    Expr::Value(checkpoint.clone().into()),
                )
                .filter(
                    Expr::col(compaction_context::Column::Owner)
                        .eq(Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::col(compaction_operation::Column::Owner))
                                    .from(compaction_operation::Entity)
                                    .and_where(
                                        Expr::col(compaction_operation::Column::Id)
                                            .eq(Expr::Value(operation.into())),
                                    )
                                    .to_owned()
                                    .into(),
                            ),
                        ))
                        .and(Expr::col(compaction_context::Column::Head).binary(
                            BinOper::Is,
                            Expr::Value(expected_head.map(str::to_owned).into()),
                        ))
                        .and(
                            Expr::col(compaction_context::Column::FormatVersion)
                                .eq(Expr::val(1_i64)),
                        ),
                )
                .exec(&txn)
                .await?;
            if changed.rows_affected != 1 {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            }
            compaction_checkpoint::Entity::update_many()
                .col_expr(compaction_checkpoint::Column::Status, Expr::val("applied"))
                .filter(
                    Expr::col(compaction_checkpoint::Column::Id)
                        .eq(Expr::Value(checkpoint.clone().into())),
                )
                .exec(&txn)
                .await?;
            compaction_operation::Entity::update_many()
                .col_expr(compaction_operation::Column::Status, Expr::val("completed"))
                .col_expr(compaction_operation::Column::Outcome, Expr::val("applied"))
                .filter(
                    Expr::col(compaction_operation::Column::Id).eq(Expr::Value(operation.into())),
                )
                .exec(&txn)
                .await?;
            compaction_runner_state::Entity::update_many()
                .col_expr(
                    compaction_runner_state::Column::Generation,
                    Expr::Value(i64::try_from(applied.generation)?.into()),
                )
                .col_expr(
                    compaction_runner_state::Column::State,
                    Expr::Value(encoded.clone().into()),
                )
                .filter(
                    Expr::col(compaction_runner_state::Column::OperationId)
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::col(compaction_runner_state::Column::Generation)
                                .eq(Expr::Value(i64::try_from(state.generation)?.into())),
                        ),
                )
                .exec(&txn)
                .await?;
            txn.commit().await?;
            Ok(super::compaction::CommitOutcome::Applied)
        })
        .await
}

use sea_orm::QuerySelect;

async fn manifest_source_exists<C: ConnectionTrait, E: EntityTrait>(
    db: &C,
    operation: &str,
    source: &str,
    source_turn: &str,
    source_thread: &str,
    columns: (E::Column, E::Column),
) -> Result<bool> {
    let (id, turn_id) = columns;
    Ok(E::find()
        .select_only()
        .column(id)
        .join(
            JoinType::InnerJoin,
            E::belongs_to(turn::Entity)
                .from(turn_id)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            thread::Entity::belongs_to(compaction_context::Entity)
                .from(thread::Column::WorkspaceId)
                .to(compaction_context::Column::WorkspaceId)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_context::Entity::belongs_to(compaction_operation::Entity)
                .from(compaction_context::Column::Owner)
                .to(compaction_operation::Column::Owner)
                .into(),
        )
        .filter(compaction_operation::Column::Id.eq(operation))
        .filter(id.eq(source))
        .filter(turn_id.eq(source_turn))
        .filter(thread::Column::Id.eq(source_thread))
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}

use sea_orm::QueryTrait;

/// Shared admission/commit predicate. Read-only preflight avoids provider work
/// for an invalid grant; commit repeats it under the atomic writer boundary.
pub(crate) async fn compaction_manifest_sources_current<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    // A foreign summary may replace accepted raw imports after output capture.
    // Prove its entire immutable DAG against the SAME bound import manifest;
    // read access to the sibling alone grants no ownership. This predicate is
    // repeated inside the head CAS, including epochs and live leaf revisions.
    // SQLite's recursive UNION deduplicates DAG nodes. The explicit 65,536
    // reference bound fails closed (including cycles without any real leaves)
    // and prevents unbounded traversal while holding database capacity. No
    // transcript/summary payloads are read. Keep this with the existing SQLite
    // JSON snapshot predicate rather than doing a racy read/check/write loop.
    let stale = db.query_one_raw(sqlite_specific_sql(r#"
WITH RECURSIVE
current_operation AS (
 SELECT o.id, o.snapshot, c.workspace_id, c.thread_id
 FROM compaction_operation o JOIN compaction_context c ON c.owner=o.owner
 WHERE o.id=?
),
accepted_imports AS (
 SELECT i.source_scope, i.source_id, i.source_version, i.source_thread
 FROM current_operation o
 JOIN compaction_operation_projection p ON p.operation_id=o.id
 JOIN compaction_frozen_history h ON h.id=p.manifest_id
  AND h.workspace_id=o.workspace_id AND h.ready=1
  AND h.identity_sha256=p.identity_sha256 AND h.imports_sha256=p.imports_sha256
  AND h.import_count=p.import_count AND h.next_import=p.import_count
 JOIN compaction_frozen_import i ON i.manifest_id=h.id
),
projected_roots AS (
 SELECT m.ordinal, m.source_scope, m.source_id, m.source_version
 FROM compaction_manifest m JOIN current_operation o ON o.id=m.operation_id
 WHERE m.reference_only=0 AND m.source_thread<>o.thread_id
  AND m.source_scope LIKE 'checkpoint:%'
  AND NOT EXISTS (SELECT 1 FROM accepted_imports i
    WHERE i.source_scope=m.source_scope AND i.source_id=m.source_id
     AND i.source_version=m.source_version AND i.source_thread=m.source_thread)
),
coverage(root, source_scope, source_id, source_version) AS (
 SELECT ordinal, source_scope, source_id, source_version FROM projected_roots
 UNION
 SELECT g.root, v.source_scope, v.source_id, v.source_version
 FROM coverage g JOIN compaction_coverage v ON v.checkpoint_id=g.source_id
 WHERE g.source_scope LIKE 'checkpoint:%'
 UNION
 SELECT g.root, 'checkpoint:'||COALESCE(p.owner,''), c.previous, COALESCE(p.identity_sha256,'')
 FROM coverage g JOIN compaction_checkpoint c ON c.id=g.source_id
 LEFT JOIN compaction_checkpoint p ON p.id=c.previous
 WHERE g.source_scope LIKE 'checkpoint:%' AND c.previous IS NOT NULL
 LIMIT 65537
),
valid_roots AS (
 SELECT r.ordinal FROM projected_roots r, current_operation o
 WHERE (SELECT COUNT(*) FROM coverage)<65537
  AND EXISTS (SELECT 1 FROM coverage g WHERE g.root=r.ordinal AND g.source_scope NOT LIKE 'checkpoint:%')
  AND NOT EXISTS (
   SELECT 1 FROM coverage g WHERE g.root=r.ordinal AND NOT EXISTS (
    SELECT 1 FROM compaction_live_sources s
    WHERE s.workspace_id=o.workspace_id AND s.source_scope=g.source_scope
     AND s.source_id=g.source_id AND s.source_version=g.source_version
     AND EXISTS (SELECT 1 FROM json_each(o.snapshot,'$.source_epochs') e WHERE e.key=s.thread_id)
     AND (
      (g.source_scope LIKE 'checkpoint:%' AND EXISTS (
       SELECT 1 FROM compaction_checkpoint c WHERE c.id=g.source_id
        AND 'checkpoint:'||c.owner=g.source_scope AND c.format_version=1
      ))
      OR (g.source_scope NOT LIKE 'checkpoint:%' AND (
       s.thread_id=o.thread_id OR EXISTS (
        SELECT 1 FROM accepted_imports i WHERE i.source_scope=s.source_scope
         AND i.source_id=s.source_id AND i.source_version=s.source_version AND i.source_thread=s.thread_id
       )
      ))
     )
   )
  )
)
SELECT m.ordinal FROM compaction_manifest m JOIN current_operation o ON o.id=m.operation_id
WHERE
 EXISTS (SELECT 1 FROM json_each(o.snapshot,'$.source_epochs') wanted
  WHERE COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id=wanted.key),0)<>wanted.value
   OR NOT EXISTS (SELECT 1 FROM thread t WHERE t.id=wanted.key AND t.workspace_id=o.workspace_id))
 OR COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id=o.thread_id),0)<>json_extract(o.snapshot,'$.projection_version')
 OR NOT EXISTS (
  SELECT 1 FROM compaction_live_sources s
  WHERE s.source_scope=m.source_scope AND s.source_id=m.source_id AND s.source_version=m.source_version
   AND s.thread_id=m.source_thread AND s.workspace_id=o.workspace_id
   AND (m.reference_only=1 OR s.thread_id=o.thread_id
    OR EXISTS (SELECT 1 FROM accepted_imports i
     WHERE i.source_scope=s.source_scope AND i.source_id=s.source_id
      AND i.source_version=s.source_version AND i.source_thread=s.thread_id)
    OR EXISTS (SELECT 1 FROM valid_roots r WHERE r.ordinal=m.ordinal))
 )
LIMIT 1
"#, [operation.into()])).await?;
    Ok(stale.is_none())
}
