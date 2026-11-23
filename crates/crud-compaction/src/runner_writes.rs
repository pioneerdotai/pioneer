//! Operation-local runner admission, manifest batches and state transitions.

use anyhow::{Result, ensure};
use pioneer_compaction::Checkpoint;
use pioneer_compaction::runner::{FailureKind, RunnerPhase, RunnerState};
use pioneer_entity::{
    compaction_attempt_observation, compaction_checkpoint, compaction_context, compaction_coverage,
    compaction_manifest, compaction_operation, compaction_runner_plan, compaction_runner_state,
    thread, turn,
};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::sea_query::{Alias, Asterisk, Expr, ExprTrait, Func, JoinType, OnConflict, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect,
    TransactionTrait,
};
use sha2::{Digest, Sha256};

use crate::runner_repository::{ManifestEntry, null_safe_eq};
use crate::runner_state::{insert_initial, update_runner_state};
use crate::sweep_work::retry_conflict;
use crate::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS};

async fn lock_operation(db: &DatabaseTransaction, id: &str) -> Result<()> {
    // This is a separate statement so the subsequent manifest counts use a
    // fresh READ COMMITTED snapshot after waiting for another publisher.
    let operation = compaction_operation::Entity::find_by_id(id)
        .select_only()
        .column(compaction_operation::Column::Id)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(operation.is_some(), "compaction operation is missing");
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub enum CanonicalSource {
    Input,
    Event,
    ProviderContext,
    ToolItem,
}
impl CanonicalSource {
    pub fn version_prefix(self) -> &'static str {
        match self {
            Self::Input => "input-revision",
            Self::Event => "event-revision",
            Self::ProviderContext => "revision",
            Self::ToolItem => "item-revision",
        }
    }
    pub fn prefix(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Event => "event",
            Self::ProviderContext => "context",
            Self::ToolItem => "item",
        }
    }
}

pub fn checkpoint_identity(checkpoint: &Checkpoint) -> Result<String> {
    let mut canonical = checkpoint.clone();
    canonical.coverage.sort();
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&canonical)?)))
}

pub async fn compaction_append_manifest(
    db: &PostgresDatabase,
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
            let exact = compaction_manifest::Entity::find()
                .filter(compaction_manifest::Column::OperationId.eq(operation))
                .filter(compaction_manifest::Column::Ordinal.eq(ordinal))
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
                "input" => CanonicalSource::Input,
                "event" => CanonicalSource::Event,
                "context" => CanonicalSource::ProviderContext,
                "item" => CanonicalSource::ToolItem,
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
    retry_conflict(|| async {
        let txn = db.begin().await?;
        lock_operation(&txn, operation).await?;
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
                CanonicalSource::Input => {
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
                CanonicalSource::Event => {
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
                CanonicalSource::ProviderContext => {
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
                CanonicalSource::ToolItem => {
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
pub async fn compaction_activate_runner(
    db: &PostgresDatabase,
    operation: &str,
    initial: &RunnerState,
) -> Result<bool> {
    ensure!(
        initial.generation == 0
            && initial.attempts == 0
            && initial.retries == 0
            && initial.portion_attempts == 0
            && initial.overflow_rejections == 0
            && initial.overflow_input_ceiling.is_none()
            && initial.corrections == 0,
        "invalid initial runner state"
    );
    let encoded = serde_json::to_string(initial)?;
    let deadline = i64::try_from(initial.deadline_ms)?;
    retry_conflict(|| async {
        let txn = db.begin().await?;
        lock_operation(&txn, operation).await?;
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
                                                .add(Expr::col((
                                                    compaction_runner_plan::Entity,
                                                    compaction_runner_plan::Column::ReferenceCount,
                                                ))),
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
        let inserted = insert_initial(&txn, operation, encoded.clone()).await?;
        let ready = compaction_runner_plan::Entity::update_many()
            .col_expr(compaction_runner_plan::Column::Ready, Expr::val(1_i64))
            .filter(
                Expr::col(compaction_runner_plan::Column::OperationId)
                    .eq(Expr::Value(operation.into())),
            )
            .filter(compaction_runner_plan::Column::Ready.ne(1_i64))
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(inserted || ready.rows_affected > 0)
    })
    .await
}

pub async fn compaction_runner_transition(
    db: &PostgresDatabase,
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
            && next.overflow_rejections <= 4
            // Total claims include rejected overflows. Preserve the independent
            // ordinary allowance, rejecting inconsistent counters before subtracting.
            && next.portion_attempts.checked_sub(next.overflow_rejections)
                .is_some_and(|ordinary| ordinary <= 3)
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
                && cp.coverage.len() <= pioneer_compaction::REPLAY_ALIAS_LIMIT
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
            proof_version: sea_orm::Set(0),
            id: sea_orm::Set(cp.id.clone()),
            operation_id: sea_orm::Set(operation.to_owned()),
            owner: sea_orm::Set(cp.owner.clone()),
            previous: sea_orm::Set(cp.previous.clone()),
            portion: sea_orm::Set(i64::try_from(next.attempts)?),
            summary: sea_orm::Set(cp.summary.clone()),
            selection: sea_orm::Set(serde_json::to_value(&cp.selection)?),
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
                        .eq(json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["projection_version"],
                        )
                        .cast_as("bigint")),
                    )
                    .and(
                        json_text(
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            &["transport"],
                        )
                        .eq(json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["admission", "selection", "transport"],
                        )),
                    )
                    .and(
                        json_text(
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            &["instance"],
                        )
                        .eq(json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["admission", "selection", "instance"],
                        )),
                    )
                    .and(
                        json_text(
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            &["model"],
                        )
                        .eq(json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["admission", "selection", "model"],
                        )),
                    )
                    .and(null_safe_eq(
                        json_text(
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            &["effort"],
                        ),
                        json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["admission", "selection", "effort"],
                        ),
                    )),
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
                observation: sea_orm::Set(serde_json::to_value(observation)?),
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
                        .and(null_safe_eq(
                            json_text(
                                Expr::col(compaction_runner_state::Column::State),
                                &["previous_checkpoint"],
                            ),
                            Expr::Value(cp.previous.clone().into()),
                        )),
                )
        });
    let (status, outcome) = match &next.phase {
        RunnerPhase::Failed {
            kind: FailureKind::Cancelled,
        } => ("cancelled", Some("cancelled")),
        RunnerPhase::Failed { .. } => ("failed", Some("runner_failed")),
        _ => ("running", None),
    };
    retry_conflict(|| async {
        let txn = db.begin().await?;
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
        let updated = update_runner_state(i64::try_from(next.generation)?, state.clone())
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
                                        Expr::col(("o", compaction_operation::Column::DeadlineMs))
                                            .eq(Expr::Value(
                                                i64::try_from(next.deadline_ms)?.into(),
                                            )),
                                    )
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Attempts))
                                            .lte(Expr::Value(i64::try_from(next.attempts)?.into())),
                                    )
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Attempts))
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
                                        Expr::col(("o", compaction_operation::Column::Correction))
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
        if let Some(cp) = candidate {
            let portion = i64::try_from(next.attempts)?;
            let next_portion = portion
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("candidate portion overflow"))?;
            compaction_operation::Entity::update_many()
                .col_expr(
                    compaction_operation::Column::NextPortion,
                    Expr::val(next_portion),
                )
                .filter(compaction_operation::Column::Id.eq(&cp.operation_id))
                .filter(compaction_operation::Column::NextPortion.lt(next_portion))
                .exec(&txn)
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
            .filter(Expr::col(compaction_operation::Column::Id).eq(Expr::Value(operation.into())))
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
pub async fn prepare_checkpoint_ancestry<C: ConnectionTrait>(
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
                        Expr::col(compaction_checkpoint::Column::Status).eq(Expr::val("candidate")),
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
                                        json_text(
                                            Expr::col((
                                                "s",
                                                compaction_runner_state::Column::State,
                                            )),
                                            &["phase", "Commit", "checkpoint"],
                                        )
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

pub fn json_text(column: Expr, path: &[&str]) -> Expr {
    Expr::expr(Func::cust(Alias::new("json_extract_path_text")).args(
        std::iter::once(column.cast_as("json")).chain(path.iter().map(|key| Expr::val(*key))),
    ))
}
