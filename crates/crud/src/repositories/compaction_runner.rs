use super::compaction::SOURCE_PAGE_BYTES;
#[cfg(test)]
use super::compaction::{CHECKPOINT_SOURCE_LIMIT, SOURCE_PAGE_ROWS, checkpoint_identity};
use crate::CrudStore;
use anyhow::{Result, ensure};
#[cfg(test)]
use pioneer_compaction::SourceRef;
use pioneer_compaction::runner::{RunnerPhase, RunnerState};
use pioneer_crud_compaction::runner_repository::null_safe_eq;
pub use pioneer_crud_compaction::runner_repository::{
    ManifestEntry, RunnerPlanRecord, compaction_bind_execution_turn,
    compaction_execution_cancelled, compaction_finish_execution, compaction_manifest_page,
    compaction_prepare_runner, compaction_reconcile_runner_state, compaction_runner_plan,
    compaction_runner_state, reconciled_terminal_runner,
};
use pioneer_crud_compaction::runner_state::update_runner_state;
use pioneer_entity::{
    compaction_attempt_observation, compaction_checkpoint, compaction_context, compaction_coverage,
    compaction_execution_stop, compaction_manifest, compaction_operation, compaction_runner_plan,
    compaction_runner_state, thread, turn,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, JoinType, Query};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sea_orm::{ConnectionTrait, TransactionTrait};

use pioneer_crud_compaction::runner_writes::json_text;

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PublicationTestPause {
    ReaderPreflight,
    BeforeWriter,
    ProofSeal,
    CheckpointOriginAcquire,
}

#[cfg(any(test, feature = "test-support"))]
struct RegisteredPublicationTestHook {
    token: std::sync::Arc<()>,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(any(test, feature = "test-support"))]
static PUBLICATION_TEST_HOOKS: std::sync::OnceLock<
    std::sync::Mutex<
        std::collections::BTreeMap<
            (usize, String, PublicationTestPause),
            RegisteredPublicationTestHook,
        >,
    >,
> = std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-support"))]
pub struct PublicationTestHookHandle {
    key: (usize, String, PublicationTestPause),
    token: std::sync::Arc<()>,
    _store: CrudStore,
    reached: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
}

#[cfg(any(test, feature = "test-support"))]
impl PublicationTestHookHandle {
    pub async fn reached(&mut self) {
        let reached = self
            .reached
            .take()
            .expect("publication hook already awaited");
        tokio::time::timeout(std::time::Duration::from_secs(10), reached)
            .await
            .expect("publication hook was not reached before the diagnostic timeout")
            .expect("publication hook participant ended before reaching the pause");
    }

    pub fn release(mut self) {
        self.release
            .take()
            .expect("publication hook already released")
            .send(())
            .expect("publication hook participant ended before release");
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for PublicationTestHookHandle {
    fn drop(&mut self) {
        let mut hooks = PUBLICATION_TEST_HOOKS
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if hooks
            .get(&self.key)
            .is_some_and(|hook| std::sync::Arc::ptr_eq(&hook.token, &self.token))
        {
            hooks.remove(&self.key);
        }
        // Dropping the release sender also unblocks a participant that already
        // took this registration and is waiting at the pause.
    }
}
#[cfg(test)]
pub(super) use pioneer_crud_compaction::proof_test_metrics::{
    PublicationWriterTestGuard, publication_test_metrics, record_proof_preparation_check,
    record_publication_heavy_check, reset_publication_test_metrics,
};

#[cfg(any(test, feature = "test-support"))]
pub fn arm_publication_test_hook(
    store: &CrudStore,
    operation: &str,
    phase: PublicationTestPause,
) -> PublicationTestHookHandle {
    let key = (
        store.connection.runtime_identity(),
        operation.to_owned(),
        phase,
    );
    let token = std::sync::Arc::new(());
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let hook = RegisteredPublicationTestHook {
        token: token.clone(),
        reached: reached_tx,
        release: release_rx,
    };
    let mut hooks = PUBLICATION_TEST_HOOKS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(!hooks.contains_key(&key), "publication hook already armed");
    hooks.insert(key.clone(), hook);
    PublicationTestHookHandle {
        key,
        token,
        _store: store.clone(),
        reached: Some(reached_rx),
        release: Some(release_tx),
    }
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub async fn trigger_publication_test_hook(
    store: &CrudStore,
    operation: &str,
    phase: PublicationTestPause,
) {
    pause_publication_test_hook(store.connection.runtime_identity(), operation, phase).await;
}

#[cfg(any(test, feature = "test-support"))]
pub(super) async fn pause_publication_test_hook(
    runtime_identity: usize,
    operation: &str,
    phase: PublicationTestPause,
) {
    let key = (runtime_identity, operation.to_owned(), phase);
    let hook = PUBLICATION_TEST_HOOKS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&key);
    if let Some(hook) = hook {
        let _ = hook.reached.send(());
        let _ = hook.release.await;
    }
}

pub(crate) use pioneer_crud_compaction::runner_writes::{
    compaction_activate_runner, compaction_append_manifest, compaction_runner_transition,
    prepare_checkpoint_ancestry,
};

pub(crate) use pioneer_crud_compaction::runner_resume::{
    compaction_resume_deadline, compaction_resume_invalid_completion,
};

/// Request-local validation of the saved candidate and its immutable coverage.
/// Live source revisions deliberately do not participate after summarization.
#[derive(Debug)]
struct PreparedRunnerPublication {
    operation: String,
    checkpoint: String,
    generation: u64,
    expected_head: Option<String>,
    identity_current: bool,
    coverage_exact: bool,
}

async fn prepare_runner_publication(
    store: &CrudStore,
    operation: &str,
    checkpoint: &str,
    generation: u64,
    expected_head: Option<&str>,
) -> Result<PreparedRunnerPublication> {
    let snapshot = store.connection.begin_read().await?;
    // Validate the saved candidate, not the current canonical history. Writer
    // publication rechecks this operation identity and generation atomically.
    let identity = snapshot
        .query_one_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            r#"
SELECT o.owner AS operation_owner,o.expected_head,
 c.workspace_id,p.operation_id AS candidate_operation,p.owner AS candidate_owner,
 s.generation AS runner_generation,
 s.state #>> '{phase,Commit,checkpoint}' AS runner_checkpoint
FROM compaction_operation o
LEFT JOIN compaction_context c ON c.owner=o.owner
LEFT JOIN compaction_checkpoint p ON p.id=$2
LEFT JOIN compaction_runner_state s ON s.operation_id=o.id
WHERE o.id=$1
"#,
            [operation.into(), checkpoint.into()],
        ))
        .await?;
    let expected_generation = i64::try_from(generation)?;
    let identity_current = if let Some(identity) = identity {
        let operation_owner: String = identity.try_get("", "operation_owner")?;
        let stored_head: Option<String> = identity.try_get("", "expected_head")?;
        let workspace: Option<String> = identity.try_get("", "workspace_id")?;
        let candidate_operation: Option<String> = identity.try_get("", "candidate_operation")?;
        let candidate_owner: Option<String> = identity.try_get("", "candidate_owner")?;
        let runner_generation: Option<i64> = identity.try_get("", "runner_generation")?;
        let runner_checkpoint: Option<String> = identity.try_get("", "runner_checkpoint")?;
        workspace.is_some()
            && candidate_operation.as_deref() == Some(operation)
            && candidate_owner.as_deref() == Some(operation_owner.as_str())
            && stored_head.as_deref() == expected_head
            && runner_generation == Some(expected_generation)
            && runner_checkpoint.as_deref() == Some(checkpoint)
    } else {
        false
    };

    let coverage_exact = if identity_current {
        #[cfg(any(test, feature = "test-support"))]
        pause_publication_test_hook(
            store.connection.runtime_identity(),
            operation,
            PublicationTestPause::ReaderPreflight,
        )
        .await;
        compaction_runner_coverage_exact(&snapshot, operation).await?
    } else {
        false
    };
    // Explicitly end the read snapshot before queuing for the writer. This
    // also releases the maintenance-read permit carried by the scoped store.
    snapshot.commit().await?;
    // Stale candidates do not publish. Validate the graph only after exact
    // saved coverage validation, with the read snapshot released.
    if identity_current && coverage_exact {
        super::checkpoint_proofs::prepare_graph(store, checkpoint).await?;
    }
    Ok(PreparedRunnerPublication {
        operation: operation.to_owned(),
        checkpoint: checkpoint.to_owned(),
        generation,
        expected_head: expected_head.map(str::to_owned),
        identity_current,
        coverage_exact,
    })
}

async fn compaction_runner_coverage_exact<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    #[cfg(test)]
    record_publication_heavy_check(operation, false);
    // Exact coverage: every selected record has a fully read version, and no
    // reference-only or unselected source has acquired coverage.
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
                            Expr::col(("p", compaction_checkpoint::Column::Id))
                                .eq(Expr::col(("c", compaction_coverage::Column::CheckpointId))),
                        )
                        .and_where(
                            Expr::col(("p", compaction_checkpoint::Column::OperationId))
                                .eq(Expr::col((
                                    compaction_manifest::Entity,
                                    compaction_manifest::Column::OperationId,
                                )))
                                .and(
                                    Expr::col(("c", compaction_coverage::Column::SourceScope)).eq(
                                        Expr::col((
                                            compaction_manifest::Entity,
                                            compaction_manifest::Column::SourceScope,
                                        )),
                                    ),
                                )
                                .and(Expr::col(("c", compaction_coverage::Column::SourceId)).eq(
                                    Expr::col((
                                        compaction_manifest::Entity,
                                        compaction_manifest::Column::SourceId,
                                    )),
                                ))
                                .and(
                                    Expr::col(("c", compaction_coverage::Column::SourceVersion))
                                        .eq(Expr::col((
                                            compaction_manifest::Entity,
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
        .into_tuple::<i64>()
        .one(db)
        .await?;
    let extra = compaction_coverage::Entity::find()
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
                                    Expr::col(("m", compaction_manifest::Column::ReferenceOnly))
                                        .eq(Expr::val(0_i64)),
                                )
                                .and(
                                    Expr::col((
                                        compaction_coverage::Entity,
                                        compaction_coverage::Column::SourceScope,
                                    ))
                                    .eq(Expr::col(("m", compaction_manifest::Column::SourceScope))),
                                )
                                .and(
                                    Expr::col((
                                        compaction_coverage::Entity,
                                        compaction_coverage::Column::SourceId,
                                    ))
                                    .eq(Expr::col(("m", compaction_manifest::Column::SourceId))),
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
        .one(db)
        .await?;
    Ok(missing.is_none() && extra.is_none())
}

/// Validate saved coverage outside writer capacity, then atomically publish.
/// A finished summary describes the versions actually consumed; subsequent
/// canonical edits/deletes and unrelated work cannot invalidate that result.
/// Candidate/manifest metadata is owned by this runner generation. Operation
/// identity, generation, cancellation and the target head can change while
/// preparing; the writer rechecks those exact control fields before commit.
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
    let prepared = store
        .run_scoped_database_quantum(|| {
            prepare_runner_publication(
                store,
                operation,
                checkpoint,
                state.generation,
                expected_head,
            )
        })
        .await?;
    #[cfg(any(test, feature = "test-support"))]
    pause_publication_test_hook(
        store.connection.runtime_identity(),
        operation,
        PublicationTestPause::BeforeWriter,
    )
    .await;
    ensure!(
        prepared.operation.as_str() == operation
            && prepared.checkpoint.as_str() == checkpoint.as_str()
            && prepared.generation == state.generation
            && prepared.expected_head.as_deref() == expected_head,
        "runner publication preflight identity mismatch"
    );
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            #[cfg(test)]
            let _writer_test_guard = PublicationWriterTestGuard::enter(operation);
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
                    .and(null_safe_eq(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::ExpectedHead,
                        )),
                        Expr::Value(expected_head.map(str::to_owned).into()),
                    )),
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
                        .is_not_null(),
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
                            json_text(
                                Expr::col(compaction_runner_state::Column::State),
                                &["phase", "Commit", "checkpoint"],
                            )
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
            if !prepared.identity_current || !prepared.coverage_exact {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            }
            let proof_ready = compaction_checkpoint::Entity::find_by_id(checkpoint)
                .select_only()
                .column(compaction_checkpoint::Column::Id)
                .filter(compaction_checkpoint::Column::OperationId.eq(operation))
                .filter(compaction_checkpoint::Column::ProofVersion.eq(1))
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            if proof_ready.is_none() {
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
                        .and(null_safe_eq(
                            Expr::col(compaction_context::Column::Head),
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
            update_runner_state(i64::try_from(applied.generation)?, encoded.clone())
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

use sea_orm::{QueryTrait, Statement};

#[cfg(test)]
use pioneer_crud_compaction::manifest::source_validation_statement as compaction_manifest_sources_current_statement;

/// Admission/read-time predicate. Validate raw inputs before consuming them;
/// publication of a completed summary never rechecks canonical source state.
pub(crate) async fn compaction_manifest_sources_current<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    #[cfg(test)]
    record_publication_heavy_check(operation, true);
    pioneer_crud_compaction::manifest::sources_current(db, operation).await
}

#[cfg(test)]
#[path = "compaction_manifest_source_lookup_tests.rs"]
mod manifest_source_lookup_tests;
