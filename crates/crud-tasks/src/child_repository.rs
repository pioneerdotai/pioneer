//! Durable child work. All deadlines are Unix seconds. Call enqueue helpers
//! inside the domain transaction; notifications belong after commit.
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_conversations::context_cleanup::{
    MODEL_HISTORY_CONTEXT_SOURCES, delete_turn_llm_context_for_settled_child,
    delete_turn_llm_context_for_turn,
};
use pioneer_crud_conversations::turn_payload::PreflightRace;
use pioneer_crud_execution::turn_runtime_snapshot::delete_turn_runtime_snapshot;
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::TurnStatus;

use anyhow::{Context, Result, ensure};
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};
use pioneer_protocol::generate_id;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};

fn token() -> String {
    generate_id(32)
}

use crate::child_scopes::{
    self, ChildScopeRace, defer_child_scope_snapshot, expand_child_scope_page,
};
pub use crate::child_scopes::{ChildScope, enqueue_scope};
pub use crate::child_work::enqueue_point;
use crate::child_work::{self, point_fence};

use crate::task_run_turn::{self, task_run_turn_from_db_model};
use pioneer_protocol::TaskRunTurn;

impl TaskChildWorkRepository {
    pub async fn list_unreconciled_terminal_task_child_turns(
        &self,
        limit: u64,
    ) -> Result<Vec<UnreconciledTerminalTaskChildTurnRecord>> {
        crate::task_child_cleanup::list_unreconciled(&self.connection, limit)
            .await?
            .into_iter()
            .map(|row| {
                let status = turn_status_from_db(&row.status).with_context(|| {
                    format!("task child turn has unknown status `{}`", row.status)
                })?;
                Ok(UnreconciledTerminalTaskChildTurnRecord {
                    thread_id: row.thread_id,
                    turn_id: row.turn_id,
                    status,
                    error: row.error,
                })
            })
            .collect()
    }

    pub async fn discover_child_points(&self, now: i64, limit: u64) -> Result<Vec<point::Model>> {
        child_work::discover_child_points(&self.connection, now, limit).await
    }

    pub async fn discover_child_points_after(
        &self,
        now: i64,
        limit: u64,
        after: Option<(i64, &str)>,
    ) -> Result<Vec<point::Model>> {
        child_work::discover_child_points_after(&self.connection, now, limit, after).await
    }

    pub async fn child_point_due_upper(&self, now: i64) -> Result<Option<(i64, String)>> {
        child_work::child_point_due_upper(&self.connection, now).await
    }

    pub async fn discover_child_points_in_pass(
        &self,
        now: i64,
        limit: u64,
        after: Option<&(i64, String)>,
        upper: &(i64, String),
    ) -> Result<Vec<point::Model>> {
        child_work::discover_child_points_in_pass(&self.connection, now, limit, after, upper).await
    }

    pub async fn defer_child_point_snapshot<F: Fn() -> i64 + Sync>(
        &self,
        snapshot: &point::Model,
        clock: &F,
    ) -> Result<bool> {
        self.retry_write_on_conflict(|| {
            child_work::defer_child_point_snapshot(&self.connection, snapshot, clock)
        })
        .await
    }

    pub async fn claim_child_point(
        &self,
        candidate: &point::Model,
        now: i64,
    ) -> Result<Option<point::Model>> {
        self.claim_child_point_with_clock(candidate, &|| now).await
    }

    pub async fn claim_child_point_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        candidate: &point::Model,
        clock: &F,
    ) -> Result<Option<point::Model>> {
        self.retry_write_on_conflict(|| {
            child_work::claim_child_point_with_clock(&self.connection, candidate, clock)
        })
        .await
    }

    pub async fn finish_child_point(
        &self,
        claim: &point::Model,
        retry_at: Option<i64>,
        done: bool,
    ) -> Result<bool> {
        self.retry_write_on_conflict(|| {
            child_work::finish_child_point(&self.connection, claim, retry_at, done)
        })
        .await
    }
}

impl TaskChildWorkRepository {
    /// Advisory read first: an idle poll never reserves the writer.
    pub async fn expand_child_scope(&self, now: i64) -> Result<bool> {
        self.expand_child_scope_with_clock(&|| now).await
    }
    pub async fn expand_child_scope_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        clock: &F,
    ) -> Result<bool> {
        let Some(candidate) = self.discover_child_scope_after(clock(), None).await? else {
            return Ok(false);
        };
        self.expand_child_scope_candidate_with_clock(&candidate, clock)
            .await
    }
    pub async fn discover_child_scope_after(
        &self,
        now: i64,
        after: Option<&(i64, String, String)>,
    ) -> Result<Option<scope::Model>> {
        child_scopes::discover_child_scope_after(&self.connection, now, after).await
    }

    pub async fn child_scope_due_upper(&self, now: i64) -> Result<Option<(i64, String, String)>> {
        child_scopes::child_scope_due_upper(&self.connection, now).await
    }

    pub async fn discover_child_scope_in_pass(
        &self,
        now: i64,
        after: Option<&(i64, String, String)>,
        upper: &(i64, String, String),
    ) -> Result<Option<scope::Model>> {
        child_scopes::discover_child_scope_in_pass(&self.connection, now, after, upper).await
    }

    pub async fn expand_child_scope_candidate_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        candidate: &scope::Model,
        clock: &F,
    ) -> Result<bool> {
        let outcome = self
            .retry_write_on_conflict(|| async {
                let claim = token();
                let tx = self.connection.begin().await?;
                let result = expand_child_scope_page(&tx, candidate, &claim, clock()).await;
                match result {
                    Ok(true) => tx.commit().await?,
                    Ok(false) => tx.rollback().await?,
                    Err(error) if error.is::<ChildScopeRace>() => {
                        return Err(PreflightRace.into());
                    }
                    Err(error) => return Err(error),
                }
                Ok(true)
            })
            .await;
        if outcome.is_err() {
            // A poisoned locator must not monopolize the sorted due head.
            // The prior transaction has dropped; unknown commits are fenced
            // against the advisory snapshot before this independent deferral.
            let deferred = self
                .retry_write_on_conflict(|| async {
                    defer_child_scope_snapshot(&self.connection, candidate, clock()).await
                })
                .await;
            if let Err(deferral) = deferred {
                return Err(outcome.unwrap_err().context(format!(
                    "child scope independent deferral also failed: {deferral:#}"
                )));
            }
        }
        outcome
    }
}

impl TaskChildWorkRepository {
    pub async fn child_point_record(
        &self,
        claim: &point::Model,
    ) -> Result<Option<UnreconciledTerminalTaskChildTurnRecord>> {
        use pioneer_entity::{task, task_run, task_run_turn, turn};
        let canonical = turn::Entity::find_by_id(&claim.turn_id)
            .select_only()
            .column(turn::Column::ThreadId)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?;
        if canonical.as_ref().is_some_and(|id| id != &claim.thread_id) {
            // This is consumer bookkeeping, not a new domain invalidation.
            // Only the exact original claim may relinquish its lease and
            // schedule a canonical retry. A newer revision/owner is untouched.
            self.retry_write_on_conflict(|| async {
                let tx = self.connection.begin().await?;
                // Domain before queue follows cleanup/producer lock order.
                let current =
                    crate::task_child_producers::lock_capsule_turn(&tx, &claim.turn_id).await?;
                let owned = point::Entity::find()
                    .filter(point_fence(claim))
                    .filter(point::Column::ThreadId.eq(&claim.thread_id))
                    .lock_exclusive()
                    .one(&tx)
                    .await?;
                if owned.is_none() {
                    tx.rollback().await?;
                    return Ok(());
                }
                if let Some(thread_id) = current {
                    let changed = point::Entity::update_many()
                        .col_expr(point::Column::ThreadId, Expr::value(thread_id))
                        .col_expr(point::Column::Revision, Expr::value(token()))
                        .col_expr(
                            point::Column::NextAttemptAt,
                            Expr::value(chrono::Utc::now().timestamp()),
                        )
                        .col_expr(point::Column::AttemptCount, Expr::value(0i64))
                        .col_expr(
                            point::Column::ClaimToken,
                            Expr::value(Option::<String>::None),
                        )
                        .filter(point_fence(claim))
                        .filter(point::Column::ThreadId.eq(&claim.thread_id))
                        .exec(&tx)
                        .await?
                        .rows_affected;
                    if changed != 1 {
                        return Err(PreflightRace.into());
                    }
                }
                tx.commit().await?;
                Ok(())
            })
            .await?;
            return Ok(None);
        }
        let Some((turn_status, turn_error)) = turn::Entity::find_by_id(&claim.turn_id)
            .filter(turn::Column::ThreadId.eq(&claim.thread_id))
            .filter(turn::Column::TurnKind.eq("conversation"))
            .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted", "blocked"]))
            .select_only()
            .columns([turn::Column::Status, turn::Column::Error])
            .into_tuple::<(String, Option<String>)>()
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        let Some((task_id, run_id, link_status)) = task_run_turn::Entity::find()
            .filter(task_run_turn::Column::TurnId.eq(&claim.turn_id))
            .filter(task_run_turn::Column::ThreadId.eq(&claim.thread_id))
            .select_only()
            .columns([
                task_run_turn::Column::TaskId,
                task_run_turn::Column::RunId,
                task_run_turn::Column::Status,
            ])
            .into_tuple::<(String, String, String)>()
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        let Some(run_status) = task_run::Entity::find_by_id(run_id)
            .filter(task_run::Column::TaskId.eq(&task_id))
            .select_only()
            .column(task_run::Column::Status)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        let Some(owner_status) = task::Entity::find_by_id(task_id)
            .select_only()
            .column(task::Column::Status)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        let terminal = matches!(
            run_status.as_str(),
            "succeeded" | "failed" | "cancelled" | "timed_out"
        );
        if terminal {
            if turn_status == "blocked" {
                return Ok(None);
            }
            use pioneer_entity::{turn_llm_context as context, turn_runtime_snapshot as snapshot};
            let snapshot_present = snapshot::Entity::find_by_id(&claim.turn_id)
                .select_only()
                .column(snapshot::Column::TurnId)
                .into_tuple::<String>()
                .one(&self.connection)
                .await?
                .is_some();
            let context_present = if snapshot_present {
                false
            } else {
                context::Entity::find()
                    .select_only()
                    .column(context::Column::Id)
                    .filter(context::Column::TurnId.eq(&claim.turn_id))
                    .filter(context::Column::Source.is_not_in(MODEL_HISTORY_CONTEXT_SOURCES))
                    .into_tuple::<String>()
                    .one(&self.connection)
                    .await?
                    .is_some()
            };
            if !snapshot_present && !context_present {
                return Ok(None);
            }
        }
        let active = matches!(
            run_status.as_str(),
            "queued" | "starting" | "running" | "waiting" | "waiting_review"
        ) && matches!(
            owner_status.as_str(),
            "draft" | "scheduled" | "queued" | "running" | "waiting" | "waiting_review"
        ) && (link_status == "in_progress"
            || (link_status == "candidate_created" && run_status != "waiting_review")
            || (link_status == "cancelled" && turn_status == "interrupted"));
        if !terminal && !active {
            return Ok(None);
        }
        Ok(Some(UnreconciledTerminalTaskChildTurnRecord {
            thread_id: claim.thread_id.clone(),
            turn_id: claim.turn_id.clone(),
            status: turn_status_from_db(&turn_status).context("unknown child turn status")?,
            error: turn_error,
        }))
    }
}

impl TaskChildWorkRepository {
    /// Queue fence plus current domain proof, capsule deletion and ACK in one
    /// writer transaction. A queue token alone never authorizes deletion.
    pub async fn cleanup_claimed_child_capsule(&self, claim: &point::Model) -> Result<bool> {
        use pioneer_entity::{task, task_run, task_run_turn as link, thread, turn};
        self.retry_write_on_conflict(|| async {
            let tx = crate::task_child_cleanup::begin_authoritative(&self.connection).await?;
            crate::task_child_cleanup::lock_authority(&tx, &claim.thread_id, &claim.turn_id)
                .await?;
            if point::Entity::find()
                .filter(point_fence(claim))
                .one(&tx)
                .await?
                .is_none()
            {
                tx.rollback().await?;
                return Ok(false);
            }
            let cleanup_ready =
                crate::task_child_cleanup::ready(&tx, &claim.thread_id, &claim.turn_id).await?;
            match cleanup_ready {
                Some(false) => {
                    tx.rollback().await?;
                    return Ok(false);
                }
                Some(true) => {}
                None => {
                    let Some(child) = link::Entity::find()
                        .filter(link::Column::TurnId.eq(&claim.turn_id))
                        .filter(link::Column::ThreadId.eq(&claim.thread_id))
                        .one(&tx)
                        .await?
                    else {
                        tx.rollback().await?;
                        return Ok(false);
                    };
                    if matches!(child.status.as_str(), "in_progress" | "blocked") {
                        tx.rollback().await?;
                        return Ok(false);
                    }
                    let Some(_run) = task_run::Entity::find_by_id(&child.run_id)
                        .filter(task_run::Column::TaskId.eq(&child.task_id))
                        .filter(task_run::Column::Status.is_in([
                            "queued",
                            "starting",
                            "running",
                            "waiting",
                            "waiting_review",
                        ]))
                        .select_only()
                        .column(task_run::Column::Id)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                    else {
                        tx.rollback().await?;
                        return Ok(false);
                    };
                    let Some(workspace) = task::Entity::find_by_id(&child.task_id)
                        .filter(task::Column::Status.is_in([
                            "draft",
                            "scheduled",
                            "queued",
                            "running",
                            "waiting",
                            "waiting_review",
                        ]))
                        .select_only()
                        .column(task::Column::WorkspaceId)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                    else {
                        tx.rollback().await?;
                        return Ok(false);
                    };
                    if thread::Entity::find_by_id(&claim.thread_id)
                        .filter(thread::Column::WorkspaceId.eq(workspace))
                        .select_only()
                        .column(thread::Column::Id)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                        .is_none()
                        || turn::Entity::find_by_id(&claim.turn_id)
                            .filter(turn::Column::ThreadId.eq(&claim.thread_id))
                            .filter(turn::Column::TurnKind.eq("conversation"))
                            .filter(turn::Column::Status.is_in([
                                "completed",
                                "failed",
                                "interrupted",
                            ]))
                            .select_only()
                            .column(turn::Column::Id)
                            .into_tuple::<String>()
                            .one(&tx)
                            .await?
                            .is_none()
                    {
                        tx.rollback().await?;
                        return Ok(false);
                    }
                }
            }
            if cleanup_ready == Some(true) {
                delete_turn_llm_context_for_settled_child(&tx, &claim.turn_id).await?;
            } else {
                delete_turn_llm_context_for_turn(&tx, &claim.turn_id).await?;
            }
            delete_turn_runtime_snapshot(&tx, &claim.turn_id).await?;
            if !(point::Entity::delete_many()
                .filter(point_fence(claim))
                .exec(&tx)
                .await?
                .rows_affected
                == 1)
            {
                return Err(PreflightRace.into());
            }
            tx.commit().await?;
            Ok(true)
        })
        .await
    }
}

impl TaskChildWorkRepository {
    pub async fn next_task_child_work_deadline(&self) -> Result<Option<i64>> {
        child_scopes::next_task_child_work_deadline(&self.connection).await
    }
}

/// A terminal task-owned child Turn whose TaskRun aggregate has not yet
/// consumed the terminal outcome, or whose terminal aggregate has a received
/// parent projection and a retained capsule. The rows are durable retry tokens:
/// reconciliation can be repeated without depending on a process-local
/// completion callback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreconciledTerminalTaskChildTurnRecord {
    pub thread_id: String,
    pub turn_id: String,
    pub status: TurnStatus,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct TaskChildWorkRepository {
    connection: PostgresDatabase,
}

impl TaskChildWorkRepository {
    pub fn new(connection: &PostgresDatabase) -> Self {
        Self {
            connection: connection.clone(),
        }
    }

    #[cfg(test)]
    fn database_connection(&self) -> PostgresDatabase {
        self.connection.clone()
    }

    #[cfg(test)]
    fn with_maintenance_access(&self) -> Self {
        Self {
            connection: self.connection.maintenance(),
        }
    }

    async fn retry_write_on_conflict<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        pioneer_postgres_pools::retry_with_backoff(
            || operation(),
            crate::retry::is_retryable,
            pioneer_postgres_pools::DEFAULT_LOCK_RETRY_ATTEMPTS,
            std::time::Duration::from_millis(
                pioneer_postgres_pools::DEFAULT_LOCK_RETRY_BASE_DELAY_MS,
            ),
        )
        .await
    }
}

impl TaskChildWorkRepository {
    pub async fn upsert_task_run_turn(&self, turn: TaskRunTurn) -> Result<TaskRunTurn> {
        let change =
            crate::work_change::TaskWorkChange::for_tasks(&self.connection, [turn.task_id.clone()]);
        let before = self.get_task_run_turn(&turn.id).await?;
        let result = async {
            self.retry_write_on_conflict(|| async {
                let transaction = self.connection.begin().await?;
                task_run_turn::upsert_turn(
                    &transaction,
                    task_run_turn::NewTaskRunTurn {
                        id: turn.id.clone(),
                        task_id: turn.task_id.clone(),
                        run_id: turn.run_id.clone(),
                        execution_id: turn.execution_id.clone(),
                        thread_id: turn.thread_id.clone(),
                        turn_id: turn.turn_id.clone(),
                        kind: turn.kind,
                        round: turn.round,
                        sequence: turn.sequence,
                        status: turn.status,
                        reviews_candidate_id: turn.reviews_candidate_id.clone(),
                        requested_by_candidate_id: turn.requested_by_candidate_id.clone(),
                        requested_by_review_event_id: turn.requested_by_review_event_id.clone(),
                        created_at: turn.created_at,
                        started_at: turn.started_at,
                        completed_at: turn.completed_at,
                    },
                )
                .await?;
                let row = task_run_turn::find_turn_by_id(&transaction, &turn.id)
                    .await?
                    .context("task run turn missing after upsert")?;
                transaction.commit().await?;
                task_run_turn_from_db_model(row)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(before.as_ref() != Some(value));
        }
        result
    }

    pub async fn get_task_run_turn(&self, id: &str) -> Result<Option<TaskRunTurn>> {
        task_run_turn::find_turn_by_id(&self.connection, id)
            .await?
            .map(task_run_turn_from_db_model)
            .transpose()
    }
}

#[cfg(test)]
#[path = "child_repository_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "child_discovery_tests.rs"]
mod discovery_tests;
