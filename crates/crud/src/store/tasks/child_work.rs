use crate::{CrudStore, UnreconciledTerminalTaskChildTurnRecord};
use anyhow::Result;
use pioneer_crud_compaction::frozen_lifetime;
use pioneer_crud_execution::turn_runtime_snapshot;
use pioneer_crud_tasks::child_repository::TaskChildWorkRepository;
use pioneer_crud_tasks::task_child_cleanup;
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};

impl CrudStore {
    pub async fn list_unreconciled_terminal_task_child_turns(
        &self,
        limit: u64,
    ) -> Result<Vec<UnreconciledTerminalTaskChildTurnRecord>> {
        TaskChildWorkRepository::new(&self.connection)
            .list_unreconciled_terminal_task_child_turns(limit)
            .await
    }

    pub async fn terminal_task_child_cleanup_ready(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<bool>> {
        task_child_cleanup::ready(&self.connection, thread_id, turn_id).await
    }

    /// Revalidate under the serialized writer and delete both capsule parts
    /// atomically. Until commit, their presence is the durable retry token.
    pub async fn cleanup_terminal_task_child_capsule(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<bool>> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let maintenance_changes = [
            self.connection
                .work_signal("maintenance.portable")
                .changing(),
            self.connection.work_signal("maintenance.html").changing(),
            self.connection.work_signal("maintenance.native").changing(),
        ];
        let maintenance_result = async {
            self.run_serialized_write(|| async {
                let tx =
                    task_child_cleanup::begin_authoritative(&self.connection).await?;
                task_child_cleanup::lock_authority(&tx, thread_id, turn_id).await?;
                let old_root =
                    turn_runtime_snapshot::find_frozen_manifest_id(&tx, turn_id).await?;

                let ready =
                    task_child_cleanup::ready(&tx, thread_id, turn_id).await?;
                if ready == Some(true) {
                    pioneer_crud_conversations::context_cleanup::delete_turn_llm_context_for_settled_child(&tx, turn_id)
                        .await?;
                    turn_runtime_snapshot::delete_turn_runtime_snapshot(&tx, turn_id).await?;
                }
                tx.commit().await?;
                Ok((
                    ready,
                    frozen_lifetime::changed_root_keys(
                        old_root.clone(),
                        if ready == Some(true) { None } else { old_root },
                    ),
                ))
            })
            .await
        }
        .await;
        let maintenance_changed = maintenance_result
            .as_ref()
            .is_ok_and(|(changed, _)| *changed == Some(true));
        if let Ok((_, keys)) = &maintenance_result {
            frozen_lifetime::complete_keys(frozen_change, keys, maintenance_changed);
        }
        if maintenance_result.is_ok() {
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result.map(|(ready, _)| ready)
    }

    pub async fn discover_child_points(&self, now: i64, limit: u64) -> Result<Vec<point::Model>> {
        TaskChildWorkRepository::new(&self.connection)
            .discover_child_points(now, limit)
            .await
    }

    pub async fn discover_child_points_after(
        &self,
        now: i64,
        limit: u64,
        after: Option<(i64, &str)>,
    ) -> Result<Vec<point::Model>> {
        TaskChildWorkRepository::new(&self.connection)
            .discover_child_points_after(now, limit, after)
            .await
    }

    pub async fn child_point_due_upper(&self, now: i64) -> Result<Option<(i64, String)>> {
        TaskChildWorkRepository::new(&self.connection)
            .child_point_due_upper(now)
            .await
    }

    pub async fn discover_child_points_in_pass(
        &self,
        now: i64,
        limit: u64,
        after: Option<&(i64, String)>,
        upper: &(i64, String),
    ) -> Result<Vec<point::Model>> {
        TaskChildWorkRepository::new(&self.connection)
            .discover_child_points_in_pass(now, limit, after, upper)
            .await
    }

    pub async fn defer_child_point_snapshot<F: Fn() -> i64 + Sync>(
        &self,
        snapshot: &point::Model,
        clock: &F,
    ) -> Result<bool> {
        TaskChildWorkRepository::new(&self.connection)
            .defer_child_point_snapshot(snapshot, clock)
            .await
    }

    pub async fn claim_child_point(
        &self,
        candidate: &point::Model,
        now: i64,
    ) -> Result<Option<point::Model>> {
        TaskChildWorkRepository::new(&self.connection)
            .claim_child_point(candidate, now)
            .await
    }

    pub async fn claim_child_point_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        candidate: &point::Model,
        clock: &F,
    ) -> Result<Option<point::Model>> {
        TaskChildWorkRepository::new(&self.connection)
            .claim_child_point_with_clock(candidate, clock)
            .await
    }

    pub async fn finish_child_point(
        &self,
        claim: &point::Model,
        retry_at: Option<i64>,
        done: bool,
    ) -> Result<bool> {
        TaskChildWorkRepository::new(&self.connection)
            .finish_child_point(claim, retry_at, done)
            .await
    }

    /// Advisory read first: an idle poll never reserves the writer.

    pub async fn expand_child_scope(&self, now: i64) -> Result<bool> {
        TaskChildWorkRepository::new(&self.connection)
            .expand_child_scope(now)
            .await
    }

    pub async fn expand_child_scope_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        clock: &F,
    ) -> Result<bool> {
        TaskChildWorkRepository::new(&self.connection)
            .expand_child_scope_with_clock(clock)
            .await
    }

    pub async fn discover_child_scope_after(
        &self,
        now: i64,
        after: Option<&(i64, String, String)>,
    ) -> Result<Option<scope::Model>> {
        TaskChildWorkRepository::new(&self.connection)
            .discover_child_scope_after(now, after)
            .await
    }

    pub async fn child_scope_due_upper(&self, now: i64) -> Result<Option<(i64, String, String)>> {
        TaskChildWorkRepository::new(&self.connection)
            .child_scope_due_upper(now)
            .await
    }

    pub async fn discover_child_scope_in_pass(
        &self,
        now: i64,
        after: Option<&(i64, String, String)>,
        upper: &(i64, String, String),
    ) -> Result<Option<scope::Model>> {
        TaskChildWorkRepository::new(&self.connection)
            .discover_child_scope_in_pass(now, after, upper)
            .await
    }

    pub async fn expand_child_scope_candidate_with_clock<F: Fn() -> i64 + Sync>(
        &self,
        candidate: &scope::Model,
        clock: &F,
    ) -> Result<bool> {
        TaskChildWorkRepository::new(&self.connection)
            .expand_child_scope_candidate_with_clock(candidate, clock)
            .await
    }

    pub async fn child_point_record(
        &self,
        claim: &point::Model,
    ) -> Result<Option<crate::UnreconciledTerminalTaskChildTurnRecord>> {
        TaskChildWorkRepository::new(&self.connection)
            .child_point_record(claim)
            .await
    }

    /// Queue fence plus current domain proof, capsule deletion and ACK in one

    /// writer transaction. A queue token alone never authorizes deletion.

    pub async fn cleanup_claimed_child_capsule(&self, claim: &point::Model) -> Result<bool> {
        TaskChildWorkRepository::new(&self.connection)
            .cleanup_claimed_child_capsule(claim)
            .await
    }

    pub async fn next_task_child_work_deadline(&self) -> Result<Option<i64>> {
        TaskChildWorkRepository::new(&self.connection)
            .next_task_child_work_deadline()
            .await
    }
}
