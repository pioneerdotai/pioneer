//! Task aggregates, contracts, and lineage.

use crate::{
    CrudStore, NewTaskRunConversationSnapshot, TaskCompletionInboxAccess, TaskCompletionInboxRow,
    TaskExecutionAdmissionRecord, TaskFinalizationRevision, TaskRootAccessFilter,
    TaskRunConversationSnapshotRecord,
};
use anyhow::Result;
use pioneer_crud_conversations::thread_lineage::task_thread_lineage_from_db_model;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_tasks::finalization_revision::task_finalization_revision_for_connection;
use pioneer_crud_tasks::task_trigger::task_trigger_from_db_model;
use pioneer_crud_tasks::{
    conversation_snapshot as task_run_conversation_snapshot, task_execution_admission, task_trigger,
};
use pioneer_protocol::{
    Task, TaskAgendaParams, TaskAgendaResponse, TaskGetResponse, TaskListParams, TaskThreadLineage,
    TaskTree, TaskTrigger,
};

impl CrudStore {
    pub async fn task_completion_inbox_patch(
        &self,
        access: &TaskCompletionInboxAccess<'_>,
        run_ids: &[String],
    ) -> Result<(Vec<TaskCompletionInboxRow>, u64)> {
        pioneer_crud_tasks::completion_inbox::addressed_patch(&self.connection, access, run_ids)
            .await
    }

    pub async fn get_task(&self, task_id: &str) -> Result<Option<TaskGetResponse>> {
        self.task_view_store().get_task(task_id).await
    }

    /// Load only the canonical Task row when authorization needs immutable
    /// task facts rather than the full aggregate. The aggregate reader below
    /// intentionally expands runs, bindings, review state, and lineage; using
    /// it in a hot execution revalidation path creates avoidable N+1 reads.
    pub async fn get_task_record(&self, task_id: &str) -> Result<Option<Task>> {
        pioneer_crud_tasks::task_event_context::TaskEventContextRepository::new(&self.connection)
            .get_task_record(task_id)
            .await
    }

    pub async fn upsert_task_actor_contract(
        &self,
        contract: &pioneer_protocol::TaskActorContract,
        now: i64,
    ) -> Result<()> {
        self.task_view_store()
            .upsert_task_actor_contract(contract, now)
            .await
    }

    pub async fn get_task_actor_contract(
        &self,
        task_id: &str,
    ) -> Result<Option<pioneer_protocol::TaskActorContract>> {
        self.task_view_store()
            .get_task_actor_contract(task_id)
            .await
    }

    pub async fn upsert_task_occurrence_contract(
        &self,
        contract: &pioneer_protocol::TaskOccurrenceContract,
        now: i64,
    ) -> Result<()> {
        self.task_occurrence_repository()
            .upsert_task_occurrence_contract(contract, now)
            .await
    }

    pub async fn get_task_occurrence_contract(
        &self,
        occurrence_id: &str,
    ) -> Result<Option<pioneer_protocol::TaskOccurrenceContract>> {
        self.task_occurrence_repository()
            .get_task_occurrence_contract(occurrence_id)
            .await
    }

    pub async fn get_task_occurrence_contract_by_run(
        &self,
        run_id: &str,
    ) -> Result<Option<pioneer_protocol::TaskOccurrenceContract>> {
        self.task_occurrence_repository()
            .get_task_occurrence_contract_by_run(run_id)
            .await
    }

    pub async fn list_task_occurrence_contracts(
        &self,
        task_id: &str,
        limit: u64,
    ) -> Result<Vec<pioneer_protocol::TaskOccurrenceContract>> {
        pioneer_crud_tasks::task_actor_contract::list_task_occurrences(
            &self.connection,
            task_id,
            limit,
        )
        .await
    }

    pub async fn next_task_occurrence_execution_generation(&self, task_id: &str) -> Result<u64> {
        pioneer_crud_tasks::task_actor_contract::next_task_occurrence_execution_generation(
            &self.connection,
            task_id,
        )
        .await
    }

    pub async fn task_occurrence_matches_execution_or_graph(
        &self,
        task_id: &str,
        execution_id: &str,
        work_graph_root_execution_id: &str,
    ) -> Result<bool> {
        pioneer_crud_tasks::task_actor_contract::task_occurrence_matches_execution_or_graph(
            &self.connection,
            task_id,
            execution_id,
            work_graph_root_execution_id,
        )
        .await
    }

    /// Return a canonical revision of every durable row that contributes to
    /// the attached-task finalization gate for one parent Turn.
    ///
    /// The token is intentionally derived from the complete, unbounded
    /// creator query. It is used as an optimistic fence around the Task
    /// Service snapshot and is checked again in the transaction that prepares
    /// the parent finalization intent.
    pub async fn task_finalization_revision(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<TaskFinalizationRevision> {
        task_finalization_revision_for_connection(&self.connection, thread_id, turn_id).await
    }

    pub async fn insert_task_run_conversation_snapshot_if_absent(
        &self,
        snapshot: NewTaskRunConversationSnapshot,
    ) -> Result<TaskRunConversationSnapshotRecord> {
        self.compaction_store()
            .insert_task_run_conversation_snapshot_if_absent(snapshot)
            .await
    }

    pub async fn get_task_run_conversation_snapshot(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskRunConversationSnapshotRecord>> {
        task_run_conversation_snapshot::get_scoped(&self.connection, run_id).await
    }

    pub async fn get_task_execution_admission(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskExecutionAdmissionRecord>> {
        task_execution_admission::find_by_task(&self.connection, task_id).await
    }

    /// Reconciles durable quota leases after restart. Admission and terminal
    /// release are normally committed with their subject event, while this
    /// pass closes legacy/crash leftovers idempotently before schedulers run.
    pub async fn reconcile_execution_admission_leases(&self) -> Result<usize> {
        self.task_execution_store()
            .reconcile_execution_admission_leases()
            .await
    }

    pub async fn delete_task_run_conversation_snapshot(&self, run_id: &str) -> Result<u64> {
        self.compaction_store()
            .delete_task_run_conversation_snapshot(run_id)
            .await
    }

    pub async fn list_tasks(&self, params: TaskListParams) -> Result<Vec<Task>> {
        self.list_tasks_scoped(params, None).await
    }

    /// Return every task causally created by one exact parent Turn. The exact
    /// workspace/thread/turn predicate is applied in SQL before ordering, and
    /// the lifecycle query is intentionally unbounded so a parent with many
    /// attached children cannot finalize from a truncated snapshot.
    pub async fn list_tasks_created_by_turn(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Vec<Task>> {
        self.task_view_store()
            .list_tasks_created_by_turn(workspace_id, thread_id, turn_id)
            .await
    }

    pub async fn list_tasks_scoped(
        &self,
        params: TaskListParams,
        access: Option<&TaskRootAccessFilter>,
    ) -> Result<Vec<Task>> {
        pioneer_crud_tasks::task_views::list_tasks_scoped(&self.connection, params, access).await
    }

    pub async fn get_task_tree(&self, task_id: &str) -> Result<Option<TaskTree>> {
        self.get_task_tree_bounded(task_id, usize::MAX).await
    }

    pub async fn get_task_tree_bounded(
        &self,
        task_id: &str,
        max_nodes: usize,
    ) -> Result<Option<TaskTree>> {
        pioneer_crud_tasks::task_views::get_task_tree_bounded(&self.connection, task_id, max_nodes)
            .await
    }

    pub async fn list_due_active_task_triggers(&self, now: i64) -> Result<Vec<TaskTrigger>> {
        task_trigger::list_due_active_triggers(&self.connection, unix_to_datetime(now))
            .await?
            .into_iter()
            .map(task_trigger_from_db_model)
            .collect()
    }

    pub async fn list_active_task_triggers(&self) -> Result<Vec<TaskTrigger>> {
        task_trigger::list_active_triggers(&self.connection)
            .await?
            .into_iter()
            .map(task_trigger_from_db_model)
            .collect()
    }

    pub async fn list_task_agenda(&self, params: TaskAgendaParams) -> Result<TaskAgendaResponse> {
        self.list_task_agenda_scoped(params, None).await
    }

    pub async fn list_task_agenda_scoped(
        &self,
        params: TaskAgendaParams,
        access: Option<&TaskRootAccessFilter>,
    ) -> Result<TaskAgendaResponse> {
        pioneer_crud_tasks::task_views::list_task_agenda_scoped(&self.connection, params, access)
            .await
    }

    pub async fn get_task_thread_lineage(
        &self,
        child_thread_id: &str,
    ) -> Result<Option<TaskThreadLineage>> {
        let row = pioneer_crud_conversations::thread_lineage::find_lineage_by_child_thread(
            &self.connection,
            child_thread_id,
        )
        .await?;
        Ok(row.map(task_thread_lineage_from_db_model))
    }

    /// Bounded direct edges for child listing and resumable subtree traversal.
    /// Callers must authorize each child and guard cycles along the DFS path.
    pub async fn list_thread_child_lineage_page(
        &self,
        parent_thread_id: &str,
        after_child_thread_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskThreadLineage>> {
        let rows = pioneer_crud_conversations::thread_lineage::list_children_page(
            &self.connection,
            parent_thread_id,
            after_child_thread_id,
            limit,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(task_thread_lineage_from_db_model)
            .collect())
    }

    pub async fn list_task_thread_lineage_for_parent(
        &self,
        parent_thread_id: &str,
    ) -> Result<Vec<TaskThreadLineage>> {
        let rows = pioneer_crud_conversations::thread_lineage::list_children_for_parent_thread(
            &self.connection,
            parent_thread_id,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(task_thread_lineage_from_db_model)
            .collect())
    }

    pub async fn list_task_thread_lineage_by_root_thread(
        &self,
        root_thread_id: &str,
    ) -> Result<Vec<TaskThreadLineage>> {
        let rows = pioneer_crud_conversations::thread_lineage::list_lineage_by_root_thread(
            &self.connection,
            root_thread_id,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(task_thread_lineage_from_db_model)
            .collect())
    }

    pub async fn list_task_thread_lineage_by_root_thread_page(
        &self,
        root_thread_id: &str,
        after: Option<&str>,
    ) -> Result<Vec<TaskThreadLineage>> {
        Ok(
            pioneer_crud_conversations::thread_lineage::list_lineage_by_root_thread_page(
                &self.connection,
                root_thread_id,
                after,
            )
            .await?
            .into_iter()
            .map(task_thread_lineage_from_db_model)
            .collect(),
        )
    }
}

impl CrudStore {
    pub(crate) fn task_view_store(&self) -> pioneer_crud_tasks::view_store::TaskViewStore<'_> {
        pioneer_crud_tasks::view_store::TaskViewStore::new(&self.connection)
    }
}
