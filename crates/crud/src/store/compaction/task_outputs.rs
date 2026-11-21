use crate::CrudStore;
use anyhow::Result;
use pioneer_compaction::SourceRef;
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_crud_compaction::history::{AcceptedTaskBasis, HistoryReadFence, TaskInputCopyAlias};
use pioneer_crud_compaction::task_output::{TaskDeliveryOutputSnapshot, TaskOutputSnapshot};

impl CrudStore {
    /// Metadata locator for an already accepted legacy array. Payload remains
    /// in its original immutable TaskRun snapshot, read through source fragments.
    pub async fn compaction_legacy_task_basis_source(
        &self,
        workspace: &str,
        parent: &str,
        run: &str,
    ) -> Result<Option<SourceRef>> {
        self.compaction_store()
            .compaction_legacy_task_basis_source(workspace, parent, run)
            .await
    }

    /// The parent basis admitted for this exact child execution. Attachment is
    /// deliberately irrelevant: it controls lifecycle/hooks, not history scope.
    /// Read only identity metadata, never the snapshot transcript or Task body.
    pub async fn compaction_task_basis_thread(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_task_basis_thread(workspace, thread, turn)
            .await
    }

    /// For a destination without a creator execution, select the most recent
    /// admitted basis whose actual input existed at the shared capture fence.
    /// This reads relationship metadata, never a newer ancestor transcript.
    pub async fn compaction_latest_task_basis_turn(
        &self,
        workspace: &str,
        thread: &str,
        fence: &HistoryReadFence,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_latest_task_basis_turn(workspace, thread, fence)
            .await
    }

    /// Read the already accepted TaskRun basis in byte-bounded fragments.
    /// Each fragment repeats the exact execution/lineage/workspace predicate;
    /// deletion or reparenting cannot yield a partially authorized transcript.
    /// Snapshot rows are immutable (insert-if-absent); decoding is outside DB
    /// capacity. Legacy arrays retain their original bytes until migration.
    pub async fn compaction_task_basis_snapshot(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<AcceptedTaskBasis>> {
        self.compaction_store()
            .compaction_task_basis_snapshot(workspace, thread, turn)
            .await
    }

    /// Exact command/outcome relationship for a canonical delivered result.
    /// This is a point metadata query: neither matching text nor a delivered
    /// status alone establishes an alias. Frozen manifests preserve this link.
    pub async fn compaction_task_delivery_command(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_task_delivery_command(workspace, thread, source)
            .await
    }

    /// Resolve exact original/copy input pairs through the durable Task run
    /// relationship. The supplied copy IDs bound the result page; ambiguity
    /// counts are computed inside each exact source/initial turn.
    pub async fn compaction_task_input_copy_aliases(
        &self,
        workspace: &str,
        input_ids: &[String],
    ) -> Result<Vec<TaskInputCopyAlias>> {
        self.compaction_store()
            .compaction_task_input_copy_aliases(workspace, input_ids)
            .await
    }

    pub(crate) async fn compaction_failed_delivery_command(
        &self,
        workspace: &str,
        thread: &str,
        command: Option<&str>,
        source: Option<&SourceRef>,
        event_fence: i64,
        before: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_failed_delivery_command(
                workspace,
                thread,
                command,
                source,
                event_fence,
                before,
            )
            .await
    }

    /// First accepted manifest wins across retries. This is one metadata write;
    /// source materialization/hashing occurred before acquiring the writer.
    /// All prepared identities are revalidated in the INSERT predicate. This
    /// record alone grants no delivery or access to the source history.
    pub async fn compaction_record_task_output(
        &self,
        workspace: &str,
        task_run_turn: &str,
        history: &FrozenHistoryRef,
    ) -> Result<TaskOutputSnapshot> {
        self.compaction_store()
            .compaction_record_task_output(workspace, task_run_turn, history)
            .await
    }

    /// Point metadata read with the complete retained Task/turn relationship.
    /// No result payload or mutable current child transcript is loaded.
    pub async fn compaction_task_output(
        &self,
        workspace: &str,
        task_run_turn: &str,
    ) -> Result<Option<TaskOutputSnapshot>> {
        self.compaction_store()
            .compaction_task_output(workspace, task_run_turn)
            .await
    }

    pub async fn compaction_delivery_output(
        &self,
        workspace: &str,
        delivery: &str,
    ) -> Result<Option<TaskDeliveryOutputSnapshot>> {
        self.compaction_store()
            .compaction_delivery_output(workspace, delivery)
            .await
    }
}
