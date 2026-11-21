use crate::{CrudStore, NewTurnRuntimeSnapshot, TurnRuntimeSnapshotRecord};
use anyhow::Result;

impl CrudStore {
    pub async fn upsert_turn_runtime_snapshot(
        &self,
        snapshot: NewTurnRuntimeSnapshot,
    ) -> Result<TurnRuntimeSnapshotRecord> {
        self.compaction_store()
            .upsert_turn_runtime_snapshot(snapshot)
            .await
    }

    pub async fn upsert_turn_runtime_snapshot_with_source_epochs(
        &self,
        snapshot: NewTurnRuntimeSnapshot,
        source_epochs: &std::collections::BTreeMap<String, u64>,
    ) -> Result<TurnRuntimeSnapshotRecord> {
        self.compaction_store()
            .upsert_turn_runtime_snapshot_with_source_epochs(snapshot, source_epochs)
            .await
    }

    pub async fn get_turn_runtime_snapshot(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnRuntimeSnapshotRecord>> {
        self.compaction_store()
            .get_turn_runtime_snapshot(turn_id)
            .await
    }

    pub async fn clear_turn_runtime_snapshot_agent_skill_versions_if_matches(
        &self,
        thread_id: &str,
        turn_id: &str,
        expected_agent_skill_versions_json: &str,
    ) -> Result<bool> {
        self.compaction_store()
            .clear_turn_runtime_snapshot_agent_skill_versions_if_matches(
                thread_id,
                turn_id,
                expected_agent_skill_versions_json,
            )
            .await
    }

    pub async fn delete_turn_runtime_snapshot(&self, turn_id: &str) -> Result<u64> {
        self.compaction_store()
            .delete_turn_runtime_snapshot(turn_id)
            .await
    }

    pub async fn delete_turn_runtime_snapshots_for_closed_turns(&self) -> Result<u64> {
        self.compaction_store()
            .delete_turn_runtime_snapshots_for_closed_turns()
            .await
    }
}
