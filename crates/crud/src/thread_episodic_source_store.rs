use crate::CrudStore;
use anyhow::Result;
use sea_orm::DatabaseTransaction;

pub(crate) use pioneer_crud_memory::episodic_source_store::PreparedSourceSave;

impl CrudStore {
    pub async fn reset_thread_episodic_projection(
        &self,
        workspace_id: &str,
        now_unix: i64,
    ) -> Result<u64> {
        pioneer_crud_memory::MemoryStore::new(self.connection.clone())
            .reset_thread_episodic_projection(workspace_id, now_unix)
            .await
    }

    pub(crate) async fn prepare_episodic_source_save(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item: &pioneer_protocol::TurnItem,
    ) -> Result<PreparedSourceSave> {
        pioneer_crud_memory::episodic_source_store::prepare_episodic_source_save(
            &self.connection,
            workspace_id,
            thread_id,
            turn_id,
            item,
        )
        .await
    }

    pub(crate) async fn commit_episodic_source_save(
        &self,
        db: &DatabaseTransaction,
        prepared: &PreparedSourceSave,
        now: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<()> {
        pioneer_crud_memory::episodic_source_store::commit_episodic_source_save(db, prepared, now)
            .await
    }
}
