use crate::CrudStore;
use anyhow::Result;
use sea_orm::DatabaseTransaction;

use pioneer_crud_memory::episodic_source_store::PreparedSourceSave;

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

pub(crate) struct EpisodicPayloadRepair<'a>(pub(crate) &'a CrudStore);

impl pioneer_crud_execution::read_model_repair::EpisodicRepair for EpisodicPayloadRepair<'_> {
    type Prepared = PreparedSourceSave;

    async fn prepare<'a>(
        &'a self,
        row: &'a pioneer_entity::turn_item::Model,
        payload_json: &'a str,
    ) -> Result<Option<Self::Prepared>> {
        self.0
            .memory_store()
            .prepare_terminal_payload_episodic_save(row, payload_json)
            .await
    }

    async fn commit<'a>(
        &'a self,
        transaction: &'a DatabaseTransaction,
        prepared: &'a Self::Prepared,
        now: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<()> {
        self.0
            .commit_episodic_source_save(transaction, prepared, now)
            .await
    }

    fn committed(&self) {
        self.0.episodic_work.notify_one();
    }
}

impl CrudStore {
    pub(crate) fn execution_read_model_repair(
        &self,
    ) -> pioneer_crud_execution::read_model_repair::ExecutionReadModelRepair<
        '_,
        EpisodicPayloadRepair<'_>,
    > {
        pioneer_crud_execution::read_model_repair::ExecutionReadModelRepair::new(
            &self.connection,
            EpisodicPayloadRepair(self),
        )
    }
}
