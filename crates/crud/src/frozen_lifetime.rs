use anyhow::Result;
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_entity::compaction_frozen_history as history;
use sea_orm::TransactionTrait;

pub(crate) use pioneer_crud_compaction::frozen::{LayoutScan, header_bounds, logical_bounds};
pub(crate) use pioneer_crud_compaction::frozen_lifetime::{
    FrozenReadHold, PreparedFrozenRoot, ReaderCounts, changed_root_keys, cleanup_blocked,
    complete_keys, exact_header, html_cli_root_changed,
};

impl crate::CrudStore {
    pub(crate) async fn acquire_frozen_header(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<(history::Model, FrozenReadHold)> {
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let (header, hold) = pioneer_crud_compaction::frozen_lifetime::acquire_header(
                &tx,
                &self.frozen_readers,
                workspace,
                descriptor,
            )
            .await?;
            tx.commit().await?;
            Ok((header, hold))
        })
        .await
    }
    pub async fn compaction_acquire_frozen_history(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<FrozenReadHold> {
        let (header, hold) = self.acquire_frozen_header(workspace, descriptor).await?;
        let mut scan = LayoutScan::new(header, 0, 2)?;
        while !scan.done() {
            scan.step(&self.connection).await?;
        }
        Ok(hold)
    }
}
