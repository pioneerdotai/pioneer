use anyhow::Result;
use pioneer_protocol::TaskDelivery;
use sea_orm::ConnectionTrait;

pub use pioneer_crud_tasks::task_projector::{
    PreparedDeliveryAuthorityBoundary, PreparedTaskProjection, TaskProjector,
    prepare_delivery_authority_boundary, prepare_task_delivery_authority,
};

pub(crate) struct CompactionTaskOutputs;

impl pioneer_crud_tasks::task_projector::TaskOutputBinder for CompactionTaskOutputs {
    async fn bind<'a, C: ConnectionTrait>(
        &'a self,
        db: &'a C,
        delivery: &'a TaskDelivery,
    ) -> Result<()> {
        crate::compaction::bind_queued_task_output(db, delivery).await
    }
}
