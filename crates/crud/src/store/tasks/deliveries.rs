//! Task delivery and user notification entry points.

use crate::{AgentCommitInput, CrudStore, NewUserNotificationOutbox, TaskRootAccessFilter};
use anyhow::Result;
use pioneer_crud_tasks::user_notification_outbox;
use pioneer_protocol::{TaskDeliveriesParams, TaskDeliveriesResponse, TaskDelivery};
use std::collections::HashMap;

impl CrudStore {
    pub async fn upsert_task_delivery_authority(
        &self,
        delivery_id: &str,
        task_id: &str,
        run_id: &str,
        author_json: &str,
        reviewer_json: Option<&str>,
        destination_route_id: Option<&str>,
        route_receipt_json: Option<&str>,
        disclosure_generation: u64,
        idempotency_key: &str,
        status: &str,
        now: i64,
    ) -> Result<()> {
        self.task_view_store()
            .upsert_task_delivery_authority(
                delivery_id,
                task_id,
                run_id,
                author_json,
                reviewer_json,
                destination_route_id,
                route_receipt_json,
                disclosure_generation,
                idempotency_key,
                status,
                now,
            )
            .await
    }

    pub async fn get_task_deliveries_by_ids(
        &self,
        workspace: &str,
        thread: &str,
        ids: &[String],
    ) -> Result<HashMap<String, TaskDelivery>> {
        self.task_view_store()
            .get_task_deliveries_by_ids(workspace, thread, ids)
            .await
    }

    pub async fn get_task_delivery(&self, delivery_id: &str) -> Result<Option<TaskDelivery>> {
        self.task_view_store().get_task_delivery(delivery_id).await
    }

    /// The exact-recipient inbox row is the notification delivery receipt.
    /// For an agent-produced result its canonical DeliverResult receipt is
    /// committed in the same transaction.
    pub async fn materialize_task_notification(
        &self,
        notification: NewUserNotificationOutbox,
        agent_action: Option<AgentCommitInput>,
    ) -> Result<pioneer_entity::user_notification_outbox::Model> {
        self.run_serialized_write(|| {
            self.materialize_task_notification_once(notification.clone(), agent_action.clone())
        })
        .await
    }

    pub async fn list_due_task_deliveries(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskDelivery>> {
        self.task_view_store()
            .list_due_task_deliveries(now, limit)
            .await
    }

    pub async fn list_task_deliveries(
        &self,
        params: TaskDeliveriesParams,
    ) -> Result<TaskDeliveriesResponse> {
        self.task_view_store().list_task_deliveries(params).await
    }

    pub async fn list_task_deliveries_scoped(
        &self,
        params: TaskDeliveriesParams,
        access: Option<&TaskRootAccessFilter>,
    ) -> Result<TaskDeliveriesResponse> {
        self.task_view_store()
            .list_task_deliveries_scoped(params, access)
            .await
    }
}
