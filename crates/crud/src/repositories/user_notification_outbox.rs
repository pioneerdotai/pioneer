use anyhow::{Context, Result};
use pioneer_entity::user_notification_outbox;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, Set,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewUserNotificationOutbox {
    pub id: String,
    pub task_delivery_id: String,
    pub workspace_id: String,
    pub recipient_principal_id: String,
    pub task_id: String,
    pub run_id: String,
    pub payload_json: String,
    pub created_at_unix: i64,
}

pub async fn insert_task_notification_idempotent<C: ConnectionTrait>(
    db: &C,
    notification: NewUserNotificationOutbox,
) -> Result<user_notification_outbox::Model> {
    let expected = notification.clone();
    let created_at = chrono::DateTime::from_timestamp(notification.created_at_unix, 0)
        .context("invalid user notification creation timestamp")?
        .fixed_offset();
    user_notification_outbox::Entity::insert(user_notification_outbox::ActiveModel {
        id: Set(notification.id),
        task_delivery_id: Set(notification.task_delivery_id.clone()),
        workspace_id: Set(notification.workspace_id),
        recipient_principal_id: Set(notification.recipient_principal_id),
        task_id: Set(notification.task_id),
        run_id: Set(notification.run_id),
        payload_json: Set(notification.payload_json),
        // Committing this exact-recipient row is the durable inbox receipt.
        // WebSocket fanout is only a live invalidation hint and must not make
        // offline delivery fail or become the authority for `Delivered`.
        status: Set("delivered".to_owned()),
        created_at: Set(created_at),
        delivered_at: Set(created_at),
        acknowledged_at: Set(None),
    })
    .on_conflict(
        OnConflict::column(user_notification_outbox::Column::TaskDeliveryId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to insert durable user notification")?;

    let persisted = user_notification_outbox::Entity::find()
        .filter(user_notification_outbox::Column::TaskDeliveryId.eq(notification.task_delivery_id))
        .one(db)
        .await
        .context("failed to reload durable user notification")?
        .context("durable user notification is missing after insert")?;
    if persisted.workspace_id != expected.workspace_id
        || persisted.recipient_principal_id != expected.recipient_principal_id
        || persisted.task_id != expected.task_id
        || persisted.run_id != expected.run_id
        || persisted.payload_json != expected.payload_json
    {
        anyhow::bail!("task delivery conflicts with its durable user notification receipt");
    }
    Ok(persisted)
}

pub async fn list_user_notifications_for_recipient<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    recipient_principal_id: &str,
    before: Option<(i64, &str)>,
    limit: usize,
) -> Result<Vec<user_notification_outbox::Model>> {
    let mut query = user_notification_outbox::Entity::find()
        .filter(user_notification_outbox::Column::WorkspaceId.eq(workspace_id))
        .filter(user_notification_outbox::Column::RecipientPrincipalId.eq(recipient_principal_id));
    if let Some((created_at_unix, id)) = before {
        let created_at = chrono::DateTime::from_timestamp(created_at_unix, 0)
            .context("invalid user notification cursor timestamp")?
            .fixed_offset();
        query = query.filter(
            Condition::any()
                .add(user_notification_outbox::Column::CreatedAt.lt(created_at))
                .add(
                    Condition::all()
                        .add(user_notification_outbox::Column::CreatedAt.eq(created_at))
                        .add(user_notification_outbox::Column::Id.lt(id)),
                ),
        );
    }
    query
        .order_by_desc(user_notification_outbox::Column::CreatedAt)
        .order_by_desc(user_notification_outbox::Column::Id)
        .limit(u64::try_from(limit).unwrap_or(u64::MAX))
        .all(db)
        .await
        .context("failed to list durable user notifications")
}

pub async fn find_user_notification_for_recipient<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    recipient_principal_id: &str,
    notification_id: &str,
) -> Result<Option<user_notification_outbox::Model>> {
    user_notification_outbox::Entity::find_by_id(notification_id)
        .filter(user_notification_outbox::Column::WorkspaceId.eq(workspace_id))
        .filter(user_notification_outbox::Column::RecipientPrincipalId.eq(recipient_principal_id))
        .one(db)
        .await
        .context("failed to load durable user notification")
}

pub async fn acknowledge_user_notification<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    recipient_principal_id: &str,
    notification_id: &str,
    acknowledged_at_unix: i64,
) -> Result<Option<user_notification_outbox::Model>> {
    let Some(model) = find_user_notification_for_recipient(
        db,
        workspace_id,
        recipient_principal_id,
        notification_id,
    )
    .await?
    else {
        return Ok(None);
    };
    if model.acknowledged_at.is_some() {
        return Ok(Some(model));
    }
    let acknowledged_at = chrono::DateTime::from_timestamp(acknowledged_at_unix, 0)
        .context("invalid user notification acknowledgement timestamp")?
        .fixed_offset();
    let mut active = model.into_active_model();
    active.status = Set("acknowledged".to_owned());
    active.acknowledged_at = Set(Some(acknowledged_at));
    active
        .update(db)
        .await
        .map(Some)
        .context("failed to acknowledge durable user notification")
}

#[cfg(test)]
mod tests {
    use sea_orm::{Database, DbBackend, Schema};

    use super::*;

    #[tokio::test]
    async fn notification_receipt_is_durable_and_idempotent_without_a_live_connection() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory database");
        let statement = Schema::new(DbBackend::Sqlite)
            .create_table_from_entity(user_notification_outbox::Entity);
        db.execute(&statement).await.expect("outbox table");
        let notification = NewUserNotificationOutbox {
            id: "notification-a".to_owned(),
            task_delivery_id: "delivery-a".to_owned(),
            workspace_id: "workspace-a".to_owned(),
            recipient_principal_id: "principal-a".to_owned(),
            task_id: "task-a".to_owned(),
            run_id: "run-a".to_owned(),
            payload_json: "{\"notificationId\":\"notification-a\"}".to_owned(),
            created_at_unix: 1_700_000_000,
        };

        let receipt = insert_task_notification_idempotent(&db, notification.clone())
            .await
            .expect("durable receipt");
        assert_eq!(receipt.status, "delivered");

        let duplicate = insert_task_notification_idempotent(&db, notification)
            .await
            .expect("idempotent durable receipt");
        assert_eq!(duplicate.id, receipt.id);
        assert_eq!(duplicate.status, "delivered");
        assert_eq!(duplicate.delivered_at, receipt.delivered_at);

        let inbox =
            list_user_notifications_for_recipient(&db, "workspace-a", "principal-a", None, 10)
                .await
                .expect("recipient inbox");
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].id, receipt.id);
        assert!(
            list_user_notifications_for_recipient(&db, "workspace-a", "principal-b", None, 10,)
                .await
                .expect("other recipient inbox")
                .is_empty()
        );

        let acknowledged = acknowledge_user_notification(
            &db,
            "workspace-a",
            "principal-a",
            "notification-a",
            1_700_000_010,
        )
        .await
        .expect("acknowledge notification")
        .expect("notification exists");
        assert_eq!(acknowledged.status, "acknowledged");
        assert_eq!(
            acknowledged
                .acknowledged_at
                .expect("acknowledgement timestamp")
                .timestamp(),
            1_700_000_010
        );
        assert!(
            acknowledge_user_notification(
                &db,
                "workspace-a",
                "principal-b",
                "notification-a",
                1_700_000_020,
            )
            .await
            .expect("foreign acknowledgement lookup")
            .is_none()
        );
    }
}
