use anyhow::{Context, Result};
use pioneer_entity::user_notification_outbox;
use sea_orm::sea_query::{Expr, OnConflict, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect,
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

#[derive(FromQueryResult)]
struct NotificationReceipt {
    #[sea_orm(nested)]
    model: user_notification_outbox::Model,
    original_payload: String,
}

pub async fn insert_task_notification_idempotent<C: ConnectionTrait>(
    db: &C,
    notification: NewUserNotificationOutbox,
) -> Result<user_notification_outbox::Model> {
    use user_notification_outbox::{Column, Entity};
    let created_at = chrono::DateTime::from_timestamp(notification.created_at_unix, 0)
        .context("invalid user notification creation timestamp")?
        .fixed_offset();
    // Bound text is cast by PostgreSQL so the json column retains the exact
    // receipt bytes. SQLx otherwise sends JSON parameters as jsonb.
    db.execute(
        &Query::insert()
            .into_table(Entity)
            .columns([
                Column::Id,
                Column::TaskDeliveryId,
                Column::WorkspaceId,
                Column::RecipientPrincipalId,
                Column::TaskId,
                Column::RunId,
                Column::PayloadJson,
                Column::Status,
                Column::CreatedAt,
                Column::DeliveredAt,
            ])
            .values_panic([
                Expr::val(notification.id),
                Expr::val(&notification.task_delivery_id),
                Expr::val(&notification.workspace_id),
                Expr::val(&notification.recipient_principal_id),
                Expr::val(&notification.task_id),
                Expr::val(&notification.run_id),
                Expr::cust_with_expr(
                    "CAST(CAST($1 AS text) AS json)",
                    Expr::val(&notification.payload_json),
                ),
                // The committed receipt itself is durable delivery.
                Expr::val("delivered"),
                Expr::val(created_at),
                Expr::val(created_at),
            ])
            .on_conflict(
                OnConflict::column(Column::TaskDeliveryId)
                    .do_nothing()
                    .to_owned(),
            )
            .to_owned(),
    )
    .await
    .context("failed to insert durable user notification")?;

    let receipt = Entity::find()
        .column_as(Expr::cust("payload_json::text"), "original_payload")
        .filter(Column::TaskDeliveryId.eq(notification.task_delivery_id))
        .into_model::<NotificationReceipt>()
        .one(db)
        .await
        .context("failed to reload durable user notification")?
        .context("durable user notification is missing after insert")?;
    let persisted = receipt.model;
    if persisted.workspace_id != notification.workspace_id
        || persisted.recipient_principal_id != notification.recipient_principal_id
        || persisted.task_id != notification.task_id
        || persisted.run_id != notification.run_id
        || receipt.original_payload != notification.payload_json
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
    user_notification_outbox::Entity::update_many()
        .col_expr(
            user_notification_outbox::Column::Status,
            Expr::val("acknowledged"),
        )
        .col_expr(
            user_notification_outbox::Column::AcknowledgedAt,
            Expr::val(acknowledged_at),
        )
        .filter(user_notification_outbox::Column::Id.eq(notification_id))
        .filter(user_notification_outbox::Column::WorkspaceId.eq(workspace_id))
        .filter(user_notification_outbox::Column::RecipientPrincipalId.eq(recipient_principal_id))
        .filter(user_notification_outbox::Column::AcknowledgedAt.is_null())
        .exec(db)
        .await
        .context("failed to acknowledge durable user notification")?;
    find_user_notification_for_recipient(db, workspace_id, recipient_principal_id, notification_id)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notification_receipt_is_durable_and_idempotent_without_a_live_connection() {
        pioneer_postgres_test::with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared(
                "INSERT INTO task_delivery \
             (id, workspace_id, task_id, run_id, delivery_key, mode, status, \
              attempt_count, max_attempts, created_at, updated_at) \
             VALUES \
             ('delivery-a', 'workspace-a', 'task-a', 'run-a', \
              'delivery-key-a', 'user_notification', 'running', 1, 3, \
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            )
            .await
            .expect("parent task delivery");
            let notification = NewUserNotificationOutbox {
                id: "notification-a".to_owned(),
                task_delivery_id: "delivery-a".to_owned(),
                workspace_id: "workspace-a".to_owned(),
                recipient_principal_id: "principal-a".to_owned(),
                task_id: "task-a".to_owned(),
                run_id: "run-a".to_owned(),
                payload_json:
                    r#"{ "sidecar": {"nul":"\u0000"}, "notificationId": "notification-a" }"#
                        .to_owned(),
                created_at_unix: 1_700_000_000,
            };

            let receipt = insert_task_notification_idempotent(db, notification.clone())
                .await
                .expect("durable receipt");
            assert_eq!(receipt.status, "delivered");

            let duplicate = insert_task_notification_idempotent(db, notification.clone())
                .await
                .expect("idempotent durable receipt");
            assert_eq!(duplicate.id, receipt.id);
            assert_eq!(duplicate.status, "delivered");
            assert_eq!(duplicate.delivered_at, receipt.delivered_at);
            assert_eq!(
                receipt.payload_json["sidecar"]["nul"],
                serde_json::json!("\0")
            );
            let changed = NewUserNotificationOutbox {
                payload_json: receipt.payload_json.to_string(),
                ..notification
            };
            assert!(
                insert_task_notification_idempotent(db, changed)
                    .await
                    .is_err()
            );

            let inbox =
                list_user_notifications_for_recipient(db, "workspace-a", "principal-a", None, 10)
                    .await
                    .expect("recipient inbox");
            assert_eq!(inbox.len(), 1);
            assert_eq!(inbox[0].id, receipt.id);
            assert!(
                list_user_notifications_for_recipient(db, "workspace-a", "principal-b", None, 10,)
                    .await
                    .expect("other recipient inbox")
                    .is_empty()
            );

            let acknowledged = acknowledge_user_notification(
                db,
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
                    db,
                    "workspace-a",
                    "principal-b",
                    "notification-a",
                    1_700_000_020,
                )
                .await
                .expect("foreign acknowledgement lookup")
                .is_none()
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}
