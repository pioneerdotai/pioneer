use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

const RETRY: &str = "task_delivery_recovery_retry";
const SOURCE_INDEX: &str = "idx_task_delivery_recovery_source";
const DUE_INDEX: &str = "idx_task_delivery_recovery_retry_due";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_index(
                Index::create()
                    .name(SOURCE_INDEX)
                    .table("task_delivery")
                    .col("status")
                    .col("updated_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(RETRY)
                    .col(text("delivery_id").primary_key())
                    .col(text("expected_attempt_id").null())
                    .col(big_integer("expected_attempt_count"))
                    .col(timestamp_with_time_zone("expected_updated_at"))
                    .col(text("retry_token"))
                    .col(
                        big_integer("next_probe_at")
                            .check(Expr::cust("typeof(next_probe_at)='integer'")),
                    )
                    .col(integer("attempts").check(Expr::col("attempts").between(1, 16)))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name(DUE_INDEX)
                    .table(RETRY)
                    .col("next_probe_at")
                    .col("delivery_id")
                    .to_owned(),
            )
            .await?;
        // Physical writes, including cascades, invalidate error bookkeeping.
        // Compare values null-safely: a same-value write preserves backoff.
        let delivery_fields = [
            "id",
            "workspace_id",
            "task_id",
            "run_id",
            "delivery_key",
            "mode",
            "thread_target",
            "target_thread_id",
            "target_user_id",
            "webhook_url",
            "webhook_url_fingerprint",
            "status",
            "next_attempt_at",
            "attempt_count",
            "max_attempts",
            "result_snapshot_json",
            "error_snapshot_json",
            "delivered_turn_id",
            "delivered_notification_id",
            "delivered_at",
            "last_error",
            "created_at",
            "updated_at",
        ];
        let attempt_fields = [
            "id",
            "delivery_id",
            "attempt_number",
            "status",
            "started_at",
            "completed_at",
            "http_status",
            "error",
            "response_fingerprint",
        ];
        let db = manager.get_connection();
        for (table, fields) in [
            ("task_delivery", &delivery_fields[..]),
            ("task_delivery_attempt", &attempt_fields[..]),
        ] {
            for event in ["insert", "update", "delete"] {
                if table == "task_delivery" && event == "insert" {
                    continue;
                }
                let condition = if event == "update" {
                    format!(
                        "WHEN {}",
                        fields
                            .iter()
                            .map(|f| format!("OLD.{f} IS NOT NEW.{f}"))
                            .collect::<Vec<_>>()
                            .join(" OR ")
                    )
                } else {
                    String::new()
                };
                let locator = if table == "task_delivery" {
                    "delivery_id=OLD.id".to_owned()
                } else {
                    // Only changes to the exact current attempt invalidate its delay.
                    let exact = |alias: &str| {
                        format!(
                            "delivery_id={alias}.delivery_id AND EXISTS(SELECT 1 FROM task_delivery d WHERE d.id={alias}.delivery_id AND d.attempt_count={alias}.attempt_number)"
                        )
                    };
                    match event {
                        "insert" => exact("NEW"),
                        "delete" => exact("OLD"),
                        _ => format!("({}) OR ({})", exact("OLD"), exact("NEW")),
                    }
                };
                db.execute_unprepared(&format!("CREATE TRIGGER {RETRY}_{table}_{event} AFTER {event} ON {table} {condition} BEGIN DELETE FROM {RETRY} WHERE {locator}; END")).await?;
            }
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in ["task_delivery", "task_delivery_attempt"] {
            for event in ["insert", "update", "delete"] {
                if table == "task_delivery" && event == "insert" {
                    continue;
                }
                manager
                    .get_connection()
                    .execute_unprepared(&format!("DROP TRIGGER IF EXISTS {RETRY}_{table}_{event}"))
                    .await?;
            }
        }
        manager
            .drop_index(Index::drop().name(DUE_INDEX).table(RETRY).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(RETRY).to_owned())
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name(SOURCE_INDEX)
                    .table("task_delivery")
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
