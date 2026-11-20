use anyhow::Result;
use pioneer_crud::CrudStore;
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_entity::{portable_payload_value, turn_item, turn_item_attempt};
use pioneer_postgres_test::with_database;
use pioneer_tasks::{TaskRuntimeInvariantScanner, TaskRuntimeInvariantViolationKind};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, EntityTrait, Statement, TransactionTrait,
};

const OBSERVED_AT: i64 = 2_000_000_000;

#[tokio::test]
async fn scan_reads_inline_and_compressed_owners_without_changing_any_rows() -> Result<()> {
    for compressed in [false, true] {
        with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared(
                r#"
                INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
                INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                    VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
                INSERT INTO "turn" (id, thread_id, status) VALUES ('turn', 'thread', 'completed');
                INSERT INTO turn_item_attempt (id, turn_id, item_id, item_type, attempt_number, status)
                    VALUES ('attempt', 'turn', 'item', 'command_execution', 1, 'running');
                "#,
            )
            .await?;
            let source =
                serde_json::json!({"message": "compressed invariant fixture ".repeat(256)})
                    .to_string();
            let payload = compressed.then(|| PreparedPayload::new(&source)).transpose()?;
            let tx = db.begin().await?;
            if let Some(payload) = &payload {
                payload.insert(&tx).await?;
            }
            tx.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn_item (id, turn_id, item_id, item_type, status, payload, payload_payload_id)
                 VALUES ('row', 'turn', 'item', 'command_execution', 'completed', $1::text::json, $2)",
                [
                    (!compressed).then_some(source.as_str()).into(),
                    payload.as_ref().map(PreparedPayload::id).into(),
                ],
            ))
            .await?;
            tx.commit().await?;
            let original_items = turn_item::Entity::find().all(db).await?;
            let original_attempts = turn_item_attempt::Entity::find().all(db).await?;
            let original_payloads = portable_payload_value::Entity::find().all(db).await?;
            if compressed {
                assert_eq!(original_payloads.len(), 1);
                assert_eq!(original_payloads[0].codec, Some(1));
            }

            let mut options = ConnectOptions::new(fixture.url().to_string());
            options
                .max_connections(1)
                .sqlx_logging(false)
                .map_sqlx_postgres_opts(|options| {
                    options.options([("default_transaction_read_only", "on")])
                });
            let reader = Database::connect(options).await?;
            assert!(
                reader
                    .execute_unprepared("UPDATE turn_item SET status = 'failed'")
                    .await
                    .is_err()
            );
            let report = TaskRuntimeInvariantScanner::new()
                .scan_store(&CrudStore::new(reader.clone()), OBSERVED_AT)
                .await?;
            reader.close().await?;
            assert_eq!(report.generated_at_unix, Some(OBSERVED_AT));
            assert_eq!(report.violation_count(), 1, "{report}");
            assert_eq!(report.error_count(), 1, "{report}");
            let violation = &report.violations[0];
            assert_eq!(violation.code, "stale_turn_item_attempt", "{report}");
            assert_eq!(violation.entity_id, "item");
            assert!(
                matches!(&violation.kind,
                    TaskRuntimeInvariantViolationKind::StaleTurnItemAttempt {
                        turn_id, item_id, attempt_id, item_status, attempt_status,
                        attempt_number: Some(1), ..
                    } if turn_id == "turn" && item_id == "item" && attempt_id == "attempt"
                        && item_status == "completed" && attempt_status == "running"
                ),
                "{report}"
            );
            assert_eq!(turn_item::Entity::find().all(db).await?, original_items);
            assert_eq!(turn_item_attempt::Entity::find().all(db).await?, original_attempts);
            assert_eq!(portable_payload_value::Entity::find().all(db).await?, original_payloads);
            Ok(())
        })
        .await?;
    }
    Ok(())
}
