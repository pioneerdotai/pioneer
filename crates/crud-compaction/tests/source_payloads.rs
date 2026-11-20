use anyhow::Result;
use pioneer_crud_compaction::source_payloads;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};
use serde_json::json;

#[tokio::test]
async fn input_and_provider_context_batches_preserve_order_exact_bytes_and_revision() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
               INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
               VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
               INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'completed');
               INSERT INTO turn_input (id, turn_id, input_index, input_type, payload)
               VALUES ('input', 'turn', 0, 'text', '{ "text": "界" }');
               INSERT INTO turn_llm_context (id, turn_id, sequence, source, payload)
               VALUES ('context', 'turn', 1, 'assistant_round', '{ "large": "🧪" }');"#,
        )
        .await?;
        let references = json!([
            {"scope": "context:turn", "id": "context", "version": "revision:1"},
            {"scope": "input:turn", "id": "input", "version": "input-revision:1"},
            {"scope": "context:turn", "id": "context", "version": "revision:1"},
        ])
        .to_string();
        let original_context = r#"{ "large": "🧪" }"#;
        let original_input = r#"{ "text": "界" }"#;

        let read = db
            .begin_with_config(
                Some(sea_orm::IsolationLevel::RepeatableRead),
                Some(sea_orm::AccessMode::ReadOnly),
            )
            .await?;
        let lengths = source_payloads::sizes(&read, "workspace", "thread", &references, 3).await?;
        assert_eq!(
            lengths
                .iter()
                .map(|row| (row.ordinal, row.bytes))
                .collect::<Vec<_>>(),
            [
                (0, i64::try_from(original_context.len())?),
                (1, i64::try_from(original_input.len())?),
                (2, i64::try_from(original_context.len())?)
            ],
        );
        let writer = fixture.connect().await?;
        writer
            .execute_unprepared(
                r#"UPDATE turn_llm_context SET payload = '{"new":1}' WHERE id = 'context'"#,
            )
            .await?;
        let saved = source_payloads::values(&read, "workspace", "thread", &references, 2).await?;
        assert_eq!(
            saved
                .iter()
                .map(|row| (row.ordinal, row.payload.as_str()))
                .collect::<Vec<_>>(),
            [(0, original_context), (1, original_input)]
        );
        read.commit().await?;
        let current = source_payloads::values(db, "workspace", "thread", &references, 3).await?;
        assert_eq!(
            current.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            [1]
        );
        assert!(
            source_payloads::values(db, "other", "thread", &references, 3)
                .await?
                .is_empty()
        );
        writer.close().await?;
        Ok(())
    })
    .await
}
