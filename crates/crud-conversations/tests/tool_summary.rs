use anyhow::Result;
use pioneer_crud_conversations::{
    tool_summary,
    turn_item::{self, ItemWrite},
};
use pioneer_crud_payloads::{BLOCK_BYTES, storage::PreparedPayload};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

#[tokio::test]
async fn tool_summary_keeps_first_labels_and_counts_only_matching_items() -> Result<()> {
    with_database(async |fixture| {
        let connection = fixture.connection();
        connection.execute_unprepared(
            "INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', '000000000000000000000', '', 'agent', 'model', 'provider', 'active');
             INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'completed');",
        ).await?;
        let time = chrono::DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")?;
        for (id, raw) in [
            ("0-no-tool", r#"{"success":true}"#),
            ("2-later", r#"{"toolName":"later","success":false}"#),
        ] {
            connection.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at)
                 VALUES ($1,'turn',$1,'dynamic_tool_call','completed',1,$2::text::json,$3,$3)",
                [id.into(), raw.into(), time.into()],
            )).await?;
        }
        let raw = format!(
            r#"{{"toolName":"first\u0000suffix","toolName":"wrong","success":true,"success":false,"ignored":"{}"}}"#,
            "é".repeat(BLOCK_BYTES),
        );
        let payload = PreparedPayload::new(&raw)?;
        let tx = connection.begin().await?;
        turn_item::upsert(&tx, ItemWrite {
            id: "1-first".to_owned(),
            turn_id: "turn".to_owned(),
            item_id: "1-first".to_owned(),
            item_type: "dynamic_tool_call".to_owned(),
            status: Some("completed".to_owned()),
            created_at: time,
            updated_at: time,
        }, &payload).await?;
        tx.commit().await?;

        let scoped = PostgresDatabase::from_single_connection(connection.clone());
        let summary = tool_summary::list(&scoped, "turn", 1).await?;
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].item_id, "1-first");
        assert_eq!(summary[0].tool_name, "first");
        assert!(summary[0].success);
        let both = tool_summary::list(&scoped, "turn", 2).await?;
        assert_eq!(both.len(), 2);
        assert_eq!(both[1].item_id, "2-later");
        assert!(!both[1].success);
        Ok(())
    }).await
}
