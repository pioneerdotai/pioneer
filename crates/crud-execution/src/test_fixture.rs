use pioneer_protocol::{
    ToolCallStatus, ToolDisplayPayload, ToolMetadata, ToolOutputPolicySnapshot, ToolStoragePayload,
    TurnItem,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

pub(crate) async fn fixture_event_scope(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> SqliteDatabase {
    let db = SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connection().clone(),
    );
    let transaction = db.begin().await.unwrap();
    for statement in [
        Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ($1,'Event validation',TRUE,FALSE)",
            [workspace.into()]),
        Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
             VALUES ($1,$2,'','agent','gpt-5.4','openai','active','user','workspace')",
            [thread.into(), workspace.into()]),
        Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin) VALUES ($1,$2,'in_progress','conversation','user')",
            [turn.into(), thread.into()]),
    ] {
        transaction.execute_raw(statement).await.unwrap();
    }
    transaction.commit().await.unwrap();
    db
}

pub(crate) fn safe_web_fetch_item(item_id: &str) -> TurnItem {
    TurnItem::WebFetch {
        id: item_id.to_owned(),
        tool_name: "web_fetch".to_owned(),
        arguments: serde_json::json!({"url": "https://example.com"}),
        status: ToolCallStatus::InProgress,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
        display: ToolDisplayPayload::Hidden,
        storage: ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(serde_json::json!({
                "url": "https://example.com",
                "statusCode": 200
            })),
        },
        recovery: None,
        url: Some("https://example.com".to_owned()),
        final_url: Some("https://example.com".to_owned()),
        status_code: Some(200),
        content_type: Some("text/html".to_owned()),
        extract_mode: None,
        resolved_mode: None,
        bytes_received: Some(1024),
        elapsed_ms: Some(42),
        truncated: Some(serde_json::json!(false)),
        title: Some("Example Domain".to_owned()),
        word_count: Some(12),
        links: Vec::new(),
        success: None,
        outcome: None,
        observation: None,
    }
}

use crate::cli_runtime_binding::FrozenBindingRoot;
use anyhow::Result;

pub(crate) struct InlineInput;

impl FrozenBindingRoot for InlineInput {
    fn locator(&self) -> Option<String> {
        None
    }

    async fn verify<'a, C: ConnectionTrait>(&'a self, _: &'a C, _: &'a str) -> Result<()> {
        Ok(())
    }
}
