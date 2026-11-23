use super::*;
use crate::event_append::{PreparedTurnEvent, append_prepared_event};
use crate::test_fixture::fixture_event_scope;
use crate::time::unix_to_datetime;
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{ItemCompletedNotification, ToolMetadata, ToolOutputPolicySnapshot};
use sea_orm::TransactionTrait;

async fn persist_completion(
    db: &PostgresDatabase,
    notification: ItemCompletedNotification,
    at: i64,
) -> Result<()> {
    let event =
        PreparedTurnEvent::prepare_for_storage(TurnEventPayload::ItemCompleted(notification))?
            .preflight_idempotency(db)
            .await?;
    let tx = db.begin().await?;
    append_prepared_event(&tx, event, unix_to_datetime(at)).await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn permanent_storage_rejects_tool_payload_policy_shape_violations() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (workspace_id, thread_id, turn_id) =
            ("ws_raw_payload", "thr_raw_payload", "turn_raw_payload");
        let db = fixture_event_scope(fixture, workspace_id, thread_id, turn_id).await;
        let timestamp = 1_700_000_000;

        let non_shell_shell_storage_item = TurnItem::DynamicToolCall {
            id: "item_shell_storage".to_owned(),
            tool_name: "read_file".to_owned(),
            arguments: serde_json::json!({"path": "/tmp/secret.txt"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            execution_class: pioneer_protocol::TurnItemExecutionClass::Standard,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Shell {
                stdout: Some("not allowed".to_owned()),
                stderr: None,
                aggregated_output: Some("not allowed".to_owned()),
                exit_code: Some(0),
                duration_ms: None,
                timed_out: None,
                truncated: false,
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let error = persist_completion(
            &db,
            ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: non_shell_shell_storage_item,
            },
            timestamp + 1,
        )
        .await
        .expect_err("non-shell shell storage should be rejected");
        assert!(format!("{error:#}").contains("storage payload must be metadata-only"));

        let metadata_only_summary_storage_item = TurnItem::DynamicToolCall {
            id: "item_summary_storage".to_owned(),
            tool_name: "read_skill".to_owned(),
            arguments: serde_json::json!({"slug": "secret-skill"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            execution_class: pioneer_protocol::TurnItemExecutionClass::Standard,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("read_skill"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Summary(pioneer_protocol::ToolOutputSummary {
                title: "read_skill completed".to_owned(),
                lines: vec!["not allowed by metadata-only storage policy".to_owned()],
                metadata: ToolMetadata::empty(),
                truncated: false,
            }),
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let error = persist_completion(
            &db,
            ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: metadata_only_summary_storage_item,
            },
            timestamp + 2,
        )
        .await
        .expect_err("summary storage should be rejected for metadata-only policy");
        assert!(format!("{error:#}").contains("storage payload must be metadata-only"));

        let external_mcp_summary = pioneer_protocol::ToolOutputSummary {
            title: "Tool: mcp:Resend/send-email".to_owned(),
            lines: vec!["Sending email".to_owned()],
            metadata: ToolMetadata::from_json(serde_json::json!({
                "server": "Resend",
                "tool": "send-email",
                "arguments": {
                    "to": "alexander.oskin@gmail.com"
                }
            })),
            truncated: false,
        };
        let external_mcp_item = TurnItem::DynamicToolCall {
            id: "item_external_mcp".to_owned(),
            tool_name: "mcp:Resend/send-email".to_owned(),
            arguments: serde_json::json!({"to": "alexander.oskin@gmail.com"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            execution_class: pioneer_protocol::TurnItemExecutionClass::Standard,
            output_policy: ToolOutputPolicySnapshot::for_external_runtime_tool_name(
                "mcp:Resend/send-email",
            ),
            display: ToolDisplayPayload::Summary(external_mcp_summary.clone()),
            storage: ToolStoragePayload::Metadata {
                metadata: external_mcp_summary.metadata.clone(),
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        persist_completion(
            &db,
            ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: external_mcp_item,
            },
            timestamp + 3,
        )
        .await
        .expect("external runtime MCP metadata storage should be accepted");

        let retained_llm_context_item = TurnItem::DynamicToolCall {
            id: "item_llm_context".to_owned(),
            tool_name: "read_file".to_owned(),
            arguments: serde_json::json!({"path": "/tmp/secret.txt"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            execution_class: pioneer_protocol::TurnItemExecutionClass::Standard,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "llmView": {
                        "kind": "json",
                        "value": "not allowed in permanent storage"
                    }
                })),
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let error = persist_completion(
            &db,
            ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: retained_llm_context_item,
            },
            timestamp + 4,
        )
        .await
        .expect_err("retained llm context should be rejected");
        assert!(format!("{error:#}").contains("retained llm context key `llmView`"));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn item_completed_rejects_terminal_tool_payload_still_in_progress() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (workspace_id, thread_id, turn_id) = (
            "ws_terminal_payload",
            "thr_terminal_payload",
            "turn_terminal_payload",
        );
        let db = fixture_event_scope(fixture, workspace_id, thread_id, turn_id).await;
        let timestamp = 1_700_000_000;

        let stuck_tool_item = TurnItem::WebFetch {
            id: "item_stuck_tool".to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({"url": "https://example.com"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com"
                })),
            },
            recovery: None,
            url: Some("https://example.com".to_owned()),
            final_url: None,
            status_code: None,
            content_type: None,
            extract_mode: None,
            resolved_mode: None,
            bytes_received: None,
            elapsed_ms: None,
            truncated: None,
            title: None,
            word_count: None,
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        };

        let error = persist_completion(
            &db,
            ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: stuck_tool_item,
            },
            timestamp + 1,
        )
        .await
        .expect_err("terminal item completion must reject active tool payload");

        assert!(format!("{error:#}").contains("cannot remain in_progress"));
        Ok(())
    })
    .await
    .unwrap();
}
