use anyhow::{Context, Result};
use pioneer_crud_conversations::{
    events::{CanonicalTurnStartedEventPayload, TurnEventPayload},
    protocol_projection::{thread_from_db_model, turn_from_db_model},
    thread_history::get_thread_history,
    turn_event::{NewEvent, insert, lock_stream},
    turn_payload,
};
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_protocol::{
    ItemCompletedNotification, ItemUpdatedNotification, PersistedActorRef, PromptManifest,
    PromptManifestProfile, SandboxMode, ThreadHistoryEventPayload, ToolCallStatus,
    TurnCompletedNotification, TurnItem, TurnStatus,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};

async fn append(
    db: &SqliteDatabase,
    sequence: i64,
    seconds: i64,
    event: TurnEventPayload,
) -> Result<()> {
    let event_id = format!("event-{sequence}");
    let body = PreparedPayload::new(&serde_json::to_string(&event)?)?;
    let tx = db.begin().await?;
    lock_stream(&tx, event.turn_id()).await?;
    insert(
        &tx,
        NewEvent {
            id: &event_id,
            thread_id: event.thread_id(),
            turn_id: event.turn_id(),
            sequence,
            event_type: event.event_type(),
            idempotency_key: &event_id,
            created_at: chrono::DateTime::from_timestamp(seconds, 0)
                .context("fixture time")?
                .fixed_offset(),
        },
        &body,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn thread_history_decodes_legacy_cli_runtime_codex_manifest_without_losing_events() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection();
        connection
            .execute_unprepared(
                "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
             INSERT INTO turn (id, thread_id, status, send_mode)
             VALUES ('turn', 'thread', 'completed', 'agent');",
            )
            .await?;
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, connection.clone());
        let thread = pioneer_entity::thread::Entity::find_by_id("thread")
            .one(connection)
            .await?
            .context("thread fixture")?;
        let thread = thread_from_db_model(thread)?.context("thread DTO fixture")?;
        let row = pioneer_entity::turn::Entity::find_by_id("turn")
            .one(connection)
            .await?
            .context("turn fixture")?;
        let mut turn = turn_from_db_model(row)?.context("turn DTO fixture")?;
        let timestamp = 1_700_000_000;
        let mut started = turn.clone();
        started.status = TurnStatus::InProgress;
        append(
            &db,
            1,
            timestamp,
            TurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                thread: thread.clone(),
                sandbox_mode: SandboxMode::FullAccess,
                turn: started,
                input: Vec::new(),
                actor: Some(PersistedActorRef::System),
                reasoning_effort: None,
                work_owner: Default::default(),
            }),
        )
        .await?;
        let before_item = TurnItem::UserMessage {
            id: "item_before_legacy_manifest".to_owned(),
            text: "before".to_owned(),
            attachments: Vec::new(),
        };
        append(
            &db,
            2,
            timestamp + 1,
            TurnEventPayload::ItemCompleted(ItemCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: turn.id.clone(),
                item: before_item.clone(),
            }),
        )
        .await?;

        turn.status = TurnStatus::Completed;
        turn.prompt_manifest = Some(PromptManifest {
            compiler_version: "legacy-fixture".to_owned(),
            profile: PromptManifestProfile::CliRuntime,
            section_ids: Vec::new(),
            fingerprint_stable: "stable".to_owned(),
            fingerprint_dynamic: "dynamic".to_owned(),
            fingerprint_full: "full".to_owned(),
            diagnostics: Vec::new(),
            hook_sources: Vec::new(),
        });
        append(
            &db,
            3,
            timestamp + 2,
            TurnEventPayload::TurnCompleted(TurnCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn: turn.clone(),
            }),
        )
        .await?;

        let terminal_row = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .filter(
                pioneer_entity::turn_event::Column::EventType
                    .eq(pioneer_protocol::constants::events::TURN_COMPLETED),
            )
            .one(connection)
            .await
            .expect("terminal event lookup should succeed")
            .expect("terminal event should exist");
        let mut legacy_json = turn_payload::events(
            &db,
            pioneer_entity::turn_event::Entity::find_by_id(&terminal_row.id),
        )
        .await?
        .pop()
        .context("canonical event")?
        .payload
        .context("canonical payload JSON")?;
        legacy_json["payload"]["turn"]["prompt_manifest"]["profile"] =
            serde_json::json!("cli_runtime_codex");
        let legacy_json = serde_json::to_string(&legacy_json).expect("legacy fixture JSON");
        assert!(legacy_json.contains(r#""cli_runtime_codex""#));
        pioneer_entity::turn_event::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_event::Column::Payload,
                Expr::value(serde_json::from_str::<serde_json::Value>(&legacy_json).unwrap()),
            )
            .col_expr(
                pioneer_entity::turn_event::Column::PayloadPayloadId,
                Expr::value(Option::<String>::None),
            )
            .filter(pioneer_entity::turn_event::Column::Id.eq(terminal_row.id))
            .exec(connection)
            .await
            .expect("legacy fixture should replace stored event JSON");

        let after_item = TurnItem::UserMessage {
            id: "item_after_legacy_manifest".to_owned(),
            text: "after".to_owned(),
            attachments: Vec::new(),
        };
        append(
            &db,
            4,
            timestamp + 3,
            TurnEventPayload::ItemUpdated(ItemUpdatedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: turn.id.clone(),
                item: after_item.clone(),
            }),
        )
        .await?;

        let history = get_thread_history(&db, &thread.id, None)
            .await
            .expect("legacy nested TurnEventPayload should decode")
            .expect("thread history should remain present");
        assert_eq!(
            history
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert!(matches!(
            &history.events[1].payload,
            ThreadHistoryEventPayload::ItemCompleted { item, .. } if item == &before_item
        ));
        assert!(matches!(
            &history.events[2].payload,
            ThreadHistoryEventPayload::TurnCompleted { turn, .. }
                if turn.prompt_manifest.as_ref().map(|manifest| manifest.profile)
                    == Some(PromptManifestProfile::CliRuntime)
        ));
        assert!(matches!(
            &history.events[3].payload,
            ThreadHistoryEventPayload::ItemUpdated { item, .. } if item == &after_item
        ));
        Ok(())
    })
    .await
    .unwrap();
}

fn safe_web_fetch_item(item_id: &str) -> TurnItem {
    use pioneer_protocol::{
        ToolDisplayPayload, ToolMetadata, ToolOutputPolicySnapshot, ToolStoragePayload,
    };

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

#[tokio::test]
async fn thread_history_never_exposes_retained_turn_llm_context() {
    pioneer_postgres_test::with_database(async |fixture| {
        use pioneer_crud_conversations::turn_context::{NewTurnLlmContextEntry, insert};
        use pioneer_protocol::ToolOutputPolicySnapshot;

        let connection = fixture.connection();
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, connection.clone());
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_history_llm_context";
        let thread_id = "thr_history_llm_context";
        let turn_id = "turn_history_llm_context";
        let item_id = "item_history_llm_context";
        connection.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws_history_llm_context','History');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('thr_history_llm_context','ws_history_llm_context','','agent','m','p','active');
             INSERT INTO turn(id,thread_id,status,turn_kind,origin)
             VALUES ('turn_history_llm_context','thr_history_llm_context','in_progress','conversation','user');",
        ).await?;
        let thread = pioneer_entity::thread::Entity::find_by_id(thread_id)
            .one(&db).await?.context("thread fixture")?;
        let turn = pioneer_entity::turn::Entity::find_by_id(turn_id)
            .one(&db).await?.context("turn fixture")?;
        append(
            &db, 1, timestamp,
            TurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                thread: thread_from_db_model(thread)?.context("thread DTO fixture")?,
                sandbox_mode: SandboxMode::FullAccess,
                turn: turn_from_db_model(turn)?.context("turn DTO fixture")?,
                input: Vec::new(),
                actor: Some(PersistedActorRef::System),
                reasoning_effort: None,
                work_owner: Default::default(),
            }),
        ).await?;

        let mut completed_item = safe_web_fetch_item(item_id);
        if let TurnItem::WebFetch {
            status, success, ..
        } = &mut completed_item
        {
            *status = ToolCallStatus::Completed;
            *success = Some(true);
        }
        append(
                &db, 2, timestamp + 1,
                TurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: completed_item,
                }),
            )
            .await
            .expect("safe completed item should persist");

        let tx = db.begin().await?;
        insert(&tx, NewTurnLlmContextEntry {
                turn_id: turn_id.to_owned(),
                item_id: Some(item_id.to_owned()),
                attempt_id: Some("1".to_owned()),
                sequence: 1,
                source: "tool_result".to_owned(),
                tool_name: Some("web_fetch".to_owned()),
                payload: serde_json::json!({
                    "output": "SECRET_WEB_FETCH_BODY_SENTINEL"
                })
                .to_string(),
                output_policy_snapshot: serde_json::json!(ToolOutputPolicySnapshot::for_tool_name(
                    "web_fetch"
                ))
                .to_string(),
                created_at: chrono::DateTime::from_timestamp(timestamp + 2, 0).unwrap().fixed_offset(),
                expires_at: None,
            })
            .await
            .expect("retained llm context should persist");
        tx.commit().await?;

        let history = get_thread_history(&db, thread_id, Some(16))
            .await
            .expect("thread history query should succeed")
            .expect("thread history should exist");
        let history_json = serde_json::to_string(&history.events)
            .expect("thread history should serialize for leakage assertion");
        assert!(!history_json.contains("SECRET_WEB_FETCH_BODY_SENTINEL"));
        Ok(())
    })
    .await
    .unwrap();
}
