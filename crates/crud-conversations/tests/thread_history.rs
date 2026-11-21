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
    PromptManifestProfile, SandboxMode, ThreadHistoryEventPayload, TurnCompletedNotification,
    TurnItem, TurnStatus,
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
