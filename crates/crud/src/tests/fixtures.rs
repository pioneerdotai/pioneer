use super::*;

pub(super) async fn read_fixture_item(
    db: &pioneer_sqlite::SqliteDatabase,
    row: pioneer_entity::turn_item::Model,
) -> anyhow::Result<TurnItem> {
    let mut items = pioneer_crud_conversations::turn_payload::hydrate_items(db, vec![row]).await?;
    pioneer_crud_conversations::turn_payload::decode_item(&items.pop().expect("one item"))
}

pub(crate) async fn postgres_store_with_workspace(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace_id: &str,
) -> CrudStore {
    let connection = fixture.connection();
    let tx = connection.begin().await.expect("begin workspace fixture");
    pioneer_entity::workspace::Entity::update_many()
        .col_expr(
            pioneer_entity::workspace::Column::IsCurrent,
            Expr::value(false),
        )
        .filter(pioneer_entity::workspace::Column::IsCurrent.eq(true))
        .exec(&tx)
        .await
        .expect("clear the seeded current workspace");
    let timestamp = unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set(workspace_id.to_owned()),
        name: Set("Test Workspace".to_owned()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
    })
    .exec(&tx)
    .await
    .expect("insert workspace fixture");
    tx.commit().await.expect("commit workspace fixture");
    let reader = fixture
        .connect_read_only()
        .await
        .expect("read-only PostgreSQL pool");
    CrudStore::new(pioneer_sqlite::SqliteDatabase::new(
        reader,
        connection.clone(),
    ))
}

pub(crate) async fn test_store_with_workspace(workspace_id: &str) -> CrudStore {
    let fixture = pioneer_postgres_test::scoped_database()
        .await
        .expect("clone the PostgreSQL CRUD fixture database");
    postgres_store_with_workspace(&fixture, workspace_id).await
}

pub(super) async fn postgres_store_with_workspace_and_query_counter(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace_id: &str,
) -> (CrudStore, Arc<AtomicU64>) {
    let mut writer = fixture.connection().clone();
    seed_observation_workspace_on(&writer, workspace_id).await;
    let mut reader = fixture
        .connect_read_only()
        .await
        .expect("read-only PostgreSQL pool");
    let query_count = Arc::new(AtomicU64::new(0));
    for connection in [&mut writer, &mut reader] {
        let query_count = Arc::clone(&query_count);
        connection.set_metric_callback(move |_| {
            query_count.fetch_add(1, Ordering::Relaxed);
        });
    }
    (
        CrudStore::new(pioneer_sqlite::SqliteDatabase::new(reader, writer)),
        query_count,
    )
}

pub(super) async fn seed_observation_workspace(store: &CrudStore, workspace_id: &str) {
    let tx = store
        .connection
        .begin()
        .await
        .expect("begin workspace fixture");
    seed_observation_workspace_on(&tx, workspace_id).await;
    tx.commit().await.expect("commit workspace fixture");
}

pub(super) async fn seed_observation_workspace_on(
    connection: &impl ConnectionTrait,
    workspace_id: &str,
) {
    pioneer_entity::workspace::Entity::update_many()
        .col_expr(
            pioneer_entity::workspace::Column::IsCurrent,
            Expr::value(false),
        )
        .filter(pioneer_entity::workspace::Column::IsCurrent.eq(true))
        .exec(connection)
        .await
        .expect("clear the seeded current workspace");
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set(workspace_id.to_owned()),
        name: Set("Observation fixture".to_owned()),
        is_active: Set(true),
        is_current: Set(true),
        ..Default::default()
    })
    .exec(connection)
    .await
    .expect("observation workspace should exist");
}

pub(super) async fn postgres_store_with_started_turn(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> (CrudStore, Thread, Turn) {
    let store = postgres_store_with_workspace(fixture, workspace_id).await;
    start_test_turn(store, workspace_id, thread_id, turn_id).await
}

pub(super) async fn test_store_with_started_turn(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> (CrudStore, Thread, Turn) {
    let store = test_store_with_workspace(workspace_id).await;
    start_test_turn(store, workspace_id, thread_id, turn_id).await
}

pub(super) async fn start_test_turn(
    store: CrudStore,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> (CrudStore, Thread, Turn) {
    let timestamp = 1_700_000_000;
    let thread = Thread {
        context_window: None,
        service_mode: None,
        context_authority: None,
        workspace_id: workspace_id.to_owned(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        preview_author: None,
        mode: ThreadMode::Agent,
        model: "gpt-5.4".to_owned(),
        model_provider: "openai".to_owned(),
        reasoning_effort: None,
        created_at: timestamp,
        updated_at: timestamp,
        status: ThreadStatus::Active,
        origin_kind: ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: Vec::new(),
    };
    let turn = Turn {
        work_owner: Default::default(),
        id: turn_id.to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        mode: Default::default(),
        author: None,
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
    };

    store
        .upsert_thread_model(&thread, pioneer_protocol::PersistedActorRef::System)
        .await
        .expect("thread should persist");
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .expect("turn should persist");

    (store, thread, turn)
}

pub(super) async fn append_optional_delivery_test_items(
    store: &CrudStore,
    thread: &Thread,
    turn_id: &str,
    count: i64,
) {
    for index in 1..=count {
        store
            .materialize_native_agent_turn_event(
                CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::SystemEvent {
                        id: format!("{turn_id}_item_{index}"),
                        level: SystemEventLevel::Info,
                        message: format!("committed {index}"),
                        code: None,
                        details: None,
                    },
                }),
                1_700_000_100 + index,
                None,
            )
            .await
            .expect("native item should enqueue both optional consumers");
    }
}

pub(super) async fn prepare_progress_finalization(
    store: &CrudStore,
    index: usize,
    at: i64,
) -> String {
    let turn_id = format!("turn_progress_{index}");
    let thread_id = format!("thread_progress_{index}");
    store
        .materialize_turn_start(
            &sample_thread("ws_progress", &thread_id, at),
            SandboxMode::FullAccess,
            &sample_turn(&turn_id),
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    store
        .prepare_turn_finalization(
            &ItemCompletedNotification {
                workspace_id: "ws_progress".into(),
                thread_id,
                turn_id: turn_id.clone(),
                item: TurnItem::AgentMessage {
                    id: format!("final_progress_{index}"),
                    text: "accepted answer".into(),
                    phase: AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
            None,
            at + index as i64,
        )
        .await
        .unwrap();
    turn_id
}

pub(super) fn cleanup_effect_preparation(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    batch_suffix: &str,
) -> pioneer_protocol::NativeTerminalEffectPreparation {
    pioneer_protocol::NativeTerminalEffectPreparation {
        batch_id: format!("{turn_id}:batch:{batch_suffix}"),
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        runtime_generation: 1,
        effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
            effect_id: format!("{turn_id}:terminal-effect:attached-task-cleanup"),
            effect_kind: pioneer_protocol::NativeTerminalEffectKind::AttachedTaskCleanup,
            gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            payload: pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
                reason: "parent turn failed".to_owned(),
                runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
            },
            max_attempts: 3,
        }],
    }
}

pub(super) async fn insert_unread_test_turn(
    store: &CrudStore,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    sort_key: &str,
    send_mode: &str,
    actor_id: Option<&str>,
    deleted: bool,
) {
    pioneer_crud_conversations::test_support::insert_unread_test_turn(
        &store.connection,
        workspace_id,
        thread_id,
        turn_id,
        sort_key,
        send_mode,
        actor_id,
        deleted,
    )
    .await;
}

pub(super) fn sample_thread(workspace_id: &str, thread_id: &str, timestamp: i64) -> Thread {
    Thread {
        context_window: None,
        service_mode: None,
        context_authority: None,
        workspace_id: workspace_id.to_owned(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        preview_author: None,
        mode: ThreadMode::Agent,
        model: "gpt-5.4".to_owned(),
        model_provider: "openai".to_owned(),
        reasoning_effort: None,
        created_at: timestamp,
        updated_at: timestamp,
        status: ThreadStatus::Active,
        origin_kind: ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: Vec::new(),
    }
}

pub(super) fn sample_turn(turn_id: &str) -> Turn {
    Turn {
        work_owner: Default::default(),
        id: turn_id.to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        mode: Default::default(),
        author: None,
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
    }
}

pub(super) async fn mark_first_turn_projection_failed(
    store: &CrudStore,
    turn_id: &str,
    next_run_at_unix: i64,
) {
    let first_event = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
        .one(&store.connection)
        .await
        .expect("first turn event lookup should succeed")
        .expect("first turn event should exist");
    pioneer_entity::turn_event_projection_state::Entity::update_many()
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::Status,
            Expr::value(pioneer_crud_conversations::event_projection::PROJECTION_STATUS_FAILED),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::NextRunAt,
            Expr::value(unix_to_datetime(next_run_at_unix)),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::ClaimToken,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::ClaimExpiresAt,
            Expr::value(Option::<sea_orm::entity::prelude::DateTimeWithTimeZone>::None),
        )
        .filter(pioneer_entity::turn_event_projection_state::Column::EventId.eq(first_event.id))
        .exec(&store.connection)
        .await
        .expect("first turn projection should become replayable");
    set_turn_projection_watermark(store, turn_id, 0).await;
}

pub(super) async fn set_turn_projection_watermark(
    store: &CrudStore,
    turn_id: &str,
    projected_through_sequence: i64,
) {
    let updated = pioneer_entity::turn_event_projection_stream_state::Entity::update_many()
        .col_expr(
            pioneer_entity::turn_event_projection_stream_state::Column::ProjectedThroughSequence,
            Expr::value(projected_through_sequence),
        )
        .filter(
            pioneer_entity::turn_event_projection_stream_state::Column::TurnId
                .eq(turn_id.to_owned()),
        )
        .exec(&store.connection)
        .await
        .expect("projection watermark fixture should update")
        .rows_affected;
    assert_eq!(updated, 1, "projection stream fixture must exist");
}

pub(super) async fn terminal_task_run_occurrence_fixture(
    occurrence_kind: Option<TurnKind>,
) -> (CrudStore, Thread, TaskRun) {
    let store = test_store_with_workspace("ws_task").await;
    terminal_task_run_occurrence_fixture_with_store(store, occurrence_kind).await
}

pub(super) async fn terminal_task_run_occurrence_fixture_with_store(
    store: CrudStore,
    occurrence_kind: Option<TurnKind>,
) -> (CrudStore, Thread, TaskRun) {
    let timestamp = 1_700_000_000;
    let thread = Thread {
        context_window: None,
        service_mode: None,
        context_authority: None,
        workspace_id: "ws_task".to_owned(),
        id: "thr_task".to_owned(),
        name: Some("TaskRun occurrence reconciliation".to_owned()),
        preview: String::new(),
        preview_author: None,
        mode: ThreadMode::Agent,
        model: "gpt-5.4".to_owned(),
        model_provider: "openai".to_owned(),
        reasoning_effort: None,
        created_at: timestamp,
        updated_at: timestamp,
        status: ThreadStatus::Active,
        origin_kind: ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: Vec::new(),
    };
    store
        .upsert_thread_model(&thread, PersistedActorRef::System)
        .await
        .expect("parent thread should persist");

    let task = sample_task(timestamp);
    let mut run = sample_task_run(timestamp);
    run.trigger_id = None;
    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: None,
                },
                TaskEventPayload::RunStarted {
                    task_id: task.id.clone(),
                    run_id: run.id.clone(),
                    started_at: timestamp + 1,
                },
                TaskEventPayload::RunCompleted {
                    task_id: task.id,
                    run_id: run.id.clone(),
                    result: None,
                    completed_at: timestamp + 2,
                },
            ],
            timestamp,
        )
        .await
        .expect("terminal TaskRun should persist");
    let run = store
        .get_task_run(run.id.as_str())
        .await
        .expect("TaskRun should load")
        .expect("TaskRun should exist");
    assert_eq!(run.status, TaskRunStatus::Succeeded);

    if let Some(turn_kind) = occurrence_kind {
        let occurrence = Turn {
            id: run.id.clone(),
            status: TurnStatus::InProgress,
            turn_kind,
            origin: TurnOrigin::ScheduledTask,
            ..sample_turn(run.id.as_str())
        };
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &occurrence,
                &[],
                PersistedActorRef::System,
            )
            .await
            .expect("occurrence Turn should persist");
    }

    (store, thread, run)
}

pub(super) fn safe_web_fetch_item(item_id: &str) -> TurnItem {
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
