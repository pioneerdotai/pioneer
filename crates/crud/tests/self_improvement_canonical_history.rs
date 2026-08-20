use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    CanonicalTurnEventPayload, CanonicalTurnEventRecord, CrudStore,
    SelfImprovementFrozenSourceRange, SelfImprovementSourceTurnRecord,
};
use pioneer_entity::turn_event;
use pioneer_protocol::{
    AgentMessagePhase, ItemCompletedNotification, ItemStartedNotification, PersistedActorRef,
    PrincipalId, SandboxMode, SystemEventLevel, TaskComposerWork, TaskMetadata, Thread, ThreadMode,
    ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, ToolCallStatus, ToolDisplayPayload,
    ToolMetadata, ToolOutputPolicySnapshot, ToolStoragePayload, Turn, TurnCompletedNotification,
    TurnFailedNotification, TurnItem, TurnKind, TurnOrigin, TurnStartParams, TurnStatus, UserInput,
    default_turn_permission_profile_snapshot,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
    QueryFilter, Set,
};

const START_AT: i64 = 1_910_000_000;
const TEST_PRINCIPAL_ID: &str = "P00000000000000000001";

async fn migrated_store() -> (DatabaseConnection, CrudStore) {
    let database = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite must open");
    Migrator::up(&database, None)
        .await
        .expect("migrations must apply");
    database
        .execute_unprepared(
            "INSERT INTO workspace (id, name, is_active, is_current) VALUES \
             ('ws_history_a', 'History A', 1, 1), \
             ('ws_history_b', 'History B', 1, 0)",
        )
        .await
        .expect("workspace fixtures must insert");
    let store = CrudStore::new(database.clone());
    (database, store)
}

#[tokio::test]
async fn frozen_source_is_revalidated_before_canonical_history_rendering() {
    let (database, store) = migrated_store().await;
    let source_thread = thread("ws_history_a", "thread_revalidated_source", START_AT);
    let source_turn = turn("turn_revalidated_source");
    start(
        &store,
        &source_thread,
        &source_turn,
        "private source sentinel",
    )
    .await;
    complete(&store, &source_thread, source_turn, START_AT + 1).await;

    let sources = store
        .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
        .await
        .expect("workspace-visible source must be selected");
    let frozen = frozen_range("ws_history_a", 0, sources);
    database
        .execute_unprepared(
            "UPDATE thread SET access_class = 'private' \
             WHERE id = 'thread_revalidated_source'",
        )
        .await
        .expect("source visibility must change");
    assert!(
        store
            .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
            .await
            .expect("source selection revalidation must succeed")
            .is_empty(),
        "private source must be removed before pagination/history hydration"
    );
    assert_eq!(
        store
            .self_improvement_source_head("ws_history_a")
            .await
            .expect("immutable source ledger head must remain readable"),
        frozen.source_upper_inclusive,
        "visibility filtering must not delete historical provenance"
    );

    let error = store
        .list_canonical_turn_events_for_self_improvement(&frozen)
        .await
        .expect_err("cached source must be rejected after visibility loss");
    assert!(
        error.to_string().contains("no longer workspace-visible"),
        "unexpected source revalidation error: {error:#}"
    );
}

fn thread(workspace_id: &str, thread_id: &str, timestamp: i64) -> Thread {
    thread_with_origin(
        workspace_id,
        thread_id,
        ThreadOriginKind::User,
        ThreadSidebarVisibility::Visible,
        timestamp,
    )
}

fn thread_with_origin(
    workspace_id: &str,
    thread_id: &str,
    origin_kind: ThreadOriginKind,
    sidebar_visibility: ThreadSidebarVisibility,
    timestamp: i64,
) -> Thread {
    Thread {
        workspace_id: workspace_id.to_owned(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        preview_author: None,
        mode: ThreadMode::Agent,
        model: "gpt-test".to_owned(),
        model_provider: "fake".to_owned(),
        reasoning_effort: None,
        created_at: timestamp,
        updated_at: timestamp,
        status: ThreadStatus::Active,
        origin_kind,
        sidebar_visibility,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: Vec::new(),
    }
}

fn turn(turn_id: &str) -> Turn {
    turn_with_identity(turn_id, TurnKind::Conversation, TurnOrigin::User)
}

fn turn_with_identity(turn_id: &str, turn_kind: TurnKind, origin: TurnOrigin) -> Turn {
    Turn {
        id: turn_id.to_owned(),
        status: TurnStatus::InProgress,
        turn_kind,
        origin,
        mode: Default::default(),
        author: None,
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: default_turn_permission_profile_snapshot(),
    }
}

fn tool_item(status: ToolCallStatus, success: Option<bool>) -> TurnItem {
    TurnItem::DynamicToolCall {
        id: "item_child_tool".to_owned(),
        tool_name: "request_tools".to_owned(),
        arguments: serde_json::json!({
            "domains": ["memory"],
            "reason": "Collect a harmless causal tool event."
        }),
        status,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("request_tools"),
        display: ToolDisplayPayload::Hidden,
        storage: ToolStoragePayload::Metadata {
            metadata: ToolMetadata::empty(),
        },
        recovery: None,
        success,
        outcome: None,
        observation: None,
    }
}

async fn start(store: &CrudStore, thread: &Thread, turn: &Turn, text: &str) {
    let actor = match thread.origin_kind {
        ThreadOriginKind::TaskRun | ThreadOriginKind::System => PersistedActorRef::System,
        ThreadOriginKind::Collaborative
        | ThreadOriginKind::DirectMessage
        | ThreadOriginKind::User => PersistedActorRef::Principal(
            PrincipalId::new(TEST_PRINCIPAL_ID).expect("test principal id should be valid"),
        ),
    };
    store
        .materialize_turn_start(
            thread,
            SandboxMode::FullAccess,
            turn,
            &[UserInput::Text {
                text: text.to_owned(),
                text_elements: Vec::new(),
            }],
            actor,
        )
        .await
        .expect("turn start must persist");
    if matches!(
        thread.origin_kind,
        ThreadOriginKind::Collaborative | ThreadOriginKind::DirectMessage | ThreadOriginKind::User
    ) && thread.sidebar_visibility == ThreadSidebarVisibility::Visible
    {
        store
            .database_connection()
            .execute_unprepared(
                format!(
                    "UPDATE thread SET access_class = 'workspace' WHERE id = '{}'",
                    thread.id
                )
                .as_str(),
            )
            .await
            .expect("canonical self-improvement fixture must be workspace-visible");
    }
}

async fn complete(store: &CrudStore, thread: &Thread, mut turn: Turn, timestamp: i64) {
    turn.status = TurnStatus::Completed;
    store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn,
            },
            timestamp,
        )
        .await
        .expect("turn completion must persist");
}

async fn fail(store: &CrudStore, thread: &Thread, mut turn: Turn, timestamp: i64) {
    turn.status = TurnStatus::Failed;
    turn.error = Some("visible failure".to_owned());
    store
        .materialize_turn_failed(
            TurnFailedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn,
            },
            timestamp,
        )
        .await
        .expect("turn failure must persist");
}

async fn seed_failed_collaborative_context(
    database: &DatabaseConnection,
    store: &CrudStore,
    parent_thread: &Thread,
    parent_turn: &Turn,
    child_thread_id: &str,
    child_turn_id: &str,
    delivery_id: &str,
    timestamp: i64,
) {
    start(
        store,
        parent_thread,
        parent_turn,
        "attempt an unavailable verification",
    )
    .await;
    complete(store, parent_thread, parent_turn.clone(), timestamp + 1).await;

    let child_thread = thread_with_origin(
        parent_thread.workspace_id.as_str(),
        child_thread_id,
        ThreadOriginKind::TaskRun,
        ThreadSidebarVisibility::Hidden,
        timestamp + 2,
    );
    let child_turn = turn_with_identity(child_turn_id, TurnKind::Conversation, TurnOrigin::User);
    start(
        store,
        &child_thread,
        &child_turn,
        "attempt an unavailable verification",
    )
    .await;
    fail(store, &child_thread, child_turn, timestamp + 3).await;

    let task_id = format!("task_{delivery_id}");
    let run_id = format!("run_{delivery_id}");
    let task_run_turn_id = format!("trt_{delivery_id}");
    let metadata = TaskMetadata {
        labels: vec!["composer".to_owned()],
        data: None,
        composer_work: Some(TaskComposerWork::v1(TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: parent_thread.id.clone(),
            turn_id: parent_turn.id.clone(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: Some("gpt-test".to_owned()),
            model_provider: Some("fake".to_owned()),
            sandbox_policy: None,
            mode: Some(ThreadMode::Agent),
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        })),
    };
    let metadata_json = serde_json::to_string(&metadata)
        .expect("failed Composer metadata must encode")
        .replace('\'', "''");
    let error_json = serde_json::json!({
        "code": "unavailable",
        "message": "verification service unavailable"
    })
    .to_string()
    .replace('\'', "''");
    database
        .execute_unprepared(&format!(
            r#"
            INSERT INTO task (
                id, workspace_id, owner_kind, created_by_thread_id, created_by_turn_id,
                executor_kind, status, title, goal, metadata_json, completed_at
            ) VALUES (
                '{task_id}', '{workspace_id}', 'thread', '{parent_thread_id}',
                '{parent_turn_id}', 'agent', 'failed', 'Composer',
                'Attempt unavailable verification', '{metadata_json}', CURRENT_TIMESTAMP
            );
            INSERT INTO task_run (
                id, task_id, run_group_id, attempt_number, run_number, status, executor_kind,
                completed_at
            ) VALUES (
                '{run_id}', '{task_id}', '{run_id}', 1, 1, 'failed', 'agent',
                CURRENT_TIMESTAMP
            );
            INSERT INTO task_run_thread_binding (
                id, task_id, run_id, thread_id, binding_kind
            ) VALUES (
                'bind_{delivery_id}', '{task_id}', '{run_id}', '{child_thread_id}',
                'primary_executor'
            );
            INSERT INTO thread_lineage (
                child_thread_id, parent_thread_id, root_thread_id, depth, origin_kind,
                created_by_thread_id, created_by_turn_id
            ) VALUES (
                '{child_thread_id}', '{parent_thread_id}', '{parent_thread_id}', 1,
                'task_run', '{parent_thread_id}', '{parent_turn_id}'
            );
            INSERT INTO task_run_conversation_snapshot (
                run_id, task_id, workspace_id, conversation_thread_id, source_turn_id,
                history_json
            ) VALUES (
                '{run_id}', '{task_id}', '{workspace_id}', '{parent_thread_id}',
                '{parent_turn_id}', '[]'
            );
            INSERT INTO task_run_turn (
                id, task_id, run_id, thread_id, turn_id, kind, round, sequence, status,
                completed_at
            ) VALUES (
                '{task_run_turn_id}', '{task_id}', '{run_id}', '{child_thread_id}',
                '{child_turn_id}', 'initial', 0, 0, 'candidate_created',
                datetime({child_completed_at}, 'unixepoch')
            );
            INSERT INTO task_delivery (
                id, workspace_id, task_id, run_id, delivery_key, mode, thread_target,
                target_thread_id,
                status, error_snapshot_json, attempt_count, max_attempts
            ) VALUES (
                '{delivery_id}', '{workspace_id}', '{task_id}', '{run_id}',
                'delivery_key_{delivery_id}', 'thread', 'origin_thread', '{parent_thread_id}',
                'delivering', '{error_json}', 1, 1
            );
            "#,
            workspace_id = parent_thread.workspace_id,
            parent_thread_id = parent_thread.id,
            parent_turn_id = parent_turn.id,
            child_completed_at = timestamp + 3,
        ))
        .await
        .expect("failed Collaborative chain must seed");

    let delivery_item = TurnItem::SystemEvent {
        id: pioneer_protocol::task_delivery_result_item_id(delivery_id),
        level: SystemEventLevel::Error,
        message: "verification service unavailable".to_owned(),
        code: Some("unavailable".to_owned()),
        details: None,
    };
    store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: parent_thread.workspace_id.clone(),
                thread_id: parent_thread.id.clone(),
                turn_id: parent_turn.id.clone(),
                item: delivery_item.clone(),
            },
            timestamp + 4,
        )
        .await
        .expect("failed delivery start must persist");
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: parent_thread.workspace_id.clone(),
                thread_id: parent_thread.id.clone(),
                turn_id: parent_turn.id.clone(),
                item: delivery_item,
            },
            timestamp + 5,
        )
        .await
        .expect("failed delivery completion must persist");
    database
        .execute_unprepared(&format!(
            "UPDATE task_delivery SET status = 'delivered', \
             delivered_turn_id = '{}', delivered_at = CURRENT_TIMESTAMP \
             WHERE id = '{}'",
            parent_turn.id, delivery_id
        ))
        .await
        .expect("failed delivery completion identity must persist");
}

fn frozen_range(
    workspace_id: &str,
    source_lower_exclusive: i64,
    anchors: Vec<SelfImprovementSourceTurnRecord>,
) -> SelfImprovementFrozenSourceRange {
    SelfImprovementFrozenSourceRange::new(
        workspace_id,
        source_lower_exclusive,
        anchors.last().expect("range requires an anchor").id,
        anchors,
    )
    .expect("frozen source range must validate")
}

fn exhaustive_canonical_variant_event_type(payload: &CanonicalTurnEventPayload) -> &'static str {
    match payload {
        CanonicalTurnEventPayload::TurnStarted(_)
        | CanonicalTurnEventPayload::ItemStarted(_)
        | CanonicalTurnEventPayload::ItemCompleted(_)
        | CanonicalTurnEventPayload::ItemUpdated(_)
        | CanonicalTurnEventPayload::ItemTimeoutDetected(_)
        | CanonicalTurnEventPayload::ItemRecoveryOpened(_)
        | CanonicalTurnEventPayload::ItemRecoveryAttached(_)
        | CanonicalTurnEventPayload::ItemRetryScheduled(_)
        | CanonicalTurnEventPayload::ItemRetryAttemptStarted(_)
        | CanonicalTurnEventPayload::ItemRecoverySucceeded(_)
        | CanonicalTurnEventPayload::ItemRecoveryExhausted(_)
        | CanonicalTurnEventPayload::ItemToolRetryScheduled(_)
        | CanonicalTurnEventPayload::ItemToolRetryResolved(_)
        | CanonicalTurnEventPayload::ItemToolRetryExhausted(_)
        | CanonicalTurnEventPayload::TurnToolLoopBudgetExceeded(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowStarted(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowExhausted(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowCheckpointed(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowContinued(_)
        | CanonicalTurnEventPayload::TurnExecutionWindowBlocked(_)
        | CanonicalTurnEventPayload::TurnPermissionAudit(_)
        | CanonicalTurnEventPayload::TurnMessageEdited(_)
        | CanonicalTurnEventPayload::TurnMessageDeleted(_)
        | CanonicalTurnEventPayload::TurnCompleted(_)
        | CanonicalTurnEventPayload::TurnFailed(_)
        | CanonicalTurnEventPayload::TurnBlocked(_) => payload.event_type(),
    }
}

#[tokio::test]
async fn canonical_history_is_exactly_bounded_ordered_and_workspace_scoped() {
    let (_database, store) = migrated_store().await;
    let thread_a = thread("ws_history_a", "thread_history_a", START_AT);

    let failed = turn("turn_failed_context");
    start(&store, &thread_a, &failed, "context before the anchor").await;
    fail(&store, &thread_a, failed, START_AT + 1).await;

    let anchor = turn("turn_selected_anchor");
    start(&store, &thread_a, &anchor, "selected anchor").await;
    complete(&store, &thread_a, anchor, START_AT + 11).await;

    let logically_later_failed = turn("turn_z_later_failed");
    start(
        &store,
        &thread_a,
        &logically_later_failed,
        "must stay outside despite an earlier terminal timestamp",
    )
    .await;
    fail(&store, &thread_a, logically_later_failed, START_AT + 2).await;

    let later = turn("turn_later_source");
    start(&store, &thread_a, &later, "must stay outside this run").await;
    complete(&store, &thread_a, later, START_AT + 21).await;

    let incomplete = turn("turn_incomplete");
    start(&store, &thread_a, &incomplete, "unfinished tail").await;

    let sources_a = store
        .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
        .await
        .expect("source range must load");
    assert_eq!(sources_a.len(), 2);
    let selected = vec![sources_a[0].clone()];
    let selected_range = frozen_range("ws_history_a", 0, selected.clone());

    let records = store
        .list_canonical_turn_events_for_self_improvement(&selected_range)
        .await
        .expect("canonical history must load");
    let retry_records = store
        .list_canonical_turn_events_for_self_improvement(&selected_range)
        .await
        .expect("canonical history retry must load");
    assert_eq!(retry_records, records);
    assert!(records.iter().all(
        |record| exhaustive_canonical_variant_event_type(&record.payload)
            == record.payload.event_type()
    ));
    let source_actors = records
        .iter()
        .filter_map(|record| record.turn_start_actor())
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        source_actors,
        vec![
            PersistedActorRef::Principal(
                PrincipalId::new(TEST_PRINCIPAL_ID).expect("test principal id should be valid")
            ),
            PersistedActorRef::Principal(
                PrincipalId::new(TEST_PRINCIPAL_ID).expect("test principal id should be valid")
            ),
        ],
        "canonical source history must preserve owning turn actors"
    );
    let ordered_turns = records
        .iter()
        .map(|record| record.turn_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_turns,
        vec![
            "turn_failed_context",
            "turn_failed_context",
            "turn_selected_anchor",
            "turn_selected_anchor",
        ]
    );
    assert!(
        records
            .iter()
            .all(|record| record.turn_id != "turn_later_source"
                && record.turn_id != "turn_z_later_failed"
                && record.turn_id != "turn_incomplete")
    );
    assert!(matches!(
        records.last().map(|record| &record.payload),
        Some(CanonicalTurnEventPayload::TurnCompleted(_))
    ));
    assert_eq!(
        records.last().map(|record| record.event_id.as_str()),
        Some(selected[0].terminal_event_id.as_str())
    );
    let mut forged_boundary = selected_range.clone();
    forged_boundary.thread_terminal_boundaries[0].terminal_event_id =
        sources_a[1].terminal_event_id.clone();
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&forged_boundary)
            .await
            .unwrap_err()
            .to_string()
            .contains("terminal boundaries are invalid")
    );
    for turn_id in ["turn_failed_context", "turn_selected_anchor"] {
        let sequences = records
            .iter()
            .filter(|record| record.turn_id == turn_id)
            .map(|record| record.sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![1, 2]);
    }

    let thread_b = thread("ws_history_b", "thread_history_b", START_AT + 30);
    let anchor_b = turn("turn_anchor_b");
    start(&store, &thread_b, &anchor_b, "workspace B").await;
    complete(&store, &thread_b, anchor_b, START_AT + 31).await;
    let source_b = store
        .list_self_improvement_source_turns_after("ws_history_b", 0, 0, 10)
        .await
        .expect("workspace B source must load");
    let mut forged_workspace_range = frozen_range("ws_history_b", 0, source_b);
    forged_workspace_range.workspace_id = "ws_history_a".to_owned();
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&forged_workspace_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid anchor")
    );
}

#[tokio::test]
async fn canonical_history_derives_legacy_actor_without_rewriting_event_payload() {
    let (database, store) = migrated_store().await;
    let source_thread = thread("ws_history_a", "thread_legacy_actor", START_AT);
    let source_turn = turn("turn_legacy_actor");
    start(&store, &source_thread, &source_turn, "legacy source actor").await;
    complete(&store, &source_thread, source_turn, START_AT + 1).await;

    let start_event = turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq("turn_legacy_actor"))
        .filter(turn_event::Column::EventType.eq("turn/started"))
        .one(&database)
        .await
        .expect("legacy fixture turn/start query must succeed")
        .expect("legacy fixture turn/start must exist");
    let mut legacy_payload =
        serde_json::from_str::<serde_json::Value>(start_event.payload.as_str())
            .expect("turn/start payload must decode");
    assert!(
        legacy_payload
            .get_mut("payload")
            .and_then(serde_json::Value::as_object_mut)
            .expect("turn/start event payload must be an object")
            .remove("actor")
            .is_some(),
        "new fixture must initially carry an actor"
    );
    let legacy_payload =
        serde_json::to_string(&legacy_payload).expect("legacy payload must encode");
    let mut active: turn_event::ActiveModel = start_event.into();
    active.payload = Set(legacy_payload.clone());
    active
        .update(&database)
        .await
        .expect("legacy payload fixture must persist");

    let sources = store
        .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
        .await
        .expect("legacy source range must load");
    let records = store
        .list_canonical_turn_events_for_self_improvement(&frozen_range("ws_history_a", 0, sources))
        .await
        .expect("legacy canonical history must load");
    assert_eq!(
        records
            .iter()
            .find(|record| matches!(&record.payload, CanonicalTurnEventPayload::TurnStarted(_)))
            .and_then(CanonicalTurnEventRecord::turn_start_actor),
        Some(&PersistedActorRef::Principal(
            PrincipalId::new(TEST_PRINCIPAL_ID).expect("test principal id should be valid")
        )),
        "legacy canonical history must derive the owning projection actor"
    );

    let persisted_payload = turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq("turn_legacy_actor"))
        .filter(turn_event::Column::EventType.eq("turn/started"))
        .one(&database)
        .await
        .expect("persisted legacy turn/start query must succeed")
        .expect("persisted legacy turn/start must exist")
        .payload;
    assert_eq!(
        persisted_payload, legacy_payload,
        "canonical history reads must not rewrite append-only legacy payloads"
    );
}

#[tokio::test]
async fn source_ids_bound_same_second_exchanges_without_random_event_id_ordering() {
    let (_database, store) = migrated_store().await;
    let visible_thread = thread("ws_history_a", "thread_same_second", START_AT);

    let failed = turn("turn_same_second_fail");
    start(&store, &visible_thread, &failed, "failed context").await;
    fail(&store, &visible_thread, failed, START_AT + 1).await;

    let first = turn("turn_same_second_one");
    start(&store, &visible_thread, &first, "first source").await;
    complete(&store, &visible_thread, first, START_AT + 1).await;
    let second = turn("turn_same_second_two");
    start(&store, &visible_thread, &second, "second source").await;
    complete(&store, &visible_thread, second, START_AT + 1).await;

    let sources = store
        .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
        .await
        .expect("same-second sources must load");
    assert_eq!(sources.len(), 2);
    let first_only = store
        .list_canonical_turn_events_for_self_improvement(&frozen_range(
            "ws_history_a",
            0,
            vec![sources[0].clone()],
        ))
        .await
        .expect("first same-second range must load");
    assert!(
        first_only
            .iter()
            .any(|event| event.turn_id == "turn_same_second_one")
    );
    assert!(
        first_only
            .iter()
            .all(|event| event.turn_id != "turn_same_second_two")
    );
    assert!(
        first_only
            .iter()
            .all(|event| event.turn_id != "turn_same_second_fail"),
        "ambiguous same-second failed context must be excluded fail-closed"
    );

    let both_range = frozen_range("ws_history_a", 0, sources);
    assert_eq!(
        both_range.thread_terminal_boundaries[0].turn_id, "turn_same_second_two",
        "same-second boundary ordering must use parent turn identity, never random event IDs"
    );
    let both = store
        .list_canonical_turn_events_for_self_improvement(&both_range)
        .await
        .expect("full same-second range must load");
    assert!(
        both.iter()
            .any(|event| event.turn_id == "turn_same_second_one")
    );
    assert!(
        both.iter()
            .any(|event| event.turn_id == "turn_same_second_two")
    );
}

#[tokio::test]
async fn previous_range_source_after_the_current_logical_boundary_is_excluded() {
    let (_database, store) = migrated_store().await;
    let visible_thread = thread("ws_history_a", "thread_reverse_delivery", START_AT);
    let earlier_parent = turn("turn_reverse_a");
    let later_parent = turn("turn_reverse_b");
    start(
        &store,
        &visible_thread,
        &earlier_parent,
        "earlier admission",
    )
    .await;
    start(&store, &visible_thread, &later_parent, "later admission").await;

    complete(
        &store,
        &visible_thread,
        later_parent,
        START_AT.saturating_add(1),
    )
    .await;
    complete(
        &store,
        &visible_thread,
        earlier_parent,
        START_AT.saturating_add(2),
    )
    .await;

    let sources = store
        .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
        .await
        .expect("reverse-delivery sources must load");
    assert_eq!(
        sources
            .iter()
            .map(|source| source.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn_reverse_b", "turn_reverse_a"],
        "source order must reflect durable completion order"
    );

    let current_range = frozen_range("ws_history_a", sources[0].id, vec![sources[1].clone()]);
    assert_eq!(
        current_range.thread_terminal_boundaries[0].turn_id,
        "turn_reverse_a"
    );
    let records = store
        .list_canonical_turn_events_for_self_improvement(&current_range)
        .await
        .expect("current reverse-delivery range must load");
    assert!(
        records
            .iter()
            .any(|event| event.turn_id == "turn_reverse_a")
    );
    assert!(
        records
            .iter()
            .all(|event| event.turn_id != "turn_reverse_b"),
        "a previous-range exchange logically after the selected anchor must stay beyond the prefix"
    );
}

#[tokio::test]
async fn collaborative_canonical_history_contains_only_the_verified_causal_bundle() {
    let (database, store) = migrated_store().await;
    let parent_thread = thread_with_origin(
        "ws_history_a",
        "thread_hist_collab",
        ThreadOriginKind::Collaborative,
        ThreadSidebarVisibility::Visible,
        START_AT,
    );
    let failed_parent_turn = turn("turn_000_hist_collab_failed");
    seed_failed_collaborative_context(
        &database,
        &store,
        &parent_thread,
        &failed_parent_turn,
        "thread_hist_failed_child",
        "turn_hist_failed_child",
        "delivery_hist_failed",
        START_AT,
    )
    .await;
    assert!(
        store
            .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
            .await
            .expect("failed delivery source query must succeed")
            .is_empty(),
        "a failed Collaborative delivery is context only, never an anchor"
    );

    let parent_turn = turn("turn_hist_collab");
    start(
        &store,
        &parent_thread,
        &parent_turn,
        "verify the release checksum",
    )
    .await;
    complete(&store, &parent_thread, parent_turn.clone(), START_AT + 21).await;
    assert!(
        store
            .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
            .await
            .expect("early admission source query must succeed")
            .is_empty(),
        "the Collaborative admission boundary is not a completed exchange"
    );
    let occurrence_turn = turn_with_identity(
        "run_hist_collab",
        TurnKind::TaskRun,
        TurnOrigin::DetachedTask,
    );
    store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &occurrence_turn,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .expect("detached occurrence turn must persist");

    let child_thread = thread_with_origin(
        "ws_history_a",
        "thread_hist_child",
        ThreadOriginKind::TaskRun,
        ThreadSidebarVisibility::Hidden,
        START_AT + 22,
    );
    let child_turn =
        turn_with_identity("turn_hist_child", TurnKind::Conversation, TurnOrigin::User);
    start(
        &store,
        &child_thread,
        &child_turn,
        "verify the release checksum",
    )
    .await;
    store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: child_thread.workspace_id.clone(),
                thread_id: child_thread.id.clone(),
                turn_id: child_turn.id.clone(),
                item: tool_item(ToolCallStatus::InProgress, None),
            },
            START_AT + 23,
        )
        .await
        .expect("child tool start must persist");
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: child_thread.workspace_id.clone(),
                thread_id: child_thread.id.clone(),
                turn_id: child_turn.id.clone(),
                item: tool_item(ToolCallStatus::Completed, Some(true)),
            },
            START_AT + 24,
        )
        .await
        .expect("child tool completion must persist");
    let child_result = TurnItem::AgentMessage {
        id: "item_child_result".to_owned(),
        text: "Checksum verified.".to_owned(),
        phase: AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    };
    store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: child_thread.workspace_id.clone(),
                thread_id: child_thread.id.clone(),
                turn_id: child_turn.id.clone(),
                item: child_result.clone(),
            },
            START_AT + 25,
        )
        .await
        .expect("child result start must persist");
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: child_thread.workspace_id.clone(),
                thread_id: child_thread.id.clone(),
                turn_id: child_turn.id.clone(),
                item: child_result,
            },
            START_AT + 26,
        )
        .await
        .expect("child result completion must persist");
    complete(&store, &child_thread, child_turn.clone(), START_AT + 27).await;

    for (thread_id, turn_id, text) in [
        (
            "thread_hist_sibling",
            "turn_hist_sibling",
            "unrelated sibling",
        ),
        (
            "thread_hist_review",
            "turn_hist_review",
            "internal reviewer",
        ),
    ] {
        let hidden_thread = thread_with_origin(
            "ws_history_a",
            thread_id,
            ThreadOriginKind::TaskRun,
            ThreadSidebarVisibility::Hidden,
            START_AT + 28,
        );
        let hidden_turn = turn_with_identity(turn_id, TurnKind::Conversation, TurnOrigin::User);
        start(&store, &hidden_thread, &hidden_turn, text).await;
        complete(&store, &hidden_thread, hidden_turn, START_AT + 29).await;
    }

    let metadata = TaskMetadata {
        labels: vec!["composer".to_owned()],
        data: None,
        composer_work: Some(TaskComposerWork::v1(TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: parent_thread.id.clone(),
            turn_id: parent_turn.id.clone(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: Some("gpt-test".to_owned()),
            model_provider: Some("fake".to_owned()),
            sandbox_policy: None,
            mode: Some(ThreadMode::Agent),
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        })),
    };
    let metadata_json = serde_json::to_string(&metadata)
        .expect("Composer metadata must encode")
        .replace('\'', "''");
    let result_json = serde_json::json!({
        "summary": "Checksum verified.",
        "artifacts": [],
        "completedByRunId": "run_hist_collab"
    })
    .to_string()
    .replace('\'', "''");
    database
        .execute_unprepared(&format!(
            r#"
            INSERT INTO task (
                id, workspace_id, owner_kind, created_by_thread_id, created_by_turn_id,
                executor_kind, status, title, goal, metadata_json, result_json, completed_at
            ) VALUES (
                'task_hist_collab', 'ws_history_a', 'thread', '{parent_thread_id}',
                '{parent_turn_id}', 'agent', 'completed', 'Composer', 'Verify checksum',
                '{metadata_json}', '{result_json}', CURRENT_TIMESTAMP
            );
            INSERT INTO task_run (
                id, task_id, run_group_id, attempt_number, run_number, status, executor_kind,
                result_json, completed_at
            ) VALUES (
                'run_hist_collab', 'task_hist_collab', 'run_hist_collab', 1, 1,
                'succeeded', 'agent', '{result_json}', CURRENT_TIMESTAMP
            );
            INSERT INTO task_run_thread_binding (
                id, task_id, run_id, thread_id, binding_kind
            ) VALUES (
                'bind_hist_collab', 'task_hist_collab', 'run_hist_collab',
                'thread_hist_child', 'primary_executor'
            );
            INSERT INTO thread_lineage (
                child_thread_id, parent_thread_id, root_thread_id, depth, origin_kind,
                created_by_thread_id, created_by_turn_id
            ) VALUES (
                'thread_hist_child', '{parent_thread_id}', '{parent_thread_id}', 1,
                'task_run', '{parent_thread_id}', 'run_hist_collab'
            );
            INSERT INTO task_run_conversation_snapshot (
                run_id, task_id, workspace_id, conversation_thread_id, source_turn_id,
                history_json
            ) VALUES (
                'run_hist_collab', 'task_hist_collab', 'ws_history_a',
                '{parent_thread_id}', '{parent_turn_id}', '[]'
            );
            INSERT INTO task_run_turn (
                id, task_id, run_id, thread_id, turn_id, kind, round, sequence, status,
                completed_at
            ) VALUES (
                'trt_hist_initial', 'task_hist_collab', 'run_hist_collab',
                'thread_hist_child', 'turn_hist_child', 'initial', 0, 0,
                'candidate_created', datetime({child_completed_at}, 'unixepoch')
            );
            INSERT INTO task_run_turn (
                id, task_id, run_id, thread_id, turn_id, kind, round, sequence, status,
                completed_at
            ) VALUES (
                'trt_hist_review', 'task_hist_collab', 'run_hist_collab',
                'thread_hist_review', 'turn_hist_review', 'review', 0, 1,
                'candidate_created', datetime({review_completed_at}, 'unixepoch')
            );
            INSERT INTO task_result_candidate (
                id, task_id, run_id, task_run_turn_id, thread_id, turn_id, round, status,
                result_json, diagnostics_json, resolved_at
            ) VALUES (
                'candidate_hist', 'task_hist_collab', 'run_hist_collab',
                'trt_hist_initial', 'thread_hist_child', 'turn_hist_child', 0,
                'accepted', '{result_json}', '[]', CURRENT_TIMESTAMP
            );
            INSERT INTO task_delivery (
                id, workspace_id, task_id, run_id, delivery_key, mode, thread_target,
                target_thread_id,
                status, result_snapshot_json, attempt_count, max_attempts
            ) VALUES (
                'delivery_hist_ok', 'ws_history_a', 'task_hist_collab', 'run_hist_collab',
                'delivery_key_hist', 'thread', 'origin_thread', '{parent_thread_id}', 'delivering',
                '{result_json}', 1, 1
            );
            "#,
            parent_thread_id = parent_thread.id,
            parent_turn_id = parent_turn.id,
            child_completed_at = START_AT + 27,
            review_completed_at = START_AT + 29,
        ))
        .await
        .expect("verified Collaborative chain must seed");

    let delivery_item = TurnItem::AgentMessage {
        id: pioneer_protocol::task_delivery_result_item_id("delivery_hist_ok"),
        text: "Checksum verified.".to_owned(),
        phase: AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    };
    store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: parent_thread.workspace_id.clone(),
                thread_id: parent_thread.id.clone(),
                turn_id: occurrence_turn.id.clone(),
                item: delivery_item.clone(),
            },
            START_AT + 30,
        )
        .await
        .expect("delivery start must persist");
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: parent_thread.workspace_id.clone(),
                thread_id: parent_thread.id.clone(),
                turn_id: occurrence_turn.id.clone(),
                item: delivery_item,
            },
            START_AT + 31,
        )
        .await
        .expect("delivery completion must persist");
    database
        .execute_unprepared(
            "UPDATE task_delivery SET status = 'delivered', \
             delivered_turn_id = 'run_hist_collab', delivered_at = CURRENT_TIMESTAMP \
             WHERE id = 'delivery_hist_ok'",
        )
        .await
        .expect("successful delivery completion identity must persist");

    let sources = store
        .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
        .await
        .expect("Collaborative source must load");
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0].task_delivery_id.as_deref(),
        Some("delivery_hist_ok")
    );
    let records = store
        .list_canonical_turn_events_for_self_improvement(&frozen_range(
            "ws_history_a",
            0,
            sources.clone(),
        ))
        .await
        .expect("Collaborative causal history must load");
    let turn_ids = records
        .iter()
        .map(|record| record.turn_id.as_str())
        .collect::<Vec<_>>();
    assert!(turn_ids.contains(&"turn_hist_collab"));
    assert!(turn_ids.contains(&"run_hist_collab"));
    assert!(turn_ids.contains(&"turn_hist_child"));
    assert!(turn_ids.contains(&"turn_000_hist_collab_failed"));
    assert!(turn_ids.contains(&"turn_hist_failed_child"));
    assert!(!turn_ids.contains(&"turn_hist_sibling"));
    assert!(!turn_ids.contains(&"turn_hist_review"));
    assert!(records.iter().any(|record| {
        matches!(
            &record.payload,
            CanonicalTurnEventPayload::ItemCompleted(notification)
                if notification.item.item_id() == "item_child_tool"
        )
    }));
    assert!(records.iter().any(|record| {
        matches!(
            &record.payload,
            CanonicalTurnEventPayload::ItemCompleted(notification)
                if notification.item.item_id()
                    == pioneer_protocol::task_delivery_result_item_id("delivery_hist_failed")
        )
    }));
    assert!(records.iter().any(|record| {
        record.turn_id == "turn_hist_failed_child"
            && matches!(
                &record.payload,
                CanonicalTurnEventPayload::TurnFailed(notification)
                    if notification.turn.status == TurnStatus::Failed
            )
    }));

    let parent_admission_end = records
        .iter()
        .position(|record| {
            record.turn_id == "turn_hist_collab"
                && matches!(&record.payload, CanonicalTurnEventPayload::TurnCompleted(_))
        })
        .expect("parent admission completion must be present");
    let child_start = records
        .iter()
        .position(|record| record.turn_id == "turn_hist_child")
        .expect("exact child must be present");
    let delivery_start = records
        .iter()
        .position(|record| {
            record.turn_id == "run_hist_collab"
                && matches!(
                    &record.payload,
                    CanonicalTurnEventPayload::ItemStarted(_)
                        | CanonicalTurnEventPayload::ItemCompleted(_)
                )
        })
        .expect("delivery events must be present");
    assert!(parent_admission_end < child_start && child_start < delivery_start);
    assert_eq!(
        records.last().map(|record| record.event_id.as_str()),
        Some(sources[0].terminal_event_id.as_str()),
        "the exact origin-thread delivery ItemCompleted is the frozen boundary"
    );

    let forged_revision_thread = thread_with_origin(
        "ws_history_a",
        "thread_hist_forged_revision",
        ThreadOriginKind::TaskRun,
        ThreadSidebarVisibility::Visible,
        START_AT + 28,
    );
    let forged_revision_turn = turn("turn_hist_forged_revision");
    start(
        &store,
        &forged_revision_thread,
        &forged_revision_turn,
        "forged revision",
    )
    .await;
    complete(
        &store,
        &forged_revision_thread,
        forged_revision_turn,
        START_AT + 29,
    )
    .await;
    database
        .execute_unprepared(&format!(
            r#"
            UPDATE task_run_turn SET sequence = 3 WHERE id = 'trt_hist_initial';
            INSERT INTO thread_lineage (
                child_thread_id, parent_thread_id, root_thread_id, depth, origin_kind,
                created_by_thread_id, created_by_turn_id
            ) VALUES (
                'thread_hist_forged_revision', 'thread_hist_collab', 'thread_hist_collab', 1,
                'task_run', 'thread_hist_collab', 'run_hist_collab'
            );
            INSERT INTO task_run_turn (
                id, task_id, run_id, thread_id, turn_id, kind, round, sequence, status,
                completed_at
            ) VALUES (
                'trt_hist_forged_revision', 'task_hist_collab', 'run_hist_collab',
                'thread_hist_forged_revision', 'turn_hist_forged_revision', 'revision', 1, 2,
                'candidate_created', datetime({}, 'unixepoch')
            );
            "#,
            START_AT + 29
        ))
        .await
        .expect("forged Collaborative revision must seed");
    let selected_range = frozen_range("ws_history_a", 0, sources);
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&selected_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("is not a hidden TaskRun thread"),
        "a causally linked but visible revision must fail closed"
    );

    database
        .execute_unprepared(
            "UPDATE thread SET sidebar_visibility = 'hidden' \
             WHERE id = 'thread_hist_forged_revision'; \
             UPDATE turn SET origin = 'detached_task' WHERE id = 'turn_hist_forged_revision';",
        )
        .await
        .expect("forged Collaborative revision identity must update");
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&selected_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("is not a terminal Conversation/User execution turn"),
        "a hidden revision with a non-production turn identity must fail closed"
    );

    database
        .execute_unprepared(&format!(
            "UPDATE turn SET origin = 'user' WHERE id = 'turn_hist_forged_revision'; \
             UPDATE task_run_turn SET completed_at = datetime({}, 'unixepoch') \
             WHERE id = 'trt_hist_forged_revision';",
            START_AT + 32
        ))
        .await
        .expect("late Collaborative revision metadata must update");
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&selected_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("did not complete before its origin delivery"),
        "a revision recorded after the frozen origin delivery must fail closed"
    );

    database
        .execute_unprepared(&format!(
            "UPDATE task_run_turn SET completed_at = datetime({}, 'unixepoch') \
             WHERE id = 'trt_hist_forged_revision'; \
             UPDATE turn_event SET created_at = datetime({}, 'unixepoch') \
             WHERE turn_id = 'turn_hist_forged_revision' AND event_type = 'turn/completed';",
            START_AT + 29,
            START_AT + 32
        ))
        .await
        .expect("late Collaborative revision terminal event must update");
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&selected_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("terminated after its origin delivery"),
        "late child terminal content must not enter an earlier frozen origin delivery"
    );

    database
        .execute_unprepared(&format!(
            "UPDATE turn_event SET created_at = datetime({}, 'unixepoch') \
             WHERE turn_id = 'turn_hist_forged_revision' AND event_type = 'turn/completed'; \
             UPDATE task_run_turn SET sequence = 0 WHERE id = 'trt_hist_initial';",
            START_AT + 29
        ))
        .await
        .expect("post-accept Collaborative revision must update");
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&selected_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("occurs after the accepted execution turn"),
        "a revision after the accepted result must not enter its origin delivery"
    );
}

#[tokio::test]
async fn canonical_history_rejects_forged_or_corrupt_source_identity() {
    let (database, store) = migrated_store().await;
    let thread = thread("ws_history_a", "thread_identity", START_AT);
    let anchor = turn("turn_identity");
    start(&store, &thread, &anchor, "identity").await;
    complete(&store, &thread, anchor, START_AT + 1).await;
    let mut source = store
        .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
        .await
        .expect("source must load");
    source[0].terminal_event_id = "forged-terminal-event".to_owned();
    let forged_range = frozen_range("ws_history_a", 0, source);
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&forged_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("does not match its persisted identity")
    );

    let persisted = store
        .list_self_improvement_source_turns_after("ws_history_a", 0, 0, 10)
        .await
        .expect("source must reload");
    let persisted_range = frozen_range("ws_history_a", 0, persisted.clone());
    database
        .execute_unprepared(
            "UPDATE turn_event SET payload = replace(payload, 'ws_history_a', 'ws_history_b') \
             WHERE turn_id = 'turn_identity' AND sequence = 1",
        )
        .await
        .expect("cross-workspace payload fixture must apply");
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&persisted_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("payload identity mismatch")
    );
    database
        .execute_unprepared(
            "UPDATE turn_event SET payload = replace(payload, 'ws_history_b', 'ws_history_a') \
             WHERE turn_id = 'turn_identity' AND sequence = 1",
        )
        .await
        .expect("cross-workspace payload fixture must restore");
    database
        .execute_unprepared(&format!(
            "UPDATE turn_event SET event_type = 'internal/forged' WHERE id = '{}'",
            persisted[0].terminal_event_id
        ))
        .await
        .expect("fixture corruption must apply");
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&persisted_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("payload type mismatch")
    );

    database
        .execute_unprepared(&format!(
            "UPDATE turn_event SET event_type = 'turn/completed', payload = '{{malformed' \
             WHERE id = '{}'",
            persisted[0].terminal_event_id
        ))
        .await
        .expect("malformed payload fixture must apply");
    assert!(
        store
            .list_canonical_turn_events_for_self_improvement(&persisted_range)
            .await
            .unwrap_err()
            .to_string()
            .contains("failed to decode canonical turn event")
    );
}
