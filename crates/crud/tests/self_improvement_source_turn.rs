use migration::{MigrationTrait, Migrator, MigratorTrait};
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    AgentMessagePhase, ItemCompletedNotification, ItemStartedNotification, SandboxMode,
    SystemEventLevel, TaskComposerWork, TaskMetadata, Thread, ThreadMode, ThreadOriginKind,
    ThreadSidebarVisibility, ThreadStatus, Turn, TurnCompletedNotification, TurnFailedNotification,
    TurnItem, TurnKind, TurnOrigin, TurnStartParams, TurnStatus,
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};

const START_AT: i64 = 1_900_000_000;

struct BeforeSelfImprovementMigrator;

impl MigratorTrait for BeforeSelfImprovementMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        Migrator::migrations()
            .into_iter()
            .filter(|migration| migration.name() != "m20260723_000001_self_improvement_core")
            .collect()
    }
}

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
             ('ws_source_a', 'Source A', 1, 1), \
             ('ws_source_b', 'Source B', 1, 0)",
        )
        .await
        .expect("workspace fixtures must insert");
    let store = CrudStore::new(database.clone());
    (database, store)
}

fn thread(
    workspace_id: &str,
    thread_id: &str,
    origin_kind: ThreadOriginKind,
    timestamp: i64,
) -> Thread {
    Thread {
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
        origin_kind,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: Vec::new(),
    }
}

fn turn(turn_id: &str, turn_kind: TurnKind, origin: TurnOrigin) -> Turn {
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
        permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
    }
}

async fn start_turn(store: &CrudStore, thread: &Thread, turn: &Turn) {
    store
        .materialize_turn_start(
            thread,
            SandboxMode::FullAccess,
            turn,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .expect("turn start must project");
    if matches!(
        thread.origin_kind,
        ThreadOriginKind::Collaborative | ThreadOriginKind::DirectMessage | ThreadOriginKind::User
    ) && thread.sidebar_visibility == ThreadSidebarVisibility::Visible
    {
        store
            .database_connection()
            .execute_unprepared(
                format!(
                    "UPDATE thread SET access_class = 'private' WHERE id = '{}'",
                    thread.id
                )
                .as_str(),
            )
            .await
            .expect("private conversation must participate in workspace learning");
    }
}

async fn complete_turn(store: &CrudStore, thread: &Thread, mut turn: Turn, terminal_at: i64) {
    turn.status = TurnStatus::Completed;
    store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn,
            },
            terminal_at,
        )
        .await
        .expect("turn completion must project");
}

async fn seed_successful_collaborative_delivery(
    database: &DatabaseConnection,
    store: &CrudStore,
    parent_thread: &Thread,
    parent_turn: &Turn,
    child_thread_id: &str,
    child_turn_id: &str,
    delivery_id: &str,
    lineage_parent_turn_id: &str,
    timestamp: i64,
) -> TurnItem {
    let mut child_thread = thread(
        parent_thread.workspace_id.as_str(),
        child_thread_id,
        ThreadOriginKind::TaskRun,
        timestamp,
    );
    child_thread.sidebar_visibility = ThreadSidebarVisibility::Hidden;
    let child_turn = turn(child_turn_id, TurnKind::Conversation, TurnOrigin::User);
    start_turn(store, &child_thread, &child_turn).await;
    complete_turn(store, &child_thread, child_turn, timestamp + 1).await;

    let task_id = format!("task_{delivery_id}");
    let run_id = format!("run_{delivery_id}");
    let task_run_turn_id = format!("trt_{child_turn_id}");
    let metadata = TaskMetadata {
        labels: vec!["composer".to_owned()],
        data: None,
        composer_work: Some(TaskComposerWork::v1(TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: parent_thread.id.clone(),
            turn_id: parent_turn.id.clone(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: Some("gpt-5.4".to_owned()),
            model_provider: Some("openai".to_owned()),
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
        "summary": "Collaborative task completed.",
        "artifacts": [],
        "completedByRunId": run_id,
    })
    .to_string()
    .replace('\'', "''");

    database
        .execute_unprepared(&format!(
            r#"
            INSERT INTO task (
                id, workspace_id, owner_kind, created_by_thread_id, created_by_turn_id,
                executor_kind, status, title, goal, metadata_json, result_json,
                completed_at
            ) VALUES (
                '{task_id}', '{workspace_id}', 'thread', '{parent_thread_id}',
                '{parent_turn_id}', 'agent', 'completed', 'Composer', 'Complete request',
                '{metadata_json}', '{result_json}', CURRENT_TIMESTAMP
            );
            INSERT INTO task_run (
                id, task_id, run_group_id, attempt_number, run_number, status, executor_kind,
                result_json, completed_at
            ) VALUES (
                '{run_id}', '{task_id}', '{run_id}', 1, 1, 'succeeded', 'agent',
                '{result_json}', CURRENT_TIMESTAMP
            );
            INSERT INTO task_run_thread_binding (
                id, task_id, run_id, thread_id, binding_kind
            ) VALUES (
                'binding_{delivery_id}', '{task_id}', '{run_id}', '{child_thread_id}',
                'primary_executor'
            );
            INSERT INTO thread_lineage (
                child_thread_id, parent_thread_id, root_thread_id, depth, origin_kind,
                created_by_thread_id, created_by_turn_id
            ) VALUES (
                '{child_thread_id}', '{parent_thread_id}', '{parent_thread_id}', 1, 'task_run',
                '{parent_thread_id}', '{lineage_parent_turn_id}'
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
                '{child_turn_id}', 'initial', 0, 0, 'candidate_created', CURRENT_TIMESTAMP
            );
            INSERT INTO task_result_candidate (
                id, task_id, run_id, task_run_turn_id, thread_id, turn_id, round, status,
                result_json, diagnostics_json, resolved_at
            ) VALUES (
                'candidate_{delivery_id}', '{task_id}', '{run_id}', '{task_run_turn_id}',
                '{child_thread_id}', '{child_turn_id}', 0, 'accepted', '{result_json}', '[]',
                CURRENT_TIMESTAMP
            );
            INSERT INTO task_delivery (
                id, workspace_id, task_id, run_id, delivery_key, mode, thread_target,
                target_thread_id,
                status, result_snapshot_json, attempt_count, max_attempts
            ) VALUES (
                '{delivery_id}', '{workspace_id}', '{task_id}', '{run_id}',
                'delivery_key_{delivery_id}', 'thread', 'origin_thread', '{parent_thread_id}',
                'delivering', '{result_json}', 1, 1
            );
            "#,
            workspace_id = parent_thread.workspace_id,
            parent_thread_id = parent_thread.id,
            parent_turn_id = parent_turn.id,
        ))
        .await
        .expect("collaborative delivery identity chain must seed");

    TurnItem::AgentMessage {
        id: pioneer_protocol::task_delivery_result_item_id(delivery_id),
        text: "Collaborative task completed.".to_owned(),
        phase: AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    }
}

async fn materialize_delivery_item(
    store: &CrudStore,
    parent_thread: &Thread,
    parent_turn: &Turn,
    item: TurnItem,
    timestamp: i64,
) {
    store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: parent_thread.workspace_id.clone(),
                thread_id: parent_thread.id.clone(),
                turn_id: parent_turn.id.clone(),
                item: item.clone(),
            },
            timestamp,
        )
        .await
        .expect("delivery item start must project");
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: parent_thread.workspace_id.clone(),
                thread_id: parent_thread.id.clone(),
                turn_id: parent_turn.id.clone(),
                item,
            },
            timestamp + 1,
        )
        .await
        .expect("delivery item completion must project");
}

async fn scalar_i64(database: &DatabaseConnection, sql: impl Into<String>) -> i64 {
    database
        .query_one_raw(Statement::from_string(DatabaseBackend::Sqlite, sql.into()))
        .await
        .expect("query must execute")
        .expect("query must return one row")
        .try_get("", "value")
        .expect("result must decode")
}

async fn scalar_string(database: &DatabaseConnection, sql: impl Into<String>) -> String {
    database
        .query_one_raw(Statement::from_string(DatabaseBackend::Sqlite, sql.into()))
        .await
        .expect("query must execute")
        .expect("query must return one row")
        .try_get("", "value")
        .expect("result must decode")
}

#[tokio::test]
async fn source_projection_is_exact_idempotent_isolated_and_monotonic() {
    let (database, store) = migrated_store().await;

    let eligible_thread = thread(
        "ws_source_a",
        "thread_eligible_a",
        ThreadOriginKind::User,
        START_AT,
    );
    let eligible_turn = turn("turn_eligible_a1", TurnKind::Conversation, TurnOrigin::User);
    start_turn(&store, &eligible_thread, &eligible_turn).await;
    database
        .execute_unprepared(
            "UPDATE thread SET access_class = 'workspace' WHERE id = 'thread_eligible_a'",
        )
        .await
        .expect("shared conversation must participate alongside private conversations");
    complete_turn(&store, &eligible_thread, eligible_turn, START_AT + 1).await;

    let mut private_thread = thread(
        "ws_source_a",
        "thread_private_source",
        ThreadOriginKind::User,
        START_AT + 2,
    );
    private_thread.model_provider = "cli_runtime:codex".to_owned();
    let private_turn = turn(
        "turn_private_source",
        TurnKind::Conversation,
        TurnOrigin::User,
    );
    start_turn(&store, &private_thread, &private_turn).await;
    database
        .execute_unprepared(
            "UPDATE thread SET access_class = 'private' \
             WHERE id = 'thread_private_source'",
        )
        .await
        .expect("private source fixture must become private before completion");
    complete_turn(&store, &private_thread, private_turn, START_AT + 3).await;
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_source_turn \
             WHERE turn_id = 'turn_private_source'",
        )
        .await,
        1,
        "private conversations must enter the workspace learning source ledger"
    );

    let first = store
        .list_self_improvement_source_turns_after("ws_source_a", 0, 0, 10)
        .await
        .expect("source range must load");
    assert_eq!(first.len(), 2);
    assert_eq!(first[1].turn_id, "turn_private_source");
    assert_eq!(first[0].turn_id, "turn_eligible_a1");
    assert_eq!(first[0].task_delivery_id, None);
    assert_eq!(first[0].terminal_at_unix, START_AT + 1);

    database
        .execute_unprepared(&format!(
            "UPDATE turn_event_projection_state \
             SET status = 'pending', next_run_at = '1970-01-01 00:00:00+00:00', \
                 claim_token = NULL, claim_expires_at = NULL, projected_at = NULL \
             WHERE event_id = '{}'",
            first[0].terminal_event_id
        ))
        .await
        .expect("completion projection must be reset for replay");
    store
        .replay_due_turn_event_projections(2_000_000_000, 10)
        .await
        .expect("terminal event replay must succeed");
    assert_eq!(
        store
            .list_self_improvement_source_turns_after("ws_source_a", 0, 0, 10)
            .await
            .expect("source range after replay")
            .len(),
        2,
        "replay must not duplicate the source identity"
    );

    let ineligible = [
        (
            "thread_collaborative_early",
            "turn_collaborative_early",
            ThreadOriginKind::Collaborative,
            TurnKind::Conversation,
            TurnOrigin::User,
        ),
        (
            "thread_task_origin",
            "turn_task_origin",
            ThreadOriginKind::TaskRun,
            TurnKind::Conversation,
            TurnOrigin::User,
        ),
        (
            "thread_user_taskturn",
            "turn_user_taskturn",
            ThreadOriginKind::User,
            TurnKind::TaskRun,
            TurnOrigin::User,
        ),
        (
            "thread_user_sched",
            "turn_user_sched",
            ThreadOriginKind::User,
            TurnKind::Conversation,
            TurnOrigin::ScheduledTask,
        ),
        (
            "thread_system",
            "turn_system",
            ThreadOriginKind::System,
            TurnKind::Conversation,
            TurnOrigin::User,
        ),
    ];
    for (index, (thread_id, turn_id, thread_origin, turn_kind, turn_origin)) in
        ineligible.into_iter().enumerate()
    {
        let thread = thread(
            "ws_source_a",
            thread_id,
            thread_origin,
            START_AT + 10 + index as i64,
        );
        let turn = turn(turn_id, turn_kind, turn_origin);
        start_turn(&store, &thread, &turn).await;
        complete_turn(&store, &thread, turn, START_AT + 20 + index as i64).await;
    }

    let failed_thread = thread(
        "ws_source_a",
        "thread_failed",
        ThreadOriginKind::User,
        START_AT + 30,
    );
    let failed_turn = turn("turn_failed", TurnKind::Conversation, TurnOrigin::User);
    start_turn(&store, &failed_thread, &failed_turn).await;
    store
        .materialize_turn_failed(
            TurnFailedNotification {
                workspace_id: "ws_source_a".to_owned(),
                thread_id: failed_thread.id.clone(),
                turn: Turn {
                    status: TurnStatus::Failed,
                    error: Some("expected test failure".to_owned()),
                    ..failed_turn
                },
            },
            START_AT + 31,
        )
        .await
        .expect("failed turn must project without becoming evidence");

    let incomplete_thread = thread(
        "ws_source_a",
        "thread_incomplete",
        ThreadOriginKind::User,
        START_AT + 40,
    );
    let incomplete_turn = turn("turn_incomplete", TurnKind::Conversation, TurnOrigin::User);
    start_turn(&store, &incomplete_thread, &incomplete_turn).await;

    assert_eq!(
        store
            .self_improvement_source_head("ws_source_a")
            .await
            .expect("workspace A head"),
        first[1].id,
        "ineligible, failed, and incomplete turns must not advance the ledger"
    );

    let workspace_b_thread = thread(
        "ws_source_b",
        "thread_eligible_b",
        ThreadOriginKind::User,
        START_AT + 50,
    );
    let workspace_b_turn = turn("turn_eligible_b", TurnKind::Conversation, TurnOrigin::User);
    start_turn(&store, &workspace_b_thread, &workspace_b_turn).await;
    complete_turn(&store, &workspace_b_thread, workspace_b_turn, START_AT + 51).await;

    let second_turn = turn("turn_eligible_a2", TurnKind::Conversation, TurnOrigin::User);
    let second_thread = thread(
        "ws_source_a",
        "thread_eligible_a2",
        ThreadOriginKind::DirectMessage,
        START_AT + 60,
    );
    start_turn(&store, &second_thread, &second_turn).await;
    complete_turn(&store, &second_thread, second_turn, START_AT + 61).await;

    let all_a = store
        .list_self_improvement_source_turns_after("ws_source_a", 0, 0, 10)
        .await
        .expect("workspace A source range");
    assert_eq!(all_a.len(), 3);
    assert!(all_a[0].id < all_a[1].id);
    assert!(all_a.iter().all(|row| row.workspace_id == "ws_source_a"));
    assert_eq!(
        store
            .list_self_improvement_source_turns_after("ws_source_a", 0, 0, 1)
            .await
            .expect("bounded source range"),
        vec![all_a[0].clone()]
    );
    assert_eq!(
        store
            .list_self_improvement_source_turns_after("ws_source_a", all_a[0].id, 0, 10)
            .await
            .expect("range after cursor"),
        vec![all_a[1].clone(), all_a[2].clone()]
    );
    assert!(
        store
            .list_self_improvement_source_turns_after(
                "ws_source_a",
                0,
                all_a[2].terminal_at_unix + 1,
                10,
            )
            .await
            .expect("effective enable boundary")
            .is_empty()
    );
    assert_eq!(
        store
            .list_self_improvement_source_turns_after("ws_source_b", 0, 0, 10)
            .await
            .expect("workspace B source range")
            .len(),
        1
    );
}

#[tokio::test]
async fn collaborative_source_requires_verified_terminal_delivery_and_is_replay_idempotent() {
    let (database, store) = migrated_store().await;
    let parent_thread = thread(
        "ws_source_a",
        "thread_collab_parent",
        ThreadOriginKind::Collaborative,
        START_AT,
    );
    let parent_turn = turn(
        "turn_collab_parent",
        TurnKind::Conversation,
        TurnOrigin::User,
    );
    start_turn(&store, &parent_thread, &parent_turn).await;
    complete_turn(&store, &parent_thread, parent_turn.clone(), START_AT + 1).await;
    assert_eq!(
        store
            .self_improvement_source_head("ws_source_a")
            .await
            .expect("source head after admission"),
        0,
        "early Collaborative completion must not become evidence"
    );

    let delivery_id = "delivery_collab_ok";
    let occurrence_turn = turn(
        "run_delivery_collab_ok",
        TurnKind::TaskRun,
        TurnOrigin::DetachedTask,
    );
    start_turn(&store, &parent_thread, &occurrence_turn).await;
    let item = seed_successful_collaborative_delivery(
        &database,
        &store,
        &parent_thread,
        &parent_turn,
        "thread_collab_child",
        "turn_collab_child",
        delivery_id,
        occurrence_turn.id.as_str(),
        START_AT + 2,
    )
    .await;
    materialize_delivery_item(
        &store,
        &parent_thread,
        &occurrence_turn,
        item.clone(),
        START_AT + 4,
    )
    .await;

    let first = store
        .list_self_improvement_source_turns_after("ws_source_a", 0, 0, 10)
        .await
        .expect("collaborative source must load");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].thread_id, parent_thread.id);
    assert_eq!(first[0].turn_id, parent_turn.id);
    assert_eq!(first[0].task_delivery_id.as_deref(), Some(delivery_id));
    assert_eq!(
        scalar_string(
            &database,
            format!(
                "SELECT turn_id AS value FROM turn_event WHERE id = '{}'",
                first[0].terminal_event_id
            )
        )
        .await,
        occurrence_turn.id,
        "the logical source parent and physical owner-delivery occurrence must remain distinct"
    );
    let first_boundary = first[0].terminal_event_id.clone();

    materialize_delivery_item(&store, &parent_thread, &occurrence_turn, item, START_AT + 6).await;
    let replayed = store
        .list_self_improvement_source_turns_after("ws_source_a", 0, 0, 10)
        .await
        .expect("replayed collaborative source must load");
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].terminal_event_id, first_boundary);

    let forged_thread = thread(
        "ws_source_a",
        "thread_collab_forged",
        ThreadOriginKind::Collaborative,
        START_AT + 10,
    );
    let forged_turn = turn(
        "turn_collab_forged",
        TurnKind::Conversation,
        TurnOrigin::User,
    );
    start_turn(&store, &forged_thread, &forged_turn).await;
    complete_turn(&store, &forged_thread, forged_turn.clone(), START_AT + 11).await;
    materialize_delivery_item(
        &store,
        &forged_thread,
        &forged_turn,
        TurnItem::AgentMessage {
            id: pioneer_protocol::task_delivery_result_item_id("missing_delivery"),
            text: "Forged result".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        },
        START_AT + 12,
    )
    .await;
    assert_eq!(
        store
            .list_self_improvement_source_turns_after("ws_source_a", 0, 0, 10)
            .await
            .expect("source range after forged delivery")
            .len(),
        1,
        "an item ID without the durable task chain must not become evidence"
    );
}

#[tokio::test]
async fn collaborative_source_rejects_mismatched_child_lineage() {
    let (database, store) = migrated_store().await;
    let parent_thread = thread(
        "ws_source_a",
        "thread_bad_lineage",
        ThreadOriginKind::Collaborative,
        START_AT,
    );
    let parent_turn = turn("turn_bad_lineage", TurnKind::Conversation, TurnOrigin::User);
    start_turn(&store, &parent_thread, &parent_turn).await;
    complete_turn(&store, &parent_thread, parent_turn.clone(), START_AT + 1).await;
    let item = seed_successful_collaborative_delivery(
        &database,
        &store,
        &parent_thread,
        &parent_turn,
        "thread_bad_child",
        "turn_bad_child",
        "delivery_bad_lineage",
        "different_parent_turn",
        START_AT + 2,
    )
    .await;
    materialize_delivery_item(&store, &parent_thread, &parent_turn, item, START_AT + 4).await;
    assert_eq!(
        store
            .self_improvement_source_head("ws_source_a")
            .await
            .expect("source head after mismatched lineage"),
        0
    );
}

#[tokio::test]
async fn collaborative_source_requires_one_exact_success_result_snapshot() {
    let (database, store) = migrated_store().await;
    let parent_thread = thread(
        "ws_source_a",
        "thread_result_mismatch",
        ThreadOriginKind::Collaborative,
        START_AT,
    );
    let parent_turn = turn(
        "turn_result_mismatch",
        TurnKind::Conversation,
        TurnOrigin::User,
    );
    start_turn(&store, &parent_thread, &parent_turn).await;
    complete_turn(&store, &parent_thread, parent_turn.clone(), START_AT + 1).await;
    let delivery_id = "delivery_result_mismatch";
    let item = seed_successful_collaborative_delivery(
        &database,
        &store,
        &parent_thread,
        &parent_turn,
        "thread_result_mismatch_child",
        "turn_result_mismatch_child",
        delivery_id,
        parent_turn.id.as_str(),
        START_AT + 2,
    )
    .await;
    database
        .execute_unprepared(&format!(
            "UPDATE task_delivery \
             SET result_snapshot_json = '{{\"summary\":\"different result\",\"artifacts\":[]}}' \
             WHERE id = '{delivery_id}'"
        ))
        .await
        .expect("mismatched delivery result fixture must update");

    materialize_delivery_item(&store, &parent_thread, &parent_turn, item, START_AT + 4).await;
    assert_eq!(
        store
            .self_improvement_source_head("ws_source_a")
            .await
            .expect("source head after mismatched result"),
        0,
        "a display item whose durable success snapshots disagree must not become evidence"
    );
}

#[tokio::test]
async fn failed_collaborative_delivery_is_not_a_source_anchor() {
    let (database, store) = migrated_store().await;
    let parent_thread = thread(
        "ws_source_a",
        "thread_failed_delivery",
        ThreadOriginKind::Collaborative,
        START_AT,
    );
    let parent_turn = turn(
        "turn_failed_delivery",
        TurnKind::Conversation,
        TurnOrigin::User,
    );
    start_turn(&store, &parent_thread, &parent_turn).await;
    complete_turn(&store, &parent_thread, parent_turn.clone(), START_AT + 1).await;
    let delivery_id = "delivery_failed";
    seed_successful_collaborative_delivery(
        &database,
        &store,
        &parent_thread,
        &parent_turn,
        "thread_failed_child",
        "turn_failed_child",
        delivery_id,
        parent_turn.id.as_str(),
        START_AT + 2,
    )
    .await;
    database
        .execute_unprepared(&format!(
            "UPDATE task SET status = 'failed', result_json = NULL \
             WHERE id = 'task_{delivery_id}'; \
             UPDATE task_run SET status = 'failed', result_json = NULL \
             WHERE id = 'run_{delivery_id}'; \
             UPDATE task_delivery \
             SET result_snapshot_json = NULL, error_snapshot_json = '{{\"code\":\"failed\",\
                 \"message\":\"expected failure\"}}' \
             WHERE id = '{delivery_id}'"
        ))
        .await
        .expect("failed delivery fixture must update");

    materialize_delivery_item(
        &store,
        &parent_thread,
        &parent_turn,
        TurnItem::SystemEvent {
            id: pioneer_protocol::task_delivery_result_item_id(delivery_id),
            level: SystemEventLevel::Error,
            message: "expected failure".to_owned(),
            code: Some("failed".to_owned()),
            details: None,
        },
        START_AT + 4,
    )
    .await;
    assert_eq!(
        store
            .self_improvement_source_head("ws_source_a")
            .await
            .expect("source head after failed delivery"),
        0,
        "failed Collaborative delivery may be bounded context but is never an anchor"
    );
}

#[tokio::test]
async fn collaborative_source_projection_is_atomic_with_delivery_item_completion() {
    let (database, store) = migrated_store().await;
    let parent_thread = thread(
        "ws_source_a",
        "thread_delivery_atomic",
        ThreadOriginKind::Collaborative,
        START_AT,
    );
    let parent_turn = turn(
        "turn_delivery_atomic",
        TurnKind::Conversation,
        TurnOrigin::User,
    );
    start_turn(&store, &parent_thread, &parent_turn).await;
    complete_turn(&store, &parent_thread, parent_turn.clone(), START_AT + 1).await;
    let delivery_id = "delivery_atomic";
    let item = seed_successful_collaborative_delivery(
        &database,
        &store,
        &parent_thread,
        &parent_turn,
        "thread_atomic_child",
        "turn_atomic_child",
        delivery_id,
        parent_turn.id.as_str(),
        START_AT + 2,
    )
    .await;
    store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: parent_thread.workspace_id.clone(),
                thread_id: parent_thread.id.clone(),
                turn_id: parent_turn.id.clone(),
                item: item.clone(),
            },
            START_AT + 4,
        )
        .await
        .expect("delivery item start must project");
    database
        .execute_unprepared(
            "CREATE TRIGGER fail_collaborative_source_insert \
             BEFORE INSERT ON self_improvement_source_turn \
             BEGIN SELECT RAISE(ABORT, 'forced collaborative ledger failure'); END;",
        )
        .await
        .expect("failure trigger must install");

    let error = store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: parent_thread.workspace_id.clone(),
                thread_id: parent_thread.id.clone(),
                turn_id: parent_turn.id.clone(),
                item,
            },
            START_AT + 5,
        )
        .await
        .expect_err("source failure must fail the delivery item projection");
    assert!(format!("{error:#}").contains("forced collaborative ledger failure"));
    assert_eq!(
        scalar_string(
            &database,
            "SELECT status AS value FROM turn_item \
             WHERE turn_id = 'turn_delivery_atomic' \
               AND item_id = 'task_delivery_result_delivery_atomic'"
        )
        .await,
        "in_progress",
        "the ItemCompleted projection must roll back with the source row"
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_source_turn"
        )
        .await,
        0
    );

    database
        .execute_unprepared("DROP TRIGGER fail_collaborative_source_insert")
        .await
        .expect("failure trigger must be removed");
    store
        .replay_due_turn_event_projections(2_000_000_000, 10)
        .await
        .expect("failed delivery projection must replay");
    assert_eq!(
        scalar_string(
            &database,
            "SELECT status AS value FROM turn_item \
             WHERE turn_id = 'turn_delivery_atomic' \
               AND item_id = 'task_delivery_result_delivery_atomic'"
        )
        .await,
        "completed"
    );
    assert_eq!(
        store
            .self_improvement_source_head("ws_source_a")
            .await
            .expect("source head after delivery replay"),
        1
    );
}

#[tokio::test]
async fn source_cursor_preserves_sqlite_integer_ids_above_i32() {
    let (database, store) = migrated_store().await;
    let first_thread = thread(
        "ws_source_a",
        "thread_large_source_one",
        ThreadOriginKind::User,
        START_AT,
    );
    let first_turn = turn(
        "turn_large_source_one",
        TurnKind::Conversation,
        TurnOrigin::User,
    );
    start_turn(&store, &first_thread, &first_turn).await;
    complete_turn(&store, &first_thread, first_turn, START_AT + 1).await;

    database
        .execute_unprepared(
            "UPDATE sqlite_sequence SET seq = 2147483647 \
             WHERE name = 'self_improvement_source_turn'",
        )
        .await
        .expect("source sequence must advance beyond i32");

    let second_thread = thread(
        "ws_source_a",
        "thread_large_source_two",
        ThreadOriginKind::User,
        START_AT + 2,
    );
    let second_turn = turn(
        "turn_large_source_two",
        TurnKind::Conversation,
        TurnOrigin::User,
    );
    start_turn(&store, &second_thread, &second_turn).await;
    complete_turn(&store, &second_thread, second_turn, START_AT + 3).await;

    let rows = store
        .list_self_improvement_source_turns_after("ws_source_a", i64::from(i32::MAX), 0, 10)
        .await
        .expect("64-bit source range must load");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, i64::from(i32::MAX) + 1);
    assert_eq!(
        store
            .self_improvement_source_head("ws_source_a")
            .await
            .expect("64-bit source head"),
        i64::from(i32::MAX) + 1
    );
}

#[tokio::test]
async fn ledger_failure_rolls_back_completed_projection_and_replay_recovers_it() {
    let (database, store) = migrated_store().await;
    let thread = thread(
        "ws_source_a",
        "thread_atomic",
        ThreadOriginKind::User,
        START_AT,
    );
    let turn = turn("turn_atomic", TurnKind::Conversation, TurnOrigin::User);
    start_turn(&store, &thread, &turn).await;
    database
        .execute_unprepared(
            "CREATE TRIGGER fail_self_improvement_source_insert \
             BEFORE INSERT ON self_improvement_source_turn \
             BEGIN SELECT RAISE(ABORT, 'forced source ledger failure'); END;",
        )
        .await
        .expect("failure trigger must install");

    let completion = Turn {
        status: TurnStatus::Completed,
        ..turn
    };
    let error = store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn: completion,
            },
            START_AT + 1,
        )
        .await
        .expect_err("ledger failure must fail the completed projection");
    assert!(
        format!("{error:#}").contains("forced source ledger failure"),
        "unexpected failure: {error:#}"
    );
    assert_eq!(
        scalar_string(
            &database,
            "SELECT status AS value FROM turn WHERE id = 'turn_atomic'"
        )
        .await,
        "in_progress"
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_source_turn"
        )
        .await,
        0
    );

    database
        .execute_unprepared("DROP TRIGGER fail_self_improvement_source_insert")
        .await
        .expect("failure trigger must be removed");
    store
        .replay_due_turn_event_projections(2_000_000_000, 10)
        .await
        .expect("failed completion projection must replay");

    assert_eq!(
        scalar_string(
            &database,
            "SELECT status AS value FROM turn WHERE id = 'turn_atomic'"
        )
        .await,
        "completed"
    );
    assert_eq!(
        store
            .self_improvement_source_head("ws_source_a")
            .await
            .expect("source head after replay"),
        1
    );
}

#[tokio::test]
async fn installing_the_schema_does_not_backfill_historical_completed_turns() {
    let database = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite must open");
    BeforeSelfImprovementMigrator::up(&database, None)
        .await
        .expect("baseline migrations must apply");
    database
        .execute_unprepared(
            r#"
            INSERT INTO workspace (id, name, is_active, is_current)
            VALUES ('ws_historical', 'Historical', 1, 1);
            INSERT INTO thread (
                id, workspace_id, preview, mode, model, model_provider, status, origin_kind
            ) VALUES (
                'thread_historical', 'ws_historical', '', 'agent', 'gpt-5.4', 'openai',
                'idle', 'user'
            );
            INSERT INTO turn (id, thread_id, status, turn_kind, origin)
            VALUES ('turn_historical', 'thread_historical', 'completed', 'conversation', 'user');
            INSERT INTO turn_event (
                id, thread_id, turn_id, sequence, event_type, payload, created_at
            ) VALUES (
                'event_historical', 'thread_historical', 'turn_historical', 1,
                'turn/completed', '{}', CURRENT_TIMESTAMP
            );
            "#,
        )
        .await
        .expect("historical canonical rows must insert");

    Migrator::up(&database, None)
        .await
        .expect("self-improvement migration must apply");
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_source_turn"
        )
        .await,
        0,
        "migration must not reinterpret old history as new evidence"
    );
}
