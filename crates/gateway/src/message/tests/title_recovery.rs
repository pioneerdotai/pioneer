//! Isolated process and completion regressions; run only after external review.
use super::super::ThreadTitleJobState;
use super::*;
use pioneer_protocol::TurnStartParams;
use std::os::unix::fs::PermissionsExt;

const FALLBACK: &str = "first user request";

fn waiting_for_source(text: &str) -> ThreadTitleJobState {
    ThreadTitleJobState::AwaitingSuccessfulTurn(text.into())
}

fn configure_title_process(processor: &mut MessageProcessor, fixture: &std::path::Path) {
    let executable = fixture.join("mock-codex");
    std::fs::write(
        &executable,
        r#"#!/usr/bin/env python3
import json, os, pathlib, sys, time
assert sys.argv[1] == 'exec'
assert '--ephemeral' in sys.argv and '--ignore-user-config' in sys.argv
assert '--resume' not in sys.argv
request = sys.stdin.read()
home = pathlib.Path(os.environ['CODEX_HOME'])
with (home/'inputs').open('a') as inputs:
    inputs.write(json.dumps(request)+'\n')
with (home/'starts').open('a') as trace:
    trace.write(str(os.getpid())+'\n')
if (home/'hang').exists(): time.sleep(600)
for event in [
    {'type':'thread.started','thread_id':'isolated-title'},
    {'type':'turn.started'},
    {'type':'item.completed','item':{'type':'agent_message','text':'Recovered title'}},
    {'type':'turn.completed','usage':{'input_tokens':12,'output_tokens':3}}
]: print(json.dumps(event), flush=True)
"#,
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = crate::isolated_test_app_config().unwrap();
    let settings_path = fixture.join(&config.gateway.settings_file_name);
    let mut settings = crate::settings::load_or_create_gateway_settings(
        &settings_path,
        config.gateway.settings_version,
        &config.gateway.settings_file_name,
    )
    .unwrap();
    let mut instance = pioneer_protocol::GatewayCliRuntimeInstanceSettings::default_codex();
    instance.binary_path = executable.to_string_lossy().into_owned();
    instance.home_path = fixture.to_string_lossy().into_owned();
    settings
        .set_cli_runtime_settings_for_tests(pioneer_protocol::GatewayCliRuntimeSettings {
            instances: vec![instance],
        })
        .unwrap();
    crate::settings::save_gateway_settings(&settings_path, &settings).unwrap();
    processor.artifact_runtime_home = fixture.into();
    // The user turn uses openai. The explicit title selection owns the deadline.
    let mut summary = test_summary_config();
    summary.title_model_provider = Some("cli_runtime:codex".into());
    summary.title_model = Some("test-model".into());
    processor.summary_config = Arc::new(summary);
    std::fs::write(fixture.join("hang"), "").unwrap();
}

fn process_count(fixture: &std::path::Path) -> usize {
    std::fs::read_to_string(fixture.join("starts"))
        .unwrap_or_default()
        .lines()
        .count()
}

async fn wait_for_process(fixture: &std::path::Path, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while process_count(fixture) < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(process_count(fixture), count);
}

async fn wait_for_title_state(
    processor: &MessageProcessor,
    thread: &str,
    state: Option<ThreadTitleJobState>,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if processor
                .title_job_runtime
                .lock()
                .await
                .get(thread)
                .cloned()
                == state
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn exhaust_title_deadline(processor: &MessageProcessor, thread: &str, source: &str) {
    let timeout = processor
        .load_cli_runtime_instances()
        .unwrap()
        .into_iter()
        .find(|instance| instance.id == "codex")
        .unwrap()
        .request_timeout_ms;
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(timeout + 1)).await;
    tokio::time::resume();
    wait_for_title_state(processor, thread, Some(waiting_for_source(source))).await;
}

async fn prepare_final(processor: &MessageProcessor, workspace: &str, thread: &str, turn: &str) {
    processor
        .crud_store
        .prepare_turn_finalization(
            &ItemCompletedNotification {
                workspace_id: workspace.into(),
                thread_id: thread.into(),
                turn_id: turn.into(),
                item: TurnItem::AgentMessage {
                    id: format!("{turn}-final"),
                    text: "user answer".into(),
                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
            None,
            now_timestamp_secs(),
        )
        .await
        .unwrap();
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TitleCompletion {
    TerminalOnly,
    NativeFinalization,
    Reconciliation,
}

async fn complete_title_turn(
    processor: &MessageProcessor,
    workspace: &str,
    thread: &str,
    turn: &str,
    path: TitleCompletion,
    fresh: bool,
) {
    if fresh && path != TitleCompletion::TerminalOnly {
        prepare_final(processor, workspace, thread, turn).await;
    }
    if path == TitleCompletion::Reconciliation {
        let result = processor
            .reconcile_prepared_native_turn_finalizations(now_timestamp_secs(), 10)
            .await
            .unwrap();
        assert_eq!(result.newly_committed_count(), u64::from(fresh));
    }
    // Also replay the completion after a reconciliation commit. The hook must
    // belong to the fresh commit, not this idempotent terminal cleanup.
    assert!(
        processor
            .complete_native_turn(thread.into(), turn.into(), None)
            .await
    );
}

async fn start_title_user_turn(
    processor: &MessageProcessor,
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    input: Vec<UserInput>,
) {
    let params: TurnStartParams = serde_json::from_value(json!({
        "thread_id":thread,"turn_id":turn,"mode":"Agent","input":input
    }))
    .unwrap();
    let outcome = processor
        .thread_manager
        .system_turn_start_with_permission_profile(params, default_test_permission_profile())
        .await
        .unwrap();
    store
        .materialize_turn_start(
            &outcome.materialization.thread,
            outcome.materialization.sandbox_mode,
            &outcome.materialization.turn,
            &outcome.materialization.input,
            pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            ),
        )
        .await
        .unwrap();
    persist_test_cli_execution_authorization_context(
        store, workspace, thread, turn, "codex", "codex",
    )
    .await;
}

#[tokio::test]
async fn codex_title_deadline_waits_for_fresh_completion_and_keeps_manual_names() {
    for (path, manual, attachment_first) in [
        (TitleCompletion::TerminalOnly, false, false),
        (TitleCompletion::NativeFinalization, false, false),
        (TitleCompletion::Reconciliation, false, true),
        (TitleCompletion::TerminalOnly, true, false),
    ] {
        let thread = "title-recovery-root";
        let first = "title-recovery-first";
        // More than six words: recovering from the display fallback would lose
        // the final word. Retained input must still include the whole source.
        let source = "first user request with complete original context";
        let fallback = super::super::fallback_title_from_first_user_text(source).unwrap();
        let text_input = || {
            vec![UserInput::Text {
                text: source.into(),
                text_elements: Vec::new(),
            }]
        };
        let input = if attachment_first {
            vec![UserInput::File {
                url: "https://example.invalid/attachment.pdf".into(),
            }]
        } else {
            text_input()
        };
        let (mut processor, store, workspace) =
            setup_execution_window_terminal_turn_with_input(thread, first, &input).await;
        let fixture = tempfile::tempdir().unwrap();
        configure_title_process(&mut processor, fixture.path());
        let text_turn = if attachment_first {
            assert!(
                processor
                    .complete_native_turn(thread.into(), first.into(), None)
                    .await
            );
            start_title_user_turn(
                &processor,
                &store,
                &workspace,
                thread,
                "title-recovery-text",
                text_input(),
            )
            .await;
            assert!(
                store
                    .get_first_thread_user_text(thread)
                    .await
                    .unwrap()
                    .is_none()
            );
            "title-recovery-text"
        } else {
            first
        };
        processor.spawn_initial_thread_title_task(thread.into(), Some(source.into()));
        wait_for_process(fixture.path(), 1).await;
        exhaust_title_deadline(&processor, thread, source).await;
        assert_eq!(
            process_count(fixture.path()),
            1,
            "deadline must not start processes two and three"
        );
        assert_eq!(
            store
                .get_thread_model(thread)
                .await
                .unwrap()
                .unwrap()
                .name
                .as_deref(),
            Some(fallback.as_str())
        );

        complete_title_turn(&processor, &workspace, thread, text_turn, path, true).await;
        wait_for_process(fixture.path(), 2).await;
        tokio::join!(
            processor.recover_thread_title(thread.into()),
            processor.recover_thread_title(thread.into())
        );
        exhaust_title_deadline(&processor, thread, source).await;
        assert_eq!(process_count(fixture.path()), 2);

        if attachment_first {
            // Existing SQL availability-fault technique: make a required read
            // fail, then restore it. No provider or input substitution is used.
            use sea_orm::ConnectionTrait;
            let background = store.with_maintenance_access().database_connection();
            background
                .execute_unprepared("ALTER TABLE thread RENAME COLUMN name TO unavailable_name")
                .await
                .unwrap();
            processor.recover_thread_title(thread.into()).await;
            wait_for_title_state(&processor, thread, Some(waiting_for_source(source))).await;
            assert_eq!(
                process_count(fixture.path()),
                2,
                "read failure waits for another completion"
            );
            background
                .execute_unprepared("ALTER TABLE thread RENAME COLUMN unavailable_name TO name")
                .await
                .unwrap();
        }
        complete_title_turn(&processor, &workspace, thread, text_turn, path, false).await;
        assert_eq!(
            processor
                .title_job_runtime
                .lock()
                .await
                .get(thread)
                .cloned(),
            Some(waiting_for_source(source))
        );
        assert_eq!(
            process_count(fixture.path()),
            2,
            "reconciliation and completion replay cannot consume another budget"
        );

        let next = "title-recovery-next";
        start_title_user_turn(
            &processor,
            &store,
            &workspace,
            thread,
            next,
            vec![UserInput::Text {
                text: "different later user request".into(),
                text_elements: Vec::new(),
            }],
        )
        .await;
        if manual {
            store
                .update_thread_name(thread, "Manual title")
                .await
                .unwrap();
        }
        std::fs::remove_file(fixture.path().join("hang")).unwrap();
        complete_title_turn(&processor, &workspace, thread, next, path, true).await;
        wait_for_title_state(&processor, thread, None).await;
        let expected = if manual {
            "Manual title"
        } else {
            "Recovered title"
        };
        assert_eq!(
            store
                .get_thread_model(thread)
                .await
                .unwrap()
                .unwrap()
                .name
                .as_deref(),
            Some(expected)
        );
        assert_eq!(process_count(fixture.path()), if manual { 2 } else { 3 });
        for input in std::fs::read_to_string(fixture.path().join("inputs"))
            .unwrap()
            .lines()
        {
            let input: String = serde_json::from_str(input).unwrap();
            assert!(
                input.contains(source),
                "every attempt retains the full original user text"
            );
        }
    }
}

#[tokio::test]
async fn collaborative_title_recovers_on_hidden_composer_result_delivery_once() {
    let provider = Arc::new(CaptureSummaryProvider::new(
        r#"<task_result>{"summary":"answer ready","data":{"answer":"ready"}}</task_result>"#,
    ));
    provider.pause_first.store(true, Ordering::SeqCst);
    let sessions = Arc::new(SessionManager::new());
    let (tx, mut rx) = mpsc::channel(256);
    let connection = register_authenticated_test_connection(&sessions, tx).await;
    let (manager, store, workspace) = setup_workspace_manager().await;
    let original = review_enabled_processor(
        phase_13_provider_registry(provider.clone()),
        sessions.clone(),
        manager,
        store.clone(),
    );
    let mut processor = original.as_ref().clone();
    let fixture = tempfile::tempdir().unwrap();
    configure_title_process(&mut processor, fixture.path());
    std::fs::remove_file(fixture.path().join("hang")).unwrap();
    let processor = Arc::new(processor);
    sessions
        .set_connection_workspace(connection, Some(workspace.clone()))
        .await;
    processor.bind_task_bridge().await;
    let thread = "title-composer-root";
    let started = processor
        .thread_manager
        .thread_start_seeded(
            connection,
            workspace.clone(),
            ThreadStartParams {
                thread_id: thread.into(),
                workspace_id: workspace.clone(),
                name: Some(FALLBACK.into()),
                model: Some("test-model".into()),
                model_provider: Some("openai".into()),
                sandbox: Some(SandboxMode::FullAccess),
                mode: Some(ThreadMode::Agent),
                origin_kind: Some(ThreadOriginKind::Collaborative),
                sidebar_visibility: Some(ThreadSidebarVisibility::Visible),
                visibility: None,
                agent_nickname: None,
                agent_role: None,
            },
            None,
            None,
        )
        .await
        .unwrap();
    store
        .upsert_thread_model(
            &started.response.thread,
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    // Fixture starts at the state produced by the process/deadline regression.
    processor
        .title_job_runtime
        .lock()
        .await
        .insert(thread.into(), waiting_for_source(FALLBACK));
    let request = generate_test_request_id("title-composer", "start");
    processor
        .process_request_for_connection(
            connection,
            &json!({
                "jsonrpc":"2.0","id":request,"method":"turn/start",
                "params":{"thread_id":thread,"turn_id":"title-composer-user-turn",
                    "input":[{"type":"text","text":FALLBACK}],"model":"test-model",
                    "model_provider":"openai","mode":"Agent"}
            })
            .to_string(),
        )
        .await;
    let response = recv_response_by_id(&mut rx, &request).await;
    let _: TurnStartResponse = serde_json::from_value(response.result).unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while provider.call_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        processor
            .title_job_runtime
            .lock()
            .await
            .get(thread)
            .cloned(),
        Some(waiting_for_source(FALLBACK)),
        "admission is not successful execution"
    );
    assert_eq!(process_count(fixture.path()), 0);
    let tasks = store
        .list_tasks_created_by_turn(&workspace, thread, "title-composer-user-turn")
        .await
        .unwrap();
    let task = tasks
        .iter()
        .find(|task| {
            task.metadata
                .as_ref()
                .and_then(|m| m.composer_work.as_ref())
                .is_some_and(|work| work.launch.thread_id == thread)
        })
        .unwrap();
    let run = store
        .get_task(&task.id)
        .await
        .unwrap()
        .unwrap()
        .runs
        .last()
        .unwrap()
        .id
        .clone();
    let lineage = wait_for_child_lineage_for_run(store.clone(), &run).await;
    assert_ne!(lineage.child_thread_id, thread);
    provider.release_paused_call();
    assert_eq!(
        wait_for_task_status(store.clone(), &task.id, TaskStatus::Completed).await,
        TaskStatus::Completed
    );
    assert_eq!(
        process_count(fixture.path()),
        0,
        "hidden child completion alone is not a user response"
    );
    processor
        .process_due_task_deliveries(now_timestamp_secs() + 1, 16)
        .await
        .unwrap();
    wait_for_thread_name_equals(store.clone(), thread, "Recovered title").await;
    wait_for_title_state(&processor, thread, None).await;
    assert_eq!(process_count(fixture.path()), 1);
    // Restore only a pending ticket to prove that durable delivery replay, not
    // a generated name check, prevents a second request series.
    store.update_thread_name(thread, FALLBACK).await.unwrap();
    processor
        .title_job_runtime
        .lock()
        .await
        .insert(thread.into(), waiting_for_source(FALLBACK));
    processor
        .process_due_task_deliveries(now_timestamp_secs() + 100, 16)
        .await
        .unwrap();
    assert_eq!(
        processor
            .title_job_runtime
            .lock()
            .await
            .get(thread)
            .cloned(),
        Some(waiting_for_source(FALLBACK))
    );
    assert_eq!(process_count(fixture.path()), 1);
}

#[tokio::test]
async fn title_recovery_excludes_internal_origins_and_hidden_user_threads() {
    let provider = Arc::new(CaptureSummaryProvider::new("must not generate"));
    let harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
    for (index, origin, visibility) in [
        (
            0,
            ThreadOriginKind::TaskRun,
            ThreadSidebarVisibility::Visible,
        ),
        (
            1,
            ThreadOriginKind::System,
            ThreadSidebarVisibility::Visible,
        ),
        (2, ThreadOriginKind::User, ThreadSidebarVisibility::Hidden),
        (
            3,
            ThreadOriginKind::Collaborative,
            ThreadSidebarVisibility::Hidden,
        ),
        (
            4,
            ThreadOriginKind::DirectMessage,
            ThreadSidebarVisibility::Hidden,
        ),
    ] {
        let id = format!("title-excluded-{index}");
        let mut thread = phase_13_test_thread(&harness.workspace_id, &id, now_timestamp_secs());
        thread.name = Some(FALLBACK.into());
        thread.origin_kind = origin;
        thread.sidebar_visibility = visibility;
        harness
            .crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &phase_13_turn(&format!("excluded-turn-{index}"), TurnStatus::InProgress),
                &[UserInput::Text {
                    text: FALLBACK.into(),
                    text_elements: Vec::new(),
                }],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .unwrap();
        harness
            .processor
            .title_job_runtime
            .lock()
            .await
            .insert(id.clone(), waiting_for_source(FALLBACK));
        harness.processor.recover_thread_title(id.clone()).await;
        wait_for_title_state(&harness.processor, &id, None).await;
        assert_eq!(
            harness
                .crud_store
                .get_thread_model(&id)
                .await
                .unwrap()
                .unwrap()
                .name
                .as_deref(),
            Some(FALLBACK)
        );
    }
    assert_eq!(provider.call_count(), 0);
}

#[tokio::test]
async fn first_title_user_text_ignores_a_task_occurrence_in_the_same_timestamp_bucket() {
    let harness = setup_phase_13_compaction_harness(test_provider()).await;
    let thread = phase_13_test_thread(
        &harness.workspace_id,
        "title-source-order",
        now_timestamp_secs(),
    );
    let mut occurrence = phase_13_turn("aaa-task-occurrence", TurnStatus::InProgress);
    occurrence.turn_kind = pioneer_protocol::TurnKind::TaskRun;
    occurrence.origin = pioneer_protocol::TurnOrigin::DetachedTask;
    for (turn, input) in [
        (
            phase_13_turn("zzz-user-message", TurnStatus::InProgress),
            vec![UserInput::Text {
                text: FALLBACK.into(),
                text_elements: Vec::new(),
            }],
        ),
        (occurrence, vec![]),
    ] {
        harness
            .crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &input,
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .unwrap();
    }
    assert_eq!(
        harness
            .crud_store
            .with_maintenance_access()
            .get_first_thread_user_text(&thread.id)
            .await
            .unwrap()
            .as_deref(),
        Some(FALLBACK)
    );
}
