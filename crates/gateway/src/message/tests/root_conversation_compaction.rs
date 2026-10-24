//! Publication-to-worker regressions. Deliberately not executed before external review.
use super::*;
use pioneer_agent::compaction::controller::{NativeContext, NativeContextController};
use pioneer_entity::compaction_history_check as check;
use sea_orm::{ConnectionTrait, EntityTrait};

struct RootClock(tokio::sync::watch::Sender<u64>);
impl RootClock {
    fn new(now: u64) -> Self {
        Self(tokio::sync::watch::channel(now).0)
    }
    fn set(&self, now: u64) {
        self.0.send_replace(now);
    }
}
#[async_trait::async_trait]
impl crate::compaction::CompactionClock for RootClock {
    fn now_ms(&self) -> u64 {
        *self.0.borrow()
    }
    async fn sleep_until(&self, deadline: u64) {
        let mut time = self.0.subscribe();
        loop {
            if *time.borrow_and_update() >= deadline {
                return;
            }
            time.changed().await.expect("clock owner");
        }
    }
}

async fn enable_worker(harness: &Phase13CompactionHarness) {
    crate::compaction::load_test_catalog();
    harness
        .processor
        .agent_manager
        .set_context_controller(Some(Arc::new(
            crate::compaction::GatewayNativeContextController::new(Arc::downgrade(
                &harness.processor,
            )),
        )))
        .await;
}

pub(super) async fn human_input(
    harness: &Phase13CompactionHarness,
    thread: &str,
    turn: &str,
    at: i64,
    text: &str,
) {
    ensure_test_superuser_execution_authority(&harness.crud_store).await;
    let mut record = phase_13_test_thread(&harness.workspace_id, thread, at);
    if let Some(existing) = harness.crud_store.get_thread_model(thread).await.unwrap() {
        record.origin_kind = existing.origin_kind;
    }
    harness
        .crud_store
        .materialize_turn_start(
            &record,
            SandboxMode::FullAccess,
            &phase_13_turn(turn, TurnStatus::InProgress),
            &[UserInput::Text {
                text: text.into(),
                text_elements: Vec::new(),
            }],
            pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            ),
        )
        .await
        .unwrap();
    // Protocol Turn has no created_at field. materialize_turn_start passes
    // thread.updated_at as the durable TurnStarted event timestamp, and the
    // projector stores that timestamp on entity::turn.created_at. Verify the
    // actual persisted boundary, so a fixture default to wall time cannot hide
    // an expiry regression.
    let durable = pioneer_entity::turn::Entity::find_by_id(turn)
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.created_at.timestamp(), at);
    persist_test_execution_authorization_context_for_principal(
        &harness.processor,
        authenticated_test_superuser().as_ref(),
        &harness.workspace_id,
        thread,
        turn,
    )
    .await;
}

pub(super) async fn answer(
    harness: &Phase13CompactionHarness,
    thread: &str,
    turn: &str,
    at: i64,
    text: &str,
) {
    harness
        .crud_store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: harness.workspace_id.clone(),
                thread_id: thread.into(),
                turn_id: turn.into(),
                item: TurnItem::AgentMessage {
                    id: format!("{turn}-answer"),
                    text: text.into(),
                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            at,
        )
        .await
        .unwrap();
    harness
        .crud_store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: harness.workspace_id.clone(),
                thread_id: thread.into(),
                turn: phase_13_turn(turn, TurnStatus::Completed),
            },
            at,
        )
        .await
        .unwrap();
}

async fn drain_worker(processor: &MessageProcessor) {
    processor.poll_completed_history_checks().await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if processor
                .completed_history_checks
                .lock()
                .await
                .values()
                .all(|job| {
                    job.handle
                        .as_ref()
                        .is_none_or(tokio::task::JoinHandle::is_finished)
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("history worker did not settle");
    processor.poll_completed_history_checks().await.unwrap();
}

#[tokio::test]
async fn published_human_answer_prepares_root_once_and_next_cli_and_native_reuse_checkpoint() {
    let summary = [
        "Goal and constraints",
        "Decisions and rationale",
        "Completed work and results",
        "Failed attempts and unknowns",
        "Current work and next step",
        "Source references",
    ]
    .map(|heading| format!("## {heading}\nroot-prepared-checkpoint-fact"))
    .join("\n\n");
    let provider = Arc::new(CaptureSummaryProvider::new(&summary));
    let harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
    enable_worker(&harness).await;
    let at = phase_13_now_secs();
    human_input(
        &harness,
        "warm-root",
        "warm-root-answer",
        at,
        "root question",
    )
    .await;
    assert!(
        harness
            .crud_store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty(),
        "a question without a published answer is not a warmup trigger"
    );
    answer(
        &harness,
        "warm-root",
        "warm-root-answer",
        at,
        &"covered-root-raw-work ".repeat(80_000),
    )
    .await;
    let due = harness
        .crud_store
        .compaction_pending_history_checks()
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].thread_id, "warm-root");
    assert_eq!(due[0].turn_id, "warm-root-answer");
    drain_worker(&harness.processor).await;
    let checkpoint = harness
        .crud_store
        .compaction_head(&crate::compaction::native_owner(
            &harness.workspace_id,
            "warm-root",
        ))
        .await
        .unwrap()
        .expect("published root answer was never compacted");
    let calls = provider.call_count();
    assert!(
        calls > 0,
        "oversized root must actually reach the summarizer double"
    );
    drain_worker(&harness.processor).await;
    assert_eq!(
        provider.call_count(),
        calls,
        "compaction lifecycle events must not feed back into warmup"
    );

    human_input(
        &harness,
        "warm-root",
        "warm-root-next",
        at + 1,
        "uncovered-next-input",
    )
    .await;
    let deadline = u64::try_from(chrono::Utc::now().timestamp_millis())
        .unwrap()
        .saturating_add(pioneer_compaction::OPERATION_MILLIS);
    let cli = harness
        .processor
        .capture_cli_transfer_projection(
            &harness.workspace_id,
            "warm-root",
            "warm-root-next",
            deadline,
        )
        .await
        .unwrap();
    assert!(
        cli.messages.iter().any(|message| message
            .provenance
            .as_ref()
            .is_some_and(|origin| origin.sources.iter().any(|source| source.id == checkpoint))),
        "the next actual CLI projection must reuse the root checkpoint"
    );
    assert!(
        !cli.messages
            .iter()
            .any(|message| message.content.contains("covered-root-raw-work")),
        "covered originals must not be sent again alongside their checkpoint"
    );
    let events = Arc::new(pioneer_runtime_events::ExecutionEventHub::new());
    let context = NativeContext {
        overflow_recovery: false,
        recovery_deadline_ms: None,
        workspace_id: harness.workspace_id.clone(),
        thread_id: "warm-root".into(),
        turn_id: "warm-root-next".into(),
        conversation_thread_id: None,
        provider_instance: "openai".into(),
        provider: provider.clone(),
        events: events.clone(),
        cancellation: tokio_util::sync::CancellationToken::new(),
    };
    let prepared =
        crate::compaction::GatewayNativeContextController::new(Arc::downgrade(&harness.processor))
            .prepare(
                &context,
                ChatRequest {
                    model: "test-model".into(),
                    messages: vec![pioneer_provider::ChatMessage::user("uncovered-next-input")],
                    temperature: None,
                    max_tokens: Some(1024),
                    tools: None,
                    tool_choice: None,
                    parallel_tool_calls: None,
                    reasoning: None,
                    compiled_prompt: None,
                },
                None,
                false,
            )
            .await
            .unwrap();
    assert!(
        prepared
            .request
            .messages
            .iter()
            .any(|message| message.content.contains("uncovered-next-input"))
    );
    assert!(prepared.request.messages.iter().any(|message| {
        message
            .provenance
            .as_ref()
            .is_some_and(|origin| origin.sources.iter().any(|source| source.id == checkpoint))
    }));
    assert!(
        !prepared
            .request
            .messages
            .iter()
            .any(|message| message.content.contains("covered-root-raw-work"))
    );
    assert_eq!(
        provider.call_count(),
        calls,
        "next request must not summarize covered history again"
    );
    events.shutdown_progress().await;
}

#[tokio::test]
async fn completed_cli_child_keeps_history_without_hypothetical_continuation_warmup() {
    let provider = Arc::new(CaptureSummaryProvider::new("must not run"));
    let harness =
        setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
    enable_worker(&harness).await;
    human_input(
        &harness,
        "child-policy-root",
        "child-policy-question",
        phase_13_now_secs(),
        "question",
    )
    .await;
    seed_cli_runtime_turn_with_text(
        &harness.crud_store,
        &harness.workspace_id,
        "fixture-cli",
        "codex",
        "child-policy-child",
        "child-policy-result",
        "child-native",
        "retained child input",
    )
    .await;
    harness.crud_store.database_connection().execute_unprepared(
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at,origin_kind) VALUES('child-policy-child','child-policy-root','child-policy-root',1,CURRENT_TIMESTAMP,'task_run')",
    ).await.unwrap();
    // Native registration remains required even for a CLI fixture: it is not
    // itself evidence that the context was published to a user conversation.
    harness
        .crud_store
        .compaction_enqueue_native_history_check(
            &harness.workspace_id,
            "child-policy-child",
            "child-policy-result",
            "{}",
        )
        .await
        .unwrap();
    answer(
        &harness,
        "child-policy-child",
        "child-policy-result",
        phase_13_now_secs(),
        &"retained-child-work ".repeat(80_000),
    )
    .await;
    drain_worker(&harness.processor).await;
    assert!(
        harness
            .crud_store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(provider.call_count(), 0);
    assert!(
        harness
            .crud_store
            .compaction_head(&crate::compaction::native_owner(
                &harness.workspace_id,
                "child-policy-child"
            ))
            .await
            .unwrap()
            .is_none()
    );
    let item = harness
        .crud_store
        .get_turn_item("child-policy-result", "child-policy-result-answer")
        .await
        .unwrap()
        .expect("disabling optional warmup must retain child results");
    assert!(
        matches!(item, TurnItem::AgentMessage { text, .. } if text.contains("retained-child-work"))
    );
}

#[tokio::test]
async fn worker_window_uses_original_activity_and_late_answer_opens_a_fresh_window() {
    // Config owner tests AppConfig layering; this boundary exercises the field
    // in the actual worker with a deterministic millisecond clock.
    let original = phase_13_now_secs() - 100000;
    for (window, offset, expected) in [
        (28800_u64, 28_799_999_u64, "fits"),
        (28800, 28_800_000, "cancelled"),
        (60, 59_999, "fits"),
        (60, 60_000, "cancelled"),
    ] {
        let provider = Arc::new(CaptureSummaryProvider::new("must not run"));
        let mut harness =
            setup_phase_13_compaction_harness(phase_13_provider_registry(provider.clone())).await;
        let clock = Arc::new(RootClock::new(original as u64 * 1000 + offset));
        let mut processor = harness.processor.as_ref().clone();
        processor
            .context_compaction_config
            .hot_conversation_window_secs = window;
        processor.compaction_clock = clock.clone();
        harness.processor = Arc::new(processor);
        enable_worker(&harness).await;
        human_input(
            &harness,
            "window-root",
            "window-question",
            original,
            "old question",
        )
        .await;
        answer(
            &harness,
            "window-root",
            "window-question",
            original,
            "old ready answer",
        )
        .await;
        drain_worker(&harness.processor).await;
        let row = check::Entity::find_by_id("window-question")
            .one(&harness.crud_store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.outcome.as_deref(),
            Some(expected),
            "strict window boundary must use original activity and configured seconds"
        );
        assert_eq!(provider.call_count(), 0);
        // Replay the exact completed event. It must neither refresh activity nor
        // reopen finished work; no activity table/queue mutation in this test.
        harness
            .crud_store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: harness.workspace_id.clone(),
                    thread_id: "window-root".into(),
                    turn: phase_13_turn("window-question", TurnStatus::Completed),
                },
                original,
            )
            .await
            .unwrap();
        drain_worker(&harness.processor).await;
        let replay = check::Entity::find_by_id("window-question")
            .one(&harness.crud_store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.revision, row.revision);
        assert_eq!(replay.outcome, row.outcome);
        let ready = original + 200000;
        clock.set(ready as u64 * 1000);
        human_input(
            &harness,
            "window-root",
            "late-question",
            original + 1,
            "long running question",
        )
        .await;
        answer(
            &harness,
            "window-root",
            "late-question",
            ready,
            "just ready answer",
        )
        .await;
        drain_worker(&harness.processor).await;
        let late = check::Entity::find_by_id("late-question")
            .one(&harness.crud_store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            late.outcome.as_deref(),
            Some("fits"),
            "late readiness must renew activity"
        );
        assert_eq!(provider.call_count(), 0);
    }
}

async fn accept_candidate(
    harness: &Phase13CompactionHarness,
    connection: ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    task_id: &str,
    run_id: &str,
    candidate: &TaskResultCandidate,
) {
    let request_id = generate_test_request_id("root-compaction", "accept");
    harness.processor.process_request_for_connection(connection, &json!({
        "jsonrpc":"2.0", "id":request_id, "method":pioneer_protocol::constants::methods::TASK_ACCEPT,
        "params":{"taskId":task_id,"runId":run_id,"candidateId":candidate.id,"reason":"approved"}
    }).to_string()).await;
    let response = recv_response_by_id(rx, &request_id).await;
    let accepted: TaskAcceptResponse = serde_json::from_value(response.result).unwrap();
    assert!(accepted.accepted);
}

async fn reviewed_task(
    harness: &Phase13CompactionHarness,
    goal: &str,
    destination: Option<&str>,
) -> (pioneer_protocol::TaskCreateResponse, TaskResultCandidate) {
    reviewed_task_in(
        harness,
        "delivery-root",
        "delivery-question",
        goal,
        destination,
    )
    .await
}

async fn reviewed_task_in(
    harness: &Phase13CompactionHarness,
    root: &str,
    question: &str,
    goal: &str,
    destination: Option<&str>,
) -> (pioneer_protocol::TaskCreateResponse, TaskResultCandidate) {
    let mut params = test_task_create_params(&harness.workspace_id, root, question, goal, 3);
    params.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Detached,
        on_parent_cancel: TaskParentTerminalAction::KeepRunning,
        on_parent_failure: TaskParentTerminalAction::KeepRunning,
        completion: TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    params.agent_spec.as_mut().unwrap().review_policy = Some(TaskAgentReviewPolicy {
        mode: TaskAgentReviewMode::UserApproval,
        max_revision_rounds: 2,
        require_explicit_acceptance: true,
        reviewers: vec![],
        resolution_strategy: TaskResultReviewResolutionStrategy::UserFinal,
    });
    if let Some(destination) = destination {
        params.delivery_policy = Some(TaskDeliveryPolicy {
            mode: TaskDeliveryMode::Thread,
            thread_target: Some(pioneer_protocol::TaskDeliveryThreadTarget::ExactThread),
            thread_id: Some(destination.into()),
            webhook_url: None,
            include_result: true,
            format: TaskDeliveryFormat::FullResult,
        });
    }
    let created = create_task_for_test(&harness.processor, params)
        .await
        .unwrap();
    let candidate = wait_for_result_candidate_status(
        harness.crud_store.clone(),
        &created.run.as_ref().unwrap().id,
        TaskResultCandidateStatus::PendingReview,
    )
    .await;
    (created, candidate)
}

#[tokio::test]
async fn accepted_deliveries_coalesce_keep_frozen_outputs_and_preserve_work_arriving_during_preparation()
 {
    let provider = Arc::new(CaptureSummaryProvider::new(
        r#"<task_result>{"summary":"accepted task fact","data":{"answer":"ok"}}</task_result>"#,
    ));
    let sessions = Arc::new(SessionManager::new());
    let (tx, mut rx) = mpsc::channel(256);
    let connection = register_authenticated_test_connection(&sessions, tx).await;
    let (manager, store, workspace) = setup_workspace_manager().await;
    sessions
        .set_connection_workspace(connection, Some(workspace.clone()))
        .await;
    let processor = review_enabled_processor(
        phase_13_provider_registry(provider.clone()),
        sessions,
        manager,
        store.clone(),
    );
    let now = phase_13_now_secs();
    let original_activity = now - 3600;
    let clock = Arc::new(RootClock::new(now as u64 * 1000));
    let mut processor_config = processor.as_ref().clone();
    processor_config.compaction_clock = clock.clone();
    let harness = Phase13CompactionHarness {
        processor: Arc::new(processor_config),
        crud_store: store,
        workspace_id: workspace,
    };
    enable_worker(&harness).await;
    harness.processor.bind_task_bridge().await;
    human_input(
        &harness,
        "delivery-root",
        "delivery-question",
        original_activity,
        "shared-root-history-once",
    )
    .await;
    // A message-only finished root has no response publication debt.
    harness
        .crud_store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: harness.workspace_id.clone(),
                thread_id: "delivery-root".into(),
                turn: phase_13_turn("delivery-question", TurnStatus::Completed),
            },
            phase_13_now_secs(),
        )
        .await
        .unwrap();

    let mut frozen = Vec::new();
    for goal in ["first-result", "second-result"] {
        let (task, candidate) = reviewed_task(&harness, goal, Some("delivery-root")).await;
        let run = task.run.as_ref().unwrap();
        let snapshot = harness
            .crud_store
            .compaction_task_output(&harness.workspace_id, &candidate.task_run_turn_id)
            .await
            .unwrap()
            .expect("review candidate must retain immutable output before acceptance");
        assert!(
            harness
                .crud_store
                .compaction_pending_history_checks()
                .await
                .unwrap()
                .is_empty(),
            "unaccepted child completion is not root publication"
        );
        accept_candidate(
            &harness,
            connection,
            &mut rx,
            &task.task.id,
            &run.id,
            &candidate,
        )
        .await;
        if frozen.is_empty() {
            assert!(
                harness
                    .crud_store
                    .compaction_pending_history_checks()
                    .await
                    .unwrap()
                    .is_empty(),
                "acceptance alone must not prepare an undelivered result"
            );
        }
        frozen.push((candidate.task_run_turn_id.clone(), snapshot));
    }
    harness
        .processor
        .process_due_task_deliveries(phase_13_now_secs() + 1, 16)
        .await
        .unwrap();
    let pending = harness
        .crud_store
        .compaction_pending_history_checks()
        .await
        .unwrap();
    assert_eq!(
        pending.len(),
        1,
        "multiple deliveries must share one root work item"
    );
    assert_eq!(pending[0].thread_id, "delivery-root");
    assert_eq!(pending[0].turn_id, "delivery-question");
    assert_eq!(
        harness
            .crud_store
            .compaction_root_history_activity(
                &harness.workspace_id,
                "delivery-root",
                "delivery-question",
            )
            .await
            .unwrap(),
        Some(original_activity * 1000),
        "ordinary results recover context but do not renew a conversation's activity"
    );
    let revision_before = pending[0].revision;
    let prepared = harness
        .processor
        .capture_current_context_basis_prepared(
            &harness.crud_store.with_maintenance_access(),
            &harness.workspace_id,
            "delivery-root",
            "delivery-question",
            None,
        )
        .await
        .unwrap();
    let joined = prepared
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        joined.matches("shared-root-history-once").count(),
        1,
        "accepted child branches must not duplicate their shared root history"
    );
    drop(prepared);

    harness
        .processor
        .arm_completed_history_preparation_barrier("delivery-question");
    harness
        .processor
        .poll_completed_history_checks()
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        harness
            .processor
            .wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        harness
            .processor
            .completed_history_checks
            .lock()
            .await
            .len(),
        1
    );
    let claimed = check::Entity::find_by_id("delivery-question")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert!(claimed.revision > revision_before);
    let (third, candidate) = reviewed_task(&harness, "third-result", Some("delivery-root")).await;
    let third_run = third.run.as_ref().unwrap();
    accept_candidate(
        &harness,
        connection,
        &mut rx,
        &third.task.id,
        &third_run.id,
        &candidate,
    )
    .await;
    harness
        .processor
        .process_due_task_deliveries(phase_13_now_secs() + 1, 16)
        .await
        .unwrap();
    let arrived = check::Entity::find_by_id("delivery-question")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert!(
        arrived.revision > claimed.revision,
        "delivery must supersede the captured work revision"
    );
    harness
        .processor
        .release_completed_history_preparation_barrier();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if harness
                .processor
                .completed_history_checks
                .lock()
                .await
                .values()
                .all(|job| {
                    job.handle
                        .as_ref()
                        .is_none_or(tokio::task::JoinHandle::is_finished)
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let retained = check::Entity::find_by_id("delivery-question")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained.revision, arrived.revision,
        "stale worker completion must lose its CAS"
    );
    assert_eq!(
        retained.state, "pending",
        "the new delivery must survive the old worker"
    );
    // Drop in-memory ownership as at shutdown; publication debt is durable.
    harness.processor.suspend_completed_history_checks().await;
    drain_worker(&harness.processor).await;
    let finished = check::Entity::find_by_id("delivery-question")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.outcome.as_deref(), Some("fits"));
    let intermediate = frozen[0].1.source_thread.clone();
    for (id, snapshot) in frozen {
        assert_eq!(
            harness
                .crud_store
                .compaction_task_output(&harness.workspace_id, &id)
                .await
                .unwrap(),
            Some(snapshot)
        );
    }
    let calls = provider.call_count();
    harness
        .processor
        .process_due_task_deliveries(phase_13_now_secs() + 1, 16)
        .await
        .unwrap();
    drain_worker(&harness.processor).await;
    assert_eq!(
        provider.call_count(),
        calls,
        "replayed delivery and compaction events must not create a loop"
    );
    assert!(
        harness
            .crud_store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );

    let (intermediate_task, intermediate_candidate) =
        reviewed_task(&harness, "intermediate-only", Some(&intermediate)).await;
    accept_candidate(
        &harness,
        connection,
        &mut rx,
        &intermediate_task.task.id,
        &intermediate_task.run.as_ref().unwrap().id,
        &intermediate_candidate,
    )
    .await;
    harness
        .processor
        .process_due_task_deliveries(phase_13_now_secs() + 1, 16)
        .await
        .unwrap();
    assert!(
        harness
            .crud_store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty(),
        "delivery available only to an internal parent must not prepare root or intermediate"
    );
    assert!(
        harness
            .crud_store
            .compaction_head(&crate::compaction::native_owner(
                &harness.workspace_id,
                &intermediate
            ))
            .await
            .unwrap()
            .is_none()
    );

    let (task, candidate) = reviewed_task(&harness, "accepted-but-none", None).await;
    accept_candidate(
        &harness,
        connection,
        &mut rx,
        &task.task.id,
        &task.run.as_ref().unwrap().id,
        &candidate,
    )
    .await;
    harness
        .processor
        .process_due_task_deliveries(phase_13_now_secs() + 1, 16)
        .await
        .unwrap();
    assert!(
        harness
            .crud_store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty(),
        "accepted result with delivery none must not prepare root"
    );
    // A later independent report is real new context, but an abandoned root
    // stays cold. The Task still runs its mandatory Native preflight normally.
    clock.set(original_activity as u64 * 1000 + 28_800_000);
    let (monitor, candidate) = reviewed_task(
        &harness,
        "independent-monitor-report",
        Some("delivery-root"),
    )
    .await;
    accept_candidate(
        &harness,
        connection,
        &mut rx,
        &monitor.task.id,
        &monitor.run.as_ref().unwrap().id,
        &candidate,
    )
    .await;
    harness
        .processor
        .process_due_task_deliveries(phase_13_now_secs() + 1, 16)
        .await
        .unwrap();
    assert_eq!(
        harness
            .crud_store
            .compaction_root_history_activity(
                &harness.workspace_id,
                "delivery-root",
                "delivery-question",
            )
            .await
            .unwrap(),
        Some(original_activity * 1000)
    );
    drain_worker(&harness.processor).await;
    let cold = check::Entity::find_by_id("delivery-question")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cold.outcome.as_deref(),
        Some("cancelled"),
        "monitor publication must not reopen an expired root window"
    );
    assert_eq!(
        provider
            .snapshot_requests()
            .iter()
            .filter(|request| {
                request.messages.first().is_some_and(|message| {
                    message.content == pioneer_compaction::summary::INSTRUCTIONS
                })
            })
            .count(),
        0,
        "cold root must not call the summarizer"
    );
}

#[tokio::test]
async fn hidden_composer_defers_root_warmup_until_its_answer_is_delivered() {
    let provider = Arc::new(CaptureSummaryProvider::new(
        r#"<task_result>{"summary":"composer and ordinary work","data":{"answer":"ready"}}</task_result>"#,
    ));
    let sessions = Arc::new(SessionManager::new());
    let (tx, mut rx) = mpsc::channel(256);
    let connection = register_authenticated_test_connection(sessions.as_ref(), tx).await;
    let (manager, store, workspace) = setup_workspace_manager().await;
    let processor = review_enabled_processor(
        phase_13_provider_registry(provider.clone()),
        sessions.clone(),
        manager,
        store.clone(),
    );
    sessions
        .set_connection_workspace(connection, Some(workspace.clone()))
        .await;
    // The RPC uses the actual typed Composer producer. Backdate only the
    // persisted fixture input below; the service publication keeps its genuine
    // timestamp. The worker clock can cross eight hours without real waiting.
    let original_input = phase_13_now_secs() - 9 * 3600;
    let clock = Arc::new(RootClock::new(original_input as u64 * 1000 + 1000));
    let mut processor = processor.as_ref().clone();
    processor.compaction_clock = clock.clone();
    let harness = Phase13CompactionHarness {
        processor: Arc::new(processor),
        crud_store: store,
        workspace_id: workspace,
    };
    enable_worker(&harness).await;
    harness.processor.bind_task_bridge().await;
    let started = harness
        .processor
        .thread_manager
        .thread_start_seeded(
            connection,
            harness.workspace_id.clone(),
            ThreadStartParams {
                thread_id: "composer-warm-root".into(),
                workspace_id: harness.workspace_id.clone(),
                name: None,
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
    harness
        .crud_store
        .upsert_thread_model(
            &started.response.thread,
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    // The double blocks the actual Composer provider call, after its real
    // hidden task, frozen history and execution authority have been admitted.
    provider.pause_first.store(true, Ordering::SeqCst);
    let request = generate_test_request_id("composer-root", "start");
    harness
        .processor
        .process_request_for_connection(
            connection,
            &json!({
                "jsonrpc":"2.0","id":request,"method":"turn/start",
                "params":{"thread_id":"composer-warm-root","turn_id":"composer-human-question",
                    "input":[{"type":"text","text":"composer-current-question"}],
                    "model":"test-model","model_provider":"openai","mode":"Agent"}
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
    let human = pioneer_entity::turn::Entity::find_by_id("composer-human-question")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(human.work_owner, "detached_task");
    let input_timestamp = chrono::DateTime::from_timestamp(original_input, 0).unwrap();
    let db = harness.crud_store.database_connection();
    db.execute_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DbBackend::Sqlite,
        "UPDATE turn SET created_at=? WHERE id='composer-human-question'",
        [input_timestamp.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
        "UPDATE turn_event SET created_at=? WHERE turn_id='composer-human-question' AND event_type='turn/started'",
        [input_timestamp.into()],
    )).await.unwrap();
    db.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
        "UPDATE compaction_root_activity SET last_user_activity_ms=? WHERE thread_id='composer-warm-root'",
        [(original_input * 1000).into()],
    )).await.unwrap();
    let activity = harness
        .crud_store
        .compaction_root_history_activity(
            &harness.workspace_id,
            "composer-warm-root",
            "composer-human-question",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(activity, original_input * 1000);
    let tasks = harness
        .crud_store
        .list_tasks_created_by_turn(
            &harness.workspace_id,
            "composer-warm-root",
            "composer-human-question",
        )
        .await
        .unwrap();
    let composer = tasks
        .iter()
        .find(|task| {
            task.metadata
                .as_ref()
                .and_then(|metadata| metadata.composer_work.as_ref())
                .is_some()
        })
        .expect("normal Composer RPC must create its hidden technical task");
    let state = harness
        .crud_store
        .get_task(&composer.id)
        .await
        .unwrap()
        .unwrap();
    let run_id = state.runs.last().unwrap().id.clone();
    let lineage = wait_for_child_lineage_for_run(harness.crud_store.clone(), &run_id).await;
    assert_ne!(lineage.child_thread_id, "composer-warm-root");
    assert!(
        harness
            .crud_store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty(),
        "visible message completion is not readiness of its hidden response"
    );

    // Simulate the upgraded root's absent activity index. Publication must
    // atomically seed debt, and the real worker must restore activity before
    // its busy decision; the test does not invoke a recovery helper directly.
    db.execute_unprepared(
        "DELETE FROM compaction_root_activity WHERE thread_id='composer-warm-root'",
    )
    .await
    .unwrap();
    let (ordinary, candidate) = reviewed_task_in(
        &harness,
        "composer-warm-root",
        "composer-human-question",
        "ordinary-during-composer",
        Some("composer-warm-root"),
    )
    .await;
    accept_candidate(
        &harness,
        connection,
        &mut rx,
        &ordinary.task.id,
        &ordinary.run.as_ref().unwrap().id,
        &candidate,
    )
    .await;
    harness
        .processor
        .process_due_task_deliveries(phase_13_now_secs() + 1, 16)
        .await
        .unwrap();
    assert!(
        harness
            .crud_store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty(),
        "seeded root debt waits for activity reconstruction in the real poll"
    );
    let seeded = check::Entity::find_by_id("composer-human-question")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seeded.purpose, "root_conversation");
    assert_eq!(seeded.state, "pending");
    drain_worker(&harness.processor).await;
    let deferred = check::Entity::find_by_id("composer-human-question")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deferred.state, "pending");
    assert_eq!(
        deferred.outcome.as_deref(),
        Some("waiting_executor"),
        "busy detection must include the technical Composer execution"
    );
    assert_eq!(
        harness
            .crud_store
            .compaction_root_history_activity(
                &harness.workspace_id,
                "composer-warm-root",
                "composer-human-question"
            )
            .await
            .unwrap(),
        Some(activity),
        "ordinary task output must not refresh human activity"
    );
    clock.set(activity as u64 + 28_800_000);
    drain_worker(&harness.processor).await;
    let expired = check::Entity::find_by_id("composer-human-question")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        expired.outcome.as_deref(),
        Some("cancelled"),
        "ordinary delivery during a long Composer run does not extend the window"
    );
    provider.release_first.notify_one();
    assert_eq!(
        wait_for_task_status(
            harness.crud_store.clone(),
            &composer.id,
            TaskStatus::Completed
        )
        .await,
        TaskStatus::Completed
    );
    assert_eq!(
        harness
            .crud_store
            .compaction_root_history_activity(
                &harness.workspace_id,
                "composer-warm-root",
                "composer-human-question"
            )
            .await
            .unwrap(),
        Some(activity),
        "child computation completion must not masquerade as response publication"
    );
    harness
        .processor
        .process_due_task_deliveries(phase_13_now_secs() + 1, 16)
        .await
        .unwrap();
    let delivered_activity = harness
        .crud_store
        .compaction_root_history_activity(
            &harness.workspace_id,
            "composer-warm-root",
            "composer-human-question",
        )
        .await
        .unwrap()
        .unwrap();
    let receipts = harness
        .crud_store
        .list_task_deliveries(TaskDeliveriesParams {
            workspace_id: harness.workspace_id.clone(),
            task_id: Some(composer.id.clone()),
            run_id: Some(run_id.clone()),
            statuses: vec![TaskDeliveryStatus::Delivered],
            limit: Some(10),
        })
        .await
        .unwrap();
    let receipt = receipts
        .first()
        .expect("actual Composer delivery must be acknowledged");
    let acknowledgement = harness.crud_store.database_connection().query_one_raw(
        sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
            "SELECT e.created_at FROM turn_event e JOIN compaction_event_revision r ON r.source_id=e.id WHERE e.turn_id=? AND e.event_type='item/completed' AND r.item_id=? AND r.present=1 AND r.projection_revision=r.revision ORDER BY e.sequence LIMIT 1",
            [receipt.delivered_turn_id.clone().unwrap().into(), pioneer_protocol::task_delivery_result_item_id(&receipt.id).into()],
        )
    ).await.unwrap().expect("exact published Composer acknowledgement");
    let ready_at: chrono::DateTime<chrono::FixedOffset> =
        acknowledgement.try_get("", "created_at").unwrap();
    assert_eq!(
        delivered_activity,
        std::cmp::max(activity, ready_at.timestamp_millis()),
        "Composer activity follows original publication, even if receipt processing crosses a clock second"
    );
    assert!(
        delivered_activity >= activity + 28_800_000,
        "this actual Composer response became ready later than eight hours after input"
    );
    clock.set(delivered_activity as u64);
    drain_worker(&harness.processor).await;
    let done = check::Entity::find_by_id("composer-human-question")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.outcome.as_deref(), Some("fits"));
    assert!(
        harness
            .crud_store
            .compaction_head(&crate::compaction::native_owner(
                &harness.workspace_id,
                &lineage.child_thread_id
            ))
            .await
            .unwrap()
            .is_none(),
        "the hidden Composer child is never warmed for hypothetical continuation"
    );
}

#[tokio::test]
async fn real_composer_native_start_preempts_background_owned_by_the_visible_root() {
    let provider = Arc::new(CaptureSummaryProvider::new(
        r#"<task_result>{"summary":"foreground completed","data":{"answer":"ready"}}</task_result>"#,
    ).with_valid_summary_completion());
    let sessions = Arc::new(SessionManager::new());
    let (tx, mut rx) = mpsc::channel(256);
    let connection = register_authenticated_test_connection(sessions.as_ref(), tx).await;
    let (manager, store, workspace) = setup_workspace_manager().await;
    let processor = review_enabled_processor(
        phase_13_provider_registry(provider.clone()),
        sessions.clone(),
        manager,
        store.clone(),
    );
    sessions
        .set_connection_workspace(connection, Some(workspace.clone()))
        .await;
    let harness = Phase13CompactionHarness {
        processor,
        crud_store: store,
        workspace_id: workspace,
    };
    enable_worker(&harness).await;
    harness.processor.bind_task_bridge().await;
    let started = harness
        .processor
        .thread_manager
        .thread_start_seeded(
            connection,
            harness.workspace_id.clone(),
            ThreadStartParams {
                thread_id: "priority-composer-root".into(),
                workspace_id: harness.workspace_id.clone(),
                name: None,
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
    harness
        .crud_store
        .upsert_thread_model(
            &started.response.thread,
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    human_input(
        &harness,
        "priority-composer-root",
        "priority-old-answer",
        phase_13_now_secs(),
        "prior root request",
    )
    .await;
    answer(
        &harness,
        "priority-composer-root",
        "priority-old-answer",
        phase_13_now_secs(),
        &"prior-root-work ".repeat(80_000),
    )
    .await;
    provider.pause_first.store(true, Ordering::SeqCst);
    harness
        .processor
        .poll_completed_history_checks()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while provider.call_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let background = harness.processor.completed_history_checks.lock().await;
    assert!(background.values().any(|job| {
        job.handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }));
    drop(background);
    // Do not release the summarizer double. The new foreground must cancel
    // the root background owner, join its cleanup and enter its technical child.
    let request = generate_test_request_id("priority-composer", "start");
    harness
        .processor
        .process_request_for_connection(
            connection,
            &json!({
                "jsonrpc":"2.0","id":request,"method":"turn/start",
                "params":{"thread_id":"priority-composer-root","turn_id":"priority-new-question",
                    "input":[{"type":"text","text":"priority-new-composer-command"}],
                    "model":"test-model","model_provider":"openai","mode":"Agent"}
            })
            .to_string(),
        )
        .await;
    let response = recv_response_by_id(&mut rx, &request).await;
    let _: TurnStartResponse = serde_json::from_value(response.result).unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if provider.snapshot_requests().iter().any(|request| {
                request.messages.first().is_none_or(|message| {
                    message.content != pioneer_compaction::summary::INSTRUCTIONS
                }) && request
                    .messages
                    .iter()
                    .any(|message| message.content.contains("priority-new-composer-command"))
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("technical Composer foreground must preempt root background without manual release");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if harness
                .processor
                .completed_history_checks
                .lock()
                .await
                .values()
                .all(|job| {
                    job.handle
                        .as_ref()
                        .is_none_or(tokio::task::JoinHandle::is_finished)
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("foreground must join the cancelled root owner, not bypass its physical key");
    let old = check::Entity::find_by_id("priority-old-answer")
        .one(&harness.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.outcome.as_deref(), Some("cancelled"));
    let tasks = harness
        .crud_store
        .list_tasks_created_by_turn(
            &harness.workspace_id,
            "priority-composer-root",
            "priority-new-question",
        )
        .await
        .unwrap();
    let composer = tasks.first().unwrap();
    assert_eq!(
        wait_for_task_status(
            harness.crud_store.clone(),
            &composer.id,
            TaskStatus::Completed
        )
        .await,
        TaskStatus::Completed
    );
    let state = harness
        .crud_store
        .get_task(&composer.id)
        .await
        .unwrap()
        .unwrap();
    let lineage =
        wait_for_child_lineage_for_run(harness.crud_store.clone(), &state.runs.last().unwrap().id)
            .await;
    assert_ne!(lineage.child_thread_id, "priority-composer-root");
    harness.processor.suspend_completed_history_checks().await;
}
