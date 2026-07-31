use super::events::EventKind;
use super::reducer::{ConversationProjector, ConversationViewState};
use super::{Conversation, ConversationEvent, MAX_EVENT_LOG_LEN, TimelineEntryStatus, TurnPhase};
use pioneer_protocol::{
    ArtifactKind, ArtifactRef, ArtifactStatus, ExecutionWindowExhaustionReason,
    ExecutionWindowStatus, ItemDeltaStream, RecoveryAction, RecoveryJobStatus, RecoveryTrigger,
    SkillId, SkillPackId, SystemEventLevel, Thread, ThreadHistoryEvent, ThreadHistoryEventPayload,
    ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, ToolCallStatus,
    ToolDisplayPayload, ToolLoopBudgetAction, ToolLoopBudgetLimitKind, ToolMetadata,
    ToolOutputPolicySnapshot, ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot,
    ToolRecoveryRetryClass, ToolRetryBudgetKind, ToolRetryBudgetUsage, ToolRetryErrorClass,
    ToolRetryExhaustionKind, ToolRetryResolution, ToolStoragePayload, Turn,
    TurnBlockedResumeMetadata, TurnExecutionWindowBlockedNotification,
    TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification, TurnItem,
    TurnItemTimeoutReason, TurnItemType, TurnKind, TurnOrigin, TurnPermissionActionKind,
    TurnPermissionAuditDecision, TurnPermissionAuditEvent, TurnPermissionAuditEventKind,
    TurnPermissionAuditRequestKey, TurnPermissionDecisionReason, TurnPermissionMode,
    TurnPermissionProfileSource, TurnSkillCapabilitySummary, TurnSkillPackCapabilitySummary,
    TurnSkillPackPresentationSummary, TurnStatus, UserInput, UserMessageAttachment,
};

const THREAD_ID: &str = "thr_000000000000000001";
const TURN_ID: &str = "turn_000000000000000001";
const PENDING_REQUEST_ID: &str = "req_000000000000000001";
const WORKSPACE_ID: &str = "ws_000000000000000001";

fn default_test_permission_profile() -> pioneer_protocol::TurnPermissionProfileSnapshot {
    pioneer_protocol::default_turn_permission_profile_snapshot()
}

fn thread_snapshot_with_turn(turn: Turn) -> Thread {
    Thread {
        workspace_id: WORKSPACE_ID.to_owned(),
        id: THREAD_ID.to_owned(),
        name: None,
        preview: String::new(),
        mode: ThreadMode::Chat,
        model: "gpt-5".to_owned(),
        model_provider: "openai".to_owned(),
        reasoning_effort: None,
        created_at: 1,
        updated_at: 2,
        status: ThreadStatus::Active,
        origin_kind: ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: vec![turn],
    }
}

fn turn_snapshot(id: &str, status: TurnStatus, error: Option<String>) -> Turn {
    Turn {
        id: id.to_owned(),
        status,
        turn_kind: Default::default(),
        origin: Default::default(),
        error,
        prompt_manifest: None,
        permission_profile: default_test_permission_profile(),
    }
}

fn pending_request_id(conversation: &Conversation) -> Option<&str> {
    conversation.pending_request_id()
}

fn apply_in_progress_turn(conversation: &mut Conversation) {
    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: default_test_permission_profile(),
        },
    });
}

#[test]
fn thread_snapshot_hydration_locks_composer_for_running_turn() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.sync_thread_snapshot(&thread_snapshot_with_turn(turn_snapshot(
        TURN_ID,
        TurnStatus::InProgress,
        None,
    )));

    assert!(!conversation.can_submit_message());
    assert_eq!(conversation.in_flight_turn_id(), Some(TURN_ID));
    assert!(conversation.projection().composer_locked);
    assert_eq!(
        conversation.projection().in_flight_turn_id.as_deref(),
        Some(TURN_ID)
    );
    assert_eq!(conversation.projection().phase_label, "running");
}

#[test]
fn task_run_occurrence_does_not_claim_foreground_on_notification_or_reload() {
    let mut conversation = Conversation::new(THREAD_ID);
    let task_turn_id = "run_0000000000000000001";
    let task_turn = Turn {
        id: task_turn_id.to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: TurnKind::TaskRun,
        origin: TurnOrigin::DetachedTask,
        error: None,
        prompt_manifest: None,
        permission_profile: default_test_permission_profile(),
    };

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: task_turn.clone(),
    });

    assert!(conversation.can_submit_message());
    assert_eq!(conversation.in_flight_turn_id(), None);
    assert!(!conversation.projection().composer_locked);

    let foreground_turn_id = "turn_000000000000000002";
    let foreground_turn = turn_snapshot(foreground_turn_id, TurnStatus::InProgress, None);
    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: foreground_turn.clone(),
    });
    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            status: TurnStatus::Completed,
            ..task_turn.clone()
        },
    });

    assert_eq!(
        conversation.in_flight_turn_id(),
        Some(foreground_turn_id),
        "task completion must not complete the concurrent foreground turn"
    );
    assert!(conversation.projection().composer_locked);

    let mut reloaded = Conversation::new(THREAD_ID);
    let mut snapshot = thread_snapshot_with_turn(foreground_turn);
    snapshot.turns.push(task_turn);
    reloaded.sync_thread_snapshot(&snapshot);

    assert_eq!(
        reloaded.in_flight_turn_id(),
        Some(foreground_turn_id),
        "reload must select the latest conversation turn, not the task occurrence"
    );
    assert!(reloaded.projection().composer_locked);
}

#[test]
fn stale_terminal_thread_snapshot_does_not_unlock_local_start() {
    let mut conversation = Conversation::new(THREAD_ID);
    let local_turn_id = "turn_000000000000000002";

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: local_turn_id.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "hello".to_owned(),
        attachments: Vec::new(),
    });

    conversation.sync_thread_snapshot(&thread_snapshot_with_turn(turn_snapshot(
        TURN_ID,
        TurnStatus::Completed,
        None,
    )));

    assert!(!conversation.can_submit_message());
    assert_eq!(conversation.in_flight_turn_id(), Some(local_turn_id));
    assert!(conversation.projection().composer_locked);
    assert_eq!(conversation.projection().phase_label, "starting");
}

fn permission_audit_event(
    event_kind: TurnPermissionAuditEventKind,
    decision: Option<TurnPermissionAuditDecision>,
) -> TurnPermissionAuditEvent {
    TurnPermissionAuditEvent {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        event_kind,
        profile_mode: TurnPermissionMode::Supervised,
        profile_source: TurnPermissionProfileSource::Composer,
        security_snapshot_id: None,
        security_snapshot_version: None,
        security_reason_code: None,
        security_capability: None,
        item_id: Some("item_tool".to_owned()),
        tool_call_id: Some("tool_call_1".to_owned()),
        tool_name: Some("shell".to_owned()),
        action_kind: Some(TurnPermissionActionKind::ShellCommand),
        request_key: Some(TurnPermissionAuditRequestKey {
            action_kind: TurnPermissionActionKind::ShellCommand,
            scope_hash: "scope_hash_only".to_owned(),
        }),
        decision,
        reason: Some(TurnPermissionDecisionReason::PolicyRequiresApproval),
        cached: false,
    }
}

#[test]
fn turn_started_projection_preserves_permission_profile() {
    let mut conversation = Conversation::new(THREAD_ID);
    let permission_profile = pioneer_protocol::compile_turn_permission_profile(
        TurnPermissionMode::Supervised,
        TurnPermissionProfileSource::Composer,
    );

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: permission_profile.clone(),
        },
    });

    assert_eq!(
        conversation.projection().turn_permission_profile(TURN_ID),
        Some(&permission_profile)
    );
}

#[test]
fn turn_projection_stores_security_summary_without_provider_strings() {
    let mut projection = ConversationViewState::default();
    let snapshot =
        pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access("/repo", 1_700);
    let summary = crate::security::ClientTurnSecuritySummary::from_execution_snapshot(&snapshot);

    projection.upsert_turn_security_summary(TURN_ID, summary.clone());

    assert_eq!(projection.turn_security_summary(TURN_ID), Some(&summary));
    let encoded = serde_json::to_string(projection.turn_security_summary(TURN_ID).unwrap())
        .expect("security summary serializes");
    assert!(!encoded.contains("danger"));
    assert!(!encoded.contains("bypass"));
}

#[test]
fn permission_audit_projection_stores_sanitized_decision_history() {
    let mut conversation = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut conversation);

    let event = permission_audit_event(
        TurnPermissionAuditEventKind::ApprovalRequested,
        Some(TurnPermissionAuditDecision::Ask),
    );
    conversation.apply(ConversationEvent::TurnPermissionAudit { event });

    let audit = conversation.projection().permission_audit_for_turn(TURN_ID);
    assert_eq!(audit.len(), 1);
    assert_eq!(
        audit[0].request_scope_hash.as_deref(),
        Some("scope_hash_only")
    );
    assert_eq!(audit[0].tool_name.as_deref(), Some("shell"));
    assert_eq!(
        conversation
            .projection()
            .permission_decision_history_for_turn(TURN_ID)
            .len(),
        1
    );

    let (_, details) = system_event_details(&conversation, "turn_permission_audit");
    let details_text = details.to_string();
    assert!(details_text.contains("scope_hash_only"));
    assert!(details_text.contains("policy_requires_approval"));
}

#[test]
fn permission_audit_history_hydrates_shared_projection_and_timeline_row() {
    let mut conversation = Conversation::new(THREAD_ID);
    let events = vec![
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: WORKSPACE_ID.to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,
                    prompt_manifest: None,
                    permission_profile: default_test_permission_profile(),
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 2_000,
            payload: ThreadHistoryEventPayload::TurnPermissionAudit(permission_audit_event(
                TurnPermissionAuditEventKind::ApprovalResolved,
                Some(TurnPermissionAuditDecision::AllowOnce),
            )),
        },
    ];

    conversation.hydrate_history(&events);

    let audit = conversation.projection().permission_audit_for_turn(TURN_ID);
    assert_eq!(audit.len(), 1);
    assert_eq!(
        audit[0].decision,
        Some(TurnPermissionAuditDecision::AllowOnce)
    );
    assert!(conversation.projection().items.iter().any(|item| matches!(
        &item.item,
        TurnItem::SystemEvent {
            code: Some(code),
            details: Some(details),
            ..
        } if code == "turn_permission_audit"
            && details["decision"] == "allow_once"
            && details.get("request_scope_hash").is_some()
    )));
}

fn system_event_details<'a>(
    conversation: &'a Conversation,
    code: &str,
) -> (&'a str, &'a serde_json::Value) {
    system_event_details_optional(conversation, code)
        .unwrap_or_else(|| panic!("expected system event with code {code}"))
}

fn system_event_details_optional<'a>(
    conversation: &'a Conversation,
    code: &str,
) -> Option<(&'a str, &'a serde_json::Value)> {
    conversation
        .projection()
        .items
        .iter()
        .find_map(|item| match &item.item {
            TurnItem::SystemEvent {
                message,
                code: Some(existing_code),
                details: Some(details),
                ..
            } if existing_code == code => Some((message.as_str(), details)),
            _ => None,
        })
}

fn window_started_notification(
    window_id: &str,
    window_index: u32,
) -> TurnExecutionWindowStartedNotification {
    TurnExecutionWindowStartedNotification {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        window_id: window_id.to_owned(),
        window_index,
        status: ExecutionWindowStatus::Running,
        started_at_unix_ms: 1_000,
    }
}

fn window_exhausted_notification(
    window_id: &str,
    window_index: u32,
) -> TurnExecutionWindowExhaustedNotification {
    TurnExecutionWindowExhaustedNotification {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        window_id: window_id.to_owned(),
        window_index,
        status: ExecutionWindowStatus::Exhausted,
        exhaustion_reason: ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
        limit: 128,
        observed: 129,
        agent_round_count: 64,
        tool_call_count: 129,
        provider_token_count: Some(42_000),
        started_at_unix_ms: 1_000,
        exhausted_at_unix_ms: 2_000,
        reason: "max_tool_calls_per_window".to_owned(),
    }
}

fn window_checkpointed_notification(
    window_id: &str,
    window_index: u32,
) -> TurnExecutionWindowCheckpointedNotification {
    TurnExecutionWindowCheckpointedNotification {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        window_id: window_id.to_owned(),
        window_index,
        status: ExecutionWindowStatus::Checkpointed,
        checkpoint_id: "chk_000000000000000001".to_owned(),
        checkpoint_kind: "execution_window_budget".to_owned(),
        payload_bytes: 4096,
        created_at_unix_ms: 2_100,
    }
}

fn window_continued_notification(
    window_id: &str,
    window_index: u32,
) -> TurnExecutionWindowContinuedNotification {
    TurnExecutionWindowContinuedNotification {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        window_id: window_id.to_owned(),
        window_index,
        status: ExecutionWindowStatus::Continued,
        previous_window_id: "win_000000000000000001".to_owned(),
        previous_window_index: 1,
        checkpoint_id: "chk_000000000000000001".to_owned(),
        continued_at_unix_ms: 2_200,
    }
}

fn window_blocked_notification() -> TurnExecutionWindowBlockedNotification {
    TurnExecutionWindowBlockedNotification {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        window_id: "win_000000000000000003".to_owned(),
        window_index: 3,
        status: ExecutionWindowStatus::Blocked,
        exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow),
        checkpoint_id: Some("chk_000000000000000003".to_owned()),
        total_windows: 3,
        total_tool_calls: 384,
        reason: "max_total_windows_exceeded".to_owned(),
        blocked_at_unix_ms: 3_000,
    }
}

fn sample_tool_recovery_policy() -> ToolRecoveryPolicySnapshot {
    ToolRecoveryPolicySnapshot {
        retry_class: ToolRecoveryRetryClass::Network,
        idempotency_mode: ToolRecoveryIdempotencyMode::Safe,
        max_attempts: 3,
        can_resume: true,
        resolved_action: RecoveryAction::RetryWithBackoff,
        base_backoff_secs: 3,
        max_wall_clock_secs: 240,
        no_progress_limit: 3,
    }
}

fn sample_web_fetch_item(
    item_id: &str,
    status: pioneer_protocol::ToolCallStatus,
    recovery_policy: Option<ToolRecoveryPolicySnapshot>,
) -> TurnItem {
    TurnItem::WebFetch {
        id: item_id.to_owned(),
        tool_name: "web_fetch".to_owned(),
        arguments: serde_json::json!({ "url": "https://example.com/policy" }),
        status,
        recovery_policy,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
        display: ToolDisplayPayload::Hidden,
        storage: ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(serde_json::json!({
                "url": "https://example.com/policy"
            })),
        },
        recovery: None,
        url: Some("https://example.com/policy".to_owned()),
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
    }
}

#[test]
fn send_stays_blocked_until_terminal_event() {
    let mut conversation = Conversation::new(THREAD_ID);

    assert!(conversation.can_submit_message());

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "hello".to_owned(),
        attachments: Vec::new(),
    });
    assert!(!conversation.can_submit_message());
    assert_eq!(pending_request_id(&conversation), Some(PENDING_REQUEST_ID));

    conversation.apply(ConversationEvent::LocalTurnStartAccepted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
    });
    assert!(!conversation.can_submit_message());

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });
    assert!(!conversation.can_submit_message());

    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });

    assert!(!conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "completing");
    assert!(conversation.tick());
    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "completed");
}

#[test]
fn user_message_stays_before_work_items_even_if_it_arrives_late() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: "item_reasoning".to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        },
    });

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::UserMessage {
            id: "item_user".to_owned(),
            text: "what car is this?".to_owned(),
            attachments: Vec::new(),
        },
    });

    let item_ids = conversation
        .projection()
        .timeline
        .iter()
        .map(|entry| entry.item_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(item_ids, vec!["item_user", "item_reasoning"]);
}

#[test]
fn agent_review_system_event_projects_to_completed_timeline_row() {
    let mut conversation = Conversation::new(THREAD_ID);
    let started_item = TurnItem::SystemEvent {
        id: "agent_review_native_review_turn_1".to_owned(),
        level: SystemEventLevel::Info,
        message: "Review started".to_owned(),
        code: Some("agent_review".to_owned()),
        details: Some(serde_json::json!({
            "status": "entered",
            "nativeTurnId": "native_review_turn_1"
        })),
    };
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: started_item,
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::SystemEvent {
            id: "agent_review_native_review_turn_1".to_owned(),
            level: SystemEventLevel::Info,
            message: "Review completed".to_owned(),
            code: Some("agent_review".to_owned()),
            details: Some(serde_json::json!({
                "status": "completed",
                "nativeTurnId": "native_review_turn_1"
            })),
        },
    });

    let entry = conversation
        .projection()
        .timeline
        .first()
        .expect("review system event should create a timeline row");
    let item = conversation
        .projection()
        .item_for_timeline_entry(entry)
        .expect("review timeline item should resolve");
    assert_eq!(item.status, TimelineEntryStatus::Completed);
    assert_eq!(item.partial_text, "Review completed");
    let (message, details) = system_event_details(&conversation, "agent_review");
    assert_eq!(message, "Review completed");
    assert_eq!(details.get("status"), Some(&serde_json::json!("completed")));
}

#[test]
fn agent_compaction_system_event_projects_to_completed_timeline_row() {
    let mut conversation = Conversation::new(THREAD_ID);
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::SystemEvent {
            id: "native_compaction_1".to_owned(),
            level: SystemEventLevel::Info,
            message: "Context compaction started".to_owned(),
            code: Some("agent_context_compaction".to_owned()),
            details: Some(serde_json::json!({
                "status": "started",
                "nativeItemKind": "contextCompaction"
            })),
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::SystemEvent {
            id: "native_compaction_1".to_owned(),
            level: SystemEventLevel::Info,
            message: "Context compaction completed".to_owned(),
            code: Some("agent_context_compaction".to_owned()),
            details: Some(serde_json::json!({
                "status": "completed",
                "nativeItemKind": "contextCompaction"
            })),
        },
    });

    let entry = conversation
        .projection()
        .timeline
        .first()
        .expect("compaction system event should create a timeline row");
    let item = conversation
        .projection()
        .item_for_timeline_entry(entry)
        .expect("compaction timeline item should resolve");
    assert_eq!(item.status, TimelineEntryStatus::Completed);
    assert_eq!(item.partial_text, "Context compaction completed");
    let (message, details) = system_event_details(&conversation, "agent_context_compaction");
    assert_eq!(message, "Context compaction completed");
    assert_eq!(details.get("status"), Some(&serde_json::json!("completed")));
}

#[test]
fn local_turn_start_projects_optimistic_user_message_with_artifacts() {
    let mut conversation = Conversation::new(THREAD_ID);
    let artifact = ArtifactRef {
        artifact_id: "art_000000000000000001".to_owned(),
        version_id: Some("ver_000000000000000001".to_owned()),
        display_name: "photo.webp".to_owned(),
        kind: ArtifactKind::Image,
        mime_type: Some("image/webp".to_owned()),
        size_bytes: Some(1234),
        sha256: Some("sha256".to_owned()),
        status: ArtifactStatus::Ready,
        preview: None,
    };

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "what is this?".to_owned(),
        attachments: vec![UserMessageAttachment::Artifact {
            artifact: artifact.clone(),
        }],
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|item| matches!(item.item, TurnItem::UserMessage { .. }))
        .expect("optimistic user message");
    assert_eq!(item.id, format!("user_{TURN_ID}"));
    assert_eq!(item.status, TimelineEntryStatus::Completed);
    let TurnItem::UserMessage {
        text, attachments, ..
    } = &item.item
    else {
        panic!("expected user message");
    };
    assert_eq!(text, "what is this?");
    assert_eq!(
        attachments,
        &vec![UserMessageAttachment::Artifact { artifact }]
    );
}

#[test]
fn durable_pack_attachments_replace_optimistic_message_without_duplicates() {
    let mut conversation = Conversation::new(THREAD_ID);
    let pack_id = SkillPackId::new("P".repeat(21)).expect("pack id");
    let attachments = vec![
        UserMessageAttachment::SkillPack {
            capability: TurnSkillPackCapabilitySummary {
                pack_id: pack_id.clone(),
                label: "Research Pack".to_owned(),
            },
        },
        UserMessageAttachment::Skill {
            capability: TurnSkillCapabilitySummary {
                skill_id: SkillId::new("S".repeat(21)).expect("skill id"),
                label: "Search".to_owned(),
                owner: None,
                slug: "search".to_owned(),
                source_kind: "user".to_owned(),
                pack: Some(TurnSkillPackPresentationSummary {
                    pack_id,
                    label: "Research Pack".to_owned(),
                }),
            },
        },
    ];

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "research this".to_owned(),
        attachments: attachments.clone(),
    });
    let durable_item = TurnItem::UserMessage {
        id: format!("user_{TURN_ID}"),
        text: "research this".to_owned(),
        attachments: attachments.clone(),
    };
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: durable_item.clone(),
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: durable_item,
    });

    let user_messages = conversation
        .projection()
        .items
        .iter()
        .filter(|item| matches!(item.item, TurnItem::UserMessage { .. }))
        .collect::<Vec<_>>();
    assert_eq!(user_messages.len(), 1);
    assert!(matches!(
        &user_messages[0].item,
        TurnItem::UserMessage {
            attachments: actual,
            ..
        } if actual == &attachments
    ));
}

#[test]
fn send_unlocks_only_on_terminal_failed_or_cancelled() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "hello".to_owned(),
        attachments: Vec::new(),
    });

    conversation.apply(ConversationEvent::TurnFailed {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Failed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: Some("network".to_owned()),

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });

    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "failed");

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: "turn_000000000000000002".to_owned(),
        pending_request_id: "req_000000000000000002".to_owned(),
        user_text: "again".to_owned(),
        attachments: Vec::new(),
    });
    assert!(!conversation.can_submit_message());

    conversation.apply(ConversationEvent::TurnFailed {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: "turn_000000000000000002".to_owned(),
            status: TurnStatus::Interrupted,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: Some("cancelled".to_owned()),

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });

    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "cancelled");
    let current_turn = conversation
        .projection()
        .turns
        .iter()
        .find(|turn| turn.id == "turn_000000000000000002")
        .expect("turn should exist");
    assert_eq!(current_turn.phase, TurnPhase::Cancelled);
}

#[test]
fn cancel_request_locks_until_rejected_or_interrupted() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "hello".to_owned(),
        attachments: Vec::new(),
    });
    conversation.apply(ConversationEvent::LocalTurnStartAccepted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
    });

    conversation.apply(ConversationEvent::LocalTurnCancelRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
    });

    assert!(!conversation.can_submit_message());
    assert!(conversation.is_cancelling_turn());
    assert_eq!(conversation.in_flight_turn_id(), Some(TURN_ID));
    assert!(conversation.projection().composer_locked);
    assert_eq!(conversation.projection().phase_label, "cancelling");

    conversation.apply(ConversationEvent::LocalTurnCancelRejected {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        error: "gateway unavailable".to_owned(),
    });

    assert!(!conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "running");
    assert_eq!(
        conversation.projection().last_error.as_deref(),
        Some("gateway unavailable")
    );

    conversation.apply(ConversationEvent::LocalTurnCancelRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
    });
    conversation.apply(ConversationEvent::TurnFailed {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Interrupted,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: Some("stopped by user".to_owned()),

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });
    conversation.apply(ConversationEvent::LocalTurnCancelRejected {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        error: "late error".to_owned(),
    });

    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "cancelled");
    assert_eq!(conversation.in_flight_turn_id(), None);
}

#[test]
fn item_delta_streams_text_until_completion() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "item_0000000000000000001".to_owned(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_0000000000000000001".to_owned(),
        delta: "hel".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_0000000000000000001".to_owned(),
        delta: "lo".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    let streaming_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_0000000000000000001")
        .expect("item should exist");
    assert_eq!(streaming_item.partial_text, "hello");
    assert_eq!(streaming_item.status, TimelineEntryStatus::Running);

    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "item_0000000000000000001".to_owned(),
            text: "hello".to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });

    let completed_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_0000000000000000001")
        .expect("item should exist");
    assert_eq!(completed_item.final_text.as_deref(), Some("hello"));
    assert_eq!(completed_item.status, TimelineEntryStatus::Completed);
}

#[test]
fn conversation_codex_reasoning_summary_does_not_overwrite_agent_message_text() {
    let mut conversation = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut conversation);

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: "codex_reasoning_item".to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        },
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "codex_reasoning_item".to_owned(),
        delta: "Reasoning summary".to_owned(),
        stream: Some(ItemDeltaStream::Generic),
        payload: Some(serde_json::json!({
            "runtimeDeltaKind": "reasoning_summary",
            "nativeItemId": "codex_reasoning_item"
        })),
        markdown: None,
        markdown_version: None,
    });
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "codex_agent_item".to_owned(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "codex_agent_item".to_owned(),
        delta: "Final answer".to_owned(),
        stream: Some(ItemDeltaStream::AgentMessage),
        payload: Some(serde_json::json!({
            "runtimeDeltaKind": "agent_message",
            "nativeItemId": "codex_agent_item"
        })),
        markdown: None,
        markdown_version: None,
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: "codex_reasoning_item".to_owned(),
            summary: vec!["Reasoning summary".to_owned()],
            content: Vec::new(),
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "codex_agent_item".to_owned(),
            text: "Final answer".to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: default_test_permission_profile(),
        },
    });

    let agent_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "codex_agent_item")
        .expect("agent message item should exist");
    assert_eq!(agent_item.partial_text, "Final answer");
    assert_eq!(agent_item.final_text.as_deref(), Some("Final answer"));
    assert_eq!(agent_item.status, TimelineEntryStatus::Completed);

    let reasoning_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "codex_reasoning_item")
        .expect("reasoning item should exist");
    assert_eq!(reasoning_item.partial_text, "Reasoning summary");
    assert!(!reasoning_item.partial_text.contains("Final answer"));
    assert_eq!(reasoning_item.status, TimelineEntryStatus::Completed);
    assert!(conversation.tick());
    assert_eq!(conversation.status_label(), "completed");
}

#[test]
fn conversation_codex_reasoning_delta_starts_missing_reasoning_item() {
    let mut conversation = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut conversation);

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "late_reasoning_item".to_owned(),
        delta: "Late reasoning summary".to_owned(),
        stream: Some(ItemDeltaStream::Generic),
        payload: Some(serde_json::json!({
            "runtimeDeltaKind": "reasoning_summary",
            "nativeItemKind": "reasoning",
            "nativeItemId": "late_reasoning_item"
        })),
        markdown: None,
        markdown_version: None,
    });

    let reasoning_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "late_reasoning_item")
        .expect("late reasoning delta should create a reasoning item");
    assert_eq!(reasoning_item.item_type, "reasoning");
    assert_eq!(reasoning_item.partial_text, "Late reasoning summary");
    assert_eq!(reasoning_item.status, TimelineEntryStatus::Running);

    let reasoning_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "late_reasoning_item")
        .expect("late reasoning item should be visible in timeline");
    assert_eq!(reasoning_entry.turn_id, TURN_ID);
}

#[test]
fn timeline_codex_tool_output_and_file_change_rows_render_existing_tool_items() {
    let mut conversation = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut conversation);

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::CommandExecution {
            id: "codex_cmd_item".to_owned(),
            tool_name: "exec_command".to_owned(),
            arguments: serde_json::json!({"nativeItemId": "codex_cmd_item"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
            display: ToolDisplayPayload::default(),
            storage: ToolStoragePayload::default(),
            recovery: None,
            command: vec!["echo".to_owned(), "hello".to_owned()],
            cwd: Some("/repo".to_owned()),
            success: None,
            outcome: None,
            observation: None,
        },
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "codex_cmd_item".to_owned(),
        delta: "hello\n".to_owned(),
        stream: Some(ItemDeltaStream::Stdout),
        payload: Some(serde_json::json!({"nativeItemId": "codex_cmd_item"})),
        markdown: None,
        markdown_version: None,
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "codex_cmd_item".to_owned(),
        delta: "warn\n".to_owned(),
        stream: Some(ItemDeltaStream::Stderr),
        payload: Some(serde_json::json!({"nativeItemId": "codex_cmd_item"})),
        markdown: None,
        markdown_version: None,
    });

    let command_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "codex_cmd_item")
        .expect("command item should exist");
    assert!(command_item.partial_text.contains("Command: echo hello"));
    assert!(command_item.partial_text.contains("hello"));
    assert!(command_item.partial_text.contains("[Stderr] warn"));

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::FileChange {
            id: "codex_file_item".to_owned(),
            tool_name: "apply_patch".to_owned(),
            arguments: serde_json::json!({"nativeItemId": "codex_file_item"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("apply_patch"),
            display: ToolDisplayPayload::default(),
            storage: ToolStoragePayload::default(),
            recovery: None,
            changed_files: vec!["src/lib.rs".to_owned()],
            exit_code: None,
            stdout: None,
            stderr: None,
            success: None,
            outcome: None,
            observation: None,
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::FileChange {
            id: "codex_file_item".to_owned(),
            tool_name: "apply_patch".to_owned(),
            arguments: serde_json::json!({"nativeItemId": "codex_file_item"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("apply_patch"),
            display: ToolDisplayPayload::default(),
            storage: ToolStoragePayload::default(),
            recovery: None,
            changed_files: vec!["src/lib.rs".to_owned()],
            exit_code: Some(0),
            stdout: None,
            stderr: None,
            success: Some(true),
            outcome: None,
            observation: None,
        },
    });

    let file_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "codex_file_item")
        .expect("file change item should exist");
    assert_eq!(file_item.status, TimelineEntryStatus::Completed);
    assert!(file_item.partial_text.contains("Changed files:"));
    assert!(file_item.partial_text.contains("- src/lib.rs"));
}

#[test]
fn item_delta_without_started_item_does_not_create_orphan_row() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "call_partial_task_tool_name".to_owned(),
        delta: "tas".to_owned(),
        stream: Some(ItemDeltaStream::ToolProgress),
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    assert!(
        conversation.projection().items.is_empty(),
        "progress for an unknown durable item must not create a visible orphan row"
    );
    assert!(
        conversation.projection().timeline.is_empty(),
        "unknown progress must not append a timeline entry"
    );
}

#[test]
fn late_item_delta_does_not_reopen_completed_item() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_late_delta_after_completion";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: "done".to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        delta: " late".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == item_id)
        .expect("item should exist");
    assert_eq!(item.status, TimelineEntryStatus::Completed);
    assert!(item.completed_at_unix_ms.is_some());
    assert!(item.partial_text.contains("late"));
}

#[test]
fn late_item_delta_does_not_move_terminal_item_clock() {
    let mut projector = ConversationProjector::default();
    let item_id = "item_terminal_clock";

    projector.start_item_view(
        item_id,
        TURN_ID,
        "agent_message",
        TimelineEntryStatus::Running,
        String::new(),
        None,
        TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
        None,
        1_000,
    );
    projector.complete_item_view(
        item_id,
        TimelineEntryStatus::Completed,
        Some("done"),
        None,
        TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: "done".to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
        None,
        2_000,
    );
    projector.append_item_delta(item_id, " late", None, 9_000);

    let item = projector
        .view_state()
        .items
        .iter()
        .find(|item| item.id == item_id)
        .expect("item should exist");
    assert_eq!(item.status, TimelineEntryStatus::Completed);
    assert_eq!(item.completed_at_unix_ms, Some(2_000));
    assert_eq!(
        item.updated_at_unix_ms,
        Some(2_000),
        "late progress must not move the terminal clock"
    );
    assert!(item.partial_text.contains("late"));
}

#[test]
fn later_item_starts_do_not_move_turn_start_clock() {
    let mut projector = ConversationProjector::default();

    projector.apply_local_turn_start_requested(TURN_ID, "hello", &[], 1_000);
    projector.start_item_view(
        "reasoning_1",
        TURN_ID,
        "reasoning",
        TimelineEntryStatus::Running,
        String::new(),
        None,
        TurnItem::Reasoning {
            id: "reasoning_1".to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        },
        None,
        5_000,
    );
    projector.start_item_view(
        "command_1",
        TURN_ID,
        "command_execution",
        TimelineEntryStatus::Running,
        String::new(),
        None,
        TurnItem::CommandExecution {
            id: "command_1".to_owned(),
            tool_name: "exec_command".to_owned(),
            arguments: serde_json::json!({"cmd": "pwd"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
            display: ToolDisplayPayload::default(),
            storage: ToolStoragePayload::default(),
            recovery: None,
            command: vec!["pwd".to_owned()],
            cwd: None,
            success: None,
            outcome: None,
            observation: None,
        },
        None,
        9_000,
    );

    let turn = projector
        .view_state()
        .turns
        .iter()
        .find(|turn| turn.id == TURN_ID)
        .expect("turn should exist");
    assert_eq!(
        turn.started_at_unix_ms,
        Some(1_000),
        "turn elapsed clock must stay anchored to turn start, not latest item start"
    );
}

#[test]
fn terminal_turn_stamps_running_items_completed_at() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_running_when_turn_completes";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: item_id.to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        },
    });
    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: default_test_permission_profile(),
        },
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == item_id)
        .expect("item should exist");
    assert_eq!(item.status, TimelineEntryStatus::Completed);
    assert!(
        item.completed_at_unix_ms.is_some(),
        "terminal turn must freeze elapsed time for running items"
    );
}

#[test]
fn late_item_started_does_not_reopen_completed_item() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_late_start_after_completion";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: "done".to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == item_id)
        .expect("item should exist");
    assert_eq!(item.status, TimelineEntryStatus::Completed);
    assert_eq!(item.final_text.as_deref(), Some("done"));
    assert_eq!(item.partial_text, "done");
}

#[test]
fn web_fetch_delta_hides_raw_content_but_keeps_progress_updates() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_web_fetch_1";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({ "url": "https://example.com" }),
            status: pioneer_protocol::ToolCallStatus::InProgress,
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
        },
    });

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        delta: "RAW_FETCHED_CONTENT_SHOULD_NOT_BE_VISIBLE".to_owned(),
        stream: Some(ItemDeltaStream::Generic),
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        delta: "fetching page metadata".to_owned(),
        stream: Some(ItemDeltaStream::ToolProgress),
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("web_fetch item should exist");
    assert!(
        !item
            .partial_text
            .contains("RAW_FETCHED_CONTENT_SHOULD_NOT_BE_VISIBLE")
    );
    assert!(
        item.partial_text
            .contains("[Progress] fetching page metadata")
    );
}

#[test]
fn web_fetch_completion_uses_summary_instead_of_output_blob() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_web_fetch_2";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({ "url": "https://example.com/page" }),
            status: pioneer_protocol::ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com/page"
                })),
            },
            recovery: None,
            url: Some("https://example.com/page".to_owned()),
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
        },
    });

    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({ "url": "https://example.com/page" }),
            status: pioneer_protocol::ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com/page",
                    "statusCode": 200
                })),
            },
            recovery: None,
            url: Some("https://example.com/page".to_owned()),
            final_url: Some("https://example.com/page".to_owned()),
            status_code: Some(200),
            content_type: Some("text/html".to_owned()),
            extract_mode: Some("readable".to_owned()),
            resolved_mode: Some("readable".to_owned()),
            bytes_received: Some(12345),
            elapsed_ms: Some(321),
            truncated: None,
            title: Some("Example Page".to_owned()),
            word_count: Some(555),
            links: Vec::new(),
            success: Some(true),
            outcome: None,
            observation: None,
        },
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("web_fetch item should exist");
    let final_text = item
        .final_text
        .as_deref()
        .expect("completed web_fetch should have final_text");
    assert!(final_text.contains("Tool `web_fetch` finished"));
    assert!(final_text.contains("URL: https://example.com/page"));
    assert!(!final_text.contains("VERY_LONG_OUTPUT_SHOULD_NOT_BE_VISIBLE"));
    assert!(!final_text.contains("VERY_LONG_CONTENT_SHOULD_NOT_BE_VISIBLE"));
}

#[test]
fn tool_item_projection_preserves_recovery_policy_without_timeline_noise() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_web_fetch_policy";
    let recovery_policy = sample_tool_recovery_policy();

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({ "url": "https://example.com/policy" }),
            status: pioneer_protocol::ToolCallStatus::InProgress,
            recovery_policy: Some(recovery_policy.clone()),
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com/policy"
                })),
            },
            recovery: None,
            url: Some("https://example.com/policy".to_owned()),
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
        },
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("web_fetch item should exist");
    assert_eq!(item.item.recovery_policy(), Some(&recovery_policy));
    assert!(!item.partial_text.contains("max_attempts"));
    assert!(!item.partial_text.contains("retry_with_backoff"));
}

#[test]
fn history_hydration_preserves_tool_recovery_policy_snapshot() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_history_policy";
    let recovery_policy = sample_tool_recovery_policy();

    conversation.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,
                    prompt_manifest: None,
                    permission_profile: default_test_permission_profile(),
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: sample_web_fetch_item(
                    item_id,
                    pioneer_protocol::ToolCallStatus::InProgress,
                    Some(recovery_policy.clone()),
                ),
            },
        },
    ]);

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("hydrated web_fetch item should exist");
    assert_eq!(item.item.recovery_policy(), Some(&recovery_policy));
}

#[test]
fn history_hydration_keeps_dynamic_model_only_body_out_of_desktop_state() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_dynamic_http_history";
    let dynamic_item = TurnItem::DynamicToolCall {
        id: item_id.to_owned(),
        tool_name: "skill.tests-http-skill.fetch-secret".to_owned(),
        arguments: serde_json::json!({}),
        status: pioneer_protocol::ToolCallStatus::Completed,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("dynamic_tool"),
        display: ToolDisplayPayload::Hidden,
        storage: ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(serde_json::json!({
                "url": "http://127.0.0.1/dynamic-secret",
                "statusCode": 200,
                "bodyHash": "sha256:test",
                "bodyBytes": 33
            })),
        },
        recovery: None,
        success: Some(true),
        outcome: None,
        observation: None,
    };

    conversation.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,
                    prompt_manifest: None,
                    permission_profile: default_test_permission_profile(),
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::DynamicToolCall {
                    id: item_id.to_owned(),
                    tool_name: "skill.tests-http-skill.fetch-secret".to_owned(),
                    arguments: serde_json::json!({}),
                    status: pioneer_protocol::ToolCallStatus::InProgress,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("dynamic_tool"),
                    display: ToolDisplayPayload::Hidden,
                    storage: ToolStoragePayload::Metadata {
                        metadata: ToolMetadata::from_json(serde_json::json!({"state": "running"})),
                    },
                    recovery: None,
                    success: None,
                    outcome: None,
                    observation: None,
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: dynamic_item,
            },
        },
    ]);

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("hydrated dynamic item should exist");
    let desktop_state = format!("{:?}", item);
    assert!(!desktop_state.contains("SECRET_DYNAMIC_HTTP_BODY_SENTINEL"));
    assert!(desktop_state.contains("bodyHash"));
}

#[test]
fn item_delta_without_started_item_does_not_break_view_state() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_unknown".to_owned(),
        delta: "noop".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    assert!(conversation.projection().timeline.is_empty());
    assert!(conversation.projection().items.is_empty());
}

#[test]
fn timeout_event_terminalizes_running_item_with_recovery_context() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_tool_timeout_1";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::DynamicToolCall {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({}),
            status: pioneer_protocol::ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({})),
            },
            recovery: None,
            success: None,
            outcome: None,
            observation: None,
        },
    });

    conversation.apply(ConversationEvent::ItemTimeoutDetected {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        item_type: TurnItemType::DynamicToolCall,
        attempt_number: 2,
        reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
        recovery_job_id: Some("job_timeout_1".to_owned()),
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("timed out item should exist");
    assert_eq!(item.status, TimelineEntryStatus::Failed);
    assert!(item.completed_at_unix_ms.is_some());
    assert!(
        item.partial_text
            .contains("[timeout] attempt #2 idle deadline exceeded")
    );

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        delta: "\nlate output".to_owned(),
        stream: Some(ItemDeltaStream::Generic),
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("timed out item should exist");
    assert_eq!(item.status, TimelineEntryStatus::Failed);
    assert!(item.partial_text.contains("late output"));
}

#[test]
fn timeout_event_without_recovery_job_marks_no_automatic_recovery() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_reasoning_timeout_1";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: item_id.to_owned(),
            summary: vec![],
            content: vec![],
        },
    });

    conversation.apply(ConversationEvent::ItemTimeoutDetected {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        item_type: TurnItemType::Reasoning,
        attempt_number: 1,
        reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
        recovery_job_id: None,
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("timed out item should exist");
    assert_eq!(item.status, TimelineEntryStatus::Failed);
    assert!(item.partial_text.contains("no automatic recovery"));
}

#[test]
fn history_hydration_restores_recovery_events_without_terminal_duplicate() {
    let item_id = "item_reasoning_recovery";
    let recovery_job_id = "job_reasoning_recovery";
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,
                    prompt_manifest: None,
                    permission_profile: default_test_permission_profile(),
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: item_id.to_owned(),
                    summary: vec![],
                    content: vec![],
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::ItemRecoveryOpened {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: item_id.to_owned(),
                item_type: TurnItemType::Reasoning,
                recovery_job_id: recovery_job_id.to_owned(),
                trigger: RecoveryTrigger::ProviderError,
                action: RecoveryAction::MarkFailed,
                attempt_number: 1,
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 4,
            created_at: 1_300,
            payload: ThreadHistoryEventPayload::ItemRecoveryExhausted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: item_id.to_owned(),
                item_type: TurnItemType::Reasoning,
                recovery_job_id: recovery_job_id.to_owned(),
                attempt_number: 1,
                status: RecoveryJobStatus::Failed,
                error_message: "recovery policy marks this failure as terminal".to_owned(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 5,
            created_at: 1_400,
            payload: ThreadHistoryEventPayload::TurnFailed {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::Failed,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: Some(format!(
                        "recovery failed for item `{item_id}`: recovery policy marks this failure as terminal"
                    )),
                    prompt_manifest: None,
                    permission_profile: default_test_permission_profile(),
                },
            },
        },
    ]);

    let system_events = conversation
        .projection()
        .items
        .iter()
        .filter(|item| item.item_type == "system_event")
        .map(|item| item.partial_text.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        system_events,
        vec![
            "Starting step recovery".to_owned(),
            "Recovery failed".to_owned()
        ]
    );
    assert!(
        system_events
            .iter()
            .all(|text| !text.contains("recovery failed"))
    );
}

#[test]
fn foreign_thread_events_do_not_modify_local_projection() {
    let mut conversation = Conversation::new("thr_local");

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: "thr_foreign".to_owned(),
        turn: Turn {
            id: "turn_foreign".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });

    assert!(conversation.projection().timeline.is_empty());
    assert!(conversation.projection().turns.is_empty());
    assert!(conversation.can_submit_message());
}

#[test]
fn replay_style_sequence_restores_final_state() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: "item_reasoning".to_owned(),
            summary: vec![],
            content: vec![],
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: "item_reasoning".to_owned(),
            summary: vec!["thinking done".to_owned()],
            content: vec![],
        },
    });
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "item_answer".to_owned(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_answer".to_owned(),
        delta: "final".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "item_answer".to_owned(),
            text: "final".to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });
    assert_eq!(conversation.status_label(), "completing");
    assert!(conversation.tick());

    let answer = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_answer")
        .expect("assistant answer should exist");
    assert_eq!(answer.final_text.as_deref(), Some("final"));
    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "completed");
}

#[test]
fn duplicate_turn_completed_event_is_ignored_after_terminal_completion() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });

    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });
    assert_eq!(conversation.status_label(), "completing");
    assert!(conversation.tick());
    assert_eq!(conversation.status_label(), "completed");

    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });

    assert_eq!(conversation.status_label(), "completed");
    assert!(!conversation.tick());
    assert!(conversation.can_submit_message());
    let turn = conversation
        .projection()
        .turns
        .iter()
        .find(|turn| turn.id == TURN_ID)
        .expect("turn should exist");
    assert_eq!(turn.phase, TurnPhase::Completed);
}

#[test]
fn turn_completed_finalizes_projection_even_when_flow_is_not_in_flight() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,

            prompt_manifest: None,

            permission_profile: default_test_permission_profile(),
        },
    });

    let turn = conversation
        .projection()
        .turns
        .iter()
        .find(|turn| turn.id == TURN_ID)
        .expect("completed turn should be projected");
    assert_eq!(turn.phase, TurnPhase::Completed);
    assert!(turn.completed_at_unix_ms.is_some());
    assert!(!conversation.tick());
    assert!(conversation.can_submit_message());
}

#[test]
fn event_log_is_bounded() {
    let mut conversation = Conversation::new(THREAD_ID);

    for idx in 0..(MAX_EVENT_LOG_LEN + 100) {
        conversation.apply(ConversationEvent::ItemDelta {
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            item_id: format!("item_{idx}"),
            delta: "noop".to_owned(),
            stream: None,
            payload: None,
            markdown: None,
            markdown_version: None,
        });
    }

    assert_eq!(conversation.event_log().len(), MAX_EVENT_LOG_LEN);
    assert_eq!(
        conversation.event_log().front().map(|event| event.sequence),
        Some(101)
    );
}

fn tool_retry_budget_usage() -> Vec<ToolRetryBudgetUsage> {
    vec![ToolRetryBudgetUsage {
        kind: ToolRetryBudgetKind::Episode,
        used: 1,
        limit: 2,
    }]
}

#[test]
fn tool_retry_events_are_logged_without_recovery_projection() {
    let mut conversation = Conversation::new(THREAD_ID);

    apply_in_progress_turn(&mut conversation);
    conversation.apply(ConversationEvent::ItemToolRetryScheduled {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_web_fetch_retry".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_1_1".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 1,
        error_class: ToolRetryErrorClass::Timeout,
        retry_hint: "retry with a smaller request".to_owned(),
        budgets: tool_retry_budget_usage(),
        failure_signature_fingerprint: "sig_timeout".to_owned(),
        reason: "recoverable_tool_output".to_owned(),
    });
    conversation.apply(ConversationEvent::ItemToolRetryResolved {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_web_fetch_retry".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_1_1".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 2,
        resolution: ToolRetryResolution::Succeeded,
        budgets: tool_retry_budget_usage(),
        reason: "successful_tool_output".to_owned(),
    });
    conversation.apply(ConversationEvent::ItemToolRetryExhausted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_web_fetch_retry".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_1_2".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 3,
        error_class: ToolRetryErrorClass::Timeout,
        exhaustion_kind: ToolRetryExhaustionKind::FailureSignature,
        budgets: tool_retry_budget_usage(),
        failure_signature_fingerprint: "sig_timeout".to_owned(),
        reason: "same_failure_signature".to_owned(),
    });
    conversation.apply(ConversationEvent::TurnToolLoopBudgetExceeded {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
        limit: 32,
        observed: 33,
        action: ToolLoopBudgetAction::ContinueInNextWindow,
        reason: "agent_rounds_exceeded".to_owned(),
    });

    let kinds = conversation
        .event_log()
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&EventKind::ItemToolRetryScheduled));
    assert!(kinds.contains(&EventKind::ItemToolRetryResolved));
    assert!(kinds.contains(&EventKind::ItemToolRetryExhausted));
    assert!(kinds.contains(&EventKind::TurnToolLoopBudgetExceeded));
    assert_eq!(conversation.status_label(), "running");
    assert!(!conversation.can_submit_message());
    let retry_scheduled = "Retrying tool web_fetch (attempt #1)";
    assert!(
        conversation
            .projection()
            .items
            .iter()
            .any(|item| item.partial_text.contains(retry_scheduled))
    );
    assert!(
        conversation
            .projection()
            .items
            .iter()
            .all(|item| !item.partial_text.contains("recovery job"))
    );
}

#[test]
fn execution_window_events_project_runtime_rows_without_ending_turn() {
    let mut conversation = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut conversation);

    conversation.apply(ConversationEvent::TurnExecutionWindowStarted {
        notification: window_started_notification("win_000000000000000001", 1),
    });
    conversation.apply(ConversationEvent::TurnExecutionWindowExhausted {
        notification: window_exhausted_notification("win_000000000000000001", 1),
    });
    conversation.apply(ConversationEvent::TurnExecutionWindowCheckpointed {
        notification: window_checkpointed_notification("win_000000000000000001", 1),
    });
    conversation.apply(ConversationEvent::TurnExecutionWindowContinued {
        notification: window_continued_notification("win_000000000000000002", 2),
    });

    let kinds = conversation
        .event_log()
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&EventKind::TurnExecutionWindowStarted));
    assert!(kinds.contains(&EventKind::TurnExecutionWindowExhausted));
    assert!(kinds.contains(&EventKind::TurnExecutionWindowCheckpointed));
    assert!(kinds.contains(&EventKind::TurnExecutionWindowContinued));

    assert_eq!(conversation.status_label(), "running");
    assert!(!conversation.can_submit_message());
    assert_eq!(
        conversation.projection().in_flight_turn_id.as_deref(),
        Some(TURN_ID)
    );

    assert!(
        system_event_details_optional(&conversation, "turn_execution_window_started").is_none(),
        "window start should stay in the event log but not create a noisy timeline item"
    );

    assert!(
        system_event_details_optional(&conversation, "turn_execution_window_exhausted").is_none(),
        "exhausted should be folded into the continued item once continuation arrives"
    );

    assert!(
        system_event_details_optional(&conversation, "turn_execution_window_checkpointed")
            .is_none(),
        "checkpoint persistence is an internal detail and should not create a timeline item"
    );

    let (continued_message, continued_details) =
        system_event_details(&conversation, "turn_execution_window_continued");
    assert_eq!(
        continued_message,
        "Continued in execution window #2 after window #1 limit"
    );
    assert_eq!(
        continued_details
            .get("previous_window_index")
            .and_then(|value| value.as_u64()),
        Some(1)
    );
    assert_eq!(
        continued_details
            .get("checkpoint_id")
            .and_then(|value| value.as_str()),
        Some("chk_000000000000000001")
    );
    assert_eq!(
        continued_details
            .get("exhaustion_reason")
            .and_then(|value| value.as_str()),
        Some("max_tool_calls_per_window"),
        "continued item should retain the earlier exhausted-window details"
    );
    assert_eq!(
        continued_details
            .get("observed")
            .and_then(|value| value.as_u64()),
        Some(129)
    );
    assert!(continued_details.get("payload").is_none());
    assert_eq!(
        conversation
            .projection()
            .items
            .iter()
            .filter(|item| matches!(
                &item.item,
                TurnItem::SystemEvent {
                    code: Some(code),
                    ..
                } if code.starts_with("turn_execution_window_")
            ))
            .count(),
        1,
        "exhausted + checkpointed + continued should render as one compact timeline item"
    );

    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: default_test_permission_profile(),
        },
    });

    assert_eq!(conversation.status_label(), "completing");
    assert!(conversation.tick());
    assert_eq!(conversation.status_label(), "completed");
    assert!(conversation.can_submit_message());
}

#[test]
fn execution_window_blocked_projects_controlled_pause_not_failure() {
    let mut conversation = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut conversation);

    conversation.apply(ConversationEvent::TurnExecutionWindowBlocked {
        notification: window_blocked_notification(),
    });

    assert_eq!(conversation.status_label(), "blocked");
    assert!(conversation.can_submit_message());
    assert_eq!(conversation.projection().in_flight_turn_id, None);
    assert_eq!(
        conversation
            .projection()
            .turns
            .iter()
            .find(|turn| turn.id == TURN_ID)
            .map(|turn| (turn.phase, turn.error.as_deref())),
        Some((TurnPhase::Blocked, Some("max_total_windows_exceeded")))
    );
    assert!(
        conversation
            .projection()
            .items
            .iter()
            .all(|item| { item.turn_id != TURN_ID || item.status != TimelineEntryStatus::Failed })
    );

    let kinds = conversation
        .event_log()
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&EventKind::TurnExecutionWindowBlocked));

    let (message, details) = system_event_details(&conversation, "turn_execution_window_blocked");
    assert_eq!(message, "Execution paused: max_total_windows_exceeded");
    assert_eq!(
        details.get("status").and_then(|value| value.as_str()),
        Some("blocked")
    );
    assert_eq!(
        details
            .get("checkpoint_id")
            .and_then(|value| value.as_str()),
        Some("chk_000000000000000003")
    );
    assert_eq!(
        details
            .get("exhaustion_reason")
            .and_then(|value| value.as_str()),
        Some("max_agent_rounds_per_window")
    );
    assert!(details.get("payload").is_none());
}

#[test]
fn turn_blocked_resume_metadata_projects_live_and_history() {
    let resume = TurnBlockedResumeMetadata {
        reason_class: "model_not_found".to_owned(),
        human_message: "Model is unavailable".to_owned(),
        resume_requirements: vec!["Configure an available model".to_owned()],
        resume_command: format!("turn.resume:{TURN_ID}"),
        blocked_recovery_job_id: Some("rec_000000000000000001".to_owned()),
        latest_checkpoint_id: Some("chk_000000000000000001".to_owned()),
        can_resume_same_turn: true,
    };
    let blocked_turn = Turn {
        id: TURN_ID.to_owned(),
        status: TurnStatus::Blocked,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: Some("model unavailable".to_owned()),
        prompt_manifest: None,
        permission_profile: default_test_permission_profile(),
    };

    let mut live = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut live);
    live.apply(ConversationEvent::TurnBlocked {
        thread_id: THREAD_ID.to_owned(),
        turn: blocked_turn.clone(),
        resume: Some(resume.clone()),
    });

    let live_turn = live
        .projection()
        .turns
        .iter()
        .find(|turn| turn.id == TURN_ID)
        .expect("blocked turn should be projected");
    assert_eq!(live_turn.phase, TurnPhase::Blocked);
    assert_eq!(live_turn.resume.as_ref(), Some(&resume));
    let (live_message, live_details) = system_event_details(&live, "turn_blocked_resumable");
    assert_eq!(live_message, "Turn blocked: Model is unavailable");
    assert_eq!(
        live_details
            .get("resume_command")
            .and_then(|value| value.as_str()),
        Some("turn.resume:turn_000000000000000001")
    );
    assert_eq!(
        live_details
            .get("blocked_recovery_job_id")
            .and_then(|value| value.as_str()),
        Some("rec_000000000000000001")
    );

    let mut replay = Conversation::new(THREAD_ID);
    replay.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: WORKSPACE_ID.to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,
                    prompt_manifest: None,
                    permission_profile: default_test_permission_profile(),
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::TurnBlocked {
                workspace_id: WORKSPACE_ID.to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: blocked_turn,
                resume: Some(resume.clone()),
            },
        },
    ]);

    let replay_turn = replay
        .projection()
        .turns
        .iter()
        .find(|turn| turn.id == TURN_ID)
        .expect("blocked turn should replay");
    assert_eq!(replay_turn.resume.as_ref(), Some(&resume));
    let (_replay_message, replay_details) = system_event_details(&replay, "turn_blocked_resumable");
    assert_eq!(
        replay_details
            .get("latest_checkpoint_id")
            .and_then(|value| value.as_str()),
        Some("chk_000000000000000001")
    );
    assert_eq!(
        replay_details
            .get("can_resume_same_turn")
            .and_then(|value| value.as_bool()),
        Some(true)
    );
}

#[test]
fn history_hydration_replays_tool_retry_events_like_live_events() {
    let mut live = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut live);
    live.apply(ConversationEvent::ItemToolRetryScheduled {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_web_fetch_retry".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_1_1".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 1,
        error_class: ToolRetryErrorClass::Timeout,
        retry_hint: "retry with a smaller request".to_owned(),
        budgets: tool_retry_budget_usage(),
        failure_signature_fingerprint: "sig_timeout".to_owned(),
        reason: "recoverable_tool_output".to_owned(),
    });
    live.apply(ConversationEvent::TurnToolLoopBudgetExceeded {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
        limit: 32,
        observed: 33,
        action: ToolLoopBudgetAction::ContinueInNextWindow,
        reason: "agent_rounds_exceeded".to_owned(),
    });

    let mut replay = Conversation::new(THREAD_ID);
    replay.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,
                    prompt_manifest: None,
                    permission_profile: default_test_permission_profile(),
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemToolRetryScheduled {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: "item_web_fetch_retry".to_owned(),
                item_type: TurnItemType::WebFetch,
                tool_retry_episode_id: "tool_retry_turn_1_1".to_owned(),
                tool_name: "web_fetch".to_owned(),
                attempt_number: 1,
                error_class: ToolRetryErrorClass::Timeout,
                retry_hint: "retry with a smaller request".to_owned(),
                budgets: tool_retry_budget_usage(),
                failure_signature_fingerprint: "sig_timeout".to_owned(),
                reason: "recoverable_tool_output".to_owned(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::TurnToolLoopBudgetExceeded {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
                limit: 32,
                observed: 33,
                action: ToolLoopBudgetAction::ContinueInNextWindow,
                reason: "agent_rounds_exceeded".to_owned(),
            },
        },
    ]);

    let live_kinds = live
        .event_log()
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    let replay_kinds = replay
        .event_log()
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(live_kinds, replay_kinds);
    assert_eq!(replay.status_label(), "running");
    assert_eq!(
        live.projection()
            .items
            .iter()
            .filter(|item| item.item_type == "system_event")
            .count(),
        replay
            .projection()
            .items
            .iter()
            .filter(|item| item.item_type == "system_event")
            .count()
    );
}

#[test]
fn history_hydration_replays_execution_window_events_like_live_events() {
    let mut live = Conversation::new(THREAD_ID);
    apply_in_progress_turn(&mut live);
    live.apply(ConversationEvent::TurnExecutionWindowStarted {
        notification: window_started_notification("win_000000000000000001", 1),
    });
    live.apply(ConversationEvent::TurnExecutionWindowExhausted {
        notification: window_exhausted_notification("win_000000000000000001", 1),
    });
    live.apply(ConversationEvent::TurnExecutionWindowCheckpointed {
        notification: window_checkpointed_notification("win_000000000000000001", 1),
    });
    live.apply(ConversationEvent::TurnExecutionWindowContinued {
        notification: window_continued_notification("win_000000000000000002", 2),
    });

    let mut replay = Conversation::new(THREAD_ID);
    replay.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: WORKSPACE_ID.to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,
                    prompt_manifest: None,
                    permission_profile: default_test_permission_profile(),
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::TurnExecutionWindowStarted(
                window_started_notification("win_000000000000000001", 1),
            ),
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::TurnExecutionWindowExhausted(
                window_exhausted_notification("win_000000000000000001", 1),
            ),
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 4,
            created_at: 1_300,
            payload: ThreadHistoryEventPayload::TurnExecutionWindowCheckpointed(
                window_checkpointed_notification("win_000000000000000001", 1),
            ),
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 5,
            created_at: 1_400,
            payload: ThreadHistoryEventPayload::TurnExecutionWindowContinued(
                window_continued_notification("win_000000000000000002", 2),
            ),
        },
    ]);

    let live_kinds = live
        .event_log()
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    let replay_kinds = replay
        .event_log()
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(live_kinds, replay_kinds);
    assert_eq!(replay.status_label(), "running");
    assert!(!replay.can_submit_message());

    let live_system_events = live
        .projection()
        .items
        .iter()
        .filter_map(|item| match &item.item {
            TurnItem::SystemEvent {
                message,
                code: Some(code),
                ..
            } => Some((code.clone(), message.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    let replay_system_events = replay
        .projection()
        .items
        .iter()
        .filter_map(|item| match &item.item {
            TurnItem::SystemEvent {
                message,
                code: Some(code),
                ..
            } => Some((code.clone(), message.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(live_system_events, replay_system_events);
}

#[test]
fn hydrate_history_restores_all_items_and_thinking_duration() {
    let mut conversation = Conversation::new(THREAD_ID);
    let events = vec![
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,

                    prompt_manifest: None,

                    permission_profile: default_test_permission_profile(),
                },
                input: vec![UserInput::Text {
                    text: "history prompt".to_owned(),
                    text_elements: Vec::new(),
                }],
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::UserMessage {
                    id: "item_user_history".to_owned(),
                    text: "history prompt".to_owned(),
                    attachments: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::UserMessage {
                    id: "item_user_history".to_owned(),
                    text: "history prompt".to_owned(),
                    attachments: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 4,
            created_at: 2_000,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_reasoning_history".to_owned(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 5,
            created_at: 32_500,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_reasoning_history".to_owned(),
                    summary: vec!["done".to_owned()],
                    content: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 6,
            created_at: 32_900,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::AgentMessage {
                    id: "item_answer_history".to_owned(),
                    text: String::new(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 7,
            created_at: 33_100,
            payload: ThreadHistoryEventPayload::ItemDelta {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: "item_answer_history".to_owned(),
                delta: "history ".to_owned(),
                stream: None,
                payload: None,
                markdown: None,
                markdown_version: None,
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 8,
            created_at: 33_200,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::AgentMessage {
                    id: "item_answer_history".to_owned(),
                    text: "history response".to_owned(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 9,
            created_at: 33_400,
            payload: ThreadHistoryEventPayload::TurnCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::Completed,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,

                    prompt_manifest: None,

                    permission_profile: default_test_permission_profile(),
                },
            },
        },
    ];

    conversation.hydrate_history(&events);

    let user_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "item_user_history")
        .expect("hydrated user entry should exist");
    let user_item = conversation
        .projection()
        .item_for_timeline_entry(user_entry)
        .expect("hydrated user timeline item should resolve");
    assert_eq!(user_item.partial_text, "history prompt");

    let reasoning_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "item_reasoning_history")
        .expect("hydrated reasoning item should exist");
    let reasoning_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_reasoning_history")
        .expect("hydrated reasoning view item should exist");
    let reasoning_timeline_item = conversation
        .projection()
        .item_for_timeline_entry(reasoning_entry)
        .expect("hydrated reasoning timeline item should resolve");
    assert!(
        matches!(reasoning_timeline_item.item, TurnItem::Reasoning { .. }),
        "timeline reasoning entry should keep typed payload"
    );
    assert_eq!(reasoning_item.started_at_unix_ms, Some(2_000));
    assert_eq!(reasoning_item.completed_at_unix_ms, Some(32_500));

    let answer_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "item_answer_history")
        .expect("hydrated assistant item should exist");
    let answer_item = conversation
        .projection()
        .item_for_timeline_entry(answer_entry)
        .expect("hydrated assistant timeline item should resolve");
    assert_eq!(answer_item.partial_text, "history response");

    assert_eq!(conversation.status_label(), "completed");
    assert!(conversation.can_submit_message());
}

#[test]
fn hydrate_history_preserves_reasoning_delta_text_when_completed_payload_is_empty() {
    let mut conversation = Conversation::new(THREAD_ID);
    let events = vec![
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,

                    prompt_manifest: None,

                    permission_profile: default_test_permission_profile(),
                },
                input: vec![UserInput::Text {
                    text: "reasoning test".to_owned(),
                    text_elements: Vec::new(),
                }],
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 2_000,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_reasoning_delta_history".to_owned(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 3_000,
            payload: ThreadHistoryEventPayload::ItemDelta {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: "item_reasoning_delta_history".to_owned(),
                delta: "Reasoning chunk".to_owned(),
                stream: Some(ItemDeltaStream::Generic),
                payload: None,
                markdown: None,
                markdown_version: None,
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 4,
            created_at: 4_000,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_reasoning_delta_history".to_owned(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 5,
            created_at: 5_000,
            payload: ThreadHistoryEventPayload::TurnCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::Completed,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    error: None,

                    prompt_manifest: None,

                    permission_profile: default_test_permission_profile(),
                },
            },
        },
    ];

    conversation.hydrate_history(&events);

    let reasoning_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "item_reasoning_delta_history")
        .expect("hydrated reasoning timeline entry should exist");
    let reasoning_timeline_item = conversation
        .projection()
        .item_for_timeline_entry(reasoning_entry)
        .expect("reasoning timeline entry should resolve to item");
    assert!(
        reasoning_timeline_item
            .partial_text
            .contains("Reasoning chunk")
    );
    assert_eq!(
        reasoning_timeline_item.status,
        TimelineEntryStatus::Completed
    );

    let reasoning_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_reasoning_delta_history")
        .expect("hydrated reasoning item should exist");
    assert!(reasoning_item.partial_text.contains("Reasoning chunk"));
    assert_eq!(reasoning_item.status, TimelineEntryStatus::Completed);
}
