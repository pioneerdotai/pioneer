use crate::CanonicalTurnEventPayload;
use pioneer_protocol::{TurnItem, TurnPermissionAuditEvent, TurnPermissionAuditEventKind};

/// Typed model-facing projection of a canonical event. Canonical events remain
/// durable and available to execution, audit and UI consumers; this policy
/// controls only whether and how their text enters model history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalEventModelProjection {
    Omit,
    Input,
    Assistant(String),
    User(String),
    Default,
}

impl CanonicalEventModelProjection {
    pub fn is_omitted(&self) -> bool {
        matches!(self, Self::Omit)
    }
}

fn meaningful_reasoning(summary: &[String], content: &[String]) -> Vec<String> {
    content
        .iter()
        .chain(summary)
        .filter(|part| !part.trim().is_empty())
        .cloned()
        .collect()
}

fn technical_system_event(item: &TurnItem) -> bool {
    let TurnItem::SystemEvent { code, details, .. } = item else {
        return false;
    };
    match code.as_deref() {
        Some("agent_thread_status_changed") => true,
        Some("agent_runtime_event") => {
            details
                .as_ref()
                .and_then(|details| details.get("nativeMethod"))
                .and_then(serde_json::Value::as_str)
                == Some("thread/tokenUsage/updated")
        }
        _ => false,
    }
}

fn permission_text(event: &TurnPermissionAuditEvent) -> String {
    let details = serde_json::json!({
        "event_kind": event.event_kind,
        "decision": event.decision,
        "reason": event.reason,
        "security_reason_code": event.security_reason_code,
        "security_capability": event.security_capability,
        "tool_name": event.tool_name,
        "action_kind": event.action_kind,
        "cached": event.cached,
    });
    format!("Historical permission event:\n{details}")
}

fn system_event_text(item: &TurnItem) -> Option<String> {
    let TurnItem::SystemEvent {
        level,
        message,
        code,
        details,
        ..
    } = item
    else {
        return None;
    };
    let event = serde_json::json!({
        "level": level,
        "message": message,
        "code": code,
        "details": details,
    });
    Some(format!("Recorded historical system event:\n{event}"))
}

pub fn canonical_item_model_projection(item: &TurnItem) -> CanonicalEventModelProjection {
    match item {
        TurnItem::Reasoning {
            summary, content, ..
        } => {
            let parts = meaningful_reasoning(summary, content);
            if parts.is_empty() {
                CanonicalEventModelProjection::Omit
            } else {
                CanonicalEventModelProjection::Assistant(format!(
                    "Reasoning recorded for a previous response:\n{}",
                    parts.join("\n")
                ))
            }
        }
        TurnItem::SystemEvent { .. } if technical_system_event(item) => {
            CanonicalEventModelProjection::Omit
        }
        TurnItem::SystemEvent { .. } => {
            CanonicalEventModelProjection::User(system_event_text(item).unwrap())
        }
        TurnItem::AgentMessage { text, .. } if text.trim().is_empty() => {
            CanonicalEventModelProjection::Omit
        }
        TurnItem::AgentMessage { text, .. } => {
            CanonicalEventModelProjection::Assistant(text.clone())
        }
        TurnItem::UserMessage { attachments, .. } if attachments.is_empty() => {
            CanonicalEventModelProjection::Omit
        }
        _ => CanonicalEventModelProjection::Default,
    }
}

pub fn canonical_event_model_projection(
    event: &CanonicalTurnEventPayload,
) -> CanonicalEventModelProjection {
    use CanonicalEventModelProjection as Projection;
    use CanonicalTurnEventPayload as Event;
    match event {
        Event::TurnStarted(value) if !value.input.is_empty() => Projection::Input,
        Event::TurnMessageEdited(value) if !value.input.is_empty() => Projection::Input,
        Event::TurnStarted(_) | Event::TurnMessageEdited(_) | Event::TurnMessageDeleted(_) => {
            Projection::Omit
        }
        Event::ItemStarted(value) if technical_system_event(&value.item) => Projection::Omit,
        Event::ItemCompleted(value) => canonical_item_model_projection(&value.item),
        Event::ItemUpdated(value) => match &value.item {
            // An update is not a completed assistant reply. Keep its historical
            // status envelope, while an empty update has no model body at all.
            TurnItem::AgentMessage { text, .. } if !text.trim().is_empty() => Projection::Default,
            _ => canonical_item_model_projection(&value.item),
        },
        Event::TurnExecutionWindowStarted(_)
        | Event::TurnExecutionWindowCheckpointed(_)
        | Event::TurnExecutionWindowContinued(_) => Projection::Omit,
        Event::TurnExecutionWindowExhausted(value) => Projection::User(format!(
            "Historical execution limit exhausted ({:?}, {}/{}): {}",
            value.exhaustion_reason, value.observed, value.limit, value.reason
        )),
        Event::TurnExecutionWindowBlocked(value) => {
            Projection::User(format!("Historical execution blocked: {}", value.reason))
        }
        Event::TurnPermissionAudit(value)
            if matches!(
                value.event_kind,
                TurnPermissionAuditEventKind::ProfileSelected
                    | TurnPermissionAuditEventKind::SecuritySnapshotResolved
            ) =>
        {
            Projection::Omit
        }
        Event::TurnPermissionAudit(value) => Projection::User(permission_text(value)),
        Event::TurnCompleted(value) => {
            Projection::User(format!("Historical turn status: {:?}", value.turn.status))
        }
        Event::TurnFailed(value) => Projection::User(format!(
            "Historical turn {:?}: {:?}",
            value.turn.status, value.turn.error
        )),
        Event::TurnBlocked(value) => {
            Projection::User(format!("Historical turn blocked: {:?}", value.turn.error))
        }
        _ => Projection::Default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ExecutionWindowExhaustionReason, ExecutionWindowStatus, ItemCompletedNotification,
        ItemUpdatedNotification, SystemEventLevel, TurnExecutionWindowBlockedNotification,
        TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
        TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification,
        TurnMessageDeletedEvent, TurnMessageEditedEvent, TurnPermissionAuditDecision,
        TurnPermissionMode, TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnStatus,
    };

    fn completed(item: TurnItem) -> CanonicalTurnEventPayload {
        CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item,
        })
    }

    fn reasoning(summary: &[&str], content: &[&str]) -> TurnItem {
        TurnItem::Reasoning {
            id: "reasoning".into(),
            summary: summary.iter().map(|value| (*value).into()).collect(),
            content: content.iter().map(|value| (*value).into()).collect(),
        }
    }

    fn agent_message(text: &str) -> TurnItem {
        TurnItem::AgentMessage {
            id: "agent-message".into(),
            text: text.into(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        }
    }

    fn system(code: &str, details: Option<serde_json::Value>) -> TurnItem {
        TurnItem::SystemEvent {
            id: "system".into(),
            level: SystemEventLevel::Info,
            message: "synthetic system event".into(),
            code: Some(code.into()),
            details,
        }
    }

    fn permission(kind: TurnPermissionAuditEventKind) -> TurnPermissionAuditEvent {
        TurnPermissionAuditEvent {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            event_kind: kind,
            profile_mode: TurnPermissionMode::FullAccess,
            profile_source: TurnPermissionProfileSource::Defaulted,
            security_snapshot_id: None,
            security_snapshot_version: None,
            security_reason_code: Some("synthetic_reason".into()),
            security_capability: None,
            item_id: None,
            tool_call_id: None,
            tool_name: Some("synthetic_tool".into()),
            action_kind: None,
            request_key: None,
            decision: Some(TurnPermissionAuditDecision::Deny),
            reason: None,
            cached: false,
        }
    }

    #[test]
    fn empty_reasoning_has_no_model_projection() {
        for item in [
            reasoning(&[], &[]),
            reasoning(&["", " \t"], &[]),
            reasoning(&[], &["\n", "   "]),
            reasoning(&[" "], &["\t"]),
        ] {
            assert_eq!(
                canonical_event_model_projection(&completed(item)),
                CanonicalEventModelProjection::Omit
            );
        }
    }

    #[test]
    fn meaningful_reasoning_is_preserved_from_each_field() {
        for (item, expected) in [
            (reasoning(&[], &["content"]), "content"),
            (reasoning(&["summary"], &[]), "summary"),
            (reasoning(&["summary"], &["content"]), "content\nsummary"),
        ] {
            assert_eq!(
                canonical_event_model_projection(&completed(item)),
                CanonicalEventModelProjection::Assistant(format!(
                    "Reasoning recorded for a previous response:\n{expected}"
                ))
            );
        }
    }

    #[test]
    fn empty_agent_messages_are_omitted_for_completed_updated_and_item_projection() {
        for text in ["", " ", "\n\t"] {
            let item = agent_message(text);
            assert!(canonical_item_model_projection(&item).is_omitted());
            assert!(canonical_event_model_projection(&completed(item.clone())).is_omitted());
            assert!(
                canonical_event_model_projection(&CanonicalTurnEventPayload::ItemUpdated(
                    ItemUpdatedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "thread".into(),
                        turn_id: "turn".into(),
                        item,
                    }
                ))
                .is_omitted()
            );
        }
        assert_eq!(
            canonical_item_model_projection(&agent_message("meaningful answer")),
            CanonicalEventModelProjection::Assistant("meaningful answer".into())
        );
        assert_eq!(
            canonical_event_model_projection(&completed(agent_message("meaningful answer"))),
            CanonicalEventModelProjection::Assistant("meaningful answer".into())
        );
        assert_eq!(
            canonical_event_model_projection(&CanonicalTurnEventPayload::ItemUpdated(
                ItemUpdatedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: agent_message("meaningful update"),
                }
            )),
            CanonicalEventModelProjection::Default
        );
    }

    #[test]
    fn only_structurally_identified_system_notifications_are_omitted() {
        assert_eq!(
            canonical_event_model_projection(&completed(system(
                "agent_thread_status_changed",
                None
            ))),
            CanonicalEventModelProjection::Omit
        );
        assert_eq!(
            canonical_event_model_projection(&completed(system(
                "agent_runtime_event",
                Some(
                    serde_json::json!({"nativeMethod":"thread/tokenUsage/updated","usage":"[REDACTED]"})
                )
            ))),
            CanonicalEventModelProjection::Omit
        );
        assert_eq!(
            canonical_event_model_projection(&completed(system(
                "agent_runtime_event",
                Some(serde_json::json!({
                    "nativeMethod": "thread/tokenUsage/updated",
                    "usage": {"inputTokens": 123, "outputTokens": 45}
                }))
            ))),
            CanonicalEventModelProjection::Omit
        );
        assert!(
            !canonical_event_model_projection(&completed(system(
                "quoted_user_text",
                Some(serde_json::json!({"text":"Thread status changed [REDACTED]"}))
            )))
            .is_omitted()
        );
        let plan = TurnItem::SystemEvent {
            id: "plan".into(),
            level: SystemEventLevel::Info,
            message: "Plan updated".into(),
            code: Some("agent_plan_updated".into()),
            details: Some(serde_json::json!({
                "plan": [{"step": "Preserve the real plan", "status": "in_progress"}]
            })),
        };
        let CanonicalEventModelProjection::User(text) = canonical_item_model_projection(&plan)
        else {
            panic!("plan event must remain model-visible");
        };
        assert!(text.contains("Preserve the real plan"));
    }

    #[test]
    fn routine_permission_snapshots_are_omitted_but_denials_and_sandbox_failures_remain() {
        for kind in [
            TurnPermissionAuditEventKind::ProfileSelected,
            TurnPermissionAuditEventKind::SecuritySnapshotResolved,
        ] {
            assert!(
                canonical_event_model_projection(&CanonicalTurnEventPayload::TurnPermissionAudit(
                    permission(kind)
                ))
                .is_omitted()
            );
        }
        for kind in [
            TurnPermissionAuditEventKind::DecisionDenied,
            TurnPermissionAuditEventKind::ApprovalRequested,
            TurnPermissionAuditEventKind::SecuritySandboxDegraded,
            TurnPermissionAuditEventKind::SecuritySandboxUnavailable,
        ] {
            assert!(
                !canonical_event_model_projection(&CanonicalTurnEventPayload::TurnPermissionAudit(
                    permission(kind)
                ))
                .is_omitted()
            );
        }
        for decision in [
            TurnPermissionAuditDecision::Allow,
            TurnPermissionAuditDecision::Deny,
            TurnPermissionAuditDecision::Cancelled,
            TurnPermissionAuditDecision::Expired,
        ] {
            let mut event = permission(TurnPermissionAuditEventKind::ApprovalResolved);
            event.decision = Some(decision);
            event.security_capability = Some(pioneer_protocol::TurnSecurityCapabilityKind::Network);
            let CanonicalEventModelProjection::User(text) = canonical_event_model_projection(
                &CanonicalTurnEventPayload::TurnPermissionAudit(event),
            ) else {
                panic!("permission resolution must remain visible");
            };
            assert!(text.contains(&serde_json::to_string(&decision).unwrap()));
            assert!(text.contains("network"));
            assert!(text.contains("synthetic_reason"));
        }
    }

    fn turn() -> pioneer_protocol::Turn {
        pioneer_protocol::Turn {
            id: "turn".into(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: pioneer_protocol::ThreadMode::Chat,
            author: None,
            reply_to_turn_id: None,
            mentions: vec![],
            message_revision: 1,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::FullAccess,
                TurnPermissionProfileSource::Defaulted,
            ),
        }
    }

    #[test]
    fn model_omission_does_not_erase_input_revision_or_deletion_markers() {
        let edited = CanonicalTurnEventPayload::TurnMessageEdited(TurnMessageEditedEvent {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn: turn(),
            input: vec![],
            changed_by: pioneer_protocol::PersistedActorRef::System,
            changed_at: 1,
        });
        let deleted = CanonicalTurnEventPayload::TurnMessageDeleted(TurnMessageDeletedEvent {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn: turn(),
            deleted_by: pioneer_protocol::PersistedActorRef::System,
            deleted_at: 2,
        });
        assert!(canonical_event_model_projection(&edited).is_omitted());
        assert!(canonical_event_model_projection(&deleted).is_omitted());
        assert_eq!(
            crate::compaction::event_projection_metadata(&edited),
            (None, "input_revision")
        );
        assert_eq!(
            crate::compaction::event_projection_metadata(&deleted),
            (None, "input_deleted")
        );
        let empty_copy = completed(TurnItem::UserMessage {
            id: "copy".into(),
            text: String::new(),
            attachments: vec![],
        });
        assert!(canonical_event_model_projection(&empty_copy).is_omitted());
        assert_eq!(
            crate::compaction::event_projection_metadata(&empty_copy),
            (Some("copy".into()), "input_copy")
        );
    }

    #[test]
    fn routine_execution_window_lifecycle_is_omitted_but_blocking_is_retained() {
        let started = TurnExecutionWindowStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            window_id: "internal-window".into(),
            window_index: 1,
            status: ExecutionWindowStatus::Running,
            started_at_unix_ms: 1,
        };
        let checkpointed = TurnExecutionWindowCheckpointedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            window_id: "internal-window".into(),
            window_index: 1,
            status: ExecutionWindowStatus::Checkpointed,
            checkpoint_id: "internal-checkpoint".into(),
            checkpoint_kind: "synthetic".into(),
            payload_bytes: 10,
            created_at_unix_ms: 2,
        };
        let continued = TurnExecutionWindowContinuedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            window_id: "internal-window-2".into(),
            window_index: 2,
            status: ExecutionWindowStatus::Continued,
            previous_window_id: "internal-window".into(),
            previous_window_index: 1,
            checkpoint_id: "internal-checkpoint".into(),
            continued_at_unix_ms: 3,
        };
        for event in [
            CanonicalTurnEventPayload::TurnExecutionWindowStarted(started),
            CanonicalTurnEventPayload::TurnExecutionWindowCheckpointed(checkpointed),
            CanonicalTurnEventPayload::TurnExecutionWindowContinued(continued),
        ] {
            assert!(canonical_event_model_projection(&event).is_omitted());
        }
        let blocked = TurnExecutionWindowBlockedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            window_id: "must-not-render".into(),
            window_index: 2,
            status: ExecutionWindowStatus::Blocked,
            exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
            checkpoint_id: Some("must-not-render".into()),
            total_windows: 2,
            total_tool_calls: 20,
            reason: "user action required".into(),
            blocked_at_unix_ms: 4,
        };
        assert_eq!(
            canonical_event_model_projection(
                &CanonicalTurnEventPayload::TurnExecutionWindowBlocked(blocked)
            ),
            CanonicalEventModelProjection::User(
                "Historical execution blocked: user action required".into()
            )
        );
        let exhausted = TurnExecutionWindowExhaustedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            window_id: "must-not-render".into(),
            window_index: 2,
            status: ExecutionWindowStatus::Exhausted,
            exhaustion_reason: ExecutionWindowExhaustionReason::MaxProviderTokensPerWindow,
            limit: 100,
            observed: 101,
            agent_round_count: 4,
            tool_call_count: 5,
            provider_token_count: Some(101),
            started_at_unix_ms: 1,
            exhausted_at_unix_ms: 5,
            reason: "continue after user confirmation".into(),
        };
        let CanonicalEventModelProjection::User(text) = canonical_event_model_projection(
            &CanonicalTurnEventPayload::TurnExecutionWindowExhausted(exhausted),
        ) else {
            panic!("execution exhaustion must remain visible");
        };
        assert!(text.contains("continue after user confirmation"));
        assert!(!text.contains("must-not-render"));
    }
}
