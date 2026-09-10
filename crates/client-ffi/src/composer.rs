use pioneer_client::composer::{
    attachments::{ComposerAttachment, ComposerAttachmentKind, composer_attachment_from_path},
    capabilities::{
        ComposerCapability, ComposerCapabilityMenuVisibility, ComposerCapabilityTarget,
        ComposerSubmissionPlan, SelectableMcpCapability, SelectableSkillCapability,
        composer_capability_menu_visibility, composer_capability_target_for_provider,
        filter_selectable_skill_capabilities_for_target, plan_composer_submission,
    },
};
use pioneer_protocol::RuntimeSummary;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerAttachmentFromPathRequest {
    pub path: String,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub kind: Option<ComposerAttachmentKind>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerCapabilityTargetRequest {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub runtimes: Vec<RuntimeSummary>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerCapabilityMenuVisibilityRequest {
    pub target: ComposerCapabilityTarget,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSubmissionPlanRequest {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub has_attachments: bool,
    #[serde(default)]
    pub capabilities: Vec<ComposerCapability>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerSkillRowsForTargetRequest {
    #[serde(default)]
    pub rows: Vec<SelectableSkillCapability>,
    pub target: ComposerCapabilityTarget,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientComposerFilterMcpRowsRequest {
    pub server_rows: Vec<SelectableMcpCapability>,
    pub tool_rows: Vec<SelectableMcpCapability>,
    pub selected_keys: Vec<String>,
    #[serde(default)]
    pub active_server_id: Option<String>,
    #[serde(default)]
    pub query: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientComposerFilterMcpRowsResult {
    pub server_rows: Vec<SelectableMcpCapability>,
    pub tool_rows: Vec<SelectableMcpCapability>,
    pub has_query: bool,
}

pub fn composer_attachment_from_path_request(
    request: ClientComposerAttachmentFromPathRequest,
) -> anyhow::Result<ComposerAttachment> {
    let path = normalize_client_file_reference(request.path.as_str())?;
    let mut attachment = composer_attachment_from_path(Path::new(path.as_str()))
        .ok_or_else(|| anyhow::anyhow!("attachment path is required"))?;
    if let Some(file_name) = request.file_name.and_then(non_empty_string) {
        attachment.file_name = file_name;
    }
    if let Some(kind) = request.kind {
        attachment.kind = kind;
    }
    Ok(attachment)
}

pub fn composer_capability_target(
    request: ClientComposerCapabilityTargetRequest,
) -> ComposerCapabilityTarget {
    composer_capability_target_for_provider(
        request.provider.as_deref(),
        request.runtimes.as_slice(),
    )
}

pub fn composer_capability_menu(
    request: ClientComposerCapabilityMenuVisibilityRequest,
) -> ComposerCapabilityMenuVisibility {
    composer_capability_menu_visibility(request.target)
}

pub fn composer_submission_plan(
    request: ClientComposerSubmissionPlanRequest,
) -> ComposerSubmissionPlan {
    plan_composer_submission(
        request.provider.as_deref(),
        request.text.as_str(),
        request.has_attachments,
        request.capabilities.as_slice(),
    )
}

pub fn composer_skill_rows_for_target(
    request: ClientComposerSkillRowsForTargetRequest,
) -> Vec<SelectableSkillCapability> {
    filter_selectable_skill_capabilities_for_target(request.rows.as_slice(), request.target)
}

pub fn filter_mcp_picker_rows(
    request: ClientComposerFilterMcpRowsRequest,
) -> ClientComposerFilterMcpRowsResult {
    let (server_rows, tool_rows, has_query) =
        pioneer_client::composer::capabilities::project_mcp_picker_rows(
            &request.server_rows,
            &request.tool_rows,
            &request.selected_keys,
            request.active_server_id.as_deref(),
            &request.query,
        );
    ClientComposerFilterMcpRowsResult {
        server_rows,
        tool_rows,
        has_query,
    }
}

pub fn normalize_client_file_reference(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow::anyhow!("attachment path is required"));
    }

    if let Ok(url) = url::Url::parse(value)
        && url.scheme() == "file"
    {
        return url
            .to_file_path()
            .map(|path| path.to_string_lossy().to_string())
            .map_err(|_| anyhow::anyhow!("invalid file URL for attachment"));
    }

    Ok(value.to_owned())
}

fn non_empty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    use pioneer_client::composer::state_machine::{ComposerDomainAction, ComposerDomainState};
    use pioneer_client::{
        composer::store::ComposerIntent,
        core::{ClientCore, ClientIntent, ClientScope},
    };

    #[test]
    fn approval_request_generation_and_planner_failure_match_the_ffi_owner() {
        use pioneer_client::cli_runtime::{approval_actions::*, approvals::*};
        use pioneer_protocol::*;
        let direct = ClientCore::shared();
        let ffi = crate::ClientFfiRuntime::default();
        ffi.initialize(r#"{"platform":"ios"}"#).unwrap();
        for core in [&direct, &ffi.core] {
            let thread: Thread = serde_json::from_value(serde_json::json!({
                "workspace_id":"ws", "id":"a", "preview":"", "mode":"Agent", "model":"model", "model_provider":"provider",
                "created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]
            })).unwrap();
            core.upsert_thread(thread);
            core.set_thread_cli_binding(
                "a",
                Some(CLIRuntimeThreadBinding {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    runtime_id: "runtime".into(),
                    runtime_kind: CLIAgentRuntimeKind::Codex,
                    status: "ready".into(),
                }),
            );
            core.apply_thread_conversation_event("ws", pioneer_client::conversation::events::ConversationEvent::TurnStarted {
                thread_id:"a".into(), turn:serde_json::from_value(serde_json::json!({"id":"turn", "status":"InProgress", "permission_profile":default_turn_permission_profile_snapshot()})).unwrap()
            }, None);
            pioneer_client::core::ClientMutationAuthority::for_test()
                .accept_thread_capabilities_for_test(
                    core,
                    AuthorizationCapabilitySnapshot {
                        schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                        authorization_revision: 1,
                        principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
                        role_key: "member".into(),
                        role: AuthorizationRolePresentation {
                            key: "member".into(),
                            display_name: "Synthetic".into(),
                            description: String::new(),
                            built_in: false,
                        },
                        global: Default::default(),
                        workspace: None,
                        thread: Some(AuthorizationThreadCapabilitySnapshot {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            capabilities: AuthorizationThreadCapabilities {
                                can_respond_to_agent_requests: true,
                                ..Default::default()
                            },
                        }),
                    },
                );
        }
        for core in [&direct, &ffi.core] {
            core.apply_pending_requests(PendingRequestsReduction::Opened(
                PendingRequest::from_cli_runtime_opened_notification(
                    CLIRuntimeRequestOpenedNotification {
                        workspace_id: "ws".into(),
                        runtime_id: "runtime".into(),
                        request_id: "request".into(),
                        thread_id: Some("a".into()),
                        turn_id: Some("turn".into()),
                        item_id: None,
                        visible_thread_ids: vec![],
                        request: CLIRuntimePendingRequest {
                            kind: CLIRuntimeRequestKind::CommandApproval,
                            title: None,
                            message: None,
                            native_request_id: None,
                            payload: None,
                        },
                    },
                ),
            ));
        }
        let dispatch = |intent: ApprovalActionIntent| {
            direct.approval_action_intent(intent.clone());
            ffi.client_intent_dispatch(&serde_json::json!({"schema_version":1,"intent":{"kind":"approval_action","intent":intent}}).to_string()).unwrap();
            let expected = direct.approval_action_snapshot("a", "request").unwrap();
            let actual = ffi.core.approval_action_snapshot("a", "request").unwrap();
            assert_eq!(
                serde_json::to_value(&expected).unwrap(),
                serde_json::to_value(&actual).unwrap()
            );
            expected
        };
        let input = dispatch(ApprovalActionIntent::Observe {
            thread_id: "a".into(),
            request_id: "request".into(),
        });
        assert!(input.can_respond);
        let generation = input.request_generation.unwrap();
        let unchanged = dispatch(ApprovalActionIntent::Respond {
            thread_id: "a".into(),
            request_id: "request".into(),
            request_generation: generation + 1,
            resolution: PendingRequestResolution::Allow,
        });
        assert_eq!(unchanged.revision, input.revision);
        let failed = dispatch(ApprovalActionIntent::Respond {
            thread_id: "a".into(),
            request_id: "request".into(),
            request_generation: generation,
            resolution: PendingRequestResolution::AllowForTurn,
        });
        assert!(matches!(failed.state, ApprovalActionState::Failed { .. }));
        let unchanged = dispatch(ApprovalActionIntent::Observe {
            thread_id: "a".into(),
            request_id: "request".into(),
        });
        assert_eq!(unchanged.revision, failed.revision);
        assert_eq!(
            direct
                .pending_requests_for_scope(Some("ws"), Some("a"))
                .len(),
            1
        );
    }

    #[test]
    fn steering_operation_and_late_completion_match_the_ffi_owner() {
        use pioneer_client::composer::store::*;
        use pioneer_protocol::*;
        let direct = ClientCore::shared();
        let ffi = crate::ClientFfiRuntime::default();
        ffi.initialize(r#"{"platform":"ios"}"#).unwrap();
        for core in [&direct, &ffi.core] {
            let thread: Thread = serde_json::from_value(serde_json::json!({
                "workspace_id":"ws", "id":"a", "preview":"", "mode":"Agent", "model":"model", "model_provider":"provider",
                "created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]
            })).unwrap();
            core.upsert_thread(thread);
            core.set_thread_cli_binding(
                "a",
                Some(CLIRuntimeThreadBinding {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    runtime_id: "runtime".into(),
                    runtime_kind: CLIAgentRuntimeKind::Codex,
                    status: "ready".into(),
                }),
            );
            core.apply_thread_conversation_event("ws", pioneer_client::conversation::events::ConversationEvent::TurnStarted {
                thread_id:"a".into(), turn:serde_json::from_value(serde_json::json!({"id":"turn", "status":"InProgress", "permission_profile":default_turn_permission_profile_snapshot()})).unwrap()
            }, None);
            pioneer_client::core::ClientMutationAuthority::for_test()
                .accept_thread_capabilities_for_test(
                    core,
                    AuthorizationCapabilitySnapshot {
                        schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                        authorization_revision: 1,
                        principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
                        role_key: "member".into(),
                        role: AuthorizationRolePresentation {
                            key: "member".into(),
                            display_name: "Synthetic".into(),
                            description: String::new(),
                            built_in: false,
                        },
                        global: Default::default(),
                        workspace: None,
                        thread: Some(AuthorizationThreadCapabilitySnapshot {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            capabilities: AuthorizationThreadCapabilities {
                                can_steer_agent_execution: true,
                                ..Default::default()
                            },
                        }),
                    },
                );
        }
        let dispatch = |intent: ComposerIntent| {
            direct.composer_intent(intent.clone());
            ffi.client_intent_dispatch(&serde_json::json!({"schema_version":1,"intent":{"kind":"composer","intent":intent}}).to_string()).unwrap();
            let expected = direct.composer_snapshot("a").unwrap();
            let actual = ffi.core.composer_snapshot("a").unwrap();
            assert_eq!(
                serde_json::to_value(&expected).unwrap(),
                serde_json::to_value(&actual).unwrap()
            );
            expected
        };
        let input = dispatch(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: Default::default(),
        });
        dispatch(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            text: "steer".into(),
        });
        let pending = dispatch(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            operation: ComposerOperationKind::Steer,
        });
        let identity = pending.operation().unwrap().identity.clone();
        dispatch(ComposerIntent::PrepareOperation {
            identity: identity.clone(),
        });
        dispatch(ComposerIntent::CompleteOperation {
            identity: identity.clone(),
            completion: ComposerOperationCompletion::Failed {
                message: "synthetic failure".into(),
            },
        });
        let pending = dispatch(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            operation: ComposerOperationKind::Steer,
        });
        let retry = pending.operation().unwrap().identity.clone();
        dispatch(ComposerIntent::PrepareOperation {
            identity: retry.clone(),
        });
        dispatch(ComposerIntent::CompleteOperation {
            identity,
            completion: ComposerOperationCompletion::Sent,
        });
        assert_eq!(direct.composer_snapshot("a").unwrap().draft().text, "steer");
        dispatch(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            text: "new draft text".into(),
        });
        dispatch(ComposerIntent::CompleteOperation {
            identity: retry,
            completion: ComposerOperationCompletion::Sent,
        });
        assert_eq!(
            direct.composer_snapshot("a").unwrap().draft().text,
            "new draft text"
        );
    }

    #[test]
    fn default_model_selection_and_controlled_mode_match_the_ffi_owner() {
        use pioneer_protocol::*;
        let direct = ClientCore::shared();
        let ffi = crate::ClientFfiRuntime::default();
        ffi.initialize(r#"{"platform":"ios"}"#).unwrap();
        let thread: Thread = serde_json::from_value(serde_json::json!({
            "workspace_id": "ws", "id": "a", "preview": "", "mode": "Agent", "model": "model", "model_provider": "provider", "reasoning_effort": "high",
            "created_at": 1, "updated_at": 1, "status": "Idle", "origin_kind": "user", "sidebar_visibility": "visible", "turns": [{
                "id": "turn", "status": "Completed", "permission_profile": default_turn_permission_profile_snapshot()
            }]
        })).unwrap();
        for core in [&direct, &ffi.core] {
            core.upsert_thread(thread.clone());
        }
        let dispatch = |intent: ComposerIntent| {
            direct.composer_intent(intent.clone());
            ffi.client_intent_dispatch(&serde_json::json!({"schema_version": 1, "intent": {"kind": "composer", "intent": intent}}).to_string()).unwrap();
            let expected = direct.composer_snapshot("a").unwrap();
            let actual = ffi.core.composer_snapshot("a").unwrap();
            assert_eq!(
                serde_json::to_value(&expected).unwrap(),
                serde_json::to_value(&actual).unwrap()
            );
            expected
        };
        let input = dispatch(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: ComposerDomainState::default(),
        });
        assert!(input.domain().selected_model.is_none());
        let input = dispatch(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            action: ComposerDomainAction::SetModeFromUser {
                mode: ThreadMode::Agent,
            },
        });
        assert_eq!(input.domain().selected_model.as_deref(), Some("model"));
        assert_eq!(
            input.domain().selected_reasoning_effort.as_deref(),
            Some("high")
        );
        let same = dispatch(ComposerIntent::SyncModelSelection {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            reset: false,
        });
        assert_eq!(input.revision(), same.revision());
        let message = dispatch(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            action: ComposerDomainAction::SetModeFromUser {
                mode: ThreadMode::Message,
            },
        });
        assert!(message.domain().selected_model.is_none());
    }

    #[test]
    fn composer_send_preflight_failure_and_duplicate_match_the_real_ffi_path() {
        struct NoFiles;
        impl pioneer_client::platform::ClientFileSystem for NoFiles {
            fn read_file(
                &self,
                _: &pioneer_client::platform::ClientPath,
            ) -> pioneer_client::ClientResult<Vec<u8>> {
                panic!("preflight must precede file access")
            }
            fn metadata(
                &self,
                _: &pioneer_client::platform::ClientPath,
            ) -> pioneer_client::ClientResult<pioneer_client::platform::ClientFileMetadata>
            {
                panic!("preflight must precede file access")
            }
            fn write_cache_file(
                &self,
                _: &str,
                _: &[u8],
            ) -> pioneer_client::ClientResult<pioneer_client::platform::ClientPath> {
                panic!("composer must not write cache")
            }
        }
        let direct = ClientCore::shared();
        let ffi = crate::ClientFfiRuntime::default();
        ffi.initialize(r#"{"platform":"ios"}"#).unwrap();
        let dispatch = |intent: ComposerIntent| {
            direct.composer_intent(intent.clone());
            ffi.client_intent_dispatch(
                &serde_json::json!({ "schema_version": 1,
                "intent": { "kind": "composer", "intent": intent } })
                .to_string(),
            )
            .unwrap();
        };
        dispatch(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: ComposerDomainState::default(),
        });
        let draft_id = direct.composer_snapshot("a").unwrap().draft_id();
        dispatch(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id,
            text: "retained draft".into(),
        });
        dispatch(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id,
            operation: pioneer_client::composer::store::ComposerOperationKind::Send,
        });
        let identity = direct
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        let request =
            serde_json::json!({ "operation": identity, "workspace_id": "workspace" }).to_string();
        // No thread or endpoint is installed: both paths must reject before transport/file effects.
        for duplicate in [false, true] {
            let before = direct.composer_snapshot("a").unwrap();
            let expected = direct
                .submit_composer_send(
                    identity.clone(),
                    &NoFiles,
                    pioneer_client::composer::workflow::ComposerSendContext {
                        workspace_id: Some("workspace".into()),
                        endpoint_kind: None,
                        failure_message: "Failed to send message".into(),
                    },
                )
                .err()
                .unwrap();
            let actual = ffi.active_thread_send_text(&request).err().unwrap();
            assert_eq!(expected.to_string(), actual);
            let scope = ClientScope::Composer {
                thread_id: "a".into(),
            };
            let actual = ffi
                .client_scoped_snapshot(
                    &serde_json::json!({"schema_version":1,"scope":scope}).to_string(),
                )
                .unwrap();
            let expected = direct
                .snapshot(&scope)
                .map(crate::client_binding::snapshot_dto);
            assert_eq!(
                serde_json::to_value(expected).unwrap(),
                serde_json::to_value(actual).unwrap()
            );
            assert_eq!(
                direct.composer_snapshot("a").unwrap().draft().text,
                "retained draft"
            );
            if duplicate {
                assert!(std::sync::Arc::ptr_eq(
                    &before,
                    &direct.composer_snapshot("a").unwrap()
                ));
            }
        }
    }

    #[test]
    fn voice_operation_cancellation_and_late_completion_match_direct_client() {
        use pioneer_client::composer::store::{ComposerOperationCompletion, ComposerOperationKind};
        let direct = ClientCore::shared();
        let ffi = crate::ClientFfiRuntime::default();
        ffi.initialize(r#"{"platform":"ios"}"#).unwrap();
        let dispatch = |intent: ComposerIntent| {
            let expected = direct.composer_intent(intent.clone());
            let actual = ffi.client_intent_dispatch(&serde_json::json!({"schema_version": 1, "intent": {"kind": "composer", "intent": intent}}).to_string()).unwrap();
            assert_eq!(
                serde_json::to_value(crate::client_binding::transition_dto(expected)).unwrap(),
                serde_json::to_value(actual).unwrap()
            );
            let scope = ClientScope::Composer {
                thread_id: "voice".into(),
            };
            let actual = ffi
                .client_scoped_snapshot(
                    &serde_json::json!({"schema_version": 1, "scope": scope}).to_string(),
                )
                .unwrap();
            let expected = direct
                .snapshot(&scope)
                .map(crate::client_binding::snapshot_dto);
            assert_eq!(
                serde_json::to_value(expected).unwrap(),
                serde_json::to_value(actual).unwrap()
            );
        };
        dispatch(ComposerIntent::Open {
            thread_id: "voice".into(),
            defaults: Default::default(),
        });
        let draft_id = direct.composer_snapshot("voice").unwrap().draft_id();
        dispatch(ComposerIntent::EditText {
            thread_id: "voice".into(),
            draft_id,
            text: "keep voice draft".into(),
        });
        dispatch(ComposerIntent::BeginOperation {
            thread_id: "voice".into(),
            draft_id,
            operation: ComposerOperationKind::Voice,
        });
        let identity = direct
            .composer_snapshot("voice")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        dispatch(ComposerIntent::StartVoiceCapture {
            identity: identity.clone(),
        });
        dispatch(ComposerIntent::StartVoiceCapture {
            identity: identity.clone(),
        });
        dispatch(ComposerIntent::VoiceSessionStarted {
            identity: identity.clone(),
            session_id: "session".into(),
        });
        dispatch(ComposerIntent::VoiceSessionStarted {
            identity: identity.clone(),
            session_id: "duplicate".into(),
        });
        dispatch(ComposerIntent::PrepareOperation {
            identity: identity.clone(),
        });
        dispatch(ComposerIntent::CompleteOperation {
            identity: identity.clone(),
            completion: ComposerOperationCompletion::Cancelled,
        });
        let before = direct.composer_snapshot("voice").unwrap();
        dispatch(ComposerIntent::CompleteOperation {
            identity,
            completion: ComposerOperationCompletion::Sent,
        });
        assert!(std::sync::Arc::ptr_eq(
            &before,
            &direct.composer_snapshot("voice").unwrap()
        ));
        assert_eq!(before.draft().text, "keep voice draft");
    }

    #[test]
    fn candidate_scoped_review_intents_and_bounded_failure_match_direct_client() {
        use pioneer_client::tasks::{
            review::TaskReviewAction, review_controller::TaskReviewIntent,
        };
        let direct = ClientCore::shared();
        let ffi = crate::ClientFfiRuntime::default();
        ffi.initialize(r#"{"platform":"ios"}"#).unwrap();
        let mut subscriptions = Vec::new();
        for (thread, candidate) in [("a", "candidate"), ("b", "candidate"), ("a", "other")] {
            let scope = ClientScope::TaskReview {
                thread_id: thread.into(),
                candidate_id: candidate.into(),
            };
            subscriptions
                .push(direct.subscribe(scope.clone(), std::num::NonZeroUsize::new(64).unwrap()));
            ffi.client_scope_acquire(
                &serde_json::json!({"schema_version":1,"scope":scope}).to_string(),
            )
            .unwrap();
            ffi.client_scoped_snapshot(
                &serde_json::json!({"schema_version":1,"scope":scope}).to_string(),
            )
            .unwrap();
            for intent in [
                TaskReviewIntent::Observe {
                    thread_id: thread.into(),
                    candidate_id: candidate.into(),
                },
                TaskReviewIntent::Perform {
                    thread_id: thread.into(),
                    candidate_id: candidate.into(),
                    action: TaskReviewAction::Accept,
                    feedback: None,
                    reason: None,
                },
                TaskReviewIntent::Observe {
                    thread_id: thread.into(),
                    candidate_id: candidate.into(),
                },
            ] {
                let intent = ClientIntent::TaskReview { intent };
                let expected =
                    crate::client_binding::transition_dto(direct.dispatch(intent.clone()));
                let actual = ffi
                    .client_intent_dispatch(
                        &serde_json::json!({"schema_version":1,"intent":intent}).to_string(),
                    )
                    .unwrap();
                assert_eq!(actual, expected);
                let expected = direct
                    .snapshot(&scope)
                    .map(crate::client_binding::snapshot_dto);
                let actual = ffi
                    .client_scoped_snapshot(
                        &serde_json::json!({"schema_version":1,"scope":scope}).to_string(),
                    )
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            }
        }
        direct.shutdown();
    }

    #[test]
    fn composer_publications_and_identity_fences_match_direct_rust_at_real_ffi_boundary() {
        let direct = ClientCore::shared();
        let ffi = crate::ClientFfiRuntime::default();
        ffi.initialize(r#"{"platform":"ios"}"#).unwrap();
        let dispatch = |intent: ComposerIntent| {
            let intent = ClientIntent::Composer { intent };
            let expected = crate::client_binding::transition_dto(direct.dispatch(intent.clone()));
            let actual = ffi
                .client_intent_dispatch(
                    &serde_json::json!({ "schema_version": 1, "intent": intent }).to_string(),
                )
                .unwrap();
            assert_eq!(expected, actual);
            for thread in ["a", "b"] {
                let scope = ClientScope::Composer {
                    thread_id: thread.into(),
                };
                let expected = direct
                    .snapshot(&scope)
                    .map(crate::client_binding::snapshot_dto);
                let actual = ffi
                    .client_scoped_snapshot(
                        &serde_json::json!({ "schema_version": 1, "scope": scope }).to_string(),
                    )
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(expected).unwrap(),
                    serde_json::to_value(actual).unwrap()
                );
            }
        };
        for thread in ["a", "b"] {
            dispatch(ComposerIntent::Open {
                thread_id: thread.into(),
                defaults: ComposerDomainState {
                    selected_mode: pioneer_protocol::ThreadMode::Agent,
                    ..Default::default()
                },
            });
        }
        let draft = direct.composer_snapshot("a").unwrap().draft_id();
        let b = direct.composer_snapshot("b").unwrap();
        let attachment = |path: &str| ComposerAttachment {
            path: path.into(),
            file_name: path.into(),
            kind: ComposerAttachmentKind::File,
            upload_state:
                pioneer_client::composer::attachments::ComposerAttachmentUploadState::Local,
        };
        let member = pioneer_client::composer::state_machine::ComposerMentionCandidate {
            principal_id: pioneer_protocol::PrincipalId::new("A".repeat(21)).unwrap(),
            display_name: "Ada".into(),
            nickname: "ada".into(),
            avatar_revision: None,
        };
        let capability: ComposerCapability = serde_json::from_value(serde_json::json!({
            "id": "mcp-server:workspace:tools", "label": "Tools", "kind": {"McpServer": {"name": "tools", "scope_kind": "workspace"}}
        })).unwrap();
        let actions = vec![
            ComposerDomainAction::SetAttachments {
                attachments: vec![attachment("first"), attachment("target")],
            },
            ComposerDomainAction::SetAttachments {
                attachments: vec![
                    attachment("inserted"),
                    attachment("target"),
                    attachment("first"),
                ],
            },
            ComposerDomainAction::RemoveAttachment {
                path: "target".into(),
            },
            ComposerDomainAction::RemoveAttachment {
                path: "target".into(),
            },
            ComposerDomainAction::AddCapability { capability },
            ComposerDomainAction::SetSkillSelections {
                selections: vec![
                    pioneer_client::composer::skill_selection::ComposerSkillSelection::SkillPack {
                        pack_id: pioneer_protocol::SkillPackId::new("P".repeat(21)).unwrap(),
                    },
                ],
            },
            ComposerDomainAction::SetReplyTarget {
                target: pioneer_client::composer::state_machine::ComposerReplyTarget {
                    turn_id: "reply".into(),
                    author_display_name: None,
                    preview: None,
                },
            },
            ComposerDomainAction::SelectMention { candidate: member },
            ComposerDomainAction::SetModelSelectionFromUser {
                provider: Some("provider".into()),
                model: Some("model".into()),
                capability_target: None,
            },
            ComposerDomainAction::SetReasoningEffortFromUser {
                effort: Some("high".into()),
            },
            ComposerDomainAction::SetPermissionMode {
                mode: pioneer_protocol::TurnPermissionMode::FullAccess,
            },
        ];
        for action in actions {
            dispatch(ComposerIntent::Domain {
                thread_id: "a".into(),
                draft_id: draft,
                action,
            });
        }
        for text in ["hello @ada", "hello @ada", "hello"] {
            dispatch(ComposerIntent::EditText {
                thread_id: "a".into(),
                draft_id: draft,
                text: text.into(),
            });
        }
        assert!(std::sync::Arc::ptr_eq(
            &b,
            &direct.composer_snapshot("b").unwrap()
        ));
        assert_eq!(
            direct
                .composer_snapshot("a")
                .unwrap()
                .domain()
                .attachments
                .iter()
                .map(|item| item.path.as_str())
                .collect::<Vec<_>>(),
            ["inserted", "first"]
        );
        dispatch(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id: draft,
            operation: pioneer_client::composer::store::ComposerOperationKind::PickFiles,
        });
        let identity = direct
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        dispatch(ComposerIntent::CompleteOperation {
            identity: identity.clone(),
            completion:
                pioneer_client::composer::store::ComposerOperationCompletion::FilesSelected {
                    attachments: vec![attachment("picked")],
                },
        });
        dispatch(ComposerIntent::CompleteOperation {
            identity,
            completion: pioneer_client::composer::store::ComposerOperationCompletion::Cancelled,
        });
        dispatch(ComposerIntent::Clear {
            thread_id: "a".into(),
            draft_id: draft,
        });
        dispatch(ComposerIntent::Clear {
            thread_id: "a".into(),
            draft_id: draft,
        });
        dispatch(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: draft,
            text: "late".into(),
        });
        dispatch(ComposerIntent::EditText {
            thread_id: "b".into(),
            draft_id: draft,
            text: "wrong draft".into(),
        });
        dispatch(ComposerIntent::ClearAll);
    }
    fn shared_message_fixture() -> (
        std::sync::Arc<ClientCore>,
        crate::ClientFfiRuntime,
        Vec<pioneer_client::core::ClientSubscription>,
    ) {
        shared_message_fixture_with_workers(true)
    }
    fn shared_message_fixture_with_workers(
        workers: bool,
    ) -> (
        std::sync::Arc<ClientCore>,
        crate::ClientFfiRuntime,
        Vec<pioneer_client::core::ClientSubscription>,
    ) {
        use pioneer_protocol::*;
        let auth = AuthMeResponse {
            gateway: AuthGatewaySnapshot {
                id: GatewayId::new("G00000000000000000001").unwrap(),
            },
            principal: AuthPrincipalSnapshot {
                id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                kind: PrincipalKind::Superuser,
                display_name: "Alice".into(),
                nickname: "alice".into(),
                avatar_revision: None,
            },
            device: AuthDeviceSnapshot {
                id: DeviceId::new("D00000000000000000001").unwrap(),
                installation_id: "synthetic".into(),
                display_name: "Test".into(),
                client_kind: ClientKind::Desktop,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: AuthSessionId::new("S00000000000000000001").unwrap(),
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                status: AuthSessionStatus::Active,
                refresh_generation: 1,
                refresh_expires_at_unix: 1000,
            },
            role_key: None,
        };
        let direct = if workers {
            ClientCore::shared()
        } else {
            std::sync::Arc::new(ClientCore::new())
        };
        let mut ffi = crate::ClientFfiRuntime::default();
        if !workers {
            ffi.core = std::sync::Arc::new(ClientCore::new());
        }
        ffi.initialize(r#"{"platform":"ios"}"#).unwrap();
        let mut leases = Vec::new();
        for core in [&direct, &ffi.core] {
            pioneer_client::core::ClientMutationAuthority::for_test()
                .accept_identity_for_test(core, auth.clone())
                .unwrap();
            core.upsert_thread(Thread {
                workspace_id: "ws".into(),
                id: "a".into(),
                name: None,
                preview: String::new(),
                preview_author: None,
                mode: ThreadMode::Chat,
                model: "model".into(),
                model_provider: "provider".into(),
                reasoning_effort: None,
                created_at: 1,
                updated_at: 2,
                status: ThreadStatus::Idle,
                origin_kind: ThreadOriginKind::User,
                sidebar_visibility: ThreadSidebarVisibility::Visible,
                agent_nickname: None,
                agent_role: None,
                visibility: None,
                turns: vec![],
            });
            core.apply_thread_timeline_page(
                ThreadTimelinePageResponse {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    projection_version: 4,
                    blocks: vec![TimelineBlock {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        block_id: "block".into(),
                        turn_id: Some("turn".into()),
                        sort_key: "1".into(),
                        started_at_unix_ms: Some(1),
                        updated_at_unix_ms: Some(2),
                        kind: TimelineBlockKind::UserMessage {
                            item_id: Some("item".into()),
                            inputs: vec![],
                            text: "original @alice".into(),
                            attachments: vec![],
                            mode: ThreadMode::Message,
                            author: Some(TurnAuthorSnapshot {
                                actor: PersistedActorRef::Principal(
                                    PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                                ),
                                display_name: "Alice".into(),
                                nickname: "alice".into(),
                                avatar_revision: None,
                                agent: None,
                            }),
                            route: None,
                            reply: None,
                            mentions: vec![],
                            revision: 3,
                            edited: false,
                            deleted: false,
                        },
                    }],
                    page: Default::default(),
                },
                pioneer_client::timeline::semantic::TopLevelPageMergeMode::Reset,
            );

            leases.push(core.subscribe(
                ClientScope::Timeline {
                    thread_id: "a".into(),
                },
                std::num::NonZeroUsize::new(16).unwrap(),
            ));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while core.thread_presentation_snapshot("a").is_none()
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            core.thread_presentation_snapshot("a").unwrap();
        }
        (direct, ffi, leases)
    }
    #[test]
    fn turn_cancellation_ffi_dispatch_and_bounded_retry_match_direct_client() {
        use pioneer_client::{
            conversation::events::ConversationEvent, core::ClientMutationAuthority,
            turns::cancellation::*,
        };
        use pioneer_protocol::*;
        let (direct, ffi, _leases) = shared_message_fixture();
        let authority = ClientMutationAuthority::for_test();
        let resources = AuthorizationOperationalResourceProjection {
            providers: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            skills: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            mcp_servers: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            ..Default::default()
        };
        for core in [&direct, &ffi.core] {
            authority.accept_thread_capabilities_for_test(
                core,
                AuthorizationCapabilitySnapshot {
                    schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                    authorization_revision: 1,
                    principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                    role_key: "member".into(),
                    role: AuthorizationRolePresentation {
                        key: "member".into(),
                        display_name: "Synthetic".into(),
                        description: String::new(),
                        built_in: false,
                    },
                    global: Default::default(),
                    workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                        workspace_id: "ws".into(),
                        capabilities: AuthorizationWorkspaceCapabilities {
                            can_use_providers: true,
                            can_use_cli_runtimes: true,
                            can_use_skills: true,
                            can_use_mcp: true,
                            ..Default::default()
                        },
                        operational_resources: resources.clone(),
                        execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                            fingerprint: "policy".into(),
                            resources: resources.clone(),
                            permission_options: vec![],
                            can_attach_artifacts: false,
                            mcp_invocation_limits: Default::default(),
                        },
                    }),
                    thread: Some(AuthorizationThreadCapabilitySnapshot {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        capabilities: AuthorizationThreadCapabilities {
                            can_cancel_agent_execution: true,
                            ..Default::default()
                        },
                    }),
                },
            );
            core.composer_intent(ComposerIntent::Open {
                thread_id: "a".into(),
                defaults: ComposerDomainState {
                    selected_mode: ThreadMode::Agent,
                    ..Default::default()
                },
            });
        }

        for core in [&direct, &ffi.core] {
            let mut source = core.existing_thread_mutation("a").unwrap();
            source
                .conversation
                .apply(ConversationEvent::LocalTurnStartRequested {
                    thread_id: "a".into(),
                    turn_id: "running".into(),
                    pending_request_id: "request".into(),
                    mode: ThreadMode::Agent,
                    user_text: "text".into(),
                    attachments: vec![],
                });
            source
                .conversation
                .apply(ConversationEvent::LocalTurnStartAccepted {
                    thread_id: "a".into(),
                    turn_id: "running".into(),
                    pending_request_id: "request".into(),
                    mode: ThreadMode::Agent,
                });
        }
        for generation in 1..=2 {
            let intent = TurnCancellationIntent {
                thread_id: "a".into(),
                reason: Some("Synthetic stop".into()),
            };
            let expected = direct.dispatch(ClientIntent::TurnCancellation {
                intent: intent.clone(),
            });
            let actual=ffi.client_intent_dispatch(&serde_json::json!({"schema_version":1,"intent":{"kind":"turn_cancellation","intent":intent}}).to_string()).unwrap();
            assert_eq!(
                serde_json::to_value(expected.outcome()).unwrap(),
                serde_json::to_value(actual.outcome).unwrap()
            );
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while [&direct, &ffi.core].iter().any(|core| {
                core.turn_cancellation_snapshot("a")
                    .is_some_and(|p| p.state == TurnCancellationState::Pending)
            }) && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            let expected = direct.turn_cancellation_snapshot("a").unwrap();
            assert_eq!(expected.identity.generation, generation);
            assert!(matches!(
                expected.state,
                TurnCancellationState::Failed { .. }
            ));
            let actual = ffi
                .client_scoped_snapshot(
                    r#"{"schema_version":1,"scope":{"kind":"turn_cancellation","thread_id":"a"}}"#,
                )
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(expected.as_ref()).unwrap(),
                actual.payload
            );
            assert_eq!(
                direct.composer_snapshot("a"),
                ffi.core.composer_snapshot("a")
            );
            assert_eq!(
                direct
                    .thread_coordinator_snapshot("a")
                    .unwrap()
                    .conversation
                    .status_label(),
                "running"
            );
            assert_eq!(
                ffi.core
                    .thread_coordinator_snapshot("a")
                    .unwrap()
                    .conversation
                    .status_label(),
                "running"
            );
        }
    }
    #[test]
    fn composer_model_picker_ffi_selects_and_closes_the_same_canonical_session() {
        use pioneer_client::composer::model_picker::*;
        use pioneer_client::core::ClientMutationAuthority;
        use pioneer_protocol::*;
        let (direct, ffi, _leases) = shared_message_fixture_with_workers(false);
        let authority = ClientMutationAuthority::for_test();
        let resources = AuthorizationOperationalResourceProjection {
            fingerprint: "resources".into(),
            provider_models_all: true,
            cli_models_all: true,
            cli_runtimes: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            providers: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            skills: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            mcp_servers: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            ..Default::default()
        };
        for core in [&direct, &ffi.core] {
            let capabilities = AuthorizationCapabilitySnapshot {
                schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                authorization_revision: 1,
                principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                role_key: "member".into(),
                role: AuthorizationRolePresentation {
                    key: "member".into(),
                    display_name: "Synthetic".into(),
                    description: String::new(),
                    built_in: false,
                },
                global: Default::default(),
                workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                    workspace_id: "ws".into(),
                    capabilities: AuthorizationWorkspaceCapabilities {
                        can_use_providers: true,
                        can_use_cli_runtimes: true,
                        can_use_skills: true,
                        can_use_mcp: true,
                        ..Default::default()
                    },
                    operational_resources: resources.clone(),
                    execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                        fingerprint: "policy".into(),
                        resources: resources.clone(),
                        permission_options: vec![],
                        can_attach_artifacts: false,
                        mcp_invocation_limits: Default::default(),
                    },
                }),
                thread: Some(AuthorizationThreadCapabilitySnapshot {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    capabilities: Default::default(),
                }),
            };
            let thread = core
                .thread_coordinator_snapshot("a")
                .unwrap()
                .thread()
                .unwrap()
                .clone();
            let (generation, connection) = core.current_auth_ticket();
            assert_eq!(
                core.accept_authorization_projection(generation, connection, capabilities.clone()),
                pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted
            );
            core.upsert_thread(thread);
            authority.accept_thread_capabilities_for_test(core, capabilities);
            core.composer_intent(ComposerIntent::Open {
                thread_id: "a".into(),
                defaults: ComposerDomainState {
                    selected_mode: ThreadMode::Agent,
                    ..Default::default()
                },
            });
        }

        let send = |intent: ComposerModelPickerIntent| {
            let expected = direct.dispatch(ClientIntent::ComposerModelPicker {
                intent: intent.clone(),
            });
            let actual = ffi.client_intent_dispatch(&serde_json::json!({"schema_version":1,"intent":{"kind":"composer_model_picker","intent":intent}}).to_string()).unwrap();
            assert_eq!(
                serde_json::to_value(expected.outcome()).unwrap(),
                serde_json::to_value(actual.outcome).unwrap()
            );
        };
        let draft_id = direct.composer_snapshot("a").unwrap().draft_id();
        send(ComposerModelPickerIntent::Open {
            thread_id: "a".into(),
            draft_id,
            deferred: false,
        });
        for core in [&direct, &ffi.core] {
            core.provider_runtime_intent(
                pioneer_client::providers::runtime::ProviderRuntimeIntent::Observe {
                    workspace_id: "ws".into(),
                },
            );
            authority.accept_composer_runtime_for_test(
                core,
                "a",
                CLIRuntimeListResponse {
                    revision: 1,
                    runtimes: vec![],
                },
            );
            authority.accept_composer_model_picker_for_test(core,"a",serde_json::from_value(serde_json::json!({"providers":[{"name":"provider"}]})).unwrap(),serde_json::from_value(serde_json::json!({"provider":"provider","models":[{"id":"one","provider":"provider","name":"One","limits":{},"capabilities":{}}]})).unwrap());
        }
        assert!(ffi.client_intent_dispatch(&serde_json::json!({"schema_version":1,"intent":{"kind":"composer","intent":{"kind":"reconcile_policy","thread_id":"a","draft_id":draft_id,"policy":{}}}}).to_string()).is_err());
        let composer = direct.composer_snapshot("a").unwrap();
        let publication = ffi
            .client_scoped_snapshot(
                r#"{"schema_version":1,"scope":{"kind":"composer","thread_id":"a"}}"#,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(composer.as_ref()).unwrap(),
            publication.payload
        );
        assert!(composer.selected_provider_ready());
        assert_eq!(
            composer.runtime_selection().unwrap().request.state,
            pioneer_client::composer::catalog::ComposerCatalogRequestState::Ready
        );
        let wrong_draft: pioneer_client::composer::store::DraftId =
            serde_json::from_value(serde_json::json!(99999)).unwrap();
        let retry = ComposerIntent::RetryRuntimeSelection {
            thread_id: "a".into(),
            draft_id: wrong_draft,
        };
        let expected = direct.dispatch(ClientIntent::Composer {
            intent: retry.clone(),
        });
        let actual = ffi.client_intent_dispatch(&serde_json::json!({"schema_version":1,"intent":{"kind":"composer","intent":retry}}).to_string()).unwrap();
        assert_eq!(
            serde_json::to_value(expected.outcome()).unwrap(),
            serde_json::to_value(actual.outcome).unwrap()
        );
        assert!(std::sync::Arc::ptr_eq(
            &composer,
            &direct.composer_snapshot("a").unwrap()
        ));
        let identity = direct
            .composer_model_picker_snapshot("a")
            .unwrap()
            .identity
            .clone();
        send(ComposerModelPickerIntent::SelectModel {
            identity: identity.clone(),
            model: "one".into(),
        });
        assert_eq!(
            direct.composer_snapshot("a"),
            ffi.core.composer_snapshot("a")
        );
        assert_eq!(
            direct
                .composer_snapshot("a")
                .unwrap()
                .domain()
                .selected_model
                .as_deref(),
            Some("one")
        );
        let current = direct.composer_model_picker_snapshot("a").unwrap();
        let actual = ffi
            .client_scoped_snapshot(
                r#"{"schema_version":1,"scope":{"kind":"composer_model_picker","thread_id":"a"}}"#,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(current.as_ref()).unwrap(),
            actual.payload
        );
        send(ComposerModelPickerIntent::SelectModel {
            identity: identity.clone(),
            model: "one".into(),
        });
        assert!(std::sync::Arc::ptr_eq(
            &current,
            &direct.composer_model_picker_snapshot("a").unwrap()
        ));
        send(ComposerModelPickerIntent::Close {
            identity: identity.clone(),
        });
        let closed = direct.composer_snapshot("a").unwrap();
        send(ComposerModelPickerIntent::SelectModel {
            identity,
            model: "one".into(),
        });
        assert!(std::sync::Arc::ptr_eq(
            &closed,
            &direct.composer_snapshot("a").unwrap()
        ));
    }
    #[test]
    fn composer_catalog_selection_and_read_only_skill_projection_match_the_ffi_owner() {
        use pioneer_client::composer::catalog::*;
        use pioneer_client::core::ClientMutationAuthority;
        use pioneer_protocol::*;
        let (direct, ffi, _leases) = shared_message_fixture();
        let authority = ClientMutationAuthority::for_test();
        let resources = AuthorizationOperationalResourceProjection {
            skills: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            mcp_servers: AuthorizationResourceSelector {
                all: true,
                ids: vec![],
            },
            ..Default::default()
        };
        let skill: SkillListItem = serde_json::from_value(serde_json::json!({
            "skill_id":"AAAAAAAAAAAAAAAAAAAAA", "owner":null, "slug":"alpha", "source_kind":"user", "display_name":"Alpha", "description":"Synthetic skill", "version":null,"fingerprint":"skill", "trust_level":"community", "install":{"managed":true,"installed":true,"lifecycle_editable":true,"install_path":null,"updated_at":null}, "policy":{"enabled":true,"allow_implicit_invocation":true,"allow_implicit_invocation_editable":true}, "health":{"status":"ok","dependency_failures":[],"security_blocks":[],"validation_issues":[]},"status":"active","status_reason":null
        })).unwrap();
        for core in [&direct, &ffi.core] {
            authority.accept_thread_capabilities_for_test(
                core,
                AuthorizationCapabilitySnapshot {
                    schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                    authorization_revision: 1,
                    principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                    role_key: "member".into(),
                    role: AuthorizationRolePresentation {
                        key: "member".into(),
                        display_name: "Synthetic".into(),
                        description: String::new(),
                        built_in: false,
                    },
                    global: Default::default(),
                    workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                        workspace_id: "ws".into(),
                        capabilities: AuthorizationWorkspaceCapabilities {
                            can_use_skills: true,
                            can_use_mcp: true,
                            ..Default::default()
                        },
                        operational_resources: resources.clone(),
                        execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                            fingerprint: "policy".into(),
                            resources: resources.clone(),
                            permission_options: vec![],
                            can_attach_artifacts: false,
                            mcp_invocation_limits: Default::default(),
                        },
                    }),
                    thread: Some(AuthorizationThreadCapabilitySnapshot {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        capabilities: Default::default(),
                    }),
                },
            );
            core.composer_intent(ComposerIntent::Open {
                thread_id: "a".into(),
                defaults: ComposerDomainState {
                    selected_mode: ThreadMode::Agent,
                    ..Default::default()
                },
            });
            authority.accept_composer_catalog_for_test(
                core,
                "a",
                pioneer_client::skills::catalog::SkillManagementProjection {
                    standalone: vec![skill.clone()],
                    packs: vec![],
                },
                vec![],
                vec![],
            );
        }
        let draft_id = direct.composer_snapshot("a").unwrap().draft_id();
        let dispatch = |intent: ComposerCatalogIntent| {
            let intent = ClientIntent::ComposerCatalog { intent };
            let expected = direct.dispatch(intent.clone());
            let actual = ffi
                .client_intent_dispatch(
                    &serde_json::json!({"schema_version":1,"intent":intent}).to_string(),
                )
                .unwrap();
            assert_eq!(
                serde_json::to_value(expected.outcome()).unwrap(),
                serde_json::to_value(actual.outcome).unwrap()
            );
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while [&direct, &ffi.core].iter().any(|core| {
                core.composer_catalog_snapshot("a")
                    .is_some_and(|p| p.skill_request.state == ComposerCatalogRequestState::Loading)
            }) && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            let expected = direct.composer_catalog_snapshot("a").unwrap();
            assert_ne!(
                expected.skill_request.state,
                ComposerCatalogRequestState::Loading
            );
            let actual = ffi
                .client_scoped_snapshot(
                    r#"{"schema_version":1,"scope":{"kind":"composer_catalog","thread_id":"a"}}"#,
                )
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(expected.as_ref()).unwrap(),
                actual.payload
            );
            assert_eq!(
                direct.composer_snapshot("a"),
                ffi.core.composer_snapshot("a")
            );
        };
        dispatch(ComposerCatalogIntent::OpenPicker {
            thread_id: "a".into(),
            draft_id,
            picker: ComposerPickerKind::Skills,
            deferred: false,
        });
        let identity = direct
            .composer_catalog_snapshot("a")
            .unwrap()
            .session
            .as_ref()
            .unwrap()
            .identity
            .clone();
        dispatch(ComposerCatalogIntent::ToggleSkill {
            identity: identity.clone(),
            selection: pioneer_client::composer::skill_selection::ComposerSkillSelection::Skill {
                skill_id: skill.skill_id,
                pack_id: None,
            },
        });
        assert_eq!(
            direct
                .composer_snapshot("a")
                .unwrap()
                .domain()
                .skill_selections
                .len(),
            1
        );
        let query = serde_json::json!({"thread_id":"a", "draft_id":draft_id, "query":"alpha"});
        let projection = ffi.composer_skill_pack_picker(&query.to_string()).unwrap();
        assert_eq!(
            projection,
            direct.composer_catalog_skill_picker("a", draft_id, "alpha")
        );
        assert_eq!(projection.standalone.len(), 1);
        let key = projection.standalone[0].key.clone();
        dispatch(ComposerCatalogIntent::RemoveSkillChip {
            thread_id: "a".into(),
            draft_id,
            key: key.clone(),
        });
        assert!(
            direct
                .composer_snapshot("a")
                .unwrap()
                .domain()
                .skill_selections
                .is_empty()
        );
        dispatch(ComposerCatalogIntent::RemoveSkillChip {
            thread_id: "a".into(),
            draft_id,
            key,
        });
        dispatch(ComposerCatalogIntent::ClosePicker {
            identity: identity.clone(),
        });
        dispatch(ComposerCatalogIntent::CommitPicker { identity });
    }
    #[test]
    fn message_deletion_confirmation_identity_and_failure_match_the_ffi_owner() {
        use pioneer_client::threads::message_deletion::*;
        let (direct, ffi, _leases) = shared_message_fixture();
        let dispatch = |intent: MessageDeletionIntent| {
            let intent = ClientIntent::MessageDeletion { intent };
            let expected = direct.dispatch(intent.clone());
            let actual = ffi
                .client_intent_dispatch(
                    &serde_json::json!({"schema_version":1,"intent":intent}).to_string(),
                )
                .unwrap();
            assert_eq!(
                serde_json::to_value(expected.outcome()).unwrap(),
                serde_json::to_value(actual.outcome).unwrap()
            );
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while [&direct, &ffi.core].iter().any(|core| {
                core.message_deletion_snapshot("a")
                    .is_some_and(|p| p.state == MessageDeletionState::Pending)
            }) && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            let expected = direct.message_deletion_snapshot("a");
            assert!(
                expected
                    .as_ref()
                    .is_none_or(|p| p.state != MessageDeletionState::Pending)
            );
            let actual = ffi
                .client_scoped_snapshot(
                    r#"{"schema_version":1,"scope":{"kind":"message_deletion","thread_id":"a"}}"#,
                )
                .unwrap();
            assert_eq!(
                serde_json::to_value(expected.as_deref()).unwrap(),
                actual.map(|p| p.payload).unwrap_or(serde_json::Value::Null)
            );
        };
        dispatch(MessageDeletionIntent::Begin {
            thread_id: "a".into(),
            turn_id: "turn".into(),
            expected_revision: 3,
        });
        let plan = direct.message_deletion_snapshot("a").unwrap().plan.clone();
        dispatch(MessageDeletionIntent::Begin {
            thread_id: "a".into(),
            turn_id: "turn".into(),
            expected_revision: 3,
        });
        dispatch(MessageDeletionIntent::Confirm {
            identity: plan.identity.clone(),
        });
        assert_eq!(
            direct.message_deletion_snapshot("a").unwrap().state,
            MessageDeletionState::Failed { conflicted: false }
        );
        dispatch(MessageDeletionIntent::Confirm {
            identity: plan.identity.clone(),
        });
        assert_eq!(direct.message_deletion_snapshot("a").unwrap().plan, plan);
        dispatch(MessageDeletionIntent::Cancel {
            identity: plan.identity.clone(),
        });
        dispatch(MessageDeletionIntent::Confirm {
            identity: plan.identity,
        });
        dispatch(MessageDeletionIntent::Begin {
            thread_id: "a".into(),
            turn_id: "turn".into(),
            expected_revision: 2,
        });
    }
    #[test]
    fn message_edit_publication_and_completion_match_at_the_ffi_boundary() {
        use pioneer_client::composer::store::{ComposerOperationCompletion, ComposerOperationKind};
        let (direct, ffi, _leases) = shared_message_fixture();
        let dispatch = |intent: ComposerIntent| {
            let intent = ClientIntent::Composer { intent };
            let expected = direct.dispatch(intent.clone());
            let actual = ffi
                .client_intent_dispatch(
                    &serde_json::json!({"schema_version":1,"intent":intent}).to_string(),
                )
                .unwrap();
            assert_eq!(
                serde_json::to_value(expected.outcome()).unwrap(),
                serde_json::to_value(actual.outcome).unwrap()
            );
            let expected = direct.composer_snapshot("a");
            let actual = ffi
                .client_scoped_snapshot(
                    r#"{"schema_version":1,"scope":{"kind":"composer","thread_id":"a"}}"#,
                )
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(expected.as_deref()).unwrap(),
                actual.payload
            );
        };
        dispatch(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: ComposerDomainState::default(),
        });
        let initial = direct.composer_snapshot("a").unwrap().draft_id();
        dispatch(ComposerIntent::StartMessageEdit {
            thread_id: "a".into(),
            draft_id: initial,
            turn_id: "turn".into(),
        });
        let editing = direct.composer_snapshot("a").unwrap();
        assert!(editing.message_edit().is_some());
        let draft = editing.draft_id();
        dispatch(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: draft,
            text: "edited text".into(),
        });
        dispatch(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id: draft,
            operation: ComposerOperationKind::EditMessage,
        });
        let identity = direct
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        dispatch(ComposerIntent::PrepareOperation {
            identity: identity.clone(),
        });
        dispatch(ComposerIntent::CompleteOperation {
            identity: identity.clone(),
            completion: ComposerOperationCompletion::MessageEditFailed { conflicted: true },
        });
        assert_eq!(
            direct.composer_snapshot("a").unwrap().draft().text,
            "edited text"
        );
        dispatch(ComposerIntent::CompleteOperation {
            identity: identity.clone(),
            completion: ComposerOperationCompletion::Sent,
        });
        assert!(
            direct
                .composer_snapshot("a")
                .unwrap()
                .message_edit()
                .unwrap()
                .conflicted
        );
        dispatch(ComposerIntent::StartMessageEdit {
            thread_id: "a".into(),
            draft_id: draft,
            turn_id: "turn".into(),
        });
        let draft = direct.composer_snapshot("a").unwrap().draft_id();
        dispatch(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id: draft,
            operation: ComposerOperationKind::EditMessage,
        });
        let identity = direct
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        dispatch(ComposerIntent::PrepareOperation {
            identity: identity.clone(),
        });
        dispatch(ComposerIntent::CompleteOperation {
            identity: identity.clone(),
            completion: ComposerOperationCompletion::Sent,
        });
        assert!(
            direct
                .composer_snapshot("a")
                .unwrap()
                .message_edit()
                .is_none()
        );
        dispatch(ComposerIntent::CompleteOperation {
            identity,
            completion: ComposerOperationCompletion::Sent,
        });
        direct.shutdown();
    }
}
