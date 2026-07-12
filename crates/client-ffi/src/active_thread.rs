use crate::contracts::ClientEvent;
use crate::threads::ClientThreadTreeSnapshot;
use pioneer_client::{
    ClientError, ClientResult,
    cli_runtime::approvals::reduce_pending_request_thread_closed_cleanup,
    cli_runtime::approvals::{PendingRequest, PendingRequestState},
    composer::{
        attachments::ComposerAttachment,
        capabilities::{
            ComposerCapability, ComposerCapabilityTarget, filter_composer_capabilities_for_target,
        },
        model_selection as composer_model_selection,
        turn_prepare::{
            ComposerSubmitAvailabilityInput, PrepareComposerTurnRequest,
            PrepareVoiceComposerSnapshotRequest, PreparedComposerTurnSubmitContext,
            PreparedVoiceComposerSnapshot, can_submit_composer_message,
            composer_has_sendable_content, prepare_composer_turn, prepare_voice_composer_snapshot,
            reduce_prepared_composer_turn_submit_success,
        },
    },
    conversation::ConversationViewState,
    notifications::effects::ClientEffect,
    providers::list::{
        cli_runtime_list_params, resolve_cli_runtime_execution_backend,
        runtime_id_from_cli_runtime_provider_key,
    },
    runtime::{ClientRuntime, ClientRuntimeNotification, ClientRuntimeNotificationContext},
    security::{ClientSecurityDiagnosticRow, ClientTurnSecuritySummary, security_diagnostic_rows},
    state::{reducers as client_state_reducers, selectors as client_selectors},
    threads::{coordinator::ThreadCoordinator, session as thread_session, start as thread_start},
    timeline::{
        labels::now_unix_ms,
        rows::TimelineRow,
        semantic::{
            SemanticTimelineCachePatch, SemanticTimelineState, TopLevelPageMergeMode,
            WorkPageMergeMode, apply_conversation_event_to_semantic_timeline,
            apply_conversation_event_to_semantic_timeline_with_patch,
            apply_semantic_timeline_live_update_with_patch,
            apply_thread_timeline_page as apply_semantic_thread_timeline_page,
            apply_turn_work_page as apply_semantic_turn_work_page, expand_turn_work,
            flatten_semantic_timeline, remove_thread_semantic_timeline,
        },
        semantic_render::{SEMANTIC_TURN_WORK_GROUP_PREFIX, render_semantic_timeline_rows},
    },
    transport::ws::command_sender as ws_commands,
    turns::{
        cancel as turn_cancel,
        start::{
            TurnStartSendReduction, apply_prepared_turn_to_thread_snapshot, now_unix_seconds,
            plan_turn_start_ids, reduce_turn_start_send_failure, reduce_turn_start_send_success,
            turn_start_params_from_plan,
        },
    },
};
use pioneer_protocol::TurnPermissionMode;
use pioneer_protocol::{
    AgentExecutionBackend, GatewayNotification, RuntimeSummary, Thread, ThreadGetParams,
    ThreadMode, ThreadTimelinePageResponse, TurnWorkPageResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    sync::{Arc, Mutex},
};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadOpenRequest {
    pub thread: Thread,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadOpenByIdRequest {
    pub thread_id: String,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadSnapshotRequest {
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientEnsureWorkspaceDraftRequest {
    pub workspace_id: String,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadUnsubscribeRequest {
    pub thread_id: String,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadEventRequest {
    pub event: ClientEvent,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ClientActiveThreadEventResult {
    pub snapshot: ClientActiveThreadSnapshot,
    pub semantic_timeline_patch: SemanticTimelineCachePatch,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadSendTextRequest {
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    pub text: String,
    #[serde(default)]
    pub selected_model: Option<String>,
    #[serde(default)]
    pub selected_provider: Option<String>,
    #[serde(default)]
    pub selected_reasoning_effort: Option<String>,
    #[serde(default)]
    pub selected_mode: Option<ThreadMode>,
    pub permission_mode: TurnPermissionMode,
    #[serde(default)]
    pub attachments: Vec<ComposerAttachment>,
    #[serde(default)]
    pub capabilities: Vec<ComposerCapability>,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ClientActiveThreadSendTextResult {
    pub thread_id: String,
    pub turn_id: String,
    pub pending_request_id: String,
    pub snapshot: ClientActiveThreadSnapshot,
    pub semantic_timeline_patch: SemanticTimelineCachePatch,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientPrepareVoiceComposerSnapshotRequest {
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub selected_model: Option<String>,
    #[serde(default)]
    pub selected_provider: Option<String>,
    #[serde(default)]
    pub selected_reasoning_effort: Option<String>,
    #[serde(default)]
    pub selected_mode: Option<ThreadMode>,
    pub permission_mode: TurnPermissionMode,
    #[serde(default)]
    pub attachments: Vec<ComposerAttachment>,
    #[serde(default)]
    pub capabilities: Vec<ComposerCapability>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadCancelTurnRequest {
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub expanded_keys: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ClientActiveThreadCancelTurnResult {
    pub cancelled: bool,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub snapshot: ClientActiveThreadSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ClientActiveThreadTurnSelection {
    selected_model: String,
    selected_provider: String,
    selected_mode: ThreadMode,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientActiveThreadClearResult {
    pub unsubscribed_thread_ids: Vec<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ClientActiveThreadUnsubscribeResult {
    pub unsubscribed_thread_id: String,
    pub snapshot: ClientActiveThreadSnapshot,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClientActiveThreadSnapshot {
    pub thread_id: Option<String>,
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub draft_thread_id: Option<String>,
    #[serde(default)]
    pub draft_workspace_id: Option<String>,
    #[serde(default)]
    pub last_active_thread_id: Option<String>,
    #[serde(default)]
    pub session_revision: u64,
    pub thread: Option<Thread>,
    pub history_loaded: bool,
    pub history_loading: bool,
    pub projection: ConversationViewState,
    pub rows: Vec<TimelineRow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_security_summary: Option<ClientTurnSecuritySummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_turn_security_diagnostics: Vec<ClientSecurityDiagnosticRow>,
    #[serde(default)]
    pub pending_requests: Vec<PendingRequest>,
}

#[derive(Clone, Default)]
pub struct ClientFfiActiveThreadState {
    inner: Arc<Mutex<ClientFfiActiveThreadInner>>,
}

#[derive(Default)]
struct ClientFfiActiveThreadInner {
    active_thread_id: Option<String>,
    draft_thread_by_workspace: HashMap<String, String>,
    last_active_thread_by_workspace: HashMap<String, String>,
    session_revision: u64,
    coordinators: HashMap<String, ThreadCoordinator>,
    semantic_timelines: SemanticTimelineState,
    pending_requests: PendingRequestState,
}

impl ClientFfiActiveThreadState {
    pub fn ensure_workspace_draft(
        &self,
        runtime: &ClientRuntime,
        request: ClientEnsureWorkspaceDraftRequest,
    ) -> anyhow::Result<ClientActiveThreadSnapshot> {
        let ClientEnsureWorkspaceDraftRequest {
            workspace_id,
            expanded_keys,
        } = request;
        let workspace_id = non_empty_string(Some(workspace_id))
            .ok_or_else(|| anyhow::anyhow!("workspace_id is required before starting draft"))?;

        if let Some(snapshot) =
            self.activate_workspace_draft(workspace_id.as_str(), expanded_keys.as_slice())?
        {
            return Ok(snapshot);
        }

        let planned_thread_id = thread_start::generate_thread_start_id();
        let response = ws_commands::thread_start(
            &runtime.ws_command_sender(),
            thread_start::thread_start_params(planned_thread_id, workspace_id.clone()),
        )?;
        let reduction = thread_start::reduce_thread_start_bootstrap_success(
            workspace_id.clone(),
            response,
            None,
        );
        let thread_id = reduction.thread_id.clone();

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        activate_thread(&mut inner, thread_id.as_str(), Some(workspace_id.as_str()));
        remember_workspace_draft(&mut inner, workspace_id.as_str(), Some(thread_id.clone()));
        upsert_thread_snapshot(&mut inner, reduction.thread);

        Ok(snapshot_from_inner(&inner, expanded_keys.as_slice()))
    }

    pub fn open_or_create_new_thread(
        &self,
        runtime: &ClientRuntime,
        request: ClientEnsureWorkspaceDraftRequest,
    ) -> anyhow::Result<ClientActiveThreadSnapshot> {
        self.ensure_workspace_draft(runtime, request)
    }

    fn activate_workspace_draft(
        &self,
        workspace_id: &str,
        expanded_keys: &[String],
    ) -> anyhow::Result<Option<ClientActiveThreadSnapshot>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        let known_thread_ids = inner.coordinators.keys().cloned().collect::<HashSet<_>>();
        let draft_thread_id = thread_session::resolve_remembered_thread_for_workspace(
            &mut inner.draft_thread_by_workspace,
            workspace_id,
            |thread_id| known_thread_ids.contains(thread_id),
        );

        let Some(draft_thread_id) = draft_thread_id else {
            return Ok(None);
        };

        activate_thread(&mut inner, draft_thread_id.as_str(), Some(workspace_id));
        Ok(Some(snapshot_from_inner(&inner, expanded_keys)))
    }

    pub fn open_thread(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadOpenRequest,
    ) -> anyhow::Result<ClientActiveThreadSnapshot> {
        let ClientActiveThreadOpenRequest {
            thread,
            expanded_keys,
        } = request;
        let thread_id = thread.id.clone();
        let workspace_id = thread.workspace_id.clone();

        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            activate_thread(&mut inner, thread_id.as_str(), Some(workspace_id.as_str()));
            upsert_thread_snapshot(&mut inner, thread);
        }

        self.ensure_thread_subscription(runtime, thread_id.as_str(), workspace_id.clone())?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        if let Some(coordinator) = inner.coordinators.get_mut(thread_id.as_str()) {
            coordinator.set_workspace_id(workspace_id.as_str());
        }

        Ok(snapshot_from_inner(&inner, expanded_keys.as_slice()))
    }

    pub fn open_thread_by_id(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadOpenByIdRequest,
    ) -> anyhow::Result<ClientActiveThreadSnapshot> {
        let thread_id = non_empty_string(Some(request.thread_id))
            .ok_or_else(|| anyhow::anyhow!("thread_id is required before opening thread"))?;
        let response =
            ws_commands::thread_get(&runtime.ws_command_sender(), ThreadGetParams { thread_id })?;

        self.open_thread(
            runtime,
            ClientActiveThreadOpenRequest {
                thread: response.thread,
                expanded_keys: request.expanded_keys,
            },
        )
    }

    pub fn snapshot(
        &self,
        request: ClientActiveThreadSnapshotRequest,
    ) -> anyhow::Result<ClientActiveThreadSnapshot> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        Ok(snapshot_from_inner(
            &inner,
            request.expanded_keys.as_slice(),
        ))
    }

    pub fn apply_event(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadEventRequest,
    ) -> anyhow::Result<ClientActiveThreadEventResult> {
        let semantic_timeline_patch =
            if let ClientEvent::GatewayNotification(notification) = request.event {
                self.apply_gateway_notification(runtime, notification)?
            } else {
                SemanticTimelineCachePatch::default()
            };

        let snapshot = self.snapshot(ClientActiveThreadSnapshotRequest {
            expanded_keys: request.expanded_keys,
        })?;

        Ok(ClientActiveThreadEventResult {
            snapshot,
            semantic_timeline_patch,
        })
    }

    pub fn apply_thread_tree_snapshot(
        &self,
        snapshot: &ClientThreadTreeSnapshot,
    ) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        for thread in snapshot.threads_by_id.values() {
            upsert_thread_snapshot(&mut inner, thread.clone());
        }

        Ok(())
    }

    pub fn apply_thread_timeline_page(
        &self,
        page: ThreadTimelinePageResponse,
        merge_mode: TopLevelPageMergeMode,
    ) -> anyhow::Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        Ok(apply_semantic_thread_timeline_page(
            &mut inner.semantic_timelines,
            page,
            merge_mode,
        ))
    }

    pub fn apply_turn_work_page(
        &self,
        page: TurnWorkPageResponse,
        merge_mode: WorkPageMergeMode,
    ) -> anyhow::Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        Ok(apply_semantic_turn_work_page(
            &mut inner.semantic_timelines,
            page,
            merge_mode,
        ))
    }

    pub fn resolve_composer_model_selection(
        &self,
        active_thread_id: Option<&str>,
        workspace_id: Option<&str>,
    ) -> anyhow::Result<Option<composer_model_selection::ComposerModelSelection>> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        Ok(client_selectors::resolve_composer_model_selection_from(
            active_thread_id,
            workspace_id,
            &inner.coordinators,
        ))
    }

    pub fn send_text_turn(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadSendTextRequest,
    ) -> anyhow::Result<ClientActiveThreadSendTextResult> {
        let ClientActiveThreadSendTextRequest {
            thread_id,
            workspace_id: _workspace_id,
            text,
            selected_model,
            selected_provider,
            selected_reasoning_effort,
            selected_mode,
            permission_mode,
            attachments,
            capabilities,
            expanded_keys,
        } = request;

        let thread_id = thread_session::require_thread_id(thread_id, "sending text")
            .map_err(anyhow::Error::msg)?;
        let ids = plan_turn_start_ids();
        let turn_id = ids.turn_id;
        let pending_request_id = ids.pending_request_id;
        let (workspace_id, endpoint_kind) = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            let coordinator = inner.coordinators.get(thread_id.as_str()).ok_or_else(|| {
                anyhow::anyhow!("active thread must be opened before starting turn")
            })?;
            (coordinator.workspace_id.clone(), None)
        };
        let selection = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            resolve_turn_selection(
                &inner,
                thread_id.as_str(),
                selected_provider,
                selected_model,
                selected_mode,
                text.as_str(),
                !attachments.is_empty(),
                !capabilities.is_empty(),
            )?
        };
        let execution_target = resolve_selected_execution_target(
            runtime,
            workspace_id.as_str(),
            Some(selection.selected_provider.as_str()),
        )?;
        let cli_runtime_selected = execution_target.execution_backend.is_some();
        let capabilities = filter_composer_capabilities_for_target(
            capabilities.as_slice(),
            execution_target.capability_target,
        );
        if !composer_has_sendable_content(
            text.as_str(),
            !attachments.is_empty(),
            !capabilities.is_empty(),
        ) {
            return Err(anyhow::anyhow!(
                "message content is required before starting turn"
            ));
        }
        let prepared = prepare_composer_turn(
            &runtime.ws_command_sender(),
            &ClientFfiFileSystem,
            PrepareComposerTurnRequest {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                endpoint_kind,
                text,
                attachments,
                capabilities,
            },
        )?;
        let turn_model_provider = if cli_runtime_selected {
            None
        } else {
            Some(selection.selected_provider.clone())
        };
        let submit_reduction = reduce_prepared_composer_turn_submit_success(
            PreparedComposerTurnSubmitContext {
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                pending_request_id: pending_request_id.clone(),
                selected_model: Some(selection.selected_model),
                selected_provider: Some(selection.selected_provider.clone()),
                turn_model_provider,
                selected_mode: Some(selection.selected_mode),
                permission_mode,
                execution_backend: execution_target.execution_backend,
                selected_reasoning_effort,
                cli_runtime_options: None,
                updated_at_unix: now_unix_seconds(),
            },
            prepared,
        );
        let thread_snapshot_update = submit_reduction.thread_snapshot_update.clone();
        let local_turn_start_requested_event =
            submit_reduction.local_turn_start_requested_event.clone();
        let turn_start_params =
            turn_start_params_from_plan(submit_reduction.turn_start_params_plan);

        let semantic_timeline_patch = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            activate_thread(&mut inner, thread_id.as_str(), Some(workspace_id.as_str()));
            clear_workspace_draft_markers(&mut inner, thread_id.as_str());
            let coordinator = inner
                .coordinators
                .get_mut(thread_id.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("active thread must be opened before starting turn")
                })?;
            coordinator
                .conversation
                .apply(local_turn_start_requested_event.clone());
            if let Some(thread) = coordinator.thread_mut() {
                apply_prepared_turn_to_thread_snapshot(
                    thread,
                    thread_snapshot_update.selected_model.as_deref(),
                    thread_snapshot_update.selected_provider.as_deref(),
                    thread_snapshot_update.selected_reasoning_effort.as_deref(),
                    thread_snapshot_update.user_text.as_str(),
                    thread_snapshot_update.updated_at_unix,
                );
            }
            apply_conversation_event_to_semantic_timeline_with_patch(
                &mut inner.semantic_timelines,
                workspace_id.as_str(),
                &local_turn_start_requested_event,
                now_unix_ms(),
            )
        };

        let send_context = submit_reduction.send_context;
        let ws_sender = runtime.ws_command_sender();
        let state = self.clone();
        let thread_id_for_send = thread_id.clone();
        std::thread::spawn(move || {
            let reduction = match ws_commands::turn_start(&ws_sender, turn_start_params) {
                Ok(response) => reduce_turn_start_send_success(send_context, response),
                Err(error) => reduce_turn_start_send_failure(send_context, format!("{error:#}")),
            };
            let _ = state.apply_turn_start_send_reduction(reduction, thread_id_for_send.as_str());
        });

        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        Ok(ClientActiveThreadSendTextResult {
            thread_id,
            turn_id,
            pending_request_id,
            snapshot: snapshot_from_inner(&inner, expanded_keys.as_slice()),
            semantic_timeline_patch,
        })
    }

    pub fn prepare_voice_composer_snapshot(
        &self,
        runtime: &ClientRuntime,
        request: ClientPrepareVoiceComposerSnapshotRequest,
    ) -> anyhow::Result<PreparedVoiceComposerSnapshot> {
        let ClientPrepareVoiceComposerSnapshotRequest {
            thread_id,
            workspace_id: _workspace_id,
            turn_id,
            selected_model,
            selected_provider,
            selected_reasoning_effort,
            selected_mode,
            permission_mode,
            attachments,
            capabilities,
        } = request;

        let thread_id = thread_session::require_thread_id(thread_id, "starting voice")
            .map_err(anyhow::Error::msg)?;
        let turn_id = match turn_id {
            Some(turn_id) if turn_id.trim().is_empty() => {
                return Err(anyhow::anyhow!(
                    "turn_id is required before preparing voice context"
                ));
            }
            Some(turn_id) => turn_id,
            None => plan_turn_start_ids().turn_id,
        };
        let (workspace_id, endpoint_kind) = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            let coordinator = inner.coordinators.get(thread_id.as_str()).ok_or_else(|| {
                anyhow::anyhow!("active thread must be opened before starting voice")
            })?;
            (coordinator.workspace_id.clone(), None)
        };
        let selection = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            resolve_voice_turn_selection(
                &inner,
                thread_id.as_str(),
                selected_provider,
                selected_model,
                selected_mode,
            )?
        };
        let execution_target = resolve_selected_execution_target(
            runtime,
            workspace_id.as_str(),
            Some(selection.selected_provider.as_str()),
        )?;
        let cli_runtime_selected = execution_target.execution_backend.is_some();
        let capabilities = filter_composer_capabilities_for_target(
            capabilities.as_slice(),
            execution_target.capability_target,
        );
        let turn_model_provider = if cli_runtime_selected {
            None
        } else {
            Some(selection.selected_provider.clone())
        };

        prepare_voice_composer_snapshot(
            &runtime.ws_command_sender(),
            &ClientFfiFileSystem,
            PrepareVoiceComposerSnapshotRequest {
                workspace_id,
                thread_id,
                turn_id,
                endpoint_kind,
                attachments,
                capabilities,
                selected_model: Some(selection.selected_model),
                selected_provider: Some(selection.selected_provider),
                turn_model_provider,
                selected_mode: Some(selection.selected_mode),
                permission_mode,
                execution_backend: execution_target.execution_backend,
                selected_reasoning_effort,
                cli_runtime_options: None,
            },
        )
    }

    pub fn cancel_turn(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadCancelTurnRequest,
    ) -> anyhow::Result<ClientActiveThreadCancelTurnResult> {
        let ClientActiveThreadCancelTurnRequest {
            reason,
            expanded_keys,
        } = request;
        let Some((thread_id, turn_id, params)) = self.apply_local_turn_cancel_request(reason)?
        else {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

            return Ok(ClientActiveThreadCancelTurnResult {
                cancelled: false,
                thread_id: inner.active_thread_id.clone(),
                turn_id: None,
                snapshot: snapshot_from_inner(&inner, expanded_keys.as_slice()),
            });
        };

        if let Err(error) = ws_commands::turn_cancel(&runtime.ws_command_sender(), params) {
            let message = format!("{error:#}");
            self.apply_local_turn_cancel_rejected(
                thread_id.as_str(),
                turn_id.as_str(),
                message.as_str(),
            )?;
            return Err(anyhow::anyhow!(message));
        }

        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        Ok(ClientActiveThreadCancelTurnResult {
            cancelled: true,
            thread_id: Some(thread_id),
            turn_id: Some(turn_id),
            snapshot: snapshot_from_inner(&inner, expanded_keys.as_slice()),
        })
    }

    pub fn clear(&self, runtime: &ClientRuntime) -> anyhow::Result<ClientActiveThreadClearResult> {
        let thread_ids = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            let plan = client_state_reducers::plan_gateway_switch_cleanup(
                &inner.coordinators,
                inner.active_thread_id.as_deref(),
            );
            let thread_ids = thread_ids_from_effects(plan.effects);
            clear_active_thread(&mut inner);
            inner.draft_thread_by_workspace.clear();
            inner.last_active_thread_by_workspace.clear();
            inner.coordinators.clear();
            inner.semantic_timelines = Default::default();
            inner.pending_requests = Default::default();
            thread_session::bump_session_revision(&mut inner.session_revision);
            thread_ids
        };

        let sender = runtime.ws_command_sender();
        for thread_id in &thread_ids {
            let _ = ws_commands::thread_unsubscribe(&sender, thread_id.clone());
        }

        Ok(ClientActiveThreadClearResult {
            unsubscribed_thread_ids: thread_ids,
        })
    }

    pub fn unsubscribe_or_close_thread(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadUnsubscribeRequest,
    ) -> anyhow::Result<ClientActiveThreadUnsubscribeResult> {
        let thread_id = non_empty_string(Some(request.thread_id))
            .ok_or_else(|| anyhow::anyhow!("thread_id is required before unsubscribe"))?;

        ws_commands::thread_unsubscribe(&runtime.ws_command_sender(), thread_id.clone())?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        remove_thread_session_state(&mut inner, thread_id.as_str());

        Ok(ClientActiveThreadUnsubscribeResult {
            unsubscribed_thread_id: thread_id,
            snapshot: snapshot_from_inner(&inner, request.expanded_keys.as_slice()),
        })
    }

    fn ensure_thread_subscription(
        &self,
        runtime: &ClientRuntime,
        thread_id: &str,
        workspace_id: String,
    ) -> anyhow::Result<()> {
        let response = ws_commands::thread_start(
            &runtime.ws_command_sender(),
            thread_start::thread_start_params(thread_id.to_owned(), workspace_id),
        )?;
        let reduction = thread_start::reduce_thread_start_subscription_success(response);

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        activate_thread(&mut inner, reduction.thread_id.as_str(), None);
        upsert_thread_snapshot(&mut inner, reduction.thread);

        Ok(())
    }

    fn apply_local_turn_cancel_request(
        &self,
        reason: Option<String>,
    ) -> anyhow::Result<Option<(String, String, pioneer_protocol::TurnCancelParams)>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        let Some(thread_id) = inner.active_thread_id.clone() else {
            return Ok(None);
        };
        let Some(coordinator) = inner.coordinators.get_mut(thread_id.as_str()) else {
            return Ok(None);
        };
        let Some(turn_id) = coordinator
            .conversation
            .in_flight_turn_id()
            .map(str::to_owned)
        else {
            return Ok(None);
        };
        let Some(cancel_request) = turn_cancel::plan_turn_cancel_request(
            thread_id.clone(),
            turn_id.clone(),
            coordinator.conversation.is_cancelling_turn(),
            reason,
        ) else {
            return Ok(None);
        };

        coordinator
            .conversation
            .apply(cancel_request.requested_event);

        Ok(Some((thread_id, turn_id, cancel_request.params)))
    }

    fn apply_local_turn_cancel_rejected(
        &self,
        thread_id: &str,
        turn_id: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        if let Some(coordinator) = inner.coordinators.get_mut(thread_id) {
            coordinator
                .conversation
                .apply(turn_cancel::local_turn_cancel_rejected_event(
                    thread_id.to_owned(),
                    turn_id.to_owned(),
                    error.to_owned(),
                ));
        }

        Ok(())
    }

    fn apply_gateway_notification(
        &self,
        runtime: &ClientRuntime,
        notification: GatewayNotification,
    ) -> anyhow::Result<SemanticTimelineCachePatch> {
        let (active_thread_id, active_workspace_id, notification_thread_workspace_matches) = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            let active_thread_id = inner.active_thread_id.clone();
            let active_workspace_id = active_thread_id
                .as_deref()
                .and_then(|thread_id| inner.coordinators.get(thread_id))
                .map(|coordinator| coordinator.workspace_id.clone());
            let notification_workspace_id = notification_workspace_id(&notification);
            let notification_thread_workspace_matches = match (
                notification_thread_id(&notification),
                notification_workspace_id,
            ) {
                (Some(thread_id), Some(workspace_id)) => inner
                    .coordinators
                    .get(thread_id)
                    .is_some_and(|coordinator| coordinator.workspace_id == workspace_id),
                _ => false,
            };

            (
                active_thread_id,
                active_workspace_id,
                notification_thread_workspace_matches,
            )
        };

        let context = ClientRuntimeNotificationContext {
            pending_thread_id: None,
            active_thread_id: active_thread_id.as_deref(),
            active_workspace_id: active_workspace_id.as_deref(),
            notification_thread_workspace_matches,
            ..Default::default()
        };

        let Some(reduction) = runtime.reduce_gateway_notification(notification, context) else {
            return Ok(SemanticTimelineCachePatch::default());
        };

        let semantic_timeline_patch = match reduction {
            ClientRuntimeNotification::ThreadStarted(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                upsert_thread_snapshot(&mut inner, reduction.thread);
                if let Some(thread_id) = reduction.set_active_thread_id {
                    activate_thread(&mut inner, thread_id.as_str(), None);
                }
                SemanticTimelineCachePatch::default()
            }
            ClientRuntimeNotification::TurnLifecycle(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                {
                    let coordinator = inner
                        .coordinators
                        .entry(reduction.thread_id.clone())
                        .or_insert_with(|| {
                            ThreadCoordinator::pending(
                                reduction.thread_id.as_str(),
                                reduction.workspace_id.as_str(),
                            )
                        });
                    coordinator
                        .conversation
                        .apply(reduction.conversation_event.clone());
                    if reduction.tick_conversation {
                        let _ = coordinator.conversation.tick();
                    }
                    if let Some(status) = reduction.thread_status
                        && let Some(thread) = coordinator.thread_mut()
                    {
                        thread.status = status;
                    }
                }
                if let Some(pending_reduction) = reduction.pending_requests {
                    inner.pending_requests.apply(pending_reduction);
                }
                clear_workspace_draft_markers(&mut inner, reduction.thread_id.as_str());
                apply_conversation_event_to_semantic_timeline_with_patch(
                    &mut inner.semantic_timelines,
                    reduction.workspace_id.as_str(),
                    &reduction.conversation_event,
                    now_unix_ms(),
                )
            }
            ClientRuntimeNotification::ConversationEvent(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                {
                    let coordinator = inner
                        .coordinators
                        .entry(reduction.thread_id.clone())
                        .or_insert_with(|| {
                            ThreadCoordinator::pending(
                                reduction.thread_id.as_str(),
                                reduction.workspace_id.as_str(),
                            )
                        });
                    coordinator
                        .conversation
                        .apply(reduction.conversation_event.clone());
                }
                apply_conversation_event_to_semantic_timeline_with_patch(
                    &mut inner.semantic_timelines,
                    reduction.workspace_id.as_str(),
                    &reduction.conversation_event,
                    now_unix_ms(),
                )
            }
            ClientRuntimeNotification::ThreadUpdated(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                upsert_thread_snapshot(&mut inner, reduction.thread);
                SemanticTimelineCachePatch::default()
            }
            ClientRuntimeNotification::ThreadClosed(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                if let Some(pending_reduction) = reduction.pending_requests {
                    inner.pending_requests.apply(pending_reduction);
                }
                if reduction.remove_thread_conversation {
                    remove_thread_session_state(&mut inner, reduction.thread_id.as_str());
                } else if reduction.clear_active_thread_if_matches {
                    let cleared = thread_session::clear_active_thread_if_matches(
                        &mut inner.active_thread_id,
                        reduction.thread_id.as_str(),
                    );
                    if cleared {
                        thread_session::bump_session_revision(&mut inner.session_revision);
                    }
                }
                SemanticTimelineCachePatch::default()
            }
            ClientRuntimeNotification::WorkspaceRefresh(_)
            | ClientRuntimeNotification::SkillsRefresh(_)
            | ClientRuntimeNotification::McpRefresh(_)
            | ClientRuntimeNotification::McpServerStatusChanged(_)
            | ClientRuntimeNotification::McpServerCatalogChanged(_)
            | ClientRuntimeNotification::ThreadArtifactsRefresh(_)
            | ClientRuntimeNotification::ArtifactThreadRefresh(_)
            | ClientRuntimeNotification::ArtifactDeletedRefresh(_)
            | ClientRuntimeNotification::CLIRuntimeRefresh(_)
            | ClientRuntimeNotification::GatewayRemoteAccessStatusChanged(_)
            | ClientRuntimeNotification::GatewayThreadEpisodicVectorRefillStatusChanged(_)
            | ClientRuntimeNotification::VoiceSessionResult(_)
            | ClientRuntimeNotification::WorkspaceChanged { .. } => {
                SemanticTimelineCachePatch::default()
            }
            ClientRuntimeNotification::SemanticTimeline(update) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                apply_semantic_timeline_live_update_with_patch(
                    &mut inner.semantic_timelines,
                    update,
                )
            }
            ClientRuntimeNotification::CLIRuntimePendingRequests { reduction, .. }
            | ClientRuntimeNotification::PendingRequests { reduction } => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                inner.pending_requests.apply(reduction);
                SemanticTimelineCachePatch::default()
            }
        };

        Ok(semantic_timeline_patch)
    }

    fn apply_turn_start_send_reduction(
        &self,
        reduction: TurnStartSendReduction,
        thread_id: &str,
    ) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        let workspace_id = inner
            .coordinators
            .get(thread_id)
            .ok_or_else(|| anyhow::anyhow!("active thread must be opened before starting turn"))?
            .workspace_id
            .clone();

        let events = match reduction {
            TurnStartSendReduction::Accepted { events } => events,
            TurnStartSendReduction::Rejected { event } => {
                vec![event]
            }
        };

        for event in events {
            {
                let coordinator = inner.coordinators.get_mut(thread_id).ok_or_else(|| {
                    anyhow::anyhow!("active thread must be opened before starting turn")
                })?;
                coordinator.conversation.apply(event.clone());
            }
            apply_conversation_event_to_semantic_timeline(
                &mut inner.semantic_timelines,
                workspace_id.as_str(),
                &event,
                now_unix_ms(),
            );
        }

        Ok(())
    }
}

fn thread_ids_from_effects(effects: Vec<ClientEffect>) -> Vec<String> {
    effects
        .into_iter()
        .flat_map(|effect| match effect {
            ClientEffect::UnsubscribeThreads { thread_ids } => thread_ids,
            ClientEffect::RefreshWorkspaceList
            | ClientEffect::RefreshGatewaySettings
            | ClientEffect::RefreshProviderLists
            | ClientEffect::QueueSkillsRefresh
            | ClientEffect::EnqueueInFlightTurnsForResume => Vec::new(),
        })
        .collect()
}

fn upsert_thread_snapshot(inner: &mut ClientFfiActiveThreadInner, thread: Thread) {
    inner
        .coordinators
        .entry(thread.id.clone())
        .and_modify(|coordinator| coordinator.set_snapshot(thread.clone()))
        .or_insert_with(|| ThreadCoordinator::new(thread));
}

fn activate_thread(
    inner: &mut ClientFfiActiveThreadInner,
    thread_id: &str,
    workspace_id: Option<&str>,
) {
    let changed = thread_session::set_active_thread_id(
        &mut inner.active_thread_id,
        Some(thread_id.to_owned()),
    );
    if changed {
        thread_session::bump_session_revision(&mut inner.session_revision);
    }

    if let Some(workspace_id) = workspace_id {
        remember_last_active_thread(inner, workspace_id, Some(thread_id.to_owned()));
    }
}

fn clear_active_thread(inner: &mut ClientFfiActiveThreadInner) {
    if thread_session::set_active_thread_id(&mut inner.active_thread_id, None) {
        thread_session::bump_session_revision(&mut inner.session_revision);
    }
}

fn remember_last_active_thread(
    inner: &mut ClientFfiActiveThreadInner,
    workspace_id: &str,
    thread_id: Option<String>,
) {
    if thread_session::remember_thread_for_workspace(
        &mut inner.last_active_thread_by_workspace,
        workspace_id,
        thread_id,
    ) {
        thread_session::bump_session_revision(&mut inner.session_revision);
    }
}

fn remember_workspace_draft(
    inner: &mut ClientFfiActiveThreadInner,
    workspace_id: &str,
    thread_id: Option<String>,
) {
    if thread_session::remember_thread_for_workspace(
        &mut inner.draft_thread_by_workspace,
        workspace_id,
        thread_id,
    ) {
        thread_session::bump_session_revision(&mut inner.session_revision);
    }
}

fn clear_workspace_draft_markers(inner: &mut ClientFfiActiveThreadInner, thread_id: &str) {
    if thread_session::clear_thread_markers(&mut inner.draft_thread_by_workspace, thread_id) {
        thread_session::bump_session_revision(&mut inner.session_revision);
    }
}

fn remove_thread_session_state(inner: &mut ClientFfiActiveThreadInner, thread_id: &str) {
    let workspace_id = inner
        .coordinators
        .get(thread_id)
        .map(|coordinator| coordinator.workspace_id.clone());
    let active_cleared =
        thread_session::clear_active_thread_if_matches(&mut inner.active_thread_id, thread_id);
    let draft_cleared =
        thread_session::clear_thread_markers(&mut inner.draft_thread_by_workspace, thread_id);
    let last_active_cleared =
        thread_session::clear_thread_markers(&mut inner.last_active_thread_by_workspace, thread_id);

    let removed_thread = inner.coordinators.remove(thread_id).is_some();
    let removed_timeline =
        remove_thread_semantic_timeline(&mut inner.semantic_timelines, thread_id);
    if let Some(workspace_id) = workspace_id {
        inner
            .pending_requests
            .apply(reduce_pending_request_thread_closed_cleanup(
                workspace_id,
                thread_id.to_owned(),
            ));
    }

    if active_cleared || draft_cleared || last_active_cleared || removed_thread || removed_timeline
    {
        thread_session::bump_session_revision(&mut inner.session_revision);
    }
}

fn snapshot_from_inner(
    inner: &ClientFfiActiveThreadInner,
    expanded_keys: &[String],
) -> ClientActiveThreadSnapshot {
    let Some(thread_id) = inner.active_thread_id.as_deref() else {
        return ClientActiveThreadSnapshot {
            session_revision: inner.session_revision,
            ..Default::default()
        };
    };
    let Some(coordinator) = inner.coordinators.get(thread_id) else {
        return ClientActiveThreadSnapshot {
            thread_id: Some(thread_id.to_owned()),
            session_revision: inner.session_revision,
            ..Default::default()
        };
    };
    let workspace_id = coordinator.workspace_id.clone();
    let draft_thread_id = thread_session::remembered_thread_for_workspace(
        &inner.draft_thread_by_workspace,
        &workspace_id,
    )
    .map(str::to_owned);
    let last_active_thread_id = thread_session::remembered_thread_for_workspace(
        &inner.last_active_thread_by_workspace,
        &workspace_id,
    )
    .map(str::to_owned);
    let (projection, rows) =
        render_active_thread_timeline(inner, thread_id, coordinator, expanded_keys);
    let active_turn_security_summary = active_turn_security_summary(&projection);
    let active_turn_security_diagnostics = active_turn_security_summary
        .as_ref()
        .map(security_diagnostic_rows)
        .unwrap_or_default();

    ClientActiveThreadSnapshot {
        thread_id: Some(thread_id.to_owned()),
        workspace_id: Some(workspace_id.clone()),
        draft_workspace_id: draft_thread_id.as_ref().map(|_| workspace_id.clone()),
        draft_thread_id,
        last_active_thread_id,
        session_revision: inner.session_revision,
        thread: coordinator.thread().cloned(),
        history_loaded: coordinator.history_loaded,
        history_loading: coordinator.history_loading,
        projection,
        rows,
        active_turn_security_summary,
        active_turn_security_diagnostics,
        pending_requests: inner
            .pending_requests
            .pending_for_scope(Some(coordinator.workspace_id.as_str()), Some(thread_id)),
    }
}

fn active_turn_security_summary(
    projection: &ConversationViewState,
) -> Option<ClientTurnSecuritySummary> {
    projection
        .in_flight_turn_id
        .as_deref()
        .and_then(|turn_id| projection.turn_security_summary(turn_id).cloned())
}

fn render_active_thread_timeline(
    inner: &ClientFfiActiveThreadInner,
    thread_id: &str,
    coordinator: &ThreadCoordinator,
    expanded_keys: &[String],
) -> (ConversationViewState, Vec<TimelineRow>) {
    let mut projection = coordinator.conversation.projection().clone();
    projection.items.clear();
    projection.timeline.clear();

    if let Some(thread) = coordinator.thread() {
        for turn in &thread.turns {
            projection.upsert_turn_snapshot_metadata(turn);
        }
    }

    let mut semantic_timelines = inner.semantic_timelines.clone();
    apply_expanded_turn_work_keys(&mut semantic_timelines, thread_id, expanded_keys);

    let Some(semantic_rows) = flatten_semantic_timeline(&semantic_timelines, thread_id) else {
        return (projection, Vec::new());
    };

    let model = render_semantic_timeline_rows(semantic_rows.rows.as_slice(), projection);
    (model.projection, model.rows)
}

fn apply_expanded_turn_work_keys(
    state: &mut SemanticTimelineState,
    thread_id: &str,
    expanded_keys: &[String],
) {
    for key in expanded_keys {
        let Some(turn_id) = key.strip_prefix(SEMANTIC_TURN_WORK_GROUP_PREFIX) else {
            continue;
        };
        if turn_id.is_empty() {
            continue;
        }
        expand_turn_work(state, thread_id.to_owned(), turn_id.to_owned());
    }
}

fn non_empty_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SelectedExecutionTarget {
    execution_backend: Option<AgentExecutionBackend>,
    capability_target: ComposerCapabilityTarget,
}

fn resolve_selected_execution_target(
    runtime: &ClientRuntime,
    workspace_id: &str,
    selected_provider: Option<&str>,
) -> anyhow::Result<SelectedExecutionTarget> {
    let Some(provider_key) = selected_provider
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
    else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
            capability_target: ComposerCapabilityTarget::Native,
        });
    };
    let Some(_) = runtime_id_from_cli_runtime_provider_key(provider_key) else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
            capability_target: ComposerCapabilityTarget::Native,
        });
    };

    let runtimes = ws_commands::cli_runtime_list(
        &runtime.ws_command_sender(),
        cli_runtime_list_params(workspace_id.to_owned()),
    )?
    .runtimes;

    selected_execution_target_from_runtimes(Some(provider_key), runtimes.as_slice())
}

fn selected_execution_target_from_runtimes(
    selected_provider: Option<&str>,
    runtimes: &[RuntimeSummary],
) -> anyhow::Result<SelectedExecutionTarget> {
    let Some(provider_key) = selected_provider
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
    else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
            capability_target: ComposerCapabilityTarget::Native,
        });
    };
    let Some(runtime_id) = runtime_id_from_cli_runtime_provider_key(provider_key) else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
            capability_target: ComposerCapabilityTarget::Native,
        });
    };

    let execution_backend =
        resolve_cli_runtime_execution_backend(Some(provider_key), runtimes, None)
            .map_err(anyhow::Error::msg)?;
    let selected_runtime = runtimes
        .iter()
        .find(|runtime| runtime.runtime_id == runtime_id && runtime.enabled)
        .ok_or_else(|| {
            anyhow::anyhow!("CLI runtime `{runtime_id}` is not available for message submission")
        })?;
    let capability_target = if selected_runtime.capabilities.supports_skills {
        ComposerCapabilityTarget::SkillCapableCli
    } else {
        ComposerCapabilityTarget::UnsupportedCli
    };

    Ok(SelectedExecutionTarget {
        execution_backend,
        capability_target,
    })
}

fn resolve_turn_selection(
    inner: &ClientFfiActiveThreadInner,
    thread_id: &str,
    requested_provider: Option<String>,
    requested_model: Option<String>,
    requested_mode: Option<ThreadMode>,
    text: &str,
    has_attachments: bool,
    has_capabilities: bool,
) -> anyhow::Result<ClientActiveThreadTurnSelection> {
    let requested_provider = non_empty_string(requested_provider);
    let requested_model = non_empty_string(requested_model);
    let requested_selection = match (requested_provider, requested_model) {
        (Some(provider), Some(model)) => Some(composer_model_selection::ComposerModelSelection {
            provider,
            model,
            selected_reasoning_effort: None,
        }),
        (None, None) => None,
        _ => {
            return Err(anyhow::anyhow!(
                "model and provider must both be selected before starting turn"
            ));
        }
    };
    let coordinator = inner
        .coordinators
        .get(thread_id)
        .ok_or_else(|| anyhow::anyhow!("active thread must be opened before starting turn"))?;
    let resolved_selection = match requested_selection {
        Some(selection) => selection,
        None => client_selectors::resolve_composer_model_selection_from(
            Some(thread_id),
            Some(coordinator.workspace_id.as_str()),
            &inner.coordinators,
        )
        .ok_or_else(|| {
            anyhow::anyhow!("model and provider must be selected before starting turn")
        })?,
    };
    let selected_mode =
        requested_mode.unwrap_or_else(composer_model_selection::default_composer_turn_mode);

    if !can_submit_composer_message(ComposerSubmitAvailabilityInput {
        gateway_connected: true,
        upload_in_progress: false,
        has_active_thread: true,
        has_complete_model_selection: true,
        conversation_can_submit: coordinator.conversation.can_submit_message(),
        text,
        has_attachments,
        has_capabilities,
    }) {
        return Err(anyhow::anyhow!(
            "active thread is not ready to start a new turn"
        ));
    }

    Ok(ClientActiveThreadTurnSelection {
        selected_model: resolved_selection.model,
        selected_provider: resolved_selection.provider,
        selected_mode,
    })
}

fn resolve_voice_turn_selection(
    inner: &ClientFfiActiveThreadInner,
    thread_id: &str,
    requested_provider: Option<String>,
    requested_model: Option<String>,
    requested_mode: Option<ThreadMode>,
) -> anyhow::Result<ClientActiveThreadTurnSelection> {
    let requested_provider = non_empty_string(requested_provider);
    let requested_model = non_empty_string(requested_model);
    let requested_selection = match (requested_provider, requested_model) {
        (Some(provider), Some(model)) => Some(composer_model_selection::ComposerModelSelection {
            provider,
            model,
            selected_reasoning_effort: None,
        }),
        (None, None) => None,
        _ => {
            return Err(anyhow::anyhow!(
                "model and provider must both be selected before starting voice"
            ));
        }
    };
    let coordinator = inner
        .coordinators
        .get(thread_id)
        .ok_or_else(|| anyhow::anyhow!("active thread must be opened before starting voice"))?;
    let resolved_selection = match requested_selection {
        Some(selection) => selection,
        None => client_selectors::resolve_composer_model_selection_from(
            Some(thread_id),
            Some(coordinator.workspace_id.as_str()),
            &inner.coordinators,
        )
        .ok_or_else(|| {
            anyhow::anyhow!("model and provider must be selected before starting voice")
        })?,
    };
    let selected_mode =
        requested_mode.unwrap_or_else(composer_model_selection::default_composer_turn_mode);

    if !coordinator.conversation.can_submit_message() {
        return Err(anyhow::anyhow!(
            "active thread is not ready to start a new voice turn"
        ));
    }

    Ok(ClientActiveThreadTurnSelection {
        selected_model: resolved_selection.model,
        selected_provider: resolved_selection.provider,
        selected_mode,
    })
}

#[derive(Clone, Copy, Debug)]
struct ClientFfiFileSystem;

impl pioneer_client::platform::ClientFileSystem for ClientFfiFileSystem {
    fn read_file(&self, path: &pioneer_client::platform::ClientPath) -> ClientResult<Vec<u8>> {
        fs::read(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to read `{}`: {error}",
                path.as_path().display()
            ))
        })
    }

    fn metadata(
        &self,
        path: &pioneer_client::platform::ClientPath,
    ) -> ClientResult<pioneer_client::platform::ClientFileMetadata> {
        let metadata = fs::metadata(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to stat `{}`: {error}",
                path.as_path().display()
            ))
        })?;
        Ok(pioneer_client::platform::ClientFileMetadata {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            is_file: metadata.is_file(),
            is_dir: metadata.is_dir(),
        })
    }

    fn write_cache_file(
        &self,
        _key: &str,
        _bytes: &[u8],
    ) -> ClientResult<pioneer_client::platform::ClientPath> {
        Err(ClientError::platform(
            "cache writes are not supported by composer upload filesystem adapter",
        ))
    }

    fn open_read(
        &self,
        path: &pioneer_client::platform::ClientPath,
    ) -> ClientResult<Box<dyn pioneer_client::platform::ClientFileReader>> {
        let file = fs::File::open(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to open `{}`: {error}",
                path.as_path().display()
            ))
        })?;
        Ok(Box::new(file))
    }
}

fn notification_thread_id(notification: &GatewayNotification) -> Option<&str> {
    match notification {
        GatewayNotification::ThreadStarted(notification) => Some(notification.thread.id.as_str()),
        GatewayNotification::ThreadUpdated(notification) => Some(notification.thread.id.as_str()),
        GatewayNotification::ThreadClosed(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnStarted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnCompleted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnFailed(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnBlocked(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemStarted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemDelta(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemCompleted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemUpdated(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemTimeoutDetected(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRecoveryOpened(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRecoveryAttached(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRetryScheduled(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRetryAttemptStarted(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRecoverySucceeded(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRecoveryExhausted(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemToolRetryScheduled(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemToolRetryResolved(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemToolRetryExhausted(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ThreadArtifactsChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::Unknown(notification) => notification.thread_id.as_deref(),
        _ => None,
    }
}

fn notification_workspace_id(notification: &GatewayNotification) -> Option<&str> {
    match notification {
        GatewayNotification::WorkspaceChanged(notification) => {
            Some(notification.workspace.id.as_str())
        }
        GatewayNotification::ThreadStarted(notification) => {
            Some(notification.thread.workspace_id.as_str())
        }
        GatewayNotification::ThreadUpdated(notification) => {
            Some(notification.thread.workspace_id.as_str())
        }
        GatewayNotification::ThreadClosed(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ThreadTreeChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ThreadAgentsDocChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::TurnStarted(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::TurnCompleted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::TurnFailed(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::TurnBlocked(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ItemStarted(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ItemDelta(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ItemCompleted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemUpdated(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ItemTimeoutDetected(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRecoveryOpened(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRecoveryAttached(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRetryScheduled(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRetryAttemptStarted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRecoverySucceeded(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRecoveryExhausted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemToolRetryScheduled(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemToolRetryResolved(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemToolRetryExhausted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ThreadArtifactsChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactCreated(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactUpdated(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactDeleted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactProjectionUpdated(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactUploadProgress(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactDownloadProgress(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::SkillsChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::McpChanged(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::McpServerStatusChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::McpServerCatalogChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::Unknown(notification) => notification.workspace_id.as_deref(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::cli_runtime::approvals::{PendingRequest, PendingRequestsReduction};
    use pioneer_client::conversation::reducer::{TurnPhase, TurnView};
    use pioneer_protocol::{
        CLIAgentRuntimeKind, McpScopeKind, RuntimeCapabilities, RuntimeStatus, ThreadOriginKind,
        ThreadSidebarVisibility, ThreadStatus, Turn, TurnKind, TurnOrigin,
        TurnPermissionApprovalRequest, TurnStatus,
    };
    use serde_json::json;

    fn thread(thread_id: &str, workspace_id: &str) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 2,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        }
    }

    fn running_turn(turn_id: &str) -> Turn {
        Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        }
    }

    fn pending_request(request_id: &str, workspace_id: &str, thread_id: &str) -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: request_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: "turn".to_owned(),
            visible_thread_ids: Vec::new(),
            tool_name: "exec_command".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: format!("{request_id}_scope"),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: None,
            details: Vec::new(),
        })
    }

    fn runtime_summary(id: &str, supports_skills: bool) -> RuntimeSummary {
        RuntimeSummary {
            runtime_id: id.to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
            display_name: id.to_owned(),
            enabled: true,
            status: RuntimeStatus::Ready,
            capabilities: RuntimeCapabilities {
                supports_skills,
                supports_threads: true,
                supports_model_list: true,
                ..Default::default()
            },
            account: None,
            version: None,
            binary_path: None,
            home_path: None,
            shadow_home_path: None,
            proxy_url: None,
            debug_native_events_enabled: false,
            models_refreshed_at_unix_ms: None,
            diagnostics: Vec::new(),
            recent_stderr: Vec::new(),
        }
    }

    fn capability(slug: &str, source_kind: &str) -> ComposerCapability {
        ComposerCapability {
            id: format!("skill:{source_kind}:{slug}"),
            label: slug.to_owned(),
            kind: pioneer_client::composer::capabilities::ComposerCapabilityKind::Skill {
                slug: slug.to_owned(),
                source_kind: source_kind.to_owned(),
            },
        }
    }

    fn mcp_capability() -> ComposerCapability {
        ComposerCapability {
            id: "mcp-server:workspace:docs".to_owned(),
            label: "docs".to_owned(),
            kind: pioneer_client::composer::capabilities::ComposerCapabilityKind::McpServer {
                name: "docs".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    #[test]
    fn text_and_voice_execution_targets_share_the_cli_capability_matrix() {
        let capabilities = vec![
            capability("user", "user"),
            mcp_capability(),
            capability("registry", "registry"),
            capability("system", "system"),
        ];
        let supported = selected_execution_target_from_runtimes(
            Some("cli_runtime:codex"),
            &[runtime_summary("codex", true)],
        )
        .expect("supported CLI should resolve");
        let unsupported = selected_execution_target_from_runtimes(
            Some("cli_runtime:legacy"),
            &[runtime_summary("legacy", false)],
        )
        .expect("unsupported CLI should resolve");
        let native = selected_execution_target_from_runtimes(Some("openai"), &[])
            .expect("native provider should resolve");

        let filter_for_path = |target: &SelectedExecutionTarget| {
            filter_composer_capabilities_for_target(
                capabilities.as_slice(),
                target.capability_target,
            )
        };
        let text_filtered = filter_for_path(&supported);
        let voice_filtered = filter_for_path(&supported);
        for filtered in [&text_filtered, &voice_filtered] {
            assert_eq!(
                filtered
                    .iter()
                    .map(|capability| capability.id.as_str())
                    .collect::<Vec<_>>(),
                vec!["skill:user:user", "skill:registry:registry"]
            );
            assert!(composer_has_sendable_content(
                "",
                false,
                !filtered.is_empty()
            ));
        }
        assert_eq!(text_filtered, voice_filtered);

        assert!(
            filter_composer_capabilities_for_target(
                capabilities.as_slice(),
                unsupported.capability_target,
            )
            .is_empty()
        );
        assert_eq!(
            filter_composer_capabilities_for_target(
                capabilities.as_slice(),
                native.capability_target,
            ),
            capabilities
        );
        assert!(supported.execution_backend.is_some());
        assert!(unsupported.execution_backend.is_some());
        assert!(native.execution_backend.is_none());
    }

    #[test]
    fn active_thread_send_text_requires_permission_mode() {
        let error = serde_json::from_value::<ClientActiveThreadSendTextRequest>(json!({
            "text": "hello"
        }))
        .expect_err("permission mode should be required");

        assert!(
            error
                .to_string()
                .contains("missing field `permission_mode`")
        );
    }

    #[test]
    fn active_thread_send_text_decodes_explicit_permission_mode() {
        let request: ClientActiveThreadSendTextRequest = serde_json::from_value(json!({
            "text": "hello",
            "permission_mode": "supervised"
        }))
        .expect("request decodes");

        assert_eq!(request.permission_mode, TurnPermissionMode::Supervised);
    }

    #[test]
    fn voice_prepare_request_decodes_explicit_turn_id() {
        let request: ClientPrepareVoiceComposerSnapshotRequest = serde_json::from_value(json!({
            "thread_id": "thread_a",
            "workspace_id": "ws_a",
            "turn_id": "turn_voice_a",
            "permission_mode": "supervised"
        }))
        .expect("request decodes");

        assert_eq!(request.thread_id.as_deref(), Some("thread_a"));
        assert_eq!(request.workspace_id.as_deref(), Some("ws_a"));
        assert_eq!(request.turn_id.as_deref(), Some("turn_voice_a"));
        assert_eq!(request.permission_mode, TurnPermissionMode::Supervised);
    }

    #[test]
    fn active_thread_snapshot_includes_active_scope_pending_requests() {
        let mut inner = ClientFfiActiveThreadInner {
            active_thread_id: Some("thread_a".to_owned()),
            ..Default::default()
        };
        inner.coordinators.insert(
            "thread_a".to_owned(),
            ThreadCoordinator::new(thread("thread_a", "ws_a")),
        );
        inner
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "req_a", "ws_a", "thread_a",
            )));
        inner
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "req_b", "ws_a", "thread_b",
            )));

        let snapshot = snapshot_from_inner(&inner, &[]);

        assert_eq!(snapshot.pending_requests.len(), 1);
        assert_eq!(snapshot.pending_requests[0].request_id, "req_a");
    }

    #[test]
    fn active_thread_snapshot_restores_running_composer_state_from_thread_snapshot() {
        let mut inner = ClientFfiActiveThreadInner {
            active_thread_id: Some("thread_a".to_owned()),
            ..Default::default()
        };
        let mut thread = thread("thread_a", "ws_a");
        thread.status = ThreadStatus::Active;
        thread.turns.push(running_turn("turn_a"));
        inner
            .coordinators
            .insert("thread_a".to_owned(), ThreadCoordinator::new(thread));

        let snapshot = snapshot_from_inner(&inner, &[]);

        assert!(snapshot.projection.composer_locked);
        assert_eq!(
            snapshot.projection.in_flight_turn_id.as_deref(),
            Some("turn_a")
        );
    }

    #[test]
    fn active_thread_snapshot_serializes_shared_security_summary_for_mobile() {
        let security_summary = ClientTurnSecuritySummary::from_execution_snapshot(
            &pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(
                "/repo", 1_700,
            ),
        );
        let projection = ConversationViewState {
            in_flight_turn_id: Some("turn_a".to_owned()),
            turns: vec![TurnView {
                id: "turn_a".to_owned(),
                phase: TurnPhase::Running,
                started_at_unix_ms: Some(42),
                completed_at_unix_ms: None,
                error: None,
                permission_profile: None,
                security_summary: Some(security_summary.clone()),
                resume: None,
            }],
            ..ConversationViewState::default()
        };

        let snapshot = ClientActiveThreadSnapshot {
            active_turn_security_summary: active_turn_security_summary(&projection),
            active_turn_security_diagnostics: security_diagnostic_rows(&security_summary),
            projection,
            ..ClientActiveThreadSnapshot::default()
        };
        let encoded = serde_json::to_value(&snapshot).expect("snapshot serializes");

        assert_eq!(
            encoded["active_turn_security_summary"]["permission_mode"],
            "full_access"
        );
        assert_eq!(
            encoded["active_turn_security_summary"]["sandbox_mode"],
            "unrestricted"
        );
        assert_eq!(
            encoded["active_turn_security_summary"]["filesystem_access"],
            "unrestricted"
        );
        assert_eq!(
            encoded["active_turn_security_summary"]["enforcement"],
            "active"
        );
        let encoded_text = encoded.to_string();
        assert!(!encoded_text.contains("danger"));
        assert!(!encoded_text.contains("bypass"));
    }

    #[test]
    fn active_thread_snapshot_serializes_security_diagnostics_for_mobile() {
        let mut security_snapshot = pioneer_protocol::TurnExecutionSecuritySnapshot::read_only(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            "/repo",
            vec![
                pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                    pioneer_protocol::TurnFilesystemAccess::Read,
                    "/repo",
                ),
            ],
            1,
        );
        security_snapshot.backend = pioneer_protocol::TurnSecurityBackendSnapshot {
            execution_backend: pioneer_protocol::TurnSecurityExecutionBackendKind::ClaudeCli,
            sandbox_backend: None,
            provider: Some("claude".to_owned()),
            capabilities: pioneer_protocol::BackendSecurityCapabilities {
                can_enforce_filesystem: false,
                can_enforce_network: false,
                can_enforce_process: false,
                supports_turn_scope_approval: true,
                supports_session_scope_approval: false,
                supports_request_permissions: true,
            },
        };
        security_snapshot.enforcement =
            pioneer_protocol::TurnSecurityEnforcementStatus::PartiallyActive {
                degraded: vec![pioneer_protocol::TurnSecurityDegradation {
                    capability: pioneer_protocol::TurnSecurityCapabilityKind::Filesystem,
                    reason: "detailed filesystem sandbox is not provider-enforced".to_owned(),
                }],
            };
        let security_summary =
            ClientTurnSecuritySummary::from_execution_snapshot(&security_snapshot);
        let diagnostics = security_diagnostic_rows(&security_summary);

        let snapshot = ClientActiveThreadSnapshot {
            active_turn_security_summary: Some(security_summary),
            active_turn_security_diagnostics: diagnostics,
            ..ClientActiveThreadSnapshot::default()
        };
        let encoded = serde_json::to_value(&snapshot).expect("snapshot serializes");

        assert_eq!(
            encoded["active_turn_security_summary"]["enforcement"],
            "degraded"
        );
        assert_eq!(
            encoded["active_turn_security_diagnostics"][0]["label"],
            "Filesystem sandbox"
        );
        assert_eq!(
            encoded["active_turn_security_diagnostics"][0]["message"],
            "detailed filesystem sandbox is not provider-enforced"
        );
    }
}
