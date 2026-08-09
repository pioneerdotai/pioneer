use crate::contracts::ClientEvent;
use crate::threads::ClientThreadTreeSnapshot;
use pioneer_client::{
    ClientError, ClientResult,
    administration::{AdministrationEventTracker, AdministrationRefetch},
    authorization::{ThreadAuthorizationScope, plan_access_changed},
    cli_runtime::approvals::{
        PendingRequest, PendingRequestState, PendingRequestsReduction,
        reduce_pending_request_thread_closed_cleanup,
    },
    composer::{
        attachments::ComposerAttachment,
        capabilities::{ComposerCapability, plan_composer_submission},
        model_selection as composer_model_selection,
        skill_selection::{ComposerSkillPickerProjection, ComposerSkillSelection},
        turn_prepare::{
            ComposerSubmitAvailabilityInput, PrepareComposerTurnRequest,
            PrepareVoiceComposerSnapshotRequest, PreparedComposerTurnSubmitContext,
            PreparedVoiceComposerSnapshot, can_submit_composer_message, prepare_composer_turn,
            prepare_voice_composer_snapshot, reduce_prepared_composer_turn_submit_success,
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
        render_fingerprint::render_fingerprint_hex,
        rows::TimelineRow,
        semantic::{
            DEFAULT_TOP_LEVEL_PAGE_LIMIT, DEFAULT_TURN_WORK_PAGE_LIMIT, SemanticTimelineCachePatch,
            SemanticTimelineLiveUpdate, SemanticTimelineState, TopLevelPageMergeMode,
            WorkPageMergeMode, apply_conversation_event_to_semantic_timeline,
            apply_conversation_event_to_semantic_timeline_with_patch,
            apply_local_composer_event_to_semantic_timeline_with_patch,
            apply_semantic_timeline_live_update_with_patch,
            apply_thread_timeline_page as apply_semantic_thread_timeline_page,
            apply_turn_work_items_get_response as apply_semantic_turn_work_items_get_response,
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
use pioneer_protocol::{
    AccessChangeKind, AccessChangedNotification, AgentExecutionBackend, GatewayNotification,
    PrincipalId, RuntimeSummary, Thread, ThreadGetParams, ThreadMode, ThreadTimelinePageParams,
    ThreadTimelinePageResponse, TimelinePageAnchor, TurnWorkItemsGetParams,
    TurnWorkItemsGetResponse, TurnWorkPageParams, TurnWorkPageResponse,
};
use pioneer_protocol::{ThreadComposerExecutionMode, TurnPermissionMode};
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
    pub visibility: Option<pioneer_protocol::ThreadVisibility>,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientActiveThreadEventResult {
    pub snapshot: ClientActiveThreadSnapshot,
    pub semantic_timeline_patch: SemanticTimelineCachePatch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_changed: Option<ClientAccessChangedLifecycle>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub administration_refetch: Vec<AdministrationRefetch>,
}

/// Payload-safe bridge projection of the shared Rust access-change plan.
///
/// This lifecycle DTO omits thread identifiers and protected cache keys.
/// First-party shells may pair it with the minimal `AccessChangedNotification`
/// to evict an exact thread cache after access has been lost.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientAccessChangedLifecycle {
    pub authorization_revision: u64,
    pub workspace_id: String,
    pub change: AccessChangeKind,
    pub applied: bool,
    pub active_scope_cleared: bool,
    pub active_thread_cleared: bool,
    pub refresh_workspace_catalog: bool,
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
    #[serde(default)]
    pub reply_to_turn_id: Option<String>,
    #[serde(default)]
    pub mentioned_principal_ids: Vec<PrincipalId>,
    pub permission_mode: TurnPermissionMode,
    #[serde(default)]
    pub attachments: Vec<ComposerAttachment>,
    #[serde(default)]
    pub capabilities: Vec<ComposerCapability>,
    #[serde(default)]
    pub skill_selections: Vec<ComposerSkillSelection>,
    #[serde(default)]
    pub skill_picker: ComposerSkillPickerProjection,
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
    #[serde(default)]
    pub skill_selections: Vec<ComposerSkillSelection>,
    #[serde(default)]
    pub skill_picker: ComposerSkillPickerProjection,
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
    selected_model: Option<String>,
    selected_provider: Option<String>,
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
    #[serde(default)]
    pub row_render_fingerprints: HashMap<String, String>,
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
    authorization_revision: Option<u64>,
    coordinators: HashMap<String, ThreadCoordinator>,
    semantic_timelines: SemanticTimelineState,
    pending_requests: PendingRequestState,
    administration_events: AdministrationEventTracker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SemanticTimelineReconcileRequest {
    ThreadNewest {
        thread_id: String,
        merge_mode: TopLevelPageMergeMode,
    },
    TurnWorkNewest {
        thread_id: String,
        turn_id: String,
        merge_mode: WorkPageMergeMode,
    },
    TurnWorkItems {
        thread_id: String,
        turn_id: String,
        work_item_ids: Vec<String>,
    },
}

#[derive(Default)]
struct ClientFfiNotificationReduction {
    semantic_timeline_patch: SemanticTimelineCachePatch,
    access_changed: Option<ClientAccessChangedLifecycle>,
    administration_refetch: Vec<AdministrationRefetch>,
}

impl ClientFfiActiveThreadState {
    /// Starts a new server authorization epoch without touching endpoint or
    /// device-session state owned by the outer runtime.
    ///
    /// A reconnect can miss access-change notifications emitted while the
    /// transport was down. Protected projections therefore cannot survive as
    /// readable cache across the boundary; the shell reloads them from
    /// current-ACL endpoints after the connection becomes ready.
    pub fn begin_authorization_epoch(&self) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        clear_authorization_derived_state(&mut inner);
        Ok(())
    }

    pub fn ensure_workspace_draft(
        &self,
        runtime: &ClientRuntime,
        request: ClientEnsureWorkspaceDraftRequest,
    ) -> anyhow::Result<ClientActiveThreadSnapshot> {
        let ClientEnsureWorkspaceDraftRequest {
            workspace_id,
            visibility,
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
            thread_start::thread_create_params(
                planned_thread_id,
                workspace_id.clone(),
                visibility.unwrap_or(pioneer_protocol::ThreadVisibility::Private),
            ),
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
        // Workspace notifications can update an inactive parent while its child is active.
        // Return the affected thread projection so every UI cache stays live without
        // changing the native active-thread selection.
        let affected_thread_id = match &request.event {
            ClientEvent::GatewayNotification(notification) => {
                notification_thread_id(notification).map(str::to_owned)
            }
            _ => None,
        };
        let notification_reduction =
            if let ClientEvent::GatewayNotification(notification) = request.event {
                self.apply_gateway_notification(runtime, notification)?
            } else {
                ClientFfiNotificationReduction::default()
            };

        let snapshot = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            let thread_id = affected_thread_id
                .as_deref()
                .or(inner.active_thread_id.as_deref());
            snapshot_for_thread_from_inner(&inner, thread_id, request.expanded_keys.as_slice())
        };

        Ok(ClientActiveThreadEventResult {
            snapshot,
            semantic_timeline_patch: notification_reduction.semantic_timeline_patch,
            access_changed: notification_reduction.access_changed,
            administration_refetch: notification_reduction.administration_refetch,
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

    pub fn apply_turn_work_items_get_response(
        &self,
        response: TurnWorkItemsGetResponse,
    ) -> anyhow::Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        Ok(apply_semantic_turn_work_items_get_response(
            &mut inner.semantic_timelines,
            response,
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
            workspace_id: requested_workspace_id,
            text,
            selected_model,
            selected_provider,
            selected_reasoning_effort,
            selected_mode,
            reply_to_turn_id,
            mentioned_principal_ids,
            permission_mode,
            attachments,
            capabilities,
            skill_selections,
            skill_picker,
            expanded_keys,
        } = request;

        let message_requested = selected_mode == Some(ThreadMode::Message);
        let message_has_execution_overrides = message_requested
            && [
                selected_model.as_deref(),
                selected_provider.as_deref(),
                selected_reasoning_effort.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|value| !value.trim().is_empty());
        if message_has_execution_overrides {
            return Err(anyhow::anyhow!(
                "Message does not accept model, provider, or reasoning overrides"
            ));
        }
        if message_requested && (!capabilities.is_empty() || !skill_selections.is_empty()) {
            return Err(anyhow::anyhow!(
                "Message does not accept execution capabilities"
            ));
        }

        let thread_id = thread_session::require_thread_id(thread_id, "sending text")
            .map_err(anyhow::Error::msg)?;
        let ids = plan_turn_start_ids();
        let turn_id = ids.turn_id;
        let pending_request_id = ids.pending_request_id;
        let (workspace_id, endpoint_kind, composer_execution_mode) = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            let coordinator = inner.coordinators.get(thread_id.as_str()).ok_or_else(|| {
                anyhow::anyhow!("active thread must be opened before starting turn")
            })?;
            (
                coordinator.workspace_id.clone(),
                None,
                coordinator
                    .thread()
                    .map(|thread| thread.origin_kind.composer_execution_mode())
                    .unwrap_or(ThreadComposerExecutionMode::ForegroundTurn),
            )
        };
        if let Some(requested_workspace_id) = requested_workspace_id.as_deref() {
            if requested_workspace_id != workspace_id {
                return Err(anyhow::anyhow!(
                    "active thread workspace `{workspace_id}` does not match composer workspace `{requested_workspace_id}`"
                ));
            }
        }

        // A planned access-token rotation replaces the WebSocket connection. The
        // replacement connection has no thread subscriptions, even though the
        // client-side thread coordinator is still warm. Re-establish the
        // authoritative workspace scope and subscription before any composer
        // preparation or optimistic local mutation.
        self.ensure_thread_subscription(runtime, thread_id.as_str(), workspace_id.clone())?;

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
                !capabilities.is_empty() || !skill_selections.is_empty(),
            )?
        };
        let is_message = selection.selected_mode == ThreadMode::Message;
        let execution_target = if is_message {
            SelectedExecutionTarget {
                execution_backend: None,
            }
        } else {
            resolve_selected_execution_target(
                runtime,
                workspace_id.as_str(),
                selection.selected_provider.as_deref(),
            )?
        };
        let cli_runtime_selected = execution_target.execution_backend.is_some();
        let submission = plan_composer_submission(
            selection.selected_provider.as_deref(),
            text.as_str(),
            !attachments.is_empty(),
            capabilities.as_slice(),
        );
        if !submission.has_composer_payload && skill_selections.is_empty() {
            return Err(anyhow::anyhow!(
                "message content is required before starting turn"
            ));
        }
        let capabilities = submission.capabilities;
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
                skill_selections,
                skill_picker,
            },
        )?;
        let turn_model_provider = if is_message || cli_runtime_selected {
            None
        } else {
            selection.selected_provider.clone()
        };
        let submit_reduction = reduce_prepared_composer_turn_submit_success(
            PreparedComposerTurnSubmitContext {
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                pending_request_id: pending_request_id.clone(),
                composer_execution_mode,
                selected_model: selection.selected_model,
                selected_provider: selection.selected_provider,
                turn_model_provider,
                selected_mode: Some(selection.selected_mode),
                reply_to_turn_id,
                mentioned_principal_ids,
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
                    selection.selected_mode,
                    thread_snapshot_update.selected_model.as_deref(),
                    thread_snapshot_update.selected_provider.as_deref(),
                    thread_snapshot_update.selected_reasoning_effort.as_deref(),
                    thread_snapshot_update.user_text.as_str(),
                    thread_snapshot_update.updated_at_unix,
                );
            }
            apply_local_composer_event_to_semantic_timeline_with_patch(
                &mut inner.semantic_timelines,
                workspace_id.as_str(),
                &local_turn_start_requested_event,
                submit_reduction.composer_execution_mode,
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
            skill_selections,
            skill_picker,
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
        let message_mode = selection.selected_mode == ThreadMode::Message;
        let selected_provider = if message_mode {
            None
        } else {
            Some(selection.selected_provider.ok_or_else(|| {
                anyhow::anyhow!("model and provider must be selected before starting voice")
            })?)
        };
        let selected_model = if message_mode {
            None
        } else {
            Some(selection.selected_model.ok_or_else(|| {
                anyhow::anyhow!("model and provider must be selected before starting voice")
            })?)
        };
        let execution_backend = if message_mode {
            None
        } else {
            resolve_selected_execution_target(
                runtime,
                workspace_id.as_str(),
                selected_provider.as_deref(),
            )?
            .execution_backend
        };
        let cli_runtime_selected = execution_backend.is_some();
        let capabilities = if message_mode {
            Vec::new()
        } else {
            plan_composer_submission(
                selected_provider.as_deref(),
                "",
                !attachments.is_empty(),
                capabilities.as_slice(),
            )
            .capabilities
        };
        let turn_model_provider = if cli_runtime_selected {
            None
        } else {
            selected_provider.clone()
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
                skill_selections,
                skill_picker,
                selected_model,
                selected_provider,
                turn_model_provider,
                selected_mode: Some(selection.selected_mode),
                permission_mode,
                execution_backend,
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

        let response = match ws_commands::turn_cancel(&runtime.ws_command_sender(), params) {
            Ok(response) => response,
            Err(error) => {
                let message = format!("{error:#}");
                self.apply_local_turn_cancel_rejected(
                    thread_id.as_str(),
                    turn_id.as_str(),
                    message.as_str(),
                )?;
                return Err(anyhow::anyhow!(message));
            }
        };

        if let Some(event) = turn_cancel::turn_cancel_response_event(response) {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            let workspace_id = inner
                .coordinators
                .get(thread_id.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("active thread must be opened before cancelling turn")
                })?
                .workspace_id
                .clone();
            let coordinator = inner
                .coordinators
                .get_mut(thread_id.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("active thread must be opened before cancelling turn")
                })?;
            coordinator.conversation.apply(event.clone());
            apply_conversation_event_to_semantic_timeline(
                &mut inner.semantic_timelines,
                workspace_id.as_str(),
                &event,
                now_unix_ms(),
            );
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
            clear_authorization_derived_state(&mut inner)
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

    pub(crate) fn ensure_thread_subscription(
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
    ) -> anyhow::Result<ClientFfiNotificationReduction> {
        if let GatewayNotification::AccessChanged(notification) = notification {
            let access_changed = self.apply_access_changed(runtime, &notification)?;
            return Ok(ClientFfiNotificationReduction {
                access_changed: Some(access_changed),
                ..Default::default()
            });
        }

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
            return Ok(ClientFfiNotificationReduction::default());
        };

        let semantic_timeline_patch = match reduction {
            ClientRuntimeNotification::AccessChanged(notification) => {
                let access_changed = self.apply_access_changed(runtime, &notification)?;
                return Ok(ClientFfiNotificationReduction {
                    access_changed: Some(access_changed),
                    ..Default::default()
                });
            }
            ClientRuntimeNotification::AdministrationChanged(event) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                let invalidation = inner.administration_events.apply_event(&event);
                return Ok(ClientFfiNotificationReduction {
                    administration_refetch: invalidation.effects,
                    ..Default::default()
                });
            }
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
            | ClientRuntimeNotification::ThreadParticipantsChanged(_)
            | ClientRuntimeNotification::CLIRuntimeRefresh(_)
            | ClientRuntimeNotification::GatewayRemoteAccessStatusChanged(_)
            | ClientRuntimeNotification::GatewayThreadEpisodicVectorRefillStatusChanged(_)
            | ClientRuntimeNotification::GatewayVoiceInputStatusChanged(_)
            | ClientRuntimeNotification::VoiceSessionResult(_)
            | ClientRuntimeNotification::WorkspaceChanged { .. } => {
                SemanticTimelineCachePatch::default()
            }
            ClientRuntimeNotification::SemanticTimeline(update) => {
                // Keep inactive parent timelines current while a child thread is on screen.
                // Desktop performs the same reconciliation in its semantic request controller;
                // the FFI boundary must own it for mobile instead of relying on screen-local caches.
                let reconcile_requests = semantic_timeline_reconcile_requests(&update);
                let patch = {
                    let mut inner = self
                        .inner
                        .lock()
                        .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                    apply_semantic_timeline_live_update_with_patch(
                        &mut inner.semantic_timelines,
                        update,
                    )
                };
                self.reconcile_semantic_timeline(runtime, reconcile_requests);
                patch
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

        Ok(ClientFfiNotificationReduction {
            semantic_timeline_patch,
            access_changed: None,
            administration_refetch: Vec::new(),
        })
    }

    fn apply_access_changed(
        &self,
        runtime: &ClientRuntime,
        notification: &AccessChangedNotification,
    ) -> anyhow::Result<ClientAccessChangedLifecycle> {
        let plan = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            let active_workspace_id = inner
                .active_thread_id
                .as_deref()
                .and_then(|thread_id| inner.coordinators.get(thread_id))
                .map(|coordinator| coordinator.workspace_id.clone());
            let known_threads = inner
                .coordinators
                .iter()
                .map(|(thread_id, coordinator)| ThreadAuthorizationScope {
                    thread_id: thread_id.clone(),
                    workspace_id: coordinator.workspace_id.clone(),
                })
                .collect::<Vec<_>>();
            let plan = plan_access_changed(
                notification,
                inner.authorization_revision,
                active_workspace_id.as_deref(),
                inner.active_thread_id.as_deref(),
                known_threads.as_slice(),
            );

            if plan.apply {
                inner.authorization_revision = Some(plan.authorization_revision);
                let previous_session_revision = inner.session_revision;

                for thread_id in &plan.invalidate_thread_ids {
                    remove_thread_session_state(&mut inner, thread_id.as_str());
                }
                let workspace_access_lost = plan.change
                    == pioneer_protocol::AccessChangeKind::WorkspaceMembership
                    && notification.access_lost != Some(false);
                let draft_removed = workspace_access_lost
                    && inner
                        .draft_thread_by_workspace
                        .remove(plan.workspace_id.as_str())
                        .is_some();
                let last_active_removed = workspace_access_lost
                    && inner
                        .last_active_thread_by_workspace
                        .remove(plan.workspace_id.as_str())
                        .is_some();
                if plan.clear_active_thread {
                    clear_active_thread(&mut inner);
                }
                if workspace_access_lost {
                    inner
                        .pending_requests
                        .apply(PendingRequestsReduction::ClearWorkspace {
                            workspace_id: plan.workspace_id.clone(),
                        });
                }

                if (draft_removed || last_active_removed)
                    && inner.session_revision == previous_session_revision
                {
                    thread_session::bump_session_revision(&mut inner.session_revision);
                }
            }

            plan
        };

        if plan.apply {
            let sender = runtime.ws_command_sender();
            for thread_id in &plan.invalidate_thread_ids {
                // Access has already been removed from local state. The remote
                // unsubscribe is deliberately best-effort because Gateway ACL
                // filtering remains authoritative after revocation.
                let _ = ws_commands::thread_unsubscribe(&sender, thread_id.clone());
            }
        }

        Ok(ClientAccessChangedLifecycle {
            authorization_revision: plan.authorization_revision,
            workspace_id: plan.workspace_id,
            change: plan.change,
            applied: plan.apply,
            active_scope_cleared: plan.clear_active_workspace,
            active_thread_cleared: plan.clear_active_thread,
            refresh_workspace_catalog: plan
                .effects
                .iter()
                .any(|effect| matches!(effect, ClientEffect::RefreshWorkspaceList)),
        })
    }

    fn reconcile_semantic_timeline(
        &self,
        runtime: &ClientRuntime,
        requests: Vec<SemanticTimelineReconcileRequest>,
    ) {
        for request in requests {
            match request {
                SemanticTimelineReconcileRequest::ThreadNewest {
                    thread_id,
                    merge_mode,
                } => {
                    let Ok(page) = ws_commands::thread_timeline_page(
                        &runtime.ws_command_sender(),
                        ThreadTimelinePageParams {
                            thread_id,
                            anchor: TimelinePageAnchor::Newest,
                            limit: Some(DEFAULT_TOP_LEVEL_PAGE_LIMIT),
                        },
                    ) else {
                        continue;
                    };
                    let _ = self.apply_thread_timeline_page(page, merge_mode);
                }
                SemanticTimelineReconcileRequest::TurnWorkNewest {
                    thread_id,
                    turn_id,
                    merge_mode,
                } => {
                    let Ok(page) = ws_commands::turn_work_page(
                        &runtime.ws_command_sender(),
                        TurnWorkPageParams {
                            thread_id,
                            turn_id,
                            anchor: TimelinePageAnchor::Newest,
                            limit: Some(DEFAULT_TURN_WORK_PAGE_LIMIT),
                        },
                    ) else {
                        continue;
                    };
                    let _ = self.apply_turn_work_page(page, merge_mode);
                }
                SemanticTimelineReconcileRequest::TurnWorkItems {
                    thread_id,
                    turn_id,
                    work_item_ids,
                } => {
                    let Ok(response) = ws_commands::turn_work_items_get(
                        &runtime.ws_command_sender(),
                        TurnWorkItemsGetParams {
                            thread_id,
                            turn_id,
                            work_item_ids,
                        },
                    ) else {
                        continue;
                    };
                    let _ = self.apply_turn_work_items_get_response(response);
                }
            }
        }
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

fn semantic_timeline_reconcile_requests(
    update: &SemanticTimelineLiveUpdate,
) -> Vec<SemanticTimelineReconcileRequest> {
    match update {
        SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(notification) => {
            vec![SemanticTimelineReconcileRequest::ThreadNewest {
                thread_id: notification.thread_id.clone(),
                merge_mode: TopLevelPageMergeMode::Merge,
            }]
        }
        SemanticTimelineLiveUpdate::TurnWorkItemsChanged(notification) => {
            let mut work_item_ids = notification.changed_work_item_ids.clone();
            work_item_ids.sort();
            work_item_ids.dedup();

            let mut requests = Vec::with_capacity(2);
            if !work_item_ids.is_empty() {
                requests.push(SemanticTimelineReconcileRequest::TurnWorkItems {
                    thread_id: notification.thread_id.clone(),
                    turn_id: notification.turn_id.clone(),
                    work_item_ids,
                });
            }
            requests.push(SemanticTimelineReconcileRequest::TurnWorkNewest {
                thread_id: notification.thread_id.clone(),
                turn_id: notification.turn_id.clone(),
                merge_mode: WorkPageMergeMode::MergeAfter,
            });
            requests
        }
        SemanticTimelineLiveUpdate::TurnWorkStateChanged(notification) => {
            vec![SemanticTimelineReconcileRequest::TurnWorkNewest {
                thread_id: notification.thread_id.clone(),
                turn_id: notification.turn_id.clone(),
                merge_mode: WorkPageMergeMode::MergeAfter,
            }]
        }
    }
}

fn clear_authorization_derived_state(inner: &mut ClientFfiActiveThreadInner) -> Vec<String> {
    let plan = client_state_reducers::plan_gateway_switch_cleanup(
        &inner.coordinators,
        inner.active_thread_id.as_deref(),
    );
    let thread_ids = thread_ids_from_effects(plan.effects);
    clear_active_thread(inner);
    inner.draft_thread_by_workspace.clear();
    inner.last_active_thread_by_workspace.clear();
    inner.coordinators.clear();
    inner.semantic_timelines = Default::default();
    inner.pending_requests = Default::default();
    inner.administration_events = AdministrationEventTracker::default();
    inner.authorization_revision = None;
    thread_session::bump_session_revision(&mut inner.session_revision);
    thread_ids
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
    snapshot_for_thread_from_inner(inner, inner.active_thread_id.as_deref(), expanded_keys)
}

fn snapshot_for_thread_from_inner(
    inner: &ClientFfiActiveThreadInner,
    thread_id: Option<&str>,
    expanded_keys: &[String],
) -> ClientActiveThreadSnapshot {
    let Some(thread_id) = thread_id else {
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
    let (projection, rows, row_render_fingerprints) =
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
        row_render_fingerprints,
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
) -> (
    ConversationViewState,
    Vec<TimelineRow>,
    HashMap<String, String>,
) {
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
        return (projection, Vec::new(), HashMap::new());
    };

    let model = render_semantic_timeline_rows(semantic_rows.rows.as_slice(), projection);
    (
        model.projection,
        model.rows,
        model
            .row_render_fingerprints
            .into_iter()
            .map(|(key, fingerprint)| (key, render_fingerprint_hex(fingerprint)))
            .collect(),
    )
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
        });
    };
    let Some(_) = runtime_id_from_cli_runtime_provider_key(provider_key) else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
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
        });
    };
    let Some(_) = runtime_id_from_cli_runtime_provider_key(provider_key) else {
        return Ok(SelectedExecutionTarget {
            execution_backend: None,
        });
    };

    let execution_backend =
        resolve_cli_runtime_execution_backend(Some(provider_key), runtimes, None)
            .map_err(anyhow::Error::msg)?;
    Ok(SelectedExecutionTarget { execution_backend })
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
    let selected_mode =
        requested_mode.unwrap_or_else(composer_model_selection::default_composer_turn_mode);
    let coordinator = inner
        .coordinators
        .get(thread_id)
        .ok_or_else(|| anyhow::anyhow!("active thread must be opened before starting turn"))?;

    if selected_mode == ThreadMode::Message {
        if !can_submit_composer_message(ComposerSubmitAvailabilityInput {
            gateway_connected: true,
            upload_in_progress: false,
            has_active_thread: true,
            selected_mode,
            has_complete_model_selection: true,
            // Message is an instant-completed Turn and never claims the
            // foreground execution slot held by Chat/Agent.
            conversation_can_submit: true,
            text,
            has_attachments,
            has_capabilities,
        }) {
            return Err(anyhow::anyhow!(
                "active thread is not ready to start a new turn"
            ));
        }

        return Ok(ClientActiveThreadTurnSelection {
            selected_model: None,
            selected_provider: None,
            selected_mode,
        });
    }

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
    if !can_submit_composer_message(ComposerSubmitAvailabilityInput {
        gateway_connected: true,
        upload_in_progress: false,
        has_active_thread: true,
        selected_mode,
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
        selected_model: Some(resolved_selection.model),
        selected_provider: Some(resolved_selection.provider),
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
    let selected_mode =
        requested_mode.unwrap_or_else(composer_model_selection::default_composer_turn_mode);
    let coordinator = inner
        .coordinators
        .get(thread_id)
        .ok_or_else(|| anyhow::anyhow!("active thread must be opened before starting voice"))?;
    if selected_mode == ThreadMode::Message {
        return Ok(ClientActiveThreadTurnSelection {
            selected_model: None,
            selected_provider: None,
            selected_mode,
        });
    }

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
    if !coordinator.conversation.can_submit_message() {
        return Err(anyhow::anyhow!(
            "active thread is not ready to start a new voice turn"
        ));
    }

    Ok(ClientActiveThreadTurnSelection {
        selected_model: Some(resolved_selection.model),
        selected_provider: Some(resolved_selection.provider),
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
        GatewayNotification::ThreadTimelineBlocksChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::TurnStarted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnCompleted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnFailed(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnBlocked(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnWorkItemsChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::TurnWorkStateChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
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
        CLIAgentRuntimeKind, McpScopeKind, RuntimeCapabilities, RuntimeStatus, SkillId,
        ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
        ThreadTimelineBlocksChangedNotification, TimelineChangeReason, Turn, TurnKind, TurnOrigin,
        TurnPermissionApprovalRequest, TurnStatus, TurnWorkBlock, TurnWorkItemsChangedNotification,
        TurnWorkPresentation, TurnWorkState, TurnWorkStateChangedNotification,
    };
    use serde_json::json;

    fn skill_id(seed: &str) -> SkillId {
        let mut value = seed
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .take(21)
            .collect::<String>();
        while value.len() < 21 {
            value.push('X');
        }
        SkillId::new(value).expect("test skill id")
    }

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
            visibility: None,
            turns: Vec::new(),
        }
    }

    fn running_turn(turn_id: &str) -> Turn {
        Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
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

    fn pending_request(request_id: &str, workspace_id: &str, thread_id: &str) -> PendingRequest {
        pending_request_visible_in(request_id, workspace_id, thread_id, &[])
    }

    fn pending_request_visible_in(
        request_id: &str,
        workspace_id: &str,
        thread_id: &str,
        visible_thread_ids: &[&str],
    ) -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: request_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: "turn".to_owned(),
            visible_thread_ids: visible_thread_ids
                .iter()
                .map(|thread_id| (*thread_id).to_owned())
                .collect(),
            tool_name: "exec_command".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: format!("{request_id}_scope"),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: None,
            details: Vec::new(),
        })
    }

    fn runtime_summary(
        id: &str,
        supports_skills: bool,
        supports_mcp_tools: bool,
        status: RuntimeStatus,
    ) -> RuntimeSummary {
        RuntimeSummary {
            runtime_id: id.to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
            display_name: id.to_owned(),
            enabled: true,
            status,
            capabilities: RuntimeCapabilities {
                supports_skills,
                supports_mcp_tools,
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
        let skill_id = skill_id(format!("{source_kind}{slug}").as_str());
        ComposerCapability {
            id: pioneer_protocol::skill_capability_key(&skill_id),
            label: slug.to_owned(),
            kind: pioneer_client::composer::capabilities::ComposerCapabilityKind::Skill {
                skill_id,
                owner: None,
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

    fn mcp_tool_capability() -> ComposerCapability {
        ComposerCapability {
            id: "mcp-tool:workspace:docs:search".to_owned(),
            label: "docs / search".to_owned(),
            kind: pioneer_client::composer::capabilities::ComposerCapabilityKind::McpTool {
                server_name: "docs".to_owned(),
                raw_tool_name: "search".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    #[test]
    fn text_and_voice_submission_preserves_capabilities_across_catalog_readiness_states() {
        let capabilities = vec![
            capability("user", "user"),
            mcp_capability(),
            capability("registry", "registry"),
            capability("browser", "system"),
            capability("unknown", "future"),
            mcp_tool_capability(),
        ];
        let cases = [
            (
                "phase-zero",
                runtime_summary("phase-zero", false, false, RuntimeStatus::Ready),
            ),
            (
                "skills-only-catalog",
                runtime_summary("skills", true, false, RuntimeStatus::Ready),
            ),
            (
                "mcp-only-catalog",
                runtime_summary("mcp", false, true, RuntimeStatus::Ready),
            ),
            (
                "live-degraded",
                runtime_summary(
                    "combined",
                    true,
                    true,
                    RuntimeStatus::Degraded {
                        message: "non-MCP diagnostic".to_owned(),
                    },
                ),
            ),
        ];
        let expected_ids = [0, 1, 2, 3, 5]
            .into_iter()
            .map(|index| capabilities[index].id.clone())
            .collect::<Vec<_>>();

        for (case, runtime) in cases {
            let provider = format!("cli_runtime:{}", runtime.runtime_id);
            let target = selected_execution_target_from_runtimes(
                Some(provider.as_str()),
                std::slice::from_ref(&runtime),
            )
            .unwrap_or_else(|error| panic!("{case} CLI should resolve: {error:#}"));
            let text_plan = plan_composer_submission(
                Some(provider.as_str()),
                "typed",
                false,
                capabilities.as_slice(),
            );
            let voice_plan = plan_composer_submission(
                Some(provider.as_str()),
                "",
                true,
                capabilities.as_slice(),
            );

            for plan in [&text_plan, &voice_plan] {
                assert_eq!(
                    plan.capabilities
                        .iter()
                        .map(|capability| capability.id.clone())
                        .collect::<Vec<_>>(),
                    expected_ids.clone(),
                    "{case}"
                );
                assert_eq!(plan.removed.len(), 1, "{case}");
                assert_eq!(plan.removed[0].capability.id, capabilities[4].id, "{case}");
            }
            assert_eq!(text_plan.capabilities, voice_plan.capabilities, "{case}");
            assert_eq!(text_plan.removed, voice_plan.removed, "{case}");
            assert_eq!(
                target.execution_backend.is_some(),
                true,
                "{case} must retain CLI execution backend"
            );
        }

        let stale_runtime = runtime_summary(
            "stale",
            true,
            true,
            RuntimeStatus::Error {
                message: "probe stale".to_owned(),
            },
        );
        let stale = selected_execution_target_from_runtimes(
            Some("cli_runtime:stale"),
            std::slice::from_ref(&stale_runtime),
        )
        .expect("stale runtime still resolves its execution backend");
        let stale_plan = plan_composer_submission(
            Some("cli_runtime:stale"),
            "typed",
            false,
            capabilities.as_slice(),
        );
        assert!(stale.execution_backend.is_some());
        assert_eq!(
            stale_plan
                .capabilities
                .iter()
                .map(|capability| capability.id.clone())
                .collect::<Vec<_>>(),
            expected_ids
        );
        assert_eq!(
            stale_plan
                .removed
                .iter()
                .map(|removed| removed.reason)
                .collect::<Vec<_>>(),
            vec![
                pioneer_client::composer::capabilities::ComposerCapabilityRemovalReason::SkillSourceNotExportable,
            ]
        );

        let native = selected_execution_target_from_runtimes(Some("openai"), &[])
            .expect("native provider should resolve");
        assert_eq!(
            plan_composer_submission(Some("openai"), "typed", false, capabilities.as_slice(),)
                .capabilities,
            capabilities
        );
        assert!(native.execution_backend.is_none());

        let missing = selected_execution_target_from_runtimes(Some("cli_runtime:missing"), &[]);
        assert!(missing.is_err());
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
    fn semantic_block_change_reconciles_parent_timeline_even_when_it_is_not_active() {
        let notification = ThreadTimelineBlocksChangedNotification {
            workspace_id: "ws_a".to_owned(),
            thread_id: "parent_thread".to_owned(),
            changed_block_ids: vec!["parent_answer".to_owned()],
            removed_block_ids: Vec::new(),
            before_cursor: None,
            after_cursor: None,
            reason: TimelineChangeReason::LiveEvent,
        };
        let requests = semantic_timeline_reconcile_requests(
            &SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(notification.clone()),
        );

        assert_eq!(
            requests,
            vec![SemanticTimelineReconcileRequest::ThreadNewest {
                thread_id: "parent_thread".to_owned(),
                merge_mode: TopLevelPageMergeMode::Merge,
            }]
        );

        let mut inner = ClientFfiActiveThreadInner {
            active_thread_id: Some("child_thread".to_owned()),
            ..Default::default()
        };
        inner.coordinators.insert(
            "parent_thread".to_owned(),
            ThreadCoordinator::new(thread("parent_thread", "ws_a")),
        );
        inner.coordinators.insert(
            "child_thread".to_owned(),
            ThreadCoordinator::new(thread("child_thread", "ws_a")),
        );
        let event = GatewayNotification::ThreadTimelineBlocksChanged(notification);
        let affected_thread_id = notification_thread_id(&event);
        let snapshot = snapshot_for_thread_from_inner(&inner, affected_thread_id, &[]);

        assert_eq!(affected_thread_id, Some("parent_thread"));
        assert_eq!(snapshot.thread_id.as_deref(), Some("parent_thread"));
        assert_eq!(inner.active_thread_id.as_deref(), Some("child_thread"));
    }

    #[test]
    fn semantic_work_change_reconciles_parent_item_and_work_state() {
        let requests = semantic_timeline_reconcile_requests(
            &SemanticTimelineLiveUpdate::TurnWorkItemsChanged(TurnWorkItemsChangedNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "parent_thread".to_owned(),
                turn_id: "parent_turn".to_owned(),
                changed_work_item_ids: vec!["task_anchor".to_owned(), "task_anchor".to_owned()],
                removed_work_item_ids: Vec::new(),
                before_cursor: None,
                after_cursor: None,
                reason: TimelineChangeReason::LiveEvent,
            }),
        );

        assert_eq!(
            requests,
            vec![
                SemanticTimelineReconcileRequest::TurnWorkItems {
                    thread_id: "parent_thread".to_owned(),
                    turn_id: "parent_turn".to_owned(),
                    work_item_ids: vec!["task_anchor".to_owned()],
                },
                SemanticTimelineReconcileRequest::TurnWorkNewest {
                    thread_id: "parent_thread".to_owned(),
                    turn_id: "parent_turn".to_owned(),
                    merge_mode: WorkPageMergeMode::MergeAfter,
                },
            ]
        );
    }

    #[test]
    fn semantic_work_state_reconciliation_preserves_loaded_work_range() {
        let requests = semantic_timeline_reconcile_requests(
            &SemanticTimelineLiveUpdate::TurnWorkStateChanged(TurnWorkStateChangedNotification {
                workspace_id: "ws_a".to_owned(),
                thread_id: "parent_thread".to_owned(),
                turn_id: "parent_turn".to_owned(),
                source_high_watermark: 2,
                projection_updated_at_unix_micros: 2,
                work: TurnWorkBlock {
                    turn_id: "parent_turn".to_owned(),
                    presentation: TurnWorkPresentation::ExpandedLive,
                    state: TurnWorkState::Running,
                    started_at_unix_ms: Some(1),
                    completed_at_unix_ms: None,
                    elapsed_ms: Some(1),
                    work_count: 100,
                    visible_work_count: 100,
                    hidden_work_count: 0,
                    has_more_before: true,
                    has_more_after: false,
                    before_cursor: None,
                    after_cursor: None,
                    first_work_item_id: Some("work_000".to_owned()),
                    last_work_item_id: Some("work_099".to_owned()),
                },
                reason: TimelineChangeReason::LiveEvent,
            }),
        );

        assert_eq!(
            requests,
            vec![SemanticTimelineReconcileRequest::TurnWorkNewest {
                thread_id: "parent_thread".to_owned(),
                turn_id: "parent_turn".to_owned(),
                merge_mode: WorkPageMergeMode::MergeAfter,
            }]
        );
    }

    #[test]
    fn active_thread_send_text_decodes_explicit_permission_mode() {
        let request: ClientActiveThreadSendTextRequest = serde_json::from_value(json!({
            "text": "hello",
            "selected_mode": "Message",
            "reply_to_turn_id": "parent-turn",
            "mentioned_principal_ids": ["P00000000000000000001"],
            "permission_mode": "supervised"
        }))
        .expect("request decodes");

        assert_eq!(request.permission_mode, TurnPermissionMode::Supervised);
        assert_eq!(request.selected_mode, Some(ThreadMode::Message));
        assert_eq!(request.reply_to_turn_id.as_deref(), Some("parent-turn"));
        assert_eq!(
            request
                .mentioned_principal_ids
                .iter()
                .map(PrincipalId::as_str)
                .collect::<Vec<_>>(),
            vec!["P00000000000000000001"]
        );
    }

    #[test]
    fn message_turn_selection_needs_no_model_or_provider() {
        let mut inner = ClientFfiActiveThreadInner::default();
        let mut message_thread = thread("thread_a", "ws_a");
        message_thread.model.clear();
        message_thread.model_provider.clear();
        let mut coordinator = ThreadCoordinator::new(message_thread);
        coordinator.conversation.apply(
            pioneer_client::conversation::ConversationEvent::TurnStarted {
                thread_id: "thread_a".to_owned(),
                turn: running_turn("running_agent"),
            },
        );
        assert!(!coordinator.conversation.can_submit_message());
        inner
            .coordinators
            .insert("thread_a".to_owned(), coordinator);

        let selection = resolve_turn_selection(
            &inner,
            "thread_a",
            None,
            None,
            Some(ThreadMode::Message),
            "hello",
            false,
            false,
        )
        .expect("Message selection");

        assert_eq!(selection.selected_mode, ThreadMode::Message);
        assert!(selection.selected_model.is_none());
        assert!(selection.selected_provider.is_none());
    }

    #[test]
    fn voice_composer_allows_message_mode_without_provider() {
        let mut inner = ClientFfiActiveThreadInner::default();
        inner.coordinators.insert(
            "thread_a".to_owned(),
            ThreadCoordinator::new(thread("thread_a", "ws_a")),
        );
        let selection =
            resolve_voice_turn_selection(&inner, "thread_a", None, None, Some(ThreadMode::Message))
                .expect("Message voice mode should not require a model");

        assert_eq!(selection.selected_mode, ThreadMode::Message);
        assert!(selection.selected_model.is_none());
        assert!(selection.selected_provider.is_none());
    }

    #[test]
    fn text_and_voice_requests_round_trip_pack_skill_selections() {
        let selection = json!({
            "kind": "skill_pack",
            "pack_id": "PPPPPPPPPPPPPPPPPPPPP"
        });
        let picker = json!({
            "standalone": [],
            "packs": [{
                "key": "skill_pack:PPPPPPPPPPPPPPPPPPPPP",
                "pack_id": "PPPPPPPPPPPPPPPPPPPPP",
                "label": "writer-pack",
                "children": [],
                "selectable": false
            }]
        });

        let text: ClientActiveThreadSendTextRequest = serde_json::from_value(json!({
            "text": "hello",
            "permission_mode": "supervised",
            "skill_selections": [selection.clone()],
            "skill_picker": picker.clone()
        }))
        .expect("text request");
        let voice: ClientPrepareVoiceComposerSnapshotRequest = serde_json::from_value(json!({
            "permission_mode": "supervised",
            "skill_selections": [selection],
            "skill_picker": picker
        }))
        .expect("voice request");

        assert_eq!(text.skill_selections, voice.skill_selections);
        assert_eq!(text.skill_picker, voice.skill_picker);
        assert!(matches!(
            text.skill_selections.as_slice(),
            [ComposerSkillSelection::SkillPack { .. }]
        ));
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
    fn access_change_uses_shared_plan_and_clears_only_the_affected_workspace() {
        let state = ClientFfiActiveThreadState::default();
        {
            let mut inner = state.inner.lock().expect("active thread state");
            inner.active_thread_id = Some("thread_revoked".to_owned());
            inner.coordinators.insert(
                "thread_allowed".to_owned(),
                ThreadCoordinator::new(thread("thread_allowed", "workspace_allowed")),
            );
            inner.coordinators.insert(
                "thread_revoked".to_owned(),
                ThreadCoordinator::new(thread("thread_revoked", "workspace_revoked")),
            );
            inner
                .draft_thread_by_workspace
                .insert("workspace_revoked".to_owned(), "thread_revoked".to_owned());
            inner
                .last_active_thread_by_workspace
                .insert("workspace_allowed".to_owned(), "thread_allowed".to_owned());
            inner
                .semantic_timelines
                .thread_mut("thread_allowed".to_owned());
            inner
                .semantic_timelines
                .thread_mut("thread_revoked".to_owned());
            inner
                .pending_requests
                .apply(PendingRequestsReduction::Opened(pending_request(
                    "request_allowed",
                    "workspace_allowed",
                    "thread_allowed",
                )));
            inner
                .pending_requests
                .apply(PendingRequestsReduction::Opened(pending_request(
                    "request_revoked",
                    "workspace_revoked",
                    "thread_revoked",
                )));
        }

        let lifecycle = state
            .apply_access_changed(
                &ClientRuntime::default(),
                &AccessChangedNotification {
                    authorization_revision: 7,
                    workspace_id: "workspace_revoked".to_owned(),
                    thread_id: None,
                    access_lost: None,
                    change: AccessChangeKind::WorkspaceMembership,
                },
            )
            .expect("access change");

        assert!(lifecycle.applied);
        assert!(lifecycle.active_scope_cleared);
        assert!(lifecycle.refresh_workspace_catalog);
        let inner = state.inner.lock().expect("active thread state");
        assert_eq!(inner.authorization_revision, Some(7));
        assert!(inner.active_thread_id.is_none());
        assert!(inner.coordinators.contains_key("thread_allowed"));
        assert!(!inner.coordinators.contains_key("thread_revoked"));
        assert!(
            inner
                .semantic_timelines
                .threads_by_id
                .contains_key("thread_allowed")
        );
        assert!(
            !inner
                .semantic_timelines
                .threads_by_id
                .contains_key("thread_revoked")
        );
        assert_eq!(inner.pending_requests.requests().len(), 1);
        assert_eq!(
            inner.pending_requests.requests()[0].workspace_id,
            "workspace_allowed"
        );
        assert_eq!(
            inner
                .last_active_thread_by_workspace
                .get("workspace_allowed")
                .map(String::as_str),
            Some("thread_allowed")
        );

        let encoded = serde_json::to_value(&lifecycle).expect("bridge lifecycle JSON");
        let encoded_text = encoded.to_string();
        assert!(!encoded_text.contains("thread_revoked"));
        assert!(!encoded_text.contains("request_revoked"));
        assert!(!encoded_text.contains("protected"));
        assert_eq!(
            serde_json::from_value::<ClientAccessChangedLifecycle>(encoded)
                .expect("bridge lifecycle round-trip"),
            lifecycle
        );
    }

    #[test]
    fn thread_access_change_clears_active_thread_without_clearing_workspace_scope() {
        let state = ClientFfiActiveThreadState::default();
        {
            let mut inner = state.inner.lock().expect("active thread state");
            inner.active_thread_id = Some("thread_revoked".to_owned());
            inner.coordinators.insert(
                "thread_revoked".to_owned(),
                ThreadCoordinator::new(thread("thread_revoked", "workspace_affected")),
            );
            inner.coordinators.insert(
                "thread_kept".to_owned(),
                ThreadCoordinator::new(thread("thread_kept", "workspace_affected")),
            );
            inner
                .draft_thread_by_workspace
                .insert("workspace_affected".to_owned(), "thread_kept".to_owned());
            inner
                .last_active_thread_by_workspace
                .insert("workspace_affected".to_owned(), "thread_kept".to_owned());
            inner
                .pending_requests
                .apply(PendingRequestsReduction::Opened(pending_request(
                    "request_revoked",
                    "workspace_affected",
                    "thread_revoked",
                )));
            inner
                .pending_requests
                .apply(PendingRequestsReduction::Opened(pending_request(
                    "request_kept",
                    "workspace_affected",
                    "thread_kept",
                )));
        }

        let lifecycle = state
            .apply_access_changed(
                &ClientRuntime::default(),
                &AccessChangedNotification {
                    authorization_revision: 8,
                    workspace_id: "workspace_affected".to_owned(),
                    thread_id: Some("thread_revoked".to_owned()),
                    access_lost: None,
                    change: AccessChangeKind::ThreadParticipantRemoved,
                },
            )
            .expect("thread access change");

        assert!(lifecycle.applied);
        assert!(!lifecycle.active_scope_cleared);
        assert!(lifecycle.active_thread_cleared);
        assert!(!lifecycle.refresh_workspace_catalog);
        let inner = state.inner.lock().expect("active thread state");
        assert_eq!(inner.authorization_revision, Some(8));
        assert!(inner.active_thread_id.is_none());
        assert!(!inner.coordinators.contains_key("thread_revoked"));
        assert!(inner.coordinators.contains_key("thread_kept"));
        assert_eq!(
            inner
                .draft_thread_by_workspace
                .get("workspace_affected")
                .map(String::as_str),
            Some("thread_kept")
        );
        assert_eq!(
            inner
                .last_active_thread_by_workspace
                .get("workspace_affected")
                .map(String::as_str),
            Some("thread_kept")
        );
        assert_eq!(inner.pending_requests.requests().len(), 1);
        assert_eq!(
            inner.pending_requests.requests()[0].thread_id.as_deref(),
            Some("thread_kept")
        );
    }

    #[test]
    fn retained_visibility_change_keeps_active_thread_and_projection() {
        let state = ClientFfiActiveThreadState::default();
        {
            let mut inner = state.inner.lock().expect("active thread state");
            inner.authorization_revision = Some(8);
            inner.active_thread_id = Some("thread_current".to_owned());
            inner.coordinators.insert(
                "thread_current".to_owned(),
                ThreadCoordinator::new(thread("thread_current", "workspace_current")),
            );
        }

        let lifecycle = state
            .apply_access_changed(
                &ClientRuntime::default(),
                &AccessChangedNotification {
                    authorization_revision: 9,
                    workspace_id: "workspace_current".to_owned(),
                    thread_id: Some("thread_current".to_owned()),
                    access_lost: Some(false),
                    change: AccessChangeKind::ThreadVisibility,
                },
            )
            .expect("retained visibility change");

        assert!(lifecycle.applied);
        assert!(!lifecycle.active_scope_cleared);
        assert!(!lifecycle.active_thread_cleared);
        assert!(!lifecycle.refresh_workspace_catalog);
        let inner = state.inner.lock().expect("active thread state");
        assert_eq!(inner.authorization_revision, Some(9));
        assert_eq!(inner.active_thread_id.as_deref(), Some("thread_current"));
        assert!(inner.coordinators.contains_key("thread_current"));
    }

    #[test]
    fn stale_access_change_is_reported_without_mutating_ffi_state() {
        let state = ClientFfiActiveThreadState::default();
        {
            let mut inner = state.inner.lock().expect("active thread state");
            inner.authorization_revision = Some(9);
            inner.active_thread_id = Some("thread_current".to_owned());
            inner.coordinators.insert(
                "thread_current".to_owned(),
                ThreadCoordinator::new(thread("thread_current", "workspace_current")),
            );
        }

        let lifecycle = state
            .apply_access_changed(
                &ClientRuntime::default(),
                &AccessChangedNotification {
                    authorization_revision: 8,
                    workspace_id: "workspace_current".to_owned(),
                    thread_id: Some("thread_current".to_owned()),
                    access_lost: None,
                    change: AccessChangeKind::ThreadVisibility,
                },
            )
            .expect("stale access change");

        assert!(!lifecycle.applied);
        assert!(!lifecycle.active_scope_cleared);
        assert!(!lifecycle.refresh_workspace_catalog);
        let inner = state.inner.lock().expect("active thread state");
        assert_eq!(inner.authorization_revision, Some(9));
        assert_eq!(inner.active_thread_id.as_deref(), Some("thread_current"));
        assert!(inner.coordinators.contains_key("thread_current"));
    }

    #[test]
    fn new_authorization_epoch_drops_protected_cache_but_not_outer_session_state() {
        let state = ClientFfiActiveThreadState::default();
        {
            let mut inner = state.inner.lock().expect("active thread state");
            inner.authorization_revision = Some(91);
            inner.active_thread_id = Some("thread_protected".to_owned());
            inner.draft_thread_by_workspace.insert(
                "workspace_protected".to_owned(),
                "thread_protected".to_owned(),
            );
            inner.last_active_thread_by_workspace.insert(
                "workspace_protected".to_owned(),
                "thread_protected".to_owned(),
            );
            inner.coordinators.insert(
                "thread_protected".to_owned(),
                ThreadCoordinator::new(thread("thread_protected", "workspace_protected")),
            );
            inner
                .semantic_timelines
                .thread_mut("thread_protected".to_owned());
            inner
                .pending_requests
                .apply(PendingRequestsReduction::Opened(pending_request(
                    "request_protected",
                    "workspace_protected",
                    "thread_protected",
                )));
        }

        state
            .begin_authorization_epoch()
            .expect("begin authorization epoch");

        let inner = state.inner.lock().expect("active thread state");
        assert!(inner.authorization_revision.is_none());
        assert!(inner.active_thread_id.is_none());
        assert!(inner.draft_thread_by_workspace.is_empty());
        assert!(inner.last_active_thread_by_workspace.is_empty());
        assert!(inner.coordinators.is_empty());
        assert!(inner.semantic_timelines.threads_by_id.is_empty());
        assert!(inner.pending_requests.requests().is_empty());
    }

    #[test]
    fn active_root_snapshot_includes_grandchild_native_permission_request() {
        let mut inner = ClientFfiActiveThreadInner {
            active_thread_id: Some("root_thread".to_owned()),
            ..Default::default()
        };
        inner.coordinators.insert(
            "root_thread".to_owned(),
            ThreadCoordinator::new(thread("root_thread", "ws_a")),
        );
        inner
            .pending_requests
            .apply(PendingRequestsReduction::Opened(
                pending_request_visible_in(
                    "req_grandchild",
                    "ws_a",
                    "grandchild_thread",
                    &["child_thread", "root_thread"],
                ),
            ));

        let snapshot = snapshot_from_inner(&inner, &[]);

        assert_eq!(snapshot.pending_requests.len(), 1);
        assert_eq!(snapshot.pending_requests[0].request_id, "req_grandchild");
        assert_eq!(
            snapshot.pending_requests[0].thread_id.as_deref(),
            Some("grandchild_thread")
        );
    }

    #[test]
    fn collaborative_parent_optimistic_snapshot_does_not_flash_foreground_running_row() {
        let mut collaborative = thread("thread_a", "ws_a");
        collaborative.origin_kind = ThreadOriginKind::Collaborative;
        let mut inner = ClientFfiActiveThreadInner {
            active_thread_id: Some("thread_a".to_owned()),
            ..Default::default()
        };
        inner
            .coordinators
            .insert("thread_a".to_owned(), ThreadCoordinator::new(collaborative));
        let event = pioneer_client::conversation::ConversationEvent::LocalTurnStartRequested {
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            pending_request_id: "pending_a".to_owned(),
            mode: ThreadMode::Agent,
            user_text: "start detached work".to_owned(),
            attachments: Vec::new(),
        };
        inner
            .coordinators
            .get_mut("thread_a")
            .expect("coordinator")
            .conversation
            .apply(event.clone());
        assert!(
            apply_local_composer_event_to_semantic_timeline_with_patch(
                &mut inner.semantic_timelines,
                "ws_a",
                &event,
                ThreadComposerExecutionMode::DetachedTask,
                10,
            )
            .changed_blocks
            .iter()
            .all(|block| !matches!(
                &block.kind,
                pioneer_protocol::TimelineBlockKind::TurnWork { .. }
            ))
        );

        let snapshot = snapshot_from_inner(&inner, &[]);
        assert_eq!(snapshot.row_render_fingerprints.len(), snapshot.rows.len());
        assert!(
            snapshot
                .row_render_fingerprints
                .values()
                .all(|fingerprint| fingerprint.len() == 16)
        );
        assert!(
            snapshot.rows.iter().all(|row| !matches!(
                &row.kind,
                pioneer_client::timeline::rows::TimelineRowKind::RunningTurn(_)
            )),
            "mobile boundary must expose the optimistic user row without a foreground running row"
        );
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
