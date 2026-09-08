use crate::contracts::ClientEvent;
#[cfg(test)]
use pioneer_client::composer::capabilities::{ComposerCapability, plan_composer_submission};
#[cfg(test)]
use pioneer_client::composer::workflow::{
    resolve_turn_selection, resolve_voice_turn_selection, selected_execution_target_from_runtimes,
};
use pioneer_client::{
    ClientError, ClientResult,
    administration::{AdministrationEventTracker, AdministrationRefetch},
    cli_runtime::approvals::PendingRequest,
    composer::{
        model_selection as composer_model_selection, turn_prepare::PreparedVoiceComposerSnapshot,
    },
    conversation::ConversationViewState,
    notifications::effects::ClientEffect,
    runtime::{ClientRuntime, ClientRuntimeNotification, ClientRuntimeNotificationContext},
    security::{ClientSecurityDiagnosticRow, ClientTurnSecuritySummary, security_diagnostic_rows},
    state::selectors as client_selectors,
    threads::coordinator::ThreadCoordinator,
    timeline::{rows::TimelineRow, semantic::SemanticTimelineCachePatch},
    transport::ws::command_sender as ws_commands,
};
use pioneer_protocol::{
    AccessChangeKind, AccessChangedNotification, GatewayNotification, Thread, ThreadGetParams,
};
#[cfg(test)]
use pioneer_protocol::{RuntimeSummary, ThreadComposerExecutionMode};
#[cfg(test)]
use pioneer_protocol::{ThreadMode, TurnPermissionMode};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
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
    pub thread_id: Option<String>,
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
    /// A live hint that the durable exact-recipient Task inbox changed. The
    /// shell reconciles the inbox through the list RPC, including reconnect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_user_notification: Option<pioneer_protocol::TaskUserNotificationDeliveredNotification>,
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
    pub operation: pioneer_client::composer::store::ComposerOperationIdentity,
    #[serde(default)]
    pub workspace_id: Option<String>,
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
    pub operation: pioneer_client::composer::store::ComposerOperationIdentity,
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
    pub semantic_timeline_patch: SemanticTimelineCachePatch,
    pub domain_revision: u64,
    pub timeline_revision: u64,
}

#[derive(Clone)]
pub struct ClientFfiActiveThreadState {
    core: Arc<pioneer_client::core::ClientCore>,
    administration_events: Arc<Mutex<AdministrationEventTracker>>,
}
impl ClientFfiActiveThreadState {
    pub(crate) fn new(core: Arc<pioneer_client::core::ClientCore>) -> Self {
        Self {
            core,
            administration_events: Default::default(),
        }
    }
}
impl Default for ClientFfiActiveThreadState {
    fn default() -> Self {
        Self::new(pioneer_client::core::ClientCore::shared())
    }
}

#[derive(Default)]
struct ClientFfiNotificationReduction {
    semantic_timeline_patch: SemanticTimelineCachePatch,
    access_changed: Option<ClientAccessChangedLifecycle>,
    administration_refetch: Vec<AdministrationRefetch>,
    task_user_notification: Option<pioneer_protocol::TaskUserNotificationDeliveredNotification>,
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
        self.core.clear_thread_stores();
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

        let id = self.core.create_workspace_thread_draft(
            &runtime.ws_command_sender(),
            &workspace_id,
            visibility.unwrap_or(pioneer_protocol::ThreadVisibility::Private),
        )?;
        Ok(snapshot_for_thread_from_inner(
            &self.core,
            Some(&id),
            &expanded_keys,
        ))
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
        let Some(id) = self.core.thread_workspace_draft(workspace_id) else {
            return Ok(None);
        };
        self.core.activate_thread(Some(&id), Some(workspace_id));
        Ok(Some(snapshot_from_inner(&self.core, expanded_keys)))
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
            let inner = self.core.as_ref();
            activate_thread(inner, thread_id.as_str(), Some(workspace_id.as_str()));
            upsert_thread_snapshot(inner, thread);
        }

        self.ensure_thread_subscription(runtime, thread_id.as_str(), workspace_id.clone())?;

        let inner = self.core.as_ref();
        if let Some(mut coordinator) = inner.existing_thread_mutation(thread_id.as_str()) {
            coordinator.set_workspace_id(workspace_id.as_str());
        }

        Ok(snapshot_for_thread_from_inner(
            inner,
            Some(&thread_id),
            expanded_keys.as_slice(),
        ))
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
        let inner = self.core.as_ref();

        Ok(snapshot_for_thread_from_inner(
            inner,
            request
                .thread_id
                .as_deref()
                .or(inner.active_thread_id().as_deref()),
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
        let (affected_thread_id, visible_thread_ids) = match &request.event {
            ClientEvent::GatewayNotification(notification) => (
                notification_thread_id(notification).map(str::to_owned),
                notification_visible_thread_ids(notification).to_vec(),
            ),
            _ => (None, Vec::new()),
        };
        let notification_reduction =
            if let ClientEvent::GatewayNotification(notification) = request.event {
                self.apply_gateway_notification(runtime, notification)?
            } else {
                ClientFfiNotificationReduction::default()
            };

        let snapshot = {
            let inner = self.core.as_ref();
            let thread_id = notification_snapshot_thread_id(
                inner.active_thread_id().as_deref(),
                affected_thread_id.as_deref(),
                visible_thread_ids.as_slice(),
            );
            snapshot_for_thread_from_inner(
                inner,
                thread_id.as_deref(),
                request.expanded_keys.as_slice(),
            )
        };

        Ok(ClientActiveThreadEventResult {
            snapshot,
            semantic_timeline_patch: notification_reduction.semantic_timeline_patch,
            access_changed: notification_reduction.access_changed,
            administration_refetch: notification_reduction.administration_refetch,
            task_user_notification: notification_reduction.task_user_notification,
        })
    }

    pub fn resolve_composer_model_selection(
        &self,
        active_thread_id: Option<&str>,
        workspace_id: Option<&str>,
    ) -> anyhow::Result<Option<composer_model_selection::ComposerModelSelection>> {
        let inner = self.core.as_ref();

        Ok(client_selectors::resolve_composer_model_selection_from(
            active_thread_id,
            workspace_id,
            &inner.thread_coordinator_snapshots(),
        ))
    }

    pub fn send_text_turn(
        &self,
        _runtime: &ClientRuntime,
        request: ClientActiveThreadSendTextRequest,
    ) -> anyhow::Result<ClientActiveThreadSendTextResult> {
        let result = self.core.submit_composer_send(
            request.operation,
            &ClientFfiFileSystem,
            pioneer_client::composer::workflow::ComposerSendContext {
                workspace_id: request.workspace_id,
                endpoint_kind: None,
                failure_message: "Failed to send message".to_owned(),
            },
        )?;
        Ok(ClientActiveThreadSendTextResult {
            thread_id: result.thread_id,
            turn_id: result.turn_id,
            pending_request_id: result.pending_request_id,
            semantic_timeline_patch: result.semantic_timeline_patch,
            snapshot: snapshot_from_inner(self.core.as_ref(), &request.expanded_keys),
        })
    }

    pub fn prepare_voice_composer_snapshot(
        &self,
        _runtime: &ClientRuntime,
        request: ClientPrepareVoiceComposerSnapshotRequest,
    ) -> anyhow::Result<PreparedVoiceComposerSnapshot> {
        self.core
            .prepare_composer_voice(&request.operation, &ClientFfiFileSystem, None)
    }

    pub fn clear(&self, runtime: &ClientRuntime) -> anyhow::Result<ClientActiveThreadClearResult> {
        let thread_ids = {
            let inner = self.core.as_ref();
            clear_authorization_derived_state(inner)
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

        let inner = self.core.as_ref();
        remove_thread_session_state(inner, thread_id.as_str());

        Ok(ClientActiveThreadUnsubscribeResult {
            unsubscribed_thread_id: thread_id,
            snapshot: snapshot_from_inner(inner, request.expanded_keys.as_slice()),
        })
    }

    pub(crate) fn ensure_thread_subscription(
        &self,
        runtime: &ClientRuntime,
        thread_id: &str,
        workspace_id: String,
    ) -> anyhow::Result<()> {
        self.core.refresh_thread_subscription(
            &runtime.ws_command_sender(),
            thread_id,
            &workspace_id,
        )
    }

    fn apply_gateway_notification(
        &self,
        runtime: &ClientRuntime,
        notification: GatewayNotification,
    ) -> anyhow::Result<ClientFfiNotificationReduction> {
        match notification {
            GatewayNotification::AccessChanged(change) => {
                return Ok(ClientFfiNotificationReduction {
                    access_changed: Some(self.apply_access_changed(runtime, &change)?),
                    ..Default::default()
                });
            }
            GatewayNotification::AuthorizationProjectionChanged(change) => {
                self.apply_authorization_projection_changed(&change)?;
                return Ok(Default::default());
            }
            _ => {}
        }
        let context = ClientRuntimeNotificationContext::default();
        match runtime.reduce_gateway_notification(notification, context) {
            Some(ClientRuntimeNotification::AdministrationChanged(event)) => {
                let invalidation = self
                    .administration_events
                    .lock()
                    .map_err(|_| anyhow::anyhow!("administration lock is poisoned"))?
                    .apply_event(&event);
                Ok(ClientFfiNotificationReduction {
                    administration_refetch: invalidation.effects,
                    ..Default::default()
                })
            }
            Some(ClientRuntimeNotification::TaskUserNotificationDelivered(notification)) => {
                Ok(ClientFfiNotificationReduction {
                    task_user_notification: Some(notification),
                    ..Default::default()
                })
            }
            _ => Ok(ClientFfiNotificationReduction::default()),
        }
    }

    fn apply_authorization_projection_changed(
        &self,
        notification: &pioneer_protocol::AuthorizationProjectionChangedNotification,
    ) -> anyhow::Result<()> {
        self.core.invalidate_threads_for_policy(notification);
        Ok(())
    }

    fn apply_access_changed(
        &self,
        _runtime: &ClientRuntime,
        notification: &AccessChangedNotification,
    ) -> anyhow::Result<ClientAccessChangedLifecycle> {
        let plan = self.core.apply_thread_access_change(notification);
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
}

fn clear_authorization_derived_state(inner: &pioneer_client::core::ClientCore) -> Vec<String> {
    let ids = inner.thread_snapshots().into_keys().collect();
    inner.clear_thread_stores();
    ids
}

fn upsert_thread_snapshot(inner: &pioneer_client::core::ClientCore, thread: Thread) {
    inner.upsert_thread(thread);
}

fn activate_thread(
    inner: &pioneer_client::core::ClientCore,
    thread_id: &str,
    workspace_id: Option<&str>,
) {
    inner.activate_thread(Some(thread_id), workspace_id);
}

fn remove_thread_session_state(inner: &pioneer_client::core::ClientCore, thread_id: &str) {
    inner.remove_thread_store(thread_id);
}

fn snapshot_from_inner(
    inner: &pioneer_client::core::ClientCore,
    expanded_keys: &[String],
) -> ClientActiveThreadSnapshot {
    snapshot_for_thread_from_inner(inner, inner.active_thread_id().as_deref(), expanded_keys)
}

fn snapshot_for_thread_from_inner(
    inner: &pioneer_client::core::ClientCore,
    thread_id: Option<&str>,
    _expanded_keys: &[String],
) -> ClientActiveThreadSnapshot {
    let Some(thread_id) = thread_id else {
        return ClientActiveThreadSnapshot {
            session_revision: inner.thread_session_revision(),
            ..Default::default()
        };
    };
    let Some(domain) = inner.thread_snapshot(thread_id) else {
        let revisions = inner
            .snapshot(&pioneer_client::core::ClientScope::Thread {
                thread_id: thread_id.to_owned(),
            })
            .map(|snapshot| snapshot.revisions());
        return ClientActiveThreadSnapshot {
            domain_revision: revisions.map_or(0, |r| r.domain().get()),
            timeline_revision: revisions.map_or(0, |r| r.presentation().get()),
            thread_id: Some(thread_id.to_owned()),
            session_revision: inner.thread_session_revision(),
            ..Default::default()
        };
    };
    let coordinator = domain.coordinator();
    let workspace_id = coordinator.workspace_id.clone();
    let draft_thread_id = inner.thread_workspace_draft(&workspace_id);
    let last_active_thread_id = inner.thread_workspace_last_active(&workspace_id);
    let (projection, rows, row_render_fingerprints) = thread_metadata_projection(&coordinator);
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
        session_revision: inner.thread_session_revision(),
        thread: coordinator.thread().cloned(),
        history_loaded: coordinator.history_loaded,
        history_loading: coordinator.history_loading,
        projection,
        rows,
        row_render_fingerprints,
        active_turn_security_summary,
        active_turn_security_diagnostics,
        semantic_timeline_patch: domain.semantic_cache_patch(),
        domain_revision: domain.revision(),
        timeline_revision: domain.timeline_revision(),
        pending_requests: domain.pending().to_vec(),
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

fn thread_metadata_projection(
    coordinator: &ThreadCoordinator,
) -> (
    ConversationViewState,
    Vec<TimelineRow>,
    HashMap<String, String>,
) {
    let mut projection = coordinator.conversation.projection().clone();
    projection.items.clear();
    projection.timeline.clear();
    (projection, Vec::new(), HashMap::new())
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
        GatewayNotification::CLIRuntimeRequestOpened(notification) => {
            notification.thread_id.as_deref()
        }
        GatewayNotification::CLIRuntimeRequestResolved(notification) => {
            notification.thread_id.as_deref()
        }
        GatewayNotification::TurnPermissionRequestOpened(notification) => {
            Some(notification.request.thread_id.as_str())
        }
        GatewayNotification::TurnPermissionRequestResolved(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ThreadArtifactsChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::Unknown(notification) => notification.thread_id.as_deref(),
        _ => None,
    }
}

fn notification_visible_thread_ids(notification: &GatewayNotification) -> &[String] {
    match notification {
        GatewayNotification::CLIRuntimeRequestOpened(notification) => {
            notification.visible_thread_ids.as_slice()
        }
        GatewayNotification::CLIRuntimeRequestResolved(notification) => {
            notification.visible_thread_ids.as_slice()
        }
        GatewayNotification::TurnPermissionRequestOpened(notification) => {
            notification.request.visible_thread_ids.as_slice()
        }
        _ => &[],
    }
}

fn notification_snapshot_thread_id(
    active_thread_id: Option<&str>,
    affected_thread_id: Option<&str>,
    visible_thread_ids: &[String],
) -> Option<String> {
    if let Some(active_thread_id) = active_thread_id
        && visible_thread_ids
            .iter()
            .any(|visible_thread_id| visible_thread_id == active_thread_id)
    {
        return Some(active_thread_id.to_owned());
    }
    affected_thread_id.or(active_thread_id).map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::cli_runtime::approvals::{PendingRequest, PendingRequestsReduction};
    use pioneer_client::conversation::reducer::{TurnPhase, TurnView};
    use pioneer_client::timeline::semantic::apply_local_composer_event_to_semantic_timeline_with_patch;
    use pioneer_protocol::{
        CLIAgentRuntimeKind, CLIRuntimePendingRequest, CLIRuntimeRequestKind,
        CLIRuntimeRequestOpenedNotification, McpScopeKind, RuntimeCapabilities, RuntimeStatus,
        SkillId, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, Turn, TurnKind,
        TurnOrigin, TurnPermissionApprovalRequest, TurnPermissionRequestOpenedNotification,
        TurnStatus,
    };
    use serde_json::json;

    #[test]
    fn direct_and_ffi_scoped_snapshots_share_revisions_and_single_reduction() {
        let direct = Arc::new(pioneer_client::core::ClientCore::new());
        let mobile = Arc::new(pioneer_client::core::ClientCore::new());
        let boundary = ClientFfiActiveThreadState::new(mobile.clone());
        for core in [&direct, &mobile] {
            core.upsert_thread(thread("a", "ws"));
            core.upsert_thread(thread("b", "ws"));
            core.activate_thread(Some("a"), Some("ws"));
        }
        let scopes = [
            pioneer_client::core::ClientScope::Thread {
                thread_id: "a".into(),
            },
            pioneer_client::core::ClientScope::Thread {
                thread_id: "b".into(),
            },
        ];
        let initial = scopes
            .iter()
            .map(|scope| crate::client_binding::snapshot_dto(direct.snapshot(scope).unwrap()))
            .collect::<Vec<_>>();
        let before = direct.thread_snapshot("a").unwrap();
        let mut updated = thread("b", "ws");
        updated.preview = "new preview".into();
        let event =
            GatewayNotification::ThreadUpdated(pioneer_protocol::ThreadUpdatedNotification {
                thread: updated,
                placement: None,
            });
        assert!(direct.apply_thread_notification(event.clone()));
        assert!(mobile.apply_thread_notification(event));
        let updated = scopes
            .iter()
            .map(|scope| crate::client_binding::snapshot_dto(mobile.snapshot(scope).unwrap()))
            .collect::<Vec<_>>();
        let wire = serde_json::json!({"initial":initial,"updated":updated});
        assert_eq!(
            wire,
            serde_json::from_str::<serde_json::Value>(include_str!(
                "../tests/fixtures/thread-registry-wire.json"
            ))
            .unwrap()
        );
        for id in ["a", "b"] {
            let rust = direct.thread_snapshot(id).unwrap();
            let ffi = boundary
                .snapshot(ClientActiveThreadSnapshotRequest {
                    thread_id: Some(id.into()),
                    expanded_keys: vec![],
                })
                .unwrap();
            assert_eq!(ffi.domain_revision, rust.revision());
            assert_eq!(ffi.timeline_revision, rust.timeline_revision());
            assert_eq!(
                serde_json::to_value(ffi.semantic_timeline_patch).unwrap(),
                serde_json::to_value(rust.semantic_cache_patch()).unwrap()
            );
            assert_eq!(
                serde_json::to_value(mobile.thread_snapshot(id).unwrap().as_ref()).unwrap(),
                serde_json::to_value(rust.as_ref()).unwrap()
            );
        }
        assert!(Arc::ptr_eq(&before, &direct.thread_snapshot("a").unwrap()));
        // Compatibility calls read the existing Client snapshot; they cannot reduce this event again.
        let revision = mobile.thread_snapshot("b").unwrap().revision();
        let result = boundary
            .apply_gateway_notification(
                &ClientRuntime::new(),
                GatewayNotification::ThreadUpdated(pioneer_protocol::ThreadUpdatedNotification {
                    thread: thread("b", "ws"),
                    placement: None,
                }),
            )
            .unwrap();
        assert!(result.semantic_timeline_patch.changed_blocks.is_empty());
        assert_eq!(revision, mobile.thread_snapshot("b").unwrap().revision());
    }

    fn install_coordinator(
        core: &pioneer_client::core::ClientCore,
        id: String,
        coordinator: ThreadCoordinator,
    ) {
        core.upsert_thread(coordinator.thread().expect("fixture thread").clone());
        *core.existing_thread_mutation(&id).expect("fixture store") = coordinator;
    }

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
            preview_author: None,
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
    fn text_and_voice_submission_preserves_capabilities_but_requires_ready_cli_runtime() {
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
                "combined-catalog",
                runtime_summary("combined", true, true, RuntimeStatus::Ready),
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

        for (case, status) in [
            (
                "degraded",
                RuntimeStatus::Degraded {
                    message: "probe degraded".to_owned(),
                },
            ),
            (
                "error",
                RuntimeStatus::Error {
                    message: "probe failed".to_owned(),
                },
            ),
        ] {
            let runtime = runtime_summary(case, true, true, status);
            let provider = format!("cli_runtime:{case}");
            assert!(
                selected_execution_target_from_runtimes(
                    Some(provider.as_str()),
                    std::slice::from_ref(&runtime),
                )
                .is_err(),
                "{case} runtime must not be used for execution"
            );

            // Catalog reconciliation is independent of readiness and must not
            // destroy the user's capability selection while Gateway retries
            // the provider in the background.
            let plan = plan_composer_submission(
                Some(provider.as_str()),
                "typed",
                false,
                capabilities.as_slice(),
            );
            assert_eq!(
                plan.capabilities
                    .iter()
                    .map(|capability| capability.id.clone())
                    .collect::<Vec<_>>(),
                expected_ids,
                "{case}"
            );
        }

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
    fn active_thread_send_text_requires_operation_identity() {
        let error = serde_json::from_value::<ClientActiveThreadSendTextRequest>(json!({}))
            .expect_err("operation identity must be required");
        assert!(error.to_string().contains("missing field `operation`"));
    }

    #[test]
    fn permission_notifications_preserve_their_exact_thread_and_workspace_scope() {
        let cli =
            GatewayNotification::CLIRuntimeRequestOpened(CLIRuntimeRequestOpenedNotification {
                workspace_id: "ws_cli".to_owned(),
                runtime_id: "codex".to_owned(),
                request_id: "request_cli".to_owned(),
                thread_id: Some("thread_cli".to_owned()),
                turn_id: Some("turn_cli".to_owned()),
                item_id: None,
                visible_thread_ids: Vec::new(),
                request: CLIRuntimePendingRequest {
                    kind: CLIRuntimeRequestKind::CommandApproval,
                    title: None,
                    message: None,
                    native_request_id: None,
                    payload: None,
                },
            });
        assert_eq!(notification_thread_id(&cli), Some("thread_cli"));

        let native = GatewayNotification::TurnPermissionRequestOpened(
            TurnPermissionRequestOpenedNotification {
                request: TurnPermissionApprovalRequest {
                    request_id: "request_native".to_owned(),
                    workspace_id: "ws_native".to_owned(),
                    thread_id: "thread_native".to_owned(),
                    turn_id: "turn_native".to_owned(),
                    visible_thread_ids: Vec::new(),
                    tool_name: "exec_command".to_owned(),
                    action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
                    scope_hash: "scope_native".to_owned(),
                    reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
                    summary: None,
                    details: Vec::new(),
                },
            },
        );
        assert_eq!(notification_thread_id(&native), Some("thread_native"));
    }

    #[test]
    fn permission_notification_projects_into_the_active_visible_ancestor() {
        assert_eq!(
            notification_snapshot_thread_id(
                Some("root_thread"),
                Some("child_thread"),
                &["root_thread".to_owned()],
            )
            .as_deref(),
            Some("root_thread")
        );
        assert_eq!(
            notification_snapshot_thread_id(
                Some("unrelated_thread"),
                Some("child_thread"),
                &["root_thread".to_owned()],
            )
            .as_deref(),
            Some("child_thread")
        );
    }

    #[test]
    fn active_thread_send_text_rejects_copied_draft_payload() {
        let value = json!({
            "operation": { "thread_id": "thread_a", "draft_id": 7, "generation": 11 }
        });
        let request: ClientActiveThreadSendTextRequest =
            serde_json::from_value(value.clone()).unwrap();
        assert_eq!(request.operation.thread_id, "thread_a");
        assert_eq!(request.operation.generation, 11);
        for field in [
            "text",
            "permission_mode",
            "selected_model",
            "reply_to_turn_id",
            "attachments",
        ] {
            let mut copied = value.clone();
            copied[field] = json!("stale copied value");
            assert!(
                serde_json::from_value::<ClientActiveThreadSendTextRequest>(copied).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn message_turn_selection_needs_no_model_or_provider() {
        let inner = pioneer_client::core::ClientCore::new();
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
        install_coordinator(&inner, "thread_a".to_owned(), coordinator);

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
        let inner = pioneer_client::core::ClientCore::new();
        install_coordinator(
            &inner,
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
    fn text_and_voice_requests_share_operation_identity_and_reject_copied_catalogs() {
        let identity = json!({"thread_id":"thread_a", "draft_id":7, "generation":11});
        let text: ClientActiveThreadSendTextRequest =
            serde_json::from_value(json!({"operation":identity})).unwrap();
        let voice: ClientPrepareVoiceComposerSnapshotRequest =
            serde_json::from_value(json!({"operation":identity})).unwrap();
        assert_eq!(text.operation, voice.operation);
        let copied = json!({"operation":identity, "skill_picker":{"packs":[],"standalone":[]}});
        assert!(
            serde_json::from_value::<ClientActiveThreadSendTextRequest>(copied.clone()).is_err()
        );
        assert!(
            serde_json::from_value::<ClientPrepareVoiceComposerSnapshotRequest>(copied).is_err()
        );
    }

    #[test]
    fn voice_prepare_request_uses_operation_identity_and_rejects_copied_draft_fields() {
        let request: ClientPrepareVoiceComposerSnapshotRequest = serde_json::from_value(json!({
            "operation": { "thread_id": "thread_a", "draft_id": 7, "generation": 11 }
        }))
        .expect("request decodes");
        assert_eq!(request.operation.thread_id, "thread_a");
        assert_eq!(request.operation.generation, 11);
        assert!(serde_json::from_value::<ClientPrepareVoiceComposerSnapshotRequest>(json!({
            "operation": request.operation, "turn_id": "shell_turn", "attachments": [], "selected_model": "shell_model"
        })).is_err());
    }

    #[test]
    fn active_thread_snapshot_includes_active_scope_pending_requests() {
        let inner = pioneer_client::core::ClientCore::new();
        inner.activate_thread(Some("thread_a"), None);
        install_coordinator(
            &inner,
            "thread_a".to_owned(),
            ThreadCoordinator::new(thread("thread_a", "ws_a")),
        );
        inner.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
            "req_a", "ws_a", "thread_a",
        )));
        inner.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
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
            let inner = state.core.as_ref();
            inner.activate_thread(Some("thread_revoked"), Some("workspace_revoked"));
            install_coordinator(
                &inner,
                "thread_allowed".to_owned(),
                ThreadCoordinator::new(thread("thread_allowed", "workspace_allowed")),
            );
            install_coordinator(
                &inner,
                "thread_revoked".to_owned(),
                ThreadCoordinator::new(thread("thread_revoked", "workspace_revoked")),
            );
            inner.remember_thread_draft("workspace_revoked", Some("thread_revoked".to_owned()));
            inner.remember_thread_last_active(
                "workspace_allowed",
                Some("thread_allowed".to_owned()),
            );
            inner
                .existing_thread_timeline_mutation("thread_allowed")
                .unwrap()
                .thread_mut("thread_allowed");
            inner
                .existing_thread_timeline_mutation("thread_revoked")
                .unwrap()
                .thread_mut("thread_revoked");
            inner.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
                "request_allowed",
                "workspace_allowed",
                "thread_allowed",
            )));
            inner.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
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
                    outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                    change: AccessChangeKind::WorkspaceMembership,
                },
            )
            .expect("access change");

        assert!(lifecycle.applied);
        assert!(lifecycle.active_scope_cleared);
        assert!(lifecycle.refresh_workspace_catalog);
        let inner = state.core.as_ref();
        assert!(inner.active_thread_id().is_none());
        assert!(
            inner
                .thread_coordinator_snapshot("thread_allowed")
                .is_some()
        );
        assert!(
            !inner
                .thread_coordinator_snapshot("thread_revoked")
                .is_some()
        );
        assert!(
            inner
                .thread_semantic_snapshot("thread_allowed")
                .thread("thread_allowed")
                .is_some()
        );
        assert!(
            !inner
                .thread_semantic_snapshot("thread_revoked")
                .thread("thread_revoked")
                .is_some()
        );
        assert_eq!(inner.pending_request_snapshot().len(), 1);
        assert_eq!(
            inner.pending_request_snapshot()[0].workspace_id,
            "workspace_allowed"
        );
        assert_eq!(
            inner
                .thread_workspace_last_active("workspace_allowed")
                .as_deref(),
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
            let inner = state.core.as_ref();
            inner.activate_thread(Some("thread_revoked"), None);
            install_coordinator(
                &inner,
                "thread_revoked".to_owned(),
                ThreadCoordinator::new(thread("thread_revoked", "workspace_affected")),
            );
            install_coordinator(
                &inner,
                "thread_kept".to_owned(),
                ThreadCoordinator::new(thread("thread_kept", "workspace_affected")),
            );
            inner.remember_thread_draft("workspace_affected", Some("thread_kept".to_owned()));
            inner.remember_thread_last_active("workspace_affected", Some("thread_kept".to_owned()));
            inner.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
                "request_revoked",
                "workspace_affected",
                "thread_revoked",
            )));
            inner.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
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
                    outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                    change: AccessChangeKind::ThreadParticipantRemoved,
                },
            )
            .expect("thread access change");

        assert!(lifecycle.applied);
        assert!(!lifecycle.active_scope_cleared);
        assert!(lifecycle.active_thread_cleared);
        assert!(!lifecycle.refresh_workspace_catalog);
        let inner = state.core.as_ref();
        assert!(inner.active_thread_id().is_none());
        assert!(
            !inner
                .thread_coordinator_snapshot("thread_revoked")
                .is_some()
        );
        assert!(inner.thread_coordinator_snapshot("thread_kept").is_some());
        assert_eq!(
            inner
                .thread_workspace_draft("workspace_affected")
                .as_deref(),
            Some("thread_kept")
        );
        assert_eq!(
            inner
                .thread_workspace_last_active("workspace_affected")
                .as_deref(),
            Some("thread_kept")
        );
        assert_eq!(inner.pending_request_snapshot().len(), 1);
        assert_eq!(
            inner.pending_request_snapshot()[0].thread_id.as_deref(),
            Some("thread_kept")
        );
    }

    #[test]
    fn retained_visibility_change_keeps_active_thread_and_projection() {
        let state = ClientFfiActiveThreadState::default();
        {
            let inner = state.core.as_ref();
            inner.invalidate_threads_for_policy(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation: pioneer_protocol::PolicyGeneration::new(8).unwrap(),
                    change: pioneer_protocol::AuthorizationChangeKind::RolePolicy,
                    affected: pioneer_protocol::AuthorizationChangeScope::Global,
                },
            );
            inner.activate_thread(Some("thread_current"), None);
            install_coordinator(
                &inner,
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
                    outcome: pioneer_protocol::AccessChangeOutcome::Retained,
                    change: AccessChangeKind::ThreadVisibility,
                },
            )
            .expect("retained visibility change");

        assert!(lifecycle.applied);
        assert!(!lifecycle.active_scope_cleared);
        assert!(!lifecycle.active_thread_cleared);
        assert!(!lifecycle.refresh_workspace_catalog);
        let inner = state.core.as_ref();
        assert_eq!(inner.active_thread_id().as_deref(), Some("thread_current"));
        assert!(
            inner
                .thread_coordinator_snapshot("thread_current")
                .is_some()
        );
    }

    #[test]
    fn stale_access_change_is_reported_without_mutating_ffi_state() {
        let state = ClientFfiActiveThreadState::default();
        {
            let inner = state.core.as_ref();
            inner.invalidate_threads_for_policy(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation: pioneer_protocol::PolicyGeneration::new(9).unwrap(),
                    change: pioneer_protocol::AuthorizationChangeKind::RolePolicy,
                    affected: pioneer_protocol::AuthorizationChangeScope::Global,
                },
            );
            inner.activate_thread(Some("thread_current"), None);
            install_coordinator(
                &inner,
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
                    outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                    change: AccessChangeKind::ThreadVisibility,
                },
            )
            .expect("stale access change");

        assert!(!lifecycle.applied);
        assert!(!lifecycle.active_scope_cleared);
        assert!(!lifecycle.refresh_workspace_catalog);
        let inner = state.core.as_ref();
        assert_eq!(inner.active_thread_id().as_deref(), Some("thread_current"));
        assert!(
            inner
                .thread_coordinator_snapshot("thread_current")
                .is_some()
        );
    }

    #[test]
    fn new_authorization_epoch_drops_protected_cache_but_not_outer_session_state() {
        let state = ClientFfiActiveThreadState::default();
        {
            let inner = state.core.as_ref();
            inner.invalidate_threads_for_policy(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation: pioneer_protocol::PolicyGeneration::new(91).unwrap(),
                    change: pioneer_protocol::AuthorizationChangeKind::RolePolicy,
                    affected: pioneer_protocol::AuthorizationChangeScope::Global,
                },
            );
            inner.activate_thread(Some("thread_protected"), None);
            inner.remember_thread_draft("workspace_protected", Some("thread_protected".to_owned()));
            inner.remember_thread_last_active(
                "workspace_protected",
                Some("thread_protected".to_owned()),
            );
            install_coordinator(
                &inner,
                "thread_protected".to_owned(),
                ThreadCoordinator::new(thread("thread_protected", "workspace_protected")),
            );
            inner
                .existing_thread_timeline_mutation("thread_protected")
                .unwrap()
                .thread_mut("thread_protected");
            inner.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
                "request_protected",
                "workspace_protected",
                "thread_protected",
            )));
        }

        state
            .begin_authorization_epoch()
            .expect("begin authorization epoch");

        let inner = state.core.as_ref();
        assert!(inner.active_thread_id().is_none());
        assert!(
            inner
                .thread_workspace_draft("workspace_protected")
                .is_none()
        );
        assert!(
            inner
                .thread_workspace_last_active("workspace_protected")
                .is_none()
        );
        assert!(inner.thread_coordinator_snapshots().is_empty());
        assert!(inner.thread_snapshots().is_empty());
        assert!(inner.pending_request_snapshot().is_empty());
    }

    #[test]
    fn policy_generation_change_invalidates_only_the_exact_mobile_scope() {
        let state = ClientFfiActiveThreadState::default();
        {
            let inner = state.core.as_ref();
            inner.invalidate_threads_for_policy(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation: pioneer_protocol::PolicyGeneration::new(40).unwrap(),
                    change: pioneer_protocol::AuthorizationChangeKind::RolePolicy,
                    affected: pioneer_protocol::AuthorizationChangeScope::Global,
                },
            );
            inner.activate_thread(Some("thread_affected"), None);
            install_coordinator(
                &inner,
                "thread_affected".to_owned(),
                ThreadCoordinator::new(thread("thread_affected", "workspace_shared")),
            );
            install_coordinator(
                &inner,
                "thread_kept".to_owned(),
                ThreadCoordinator::new(thread("thread_kept", "workspace_shared")),
            );
        }

        state
            .apply_authorization_projection_changed(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation: pioneer_protocol::PolicyGeneration::new(41).unwrap(),
                    change: pioneer_protocol::AuthorizationChangeKind::ThreadAcl,
                    affected: pioneer_protocol::AuthorizationChangeScope::PrincipalThread {
                        principal_id: pioneer_protocol::PrincipalId::new("P00000000000000000001")
                            .unwrap(),
                        workspace_id: "workspace_shared".to_owned(),
                        thread_id: "thread_affected".to_owned(),
                    },
                },
            )
            .expect("typed policy invalidation");

        let inner = state.core.as_ref();
        assert_eq!(inner.active_thread_id().as_deref(), Some("thread_affected"));
        assert!(
            !inner
                .thread_coordinator_snapshot("thread_affected")
                .is_some()
        );
        assert!(inner.thread_coordinator_snapshot("thread_kept").is_some());

        {
            let inner = state.core.as_ref();
            install_coordinator(
                &inner,
                "thread_same_generation".to_owned(),
                ThreadCoordinator::new(thread("thread_same_generation", "workspace_shared")),
            );
        }
        state
            .apply_authorization_projection_changed(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation: pioneer_protocol::PolicyGeneration::new(41).unwrap(),
                    change: pioneer_protocol::AuthorizationChangeKind::ThreadAcl,
                    affected: pioneer_protocol::AuthorizationChangeScope::Thread {
                        workspace_id: "workspace_shared".to_owned(),
                        thread_id: "thread_same_generation".to_owned(),
                    },
                },
            )
            .expect("typed event paired with access event at the same generation");
        assert!(
            !state
                .core
                .thread_coordinator_snapshot("thread_same_generation")
                .is_some()
        );

        state
            .apply_authorization_projection_changed(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation: pioneer_protocol::PolicyGeneration::new(40).unwrap(),
                    change: pioneer_protocol::AuthorizationChangeKind::RolePolicy,
                    affected: pioneer_protocol::AuthorizationChangeScope::Global,
                },
            )
            .expect("stale typed invalidation");
        let inner = state.core.as_ref();
        assert!(inner.thread_coordinator_snapshot("thread_kept").is_some());
    }

    #[test]
    fn active_root_snapshot_includes_grandchild_native_permission_request() {
        let inner = pioneer_client::core::ClientCore::new();
        inner.activate_thread(Some("root_thread"), None);
        install_coordinator(
            &inner,
            "root_thread".to_owned(),
            ThreadCoordinator::new(thread("root_thread", "ws_a")),
        );
        inner.apply_pending_requests(PendingRequestsReduction::Opened(
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
        let inner = Arc::new(pioneer_client::core::ClientCore::new());
        inner.activate_thread(Some("thread_a"), None);
        install_coordinator(
            &inner,
            "thread_a".to_owned(),
            ThreadCoordinator::new(collaborative),
        );
        let event = pioneer_client::conversation::ConversationEvent::LocalTurnStartRequested {
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            pending_request_id: "pending_a".to_owned(),
            mode: ThreadMode::Agent,
            user_text: "start detached work".to_owned(),
            attachments: Vec::new(),
        };
        inner
            .existing_thread_mutation("thread_a")
            .expect("coordinator")
            .conversation
            .apply(event.clone());
        assert!(
            apply_local_composer_event_to_semantic_timeline_with_patch(
                &mut inner.existing_thread_timeline_mutation("thread_a").unwrap(),
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

        let _subscription = inner.subscribe(
            pioneer_client::core::ClientScope::Timeline {
                thread_id: "thread_a".into(),
            },
            std::num::NonZeroUsize::new(8).unwrap(),
        );
        let snapshot = inner
            .thread_presentation_snapshot("thread_a")
            .unwrap()
            .timeline();
        assert!(snapshot.rows().iter().any(|row| matches!(row.value(), pioneer_client::timeline::presentation::TimelineRenderRow::Timeline(row) if matches!(row.kind, pioneer_client::timeline::rows::TimelineRowKind::UserMessage { .. }))));
        assert!(snapshot.rows().iter().all(|row| !matches!(row.value(), pioneer_client::timeline::presentation::TimelineRenderRow::Timeline(row) if matches!(row.kind, pioneer_client::timeline::rows::TimelineRowKind::RunningTurn(_)))));
        let metadata = snapshot_from_inner(&inner, &[]);
        assert!(
            metadata.rows.is_empty()
                && metadata.projection.items.is_empty()
                && metadata.projection.timeline.is_empty()
        );
    }

    #[test]
    fn active_thread_snapshot_restores_running_composer_state_from_thread_snapshot() {
        let inner = pioneer_client::core::ClientCore::new();
        inner.activate_thread(Some("thread_a"), None);
        let mut thread = thread("thread_a", "ws_a");
        thread.status = ThreadStatus::Active;
        thread.turns.push(running_turn("turn_a"));
        install_coordinator(
            &inner,
            "thread_a".to_owned(),
            ThreadCoordinator::new(thread),
        );

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
