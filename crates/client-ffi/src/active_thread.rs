use crate::contracts::ClientEvent;
use crate::threads::ClientThreadTreeSnapshot;
use pioneer_client::{
    ClientError, ClientResult,
    composer::{
        attachments::ComposerAttachment,
        capabilities::ComposerCapability,
        model_selection as composer_model_selection,
        turn_prepare::{
            ComposerSubmitAvailabilityInput, PrepareComposerTurnRequest,
            PreparedComposerTurnSubmitContext, can_submit_composer_message,
            composer_has_sendable_content, prepare_composer_turn,
            reduce_prepared_composer_turn_submit_success,
        },
    },
    conversation::ConversationViewState,
    notifications::effects::ClientEffect,
    notifications::router::TurnTimelineRefreshReduction,
    runtime::{ClientRuntime, ClientRuntimeNotification, ClientRuntimeNotificationContext},
    state::{reducers as client_state_reducers, selectors as client_selectors},
    threads::{
        coordinator::ThreadCoordinator,
        history::{
            composed_task_turn_timeline_param, composed_task_turn_timeline_params,
            reduce_composed_turn_timeline_refresh_success, reduce_thread_history_load_success,
            thread_history_params,
        },
        start as thread_start,
    },
    timeline::rows::{TimelineRow, build_timeline_rows},
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
    GatewayNotification, Thread, ThreadHistoryResponse, ThreadMode, TurnTimelineResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    sync::Mutex,
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
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientActiveThreadSnapshotRequest {
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
    pub selected_mode: Option<ThreadMode>,
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClientActiveThreadSnapshot {
    pub thread_id: Option<String>,
    pub workspace_id: Option<String>,
    pub thread: Option<Thread>,
    pub history_loaded: bool,
    pub history_loading: bool,
    pub projection: ConversationViewState,
    pub rows: Vec<TimelineRow>,
}

#[derive(Default)]
pub struct ClientFfiActiveThreadState {
    inner: Mutex<ClientFfiActiveThreadInner>,
}

#[derive(Default)]
struct ClientFfiActiveThreadInner {
    active_thread_id: Option<String>,
    coordinators: HashMap<String, ThreadCoordinator>,
}

impl ClientFfiActiveThreadState {
    pub fn open_thread(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadOpenRequest,
    ) -> anyhow::Result<ClientActiveThreadSnapshot> {
        let thread_id = request.thread.id.clone();
        let workspace_id = request.thread.workspace_id.clone();

        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            inner.active_thread_id = Some(thread_id.clone());
            inner
                .coordinators
                .entry(thread_id.clone())
                .and_modify(|coordinator| coordinator.set_snapshot(request.thread.clone()))
                .or_insert_with(|| ThreadCoordinator::new(request.thread.clone()));
        }

        self.ensure_thread_subscription(runtime, thread_id.as_str(), workspace_id.clone())?;

        let should_load = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            inner
                .coordinators
                .get(thread_id.as_str())
                .is_some_and(|coordinator| {
                    !coordinator.history_loaded && !coordinator.history_loading
                })
        };

        if should_load {
            self.load_thread_history(runtime, thread_id.as_str())?;
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        if let Some(coordinator) = inner.coordinators.get_mut(thread_id.as_str()) {
            coordinator.set_workspace_id(workspace_id.as_str());
        }

        Ok(snapshot_from_inner(
            &inner,
            &expanded_key_set(&request.expanded_keys),
        ))
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
            &expanded_key_set(&request.expanded_keys),
        ))
    }

    pub fn apply_event(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadEventRequest,
    ) -> anyhow::Result<ClientActiveThreadSnapshot> {
        if let ClientEvent::GatewayNotification(notification) = request.event {
            self.apply_gateway_notification(runtime, notification)?;
        }

        self.snapshot(ClientActiveThreadSnapshotRequest {
            expanded_keys: request.expanded_keys,
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
            inner
                .coordinators
                .entry(thread.id.clone())
                .and_modify(|coordinator| coordinator.set_snapshot(thread.clone()))
                .or_insert_with(|| ThreadCoordinator::new(thread.clone()));
        }

        Ok(())
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
            workspace_id,
            text,
            selected_model,
            selected_provider,
            selected_mode,
            attachments,
            capabilities,
            expanded_keys,
        } = request;

        if !composer_has_sendable_content(
            text.as_str(),
            !attachments.is_empty(),
            !capabilities.is_empty(),
        ) {
            return Err(anyhow::anyhow!(
                "message content is required before starting turn"
            ));
        }

        let requested_thread_id = non_empty_string(thread_id);
        let thread_id = match requested_thread_id {
            Some(thread_id) => thread_id,
            None => self.start_thread_for_text_turn(runtime, workspace_id)?,
        };
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
        let prepared = prepare_composer_turn(
            &runtime.ws_command_sender(),
            &ClientFfiFileSystem,
            PrepareComposerTurnRequest {
                workspace_id,
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                endpoint_kind,
                text,
                attachments,
                capabilities,
            },
        )?;
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
                prepared.user_text.as_str(),
                !prepared.attachments.is_empty(),
                !prepared.capabilities.is_empty(),
            )?
        };
        let submit_reduction = reduce_prepared_composer_turn_submit_success(
            PreparedComposerTurnSubmitContext {
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                pending_request_id: pending_request_id.clone(),
                selected_model: Some(selection.selected_model),
                selected_provider: Some(selection.selected_provider),
                selected_mode: Some(selection.selected_mode),
                updated_at_unix: now_unix_seconds(),
            },
            prepared,
        );
        let thread_snapshot_update = submit_reduction.thread_snapshot_update.clone();
        let local_turn_start_requested_event =
            submit_reduction.local_turn_start_requested_event.clone();
        let turn_start_params =
            turn_start_params_from_plan(submit_reduction.turn_start_params_plan);
        let send_context = submit_reduction.send_context;

        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            inner.active_thread_id = Some(thread_id.clone());
            let coordinator = inner
                .coordinators
                .get_mut(thread_id.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("active thread must be opened before starting turn")
                })?;
            coordinator
                .conversation
                .apply(local_turn_start_requested_event);
            if let Some(thread) = coordinator.thread_mut() {
                apply_prepared_turn_to_thread_snapshot(
                    thread,
                    thread_snapshot_update.selected_model.as_deref(),
                    thread_snapshot_update.selected_provider.as_deref(),
                    thread_snapshot_update.user_text.as_str(),
                    thread_snapshot_update.updated_at_unix,
                );
            }
        }

        match ws_commands::turn_start(&runtime.ws_command_sender(), turn_start_params) {
            Ok(response) => {
                self.apply_turn_start_send_reduction(
                    reduce_turn_start_send_success(send_context, response),
                    thread_id.as_str(),
                )?;
            }
            Err(error) => {
                let message = format!("{error:#}");
                self.apply_turn_start_send_reduction(
                    reduce_turn_start_send_failure(send_context, message.clone()),
                    thread_id.as_str(),
                )?;
                return Err(anyhow::anyhow!(message));
            }
        }

        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

        Ok(ClientActiveThreadSendTextResult {
            thread_id,
            turn_id,
            pending_request_id,
            snapshot: snapshot_from_inner(&inner, &expanded_key_set(&expanded_keys)),
        })
    }

    pub fn cancel_turn(
        &self,
        runtime: &ClientRuntime,
        request: ClientActiveThreadCancelTurnRequest,
    ) -> anyhow::Result<ClientActiveThreadCancelTurnResult> {
        let expanded_keys = expanded_key_set(&request.expanded_keys);
        let Some((thread_id, turn_id, params)) =
            self.apply_local_turn_cancel_request(request.reason)?
        else {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;

            return Ok(ClientActiveThreadCancelTurnResult {
                cancelled: false,
                thread_id: inner.active_thread_id.clone(),
                turn_id: None,
                snapshot: snapshot_from_inner(&inner, &expanded_keys),
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
            snapshot: snapshot_from_inner(&inner, &expanded_keys),
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
            inner.active_thread_id = None;
            inner.coordinators.clear();
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
        inner.active_thread_id = Some(reduction.thread_id.clone());
        inner
            .coordinators
            .entry(reduction.thread_id.clone())
            .and_modify(|coordinator| coordinator.set_snapshot(reduction.thread.clone()))
            .or_insert_with(|| ThreadCoordinator::new(reduction.thread));

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

    fn start_thread_for_text_turn(
        &self,
        runtime: &ClientRuntime,
        workspace_id: Option<String>,
    ) -> anyhow::Result<String> {
        let workspace_id = non_empty_string(workspace_id)
            .ok_or_else(|| anyhow::anyhow!("workspace_id is required before starting thread"))?;
        let planned_thread_id = thread_start::generate_thread_start_id();
        let response = ws_commands::thread_start(
            &runtime.ws_command_sender(),
            thread_start::thread_start_params(planned_thread_id, workspace_id.clone()),
        )?;
        let reduction =
            thread_start::reduce_thread_start_bootstrap_success(workspace_id, response, None);
        let thread_id = reduction.thread_id.clone();

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        inner.active_thread_id = Some(thread_id.clone());
        inner
            .coordinators
            .entry(thread_id.clone())
            .and_modify(|coordinator| coordinator.set_snapshot(reduction.thread.clone()))
            .or_insert_with(|| ThreadCoordinator::new(reduction.thread));

        Ok(thread_id)
    }

    fn load_thread_history(&self, runtime: &ClientRuntime, thread_id: &str) -> anyhow::Result<()> {
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            if let Some(coordinator) = inner.coordinators.get_mut(thread_id) {
                if coordinator.history_loaded || coordinator.history_loading {
                    return Ok(());
                }
                coordinator.history_loading = true;
            }
        }

        let load_result = (|| {
            let response = ws_commands::thread_history(
                &runtime.ws_command_sender(),
                thread_history_params(thread_id.to_owned(), None),
            )?;
            let timelines = load_task_turn_timelines(runtime, &response);
            Ok::<_, anyhow::Error>((response, timelines))
        })();

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        if let Some(coordinator) = inner.coordinators.get_mut(thread_id) {
            coordinator.history_loading = false;
        }

        let (response, timelines) = load_result?;
        let reduction = reduce_thread_history_load_success(thread_id, response, timelines);
        let pioneer_client::threads::history::ThreadHistoryLoadSuccessReduction::Apply(reduction) =
            reduction
        else {
            return Ok(());
        };

        if let Some(coordinator) = inner.coordinators.get_mut(reduction.thread_id.as_str()) {
            coordinator.workspace_id = reduction.workspace_id;
            coordinator.conversation.hydrate_history(&reduction.events);
            for timeline in &reduction.timelines {
                coordinator
                    .conversation
                    .apply_composed_turn_timeline(timeline);
            }
            coordinator.history_loaded = reduction.mark_history_loaded;
        }

        Ok(())
    }

    fn apply_gateway_notification(
        &self,
        runtime: &ClientRuntime,
        notification: GatewayNotification,
    ) -> anyhow::Result<()> {
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
            return Ok(());
        };

        match reduction {
            ClientRuntimeNotification::ThreadStarted(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                inner
                    .coordinators
                    .entry(reduction.thread_id.clone())
                    .and_modify(|coordinator| coordinator.set_snapshot(reduction.thread.clone()))
                    .or_insert_with(|| ThreadCoordinator::new(reduction.thread));
                if let Some(thread_id) = reduction.set_active_thread_id {
                    inner.active_thread_id = Some(thread_id);
                }
            }
            ClientRuntimeNotification::TurnLifecycle(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                let coordinator = inner
                    .coordinators
                    .entry(reduction.thread_id.clone())
                    .or_insert_with(|| {
                        ThreadCoordinator::pending(
                            reduction.thread_id.as_str(),
                            reduction.workspace_id.as_str(),
                        )
                    });
                coordinator.conversation.apply(reduction.conversation_event);
                if reduction.tick_conversation {
                    let _ = coordinator.conversation.tick();
                }
                if let Some(status) = reduction.thread_status
                    && let Some(thread) = coordinator.thread_mut()
                {
                    thread.status = status;
                }
            }
            ClientRuntimeNotification::ConversationEvent(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                let coordinator = inner
                    .coordinators
                    .entry(reduction.thread_id.clone())
                    .or_insert_with(|| {
                        ThreadCoordinator::pending(
                            reduction.thread_id.as_str(),
                            reduction.workspace_id.as_str(),
                        )
                    });
                coordinator.conversation.apply(reduction.conversation_event);
            }
            ClientRuntimeNotification::ThreadUpdated(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                inner
                    .coordinators
                    .entry(reduction.thread_id.clone())
                    .and_modify(|coordinator| coordinator.set_snapshot(reduction.thread.clone()))
                    .or_insert_with(|| ThreadCoordinator::new(reduction.thread));
            }
            ClientRuntimeNotification::ThreadClosed(reduction) => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
                if reduction.clear_active_thread_if_matches
                    && inner.active_thread_id.as_deref() == Some(reduction.thread_id.as_str())
                {
                    inner.active_thread_id = None;
                }
                if reduction.remove_thread_conversation {
                    inner.coordinators.remove(reduction.thread_id.as_str());
                }
            }
            ClientRuntimeNotification::TurnTimelineRefresh(reduction) => {
                self.refresh_turn_timeline(runtime, reduction)?;
            }
            ClientRuntimeNotification::WorkspaceRefresh(_)
            | ClientRuntimeNotification::SkillsRefresh(_)
            | ClientRuntimeNotification::McpRefresh(_)
            | ClientRuntimeNotification::McpServerStatusChanged(_)
            | ClientRuntimeNotification::McpServerCatalogChanged(_)
            | ClientRuntimeNotification::ThreadArtifactsRefresh(_)
            | ClientRuntimeNotification::ArtifactThreadRefresh(_)
            | ClientRuntimeNotification::ArtifactDeletedRefresh(_)
            | ClientRuntimeNotification::WorkspaceChanged { .. } => {}
        }

        Ok(())
    }

    fn refresh_turn_timeline(
        &self,
        runtime: &ClientRuntime,
        reduction: TurnTimelineRefreshReduction,
    ) -> anyhow::Result<()> {
        if !reduction.queue_turn_timeline_refresh {
            return Ok(());
        }

        {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
            if !inner
                .coordinators
                .get(reduction.thread_id.as_str())
                .is_some_and(|coordinator| coordinator.history_loaded)
            {
                return Ok(());
            }
        }

        let timeline = ws_commands::turn_timeline(
            &runtime.ws_command_sender(),
            composed_task_turn_timeline_param(reduction.thread_id.clone(), reduction.turn_id),
        )?;
        let timeline_reduction = reduce_composed_turn_timeline_refresh_success(timeline);

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("active thread lock is poisoned"))?;
        if let Some(coordinator) = inner
            .coordinators
            .get_mut(timeline_reduction.thread_id.as_str())
        {
            coordinator
                .conversation
                .apply_composed_turn_timeline(&timeline_reduction.timeline);
        }

        Ok(())
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
        let coordinator = inner
            .coordinators
            .get_mut(thread_id)
            .ok_or_else(|| anyhow::anyhow!("active thread must be opened before starting turn"))?;

        match reduction {
            TurnStartSendReduction::Accepted { events } => {
                for event in events {
                    coordinator.conversation.apply(event);
                }
            }
            TurnStartSendReduction::Rejected { event } => {
                coordinator.conversation.apply(event);
            }
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
            | ClientEffect::QueueSkillsRefresh
            | ClientEffect::EnqueueInFlightTurnsForResume => Vec::new(),
        })
        .collect()
}

fn snapshot_from_inner(
    inner: &ClientFfiActiveThreadInner,
    expanded: &HashSet<String>,
) -> ClientActiveThreadSnapshot {
    let Some(thread_id) = inner.active_thread_id.as_deref() else {
        return ClientActiveThreadSnapshot::default();
    };
    let Some(coordinator) = inner.coordinators.get(thread_id) else {
        return ClientActiveThreadSnapshot {
            thread_id: Some(thread_id.to_owned()),
            ..Default::default()
        };
    };
    let projection = coordinator.conversation.projection().clone();
    let rows = build_timeline_rows(&projection, expanded);

    ClientActiveThreadSnapshot {
        thread_id: Some(thread_id.to_owned()),
        workspace_id: Some(coordinator.workspace_id.clone()),
        thread: coordinator.thread().cloned(),
        history_loaded: coordinator.history_loaded,
        history_loading: coordinator.history_loading,
        projection,
        rows,
    }
}

fn expanded_key_set(keys: &[String]) -> HashSet<String> {
    keys.iter().cloned().collect()
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
        (Some(provider), Some(model)) => {
            Some(composer_model_selection::ComposerModelSelection { provider, model })
        }
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

fn load_task_turn_timelines(
    runtime: &ClientRuntime,
    response: &ThreadHistoryResponse,
) -> Vec<TurnTimelineResponse> {
    composed_task_turn_timeline_params(response)
        .into_iter()
        .filter_map(|params| ws_commands::turn_timeline(&runtime.ws_command_sender(), params).ok())
        .collect()
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
        GatewayNotification::TurnTimelineChanged(notification) => {
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
        GatewayNotification::TurnTimelineChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::Unknown(notification) => notification.workspace_id.as_deref(),
        _ => None,
    }
}
