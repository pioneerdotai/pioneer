use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::{AsyncApp, Context, WeakEntity, prelude::*};
use pioneer_client::threads::scope::{ThreadScopeAction, ThreadScopePendingAction};
use pioneer_protocol::{
    AuthorizationCapabilitiesParams, ThreadParticipantChangeKind, ThreadParticipantSummary,
    ThreadParticipantsChangedNotification, ThreadParticipantsListParams,
    ThreadParticipantsResponse,
};
use std::time::Duration;
use tracing::warn;

const THREAD_CAPABILITY_REFRESH_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ThreadCapabilityRefreshDecision {
    Complete,
    Retry,
    Stale,
}

impl PioneerDesktop {
    pub(in crate::app) fn toggle_thread_members_sidebar(&mut self, cx: &mut Context<Self>) {
        self.show_thread_members_sidebar = !self.show_thread_members_sidebar;
        if self.show_thread_members_sidebar {
            self.show_thread_artifacts_sidebar = false;
            self.ensure_active_thread_members_loaded(true, cx);
        }
        cx.notify();
    }

    pub(in crate::app) fn open_thread_members_sidebar(&mut self, cx: &mut Context<Self>) {
        self.show_thread_members_sidebar = true;
        self.show_thread_artifacts_sidebar = false;
        self.ensure_active_thread_members_loaded(true, cx);
        cx.notify();
    }

    /// Loads operational capabilities for every active thread independently
    /// from the optional participants panel. Internal task/subagent threads do
    /// not have their own user-facing participant list, but the Gateway can
    /// still project their permissions through persisted root lineage.
    pub(in crate::app) fn ensure_active_thread_capabilities_loaded(
        &mut self,
        force: bool,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        let Some(workspace_id) = self
            .thread_coordinator(thread_id.as_str())
            .and_then(|coordinator| coordinator.thread())
            .map(|thread| thread.workspace_id.clone())
        else {
            return;
        };
        if !force && self.thread_scope_capabilities_thread_id.as_deref() == Some(thread_id.as_str())
        {
            return;
        }
        if self.thread_scope_capabilities_loading_thread_id.as_deref() == Some(thread_id.as_str()) {
            return;
        }
        let Some(expected_principal_id) = self
            .gateway
            .current_auth
            .as_ref()
            .map(|auth| auth.principal.id.clone())
        else {
            return;
        };
        let is_runtime_draft = self.draft_thread_id() == Some(thread_id.as_str());

        self.thread_scope_capabilities_refresh_generation = self
            .thread_scope_capabilities_refresh_generation
            .wrapping_add(1);
        let generation = self.thread_scope_capabilities_refresh_generation;
        let connection_id = self.gateway.ws_connection_id;
        self.thread_scope_capabilities_loading_thread_id = Some(thread_id.clone());
        let sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                for attempt in 0..=THREAD_CAPABILITY_REFRESH_RETRY_DELAYS.len() {
                    let request_sender = sender.clone();
                    let request_workspace_id = workspace_id.clone();
                    let request_thread_id = thread_id.clone();
                    let request_principal_id = expected_principal_id.clone();
                    let result = cx
                        .background_spawn(async move {
                            let snapshot = request_sender.authorization_capabilities(
                                AuthorizationCapabilitiesParams {
                                    workspace_id: Some(request_workspace_id.clone()),
                                    thread_id: Some(request_thread_id.clone()),
                                },
                            )?;
                            anyhow::ensure!(
                                pioneer_client::authorization::authorization_capability_snapshot_is_compatible(
                                    &snapshot,
                                    &request_principal_id,
                                    Some(request_workspace_id.as_str()),
                                    Some(request_thread_id.as_str()),
                                ),
                                "Gateway returned an incompatible thread capability snapshot"
                            );
                            anyhow::Ok(snapshot)
                        })
                        .await;

                    let decision = this
                        .update(&mut cx, |view, cx| {
                            let context_matches = view.gateway.connection_state
                                == GatewayConnectionState::Connected
                                && view.thread_scope_capabilities_refresh_generation == generation
                                && view.gateway.ws_connection_id == connection_id
                                && view.current_active_thread_id() == Some(thread_id.as_str())
                                && view
                                    .thread_workspace_id(thread_id.as_str())
                                    .is_some_and(|current| current == workspace_id.as_str())
                                && view
                                    .gateway
                                    .current_auth
                                    .as_ref()
                                    .is_some_and(|auth| {
                                        auth.principal.id == expected_principal_id
                                    });
                            if !context_matches {
                                return ThreadCapabilityRefreshDecision::Stale;
                            }

                            match result {
                                Ok(snapshot) => {
                                    if view.gateway.authorization_projections.accept(snapshot)
                                        != pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted
                                    {
                                        return ThreadCapabilityRefreshDecision::Retry;
                                    }
                                    view.gateway.authorization_revision = view
                                        .gateway
                                        .authorization_projections
                                        .accepted_revision();
                                    let coherent = view
                                        .gateway
                                        .authorization_projections
                                        .snapshot(
                                            Some(workspace_id.as_str()),
                                            (!is_runtime_draft).then_some(thread_id.as_str()),
                                        );
                                    view.thread_scope_capabilities =
                                        pioneer_client::authorization::thread_presentation_capabilities(
                                            coherent
                                                .as_ref()
                                                .and_then(|snapshot| snapshot
                                                .thread
                                                .as_ref()
                                                .map(|thread| &thread.capabilities)),
                                        );
                                    view.gateway.capability_snapshot = view
                                        .gateway
                                        .authorization_projections
                                        .snapshot(Some(workspace_id.as_str()), None)
                                        .or_else(|| {
                                            view.gateway
                                                .authorization_projections
                                                .snapshot(None, None)
                                        });
                                    view.reconcile_composer_draft_with_capabilities();
                                    view.thread_scope_capabilities_thread_id =
                                        Some(thread_id.clone());
                                    view.thread_scope_capabilities_loading_thread_id = None;
                                    cx.notify();
                                    ThreadCapabilityRefreshDecision::Complete
                                }
                                Err(error) => {
                                    warn!(
                                        attempt,
                                        thread_id = thread_id.as_str(),
                                        error = %format!("{error:#}"),
                                        "thread capability refresh failed"
                                    );
                                    ThreadCapabilityRefreshDecision::Retry
                                }
                            }
                        })
                        .unwrap_or(ThreadCapabilityRefreshDecision::Stale);

                    match decision {
                        ThreadCapabilityRefreshDecision::Complete
                        | ThreadCapabilityRefreshDecision::Stale => return,
                        ThreadCapabilityRefreshDecision::Retry
                            if attempt < THREAD_CAPABILITY_REFRESH_RETRY_DELAYS.len() =>
                        {
                            cx.background_executor()
                                .timer(THREAD_CAPABILITY_REFRESH_RETRY_DELAYS[attempt])
                                .await;
                        }
                        ThreadCapabilityRefreshDecision::Retry => {
                            let _ = this.update(&mut cx, |view, _| {
                                if view.thread_scope_capabilities_refresh_generation == generation
                                    && view
                                        .thread_scope_capabilities_loading_thread_id
                                        .as_deref()
                                        == Some(thread_id.as_str())
                                {
                                    view.thread_scope_capabilities_loading_thread_id = None;
                                }
                            });
                            return;
                        }
                    }
                }
            }
        })
        .detach();
    }

    pub(in crate::app) fn ensure_active_thread_members_loaded(
        &mut self,
        force: bool,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || self.thread_members_loading
        {
            return;
        }
        let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        let Some(thread) = self
            .thread_coordinator(thread_id.as_str())
            .and_then(|coordinator| coordinator.thread())
        else {
            return;
        };
        if thread.visibility != Some(pioneer_protocol::ThreadVisibility::Private) {
            // Workspace-visible threads use the workspace directory and
            // internal task/subagent threads inherit their root ACL. Neither
            // has a user-editable private participant list.
            self.thread_members_thread_id = None;
            self.thread_members.clear();
            self.thread_members_loading = false;
            self.thread_scope_error = None;
            return;
        }
        if !force && self.thread_members_thread_id.as_deref() == Some(thread_id.as_str()) {
            return;
        }
        if !matches!(self.thread_scope_pending, ThreadScopePendingAction::Idle) {
            return;
        }

        let workspace_id = thread.workspace_id.clone();
        self.thread_members_thread_id = Some(thread_id.clone());
        self.thread_members.clear();
        self.thread_members_loading = true;
        self.thread_scope_pending = ThreadScopePendingAction::Pending {
            action: ThreadScopeAction::ListParticipants,
        };
        self.thread_scope_error = None;

        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.thread_participants_list(ThreadParticipantsListParams {
                            workspace_id: workspace_id.clone(),
                            thread_id: thread_id.clone(),
                        })
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.thread_members_loading = false;
                    if matches!(
                        view.thread_scope_pending,
                        ThreadScopePendingAction::Pending {
                            action: ThreadScopeAction::ListParticipants
                        }
                    ) {
                        view.thread_scope_pending = ThreadScopePendingAction::Idle;
                    }
                    match result {
                        Ok(response)
                            if view.thread_members_thread_id.as_deref()
                                == Some(response.thread_id.as_str()) =>
                        {
                            view.thread_members = participant_summaries(response);
                            view.thread_scope_error = None;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "thread participant list failed"
                            );
                            view.thread_scope_error =
                                Some(t!("thread.scope.unavailable").to_string());
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn apply_thread_participants_changed_notification(
        &mut self,
        notification: ThreadParticipantsChangedNotification,
        cx: &mut Context<Self>,
    ) {
        if self.thread_members_thread_id.as_deref() != Some(notification.thread_id.as_str()) {
            return;
        }
        match notification.change {
            ThreadParticipantChangeKind::Added => {
                if !self
                    .thread_members
                    .iter()
                    .any(|member| member.principal_id == notification.principal_id)
                {
                    self.thread_members.push(ThreadParticipantSummary {
                        principal_id: notification.principal_id,
                    });
                }
            }
            ThreadParticipantChangeKind::Removed => self
                .thread_members
                .retain(|member| member.principal_id != notification.principal_id),
        }
        cx.notify();
    }
}

pub(super) fn participant_summaries(
    response: ThreadParticipantsResponse,
) -> Vec<ThreadParticipantSummary> {
    if response.participants.is_empty() {
        response
            .participant_ids
            .into_iter()
            .map(|principal_id| ThreadParticipantSummary { principal_id })
            .collect()
    } else {
        response.participants
    }
}
