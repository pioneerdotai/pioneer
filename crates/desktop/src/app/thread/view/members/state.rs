use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::{AsyncApp, Context, WeakEntity, prelude::*};
use pioneer_client::threads::scope::{ThreadScopeAction, ThreadScopePendingAction};
use pioneer_protocol::{
    ThreadParticipantChangeKind, ThreadParticipantSummary, ThreadParticipantsChangedNotification,
    ThreadParticipantsListParams, ThreadParticipantsResponse, ThreadVisibility,
};
use tracing::warn;

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
        if thread.visibility != Some(ThreadVisibility::Private) {
            // Workspace-visible threads use the whole workspace directory.
            // Keep the private-participant cache unbound so switching this
            // same thread back to private triggers an authoritative load.
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
                            workspace_id,
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
