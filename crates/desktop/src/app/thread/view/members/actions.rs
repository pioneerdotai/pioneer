use super::state::participant_summaries;
use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui_kit::{AsyncApp, Context, WeakEntity, prelude::*};
use pioneer_client::threads::scope::{ThreadScopeAction, ThreadScopePendingAction};
use pioneer_protocol::{
    PrincipalId, ThreadParticipantMutationParams, ThreadUpdateParams, ThreadVisibility,
};
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn update_thread_visibility(
        &mut self,
        thread_id: String,
        visibility: ThreadVisibility,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || !matches!(self.thread_scope_pending, ThreadScopePendingAction::Idle)
        {
            return;
        }
        let Some(thread) = self
            .thread_coordinator(thread_id.as_str())
            .and_then(|coordinator| coordinator.thread())
        else {
            return;
        };
        if self.thread_scope_capabilities_thread_id.as_deref() != Some(thread_id.as_str())
            || !self.thread_scope_capabilities.can_manage_thread
        {
            return;
        }
        let workspace_id = thread.workspace_id.clone();
        let action = ThreadScopeAction::UpdateVisibility { visibility };

        self.thread_scope_pending = ThreadScopePendingAction::Pending { action };
        self.thread_scope_error = None;
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.thread_update(ThreadUpdateParams {
                            workspace_id,
                            thread_id,
                            name: None,
                            visibility: Some(visibility),
                            archived: None,
                        })
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.thread_scope_pending = ThreadScopePendingAction::Idle;
                    match result {
                        Ok(response) => {
                            view.thread_scope_error = None;
                            view.upsert_thread_snapshot(response.thread);
                        }
                        Err(error) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "thread visibility update failed"
                            );
                            view.thread_scope_error =
                                Some(t!("thread.scope.action_failed").to_string());
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn add_thread_member(
        &mut self,
        principal_id: PrincipalId,
        cx: &mut Context<Self>,
    ) {
        self.mutate_thread_member(principal_id, true, cx);
    }

    pub(super) fn remove_thread_member(
        &mut self,
        principal_id: PrincipalId,
        cx: &mut Context<Self>,
    ) {
        self.mutate_thread_member(principal_id, false, cx);
    }

    fn mutate_thread_member(
        &mut self,
        principal_id: PrincipalId,
        add: bool,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || !matches!(self.thread_scope_pending, ThreadScopePendingAction::Idle)
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
            return;
        }
        if self.thread_scope_capabilities_thread_id.as_deref() != Some(thread_id.as_str())
            || !self
                .thread_scope_capabilities
                .can_manage_private_participants
        {
            return;
        }
        let workspace_id = thread.workspace_id.clone();
        let action = if add {
            ThreadScopeAction::AddParticipant {
                principal_id: principal_id.clone(),
            }
        } else {
            ThreadScopeAction::RemoveParticipant {
                principal_id: principal_id.clone(),
            }
        };
        self.thread_scope_pending = ThreadScopePendingAction::Pending {
            action: action.clone(),
        };
        self.thread_scope_error = None;

        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        let params = ThreadParticipantMutationParams {
                            workspace_id,
                            thread_id,
                            principal_id,
                        };
                        if add {
                            sender.thread_participant_add(params)
                        } else {
                            sender.thread_participant_remove(params)
                        }
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.thread_scope_pending = ThreadScopePendingAction::Idle;
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
                                "thread participant mutation failed"
                            );
                            view.thread_scope_error =
                                Some(t!("thread.scope.action_failed").to_string());
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn desktop_visibility_action_uses_existing_rpc_without_scope_dialog() {
        let source = include_str!("actions.rs");
        assert!(source.contains("thread_update"));
        assert!(!source.contains(&["open", "_dialog"].concat()));
        assert!(!source.contains(&["thread_participants", "_list"].concat()));
        assert!(!source.contains(&["refresh_thread", "_list"].concat()));
    }
}
