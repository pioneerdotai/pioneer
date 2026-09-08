mod view;
use crate::{
    binding::ThreadBindings,
    member_picker::*,
    screen::{GatewayConnectionState, TimelineView},
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    avatars::PrincipalId,
    composer::state_machine::ComposerMentionCandidate,
    core::{ClientCore, ClientScope},
    gateway::identity_authorization::IdentityAuthorizationPublication,
    threads::{capabilities::ThreadCapabilityPublication, members::*, scope::*},
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::{path::PathBuf, sync::Arc};

pub(crate) struct ThreadMembersView {
    client: Arc<ClientCore>,
    thread_id: String,
    binding: Arc<ThreadBindings>,
    screen: WeakEntity<TimelineView>,
    thread_member_input: Option<Arc<ThreadMemberPublication>>,
    capability_input: Option<Arc<ThreadCapabilityPublication>>,
    identity_input: Option<Arc<IdentityAuthorizationPublication>>,
    connection_state: GatewayConnectionState,
    thread_member_select: MemberPickerState,
    thread_member_items: Vec<ComposerMentionCandidate>,
    avatar_paths: Vec<Option<PathBuf>>,
    _binding_task: Task<()>,
    _subscriptions: Vec<Subscription>,
}
impl ThreadMembersView {
    pub(crate) fn set_visible(&self, visible: bool) {
        self.binding.set_active(visible);
    }

    pub(crate) fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        registrar: Arc<dyn ClientBindingRegistrar>,
        screen: WeakEntity<TimelineView>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let scopes = vec![
            ClientScope::ThreadMember {
                thread_id: thread_id.clone(),
            },
            ClientScope::ThreadCapability {
                thread_id: thread_id.clone(),
            },
            ClientScope::Thread {
                thread_id: thread_id.clone(),
            },
            ClientScope::Administration { workspace_id: None },
            ClientScope::Session,
        ];
        let initial = scopes
            .iter()
            .filter_map(|scope| client.snapshot(scope))
            .collect();
        let binding = ThreadBindings::scoped(registrar, scopes, initial);
        cx.new(|cx: &mut Context<Self>| {
            let select = cx.new(|cx| new_member_picker_state(window, cx));
            let selected = cx.subscribe_in(
                &select,
                window,
                |view,
                 select,
                 event: &gpui_kit::component::combobox::ComboboxEvent<MemberPickerDelegate>,
                 window,
                 cx| {
                    let gpui_kit::component::combobox::ComboboxEvent::Confirm(candidates) = event
                    else {
                        return;
                    };
                    let Some(candidate) = candidates.first().cloned() else {
                        return;
                    };
                    let weak = cx.entity().downgrade();
                    let select = select.downgrade();
                    window.defer(cx, move |_, cx| {
                        let _ = weak.update(cx, |view, _| {
                            view.client
                                .thread_member_intent(ThreadMemberIntent::Perform {
                                    thread_id: view.thread_id.clone(),
                                    action: ThreadScopeAction::AddParticipant {
                                        principal_id: candidate.principal_id,
                                    },
                                });
                        });
                        let _ = select.update(cx, |select, cx| select.clear_selection(cx));
                    });
                },
            );
            let mut subscriptions = vec![selected];
            if let Some(screen) = screen.upgrade() {
                subscriptions.push(cx.subscribe_in(
                    &screen,
                    window,
                    |view, _, _: &crate::avatar::ThreadAvatarChanged, window, cx| {
                        view.synchronize_picker(window, cx);
                        cx.notify();
                    },
                ));
            }
            let input = binding.clone();
            let mut changes = input.watch();
            let binding_task = cx.spawn_in(window, async move |view, cx| {
                while changes.changed().await.is_ok() {
                    if input.drain().is_empty() {
                        continue;
                    }
                    if view
                        .update_in(cx, |view, window, cx| view.synchronize(window, cx))
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let mut view = Self {
                client,
                thread_id,
                binding,
                screen,
                thread_member_input: None,
                capability_input: None,
                identity_input: None,
                connection_state: GatewayConnectionState::Disconnected,
                thread_member_select: select,
                thread_member_items: Vec::new(),
                avatar_paths: Vec::new(),
                _binding_task: binding_task,
                _subscriptions: subscriptions,
            };
            view.synchronize(window, cx);
            view
        })
    }
    pub(crate) fn retry(&self) {
        self.client.thread_member_intent(ThreadMemberIntent::Retry {
            thread_id: self.thread_id.clone(),
        });
        self.client.thread_capability_intent(
            pioneer_client::threads::capabilities::ThreadCapabilityIntent::Retry {
                thread_id: self.thread_id.clone(),
            },
        );
    }
    pub(crate) fn observe(&self) {
        self.client
            .thread_member_intent(ThreadMemberIntent::Observe {
                thread_id: self.thread_id.clone(),
            });
        self.client.thread_capability_intent(
            pioneer_client::threads::capabilities::ThreadCapabilityIntent::Observe {
                thread_id: self.thread_id.clone(),
            },
        );
    }
    fn synchronize(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.thread_member_input = self.client.thread_member_snapshot(&self.thread_id);
        self.capability_input = self.client.thread_capability_snapshot(&self.thread_id);
        self.identity_input = self
            .binding
            .publication(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.snapshot().payload::<IdentityAuthorizationPublication>());
        self.connection_state = self
            .binding
            .publication(&ClientScope::Session)
            .and_then(|p| {
                p.typed::<pioneer_client::gateway::session_controller::GatewaySessionPublication>()
            })
            .and_then(|p| {
                p.payload()
                    .status
                    .as_ref()
                    .map(|status| status.connection_state)
            })
            .unwrap_or(GatewayConnectionState::Disconnected);
        self.synchronize_picker(window, cx);
        cx.notify();
    }
    fn synchronize_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let candidates = self
            .thread_member_input
            .as_ref()
            .map(|input| input.add_candidates(self.is_private()))
            .unwrap_or_default();
        let paths = candidates
            .iter()
            .map(|candidate| self.avatar_path(&candidate.principal_id, cx))
            .collect::<Vec<_>>();
        if self.thread_member_items == candidates && self.avatar_paths == paths {
            return;
        }
        self.avatar_paths = paths;
        // Avatar publications can change rows while domain candidates stay equal.
        let items = member_picker_items(candidates.iter().cloned(), |id| self.avatar_path(id, cx));
        self.thread_member_items = candidates;
        self.thread_member_select
            .update(cx, |state, cx| state.set_items(items, window, cx));
    }
    fn avatar_path(&self, id: &PrincipalId, cx: &App) -> Option<PathBuf> {
        self.screen.upgrade().and_then(|screen| {
            screen
                .read(cx)
                .member_avatar_state
                .presentation(id)
                .and_then(|avatar| avatar.cached_image_path.clone())
        })
    }
    fn is_private(&self) -> bool {
        self.client
            .thread_coordinator_snapshot(&self.thread_id)
            .and_then(|coordinator| coordinator.thread().cloned())
            .is_some_and(|thread| thread.visibility == Some(ThreadVisibility::Private))
    }
    fn thread_scope_capabilities(
        &self,
    ) -> pioneer_client::authorization::ThreadPresentationCapabilities {
        pioneer_client::authorization::thread_presentation_capabilities(
            self.capability_input
                .as_ref()
                .and_then(|input| input.snapshot.as_ref())
                .and_then(|snapshot| snapshot.thread.as_ref())
                .map(|thread| &thread.capabilities),
        )
    }
    fn thread_scope_pending(&self) -> ThreadScopePendingAction {
        match self
            .thread_member_input
            .as_ref()
            .map(|input| &input.request)
        {
            Some(ThreadMemberRequestState::Loading { action }) => {
                ThreadScopePendingAction::Pending {
                    action: action.clone(),
                }
            }
            _ => ThreadScopePendingAction::Idle,
        }
    }
    fn thread_members_loading(&self) -> bool {
        self.thread_member_input
            .as_ref()
            .is_some_and(|input| input.participants_request == ThreadMemberReadState::Loading)
    }
    fn thread_member_directory_loading(&self) -> bool {
        self.thread_member_input
            .as_ref()
            .is_some_and(|input| input.workspace_request == ThreadMemberReadState::Loading)
    }
    fn thread_scope_error(&self) -> Option<String> {
        match self
            .thread_member_input
            .as_ref()
            .map(|input| &input.request)
        {
            Some(ThreadMemberRequestState::Failed {
                action: ThreadScopeAction::ListParticipants,
                ..
            }) => Some(t!("thread.scope.unavailable").to_string()),
            Some(ThreadMemberRequestState::Failed { .. }) => {
                Some(t!("thread.scope.action_failed").to_string())
            }
            _ => None,
        }
    }
    fn remove_thread_member(&mut self, principal_id: PrincipalId, _: &mut Context<Self>) {
        self.client
            .thread_member_intent(ThreadMemberIntent::Perform {
                thread_id: self.thread_id.clone(),
                action: ThreadScopeAction::RemoveParticipant { principal_id },
            });
    }
}
impl Render for ThreadMembersView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_thread_members_panel(window, cx)
    }
}
impl Drop for ThreadMembersView {
    fn drop(&mut self) {
        self.binding.clear();
    }
}
