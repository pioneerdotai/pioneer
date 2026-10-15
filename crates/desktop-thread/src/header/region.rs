#[path = "rename.rs"]
mod rename;
use super::{HeaderAction, HeaderCommand, ThreadHeader};
use crate::{
    binding::ThreadBindings,
    panel_layout::{ThreadPanelKind, ThreadPanelLayoutStore},
    ports::*,
};
use gpui_kit::component::WindowExt;
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    threads::{
        members::{ThreadMemberIntent, ThreadMemberRequestState},
        scope::{ThreadScopeAction, ThreadScopePendingAction},
    },
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::{cell::Cell, rc::Rc, sync::Arc};

pub(crate) struct HeaderBack;
pub(crate) struct ThreadHeaderView {
    client: Arc<ClientCore>,
    thread_id: String,
    binding: Arc<ThreadBindings>,
    layout: Entity<ThreadPanelLayoutStore>,
    files: Arc<dyn ThreadFilePort>,
    operation: ThreadPresentationOperation,
    focus: FocusHandle,
    dialog_open: Rc<Cell<bool>>,
    _binding_task: Task<()>,
    _release: Subscription,
}
impl EventEmitter<HeaderBack> for ThreadHeaderView {}
impl ThreadHeaderView {
    pub(crate) fn set_visible(&self, visible: bool) {
        self.binding.set_active(visible);
    }

    pub(crate) fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        registrar: Arc<dyn ClientBindingRegistrar>,
        layout: Entity<ThreadPanelLayoutStore>,
        files: Arc<dyn ThreadFilePort>,
        mount: u64,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let scopes = vec![
            ClientScope::Thread {
                thread_id: thread_id.clone(),
            },
            ClientScope::ThreadMember {
                thread_id: thread_id.clone(),
            },
            ClientScope::ThreadCapability {
                thread_id: thread_id.clone(),
            },
            ClientScope::Administration { workspace_id: None },
            ClientScope::Session,
            ClientScope::Navigation,
        ];
        let initial = scopes
            .iter()
            .filter_map(|scope| client.snapshot(scope))
            .collect();
        let binding = ThreadBindings::scoped(registrar, scopes, initial);
        cx.new(|cx: &mut Context<Self>| {
            let input = binding.clone();
            let mut changes = input.watch();
            let binding_task = cx.spawn(async move |view, cx| {
                while changes.changed().await.is_ok() {
                    if input.drain().is_empty() {
                        continue;
                    }
                    if view.update(cx, |_, cx| cx.notify()).is_err() {
                        break;
                    }
                }
            });
            let release = cx.on_release_in(window, |view, window, cx| {
                if view.dialog_open.replace(false) {
                    window.close_dialog(cx);
                }
            });
            Self {
                operation: ThreadPresentationOperation::new(thread_id.clone(), mount, 0),
                client,
                thread_id,
                binding,
                layout,
                files,
                focus: cx.focus_handle(),
                dialog_open: Rc::new(Cell::new(false)),
                _binding_task: binding_task,
                _release: release,
            }
        })
    }
    fn can_manage(&self) -> bool {
        let principal = self.binding.publication(&ClientScope::Administration { workspace_id: None }).and_then(|p| p.snapshot().payload::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>());
        principal
            .and_then(|p| p.capabilities.snapshot(None, None))
            .is_some_and(|p| {
                pioneer_client::authorization::principal_presentation_capabilities(&p)
                    .can_manage_all_threads
            })
            || self.binding.publication(&ClientScope::ThreadCapability { thread_id: self.thread_id.clone() })
                .and_then(|p| p.snapshot().payload::<pioneer_client::threads::capabilities::ThreadCapabilityPublication>())
                .and_then(|p| p.snapshot.clone())
                .and_then(|p| p.thread)
                .is_some_and(|t| {
                    pioneer_client::authorization::thread_presentation_capabilities(Some(
                        &t.capabilities,
                    ))
                    .can_manage_thread
                })
    }
    fn command(&mut self, action: &HeaderAction, window: &mut Window, cx: &mut Context<Self>) {
        if action.operation != self.operation {
            return;
        }
        match &action.command {
            HeaderCommand::Back => cx.emit(HeaderBack),
            HeaderCommand::Rename => {
                self.open_rename_thread_dialog(self.thread_id.clone(), window, cx)
            }
            HeaderCommand::Members => self
                .layout
                .update(cx, |layout, cx| layout.open(ThreadPanelKind::Members, cx)),
            HeaderCommand::SetVisibility(visibility) => {
                self.client
                    .thread_member_intent(ThreadMemberIntent::Perform {
                        thread_id: self.thread_id.clone(),
                        action: ThreadScopeAction::UpdateVisibility {
                            visibility: *visibility,
                        },
                    });
            }
            HeaderCommand::SetFileOpener(opener) => {
                if let Err(error) =
                    self.files
                        .select_file_opener(&self.operation, opener.as_deref(), cx)
                {
                    tracing::warn!(error = %error, "failed to save thread file opener override");
                }
                cx.notify();
            }
        }
    }
}
impl Render for ThreadHeaderView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let coordinator = self
            .binding
            .publication(&ClientScope::Thread {
                thread_id: self.thread_id.clone(),
            })
            .and_then(|p| {
                p.snapshot()
                    .payload::<pioneer_client::threads::registry::ThreadDomainSnapshot>()
            })
            .map(|p| p.coordinator());
        let thread = coordinator
            .as_ref()
            .and_then(|coordinator| coordinator.thread());
        let navigation = self
            .binding
            .publication(&ClientScope::Navigation)
            .and_then(|p| {
                p.snapshot()
                    .payload::<pioneer_client::navigation::ClientNavigationState>()
            })
            .unwrap_or_default();
        let lineage = navigation
            .lineage()
            .iter()
            .rev()
            .find(|entry| entry.child_thread_id() == self.thread_id);
        let draft = coordinator.as_ref().is_some_and(|coordinator| {
            navigation.draft(&coordinator.workspace_id) == Some(&self.thread_id)
        });
        let title = lineage
            .map(|entry| entry.title().to_owned())
            .or_else(|| {
                (!draft)
                    .then(|| thread.and_then(pioneer_client::threads::title::thread_display_title))
                    .flatten()
            })
            .unwrap_or_else(|| t!("sidebar.thread.untitled").to_string());
        let member = self
            .binding
            .publication(&ClientScope::ThreadMember {
                thread_id: self.thread_id.clone(),
            })
            .and_then(|p| {
                p.snapshot()
                    .payload::<pioneer_client::threads::members::ThreadMemberPublication>()
            });
        let pending = match member.as_ref().map(|member| &member.request) {
            Some(ThreadMemberRequestState::Loading { action }) => {
                ThreadScopePendingAction::Pending {
                    action: action.clone(),
                }
            }
            _ => ThreadScopePendingAction::Idle,
        };
        let connected = self.binding.publication(&ClientScope::Session).and_then(|p| p.snapshot().payload::<pioneer_client::gateway::session_controller::GatewaySessionPublication>()).and_then(|p| p.status.clone()).is_some_and(|status| status.connection_state == crate::screen::GatewayConnectionState::Connected);
        div()
            .key_context("ThreadHeader")
            .track_focus(&self.focus)
            .on_action(cx.listener(Self::command))
            .child(ThreadHeader {
                action_region: self.focus.clone(),
                operation: self.operation.clone(),
                title,
                task_child: lineage.is_some(),
                materialized: !draft && thread.is_some(),
                can_manage: self.can_manage(),
                visibility: thread.and_then(|thread| thread.visibility),
                status: thread.map(|thread| thread.status),
                connected,
                pending,
                file_openers: self.files.file_openers(&self.thread_id, cx),
            })
    }
}
impl Drop for ThreadHeaderView {
    fn drop(&mut self) {
        self.binding.clear();
    }
}
