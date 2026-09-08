use crate::{
    assets::PioneerIconName,
    binding::ThreadBindings,
    panel_layout::{ThreadPanelControlsView, ThreadPanelLayoutStore},
};
use gpui_kit::{
    component::{button::*, popover::Popover, theme::ActiveTheme, *},
    prelude::*,
    *,
};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    state::snapshot::{ActiveThreadSnapshot, ActiveThreadStatusSnapshot},
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::Arc;

pub(crate) struct ThreadFooterView {
    client: Arc<ClientCore>,
    thread_id: String,
    binding: Arc<ThreadBindings>,
    controls: Entity<ThreadPanelControlsView>,
    _changes: Task<()>,
}
impl ThreadFooterView {
    pub(crate) fn set_visible(&self, visible: bool) {
        self.binding.set_active(visible);
    }

    pub(crate) fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        registrar: Arc<dyn ClientBindingRegistrar>,
        layout: Entity<ThreadPanelLayoutStore>,
        cx: &mut Context<Self>,
    ) -> Self {
        let scopes = vec![
            ClientScope::Thread {
                thread_id: thread_id.clone(),
            },
            ClientScope::Session,
        ];
        let initial = scopes
            .iter()
            .filter_map(|scope| client.snapshot(scope))
            .collect();
        let binding = ThreadBindings::scoped(registrar, scopes, initial);
        let controls = cx.new(|cx| ThreadPanelControlsView::new(layout, cx));
        let input = binding.clone();
        let mut changes = input.watch();
        let task = cx.spawn(async move |view, cx| {
            while changes.changed().await.is_ok() {
                if input.drain().is_empty() {
                    continue;
                }
                if view.update(cx, |_, cx| cx.notify()).is_err() {
                    break;
                }
            }
        });
        Self {
            client,
            thread_id,
            binding,
            controls,
            _changes: task,
        }
    }
    fn status_text(&self) -> String {
        let coordinator = self.client.thread_coordinator_snapshot(&self.thread_id);
        let draft = coordinator.as_ref().is_some_and(|coordinator| {
            self.client
                .thread_workspace_draft(&coordinator.workspace_id)
                .as_deref()
                == Some(&self.thread_id)
        });
        let connected = self.binding.publication(&ClientScope::Session).and_then(|p| p.snapshot().payload::<pioneer_client::gateway::session_controller::GatewaySessionPublication>()).and_then(|p| p.status.clone()).is_some_and(|status| status.connection_state == crate::screen::GatewayConnectionState::Connected);
        let snapshot = ActiveThreadSnapshot::from_coordinator(
            Some(&self.thread_id),
            coordinator.as_deref(),
            draft,
            connected,
            self.client.thread_start_snapshot().in_progress,
        )
        .status;
        match snapshot {
            ActiveThreadStatusSnapshot::GatewayDisconnected => {
                t!("bottom_bar.gateway_disconnected")
            }
            ActiveThreadStatusSnapshot::StartingThread => t!("bottom_bar.starting_thread"),
            ActiveThreadStatusSnapshot::FinishingTurn => t!("bottom_bar.finishing_turn"),
            ActiveThreadStatusSnapshot::TurnRunning { turn_id } => {
                t!("bottom_bar.turn_running", turn_id = turn_id)
            }
            ActiveThreadStatusSnapshot::PreviousTurnFailed => t!("bottom_bar.previous_turn_failed"),
            ActiveThreadStatusSnapshot::TurnCancelled => t!("bottom_bar.turn_cancelled"),
            ActiveThreadStatusSnapshot::TurnCompleted => t!("bottom_bar.turn_completed"),
            ActiveThreadStatusSnapshot::Ready => t!("bottom_bar.ready"),
            ActiveThreadStatusSnapshot::StartingTurn => t!("bottom_bar.starting_turn"),
            ActiveThreadStatusSnapshot::AgentProcessing => t!("bottom_bar.agent_processing"),
        }
        .to_string()
    }
}
impl Render for ThreadFooterView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let status_text = self.status_text();
        h_flex()
            .w_full()
            .h_8()
            .flex_none()
            .px_2()
            .border_t_1()
            .border_color(cx.theme().border)
            .items_center()
            .justify_end()
            .gap_1()
            .child(
                Popover::new("active-thread-status-popover")
                    .anchor(Anchor::BottomRight)
                    .trigger(
                        Button::new("active-thread-status-trigger")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::MessageCircle)
                                    .size_3p5()
                                    .opacity(0.6),
                            ),
                    )
                    .content(move |_, _, _| {
                        v_flex().w(px(320.)).gap_2().p_1().child(
                            div()
                                .text_xs()
                                .line_height(relative(1.15))
                                .whitespace_normal()
                                .child(status_text.clone()),
                        )
                    }),
            )
            .child(self.controls.clone())
    }
}
impl Drop for ThreadFooterView {
    fn drop(&mut self) {
        self.binding.clear();
    }
}
