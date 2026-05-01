mod view;

use crate::app::root::{GatewayConnectionState, MainContentView, PioneerDesktop};

impl PioneerDesktop {
    pub(in crate::app) fn should_show_active_thread_status(&self) -> bool {
        self.main_content_view == MainContentView::Threads
            && self.current_active_thread_id().is_some()
    }

    pub(in crate::app) fn active_thread_status_text(&self) -> String {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return t!("bottom_bar.gateway_disconnected").to_string();
        }

        if self.current_active_thread_id().is_none() && self.has_in_flight_thread_start() {
            return t!("bottom_bar.starting_thread").to_string();
        }

        let status_label = self
            .active_thread_conversation()
            .map(|conversation| conversation.status_label())
            .unwrap_or("idle");

        if status_label == "completing" {
            return t!("bottom_bar.finishing_turn").to_string();
        }

        if let Some(turn_id) = self
            .active_thread_conversation()
            .and_then(|conversation| conversation.in_flight_turn_id())
        {
            return t!("bottom_bar.turn_running", turn_id = turn_id).to_string();
        }

        match status_label {
            "failed" => t!("bottom_bar.previous_turn_failed").to_string(),
            "cancelled" => t!("bottom_bar.turn_cancelled").to_string(),
            "completed" => t!("bottom_bar.turn_completed").to_string(),
            "idle" => t!("bottom_bar.ready").to_string(),
            "starting" => t!("bottom_bar.starting_turn").to_string(),
            "running" => t!("bottom_bar.agent_processing").to_string(),
            _ => t!("bottom_bar.ready").to_string(),
        }
    }
}
