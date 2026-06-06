mod view;

use crate::app::root::{MainContentView, PioneerDesktop};
use pioneer_client::state::snapshot::ActiveThreadStatusSnapshot;

impl PioneerDesktop {
    pub(in crate::app) fn should_show_active_thread_status(&self) -> bool {
        self.main_content_view == MainContentView::Threads
            && self.current_active_thread_id().is_some()
    }

    pub(in crate::app) fn active_thread_status_text(&self) -> String {
        let snapshot = self.client_snapshot().active_thread.status;

        match snapshot {
            ActiveThreadStatusSnapshot::GatewayDisconnected => {
                t!("bottom_bar.gateway_disconnected").to_string()
            }
            ActiveThreadStatusSnapshot::StartingThread => {
                t!("bottom_bar.starting_thread").to_string()
            }
            ActiveThreadStatusSnapshot::FinishingTurn => {
                t!("bottom_bar.finishing_turn").to_string()
            }
            ActiveThreadStatusSnapshot::TurnRunning { turn_id } => {
                t!("bottom_bar.turn_running", turn_id = turn_id).to_string()
            }
            ActiveThreadStatusSnapshot::PreviousTurnFailed => {
                t!("bottom_bar.previous_turn_failed").to_string()
            }
            ActiveThreadStatusSnapshot::TurnCancelled => {
                t!("bottom_bar.turn_cancelled").to_string()
            }
            ActiveThreadStatusSnapshot::TurnCompleted => {
                t!("bottom_bar.turn_completed").to_string()
            }
            ActiveThreadStatusSnapshot::Ready => t!("bottom_bar.ready").to_string(),
            ActiveThreadStatusSnapshot::StartingTurn => t!("bottom_bar.starting_turn").to_string(),
            ActiveThreadStatusSnapshot::AgentProcessing => {
                t!("bottom_bar.agent_processing").to_string()
            }
        }
    }
}
