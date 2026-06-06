use super::*;
use pioneer_client::threads::start::{self as thread_start, ThreadStartDrivePlan};

impl PioneerDesktop {
    pub(crate) fn request_thread_start_if_needed(&mut self) {
        let draft_thread_id = self.draft_thread_id().map(str::to_owned);
        let draft_thread_exists = draft_thread_id
            .as_deref()
            .is_some_and(|thread_id| self.thread_coordinator(thread_id).is_some());
        let plan = thread_start::plan_thread_start_request(
            draft_thread_id.as_deref(),
            draft_thread_exists,
            self.thread_start_coordinator(),
        );

        if let Some(thread_id) = plan.clear_draft_thread_id {
            self.clear_draft_thread_if_matches(thread_id.as_str());
        }

        if plan.ensure_pending_thread_id {
            thread_start::ensure_pending_thread_start_id(
                self.thread_start_coordinator_mut(),
                thread_start::generate_thread_start_id(),
            );
        }

        if !plan.enqueue_start_request {
            return;
        }

        self.enqueue_thread_start_request();
    }

    pub(crate) fn drive_thread_start_queue(&mut self, cx: &mut Context<Self>) -> bool {
        let draft_thread_id = self.draft_thread_id().map(str::to_owned);
        let draft_thread_exists = draft_thread_id
            .as_deref()
            .is_some_and(|thread_id| self.thread_coordinator(thread_id).is_some());
        if let Some(thread_id) = draft_thread_id.as_deref() {
            if !draft_thread_exists {
                self.clear_draft_thread_if_matches(thread_id);
            }
        }

        let plan = thread_start::plan_thread_start_drive(
            draft_thread_id.as_deref(),
            draft_thread_exists,
            self.gateway.ws_connection_id,
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.thread_start_coordinator(),
            self.thread_start_requested,
        );

        match plan {
            ThreadStartDrivePlan::ClearQueue => {
                self.clear_thread_start_queue();
                false
            }
            ThreadStartDrivePlan::Start { connection_id } => {
                if !self.dequeue_thread_start_request() {
                    return false;
                }
                self.ensure_thread_started(connection_id, cx)
            }
            ThreadStartDrivePlan::NotReady => false,
        }
    }
}
