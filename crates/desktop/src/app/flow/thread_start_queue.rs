use super::*;

impl PioneerDesktop {
    pub(crate) fn request_thread_start_if_needed(&mut self) {
        if let Some(existing_draft_thread_id) = self.draft_thread_id().map(str::to_owned) {
            if self
                .thread_coordinator(existing_draft_thread_id.as_str())
                .is_some()
            {
                return;
            }
            self.clear_draft_thread_if_matches(existing_draft_thread_id.as_str());
        }

        if self.thread_start_coordinator().in_progress {
            return;
        }

        let start = self.thread_start_coordinator_mut();
        if start.pending_thread_id.is_none() {
            start.pending_thread_id = Some(generate_id(ID_LEN));
        }

        self.enqueue_thread_start_request();
    }

    pub(crate) fn drive_thread_start_queue(&mut self, cx: &mut Context<Self>) -> bool {
        if let Some(existing_draft_thread_id) = self.draft_thread_id().map(str::to_owned) {
            if self
                .thread_coordinator(existing_draft_thread_id.as_str())
                .is_some()
            {
                self.clear_thread_start_queue();
                return false;
            }
            self.clear_draft_thread_if_matches(existing_draft_thread_id.as_str());
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            return false;
        };

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return false;
        }

        if self.thread_start_coordinator().in_progress {
            return false;
        }

        if !self.dequeue_thread_start_request() {
            return false;
        }

        self.ensure_thread_started(connection_id, cx)
    }
}
