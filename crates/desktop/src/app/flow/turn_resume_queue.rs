use super::*;

impl PioneerDesktop {
    pub(in crate::app::flow) fn has_in_flight_turn(&self) -> bool {
        self.has_any_in_flight_turn()
    }

    pub(in crate::app::flow) fn should_resume_in_flight_turn(&self) -> bool {
        self.has_in_flight_turn()
    }

    pub(in crate::app::flow) fn thread_resume_state_mut(
        &mut self,
        thread_id: &str,
    ) -> Option<&mut crate::app::root::ThreadResumeCoordinator> {
        self.thread_coordinator_mut(thread_id)
            .map(|coordinator| &mut coordinator.resume)
    }

    pub(in crate::app::flow) fn reset_thread_resume_state(&mut self, thread_id: &str) {
        if let Some(resume) = self.thread_resume_state_mut(thread_id) {
            resume.in_progress = false;
            resume.retry_attempt = 0;
            resume.next_attempt_at = None;
        }
    }

    pub(in crate::app::flow) fn enqueue_in_flight_turns_for_resume(&mut self) {
        let thread_ids: Vec<String> = self
            .thread_coordinators
            .iter()
            .filter_map(|(thread_id, coordinator)| {
                coordinator
                    .conversation
                    .in_flight_turn_id()
                    .is_some()
                    .then_some(thread_id.to_owned())
            })
            .collect();

        for thread_id in thread_ids {
            self.enqueue_turn_resume_thread(thread_id);
        }
    }

    pub(in crate::app::flow) fn drive_turn_resume_queue(&mut self, cx: &mut Context<Self>) -> bool {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return false;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return false;
        };

        while let Some(thread_id) = self.dequeue_turn_resume_thread() {
            if self
                .thread_resume_state_mut(thread_id.as_str())
                .is_some_and(|resume| resume.in_progress)
            {
                continue;
            }

            if self
                .in_flight_turn_id_for_thread(thread_id.as_str())
                .is_none()
            {
                self.reset_thread_resume_state(thread_id.as_str());
                continue;
            }

            self.resume_in_flight_turn(thread_id, connection_id, cx);
            return true;
        }

        false
    }
}
