use super::*;
use pioneer_client::threads::resume::{
    self as thread_resume, TurnResumeQueueConnectionPlan, TurnResumeQueueItemPlan,
};

impl PioneerDesktop {
    pub(in crate::app::flow) fn has_in_flight_turn(&self) -> bool {
        self.has_any_in_flight_turn()
    }

    pub(in crate::app::flow) fn should_resume_in_flight_turn(&self) -> bool {
        self.has_in_flight_turn()
    }

    pub(in crate::app::flow) fn thread_resume_state_mut(
        &self,
        thread_id: &str,
    ) -> Option<pioneer_client::threads::registry::ResumeMutation<'_>> {
        self.thread_coordinator_mut(thread_id)
            .map(|mutation| mutation.resume_mutation())
    }

    pub(in crate::app::flow) fn reset_thread_resume_state(&mut self, thread_id: &str) {
        if let Some(mut resume) = self.thread_resume_state_mut(thread_id) {
            thread_resume::reset_thread_resume_coordinator(&mut resume);
        }
    }

    pub(in crate::app::flow) fn enqueue_in_flight_turns_for_resume(&mut self) {
        let thread_ids =
            thread_resume::thread_ids_with_in_flight_turns(&self.thread_coordinator_snapshots());

        for thread_id in thread_ids {
            self.enqueue_turn_resume_thread(thread_id);
        }
    }

    pub(in crate::app::flow) fn drive_turn_resume_queue(&mut self, cx: &mut Context<Self>) -> bool {
        let connection_id = match thread_resume::plan_turn_resume_queue_connection(
            self.gateway.ws_connection_id,
            self.gateway.connection_state == GatewayConnectionState::Connected,
        ) {
            TurnResumeQueueConnectionPlan::Drive { connection_id } => connection_id,
            TurnResumeQueueConnectionPlan::NotReady => return false,
        };

        while let Some(thread_id) = self.dequeue_turn_resume_thread() {
            let resume_in_progress = self
                .thread_coordinator(thread_id.as_str())
                .is_some_and(|coordinator| coordinator.resume.in_progress);
            let has_in_flight_turn = self
                .in_flight_turn_id_for_thread(thread_id.as_str())
                .is_some();

            match thread_resume::plan_turn_resume_queue_item(resume_in_progress, has_in_flight_turn)
            {
                TurnResumeQueueItemPlan::Skip => continue,
                TurnResumeQueueItemPlan::ResetMissingTurn => {
                    self.reset_thread_resume_state(thread_id.as_str());
                    continue;
                }
                TurnResumeQueueItemPlan::Resume => {
                    self.resume_in_flight_turn(thread_id, connection_id, cx);
                    return false;
                }
            }
        }

        false
    }
}
