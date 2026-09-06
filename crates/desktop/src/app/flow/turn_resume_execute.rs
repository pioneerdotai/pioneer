use super::*;
impl PioneerDesktop {
    pub(in crate::app::flow) fn resume_in_flight_turn(
        &mut self,
        thread_id: String,
        _connection_id: u64,
        _cx: &mut Context<Self>,
    ) {
        self.gateway
            .client_runtime
            .client_core()
            .schedule_thread_resume(&thread_id);
    }
}
