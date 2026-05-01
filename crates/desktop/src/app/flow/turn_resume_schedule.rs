use super::*;

impl PioneerDesktop {
    pub(in crate::app::flow) fn schedule_turn_resume_after(
        &mut self,
        connection_id: u64,
        thread_id: &str,
        delay: Duration,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.ws_connection_id != Some(connection_id)
            || self.gateway.connection_state != GatewayConnectionState::Connected
        {
            self.reset_thread_resume_state(thread_id);
            return;
        }

        let Some(resume) = self.thread_resume_state_mut(thread_id) else {
            return;
        };

        resume.next_attempt_at = Some(std::time::Instant::now() + delay);

        let retry_attempt = resume.retry_attempt;
        let thread_id = thread_id.to_owned();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                cx.background_executor().timer(delay).await;
                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id)
                        || view.gateway.connection_state != GatewayConnectionState::Connected
                    {
                        return;
                    }

                    let Some(resume) = view.thread_resume_state_mut(thread_id.as_str()) else {
                        return;
                    };

                    if resume.retry_attempt != retry_attempt || resume.in_progress {
                        return;
                    }

                    if view
                        .in_flight_turn_id_for_thread(thread_id.as_str())
                        .is_none()
                    {
                        view.reset_thread_resume_state(thread_id.as_str());
                        return;
                    }

                    view.enqueue_turn_resume_thread(thread_id.clone());
                    let resumed = view.drive_turn_resume_queue(cx);
                    if resumed {
                        cx.notify();
                    }
                });
            }
        })
        .detach();
    }

    pub(in crate::app::flow) fn schedule_turn_resume_retry(
        &mut self,
        connection_id: u64,
        thread_id: &str,
        error: &anyhow::Error,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.ws_connection_id != Some(connection_id)
            || self.gateway.connection_state != GatewayConnectionState::Connected
        {
            self.reset_thread_resume_state(thread_id);
            return;
        }

        let mut attempt = 1;
        let mut delay = turn_resume_retry_delay(0);
        if let Some(resume) = self.thread_resume_state_mut(thread_id) {
            delay = turn_resume_retry_delay(resume.retry_attempt);
            attempt = resume.retry_attempt.saturating_add(1);
            resume.retry_attempt = attempt;
            resume.next_attempt_at = Some(std::time::Instant::now() + delay);
        }
        self.schedule_turn_resume_after(connection_id, thread_id, delay, cx);

        warn!(
            attempt,
            thread_id,
            retry_after_ms = delay.as_millis(),
            error = %format!("{error:#}"),
            "failed to resume in-flight turn; scheduling retry"
        );
    }
}
