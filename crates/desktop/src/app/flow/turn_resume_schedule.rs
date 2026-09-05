use super::*;
use pioneer_client::threads::resume::{self as thread_resume, ScheduledTurnResumePlan};

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

        let schedule_plan = thread_resume::schedule_turn_resume_after_state(
            resume,
            delay,
            std::time::Instant::now(),
        );
        let retry_attempt = schedule_plan.retry_attempt;
        let thread_id = thread_id.to_owned();

        pioneer_observability::record_qualification_diagnostic!(record_animation_activity(
            pioneer_observability::AnimationSourceId::TurnResumeScheduleClock,
            pioneer_observability::DiagnosticAction::Scheduled,
            pioneer_observability::Visibility::NotApplicable,
        ));
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                cx.background_executor().timer(delay).await;
                pioneer_observability::record_qualification_diagnostic!(
                    record_animation_activity(
                        pioneer_observability::AnimationSourceId::TurnResumeScheduleClock,
                        pioneer_observability::DiagnosticAction::Woke,
                        pioneer_observability::Visibility::NotApplicable,
                    )
                );
                #[cfg(not(feature = "qualification-diagnostics"))]
                {
                    let _ = this.update(&mut cx, |view, cx| {
                        if view.gateway.ws_connection_id != Some(connection_id)
                            || view.gateway.connection_state != GatewayConnectionState::Connected
                        {
                            return;
                        }

                        let has_in_flight_turn = view
                            .in_flight_turn_id_for_thread(thread_id.as_str())
                            .is_some();
                        let Some(resume) = view.thread_resume_state_mut(thread_id.as_str()) else {
                            return;
                        };

                        match thread_resume::plan_scheduled_turn_resume(
                            resume,
                            retry_attempt,
                            has_in_flight_turn,
                        ) {
                            ScheduledTurnResumePlan::Skip => {}
                            ScheduledTurnResumePlan::ResetMissingTurn => {
                                view.reset_thread_resume_state(thread_id.as_str());
                            }
                            ScheduledTurnResumePlan::Resume => {
                                view.enqueue_turn_resume_thread(thread_id.clone());
                                let resumed = view.drive_turn_resume_queue(cx);
                                if resumed {
                                    cx.notify();
                                }
                            }
                        }
                    });
                }
                #[cfg(feature = "qualification-diagnostics")]
                {
                    let handoff = this.update(&mut cx, |view, cx| {
                        if view.gateway.ws_connection_id != Some(connection_id)
                            || view.gateway.connection_state != GatewayConnectionState::Connected
                        {
                            return false;
                        }

                        let has_in_flight_turn = view
                            .in_flight_turn_id_for_thread(thread_id.as_str())
                            .is_some();
                        let Some(resume) = view.thread_resume_state_mut(thread_id.as_str()) else {
                            return false;
                        };

                        match thread_resume::plan_scheduled_turn_resume(
                            resume,
                            retry_attempt,
                            has_in_flight_turn,
                        ) {
                            ScheduledTurnResumePlan::Skip => false,
                            ScheduledTurnResumePlan::ResetMissingTurn => {
                                view.reset_thread_resume_state(thread_id.as_str());
                                false
                            }
                            ScheduledTurnResumePlan::Resume => {
                                pioneer_observability::record_qualification_diagnostic!(
                                    record_animation_activity(
                                        pioneer_observability::AnimationSourceId::TurnResumeScheduleClock,
                                        pioneer_observability::DiagnosticAction::Requested,
                                        pioneer_observability::Visibility::NotApplicable,
                                    )
                                );
                                view.enqueue_turn_resume_thread(thread_id.clone());
                                let resumed = view.drive_turn_resume_queue(cx);
                                if resumed {
                                    cx.notify();
                                }
                                true
                            }
                        }
                    });
                    pioneer_observability::record_qualification_diagnostic!(
                        record_animation_activity(
                            pioneer_observability::AnimationSourceId::TurnResumeScheduleClock,
                            if matches!(handoff, Ok(true)) {
                                pioneer_observability::DiagnosticAction::Completed
                            } else {
                                pioneer_observability::DiagnosticAction::Cancelled
                            },
                            pioneer_observability::Visibility::NotApplicable,
                        )
                    );
                }
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

        let retry_plan = thread_resume::apply_turn_resume_retry(
            self.thread_resume_state_mut(thread_id),
            std::time::Instant::now(),
        );
        let attempt = retry_plan.attempt;
        let delay = retry_plan.delay;
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
