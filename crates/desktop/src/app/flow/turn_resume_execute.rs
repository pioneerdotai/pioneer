use super::*;
use pioneer_client::threads::resume as thread_resume;

impl PioneerDesktop {
    pub(in crate::app::flow) fn resume_in_flight_turn(
        &mut self,
        thread_id: String,
        connection_id: u64,
        cx: &mut Context<Self>,
    ) {
        let Some(turn_id) = self.in_flight_turn_id_for_thread(thread_id.as_str()) else {
            self.reset_thread_resume_state(thread_id.as_str());
            return;
        };

        let Some(resume) = self.thread_resume_state_mut(thread_id.as_str()) else {
            return;
        };

        thread_resume::begin_turn_resume_attempt(resume);

        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let thread_id_for_sync = thread_id.clone();
            let turn_id_for_sync = turn_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let params = thread_resume::turn_resume_snapshot_params(
                            thread_id_for_sync,
                            turn_id_for_sync,
                        );
                        let turn = ws_sender.turn_get(params.turn)?;
                        let items = ws_sender.turn_items(params.items)?;
                        Ok::<_, anyhow::Error>((turn, items))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if let Some(coordinator) = view.thread_coordinator_mut(thread_id.as_str()) {
                        thread_resume::finish_turn_resume_attempt(&mut coordinator.resume);
                    }

                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok((turn_snapshot, item_snapshot)) => {
                            let reduction = thread_resume::reduce_turn_resume_snapshot_result(
                                thread_id.as_str(),
                                turn_snapshot,
                                item_snapshot,
                            );
                            view.apply_turn_resume_snapshot_reduction(connection_id, reduction, cx);
                        }
                        Err(error) => {
                            match thread_resume::plan_turn_resume_snapshot_failure(
                                thread_id.as_str(),
                            ) {
                                thread_resume::TurnResumeSnapshotFailurePlan::Retry {
                                    thread_id,
                                } => {
                                    view.schedule_turn_resume_retry(
                                        connection_id,
                                        thread_id.as_str(),
                                        &error,
                                        cx,
                                    );
                                }
                            }
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn apply_turn_resume_snapshot_reduction(
        &mut self,
        connection_id: u64,
        reduction: thread_resume::TurnResumeSnapshotReduction,
        cx: &mut Context<Self>,
    ) {
        match reduction {
            thread_resume::TurnResumeSnapshotReduction::ThreadMismatch {
                expected_thread_id,
                actual_thread_id,
                retry_after,
            } => {
                self.schedule_turn_resume_after(
                    connection_id,
                    expected_thread_id.as_str(),
                    retry_after,
                    cx,
                );
                warn!(
                    expected_thread_id = expected_thread_id.as_str(),
                    actual_thread_id = actual_thread_id.as_str(),
                    "turn/get returned snapshot for another thread; retry later"
                );
            }
            thread_resume::TurnResumeSnapshotReduction::Apply(reduction) => {
                self.apply_turn_resume_snapshot_apply_reduction(connection_id, reduction, cx);
            }
        }
    }

    fn apply_turn_resume_snapshot_apply_reduction(
        &mut self,
        connection_id: u64,
        reduction: thread_resume::TurnResumeSnapshotApplyReduction,
        cx: &mut Context<Self>,
    ) {
        let thread_id = reduction.thread_id;
        let workspace_id = reduction.workspace_id;

        self.upsert_thread_for_workspace(thread_id.as_str(), workspace_id.as_str());

        for event in reduction.replay_events {
            self.upsert_thread_conversation_mut(thread_id.as_str(), workspace_id.as_str())
                .apply(event);
        }

        if let Some(event) = reduction.terminal_event {
            self.upsert_thread_conversation_mut(thread_id.as_str(), workspace_id.as_str())
                .apply(event);
            if reduction.tick_conversation_after_terminal_event
                && let Some(conversation) = self.thread_conversation_mut(thread_id.as_str())
            {
                let _ = conversation.tick();
            }
        }

        if let Some(delay) = reduction.schedule_after {
            self.schedule_turn_resume_after(connection_id, thread_id.as_str(), delay, cx);
        }
        if reduction.reset_thread_resume {
            self.reset_thread_resume_state(thread_id.as_str());
        }
    }
}
