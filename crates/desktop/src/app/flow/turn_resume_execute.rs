use super::*;
use pioneer_client::threads::resume::{self as thread_resume, TurnResumeStatusPlan};

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
                            if !thread_resume::turn_snapshot_matches_thread(
                                thread_id.as_str(),
                                turn_snapshot.thread_id.as_str(),
                            ) {
                                view.schedule_turn_resume_after(
                                    connection_id,
                                    thread_id.as_str(),
                                    thread_resume::TURN_RESUME_MISMATCH_RETRY_DELAY,
                                    cx,
                                );
                                warn!(
                                    expected_thread_id = thread_id.as_str(),
                                    actual_thread_id = turn_snapshot.thread_id.as_str(),
                                    "turn/get returned snapshot for another thread; retry later"
                                );
                                return;
                            }

                            let target_thread_id = turn_snapshot.thread_id.clone();
                            let target_workspace_id = turn_snapshot.workspace_id.clone();

                            view.upsert_thread_for_workspace(
                                target_thread_id.as_str(),
                                target_workspace_id.as_str(),
                            );

                            let replay_events = thread_resume::turn_items_replay_events(
                                &turn_snapshot,
                                item_snapshot,
                            );
                            for event in replay_events {
                                view.upsert_thread_conversation_mut(
                                    target_thread_id.as_str(),
                                    target_workspace_id.as_str(),
                                )
                                .apply(event);
                            }

                            match thread_resume::plan_turn_resume_after_status(
                                turn_snapshot.turn.status.clone(),
                            ) {
                                TurnResumeStatusPlan::PollAfter(delay) => {
                                    view.schedule_turn_resume_after(
                                        connection_id,
                                        target_thread_id.as_str(),
                                        delay,
                                        cx,
                                    );
                                }
                                TurnResumeStatusPlan::Complete => {
                                    if let Some(event) = thread_resume::turn_resume_terminal_event(
                                        target_thread_id.clone(),
                                        turn_snapshot.turn,
                                    ) {
                                        view.upsert_thread_conversation_mut(
                                            target_thread_id.as_str(),
                                            target_workspace_id.as_str(),
                                        )
                                        .apply(event);
                                        if let Some(conversation) =
                                            view.thread_conversation_mut(target_thread_id.as_str())
                                        {
                                            let _ = conversation.tick();
                                        }
                                    }
                                    view.reset_thread_resume_state(target_thread_id.as_str());
                                }
                                TurnResumeStatusPlan::Fail => {
                                    if let Some(event) = thread_resume::turn_resume_terminal_event(
                                        target_thread_id.clone(),
                                        turn_snapshot.turn,
                                    ) {
                                        view.upsert_thread_conversation_mut(
                                            target_thread_id.as_str(),
                                            target_workspace_id.as_str(),
                                        )
                                        .apply(event);
                                    }
                                    view.reset_thread_resume_state(target_thread_id.as_str());
                                }
                                TurnResumeStatusPlan::Block => {
                                    if let Some(event) = thread_resume::turn_resume_terminal_event(
                                        target_thread_id.clone(),
                                        turn_snapshot.turn,
                                    ) {
                                        view.upsert_thread_conversation_mut(
                                            target_thread_id.as_str(),
                                            target_workspace_id.as_str(),
                                        )
                                        .apply(event);
                                    }
                                    view.reset_thread_resume_state(target_thread_id.as_str());
                                }
                                TurnResumeStatusPlan::Reset => {
                                    view.reset_thread_resume_state(target_thread_id.as_str());
                                }
                            }
                        }
                        Err(error) => {
                            view.schedule_turn_resume_retry(
                                connection_id,
                                thread_id.as_str(),
                                &error,
                                cx,
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }
}
