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

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                let turn_result = cx
                    .background_spawn({
                        let ws_sender = ws_sender.clone();
                        let params = thread_resume::turn_resume_turn_params(
                            thread_id.clone(),
                            turn_id.clone(),
                        );
                        async move { ws_sender.turn_get(params) }
                    })
                    .await;

                let turn_snapshot = match turn_result {
                    Ok(turn_snapshot) => turn_snapshot,
                    Err(error) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            view.finish_and_retry_turn_resume(
                                connection_id,
                                thread_id.as_str(),
                                &error,
                                cx,
                            );
                        });
                        return;
                    }
                };

                if !thread_resume::turn_snapshot_matches_scope(
                    thread_id.as_str(),
                    turn_id.as_str(),
                    &turn_snapshot,
                ) {
                    let reduction = thread_resume::reduce_turn_resume_turn_snapshot(
                        thread_id.as_str(),
                        turn_id.as_str(),
                        turn_snapshot,
                    );
                    let _ = this.update(&mut cx, |view, cx| {
                        view.finish_turn_resume_attempt(thread_id.as_str());
                        if view.gateway.ws_connection_id == Some(connection_id) {
                            view.apply_turn_resume_snapshot_reduction(connection_id, reduction, cx);
                        }
                        cx.notify();
                    });
                    return;
                }

                let mut after_sequence = None;
                loop {
                    let page_result = cx
                        .background_spawn({
                            let ws_sender = ws_sender.clone();
                            let params = thread_resume::turn_resume_items_page_params(
                                thread_id.clone(),
                                turn_id.clone(),
                                after_sequence,
                            );
                            async move { ws_sender.turn_items_page(params) }
                        })
                        .await
                        .and_then(|page| {
                            thread_resume::reduce_turn_resume_items_page(
                                &turn_snapshot,
                                after_sequence,
                                page,
                            )
                        });

                    let page = match page_result {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = this.update(&mut cx, |view, cx| {
                                view.finish_and_retry_turn_resume(
                                    connection_id,
                                    thread_id.as_str(),
                                    &error,
                                    cx,
                                );
                            });
                            return;
                        }
                    };
                    let next_cursor = page.next_cursor;
                    let applied = this.update(&mut cx, |view, cx| {
                        if view.gateway.ws_connection_id != Some(connection_id) {
                            view.finish_turn_resume_attempt(thread_id.as_str());
                            return false;
                        }
                        view.apply_turn_resume_items_page_reduction(page);
                        cx.notify();
                        true
                    });
                    if !matches!(applied, Ok(true)) {
                        return;
                    }

                    let Some(next_cursor) = next_cursor else {
                        break;
                    };
                    after_sequence = Some(next_cursor);
                }

                let reduction = thread_resume::reduce_turn_resume_turn_snapshot(
                    thread_id.as_str(),
                    turn_id.as_str(),
                    turn_snapshot,
                );
                let _ = this.update(&mut cx, |view, cx| {
                    view.finish_turn_resume_attempt(thread_id.as_str());
                    if view.gateway.ws_connection_id == Some(connection_id) {
                        view.apply_turn_resume_snapshot_reduction(connection_id, reduction, cx);
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn finish_turn_resume_attempt(&mut self, thread_id: &str) {
        if let Some(coordinator) = self.thread_coordinator_mut(thread_id) {
            thread_resume::finish_turn_resume_attempt(&mut coordinator.resume);
        }
    }

    fn finish_and_retry_turn_resume(
        &mut self,
        connection_id: u64,
        thread_id: &str,
        error: &anyhow::Error,
        cx: &mut Context<Self>,
    ) {
        self.finish_turn_resume_attempt(thread_id);
        if self.gateway.ws_connection_id != Some(connection_id) {
            return;
        }
        match thread_resume::plan_turn_resume_snapshot_failure(thread_id) {
            thread_resume::TurnResumeSnapshotFailurePlan::Retry { thread_id } => {
                self.schedule_turn_resume_retry(connection_id, thread_id.as_str(), error, cx);
            }
        }
        cx.notify();
    }

    fn apply_turn_resume_items_page_reduction(
        &mut self,
        reduction: thread_resume::TurnResumeItemsPageReduction,
    ) {
        self.upsert_thread_for_workspace(
            reduction.thread_id.as_str(),
            reduction.workspace_id.as_str(),
        );
        for event in reduction.replay_events {
            self.upsert_thread_conversation_mut(
                reduction.thread_id.as_str(),
                reduction.workspace_id.as_str(),
            )
            .apply(event);
        }
    }

    fn apply_turn_resume_snapshot_reduction(
        &mut self,
        connection_id: u64,
        reduction: thread_resume::TurnResumeSnapshotReduction,
        cx: &mut Context<Self>,
    ) {
        match reduction {
            thread_resume::TurnResumeSnapshotReduction::ScopeMismatch {
                expected_thread_id,
                actual_thread_id,
                expected_turn_id,
                actual_turn_id,
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
                    expected_turn_id = expected_turn_id.as_str(),
                    actual_turn_id = actual_turn_id.as_str(),
                    "turn/get returned snapshot outside the requested scope; retry later"
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
