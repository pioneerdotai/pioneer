use super::*;
use crate::app::conversation::ConversationEvent;

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

        resume.in_progress = true;
        resume.next_attempt_at = None;

        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let thread_id_for_sync = thread_id.clone();
            let turn_id_for_sync = turn_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let turn = ws_sender.turn_get(TurnGetParams {
                            thread_id: thread_id_for_sync.clone(),
                            turn_id: turn_id_for_sync.clone(),
                        })?;
                        let items = ws_sender.turn_items(TurnItemsParams {
                            thread_id: thread_id_for_sync,
                            turn_id: turn_id_for_sync,
                        })?;
                        Ok::<_, anyhow::Error>((turn, items))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if let Some(coordinator) = view.thread_coordinator_mut(thread_id.as_str()) {
                        coordinator.resume.in_progress = false;
                    }

                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok((turn_snapshot, item_snapshot)) => {
                            if turn_snapshot.thread_id != thread_id {
                                if let Some(resume) =
                                    view.thread_resume_state_mut(thread_id.as_str())
                                {
                                    resume.next_attempt_at =
                                        Some(std::time::Instant::now() + Duration::from_secs(5));
                                }
                                view.schedule_turn_resume_after(
                                    connection_id,
                                    thread_id.as_str(),
                                    Duration::from_secs(5),
                                    cx,
                                );
                                warn!(
                                    expected_thread_id = thread_id.as_str(),
                                    actual_thread_id = turn_snapshot.thread_id.as_str(),
                                    "turn/get returned snapshot for another thread; retry later"
                                );
                                return;
                            }

                            view.upsert_thread_for_workspace(
                                thread_id.as_str(),
                                turn_snapshot.workspace_id.as_str(),
                            );

                            for event in item_snapshot.events {
                                match event.payload {
                                    TurnItemEventPayload::ItemStarted {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemStarted {
                                                thread_id,
                                                turn_id,
                                                item,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemDelta {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        delta,
                                        stream,
                                        payload,
                                        markdown,
                                        markdown_version,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemDelta {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                delta,
                                                stream,
                                                payload,
                                                markdown,
                                                markdown_version,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemCompleted {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemCompleted {
                                                thread_id,
                                                turn_id,
                                                item,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemUpdated {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemUpdated {
                                                thread_id,
                                                turn_id,
                                                item,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemTimeoutDetected {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        attempt_number,
                                        reason,
                                        recovery_job_id,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemTimeoutDetected {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                attempt_number,
                                                reason,
                                                recovery_job_id,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemRecoveryOpened {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        recovery_job_id,
                                        attempt_number,
                                        ..
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemRecoveryOpened {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                recovery_job_id,
                                                attempt_number,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemRecoveryAttached {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        recovery_job_id,
                                        recovery_item_id,
                                        recovery_item_type,
                                        existing_status,
                                        next_attempt_number,
                                        ..
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemRecoveryAttached {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                recovery_job_id,
                                                recovery_item_id,
                                                recovery_item_type,
                                                existing_status,
                                                next_attempt_number,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemRetryScheduled {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        recovery_job_id,
                                        attempt_number,
                                        next_run_at_unix,
                                        reason,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemRetryScheduled {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                recovery_job_id,
                                                attempt_number,
                                                next_run_at_unix,
                                                reason,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemRetryAttemptStarted {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        recovery_job_id,
                                        attempt_number,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemRetryAttemptStarted {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                recovery_job_id,
                                                attempt_number,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemRecoverySucceeded {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        recovery_job_id,
                                        attempt_number,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemRecoverySucceeded {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                recovery_job_id,
                                                attempt_number,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemRecoveryExhausted {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        recovery_job_id,
                                        attempt_number,
                                        status,
                                        error_message,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemRecoveryExhausted {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                recovery_job_id,
                                                attempt_number,
                                                status,
                                                error_message,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemToolRetryScheduled {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        tool_retry_episode_id,
                                        tool_name,
                                        attempt_number,
                                        error_class,
                                        retry_hint,
                                        budgets,
                                        failure_signature_fingerprint,
                                        reason,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemToolRetryScheduled {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                tool_retry_episode_id,
                                                tool_name,
                                                attempt_number,
                                                error_class,
                                                retry_hint,
                                                budgets,
                                                failure_signature_fingerprint,
                                                reason,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemToolRetryResolved {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        tool_retry_episode_id,
                                        tool_name,
                                        attempt_number,
                                        resolution,
                                        budgets,
                                        reason,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemToolRetryResolved {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                tool_retry_episode_id,
                                                tool_name,
                                                attempt_number,
                                                resolution,
                                                budgets,
                                                reason,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::ItemToolRetryExhausted {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        item_id,
                                        item_type,
                                        tool_retry_episode_id,
                                        tool_name,
                                        attempt_number,
                                        error_class,
                                        exhaustion_kind,
                                        budgets,
                                        failure_signature_fingerprint,
                                        reason,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::ItemToolRetryExhausted {
                                                thread_id,
                                                turn_id,
                                                item_id,
                                                item_type,
                                                tool_retry_episode_id,
                                                tool_name,
                                                attempt_number,
                                                error_class,
                                                exhaustion_kind,
                                                budgets,
                                                failure_signature_fingerprint,
                                                reason,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::TurnToolLoopBudgetExceeded {
                                        workspace_id,
                                        thread_id,
                                        turn_id,
                                        limit_kind,
                                        limit,
                                        observed,
                                        action,
                                        reason,
                                    } => {
                                        if thread_id != turn_snapshot.thread_id
                                            || workspace_id != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::TurnToolLoopBudgetExceeded {
                                                thread_id,
                                                turn_id,
                                                limit_kind,
                                                limit,
                                                observed,
                                                action,
                                                reason,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::TurnExecutionWindowStarted(
                                        notification,
                                    ) => {
                                        if notification.thread_id != turn_snapshot.thread_id
                                            || notification.workspace_id
                                                != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        let thread_id = notification.thread_id.clone();
                                        let workspace_id = notification.workspace_id.clone();
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::TurnExecutionWindowStarted {
                                                notification,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::TurnExecutionWindowExhausted(
                                        notification,
                                    ) => {
                                        if notification.thread_id != turn_snapshot.thread_id
                                            || notification.workspace_id
                                                != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        let thread_id = notification.thread_id.clone();
                                        let workspace_id = notification.workspace_id.clone();
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::TurnExecutionWindowExhausted {
                                                notification,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::TurnExecutionWindowCheckpointed(
                                        notification,
                                    ) => {
                                        if notification.thread_id != turn_snapshot.thread_id
                                            || notification.workspace_id
                                                != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        let thread_id = notification.thread_id.clone();
                                        let workspace_id = notification.workspace_id.clone();
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::TurnExecutionWindowCheckpointed {
                                                notification,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::TurnExecutionWindowContinued(
                                        notification,
                                    ) => {
                                        if notification.thread_id != turn_snapshot.thread_id
                                            || notification.workspace_id
                                                != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        let thread_id = notification.thread_id.clone();
                                        let workspace_id = notification.workspace_id.clone();
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::TurnExecutionWindowContinued {
                                                notification,
                                            },
                                        );
                                    }
                                    TurnItemEventPayload::TurnExecutionWindowBlocked(
                                        notification,
                                    ) => {
                                        if notification.thread_id != turn_snapshot.thread_id
                                            || notification.workspace_id
                                                != turn_snapshot.workspace_id
                                        {
                                            continue;
                                        }
                                        let thread_id = notification.thread_id.clone();
                                        let workspace_id = notification.workspace_id.clone();
                                        view.upsert_thread_conversation_mut(
                                            thread_id.as_str(),
                                            workspace_id.as_str(),
                                        )
                                        .apply(
                                            ConversationEvent::TurnExecutionWindowBlocked {
                                                notification,
                                            },
                                        );
                                    }
                                }
                            }

                            let target_thread_id = turn_snapshot.thread_id.clone();
                            let target_workspace_id = turn_snapshot.workspace_id.clone();

                            match turn_snapshot.turn.status {
                                pioneer_protocol::TurnStatus::InProgress => {
                                    view.schedule_turn_resume_after(
                                        connection_id,
                                        target_thread_id.as_str(),
                                        Duration::from_millis(800),
                                        cx,
                                    );
                                }
                                pioneer_protocol::TurnStatus::Completed => {
                                    view.upsert_thread_conversation_mut(
                                        target_thread_id.as_str(),
                                        target_workspace_id.as_str(),
                                    )
                                    .apply(
                                        ConversationEvent::TurnCompleted {
                                            thread_id: target_thread_id.clone(),
                                            turn: turn_snapshot.turn,
                                        },
                                    );
                                    if let Some(conversation) =
                                        view.thread_conversation_mut(target_thread_id.as_str())
                                    {
                                        let _ = conversation.tick();
                                    }
                                    view.reset_thread_resume_state(target_thread_id.as_str());
                                }
                                pioneer_protocol::TurnStatus::Failed
                                | pioneer_protocol::TurnStatus::Interrupted => {
                                    view.upsert_thread_conversation_mut(
                                        target_thread_id.as_str(),
                                        target_workspace_id.as_str(),
                                    )
                                    .apply(
                                        ConversationEvent::TurnFailed {
                                            thread_id: target_thread_id.clone(),
                                            turn: turn_snapshot.turn,
                                        },
                                    );
                                    view.reset_thread_resume_state(target_thread_id.as_str());
                                }
                                pioneer_protocol::TurnStatus::Blocked => {
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
