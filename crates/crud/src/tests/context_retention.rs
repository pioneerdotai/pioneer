use super::*;

#[tokio::test]
async fn context_writes_only_wake_relevant_maintenance_and_replay_stays_quiet() {
    pioneer_postgres_test::with_database(async |fixture| {
        use futures_util::FutureExt;
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_context_wake",
            "thr_context_wake",
            "turn_context_wake",
        )
        .await;
        let database = store.database_connection();
        let mut frozen = database.work_signal("maintenance.frozen").subscribe();
        let mut html = database.work_signal("maintenance.html").subscribe();
        let mut tasks = database.work_signal("tasks.lifecycle").subscribe();
        for (index, (source, tool_name, relevant)) in [
            ("assistant_round", None, false),
            ("tool_result", Some("read_file"), false),
            ("tool_result_v2", Some("read_file"), false),
            ("tool_result_v2", Some("html_preview"), true),
        ]
        .into_iter()
        .enumerate()
        {
            let entry = NewTurnLlmContextEntry {
                turn_id: "turn_context_wake".into(),
                item_id: Some(format!("context-{index}")),
                attempt_id: None,
                sequence: 100 + index as i64,
                source: source.into(),
                tool_name: tool_name.map(str::to_owned),
                payload: "{}".into(),
                output_policy_snapshot: "{}".into(),
                created_at: unix_to_datetime(1_700_000_100),
                expires_at: None,
            };
            let key = format!("delivery-{index}");
            let first = store
                .append_turn_llm_context(entry.clone(), &key)
                .await
                .unwrap();
            assert!(!first.already_committed);
            assert!(frozen.changed().now_or_never().is_none());
            assert_eq!(html.changed().now_or_never().is_some(), relevant);
            assert!(tasks.changed().now_or_never().is_some());
            html.begin_pass(&database).await.unwrap();
            tasks.begin_pass(&database).await.unwrap();

            let replay = store
                .append_turn_llm_context(entry.clone(), &key)
                .await
                .unwrap();
            assert!(replay.already_committed);
            assert_eq!(replay.entry.id, first.entry.id);
            assert!(frozen.changed().now_or_never().is_none());
            assert!(html.changed().now_or_never().is_none());
            assert!(tasks.changed().now_or_never().is_none());

            // The direct insert is a distinct context entry, not another
            // write to the sequence already allocated by the append API.
            let mut direct = entry;
            direct.sequence = first.entry.sequence + 1;
            store.insert_turn_llm_context(direct).await.unwrap();
            assert!(frozen.changed().now_or_never().is_none());
            assert_eq!(html.changed().now_or_never().is_some(), relevant);
            assert!(tasks.changed().now_or_never().is_some());
            html.begin_pass(&database).await.unwrap();
            tasks.begin_pass(&database).await.unwrap();
        }
        // Removing transient context can release HTML protection; a second
        // cleanup is a known no-op, and neither operation changes frozen roots.
        assert!(
            store
                .delete_turn_llm_context_for_turn("turn_context_wake")
                .await
                .unwrap()
                > 0
        );
        assert!(html.changed().now_or_never().is_some());
        assert!(frozen.changed().now_or_never().is_none());
        html.begin_pass(&database).await.unwrap();
        assert_eq!(
            store
                .delete_turn_llm_context_for_turn("turn_context_wake")
                .await
                .unwrap(),
            0
        );
        assert!(html.changed().now_or_never().is_none());
        assert!(frozen.changed().now_or_never().is_none());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_context_append_keeps_only_relevant_uncertainty_and_writer_fence() {
    use futures_util::FutureExt;
    for preview in [false, true] {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, _, _) = postgres_store_with_started_turn(
                fixture,
                "ws_context_cancel",
                "thr_context_cancel",
                "turn_context_cancel",
            )
            .await;
            let database = store.database_connection();
            let mut html = database.work_signal("maintenance.html").subscribe();
            let mut frozen = database.work_signal("maintenance.frozen").subscribe();
            let transaction = database.begin().await.unwrap();
            let entry = NewTurnLlmContextEntry {
                turn_id: "turn_context_cancel".into(),
                item_id: Some("cancelled".into()),
                attempt_id: None,
                sequence: 0,
                source: "tool_result_v2".into(),
                tool_name: Some(if preview { "html_preview" } else { "read_file" }.into()),
                payload: "{}".into(),
                output_policy_snapshot: "{}".into(),
                created_at: unix_to_datetime(1_700_000_100),
                expires_at: None,
            };
            let mut append = Box::pin(store.append_turn_llm_context(entry, "cancelled"));
            assert!(
                append.as_mut().now_or_never().is_none(),
                "writer is occupied"
            );
            drop(append);
            assert_eq!(html.changed().now_or_never().is_some(), preview);
            assert!(frozen.changed().now_or_never().is_none());
            if preview {
                let mut fence = Box::pin(html.begin_pass(&database));
                assert!(
                    fence.as_mut().now_or_never().is_none(),
                    "unknown outcome must settle the writer"
                );
                transaction.rollback().await.unwrap();
                fence.await.unwrap();
            } else {
                transaction.rollback().await.unwrap();
            }
            assert!(
                store
                    .list_turn_llm_context("turn_context_cancel")
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(html.changed().now_or_never().is_none());
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn shell_controls_release_only_after_canonical_or_definitive_recovery_settlement() {
    pioneer_postgres_test::with_database(async |fixture| {
        let now = 1_700_000_000;
        let (store, thread, mut failed) = postgres_store_with_started_turn(fixture,
            "ws_shell_controls",
            "thread_shell_controls",
            "turn_shell_controls",
        )
        .await;
        let (_, _, aborted) = start_test_turn(
            store.clone(),
            "ws_shell_controls",
            &thread.id,
            "turn_shell_abort",
        )
        .await;
        let (_, _, foreign) = start_test_turn(
            store.clone(),
            "ws_shell_controls",
            &thread.id,
            "turn_shell_foreign",
        )
        .await;
        for turn_id in [&failed.id, &aborted.id] {
            turn_execution::insert_immutable(
                &store.connection,
                NewTurnExecution {
                    turn_id: turn_id.clone(),
                    thread_id: thread.id.clone(),
                    workspace_id: thread.workspace_id.clone(),
                    executor_kind: TurnExecutorKind::NativeAgent,
                    executor_key: None,
                    status: TurnExecutionStatus::Running,
                    owner_id: "owner".into(),
                    lease_until: unix_to_datetime(now + 60),
                    created_at: unix_to_datetime(now),
                },
            )
            .await
            .unwrap();
        }
        for (index, (turn_id, item, expiry)) in [
            (&failed.id, "abandoned", None),
            (&failed.id, "expired", Some(unix_to_datetime(now - 1))),
            (&failed.id, "successful", None),
            (&aborted.id, "abandoned", None),
            (&foreign.id, "abandoned", None),
        ]
        .into_iter()
        .enumerate()
        {
            store
                .insert_turn_llm_context(NewTurnLlmContextEntry {
                    turn_id: turn_id.clone(),
                    item_id: Some(item.into()),
                    attempt_id: None,
                    sequence: index as i64 + 1,
                    source: "shell_control_v1".into(),
                    tool_name: Some("write_stdin".into()),
                    payload: r#"{"kind":"json","value":{"session_id":27},"truncated":false}"#
                        .into(),
                    output_policy_snapshot: serde_json::to_string(
                        &ToolOutputPolicySnapshot::for_tool_name("write_stdin"),
                    )
                    .unwrap(),
                    created_at: unix_to_datetime(now),
                    expires_at: expiry,
                })
                .await
                .unwrap();
        }
        assert_eq!(store.delete_expired_turn_llm_context().await.unwrap(), 0);
        assert_eq!(
            store
                .delete_turn_llm_context_for_turn(&failed.id)
                .await
                .unwrap(),
            0
        );
        // A real canonical append settles only its own receipt, even on an active Turn.
        let message = serde_json::json!({"role":"tool","tool_call_id":"call-success","name":"write_stdin","content":r#"{"session_id":27}"#});
        store
            .append_turn_llm_context(
                NewTurnLlmContextEntry {
                    turn_id: failed.id.clone(),
                    item_id: Some("successful".into()),
                    attempt_id: None,
                    sequence: 0,
                    source: "tool_result_v2".into(),
                    tool_name: Some("write_stdin".into()),
                    payload: serde_json::to_string(&pioneer_protocol::ToolResultView::Json {
                        value: serde_json::to_value(message).unwrap(),
                        truncated: false,
                    })
                    .unwrap(),
                    output_policy_snapshot: "{}".into(),
                    created_at: unix_to_datetime(now),
                    expires_at: None,
                },
                "successful-delivery",
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .release_settled_shell_controls(&failed.id)
                .await
                .unwrap(),
            1
        );
        assert!(
            store
                .shell_control_receipt(&thread.workspace_id, &thread.id, &failed.id, "abandoned")
                .await
                .unwrap()
                .is_some()
        );
        let job = store
            .enqueue_recovery_job(
                failed.id.clone(),
                "abandoned".into(),
                TurnItemType::CommandExecution,
                None,
                RecoveryTrigger::ProjectionFailure,
                RecoveryAction::RetryWithBackoff,
                None,
                None,
                None,
                None,
                1,
                2,
                serde_json::json!({}),
                serde_json::json!({}),
                now + 1,
            )
            .await
            .unwrap();
        failed.status = TurnStatus::Failed;
        failed.error = Some("terminal materialization/budget failed after control ACK".into());
        store
            .materialize_turn_failed(
                TurnFailedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn: failed.clone(),
                },
                now + 2,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .release_settled_shell_controls(&failed.id)
                .await
                .unwrap(),
            0,
            "pending recovery owns the receipt after terminal status"
        );
        assert_eq!(
            store
                .delete_turn_llm_context_for_terminal_turns()
                .await
                .unwrap(),
            0
        );
        assert_eq!(store.delete_expired_turn_llm_context().await.unwrap(), 0);
        assert!(
            !store
                .list_turn_llm_context(&failed.id)
                .await
                .unwrap()
                .iter()
                .any(|row| row.item_id.as_deref() == Some("abandoned")
                    && row.source == "tool_result_v2")
        );
        assert_eq!(
            store
                .cancel_open_recovery_jobs_for_turn(
                    &failed.id,
                    None,
                    Some("definitive cancellation".into()),
                    now + 3
                )
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .get_recovery_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RecoveryJobStatus::Cancelled
        );
        let maintenance = store.with_maintenance_access();
        let (interactive, background) = tokio::join!(
            store.release_settled_shell_controls(&failed.id),
            maintenance.release_settled_shell_controls(&failed.id)
        );
        assert_eq!(
            interactive.unwrap() + background.unwrap(),
            0,
            "definitive cancellation already released controls atomically"
        );
        assert!(
            store
                .shell_control_receipt(&thread.workspace_id, &thread.id, &failed.id, "abandoned")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .shell_control_receipt(
                    &thread.workspace_id,
                    &thread.id,
                    "turn_shell_foreign",
                    "abandoned"
                )
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .shell_control_receipt(&thread.workspace_id, &thread.id, &aborted.id, "abandoned")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            !store
                .list_turn_llm_context(&failed.id)
                .await
                .unwrap()
                .iter()
                .any(|row| row.item_id.as_deref() == Some("abandoned")
                    && row.source == "tool_result_v2")
        );
        // Exercise both real Blocked-job orderings; the audit status must not
        // be rewritten to Failed/Cancelled merely to release control metadata.
        for settle_before_abort in [true, false] {
            let mut blocked = if settle_before_abort {
                aborted.clone()
            } else {
                let (_, _, blocked) = start_test_turn(
                    store.clone(),
                    &thread.workspace_id,
                    &thread.id,
                    "turn_shell_abort_pending",
                )
                .await;
                turn_execution::insert_immutable(
                    &store.connection,
                    NewTurnExecution {
                        turn_id: blocked.id.clone(),
                        thread_id: thread.id.clone(),
                        workspace_id: thread.workspace_id.clone(),
                        executor_kind: TurnExecutorKind::NativeAgent,
                        executor_key: None,
                        status: TurnExecutionStatus::Running,
                        owner_id: "owner".into(),
                        lease_until: unix_to_datetime(now + 60),
                        created_at: unix_to_datetime(now),
                    },
                )
                .await
                .unwrap();
                store
                    .insert_turn_llm_context(NewTurnLlmContextEntry {
                        turn_id: blocked.id.clone(),
                        item_id: Some("abandoned".into()),
                        attempt_id: None,
                        sequence: 1,
                        source: "shell_control_v1".into(),
                        tool_name: Some("write_stdin".into()),
                        payload: "{}".into(),
                        output_policy_snapshot: "{}".into(),
                        created_at: unix_to_datetime(now),
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                blocked
            };
            let job = store
                .enqueue_recovery_job(
                    blocked.id.clone(),
                    "abandoned".into(),
                    TurnItemType::CommandExecution,
                    None,
                    RecoveryTrigger::ProjectionFailure,
                    RecoveryAction::RetryWithBackoff,
                    None,
                    None,
                    None,
                    None,
                    1,
                    2,
                    serde_json::json!({}),
                    serde_json::json!({}),
                    now + 3,
                )
                .await
                .unwrap();
            assert!(
                store
                    .mark_recovery_job_terminal(
                        &job.id,
                        RecoveryJobStatus::Blocked,
                        Some("await explicit resume".into()),
                        now + 3
                    )
                    .await
                    .unwrap()
            );
            blocked.status = TurnStatus::Blocked;
            blocked.error = Some("await explicit resume".into());
            store
                .materialize_turn_blocked(
                    pioneer_protocol::TurnBlockedNotification {
                        workspace_id: thread.workspace_id.clone(),
                        thread_id: thread.id.clone(),
                        turn: blocked.clone(),
                        resume: None,
                    },
                    now + 4,
                )
                .await
                .unwrap();
            let claims = store
                .claim_due_recovery_terminalizations(now + 5, 45, 10)
                .await
                .unwrap();
            assert_eq!(claims.len(), 1);
            if settle_before_abort {
                assert!(matches!(
                    store
                        .apply_claimed_recovery_terminalization(
                            &claims[0],
                            None,
                            RecoveryTerminalCleanupPlan {
                                runtime_generation: 1,
                                runtime_contract: "pioneer.test.attached-task-cleanup.v1".into(),
                            },
                            now + 5,
                        )
                        .await
                        .unwrap(),
                    RecoveryTerminalizationApplyOutcome::Applied(_)
                ));
                assert!(
                    !store
                        .get_recovery_job(&job.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .resolution_pending
                );
            }
            assert_eq!(
                store
                    .delete_turn_llm_context_for_turn(&blocked.id)
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                store
                    .release_settled_shell_controls(&blocked.id)
                    .await
                    .unwrap(),
                0
            );
            assert!(
                store
                    .shell_control_receipt(
                        &thread.workspace_id,
                        &thread.id,
                        &blocked.id,
                        "abandoned"
                    )
                    .await
                    .unwrap()
                    .is_some(),
                "Blocked Turn still owns its undelivered controls"
            );
            assert!(
                !store
                    .list_turn_llm_context(&blocked.id)
                    .await
                    .unwrap()
                    .iter()
                    .any(|row| row.source == "tool_result_v2")
            );
            let execution = turn_execution::find(&store.connection, &blocked.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(execution.status, TurnExecutionStatus::Blocked);
            assert!(execution.completed_at.is_some());
            // This reaches the existing tool-resume snapshot gate, rather than
            // rejecting the Blocked Turn/job as absent. No execution is started.
            assert!(matches!(
                store
                    .resume_blocked_turn_recovery(
                        &thread.id,
                        &blocked.id,
                        Some(&job.id),
                        now + 6,
                        "owner",
                        now + 60
                    )
                    .await
                    .unwrap(),
                BlockedTurnRecoveryResumeOutcome::MissingRuntimeSnapshot { .. }
            ));
            // Use the existing authoritative status transaction, including its
            // status history, terminal execution settlement and child publication.
            // Do not append a conflicting second canonical terminal event.
            assert!(
                store
                    .update_turn_status(
                        &thread.id,
                        &blocked.id,
                        TurnStatus::Interrupted,
                        Some("definitive abort without canonical append"),
                        now + 7
                    )
                    .await
                    .unwrap()
            );
            assert!(matches!(
                store
                    .resume_blocked_turn_recovery(
                        &thread.id,
                        &blocked.id,
                        Some(&job.id),
                        now + 8,
                        "owner",
                        now + 60
                    )
                    .await
                    .unwrap(),
                BlockedTurnRecoveryResumeOutcome::NotFound
            ));
            if settle_before_abort {
                let (foreground, background) = tokio::join!(
                    store.release_settled_shell_controls(&blocked.id),
                    maintenance.release_settled_shell_controls(&blocked.id)
                );
                assert_eq!(foreground.unwrap() + background.unwrap(), 1);
            } else {
                assert_eq!(
                    store
                        .delete_turn_llm_context_for_terminal_turns()
                        .await
                        .unwrap(),
                    0,
                    "pending outbox must survive definitive abort until supersession"
                );
                assert_eq!(
                    store
                        .classify_claimed_recovery_terminalization(&claims[0], now + 8)
                        .await
                        .unwrap(),
                    RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(
                        TurnStatus::Interrupted
                    )
                );
            }
            let audit = store.get_recovery_job(&job.id).await.unwrap().unwrap();
            assert_eq!(audit.status, RecoveryJobStatus::Blocked);
            assert!(!audit.resolution_pending);
            assert!(
                store
                    .list_turn_llm_context(&blocked.id)
                    .await
                    .unwrap()
                    .is_empty(),
                "no receipt or fabricated canonical row after definitive settlement"
            );
            assert_eq!(
                store
                    .delete_turn_llm_context_for_turn(&blocked.id)
                    .await
                    .unwrap(),
                0
            );
            assert!(
                store
                    .shell_control_receipt(
                        &thread.workspace_id,
                        &thread.id,
                        &foreign.id,
                        "abandoned"
                    )
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(
            store.list_turn_llm_context(&failed.id).await.unwrap().len(),
            1,
            "successful canonical remains intact"
        );
        Ok(())
    }).await.unwrap();
}
