use super::*;

#[tokio::test]
async fn native_terminal_effect_outbox_activates_atomically_and_recovers_expired_claim_after_restart()
 {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_terminal_effect_atomic";
        let thread_id = "thr_terminal_effect_atomic";
        let turn_id = "turn_terminal_effect_atomic";
        let (store, _, mut turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let preparation =
            cleanup_effect_preparation(workspace_id, thread_id, turn_id, "generation-1");
        let effect_id = preparation.effects[0].effect_id.clone();
        store
            .prepare_native_terminal_effects(preparation.clone(), 1_700_000_100)
            .await
            .expect("terminal effect should prepare");
        store
            .prepare_native_terminal_effects(preparation.clone(), 1_700_000_100)
            .await
            .expect("exact preparation replay should be idempotent");
        assert_eq!(
            store
                .native_terminal_effect_status(effect_id.as_str())
                .await
                .expect("effect status should load")
                .expect("effect should exist")
                .status,
            "prepared"
        );

        store
            .connection
            .execute_unprepared(
                "CREATE FUNCTION reject_terminal_effect_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected terminal commit failure'; END $$; CREATE TRIGGER reject_terminal_effect_commit BEFORE UPDATE OF status ON turn FOR EACH ROW WHEN (NEW.id = 'turn_terminal_effect_atomic' AND NEW.status = 'completed') EXECUTE FUNCTION reject_terminal_effect_commit()",
            )
            .await
            .expect("fault trigger should install");
        turn.status = TurnStatus::Completed;
        assert!(
            store
                .materialize_turn_completed(
                    TurnCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn: turn.clone(),
                    },
                    1_700_000_101,
                )
                .await
                .is_err(),
            "terminal projection fault must roll back outbox activation"
        );
        assert_eq!(
            store
                .native_terminal_effect_status(effect_id.as_str())
                .await
                .expect("effect status should load after rollback")
                .expect("effect should remain")
                .status,
            "prepared"
        );
        store
            .connection
            .execute_unprepared("DROP TRIGGER reject_terminal_effect_commit ON turn")
            .await
            .expect("fault trigger should uninstall");
        let replay = store
            .replay_due_turn_event_projections(1_700_000_103, 10)
            .await
            .expect("durable terminal projection should recover after the fault");
        assert_eq!(replay.projected, 1);
        assert_eq!(
            store
                .native_terminal_effect_status(effect_id.as_str())
                .await
                .expect("effect status should load after terminal commit")
                .expect("effect should remain")
                .status,
            "ready"
        );
        let committed_row =
            pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(effect_id.clone())
                .one(&store.connection)
                .await
                .expect("committed effect lookup should succeed")
                .expect("committed effect should exist");
        let immutable_payload_hash = committed_row.payload_identity_sha256.clone();

        let late_effect = pioneer_protocol::NativeTerminalEffectPreparation {
            batch_id: format!("{turn_id}:batch:late"),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            runtime_generation: 1,
            effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                effect_id: format!("{turn_id}:terminal-effect:post-turn"),
                effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                payload:
                    pioneer_protocol::NativeTerminalEffectPayload::PostTurnHookPreparationFailed {
                        failure: pioneer_protocol::NativeTerminalEffectPreparationFailure::HookRuntimeUnavailable,
                    },
                max_attempts: 3,
            }],
        };
        assert!(
            store
                .prepare_native_terminal_effects(late_effect, 1_700_000_103)
                .await
                .is_err(),
            "new obligations cannot be invented after the canonical terminal commit"
        );

        let first_claim = store
            .claim_due_native_terminal_effects_at(1_700_000_103, 5, 10)
            .await
            .expect("ready effect should claim")
            .records;
        assert_eq!(first_claim.len(), 1);
        assert_eq!(first_claim[0].attempt_count, 1);
        assert_eq!(
            store
                .next_native_terminal_effect_execution_deadline()
                .await
                .unwrap(),
            Some(1_700_000_108),
            "the claimed row retains its lease wake after an empty claim"
        );

        // A fresh store models restart after the side-effect worker claimed
        // the row but before it acknowledged an outcome.
        let restarted = CrudStore::new(store.database_connection());
        assert!(
            restarted
                .claim_due_native_terminal_effects_at(1_700_000_106, 5, 10)
                .await
                .expect("unexpired claim lookup should succeed")
                .records
                .is_empty()
        );
        let reclaimed = restarted
            .claim_due_native_terminal_effects_at(1_700_000_109, 5, 10)
            .await
            .expect("expired claim should recover after restart")
            .records;
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].effect_id, effect_id);
        assert_eq!(reclaimed[0].attempt_count, 2);
        assert!(
            restarted
                .complete_native_terminal_effect(
                    reclaimed[0].effect_id.as_str(),
                    reclaimed[0].claim_token.as_str(),
                    1_700_000_109,
                )
                .await
                .expect("effect completion should persist")
        );
        assert!(
            restarted
                .claim_due_native_terminal_effects_at(1_700_000_200, 5, 10)
                .await
                .expect("succeeded effect lookup should succeed")
                .records
                .is_empty(),
            "a succeeded obligation must never be claimed twice"
        );
        let compacted =
            pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(effect_id.clone())
                .one(&restarted.connection)
                .await
                .expect("compacted effect lookup should succeed")
                .expect("compacted effect should remain");
        assert_eq!(compacted.payload_json, r#"{"compacted":true}"#);
        assert!(
            restarted
                .next_native_terminal_effect_execution_deadline()
                .await
                .unwrap()
                .is_none(),
            "completed history does not arm another discovery timer"
        );
        assert_eq!(
            compacted.payload_sha256,
            native_terminal_effect_outbox::payload_sha256_hex(compacted.payload_json.as_str())
        );
        assert_eq!(compacted.payload_identity_sha256, immutable_payload_hash);
        assert_eq!(compacted.payload_sha256.len(), 64);

        restarted
            .prepare_native_terminal_effects(preparation.clone(), 1_700_000_201)
            .await
            .expect("exact committed replay must use the retained payload hash");
        let mut conflicting = preparation;
        conflicting.effects[0].payload =
            pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
                reason: "conflicting cleanup reason".to_owned(),
                runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
            };
        assert!(
            restarted
                .prepare_native_terminal_effects(conflicting, 1_700_000_202)
                .await
                .is_err(),
            "compaction must not weaken immutable replay conflict detection"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn terminal_effect_preparation_failure_becomes_explicit_unresolved_obligation() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_004_000;
        let workspace_id = "ws_terminal_effect_prepare_failure";
        let thread_id = "thr_terminal_effect_prepare_failure";
        let turn_id = "turn_terminal_effect_prepare_failure";
        let (store, _, mut terminal_turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let effect_id = format!("{turn_id}:terminal-effect:post-turn");
        store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: format!("{turn_id}:batch:prepare-failure"),
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    runtime_generation: 1,
                    effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                        effect_id: effect_id.clone(),
                        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                        gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHookPreparationFailed {
                            failure: pioneer_protocol::NativeTerminalEffectPreparationFailure::PayloadTooLarge,
                        },
                        max_attempts: 3,
                    }],
                },
                timestamp,
            )
            .await
            .expect("typed preparation failure must persist before terminal commit");
        terminal_turn.status = TurnStatus::Failed;
        store
            .materialize_turn_failed(
                TurnFailedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: terminal_turn,
                },
                timestamp + 1,
            )
            .await
            .expect("terminal commit should activate failure accounting");

        let status = store
            .native_terminal_effect_status(effect_id.as_str())
            .await
            .expect("failure status should load")
            .expect("failure obligation should remain inspectable");
        assert_eq!(status.status, "unresolved");
        assert_eq!(
            status.last_error_code.as_deref(),
            Some("terminal_effect_preparation_failed")
        );
        assert!(
            store
                .claim_due_native_terminal_effects_at(timestamp + 2, 10, 10)
                .await
                .expect("failure claim scan should succeed")
                .records
                .is_empty(),
            "a preparation failure is diagnostic state, never an executable fake request"
        );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn acceptance_gated_preparation_failure_waits_for_authoritative_acceptance() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_004_100;
        let workspace_id = "ws_terminal_effect_gated_prepare_failure";
        let thread_id = "thr_terminal_effect_gated_prepare_failure";
        let turn_id = "turn_terminal_effect_gated_prepare_failure";
        let (store, _, mut terminal_turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let effect_id = format!("{turn_id}:terminal-effect:post-turn");
        store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: format!("{turn_id}:batch:gated-prepare-failure"),
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    runtime_generation: 1,
                    effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                        effect_id: effect_id.clone(),
                        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                        gate: pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHookPreparationFailed {
                            failure: pioneer_protocol::NativeTerminalEffectPreparationFailure::HandlerSnapshotUnavailable,
                        },
                        max_attempts: 3,
                    }],
                },
                timestamp,
            )
            .await
            .expect("gated preparation failure should persist");
        terminal_turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: terminal_turn,
                },
                timestamp + 1,
            )
            .await
            .expect("terminal commit should preserve the unresolved gate");
        assert_eq!(
            store
                .native_terminal_effect_status(effect_id.as_str())
                .await
                .expect("gated failure status should load")
                .expect("gated failure should exist")
                .status,
            "waiting_acceptance"
        );

        let candidate = terminal_effect_gates::candidate_fixture(
            &store,
            thread_id,
            turn_id,
            "gated_failure",
            TaskResultCandidateStatus::Accepted,
            timestamp + 2,
        )
        .await;
        store
            .upsert_task_result_candidate(candidate)
            .await
            .expect("authoritative acceptance should resolve the failure gate");
        let status = store
            .native_terminal_effect_status(effect_id.as_str())
            .await
            .expect("resolved failure status should load")
            .expect("resolved failure should remain inspectable");
        assert_eq!(status.status, "unresolved");
        assert_eq!(
            status.last_error_code.as_deref(),
            Some("terminal_effect_preparation_failed")
        );
        assert!(
            store
                .claim_due_native_terminal_effects_at(timestamp + 3, 10, 1)
                .await
                .expect("preparation failure scan should succeed")
                .records
                .is_empty(),
            "a typed preparation failure must never become executable work"
        );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn native_terminal_cleanup_activates_for_interrupted_turn() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_005_500;
        let workspace_id = "ws_interrupted_cleanup";
        let thread_id = "thr_interrupted_cleanup";
        let turn_id = "turn_interrupted_cleanup";
        let (store, _, mut interrupted_turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let preparation = cleanup_effect_preparation(workspace_id, thread_id, turn_id, "cancelled");
        let effect_id = preparation.effects[0].effect_id.clone();
        store
            .prepare_native_terminal_effects(preparation, timestamp)
            .await
            .expect("cancel cleanup should prepare before terminal commit");

        interrupted_turn.status = TurnStatus::Interrupted;
        interrupted_turn.error = Some("cancelled by user".to_owned());
        store
            .materialize_turn_failed(
                TurnFailedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: interrupted_turn,
                },
                timestamp + 1,
            )
            .await
            .expect("interrupted canonical terminal transaction should commit");

        let claim = store
            .claim_due_native_terminal_effects_at(timestamp + 1, 10, 1)
            .await
            .expect("interrupted cleanup should become claimable")
            .records
            .pop()
            .expect("interrupted cleanup claim");
        assert_eq!(claim.effect_id, effect_id);
        assert!(matches!(
            claim.payload,
            pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup { .. }
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn recovered_turn_supersedes_prepared_provider_failure_cleanup_before_terminal_commit() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_006_000;
        let workspace_id = "ws_recovered_cleanup";
        let thread_id = "thr_recovered_cleanup";
        let turn_id = "turn_recovered_cleanup";
        let (store, _, mut terminal_turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let preparation =
            cleanup_effect_preparation(workspace_id, thread_id, turn_id, "provider-failure");
        let cleanup_effect_id = preparation.effects[0].effect_id.clone();
        store
            .prepare_native_terminal_effects(preparation, timestamp)
            .await
            .expect("provider failure cleanup should prepare");

        store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: format!("{turn_id}:batch:recovered-success"),
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    runtime_generation: 2,
                    effects: Vec::new(),
                },
                timestamp + 1,
            )
            .await
            .expect("recovered terminal plan should supersede stale cleanup");
        assert_eq!(
            store
                .native_terminal_effect_status(cleanup_effect_id.as_str())
                .await
                .expect("cleanup status should load")
                .expect("superseded cleanup should remain inspectable")
                .status,
            "superseded"
        );

        terminal_turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: terminal_turn,
                },
                timestamp + 2,
            )
            .await
            .expect("recovered Turn should commit successfully");
        assert!(
            store
                .claim_due_native_terminal_effects_at(timestamp + 2, 10, 10)
                .await
                .expect("stale cleanup scan should succeed")
                .records
                .is_empty(),
            "successful recovery must not cancel attached tasks using the stale provider-failure plan"
        );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn native_terminal_effect_outbox_converges_for_acceptance_before_completion() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_010_000;
        let workspace_id = "ws_terminal_effect_accept";
        let thread_id = "thr_terminal_effect_accept";
        let turn_id = "turn_terminal_effect_accept";
        let (store, _, mut terminal_turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;

        let mut task = sample_task(timestamp);
        task.id = "task_terminal_effect_accept".to_owned();
        task.workspace_id = workspace_id.to_owned();
        task.created_by_thread_id = Some("parent_thread".to_owned());
        task.created_by_turn_id = Some("parent_turn".to_owned());
        let mut run = sample_task_run(timestamp);
        run.id = "run_terminal_effect_accept".to_owned();
        run.task_id = task.id.clone();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();
        store
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::RunCreated {
                        run: run.clone(),
                        agent_spec: None,
                    },
                ],
                timestamp,
            )
            .await
            .expect("task aggregate should persist");
        let task_run_turn = TaskRunTurn {
            id: "task_run_turn_effect_accept".to_owned(),
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            execution_id: None,
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: timestamp,
            started_at: Some(timestamp),
            completed_at: Some(timestamp),
        };
        store
            .upsert_task_run_turn(task_run_turn.clone())
            .await
            .expect("task-run Turn should persist");
        let candidate_id = "candidate_effect_accept";
        store
            .upsert_task_result_candidate(TaskResultCandidate {
                id: candidate_id.to_owned(),
                task_id: task.id,
                run_id: run.id,
                task_run_turn_id: task_run_turn.id,
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                round: 0,
                status: TaskResultCandidateStatus::Accepted,
                result: Some(TaskResult {
                    summary: Some("accepted before completion".to_owned()),
                    data: None,
                    artifacts: Vec::new(),
                    completed_by_run_id: Some("run_terminal_effect_accept".to_owned()),
                }),
                extraction_error: None,
                summary: Some("accepted before completion".to_owned()),
                diagnostics: Vec::new(),
                final_review_event_id: Some("review_effect_accept".to_owned()),
                created_at: timestamp,
                updated_at: timestamp,
                resolved_at: Some(timestamp),
            })
            .await
            .expect("accepted candidate should persist before outbox preparation");

        let effect_id = format!("{turn_id}:terminal-effect:post-turn");
        let preparation = pioneer_protocol::NativeTerminalEffectPreparation {
            batch_id: format!("{turn_id}:batch:accepted"),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            runtime_generation: 1,
            effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                effect_id: effect_id.clone(),
                effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                gate: pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                    request: serde_json::json!({"durable": true}),
                    runtime_snapshot: serde_json::json!({
                        "schema_version": 1,
                        "subscriptions": [],
                        "handlers": []
                    }),
                },
                max_attempts: 5,
            }],
        };
        store
            .prepare_native_terminal_effects(preparation.clone(), timestamp + 1)
            .await
            .expect("candidate-gated effect should prepare");
        terminal_turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: terminal_turn,
                },
                timestamp + 2,
            )
            .await
            .expect("terminal commit should observe prior durable acceptance");
        let status = store
            .native_terminal_effect_status(effect_id.as_str())
            .await
            .expect("effect status should load")
            .expect("effect should exist");
        assert_eq!(status.status, "ready");
        assert_eq!(status.accepted_candidate_id.as_deref(), Some(candidate_id));
        store
            .prepare_native_terminal_effects(preparation, timestamp + 3)
            .await
            .expect("exact preparation after terminal commit should be idempotent");
        let status = store
            .native_terminal_effect_status(effect_id.as_str())
            .await
            .expect("effect status should reload")
            .expect("effect should remain");
        assert_eq!(status.status, "ready");
        assert_eq!(status.attempt_count, 0);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn atomic_auto_accept_batch_prepares_each_gate_after_prior_event_projection() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_015_000;
        let workspace_id = "ws_atomic_auto_accept";
        let thread_id = "thr_atomic_auto_accept";
        let turn_id = "turn_atomic_auto_accept";
        let (store, _, mut terminal_turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let effect_id = format!("{turn_id}:terminal-effect:post-turn");
        store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: format!("{turn_id}:batch:auto-accept"),
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    runtime_generation: 1,
                    effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                        effect_id: effect_id.clone(),
                        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                        gate: pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                            request: serde_json::json!({"durable": true}),
                            runtime_snapshot: serde_json::json!({
                                "schema_version": 1,
                                "subscriptions": [],
                                "handlers": []
                            }),
                        },
                        max_attempts: 5,
                    }],
                },
                timestamp,
            )
            .await
            .expect("candidate-gated effect should prepare");
        terminal_turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: terminal_turn,
                },
                timestamp + 1,
            )
            .await
            .expect("terminal commit should leave the effect waiting for acceptance");

        let mut task = sample_task(timestamp + 2);
        task.id = "task_atomic_auto_accept".to_owned();
        task.workspace_id = workspace_id.to_owned();
        task.created_by_thread_id = Some("parent_thread_atomic_auto_accept".to_owned());
        task.created_by_turn_id = Some("parent_turn_atomic_auto_accept".to_owned());
        let mut run = sample_task_run(timestamp + 2);
        run.id = "run_atomic_auto_accept".to_owned();
        run.task_id = task.id.clone();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();
        store
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::RunCreated {
                        run: run.clone(),
                        agent_spec: None,
                    },
                ],
                timestamp + 2,
            )
            .await
            .expect("task aggregate should persist");

        let task_run_turn = TaskRunTurn {
            id: "task_run_turn_atomic_auto_accept".to_owned(),
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            execution_id: None,
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: timestamp + 3,
            started_at: Some(timestamp + 3),
            completed_at: Some(timestamp + 4),
        };
        let review_event_id = "review_atomic_auto_accept";
        let candidate = TaskResultCandidate {
            id: "candidate_atomic_auto_accept".to_owned(),
            task_id: task.id,
            run_id: run.id,
            task_run_turn_id: task_run_turn.id.clone(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            round: 0,
            status: TaskResultCandidateStatus::Accepted,
            result: Some(TaskResult {
                summary: Some("auto accepted".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some("run_atomic_auto_accept".to_owned()),
            }),
            extraction_error: None,
            summary: Some("auto accepted".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: Some(review_event_id.to_owned()),
            created_at: timestamp + 4,
            updated_at: timestamp + 4,
            resolved_at: Some(timestamp + 4),
        };
        let review_event = TaskResultReviewEvent {
            id: review_event_id.to_owned(),
            candidate_id: candidate.id.clone(),
            task_id: candidate.task_id.clone(),
            run_id: candidate.run_id.clone(),
            task_run_turn_id: task_run_turn.id.clone(),
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer: pioneer_protocol::TaskResultReviewerRef::RuntimePolicy,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::SystemAuto,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: timestamp + 4,
        };

        let events = vec![
            TaskEventPayload::TaskRunTurnCompleted {
                task_run_turn: task_run_turn.clone(),
            },
            TaskEventPayload::TaskResultCandidateCreated {
                candidate: candidate.clone(),
            },
            TaskEventPayload::TaskResultReviewEventRecorded {
                review_event: review_event.clone(),
            },
            TaskEventPayload::TaskResultCandidateAccepted {
                candidate: candidate.clone(),
                review_event_id: review_event.id.clone(),
            },
        ];
        store
            .append_task_events(events.clone(), timestamp + 4)
            .await
            .expect("auto-accept batch should observe each preceding projection");
        store
            .append_task_events(events, timestamp + 4)
            .await
            .expect("auto-accept batch replay should be idempotent");

        let status = store
            .native_terminal_effect_status(effect_id.as_str())
            .await
            .expect("effect status should load")
            .expect("effect should remain durable");
        assert_eq!(status.status, "ready");
        assert_eq!(
            status.accepted_candidate_id.as_deref(),
            Some(candidate.id.as_str())
        );
        assert_eq!(
            store
                .get_accepted_task_result_candidate(candidate.run_id.as_str())
                .await
                .expect("accepted candidate should load")
                .map(|persisted| persisted.id),
            Some(candidate.id)
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_terminal_effect_outbox_converges_for_completion_before_acceptance_after_restart() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_020_000;
        let workspace_id = "ws_effect_late_accept";
        let thread_id = "thr_effect_late_accept";
        let turn_id = "turn_effect_late_accept";
        let (store, _, mut terminal_turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let effect_id = format!("{turn_id}:terminal-effect:post-turn");
        store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: format!("{turn_id}:batch:waiting"),
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    runtime_generation: 1,
                    effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                        effect_id: effect_id.clone(),
                        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                        gate: pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                            request: serde_json::json!({"durable": true}),
                            runtime_snapshot: serde_json::json!({
                                "schema_version": 1,
                                "subscriptions": [],
                                "handlers": []
                            }),
                        },
                        max_attempts: 5,
                    }],
                },
                timestamp,
            )
            .await
            .expect("candidate-gated effect should prepare before acceptance");
        terminal_turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: terminal_turn,
                },
                timestamp + 1,
            )
            .await
            .expect("terminal commit should preserve the waiting gate");
        assert_eq!(
            store
                .native_terminal_effect_status(effect_id.as_str())
                .await
                .expect("effect status should load")
                .expect("effect should exist")
                .status,
            "waiting_acceptance"
        );

        // Restart before the durable candidate exists. Candidate projection
        // must wake the already-terminal obligation without an in-memory
        // rendezvous or a second completion event.
        let restarted = CrudStore::new(store.database_connection());
        let mut task = sample_task(timestamp + 2);
        task.id = "task_effect_late_accept".to_owned();
        task.workspace_id = workspace_id.to_owned();
        task.created_by_thread_id = Some("parent_thread_late".to_owned());
        task.created_by_turn_id = Some("parent_turn_late".to_owned());
        let mut run = sample_task_run(timestamp + 2);
        run.id = "run_effect_late_accept".to_owned();
        run.task_id = task.id.clone();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();
        restarted
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::RunCreated {
                        run: run.clone(),
                        agent_spec: None,
                    },
                ],
                timestamp + 2,
            )
            .await
            .expect("late-acceptance task aggregate should persist");
        let task_run_turn = TaskRunTurn {
            id: "task_run_turn_late_accept".to_owned(),
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            execution_id: None,
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: timestamp + 2,
            started_at: Some(timestamp + 2),
            completed_at: Some(timestamp + 2),
        };
        restarted
            .upsert_task_run_turn(task_run_turn.clone())
            .await
            .expect("late-acceptance task-run Turn should persist");
        let candidate_id = "candidate_effect_late_accept";
        let extraction_failed = TaskResultCandidate {
            id: candidate_id.to_owned(),
            task_id: task.id,
            run_id: run.id,
            task_run_turn_id: task_run_turn.id,
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            round: 0,
            // Extraction failure is recoverable and must not discard the
            // acceptance-gated terminal effect before a durable review can
            // accept the candidate.
            status: TaskResultCandidateStatus::ExtractionFailed,
            result: Some(TaskResult {
                summary: Some("accepted after restart".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some("run_effect_late_accept".to_owned()),
            }),
            extraction_error: None,
            summary: Some("accepted after restart".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: None,
            created_at: timestamp + 3,
            updated_at: timestamp + 3,
            resolved_at: None,
        };
        restarted
            .upsert_task_result_candidate(extraction_failed)
            .await
            .expect("recoverable extraction failure should persist after restart");
        assert_eq!(
            restarted
                .native_terminal_effect_status(effect_id.as_str())
                .await
                .expect("effect should remain readable after extraction failure")
                .expect("effect should remain present after extraction failure")
                .status,
            "waiting_acceptance",
            "a recoverable extraction failure must not discard acceptance-gated work"
        );
        restarted
            .database_connection()
            .execute_unprepared(
                "CREATE FUNCTION reject_candidate_gate_release() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected gate release failure'; END $$; CREATE TRIGGER reject_candidate_gate_release BEFORE UPDATE ON native_terminal_effect_outbox FOR EACH ROW WHEN (OLD.effect_id = 'turn_effect_late_accept:terminal-effect:post-turn' \
                      AND NEW.status = 'ready') EXECUTE FUNCTION reject_candidate_gate_release()",
            )
            .await
            .expect("candidate gate fault trigger should install");
        assert!(
            restarted
                .update_task_result_candidate_resolution(
                    candidate_id,
                    TaskResultCandidateStatus::Accepted,
                    Some("review_effect_late_accept"),
                    Some(timestamp + 4),
                    timestamp + 4,
                )
                .await
                .is_err(),
            "an outbox write fault must roll back candidate acceptance"
        );
        assert_eq!(
            restarted
                .get_task_result_candidate(candidate_id)
                .await
                .expect("candidate should remain readable")
                .expect("candidate should remain present")
                .status,
            TaskResultCandidateStatus::ExtractionFailed
        );
        assert_eq!(
            restarted
                .native_terminal_effect_status(effect_id.as_str())
                .await
                .expect("effect should remain readable after rollback")
                .expect("effect should remain present")
                .status,
            "waiting_acceptance"
        );
        restarted
            .database_connection()
            .execute_unprepared("DROP TRIGGER reject_candidate_gate_release ON native_terminal_effect_outbox")
            .await
            .expect("candidate gate fault trigger should uninstall");
        restarted
            .update_task_result_candidate_resolution(
                candidate_id,
                TaskResultCandidateStatus::Accepted,
                Some("review_effect_late_accept"),
                Some(timestamp + 5),
                timestamp + 5,
            )
            .await
            .expect("late durable acceptance should release the gate")
            .expect("accepted candidate should remain present");
        restarted
            .update_task_result_candidate_resolution(
                candidate_id,
                TaskResultCandidateStatus::Accepted,
                Some("review_effect_late_accept"),
                Some(timestamp + 5),
                timestamp + 5,
            )
            .await
            .expect("duplicate durable acceptance should be idempotent");
        let status = restarted
            .native_terminal_effect_status(effect_id.as_str())
            .await
            .expect("released effect status should load")
            .expect("released effect should exist");
        assert_eq!(status.status, "ready");
        assert_eq!(status.accepted_candidate_id.as_deref(), Some(candidate_id));
        assert_eq!(status.attempt_count, 0);
        assert!(
            restarted
                .update_task_result_candidate_resolution(
                    candidate_id,
                    TaskResultCandidateStatus::Rejected,
                    Some("review_conflicting_rejection"),
                    Some(timestamp + 6),
                    timestamp + 6,
                )
                .await
                .is_err(),
            "accepted candidate state and its released side effect must be irreversible"
        );
        let accepted = restarted
            .get_task_result_candidate(candidate_id)
            .await
            .expect("accepted candidate should remain readable")
            .expect("accepted candidate should remain present");
        assert_eq!(accepted.status, TaskResultCandidateStatus::Accepted);
        Ok(())
    })
    .await
    .unwrap();
}
