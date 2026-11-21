use super::*;
use pioneer_crud_execution::recovery_terminalization_outbox;
use pioneer_crud_execution::turn_item_terminal::tool_call_status;

#[tokio::test]
async fn recovery_diagnostic_rolls_back_with_outbox_and_survives_restart() {
    for status in [RecoveryJobStatus::Exhausted, RecoveryJobStatus::Blocked] {
        recovery_diagnostic_atomic_restart_impl(status).await;
    }
}

#[tokio::test]
async fn terminal_recovery_job_outbox_atomically_closes_item_and_turn_after_restart() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_recovery_terminal_outbox",
            "thr_recovery_terminal_outbox",
            "turn_recovery_outbox",
        )
        .await;
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: "ws_recovery_terminal_outbox".to_owned(),
                    thread_id: "thr_recovery_terminal_outbox".to_owned(),
                    turn_id: "turn_recovery_outbox".to_owned(),
                    item: safe_web_fetch_item("tool_recovery_outbox"),
                },
                1_700_000_001,
            )
            .await
            .expect("tool item should start");
        let job = store
            .enqueue_recovery_job(
                "turn_recovery_outbox".to_owned(),
                "tool_recovery_outbox".to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::RetryWithBackoff,
                Some("tool timed out".to_owned()),
                None,
                None,
                None,
                1,
                2,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_002,
            )
            .await
            .expect("recovery job should enqueue");
        assert!(
            store
                .mark_recovery_job_terminal(
                    job.id.as_str(),
                    RecoveryJobStatus::Exhausted,
                    Some("recovery budget exhausted".to_owned()),
                    1_700_000_003,
                )
                .await
                .expect("terminal job and outbox should commit")
        );

        let restarted = CrudStore::new(store.database_connection());
        let claimed = restarted
            .claim_due_recovery_terminalizations(1_700_000_004, 45, 10)
            .await
            .expect("terminalization should survive restart");
        assert_eq!(claimed.len(), 1);
        let outcome = restarted
            .apply_claimed_recovery_terminalization(
                &claimed[0],
                None,
                RecoveryTerminalCleanupPlan {
                    runtime_generation: 77,
                    runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
                },
                1_700_000_004,
            )
            .await
            .expect("item and Turn should commit atomically");
        let RecoveryTerminalizationApplyOutcome::Applied(applied) = outcome else {
            panic!("running Turn terminalization must not be superseded");
        };
        assert_eq!(
            applied.newly_terminal_turn.as_ref().map(|turn| turn.status),
            Some(TurnStatus::Failed)
        );
        assert!(applied.final_item.is_some());
        let cleanup = restarted
            .claim_due_native_terminal_effects_at(1_700_000_004, 90, 10)
            .await
            .expect("recovery terminal cleanup should activate atomically")
            .records;
        assert_eq!(cleanup.len(), 1);
        assert_eq!(cleanup[0].runtime_generation, 77);
        assert!(matches!(
            cleanup[0].payload,
            pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup { .. }
        ));
        let (_, terminal) = restarted
            .get_turn("thr_recovery_terminal_outbox", "turn_recovery_outbox")
            .await
            .expect("turn lookup should succeed")
            .expect("turn should exist");
        assert_eq!(terminal.status, TurnStatus::Failed);
        assert_eq!(terminal.error.as_deref(), Some("Recovery failed."));
        assert!(
            restarted
                .get_failed_turn_recovery_diagnostic("turn_recovery_outbox")
                .await
                .unwrap()
                .is_none()
        );

        let item = pioneer_crud_conversations::turn_reads::find_turn_item(
            &restarted.connection,
            "turn_recovery_outbox",
            "tool_recovery_outbox",
        )
        .await
        .expect("tool item lookup should succeed")
        .expect("tool item should exist");
        let item: TurnItem = read_fixture_item(&restarted.connection, item)
            .await
            .expect("terminal item payload should decode");
        assert_eq!(tool_call_status(&item), Some(ToolCallStatus::Failed));
        assert!(
            restarted
                .claim_due_recovery_terminalizations(1_700_000_100, 45, 10)
                .await
                .expect("delivered outbox lookup should succeed")
                .is_empty(),
            "delivered terminalization must not execute twice"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocked_recovery_outbox_preserves_the_durable_resume_reason_after_event_loss() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_recovery_blocked_outbox",
            "thr_recovery_blocked_outbox",
            "turn_recovery_blocked_outbox",
        )
        .await;
        let root_reason = "provider credentials must be refreshed";
        let fallback_reason =
            "cannot restore recovery because durable turn runtime snapshot is missing";
        let job = store
            .enqueue_recovery_job(
                "turn_recovery_blocked_outbox".to_owned(),
                "reasoning_recovery_blocked_outbox".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::RestartTurn,
                Some(root_reason.to_owned()),
                None,
                None,
                None,
                0,
                2,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_001,
            )
            .await
            .expect("recovery job should enqueue");
        assert!(
            store
                .mark_recovery_job_terminal(
                    job.id.as_str(),
                    RecoveryJobStatus::Blocked,
                    Some(fallback_reason.to_owned()),
                    1_700_000_002,
                )
                .await
                .expect("blocked job and outbox should commit")
        );

        let claimed = store
            .claim_due_recovery_terminalizations(1_700_000_003, 45, 10)
            .await
            .expect("blocked terminalization should claim");
        assert_eq!(claimed.len(), 1);
        let resume = TurnBlockedResumeMetadata {
            reason_class: "auth_or_config".to_owned(),
            human_message: root_reason.to_owned(),
            resume_requirements: vec!["Refresh provider credentials.".to_owned()],
            resume_command: "turn.resume:turn_recovery_blocked_outbox".to_owned(),
            blocked_recovery_job_id: Some(job.id.clone()),
            latest_checkpoint_id: None,
            can_resume_same_turn: true,
        };
        store
            .apply_claimed_recovery_terminalization(
                &claimed[0],
                Some(resume),
                RecoveryTerminalCleanupPlan {
                    runtime_generation: 1,
                    runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
                },
                1_700_000_003,
            )
            .await
            .expect("outbox-only blocked terminalization should commit");

        let (_, blocked) = store
            .get_turn(
                "thr_recovery_blocked_outbox",
                "turn_recovery_blocked_outbox",
            )
            .await
            .expect("blocked turn lookup should succeed")
            .expect("blocked turn should exist");
        assert_eq!(blocked.status, TurnStatus::Blocked);
        let error = blocked.error.as_deref().unwrap_or_default();
        assert!(error.contains(root_reason), "root reason was lost: {error}");
        assert!(
            error.contains(job.id.as_str()),
            "recovery fence was lost: {error}"
        );
        assert!(
            !error.contains(fallback_reason),
            "secondary recovery failure replaced the root reason: {error}"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn recovery_terminalization_rolls_back_both_item_and_turn_then_reclaims_expired_lease() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(fixture,
            "ws_recovery_terminal_fault",
            "thr_recovery_terminal_fault",
            "turn_recovery_fault",
        )
        .await;
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: "ws_recovery_terminal_fault".to_owned(),
                    thread_id: "thr_recovery_terminal_fault".to_owned(),
                    turn_id: "turn_recovery_fault".to_owned(),
                    item: safe_web_fetch_item("tool_recovery_fault"),
                },
                1_700_000_001,
            )
            .await
            .expect("tool item should start");
        let job = store
            .enqueue_recovery_job(
                "turn_recovery_fault".to_owned(),
                "tool_recovery_fault".to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::MarkFailed,
                Some("terminal recovery fault".to_owned()),
                None,
                None,
                None,
                1,
                1,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_002,
            )
            .await
            .expect("recovery job should enqueue");
        assert!(
            store
                .mark_recovery_job_terminal(
                    job.id.as_str(),
                    RecoveryJobStatus::Failed,
                    Some("terminal recovery fault".to_owned()),
                    1_700_000_003,
                )
                .await
                .expect("terminal job should commit with outbox")
        );
        let first_claim = store
            .claim_due_recovery_terminalizations(1_700_000_004, 5, 10)
            .await
            .expect("terminalization should claim");
        assert_eq!(first_claim.len(), 1);
        store
            .connection
            .execute_unprepared(
                "CREATE FUNCTION reject_recovery_turn_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected recovery terminal crash'; END $$; CREATE TRIGGER reject_recovery_turn_failure BEFORE UPDATE OF status ON turn FOR EACH ROW WHEN (NEW.id = 'turn_recovery_fault' AND NEW.status = 'failed') EXECUTE FUNCTION reject_recovery_turn_failure()",
            )
            .await
            .expect("fault trigger should install");
        assert!(
            store
                .apply_claimed_recovery_terminalization(
                    &first_claim[0],
                    None,
                    RecoveryTerminalCleanupPlan {
                        runtime_generation: 1,
                        runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
                    },
                    1_700_000_004,
                )
                .await
                .is_err()
        );
        let (_, running) = store
            .get_turn("thr_recovery_terminal_fault", "turn_recovery_fault")
            .await
            .expect("turn lookup should succeed")
            .expect("turn should exist");
        assert_eq!(running.status, TurnStatus::InProgress);
        let running_item = pioneer_crud_conversations::turn_reads::find_turn_item(
            &store.connection,
            "turn_recovery_fault",
            "tool_recovery_fault",
        )
        .await
        .expect("tool item lookup should succeed")
        .expect("tool item should exist");
        let running_item: TurnItem = read_fixture_item(&store.connection, running_item)
            .await
            .expect("running item should decode");
        assert_eq!(
            tool_call_status(&running_item),
            Some(ToolCallStatus::InProgress),
            "item completion must roll back with Turn terminalization"
        );

        store
            .connection
            .execute_unprepared("DROP TRIGGER reject_recovery_turn_failure ON turn")
            .await
            .expect("fault trigger should uninstall");
        let restarted = CrudStore::new(store.database_connection());
        let reclaimed = restarted
            .claim_due_recovery_terminalizations(1_700_000_010, 5, 10)
            .await
            .expect("expired claim should be recovered after restart");
        assert_eq!(reclaimed.len(), 1);
        assert_ne!(reclaimed[0].claim_token, first_claim[0].claim_token);
        use futures_util::FutureExt;
        let database = restarted.database_connection();
        let mut consumers = [
            "projection",
            "compaction",
            "maintenance.zstd",
            "maintenance.receipts",
            "maintenance.native",
        ]
        .map(|domain| database.work_signal(domain).subscribe());
        for consumer in &mut consumers {
            consumer.begin_pass(&database).await.unwrap();
        }
        restarted
            .apply_claimed_recovery_terminalization(
                &reclaimed[0],
                None,
                RecoveryTerminalCleanupPlan {
                    runtime_generation: 1,
                    runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
                },
                1_700_000_010,
            )
            .await
            .expect("reclaimed terminalization should converge");
        for consumer in &mut consumers {
            assert!(
                consumer.changed().now_or_never().is_some(),
                "atomic recovery publisher wakes dependent owners after commit"
            );
        }
        assert_eq!(
            restarted
                .get_turn("thr_recovery_terminal_fault", "turn_recovery_fault")
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Failed
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn recovery_terminalization_cancels_claim_superseded_by_authoritative_terminal_status() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_recovery_terminal_superseded";
        let thread_id = "thr_recovery_terminal_superseded";
        let turn_id = "turn_recovery_superseded";
        let (store, _, mut turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn,
                },
                1_700_000_001,
            )
            .await
            .expect("authoritative completion should persist");
        let job = store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_recovery_superseded".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::MarkFailed,
                Some("obsolete recovery failure".to_owned()),
                None,
                None,
                None,
                1,
                1,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_002,
            )
            .await
            .expect("recovery job should enqueue");
        assert!(
            store
                .mark_recovery_job_terminal(
                    job.id.as_str(),
                    RecoveryJobStatus::Failed,
                    Some("obsolete recovery failure".to_owned()),
                    1_700_000_003,
                )
                .await
                .expect("terminal job should enqueue its outbox row")
        );
        let claimed = store
            .claim_due_recovery_terminalizations(1_700_000_004, 45, 10)
            .await
            .expect("stale terminalization should claim");
        assert_eq!(claimed.len(), 1);

        let disposition = store
            .classify_claimed_recovery_terminalization(&claimed[0], 1_700_000_004)
            .await
            .expect("newer terminal status should be classified without retry");
        assert_eq!(
            disposition,
            RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(
                TurnStatus::Completed
            )
        );
        let outbox = pioneer_entity::recovery_terminalization_outbox::Entity::find_by_id(job.id)
            .one(&store.connection)
            .await
            .expect("outbox lookup should succeed")
            .expect("outbox audit row should remain");
        assert_eq!(
            outbox.status,
            recovery_terminalization_outbox::STATUS_CANCELLED
        );
        assert!(outbox.claim_token.is_none());
        assert!(
            outbox
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("Completed"))
        );
        assert!(
            store
                .claim_due_recovery_terminalizations(1_800_000_000, 45, 10)
                .await
                .expect("cancelled terminalization lookup should succeed")
                .is_empty(),
            "superseded terminalization must never be retried"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn already_terminal_recovery_consumes_legacy_row_without_rewriting_committed_cleanup() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_recovery_terminal_legacy";
        let thread_id = "thr_recovery_terminal_legacy";
        let turn_id = "turn_recovery_legacy";
        let (store, _, mut turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let original_preparation =
            cleanup_effect_preparation(workspace_id, thread_id, turn_id, "original");
        let effect_id = original_preparation.effects[0].effect_id.clone();
        store
            .prepare_native_terminal_effects(original_preparation, 1_700_000_001)
            .await
            .expect("canonical cleanup should prepare");
        turn.status = TurnStatus::Failed;
        turn.error = Some("canonical failure".to_owned());
        store
            .materialize_turn_failed(
                TurnFailedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn,
                },
                1_700_000_002,
            )
            .await
            .expect("canonical failure should activate cleanup");
        let original_effect =
            pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(effect_id.clone())
                .one(&store.connection)
                .await
                .expect("cleanup lookup should succeed")
                .expect("canonical cleanup should exist");
        assert!(original_effect.terminal_committed_at.is_some());

        pioneer_entity::turn::Entity::update_many()
            .col_expr(
                pioneer_entity::turn::Column::PromptManifestJson,
                Expr::value(r#"{"profile":"retired_prompt_variant"}"#.to_owned()),
            )
            .filter(pioneer_entity::turn::Column::Id.eq(turn_id.to_owned()))
            .exec(&store.connection)
            .await
            .expect("legacy prompt fixture should persist");
        let job = store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_recovery_legacy".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::MarkFailed,
                Some("later reconstructed failure".to_owned()),
                None,
                None,
                None,
                1,
                1,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_003,
            )
            .await
            .expect("legacy recovery job should enqueue");
        assert!(
            store
                .mark_recovery_job_terminal(
                    job.id.as_str(),
                    RecoveryJobStatus::Failed,
                    Some("later reconstructed failure".to_owned()),
                    1_700_000_004,
                )
                .await
                .expect("legacy recovery terminalization should enqueue")
        );
        let claimed = store
            .claim_due_recovery_terminalizations(1_700_000_005, 45, 10)
            .await
            .expect("legacy terminalization should claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(
            store
                .classify_claimed_recovery_terminalization(&claimed[0], 1_700_000_005)
                .await
                .expect("legacy terminalization classification should succeed"),
            RecoveryTerminalizationClaimDisposition::AlreadyTerminal
        );

        let outcome = store
            .apply_claimed_recovery_terminalization(
                &claimed[0],
                None,
                RecoveryTerminalCleanupPlan {
                    runtime_generation: 99,
                    runtime_contract: "pioneer.test.new-cleanup-contract.v2".to_owned(),
                },
                1_700_000_005,
            )
            .await
            .expect("already-terminal legacy row should converge");
        let RecoveryTerminalizationApplyOutcome::Applied(applied) = outcome else {
            panic!("matching terminal status must consume rather than supersede recovery");
        };
        assert!(applied.newly_terminal_turn.is_none());

        let delivered = pioneer_entity::recovery_terminalization_outbox::Entity::find_by_id(job.id)
            .one(&store.connection)
            .await
            .expect("terminalization lookup should succeed")
            .expect("terminalization audit row should remain");
        assert_eq!(
            delivered.status,
            recovery_terminalization_outbox::STATUS_DELIVERED
        );
        let retained_effect =
            pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(effect_id)
                .one(&store.connection)
                .await
                .expect("retained cleanup lookup should succeed")
                .expect("retained cleanup should exist");
        assert_eq!(
            retained_effect.payload_identity_sha256, original_effect.payload_identity_sha256,
            "recovery must preserve the canonical committed cleanup payload"
        );
        assert_eq!(retained_effect.payload_json, original_effect.payload_json);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn atomic_recovery_enqueue_reuses_terminal_job_until_outbox_resolution() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_recovery_episode_guard";
        let thread_id = "thr_recovery_episode_guard";
        let turn_id = "turn_recovery_guard";
        let (store, _, mut turn) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;

        let enqueue = |item_id: &str, now_unix| {
            store.enqueue_recovery_job_if_no_unresolved(
                turn_id.to_owned(),
                item_id.to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::RuntimeFailure,
                RecoveryAction::MarkFailed,
                Some("observation gap".to_owned()),
                None,
                None,
                None,
                0,
                1,
                serde_json::json!({}),
                serde_json::json!({}),
                now_unix,
            )
        };

        let AtomicRecoveryJobEnqueueOutcome::Created(first) =
            enqueue("runtime:first", 1_700_000_001)
                .await
                .expect("first recovery episode should enqueue")
        else {
            panic!("first recovery episode must be created");
        };
        let AtomicRecoveryJobEnqueueOutcome::Reused(reused_pending) =
            enqueue("runtime:duplicate", 1_700_000_002)
                .await
                .expect("pending recovery episode should be reused")
        else {
            panic!("pending recovery episode must be reused");
        };
        assert_eq!(reused_pending.id, first.id);

        assert!(
            store
                .mark_recovery_job_terminal(
                    first.id.as_str(),
                    RecoveryJobStatus::Failed,
                    Some("terminal observation gap".to_owned()),
                    1_700_000_003,
                )
                .await
                .expect("recovery job should become terminal")
        );
        let AtomicRecoveryJobEnqueueOutcome::Reused(reused_terminal) =
            enqueue("runtime:after-terminal", 1_700_000_004)
                .await
                .expect("unresolved terminalization should retain episode ownership")
        else {
            panic!("terminal recovery with unresolved outbox must be reused");
        };
        assert_eq!(reused_terminal.id, first.id);

        turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn,
                },
                1_700_000_005,
            )
            .await
            .expect("authoritative completion should persist");
        let claimed = store
            .claim_due_recovery_terminalizations(1_700_000_006, 45, 10)
            .await
            .expect("terminalization should claim");
        assert_eq!(claimed.len(), 1);
        assert!(matches!(
            store
                .classify_claimed_recovery_terminalization(&claimed[0], 1_700_000_006)
                .await
                .expect("terminalization should be superseded"),
            RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(
                TurnStatus::Completed
            )
        ));

        let AtomicRecoveryJobEnqueueOutcome::Created(next) =
            enqueue("runtime:next-episode", 1_700_000_007)
                .await
                .expect("resolved episode should release the database guard")
        else {
            panic!("a new recovery may start only after durable resolution");
        };
        assert_ne!(next.id, first.id);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn superseded_terminalization_batch_has_constant_query_count() {
    pioneer_postgres_test::with_database(async |fixture| {
        const BATCH_SIZE: usize = 64;
        let workspace_id = "ws_recovery_batch_queries";
        let (store, query_count) =
            postgres_store_with_workspace_and_query_counter(fixture, workspace_id).await;

        for index in 0..BATCH_SIZE {
            let timestamp = 1_700_000_000 + i64::try_from(index).unwrap_or_default();
            let thread_id = format!("thr_recovery_batch_{index}");
            let turn_id = format!("turn_recovery_batch_{index}");
            let thread = Thread {
                context_window: None,
                service_mode: None,
                context_authority: None,
                workspace_id: workspace_id.to_owned(),
                id: thread_id.clone(),
                name: None,
                preview: String::new(),
                preview_author: None,
                mode: ThreadMode::Agent,
                model: "gpt-5.4".to_owned(),
                model_provider: "openai".to_owned(),
                reasoning_effort: None,
                created_at: timestamp,
                updated_at: timestamp,
                status: ThreadStatus::Active,
                origin_kind: ThreadOriginKind::User,
                sidebar_visibility: ThreadSidebarVisibility::Visible,
                agent_nickname: None,
                agent_role: None,
                visibility: None,
                turns: Vec::new(),
            };
            let mut turn = Turn {
                work_owner: Default::default(),
                id: turn_id.clone(),
                status: TurnStatus::InProgress,
                turn_kind: Default::default(),
                origin: Default::default(),
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            };
            store
                .upsert_thread_model(&thread, pioneer_protocol::PersistedActorRef::System)
                .await
                .expect("batch thread should persist");
            store
                .materialize_turn_start(
                    &thread,
                    SandboxMode::FullAccess,
                    &turn,
                    &[],
                    pioneer_protocol::PersistedActorRef::System,
                )
                .await
                .expect("batch Turn should persist");
            turn.status = TurnStatus::Completed;
            store
                .materialize_turn_completed(
                    TurnCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id,
                        turn,
                    },
                    timestamp + 1,
                )
                .await
                .expect("authoritative terminal Turn should persist");
            let job = store
                .enqueue_recovery_job(
                    turn_id,
                    format!("runtime:batch:{index}"),
                    TurnItemType::Reasoning,
                    None,
                    RecoveryTrigger::RuntimeFailure,
                    RecoveryAction::MarkFailed,
                    Some("obsolete recovery".to_owned()),
                    None,
                    None,
                    None,
                    0,
                    1,
                    serde_json::json!({}),
                    serde_json::json!({}),
                    timestamp + 2,
                )
                .await
                .expect("batch recovery should enqueue");
            assert!(
                store
                    .mark_recovery_job_terminal(
                        job.id.as_str(),
                        RecoveryJobStatus::Failed,
                        Some("obsolete recovery".to_owned()),
                        timestamp + 3,
                    )
                    .await
                    .expect("batch recovery should become terminal")
            );
        }

        query_count.store(0, Ordering::Relaxed);
        let claimed = store
            .claim_due_recovery_terminalizations(1_700_001_000, 45, BATCH_SIZE as u64)
            .await
            .expect("full terminalization batch should claim");
        assert_eq!(claimed.len(), BATCH_SIZE);
        let classified = store
            .classify_claimed_recovery_terminalization_batch(&claimed, 1_700_001_000)
            .await
            .expect("full terminalization batch should classify");
        assert_eq!(classified.len(), BATCH_SIZE);
        assert!(classified.iter().all(|(_, disposition)| matches!(
            disposition,
            RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(
                TurnStatus::Completed
            )
        )));
        let measured = query_count.load(Ordering::Relaxed);
        assert!(
            measured <= 8,
            "claiming and classifying {BATCH_SIZE} stale rows issued {measured} SQL statements"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn permanently_invalid_terminalization_payload_is_quarantined_instead_of_retried() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_recovery_apply_poison";
        let thread_id = "thr_recovery_apply_poison";
        let turn_id = "turn_recovery_apply_poison";
        let (store, _, _) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let item_id = "tool_recovery_apply_poison";
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: safe_web_fetch_item(item_id),
                },
                1_700_000_001,
            )
            .await
            .expect("tool item should start");
        let job = store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                item_id.to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::RuntimeFailure,
                RecoveryAction::MarkFailed,
                Some("terminal recovery".to_owned()),
                None,
                None,
                None,
                0,
                1,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_002,
            )
            .await
            .expect("recovery job should enqueue");
        assert!(
            store
                .mark_recovery_job_terminal(
                    job.id.as_str(),
                    RecoveryJobStatus::Failed,
                    Some("terminal recovery".to_owned()),
                    1_700_000_003,
                )
                .await
                .expect("recovery job should become terminal")
        );
        pioneer_entity::turn_item::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_item::Column::Payload,
                Expr::value(serde_json::json!({"retired_invalid_payload":true})),
            )
            .col_expr(
                pioneer_entity::turn_item::Column::PayloadPayloadId,
                Expr::value(Option::<String>::None),
            )
            .filter(pioneer_entity::turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(pioneer_entity::turn_item::Column::ItemId.eq(item_id.to_owned()))
            .exec(&store.connection)
            .await
            .expect("poison payload fixture should persist");

        let claimed = store
            .claim_due_recovery_terminalizations(1_700_000_004, 45, 10)
            .await
            .expect("terminalization should claim");
        use futures_util::FutureExt;
        let database = store.database_connection();
        let mut native = database.work_signal("maintenance.native").subscribe();
        native.begin_pass(&database).await.unwrap();
        let classified = store
            .classify_claimed_recovery_terminalization_batch(&claimed, 1_700_000_004)
            .await
            .expect("terminalization should pass early classification");
        assert_eq!(classified.len(), 1);
        assert_eq!(
            classified[0].1,
            RecoveryTerminalizationClaimDisposition::NeedsCommit
        );
        assert!(
            native.changed().now_or_never().is_none(),
            "read-only classification does not release a dependency"
        );
        let outcome = store
            .apply_claimed_recovery_terminalization(
                &classified[0].0,
                None,
                RecoveryTerminalCleanupPlan {
                    runtime_generation: 1,
                    runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
                },
                1_700_000_004,
            )
            .await
            .expect("permanent payload error should converge through quarantine");
        assert_eq!(
            outcome,
            RecoveryTerminalizationApplyOutcome::Quarantined {
                reason_code: "invalid_turn_item_payload".to_owned(),
            }
        );
        assert!(
            store
                .claim_due_recovery_terminalizations(1_800_000_000, 45, 10)
                .await
                .expect("quarantined lookup should succeed")
                .is_empty(),
            "permanent payload poison must never re-enter the retry loop"
        );
        let retained = store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("recovery audit lookup should succeed")
            .expect("recovery audit should remain");
        assert!(!retained.resolution_pending);
        assert!(
            native.changed().now_or_never().is_some(),
            "quarantine releases the recovery dependency even without publishing an event"
        );
        let (_, running) = store
            .get_turn(thread_id, turn_id)
            .await
            .expect("Turn lookup should succeed")
            .expect("Turn should remain");
        assert_eq!(running.status, TurnStatus::InProgress);
        Ok(())
    })
    .await
    .unwrap();
}

async fn recovery_diagnostic_atomic_restart_impl(status: RecoveryJobStatus) {
    pioneer_postgres_test::with_database(async |fixture| {
        use pioneer_protocol::{
            ProviderFailureClass, ProviderFailureStage, ProviderTransportKind, RecoveryDiagnostic,
            RecoveryProviderFailure, RecoveryStopReason,
        };
        let (store, _, _) =
            postgres_store_with_started_turn(fixture, "ws_diag_atomic", "thr_diag_atomic", "turn_diag_atomic")
                .await;
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: "ws_diag_atomic".to_owned(),
                    thread_id: "thr_diag_atomic".to_owned(),
                    turn_id: "turn_diag_atomic".to_owned(),
                    item: safe_web_fetch_item("tool_diag_atomic"),
                },
                1_700_000_001,
            )
            .await
            .unwrap();
        let initial = RecoveryDiagnostic {
            last_failure: Some(RecoveryProviderFailure {
                raw_response: None,
                error_reason: None,
                request_id: None,
                class: ProviderFailureClass::StreamTruncated,
                stage: ProviderFailureStage::MidStream,
                transport: ProviderTransportKind::Stream,
                http_status: None,
                retry_after_ms: None,
            }),
            stop_reason: None,
        };
        let job = match store
            .enqueue_recovery_job_with_diagnostic_if_no_unresolved(
                "turn_diag_atomic".to_owned(),
                "tool_diag_atomic".to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::RetryWithBackoff,
                Some("raw initial secret".to_owned()),
                Some(ProviderFailureClass::StreamTruncated),
                Some(ProviderFailureStage::MidStream),
                None,
                0,
                2,
                serde_json::json!({}),
                serde_json::json!({}),
                Some(initial.clone()),
                1_700_000_002,
            )
            .await
            .unwrap()
        {
            AtomicRecoveryJobEnqueueOutcome::Created(job) => job,
            _ => panic!("new job expected"),
        };
        let claimed = store
            .claim_due_recovery_jobs(1_700_000_003, 45, 1)
            .await
            .unwrap();
        store
            .mark_claimed_recovery_job_active(
                &job.id,
                claimed[0].claim_token.as_deref().unwrap(),
                "diag_attempt",
                1_700_000_003,
            )
            .await
            .unwrap();
        let mut response = pioneer_protocol::ProviderErrorResponse::from_bytes(
            format!(
                "{} original provider diagnostic body",
                "x".repeat(20 * 1024)
            )
            .as_bytes(),
            false,
        );
        response.processing_error = Some("Responses completed reasoning state conflict".into());
        response.stream_context = Some("data: original completed reasoning event\n\n".into());
        let final_diagnostic = RecoveryDiagnostic {
            last_failure: Some(RecoveryProviderFailure {
                raw_response: Some(response),
                error_reason: Some(pioneer_protocol::ProviderErrorReason::PermissionDenied),
                request_id: Some(
                    pioneer_protocol::ProviderRequestId::try_from("gen-atomic_fixture".to_owned())
                        .unwrap(),
                ),
                class: ProviderFailureClass::AuthOrPermission,
                stage: ProviderFailureStage::Connect,
                transport: ProviderTransportKind::NonStream,
                http_status: Some(403),
                retry_after_ms: None,
            }),
            stop_reason: Some(if status == RecoveryJobStatus::Blocked {
                RecoveryStopReason::NoProgress
            } else {
                RecoveryStopReason::AttemptsExhausted
            }),
        };
        let raw = "raw body credential=secret https://secret.example /private/key HTTP 401";
        assert!(
            !store
                .mark_recovery_job_terminal_after_attempt_with_diagnostic(
                    &job.id,
                    "stale_attempt",
                    status,
                    Some(raw.to_owned()),
                    Some(final_diagnostic.clone()),
                    1_700_000_004,
                    None,
                )
                .await
                .unwrap()
        );
        store.connection.execute_unprepared("CREATE FUNCTION reject_diag_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test outbox failure'; END $$; CREATE TRIGGER reject_diag_outbox BEFORE INSERT ON recovery_terminalization_outbox FOR EACH ROW EXECUTE FUNCTION reject_diag_outbox()").await.unwrap();
        assert!(
            store
                .mark_recovery_job_terminal_after_attempt_with_diagnostic(
                    &job.id,
                    "diag_attempt",
                    status,
                    Some(raw.to_owned()),
                    Some(final_diagnostic.clone()),
                    1_700_000_004,
                    None,
                )
                .await
                .is_err()
        );
        let rolled_back = store.get_recovery_job(&job.id).await.unwrap().unwrap();
        assert_eq!(rolled_back.status, RecoveryJobStatus::Active);
        assert_eq!(rolled_back.run_count, 0);
        assert_eq!(rolled_back.diagnostic, Some(initial));
        assert!(rolled_back.last_failure_attempt_id.is_none());
        store
            .connection
            .execute_unprepared("DROP TRIGGER reject_diag_outbox ON recovery_terminalization_outbox")
            .await
            .unwrap();
        assert!(
            store
                .mark_recovery_job_terminal_after_attempt_with_diagnostic(
                    &job.id,
                    "diag_attempt",
                    status,
                    Some(raw.to_owned()),
                    Some(final_diagnostic.clone()),
                    1_700_000_004,
                    None,
                )
                .await
                .unwrap()
        );
        let restarted = CrudStore::new(store.database_connection());
        let terminal = restarted.get_recovery_job(&job.id).await.unwrap().unwrap();
        assert_eq!(terminal.diagnostic.as_ref(), Some(&final_diagnostic));
        assert_eq!(
            terminal.error_class,
            Some(ProviderFailureClass::StreamTruncated)
        );
        let claims = restarted
            .claim_due_recovery_terminalizations(1_700_000_005, 45, 10)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        let applied = restarted
            .apply_claimed_recovery_terminalization(
                &claims[0],
                (status == RecoveryJobStatus::Blocked).then(|| TurnBlockedResumeMetadata {
                    reason_class: "execution_budget".to_owned(),
                    human_message: "max_consecutive_no_progress_windows reached: limit=3"
                        .to_owned(),
                    resume_requirements: Vec::new(),
                    resume_command: "turn.resume:turn_diag_atomic".to_owned(),
                    blocked_recovery_job_id: Some(job.id.clone()),
                    latest_checkpoint_id: None,
                    can_resume_same_turn: false,
                }),
                RecoveryTerminalCleanupPlan {
                    runtime_generation: 77,
                    runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
                },
                1_700_000_005,
            )
            .await
            .unwrap();
        let RecoveryTerminalizationApplyOutcome::Applied(applied) = applied else {
            panic!("terminalization expected");
        };
        let expected = final_diagnostic.public_message();
        let expected_turn_message = if status == RecoveryJobStatus::Blocked {
            format!("{expected} (recovery job {})", job.id)
        } else {
            expected.clone()
        };
        let terminal_turn = applied.newly_terminal_turn.unwrap();
        assert_eq!(
            terminal_turn.error.as_deref(),
            Some(expected_turn_message.as_str())
        );
        let serialized = serde_json::to_string(&terminal_turn).unwrap();
        assert!(serialized.contains("HTTP 403"));
        for secret in [
            "credential",
            "secret.example",
            "/private",
            "HTTP 401",
            "gen-atomic_fixture",
        ] {
            assert!(!serialized.contains(secret));
        }
        if status == RecoveryJobStatus::Blocked {
            assert!(applied.final_item.is_none());
            assert_eq!(
                restarted
                    .get_blocked_turn_recovery_diagnostic("turn_diag_atomic")
                    .await
                    .unwrap(),
                Some(final_diagnostic.clone())
            );
            assert!(
                restarted
                    .get_failed_turn_recovery_diagnostic("turn_diag_atomic")
                    .await
                    .unwrap()
                    .is_none()
            );
            // A stale/resumed/pending job cannot lend its facts to this result.
            for job_status in ["pending", "active", "succeeded", "cancelled"] {
                restarted
                    .connection
                    .execute_unprepared(&format!(
                        "UPDATE recovery_job SET status = '{job_status}' WHERE id = '{}'",
                        job.id
                    ))
                    .await
                    .unwrap();
                assert!(
                    restarted
                        .get_blocked_turn_recovery_diagnostic("turn_diag_atomic")
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            restarted
                .connection
                .execute_unprepared(&format!(
                    "UPDATE recovery_job SET status = 'blocked' WHERE id = '{}'",
                    job.id
                ))
                .await
                .unwrap();
            restarted
                .connection
                .execute_unprepared(
                    "UPDATE turn SET error = 'authorization denied' WHERE id = 'turn_diag_atomic'",
                )
                .await
                .unwrap();
            assert!(
                restarted
                    .get_blocked_turn_recovery_diagnostic("turn_diag_atomic")
                    .await
                    .unwrap()
                    .is_none()
            );
            restarted
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "UPDATE turn SET error = $1 WHERE id = 'turn_diag_atomic'",
                    [expected_turn_message.into()],
                ))
                .await
                .unwrap();
            let current = turn_event::latest_event_for_turn(&restarted.connection, "turn_diag_atomic").await?
                .expect("blocked turn must retain its terminal event");
            assert!(matches!(&current.payload, TurnEventPayload::TurnBlocked(_)));
            let mut changed = serde_json::to_value(&current.payload)?;
            changed["payload"]["resume"]["blocked_recovery_job_id"] = serde_json::json!("unrelated_job");
            let body = pioneer_crud_payloads::storage::PreparedPayload::new(
                &serde_json::to_string(&changed)?,
            )?;
            let write = restarted.connection.begin().await?;
            body.insert(&write).await?;
            assert_eq!(write.execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "UPDATE turn_event SET payload=NULL,payload_payload_id=$2 WHERE id=$1",
                [current.id.into(), body.id().into()],
            )).await?.rows_affected(), 1);
            write.commit().await?;
            assert!(
                restarted
                    .get_blocked_turn_recovery_diagnostic("turn_diag_atomic")
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            assert_eq!(
                restarted
                    .get_failed_turn_recovery_diagnostic("turn_diag_atomic")
                    .await
                    .unwrap(),
                Some(final_diagnostic)
            );
            assert!(
                restarted
                    .get_blocked_turn_recovery_diagnostic("turn_diag_atomic")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                serde_json::to_string(&applied.final_item.unwrap())
                    .unwrap()
                    .contains("HTTP 403")
            );
        }
        assert!(
            restarted
                .claim_due_recovery_terminalizations(1_700_000_100, 45, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            !restarted
                .mark_recovery_job_terminal_after_attempt_with_diagnostic(
                    &job.id,
                    "diag_attempt",
                    status,
                    Some("duplicate".to_owned()),
                    None,
                    1_700_000_100,
                    None,
                )
                .await
                .unwrap()
        );
        Ok(())
    })
    .await
    .unwrap();
}
