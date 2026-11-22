//! Terminal-effect retry, recovery and retention.
use pioneer_crud_execution::native_terminal_effect_outbox as outbox;
use pioneer_crud_execution::terminal_effect_store::TerminalEffectStore;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, Statement, TransactionTrait,
    sea_query::Expr,
};

async fn fixture_database(
    fixture: &pioneer_postgres_test::TestDatabase,
) -> anyhow::Result<PostgresDatabase> {
    let writer = fixture.connection();
    writer.execute_unprepared(
        "INSERT INTO workspace (id, name) VALUES ('ws_post_turn_reopen', 'Fixture');
         INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
         VALUES ('thr_post_turn_reopen', 'ws_post_turn_reopen', '', 'agent', 'model', 'provider', 'active');
         INSERT INTO turn (id, thread_id, status)
         VALUES ('turn_post_turn_reopen', 'thr_post_turn_reopen', 'in_progress');",
    ).await?;
    Ok(PostgresDatabase::new(
        fixture.connect_read_only().await?,
        writer.clone(),
    ))
}

async fn activate_post_turn_effect(
    database: &PostgresDatabase,
    timestamp: i64,
) -> anyhow::Result<String> {
    let workspace_id = "ws_post_turn_reopen";
    let thread_id = "thr_post_turn_reopen";
    let turn_id = "turn_post_turn_reopen";
    let store = TerminalEffectStore::new(database);
    let effect_id = format!("{turn_id}:terminal-effect:post-turn");
    store
        .prepare_native_terminal_effects(
            pioneer_protocol::NativeTerminalEffectPreparation {
                batch_id: format!("{turn_id}:batch:post-turn-reopen"),
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                runtime_generation: 1,
                effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                    effect_id: effect_id.clone(),
                    effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                    gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                    payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                        request: serde_json::json!({}),
                        runtime_snapshot: serde_json::json!({}),
                    },
                    max_attempts: 1,
                }],
            },
            timestamp,
        )
        .await
        .expect("post-turn effect should prepare");
    let activation = outbox::prepare_activation_for_terminal(database, turn_id).await?;
    let tx = database.begin().await?;
    tx.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn_post_turn_reopen'")
        .await?;
    outbox::activate_prepared_for_terminal(&tx, activation, unix_to_datetime(timestamp + 1))
        .await
        .expect("terminal commit should activate post-turn effect");
    tx.commit().await?;
    Ok(effect_id)
}

#[tokio::test]
async fn retryable_post_turn_effect_reopens_after_bounded_cooldown() {
    for code in [
        "memory.post_turn_extractor.provider_network_transient",
        "memory.post_turn_extractor.provider_stream_stall",
        "memory.post_turn_extractor.provider_stream_truncated",
        "memory.post_turn_extractor.provider_rate_limited",
        "memory.post_turn_extractor.provider_5xx",
    ] {
        assert_neighbor_post_turn_effect_reopens(code).await;
    }
}

async fn assert_neighbor_post_turn_effect_reopens(code: &str) {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_040_000;
        let database = fixture_database(fixture).await?;
        let store = TerminalEffectStore::new(&database);
        let effect_id = activate_post_turn_effect(&database, timestamp).await?;

        let first = outbox::claim_records(&database, timestamp + 1, 10, 1, &|| timestamp + 1)
            .await
            .expect("first attempt should claim")
            .records
            .pop()
            .expect("first attempt must exist");
        let mut retry_wake = database.work_signal("control.native_retry").subscribe();
        retry_wake.begin_pass(&database).await.unwrap();
        assert_eq!(
            store
                .next_native_terminal_effect_recovery_deadline(timestamp + 1)
                .await
                .unwrap(),
            None,
            "a running effect is not an exhausted retry obligation"
        );
        assert!(
            store
                .fail_native_terminal_effect(
                    effect_id.as_str(),
                    first.claim_token.as_str(),
                    code,
                    "provider request failed (network_transient)",
                    true,
                    timestamp + 2,
                    timestamp + 1,
                )
                .await
                .expect("exhausted transient failure should persist")
        );
        assert!(futures_util::FutureExt::now_or_never(retry_wake.wait(None)).is_some());
        retry_wake.begin_pass(&database).await.unwrap();
        assert_eq!(
            store
                .next_native_terminal_effect_recovery_deadline(timestamp + 1)
                .await
                .unwrap(),
            Some(timestamp + 3_601),
            "the cooldown belongs to the committed failure, independent of worker cadence"
        );
        assert_eq!(
            store
                .next_native_terminal_effect_recovery_deadline(timestamp + 86_401)
                .await
                .unwrap(),
            None,
            "expired history must not schedule empty recovery scans"
        );
        assert!(
            !store
                .fail_native_terminal_effect(
                    &effect_id,
                    &first.claim_token,
                    code,
                    "stale failure",
                    true,
                    timestamp + 2,
                    timestamp + 1,
                )
                .await
                .unwrap()
        );
        assert!(
            futures_util::FutureExt::now_or_never(retry_wake.wait(None)).is_none(),
            "a rejected stale claim must not wake its own discovery loop"
        );
        assert_eq!(
            outbox::load_status(&database, effect_id.as_str())
                .await
                .expect("status should load")
                .expect("effect should exist")
                .status,
            "unresolved"
        );

        assert_eq!(
            store
                .requeue_retryable_unresolved_native_terminal_effects(timestamp + 3_602, 1)
                .await
                .expect("recovery scan should succeed"),
            1
        );
        assert_eq!(
            store
                .next_native_terminal_effect_recovery_deadline(timestamp + 3_602)
                .await
                .unwrap(),
            None,
            "requeue consumed this exhausted obligation"
        );
        assert!(
            futures_util::FutureExt::now_or_never(retry_wake.wait(None)).is_none(),
            "the requeue ACK must not restart exhausted-effect discovery"
        );
        let reopened =
            outbox::claim_records(&database, timestamp + 3_602, 10, 1, &|| timestamp + 3_602)
                .await
                .expect("recovery scan should succeed")
                .records
                .pop()
                .expect("recent transient post-turn failure should reopen");
        assert_eq!(reopened.effect_id, effect_id);
        assert_eq!(reopened.attempt_count, 1);
        assert_eq!(reopened.max_attempts, 8);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn memory_write_recovery_is_classified_and_legacy_revalidation_is_bounded() {
    for (code, checkpoint, delay, expected, revalidated_code) in [
        (
            "memory.post_turn_extractor.write_invalid_input",
            true,
            3_602,
            0,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.write_domain_rejected",
            true,
            3_602,
            0,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.write_unclassified",
            true,
            3_602,
            0,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.write_storage_transient",
            true,
            3_602,
            1,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.write_failed",
            true,
            3_602,
            1,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.write_failed",
            true,
            3_602,
            1,
            "memory.post_turn_extractor.write_storage_transient",
        ),
        (
            "memory.post_turn_extractor.write_failed",
            false,
            3_602,
            0,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.write_failed",
            true,
            3_599,
            0,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.write_failed",
            true,
            86_401,
            0,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.legacy_write_revalidate",
            true,
            3_602,
            0,
            "memory.post_turn_extractor.write_unclassified",
        ),
        (
            "memory.post_turn_extractor.manifest_domain_rejected",
            false,
            3_602,
            0,
            "memory.post_turn_extractor.manifest_unclassified",
        ),
        (
            "memory.post_turn_extractor.manifest_invalid_stored_data",
            false,
            3_602,
            0,
            "memory.post_turn_extractor.manifest_unclassified",
        ),
        (
            "memory.post_turn_extractor.manifest_unclassified",
            false,
            3_602,
            0,
            "memory.post_turn_extractor.manifest_unclassified",
        ),
        (
            "memory.post_turn_extractor.manifest_storage_transient",
            false,
            3_602,
            1,
            "memory.post_turn_extractor.manifest_unclassified",
        ),
        (
            "memory.post_turn_extractor.manifest_failed",
            false,
            3_602,
            1,
            "memory.post_turn_extractor.manifest_unclassified",
        ),
        (
            "memory.post_turn_extractor.manifest_failed",
            true,
            3_602,
            1,
            "memory.post_turn_extractor.manifest_domain_rejected",
        ),
        (
            "memory.post_turn_extractor.manifest_failed",
            false,
            3_602,
            1,
            "memory.post_turn_extractor.manifest_invalid_stored_data",
        ),
        (
            "memory.post_turn_extractor.manifest_failed",
            false,
            3_602,
            1,
            "memory.post_turn_extractor.manifest_storage_transient",
        ),
        (
            "memory.post_turn_extractor.manifest_failed",
            false,
            3_599,
            0,
            "memory.post_turn_extractor.manifest_unclassified",
        ),
        (
            "memory.post_turn_extractor.manifest_failed",
            false,
            86_401,
            0,
            "memory.post_turn_extractor.manifest_unclassified",
        ),
        (
            "memory.post_turn_extractor.legacy_manifest_revalidate",
            false,
            3_602,
            0,
            "memory.post_turn_extractor.manifest_unclassified",
        ),
        ("effect_timeout", false, 3_602, 1, "effect_timeout"),
        (
            "memory.post_turn_extractor.provider_network_transient",
            false,
            3_602,
            1,
            "effect_timeout",
        ),
    ] {
        pioneer_postgres_test::with_database(async |fixture| {
            let timestamp = 1_700_040_000;
            let database = fixture_database(fixture).await?.maintenance();
            let store = TerminalEffectStore::new(&database);
            let effect_id = activate_post_turn_effect(&database, timestamp).await?;

            let first = outbox::claim_records(&database, timestamp + 1, 10, 1, &|| timestamp + 1)
                .await
                .expect("first attempt should claim")
                .records
                .pop()
                .expect("first attempt must exist");
            if checkpoint {
                store.store_native_terminal_effect_handler_checkpoint(
                    &effect_id, &first.claim_token, r#"{"schema_version":1,"raw_json":"{\"facts\":[]}","model":"model","model_provider":"provider"}"#, timestamp + 1,
                ).await.unwrap();
            }
            assert!(
                store
                    .fail_native_terminal_effect(
                        effect_id.as_str(),
                        first.claim_token.as_str(),
                        code,
                        "safe write failure",
                        matches!(
                            code,
                            "memory.post_turn_extractor.write_storage_transient"
                                | "memory.post_turn_extractor.write_failed"
                        ),
                        timestamp + 2,
                        timestamp + 1,
                    )
                    .await
                    .expect("exhausted transient failure should persist")
            );
            assert_eq!(
                outbox::load_status(&database, effect_id.as_str()).await
                    .expect("status should load")
                    .expect("effect should exist")
                    .status,
                "unresolved"
            );
            assert_eq!(
                store
                    .requeue_retryable_unresolved_native_terminal_effects(timestamp + delay, 0)
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                store
                    .requeue_retryable_unresolved_native_terminal_effects(timestamp + delay, 1)
                    .await
                    .unwrap(),
                expected,
                "{code}, delay={delay}"
            );
            if expected == 0 {
                assert!(
                    outbox::claim_records(&database, timestamp + delay, 10, 1, &|| timestamp + delay)
                        .await
                        .unwrap()
                        .records
                        .is_empty()
                );
                return Ok(());
            }
            // Repeat discovery through a newly scoped handle, as after restart.
            assert_eq!(
                TerminalEffectStore::new(&database.maintenance()).requeue_retryable_unresolved_native_terminal_effects(timestamp + delay, 1)
                    .await
                    .unwrap(),
                0
            );
            let reopened = outbox::claim_records(&database, timestamp + delay, 10, 1, &|| timestamp + delay)
                .await
                .unwrap()
                .records
                .pop()
                .unwrap();
            assert_eq!(reopened.effect_id, effect_id);
            assert_eq!(
                reopened.legacy_manifest_revalidation,
                code == "memory.post_turn_extractor.manifest_failed"
            );
            assert!(
                !store
                    .fail_native_terminal_effect(
                        &effect_id,
                        &first.claim_token,
                        "stale_owner",
                        "stale result",
                        false,
                        timestamp + delay,
                        timestamp + delay
                    )
                    .await
                    .unwrap()
            );

            if matches!(
                code,
                "memory.post_turn_extractor.write_failed"
                    | "memory.post_turn_extractor.manifest_failed"
            ) {
                assert_eq!(
                    reopened.max_attempts, 1,
                    "legacy revalidation must not expand the budget"
                );
                assert_eq!(reopened.attempt_count, reopened.max_attempts);
                assert_eq!(
                    outbox::load_status(&database, &effect_id).await
                        .unwrap()
                        .unwrap()
                        .last_error_code
                        .as_deref(),
                    Some(if code == "memory.post_turn_extractor.manifest_failed" {
                        "memory.post_turn_extractor.legacy_manifest_revalidate"
                    } else {
                        "memory.post_turn_extractor.legacy_write_revalidate"
                    })
                );
                assert_eq!(
                    store
                        .native_terminal_effect_handler_checkpoint(
                            &effect_id,
                            &reopened.claim_token
                        )
                        .await
                        .unwrap()
                        .is_some(),
                    checkpoint
                );
                // The revalidated cause replaces the legacy marker and stays terminal.
                store
                    .fail_native_terminal_effect(
                        &effect_id,
                        &reopened.claim_token,
                        revalidated_code,
                        "safe classified failure",
                        matches!(
                            revalidated_code,
                            "memory.post_turn_extractor.write_storage_transient"
                                | "memory.post_turn_extractor.manifest_storage_transient"
                        ),
                        timestamp + delay + 1,
                        timestamp + delay,
                    )
                    .await
                    .unwrap();
                let reclassified_recovery = store
                    .requeue_retryable_unresolved_native_terminal_effects(
                        timestamp + delay + 3_601,
                        1,
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    reclassified_recovery,
                    u64::from(matches!(
                        revalidated_code,
                        "memory.post_turn_extractor.write_storage_transient"
                            | "memory.post_turn_extractor.manifest_storage_transient"
                    ),)
                );
            } else {
                assert_eq!(
                    reopened.max_attempts, 8,
                    "preserve existing transient recovery budget"
                );
            }
                Ok(())
            }).await.unwrap();
    }
}

fn cleanup_effect_preparation(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    batch_suffix: &str,
) -> pioneer_protocol::NativeTerminalEffectPreparation {
    pioneer_protocol::NativeTerminalEffectPreparation {
        batch_id: format!("{turn_id}:batch:{batch_suffix}"),
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        runtime_generation: 1,
        effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
            effect_id: format!("{turn_id}:terminal-effect:attached-task-cleanup"),
            effect_kind: pioneer_protocol::NativeTerminalEffectKind::AttachedTaskCleanup,
            gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            payload: pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
                reason: "parent turn failed".to_owned(),
                runtime_contract: "pioneer.test.attached-task-cleanup.v1".to_owned(),
            },
            max_attempts: 3,
        }],
    }
}

#[tokio::test]
async fn native_terminal_effect_retention_is_bounded_and_preserves_recovery_authority() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_terminal_effect_retention";
        let thread_id = "thr_terminal_effect_retention";
        let turn_ids = [
            "turn_effect_old_succeeded",
            "turn_effect_old_discarded",
            "turn_effect_old_unresolved",
            "turn_effect_old_pending",
            "turn_effect_recent_succeeded",
        ];
        let writer = fixture.connection();
        writer.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws_terminal_effect_retention', 'Fixture');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thr_terminal_effect_retention', 'ws_terminal_effect_retention', '', 'agent', 'm', 'p', 'active');",
        ).await?;
        for turn_id in turn_ids {
            writer.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn (id, thread_id, status) VALUES ($1, $2, 'in_progress')",
                [turn_id.into(), thread_id.into()],
            )).await?;
        }
        let database = PostgresDatabase::new(fixture.connect_read_only().await?, writer.clone());
        let store = TerminalEffectStore::new(&database);
        for turn_id in turn_ids {
            store
                .prepare_native_terminal_effects(
                    cleanup_effect_preparation(workspace_id, thread_id, turn_id, "retention"),
                    1_700_000_000,
                )
                .await
                .expect("retention fixture effect should prepare");
        }

        let old_completed = unix_to_datetime(1_700_000_100);
        let recent_completed = unix_to_datetime(1_700_100_000);
        for (turn_id, status, completed_at) in [
            (turn_ids[0], "succeeded", Some(old_completed)),
            (turn_ids[1], "discarded", Some(old_completed)),
            (turn_ids[2], "unresolved", Some(old_completed)),
            (turn_ids[3], "prepared", None),
            (turn_ids[4], "succeeded", Some(recent_completed)),
        ] {
            pioneer_entity::native_terminal_effect_outbox::Entity::update_many()
                .col_expr(
                    pioneer_entity::native_terminal_effect_outbox::Column::Status,
                    Expr::value(status.to_owned()),
                )
                .col_expr(
                    pioneer_entity::native_terminal_effect_outbox::Column::CompletedAt,
                    Expr::value(completed_at),
                )
                .col_expr(
                    pioneer_entity::native_terminal_effect_outbox::Column::TerminalCommittedAt,
                    Expr::value(completed_at),
                )
                .filter(
                    pioneer_entity::native_terminal_effect_outbox::Column::TurnId
                        .eq(turn_id.to_owned()),
                )
                .exec(&database)
                .await
                .expect("retention fixture status should persist");
        }

        assert_eq!(
            store
                .purge_resolved_native_terminal_effects_before(1_700_050_000, 1)
                .await
                .expect("first bounded purge should succeed"),
            1,
            "one invocation must honor its row limit"
        );
        assert_eq!(
            store
                .purge_resolved_native_terminal_effects_before(1_700_050_000, 10)
                .await
                .expect("second bounded purge should succeed"),
            1,
            "the remaining old resolved row should be purged"
        );

        for turn_id in [turn_ids[2], turn_ids[3], turn_ids[4]] {
            let effect_id = format!("{turn_id}:terminal-effect:attached-task-cleanup");
            assert!(
                pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(effect_id)
                    .one(&database)
                    .await
                    .expect("retained effect lookup should succeed")
                    .is_some(),
                "pending, unresolved, and recent recovery authority must be retained"
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_terminal_effect_outbox_exhausts_retry_budget_into_explicit_unresolved_state() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_030_000;
        let workspace_id = "ws_post_turn_reopen";
        let thread_id = "thr_post_turn_reopen";
        let turn_id = "turn_post_turn_reopen";
        let database = fixture_database(fixture).await?;
        let store = TerminalEffectStore::new(&database);
        let mut preparation =
            cleanup_effect_preparation(workspace_id, thread_id, turn_id, "retry-budget");
        preparation.effects[0].max_attempts = 2;
        let effect_id = preparation.effects[0].effect_id.clone();
        store
            .prepare_native_terminal_effects(preparation, timestamp)
            .await
            .expect("retry-bounded effect should prepare");
        let activation = outbox::prepare_activation_for_terminal(&database, turn_id).await?;
        let tx = database.begin().await?;
        tx.execute_unprepared("UPDATE turn SET status='failed' WHERE id='turn_post_turn_reopen'")
            .await?;
        outbox::activate_prepared_for_terminal(&tx, activation, unix_to_datetime(timestamp + 1))
            .await
            .expect("terminal failure should activate cleanup obligation");
        tx.commit().await?;

        let first = outbox::claim_records(&database, timestamp + 1, 10, 1, &|| timestamp + 1)
            .await
            .expect("first attempt should claim")
            .records
            .pop()
            .expect("first attempt must exist");
        assert_eq!(first.attempt_count, 1);
        assert!(
            store
                .fail_native_terminal_effect(
                    effect_id.as_str(),
                    first.claim_token.as_str(),
                    "cleanup_unavailable",
                    "injected retryable cleanup failure",
                    true,
                    timestamp + 2,
                    timestamp + 1,
                )
                .await
                .expect("first failure should schedule a retry")
        );

        let second = outbox::claim_records(&database, timestamp + 2, 10, 1, &|| timestamp + 2)
            .await
            .expect("second attempt should claim")
            .records
            .pop()
            .expect("second attempt must exist");
        assert_eq!(second.attempt_count, 2);
        assert!(
            store
                .fail_native_terminal_effect(
                    effect_id.as_str(),
                    second.claim_token.as_str(),
                    "cleanup_unavailable",
                    "injected final cleanup failure",
                    true,
                    timestamp + 3,
                    timestamp + 2,
                )
                .await
                .expect("exhausted failure should terminalize explicitly")
        );
        let status = outbox::load_status(&database, effect_id.as_str())
            .await
            .expect("exhausted status should load")
            .expect("effect should remain inspectable");
        assert_eq!(status.status, "unresolved");
        assert_eq!(status.attempt_count, 2);
        assert_eq!(
            status.last_error_code.as_deref(),
            Some("cleanup_unavailable")
        );
        assert!(
            outbox::claim_records(&database, timestamp + 100, 10, 1, &|| timestamp + 100)
                .await
                .expect("terminal status lookup should succeed")
                .records
                .is_empty(),
            "an unresolved obligation must not retry forever"
        );
        assert!(
            !store
                .complete_native_terminal_effect(
                    effect_id.as_str(),
                    second.claim_token.as_str(),
                    timestamp + 100,
                )
                .await
                .expect("stale completion should be fenced")
        );
        assert_eq!(
            store
                .native_terminal_effect_stats()
                .await
                .expect("outbox stats should load")
                .unresolved,
            1
        );
        Ok(())
    })
    .await
    .unwrap();
}
