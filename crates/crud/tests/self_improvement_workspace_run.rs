use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    CrudStore, NewSelfImprovementRun, SelfImprovementFinalizationAuthority,
    SelfImprovementRunMutationResult,
};
use sea_orm::{
    ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait,
};

const NOW: i64 = 1_900_100_000;

async fn migrated_store() -> (DatabaseConnection, CrudStore) {
    let database = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite must open");
    Migrator::up(&database, None)
        .await
        .expect("migrations must apply");
    database
        .execute_unprepared(
            "INSERT INTO workspace (id, name, is_active, is_current) VALUES \
             ('ws_run_a', 'Run A', 1, 1), \
             ('ws_run_b', 'Run B', 1, 0)",
        )
        .await
        .expect("workspace fixtures must insert");
    let store = CrudStore::new(database.clone());
    (database, store)
}

async fn insert_source(
    database: &DatabaseConnection,
    workspace_id: &str,
    suffix: &str,
    terminal_at: i64,
) {
    let terminal_at = chrono::DateTime::from_timestamp(terminal_at, 0)
        .expect("source fixture timestamp must be valid")
        .fixed_offset();
    let thread_id = format!("thread_{suffix}");
    let turn_id = format!("turn_{suffix}");
    let event_id = format!("event_{suffix}");
    let transaction = database
        .begin()
        .await
        .expect("source fixture transaction must begin");
    transaction
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO thread (
                id, workspace_id, preview, mode, model, model_provider, status, origin_kind,
                access_class, created_at, updated_at
             ) VALUES (?, ?, '', 'agent', 'gpt-test', 'fake', 'active', 'user',
                'workspace', ?, ?)",
            [
                thread_id.clone().into(),
                workspace_id.into(),
                terminal_at.into(),
                terminal_at.into(),
            ],
        ))
        .await
        .expect("source fixture parent thread must insert");
    transaction
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO turn (
                id, thread_id, status, turn_kind, origin, created_at, updated_at
             ) VALUES (?, ?, 'completed', 'conversation', 'user', ?, ?)",
            [
                turn_id.clone().into(),
                thread_id.clone().into(),
                terminal_at.into(),
                terminal_at.into(),
            ],
        ))
        .await
        .expect("source fixture parent turn must insert");
    transaction
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO turn_event (
                id, thread_id, turn_id, sequence, event_type, payload, created_at
             ) VALUES (?, ?, ?, 1, 'turn/completed', '{}', ?)",
            [
                event_id.clone().into(),
                thread_id.clone().into(),
                turn_id.clone().into(),
                terminal_at.into(),
            ],
        ))
        .await
        .expect("source fixture terminal event must insert");
    transaction
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO self_improvement_source_turn (
                workspace_id, thread_id, turn_id, terminal_event_id, terminal_at, created_at
             ) VALUES (?, ?, ?, ?, ?, ?)",
            [
                workspace_id.into(),
                thread_id.into(),
                turn_id.into(),
                event_id.into(),
                terminal_at.into(),
                terminal_at.into(),
            ],
        ))
        .await
        .expect("source fixture must insert");
    transaction
        .commit()
        .await
        .expect("source fixture transaction must commit");
}

async fn scalar_i64(database: &DatabaseConnection, sql: impl Into<String>) -> i64 {
    database
        .query_one_raw(Statement::from_string(DatabaseBackend::Sqlite, sql.into()))
        .await
        .expect("query must execute")
        .expect("query must return one row")
        .try_get("", "value")
        .expect("result must decode")
}

#[tokio::test]
async fn frozen_source_range_excludes_delayed_parallel_and_cross_workspace_rows() {
    let (database, store) = migrated_store().await;
    insert_source(&database, "ws_run_a", "before_activation", NOW - 10).await;
    let active = store
        .activate_self_improvement_workspace("ws_run_a", NOW)
        .await
        .expect("workspace must activate at the current source head");
    assert_eq!(active.cursor_source_id, 1);

    // This row is projected after activation but carries its exact old terminal time.
    insert_source(&database, "ws_run_a", "delayed_old_completion", NOW - 1).await;
    insert_source(&database, "ws_run_a", "selected", NOW + 1).await;
    insert_source(&database, "ws_run_b", "other_workspace", NOW + 1).await;
    insert_source(&database, "ws_run_a", "parallel_backlog", NOW + 2).await;

    let selected = store
        .list_self_improvement_source_turns_after(
            "ws_run_a",
            active.cursor_source_id,
            active
                .effective_enabled_at_unix
                .expect("active state must carry its activation timestamp"),
            1,
        )
        .await
        .expect("bounded source selection must load");
    assert_eq!(
        selected
            .iter()
            .map(|source| source.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn_selected"]
    );
    let frozen_upper = selected[0].id;

    insert_source(&database, "ws_run_a", "concurrent_after_freeze", NOW + 3).await;
    let first_read = store
        .list_frozen_self_improvement_source_range(
            "ws_run_a",
            active.cursor_source_id,
            frozen_upper,
            active.effective_enabled_at_unix.unwrap(),
        )
        .await
        .expect("frozen source range must load");
    let retry_read = store
        .list_frozen_self_improvement_source_range(
            "ws_run_a",
            active.cursor_source_id,
            frozen_upper,
            active.effective_enabled_at_unix.unwrap(),
        )
        .await
        .expect("frozen source retry must load");
    assert_eq!(retry_read, first_read);
    assert_eq!(first_read, selected);

    let backlog = store
        .list_self_improvement_source_turns_after(
            "ws_run_a",
            frozen_upper,
            active.effective_enabled_at_unix.unwrap(),
            10,
        )
        .await
        .expect("backlog after the frozen upper must remain available");
    assert_eq!(
        backlog
            .iter()
            .map(|source| source.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn_parallel_backlog", "turn_concurrent_after_freeze"]
    );
    assert!(
        first_read
            .iter()
            .all(|source| source.workspace_id == "ws_run_a")
    );
}

fn new_run(
    workspace_id: &str,
    activation_epoch: i64,
    source_lower_exclusive: i64,
    source_upper_inclusive: i64,
) -> NewSelfImprovementRun {
    NewSelfImprovementRun {
        workspace_id: workspace_id.to_owned(),
        activation_epoch,
        scheduled_date_utc: "2030-03-01".to_owned(),
        source_lower_exclusive,
        source_upper_inclusive,
        learner_provider: "openai".to_owned(),
        learner_model: "gpt-5.4".to_owned(),
        reviewer_provider: "openai".to_owned(),
        reviewer_model: "gpt-5.4".to_owned(),
        pipeline_contract_version: "self-improvement-v1".to_owned(),
    }
}

#[tokio::test]
async fn activation_baselines_head_and_daily_run_identity_freezes_the_range() {
    let (database, store) = migrated_store().await;
    let initial = store
        .get_or_create_self_improvement_workspace_state("ws_run_a", NOW)
        .await
        .expect("inactive state must initialize");
    assert_eq!(initial.activation_epoch, 0);
    assert_eq!(initial.cursor_source_id, 0);
    assert_eq!(initial.effective_enabled_at_unix, None);

    insert_source(&database, "ws_run_a", "before_activation", NOW + 1).await;
    let active = store
        .activate_self_improvement_workspace("ws_run_a", NOW + 2)
        .await
        .expect("workspace must activate");
    assert_eq!(active.activation_epoch, 1);
    assert_eq!(active.cursor_source_id, 1);
    assert_eq!(active.effective_enabled_at_unix, Some(NOW + 2));

    insert_source(&database, "ws_run_a", "after_activation_1", NOW + 3).await;
    let active_again = store
        .activate_self_improvement_workspace("ws_run_a", NOW + 4)
        .await
        .expect("already-active activation must be idempotent");
    assert_eq!(active_again.activation_epoch, 1);
    assert_eq!(
        active_again.cursor_source_id, 1,
        "repeat activation must not move the baseline"
    );
    assert_eq!(active_again.effective_enabled_at_unix, Some(NOW + 2));

    let first = store
        .create_or_get_self_improvement_run(
            new_run(
                "ws_run_a",
                active.activation_epoch,
                active.cursor_source_id,
                2,
            ),
            NOW + 5,
        )
        .await
        .expect("daily run must be created");
    assert_eq!(first.source_lower_exclusive, 1);
    assert_eq!(first.source_upper_inclusive, 2);
    assert_eq!(first.status, "pending");

    insert_source(&database, "ws_run_a", "after_creation", NOW + 6).await;
    let mut replay_input = new_run(
        "ws_run_a",
        active.activation_epoch,
        active.cursor_source_id,
        3,
    );
    replay_input.learner_model = "gpt-newer".to_owned();
    let replay = store
        .create_or_get_self_improvement_run(replay_input, NOW + 7)
        .await
        .expect("duplicate logical day must return its frozen run");
    assert_eq!(replay.id, first.id);
    assert_eq!(
        replay.source_upper_inclusive, 2,
        "newly arrived source must not alter a frozen daily range"
    );
    assert_eq!(
        replay.learner_model, "gpt-5.4",
        "new selections must not mutate a frozen daily run"
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_run WHERE workspace_id = 'ws_run_a'"
        )
        .await,
        1
    );

    let oldest = store
        .get_oldest_unresolved_self_improvement_run("ws_run_a", active.activation_epoch)
        .await
        .expect("oldest unresolved lookup must execute")
        .expect("pending daily run must be unresolved");
    assert_eq!(oldest.id, first.id);
    let mut next_day = new_run(
        "ws_run_a",
        active.activation_epoch,
        active.cursor_source_id,
        3,
    );
    next_day.scheduled_date_utc = "2030-03-02".to_owned();
    let blocked = store
        .create_or_get_self_improvement_run(next_day, NOW + 8)
        .await
        .expect_err("a newer source range must wait for the oldest unresolved run");
    assert!(format!("{blocked:#}").contains("must resolve"));

    let claimed = store
        .claim_available_self_improvement_run(
            "ws_run_a",
            first.id.as_str(),
            active.activation_epoch,
            "gateway-before-disable",
            NOW + 9,
            NOW + 100,
        )
        .await
        .expect("unfinished run claim must execute")
        .expect("unfinished run must be claimed");
    assert_eq!(claimed.status, "running");

    let inactive = store
        .deactivate_self_improvement_workspace("ws_run_a", NOW + 10)
        .await
        .expect("disable transition must commit");
    assert_eq!(
        inactive.activation_epoch, 1,
        "disable must not create a new activation epoch"
    );
    assert_eq!(inactive.cursor_source_id, 1);
    assert_eq!(inactive.effective_enabled_at_unix, None);
    let cancelled = store
        .get_self_improvement_run("ws_run_a", first.id.as_str())
        .await
        .expect("cancelled run must load")
        .expect("cancelled run must still exist");
    assert_eq!(cancelled.status, "cancelled");
    assert!(cancelled.claim_token.is_none());
    assert!(cancelled.claimed_by.is_none());
    assert!(cancelled.lease_expires_at_unix.is_none());
    assert_eq!(
        cancelled.last_error.as_deref(),
        Some("self_improvement_disabled")
    );

    let reactivated = store
        .activate_self_improvement_workspace("ws_run_a", NOW + 11)
        .await
        .expect("re-enable transition must commit");
    assert_eq!(reactivated.activation_epoch, 2);
    assert_eq!(
        reactivated.cursor_source_id, 3,
        "re-enable must baseline every source observed while disabled"
    );
    assert_eq!(reactivated.effective_enabled_at_unix, Some(NOW + 11));
}

#[tokio::test]
async fn claims_checkpoints_and_reads_are_workspace_scoped_and_fenced() {
    let (database, store) = migrated_store().await;
    let state_a = store
        .activate_self_improvement_workspace("ws_run_a", NOW)
        .await
        .expect("workspace A must activate");
    let state_b = store
        .activate_self_improvement_workspace("ws_run_b", NOW)
        .await
        .expect("workspace B must activate");
    insert_source(&database, "ws_run_a", "a1", NOW + 1).await;
    insert_source(&database, "ws_run_b", "b1", NOW + 1).await;

    let run_a = store
        .create_or_get_self_improvement_run(
            new_run(
                "ws_run_a",
                state_a.activation_epoch,
                state_a.cursor_source_id,
                1,
            ),
            NOW + 2,
        )
        .await
        .expect("workspace A run must create");
    let run_b = store
        .create_or_get_self_improvement_run(
            new_run(
                "ws_run_b",
                state_b.activation_epoch,
                state_b.cursor_source_id,
                2,
            ),
            NOW + 2,
        )
        .await
        .expect("workspace B run must create");
    assert_ne!(run_a.id, run_b.id);
    assert!(
        store
            .get_self_improvement_run("ws_run_b", run_a.id.as_str())
            .await
            .expect("cross-workspace lookup must execute")
            .is_none()
    );

    let claimed = store
        .claim_available_self_improvement_run(
            "ws_run_a",
            run_a.id.as_str(),
            state_a.activation_epoch,
            "gateway-a",
            NOW + 3,
            NOW + 100,
        )
        .await
        .expect("first claim must execute")
        .expect("first claim must win");
    assert_eq!(claimed.status, "running");
    assert_eq!(claimed.attempt_count, 1);
    assert!(
        store
            .claim_available_self_improvement_run(
                "ws_run_a",
                run_a.id.as_str(),
                state_a.activation_epoch,
                "gateway-b",
                NOW + 4,
                NOW + 100,
            )
            .await
            .expect("second claim must execute")
            .is_none(),
        "a live lease must reject a second owner"
    );

    let fence = claimed.fence().expect("claimed run must expose its fence");
    assert_eq!(
        store
            .save_self_improvement_run_checkpoint(
                &fence,
                r#"{"nextChunk":1}"#,
                r#"{"observations":[]}"#,
                NOW + 5,
            )
            .await
            .expect("valid checkpoint must save"),
        SelfImprovementRunMutationResult::Applied
    );

    let mut stale_token = fence.clone();
    stale_token.claim_token.push_str("-stale");
    assert_eq!(
        store
            .save_self_improvement_run_checkpoint(
                &stale_token,
                r#"{"nextChunk":2}"#,
                r#"{"observations":["stale"]}"#,
                NOW + 6,
            )
            .await
            .expect("stale token must be reported"),
        SelfImprovementRunMutationResult::LostAuthority
    );
    let mut wrong_workspace = fence.clone();
    wrong_workspace.workspace_id = "ws_run_b".to_owned();
    assert_eq!(
        store
            .save_self_improvement_run_checkpoint(
                &wrong_workspace,
                r#"{"nextChunk":2}"#,
                r#"{"observations":["wrong-workspace"]}"#,
                NOW + 6,
            )
            .await
            .expect("wrong workspace must be reported"),
        SelfImprovementRunMutationResult::LostAuthority
    );
    let mut stale_epoch = fence.clone();
    stale_epoch.activation_epoch += 1;
    assert_eq!(
        store
            .save_self_improvement_run_checkpoint(
                &stale_epoch,
                r#"{"nextChunk":2}"#,
                r#"{"observations":["stale-epoch"]}"#,
                NOW + 6,
            )
            .await
            .expect("stale epoch must be reported"),
        SelfImprovementRunMutationResult::LostAuthority
    );
    let mut stale_upper_boundary = fence.clone();
    stale_upper_boundary.source_upper_inclusive += 1;
    assert_eq!(
        store
            .save_self_improvement_run_checkpoint(
                &stale_upper_boundary,
                r#"{"nextChunk":2}"#,
                r#"{"observations":["stale-upper-boundary"]}"#,
                NOW + 6,
            )
            .await
            .expect("stale upper boundary must be reported"),
        SelfImprovementRunMutationResult::LostAuthority
    );

    database
        .execute_unprepared(
            "UPDATE self_improvement_workspace_state \
             SET cursor_source_id = 1 WHERE workspace_id = 'ws_run_a'",
        )
        .await
        .expect("cursor fixture must update");
    assert_eq!(
        store
            .save_self_improvement_run_checkpoint(
                &fence,
                r#"{"nextChunk":2}"#,
                r#"{"observations":["stale-cursor"]}"#,
                NOW + 7,
            )
            .await
            .expect("stale cursor must be reported"),
        SelfImprovementRunMutationResult::LostAuthority
    );

    let persisted = store
        .get_self_improvement_run("ws_run_a", run_a.id.as_str())
        .await
        .expect("run must load")
        .expect("run must exist");
    assert_eq!(
        persisted.analysis_cursor_json.as_deref(),
        Some(r#"{"nextChunk":1}"#)
    );
    assert_eq!(
        persisted.analysis_digest_json.as_deref(),
        Some(r#"{"observations":[]}"#)
    );

    database
        .execute_unprepared(
            "UPDATE self_improvement_workspace_state \
             SET effective_enabled_at = NULL WHERE workspace_id = 'ws_run_a'",
        )
        .await
        .expect("disable fixture must update");
    assert_eq!(
        store
            .heartbeat_self_improvement_run(&fence, NOW + 8, NOW + 100)
            .await
            .expect("disabled workspace heartbeat must execute"),
        SelfImprovementRunMutationResult::LostAuthority,
        "the run update itself must atomically require an effective workspace"
    );
}

#[tokio::test]
async fn two_gateways_claim_once_and_expired_lease_rotates_authority() {
    let (database, gateway_a) = migrated_store().await;
    let gateway_b = CrudStore::new(database.clone());
    let state = gateway_a
        .activate_self_improvement_workspace("ws_run_a", NOW)
        .await
        .expect("workspace must activate");
    insert_source(&database, "ws_run_a", "two_gateway", NOW + 1).await;
    let run = gateway_a
        .create_or_get_self_improvement_run(
            new_run(
                "ws_run_a",
                state.activation_epoch,
                state.cursor_source_id,
                1,
            ),
            NOW + 2,
        )
        .await
        .expect("run must create");

    let (claim_a, claim_b) = tokio::join!(
        gateway_a.claim_available_self_improvement_run(
            "ws_run_a",
            run.id.as_str(),
            state.activation_epoch,
            "gateway-a",
            NOW + 3,
            NOW + 10,
        ),
        gateway_b.claim_available_self_improvement_run(
            "ws_run_a",
            run.id.as_str(),
            state.activation_epoch,
            "gateway-b",
            NOW + 3,
            NOW + 10,
        ),
    );
    let claim_a = claim_a.expect("gateway A claim must execute");
    let claim_b = claim_b.expect("gateway B claim must execute");
    assert_eq!(
        (claim_a.is_some() as usize) + (claim_b.is_some() as usize),
        1,
        "one conditional update must elect exactly one owner"
    );
    let first = claim_a.or(claim_b).expect("one owner must win");
    let first_fence = first.fence().expect("first owner must have a fence");

    assert_eq!(
        gateway_a
            .heartbeat_self_improvement_run(&first_fence, NOW + 4, NOW + 20)
            .await
            .expect("heartbeat must execute"),
        SelfImprovementRunMutationResult::Applied
    );
    assert_eq!(
        gateway_a
            .get_next_self_improvement_retry_at()
            .await
            .expect("lease-expiry wakeup must load"),
        Some(NOW + 20),
        "a restarted supervisor must wake when the running run becomes reclaimable"
    );
    assert!(
        gateway_b
            .claim_available_self_improvement_run(
                "ws_run_a",
                run.id.as_str(),
                state.activation_epoch,
                "gateway-before-expiry",
                NOW + 19,
                NOW + 30,
            )
            .await
            .expect("live-lease claim must execute")
            .is_none(),
        "a live heartbeat must protect the current owner"
    );

    let reclaimed = gateway_b
        .claim_available_self_improvement_run(
            "ws_run_a",
            run.id.as_str(),
            state.activation_epoch,
            "gateway-takeover",
            NOW + 20,
            NOW + 40,
        )
        .await
        .expect("expired lease reclaim must execute")
        .expect("expired lease must be reclaimed");
    let reclaimed_fence = reclaimed
        .fence()
        .expect("reclaimed owner must have a fence");
    assert_ne!(first_fence.claim_token, reclaimed_fence.claim_token);
    assert_eq!(reclaimed.claimed_by.as_deref(), Some("gateway-takeover"));
    assert_eq!(reclaimed.attempt_count, 2);

    for result in [
        gateway_a
            .heartbeat_self_improvement_run(&first_fence, NOW + 21, NOW + 50)
            .await
            .expect("stale heartbeat must execute"),
        gateway_a
            .save_self_improvement_run_checkpoint(
                &first_fence,
                r#"{"nextChunk":9}"#,
                r#"{"observations":["stale"]}"#,
                NOW + 21,
            )
            .await
            .expect("stale checkpoint must execute"),
        gateway_a
            .return_self_improvement_run_to_pending(&first_fence, NOW + 21, NOW + 30, "stale_retry")
            .await
            .expect("stale pending transition must execute"),
        gateway_a
            .yield_self_improvement_run_after_budget(&first_fence, NOW + 21, NOW + 30)
            .await
            .expect("stale budget yield must execute"),
        gateway_a
            .fail_claimed_self_improvement_run(&first_fence, NOW + 21, "stale_failure")
            .await
            .expect("stale failed transition must execute"),
        gateway_a
            .cancel_claimed_self_improvement_run(&first_fence, NOW + 21, "stale_cancel")
            .await
            .expect("stale cancelled transition must execute"),
    ] {
        assert_eq!(result, SelfImprovementRunMutationResult::LostAuthority);
    }

    let persisted = gateway_b
        .get_self_improvement_run("ws_run_a", run.id.as_str())
        .await
        .expect("reclaimed run must load")
        .expect("reclaimed run must exist");
    assert_eq!(persisted.status, "running");
    assert_eq!(persisted.claim_token, Some(reclaimed_fence.claim_token));
    assert_eq!(persisted.last_error, None);
    assert_eq!(persisted.analysis_cursor_json, None);
}

#[tokio::test]
async fn failed_requeue_and_authority_reset_atomically_require_an_active_workspace() {
    let (database, store) = migrated_store().await;
    let state = store
        .activate_self_improvement_workspace("ws_run_a", NOW)
        .await
        .expect("workspace must activate");
    insert_source(&database, "ws_run_a", "disabled_requeue", NOW + 1).await;
    let run = store
        .create_or_get_self_improvement_run(
            new_run(
                "ws_run_a",
                state.activation_epoch,
                state.cursor_source_id,
                1,
            ),
            NOW + 2,
        )
        .await
        .expect("run must create");
    let claimed = store
        .claim_available_self_improvement_run(
            "ws_run_a",
            run.id.as_str(),
            state.activation_epoch,
            "gateway-disabled-requeue",
            NOW + 3,
            NOW + 100,
        )
        .await
        .expect("claim must execute")
        .expect("claim must win");
    assert_eq!(
        store
            .fail_claimed_self_improvement_run(
                &claimed.fence().expect("claim must expose a fence"),
                NOW + 4,
                "infrastructure_retry_exhausted",
            )
            .await
            .expect("failed transition must execute"),
        SelfImprovementRunMutationResult::Applied
    );
    let failed = store
        .get_self_improvement_run("ws_run_a", run.id.as_str())
        .await
        .expect("failed run must query")
        .expect("failed run must exist");

    database
        .execute_unprepared(
            "UPDATE self_improvement_workspace_state \
             SET effective_enabled_at = NULL WHERE workspace_id = 'ws_run_a'",
        )
        .await
        .expect("disabled-state fixture must update");

    assert_eq!(
        store
            .requeue_failed_self_improvement_run(&failed, NOW + 5)
            .await
            .expect("disabled requeue must execute"),
        SelfImprovementRunMutationResult::LostAuthority
    );
    let replacement = SelfImprovementFinalizationAuthority {
        effective_enabled: true,
        learner_provider: "openai".to_owned(),
        learner_model: "gpt-5.5".to_owned(),
        reviewer_provider: "openai".to_owned(),
        reviewer_model: "gpt-5.5".to_owned(),
        pipeline_contract_version: "self-improvement-v2".to_owned(),
    };
    assert_eq!(
        store
            .reset_unfinished_self_improvement_run_authority(&failed, &replacement, NOW + 6)
            .await
            .expect("disabled authority reset must execute"),
        SelfImprovementRunMutationResult::LostAuthority
    );

    let persisted = store
        .get_self_improvement_run("ws_run_a", run.id.as_str())
        .await
        .expect("run must query after rejected mutations")
        .expect("run must still exist");
    assert_eq!(persisted.status, "failed");
    assert_eq!(persisted.learner_model, "gpt-5.4");
    assert_eq!(persisted.pipeline_contract_version, "self-improvement-v1");
}

#[tokio::test]
async fn claimed_status_transitions_share_the_full_fence_and_clear_lease() {
    for target in ["pending", "failed", "cancelled"] {
        let (database, store) = migrated_store().await;
        let state = store
            .activate_self_improvement_workspace("ws_run_a", NOW)
            .await
            .expect("workspace must activate");
        insert_source(&database, "ws_run_a", target, NOW + 1).await;
        let run = store
            .create_or_get_self_improvement_run(
                new_run(
                    "ws_run_a",
                    state.activation_epoch,
                    state.cursor_source_id,
                    1,
                ),
                NOW + 2,
            )
            .await
            .expect("run must create");
        let claimed = store
            .claim_available_self_improvement_run(
                "ws_run_a",
                run.id.as_str(),
                state.activation_epoch,
                "gateway-transition",
                NOW + 3,
                NOW + 100,
            )
            .await
            .expect("claim must execute")
            .expect("claim must win");
        let fence = claimed.fence().expect("claim must expose a fence");
        assert_eq!(
            store
                .save_self_improvement_run_checkpoint(
                    &fence,
                    r#"{"nextChunkIndex":1}"#,
                    r#"{"validated":null}"#,
                    NOW + 4,
                )
                .await
                .expect("checkpoint fixture must save"),
            SelfImprovementRunMutationResult::Applied
        );
        let result = match target {
            "pending" => {
                store
                    .return_self_improvement_run_to_pending(
                        &fence,
                        NOW + 4,
                        NOW + 20,
                        "provider_transport_failed",
                    )
                    .await
            }
            "failed" => {
                store
                    .fail_claimed_self_improvement_run(
                        &fence,
                        NOW + 4,
                        "infrastructure_retry_exhausted",
                    )
                    .await
            }
            "cancelled" => {
                store
                    .cancel_claimed_self_improvement_run(&fence, NOW + 4, "settings_invalidated")
                    .await
            }
            _ => unreachable!(),
        }
        .expect("fenced transition must execute");
        assert_eq!(result, SelfImprovementRunMutationResult::Applied);

        let persisted = store
            .get_self_improvement_run("ws_run_a", run.id.as_str())
            .await
            .expect("transitioned run must query")
            .expect("transitioned run must exist");
        assert_eq!(persisted.status, target);
        assert_eq!(persisted.claim_token, None);
        assert_eq!(persisted.claimed_by, None);
        assert_eq!(persisted.lease_expires_at_unix, None);
        assert_eq!(
            persisted.next_attempt_at_unix.is_some(),
            target == "pending"
        );
        assert_eq!(
            persisted.analysis_cursor_json.is_none(),
            target == "cancelled",
            "only cancellation is terminal and must clear resumable analysis"
        );
        assert_eq!(
            persisted.analysis_digest_json.is_none(),
            target == "cancelled",
            "only cancellation is terminal and must clear resumable analysis"
        );
    }
}

#[tokio::test]
async fn wake_budget_yield_preserves_checkpoint_and_resets_failure_attempts() {
    let (database, store) = migrated_store().await;
    let state = store
        .activate_self_improvement_workspace("ws_run_a", NOW)
        .await
        .expect("workspace must activate");
    insert_source(&database, "ws_run_a", "budget_yield", NOW + 1).await;
    let run = store
        .create_or_get_self_improvement_run(
            new_run(
                "ws_run_a",
                state.activation_epoch,
                state.cursor_source_id,
                1,
            ),
            NOW + 2,
        )
        .await
        .expect("run must create");
    let claimed = store
        .claim_available_self_improvement_run(
            "ws_run_a",
            run.id.as_str(),
            state.activation_epoch,
            "gateway-budget",
            NOW + 3,
            NOW + 100,
        )
        .await
        .expect("run must claim")
        .expect("claim must win");
    let fence = claimed.fence().expect("claim must expose a fence");
    store
        .save_self_improvement_run_checkpoint(
            &fence,
            r#"{"nextChunkIndex":2}"#,
            r#"{"validated":{"digestRevision":2}}"#,
            NOW + 4,
        )
        .await
        .expect("checkpoint must save");
    assert_eq!(
        store
            .yield_self_improvement_run_after_budget(&fence, NOW + 5, NOW + 6)
            .await
            .expect("budget yield must execute"),
        SelfImprovementRunMutationResult::Applied
    );
    let yielded = store
        .get_self_improvement_run("ws_run_a", run.id.as_str())
        .await
        .expect("yielded run must query")
        .expect("yielded run must exist");
    assert_eq!(yielded.status, "pending");
    assert_eq!(yielded.attempt_count, 0);
    assert_eq!(yielded.next_attempt_at_unix, Some(NOW + 6));
    assert_eq!(
        yielded.analysis_cursor_json.as_deref(),
        Some(r#"{"nextChunkIndex":2}"#)
    );
    assert_eq!(
        yielded.analysis_digest_json.as_deref(),
        Some(r#"{"validated":{"digestRevision":2}}"#)
    );
    assert_eq!(yielded.last_error, None);
}

#[tokio::test]
async fn stale_state_rejects_run_creation_and_invalid_checkpoint_payloads() {
    let (database, store) = migrated_store().await;
    let state = store
        .activate_self_improvement_workspace("ws_run_a", NOW)
        .await
        .expect("workspace must activate");
    insert_source(&database, "ws_run_a", "validation", NOW + 1).await;

    let stale = store
        .create_or_get_self_improvement_run(
            new_run(
                "ws_run_a",
                state.activation_epoch + 1,
                state.cursor_source_id,
                1,
            ),
            NOW + 2,
        )
        .await
        .expect_err("stale epoch must reject run creation");
    assert!(format!("{stale:#}").contains("stale"));

    let run = store
        .create_or_get_self_improvement_run(
            new_run(
                "ws_run_a",
                state.activation_epoch,
                state.cursor_source_id,
                1,
            ),
            NOW + 2,
        )
        .await
        .expect("valid run must create");
    let claimed = store
        .claim_available_self_improvement_run(
            "ws_run_a",
            run.id.as_str(),
            state.activation_epoch,
            "gateway-a",
            NOW + 3,
            NOW + 100,
        )
        .await
        .expect("claim must execute")
        .expect("claim must win");
    let invalid_json = store
        .save_self_improvement_run_checkpoint(
            &claimed.fence().expect("claim fence"),
            "{not-json}",
            r#"{"observations":[]}"#,
            NOW + 4,
        )
        .await
        .expect_err("invalid checkpoint JSON must reject");
    assert!(format!("{invalid_json:#}").contains("valid JSON"));
}
