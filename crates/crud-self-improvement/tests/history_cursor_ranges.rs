use pioneer_crud_self_improvement::store::SelfImprovementStore;
use pioneer_crud_self_improvement::{
    NewSelfImprovementRun, SelfImprovementFinalizationAuthority, SelfImprovementRunMutationResult,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};

const NOW: i64 = 1_900_100_000;

#[tokio::test]
async fn history_rewinds_legacy_baseline_but_excludes_completed_ranges_across_epochs() {
    pioneer_postgres_test::with_database(async |fixture| {
    let (database, store) = migrated_store(fixture).await?;
    for suffix in ["missed", "consumed", "missed_later"] {
        insert_source(&database, "ws_run_a", suffix, NOW - 10).await;
    }
    store
        .activate_self_improvement_workspace("ws_run_a", NOW)
        .await
        .unwrap();
    database.execute_unprepared("UPDATE self_improvement_workspace_state SET cursor_source_id = 1 WHERE workspace_id = 'ws_run_a'").await.unwrap();
    let run = store
        .create_or_get_self_improvement_run(new_run("ws_run_a", 1, 1, 2), NOW)
        .await
        .unwrap();
    database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE self_improvement_run SET status = 'completed' WHERE id = $1",
            [run.id.into()],
        ))
        .await
        .unwrap();
    database.execute_unprepared("UPDATE self_improvement_workspace_state SET cursor_source_id = 3 WHERE workspace_id = 'ws_run_a'").await.unwrap();
    store
        .deactivate_self_improvement_workspace("ws_run_a", NOW + 1)
        .await
        .unwrap();
    let active = store
        .activate_self_improvement_workspace("ws_run_a", NOW + 2)
        .await
        .unwrap();
    assert_eq!(
        active.cursor_source_id, 3,
        "re-enable preserves durable progress"
    );
    store
        .reconcile_self_improvement_history_cursor("ws_run_a")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_self_improvement_workspace_state("ws_run_a")
            .await
            .unwrap()
            .unwrap()
            .cursor_source_id,
        0
    );
    let selected = store
        .list_self_improvement_source_turns_after("ws_run_a", 0, NOW + 2, 10)
        .await
        .unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|source| source.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn_missed", "turn_missed_later"]
    );
    assert_eq!(
        selected,
        store
            .list_frozen_self_improvement_source_range("ws_run_a", 0, 3, NOW + 2)
            .await
            .unwrap()
    );
    store
        .reconcile_self_improvement_history_cursor("ws_run_a")
        .await
        .unwrap();
    assert_eq!(
        selected,
        store
            .list_self_improvement_source_turns_after("ws_run_a", 0, NOW + 2, 10)
            .await
            .unwrap()
    );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn history_reconciliation_never_rewinds_an_unresolved_frozen_run() {
    pioneer_postgres_test::with_database(async |fixture| {
    let (database, store) = migrated_store(fixture).await?;
    insert_source(&database, "ws_run_a", "missed", NOW - 10).await;
    insert_source(&database, "ws_run_a", "in_flight", NOW + 1).await;
    store
        .activate_self_improvement_workspace("ws_run_a", NOW)
        .await
        .unwrap();
    database.execute_unprepared("UPDATE self_improvement_workspace_state SET cursor_source_id = 1 WHERE workspace_id = 'ws_run_a'").await.unwrap();
    let run = store
        .create_or_get_self_improvement_run(new_run("ws_run_a", 1, 1, 2), NOW + 2)
        .await
        .unwrap();
    store
        .reconcile_self_improvement_history_cursor("ws_run_a")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_self_improvement_workspace_state("ws_run_a")
            .await
            .unwrap()
            .unwrap()
            .cursor_source_id,
        1
    );
    assert_eq!(
        store
            .get_self_improvement_run("ws_run_a", &run.id)
            .await
            .unwrap()
            .unwrap(),
        run
    );
    store
        .deactivate_self_improvement_workspace("ws_run_a", NOW + 3)
        .await
        .unwrap();
    store
        .activate_self_improvement_workspace("ws_run_a", NOW + 4)
        .await
        .unwrap();
    store
        .reconcile_self_improvement_history_cursor("ws_run_a")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_self_improvement_workspace_state("ws_run_a")
            .await
            .unwrap()
            .unwrap()
            .cursor_source_id,
        0
    );
        Ok(())
    })
    .await
    .unwrap();
}

async fn migrated_store(
    fixture: &pioneer_postgres_test::TestDatabase,
) -> anyhow::Result<(DatabaseConnection, SelfImprovementStore)> {
    let writer = fixture.connection();
    writer
        .execute_unprepared(
            "UPDATE workspace SET is_current = false WHERE is_current;
         INSERT INTO workspace (id, name, is_active, is_current) VALUES
            ('ws_run_a', 'Run A', true, true),
            ('ws_run_b', 'Run B', true, false)",
        )
        .await?;
    let store = SelfImprovementStore::new(pioneer_sqlite::SqliteDatabase::new(
        fixture.connect_read_only().await?,
        writer.clone(),
    ));
    Ok((writer.clone(), store))
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
            DatabaseBackend::Postgres,
            "INSERT INTO thread (
                id, workspace_id, preview, mode, model, model_provider, status, origin_kind,
                access_class, created_at, updated_at
             ) VALUES ($1, $2, '', 'agent', 'gpt-test', 'fake', 'active', 'user',
                'workspace', $3, $4)",
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
            DatabaseBackend::Postgres,
            "INSERT INTO turn (
                id, thread_id, status, turn_kind, origin, created_at, updated_at
             ) VALUES ($1, $2, 'completed', 'conversation', 'user', $3, $4)",
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
            DatabaseBackend::Postgres,
            "INSERT INTO turn_event (
                id, thread_id, turn_id, sequence, event_type, payload, created_at
             ) VALUES ($1, $2, $3, 1, 'turn/completed', '{}', $4)",
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
            DatabaseBackend::Postgres,
            "INSERT INTO self_improvement_source_turn (
                workspace_id, thread_id, turn_id, terminal_event_id, terminal_at, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6)",
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

#[tokio::test]
async fn frozen_source_range_includes_history_but_excludes_parallel_and_cross_workspace_rows() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (database, store) = migrated_store(fixture).await?;
        insert_source(&database, "ws_run_a", "before_activation", NOW - 10).await;
        let active = store
            .activate_self_improvement_workspace("ws_run_a", NOW)
            .await
            .expect("workspace must activate without skipping historical sources");
        assert_eq!(active.cursor_source_id, 0);

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
                3,
            )
            .await
            .expect("bounded source selection must load");
        assert_eq!(
            selected
                .iter()
                .map(|source| source.turn_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "turn_before_activation",
                "turn_delayed_old_completion",
                "turn_selected"
            ]
        );
        let frozen_upper = selected.last().unwrap().id;

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
        Ok(())
    })
    .await
    .unwrap();
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
        learner_reasoning_effort: None,
        reviewer_provider: "openai".to_owned(),
        reviewer_model: "gpt-5.4".to_owned(),
        reviewer_reasoning_effort: None,
        pipeline_contract_version: "self-improvement-v1".to_owned(),
    }
}
