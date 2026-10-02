use super::*;
use crate::message::reconciliation_diagnostics::{Operation, Reporter};
use pioneer_entity::{task_run, task_run_occurrence_reconcile_pending as pending, turn};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};

const NOW: i64 = 4_000_000_000;

async fn fixture() -> (MessageProcessor, Arc<CrudStore>) {
    let (processor, store, workspace) =
        setup_execution_window_terminal_turn("tracker_thread", "tracker_0").await;
    let thread = processor
        .thread_manager
        .thread_get("tracker_thread")
        .await
        .unwrap();
    let (_, first) = store
        .get_turn("tracker_thread", "tracker_0")
        .await
        .unwrap()
        .unwrap();
    pioneer_entity::task::Entity::insert(pioneer_entity::task::ActiveModel {
        id: Set("tracker_task".into()),
        workspace_id: Set(workspace),
        owner_kind: Set("thread".into()),
        owner_id: Set(Some("tracker_thread".into())),
        executor_kind: Set("agent".into()),
        status: Set("running".into()),
        title: Set("Tracker".into()),
        goal: Set("Regression fixture".into()),
        ..Default::default()
    })
    .exec(&store.database_connection())
    .await
    .unwrap();
    for index in 0..66 {
        let id = format!("tracker_{index}");
        if index > 0 {
            let occurrence = Turn {
                id: id.clone(),
                turn_kind: TurnKind::TaskRun,
                ..first.clone()
            };
            store
                .materialize_turn_start(
                    &thread,
                    SandboxMode::FullAccess,
                    &occurrence,
                    &[],
                    pioneer_protocol::PersistedActorRef::System,
                )
                .await
                .unwrap();
        } else {
            turn::Entity::update_many()
                .col_expr(turn::Column::TurnKind, Expr::val("task_run"))
                .filter(turn::Column::Id.eq(&id))
                .exec(&store.database_connection())
                .await
                .unwrap();
        }
        task_run::Entity::insert(task_run::ActiveModel {
            id: Set(id.clone()),
            task_id: Set("tracker_task".into()),
            run_group_id: Set(id.clone()),
            attempt_number: Set(1),
            run_number: Set(index + 1),
            status: Set("succeeded".into()),
            executor_kind: Set("agent".into()),
            ..Default::default()
        })
        .exec(&store.database_connection())
        .await
        .unwrap();
    }
    // First generation is poison; the next 63 must still make progress.
    turn::Entity::update_many()
        .col_expr(turn::Column::PromptManifestJson, Expr::val("{"))
        .filter(turn::Column::Id.eq("tracker_0"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    (processor, store)
}

#[tokio::test]
async fn poison_candidate_does_not_abort_batch_and_all_selected_rows_consume_budget() {
    let (processor, store) = fixture().await;
    let result = processor
        .scoped_for_background_reconciliation()
        .reconcile_task_run_occurrences_at(NOW, 10000)
        .await
        .unwrap();
    assert_eq!(result.selected, 64);
    assert_eq!(result.claimed, 64);
    assert_eq!(result.changed, 63);
    assert_eq!(result.repair_errors, 1);
    assert!(result.first_error.is_some());
    let poison = pending::Entity::find_by_id("tracker_0")
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poison.attempt_count, 1);
    assert_eq!(poison.next_attempt_at, NOW + 5);
    let next = processor
        .reconcile_task_run_occurrences_at(NOW, 64)
        .await
        .unwrap();
    assert_eq!(
        next.selected, 2,
        "pass cannot replenish its original budget or retry whole batch"
    );
    assert_eq!(next.changed, 2);
    assert_eq!(
        pending::Entity::find_by_id("tracker_0")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap(),
        poison
    );
    turn::Entity::update_many()
        .col_expr(turn::Column::PromptManifestJson, Expr::val("{}"))
        .filter(turn::Column::Id.eq("tracker_0"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    let repaired = processor
        .reconcile_task_run_occurrences_at(NOW + 5, 64)
        .await
        .unwrap();
    assert_eq!(repaired.changed, 1);
    assert!(repaired.first_error.is_none());
}

#[tokio::test]
async fn partial_success_is_not_reported_as_recovery_and_diagnostics_are_allowlisted() {
    use sentry::SentryFutureExt;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    let (processor, _) = fixture().await;
    let transport = sentry::test::TestTransport::new();
    let options = sentry::ClientOptions::new()
        .dsn("https://public@sentry.invalid/1")
        .transport(transport.clone());
    let hub = Arc::new(sentry::Hub::new(
        Some(Arc::new(options.into())),
        Arc::new(Default::default()),
    ));
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(pioneer_observability::sentry_tracing_layer()),
    );
    async {
        let mut reporter = Reporter::new(Operation::TaskRunOccurrence);
        let result = processor.reconcile_task_run_occurrences_at(NOW, 64).await;
        assert_eq!(result.as_ref().unwrap().changed, 63);
        reporter.observe_occurrences(&result, std::time::Instant::now());
        tracing::error!("after partial occurrence pass");
    }
    .with_subscriber(dispatch)
    .bind_hub(hub)
    .await;
    let events = transport.fetch_and_clear_events();
    let diagnostics = events
        .iter()
        .filter(|e| e.message.as_deref() == Some("task parent occurrence reconciler failed"))
        .collect::<Vec<_>>();
    assert_eq!(diagnostics.len(), 1);
    let serialized = serde_json::to_string(&events).unwrap();
    assert!(!serialized.contains("tracker_0"));
    assert!(!serialized.contains("prompt_manifest"));
    assert!(!serialized.contains("reconciliation recovered after observed failures"));
}

#[tokio::test]
async fn failed_post_commit_notification_does_not_restore_pending_or_repeat_domain_change() {
    let (processor, store) = fixture().await;
    let candidate = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.run_id == "tracker_1")
        .unwrap();
    let claim = store
        .claim_task_run_occurrence_reconcile(&candidate, NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&claim, NOW)
            .await
            .unwrap(),
        pioneer_crud::TaskRunOccurrenceTerminalizationOutcome::Changed
    );
    assert!(
        pending::Entity::find_by_id("tracker_1")
            .one(&store.database_connection())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_turn("tracker_thread", "tracker_1")
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Completed
    );
    // Closing only this isolated fixture produces a real typed fanout lookup
    // failure after the repair transaction has already committed.
    store.database_connection().close().await.unwrap();
    assert!(
        processor
            .notify_task_run_occurrence_terminal("tracker_1")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn transient_retry_classifier_error_does_not_replenish_whole_batch_budget() {
    use sea_orm::ConnectionTrait;
    let (processor, store) = fixture().await;
    turn::Entity::update_many()
        .col_expr(turn::Column::PromptManifestJson, Expr::val("{}"))
        .filter(turn::Column::Id.eq("tracker_0"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    // A real SQLite rejection with a message accepted by the existing broad
    // transient-access retry classifier. It must stay one candidate error,
    // rather than rerunning discovery/reserving additional attempts in this pass.
    store.database_connection().execute_unprepared("CREATE TRIGGER reject_tracker_claim BEFORE UPDATE OF claim_token ON task_run_occurrence_reconcile_pending WHEN OLD.run_id='tracker_0' BEGIN SELECT RAISE(ABORT,'failed to acquire connection from pool'); END").await.unwrap();
    let result = processor
        .reconcile_task_run_occurrences_at(NOW, 64)
        .await
        .unwrap();
    assert!(pioneer_sqlite::is_anyhow_sqlite_transient_access(
        result.first_error.as_ref().unwrap()
    ));
    assert_eq!(result.selected, 64);
    assert_eq!(result.claim_errors, 1);
    assert_eq!(result.claimed, 63);
    assert_eq!(result.changed, 63);
    assert_eq!(
        pending::Entity::find_by_id("tracker_0")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap()
            .attempt_count,
        0
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_tracker_claim")
        .await
        .unwrap();
    let next = processor
        .reconcile_task_run_occurrences_at(NOW, 64)
        .await
        .unwrap();
    assert_eq!(next.selected, 3);
    assert_eq!(next.changed, 3);
}
