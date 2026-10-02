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
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
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
    assert_eq!(result.claim_deferrals, 1);
    assert_eq!(result.claimed, 63);
    assert_eq!(result.changed, 63);
    assert_eq!(
        pending::Entity::find_by_id("tracker_0")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap()
            .attempt_count,
        1
    );
    let other = processor
        .reconcile_task_run_occurrences_at(NOW, 64)
        .await
        .unwrap();
    assert_eq!(other.selected, 2);
    assert_eq!(other.changed, 2);
    for _ in 0..3 {
        let idle = processor
            .reconcile_task_run_occurrences_at(NOW + 4, 64)
            .await
            .unwrap();
        assert_eq!(idle.selected, 0);
        assert_eq!(idle.claim_errors, 0);
        assert_eq!(idle.pending, Some(true));
    }
    let refused_again = processor
        .reconcile_task_run_occurrences_at(NOW + 5, 64)
        .await
        .unwrap();
    assert_eq!(refused_again.selected, 1);
    assert_eq!(refused_again.claim_errors, 1);
    assert_eq!(refused_again.claim_deferrals, 1);
    assert_eq!(refused_again.changed, 0);
    let deferred = pending::Entity::find_by_id("tracker_0")
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deferred.attempt_count, 2);
    assert_eq!(deferred.next_attempt_at, NOW + 15);
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_tracker_claim")
        .await
        .unwrap();
    assert_eq!(
        processor
            .reconcile_task_run_occurrences_at(NOW + 14, 64)
            .await
            .unwrap()
            .selected,
        0
    );
    let next = processor
        .reconcile_task_run_occurrences_at(NOW + 15, 64)
        .await
        .unwrap();
    assert_eq!(next.selected, 1);
    assert_eq!(next.changed, 1);
    assert_eq!(next.pending, Some(false));
}

#[tokio::test]
async fn sixty_four_persistent_claim_refusals_defer_and_allow_candidates_after_budget_to_progress()
{
    use sea_orm::ConnectionTrait;
    let (processor, store) = fixture().await;
    turn::Entity::update_many()
        .col_expr(turn::Column::PromptManifestJson, Expr::val("{}"))
        .filter(turn::Column::Id.eq("tracker_0"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    // SQLite trigger DDL requires raw SQL. Refuse precisely the first 64
    // generations on token UPDATE; due/count CAS writes remain available.
    store.database_connection().execute_unprepared("CREATE TRIGGER reject_first_claims BEFORE UPDATE OF claim_token ON task_run_occurrence_reconcile_pending WHEN OLD.generation <= 64 BEGIN SELECT RAISE(ABORT,'individual claim refusal'); END").await.unwrap();
    let first = processor
        .reconcile_task_run_occurrences_at(NOW, 10000)
        .await
        .unwrap();
    assert_eq!(first.selected, 64);
    assert_eq!(first.claim_errors, 64);
    assert_eq!(first.claim_deferrals, 64);
    assert_eq!(first.claimed, 0);
    assert_eq!(first.changed, 0);
    let second = processor
        .reconcile_task_run_occurrences_at(NOW, 64)
        .await
        .unwrap();
    assert_eq!(second.selected, 2, "no fresh discovery inside first pass");
    assert_eq!(second.changed, 2);
    let deferred = pending::Entity::find()
        .all(&store.database_connection())
        .await
        .unwrap();
    assert_eq!(deferred.len(), 64);
    assert!(
        deferred.iter().all(|p| p.attempt_count == 1
            && p.next_attempt_at == NOW + 5
            && p.claim_token.is_none())
    );
    for _ in 0..3 {
        let idle = processor
            .reconcile_task_run_occurrences_at(NOW + 4, 64)
            .await
            .unwrap();
        assert_eq!(idle.selected, 0);
        assert_eq!(idle.claim_errors, 0);
        assert_eq!(idle.pending, Some(true));
    }
    // Still blocked at the next due time: persistent errors grow durable
    // backoff, and never prepare a domain repair without a successful claim.
    let repeat = processor
        .reconcile_task_run_occurrences_at(NOW + 5, 64)
        .await
        .unwrap();
    assert_eq!(repeat.selected, 64);
    assert_eq!(repeat.claim_deferrals, 64);
    assert_eq!(repeat.changed, 0);
    assert!(
        pending::Entity::find()
            .all(&store.database_connection())
            .await
            .unwrap()
            .iter()
            .all(|p| p.attempt_count == 2 && p.next_attempt_at == NOW + 15)
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_first_claims")
        .await
        .unwrap();
    assert_eq!(
        processor
            .reconcile_task_run_occurrences_at(NOW + 14, 64)
            .await
            .unwrap()
            .selected,
        0
    );
    let repaired = processor
        .reconcile_task_run_occurrences_at(NOW + 15, 64)
        .await
        .unwrap();
    assert_eq!(repaired.selected, 64);
    assert_eq!(repaired.changed, 64);
    assert_eq!(repaired.pending, Some(false));
}

#[tokio::test]
async fn long_pass_uses_each_claim_reservation_time_instead_of_batch_start() {
    use std::sync::atomic::{AtomicI64, Ordering};
    let (processor, store) = fixture().await;
    // Leave every selected repair unresolved so its actual reserved due time
    // remains observable. Non-significant JSON edits do not wake/reset tracking.
    turn::Entity::update_many()
        .col_expr(turn::Column::PromptManifestJson, Expr::val("{"))
        .filter(turn::Column::ThreadId.eq("tracker_thread"))
        .exec(&store.database_connection())
        .await
        .unwrap();
    let time = AtomicI64::new(NOW);
    let summary = processor
        .reconcile_task_run_occurrences_with_clock(&|| time.fetch_add(100, Ordering::SeqCst), 64)
        .await
        .unwrap();
    assert_eq!(summary.selected, 64);
    assert_eq!(summary.claimed, 64);
    assert_eq!(summary.repair_errors, 64);
    for index in 0..64 {
        let row = pending::Entity::find_by_id(format!("tracker_{index}"))
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        // discovery, then advisory check / admitted clock / repair clock for
        // every candidate. The admitted clock supplies the complete 5 seconds.
        assert_eq!(row.next_attempt_at, NOW + 200 + index * 300 + 5);
    }
}

#[tokio::test]
async fn occurrence_reporting_waits_through_idle_backoff_until_background_or_event_repair_resolves_queue()
 {
    use sentry::SentryFutureExt;
    use tracing::instrument::WithSubscriber;
    for event_driven in [false, true] {
        let (processor, store) = fixture().await;
        let (hub, transport, dispatch) = super::reconciliation_workers::local_capture();
        async {
            let mut occurrence = Reporter::new(Operation::TaskRunOccurrence);
            let mut native = Reporter::new(Operation::NativeFinalization);
            let at = std::time::Instant::now();
            let first = processor.reconcile_task_run_occurrences_at(NOW, 64).await;
            assert_eq!(first.as_ref().unwrap().repair_errors, 1);
            occurrence.observe_occurrences(&first, at);
            native.observe::<usize>(&Err(anyhow::anyhow!("native fixture failure")), at);
            native.observe(&Ok(0usize), at); // Independent semantics stay intact.
            tracing::error!("occurrence unresolved stage");
            let others = processor.reconcile_task_run_occurrences_at(NOW, 64).await;
            assert_eq!(others.as_ref().unwrap().changed, 2);
            occurrence.observe_occurrences(&others, at);
            tracing::error!("occurrence unresolved stage");
            for _ in 0..3 {
                let idle = processor
                    .reconcile_task_run_occurrences_at(NOW + 4, 64)
                    .await;
                let summary = idle.as_ref().unwrap();
                assert_eq!(summary.selected, 0);
                assert!(summary.first_error.is_none());
                assert_eq!(summary.pending, Some(true));
                occurrence.observe_occurrences(&idle, at);
                tracing::error!("occurrence unresolved stage");
            }
            turn::Entity::update_many()
                .col_expr(turn::Column::PromptManifestJson, Expr::val("{}"))
                .filter(turn::Column::Id.eq("tracker_0"))
                .exec(&store.database_connection())
                .await
                .unwrap();
            if event_driven {
                assert_eq!(
                    processor
                        .mark_task_run_occurrence_turn_terminal("tracker_0")
                        .await
                        .unwrap(),
                    pioneer_crud::TaskRunOccurrenceTerminalizationOutcome::Changed
                );
            }
            let resolved = processor
                .reconcile_task_run_occurrences_at(NOW + 5, 64)
                .await;
            assert_eq!(
                resolved.as_ref().unwrap().changed,
                usize::from(!event_driven)
            );
            assert_eq!(resolved.as_ref().unwrap().pending, Some(false));
            occurrence.observe_occurrences(&resolved, at);
            tracing::error!("occurrence resolved stage");
            let idle = processor
                .reconcile_task_run_occurrences_at(NOW + 5, 64)
                .await;
            occurrence.observe_occurrences(&idle, at);
            tracing::error!("occurrence resolved stage");
        }
        .with_subscriber(dispatch)
        .bind_hub(hub)
        .await;
        let events = transport.fetch_and_clear_events();
        assert_eq!(events.iter().filter(|e|e.message.as_deref()==Some("task parent occurrence reconciler failed")).count(),1,
            "idle passes do not imitate new failed attempts");
        for event in events
            .iter()
            .filter(|e| e.message.as_deref() == Some("occurrence unresolved stage"))
        {
            assert!(!event.breadcrumbs.iter().any(|b| b.message.as_deref()
                == Some("reconciliation recovered after observed failures")
                && b.data["operation"] == "task_run_occurrence"));
            assert!(event.breadcrumbs.iter().any(|b| b.message.as_deref()
                == Some("reconciliation recovered after observed failures")
                && b.data["operation"] == "native_turn_finalization"));
        }
        let resolved = events
            .iter()
            .filter(|e| e.message.as_deref() == Some("occurrence resolved stage"))
            .collect::<Vec<_>>();
        assert_eq!(resolved.len(), 2);
        for event in resolved {
            assert_eq!(
                event
                    .breadcrumbs
                    .iter()
                    .filter(|b| b.message.as_deref()
                        == Some("reconciliation recovered after observed failures")
                        && b.data["operation"] == "task_run_occurrence")
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn failed_pending_observation_and_unknown_state_never_report_occurrence_recovery() {
    use sentry::SentryFutureExt;
    use tracing::instrument::WithSubscriber;
    let (processor, store) = fixture().await;
    let (hub, transport, dispatch) = super::reconciliation_workers::local_capture();
    async {
        let mut reporter = Reporter::new(Operation::TaskRunOccurrence);
        let at = std::time::Instant::now();
        let first = processor.reconcile_task_run_occurrences_at(NOW, 64).await;
        reporter.observe_occurrences(&first, at);
        let mut summary = processor
            .reconcile_task_run_occurrences_at(NOW, 64)
            .await
            .unwrap();
        assert_eq!(summary.changed, 2);
        store.database_connection().close().await.unwrap();
        // Same summary path as the batch-end probe, with a real storage error.
        summary.record_pending_state(store.has_pending_task_run_occurrence_reconcile().await);
        assert_eq!(summary.pending, None);
        assert_eq!(summary.queue_state_errors, 1);
        assert!(summary.first_error.is_some());
        reporter.observe_occurrences(&Ok(summary), at);
        reporter.observe_occurrences(
            &Ok(crate::message::tasks::TaskRunOccurrenceReconcileSummary::default()),
            at,
        );
        tracing::error!("after unknown occurrence queue state");
    }
    .with_subscriber(dispatch)
    .bind_hub(hub)
    .await;
    let events = transport.fetch_and_clear_events();
    assert!(events.iter().all(|event| {
        event.breadcrumbs.iter().all(|b| {
            b.message.as_deref() != Some("reconciliation recovered after observed failures")
        })
    }));
}
