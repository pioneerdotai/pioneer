//! Regression code only. These tests must be run after implementation review.
use super::*;
use crate::TaskRunOccurrenceReconcileClaim;
use crate::repositories::task_run_occurrence_reconcile as queue;
use crate::task_run_occurrence::{Preparation, PreparedOccurrence};
use pioneer_entity::{
    task_run as runs, task_run_occurrence_reconcile_pending as pending,
    task_run_occurrence_reconcile_sequence as sequence, turn as turns,
};
use sea_orm::IntoActiveModel;
use sea_orm::sea_query::{ExprTrait, Query};

const NOW: i64 = 4_000_000_000;
const MIGRATION: &str = "m20261002_000001_task_run_occurrence_reconcile";

fn rollback_through_parent_migration() -> u32 {
    let migrations = Migrator::migrations();
    let position = migrations
        .iter()
        .position(|migration| migration.name() == MIGRATION)
        .expect("parent tracker migration must remain registered");
    u32::try_from(migrations.len() - position).unwrap()
}

async fn row(store: &CrudStore, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id.to_owned())
        .one(&store.connection)
        .await
        .unwrap()
}
async fn generation(store: &CrudStore) -> i64 {
    sequence::Entity::find_by_id(1)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap()
        .generation
}
async fn claimed(store: &CrudStore) -> TaskRunOccurrenceReconcileClaim {
    let candidates = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    store
        .claim_task_run_occurrence_reconcile(&candidates[0], &|| NOW)
        .await
        .unwrap()
        .unwrap()
}
async fn prepared(store: &CrudStore, id: &str) -> PreparedOccurrence {
    let Preparation::Ready(prepared) = store
        .with_maintenance_reads_and_critical_writes()
        .prepare_task_run_occurrence(id.to_owned(), NOW, true)
        .await
        .unwrap()
    else {
        panic!("expected mismatch")
    };
    *prepared
}
async fn run_status(store: &CrudStore, id: &str, status: &str) {
    runs::Entity::update_many()
        .col_expr(runs::Column::Status, Expr::val(status))
        .filter(runs::Column::Id.eq(id))
        .exec(&store.connection)
        .await
        .unwrap();
}
async fn turn_status(store: &CrudStore, id: &str, status: &str) {
    turns::Entity::update_many()
        .col_expr(turns::Column::Status, Expr::val(status))
        .filter(turns::Column::Id.eq(id))
        .exec(&store.connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn predicate_matrix_and_unknown_null_semantics_match_original_sql() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    for (run_status_value, target) in [
        ("succeeded", Some("completed")),
        ("failed", Some("failed")),
        ("timed_out", Some("failed")),
        ("blocked", Some("blocked")),
        ("cancelled", Some("interrupted")),
        ("running", None),
        ("unknown", None),
    ] {
        run_status(&store, &run.id, run_status_value).await;
        for status in [
            "in_progress",
            "completed",
            "failed",
            "blocked",
            "interrupted",
            "unknown",
        ] {
            turn_status(&store, &run.id, status).await;
            let expected = target.is_some_and(|target| target != status);
            assert_eq!(
                row(&store, &run.id).await.is_some(),
                expected,
                "{run_status_value}/{status}"
            );
            assert_eq!(
                queue::is_mismatch(&store.connection, &run.id)
                    .await
                    .unwrap(),
                expected
            );
        }
    }
    // Test the exact repository expression with nullable values, keeping all
    // production NOT NULL constraints installed.
    for (run, turn, expected) in [
        (None, Some("failed"), false),
        (Some("succeeded"), None, false),
        (Some("unknown"), Some("failed"), false),
        (Some("succeeded"), Some("unknown"), true),
        (Some("blocked"), Some("blocked"), false),
    ] {
        let query = Query::select()
            .expr(Expr::val(1))
            .and_where(queue::mismatch_status_predicate(
                Expr::val(run).into(),
                Expr::val(turn).into(),
            ))
            .to_owned();
        assert_eq!(
            store
                .connection
                .query_one_raw(DatabaseBackend::Sqlite.build(&query))
                .await
                .unwrap()
                .is_some(),
            expected
        );
    }
}

#[tokio::test]
async fn same_status_error_only_and_noncanonical_or_missing_pairs_do_not_enqueue() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    turn_status(&store, &run.id, "completed").await;
    runs::Entity::update_many()
        .col_expr(
            runs::Column::ErrorJson,
            Expr::val(Some("{\"message\":\"different\"}")),
        )
        .filter(runs::Column::Id.eq(&run.id))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_none());
    turns::Entity::update_many()
        .col_expr(turns::Column::Status, Expr::val("failed"))
        .col_expr(turns::Column::TurnKind, Expr::val("conversation"))
        .filter(turns::Column::Id.eq(&run.id))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_none());
    let (missing, _, run) = terminal_task_run_occurrence_fixture(None).await;
    assert!(row(&missing, &run.id).await.is_none());
}

#[tokio::test]
async fn both_late_inserts_deletes_and_id_moves_track_exact_current_pairs() {
    let (store, thread, run) = terminal_task_run_occurrence_fixture(None).await;
    let occurrence = Turn {
        turn_kind: TurnKind::TaskRun,
        ..sample_turn(&run.id)
    };
    // Exercise physical source DML on a valid Turn without event-history FK
    // dependents. A projected Turn's identity cannot be renamed independently
    // of those dependents; this fixture must not disable their constraints.
    crate::repositories::turn::upsert_turn(
        &store.connection,
        &run.id,
        &thread.id,
        &occurrence,
        None,
        None,
        unix_to_datetime(1_700_000_000),
        unix_to_datetime(1_700_000_000),
    )
    .await
    .unwrap();
    let first = row(&store, &run.id).await.unwrap();
    let saved = runs::Entity::find_by_id(run.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    runs::Entity::delete_by_id(run.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_none());
    runs::Entity::insert(saved.into_active_model())
        .exec(&store.connection)
        .await
        .unwrap();
    let second = row(&store, &run.id).await.unwrap();
    assert!(
        second.generation > first.generation,
        "delete/reinsert must exclude ABA"
    );
    for (old, new) in [
        (run.id.as_str(), "moved_run"),
        ("moved_run", run.id.as_str()),
    ] {
        runs::Entity::update_many()
            .col_expr(runs::Column::Id, Expr::val(new))
            .filter(runs::Column::Id.eq(old))
            .exec(&store.connection)
            .await
            .unwrap();
        assert!(row(&store, "moved_run").await.is_none());
    }
    let third = row(&store, &run.id).await.unwrap();
    assert!(third.generation > second.generation);
    for (old, new) in [
        (run.id.as_str(), "moved_turn"),
        ("moved_turn", run.id.as_str()),
    ] {
        // INSERT always creates compaction bookkeeping with a non-cascading
        // identity FK. Move that dependent atomically with this physical DML
        // fixture, preserving its sequence and enforcing every FK throughout.
        use pioneer_entity::compaction_turn_creation as creation;
        let tx = store.connection.begin().await.unwrap();
        let mut dependent = creation::Entity::find()
            .filter(creation::Column::TurnId.eq(old))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        creation::Entity::delete_by_id(dependent.sequence)
            .exec(&tx)
            .await
            .unwrap();
        turns::Entity::update_many()
            .col_expr(turns::Column::Id, Expr::val(new))
            .filter(turns::Column::Id.eq(old))
            .exec(&tx)
            .await
            .unwrap();
        dependent.turn_id = new.to_owned();
        creation::Entity::insert(dependent.into_active_model())
            .exec(&tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(row(&store, "moved_turn").await.is_none());
        if new == "moved_turn" {
            assert!(row(&store, &run.id).await.is_none());
        }
    }
    assert!(row(&store, &run.id).await.unwrap().generation > third.generation);
    turns::Entity::delete_by_id(run.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_none());
}

#[tokio::test]
async fn trigger_updates_preserve_retry_and_ignore_noops_heartbeat_updated_at() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let claim = claimed(&store).await;
    let before = row(&store, &run.id).await.unwrap();
    runs::Entity::update_many()
        .col_expr(runs::Column::Status, Expr::val("succeeded"))
        .col_expr(
            runs::Column::HeartbeatAt,
            Expr::val(Some(unix_to_datetime(NOW))),
        )
        .col_expr(runs::Column::UpdatedAt, Expr::val(unix_to_datetime(NOW)))
        .filter(runs::Column::Id.eq(&run.id))
        .exec(&store.connection)
        .await
        .unwrap();
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
    runs::Entity::update_many()
        .col_expr(
            runs::Column::CompletedAt,
            Expr::val(Some(unix_to_datetime(NOW - 1))),
        )
        .filter(runs::Column::Id.eq(&run.id))
        .exec(&store.connection)
        .await
        .unwrap();
    let after = row(&store, &run.id).await.unwrap();
    assert!(after.generation > before.generation);
    assert_eq!(after.next_attempt_at, claim.next_attempt_at);
    assert_eq!(after.attempt_count, claim.attempt_count);
    assert!(after.claim_token.is_none());
    assert!(
        store
            .discover_task_run_occurrence_reconcile(NOW, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&claim, NOW)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::StaleClaim
    );
}

#[tokio::test]
async fn one_holder_at_a_time_reclaim_new_token_and_old_holder_cannot_commit() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let candidate = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    let (a, b) = tokio::join!(
        store.claim_task_run_occurrence_reconcile(&candidate, &|| NOW),
        store.claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(usize::from(a.is_some()) + usize::from(b.is_some()), 1);
    let first = a.or(b).unwrap();
    let old_preparation = prepared(&store, &run.id).await;
    assert_eq!(first.next_attempt_at, NOW + 5);
    assert!(
        store
            .claim_task_run_occurrence_reconcile(&candidate, &|| NOW + 4)
            .await
            .unwrap()
            .is_none()
    );
    let second = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW + 5)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.claim_token, second.claim_token);
    assert_eq!(first.generation, second.generation);
    assert_eq!(second.next_attempt_at, NOW + 15);
    assert_eq!(
        store
            .with_maintenance_reads_and_critical_writes()
            .commit_task_run_occurrence(old_preparation, Some(first))
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::StaleClaim
    );
    assert_eq!(
        row(&store, &run.id).await.unwrap().claim_token.as_deref(),
        Some(second.claim_token.as_str())
    );
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&second, NOW + 5)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::Changed
    );
    assert!(row(&store, &run.id).await.is_none());
}

#[tokio::test]
async fn run_and_turn_freshness_ignores_equal_timestamps_and_checks_all_used_fields() {
    for change in [
        "completed_at",
        "error_json",
        "turn_error",
        "turn_prompt",
        "thread_id",
        "thread_workspace",
        "resume",
    ] {
        let (store, thread, run) =
            terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
        let claim = claimed(&store).await;
        let prepared = prepared(&store, &run.id).await;
        let run_time = runs::Entity::find_by_id(run.id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .updated_at;
        let turn_time = turns::Entity::find_by_id(run.id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .updated_at;
        match change {
            "completed_at" => {
                runs::Entity::update_many()
                    .col_expr(
                        runs::Column::CompletedAt,
                        Expr::val(Some(unix_to_datetime(NOW))),
                    )
                    .filter(runs::Column::Id.eq(&run.id))
                    .exec(&store.connection)
                    .await
                    .unwrap();
            }
            "error_json" => {
                runs::Entity::update_many()
                    .col_expr(runs::Column::ErrorJson, Expr::val(Some("{}")))
                    .filter(runs::Column::Id.eq(&run.id))
                    .exec(&store.connection)
                    .await
                    .unwrap();
            }
            "turn_error" => {
                turns::Entity::update_many()
                    .col_expr(turns::Column::Error, Expr::val(Some("changed")))
                    .filter(turns::Column::Id.eq(&run.id))
                    .exec(&store.connection)
                    .await
                    .unwrap();
            }
            "turn_prompt" => {
                turns::Entity::update_many()
                    .col_expr(
                        turns::Column::PromptManifestJson,
                        Expr::val("{\"changed\":true}"),
                    )
                    .filter(turns::Column::Id.eq(&run.id))
                    .exec(&store.connection)
                    .await
                    .unwrap();
            }
            "thread_id" => {
                turns::Entity::update_many()
                    .col_expr(turns::Column::ThreadId, Expr::val("other_thread"))
                    .filter(turns::Column::Id.eq(&run.id))
                    .exec(&store.connection)
                    .await
                    .unwrap();
            }
            "thread_workspace" => {
                pioneer_entity::thread::Entity::update_many()
                    .col_expr(
                        pioneer_entity::thread::Column::WorkspaceId,
                        Expr::val("other_workspace"),
                    )
                    .filter(pioneer_entity::thread::Column::Id.eq(&thread.id))
                    .exec(&store.connection)
                    .await
                    .unwrap();
            }
            _ => run_status(&store, &run.id, "running").await,
        }
        assert_eq!(
            runs::Entity::find_by_id(run.id.clone())
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .updated_at,
            run_time
        );
        assert_eq!(
            turns::Entity::find_by_id(run.id.clone())
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .updated_at,
            turn_time
        );
        assert_eq!(
            store
                .with_maintenance_reads_and_critical_writes()
                .commit_task_run_occurrence(prepared, Some(claim))
                .await
                .unwrap(),
            TaskRunOccurrenceTerminalizationOutcome::StaleClaim,
            "{change}"
        );
        assert_eq!(
            turns::Entity::find_by_id(run.id.clone())
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .status,
            "in_progress"
        );
    }
}

#[tokio::test]
async fn source_rollback_and_projection_failure_roll_back_tracker_and_event() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    turn_status(&store, &run.id, "completed").await;
    let seq = generation(&store).await;
    let tx = store.connection.begin().await.unwrap();
    runs::Entity::update_many()
        .col_expr(runs::Column::Status, Expr::val("failed"))
        .filter(runs::Column::Id.eq(&run.id))
        .exec(&tx)
        .await
        .unwrap();
    assert!(
        pending::Entity::find_by_id(run.id.clone())
            .one(&tx)
            .await
            .unwrap()
            .is_some()
    );
    tx.rollback().await.unwrap();
    assert!(row(&store, &run.id).await.is_none());
    assert_eq!(generation(&store).await, seq);
    turn_status(&store, &run.id, "in_progress").await;
    let claim = claimed(&store).await;
    let before = row(&store, &run.id).await.unwrap();
    let event_count = pioneer_entity::turn_event::Entity::find()
        .count(&store.connection)
        .await
        .unwrap();
    // Test-only SQLite trigger: status history is written after the Turn upsert,
    // hence failure rolls back its status and the tracker trigger's deletion.
    store.connection.execute_unprepared("CREATE TRIGGER reject_occurrence_history BEFORE INSERT ON turn_status_history BEGIN SELECT RAISE(ABORT,'fixture projection blocker'); END").await.unwrap();
    assert!(
        store
            .reconcile_claimed_task_run_occurrence(&claim, NOW)
            .await
            .is_err()
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        event_count
    );
    assert_eq!(
        turns::Entity::find_by_id(run.id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .status,
        "in_progress"
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER reject_occurrence_history")
        .await
        .unwrap();
    let next = store
        .claim_task_run_occurrence_reconcile(
            &TaskRunOccurrenceReconcileCandidate {
                run_id: run.id.clone(),
                generation: claim.generation,
            },
            &|| claim.next_attempt_at,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&next, next.next_attempt_at - 10)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::Changed
    );
    assert!(row(&store, &run.id).await.is_none());
}

#[tokio::test]
async fn poison_missing_thread_keeps_backoff_and_allows_later_repair() {
    let (store, thread, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    turns::Entity::update_many()
        .col_expr(turns::Column::ThreadId, Expr::val("missing_thread"))
        .filter(turns::Column::Id.eq(&run.id))
        .exec(&store.connection)
        .await
        .unwrap();
    let claim = claimed(&store).await;
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&claim, NOW)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::NotFound
    );
    assert_eq!(
        row(&store, &run.id).await.unwrap().next_attempt_at,
        claim.next_attempt_at
    );
    assert!(
        store
            .discover_task_run_occurrence_reconcile(NOW, 64)
            .await
            .unwrap()
            .is_empty()
    );
    // Repair a dependency without any new source-pair UPDATE. Pending must
    // remain retryable even though neither source trigger can wake it.
    let restored_thread = Thread {
        id: "missing_thread".to_owned(),
        ..thread
    };
    store
        .upsert_thread_model(&restored_thread, PersistedActorRef::System)
        .await
        .unwrap();
    let current = row(&store, &run.id).await.unwrap();
    assert_eq!(current.next_attempt_at, claim.next_attempt_at);
    assert_eq!(current.generation, claim.generation);
    let next = store
        .claim_task_run_occurrence_reconcile(
            &TaskRunOccurrenceReconcileCandidate {
                run_id: run.id.clone(),
                generation: current.generation,
            },
            &|| claim.next_attempt_at,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&next, claim.next_attempt_at)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::Changed
    );
}

#[tokio::test]
async fn ordinary_event_driven_repair_can_overtake_claimed_background_work() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let claim = claimed(&store).await;
    let prepared = prepared(&store, &run.id).await;
    assert_eq!(
        store
            .compare_and_materialize_task_run_occurrence_terminal(&run.id, NOW)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::Changed
    );
    assert_eq!(
        store
            .with_maintenance_reads_and_critical_writes()
            .commit_task_run_occurrence(prepared, Some(claim))
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::StaleClaim
    );
    assert!(row(&store, &run.id).await.is_none());
}

#[tokio::test]
async fn migration_accepts_history_tracks_later_old_updates_and_down_removes_objects() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let maintenance = store.with_maintenance_access();
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::down(&*tx, Some(rollback_through_parent_migration()))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        row(&store, &run.id).await.is_none(),
        "no history backfill on install"
    );
    assert_eq!(generation(&store).await, 0);
    // A same-value source update is not a new discrepancy; a later real update
    // of this pre-install ID is tracked regardless of its creation timestamp.
    run_status(&store, &run.id, "succeeded").await;
    assert!(row(&store, &run.id).await.is_none());
    runs::Entity::update_many()
        .col_expr(
            runs::Column::CompletedAt,
            Expr::val(Some(unix_to_datetime(NOW))),
        )
        .filter(runs::Column::Id.eq(&run.id))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_some());
    assert_eq!(
        pending::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        1
    );
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::down(&*tx, Some(rollback_through_parent_migration()))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let objects = store
        .connection
        .query_all_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE name LIKE ?",
            ["task_run_occurrence_reconcile%".into()],
        ))
        .await
        .unwrap();
    assert!(objects.is_empty());
    let indexes = store
        .connection
        .query_all_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE name=?",
            ["idx_task_run_occurrence_reconcile_due".into()],
        ))
        .await
        .unwrap();
    assert!(indexes.is_empty());
    assert!(
        Migrator::migrations()
            .iter()
            .any(|migration| migration.name() == MIGRATION)
    );
}

#[tokio::test]
async fn generation_overflow_and_missing_singleton_reject_source_write_atomically() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    sequence::Entity::update_many()
        .col_expr(sequence::Column::Generation, Expr::val(i64::MAX))
        .exec(&store.connection)
        .await
        .unwrap();
    let before = row(&store, &run.id).await.unwrap();
    assert!(
        runs::Entity::update_many()
            .col_expr(
                runs::Column::CompletedAt,
                Expr::val(Some(unix_to_datetime(NOW)))
            )
            .filter(runs::Column::Id.eq(&run.id))
            .exec(&store.connection)
            .await
            .is_err()
    );
    assert_eq!(generation(&store).await, i64::MAX);
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
    sequence::Entity::delete_by_id(1)
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(
        turns::Entity::update_many()
            .col_expr(turns::Column::Status, Expr::val("completed"))
            .filter(turns::Column::Id.eq(&run.id))
            .exec(&store.connection)
            .await
            .is_err()
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
}

#[tokio::test]
async fn due_plan_empty_deferred_and_due_backlog_use_covering_index_without_sort() {
    let store = test_store_with_workspace("ws_task").await;
    for phase in ["empty", "deferred", "backlog"] {
        if phase != "empty" {
            // Service rows are fixture data, never a scan of domain history.
            for chunk in 0..32 {
                let values = (0..128)
                    .map(|offset| {
                        let id = chunk * 128 + offset;
                        pending::ActiveModel {
                            run_id: Set(format!("plan_{id:05}")),
                            generation: Set(id + 1),
                            next_attempt_at: Set(if phase == "deferred" {
                                NOW + 100
                            } else {
                                NOW - 100
                            }),
                            attempt_count: Set(0),
                            claim_token: Set(None),
                        }
                    })
                    .collect::<Vec<_>>();
                if phase == "deferred" {
                    pending::Entity::insert_many(values)
                        .exec(&store.connection)
                        .await
                        .unwrap();
                }
            }
            if phase == "backlog" {
                pending::Entity::update_many()
                    .col_expr(pending::Column::NextAttemptAt, Expr::val(NOW - 100))
                    .exec(&store.connection)
                    .await
                    .unwrap();
            }
        }
        let statement = DatabaseBackend::Sqlite.build(&queue::due_query(NOW, 64));
        assert!(!statement.sql.contains("task_run "));
        assert!(!statement.sql.contains("JOIN"));
        // EXPLAIN QUERY PLAN has no SeaQuery builder; preserve builder values.
        let explain = Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!("EXPLAIN QUERY PLAN {}", statement.sql),
            statement.values.unwrap().0,
        );
        let plan = store.connection.query_all_raw(explain).await.unwrap();
        let details = plan
            .into_iter()
            .map(|r| r.try_get::<String>("", "detail").unwrap())
            .collect::<Vec<_>>();
        assert!(
            details
                .iter()
                .any(|d| d.contains("COVERING INDEX idx_task_run_occurrence_reconcile_due")),
            "{phase}: {details:?}"
        );
        assert!(
            details.iter().all(|d| !d.contains("TEMP B-TREE")),
            "{phase}: {details:?}"
        );
        assert_eq!(
            store
                .discover_task_run_occurrence_reconcile(NOW, 10000)
                .await
                .unwrap()
                .len(),
            if phase == "backlog" { 64 } else { 0 }
        );
    }
}

#[test]
fn backoff_is_exponential_bounded_and_attempt_counter_has_finite_policy() {
    assert_eq!(
        (1..=8).map(queue::retry_delay).collect::<Vec<_>>(),
        vec![5, 10, 20, 40, 80, 160, 300, 300]
    );
    assert_eq!(queue::retry_delay(16), 300);
    assert_eq!(queue::retry_delay(i64::MAX), 300);
}

#[tokio::test]
async fn every_terminal_mapping_commits_through_common_turn_event_projector() {
    for (status, target) in [
        ("succeeded", TurnStatus::Completed),
        ("failed", TurnStatus::Failed),
        ("timed_out", TurnStatus::Failed),
        ("blocked", TurnStatus::Blocked),
        ("cancelled", TurnStatus::Interrupted),
    ] {
        let (store, thread, run) =
            terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
        run_status(&store, &run.id, status).await;
        // A false already-terminal status is repaired too.
        turn_status(
            &store,
            &run.id,
            if target == TurnStatus::Completed {
                "failed"
            } else {
                "completed"
            },
        )
        .await;
        let claim = claimed(&store).await;
        assert_eq!(
            store
                .reconcile_claimed_task_run_occurrence(&claim, NOW)
                .await
                .unwrap(),
            TaskRunOccurrenceTerminalizationOutcome::Changed
        );
        assert_eq!(
            store
                .get_turn(&thread.id, &run.id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            target
        );
        assert!(row(&store, &run.id).await.is_none());
    }
}

async fn running_occurrence_fixture() -> (CrudStore, Thread, TaskRun) {
    use pioneer_protocol::{TaskActorContract, TaskDeliveryActorContract, TaskReviewerIntent};
    let store = test_store_with_workspace("ws_task").await;
    let thread = sample_thread("ws_task", "thr_task", 1_700_000_000);
    let task = sample_task(1_700_000_000);
    let mut run = sample_task_run(1_700_000_000);
    run.trigger_id = None;
    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: None,
                },
                TaskEventPayload::RunStarted {
                    task_id: task.id.clone(),
                    run_id: run.id.clone(),
                    started_at: 1_700_000_001,
                },
            ],
            1_700_000_000,
        )
        .await
        .unwrap();
    store
        .upsert_task_actor_contract(
            &TaskActorContract {
                task_id: task.id.clone(),
                workspace_id: "ws_task".into(),
                creator: PersistedActorRef::System,
                creator_presentation_snapshot: None,
                reviewer: TaskReviewerIntent::RuntimeAuto,
                execution_destination_thread_id: None,
                execution_route_id: None,
                execution_route_receipt_json: None,
                execution_route_expires_at_millis: None,
                delivery: TaskDeliveryActorContract {
                    enabled: false,
                    destination_thread_id: None,
                    destination_user_id: None,
                    destination_webhook_url_fingerprint: None,
                    route_id: None,
                    return_route_id: None,
                    author_snapshot: None,
                    route_receipt_json: None,
                    disclosure_generation: 1,
                    route_expires_at_millis: None,
                },
                launch: None,
                requested_identity_json: None,
                resolved_identity_id: None,
                resolved_profile_id: None,
                source_config_fingerprint: None,
                derived_child_launch_grant_json: None,
                creator_work_graph_root_execution_id: None,
                work_graph_root_execution_id: None,
                root_resource_scope_id: None,
                accounting_attribution: None,
                controller_principal_id: None,
                revision: 1,
            },
            1_700_000_001,
        )
        .await
        .unwrap();
    store
        .upsert_task_occurrence_contract(
            &TaskOccurrenceContract {
                occurrence_id: run.id.clone(),
                task_id: task.id,
                run_id: run.id.clone(),
                trigger_id: None,
                occurrence_key: format!("immediate:{}", run.id),
                execution_generation: 1,
                agent_execution_id: None,
                work_graph_root_execution_id: None,
                root_resource_scope_id: None,
                status: TaskOccurrenceStatus::Running,
                queue_position: None,
                retry_attempt: 0,
                action_idempotency_key: format!("task:{}", run.id),
                route_id: None,
                result_return_route_id: None,
                delivery_plan: None,
                terminal_reason: None,
            },
            1_700_000_001,
        )
        .await
        .unwrap();
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &Turn {
                turn_kind: TurnKind::TaskRun,
                ..sample_turn(&run.id)
            },
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    (store, thread, run)
}

#[tokio::test]
async fn terminal_commit_without_fanout_and_generic_recovery_replay_change_tracker() {
    let (store, thread, run) = running_occurrence_fixture().await;
    assert!(row(&store, &run.id).await.is_none());
    // Use the real prepared terminal-domain commit, without Gateway fanout.
    let prepared = store
        .prepare_task_terminal_transition(TaskEventPayload::RunCompleted {
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            result: None,
            completed_at: 1_700_000_003,
        })
        .await
        .unwrap();
    store
        .commit_task_terminal_transition(prepared)
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_some());
    let claim = claimed(&store).await;
    store
        .reconcile_claimed_task_run_occurrence(&claim, NOW)
        .await
        .unwrap();
    // Direct store recovery with the same timestamp is still tracked.
    store
        .update_turn_status(
            &thread.id,
            &run.id,
            TurnStatus::Interrupted,
            Some("recovery"),
            1_700_000_002,
        )
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_some());
    let failed = Turn {
        status: TurnStatus::Failed,
        turn_kind: TurnKind::TaskRun,
        ..sample_turn(&run.id)
    };
    let event = CanonicalTurnEventPayload::TurnFailed(TurnFailedNotification {
        workspace_id: thread.workspace_id.clone(),
        thread_id: thread.id.clone(),
        turn: failed,
    });
    // Real envelope/projection APIs; make its receipt due again to model replay
    // of an old Turn after a newer generic write restored its status.
    store
        .materialize_native_agent_turn_event(event, 1_700_000_004, None)
        .await
        .unwrap();
    let receipt = pioneer_entity::turn_event_projection_state::Entity::find()
        .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(&run.id))
        .order_by_desc(pioneer_entity::turn_event_projection_state::Column::Sequence)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    store
        .update_turn_status(
            &thread.id,
            &run.id,
            TurnStatus::Completed,
            None,
            1_700_000_004,
        )
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_none());
    let p = pioneer_entity::turn_event_projection_state::Entity::update_many()
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::Status,
            Expr::val("failed"),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::NextRunAt,
            Expr::val(unix_to_datetime(NOW)),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::ClaimToken,
            Expr::val(None::<String>),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::ClaimExpiresAt,
            Expr::val(None::<sea_orm::entity::prelude::DateTimeWithTimeZone>),
        )
        .filter(pioneer_entity::turn_event_projection_state::Column::EventId.eq(&receipt.event_id));
    p.exec(&store.connection).await.unwrap();
    set_turn_projection_watermark(&store, &run.id, receipt.sequence - 1).await;
    assert_eq!(
        store
            .replay_due_turn_event_projections(NOW, 64)
            .await
            .unwrap()
            .projected,
        1
    );
    assert!(row(&store, &run.id).await.is_some());
}

#[tokio::test]
async fn resume_of_old_blocked_run_invalidates_claim_then_subsequent_completion_tracks_again() {
    let (store, task, run, job, child_thread, child_turn) =
        task_owned_resume_conflict_fixture("ws_task", "tracker_resume").await;
    let parent = sample_thread("ws_task", "tracker_parent", 1_700_011_000);
    let canonical = Turn {
        turn_kind: TurnKind::TaskRun,
        origin: TurnOrigin::ScheduledTask,
        ..sample_turn(&run.id)
    };
    store
        .materialize_turn_start(
            &parent,
            SandboxMode::FullAccess,
            &canonical,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let claim = claimed(&store).await;
    let outcome = store
        .resume_task_owned_turn(
            &child_thread,
            &child_turn,
            Some(&job.id),
            1_700_011_100,
            "resume_owner",
            1_700_011_200,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome,
        TaskOwnedTurnResumeOutcome::Resumed { .. }
    ));
    assert!(row(&store, &run.id).await.is_none());
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&claim, NOW)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::StaleClaim
    );
    store
        .append_task_event(
            TaskEventPayload::RunCompleted {
                task_id: task.id,
                run_id: run.id.clone(),
                result: None,
                completed_at: 1_700_011_300,
            },
            1_700_011_300,
        )
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.unwrap().generation > claim.generation);
}

#[derive(Default)]
struct Routes {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadClass>>,
    writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    maintenance_queued: tokio::sync::Notify,
    watch_claim: std::sync::atomic::AtomicBool,
    critical_queued: tokio::sync::Notify,
    watch_repair: std::sync::atomic::AtomicBool,
}
impl pioneer_sqlite::SqliteReadObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        if let pioneer_sqlite::SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl pioneer_sqlite::SqliteWriteObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
        if self.watch_claim.load(Ordering::SeqCst)
            && matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Enqueued {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                }
            )
        {
            self.maintenance_queued.notify_one();
        }
        if self.watch_repair.load(Ordering::SeqCst)
            && matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Enqueued {
                    class: pioneer_sqlite::SqliteWriteClass::Critical,
                    ..
                }
            )
        {
            self.critical_queued.notify_one();
        }
    }
}
async fn disk_store(path: &std::path::Path, routes: Arc<Routes>) -> CrudStore {
    let mut options = sea_orm::ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| o.pragma("journal_mode", "WAL"));
    let writer = Database::connect(options).await.unwrap();
    let executor = pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, routes.clone());
    // Follow startup ordering: migrate through the executor before opening the
    // physical read-only pool and before any source recovery or replay.
    executor
        .run_migrations::<Migrator>(pioneer_sqlite::SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let mut options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(path));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(options).await.unwrap();
    CrudStore::new(
        pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(reader, executor, routes),
    )
}
async fn populate_disk_pair(store: &CrudStore) -> String {
    let timestamp = unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_task".into()),
        name: Set("Tracker".into()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
    })
    .exec(&store.connection)
    .await
    .unwrap();
    let task = sample_task(1_700_000_000);
    let mut run = sample_task_run(1_700_000_000);
    run.trigger_id = None;
    let parent = sample_thread("ws_task", "tracker_disk_parent", 1_700_000_000);
    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: None,
                },
                TaskEventPayload::RunCompleted {
                    task_id: task.id,
                    run_id: run.id.clone(),
                    result: None,
                    completed_at: 1_700_000_001,
                },
            ],
            1_700_000_000,
        )
        .await
        .unwrap();
    store
        .materialize_turn_start(
            &parent,
            SandboxMode::FullAccess,
            &Turn {
                turn_kind: TurnKind::TaskRun,
                ..sample_turn(&run.id)
            },
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    run.id
}
async fn remove_disk_fixture(store: CrudStore, path: std::path::PathBuf) {
    store.connection.close().await.unwrap();
    std::fs::remove_file(&path).unwrap();
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn reopen_retains_pending_generation_claim_and_backoff_without_completed_log() {
    let path = std::env::temp_dir().join(format!(
        "pioneer-occurrence-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let store = disk_store(&path, Arc::new(Routes::default())).await;
    let id = populate_disk_pair(&store).await;
    let claim = claimed(&store).await;
    let before = row(&store, &id).await.unwrap();
    let seq = generation(&store).await;
    store.connection.close().await.unwrap();
    let store = disk_store(&path, Arc::new(Routes::default())).await;
    assert_eq!(row(&store, &id).await.unwrap(), before);
    assert_eq!(generation(&store).await, seq);
    assert!(
        store
            .discover_task_run_occurrence_reconcile(NOW, 64)
            .await
            .unwrap()
            .is_empty()
    );
    let candidate = store
        .discover_task_run_occurrence_reconcile(claim.next_attempt_at, 64)
        .await
        .unwrap()
        .remove(0);
    let next = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| claim.next_attempt_at)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&next, claim.next_attempt_at)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::Changed
    );
    assert!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        generation(&store).await,
        seq,
        "sequence survives draining the set"
    );
    remove_disk_fixture(store, path).await;
}

#[tokio::test]
async fn scheduling_routes_cancellation_and_interactive_reads_use_existing_executor() {
    use pioneer_sqlite::{SqliteReadClass, SqliteWriteClass, SqliteWriteEvent};
    use std::time::Duration;
    let path = std::env::temp_dir().join(format!(
        "pioneer-occurrence-routing-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let routes = Arc::new(Routes::default());
    let store = disk_store(&path, routes.clone()).await;
    let id = populate_disk_pair(&store).await;
    routes.reads.lock().unwrap().clear();
    routes.writes.lock().unwrap().clear();
    let candidate = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    assert!(
        routes
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|c| *c == SqliteReadClass::Maintenance)
    );
    assert!(
        routes.writes.lock().unwrap().is_empty(),
        "discovery does not reserve writer"
    );
    let hold = store.connection.begin().await.unwrap();
    let before = row(&store, &id).await.unwrap();
    routes.watch_claim.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let c = candidate.clone();
    let waiting = tokio::spawn(async move {
        worker
            .claim_task_run_occurrence_reconcile(&c, &|| NOW)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.maintenance_queued.notified())
        .await
        .unwrap();
    assert!(
        store.get_task_run(&id).await.unwrap().is_some(),
        "interactive reader remains available"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    assert_eq!(
        row(&store, &id).await.unwrap(),
        before,
        "cancelled queued claim never changed durable attempt"
    );
    assert!(routes.writes.lock().unwrap().iter().any(|e|matches!(e,SqliteWriteEvent::Cancelled {class:SqliteWriteClass::Maintenance,queue,..} if queue.maintenance==0)));
    routes.reads.lock().unwrap().clear();
    routes.writes.lock().unwrap().clear();
    let claim = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
        .await
        .unwrap()
        .unwrap();
    assert!(
        routes
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|c| *c == SqliteReadClass::Maintenance)
    );
    assert!(routes.writes.lock().unwrap().iter().all(|e| !matches!(e,SqliteWriteEvent::Enqueued {class,..} if *class!=SqliteWriteClass::Maintenance)));
    routes.reads.lock().unwrap().clear();
    routes.writes.lock().unwrap().clear();
    let events_before = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(&id))
        .all(&store.connection)
        .await
        .unwrap();
    let turn_before = turns::Entity::find_by_id(&id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    let pending_before = row(&store, &id).await.unwrap();
    let hold = store
        .with_maintenance_access()
        .connection
        .begin()
        .await
        .unwrap();
    routes.watch_repair.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let attempt = claim.clone();
    let cancellation = tokio::spawn(async move {
        worker
            .reconcile_claimed_task_run_occurrence(&attempt, NOW)
            .await
    });
    // Critical admission occurs only after actual event preparation completes.
    tokio::time::timeout(Duration::from_secs(5), routes.critical_queued.notified())
        .await
        .unwrap();
    assert!(store.get_task_run(&id).await.unwrap().is_some());
    cancellation.abort();
    assert!(cancellation.await.unwrap_err().is_cancelled());
    assert!(routes.writes.lock().unwrap().iter().any(|e| matches!(e,
        SqliteWriteEvent::Cancelled {class:SqliteWriteClass::Critical,queue,..} if queue.critical == 0)));
    hold.rollback().await.unwrap();
    assert_eq!(
        turns::Entity::find_by_id(&id)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap(),
        turn_before
    );
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(&id))
            .all(&store.connection)
            .await
            .unwrap(),
        events_before
    );
    assert_eq!(row(&store, &id).await.unwrap(), pending_before);
    assert!(
        store
            .discover_task_run_occurrence_reconcile(NOW, 64)
            .await
            .unwrap()
            .is_empty()
    );
    let next = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| claim.next_attempt_at)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(next.claim_token, claim.claim_token);
    routes.reads.lock().unwrap().clear();
    routes.writes.lock().unwrap().clear();
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&next, claim.next_attempt_at)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::Changed
    );
    assert!(
        routes
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|c| *c == SqliteReadClass::Maintenance)
    );
    assert!(routes.writes.lock().unwrap().iter().any(|e| matches!(
        e,
        SqliteWriteEvent::Enqueued {
            class: SqliteWriteClass::Critical,
            ..
        }
    )));
    assert!(!routes.writes.lock().unwrap().iter().any(|e| matches!(
        e,
        SqliteWriteEvent::Enqueued {
            class: SqliteWriteClass::Interactive,
            ..
        }
    )));
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
    // Mutating RETURNING must route to the writer, even through query_one_raw.
    store.with_maintenance_access().connection.query_one_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE task_run_occurrence_reconcile_sequence SET generation=generation WHERE singleton=? RETURNING generation",[1.into()])).await.unwrap().unwrap();
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
    remove_disk_fixture(store, path).await;
}

#[tokio::test]
async fn migration_installation_and_completion_marker_rollback_together() {
    let store = test_store_with_workspace("ws_task")
        .await
        .with_maintenance_access();
    let tx = store.connection.begin().await.unwrap();
    Migrator::down(&*tx, Some(rollback_through_parent_migration()))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // Deliberately collide with the second installed trigger, after the new
    // tables/index/seed and first trigger have already been written.
    store.connection.execute_unprepared("CREATE TRIGGER task_run_occurrence_reconcile_task_run_update AFTER UPDATE ON task_run WHEN 0 BEGIN SELECT 1; END").await.unwrap();
    let tx = store.connection.begin().await.unwrap();
    assert!(Migrator::up(&*tx, None).await.is_err());
    tx.rollback().await.unwrap();
    let query = Query::select()
        .column("name")
        .from("sqlite_master")
        .and_where(Expr::col("name").is_in([
            "task_run_occurrence_reconcile_pending",
            "task_run_occurrence_reconcile_sequence",
            "task_run_occurrence_reconcile_task_run_insert",
            "idx_task_run_occurrence_reconcile_due",
        ]))
        .to_owned();
    assert!(
        store
            .connection
            .query_all_raw(DatabaseBackend::Sqlite.build(&query))
            .await
            .unwrap()
            .is_empty()
    );
    let marker = Query::select()
        .column("version")
        .from("seaql_migrations")
        .and_where(Expr::col("version").eq(MIGRATION))
        .to_owned();
    assert!(
        store
            .connection
            .query_all_raw(DatabaseBackend::Sqlite.build(&marker))
            .await
            .unwrap()
            .is_empty()
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER task_run_occurrence_reconcile_task_run_update")
        .await
        .unwrap();
    let tx = store.connection.begin().await.unwrap();
    Migrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(generation(&store).await, 0);
}

#[tokio::test]
async fn changed_after_discovery_loses_claim_and_reincarnation_cannot_accept_old_result() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let candidate = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    turn_status(&store, &run.id, "failed").await;
    assert!(
        store
            .claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
            .await
            .unwrap()
            .is_none()
    );
    let claim = claimed(&store).await;
    let preparation = prepared(&store, &run.id).await;
    let saved = runs::Entity::find_by_id(run.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    runs::Entity::delete_by_id(run.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    runs::Entity::insert(saved.into_active_model())
        .exec(&store.connection)
        .await
        .unwrap();
    let current = row(&store, &run.id).await.unwrap();
    assert!(current.generation > claim.generation);
    assert_eq!(
        store
            .with_maintenance_reads_and_critical_writes()
            .commit_task_run_occurrence(preparation, Some(claim))
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::StaleClaim
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), current);
}

#[tokio::test]
async fn attempt_count_saturates_but_poison_row_stays_retryable_forever() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let candidate = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    let mut now = NOW;
    for attempt in 1..=24 {
        let claim = store
            .claim_task_run_occurrence_reconcile(&candidate, &|| now)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.attempt_count, std::cmp::min(attempt, 16));
        assert_eq!(claim.next_attempt_at, now + queue::retry_delay(attempt));
        assert_eq!(
            row(&store, &run.id).await.unwrap().claim_token.as_deref(),
            Some(claim.claim_token.as_str())
        );
        now = claim.next_attempt_at;
    }
}

#[tokio::test]
async fn recovery_completed_at_write_tracks_preinstall_run_without_history_discovery() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    runs::Entity::update_many()
        .col_expr(
            runs::Column::CompletedAt,
            Expr::val(None::<sea_orm::entity::prelude::DateTimeWithTimeZone>),
        )
        .filter(runs::Column::Id.eq(&run.id))
        .exec(&store.connection)
        .await
        .unwrap();
    let maintenance = store.with_maintenance_access();
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::down(&*tx, Some(rollback_through_parent_migration()))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    assert!(row(&store, &run.id).await.is_none());
    // The existing generic recovery still owns its algorithm. Its actual
    // completed_at UPDATE, after tracker installation, supplies the work item.
    maintenance
        .repair_terminal_runs_missing_completed_at()
        .await
        .unwrap();
    assert!(row(&store, &run.id).await.is_some());
}

#[tokio::test]
async fn auxiliary_native_effect_payload_edit_with_same_hash_timestamp_rolls_back_repair() {
    let (store, thread, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let effect = cleanup_effect_preparation("ws_task", &thread.id, &run.id, "occurrence_effect");
    let id = effect.effects[0].effect_id.clone();
    store
        .prepare_native_terminal_effects(effect, 1_700_000_001)
        .await
        .unwrap();
    let original = pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    let claim = claimed(&store).await;
    let prepared = prepared(&store, &run.id).await;
    pioneer_entity::native_terminal_effect_outbox::Entity::update_many()
        .col_expr(
            pioneer_entity::native_terminal_effect_outbox::Column::PayloadJson,
            Expr::val("{}"),
        )
        .filter(pioneer_entity::native_terminal_effect_outbox::Column::EffectId.eq(&id))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(
        store
            .with_maintenance_reads_and_critical_writes()
            .commit_task_run_occurrence(prepared, Some(claim.clone()))
            .await
            .is_err()
    );
    assert_eq!(
        row(&store, &run.id).await.unwrap().claim_token.as_deref(),
        Some(claim.claim_token.as_str())
    );
    assert_eq!(
        store
            .get_turn(&thread.id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    pioneer_entity::native_terminal_effect_outbox::Entity::update_many()
        .col_expr(
            pioneer_entity::native_terminal_effect_outbox::Column::PayloadJson,
            Expr::val(original.payload_json),
        )
        .filter(pioneer_entity::native_terminal_effect_outbox::Column::EffectId.eq(&id))
        .exec(&store.connection)
        .await
        .unwrap();
    let next = store
        .claim_task_run_occurrence_reconcile(
            &TaskRunOccurrenceReconcileCandidate {
                run_id: run.id.clone(),
                generation: claim.generation,
            },
            &|| claim.next_attempt_at,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&next, claim.next_attempt_at)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::Changed
    );
}

#[tokio::test]
async fn failed_claim_deferral_fences_generation_token_due_count_and_ambiguous_success() {
    use crate::{
        TaskRunOccurrenceClaimDeferral as Deferral, TaskRunOccurrenceClaimFailurePhase as Phase,
    };
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let maintenance = store.with_maintenance_access();
    let candidate = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    // SQLite trigger DDL has no SeaQuery builder. This rejects only token SET,
    // preserving ordinary service bookkeeping writes for the same candidate.
    maintenance.connection.execute_unprepared("CREATE TRIGGER reject_claim BEFORE UPDATE OF claim_token ON task_run_occurrence_reconcile_pending BEGIN SELECT RAISE(ABORT,'individual claim refusal'); END").await.unwrap();
    let failure = match queue::claim(
        &maintenance.connection,
        &candidate,
        "rejected".into(),
        &|| NOW,
    )
    .await
    {
        Err(failure) => failure,
        _ => panic!("claim must be rejected"),
    };
    assert_eq!(failure.phase, Phase::Reservation);
    let snapshot = failure.snapshot.unwrap();
    assert_eq!(
        queue::defer_failed_claim(&maintenance.connection, &snapshot, &|| NOW)
            .await
            .unwrap(),
        Deferral::Deferred
    );
    let deferred = row(&store, &run.id).await.unwrap();
    assert_eq!(deferred.attempt_count, 1);
    assert_eq!(deferred.next_attempt_at, NOW + 5);
    assert_eq!(deferred.claim_token, snapshot.claim_token);
    assert_eq!(
        queue::defer_failed_claim(&maintenance.connection, &snapshot, &|| NOW + 100)
            .await
            .unwrap(),
        Deferral::StateChanged
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), deferred);
    // Source refresh must also invalidate the token. Remove the injected token
    // write blocker before changing the source, while retaining the failed
    // attempt's exact snapshot to test its later stale deferral.
    maintenance
        .connection
        .execute_unprepared("DROP TRIGGER reject_claim")
        .await
        .unwrap();
    // A late result of the same refusal cannot postpone a new generation.
    runs::Entity::update_many()
        .col_expr(
            runs::Column::ErrorJson,
            Expr::val("{\"message\":\"changed\"}"),
        )
        .filter(runs::Column::Id.eq(&run.id))
        .exec(&store.connection)
        .await
        .unwrap();
    let updated = row(&store, &run.id).await.unwrap();
    assert_eq!(
        queue::defer_failed_claim(&maintenance.connection, &deferred, &|| NOW + 100)
            .await
            .unwrap(),
        Deferral::StateChanged
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), updated);
    let candidate = TaskRunOccurrenceReconcileCandidate {
        run_id: run.id.clone(),
        generation: updated.generation,
    };
    let first = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW + 5)
        .await
        .unwrap()
        .unwrap();
    let old_holder_snapshot = row(&store, &run.id).await.unwrap();
    let second = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| first.next_attempt_at)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.claim_token, second.claim_token);
    let current = row(&store, &run.id).await.unwrap();
    assert_eq!(
        queue::defer_failed_claim(&maintenance.connection, &old_holder_snapshot, &|| NOW + 100)
            .await
            .unwrap(),
        Deferral::StateChanged
    );
    // Model a lost commit acknowledgement: the successful token/due/count are
    // durable, but the caller holds only its pre-commit snapshot for deferral.
    assert_eq!(
        queue::defer_failed_claim(&maintenance.connection, &updated, &|| NOW + 100)
            .await
            .unwrap(),
        Deferral::StateChanged
    );
    assert_eq!(row(&store, &run.id).await.unwrap(), current);
}

#[tokio::test]
async fn unavailable_bookkeeping_is_reported_without_claim_or_durable_backoff_promise() {
    use crate::{
        TaskRunOccurrenceClaimDeferral as Deferral, TaskRunOccurrenceClaimFailure,
        TaskRunOccurrenceClaimFailurePhase as Phase,
    };
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let candidate = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    let before = row(&store, &run.id).await.unwrap();
    // Reject every service update, including the fallback deferral. Trigger
    // DDL is SQLite-specific; no production invariant is changed by this test.
    store.connection.execute_unprepared("CREATE TRIGGER reject_bookkeeping BEFORE UPDATE ON task_run_occurrence_reconcile_pending BEGIN SELECT RAISE(ABORT,'bookkeeping unavailable'); END").await.unwrap();
    let failure = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
        .await
        .unwrap_err()
        .downcast::<TaskRunOccurrenceClaimFailure>()
        .unwrap();
    assert_eq!(failure.phase, Phase::Reservation);
    assert_eq!(failure.deferral, Deferral::Failed);
    assert!(failure.deferral_error.is_some());
    assert_eq!(row(&store, &run.id).await.unwrap(), before);
    store.connection.clone().close().await.unwrap();
    let failure = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| NOW)
        .await
        .unwrap_err()
        .downcast::<TaskRunOccurrenceClaimFailure>()
        .unwrap();
    assert_eq!(failure.phase, Phase::AdvisoryRead);
    assert_eq!(failure.deferral, Deferral::NoSnapshot);
}

#[tokio::test]
async fn claim_uses_time_after_discovery_instead_of_the_advisory_clock() {
    let (store, _, _) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let clock = std::sync::atomic::AtomicI64::new(NOW);
    let candidate = store
        .discover_task_run_occurrence_reconcile(clock.load(Ordering::SeqCst), 64)
        .await
        .unwrap()
        .remove(0);
    clock.store(NOW + 100, Ordering::SeqCst);
    let claim = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| clock.load(Ordering::SeqCst))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.next_attempt_at, NOW + 105);
    assert_eq!(
        row(&store, &candidate.run_id)
            .await
            .unwrap()
            .next_attempt_at,
        NOW + 105
    );
}

#[tokio::test]
async fn claim_and_failed_claim_deferral_use_time_after_waiting_for_writer() {
    use std::time::Duration;
    let path = std::env::temp_dir().join(format!(
        "pioneer-occurrence-clock-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let routes = Arc::new(Routes::default());
    let store = disk_store(&path, routes.clone()).await;
    let id = populate_disk_pair(&store).await;
    let candidate = store
        .discover_task_run_occurrence_reconcile(NOW, 64)
        .await
        .unwrap()
        .remove(0);
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(NOW));
    let hold = store.connection.begin().await.unwrap();
    routes.watch_claim.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let c = candidate.clone();
    let time = clock.clone();
    let waiting = tokio::spawn(async move {
        worker
            .claim_task_run_occurrence_reconcile(&c, &|| time.load(Ordering::SeqCst))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.maintenance_queued.notified())
        .await
        .unwrap();
    clock.store(NOW + 100, Ordering::SeqCst);
    hold.rollback().await.unwrap();
    let claim = waiting.await.unwrap().unwrap().unwrap();
    assert_eq!(claim.next_attempt_at, NOW + 105);
    let snapshot = row(&store, &id).await.unwrap();
    clock.store(claim.next_attempt_at, Ordering::SeqCst);
    routes.watch_claim.store(false, Ordering::SeqCst);
    let hold = store.connection.begin().await.unwrap();
    routes.watch_claim.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let time = clock.clone();
    let waiting = tokio::spawn(async move {
        queue::defer_failed_claim(
            &worker.with_maintenance_access().connection,
            &snapshot,
            &|| time.load(Ordering::SeqCst),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.maintenance_queued.notified())
        .await
        .unwrap();
    clock.store(NOW + 200, Ordering::SeqCst);
    hold.rollback().await.unwrap();
    assert_eq!(
        waiting.await.unwrap().unwrap(),
        crate::TaskRunOccurrenceClaimDeferral::Deferred
    );
    let row = row(&store, &id).await.unwrap();
    assert_eq!(row.attempt_count, 2);
    assert_eq!(row.next_attempt_at, NOW + 210);
    assert_eq!(row.claim_token.as_deref(), Some(claim.claim_token.as_str()));
    remove_disk_fixture(store, path).await;
}

#[tokio::test]
async fn background_repair_waits_for_predecessor_then_retries_without_source_pair_change() {
    let (store, _, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;
    let run_before = runs::Entity::find_by_id(&run.id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    let turn_before = turns::Entity::find_by_id(&run.id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    mark_first_turn_projection_failed(&store, &run.id, NOW + 1).await;
    let claim = claimed(&store).await;
    let events_before = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(&run.id))
        .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
        .all(&store.connection)
        .await
        .unwrap();
    assert_eq!(events_before.len(), 1);
    assert!(
        store
            .reconcile_claimed_task_run_occurrence(&claim, NOW)
            .await
            .is_err()
    );
    assert_eq!(
        row(&store, &run.id).await.unwrap().next_attempt_at,
        claim.next_attempt_at
    );
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(&run.id))
            .all(&store.connection)
            .await
            .unwrap(),
        events_before
    );
    // Reproject the real predecessor using the existing prepared projection
    // and receipt/watermark transaction. Status/id/binding stay unchanged.
    assert_eq!(
        store
            .replay_due_turn_event_projections(NOW + 1, 64)
            .await
            .unwrap()
            .projected,
        1
    );
    let turn_after = turns::Entity::find_by_id(&run.id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(turn_after.status, turn_before.status);
    assert_eq!(
        runs::Entity::find_by_id(&run.id)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap(),
        run_before
    );
    assert_eq!(
        row(&store, &run.id).await.unwrap().generation,
        claim.generation
    );
    let candidate = TaskRunOccurrenceReconcileCandidate {
        run_id: run.id.clone(),
        generation: claim.generation,
    };
    assert!(
        store
            .claim_task_run_occurrence_reconcile(&candidate, &|| NOW + 4)
            .await
            .unwrap()
            .is_none()
    );
    let next = store
        .claim_task_run_occurrence_reconcile(&candidate, &|| claim.next_attempt_at)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&next, claim.next_attempt_at)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::Changed
    );
    assert_eq!(
        store
            .reconcile_claimed_task_run_occurrence(&claim, claim.next_attempt_at)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::StaleClaim
    );
    assert_eq!(
        store
            .compare_and_materialize_task_run_occurrence_terminal(&run.id, NOW + 6)
            .await
            .unwrap(),
        TaskRunOccurrenceTerminalizationOutcome::AlreadyConsistent
    );
    let events = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(&run.id))
        .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
        .all(&store.connection)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].id, events_before[0].id);
    assert_eq!(events[1].sequence, events[0].sequence + 1);
    let stream = pioneer_entity::turn_event_projection_stream_state::Entity::find_by_id(&run.id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stream.projected_through_sequence, events[1].sequence);
    let receipts = pioneer_entity::turn_event_projection_state::Entity::find()
        .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(&run.id))
        .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
        .all(&store.connection)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 2);
    assert!(receipts.iter().all(|receipt| receipt.status == "projected"));
    assert_eq!(receipts[0].sequence, events[0].sequence);
    assert_eq!(receipts[1].sequence, events[1].sequence);
    assert!(row(&store, &run.id).await.is_none());
}
