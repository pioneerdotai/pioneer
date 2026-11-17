use super::*;
// Written regression coverage. CLOSED GATE: do not execute before acceptance.
mod child_work_regressions {
    use super::test_store_with_workspace;
    use crate::repositories::task_child_work::{ChildScope, enqueue_point, enqueue_scope};
    use pioneer_entity::{
        task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
    };
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};

    #[tokio::test]
    async fn point_crash_before_wake_expiry_and_new_revision_reject_stale_ack() {
        let store = test_store_with_workspace("child-work-expiry").await;
        let db = store.database_connection();
        let tx = db.begin().await.unwrap();
        enqueue_point(&tx, "thread", "turn", 100).await.unwrap();
        tx.commit().await.unwrap(); // Intentionally no wake.
        let candidate = store.discover_child_points(100, 1).await.unwrap().remove(0);
        let first = store
            .claim_child_point(&candidate, 100)
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .discover_child_points(159, 1)
                .await
                .unwrap()
                .is_empty()
        );
        let expired = store.discover_child_points(160, 1).await.unwrap().remove(0);
        let second = store
            .claim_child_point(&expired, 160)
            .await
            .unwrap()
            .unwrap();
        assert!(!store.finish_child_point(&first, None, true).await.unwrap());
        let tx = db.begin().await.unwrap();
        enqueue_point(&tx, "thread", "turn", 161).await.unwrap();
        tx.commit().await.unwrap();
        assert!(!store.finish_child_point(&second, None, true).await.unwrap());
        assert_eq!(store.discover_child_points(161, 1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn parked_dependency_repair_and_delete_reinsert_fence() {
        let store = test_store_with_workspace("child-work-aba").await;
        let db = store.database_connection();
        let tx = db.begin().await.unwrap();
        enqueue_point(&tx, "t", "x", 1).await.unwrap();
        tx.commit().await.unwrap();
        let row = store.discover_child_points(1, 1).await.unwrap().remove(0);
        let claim = store.claim_child_point(&row, 1).await.unwrap().unwrap();
        assert!(store.finish_child_point(&claim, None, false).await.unwrap());
        assert!(
            store
                .discover_child_points(1000, 1)
                .await
                .unwrap()
                .is_empty()
        );
        let tx = db.begin().await.unwrap();
        point::Entity::delete_by_id("x").exec(&tx).await.unwrap();
        enqueue_point(&tx, "t", "x", 2).await.unwrap();
        tx.commit().await.unwrap();
        assert!(!store.finish_child_point(&claim, None, true).await.unwrap());
        assert_eq!(store.discover_child_points(2, 1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn producer_rollback_does_not_publish_work() {
        let store = test_store_with_workspace("child-work-rollback").await;
        let db = store.database_connection();
        let tx = db.begin().await.unwrap();
        enqueue_point(&tx, "t", "x", 1).await.unwrap();
        enqueue_scope(&tx, ChildScope::Task("a".into()), 1)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert!(
            store
                .discover_child_points(100, 32)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!store.expand_child_scope(100, false).await.unwrap());
    }

    #[tokio::test]
    async fn continuous_invalidations_keep_active_pass_lease_and_cursor() {
        let store = test_store_with_workspace("child-work-pass").await;
        let db = store.database_connection();
        let tx = db.begin().await.unwrap();
        enqueue_scope(&tx, ChildScope::Task("a".into()), 1)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let before = scope::Entity::find_by_id(("task".to_string(), "a".to_string()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let tx = db.begin().await.unwrap();
        scope::Entity::update_many()
            .filter(scope::Column::ScopeKind.eq("task"))
            .filter(scope::Column::ScopeKey.eq("a"))
            .col_expr(scope::Column::ClaimToken, Expr::value("holder"))
            .col_expr(
                scope::Column::PassRevision,
                Expr::value(&before.requested_revision),
            )
            .col_expr(scope::Column::Cursor, Expr::value("middle"))
            .col_expr(scope::Column::UpperWatermark, Expr::value("upper"))
            .col_expr(scope::Column::NextAttemptAt, Expr::value(61i64))
            .exec(&tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        for now in 2..20 {
            let tx = db.begin().await.unwrap();
            enqueue_scope(&tx, ChildScope::Task("a".into()), now)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let after = scope::Entity::find_by_id(("task".to_string(), "a".to_string()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(after.pass_revision, Some(before.requested_revision.clone()));
            assert_eq!(after.cursor.as_deref(), Some("middle"));
            assert_eq!(after.upper_watermark.as_deref(), Some("upper"));
            assert_eq!(after.claim_token.as_deref(), Some("holder"));
            assert_eq!(after.next_attempt_at, Some(61));
            assert_ne!(after.requested_revision, before.requested_revision);
        }
        // Between pages the lease is released but the pass/cursor remains.
        // Constant new requests must not move an already due pass behind the
        // consumer's discovery time or restart its progress.
        scope::Entity::update_many()
            .filter(scope::Column::ScopeKind.eq("task"))
            .filter(scope::Column::ScopeKey.eq("a"))
            .col_expr(
                scope::Column::ClaimToken,
                Expr::value(Option::<String>::None),
            )
            .col_expr(scope::Column::NextAttemptAt, Expr::value(1i64))
            .exec(&db)
            .await
            .unwrap();
        for now in 100..118 {
            let tx = db.begin().await.unwrap();
            enqueue_scope(&tx, ChildScope::Task("a".into()), now)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let due = store
                .discover_child_scope_after(99, false, None)
                .await
                .unwrap()
                .expect("new invalidation must retain already-due eligibility");
            assert_eq!(due.next_attempt_at, Some(1));
            assert_eq!(due.cursor.as_deref(), Some("middle"));
            assert_eq!(due.pass_revision.as_ref(), Some(&before.requested_revision));
        }
    }

    #[tokio::test]
    async fn empty_seed_eof_persists_across_restart() {
        let store = test_store_with_workspace("child-work-seed").await;
        assert!(store.expand_child_scope(1, true).await.unwrap());
        let restarted = crate::CrudStore::new(store.database_connection());
        assert!(!restarted.expand_child_scope(1000, true).await.unwrap());
        assert!(
            restarted
                .discover_child_points(1000, 32)
                .await
                .unwrap()
                .is_empty()
        );
        let marker = scope::Entity::find_by_id(("historical-seed".to_string(), "v1".to_string()))
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert!(marker.next_attempt_at.is_none());
        assert!(marker.claim_token.is_none());
    }
}

#[test]
fn child_ready_renderer_is_portable_and_preserves_positive_launch_proof() {
    use sea_orm::DbBackend;
    for backend in [DbBackend::Sqlite, DbBackend::Postgres, DbBackend::MySql] {
        let sql = crate::repositories::task_child_cleanup::render_ready(backend);
        assert!(!sql.contains("INDEXED BY"));
        assert!(!sql.contains(" || "));
        assert!(!sql.contains("trigger_id IS r.trigger_id"));
        assert!(sql.contains("accepted_source.thread_id = parent.thread_id"));
        assert!(sql.contains("parent.turn_kind = 'task_run'"));
        assert!(sql.contains("parent.turn_kind = 'conversation'"));
        assert!(sql.contains("s.projected_through_sequence >= s.accepted_terminal_sequence"));
    }
}

mod child_queue_pass_regressions {
    use super::*;
    use crate::repositories::task_child_work::{ChildScope, enqueue_scope};
    use pioneer_entity::{
        task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
    };

    async fn link(store: &CrudStore, id: &str, sequence: u32) {
        store
            .upsert_task_run_turn(TaskRunTurn {
                id: id.into(),
                task_id: "seed-task".into(),
                run_id: "seed-run".into(),
                execution_id: None,
                thread_id: "seed-thread".into(),
                turn_id: format!("turn-{id}"),
                kind: TaskRunTurnKind::Initial,
                round: sequence,
                sequence,
                status: TaskRunTurnStatus::InProgress,
                reviews_candidate_id: None,
                requested_by_candidate_id: None,
                requested_by_review_event_id: None,
                created_at: 100,
                started_at: None,
                completed_at: None,
            })
            .await
            .unwrap();
    }
    async fn seed(store: &CrudStore) -> scope::Model {
        scope::Entity::find_by_id(("historical-seed".to_owned(), "v1".to_owned()))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
    }
    #[tokio::test]
    async fn invalidation_behind_cursor_and_above_upper_finishes_current_pass_before_rescan() {
        let store = test_store_with_workspace("ws_task").await;
        pioneer_sqlite::apply_sqlite_pragmas(&store.connection)
            .await
            .unwrap();
        // Historical FK-disabled metadata can be orphaned. Seed must carry its
        // locators without interpreting that as destructive domain authority.
        for n in 0..40 {
            link(&store, &format!("m{n:03}"), n).await;
        }
        point::Entity::delete_many()
            .exec(&store.connection)
            .await
            .unwrap();
        assert!(store.expand_child_scope(100, true).await.unwrap());
        let first = seed(&store).await;
        assert_eq!(first.cursor.as_deref(), Some("m031"));
        assert_eq!(first.upper_watermark.as_deref(), Some("m039"));
        link(&store, "a-behind", 40).await;
        link(&store, "z-above", 41).await;
        // Treat the point obligations as already drained. Scope's next pass
        // still has to enumerate both late insertions after an explicit resync.
        point::Entity::delete_many()
            .exec(&store.connection)
            .await
            .unwrap();
        let tx = store.connection.begin().await.unwrap();
        enqueue_scope(&tx, ChildScope::HistoricalSeed, 101)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let invalidated = seed(&store).await;
        assert_eq!(invalidated.cursor, first.cursor);
        assert_eq!(invalidated.upper_watermark, first.upper_watermark);
        assert_eq!(invalidated.pass_revision, first.pass_revision);
        assert_ne!(invalidated.requested_revision, first.requested_revision);
        assert!(store.expand_child_scope(101, true).await.unwrap()); // Old pass EOF.
        assert!(
            point::Entity::find_by_id("turn-a-behind")
                .one(&store.connection)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            point::Entity::find_by_id("turn-z-above")
                .one(&store.connection)
                .await
                .unwrap()
                .is_none()
        );
        assert!(seed(&store).await.next_attempt_at.is_some());
        assert!(store.expand_child_scope(101, true).await.unwrap()); // New pass first page.
        assert!(
            point::Entity::find_by_id("turn-a-behind")
                .one(&store.connection)
                .await
                .unwrap()
                .is_some()
        );
        assert!(store.expand_child_scope(101, true).await.unwrap());
        assert!(
            point::Entity::find_by_id("turn-z-above")
                .one(&store.connection)
                .await
                .unwrap()
                .is_some()
        );
        assert!(seed(&store).await.next_attempt_at.is_none());
    }
    #[tokio::test]
    async fn old_eof_snapshot_and_aba_cannot_delete_new_scope_incarnation() {
        let store = test_store_with_workspace("ws_task").await;
        let old = seed(&store).await;
        let tx = store.connection.begin().await.unwrap();
        scope::Entity::delete_by_id((old.scope_kind.clone(), old.scope_key.clone()))
            .exec(&tx)
            .await
            .unwrap();
        enqueue_scope(&tx, ChildScope::HistoricalSeed, 1)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let replacement = seed(&store).await;
        assert_ne!(replacement.incarnation_token, old.incarnation_token);
        store
            .expand_child_scope_candidate_with_clock(&old, &|| 100, true)
            .await
            .unwrap();
        assert_eq!(
            seed(&store).await,
            replacement,
            "a delayed EOF candidate cannot ACK a replacement"
        );
        // A new request arriving after EOF revives the parked seed and is due.
        store.expand_child_scope(100, true).await.unwrap();
        let tx = store.connection.begin().await.unwrap();
        enqueue_scope(&tx, ChildScope::HistoricalSeed, 101)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(seed(&store).await.next_attempt_at.is_some());
    }
    #[tokio::test]
    async fn expired_scope_lease_recovers_and_poison_scope_does_not_block_other_scopes() {
        let store = test_store_with_workspace("ws_task").await;
        let tx = store.connection.begin().await.unwrap();
        enqueue_scope(&tx, ChildScope::Task("healthy".into()), 1)
            .await
            .unwrap();
        enqueue_scope(
            &tx,
            ChildScope::CreatorTurn {
                thread_id: "t".into(),
                turn_id: "x".into(),
            },
            0,
        )
        .await
        .unwrap();
        scope::Entity::update_many()
            .filter(scope::Column::ScopeKind.eq("creator-turn"))
            .col_expr(scope::Column::ScopeKey, Expr::value("corrupt-pair"))
            .exec(&tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(store.expand_child_scope(1, false).await.is_err());
        assert!(
            store.expand_child_scope(1, false).await.unwrap(),
            "poison row is deferred behind healthy work"
        );
        let tx = store.connection.begin().await.unwrap();
        scope::Entity::update_many()
            .filter(scope::Column::ScopeKind.eq("historical-seed"))
            .col_expr(scope::Column::ClaimToken, Expr::value("dead-worker"))
            .col_expr(scope::Column::NextAttemptAt, Expr::value(50i64))
            .exec(&tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(!store.expand_child_scope(49, true).await.unwrap());
        assert!(store.expand_child_scope(50, true).await.unwrap());
        assert!(seed(&store).await.next_attempt_at.is_none());
    }
    #[tokio::test]
    async fn claim_deadline_uses_post_admission_clock() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let store = test_store_with_workspace("ws_task").await;
        let tx = store.connection.begin().await.unwrap();
        crate::repositories::task_child_work::enqueue_point(&tx, "thread", "turn", 100)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let candidate = store.discover_child_points(100, 1).await.unwrap().remove(0);
        let calls = AtomicUsize::new(0);
        let claim = store
            .claim_child_point_with_clock(&candidate, &|| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    100
                } else {
                    400
                }
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            claim.next_attempt_at,
            Some(460),
            "waiting for the writer must not consume the lease"
        );
    }
}

#[test]
fn bound_child_cleanup_statement_quotes_outer_turn_and_binds_each_backend() {
    use sea_orm::DbBackend;
    for backend in [DbBackend::Sqlite, DbBackend::Postgres, DbBackend::MySql] {
        let stmt = crate::repositories::task_child_cleanup::ready_statement(
            backend,
            "thread-secret",
            "turn-secret",
        );
        assert!(!stmt.sql.contains("thread-secret"));
        assert!(!stmt.sql.contains("turn-secret"));
        assert_eq!(stmt.values.as_ref().unwrap().0.len(), 2);
        if backend == DbBackend::Postgres {
            assert!(stmt.sql.contains("t.thread_id = $1 AND t.id = $2"));
            assert!(!stmt.sql.contains('?'));
        } else {
            assert!(stmt.sql.contains("t.thread_id = ? AND t.id = ?"));
        }
        if backend == DbBackend::MySql {
            assert!(stmt.sql.contains("JOIN `turn` t"));
            assert!(!stmt.sql.contains("\"turn\""));
        } else {
            assert!(stmt.sql.contains("JOIN \"turn\" t"));
        }
    }
}

#[tokio::test]
async fn empty_child_polls_use_physical_read_only_reader_without_writer_admission() {
    use sea_orm::{ConnectOptions, Database, EntityTrait};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Observer(AtomicUsize);
    impl pioneer_sqlite::SqliteWriteObserver for Observer {
        fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
            if let pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } = event {
                assert_eq!(class, pioneer_sqlite::SqliteWriteClass::Maintenance);
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("empty-child.sqlite");
    let writer = Database::connect(format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .unwrap();
    pioneer_sqlite::apply_sqlite_pragmas(&writer).await.unwrap();
    let observer = Arc::new(Observer(AtomicUsize::new(0)));
    let executor = pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, observer.clone());
    executor
        .run_migrations::<migration::Migrator>(pioneer_sqlite::SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(options).await.unwrap();
    let store = crate::CrudStore::new(pioneer_sqlite::SqliteDatabase::from_executor(
        reader, executor,
    ))
    .with_maintenance_access();
    assert!(
        store
            .database_connection()
            .reader_query_only_enabled()
            .await
            .unwrap()
    );
    observer.0.store(0, Ordering::SeqCst);
    assert!(
        store
            .discover_child_points(100, 32)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.child_point_due_upper(100).await.unwrap().is_none());
    assert!(store.child_scope_due_upper(100).await.unwrap().is_none());
    assert!(
        store
            .discover_child_points_in_pass(100, 32, None, &(100, "z".into()))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .discover_child_scope_in_pass(100, None, &(100, "z".into(), "z".into()))
            .await
            .unwrap()
            .is_none()
    );
    assert!(!store.expand_child_scope(100, false).await.unwrap());
    assert!(!store.child_work_queues_ready().await.unwrap());
    store.discover_legacy_child_work(32).await.unwrap();
    assert_eq!(
        observer.0.load(Ordering::SeqCst),
        0,
        "an empty advisory poll must never reserve writer capacity"
    );
    // Show the observer is attached to this exact scoped handle.
    let tx = store.database_connection().begin().await.unwrap();
    crate::repositories::task_child_work::enqueue_point(&tx, "t", "x", 100)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(observer.0.load(Ordering::SeqCst) > 0);
    assert!(
        pioneer_entity::task_child_reconcile_pending::Entity::find_by_id("x")
            .one(&store.database_connection())
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn child_preflight_race_restarts_the_entire_short_transaction_after_rollback() {
    use crate::repositories::task_child_work::enqueue_point;
    use pioneer_entity::task_child_reconcile_pending as point;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let store = test_store_with_workspace("ws_task").await;
    let attempts = AtomicUsize::new(0);
    store
        .run_serialized_write(|| async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            let tx = store.connection.begin().await?;
            if attempt == 0 {
                enqueue_point(&tx, "t", "rolled-back-attempt", 1).await?;
                return Err(crate::repositories::portable_payload::PreflightRace.into());
            }
            assert!(
                point::Entity::find_by_id("rolled-back-attempt")
                    .one(&tx)
                    .await?
                    .is_none(),
                "retry must use a new transaction, not the old snapshot/write set"
            );
            enqueue_point(&tx, "t", "committed-attempt", 1).await?;
            tx.commit().await?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(
        point::Entity::find_by_id("committed-attempt")
            .one(&store.connection)
            .await
            .unwrap()
            .is_some()
    );
    assert!(!crate::repositories::task_child_work::is_transaction_retry(
        &anyhow::anyhow!(sea_orm::DbErr::Custom("unknown commit outcome".into()))
    ));
}

#[tokio::test]
async fn independently_fenced_claim_failure_deferral_preserves_an_ambiguous_committed_claim() {
    use pioneer_entity::task_child_reconcile_pending as point;
    let store = test_store_with_workspace("ws_task").await;
    let tx = store.connection.begin().await.unwrap();
    crate::repositories::task_child_work::enqueue_point(&tx, "t", "unknown-claim", 100)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let preclaim = store.discover_child_points(100, 1).await.unwrap().remove(0);
    let committed = store
        .claim_child_point_with_clock(&preclaim, &|| 100)
        .await
        .unwrap()
        .unwrap();
    // Model the lost claim response: the consumer knows only preclaim snapshot.
    assert!(
        !store
            .defer_child_point_snapshot(&preclaim, &|| 101)
            .await
            .unwrap()
    );
    assert_eq!(
        point::Entity::find_by_id("unknown-claim")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    assert!(
        store
            .discover_child_points(159, 1)
            .await
            .unwrap()
            .is_empty()
    );
    let due = store.discover_child_points(160, 1).await.unwrap().remove(0);
    assert!(
        store
            .claim_child_point_with_clock(&due, &|| 160)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn seed_restart_distinguishes_deferred_claimed_missing_and_committed_eof() {
    use crate::repositories::task_child_work::ChildSeedOutcome;
    use pioneer_entity::task_child_reconcile_scope as scope;
    let store = test_store_with_workspace("ws_task").await;
    let tx = store.connection.begin().await.unwrap();
    scope::Entity::update_many()
        .filter(scope::Column::ScopeKind.eq("historical-seed"))
        .col_expr(scope::Column::NextAttemptAt, Expr::value(200i64))
        .exec(&tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let restarted = crate::CrudStore::new(store.database_connection());
    assert_eq!(
        restarted.seed_child_work_with_clock(&|| 100).await.unwrap(),
        ChildSeedOutcome::Deferred { retry_at: 200 }
    );
    assert_eq!(
        restarted.seed_child_work_with_clock(&|| 199).await.unwrap(),
        ChildSeedOutcome::Deferred { retry_at: 200 }
    );
    let tx = store.connection.begin().await.unwrap();
    scope::Entity::update_many()
        .filter(scope::Column::ScopeKind.eq("historical-seed"))
        .col_expr(scope::Column::ClaimToken, Expr::value("old-process"))
        .exec(&tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        restarted.seed_child_work_with_clock(&|| 199).await.unwrap(),
        ChildSeedOutcome::Claimed { expires_at: 200 }
    );
    assert_eq!(
        restarted.seed_child_work_with_clock(&|| 200).await.unwrap(),
        ChildSeedOutcome::CommittedDone
    );
    assert_eq!(
        restarted.seed_child_work_with_clock(&|| 201).await.unwrap(),
        ChildSeedOutcome::CommittedDone
    );
    scope::Entity::delete_by_id(("historical-seed".to_owned(), "v1".to_owned()))
        .exec(&store.connection)
        .await
        .unwrap();
    assert_eq!(
        restarted.seed_child_work_with_clock(&|| 202).await.unwrap(),
        ChildSeedOutcome::Missing
    );
}

#[tokio::test]
async fn failed_seed_claim_retains_owned_deferred_work_until_post_deadline_eof() {
    use crate::repositories::task_child_work::ChildSeedOutcome;
    let store = test_store_with_workspace("ws_task").await;
    store.connection.execute_unprepared("CREATE TRIGGER test_seed_claim_refusal BEFORE UPDATE ON task_child_reconcile_scope WHEN NEW.scope_kind='historical-seed' AND NEW.claim_token IS NOT NULL BEGIN SELECT RAISE(FAIL,'seed claim refused'); END").await.unwrap();
    assert_eq!(
        store.seed_child_work_with_clock(&|| 100).await.unwrap(),
        ChildSeedOutcome::Deferred { retry_at: 102 }
    );
    let restarted = crate::CrudStore::new(store.database_connection());
    assert_eq!(
        restarted.seed_child_work_with_clock(&|| 101).await.unwrap(),
        ChildSeedOutcome::Deferred { retry_at: 102 }
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER test_seed_claim_refusal")
        .await
        .unwrap();
    assert_eq!(
        restarted.seed_child_work_with_clock(&|| 102).await.unwrap(),
        ChildSeedOutcome::CommittedDone
    );
}
