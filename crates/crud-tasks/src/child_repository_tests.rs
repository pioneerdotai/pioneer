use super::*;
use crate::values::unix_to_datetime;
use pioneer_protocol::{TaskRunTurnKind, TaskRunTurnStatus};

async fn postgres_store_with_workspace(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace_id: &str,
) -> TaskChildWorkRepository {
    let writer = fixture.connection();
    let timestamp = unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: sea_orm::Set(workspace_id.to_owned()),
        name: sea_orm::Set("Test Workspace".to_owned()),
        is_active: sea_orm::Set(true),
        is_current: sea_orm::Set(false),
        created_at: sea_orm::Set(timestamp),
        updated_at: sea_orm::Set(timestamp),
    })
    .exec(writer)
    .await
    .expect("insert Task child workspace fixture");
    let reader = fixture.connect_read_only().await.unwrap();
    TaskChildWorkRepository::new(&PostgresDatabase::new(reader, writer.clone()))
}
mod child_queue_pass_regressions {
    use super::*;
    use super::{ChildScope, enqueue_scope};
    use pioneer_entity::{
        task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
    };

    async fn link(store: &TaskChildWorkRepository, id: &str, sequence: u32) {
        store
            .connection
            .execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DbBackend::Postgres,
                "INSERT INTO turn(id,thread_id,status) VALUES($1,'seed-thread','in_progress')",
                [format!("turn-{id}").into()],
            ))
            .await
            .unwrap();
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
    async fn child_scope(store: &TaskChildWorkRepository) -> Option<scope::Model> {
        scope::Entity::find_by_id(("child-thread".to_owned(), "seed-thread".to_owned()))
            .one(&store.connection)
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn invalidation_behind_cursor_and_above_upper_finishes_current_pass_before_rescan() {
        pioneer_postgres_test::with_database(async |fixture| {
            let store = postgres_store_with_workspace(fixture, "ws_task").await;

            store.connection.execute_unprepared(
                "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
                 VALUES ('seed-thread','ws_task','','agent','m','p','active');
                 INSERT INTO task(id,workspace_id,owner_kind,executor_kind,status,title,goal)
                 VALUES ('seed-task','ws_task','system','agent','running','Fixture','Fixture');
                 INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind)
                 VALUES ('seed-run','seed-task','seed-run',1,1,'running','agent');",
            ).await.unwrap();
            for n in 0..40 {
                link(&store, &format!("m{n:03}"), n).await;
            }
            point::Entity::delete_many()
                .exec(&store.connection)
                .await
                .unwrap();
            scope::Entity::delete_many().exec(&store.connection).await.unwrap();
            super::enqueue_scope(
                &store.connection,
                ChildScope::ChildThread("seed-thread".into()),
                100,
            ).await.unwrap();
            assert!(store.expand_child_scope(100).await.unwrap());
            let first = child_scope(&store).await.unwrap();
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
            enqueue_scope(&tx, ChildScope::ChildThread("seed-thread".into()), 101)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let invalidated = child_scope(&store).await.unwrap();
            assert_eq!(invalidated.cursor, first.cursor);
            assert_eq!(invalidated.upper_watermark, first.upper_watermark);
            assert_eq!(invalidated.pass_revision, first.pass_revision);
            assert_ne!(invalidated.requested_revision, first.requested_revision);
            assert!(store.expand_child_scope(101).await.unwrap()); // Old pass EOF.
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
            assert!(child_scope(&store).await.unwrap().next_attempt_at.is_some());
            assert!(store.expand_child_scope(101).await.unwrap()); // New pass first page.
            assert!(
                point::Entity::find_by_id("turn-a-behind")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(store.expand_child_scope(101).await.unwrap());
            assert!(
                point::Entity::find_by_id("turn-z-above")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(child_scope(&store).await.is_none());
            Ok(())
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn old_eof_snapshot_and_aba_cannot_delete_new_scope_incarnation() {
        pioneer_postgres_test::with_database(async |fixture| {
            let store = postgres_store_with_workspace(fixture, "ws_task").await;
            enqueue_scope(
                &store.connection,
                ChildScope::ChildThread("seed-thread".into()),
                1,
            )
            .await
            .unwrap();
            let old = child_scope(&store).await.unwrap();
            let tx = store.connection.begin().await.unwrap();
            scope::Entity::delete_by_id((old.scope_kind.clone(), old.scope_key.clone()))
                .exec(&tx)
                .await
                .unwrap();
            enqueue_scope(&tx, ChildScope::ChildThread("seed-thread".into()), 1)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let replacement = child_scope(&store).await.unwrap();
            assert_ne!(replacement.incarnation_token, old.incarnation_token);
            store
                .expand_child_scope_candidate_with_clock(&old, &|| 100)
                .await
                .unwrap();
            assert_eq!(
                child_scope(&store).await.unwrap(),
                replacement,
                "a delayed EOF candidate cannot ACK a replacement"
            );
            // A new request after EOF is due under a fresh incarnation.
            store.expand_child_scope(100).await.unwrap();
            let tx = store.connection.begin().await.unwrap();
            enqueue_scope(&tx, ChildScope::ChildThread("seed-thread".into()), 101)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            assert!(child_scope(&store).await.unwrap().next_attempt_at.is_some());
            Ok(())
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn expired_scope_lease_recovers_and_poison_scope_does_not_block_other_scopes() {
        pioneer_postgres_test::with_database(async |fixture| {
            let store = postgres_store_with_workspace(fixture, "ws_task").await;
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
            assert!(store.expand_child_scope(1).await.is_err());
            assert!(
                store.expand_child_scope(1).await.unwrap(),
                "poison row is deferred behind healthy work"
            );
            let tx = store.connection.begin().await.unwrap();
            // Leave a single live scope so discovery tests its exact lease.
            scope::Entity::delete_many().exec(&tx).await.unwrap();
            enqueue_scope(&tx, ChildScope::ChildThread("seed-thread".into()), 1)
                .await
                .unwrap();
            scope::Entity::update_many()
                .filter(scope::Column::ScopeKind.eq("child-thread"))
                .col_expr(scope::Column::ClaimToken, Expr::value("dead-worker"))
                .col_expr(scope::Column::NextAttemptAt, Expr::value(50i64))
                .exec(&tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            assert!(!store.expand_child_scope(49).await.unwrap());
            assert!(store.expand_child_scope(50).await.unwrap());
            assert!(child_scope(&store).await.is_none());
            Ok(())
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn claim_deadline_uses_post_admission_clock() {
        pioneer_postgres_test::with_database(async |fixture| {
            use std::sync::atomic::{AtomicUsize, Ordering};
            let store = postgres_store_with_workspace(fixture, "ws_task").await;
            let tx = store.connection.begin().await.unwrap();
            super::enqueue_point(&tx, "thread", "turn", 100)
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
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[pioneer_postgres_test::postgres_test]
async fn empty_child_polls_use_physical_read_only_reader_without_writer_admission() {
    use sea_orm::EntityTrait;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Observer(AtomicUsize);
    impl pioneer_postgres_pools::PostgresWriteObserver for Observer {
        fn observe(&self, event: pioneer_postgres_pools::PostgresWriteEvent) {
            if let pioneer_postgres_pools::PostgresWriteEvent::Acquired { class, .. } = event {
                assert_eq!(
                    class,
                    pioneer_postgres_pools::PostgresWriteClass::Maintenance
                );
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let observer = Arc::new(Observer(AtomicUsize::new(0)));
    let executor =
        pioneer_postgres_pools::PostgresWriteExecutor::with_observer(writer, observer.clone());
    let store = TaskChildWorkRepository::new(&PostgresDatabase::from_executor(reader, executor))
        .with_maintenance_access();
    assert!(
        store
            .database_connection()
            .reader_is_read_only()
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
    assert!(!store.expand_child_scope(100).await.unwrap());
    assert_eq!(
        observer.0.load(Ordering::SeqCst),
        0,
        "an empty advisory poll must never reserve writer capacity"
    );
    // Show the observer is attached to this exact scoped handle.
    let tx = store.database_connection().begin().await.unwrap();
    super::enqueue_point(&tx, "t", "x", 100).await.unwrap();
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
    pioneer_postgres_test::with_database(async |fixture| {
        use super::enqueue_point;
        use pioneer_entity::task_child_reconcile_pending as point;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let store = postgres_store_with_workspace(fixture, "ws_task").await;
        let attempts = AtomicUsize::new(0);
        store
            .retry_write_on_conflict(|| async {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let tx = store.connection.begin().await?;
                if attempt == 0 {
                    enqueue_point(&tx, "t", "rolled-back-attempt", 1).await?;
                    return Err(PreflightRace.into());
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
        assert!(!pioneer_postgres_pools::is_retryable_database_conflict(
            &anyhow::anyhow!(sea_orm::DbErr::Custom("unknown commit outcome".into()))
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn independently_fenced_claim_failure_deferral_preserves_an_ambiguous_committed_claim() {
    pioneer_postgres_test::with_database(async |fixture| {
        use pioneer_entity::task_child_reconcile_pending as point;
        let store = postgres_store_with_workspace(fixture, "ws_task").await;
        let tx = store.connection.begin().await.unwrap();
        super::enqueue_point(&tx, "t", "unknown-claim", 100)
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
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn failed_child_scope_claim_remains_deferred_until_its_deadline_after_reopen() {
    use super::{ChildScope, enqueue_scope};
    use pioneer_entity::task_child_reconcile_scope as scope;

    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let store = postgres_store_with_workspace(&fixture, "ws_task").await;
    enqueue_scope(&store.connection, ChildScope::Task("scope".into()), 100)
        .await
        .unwrap();
    store
        .connection
        .execute_unprepared(
            "CREATE FUNCTION test_scope_claim_refusal() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'scope claim refused'; END; $$;
         CREATE TRIGGER test_scope_claim_refusal BEFORE UPDATE ON task_child_reconcile_scope
         FOR EACH ROW WHEN (NEW.scope_key='scope' AND NEW.claim_token IS NOT NULL)
         EXECUTE FUNCTION test_scope_claim_refusal()",
        )
        .await
        .unwrap();
    assert!(store.expand_child_scope(100).await.is_err());
    let reader = fixture.connect_read_only().await.unwrap();
    let writer = fixture.connect().await.unwrap();
    let restarted = TaskChildWorkRepository::new(&PostgresDatabase::new(reader, writer));
    assert_eq!(
        restarted.child_scope_due_upper(102).await.unwrap(),
        Some((102, "task".into(), "scope".into()))
    );
    assert!(!restarted.expand_child_scope(101).await.unwrap());
    store
        .connection
        .execute_unprepared(
            "DROP TRIGGER test_scope_claim_refusal ON task_child_reconcile_scope;
         DROP FUNCTION test_scope_claim_refusal()",
        )
        .await
        .unwrap();
    assert!(restarted.expand_child_scope(102).await.unwrap());
    assert!(
        scope::Entity::find_by_id(("task".to_owned(), "scope".to_owned()))
            .one(&restarted.connection)
            .await
            .unwrap()
            .is_none()
    );
}
