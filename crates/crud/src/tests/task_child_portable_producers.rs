//! Primary publication and portable before-image regressions. NEVER RUN before
//! external acceptance. Remote cases require a disposable, provisioned MySQL
//! schema (including work tables/migration marker) and workspace `ws_task`.
use super::*;
use crate::repositories::{task_child_producers as producers, turn_llm_context as context};
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};

const AT: i64 = 1_700_000_000;

fn entry(id: &str, sequence: i64) -> context::NewTurnLlmContextEntry {
    context::NewTurnLlmContextEntry {
        turn_id: id.into(),
        item_id: None,
        attempt_id: None,
        sequence,
        source: "user".into(),
        tool_name: None,
        payload: "{}".into(),
        output_policy_snapshot: "{}".into(),
        created_at: unix_to_datetime(AT),
        expires_at: None,
    }
}

#[tokio::test]
async fn late_capsule_birth_survives_prior_ack_without_link_or_terminal_status() {
    let store = test_store_with_workspace("ws_task").await;
    pioneer_sqlite::apply_sqlite_pragmas(&store.connection)
        .await
        .unwrap();
    let db = store.database_connection();
    // No Turn and no Link: publication must retain an obligation even when
    // neither cross-table dependency exists in the source transaction.
    let tx = db.begin().await.unwrap();
    context::insert_turn_llm_context(&tx, entry("late-birth", 1))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let old = point::Entity::find_by_id("late-birth")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let claim = store
        .claim_child_point(&old, i64::MAX / 2)
        .await
        .unwrap()
        .unwrap();
    assert!(store.finish_child_point(&claim, None, true).await.unwrap());
    let tx = db.begin().await.unwrap();
    context::delete_turn_llm_context_for_turn(&tx, "late-birth")
        .await
        .unwrap();
    context::insert_turn_llm_context(&tx, entry("late-birth", 2))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let fresh = point::Entity::find_by_id("late-birth")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(fresh.revision, old.revision);
    assert!(fresh.next_attempt_at.is_some());
    assert!(!store.finish_child_point(&claim, None, true).await.unwrap());
    // Further payload appends do not wake/replace the existing obligation.
    let tx = db.begin().await.unwrap();
    context::insert_turn_llm_context(&tx, entry("late-birth", 3))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        point::Entity::find_by_id("late-birth")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .revision,
        fresh.revision
    );
}

#[tokio::test]
async fn stale_advisory_locator_is_revised_before_no_record_ack() {
    let store = test_store_with_workspace("ws_task").await;
    pioneer_sqlite::apply_sqlite_pragmas(&store.connection)
        .await
        .unwrap();
    let header = sample_thread("ws_task", "canonical-thread", AT);
    store
        .upsert_thread_model(&header, PersistedActorRef::System)
        .await
        .unwrap();
    let db = store.database_connection();
    turn::upsert_turn(
        &db,
        "locator-turn",
        &header.id,
        &sample_turn("locator-turn"),
        None,
        None,
        unix_to_datetime(AT),
        unix_to_datetime(AT),
    )
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    crate::repositories::task_child_work::enqueue_point(&tx, "stale-thread", "locator-turn", AT)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let candidate = point::Entity::find_by_id("locator-turn")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let claim = store
        .claim_child_point(&candidate, AT)
        .await
        .unwrap()
        .unwrap();
    // A refused conditional repair is an error, never permission for the
    // caller's no-record ACK. The original lease must remain intact.
    db.execute_unprepared("CREATE TRIGGER test_ignore_locator_repair BEFORE UPDATE ON task_child_reconcile_pending WHEN OLD.turn_id='locator-turn' AND NEW.thread_id='canonical-thread' BEGIN SELECT RAISE(IGNORE); END").await.unwrap();
    assert!(store.child_point_record(&claim).await.is_err());
    assert_eq!(
        point::Entity::find_by_id("locator-turn")
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        claim
    );
    db.execute_unprepared("DROP TRIGGER test_ignore_locator_repair")
        .await
        .unwrap();
    assert!(store.child_point_record(&claim).await.unwrap().is_none());
    assert!(!store.finish_child_point(&claim, None, true).await.unwrap());
    let repaired = point::Entity::find_by_id("locator-turn")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repaired.thread_id, header.id);
    assert_ne!(repaired.revision, claim.revision);
    assert!(repaired.next_attempt_at.is_some());
    assert!(repaired.claim_token.is_none());
    assert_eq!(repaired.attempt_count, 0);
    assert!(
        store
            .discover_child_points(i64::MAX / 4, 32)
            .await
            .unwrap()
            .iter()
            .any(|row| row.turn_id == repaired.turn_id && row.revision == repaired.revision)
    );
}

#[tokio::test]
async fn old_wrong_locator_claim_cannot_rotate_new_canonical_revision_or_lease() {
    let store = test_store_with_workspace("ws_task").await;
    pioneer_sqlite::apply_sqlite_pragmas(&store.connection)
        .await
        .unwrap();
    let db = store.database_connection();
    for id in ["claim-thread-a", "claim-thread-b"] {
        store
            .upsert_thread_model(&sample_thread("ws_task", id, AT), PersistedActorRef::System)
            .await
            .unwrap();
    }
    let mut terminal = sample_turn("moved-claim-turn");
    terminal.status = TurnStatus::Completed;
    let init = db.begin().await.unwrap();
    turn::upsert_turn(
        &init,
        &terminal.id,
        "claim-thread-a",
        &terminal,
        None,
        None,
        unix_to_datetime(AT),
        unix_to_datetime(AT),
    )
    .await
    .unwrap();
    init.commit().await.unwrap();
    let first = point::Entity::find_by_id(&terminal.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let old_claim = store
        .claim_child_point(&first, i64::MAX / 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old_claim.thread_id, "claim-thread-a");

    // A real domain move publishes B while the old consumer still holds A.
    let moved = db.begin().await.unwrap();
    turn::upsert_turn(
        &moved,
        &terminal.id,
        "claim-thread-b",
        &terminal,
        None,
        None,
        unix_to_datetime(AT),
        unix_to_datetime(AT + 1),
    )
    .await
    .unwrap();
    moved.commit().await.unwrap();
    let canonical = point::Entity::find_by_id(&terminal.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let new_claim = store
        .claim_child_point(&canonical, i64::MAX / 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new_claim.thread_id, "claim-thread-b");
    assert_ne!(new_claim.revision, old_claim.revision);
    assert!(new_claim.claim_token.is_some());
    assert_ne!(new_claim.claim_token, old_claim.claim_token);
    assert!(
        store
            .child_point_record(&old_claim)
            .await
            .unwrap()
            .is_none()
    );
    let retained = point::Entity::find_by_id(&terminal.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained, new_claim,
        "old consumer must preserve every field of the newer lease"
    );
    assert!(
        !store
            .finish_child_point(&old_claim, None, true)
            .await
            .unwrap()
    );
    assert_eq!(
        point::Entity::find_by_id(&terminal.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        new_claim
    );
}

#[cfg(feature = "mysql")]
async fn remote_capsule_parent<C: sea_orm::ConnectionTrait>(db: &C, id: &str, thread_id: &str) {
    let header = sample_thread("ws_task", thread_id, AT);
    crate::repositories::thread::upsert_thread_with_creator(
        db,
        &header,
        &PersistedActorRef::System,
        unix_to_datetime(AT),
        unix_to_datetime(AT),
    )
    .await
    .unwrap();
    turn::upsert_turn(
        db,
        id,
        thread_id,
        &sample_turn(id),
        None,
        None,
        unix_to_datetime(AT),
        unix_to_datetime(AT),
    )
    .await
    .unwrap();
}

#[cfg(feature = "mysql")]
async fn remote() -> sea_orm::DatabaseConnection {
    let url = std::env::var("PIONEER_DISPOSABLE_MYSQL_TEST_URL")
        .expect("explicit disposable provisioned MySQL schema required");
    assert!(url.starts_with("mysql://"));
    sea_orm::Database::connect(url).await.unwrap()
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires disposable provisioned MySQL; closed validation gate"]
async fn mysql_rr_workspace_restore_after_drained_scope_publishes_again() {
    let db = remote().await;
    let id = pioneer_protocol::generate_id(21);
    let a = sample_thread("ws_task", &id, AT);
    let mut b = a.clone();
    // The remote fixture also provisions workspace ws_task_b.
    b.workspace_id = "ws_task_b".into();
    async fn write(db: &impl sea_orm::ConnectionTrait, row: &Thread) {
        crate::repositories::thread::upsert_thread_with_creator(
            db,
            row,
            &PersistedActorRef::System,
            unix_to_datetime(AT),
            unix_to_datetime(AT),
        )
        .await
        .unwrap();
    }
    let init = db.begin().await.unwrap();
    write(&init, &a).await;
    init.commit().await.unwrap();
    let stale = db
        .begin_with_config(Some(sea_orm::IsolationLevel::RepeatableRead), None)
        .await
        .unwrap();
    let observed = pioneer_entity::thread::Entity::find_by_id(&id)
        .one(&stale)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.workspace_id, a.workspace_id);
    let other = db.begin().await.unwrap();
    write(&other, &b).await;
    other.commit().await.unwrap();
    // Model a fully drained B invalidation, before the delayed A write.
    scope::Entity::delete_many()
        .filter(scope::Column::ScopeKey.eq(&id))
        .exec(&db)
        .await
        .unwrap();
    write(&stale, &a).await;
    stale.commit().await.unwrap();
    for kind in ["child-thread", "creator-thread", "parent-thread"] {
        assert!(
            scope::Entity::find_by_id((kind.to_owned(), id.clone()))
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires disposable provisioned MySQL; closed validation gate"]
async fn mysql_rr_late_source_commit_after_other_publication_ack_keeps_ticket() {
    let db = remote().await;
    for turn_existed in [false, true] {
        let id = pioneer_protocol::generate_id(21);
        let thread_id = pioneer_protocol::generate_id(21);
        let header = sample_thread("ws_task", &thread_id, AT);
        let init = db.begin().await.unwrap();
        crate::repositories::thread::upsert_thread_with_creator(
            &init,
            &header,
            &PersistedActorRef::System,
            unix_to_datetime(AT),
            unix_to_datetime(AT),
        )
        .await
        .unwrap();
        if turn_existed {
            let mut pending = sample_turn(&id);
            pending.status = TurnStatus::InProgress;
            turn::upsert_turn(
                &init,
                &id,
                &thread_id,
                &pending,
                None,
                None,
                unix_to_datetime(AT),
                unix_to_datetime(AT),
            )
            .await
            .unwrap();
        }
        init.commit().await.unwrap();
        let source = db
            .begin_with_config(Some(sea_orm::IsolationLevel::RepeatableRead), None)
            .await
            .unwrap();
        // Establish the source snapshot with either absent or nonterminal Turn.
        let before = pioneer_entity::turn::Entity::find_by_id(&id)
            .one(&source)
            .await
            .unwrap();
        assert_eq!(before.is_some(), turn_existed);
        let other = db.begin().await.unwrap();
        let mut terminal = sample_turn(&id);
        terminal.status = TurnStatus::Completed;
        turn::upsert_turn(
            &other,
            &id,
            &thread_id,
            &terminal,
            None,
            None,
            unix_to_datetime(AT),
            unix_to_datetime(AT),
        )
        .await
        .unwrap();
        producers::point(&other, &thread_id, &id).await.unwrap();
        other.commit().await.unwrap();
        // The no-capsule ACK drains the prior publication before source commit.
        point::Entity::delete_by_id(&id).exec(&db).await.unwrap();
        context::insert_turn_llm_context(&source, entry(&id, 1))
            .await
            .unwrap();
        source.commit().await.unwrap();
        let point = point::Entity::find_by_id(&id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(point.next_attempt_at.is_some());
        assert_eq!(point.thread_id, thread_id);
    }
}

#[tokio::test]
async fn ambiguous_transport_outcome_is_not_replayed() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let store = test_store_with_workspace("ws_task").await;
    pioneer_sqlite::apply_sqlite_pragmas(&store.connection)
        .await
        .unwrap();
    let attempts = AtomicUsize::new(0);
    let result: anyhow::Result<()> = store
        .run_serialized_write(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(
                    sea_orm::DbErr::Exec(sea_orm::RuntimeErr::SqlxError(std::sync::Arc::new(
                        sea_orm::sqlx::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "unknown commit outcome",
                        )),
                    )))
                    .into(),
                )
            }
        })
        .await;
    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires disposable provisioned MySQL; closed validation gate"]
async fn mysql_numeric_timeout_rolls_back_entire_transaction_before_replay() {
    use sea_orm::sea_query::Expr;
    use sea_orm::{ConnectionTrait, QuerySelect};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let db = remote().await;
    let id = pioneer_protocol::generate_id(21);
    let row = sample_thread("ws_task", &id, AT);
    let init = db.begin().await.unwrap();
    crate::repositories::thread::upsert_thread_with_creator(
        &init,
        &row,
        &PersistedActorRef::System,
        unix_to_datetime(AT),
        unix_to_datetime(AT),
    )
    .await
    .unwrap();
    // Keep the context's real Turn on a different Thread from the lock
    // target, so FK/header locking cannot cause the timeout prematurely.
    remote_capsule_parent(&init, &id, &pioneer_protocol::generate_id(21)).await;
    init.commit().await.unwrap();
    let owner = db.begin().await.unwrap();
    pioneer_entity::thread::Entity::find_by_id(&id)
        .lock_exclusive()
        .one(&owner)
        .await
        .unwrap()
        .unwrap();
    let owner = Arc::new(tokio::sync::Mutex::new(Some(owner)));
    let attempts = AtomicUsize::new(0);
    // Use the actual owning retry loop, with real driver errors. Its SQLite
    // handle only supplies the retry policy; all remote writes own their Tx.
    let store = test_store_with_workspace("ws_task").await;
    pioneer_sqlite::apply_sqlite_pragmas(&store.connection)
        .await
        .unwrap();
    store
        .run_serialized_write(|| {
            let owner = owner.clone();
            let db = &db;
            let id = &id;
            let attempts = &attempts;
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let tx = db
                    .begin_with_config(Some(sea_orm::IsolationLevel::RepeatableRead), None)
                    .await?;
                tx.execute_unprepared("SET SESSION innodb_lock_wait_timeout = 1")
                    .await?;
                assert!(
                    pioneer_entity::turn_llm_context::Entity::find()
                        .filter(pioneer_entity::turn_llm_context::Column::TurnId.eq(id))
                        .one(&tx)
                        .await?
                        .is_none(),
                    "previous attempt's earlier statements must have rolled back"
                );
                context::insert_turn_llm_context(&tx, entry(id, 1)).await?;
                if attempt == 0 {
                    let error = pioneer_entity::thread::Entity::update_many()
                        .filter(pioneer_entity::thread::Column::Id.eq(id))
                        .col_expr(pioneer_entity::thread::Column::Name, Expr::value("blocked"))
                        .exec(&tx)
                        .await
                        .unwrap_err();
                    let wrapped: anyhow::Error = error.into();
                    assert!(crate::repositories::task_child_work::is_transaction_retry(
                        &wrapped
                    ));
                    // MySQL timeout can abort only the statement. Explicit rollback
                    // here models the required owner teardown before retry.
                    tx.rollback().await?;
                    let held = owner.lock().await.take().unwrap();
                    held.rollback().await?;
                    return Err(wrapped);
                }
                tx.commit().await?;
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires disposable provisioned MySQL; closed validation gate"]
async fn mysql_driver_sqlstate_and_numeric_error_contract() {
    use sea_orm::ConnectionTrait;
    let db = remote().await;
    for (state, number, retry) in [
        ("HY000", 1205, true),
        ("40001", 1213, true),
        ("HY000", 1213, true),
        ("HY000", 1644, false),
    ] {
        // Server SIGNAL constructs genuine driver error packets. This checks
        // normalization, distinct from the real lock-timeout boundary above.
        let sql = format!(
            "SIGNAL SQLSTATE '{state}' SET MYSQL_ERRNO = {number}, MESSAGE_TEXT = 'classifier fixture'"
        );
        let error = db.execute_unprepared(&sql).await.unwrap_err();
        let error: anyhow::Error = error.into();
        assert_eq!(
            crate::repositories::task_child_work::is_transaction_retry(&error),
            retry
        );
    }
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires disposable provisioned MySQL; closed validation gate"]
async fn mysql_rr_context_recreation_after_cleanup_is_birth_not_payload_append() {
    let db = remote().await;
    let id = pioneer_protocol::generate_id(21);
    let init = db.begin().await.unwrap();
    remote_capsule_parent(&init, &id, &pioneer_protocol::generate_id(21)).await;
    context::insert_turn_llm_context(&init, entry(&id, 1))
        .await
        .unwrap();
    init.commit().await.unwrap();
    let source = db
        .begin_with_config(Some(sea_orm::IsolationLevel::RepeatableRead), None)
        .await
        .unwrap();
    assert!(
        pioneer_entity::turn_llm_context::Entity::find()
            .filter(pioneer_entity::turn_llm_context::Column::TurnId.eq(&id))
            .one(&source)
            .await
            .unwrap()
            .is_some()
    );
    // Exercise the source-side consequence of a committed cleanup. Domain
    // authorization itself has separate primary cleanup regressions.
    let cleanup = db.begin().await.unwrap();
    context::delete_turn_llm_context_for_turn(&cleanup, &id)
        .await
        .unwrap();
    point::Entity::delete_by_id(&id)
        .exec(&cleanup)
        .await
        .unwrap();
    cleanup.commit().await.unwrap();
    context::insert_turn_llm_context(&source, entry(&id, 2))
        .await
        .unwrap();
    source.commit().await.unwrap();
    assert!(
        point::Entity::find_by_id(&id)
            .one(&db)
            .await
            .unwrap()
            .is_some(),
        "a stale pre-cleanup presence must not suppress the new birth"
    );
}
