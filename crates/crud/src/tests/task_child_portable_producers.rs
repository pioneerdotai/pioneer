//! Publication, before-image and PostgreSQL retry regressions for child work.
use super::*;
use pioneer_crud_tasks::task_child_producers as producers;
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};

const AT: i64 = 1_700_000_000;

fn entry(
    id: &str,
    sequence: i64,
) -> pioneer_crud_conversations::turn_context::NewTurnLlmContextEntry {
    pioneer_crud_conversations::turn_context::NewTurnLlmContextEntry {
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
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_task").await;

        let db = store.database_connection();
        // No Turn and no Link: publication must retain an obligation even when
        // neither cross-table dependency exists in the source transaction.
        let tx = db.begin().await.unwrap();
        crate::store::conversations::context_writes::insert_turn_llm_context(
            &tx,
            entry("late-birth", 1),
        )
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
        pioneer_crud_conversations::context_cleanup::delete_turn_llm_context_for_turn(
            &tx,
            "late-birth",
        )
        .await
        .unwrap();
        crate::store::conversations::context_writes::insert_turn_llm_context(
            &tx,
            entry("late-birth", 2),
        )
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
        crate::store::conversations::context_writes::insert_turn_llm_context(
            &tx,
            entry("late-birth", 3),
        )
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stale_advisory_locator_is_revised_before_no_record_ack() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_task").await;

        let header = sample_thread("ws_task", "canonical-thread", AT);
        store
            .upsert_thread_model(&header, PersistedActorRef::System)
            .await
            .unwrap();
        let db = store.database_connection();
        let tx = db.begin().await.unwrap();
        crate::store::conversations::turn_writes::upsert_turn(
            &tx,
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
        pioneer_crud_tasks::child_work::enqueue_point(&tx, "stale-thread", "locator-turn", AT)
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
        db.execute_unprepared("CREATE FUNCTION test_ignore_locator_repair() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$; CREATE TRIGGER test_ignore_locator_repair BEFORE UPDATE ON task_child_reconcile_pending FOR EACH ROW WHEN (OLD.turn_id='locator-turn' AND NEW.thread_id='canonical-thread') EXECUTE FUNCTION test_ignore_locator_repair()").await.unwrap();
        assert!(store.child_point_record(&claim).await.is_err());
        assert_eq!(
            point::Entity::find_by_id("locator-turn")
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            claim
        );
        db.execute_unprepared("DROP TRIGGER test_ignore_locator_repair ON task_child_reconcile_pending; DROP FUNCTION test_ignore_locator_repair()")
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
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn old_wrong_locator_claim_cannot_rotate_new_canonical_revision_or_lease() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_task").await;

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
        crate::store::conversations::turn_writes::upsert_turn(
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
        crate::store::conversations::turn_writes::upsert_turn(
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
        Ok(())
    })
    .await
    .unwrap();
}

async fn seed_capsule_parent<C: sea_orm::ConnectionTrait>(db: &C, id: &str, thread_id: &str) {
    let header = sample_thread("ws_task", thread_id, AT);
    crate::store::conversations::thread_writes::upsert_thread_with_creator(
        db,
        &header,
        &PersistedActorRef::System,
        unix_to_datetime(AT),
        unix_to_datetime(AT),
    )
    .await
    .unwrap();
    crate::store::conversations::turn_writes::upsert_turn(
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

#[pioneer_postgres_test::postgres_test]
async fn postgres_workspace_restore_after_drained_scope_publishes_again() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    postgres_store_with_workspace(&fixture, "ws_task").await;
    let db = fixture.connection();
    db.execute_unprepared(
        "INSERT INTO workspace (id,name) VALUES ('ws_task_b','Second workspace')",
    )
    .await
    .unwrap();
    let id = pioneer_protocol::generate_id(21);
    let a = sample_thread("ws_task", &id, AT);
    let mut b = a.clone();
    b.workspace_id = "ws_task_b".into();
    async fn write(db: &impl sea_orm::ConnectionTrait, row: &Thread) -> anyhow::Result<()> {
        crate::store::conversations::thread_writes::upsert_thread_with_creator(
            db,
            row,
            &PersistedActorRef::System,
            unix_to_datetime(AT),
            unix_to_datetime(AT),
        )
        .await
    }
    let init = db.begin().await.unwrap();
    write(&init, &a).await.unwrap();
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
    write(&other, &b).await.unwrap();
    other.commit().await.unwrap();
    // Model a fully drained B invalidation, before the delayed A write.
    scope::Entity::delete_many()
        .filter(scope::Column::ScopeKey.eq(&id))
        .exec(db)
        .await
        .unwrap();
    let conflict = write(&stale, &a).await.unwrap_err();
    assert!(pioneer_sqlite::is_retryable_database_conflict(&conflict));
    stale.rollback().await.unwrap();
    let restored = db.begin().await.unwrap();
    write(&restored, &a).await.unwrap();
    restored.commit().await.unwrap();
    for kind in ["child-thread", "creator-thread", "parent-thread"] {
        assert!(
            scope::Entity::find_by_id((kind.to_owned(), id.clone()))
                .one(db)
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn postgres_late_source_commit_after_other_publication_ack_keeps_ticket() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let store = postgres_store_with_workspace(&fixture, "ws_task").await;
    let db = fixture.connection();
    for turn_existed in [false, true] {
        let id = pioneer_protocol::generate_id(21);
        let thread_id = pioneer_protocol::generate_id(21);
        let header = sample_thread("ws_task", &thread_id, AT);
        let init = db.begin().await.unwrap();
        crate::store::conversations::thread_writes::upsert_thread_with_creator(
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
            crate::store::conversations::turn_writes::upsert_turn(
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
        // Establish the snapshot before the other transaction publishes and ACKs.
        let before = pioneer_entity::turn::Entity::find_by_id(&id)
            .one(&source)
            .await
            .unwrap();
        assert_eq!(before.is_some(), turn_existed);
        let other = db.begin().await.unwrap();
        let mut terminal = sample_turn(&id);
        terminal.status = TurnStatus::Completed;
        crate::store::conversations::turn_writes::upsert_turn(
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
        point::Entity::delete_by_id(&id).exec(db).await.unwrap();
        let birth = crate::store::conversations::context_writes::insert_turn_llm_context(
            &source,
            entry(&id, 1),
        )
        .await;
        if turn_existed {
            let conflict = birth.unwrap_err();
            assert!(pioneer_sqlite::is_retryable_database_conflict(&conflict));
            source.rollback().await.unwrap();
            let retry = db.begin().await.unwrap();
            crate::store::conversations::context_writes::insert_turn_llm_context(
                &retry,
                entry(&id, 1),
            )
            .await
            .unwrap();
            retry.commit().await.unwrap();
        } else {
            birth.unwrap();
            source.commit().await.unwrap();
        }
        let pending = point::Entity::find_by_id(&id)
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert!(pending.next_attempt_at.is_some());
        if !turn_existed {
            assert!(
                pending.thread_id.is_empty(),
                "stale snapshot cannot bind the new Turn"
            );
            let claim = store
                .claim_child_point(&pending, i64::MAX / 2)
                .await
                .unwrap()
                .unwrap();
            assert!(store.child_point_record(&claim).await.unwrap().is_none());
            let rebound = point::Entity::find_by_id(&id)
                .one(db)
                .await
                .unwrap()
                .unwrap();
            assert!(rebound.next_attempt_at.is_some());
            assert_ne!(rebound.revision, pending.revision);
            assert_eq!(rebound.thread_id, thread_id);
        } else {
            assert_eq!(pending.thread_id, thread_id);
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn postgres_serialization_abort_rolls_back_entire_transaction_before_replay() {
    use sea_orm::sea_query::Expr;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let store = postgres_store_with_workspace(&fixture, "ws_task").await;
    let db = fixture.connection();
    let id = pioneer_protocol::generate_id(21);
    let init = db.begin().await.unwrap();
    let row = sample_thread("ws_task", &id, AT);
    crate::store::conversations::thread_writes::upsert_thread_with_creator(
        &init,
        &row,
        &PersistedActorRef::System,
        unix_to_datetime(AT),
        unix_to_datetime(AT),
    )
    .await
    .unwrap();
    // Context belongs to a different Thread from the row revised concurrently.
    seed_capsule_parent(&init, &id, &pioneer_protocol::generate_id(21)).await;
    init.commit().await.unwrap();
    let attempts = AtomicUsize::new(0);
    let competing = fixture.connect().await.unwrap();
    let competing = Arc::new(competing);
    store
        .run_serialized_write(|| {
            let id = &id;
            let attempts = &attempts;
            let competing = competing.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let tx = db
                    .begin_with_config(Some(sea_orm::IsolationLevel::RepeatableRead), None)
                    .await?;
                assert!(
                    pioneer_entity::turn_llm_context::Entity::find()
                        .filter(pioneer_entity::turn_llm_context::Column::TurnId.eq(id))
                        .one(&tx)
                        .await?
                        .is_none(),
                    "the previous attempt's context write must roll back",
                );
                crate::store::conversations::context_writes::insert_turn_llm_context(
                    &tx,
                    entry(id, 1),
                )
                .await?;
                if attempt == 0 {
                    pioneer_entity::thread::Entity::update_many()
                        .filter(pioneer_entity::thread::Column::Id.eq(id))
                        .col_expr(
                            pioneer_entity::thread::Column::Name,
                            Expr::value("concurrent edit"),
                        )
                        .exec(competing.as_ref())
                        .await?;
                    let error: anyhow::Error = pioneer_entity::thread::Entity::update_many()
                        .filter(pioneer_entity::thread::Column::Id.eq(id))
                        .col_expr(
                            pioneer_entity::thread::Column::Name,
                            Expr::value("obsolete edit"),
                        )
                        .exec(&tx)
                        .await
                        .unwrap_err()
                        .into();
                    assert!(pioneer_sqlite::is_retryable_database_conflict(&error));
                    tx.rollback().await?;
                    return Err(error);
                }
                tx.commit().await?;
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    competing.close_by_ref().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn postgres_context_recreation_after_cleanup_is_birth_not_payload_append() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    postgres_store_with_workspace(&fixture, "ws_task").await;
    let db = fixture.connection();
    let id = pioneer_protocol::generate_id(21);
    let init = db.begin().await.unwrap();
    seed_capsule_parent(&init, &id, &pioneer_protocol::generate_id(21)).await;
    crate::store::conversations::context_writes::insert_turn_llm_context(&init, entry(&id, 1))
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
    pioneer_crud_conversations::context_cleanup::delete_turn_llm_context_for_turn(&cleanup, &id)
        .await
        .unwrap();
    point::Entity::delete_by_id(&id)
        .exec(&cleanup)
        .await
        .unwrap();
    cleanup.commit().await.unwrap();
    let conflict = crate::store::conversations::context_writes::insert_turn_llm_context(
        &source,
        entry(&id, 2),
    )
    .await
    .unwrap_err();
    assert!(pioneer_sqlite::is_retryable_database_conflict(&conflict));
    source.rollback().await.unwrap();
    let retry = db.begin().await.unwrap();
    crate::store::conversations::context_writes::insert_turn_llm_context(&retry, entry(&id, 2))
        .await
        .unwrap();
    retry.commit().await.unwrap();
    assert!(
        point::Entity::find_by_id(&id)
            .one(db)
            .await
            .unwrap()
            .is_some(),
        "a stale pre-cleanup presence must not suppress the new birth"
    );
}
