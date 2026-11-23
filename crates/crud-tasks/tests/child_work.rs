use pioneer_crud_tasks::child_work::{
    claim_child_point, discover_child_points, enqueue_point, finish_child_point,
};
use pioneer_entity::task_child_reconcile_pending as point;
use sea_orm::{EntityTrait, TransactionTrait};

#[tokio::test]
async fn point_crash_before_wake_expiry_and_new_revision_reject_stale_ack() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        let reader = fixture.connect_read_only().await.unwrap();
        let tx = db.begin().await.unwrap();
        enqueue_point(&tx, "thread", "turn", 100).await.unwrap();
        tx.commit().await.unwrap(); // Intentionally no wake.
        let candidate = discover_child_points(&reader, 100, 1)
            .await
            .unwrap()
            .remove(0);
        let first = claim_child_point(db, &candidate, 100)
            .await
            .unwrap()
            .unwrap();
        assert!(
            discover_child_points(&reader, 159, 1)
                .await
                .unwrap()
                .is_empty()
        );
        let expired = discover_child_points(&reader, 160, 1)
            .await
            .unwrap()
            .remove(0);
        let second = claim_child_point(db, &expired, 160).await.unwrap().unwrap();
        assert!(!finish_child_point(db, &first, None, true).await.unwrap());
        let tx = db.begin().await.unwrap();
        enqueue_point(&tx, "thread", "turn", 161).await.unwrap();
        tx.commit().await.unwrap();
        assert!(!finish_child_point(db, &second, None, true).await.unwrap());
        assert_eq!(
            discover_child_points(&reader, 161, 1).await.unwrap().len(),
            1
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn parked_dependency_repair_and_delete_reinsert_fence() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        let reader = fixture.connect_read_only().await.unwrap();
        let tx = db.begin().await.unwrap();
        enqueue_point(&tx, "t", "x", 1).await.unwrap();
        tx.commit().await.unwrap();
        let row = discover_child_points(&reader, 1, 1)
            .await
            .unwrap()
            .remove(0);
        let claim = claim_child_point(db, &row, 1).await.unwrap().unwrap();
        assert!(finish_child_point(db, &claim, None, false).await.unwrap());
        assert!(
            discover_child_points(&reader, 1000, 1)
                .await
                .unwrap()
                .is_empty()
        );
        let tx = db.begin().await.unwrap();
        point::Entity::delete_by_id("x").exec(&tx).await.unwrap();
        enqueue_point(&tx, "t", "x", 2).await.unwrap();
        tx.commit().await.unwrap();
        assert!(!finish_child_point(db, &claim, None, true).await.unwrap());
        assert_eq!(discover_child_points(&reader, 2, 1).await.unwrap().len(), 1);
        Ok(())
    })
    .await
    .unwrap();
}

mod scope_page_regressions {
    use pioneer_crud_tasks::child_scopes::{ChildScope, discover_child_scope_after, enqueue_scope};
    use pioneer_crud_tasks::child_work::{discover_child_points, enqueue_point};
    use pioneer_entity::task_child_reconcile_scope as scope;
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};

    #[tokio::test]
    async fn producer_rollback_does_not_publish_work() {
        pioneer_postgres_test::with_database(async |fixture| {
            let db = fixture.connection();
            let tx = db.begin().await.unwrap();
            enqueue_point(&tx, "t", "x", 1).await.unwrap();
            enqueue_scope(&tx, ChildScope::Task("a".into()), 1)
                .await
                .unwrap();
            tx.rollback().await.unwrap();
            assert!(discover_child_points(db, 100, 32).await.unwrap().is_empty());
            assert!(
                discover_child_scope_after(db, 100, None)
                    .await
                    .unwrap()
                    .is_none()
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn continuous_invalidations_keep_active_pass_lease_and_cursor() {
        pioneer_postgres_test::with_database(async |fixture| {
            let db = fixture.connection();
            let tx = db.begin().await.unwrap();
            enqueue_scope(&tx, ChildScope::Task("a".into()), 1)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let before = scope::Entity::find_by_id(("task".to_string(), "a".to_string()))
                .one(db)
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
                    .one(db)
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
                .exec(db)
                .await
                .unwrap();
            for now in 100..118 {
                let tx = db.begin().await.unwrap();
                enqueue_scope(&tx, ChildScope::Task("a".into()), now)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                let due = discover_child_scope_after(db, 99, None)
                    .await
                    .unwrap()
                    .expect("new invalidation must retain already-due eligibility");
                assert_eq!(due.next_attempt_at, Some(1));
                assert_eq!(due.cursor.as_deref(), Some("middle"));
                assert_eq!(due.pass_revision.as_ref(), Some(&before.requested_revision));
            }
            Ok(())
        })
        .await
        .unwrap();
    }
}
