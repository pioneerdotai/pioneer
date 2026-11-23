use pioneer_crud_conversations::thread_lineage::{find_lineage_by_child_thread, upsert_lineage};
use pioneer_protocol::TaskThreadLineage;
use sea_orm::TransactionTrait;

#[pioneer_postgres_test::postgres_test]
async fn reviewer_lineage_can_precede_thread_creation_and_deduplicates_replays() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let connection = fixture.connection();
    let mut lineage = TaskThreadLineage {
        child_thread_id: "reviewer-child".into(),
        parent_thread_id: "parent".into(),
        root_thread_id: "root".into(),
        depth: 1,
        origin_kind: Some("task_review".into()),
        created_by_thread_id: Some("parent".into()),
        created_by_turn_id: Some("origin".into()),
        created_at: 1_700_000_000,
    };

    let abandoned = connection.begin().await.unwrap();
    assert!(upsert_lineage(&abandoned, &lineage).await.unwrap());
    abandoned.rollback().await.unwrap();
    assert!(
        find_lineage_by_child_thread(connection, &lineage.child_thread_id)
            .await
            .unwrap()
            .is_none()
    );

    let transaction = connection.begin().await.unwrap();
    assert!(upsert_lineage(&transaction, &lineage).await.unwrap());
    assert!(!upsert_lineage(&transaction, &lineage).await.unwrap());
    transaction.commit().await.unwrap();

    lineage.created_by_turn_id = None;
    let transaction = connection.begin().await.unwrap();
    assert!(upsert_lineage(&transaction, &lineage).await.unwrap());
    assert!(!upsert_lineage(&transaction, &lineage).await.unwrap());
    transaction.commit().await.unwrap();
    let row = find_lineage_by_child_thread(connection, &lineage.child_thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.parent_thread_id, lineage.parent_thread_id);
    assert_eq!(row.created_by_turn_id, None);
    assert_eq!(row.created_at.timestamp(), lineage.created_at);
}
