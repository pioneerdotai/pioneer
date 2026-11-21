use super::FrozenStorageLifetimeProgress;
use crate::store::CompactionStore;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

async fn scalar(store: &CompactionStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(
            store.database_connection().get_database_backend(),
            sql,
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

#[pioneer_postgres_test::postgres_test]
async fn cancelled_frozen_conversion_reopens_with_a_read_only_pool() {
    let mut frozen_conversion_progress = FrozenStorageLifetimeProgress::default();
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    crate::plan_test_support::seed_revision_fixture(&writer, false).await;
    let reader = fixture.connect_read_only().await.unwrap();
    let database = SqliteDatabase::new(reader, writer);
    assert!(database.reader_query_only_enabled().await.unwrap());
    let store =
        CompactionStore::new(database.clone(), Default::default()).with_maintenance_access();
    // Complete bounded scope discovery while no frozen rows exist. The
    // cancellation below then waits for a real conversion writer, not a
    // metadata-only prerequisite read.
    for _ in 0..64 {
        store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap();
    }
    let reference: pioneer_compaction::frozen::FrozenMessageRef = serde_json::from_value(serde_json::json!({
        "logical_turn_id":"turn", "source_thread":"thread", "context_thread":null, "unit_id":"u",
        "sources":[{"scope":"event:turn","id":"event-1","version":"event-revision:1"}],
        "inherited":false,"complete":true,"protected_input":false,"wire_sha256":"a".repeat(64),
        "replay_source":null,"tool_call_id":null,"tool_name":null
    })).unwrap();
    for id in ["storage-a", "storage-b"] {
        let descriptor = pioneer_compaction::frozen::FrozenHistoryRef {
            format: 1,
            manifest_id: id.into(),
            messages: 1,
            identity_sha256: "b".repeat(64),
        };
        store
            .compaction_begin_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap();
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                id,
                0,
                std::slice::from_ref(&reference),
            )
            .await
            .unwrap();
        assert!(
            store
                .compaction_finish_frozen_history("ws", "thread", &descriptor)
                .await
                .unwrap()
        );
    }
    let layouts_before = scalar(&store, "SELECT count(*) AS n FROM compaction_frozen_layout").await;
    let held = database.begin().await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            store.compact_frozen_storage_quantum(&mut frozen_conversion_progress)
        )
        .await
        .is_err()
    );
    held.rollback().await.unwrap();
    assert_eq!(
        scalar(&store, "SELECT count(*) AS n FROM compaction_frozen_layout").await,
        layouts_before
    );
    for _ in 0..4 {
        assert!(
            store
                .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
                .await
                .unwrap()
        );
    }
    drop(store);
    drop(database);
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let reopened = CompactionStore::new(SqliteDatabase::new(reader, writer), Default::default())
        .with_maintenance_access();
    let mut frozen_conversion_progress = FrozenStorageLifetimeProgress::default();
    for _ in 0..512 {
        reopened
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap();
    }
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) AS n FROM compaction_frozen_message_data"
        )
        .await,
        1
    );
    for id in ["storage-a", "storage-b"] {
        assert_eq!(
            reopened
                .compaction_frozen_history_page("ws", "thread", id, 0)
                .await
                .unwrap(),
            vec![reference.clone()]
        );
    }
}
