use pioneer_crud::CrudStore;
use pioneer_memory::{
    BackendDeleteRequest, BackendGetRequest, BackendPutRequest, MemoryBackend,
    PostgresMemoryBackend,
};
use pioneer_postgres_test::{TestDatabase, with_database};
use pioneer_protocol::{
    MemoryCategory, MemoryScope, MemoryScopeKind, MemorySensitivity, MemoryStatus,
};
use sea_orm::ConnectionTrait;
use std::sync::Arc;

async fn store(database: &TestDatabase) -> Arc<CrudStore> {
    let writer = database.connect().await.unwrap();
    let reader = database.connect_read_only().await.unwrap();
    Arc::new(CrudStore::new(pioneer_sqlite::SqliteDatabase::new(
        reader, writer,
    )))
}
fn scope(key: &str) -> MemoryScope {
    MemoryScope {
        kind: MemoryScopeKind::User,
        key: key.into(),
    }
}
fn put_request(memory_id: &str, scope: MemoryScope, key: &str, content: &str) -> BackendPutRequest {
    BackendPutRequest {
        memory_id: memory_id.to_owned(),
        scope,
        namespace: None,
        category: MemoryCategory::Identity,
        key: Some(key.to_owned()),
        content: content.to_owned(),
        sensitivity: MemorySensitivity::Normal,
        metadata_json: None,
        source_thread_id: Some("thread_backend".to_owned()),
        source_turn_id: Some("turn_backend".to_owned()),
        source_item_id: Some("item_backend".to_owned()),
        created_by_kind: None,
        created_by_id: None,
        policy_version: "test-policy".to_owned(),
        status: MemoryStatus::Active,
        idempotency_key: None,
    }
}

#[tokio::test]
async fn complete_payload_updates_are_durable_and_scope_bound() {
    with_database(async |database| {
        let store = store(database).await;
        let backend = PostgresMemoryBackend::new(store.clone());
        let first = "完整内容 Unicode ".repeat(100);
        let put = backend
            .put(put_request("payload", scope("owner"), "fact", &first))
            .await
            .unwrap();
        assert_eq!(put.payload_version, 1);
        let reopened = PostgresMemoryBackend::new(self::store(database).await);
        let get = || BackendGetRequest {
            memory_id: "payload".into(),
            scope: scope("owner"),
            scope_key_hash: None,
        };
        assert_eq!(reopened.get(get()).await.unwrap().unwrap().content, first);
        assert!(
            reopened
                .get(BackendGetRequest {
                    scope: scope("other"),
                    ..get()
                })
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reopened
                .put(put_request(
                    "payload",
                    scope("other"),
                    "fact",
                    "scope takeover"
                ))
                .await
                .is_err()
        );
        let updated = reopened
            .put(put_request(
                "payload",
                scope("owner"),
                "fact",
                "replacement full body",
            ))
            .await
            .unwrap();
        assert_eq!(updated.payload_version, 2);
        assert_eq!(
            backend.get(get()).await.unwrap().unwrap().content,
            "replacement full body"
        );
        assert!(
            !backend
                .delete(BackendDeleteRequest {
                    memory_id: "payload".into(),
                    scope: scope("other"),
                    scope_key_hash: None
                })
                .await
                .unwrap()
                .deleted
        );
        let delete = || BackendDeleteRequest {
            memory_id: "payload".into(),
            scope: scope("owner"),
            scope_key_hash: None,
        };
        assert!(backend.delete(delete()).await.unwrap().deleted);
        assert!(!backend.delete(delete()).await.unwrap().deleted);
        assert!(reopened.get(get()).await.unwrap().is_none());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_update_preserves_previous_complete_payload() {
    with_database(async |database| {
    let store = store(database).await;
    let backend = PostgresMemoryBackend::new(store.clone());
    backend
        .put(put_request("payload", scope("owner"), "fact", "original"))
        .await
        .unwrap();
    store.database_connection().execute_unprepared("CREATE FUNCTION reject_payload_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected update failure'; END; $$;
         CREATE TRIGGER reject_payload_update BEFORE UPDATE ON agent_memory_payloads FOR EACH ROW EXECUTE FUNCTION reject_payload_update()").await.unwrap();
    assert!(
        backend
            .put(put_request(
                "payload",
                scope("owner"),
                "fact",
                "replacement"
            ))
            .await
            .is_err()
    );
    assert_eq!(
        backend
            .get(BackendGetRequest {
                memory_id: "payload".into(),
                scope: scope("owner"),
                scope_key_hash: None
            })
            .await
            .unwrap()
            .unwrap()
            .content,
        "original"
    );
        Ok(())
    }).await.unwrap();
}
