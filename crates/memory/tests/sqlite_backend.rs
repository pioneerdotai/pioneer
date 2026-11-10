use migration::{Migrator, MigratorTrait};
use pioneer_crud::CrudStore;
use pioneer_memory::{
    BackendDeleteRequest, BackendGetRequest, BackendPutRequest, MemoryBackend, SqliteMemoryBackend,
};
use pioneer_protocol::{
    MemoryCategory, MemoryScope, MemoryScopeKind, MemorySensitivity, MemoryStatus,
};
use sea_orm::{ConnectionTrait, Database};
use std::sync::Arc;

async fn store() -> Arc<CrudStore> {
    pioneer_sqlite::search::register_auto_extension_once().unwrap();
    let connection = Database::connect({
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
        options
    })
    .await
    .unwrap();
    Migrator::up(&connection, None).await.unwrap();
    Arc::new(CrudStore::new(connection))
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
    let store = store().await;
    let backend = SqliteMemoryBackend::new(store.clone());
    let first = "完整内容 Unicode ".repeat(100);
    let put = backend
        .put(put_request("payload", scope("owner"), "fact", &first))
        .await
        .unwrap();
    assert_eq!(put.payload_version, 1);
    let reopened = SqliteMemoryBackend::new(store.clone());
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
}

#[tokio::test]
async fn failed_update_preserves_previous_complete_payload() {
    let store = store().await;
    let backend = SqliteMemoryBackend::new(store.clone());
    backend
        .put(put_request("payload", scope("owner"), "fact", "original"))
        .await
        .unwrap();
    store.database_connection().execute_unprepared("CREATE TRIGGER reject_payload_update BEFORE UPDATE ON agent_memory_payloads BEGIN SELECT RAISE(ABORT, 'injected update failure'); END;").await.unwrap();
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
}
