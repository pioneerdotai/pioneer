use memvid_core::Memvid;
use migration::{Migrator, MigratorTrait};
use pioneer_crud::CrudStore;
use pioneer_memory::{
    BackendDeleteRequest, BackendGetRequest, BackendPutRequest, BackendSearchRequest,
    BackendSearchScope, MemoryBackend, MemvidMemoryBackend, MemvidMemoryBackendConfig,
    memvid_search_request,
};
use pioneer_protocol::{
    MemoryCategory, MemoryScope, MemoryScopeKind, MemorySensitivity, MemoryStatus,
};
use sea_orm::Database;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

struct BackendHarness {
    _temp_dir: TempDir,
    store: Arc<CrudStore>,
    config: MemvidMemoryBackendConfig,
    backend: MemvidMemoryBackend,
}

impl BackendHarness {
    async fn new() -> Self {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite");
        Migrator::up(&connection, None).await.expect("migrate");
        let store = Arc::new(CrudStore::new(connection));
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config = MemvidMemoryBackendConfig::new(temp_dir.path().join("capsules"));
        let backend = MemvidMemoryBackend::new(store.clone(), config.clone());
        Self {
            _temp_dir: temp_dir,
            store,
            config,
            backend,
        }
    }

    fn reopened_backend(&self) -> MemvidMemoryBackend {
        MemvidMemoryBackend::new(self.store.clone(), self.config.clone())
    }
}

fn scope(kind: MemoryScopeKind, key: &str) -> MemoryScope {
    MemoryScope {
        kind,
        key: key.to_owned(),
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

fn get_request(
    memory_id: &str,
    scope: MemoryScope,
    put: &pioneer_memory::BackendPutResult,
) -> BackendGetRequest {
    BackendGetRequest {
        memory_id: memory_id.to_owned(),
        scope,
        scope_key_hash: None,
        capsule_id: put.capsule_id.clone(),
        capsule_ref: put.capsule_ref.clone(),
        frame_id: put.frame_id,
        frame_uri: put.frame_uri.clone(),
    }
}

fn delete_request(
    memory_id: &str,
    scope: MemoryScope,
    put: &pioneer_memory::BackendPutResult,
) -> BackendDeleteRequest {
    BackendDeleteRequest {
        memory_id: memory_id.to_owned(),
        scope,
        scope_key_hash: None,
        capsule_id: put.capsule_id.clone(),
        capsule_ref: put.capsule_ref.clone(),
        frame_id: put.frame_id,
        frame_uri: put.frame_uri.clone(),
    }
}

fn capsule_path(storage_uri: &str) -> PathBuf {
    PathBuf::from(
        storage_uri
            .strip_prefix("file://")
            .expect("memvid file URI"),
    )
}

async fn search_request(
    store: &CrudStore,
    query: &str,
    scopes: Vec<MemoryScope>,
) -> BackendSearchRequest {
    let resolved = store
        .resolve_memory_scopes(scopes.clone())
        .await
        .expect("resolve search scopes")
        .into_iter()
        .map(|scope| BackendSearchScope {
            scope: scope.scope,
            scope_key_hash: scope.scope_key_hash,
            workspace_id: scope.workspace_id,
            capsule_ref: None,
        })
        .collect();
    BackendSearchRequest {
        query: query.to_owned(),
        scopes,
        resolved_scopes: resolved,
        limit: 10,
    }
}

#[tokio::test]
async fn put_get_reopen_and_update_by_uri() {
    let harness = BackendHarness::new().await;
    let user_scope = scope(MemoryScopeKind::User, "default");
    let put = harness
        .backend
        .put(put_request(
            "mem_backend_birthday",
            user_scope.clone(),
            "user.birthday",
            "The user's birthday is September 12.",
        ))
        .await
        .expect("put");
    assert!(
        put.capsule_ref
            .as_deref()
            .unwrap()
            .starts_with("mv2://pioneer/user/")
    );
    assert!(!put.capsule_ref.as_deref().unwrap().contains("default"));
    let resolved = harness
        .store
        .resolve_memory_scope(user_scope.clone())
        .await
        .expect("resolve scope");
    let capsule = harness
        .store
        .find_agent_memory_capsule_by_ref(put.capsule_ref.as_deref().expect("capsule ref"))
        .await
        .expect("find capsule")
        .expect("capsule exists");
    let path = capsule_path(capsule.storage_uri.as_str());
    assert!(path.exists());
    assert!(
        path.to_string_lossy()
            .contains(resolved.scope_key_hash.as_str())
    );
    assert!(!path.to_string_lossy().contains("default"));
    {
        let memvid = Memvid::open_read_only(path.as_path()).expect("open memvid");
        let frame = memvid
            .frame_by_uri(put.frame_uri.as_deref().expect("frame uri"))
            .expect("frame by uri");
        assert_eq!(
            frame
                .extra_metadata
                .get("pioneer.memory_id")
                .map(String::as_str),
            Some("mem_backend_birthday")
        );
        assert_eq!(
            frame
                .extra_metadata
                .get("pioneer.scope_key_hash")
                .map(String::as_str),
            Some(resolved.scope_key_hash.as_str())
        );
        assert_eq!(
            frame
                .extra_metadata
                .get("pioneer.category")
                .map(String::as_str),
            Some("identity")
        );
        assert_eq!(
            frame.extra_metadata.get("pioneer.key").map(String::as_str),
            Some("user.birthday")
        );
        assert!(!frame.extra_metadata.contains_key("pioneer.source_kind"));
    }

    let loaded = harness
        .backend
        .get(get_request(
            "mem_backend_birthday",
            user_scope.clone(),
            &put,
        ))
        .await
        .expect("get")
        .expect("payload");
    assert_eq!(loaded.content, "The user's birthday is September 12.");

    let reopened = harness.reopened_backend();
    let reopened_loaded = reopened
        .get(get_request(
            "mem_backend_birthday",
            user_scope.clone(),
            &put,
        ))
        .await
        .expect("reopened get")
        .expect("payload after reopen");
    assert_eq!(reopened_loaded.content, loaded.content);

    let updated = reopened
        .put(put_request(
            "mem_backend_birthday",
            user_scope.clone(),
            "user.birthday",
            "The user's birthday is September 13.",
        ))
        .await
        .expect("update");
    assert_eq!(put.frame_uri, updated.frame_uri);
    let updated_loaded = reopened
        .get(get_request("mem_backend_birthday", user_scope, &updated))
        .await
        .expect("get updated")
        .expect("updated payload");
    assert_eq!(
        updated_loaded.content,
        "The user's birthday is September 13."
    );
    let old_hits = reopened
        .search(
            search_request(
                &harness.store,
                "\"September 12\"",
                vec![scope(MemoryScopeKind::User, "default")],
            )
            .await,
        )
        .await
        .expect("search old content");
    assert!(old_hits.is_empty());
    let new_hits = reopened
        .search(
            search_request(
                &harness.store,
                "\"September 13\"",
                vec![scope(MemoryScopeKind::User, "default")],
            )
            .await,
        )
        .await
        .expect("search new content");
    assert_eq!(new_hits.len(), 1);
}

#[tokio::test]
async fn search_is_limited_to_requested_scope_capsules() {
    let harness = BackendHarness::new().await;
    let user_scope = scope(MemoryScopeKind::User, "default");
    let workspace_scope = scope(MemoryScopeKind::Workspace, "ws_backend");
    harness
        .backend
        .put(put_request(
            "mem_user_city",
            user_scope.clone(),
            "city",
            "Shared token belongs to the user scope.",
        ))
        .await
        .expect("put user");
    harness
        .backend
        .put(put_request(
            "mem_workspace_city",
            workspace_scope.clone(),
            "city",
            "Shared token belongs to the workspace scope.",
        ))
        .await
        .expect("put workspace");

    let user_hits = harness
        .backend
        .search(search_request(&harness.store, "Shared token", vec![user_scope]).await)
        .await
        .expect("search user");
    assert_eq!(user_hits.len(), 1);
    assert_eq!(user_hits[0].memory_id, "mem_user_city");

    let workspace_hits = harness
        .backend
        .search(search_request(&harness.store, "Shared token", vec![workspace_scope]).await)
        .await
        .expect("search workspace");
    assert_eq!(workspace_hits.len(), 1);
    assert_eq!(workspace_hits[0].memory_id, "mem_workspace_city");
}

#[tokio::test]
async fn delete_survives_reopen_and_removes_search_hits() {
    let harness = BackendHarness::new().await;
    let user_scope = scope(MemoryScopeKind::User, "default");
    let put = harness
        .backend
        .put(put_request(
            "mem_delete",
            user_scope.clone(),
            "delete",
            "This payload should disappear from memvid search.",
        ))
        .await
        .expect("put");

    let reopened = harness.reopened_backend();
    let deleted = reopened
        .delete(delete_request("mem_delete", user_scope.clone(), &put))
        .await
        .expect("delete");
    assert!(deleted.deleted);

    let reopened_again = harness.reopened_backend();
    let missing = reopened_again
        .get(get_request("mem_delete", user_scope.clone(), &put))
        .await
        .expect("get after delete");
    assert!(missing.is_none());

    let hits = reopened_again
        .search(search_request(&harness.store, "disappear", vec![user_scope]).await)
        .await
        .expect("search after delete");
    assert!(hits.is_empty());
}

#[tokio::test]
async fn primary_capsule_registry_is_idempotent_per_scope() {
    let harness = BackendHarness::new().await;
    let user_scope = scope(MemoryScopeKind::User, "default");
    let first = harness
        .backend
        .put(put_request(
            "mem_capsule_one",
            user_scope.clone(),
            "one",
            "First memory in the capsule.",
        ))
        .await
        .expect("first put");
    let second = harness
        .backend
        .put(put_request(
            "mem_capsule_two",
            user_scope.clone(),
            "two",
            "Second memory in the capsule.",
        ))
        .await
        .expect("second put");
    assert_eq!(first.capsule_id, second.capsule_id);
    assert_eq!(first.capsule_ref, second.capsule_ref);

    let capsule = harness
        .store
        .find_primary_agent_memory_capsule(user_scope)
        .await
        .expect("find capsule")
        .expect("capsule");
    assert_eq!(capsule.active_record_count, 2);
    assert_eq!(capsule.backend, "memvid");
    assert_eq!(capsule.format, "mv2");
}

#[test]
fn explicit_search_request_helper_does_not_rely_on_default() {
    let request = memvid_search_request("birthday", 5, 180);
    assert_eq!(request.query, "birthday");
    assert_eq!(request.top_k, 5);
    assert_eq!(request.snippet_chars, 180);
    assert!(request.uri.is_none());
    assert!(request.scope.is_none());
    assert!(request.cursor.is_none());
    assert!(request.temporal.is_none());
    assert!(request.as_of_frame.is_none());
    assert!(request.as_of_ts.is_none());
    assert!(request.no_sketch);
    assert!(request.acl_context.is_none());
}
