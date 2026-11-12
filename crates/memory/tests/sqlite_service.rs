use migration::{Migrator, MigratorTrait};
use pioneer_crud::{CrudStore, global_agent_memory_scope_key, workspace_agent_memory_scope_key};
use pioneer_memory::{
    BackendPutRequest, MemoryBackend, MemoryOperationContext, MemoryReadPolicy, MemoryService,
    MemoryServiceConfig, SqliteMemoryBackend,
};
use pioneer_protocol::{
    MemoryActor, MemoryActorKind, MemoryCategory, MemoryForgetParams, MemoryForgetTarget,
    MemoryGetParams, MemoryListParams, MemoryRememberParams, MemoryScope, MemoryScopeKind,
    MemorySearchParams, MemorySensitivity, MemoryStatus,
};
use sea_orm::Database;
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;

struct ServiceHarness {
    _temp_dir: TempDir,
    store: Arc<CrudStore>,
    backend: Arc<SqliteMemoryBackend>,
    service: MemoryService,
}

impl ServiceHarness {
    async fn persistent(temp_dir: TempDir) -> Self {
        let url = format!(
            "sqlite://{}?mode=rwc",
            temp_dir.path().join("control.sqlite").display()
        );
        pioneer_sqlite::search::register_auto_extension_once().unwrap();
        let connection = Database::connect({
            let mut options = sea_orm::ConnectOptions::new(url);
            options
                .max_connections(1)
                .sqlx_logging(false)
                .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
            options
        })
        .await
        .unwrap();
        Migrator::up(&connection, None).await.unwrap();
        let store = Arc::new(CrudStore::new(connection));
        let backend = Arc::new(SqliteMemoryBackend::new(store.clone()));
        let service = MemoryService::new(
            store.clone(),
            backend.clone(),
            MemoryServiceConfig::default(),
        );
        Self {
            _temp_dir: temp_dir,
            store,
            backend,
            service,
        }
    }

    async fn reopen(self) -> Self {
        let Self {
            _temp_dir,
            store,
            backend,
            service,
        } = self;
        drop(service);
        drop(backend);
        drop(store);
        Self::persistent(_temp_dir).await
    }

    async fn new() -> Self {
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
        .expect("connect sqlite");
        Migrator::up(&connection, None).await.expect("migrate");
        let store = Arc::new(CrudStore::new(connection));
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let backend = Arc::new(SqliteMemoryBackend::new(store.clone()));
        let backend_for_service: Arc<dyn MemoryBackend> = backend.clone();
        let service = MemoryService::new(
            store.clone(),
            backend_for_service,
            MemoryServiceConfig::default(),
        );
        Self {
            _temp_dir: temp_dir,
            store,
            backend,
            service,
        }
    }
}

fn scope(kind: MemoryScopeKind, key: &str) -> MemoryScope {
    MemoryScope {
        kind,
        key: key.to_owned(),
    }
}

fn user_context(now: i64) -> MemoryOperationContext {
    MemoryOperationContext {
        now_unix: Some(now),
        actor: Some(MemoryActor {
            kind: MemoryActorKind::User,
            id: Some("user_alexander".to_owned()),
        }),
        ..Default::default()
    }
}

fn workspace_context(workspace_id: &str, now: i64) -> MemoryOperationContext {
    MemoryOperationContext {
        workspace_id: Some(workspace_id.to_owned()),
        now_unix: Some(now),
        actor: Some(MemoryActor {
            kind: MemoryActorKind::User,
            id: Some("user_alexander".to_owned()),
        }),
        ..Default::default()
    }
}

fn remember_params(scope: MemoryScope, key: Option<&str>, content: &str) -> MemoryRememberParams {
    MemoryRememberParams {
        scope,
        category: MemoryCategory::Identity,
        namespace: None,
        key: key.map(str::to_owned),
        content: content.to_owned(),
        sensitivity: Some(MemorySensitivity::Normal),
        confidence: None,
        importance: None,
        provenance: None,
        source_context_kind: None,
        idempotency_key: None,
        supersedes: None,
        metadata: BTreeMap::new(),
    }
}

struct FirstPutBarrier {
    inner: Arc<SqliteMemoryBackend>,
    arrivals: std::sync::atomic::AtomicUsize,
    barrier: tokio::sync::Barrier,
}

#[async_trait::async_trait]
impl MemoryBackend for FirstPutBarrier {
    async fn put(
        &self,
        request: BackendPutRequest,
    ) -> anyhow::Result<pioneer_memory::BackendPutResult> {
        let index = self
            .arrivals
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let result = self.inner.put(request).await?;
        if index < 2 {
            self.barrier.wait().await;
        }
        Ok(result)
    }
    async fn get(
        &self,
        request: pioneer_memory::BackendGetRequest,
    ) -> anyhow::Result<Option<pioneer_memory::BackendPayload>> {
        self.inner.get(request).await
    }
    async fn search(
        &self,
        request: pioneer_memory::BackendSearchRequest,
    ) -> anyhow::Result<Vec<pioneer_memory::BackendSearchHit>> {
        self.inner.search(request).await
    }
    async fn delete(
        &self,
        request: pioneer_memory::BackendDeleteRequest,
    ) -> anyhow::Result<pioneer_memory::BackendDeleteResult> {
        self.inner.delete(request).await
    }
}

#[tokio::test]
async fn concurrent_first_keyed_puts_publish_one_consistent_sqlite_identity() {
    let harness = ServiceHarness::new().await;
    let backend = Arc::new(FirstPutBarrier {
        inner: harness.backend.clone(),
        arrivals: std::sync::atomic::AtomicUsize::new(0),
        barrier: tokio::sync::Barrier::new(2),
    });
    let first = MemoryService::new(
        harness.store.clone(),
        backend.clone(),
        MemoryServiceConfig::default(),
    );
    let second = MemoryService::new(
        harness.store.clone(),
        backend,
        MemoryServiceConfig::default(),
    );
    let (a, b) = tokio::join!(
        first.remember(
            user_context(200),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("race.key"),
                "Concurrent fact A"
            )
        ),
        second.remember(
            user_context(200),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("race.key"),
                "Concurrent fact B"
            )
        ),
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.record.id, b.record.id);
    assert_ne!(a.created, b.created);
    let reopened_backend = Arc::new(SqliteMemoryBackend::new(harness.store.clone()));
    let reopened = MemoryService::new(
        harness.store.clone(),
        reopened_backend,
        MemoryServiceConfig::default(),
    );
    let records = reopened
        .list(user_context(201), MemoryListParams::default())
        .await
        .unwrap()
        .records;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, a.record.id);
    assert!(
        reopened
            .get_by_key(
                user_context(201),
                scope(MemoryScopeKind::User, "default"),
                None,
                "race.key".to_owned()
            )
            .await
            .unwrap()
            .record
            .is_some()
    );
    let hits = reopened
        .search(
            user_context(201),
            MemorySearchParams {
                query: "Concurrent fact".to_owned(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(hits.hits.len(), 1);
    assert_eq!(
        harness
            .store
            .get_agent_memory_record(&a.record.id, true)
            .await
            .unwrap()
            .unwrap()
            .repair_status,
        "ok"
    );
    let events = harness
        .store
        .list_agent_memory_events(&a.record.id, 10)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_kind == "created")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_kind == "updated")
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_replacement_publication_keeps_old_sqlite_record_active() {
    use sea_orm::ConnectionTrait;
    let harness = ServiceHarness::persistent(tempfile::tempdir().unwrap()).await;
    let old = harness
        .service
        .remember(
            user_context(300),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("replacement.key"),
                "Original fact",
            ),
        )
        .await
        .unwrap();
    // Failure occurs after the supersede UPDATE but before replacement INSERT,
    // inside the actual SQL publication transaction, not in a mock service.
    harness.store.database_connection().execute_unprepared(
        "CREATE TRIGGER fail_memory_replacement BEFORE INSERT ON agent_memory WHEN NEW.content_preview = 'Replacement fact' BEGIN SELECT RAISE(ABORT, 'injected replacement publication failure'); END;"
    ).await.unwrap();
    let mut params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("replacement.key"),
        "Replacement fact",
    );
    params.supersedes = Some(old.record.id.clone());
    assert!(
        harness
            .service
            .remember(user_context(301), params.clone())
            .await
            .is_err()
    );
    let harness = harness.reopen().await;
    let still_active = harness
        .service
        .get_by_key(
            user_context(302),
            scope(MemoryScopeKind::User, "default"),
            None,
            "replacement.key".to_owned(),
        )
        .await
        .unwrap()
        .record
        .unwrap();
    assert_eq!(still_active.id, old.record.id);
    assert_eq!(still_active.content, "Original fact");
    assert_eq!(
        harness
            .store
            .list_agent_memory_events(&old.record.id, 10)
            .await
            .unwrap()
            .iter()
            .filter(|event| event.event_kind == "superseded")
            .count(),
        0
    );
    harness
        .store
        .database_connection()
        .execute_unprepared("DROP TRIGGER fail_memory_replacement")
        .await
        .unwrap();
    let replaced = harness
        .service
        .remember(user_context(303), params)
        .await
        .unwrap();
    let harness = harness.reopen().await;
    assert_ne!(replaced.record.id, old.record.id);
    assert_eq!(
        harness
            .store
            .get_agent_memory_record(&old.record.id, true)
            .await
            .unwrap()
            .unwrap()
            .superseded_by
            .as_deref(),
        Some(replaced.record.id.as_str())
    );
    let current = harness
        .service
        .get_by_key(
            user_context(304),
            scope(MemoryScopeKind::User, "default"),
            None,
            "replacement.key".to_owned(),
        )
        .await
        .unwrap()
        .record
        .unwrap();
    assert_eq!(current.id, replaced.record.id);
    assert_eq!(current.content, "Replacement fact");
}

#[tokio::test]
async fn remember_get_and_list_use_sqlite_payloads() {
    let harness = ServiceHarness::new().await;
    let full_content = format!(
        "The user's birthday is September 12. {}",
        "full payload ".repeat(40)
    )
    .trim_end()
    .to_owned();
    let remembered = harness
        .service
        .remember(
            user_context(100),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.birthday"),
                full_content.as_str(),
            ),
        )
        .await
        .expect("remember");
    assert_eq!(remembered.record.content, full_content);
    let control = harness
        .store
        .get_agent_memory_record(remembered.record.id.as_str(), true)
        .await
        .expect("control row")
        .expect("control row exists");
    assert!(control.payload_version > 0);
    let preview = control.content_preview.as_deref().expect("preview");
    assert!(full_content.starts_with(preview));
    assert!(preview.len() < full_content.len());
    assert!(
        !control
            .metadata_json
            .as_deref()
            .unwrap_or_default()
            .contains(&full_content)
    );

    let loaded = harness
        .service
        .get(
            user_context(101),
            MemoryGetParams {
                memory_id: remembered.record.id.clone(),
                include_deleted: false,
            },
        )
        .await
        .expect("get")
        .record
        .expect("record");
    assert_eq!(loaded.content, full_content);

    let listed = harness
        .service
        .list(
            user_context(102),
            MemoryListParams {
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                ..Default::default()
            },
        )
        .await
        .expect("list");
    assert_eq!(listed.records.len(), 1);
    assert_eq!(listed.records[0].content, full_content);
}

struct PauseBeforePublication {
    inner: Arc<SqliteMemoryBackend>,
    ready: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl MemoryBackend for PauseBeforePublication {
    async fn put(
        &self,
        request: BackendPutRequest,
    ) -> anyhow::Result<pioneer_memory::BackendPutResult> {
        self.inner.put(request).await
    }
    async fn prepare_put(
        &self,
        request: BackendPutRequest,
    ) -> anyhow::Result<pioneer_memory::BackendPutResult> {
        let _prepared = self.inner.prepare_put(request).await?;
        self.ready.notify_one();
        std::future::pending().await
    }
    async fn get(
        &self,
        request: pioneer_memory::BackendGetRequest,
    ) -> anyhow::Result<Option<pioneer_memory::BackendPayload>> {
        self.inner.get(request).await
    }
    async fn search(
        &self,
        request: pioneer_memory::BackendSearchRequest,
    ) -> anyhow::Result<Vec<pioneer_memory::BackendSearchHit>> {
        self.inner.search(request).await
    }
    async fn delete(
        &self,
        request: pioneer_memory::BackendDeleteRequest,
    ) -> anyhow::Result<pioneer_memory::BackendDeleteResult> {
        self.inner.delete(request).await
    }
}

#[tokio::test]
async fn cancellation_before_atomic_publication_preserves_owner_without_orphan() {
    let harness = ServiceHarness::persistent(tempfile::tempdir().unwrap()).await;
    let old = harness
        .service
        .remember(
            user_context(800),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("cancel.key"),
                "Cancellation marker old",
            ),
        )
        .await
        .unwrap();
    let ready = Arc::new(tokio::sync::Notify::new());
    let paused = MemoryService::new(
        harness.store.clone(),
        Arc::new(PauseBeforePublication {
            inner: harness.backend.clone(),
            ready: ready.clone(),
        }),
        MemoryServiceConfig::default(),
    );
    let mut replacement = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("cancel.key"),
        "Cancellation marker new",
    );
    replacement.supersedes = Some(old.record.id.clone());
    let writer = tokio::spawn(async move { paused.remember(user_context(801), replacement).await });
    tokio::time::timeout(std::time::Duration::from_secs(30), ready.notified())
        .await
        .unwrap();
    // Payload preparation must not occupy the serialized SQL writer.
    let maintenance = MemoryService::new(
        Arc::new(harness.store.with_maintenance_access()),
        harness.backend.clone(),
        MemoryServiceConfig::default(),
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        maintenance.remember(
            user_context(802),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("other.key"),
                "Independent maintenance fact",
            ),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    drop(maintenance);
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    let harness = harness.reopen().await;
    let current = harness
        .service
        .get_by_key(
            user_context(803),
            scope(MemoryScopeKind::User, "default"),
            None,
            "cancel.key".to_owned(),
        )
        .await
        .unwrap()
        .record
        .unwrap();
    assert_eq!(current.id, old.record.id);
    assert_eq!(current.content, "Cancellation marker old");
    let hits = harness
        .service
        .search(
            user_context(804),
            MemorySearchParams {
                query: "Cancellation marker".to_owned(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        !hits
            .hits
            .iter()
            .any(|hit| hit.record.content == "Cancellation marker new")
    );
    use sea_orm::{ConnectionTrait, DbBackend, Statement};
    let payload_count = harness
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM agent_memory_payloads",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "n")
        .unwrap();
    assert_eq!(
        payload_count, 2,
        "only old owner and independent write were persisted"
    );
    let jobs = harness
        .service
        .claim_due_repair_jobs(805, 60, "cancel_repair", 10)
        .await
        .unwrap();
    assert!(
        jobs.is_empty(),
        "atomic publication needs no orphan cleanup job"
    );
    let mut resumed = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("cancel.key"),
        "Cancellation marker new",
    );
    resumed.supersedes = Some(old.record.id.clone());
    let published = harness
        .service
        .remember(user_context(807), resumed)
        .await
        .unwrap();
    assert_ne!(published.record.id, old.record.id);
    assert_eq!(published.record.content, "Cancellation marker new");
    let owner_scope = scope(MemoryScopeKind::User, "default");
    assert_eq!(
        harness
            .store
            .get_memory_payload(&old.record.id, owner_scope.clone())
            .await
            .unwrap()
            .as_deref(),
        Some("Cancellation marker old")
    );
    assert_eq!(
        harness
            .store
            .get_memory_payload(&published.record.id, owner_scope)
            .await
            .unwrap()
            .as_deref(),
        Some("Cancellation marker new")
    );
}

#[tokio::test]
async fn search_results_are_filtered_by_control_plane() {
    let harness = ServiceHarness::new().await;
    let identity = harness
        .service
        .remember(
            user_context(200),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("identity.filter"),
                "Control plane filter token belongs to identity.",
            ),
        )
        .await
        .expect("remember identity");
    let mut preference_params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("preference.filter"),
        "Control plane filter token belongs to preference.",
    );
    preference_params.category = MemoryCategory::Preference;
    harness
        .service
        .remember(user_context(201), preference_params)
        .await
        .expect("remember preference");

    let search = harness
        .service
        .search(
            user_context(202),
            MemorySearchParams {
                query: "Control plane filter token".to_owned(),
                categories: vec![MemoryCategory::Identity],
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert_eq!(search.hits.len(), 1);
    assert_eq!(search.hits[0].record.id, identity.record.id);
}

#[tokio::test]
async fn deleted_control_plane_row_suppresses_stale_sqlite_hit() {
    let harness = ServiceHarness::new().await;
    let remembered = harness
        .service
        .remember(
            user_context(300),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("stale.delete"),
                "Stale sqlite hit should be hidden after tombstone.",
            ),
        )
        .await
        .expect("remember");
    use sea_orm::ConnectionTrait;
    harness.store.database_connection().execute_unprepared(
        "CREATE TRIGGER fail_payload_delete BEFORE DELETE ON agent_memory_payloads BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END;"
    ).await.unwrap();

    harness
        .service
        .forget(
            user_context(301),
            MemoryForgetParams {
                target: MemoryForgetTarget::Id {
                    memory_id: remembered.record.id.clone(),
                },
                reason: Some("test".to_owned()),
                actor: None,
                dry_run: false,
            },
        )
        .await
        .expect("forget");

    let search = harness
        .service
        .search(
            user_context(302),
            MemorySearchParams {
                query: "Stale sqlite hit".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());

    let row = harness
        .store
        .get_agent_memory_record(remembered.record.id.as_str(), true)
        .await
        .expect("row")
        .expect("row exists");
    assert_eq!(row.status, MemoryStatus::Deleted);

    let claimed = harness
        .service
        .claim_due_repair_jobs(303, 60, "repair_worker", 10)
        .await
        .expect("claim");
    assert!(
        claimed
            .iter()
            .any(|job| job.job_kind == "backend_delete_failed")
    );
    assert!(
        !claimed
            .iter()
            .filter_map(|job| job.payload_json.as_deref())
            .any(|payload| payload.contains("Stale sqlite hit should be hidden"))
    );
}

#[tokio::test]
async fn superseded_control_plane_row_suppresses_stale_sqlite_hit() {
    let harness = ServiceHarness::new().await;
    let old = harness
        .service
        .remember(
            user_context(350),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("sqlite.superseded"),
                "retiredpayloadneedle should be hidden.",
            ),
        )
        .await
        .expect("remember old");
    let mut replacement_params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("sqlite.superseded"),
        "SQLite replacement memory is authoritative.",
    );
    replacement_params.supersedes = Some(old.record.id.clone());
    harness
        .service
        .remember(user_context(351), replacement_params)
        .await
        .expect("remember replacement");

    let search = harness
        .service
        .search(
            user_context(352),
            MemorySearchParams {
                query: "retiredpayloadneedle".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());
}

#[tokio::test]
async fn missing_payload_creates_redacted_repair_diagnostic() {
    let harness = ServiceHarness::new().await;
    let remembered = harness
        .service
        .remember(
            user_context(400),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("payload.missing"),
                "Missing payload content must not leak into diagnostics.",
            ),
        )
        .await
        .expect("remember");
    let row = harness
        .store
        .get_agent_memory_record(remembered.record.id.as_str(), true)
        .await
        .expect("row")
        .expect("row exists");
    // Inject physical payload loss through the project database path. Ordinary
    // backend deletion now protects an active, non-quarantined control owner.
    use sea_orm::{ConnectionTrait, DbBackend, Statement};
    harness
        .store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "DELETE FROM agent_memory_payloads WHERE memory_id=?",
            [row.id.clone().into()],
        ))
        .await
        .unwrap();

    let missing = harness
        .service
        .get(
            user_context(401),
            MemoryGetParams {
                memory_id: remembered.record.id.clone(),
                include_deleted: false,
            },
        )
        .await
        .expect("get missing");
    assert!(missing.record.is_none());
    let missing_search = harness
        .service
        .search(
            user_context(401),
            MemorySearchParams {
                query: "Missing payload content".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search missing");
    assert!(missing_search.hits.is_empty());

    let repaired_row = harness
        .store
        .get_agent_memory_record(remembered.record.id.as_str(), true)
        .await
        .expect("repaired row")
        .expect("row exists");
    assert_eq!(repaired_row.repair_status, "repair_needed");

    let claimed = harness
        .service
        .claim_due_repair_jobs(i64::MAX / 2, 60, "repair_worker", 10)
        .await
        .expect("claim");
    assert!(
        claimed
            .iter()
            .any(|job| job.job_kind == "backend_payload_missing")
    );
    assert!(
        !claimed
            .iter()
            .filter_map(|job| job.payload_json.as_deref())
            .any(|payload| payload.contains("Missing payload content"))
    );
}

#[tokio::test]
async fn workspace_agent_memory_and_global_allowances_are_enforced() {
    let harness = ServiceHarness::new().await;
    let agent_id = "agent_research";
    let workspace_a_agent = workspace_agent_memory_scope_key("ws_sqlite_a", agent_id);
    let global_agent = global_agent_memory_scope_key(agent_id);

    let workspace_memory = harness
        .service
        .remember(
            workspace_context("ws_sqlite_a", 600),
            remember_params(
                scope(MemoryScopeKind::Agent, workspace_a_agent.as_str()),
                Some("agent.note"),
                "SQLite workspace A agent memory.",
            ),
        )
        .await
        .expect("remember workspace agent");
    harness
        .service
        .remember(
            user_context(601),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.global"),
                "SQLite global user memory.",
            ),
        )
        .await
        .expect("remember global user");
    let global_agent_memory = harness
        .service
        .remember(
            MemoryOperationContext {
                allow_global_agent: true,
                now_unix: Some(602),
                ..Default::default()
            },
            remember_params(
                scope(MemoryScopeKind::Agent, global_agent.as_str()),
                Some("agent.global"),
                "SQLite global agent memory.",
            ),
        )
        .await
        .expect("remember global agent");

    let leaked_workspace_agent = harness
        .service
        .search(
            workspace_context("ws_sqlite_b", 603),
            MemorySearchParams {
                query: "SQLite workspace A agent memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::Agent, workspace_a_agent.as_str())],
                ..Default::default()
            },
        )
        .await
        .expect("search wrong workspace");
    assert!(leaked_workspace_agent.hits.is_empty());

    let visible_workspace_agent = harness
        .service
        .search(
            workspace_context("ws_sqlite_a", 604),
            MemorySearchParams {
                query: "SQLite workspace A agent memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::Agent, workspace_a_agent.as_str())],
                ..Default::default()
            },
        )
        .await
        .expect("search right workspace");
    assert_eq!(visible_workspace_agent.hits.len(), 1);
    assert_eq!(
        visible_workspace_agent.hits[0].record.id,
        workspace_memory.record.id
    );

    let blocked_global_user = harness
        .service
        .search(
            workspace_context("ws_sqlite_a", 605),
            MemorySearchParams {
                query: "SQLite global user memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                ..Default::default()
            },
        )
        .await
        .expect("blocked global user");
    assert!(blocked_global_user.hits.is_empty());

    let allowed_global_user = harness
        .service
        .search(
            MemoryOperationContext {
                allow_global_user: true,
                ..workspace_context("ws_sqlite_a", 606)
            },
            MemorySearchParams {
                query: "SQLite global user memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                ..Default::default()
            },
        )
        .await
        .expect("allowed global user");
    assert_eq!(allowed_global_user.hits.len(), 1);

    let blocked_global_agent = harness
        .service
        .search(
            workspace_context("ws_sqlite_a", 607),
            MemorySearchParams {
                query: "SQLite global agent memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::Agent, global_agent.as_str())],
                ..Default::default()
            },
        )
        .await
        .expect("blocked global agent");
    assert!(blocked_global_agent.hits.is_empty());

    let allowed_global_agent = harness
        .service
        .search(
            MemoryOperationContext {
                allow_global_agent: true,
                ..workspace_context("ws_sqlite_a", 608)
            },
            MemorySearchParams {
                query: "SQLite global agent memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::Agent, global_agent.as_str())],
                ..Default::default()
            },
        )
        .await
        .expect("allowed global agent");
    assert_eq!(allowed_global_agent.hits.len(), 1);
    assert_eq!(
        allowed_global_agent.hits[0].record.id,
        global_agent_memory.record.id
    );
}

#[tokio::test]
async fn unpublished_payload_is_ineligible_before_search_limit() {
    let harness = ServiceHarness::new().await;
    harness
        .backend
        .put(BackendPutRequest {
            memory_id: "mem_sqlite_backend_only".to_owned(),
            scope: scope(MemoryScopeKind::User, "default"),
            namespace: None,
            category: MemoryCategory::Identity,
            key: Some("backend.only".to_owned()),
            content: "Backend-only sqlite stale memory.".to_owned(),
            sensitivity: MemorySensitivity::Normal,
            metadata_json: None,
            source_thread_id: None,
            source_turn_id: None,
            source_item_id: None,
            created_by_kind: None,
            created_by_id: None,
            policy_version: "test-policy".to_owned(),
            status: MemoryStatus::Active,
            idempotency_key: None,
        })
        .await
        .expect("direct backend put");

    let search = harness
        .service
        .search(
            user_context(650),
            MemorySearchParams {
                query: "Backend-only sqlite stale".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("service search");
    assert!(search.hits.is_empty());

    let claimed = harness
        .service
        .claim_due_repair_jobs(651, 60, "repair_worker", 10)
        .await
        .expect("claim");
    assert!(claimed.is_empty());
}

#[tokio::test]
async fn service_search_respects_sensitivity_policy_with_sqlite() {
    let harness = ServiceHarness::new().await;
    for (key, sensitivity) in [
        ("normal", MemorySensitivity::Normal),
        ("personal", MemorySensitivity::Personal),
        ("secret", MemorySensitivity::SecretLike),
        ("regulated", MemorySensitivity::Regulated),
    ] {
        let mut params = remember_params(
            scope(MemoryScopeKind::User, "default"),
            Some(key),
            format!("SQLite sensitivity token {key}").as_str(),
        );
        params.sensitivity = Some(sensitivity);
        harness
            .service
            .remember(user_context(700), params)
            .await
            .expect("remember sensitivity row");
    }

    let default_search = harness
        .service
        .search(
            user_context(701),
            MemorySearchParams {
                query: "SQLite sensitivity token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("default search");
    let mut default_keys = default_search
        .hits
        .iter()
        .filter_map(|hit| hit.record.key.as_deref())
        .collect::<Vec<_>>();
    default_keys.sort_unstable();
    assert_eq!(default_keys, vec!["normal", "personal"]);

    let allow_all = harness
        .service
        .search(
            MemoryOperationContext {
                read_policy: Some(MemoryReadPolicy::allow_all()),
                ..user_context(702)
            },
            MemorySearchParams {
                query: "SQLite sensitivity token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("allow all search");
    assert_eq!(allow_all.hits.len(), 4);
}

#[tokio::test]
async fn sensitivity_and_quarantine_are_applied_before_the_candidate_limit() {
    let harness = ServiceHarness::new().await;
    let mut config = MemoryServiceConfig::default();
    config.max_limit = 1;
    config.ranking.backend_candidate_multiplier = 1;
    config.ranking.max_backend_candidates = 1;
    let service = MemoryService::new(harness.store.clone(), harness.backend.clone(), config);
    for number in 0..5 {
        let mut params = remember_params(
            scope(MemoryScopeKind::User, "default"),
            Some(&format!("hidden.{number}")),
            "topkneedle",
        );
        params.sensitivity = Some(MemorySensitivity::SecretLike);
        service.remember(user_context(800), params).await.unwrap();
    }
    let quarantined = service
        .remember(
            user_context(800),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("quarantined"),
                "topkneedle",
            ),
        )
        .await
        .unwrap();
    harness
        .store
        .create_agent_memory_quarantine_marker(pioneer_crud::NewAgentMemoryQuarantine {
            id: None,
            memory_id: quarantined.record.id,
            workspace_id: None,
            reason_code:
                pioneer_protocol::MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
            actor: pioneer_crud::MemoryLifecycleActorRecord {
                kind: pioneer_protocol::MemoryLifecycleActorKind::Admin,
                id: Some("test".into()),
            },
            details_json: None,
            created_at_unix: 801,
        })
        .await
        .unwrap();
    let allowed = service
        .remember(
            user_context(802),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("allowed"),
                &format!("topkneedle {}", "padding ".repeat(100)),
            ),
        )
        .await
        .unwrap();
    let result = service
        .search(
            user_context(803),
            MemorySearchParams {
                query: "topkneedle".into(),
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].record.id, allowed.record.id);
    assert_eq!(result.hits[0].record.content, allowed.record.content);
}

#[tokio::test]
async fn full_sqlite_payload_survives_restore_and_missing_original_is_not_rebuilt_from_preview() {
    use pioneer_memory::{MemoryQuarantineRequest, MemoryRestoreRequest};
    use pioneer_protocol::MemoryLifecycleReasonCode;
    let h = ServiceHarness::new().await;
    let owner_scope = scope(MemoryScopeKind::User, "default");
    let full = "Полный оригинал длиннее превью 🌍 "
        .repeat(100)
        .trim()
        .to_owned();
    let remembered = h
        .service
        .remember(
            user_context(1700),
            remember_params(owner_scope.clone(), Some("full.restore"), &full),
        )
        .await
        .unwrap();
    let before = h
        .store
        .get_memory_payload_versioned(&remembered.record.id, owner_scope.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.0, full);
    for (cleanup, base) in [(false, 1701), (true, 1720)] {
        h.service
            .quarantine_memory(
                user_context(base),
                MemoryQuarantineRequest {
                    memory_id: remembered.record.id.clone(),
                    reason_code: MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
                    actor: None,
                    details_json: None,
                    schedule_backend_cleanup: cleanup,
                },
            )
            .await
            .unwrap();
        let jobs = h
            .service
            .claim_due_repair_jobs(base + 1, 60, "restore_worker", 10)
            .await
            .unwrap();
        assert_eq!(jobs.len(), usize::from(cleanup));
        for job in jobs {
            h.service
                .process_repair_job(&job.id, "restore_worker", base + 2)
                .await
                .unwrap();
        }
        h.service
            .restore_quarantined_memory(
                user_context(base + 3),
                MemoryRestoreRequest {
                    memory_id: remembered.record.id.clone(),
                    actor: None,
                    schedule_backend_reindex: true,
                },
            )
            .await
            .unwrap();
        let jobs = h
            .service
            .claim_due_repair_jobs(base + 4, 60, "restore_worker", 10)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        let result = h
            .service
            .process_repair_job(&jobs[0].id, "restore_worker", base + 5)
            .await
            .unwrap()
            .unwrap();
        let payload = h
            .store
            .get_memory_payload_versioned(&remembered.record.id, owner_scope.clone())
            .await
            .unwrap();
        let control = h
            .store
            .get_agent_memory_record(&remembered.record.id, true)
            .await
            .unwrap()
            .unwrap();
        if cleanup {
            assert_eq!(result.status, "pending");
            assert!(payload.is_none());
            assert_eq!(control.repair_status, "repair_needed");
            assert!(control.content_preview.as_ref().unwrap().chars().count() <= 240);
        } else {
            assert_eq!(result.status, "completed");
            assert_eq!(
                payload,
                Some(before.clone()),
                "reindex must not rewrite body or revision"
            );
            assert_eq!(control.repair_status, "ok");
            let retained = h
                .backend
                .delete(pioneer_memory::BackendDeleteRequest {
                    memory_id: remembered.record.id.clone(),
                    scope: scope(MemoryScopeKind::User, ""),
                    scope_key_hash: Some(control.scope_key_hash.clone()),
                })
                .await
                .unwrap();
            assert!(
                !retained.deleted,
                "late orphan cleanup must preserve a published owner"
            );
            assert_eq!(
                h.store
                    .get_memory_payload_versioned(&remembered.record.id, owner_scope.clone())
                    .await
                    .unwrap(),
                Some(before.clone())
            );
        }
    }
}

#[tokio::test]
async fn orphan_repair_uses_stored_scope_hash_and_preserves_other_payloads() {
    let h = ServiceHarness::new().await;
    let target_scope = scope(MemoryScopeKind::User, "target");
    let other_scope = scope(MemoryScopeKind::User, "other");
    for (id, owner, content) in [
        (
            "target-payload",
            target_scope.clone(),
            "target full payload",
        ),
        ("other-payload", other_scope.clone(), "other full payload"),
    ] {
        h.backend
            .put(BackendPutRequest {
                memory_id: id.into(),
                scope: owner,
                namespace: None,
                category: MemoryCategory::Identity,
                key: None,
                content: content.into(),
                sensitivity: MemorySensitivity::Normal,
                metadata_json: None,
                source_thread_id: None,
                source_turn_id: None,
                source_item_id: None,
                created_by_kind: None,
                created_by_id: None,
                policy_version: "test".into(),
                status: MemoryStatus::Active,
                idempotency_key: None,
            })
            .await
            .unwrap();
    }
    let real_hash =
        pioneer_crud::memory_scope_key_hash(target_scope.kind, &target_scope.key).unwrap();
    let wrong_hash =
        pioneer_crud::memory_scope_key_hash(other_scope.kind, &other_scope.key).unwrap();
    for (hash, expected_status, base) in [
        (Some(wrong_hash), "failed", 1800),
        (None, "failed", 1810),
        (Some(real_hash.clone()), "completed", 1820),
        (Some(real_hash), "completed", 1830),
    ] {
        let job = h
            .service
            .enqueue_repair_job(
                pioneer_crud::NewAgentMemoryRepairJob {
                    job_kind: "backend_delete_failed".into(),
                    workspace_id: None,
                    scope_kind: Some(MemoryScopeKind::User),
                    scope_key_hash: hash,
                    memory_id: Some("target-payload".into()),
                    priority: 10,
                    max_attempts: 1,
                    scheduled_at_unix: base,
                    payload_json: None,
                },
                base,
            )
            .await
            .unwrap();
        let claimed = h
            .service
            .claim_due_repair_jobs(base, 60, "scope_worker", 10)
            .await
            .unwrap();
        assert!(claimed.iter().any(|claim| claim.id == job.id));
        let result = h
            .service
            .process_repair_job(&job.id, "scope_worker", base + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, expected_status);
        assert_eq!(
            h.store
                .get_memory_payload("other-payload", other_scope.clone())
                .await
                .unwrap()
                .as_deref(),
            Some("other full payload")
        );
        let target = h
            .store
            .get_memory_payload("target-payload", target_scope.clone())
            .await
            .unwrap();
        if expected_status == "failed" {
            assert_eq!(target.as_deref(), Some("target full payload"));
        } else {
            assert!(target.is_none());
        }
    }
}

#[tokio::test]
async fn cancellation_of_in_place_preparation_keeps_published_full_body() {
    let h = ServiceHarness::persistent(tempfile::tempdir().unwrap()).await;
    let full = "original full body ".repeat(100).trim().to_owned();
    let scope = scope(MemoryScopeKind::User, "default");
    let old = h
        .service
        .remember(
            user_context(1900),
            remember_params(scope.clone(), Some("atomic.update"), &full),
        )
        .await
        .unwrap();
    let ready = Arc::new(tokio::sync::Notify::new());
    let paused = MemoryService::new(
        h.store.clone(),
        Arc::new(PauseBeforePublication {
            inner: h.backend.clone(),
            ready: ready.clone(),
        }),
        MemoryServiceConfig::default(),
    );
    let params = remember_params(
        scope.clone(),
        Some("atomic.update"),
        "replacement must not escape preparation",
    );
    let task = tokio::spawn(async move { paused.remember(user_context(1901), params).await });
    tokio::time::timeout(std::time::Duration::from_secs(30), ready.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let h = h.reopen().await;
    let owner = h
        .service
        .get_by_key(
            user_context(1902),
            scope.clone(),
            None,
            "atomic.update".into(),
        )
        .await
        .unwrap()
        .record
        .unwrap();
    assert_eq!(owner.id, old.record.id);
    assert_eq!(owner.content, full);
    assert_eq!(
        h.store
            .get_memory_payload(&owner.id, scope)
            .await
            .unwrap()
            .as_deref(),
        Some(full.as_str())
    );
}
