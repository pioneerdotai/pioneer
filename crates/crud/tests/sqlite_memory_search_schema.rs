//! Schema/upgrade owner: actual migrations and bounded reset through CrudStore.
use migration::{Migrator, MigratorTrait};
use pioneer_crud::{CrudStore, MemoryPayloadResetOutcome};
use pioneer_protocol::{MemoryScope, MemoryScopeKind};
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

async fn scalar(store: &CrudStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get_by_index(0)
        .unwrap()
}

const LEGACY_INDEXES: &[&str] = &[
    "idx_agent_memory_capsule_repair_status",
    "idx_agent_memory_capsule_scope",
    "idx_agent_memory_capsule_status",
    "idx_agent_memory_capsule_workspace_status",
    "sqlite_autoindex_agent_memory_capsule_1",
    "uidx_agent_memory_capsule_ref",
    "uidx_agent_memory_capsule_scope_slot",
    "idx_thread_episodic_capsules_repair_status",
    "idx_thread_episodic_capsules_workspace_status",
    "idx_thread_episodic_capsules_write_state",
    "sqlite_autoindex_thread_episodic_capsules_1",
    "uidx_thread_episodic_capsules_active_write",
    "uidx_thread_episodic_capsules_ref",
    "uidx_thread_episodic_capsules_segment",
    "idx_agent_memory_capsule",
    "idx_agent_memory_repair_capsule",
    "uidx_thread_episodic_index_jobs_item_capsule",
];
async fn legacy_index_count(store: &CrudStore) -> i64 {
    let names = LEGACY_INDEXES
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(",");
    scalar(
        store,
        &format!("SELECT count(*) FROM sqlite_master WHERE type='index' AND name IN ({names})"),
    )
    .await
}
async fn assert_current_schema(store: &CrudStore) {
    assert_eq!(scalar(store,"SELECT count(*) FROM sqlite_master WHERE name IN ('agent_memory_capsule','thread_episodic_capsules')").await,0);
    assert_eq!(legacy_index_count(store).await, 0);
    for (table, columns) in [
        (
            "agent_memory",
            "'capsule_id','capsule_ref','frame_id','frame_uri','frame_version'",
        ),
        ("agent_memory_repair_job", "'capsule_id'"),
        (
            "thread_episodic_items",
            "'capsule_id','capsule_ref','segment_index','frame_id','frame_uri'",
        ),
        (
            "thread_episodic_index_jobs",
            "'capsule_id','capsule_ref','segment_index','frame_uri','capacity_error_count','graph_enrichment_state'",
        ),
    ] {
        assert_eq!(
            scalar(
                store,
                &format!(
                    "SELECT count(*) FROM pragma_table_info('{table}') WHERE name IN ({columns})"
                )
            )
            .await,
            0,
            "{table}"
        );
    }
    assert_eq!(scalar(store,"SELECT count(*) FROM pragma_table_info('agent_delegation_route') WHERE name IN ('home_capsule_id','source_capsule_id')").await,2,"delegation capsules are unrelated business metadata");
    assert_eq!(
        scalar(
            store,
            "SELECT count(*) FROM pragma_table_info('agent_memory') WHERE name='payload_version'"
        )
        .await,
        1
    );
    assert_eq!(scalar(store,"SELECT count(*) FROM pragma_table_info('thread_episodic_items') WHERE name='search_text'").await,1);
    assert_eq!(scalar(store,"SELECT count(*) FROM pragma_table_info('thread_episodic_embedding_artifacts') WHERE name='vector_bytes'").await,1);
    for name in [
        "agent_memory",
        "agent_memory_payloads",
        "agent_memory_policy_decision",
        "agent_memory_quality_decision",
        "agent_memory_quarantine",
        "agent_memory_identity",
        "agent_memory_candidate",
        "thread_episodic_embedding_artifacts",
        "thread_episodic_items",
        "thread_episodic_index_jobs",
        "thread_episodic_exclusions",
        "thread_episodic_thread_directory",
        "thread_episodic_recall_events",
        "turn_item",
        "task",
    ] {
        assert_eq!(
            scalar(
                store,
                &format!("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{name}'")
            )
            .await,
            1,
            "{name}"
        );
    }
    for fts in ["agent_memory_fts", "thread_episodic_fts"] {
        for suffix in ["", "_data", "_idx", "_docsize", "_config"] {
            assert_eq!(scalar(store,&format!("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{fts}{suffix}'")).await,1,"{fts}{suffix}");
        }
    }
    for table in ["agent_memory_payloads", "thread_episodic_items"] {
        for event in ["insert", "update", "delete"] {
            assert_eq!(scalar(store,&format!("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name='{table}_fts_{event}'")).await,1);
        }
    }
    for event in ["insert", "update", "delete", "initialize"] {
        assert_eq!(scalar(store,&format!("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name='episodic_directory_count_{event}'")).await,1);
    }
    assert_eq!(scalar(store,"SELECT count(*) FROM sqlite_master WHERE sql IS NOT NULL AND (instr(sql,'agent_memory_capsule')>0 OR instr(sql,'thread_episodic_capsules')>0)").await,0,"no dangling schema dependencies");
}

#[tokio::test]
async fn fresh_and_upgraded_sqlite_memory_schema_round_trip_without_canonical_data_loss() {
    pioneer_sqlite::search::register_auto_extension_once().unwrap();
    let fresh = Database::connect({
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
        options
    })
    .await
    .unwrap();
    Migrator::up(&fresh, None).await.unwrap();
    assert_current_schema(&CrudStore::new(fresh)).await;

    let database = Database::connect({
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
        options
    })
    .await
    .unwrap();
    let preceding = Migrator::migrations()
        .iter()
        .position(|migration| migration.name() == "m20261108_000001_sqlite_memory_search")
        .unwrap();
    Migrator::up(&database, Some(preceding as u32))
        .await
        .unwrap();
    database
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('upgrade-ws','canonical workspace',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status) VALUES('upgrade-thread','upgrade-ws','','agent','model','provider','active')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin) VALUES('upgrade-turn','upgrade-thread','completed','conversation','user')",
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES('canonical-item','upgrade-turn','canonical-item','user_message','completed',0,'{\"type\":\"user_message\",\"id\":\"canonical-item\",\"text\":\"history survives\",\"attachments\":[]}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,executor_kind,status,title,goal) VALUES('upgrade-task','upgrade-ws','user','agent','draft','preserved task','preserved goal')",
        "INSERT INTO agent_memory(id,scope_kind,scope_key,scope_key_hash,category,content_preview,created_at,updated_at) VALUES('legacy-memory','user','owner','legacy-hash','identity','derived old preview','2020-01-01 00:00:00+00:00','2020-01-01 00:00:00+00:00')",
    ] {
        database.execute_unprepared(sql).await.unwrap();
    }
    // The upgrade fixture really contains all 17 old indexes, including both
    // automatic PK indexes; absence assertions after up are not vacuous.
    assert_eq!(
        legacy_index_count(&CrudStore::new(database.clone())).await,
        17
    );
    Migrator::up(&database, Some(1)).await.unwrap();
    let store = CrudStore::new(database.clone());
    assert_current_schema(&store).await;
    assert_eq!(scalar(&store, "SELECT count(*) FROM turn_item WHERE id='canonical-item' AND payload LIKE '%history survives%'").await, 1);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) FROM task WHERE id='upgrade-task' AND title='preserved task'"
        )
        .await,
        1
    );
    // Schema installation only seeds maintenance; it must not erase or scan old payload rows.
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) FROM agent_memory WHERE id='legacy-memory' AND status='active'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(&store, "SELECT count(*) FROM agent_memory_payloads").await,
        0
    );
    Migrator::down(&database, Some(1)).await.unwrap();
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) FROM pragma_table_info('agent_memory') WHERE name='frame_version'"
        )
        .await,
        1
    );
    assert_eq!(scalar(&store, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='thread_episodic_capsules'").await, 1);
    Migrator::up(&database, Some(1)).await.unwrap();
    assert_current_schema(&store).await;
    assert_eq!(scalar(&store, "SELECT count(*) FROM turn_item WHERE id='canonical-item' AND payload LIKE '%history survives%'").await, 1);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) FROM task WHERE id='upgrade-task' AND goal='preserved goal'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn legacy_payload_reset_is_bounded_resumable_and_preserves_new_sqlite_payloads() {
    pioneer_sqlite::search::register_auto_extension_once().unwrap();
    let database = Database::connect({
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
        options
    })
    .await
    .unwrap();
    Migrator::up(&database, None).await.unwrap();
    let store = CrudStore::new(database).with_maintenance_access();
    store.database_connection().execute_unprepared("WITH RECURSIVE rows(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM rows WHERE n<129) INSERT INTO agent_memory(id,scope_kind,scope_key,scope_key_hash,category,content_preview,created_at,updated_at) SELECT printf('legacy-%03d',n),'user','owner','legacy-hash','identity','old derived preview','2020-01-01 00:00:00+00:00','2020-01-01 00:00:00+00:00' FROM rows").await.unwrap();
    let scope = MemoryScope {
        kind: MemoryScopeKind::User,
        key: "owner".into(),
    };
    store
        .put_memory_payload("new-payload", scope.clone(), "new complete SQLite payload")
        .await
        .unwrap();
    let hash = pioneer_crud::memory_scope_key_hash(scope.kind, &scope.key).unwrap();
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO agent_memory(id,scope_kind,scope_key,scope_key_hash,category,content_preview,created_at,updated_at) VALUES('new-payload','user','owner',?,'identity','new preview','2020-01-01 00:00:00+00:00','2020-01-01 00:00:00+00:00')", [hash.into()])).await.unwrap();
    assert_eq!(
        store
            .reset_legacy_memory_payload_quantum(1_800_000_000)
            .await
            .unwrap(),
        MemoryPayloadResetOutcome::Progress
    );
    let remaining = scalar(
        &store,
        "SELECT count(*) FROM agent_memory WHERE id LIKE 'legacy-%' AND status='active'",
    )
    .await;
    assert!(remaining > 0 && remaining < 130);
    // A restarted owner gets a fresh facade and resumes the durable cursor.
    let restarted = CrudStore::new(store.database_connection()).with_maintenance_access();
    let mut eof = false;
    for _ in 0..8 {
        match restarted
            .reset_legacy_memory_payload_quantum(1_800_000_001)
            .await
            .unwrap()
        {
            MemoryPayloadResetOutcome::Progress => {}
            MemoryPayloadResetOutcome::FilesPending => {
                eof = true;
                break;
            }
            MemoryPayloadResetOutcome::Complete => {
                panic!("file-cleanup completion was not acknowledged")
            }
        }
    }
    assert!(eof);
    assert_eq!(scalar(&restarted, "SELECT count(*) FROM agent_memory WHERE id LIKE 'legacy-%' AND (status!='deleted' OR content_preview IS NOT NULL)").await, 0);
    assert_eq!(
        scalar(
            &restarted,
            "SELECT count(*) FROM agent_memory WHERE id='new-payload' AND status='active'"
        )
        .await,
        1
    );
    assert_eq!(
        restarted
            .get_memory_payload("new-payload", scope)
            .await
            .unwrap()
            .as_deref(),
        Some("new complete SQLite payload")
    );
    restarted
        .complete_legacy_memory_reset(1_800_000_002)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .reset_legacy_memory_payload_quantum(1_800_000_003)
            .await
            .unwrap(),
        MemoryPayloadResetOutcome::Complete
    );
}

#[tokio::test]
async fn artifacts_reject_invalid_input_and_reuse_only_the_same_identity() {
    pioneer_sqlite::search::register_auto_extension_once().unwrap();
    let database = Database::connect({
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
        options
    })
    .await
    .unwrap();
    Migrator::up(&database, None).await.unwrap();
    let store = CrudStore::new(database).with_maintenance_access();
    let artifact = pioneer_crud::NewThreadEpisodicEmbeddingArtifactRecord {
        id: "artifact".into(),
        workspace_id: "workspace".into(),
        pipeline_identity_hash: "pipeline".into(),
        input_hash: "input".into(),
        provider_id: "test".into(),
        model: "test-model".into(),
        dimension: 3,
        normalized: false,
        vector: vec![2.0, 0.0, 1.0],
    };
    for vector in [
        vec![1.0],
        vec![f32::NAN, 0.0, 1.0],
        vec![f32::INFINITY, 0.0, 1.0],
        vec![0.0; 3],
    ] {
        assert!(
            store
                .insert_thread_episodic_embedding_artifact_if_absent(
                    pioneer_crud::NewThreadEpisodicEmbeddingArtifactRecord {
                        vector,
                        ..artifact.clone()
                    },
                    1_800_000_000
                )
                .await
                .is_err()
        );
        assert!(
            store
                .find_thread_episodic_embedding_artifact("artifact")
                .await
                .unwrap()
                .is_none()
        );
    }
    let empty = pioneer_crud::NewThreadEpisodicEmbeddingArtifactRecord {
        dimension: 0,
        vector: Vec::new(),
        ..artifact.clone()
    };
    assert!(
        store
            .insert_thread_episodic_embedding_artifact_if_absent(empty, 1_800_000_000)
            .await
            .is_err()
    );
    store
        .insert_thread_episodic_embedding_artifact_if_absent(artifact.clone(), 1_800_000_000)
        .await
        .unwrap();
    let reused = store
        .insert_thread_episodic_embedding_artifact_if_absent(artifact.clone(), 1_800_000_001)
        .await
        .unwrap();
    assert_eq!(reused.vector, vec![2.0, 0.0, 1.0]);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) FROM thread_episodic_embedding_artifacts"
        )
        .await,
        1
    );
    let conflicting = pioneer_crud::NewThreadEpisodicEmbeddingArtifactRecord {
        workspace_id: "other-workspace".into(),
        ..artifact
    };
    assert!(
        store
            .insert_thread_episodic_embedding_artifact_if_absent(conflicting, 1_800_000_002)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .find_thread_episodic_embedding_artifact("artifact")
            .await
            .unwrap()
            .unwrap()
            .workspace_id,
        "workspace"
    );
}

#[tokio::test]
async fn historical_quarantine_reason_remains_readable_through_current_repository() {
    let database = Database::connect({
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("foreign_keys", "OFF"));
        options
    })
    .await
    .unwrap();
    Migrator::up(&database, None).await.unwrap();
    let store = CrudStore::new(database);
    store.database_connection().execute_unprepared("INSERT INTO agent_memory_quarantine(id,memory_id,reason_code,actor_kind,created_at) VALUES('historic-quarantine','historic-memory','memvid_stale_vector','repair_job',CURRENT_TIMESTAMP)").await.unwrap();
    let history = store
        .get_active_agent_memory_quarantine("historic-memory")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        history.reason_code,
        pioneer_protocol::MemoryLifecycleReasonCode::StaleEmbeddingProjection
    );
    assert_eq!(
        serde_json::to_string(&history.reason_code).unwrap(),
        "\"stale_embedding_projection\""
    );
}
