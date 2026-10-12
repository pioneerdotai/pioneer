//! Future isolated fixtures. Deliberately not executed/compiled before review.
use migration::{Migrator, MigratorTrait};
use pioneer_crud::CrudStore;
use sea_orm::{ConnectionTrait, DbBackend, Statement};

async fn fixture() -> CrudStore {
    let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
    options.max_connections(1).min_connections(1);
    let db = sea_orm::Database::connect(options).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    let pragma: i64 = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA foreign_keys",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "foreign_keys")
        .unwrap();
    assert_eq!(pragma, 0, "production single writer uses FK OFF");
    Migrator::up(&db, None).await.unwrap();
    let store = CrudStore::new(db);
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    store
}
async fn count(store: &CrudStore, view: &str, id: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            format!("SELECT COUNT(*) AS n FROM {view} WHERE manifest_id=?"),
            [id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

#[tokio::test]
async fn physical_source_counts_and_expiry_do_not_clip_foreign_streams() {
    let store = fixture().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES('p','ws','thread','digest',2,2,1,1,'imports',1),('h','ws','thread','digest-h',4,4,3,3,'imports-h',1)",
        "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('p',0,'m0',2),('p',1,'m1',2),('p',2,'m2',2),('p',3,'m3',2)",
        "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('p',0,0,'event:turn','e','event-revision:1','thread','i0',2),('p',1,2,'event:turn','e1','event-revision:1','thread','i1',2),('p',2,3,'event:turn','e2','event-revision:1','thread','i2',2)",
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('h',0,0,1),('h',1,0,1)",
        "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('h',0,0,4,'p'),('h',1,0,3,'p')",
        "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id='h'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_eq!(count(&store, "compaction_frozen_message", "p").await, 2);
    assert_eq!(count(&store, "compaction_frozen_import", "p").await, 1);
    assert_eq!(count(&store, "compaction_frozen_message", "h").await, 4);
    assert_eq!(count(&store, "compaction_frozen_import", "h").await, 3);
    db.execute_unprepared("UPDATE compaction_frozen_history SET expired=1 WHERE id='p'")
        .await
        .unwrap();
    assert_eq!(count(&store, "compaction_frozen_message", "p").await, 0);
    assert_eq!(count(&store, "compaction_frozen_import", "p").await, 0);
    assert_eq!(count(&store, "compaction_frozen_message", "h").await, 4);
    assert_eq!(count(&store, "compaction_frozen_import", "h").await, 3);
    assert!(
        db.execute_unprepared("UPDATE compaction_frozen_history SET expired=0 WHERE id='p'")
            .await
            .is_err()
    );
    assert_eq!(
        count(&store, "compaction_frozen_message_data", "p").await,
        4
    );
}

#[tokio::test]
async fn incomplete_streams_expose_only_committed_kind_specific_prefix() {
    let store = fixture().await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES('p','ws','thread','digest',4,2,3,1,'imports',0)").await.unwrap();
    db.execute_unprepared("INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('p',0,'m0',2),('p',1,'m1',2),('p',2,'m2',2)").await.unwrap();
    assert_eq!(count(&store, "compaction_frozen_message", "p").await, 2);
    let descriptor = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: "p".into(),
        messages: 4,
        identity_sha256: "digest".into(),
    };
    assert!(
        store
            .compaction_acquire_frozen_history("ws", &descriptor)
            .await
            .is_err()
    );
    db.execute_unprepared("UPDATE compaction_frozen_history SET next_ordinal=5 WHERE id='p'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_frozen_history_owner("ws", &descriptor)
            .await
            .is_err()
    );
}

fn thread_binding(cursor: String) -> pioneer_crud::NewCliRuntimeThreadBinding {
    let now = chrono::Utc::now().fixed_offset();
    pioneer_crud::NewCliRuntimeThreadBinding {
        thread_id: "thread".into(),
        workspace_id: "ws".into(),
        runtime_id: "runtime".into(),
        runtime_kind: "claude".into(),
        native_thread_id: "native".into(),
        native_session_id: None,
        native_root_thread_id: None,
        native_cwd: None,
        native_model: None,
        resume_cursor_json: cursor,
        status: "active".into(),
        created_at: now,
        updated_at: now,
    }
}
async fn locator(store: &CrudStore) -> Option<String> {
    use sea_orm::EntityTrait;
    pioneer_entity::thread_cli_runtime_binding::Entity::find_by_id("thread")
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap()
        .frozen_manifest_id
}
#[tokio::test]
async fn cli_root_replacement_and_malformed_write_preserve_exact_locator() {
    let store = fixture().await;
    let descriptor = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: "cli-input".into(),
        messages: 0,
        identity_sha256: "a".repeat(64),
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &descriptor)
        .await
        .unwrap();
    let hold = store
        .compaction_finish_frozen_history_held("ws", "thread", &descriptor)
        .await
        .unwrap();
    let cursor = serde_json::json!({"pioneerContext": {
        "version": 4, "nativeThreadId": "native", "acceptedTurnId": "accepted",
        "acceptedTurnRevision": 1, "acceptedTurnDeleted": false,
        "contextOwnerThreadId": "thread", "contextHistoryJson": serde_json::to_string(&descriptor).unwrap()
    }}).to_string();
    store
        .upsert_cli_runtime_thread_binding(thread_binding(cursor.clone()))
        .await
        .unwrap();
    drop(hold);
    assert_eq!(locator(&store).await.as_deref(), Some("cli-input"));
    assert!(
        store
            .upsert_cli_runtime_thread_binding(thread_binding("{\"pioneerContext\":{}}".into()))
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get_cli_runtime_thread_binding("thread")
            .await
            .unwrap()
            .unwrap()
            .resume_cursor_json,
        cursor
    );
    assert_eq!(locator(&store).await.as_deref(), Some("cli-input"));
    store
        .upsert_cli_runtime_thread_binding(thread_binding("{\"provider\":\"claude\"}".into()))
        .await
        .unwrap();
    assert_eq!(locator(&store).await, None);
    store
        .database_connection()
        .execute_unprepared("UPDATE compaction_frozen_history SET expired=1 WHERE id='cli-input'")
        .await
        .unwrap();
    assert!(
        store
            .upsert_cli_runtime_thread_binding(thread_binding(cursor))
            .await
            .is_err()
    );
    assert_eq!(locator(&store).await, None);
}
#[tokio::test]
async fn root_default_unknown_is_rejected_and_metadata_update_preserves_locator() {
    let store = fixture().await;
    let db = store.database_connection();
    assert!(db.execute_unprepared("INSERT INTO thread_cli_runtime_binding(thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,status) VALUES('thread','ws','runtime','claude','native','active')").await.is_err());
    store
        .upsert_cli_runtime_thread_binding(thread_binding("{}".into()))
        .await
        .unwrap();
    db.execute_unprepared("UPDATE thread_cli_runtime_binding SET status='active',updated_at=CURRENT_TIMESTAMP WHERE thread_id='thread'").await.unwrap();
    assert_eq!(locator(&store).await, None);
    assert!(
        db.execute_unprepared(
            "UPDATE thread_cli_runtime_binding SET frozen_manifest_id='' WHERE thread_id='thread'"
        )
        .await
        .is_err()
    );
    assert_eq!(locator(&store).await, None);
}
#[tokio::test]
async fn overlapping_layout_across_page_boundary_is_a_consistency_error() {
    let store = fixture().await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,ready) VALUES('spans','ws','thread','digest',260,260,1)").await.unwrap();
    db.execute_unprepared("INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('spans',0,0,1)").await.unwrap();
    for ordinal in 0..130 {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('spans',0,?1,?2,'spans')", [((ordinal*2) as i64).into(), ((ordinal*2+2) as i64).into()])).await.unwrap();
    }
    db.execute_unprepared("INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('spans',0,255,257,'spans')").await.unwrap();
    db.execute_unprepared(
        "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id='spans'",
    )
    .await
    .unwrap();
    let descriptor = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: "spans".into(),
        messages: 260,
        identity_sha256: "digest".into(),
    };
    assert!(
        store
            .compaction_acquire_frozen_history("ws", &descriptor)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn claude_reuse_force_new_and_provider_switch_keep_verified_root_semantics() {
    use pioneer_crud::PrepareClaudeProviderSessionBinding;
    let store = fixture().await;
    let descriptor = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: "claude-input".into(),
        messages: 0,
        identity_sha256: pioneer_crud::compaction::EMPTY_FROZEN_IMPORT_SHA256.into(),
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &descriptor)
        .await
        .unwrap();
    let hold = store
        .compaction_finish_frozen_history_held("ws", "thread", &descriptor)
        .await
        .unwrap();
    let cursor = serde_json::json!({"pioneerContext": {
        "version": 4, "nativeThreadId": "native", "acceptedTurnId": "accepted",
        "acceptedTurnRevision": 1, "acceptedTurnDeleted": false, "contextOwnerThreadId": "thread",
        "contextHistoryJson": serde_json::to_string(&descriptor).unwrap()
    }})
    .to_string();
    let request = |binding, force_new| PrepareClaudeProviderSessionBinding {
        thread_binding: binding,
        proposed_provider_session_id: uuid::Uuid::new_v4().to_string(),
        force_new,
    };
    let created = store
        .prepare_claude_provider_session_binding(request(thread_binding(cursor.clone()), false))
        .await
        .unwrap();
    drop(hold);
    assert_eq!(locator(&store).await.as_deref(), Some("claude-input"));
    let reused = store
        .prepare_claude_provider_session_binding(request(thread_binding("{}".into()), false))
        .await
        .unwrap();
    assert_eq!(reused.binding.resume_cursor_json, cursor);
    assert_eq!(
        reused.binding.native_thread_id,
        created.binding.native_thread_id
    );
    assert_eq!(locator(&store).await.as_deref(), Some("claude-input"));
    store
        .prepare_claude_provider_session_binding(request(thread_binding("{}".into()), true))
        .await
        .unwrap();
    assert_eq!(locator(&store).await, None);
    let mut other = thread_binding("{}".into());
    other.runtime_id = "codex-runtime".into();
    other.runtime_kind = "codex".into();
    store
        .upsert_cli_runtime_thread_binding(other)
        .await
        .unwrap();
    store
        .prepare_claude_provider_session_binding(request(thread_binding("{}".into()), true))
        .await
        .unwrap();
    assert_eq!(locator(&store).await, None);
    assert_eq!(
        store
            .get_cli_runtime_thread_binding("thread")
            .await
            .unwrap()
            .unwrap()
            .runtime_kind,
        "claude"
    );
}
