use pioneer_compaction::{SourceRef, frozen::FrozenMessageRef};
use pioneer_crud_compaction::frozen_capture::{
    ReadyLayoutCheck, compaction_append_frozen_history,
    compaction_begin_frozen_history_with_imports, compaction_finish_frozen_history,
    compaction_frozen_history_page, mark_storage_registered,
};
use pioneer_crud_compaction::frozen_import::{
    EMPTY_FROZEN_IMPORT_SHA256, compaction_frozen_import_page,
};
use pioneer_crud_compaction::frozen_storage::{FrozenConversionProgress, maintain, prepare};
use pioneer_entity::compaction_frozen_message_data;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{
    ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait, Statement, TransactionTrait,
};

#[pioneer_postgres_test::postgres_test]
async fn restart_staged_copy_next_does_not_publish_wrong_backing() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connection().clone(),
    );
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES('ws','Fixture');
        INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
            VALUES('thread','ws','','agent','model','provider','active')",
    )
    .await
    .unwrap();
    let refs = shared_refs(2);
    capture(&db, "base", &refs, true).await;
    capture(&db, "staged", &refs, false).await;
    db.execute_unprepared("INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,candidate,compared,copy_to,copy_next) VALUES('staged',0,0,1,'base',2,2,2)").await.unwrap();
    // Plausible old progress with corrupt physical source. Direct rows still
    // exist, so rejecting activation must keep the old logical layout usable.
    db.execute_unprepared("INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('staged',0,0,2,'staged')").await.unwrap();
    let mut progress = FrozenConversionProgress::default();
    let mut rejected = false;
    for _ in 0..128 {
        if maintain(&db, &mut progress, None).await.is_err() {
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    let state=db.query_one_raw(Statement::from_string(DbBackend::Postgres,"SELECT active,failed FROM compaction_frozen_layout WHERE manifest_id='staged' AND kind=0")).await.unwrap().unwrap();
    assert_eq!(state.try_get::<i64>("", "active").unwrap(), 0);
    assert_eq!(state.try_get::<i64>("", "failed").unwrap(), 1);
    assert!(
        compaction_frozen_history_page(&db, "ws", "thread", "staged", 0)
            .await
            .is_err(),
        "corrupt layout is explicit, not empty history"
    );
    db.execute_unprepared(
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,failed) VALUES('staged',1,0,1,1)"
    ).await.unwrap();
    assert!(
        compaction_frozen_import_page(&db, "ws", "thread", "staged", 0)
            .await
            .is_err(),
        "a failed import layout is explicit even when its stream is empty"
    );
    assert_eq!(
        compaction_frozen_message_data::Entity::find()
            .count(&db)
            .await
            .unwrap(),
        4,
    );
    assert_eq!(
        compaction_frozen_history_page(&db, "ws", "thread", "base", 0)
            .await
            .unwrap(),
        refs,
    );
}

fn shared_refs(count: usize) -> Vec<pioneer_compaction::frozen::FrozenMessageRef> {
    (0..count)
        .map(|i| pioneer_compaction::frozen::FrozenMessageRef {
            logical_turn_id: Some("turn".into()),
            context_thread: None,
            source_thread: "thread".into(),
            unit_id: format!("u{i}"),
            sources: vec![SourceRef {
                scope: "event:turn".into(),
                id: format!("e{i}"),
                version: "event-revision:1".into(),
            }],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "a".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        })
        .collect()
}
fn shared_descriptor(
    id: &str,
    refs: &[pioneer_compaction::frozen::FrozenMessageRef],
) -> pioneer_compaction::frozen::FrozenHistoryRef {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for r in refs {
        let b = serde_json::to_vec(r).unwrap();
        digest.update((b.len() as u64).to_be_bytes());
        digest.update(b);
    }
    pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: id.into(),
        messages: refs.len() as u64,
        identity_sha256: hex::encode(digest.finalize()),
    }
}

async fn capture(db: &SqliteDatabase, id: &str, refs: &[FrozenMessageRef], register: bool) {
    let descriptor = shared_descriptor(id, refs);
    compaction_begin_frozen_history_with_imports(
        db,
        "ws",
        "thread",
        &descriptor,
        0,
        EMPTY_FROZEN_IMPORT_SHA256,
    )
    .await
    .unwrap();
    if register {
        let wire = refs
            .iter()
            .map(|r| serde_json::to_string(r).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(prepare(db, "ws", "thread", id, 0, &wire).await.unwrap(), 0);
        assert_eq!(prepare(db, "ws", "thread", id, 1, &[]).await.unwrap(), 0);
        mark_storage_registered(db, id).await.unwrap();
    }
    compaction_append_frozen_history(db, "ws", "thread", id, 0, refs)
        .await
        .unwrap();
    let layout = ReadyLayoutCheck::prepare(db, id).await.unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        compaction_finish_frozen_history(&tx, "ws", "thread", &descriptor, layout.as_ref())
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
}
