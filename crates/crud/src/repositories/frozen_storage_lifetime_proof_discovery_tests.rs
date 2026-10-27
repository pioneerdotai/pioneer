use super::*;
use std::sync::{Arc, Mutex};

const MIGRATION: &str = "m20261026_000002_frozen_proof_discovery_index";
type RecordedStatements = Arc<Mutex<Vec<Statement>>>;

// Isolated metadata-only fixtures. Synthetic seals test discovery membership,
// never authority to publish; real sealing is covered by the queued-proof test.
async fn metadata_fixture(
    before_index: bool,
) -> (CrudStore, sea_orm::DatabaseConnection, RecordedStatements) {
    let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
    options.max_connections(1).min_connections(1);
    let mut db = Database::connect(options).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    let before = Migrator::migrations()
        .iter()
        .position(|migration| migration.name() == MIGRATION)
        .expect("proof discovery index must be registered");
    Migrator::up(&db, before_index.then_some(before as u32))
        .await
        .unwrap();
    let statements = RecordedStatements::default();
    let captured = statements.clone();
    db.set_metric_callback(move |info| {
        captured.lock().unwrap().push(info.statement.clone());
    });
    let store = CrudStore::new(db.clone()).with_maintenance_access();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,next_portion) VALUES('failed','owner','failed','failed','{}',1000,0)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,next_portion) VALUES('completed','owner','completed','completed','{}',1000,0)",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,next_portion) VALUES('raw','owner','raw','failed','{}',1000,0)",
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready) VALUES('origin','ws','thread','identity','imports',0,0,0,0,1)",
        "INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT id,'origin','identity','imports',0 FROM compaction_operation WHERE id IN ('failed','completed')",
    ] {
        store.connection.execute_unprepared(sql).await.unwrap();
    }
    (store, db, statements)
}

async fn header(store: &CrudStore, id: &str, op: &str, status: &str, version: i64) {
    store.connection.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status,proof_version) VALUES(?1,?2,'owner',COALESCE((SELECT portion+1 FROM compaction_checkpoint WHERE operation_id=?2 ORDER BY portion DESC LIMIT 1),0),'summary','identity','{}',1,1,?3,?4)",
        [id.into(), op.into(), status.into(), version.into()],
    )).await.unwrap();
}

async fn raw_candidate_fixture() -> (CrudStore, Checkpoint) {
    use crate::repositories::compaction::{CanonicalSource, SourceAssertion};
    use pioneer_compaction::{
        CompactionMode, CompactionPlan, CompactionSettings, CoverageDomain, OperationSnapshot,
    };
    let (store, _, _) = metadata_fixture(false).await;
    for sql in [
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES('source','thread','turn',1,'fixture','original',CURRENT_TIMESTAMP)",
    ] {
        store.connection.execute_unprepared(sql).await.unwrap();
    }
    let revision = store
        .connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT revision FROM compaction_event_revision WHERE source_id='source'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "revision")
        .unwrap();
    let source = SourceAssertion {
        revision: Some(revision),
        kind: CanonicalSource::Event,
        turn_id: "turn".into(),
        id: "source".into(),
        payload: "original".into(),
    };
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "p".into(),
        model: "m".into(),
        effort: None,
    };
    let snapshot = OperationSnapshot {
        id: "raw-proof".into(),
        owner: "owner".into(),
        expected_checkpoint: None,
        projection_version: 1,
        source_epochs: BTreeMap::new(),
        admission: CompactionSettings::default()
            .admit(&selection, None, 10)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: CoverageDomain::OwnContribution,
            compact: vec![0],
            retain: vec![],
            coverage: vec![source.reference()],
            fingerprint: "raw-proof".into(),
        },
    };
    store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    let checkpoint = Checkpoint {
        id: "a-demoted".into(),
        operation_id: snapshot.id,
        owner: snapshot.owner,
        previous: None,
        summary: "saved summary".into(),
        selection,
        coverage: snapshot.plan.coverage,
        projection_version: 1,
        format_version: 1,
    };
    store
        .compaction_save_candidate(&checkpoint, 0)
        .await
        .unwrap();
    (store, checkpoint)
}

pub(super) async fn queued_raw_candidate_rechecks_concurrent_seal() {
    let (store, checkpoint) = raw_candidate_fixture().await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    assert!(progress.proof_quantum(&store).await.unwrap());
    assert_eq!(
        progress.proof_pending,
        VecDeque::from([checkpoint.id.clone()])
    );
    checkpoint_proofs::prepare_graph(&store, &checkpoint.id)
        .await
        .unwrap();
    assert!(progress.proof_quantum(&store).await.unwrap());
    assert!(
        progress.proof.is_some(),
        "a private raw seal still needs full graph preparation"
    );
    for _ in 0..500 {
        assert!(progress.proof_quantum(&store).await.unwrap());
        if progress.proof.is_none() {
            break;
        }
    }
    assert!(progress.proof.is_none());
    assert_eq!(progress.accounting.proofs, 1);
    assert!(store.frozen_readers.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sparse_unsealed_pages_seek_past_seals_and_keep_parameterized_order() {
    let (store, _, statements) = metadata_fixture(false).await;
    let mut unsealed = Vec::new();
    for i in 0..530 {
        let id = format!("cp-{i:04}");
        // Include adjacent unsealed rows on both sides of the old 64-ID page.
        let version = if i % 8 == 7 || i == 64 { 0 } else { 1 };
        header(&store, &id, "raw", "candidate", version).await;
        if version == 0 {
            unsealed.push(id);
        }
    }
    let mut progress = FrozenStorageLifetimeProgress::default();
    statements.lock().unwrap().clear();
    assert!(progress.proof_quantum(&store).await.unwrap());
    assert_eq!(progress.proof_after, unsealed[63]);
    assert_eq!(
        progress.proof_pending,
        VecDeque::from(unsealed[..64].to_vec())
    );
    assert!(
        progress.proof.is_none(),
        "metadata discovery does not prepare graphs"
    );
    let mut statement = statements.lock().unwrap().last().unwrap().clone();
    // Explain the actual SeaQuery statement with its original bound values.
    // Assert the useful access property, never an entire plan/SQL snapshot.
    statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
    let plan = store.connection.query_all_raw(statement).await.unwrap();
    let details: Vec<String> = plan
        .iter()
        .map(|row| row.try_get("", "detail").unwrap())
        .collect();
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("SEARCH compaction_checkpoint ")
                && detail.contains("checkpoint_proof_discovery")
                && detail.contains("proof_version=?")
                && detail.contains("id>?")),
        "{details:#?}"
    );
    assert!(
        !details.iter().any(|detail| detail.contains("TEMP B-TREE")),
        "{details:#?}"
    );

    // Isolate page discovery from dequeue: the membership assertions above
    // prove this page contains only unsealed candidates before consuming it.
    progress.proof_pending.clear();
    assert!(progress.proof_quantum(&store).await.unwrap());
    assert_eq!(
        progress.proof_pending,
        VecDeque::from(unsealed[64..].to_vec())
    );
    assert_eq!(progress.proof_after, *unsealed.last().unwrap());
    progress.proof_pending.clear();
    assert!(!progress.proof_quantum(&store).await.unwrap());
    assert!(progress.proof_after.is_empty());
}

#[tokio::test]
async fn discovery_keeps_all_eligibility_branches_and_dequeue_rechecks_scope_and_format() {
    let (store, _, _) = metadata_fixture(false).await;
    for (id, op, status) in [
        ("a-applied", "failed", "applied"),
        ("b-retained", "completed", "retained"),
        ("c-no-projection", "raw", "candidate"),
        ("d-retained-failed", "failed", "retained"),
        ("e-private-bound", "completed", "candidate"),
    ] {
        header(&store, id, op, status, 0).await;
    }
    let mut progress = FrozenStorageLifetimeProgress::default();
    assert!(progress.proof_quantum(&store).await.unwrap());
    assert_eq!(
        progress.proof_pending,
        VecDeque::from([
            "a-applied".to_owned(),
            "b-retained".to_owned(),
            "c-no-projection".to_owned(),
        ])
    );
    assert_eq!(progress.proof_after, "e-private-bound");
    // These rows were really discovered, then their scalar authority changed.
    store
        .connection
        .execute_unprepared(
            "UPDATE compaction_checkpoint SET format_version=2 WHERE id='a-applied'",
        )
        .await
        .unwrap();
    progress.phase = 1;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(!progress.pass_done[1] && progress.proof.is_none());
    assert_eq!(progress.proof_pending.front().unwrap(), "b-retained");
    store
        .connection
        .execute_unprepared("UPDATE compaction_context SET workspace_id='' WHERE owner='owner'")
        .await
        .unwrap();
    progress.phase = 1;
    assert!(progress.quantum(&store).await.unwrap());
    assert!(!progress.pass_done[1] && progress.proof.is_none());
    assert_eq!(progress.proof_pending.front().unwrap(), "c-no-projection");
    assert!(
        checkpoint::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .iter()
            .all(|row| row.proof_version == 0)
    );
    assert!(store.frozen_readers.lock().unwrap().is_empty());
    for sql in [
        "UPDATE compaction_checkpoint SET format_version=1 WHERE id='a-applied'",
        "UPDATE compaction_context SET workspace_id='ws' WHERE owner='owner'",
    ] {
        store.connection.execute_unprepared(sql).await.unwrap();
    }
    progress.proof_pending.clear();
    assert!(!progress.proof_quantum(&store).await.unwrap());
    assert!(progress.proof_quantum(&store).await.unwrap());
    assert_eq!(
        progress.proof_pending,
        VecDeque::from([
            "a-applied".to_owned(),
            "b-retained".to_owned(),
            "c-no-projection".to_owned(),
        ]),
        "repaired candidates return on the next pass"
    );
}

#[tokio::test]
async fn demotion_behind_cursor_is_rediscovered_after_pass_and_restart() {
    let (store, checkpoint) = raw_candidate_fixture().await;
    checkpoint_proofs::prepare_graph(&store, &checkpoint.id)
        .await
        .unwrap();
    header(&store, "z-refused", "raw", "candidate", 0).await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    assert!(progress.proof_quantum(&store).await.unwrap());
    assert_eq!(
        progress.proof_pending,
        VecDeque::from(["z-refused".to_owned()])
    );
    assert_eq!(progress.proof_after, "z-refused");
    store
        .connection
        .execute_unprepared("UPDATE compaction_checkpoint SET proof_version=0 WHERE id='a-demoted'")
        .await
        .unwrap();
    // The metadata-only candidate is deliberately malformed. Refusal advances
    // this visit without becoming EOF or hiding rows from a repaired visit.
    let mut refused = false;
    for _ in 0..100 {
        let before = progress.accounting.refused;
        progress.phase = 1;
        assert!(progress.quantum(&store).await.unwrap());
        assert!(!progress.pass_done[1]);
        if progress.accounting.refused > before {
            refused = true;
            break;
        }
    }
    assert!(refused && progress.proof.is_none() && progress.proof_pending.is_empty());
    assert_eq!(progress.proof_after, "z-refused");
    // Only the next successful empty read completes this family. Mark the
    // other families complete to exercise the real global-pass reset.
    progress.pass_done = [true, false, true, true, true, true];
    progress.phase = 1;
    assert!(!progress.quantum(&store).await.unwrap());
    assert_eq!(progress.pass_done, [false; 6]);
    assert!(progress.proof_after.is_empty());
    progress.phase = 1;
    assert!(progress.quantum(&store).await.unwrap());
    let expected = VecDeque::from(["a-demoted".to_owned(), "z-refused".to_owned()]);
    assert_eq!(progress.proof_pending, expected);
    let mut restart = FrozenStorageLifetimeProgress::default();
    assert!(restart.proof_quantum(&store).await.unwrap());
    assert_eq!(restart.proof_pending, expected);
    for _ in 0..500 {
        assert!(restart.proof_quantum(&store).await.unwrap());
        if restart.accounting.proofs == 1 {
            break;
        }
    }
    assert_eq!(restart.accounting.proofs, 1);
    assert_eq!(
        checkpoint::Entity::find_by_id(&checkpoint.id)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .proof_version,
        1,
        "demoted raw candidate is prepared again"
    );
}

#[tokio::test]
async fn discovery_database_error_preserves_cursor_and_does_not_latch_eof() {
    let (store, _, _) = metadata_fixture(false).await;
    header(&store, "a", "raw", "candidate", 0).await;
    header(&store, "b", "raw", "candidate", 0).await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    progress.proof_after = "a".into();
    store
        .connection
        .execute_unprepared(
            "ALTER TABLE compaction_operation_projection RENAME TO unavailable_projection",
        )
        .await
        .unwrap();
    progress.phase = 1;
    let error = progress.quantum(&store).await.unwrap_err();
    assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
    assert!(!progress.pass_done[1]);
    assert_eq!(progress.proof_after, "a");
    assert!(progress.proof_pending.is_empty());
    store
        .connection
        .execute_unprepared(
            "ALTER TABLE unavailable_projection RENAME TO compaction_operation_projection",
        )
        .await
        .unwrap();
    progress.phase = 1;
    assert!(progress.quantum(&store).await.unwrap());
    assert_eq!(progress.proof_pending, VecDeque::from(["b".to_owned()]));
    assert_eq!(progress.proof_after, "b");
    assert!(!progress.pass_done[1]);
}

async fn index_columns(store: &CrudStore, name: &str) -> Vec<String> {
    store
        .connection
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
            [name.into()],
        ))
        .await
        .unwrap()
        .iter()
        .map(|row| row.try_get("", "name").unwrap())
        .collect()
}

#[tokio::test]
async fn incremental_proof_index_upgrade_preserves_headers_and_other_indexes() {
    let (store, db, _) = metadata_fixture(true).await;
    header(&store, "legacy-unsealed", "raw", "candidate", 0).await;
    header(&store, "legacy-sealed", "raw", "candidate", 1).await;
    let before = checkpoint::Entity::find()
        .order_by_asc(checkpoint::Column::Id)
        .all(&store.connection)
        .await
        .unwrap();
    assert!(
        index_columns(&store, "checkpoint_proof_discovery")
            .await
            .is_empty()
    );
    Migrator::up(&db, None).await.unwrap();
    assert_eq!(
        index_columns(&store, "checkpoint_proof_discovery").await,
        ["proof_version", "id"]
    );
    let indexes = ["checkpoint_proof_operation", "checkpoint_unproved_owner"];
    let mut others = Vec::new();
    for name in indexes {
        let columns = index_columns(&store, name).await;
        assert!(!columns.is_empty());
        others.push(columns);
    }
    // Explicit retry/down exercises only this incremental migration; never
    // roll back the historical frozen-lifetime migration or a neighbor.
    let migrations = Migrator::migrations();
    let migration = migrations.iter().find(|m| m.name() == MIGRATION).unwrap();
    let manager = migration::SchemaManager::new(&db);
    migration.up(&manager).await.unwrap();
    migration.down(&manager).await.unwrap();
    assert!(
        index_columns(&store, "checkpoint_proof_discovery")
            .await
            .is_empty()
    );
    migration.up(&manager).await.unwrap();
    assert_eq!(
        index_columns(&store, "checkpoint_proof_discovery").await,
        ["proof_version", "id"]
    );
    for (name, columns) in indexes.into_iter().zip(others) {
        assert_eq!(index_columns(&store, name).await, columns);
    }
    let after = checkpoint::Entity::find()
        .order_by_asc(checkpoint::Column::Id)
        .all(&store.connection)
        .await
        .unwrap();
    assert_eq!(after, before);
}
