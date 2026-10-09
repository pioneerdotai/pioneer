//! Regression sources for Proposal 73 P1. Not executed or test-target compiled.
use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};

const NAME: &str = "m20261008_000001_frozen_history_proofs";
fn position() -> usize {
    Migrator::migrations()
        .iter()
        .position(|m| m.name() == NAME)
        .unwrap()
}
async fn scalar(db: &DatabaseConnection, sql: &str) -> i64 {
    db.query_one_raw(Statement::from_string(DbBackend::Sqlite, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}
async fn before() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, Some(position() as u32)).await.unwrap();
    db
}
async fn fixture(db: &DatabaseConnection) {
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('th','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_context(owner,workspace_id,thread_id) VALUES ('owner','ws','th')",
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES ('op','owner','fp','running','{}',1)",
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES ('cp','op','owner',0,'summary','hash','{}',1,1,'candidate')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
}
async fn headers(db: &DatabaseConnection) {
    for (id, count) in [("B", 10), ("M", 40), ("zero", 0)] {
        db.execute_unprepared(&format!("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,import_count,imports_sha256) VALUES ('{id}','ws','th','digest',{count},{count},'imports')")).await.unwrap();
    }
}

#[tokio::test]
async fn registered_ddl_failure_rolls_back_all_columns_and_marker() {
    let db = before().await;
    // Force failure after many ALTER statements: no earlier change may survive.
    db.execute_unprepared("CREATE TABLE compaction_checkpoint_proof (sentinel TEXT)")
        .await
        .unwrap();
    assert!(Migrator::up(&db, None).await.is_err());
    assert_eq!(scalar(&db,"SELECT count(*) n FROM pragma_table_info('compaction_operation') WHERE name='frozen_publication_contract'").await,0);
    assert_eq!(scalar(&db,"SELECT count(*) n FROM seaql_migrations WHERE version='m20261008_000001_frozen_history_proofs'").await,0);
    db.execute_unprepared("DROP TABLE compaction_checkpoint_proof")
        .await
        .unwrap();
    Migrator::up(&db, None).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    assert_eq!(scalar(&db,"SELECT count(*) n FROM seaql_migrations WHERE version='m20261008_000001_frozen_history_proofs'").await,1);
    assert!(Migrator::down(&db, Some(1)).await.is_err());
    assert_eq!(scalar(&db,"SELECT count(*) n FROM seaql_migrations WHERE version='m20261008_000001_frozen_history_proofs'").await,1);
}

#[tokio::test]
async fn defaults_null_accounting_root_checks_and_metadata_only_revision() {
    let db = before().await;
    fixture(&db).await;
    db.execute_unprepared("INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) VALUES ('run','task','ws','th','[]',CURRENT_TIMESTAMP)").await.unwrap();
    let revision = scalar(
        &db,
        "SELECT revision n FROM compaction_task_basis_revision WHERE run_id='run'",
    )
    .await;
    Migrator::up(&db, None).await.unwrap();
    for phase in [
        "root_task",
        "root_runtime",
        "root_cli_thread",
        "root_cli_turn",
        "proof_backfill",
        "cleanup_seed",
        "detach",
        "logical_reclaim",
    ] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_maintenance_progress(workspace_id,phase) VALUES('ws',?)",
            [phase.into()],
        ))
        .await
        .unwrap();
    }
    assert_eq!(scalar(&db,"SELECT count(*) n FROM compaction_operation WHERE frozen_publication_contract='legacy_unknown' AND frozen_accounting_mode='legacy_bound' AND frozen_inventory_state='unknown' AND frozen_checkpoint_count IS NULL AND frozen_prepared_checkpoint_count IS NULL AND frozen_proof_state='legacy'").await,1);
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) n FROM compaction_checkpoint WHERE frozen_accounting_state='uncounted'"
        )
        .await,
        1
    );
    for sql in [
        "UPDATE compaction_operation SET frozen_proof_state='complete' WHERE id='op'",
        "UPDATE compaction_operation SET frozen_accounting_mode='live_known',frozen_inventory_state='known',frozen_checkpoint_count=0,frozen_prepared_checkpoint_count=0 WHERE id='op'",
        "UPDATE task_run_conversation_snapshot SET frozen_root_state='manifest' WHERE run_id='run'",
        "UPDATE task_run_conversation_snapshot SET frozen_manifest_id='B' WHERE run_id='run'",
    ] {
        assert!(db.execute_unprepared(sql).await.is_err(), "{sql}");
    }
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET frozen_root_state='none' WHERE run_id='run'",
    )
    .await
    .unwrap();
    assert_eq!(
        scalar(
            &db,
            "SELECT revision n FROM compaction_task_basis_revision WHERE run_id='run'"
        )
        .await,
        revision
    );
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET history_json='[1]' WHERE run_id='run'",
    )
    .await
    .unwrap();
    assert_eq!(
        scalar(
            &db,
            "SELECT revision n FROM compaction_task_basis_revision WHERE run_id='run'"
        )
        .await,
        revision + 1
    );
    // Derived scalars have no parent FK; orphan bytes remain authoritative.
    db.execute_unprepared("UPDATE task_run_conversation_snapshot SET frozen_root_state='manifest',frozen_manifest_id='absent' WHERE run_id='run'").await.unwrap();
    db.execute_unprepared("UPDATE compaction_operation SET frozen_publication_contract='assertion_compat' WHERE id='op'").await.unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE compaction_operation SET frozen_proof_state='pending' WHERE id='op'"
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn count_bounds_both_kinds_keep_foreign_tail_and_append_before_next() {
    let db = before().await;
    Migrator::up(&db, None).await.unwrap();
    fixture(&db).await;
    headers(&db).await;
    for ordinal in 0..40 {
        db.execute_unprepared(&format!(
            "INSERT INTO compaction_frozen_message_data VALUES ('B',{ordinal},'reference',{bytes})",
            bytes = 9
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!("INSERT INTO compaction_frozen_import_data VALUES ('B',{ordinal},0,'scope','source','opaque:v/1','th','proof',5)")).await.unwrap();
    }
    for (kind, table) in [(0, "message"), (1, "import")] {
        assert_eq!(
            scalar(
                &db,
                &format!("SELECT count(*) n FROM compaction_frozen_{table} WHERE manifest_id='B'")
            )
            .await,
            10
        );
        assert_eq!(
            scalar(
                &db,
                &format!(
                    "SELECT count(*) n FROM compaction_frozen_{table} WHERE manifest_id='zero'"
                )
            )
            .await,
            0
        );
        // ready=0/next=0 must still expose declared-count rows for exact append.
        assert_eq!(scalar(&db,&format!("SELECT count(*) n FROM compaction_frozen_{table} WHERE manifest_id='B' AND ordinal=9")).await,1);
        db.execute_unprepared(&format!(
            "INSERT INTO compaction_frozen_layout(manifest_id,kind,active) VALUES ('M',{kind},1)"
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO compaction_frozen_span VALUES ('M',{kind},0,40,'B')"
        ))
        .await
        .unwrap();
        db.execute_unprepared(
            "UPDATE compaction_frozen_history SET availability='released' WHERE id='B'",
        )
        .await
        .unwrap();
        assert_eq!(
            scalar(
                &db,
                &format!("SELECT count(*) n FROM compaction_frozen_{table} WHERE manifest_id='M'")
            )
            .await,
            40
        );
        assert_eq!(
            scalar(
                &db,
                &format!("SELECT count(*) n FROM compaction_frozen_{table} WHERE manifest_id='B'")
            )
            .await,
            0
        );
        db.execute_unprepared(
            "UPDATE compaction_frozen_history SET availability='resident' WHERE id='B'",
        )
        .await
        .unwrap();
        // A span beyond target count must not expose extra rows.
        db.execute_unprepared(&format!(
            "INSERT INTO compaction_frozen_layout(manifest_id,kind,active) VALUES ('B',{kind},1)"
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO compaction_frozen_span VALUES ('B',{kind},0,40,'B')"
        ))
        .await
        .unwrap();
        assert_eq!(
            scalar(
                &db,
                &format!("SELECT count(*) n FROM compaction_frozen_{table} WHERE manifest_id='B'")
            )
            .await,
            10
        );
    }
}

#[tokio::test]
async fn durable_dirty_events_and_physical_delete_does_not_dirty_itself() {
    let db = before().await;
    Migrator::up(&db, None).await.unwrap();
    fixture(&db).await;
    headers(&db).await;
    assert_eq!(scalar(&db,"SELECT count(*) n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND state='queued'").await,2);
    for kind in [0, 1] {
        db.execute_unprepared(&format!("INSERT INTO compaction_frozen_layout(manifest_id,kind,active,candidate) VALUES ('M',{kind},0,'B')")).await.unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO compaction_frozen_span VALUES ('M',{kind},0,40,'B')"
        ))
        .await
        .unwrap();
        let seq=scalar(&db,&format!("SELECT dirty_seq n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND kind={kind}")).await;
        db.execute_unprepared(&format!(
            "UPDATE compaction_frozen_span SET end=10 WHERE manifest_id='M' AND kind={kind}"
        ))
        .await
        .unwrap();
        assert_eq!(scalar(&db,&format!("SELECT dirty_seq n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND kind={kind}")).await,seq+1);
        db.execute_unprepared(&format!(
            "DELETE FROM compaction_frozen_span WHERE manifest_id='M' AND kind={kind}"
        ))
        .await
        .unwrap();
        assert_eq!(scalar(&db,&format!("SELECT dirty_seq n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND kind={kind}")).await,seq+2);
    }
    db.execute_unprepared(
        "UPDATE compaction_frozen_cleanup SET state='idle' WHERE manifest_id='B'",
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO compaction_frozen_use VALUES ('use','B',0,'read','ws')")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM compaction_frozen_use WHERE use_id='use'")
        .await
        .unwrap();
    assert_eq!(scalar(&db,"SELECT count(*) n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND state='queued'").await,2);
    db.execute_unprepared("INSERT INTO compaction_frozen_message_data VALUES ('B',0,'r',1)")
        .await
        .unwrap();
    let seq = scalar(
        &db,
        "SELECT dirty_seq n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND kind=0",
    )
    .await;
    db.execute_unprepared("DELETE FROM compaction_frozen_message_data WHERE manifest_id='B'")
        .await
        .unwrap();
    assert_eq!(
        scalar(
            &db,
            "SELECT dirty_seq n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND kind=0"
        )
        .await,
        seq
    );
    db.execute_unprepared(
        "UPDATE compaction_frozen_cleanup SET state='quarantined' WHERE manifest_id='B'",
    )
    .await
    .unwrap();
    db.execute_unprepared("UPDATE compaction_frozen_history SET storage_generation=1 WHERE id='B'")
        .await
        .unwrap();
    assert_eq!(scalar(&db,"SELECT count(*) n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND state='quarantined' AND storage_generation=1").await,2);
    // Layout deletion removes inactive candidate dependency for BOTH kinds.
    let seq = scalar(
        &db,
        "SELECT dirty_seq n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND kind=1",
    )
    .await;
    db.execute_unprepared("DELETE FROM compaction_frozen_layout WHERE manifest_id='M' AND kind=0")
        .await
        .unwrap();
    assert_eq!(
        scalar(
            &db,
            "SELECT dirty_seq n FROM compaction_frozen_cleanup WHERE manifest_id='B' AND kind=1"
        )
        .await,
        seq + 1
    );
}

#[tokio::test]
async fn proof_opaque_versions_conflicting_claims_constraints_and_fk_ownership() {
    let db = before().await;
    Migrator::up(&db, None).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=ON")
        .await
        .unwrap();
    fixture(&db).await;
    headers(&db).await;
    db.execute_unprepared("INSERT INTO compaction_checkpoint_proof(checkpoint_id,operation_id,owner,workspace_id,thread_id,checkpoint_identity_sha256,projection_version,format_version,coverage_domain,origin_manifest_id,origin_message_count,origin_identity_sha256,origin_import_count,origin_imports_sha256,coverage_count,coverage_sha256,alias_count,aliases_sha256,evidence_count,evidence_sha256) VALUES ('cp','op','owner','ws','th','hash',1,1,'own_contribution','B',10,'digest',10,'imports',0,'coverage',2,'aliases',1,'evidence')").await.unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE compaction_checkpoint_proof SET state='prepared' WHERE checkpoint_id='cp'"
        )
        .await
        .is_err()
    );
    for (ordinal, covered) in [(0, "one"), (1, "two")] {
        db.execute_unprepared(&format!("INSERT INTO compaction_checkpoint_replay_proof VALUES ('cp',{ordinal},'th','event:turn','{covered}','opaque:before','th','input:turn','same','opaque:after',NULL)")).await.unwrap();
    }
    db.execute_unprepared("INSERT INTO compaction_checkpoint_event_input_proof VALUES ('cp',0,'th','event:turn','one','opaque:before','deleted')").await.unwrap();
    assert_eq!(scalar(&db,"SELECT count(*) n FROM compaction_checkpoint_replay_proof WHERE covered_version='opaque:before' AND replay_version='opaque:after'").await,2);
    assert!(db.execute_unprepared("INSERT INTO compaction_checkpoint_event_input_proof VALUES ('cp',1,'th','scope','id','version','invalid')").await.is_err());
    db.execute_unprepared("UPDATE compaction_checkpoint_proof SET next_alias=2,next_evidence=1,state='prepared' WHERE checkpoint_id='cp'").await.unwrap();
    assert!(
        db.execute_unprepared(
            "INSERT INTO compaction_frozen_use VALUES ('bad','missing',0,'read','ws')"
        )
        .await
        .is_err()
    );
    db.execute_unprepared("INSERT INTO compaction_frozen_use VALUES ('good','B',0,'read','ws')")
        .await
        .unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE compaction_frozen_cleanup SET state='running' WHERE manifest_id='B'"
        )
        .await
        .is_err()
    );
    db.execute_unprepared("UPDATE compaction_frozen_cleanup SET state='running',pass_seq=dirty_seq,ceiling_ordinal=-1 WHERE manifest_id='B'").await.unwrap();
    db.execute_unprepared("DELETE FROM compaction_frozen_history WHERE id='B'")
        .await
        .unwrap();
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) n FROM compaction_frozen_use WHERE manifest_id='B'"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) n FROM compaction_frozen_cleanup WHERE manifest_id='B'"
        )
        .await,
        0
    );
    // Provenance receipt deliberately has no origin FK.
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) n FROM compaction_checkpoint_proof WHERE origin_manifest_id='B'"
        )
        .await,
        1
    );
    db.execute_unprepared("DELETE FROM compaction_checkpoint WHERE id='cp'")
        .await
        .unwrap();
    assert_eq!(
        scalar(&db, "SELECT count(*) n FROM compaction_checkpoint_proof").await,
        0
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) n FROM compaction_checkpoint_replay_proof"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) n FROM compaction_checkpoint_event_input_proof"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn workspace_inventory_keys_have_ordered_seek_indexes() {
    let db = before().await;
    Migrator::up(&db, None).await.unwrap();
    for name in [
        "task_run_conversation_snapshot_frozen_inventory",
        "turn_runtime_snapshot_frozen_inventory",
        "thread_cli_runtime_binding_frozen_inventory",
        "turn_cli_runtime_binding_frozen_inventory",
        "compaction_frozen_history_frozen_inventory",
    ] {
        assert_eq!(
            scalar(
                &db,
                &format!(
                    "SELECT COUNT(*) n FROM sqlite_schema WHERE type='index' AND name='{name}'"
                )
            )
            .await,
            1
        );
    }
}
