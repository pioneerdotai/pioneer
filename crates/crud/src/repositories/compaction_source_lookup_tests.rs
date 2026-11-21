use super::*;
use pioneer_sqlite::SqliteDatabase;

#[pioneer_postgres_test::postgres_test]
async fn checkpoint_payload_public_path_returns_the_exact_summary() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection();
    writer
        .execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws','fixture');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('root-thread','ws','','agent','m','p','active');
         INSERT INTO compaction_context(workspace_id,thread_id,owner,format_version)
             VALUES ('ws','root-thread','root-owner',1);
         INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms)
             VALUES ('root-operation','root-owner','root-fixture','completed','{}',1);
         INSERT INTO compaction_checkpoint
             (id,operation_id,owner,portion,summary,identity_sha256,selection,
              projection_version,format_version,status)
             VALUES ('root-checkpoint','root-operation','root-owner',0,
                     'root summary','root-version','{}',0,1,'applied');",
        )
        .await
        .unwrap();
    let store = CrudStore::new(SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        writer.clone(),
    ))
    .with_maintenance_access();
    let reference = SourceRef {
        scope: "checkpoint:root-owner".into(),
        id: "root-checkpoint".into(),
        version: "root-version".into(),
    };
    assert_eq!(
        store
            .compaction_reference_payload("ws", "root-thread", &reference)
            .await
            .unwrap()
            .as_deref(),
        Some("root summary")
    );
}
