use anyhow::Result;
use pioneer_crud_compaction::checkpoint_projection::{
    compaction_checkpoint_body, compaction_checkpoint_metadata,
};
use sea_orm::ConnectionTrait;

#[pioneer_postgres_test::postgres_test]
async fn checkpoint_body_and_metadata_decode_the_persisted_model_selection() -> Result<()> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let db = fixture.connection();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES('ws-projection','Fixture');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,created_at,updated_at)
         VALUES('thread-projection','ws-projection','','agent','m','p','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         INSERT INTO compaction_context(owner,workspace_id,thread_id)
         VALUES('owner-projection','ws-projection','thread-projection');
         INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms)
         VALUES('operation-projection','owner-projection','fingerprint','running','{}',1000);
         INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status)
         VALUES('checkpoint-projection','operation-projection','owner-projection',0,'Saved summary','identity',
                '{\"model\":\"stored-model\", \"transport\":\"api\", \"instance\":\"stored-provider\", \"effort\":null}',7,1,'candidate')",
    ).await?;
    let body = compaction_checkpoint_body(db, "checkpoint-projection")
        .await?
        .unwrap();
    let metadata = compaction_checkpoint_metadata(db, "checkpoint-projection")
        .await?
        .unwrap();
    assert_eq!(body.summary, "Saved summary");
    assert_eq!(body.selection, metadata.selection);
    assert_eq!(metadata.selection.model, "stored-model");
    assert_eq!(metadata.selection.instance, "stored-provider");
    assert_eq!(metadata.projection_version, 7);
    assert_eq!(metadata.format_version, 1);
    assert!(
        compaction_checkpoint_metadata(db, "missing")
            .await?
            .is_none()
    );
    Ok(())
}
