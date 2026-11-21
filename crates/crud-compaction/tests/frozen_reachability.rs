use anyhow::Result;
use pioneer_crud_compaction::frozen_reachability::Reachability;
use pioneer_postgres_test::with_database;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::ConnectionTrait;

#[tokio::test]
async fn reverse_traversal_distinguishes_unpublished_and_published_descendants() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        writer
            .execute_unprepared(
                "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
                 INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                 VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
                 INSERT INTO compaction_context (owner, workspace_id, thread_id)
                 VALUES ('owner', 'workspace', 'thread');
                 INSERT INTO compaction_operation (id, owner, fingerprint, status, snapshot, deadline_ms)
                 VALUES ('operation', 'owner', 'fingerprint', 'running', '{}', 1000);
                 INSERT INTO compaction_checkpoint
                    (id, operation_id, owner, portion, summary, identity_sha256, selection,
                     projection_version, format_version, status, previous)
                 VALUES
                    ('parent', 'operation', 'owner', 0, 'parent', 'parent-hash', '{}',
                     1, 1, 'candidate', NULL),
                    ('child', 'operation', 'owner', 1, 'child', 'child-hash', '{}',
                     1, 1, 'candidate', 'parent');",
            )
            .await?;
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, writer.clone());

        for status in ["candidate", "applied"] {
            if status == "applied" {
                writer
                    .execute_unprepared("UPDATE compaction_checkpoint SET status='applied' WHERE id='child'")
                    .await?;
            }
            let mut scan = Reachability::new("parent", "workspace");
            for _ in 0..32 {
                if scan.done() {
                    break;
                }
                scan.step(&db).await?;
            }
            assert!(scan.done(), "two-node reverse scan exceeded its quantum budget");
            assert_eq!(scan.reachable(), status == "applied");
        }

        let mut foreign = Reachability::new("parent", "other-workspace");
        assert!(
            foreign
                .step(&db)
                .await
                .expect_err("foreign checkpoint admitted")
                .to_string()
                .contains("scope corrupt")
        );
        Ok(())
    })
    .await
}
