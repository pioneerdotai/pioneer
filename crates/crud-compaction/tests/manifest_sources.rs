use anyhow::Result;
use pioneer_crud_compaction::manifest::sources_current;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn current_sources_reads_the_revision_written_by_the_context_trigger() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status) VALUES ('turn', 'thread', 'completed');
            INSERT INTO turn_llm_context (id, turn_id, sequence, source, payload)
                VALUES ('source', 'turn', 1, 'message_v2', '{}');
            INSERT INTO compaction_context (owner, workspace_id, thread_id)
                VALUES ('owner', 'w', 'thread');
            INSERT INTO compaction_operation (id, owner, fingerprint, status, snapshot, deadline_ms)
                VALUES ('operation', 'owner', 'fingerprint', 'running', '{"source_epochs":{"thread":1}}', 1000);
            INSERT INTO compaction_manifest
                (operation_id, ordinal, unit_ordinal, reference_only, source_thread,
                 source_scope, source_id, source_version)
                SELECT 'operation', 0, 0, 0, 'thread', 'context:turn', 'source', 'revision:'||revision
                FROM compaction_source_revision WHERE source_id='source';
            "#,
        )
        .await?;

        assert!(sources_current(db, "operation").await?);
        let concurrent = database.connect().await?;
        let tx = concurrent.begin().await?;
        assert_eq!(
            tx.execute_unprepared("UPDATE turn_llm_context SET payload = '{\"revised\":true}' WHERE id = 'source'")
                .await?
                .rows_affected(), 1,
        );
        assert!(!sources_current(&tx, "operation").await?);
        assert!(sources_current(db, "operation").await?);
        tx.rollback().await?;
        assert!(sources_current(db, "operation").await?);

        db.execute_unprepared("DELETE FROM turn_llm_context WHERE id = 'source'")
            .await?;
        assert!(!sources_current(db, "operation").await?);
        concurrent.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn inherited_checkpoint_checks_coverage_and_previous_edges() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('local', 'w', '', 'chat', 'model', 'provider', 'active'),
                       ('foreign', 'w', '', 'chat', 'model', 'provider', 'active');
            INSERT INTO compaction_context (owner, workspace_id, thread_id)
                VALUES ('local-owner', 'w', 'local'), ('foreign-owner', 'w', 'foreign');
            INSERT INTO compaction_operation (id, owner, fingerprint, status, snapshot, deadline_ms)
                VALUES ('consumer', 'local-owner', 'local-fingerprint', 'running',
                        '{"plan":{"coverage_domain":"working_context"}}', 1000),
                       ('producer', 'foreign-owner', 'foreign-fingerprint', 'completed', '{}', 1000);
            INSERT INTO compaction_checkpoint
                (id, operation_id, owner, previous, portion, summary, identity_sha256, selection,
                 projection_version, format_version, status)
                VALUES ('base', 'producer', 'foreign-owner', NULL, 0, 'base', 'base-hash', '{}', 1, 1, 'retained'),
                       ('head', 'producer', 'foreign-owner', 'base', 1, 'head', 'head-hash', '{}', 1, 1, 'applied');
            INSERT INTO compaction_manifest
                (operation_id, ordinal, unit_ordinal, reference_only, source_thread,
                 source_scope, source_id, source_version)
                VALUES ('producer', 0, 0, 0, 'local', 'context:historical-turn', 'base-source', 'revision:3'),
                       ('producer', 1, 1, 0, 'local', 'context:historical-turn', 'head-source', 'revision:7'),
                       ('consumer', 0, 0, 0, 'foreign', 'checkpoint:foreign-owner', 'head', 'head-hash');
            INSERT INTO compaction_coverage (checkpoint_id, source_scope, source_id, source_version)
                VALUES ('base', 'context:historical-turn', 'base-source', 'revision:3'),
                       ('head', 'context:historical-turn', 'head-source', 'revision:7');
            "#,
        )
        .await?;

        assert!(sources_current(db, "consumer").await?);
        db.execute_unprepared(
            "UPDATE compaction_manifest SET source_thread='foreign' WHERE operation_id='producer' AND ordinal=0",
        )
        .await?;
        assert!(!sources_current(db, "consumer").await?);
        Ok(())
    })
    .await
}
