use anyhow::{Context, Result};
use pioneer_crud_artifacts::{
    artifact::{ArtifactBindingTargetRecord, IngestArtifactMetadataRecord, NewArtifactBlobRecord},
    ingest::PreparedArtifactIngest,
    mutations::bind_artifact,
};
use pioneer_entity::artifact_binding;
use pioneer_postgres_test::with_database;
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{
    ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait, Statement, TransactionTrait,
};
use std::time::Duration;

#[tokio::test]
async fn current_version_binding_waits_for_a_concurrent_version_change() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        writer.execute_unprepared("INSERT INTO workspace (id, name) VALUES ('ws', 'Workspace')").await?;
        let prepared = PreparedArtifactIngest::new(
            NewArtifactBlobRecord {
                workspace_id: "ws".into(),
                sha256: "a".repeat(64),
                size_bytes: 16,
                mime_type: None,
                storage_backend: "memory".into(),
                storage_key: "fixture-blob".into(),
                metadata: Default::default(),
            },
            IngestArtifactMetadataRecord {
                workspace_id: "ws".into(),
                primary_thread_id: None,
                display_name: "Artifact".into(),
                kind: ArtifactKind::File,
                mime_type: None,
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: None,
                metadata: Default::default(),
            },
            None, Default::default(), None, None,
        )?;
        let seed = writer.begin().await?;
        let saved = prepared.apply(&seed).await?;
        seed.commit().await?;

        let change = writer.begin().await?;
        change.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO artifact_version (id, workspace_id, artifact_id, version_number, blob_id)
             SELECT 'new-version',workspace_id,artifact_id,2,blob_id FROM artifact_version WHERE id=$1",
            [saved.version.id.clone().into()],
        )).await?;
        change.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE artifact SET current_version_id='new-version' WHERE id=$1",
            [saved.artifact.id.clone().into()],
        )).await?;

        let candidate_writer = fixture.connect_with_limit(1).await?;
        let pid: i32 = candidate_writer.query_one_raw(Statement::from_string(
            DbBackend::Postgres, "SELECT pg_backend_pid()",
        )).await?.context("binding backend PID")?.try_get_by_index(0)?;
        let scoped = SqliteDatabase::new(fixture.connect_read_only().await?, candidate_writer);
        let target = ArtifactBindingTargetRecord {
            thread_id: None, turn_id: None, message_id: None, turn_item_id: None,
            tool_call_id: None, task_id: None, task_run_id: None,
            binding_kind: ArtifactBindingKind::ManualAttach,
            direction: ArtifactBindingDirection::Input, role: None, item_index: None,
        };
        let artifact_id = saved.artifact.id.clone();
        let candidate = tokio::spawn({
            let scoped = scoped.clone();
            let target = target.clone();
            async move {
                bind_artifact(&scoped, "ws", &artifact_id, None, target, Default::default()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = writer.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT cardinality(pg_blocking_pids($1)) > 0",
                    [pid.into()],
                )).await?.context("binding lock observation")?.try_get_by_index(0)?;
                if blocked {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.context("binding did not wait for the current-version update")??;

        change.commit().await?;
        let failure = candidate.await?.expect_err("the outdated implicit binding must fail");
        assert!(failure.to_string().contains("current version changed"));
        assert_eq!(artifact_binding::Entity::find().count(writer).await?, 0);

        let current = bind_artifact(
            &scoped, "ws", &saved.artifact.id, None, target.clone(), Default::default(),
        ).await?;
        assert_eq!(current.version_id.as_deref(), Some("new-version"));
        let historical = bind_artifact(
            &scoped, "ws", &saved.artifact.id, Some(&saved.version.id), target, Default::default(),
        ).await?;
        assert_eq!(historical.version_id.as_deref(), Some(saved.version.id.as_str()));
        Ok(())
    }).await
}
