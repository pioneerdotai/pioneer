use anyhow::{Context, Result};
use pioneer_crud_artifacts::artifact::{ArtifactRepository, NewArtifactBlobRecord};
use pioneer_entity::artifact_blob;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, Statement, TransactionTrait};
use std::time::Duration;

#[tokio::test]
async fn concurrent_blob_creators_reuse_the_committed_content_identity() -> Result<()> {
    for commit_first in [true, false] {
        with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared("INSERT INTO workspace (id, name) VALUES ('w', 'Workspace')")
                .await?;
            let now = chrono::Utc::now().fixed_offset();
            let metadata = r#"{ "marker": "проба\u0000" }"#;
            let input = NewArtifactBlobRecord {
                workspace_id: "w".into(),
                sha256: "a".repeat(64),
                size_bytes: 10,
                mime_type: Some("text/plain".into()),
                storage_backend: "local".into(),
                storage_key: "test.txt".into(),
                metadata: Default::default(),
            };
            let first = db.begin().await?;
            ArtifactRepository::new()
                .find_or_create_blob_prepared(
                    &first,
                    input.clone(),
                    "first".into(),
                    now,
                    10,
                    metadata.into(),
                )
                .await?;
            let second = db.begin().await?;
            let pid: i32 = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .context("missing second backend PID")?
                .try_get("", "pid")?;
            let writer = tokio::spawn(async move {
                let saved = ArtifactRepository::new()
                    .find_or_create_blob_prepared(
                        &second,
                        input,
                        "second".into(),
                        now,
                        10,
                        metadata.into(),
                    )
                    .await?;
                second.commit().await?;
                Ok::<_, anyhow::Error>(saved.id)
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: bool = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                            [pid.into()],
                        ))
                        .await?
                        .context("no lock observation")?
                        .try_get("", "blocked")?;
                    if blocked {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("second blob creator did not wait for the first")??;
            let expected = if commit_first {
                first.commit().await?;
                "first"
            } else {
                first.rollback().await?;
                "second"
            };
            assert_eq!(writer.await??, expected);
            let row = artifact_blob::Entity::find_by_id(expected)
                .one(db)
                .await?
                .context("missing winner")?;
            assert_eq!(row.metadata_json.as_bytes(), metadata.as_bytes());
            Ok(())
        })
        .await?;
    }
    Ok(())
}
