use pioneer_crud_artifacts::html_preview_discovery::{
    html_preview_artifact_frontier, html_preview_blob_frontier, html_preview_gc_blob_page,
    html_preview_gc_blob_page_through, html_preview_gc_deadline,
    html_preview_released_copies_page_through,
};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

#[tokio::test]
async fn gc_deadline_crossing_behind_a_finished_page_is_retained_for_the_next_pass() {
    pioneer_postgres_test::with_database(async |database| {
        let writer = database.connection();
        writer.execute_unprepared("INSERT INTO workspace (id, name) VALUES ('ws', 'Workspace')").await?;
        let db = PostgresDatabase::new(database.connect_read_only().await?, writer.clone());
        writer.execute_unprepared(r#"INSERT INTO artifact_blob(id,workspace_id,sha256,size_bytes,storage_backend,storage_key,created_at,metadata_json)
            VALUES('deadline-blob','ws','deadline-sha',1,'local','deadline-key','2020-01-01 00:00:00 +00:00','{"htmlToolTemporary":1}')"#).await?;
        let created_ms = 1_577_836_800_000_i64;
        let grace_ms = 60_000_i64;
        let cutoff = created_ms - 1;
        let (after, candidates) = html_preview_gc_blob_page(&db, "", cutoff).await.unwrap();
        assert!(candidates.is_empty());
        assert!(
            html_preview_gc_blob_page(&db, &after, cutoff)
                .await
                .unwrap()
                .0
                .is_empty()
        );
        // The real clock is already later than this fixture's grace deadline.
        // Recomputing a "future only" cutoff at EOF would silently lose it.
        assert_eq!(
            html_preview_gc_deadline(&db, grace_ms, cutoff)
                .await
                .unwrap(),
            Some(1_577_836_860)
        );
        let (_, candidates) = html_preview_gc_blob_page(&db, "", created_ms)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].1.blob_id, "deadline-blob");
        assert_eq!(
            html_preview_gc_deadline(&db, grace_ms, created_ms)
                .await
                .unwrap(),
            None
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn finite_copy_and_gc_pages_revisit_older_candidates_despite_new_suffix_rows() {
    pioneer_postgres_test::with_database(async |database| {
        let writer = database.connection();
        writer.execute_unprepared("INSERT INTO workspace (id, name) VALUES ('ws', 'Workspace')").await?;
        let db = PostgresDatabase::new(database.connect_read_only().await?, writer.clone());
        for id in ["a", "m"] {
            io_copy_source(writer, id, "{}").await?;
        }
        let upper = html_preview_artifact_frontier(&db).await.unwrap();
        let (mut after, _) = html_preview_released_copies_page_through(&db, "", &upper)
            .await
            .unwrap();
        let metadata = r#"{"htmlTool":{"retention":"released","releasedCopies":["old.html"]}}"#;
        writer
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE artifact SET metadata_json=$1 WHERE id='a'",
                [metadata.into()],
            ))
            .await
            .unwrap();
        let mut finished = false;
        for suffix in 0..4 {
            io_copy_source(writer, &format!("z-{suffix}"), "{}").await?;
            let (next, _) = html_preview_released_copies_page_through(&db, &after, &upper)
                .await
                .unwrap();
            after = next;
            if after.is_empty() {
                finished = true;
                break;
            }
        }
        assert!(finished);
        let next_upper = html_preview_artifact_frontier(&db).await.unwrap();
        let (id, cleanup) = html_preview_released_copies_page_through(&db, "", &next_upper)
            .await
            .unwrap();
        assert_eq!(id, "a");
        assert_eq!(cleanup, Some(("ws".to_owned(), metadata.to_owned())));

        writer.execute_unprepared(r#"INSERT INTO artifact_blob(id,workspace_id,sha256,size_bytes,storage_backend,storage_key,created_at,metadata_json)
            VALUES('a','ws','a',1,'local','a','2020-01-01 00:00:00 +00:00','{}')"#).await?;
        let upper = html_preview_blob_frontier(&db).await.unwrap();
        let cutoff = 1_577_836_800_000_i64;
        let (after, candidates) = html_preview_gc_blob_page_through(&db, "", cutoff, &upper)
            .await
            .unwrap();
        assert!(candidates.is_empty());
        writer.execute_unprepared(r#"UPDATE artifact_blob SET metadata_json='{"htmlToolTemporary":1}' WHERE id='a'"#)
            .await?;
        writer.execute_unprepared("INSERT INTO artifact_blob(id,workspace_id,sha256,size_bytes,storage_backend,storage_key) VALUES('z','ws','z',1,'local','z')").await?;
        assert!(
            html_preview_gc_blob_page_through(&db, &after, cutoff, &upper)
                .await
                .unwrap()
                .0
                .is_empty()
        );
        let next_upper = html_preview_blob_frontier(&db).await.unwrap();
        let (_, candidates) = html_preview_gc_blob_page_through(&db, "", cutoff, &next_upper)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].1.blob_id, "a");
        Ok(())
    })
    .await
    .unwrap();
}

async fn io_copy_source(db: &DatabaseConnection, id: &str, metadata: &str) -> anyhow::Result<()> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO artifact (id, workspace_id, display_name, kind, mime_type, status,
            created_by_kind, created_by_actor_id, created_at, updated_at, metadata_json)
         VALUES ($1, 'ws', 'preview', 'image', 'image/png', 'deleted', 'tool', 'html_preview',
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $2)",
        [id.into(), metadata.into()],
    ))
    .await?;
    Ok(())
}
