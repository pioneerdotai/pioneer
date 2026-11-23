use anyhow::{Context, Result};
use pioneer_crud_artifacts::patch_history::{
    PatchSnapshotWrite, find_patch_snapshot, find_patch_snapshot_sizes,
    find_patch_snapshot_with_bounds, insert_patch_snapshot, patch_snapshot_metrics,
};
use pioneer_postgres_test::with_database;
use sea_orm::TransactionTrait;

#[tokio::test]
async fn snapshot_retains_exact_json_spelling_and_counts_stored_bytes() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        let text = r#"{ "separator": "←", "zero": "\u0000" }"#;
        let hash = vec![17; 32];
        let snapshot = PatchSnapshotWrite {
            domain_id: "test".into(),
            content_hash: hash.clone(),
            byte_len: 33,
            encoding: "zstd".into(),
            line_endings_json: text.into(),
            compressed_bytes: vec![0, 255, 84],
            raw_byte_len: 33,
            ref_count: 2,
        };
        let transaction = db.begin().await?;
        insert_patch_snapshot(&transaction, snapshot.clone()).await?;
        assert!(find_patch_snapshot(db, "test", &hash, 33).await?.is_none());
        transaction.commit().await?;
        let stored = find_patch_snapshot(db, "test", &hash, 33)
            .await?
            .context("missing snapshot after commit")?;
        assert_eq!(stored.line_endings_json.as_bytes(), text.as_bytes());
        assert_eq!(stored.compressed_bytes, snapshot.compressed_bytes);

        let sizes = find_patch_snapshot_sizes(db, "test", &hash, 33)
            .await?
            .context("missing snapshot sizes")?;
        assert_eq!(sizes.line_endings_bytes, i64::try_from(text.len())?);
        assert_eq!(sizes.compressed_bytes, 3);
        assert!(
            find_patch_snapshot_with_bounds(
                db,
                "test",
                &hash,
                33,
                3,
                i64::try_from(text.len())? - 1
            )
            .await?
            .is_none()
        );
        let metrics = patch_snapshot_metrics(db).await?;
        assert_eq!(metrics.blobs, 1);
        assert_eq!(metrics.logical_bytes, 33);
        assert_eq!(metrics.physical_bytes, 3);
        assert_eq!(metrics.references, 2);
        assert_eq!(metrics.referenced_logical_bytes, 66);
        Ok(())
    })
    .await
}
