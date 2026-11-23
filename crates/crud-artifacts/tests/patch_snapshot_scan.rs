use anyhow::Result;
use pioneer_crud_artifacts::patch_history::{
    PatchSnapshotKeyCursor, begin_patch_snapshot_scan, expected_patch_snapshot_reference_exists,
    finish_patch_snapshot_scan, increment_expected_patch_snapshot_reference,
    list_expected_patch_snapshot_references, touch_patch_snapshot_scan,
};
use pioneer_postgres_test::with_database;
use sea_orm::TransactionTrait;

#[tokio::test]
async fn patch_reference_scans_keep_isolated_counts_across_connections() -> Result<()> {
    with_database(async |database| {
        let reader = database.connection();
        let writer = database.connect().await?;
        let first = begin_patch_snapshot_scan(reader).await?;
        let second = begin_patch_snapshot_scan(&writer).await?;
        let hash = vec![31; 32];

        let tx = writer.begin().await?;
        touch_patch_snapshot_scan(&tx, &first).await?;
        increment_expected_patch_snapshot_reference(&tx, &first, "thread", &hash, 5).await?;
        assert!(
            !expected_patch_snapshot_reference_exists(reader, &first, "thread", &hash, 5).await?
        );
        tx.commit().await?;

        increment_expected_patch_snapshot_reference(reader, &first, "thread", &hash, 5).await?;
        increment_expected_patch_snapshot_reference(reader, &first, "thread", &hash, 6).await?;
        increment_expected_patch_snapshot_reference(&writer, &second, "thread", &hash, 5).await?;
        let page = list_expected_patch_snapshot_references(reader, &first, None, 1).await?;
        assert_eq!((page[0].byte_len, page[0].ref_count), (5, 2));
        let cursor = PatchSnapshotKeyCursor {
            domain_id: page[0].domain_id.clone(),
            content_hash: page[0].content_hash.clone(),
            byte_len: page[0].byte_len,
        };
        let next =
            list_expected_patch_snapshot_references(&writer, &first, Some(&cursor), 1).await?;
        assert_eq!((next[0].byte_len, next[0].ref_count), (6, 1));
        finish_patch_snapshot_scan(reader, &first).await?;
        assert!(
            list_expected_patch_snapshot_references(&writer, &first, None, 2)
                .await?
                .is_empty()
        );
        let kept = list_expected_patch_snapshot_references(reader, &second, None, 2).await?;
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].ref_count, 1);
        finish_patch_snapshot_scan(&writer, &second).await?;
        writer.close().await?;
        Ok(())
    })
    .await
}
