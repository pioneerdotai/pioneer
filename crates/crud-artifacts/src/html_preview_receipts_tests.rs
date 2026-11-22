use super::*;
use crate::html_preview_io::{HtmlPreviewIoLocator, HtmlPreviewIoQueue, find_work, request_at};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::TestDatabase;
use sea_orm::{DbBackend, Statement};

async fn fixture(database: &TestDatabase) -> Result<PostgresDatabase> {
    let db = PostgresDatabase::new(
        database.connect_read_only().await?,
        database.connect_with_limit(1).await?,
    )
    .maintenance();
    db.validate_reader().await?;
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
    )
    .await?;
    Ok(db)
}

fn io_queue_key(id: &str) -> String {
    HtmlPreviewIoLocator::new(id).key().to_owned()
}

async fn request(
    db: &impl ConnectionTrait,
    family: HtmlPreviewIoFamily,
    id: &str,
    ordinal: i64,
    workspace: &str,
) -> Result<()> {
    request_at(
        db,
        family,
        &HtmlPreviewIoLocator::new(id),
        ordinal,
        workspace,
        0,
    )
    .await
}

#[tokio::test]
async fn io_zero_row_copy_cas_keeps_claim_durable() {
    pioneer_postgres_test::with_database(async |database| {
        let db = fixture(database).await?;
        let notify = tokio::sync::Notify::new();
        let receipts = HtmlPreviewReceipts::new(&db, &notify);
        db.execute_unprepared(
            "UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1",
        )
        .await?;
        let tx = db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "missing-source", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let claim = HtmlPreviewIoQueue::new(&db)
            .claim_html_preview_io_work(0, 60_000)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !receipts
                .finish_html_preview_copy_cleanup_claimed(&claim, "stale", "prepared")
                .await
                .unwrap()
        );
        let row = find_work("copies".to_owned(), io_queue_key("missing-source"), 0_i64)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, "running");
        assert_eq!(row.claim_token, claim.row.claim_token);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_legacy_copy_callback_commits_source_and_ack_atomically() {
    pioneer_postgres_test::with_database(async |database| {
        let db = fixture(database).await?;
        let notify = tokio::sync::Notify::new();
        let receipts = HtmlPreviewReceipts::new(&db, &notify);
        let previous = r#"{"htmlTool":{"retention":"released","releasedCopies":["x"]}}"#;
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO artifact(id,workspace_id,display_name,kind,mime_type,status,created_by_kind,created_by_actor_id,created_at,updated_at,metadata_json)
             VALUES('callback','ws','preview','image','image/png','deleted','tool','html_preview',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,$1)",
            [previous.into()])).await?;
        let tx = db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "callback", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            receipts.finish_html_preview_copy_cleanup("callback", "stale", "{}")
                .await
                .is_err()
        );
        assert!(
            find_work("copies".to_owned(), io_queue_key("callback"), 0_i64)
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
        receipts.finish_html_preview_copy_cleanup("callback", previous, "{}")
            .await
            .unwrap();
        assert!(
            find_work("copies".to_owned(), io_queue_key("callback"), 0_i64)
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            artifact::Entity::find_by_id("callback")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .metadata_json,
            "{}"
        );
        Ok(())
    })
    .await
    .unwrap();
}
