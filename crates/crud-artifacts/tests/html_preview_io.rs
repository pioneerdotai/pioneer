use anyhow::Result;
use pioneer_crud_artifacts::html_preview_io::{
    HtmlPreviewIoFamily, HtmlPreviewIoLocator, HtmlPreviewIoQueue, ack, find_work, request_at,
};
use pioneer_postgres_pools::{
    PostgresDatabase, PostgresReadClass, PostgresReadEvent, PostgresReadObserver,
    PostgresWriteClass, PostgresWriteEvent, PostgresWriteExecutor, PostgresWriteObserver,
};
use pioneer_postgres_test::TestDatabase;
use sea_orm::{ConnectionTrait, TransactionTrait};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Routes {
    reads: AtomicUsize,
    writes: AtomicUsize,
}

impl PostgresReadObserver for Routes {
    fn observe(&self, event: PostgresReadEvent) {
        if let PostgresReadEvent::OperationFinished { class, .. } = event {
            assert_eq!(class, PostgresReadClass::Maintenance);
            self.reads.fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl PostgresWriteObserver for Routes {
    fn observe(&self, event: PostgresWriteEvent) {
        if let PostgresWriteEvent::Acquired { class, .. } = event {
            assert_eq!(class, PostgresWriteClass::Maintenance);
            self.writes.fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl Routes {
    fn reset(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
    }
    fn counts(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
        )
    }
}
struct Fixture {
    db: PostgresDatabase,
    routes: Arc<Routes>,
}
impl Fixture {
    async fn sql(&self, sql: &str) {
        self.db.execute_unprepared(sql).await.unwrap();
    }
}
async fn fixture(database: &TestDatabase) -> Fixture {
    let routes = Arc::new(Routes::default());
    let writer = PostgresWriteExecutor::with_observer(
        database.connect_with_limit(1).await.unwrap(),
        routes.clone(),
    );
    let reader = database.connect_read_only().await.unwrap();
    let db = PostgresDatabase::from_executor_with_read_observer(reader, writer, routes.clone())
        .maintenance();
    db.execute_unprepared("INSERT INTO workspace(id,name) VALUES('ws','Fixture')")
        .await
        .unwrap();
    Fixture { db, routes }
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
async fn io_enqueue_rollback_and_idle_poll_never_claim_writer() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        let queue = HtmlPreviewIoQueue::new(&f.db);
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "rollback-copies", 0, "ws")
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert!(
            find_work("copies".to_owned(), io_queue_key("rollback-copies"), 0_i64)
                .one(&f.db)
                .await
                .unwrap()
                .is_none()
        );
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        f.routes.reset();
        assert!(
            queue
                .claim_html_preview_io_work(0, 60_000)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(f.routes.counts(), (1, 0));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_takeover_and_reinsert_reject_stale_worker() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        let queue = HtmlPreviewIoQueue::new(&f.db);
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "missing", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let old = queue
            .claim_html_preview_io_work(0, 10)
            .await
            .unwrap()
            .unwrap();
        let owner = queue
            .claim_html_preview_io_work(10, 10)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(owner.row.claim_token, old.row.claim_token);
        let tx = f.db.begin().await.unwrap();
        assert!(!ack(&tx, &old).await.unwrap());
        assert!(ack(&tx, &owner).await.unwrap());
        tx.commit().await.unwrap();
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "missing", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let next = queue
            .claim_html_preview_io_work(20, 10)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(owner.row.incarnation, next.row.incarnation);
        let tx = f.db.begin().await.unwrap();
        assert!(!ack(&tx, &owner).await.unwrap());
        tx.rollback().await.unwrap();
        assert!(
            find_work("copies".to_owned(), io_queue_key("missing"), 0_i64)
                .one(&f.db)
                .await
                .unwrap()
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_producer_during_claim_preserves_pass_and_ack_keeps_new_request() {
    pioneer_postgres_test::with_database(async |database| {
        let f = fixture(database).await;
        let queue = HtmlPreviewIoQueue::new(&f.db);
        f.sql("UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1")
            .await;
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "late", 0, "ws")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let old = queue
            .claim_html_preview_io_work(0, 10)
            .await
            .unwrap()
            .unwrap();
        let tx = f.db.begin().await.unwrap();
        request(&tx, HtmlPreviewIoFamily::Copies, "late", 0, "ws")
            .await
            .unwrap();
        let active = find_work("copies".to_owned(), io_queue_key("late"), 0_i64)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.claim_token, old.row.claim_token);
        assert_eq!(active.due_at, old.row.due_at);
        assert_eq!(active.pass_revision, old.row.pass_revision);
        assert_ne!(active.requested_revision, old.row.requested_revision);
        assert!(ack(&tx, &old).await.unwrap());
        tx.commit().await.unwrap();
        let next = queue
            .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.row.pass_revision, active.requested_revision);
        Ok(())
    })
    .await
    .unwrap();
}
