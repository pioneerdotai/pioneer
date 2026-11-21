use anyhow::Result;
use pioneer_crud_execution::native_event_cleanup::run_quantum;
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
    SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[derive(Default)]
struct Observer {
    reads: AtomicU64,
    writes: AtomicU64,
    queued: AtomicU64,
}
impl SqliteReadObserver for Observer {
    fn observe(&self, event: SqliteReadEvent) {
        if matches!(
            event,
            SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Maintenance,
                ..
            }
        ) {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl SqliteWriteObserver for Observer {
    fn observe(&self, event: SqliteWriteEvent) {
        match event {
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Maintenance,
                ..
            } => {
                self.writes.fetch_add(1, Ordering::SeqCst);
            }
            SqliteWriteEvent::Enqueued {
                class: SqliteWriteClass::Maintenance,
                ..
            } => {
                self.queued.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn production_api_empty_queue_uses_one_maintenance_read_without_writer() -> Result<()> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let observer = Arc::new(Observer::default());
    let reader = fixture.connect_read_only().await?;
    let writer = fixture.connect_with_limit(1).await?;
    let db = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    db.maintenance().validate_reader().await?;
    assert!(run_quantum(&db).await.is_err());
    let reads = observer.reads.load(Ordering::SeqCst);
    let writes = observer.writes.load(Ordering::SeqCst);
    let m = run_quantum(&db.maintenance()).await?;
    assert_eq!(m.prepare_reads, 1);
    assert_eq!(m.jobs_examined, 0);
    assert_eq!(observer.reads.load(Ordering::SeqCst) - reads, 1);
    assert_eq!(observer.writes.load(Ordering::SeqCst), writes);
    Ok(())
}
