use pioneer_artifacts::{
    ArtifactService, HtmlPreviewMaintenanceOutcome, HtmlPreviewMaintenanceProgress,
};
use pioneer_crud::{CrudStore, HtmlPreviewIoClaim, HtmlPreviewIoOutcome, HtmlPreviewIoSource};
use pioneer_sqlite::background::{Plan, Ready};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const RETRY: Duration = Duration::from_secs(60);
const CLAIM_MS: i64 = 300_000;

/// The existing owner joins this worker on shutdown. Queue claims survive
/// cancellation; traversal state applies only to the retained history sweeps.
#[derive(Default)]
pub(crate) struct Progress {
    previews: HtmlPreviewMaintenanceProgress,
    witness_after: (String, i64),
    blocked_after: (String, String, i64),
    finished: [bool; 2],
    io_seeded: bool,
    blocked_finished: bool,
    #[cfg(test)]
    quantum_observation: Option<pioneer_sqlite::background::Observation>,
    #[cfg(test)]
    quantum_rescan: bool,
}

async fn settle(
    store: &CrudStore,
    claim: &HtmlPreviewIoClaim,
    outcome: HtmlPreviewIoOutcome,
) -> anyhow::Result<()> {
    // A lost fence is explicitly a conflict, never an acknowledgement.
    anyhow::ensure!(
        store
            .defer_html_preview_io_work(claim, outcome, chrono::Utc::now().timestamp_millis())
            .await?,
        "HTML IO claim fence lost"
    );
    Ok(())
}

async fn io_quantum(
    store: &CrudStore,
    service: &ArtifactService,
) -> anyhow::Result<(bool, Option<i64>)> {
    let (claim, deadline) = store
        .claim_html_preview_io_work_with_deadline(chrono::Utc::now().timestamp_millis(), CLAIM_MS)
        .await?;
    let Some(claim) = claim else {
        return Ok((false, deadline));
    };
    match claim.row.family.as_str() {
        "seed_witness" | "seed_copies" => {
            anyhow::ensure!(
                store.html_preview_io_seed_quantum(&claim).await?,
                "HTML IO seed fence lost"
            );
        }
        "witness" => match store.load_html_preview_io_witness(&claim).await? {
            HtmlPreviewIoSource::Ready(witness) => {
                match crate::compaction::frozen::prepare_html_preview_archive_witness_hash(
                    store, &witness,
                )
                .await
                {
                    Ok(hash) => {
                        if !store.record_html_preview_archive_witness_claimed(&claim, &witness, &hash).await? {
                            settle(store, &claim, HtmlPreviewIoOutcome::Retry).await?;
                        }
                    }
                    // Restoration and storage failure do not prove permanent
                    // invalidity. The point loader quarantines proven invalid data.
                    Err(error) if error.downcast_ref::<crate::compaction::frozen::InvalidHtmlPreviewArchiveWitness>().is_some() => {
                        settle(store, &claim, HtmlPreviewIoOutcome::Quarantine).await?;
                    }
                    Err(_) => settle(store, &claim, HtmlPreviewIoOutcome::Retry).await?,
                }
            }
            HtmlPreviewIoSource::Retry => {
                settle(store, &claim, HtmlPreviewIoOutcome::Retry).await?
            }
            HtmlPreviewIoSource::Obsolete => {
                settle(store, &claim, HtmlPreviewIoOutcome::Done).await?
            }
            HtmlPreviewIoSource::Quarantine => {
                settle(store, &claim, HtmlPreviewIoOutcome::Quarantine).await?
            }
        },
        "copies" => match store.load_html_preview_io_copies(&claim).await? {
            HtmlPreviewIoSource::Ready((workspace, previous)) => {
                match service
                    .prepare_html_preview_copy_cleanup(&workspace, &previous)
                    .await
                {
                    Ok(Some(prepared)) => {
                        if !store
                            .finish_html_preview_copy_cleanup_claimed(&claim, &previous, &prepared)
                            .await?
                        {
                            settle(store, &claim, HtmlPreviewIoOutcome::Retry).await?;
                        }
                    }
                    Ok(None) => settle(store, &claim, HtmlPreviewIoOutcome::Retry).await?,
                    Err(pioneer_artifacts::ArtifactError::ContentInvariant { .. }) => {
                        settle(store, &claim, HtmlPreviewIoOutcome::Quarantine).await?
                    }
                    Err(_) => settle(store, &claim, HtmlPreviewIoOutcome::Retry).await?,
                }
            }
            HtmlPreviewIoSource::Retry => {
                settle(store, &claim, HtmlPreviewIoOutcome::Retry).await?
            }
            HtmlPreviewIoSource::Obsolete => {
                settle(store, &claim, HtmlPreviewIoOutcome::Done).await?
            }
            HtmlPreviewIoSource::Quarantine => {
                settle(store, &claim, HtmlPreviewIoOutcome::Quarantine).await?
            }
        },
        _ => settle(store, &claim, HtmlPreviewIoOutcome::Quarantine).await?,
    }
    Ok((true, None))
}

#[cfg(test)]
pub(crate) async fn quantum(
    store: &CrudStore,
    service: &ArtifactService,
    progress: &mut Progress,
) -> anyhow::Result<bool> {
    let store = store.with_maintenance_access();
    // Foreground fixtures drive the same finite passes without an Owner. Keep
    // committed dependency changes across their handoff, as run_phase does.
    let work = store.background_work("storage.html.retention");
    let observed = work.observe();
    if progress
        .quantum_observation
        .is_some_and(|old| old != observed)
    {
        progress.quantum_rescan = true;
    }
    // Bounded queue recovery runs even after a lost local wake or restart.
    if !progress.blocked_finished {
        match store
            .recover_html_preview_io_blocked(&progress.blocked_after)
            .await?
        {
            Some(next) => progress.blocked_after = next,
            None => {
                progress.blocked_after = Default::default();
                progress.blocked_finished = true;
            }
        }
    }
    let io_result = io_quantum(&store, service).await;
    if !progress.finished[0] && !progress.io_seeded {
        progress.io_seeded = store.html_preview_io_seed_complete().await?;
    }
    let mut witness_error = None;
    if progress.io_seeded {
        progress.finished[0] = true;
    } else if !progress.finished[0] {
        let (next, witness) = store
            .html_preview_archive_witness_page(&progress.witness_after)
            .await?;
        if let Some(next) = next {
            progress.witness_after = next;
        } else {
            progress.witness_after = (String::new(), 0);
            progress.finished[0] = true;
        }
        if let Some(witness) = witness {
            if let Err(error) =
                crate::compaction::frozen::prepare_html_preview_archive_witness(&store, &witness)
                    .await
            {
                witness_error = Some(error);
            }
        }
    }
    let mut deferred = false;
    if !progress.finished[1] {
        match service
            .continue_html_preview_maintenance(&mut progress.previews)
            .await?
        {
            HtmlPreviewMaintenanceOutcome::More => {}
            HtmlPreviewMaintenanceOutcome::Idle => progress.finished[1] = true,
            HtmlPreviewMaintenanceOutcome::Deferred => deferred = true,
        }
    }
    // One poison IO row cannot prevent progress of retirement and BlobGC.
    let (io_more, _) = io_result?;
    if let Some(error) = witness_error {
        return Err(error);
    }
    let more = io_more
        || !progress.blocked_finished
        || !progress.finished[0]
        || (!deferred && !progress.finished[1]);
    let completed = work.observe();
    progress.quantum_rescan |= completed != observed;
    progress.quantum_observation = Some(completed);
    if !more && std::mem::take(&mut progress.quantum_rescan) {
        progress.finished = [false; 2];
        progress.blocked_finished = false;
        progress.previews.rediscover();
        return Ok(true);
    }
    Ok(more)
}

#[derive(Clone, Copy)]
enum Phase {
    Io,
    History,
    Retention,
}
impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Io => "storage.html.io",
            Self::History => "storage.html.history",
            Self::Retention => "storage.html.retention",
        }
    }
}

async fn history_quantum(store: &CrudStore, progress: &mut Progress) -> anyhow::Result<bool> {
    // Raw16 keys, state filtering inside the page. EOF remains closed until
    // a relevant source/dependency generation, never an IO-only hint or ACK.
    if !progress.blocked_finished {
        match store
            .recover_html_preview_io_blocked(&progress.blocked_after)
            .await?
        {
            Some(next) => progress.blocked_after = next,
            None => {
                progress.blocked_after = Default::default();
                progress.blocked_finished = true;
            }
        }
    }
    if !progress.finished[0] && !progress.io_seeded {
        progress.io_seeded = store.html_preview_io_seed_complete().await?;
    }
    if progress.io_seeded {
        progress.finished[0] = true;
    } else if !progress.finished[0] {
        let (next, witness) = store
            .html_preview_archive_witness_page(&progress.witness_after)
            .await?;
        if let Some(next) = next {
            progress.witness_after = next;
        } else {
            progress.witness_after = Default::default();
            progress.finished[0] = true;
        }
        if let Some(witness) = witness {
            crate::compaction::frozen::prepare_html_preview_archive_witness(store, &witness)
                .await?;
        }
    }
    Ok(!progress.blocked_finished || !progress.finished[0])
}

fn deadline_plan(at: Option<i64>) -> anyhow::Result<Plan> {
    match at {
        None => Ok(Plan::Dormant),
        Some(at) => Ok(Plan::WallDeadline(
            chrono::DateTime::from_timestamp_millis(at)
                .ok_or_else(|| anyhow::anyhow!("invalid HTML deadline"))?
                .into(),
        )),
    }
}

#[cfg(test)]
#[derive(Default)]
struct SettledBarrier {
    armed: std::sync::atomic::AtomicU8,
    entered: [tokio::sync::Notify; 3],
    release: [tokio::sync::Notify; 3],
    retrying: tokio::sync::Notify,
}

async fn run_phase(
    store: Arc<CrudStore>,
    service: Arc<ArtifactService>,
    phase: Phase,
    cancellation: CancellationToken,
) {
    let store = store.with_maintenance_access();
    let work = store.background_work(phase.name());
    let Some(mut owner) = work.acquire() else {
        return;
    };
    let mut progress = Progress::default();
    let mut previous = None;
    let mut rescan = false;
    loop {
        let (ready, activity_release) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            ready = owner.ready() => (ready, false),
            _ = service.html_preview_release_notified(), if matches!(phase, Phase::Retention) => (Ready::Work, true),
        };
        if ready == Ready::Closed {
            return;
        }
        if ready == Ready::SettlementRequired {
            let settle_observation = owner.observe();
            progress.finished[0] = false;
            progress.blocked_finished = false;
            progress.previews.rediscover();
            rescan = !matches!(phase, Phase::Io);
            if store
                .database_connection()
                .settle_background_change(&owner)
                .await
                .is_err()
            {
                owner.retry_after(settle_observation, RETRY);
            }
            continue;
        }
        let observed = owner.observe();
        if previous.is_some_and(|old| old != observed) {
            progress.finished[0] = false;
            progress.blocked_finished = false;
            progress.previews.rediscover();
            rescan = !matches!(phase, Phase::Io); // preserve old-key input through active cursors
        }
        previous = Some(observed);
        if activity_release {
            progress.previews.dependency_released();
        }
        if progress
            .previews
            .next_deadline()
            .is_some_and(|at| at <= chrono::Utc::now().timestamp_millis())
        {
            progress.previews.grace_elapsed();
        }
        let result: anyhow::Result<(bool, Option<i64>)> = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = owner.scope(crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::HtmlPreviewMaintenance,
                pioneer_observability::DatabaseOperationSite::HtmlPreviewQuantum,
                async {
                    match phase {
                        Phase::Io => io_quantum(&store, &service).await,
                        Phase::History => Ok((history_quantum(&store, &mut progress).await?, None)),
                        Phase::Retention => {
                            let more = matches!(service.continue_html_preview_maintenance(&mut progress.previews).await?, HtmlPreviewMaintenanceOutcome::More);
                            Ok((more, progress.previews.next_deadline()))
                        }
                    }
                },
            )) => result,
        };
        match result {
            Ok((more, deadline)) if !owner.requires_settlement() => {
                let more = if !more && std::mem::take(&mut rescan) {
                    progress.finished[0] = false;
                    progress.blocked_finished = false;
                    progress.previews.rediscover();
                    true
                } else {
                    more
                };
                let plan = if more {
                    Ok(Plan::Runnable)
                } else {
                    deadline_plan(deadline)
                };
                match plan {
                    Ok(plan) => {
                        let settled = owner.publish(observed, plan);
                        #[cfg(test)]
                        if settled && !matches!(plan, Plan::Runnable) {
                            let index = match phase {
                                Phase::Io => 0,
                                Phase::History => 1,
                                Phase::Retention => 2,
                            };
                            let barrier = store
                                .database_connection()
                                .shared_resource::<SettledBarrier>();
                            let mask = 1 << index;
                            if barrier
                                .armed
                                .fetch_and(!mask, std::sync::atomic::Ordering::AcqRel)
                                & mask
                                != 0
                            {
                                barrier.entered[index].notify_one();
                                tokio::select! { _ = cancellation.cancelled() => return, _ = barrier.release[index].notified() => {} }
                            }
                        }
                        #[cfg(not(test))]
                        let _ = settled;
                    }
                    Err(_) => {
                        owner.retry_after(observed, RETRY);
                    }
                }
                if more {
                    tokio::select! { _ = cancellation.cancelled() => return, _ = tokio::time::sleep(Duration::from_millis(10)) => {} }
                }
            }
            _ => {
                tracing::warn!("HTML preview maintenance deferred; durable work retained");
                owner.retry_after(observed, RETRY);
                #[cfg(test)]
                store
                    .database_connection()
                    .shared_resource::<SettledBarrier>()
                    .retrying
                    .notify_one();
            }
        }
    }
}

pub(super) async fn run(
    store: Arc<CrudStore>,
    service: Arc<ArtifactService>,
    cancellation: CancellationToken,
) {
    // Independent quotas/error paths; all share the common runtime mechanism.
    tokio::join!(
        run_phase(
            store.clone(),
            service.clone(),
            Phase::Io,
            cancellation.clone()
        ),
        run_phase(
            store.clone(),
            service.clone(),
            Phase::History,
            cancellation.clone()
        ),
        run_phase(store, service, Phase::Retention, cancellation),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use migration::{Migrator, MigratorTrait};
    use pioneer_artifacts::ArtifactBlobStore;
    use pioneer_entity::html_preview_io_work as work;
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
        SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
    };
    use sea_orm::{ConnectionTrait, Database, EntityTrait};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Routes {
        reads: Mutex<Vec<SqliteReadEvent>>,
        writes: Mutex<Vec<SqliteWriteEvent>>,
    }
    impl SqliteReadObserver for Routes {
        fn observe(&self, event: SqliteReadEvent) {
            self.reads.lock().unwrap().push(event);
        }
    }
    impl SqliteWriteObserver for Routes {
        fn observe(&self, event: SqliteWriteEvent) {
            self.writes.lock().unwrap().push(event);
        }
    }
    impl Routes {
        fn reset(&self) {
            self.reads.lock().unwrap().clear();
            self.writes.lock().unwrap().clear();
        }
        fn count(&self) -> usize {
            self.reads
                .lock()
                .unwrap()
                .iter()
                .filter(|e| matches!(e, SqliteReadEvent::OperationFinished { .. }))
                .count()
        }
        fn assert_idle_io_only(&self) {
            let reads = self.reads.lock().unwrap();
            assert_eq!(
                reads
                    .iter()
                    .filter(|e| matches!(e, SqliteReadEvent::OperationFinished { .. }))
                    .count(),
                1
            );
            // Include starts/releases, so an unfinished second DB operation
            // cannot be hidden by counting only its predecessor's completion.
            assert_eq!(
                reads
                    .iter()
                    .filter(|e| matches!(e, SqliteReadEvent::AdmissionEnqueued { .. }))
                    .count(),
                1
            );
            assert_eq!(
                reads
                    .iter()
                    .filter(|e| matches!(e, SqliteReadEvent::AdmissionReleased { .. }))
                    .count(),
                1
            );
            assert!(reads.iter().all(|e| !matches!(
                e,
                SqliteReadEvent::OperationFinished {
                    class: SqliteReadClass::Interactive,
                    ..
                }
            )));
            assert!(
                self.writes.lock().unwrap().is_empty(),
                "empty IO continuation must not even enqueue a writer"
            );
        }
    }

    // Physical SQL completes on real IO while virtual scheduling is paused.
    async fn drive_until<F: std::future::Future<Output = ()>>(
        mut worker: std::pin::Pin<&mut impl std::future::Future<Output = ()>>,
        signal: F,
    ) {
        tokio::pin!(signal);
        let limit = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                std::time::Instant::now() < limit,
                "HTML owner did not settle"
            );
            assert!(worker.as_mut().now_or_never().is_none());
            if signal.as_mut().now_or_never().is_some() {
                break;
            }
            tokio::task::yield_now().await;
            // Yield between bounded portions without auto-advancing real SQL.
            tokio::time::advance(Duration::from_millis(10)).await;
        }
    }

    async fn html_fixture() -> (tempfile::TempDir, Arc<CrudStore>, Arc<Routes>) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("html-runner.sqlite");
        pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
        options.max_connections(1).sqlx_logging(false);
        let writer = Database::connect(options).await.unwrap();
        writer
            .execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        Migrator::up(&writer, None).await.unwrap();
        writer
            .execute_unprepared("PRAGMA journal_mode=WAL")
            .await
            .unwrap();
        writer
            .execute_unprepared(
                "UPDATE html_preview_io_work SET state='done',due_at=NULL,seed_version=1",
            )
            .await
            .unwrap();
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
        options
            .max_connections(2)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|o| o.pragma("query_only", "ON"));
        let reader = Database::connect(options).await.unwrap();
        let routes = Arc::new(Routes::default());
        let store = Arc::new(
            CrudStore::new(SqliteDatabase::from_executor_with_read_observer(
                reader,
                SqliteWriteExecutor::with_observer(writer, routes.clone()),
                routes.clone(),
            ))
            .with_maintenance_access(),
        );
        assert!(
            store
                .database_connection()
                .reader_query_only_enabled()
                .await
                .unwrap()
        );
        (directory, store, routes)
    }

    struct FailBlobOnce {
        local: pioneer_artifacts::LocalArtifactBlobStore,
        key: Mutex<Option<String>>,
    }
    #[async_trait::async_trait]
    impl pioneer_artifacts::ArtifactBlobStore for FailBlobOnce {
        fn local_artifact_root(&self) -> Option<std::path::PathBuf> {
            self.local.local_artifact_root()
        }
        fn local_blob_path(
            &self,
            w: &str,
            k: &str,
        ) -> pioneer_artifacts::ArtifactResult<Option<std::path::PathBuf>> {
            self.local.local_blob_path(w, k)
        }
        async fn put_bytes(
            &self,
            w: &str,
            i: pioneer_artifacts::ArtifactBlobInput,
        ) -> pioneer_artifacts::ArtifactResult<pioneer_artifacts::StoredArtifactBlob> {
            self.local.put_bytes(w, i).await
        }
        async fn open_read(
            &self,
            w: &str,
            k: &str,
        ) -> pioneer_artifacts::ArtifactResult<pioneer_artifacts::ArtifactReadHandle> {
            self.local.open_read(w, k).await
        }
        async fn materialize_readable_copy(
            &self,
            w: &str,
            k: &str,
            n: &str,
        ) -> pioneer_artifacts::ArtifactResult<std::path::PathBuf> {
            self.local.materialize_readable_copy(w, k, n).await
        }
        async fn delete_unreferenced(
            &self,
            w: &str,
            k: &str,
        ) -> pioneer_artifacts::ArtifactResult<()> {
            self.local.delete_unreferenced(w, k).await
        }
        async fn delete_unreferenced_guarded(
            &self,
            w: &str,
            k: &str,
            guard: pioneer_artifacts::ArtifactBlobMutationGuard,
        ) -> pioneer_artifacts::ArtifactResult<()> {
            let fail = {
                let mut key = self.key.lock().unwrap();
                if key.as_deref() == Some(k) {
                    key.take();
                    true
                } else {
                    false
                }
            };
            if fail {
                return Err(pioneer_artifacts::ArtifactError::Io {
                    message: "one-shot unlink failure".into(),
                    source: std::io::ErrorKind::PermissionDenied.into(),
                });
            }
            self.local.delete_unreferenced_guarded(w, k, guard).await
        }
    }

    #[tokio::test]
    async fn retention_error_timer_revisits_failed_blob_after_healthy_tail_then_stays_dormant() {
        use pioneer_artifacts::ArtifactBlobInput;
        let (directory, store, routes) = html_fixture().await;
        let blobs = Arc::new(FailBlobOnce {
            local: pioneer_artifacts::LocalArtifactBlobStore::new(directory.path()),
            key: Mutex::new(None),
        });
        let mut rows = Vec::new();
        for index in 0..17 {
            let physical = blobs
                .put_bytes(
                    "ws",
                    ArtifactBlobInput::Bytes(format!("temporary preview {index}").into_bytes()),
                )
                .await
                .unwrap();
            let row = store
                .find_or_create_artifact_blob(pioneer_crud::NewArtifactBlobRecord {
                    workspace_id: "ws".into(),
                    sha256: physical.sha256,
                    size_bytes: physical.size_bytes,
                    mime_type: None,
                    storage_backend: physical.storage_backend,
                    storage_key: physical.storage_key,
                    metadata: [("htmlToolTemporary".into(), serde_json::json!(true))]
                        .into_iter()
                        .collect(),
                })
                .await
                .unwrap();
            rows.push(row);
        }
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        *blobs.key.lock().unwrap() = Some(rows[0].storage_key.clone());
        let service = Arc::new(ArtifactService::new_with_policies(
            store.clone(),
            blobs.clone(),
            pioneer_artifacts::ArtifactQuotaPolicy::default(),
            pioneer_artifacts::ArtifactGcPolicy {
                grace_secs: 0,
                ..Default::default()
            },
        ));
        let cancellation = CancellationToken::new();
        let barrier = store
            .database_connection()
            .shared_resource::<SettledBarrier>();
        tokio::time::pause();
        let mut worker = Box::pin(run_phase(
            store.clone(),
            service,
            Phase::Retention,
            cancellation.clone(),
        ));
        drive_until(worker.as_mut(), barrier.retrying.notified()).await;
        assert!(worker.as_mut().now_or_never().is_none());
        // Failure happened before any mutation for this row; no Unknown fence
        // or later producer may accidentally stand in for the error timer.
        let db = store.database_connection();
        assert!(
            pioneer_entity::artifact_blob::Entity::find_by_id(&rows[0].id)
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            pioneer_entity::artifact_blob::Entity::find_by_id(&rows[1].id)
                .one(&db)
                .await
                .unwrap()
                .is_none(),
            "healthy rows in the failed quantum still progress"
        );
        assert!(
            pioneer_entity::artifact_blob::Entity::find_by_id(&rows[16].id)
                .one(&db)
                .await
                .unwrap()
                .is_some(),
            "raw16 leaves a healthy tail"
        );
        barrier.armed.store(4, std::sync::atomic::Ordering::Release);
        tokio::time::advance(RETRY).await;
        drive_until(worker.as_mut(), barrier.entered[2].notified()).await;
        for row in &rows {
            assert!(
                pioneer_entity::artifact_blob::Entity::find_by_id(&row.id)
                    .one(&db)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                !blobs
                    .local
                    .local_blob_path("ws", &row.storage_key)
                    .unwrap()
                    .unwrap()
                    .exists()
            );
        }
        barrier.release[2].notify_one();
        assert!(worker.as_mut().now_or_never().is_none());
        routes.reset();
        tokio::time::advance(Duration::from_secs(300)).await;
        assert!(worker.as_mut().now_or_never().is_none());
        assert_eq!(routes.count(), 0);
        assert!(routes.writes.lock().unwrap().is_empty());
        cancellation.cancel();
        worker.await;
    }

    #[tokio::test]
    async fn html_owners_keep_eof_on_io_hints_and_recover_source_and_real_deadline() {
        let (directory, store, routes) = html_fixture().await;
        let service = Arc::new(ArtifactService::new_with_policies(
            store.clone(),
            Arc::new(pioneer_artifacts::LocalArtifactBlobStore::new(
                directory.path(),
            )),
            pioneer_artifacts::ArtifactQuotaPolicy::default(),
            pioneer_artifacts::ArtifactGcPolicy {
                grace_secs: 1,
                ..Default::default()
            },
        ));
        let cancellation = CancellationToken::new();
        let barrier = store
            .database_connection()
            .shared_resource::<SettledBarrier>();
        // Stabilize only durable startup markers, preserving the real runner.
        let mut progress = Progress::default();
        for _ in 0..32 {
            if !quantum(&store, &service, &mut progress).await.unwrap() {
                break;
            }
        }
        assert!(progress.blocked_finished && progress.finished.iter().all(|done| *done));
        routes.reset();
        tokio::time::pause();
        barrier.armed.store(7, std::sync::atomic::Ordering::Release);
        let mut worker = Box::pin(run(store.clone(), service, cancellation.clone()));
        drive_until(worker.as_mut(), async {
            for signal in &barrier.entered {
                signal.notified().await;
            }
        })
        .await;
        assert!(routes.writes.lock().unwrap().is_empty());
        for signal in &barrier.release {
            signal.notify_one();
        }
        assert!(worker.as_mut().now_or_never().is_none());
        routes.reset();
        tokio::time::advance(Duration::from_secs(300)).await;
        assert!(worker.as_mut().now_or_never().is_none());
        assert_eq!(
            routes.count(),
            0,
            "all successful Empty owners stay dormant"
        );
        for _ in 0..5 {
            barrier.armed.store(1, std::sync::atomic::Ordering::Release);
            store.notify_html_preview_io_work();
            drive_until(worker.as_mut(), barrier.entered[0].notified()).await;
            routes.assert_idle_io_only();
            barrier.release[0].notify_one();
            assert!(worker.as_mut().now_or_never().is_none());
            routes.reset();
            tokio::time::advance(Duration::from_secs(60)).await;
            assert!(worker.as_mut().now_or_never().is_none());
            assert_eq!(
                routes.count(),
                0,
                "IO-only hint must not reopen history/retention"
            );
        }
        use sea_orm::Set;
        use sha2::{Digest, Sha256};
        let key = hex::encode(Sha256::digest(b"missing-source"));
        let database = store.database_connection();
        // Fixture models an owned source transaction with its guard alive
        // through commit, without the obsolete unsupported foreign-writer poll.
        let change = store.background_work("storage.html.history").changing();
        work::Entity::insert(work::ActiveModel {
            family: Set("copies".into()),
            object_id: Set(key.clone()),
            ordinal: Set(0),
            workspace_id: Set("ws".into()),
            incarnation: Set("late".into()),
            requested_revision: Set("late".into()),
            pass_revision: Set("".into()),
            state: Set("blocked".into()),
            cursor_id: Set(Some("missing-source".into())),
            due_at: Set(None),
            seed_version: Set(0),
            ..Default::default()
        })
        .exec_without_returning(&database)
        .await
        .unwrap();
        barrier.armed.store(3, std::sync::atomic::Ordering::Release);
        change.finish(true);
        drive_until(worker.as_mut(), async {
            barrier.entered[1].notified().await;
            barrier.entered[0].notified().await;
        })
        .await;
        assert!(
            work::Entity::find_by_id(("copies".to_owned(), key.clone(), 0_i64))
                .one(&database)
                .await
                .unwrap()
                .is_none(),
            "released source reaches fenced claim and obsolete ACK"
        );
        assert!(routes.writes.lock().unwrap().iter().any(|e| matches!(
            e,
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Maintenance,
                ..
            }
        )));
        for signal in &barrier.release {
            signal.notify_one();
        }
        assert!(worker.as_mut().now_or_never().is_none());
        // Observe a real future millisecond deadline, then let only that timer
        // expire. Real 1.1s wall time is intentional: virtual time cannot
        // pretend UTC advanced. There is no real-minute wait or fresh commit.
        let change = store.background_work("storage.html.io").changing();
        let due = chrono::Utc::now().timestamp_millis() + 1_000;
        work::Entity::insert(work::ActiveModel {
            family: Set("copies".into()),
            object_id: Set(key.clone()),
            ordinal: Set(0),
            workspace_id: Set("ws".into()),
            incarnation: Set("timer".into()),
            requested_revision: Set("timer".into()),
            pass_revision: Set("".into()),
            state: Set("timed".into()),
            cursor_id: Set(Some("missing-source".into())),
            due_at: Set(Some(due)),
            seed_version: Set(0),
            ..Default::default()
        })
        .exec_without_returning(&database)
        .await
        .unwrap();
        routes.reset();
        barrier.armed.store(1, std::sync::atomic::Ordering::Release);
        change.finish(true);
        drive_until(worker.as_mut(), barrier.entered[0].notified()).await;
        assert!(
            routes.writes.lock().unwrap().is_empty(),
            "future IO may not claim early"
        );
        barrier.release[0].notify_one();
        assert!(worker.as_mut().now_or_never().is_none());
        tokio::time::resume();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        tokio::time::pause();
        barrier.armed.store(1, std::sync::atomic::Ordering::Release);
        drive_until(worker.as_mut(), barrier.entered[0].notified()).await;
        assert!(
            work::Entity::find_by_id(("copies".to_owned(), key, 0_i64))
                .one(&database)
                .await
                .unwrap()
                .is_none()
        );
        barrier.release[0].notify_one();
        assert!(worker.as_mut().now_or_never().is_none());
        // Lower supported metadata publication wakes retention after EOF. Its
        // future orphan grace is collected by the same raw16 page, then the
        // clock alone completes physical cleanup with no later source commit.
        let hash = hex::encode(Sha256::digest(b"orphan grace"));
        let blob = store
            .find_or_create_artifact_blob(pioneer_crud::NewArtifactBlobRecord {
                workspace_id: "ws".into(),
                sha256: hash.clone(),
                size_bytes: 12,
                mime_type: None,
                storage_backend: "local".into(),
                storage_key: format!("sha256/{}/{}/{}", &hash[..2], &hash[2..4], hash),
                metadata: [("htmlToolTemporary".into(), serde_json::json!(true))]
                    .into_iter()
                    .collect(),
            })
            .await
            .unwrap();
        barrier.armed.store(6, std::sync::atomic::Ordering::Release);
        drive_until(worker.as_mut(), async {
            barrier.entered[1].notified().await;
            barrier.entered[2].notified().await;
        })
        .await;
        assert!(
            pioneer_entity::artifact_blob::Entity::find_by_id(&blob.id)
                .one(&database)
                .await
                .unwrap()
                .is_some()
        );
        barrier.release[1].notify_one();
        barrier.release[2].notify_one();
        assert!(worker.as_mut().now_or_never().is_none());
        for _ in 0..3 {
            barrier.armed.store(1, std::sync::atomic::Ordering::Release);
            store.notify_html_preview_io_work();
            drive_until(worker.as_mut(), barrier.entered[0].notified()).await;
            barrier.release[0].notify_one();
            assert!(worker.as_mut().now_or_never().is_none());
        }
        tokio::time::resume();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        tokio::time::pause();
        barrier.armed.store(4, std::sync::atomic::Ordering::Release);
        drive_until(worker.as_mut(), barrier.entered[2].notified()).await;
        assert!(
            pioneer_entity::artifact_blob::Entity::find_by_id(&blob.id)
                .one(&database)
                .await
                .unwrap()
                .is_none(),
            "IO hints did not move grace eligibility; timer cleanup reached durable deletion"
        );
        cancellation.cancel();
        worker.await;
    }
}
