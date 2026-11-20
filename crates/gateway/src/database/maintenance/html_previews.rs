use pioneer_artifacts::{
    ArtifactService, HtmlPreviewMaintenanceOutcome, HtmlPreviewMaintenanceProgress,
};
use pioneer_crud::{CrudStore, HtmlPreviewIoClaim, HtmlPreviewIoOutcome, HtmlPreviewIoSource};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const RETRY: Duration = Duration::from_secs(60);
const CLAIM_MS: i64 = 300_000;

/// The existing owner joins this worker on shutdown. Queue claims survive
/// cancellation; each discovery cursor retains its frontier until EOF.
#[derive(Default)]
pub(crate) struct Progress {
    previews: HtmlPreviewMaintenanceProgress,
    blocked_after: (String, String, i64),
    blocked_upper: Option<(String, String, i64)>,
    previews_finished: bool,
    revisit: bool,
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

async fn io_quantum(store: &CrudStore, service: &ArtifactService) -> anyhow::Result<bool> {
    let Some(claim) = store
        .claim_html_preview_io_work(chrono::Utc::now().timestamp_millis(), CLAIM_MS)
        .await?
    else {
        return Ok(false);
    };
    match claim.row.family.as_str() {
        "seed_witness" | "seed_copies" if !claim.row.workspace_id.is_empty() => {
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
        "copies" => service.cleanup_claimed_html_preview_copy(&claim).await?,
        _ => settle(store, &claim, HtmlPreviewIoOutcome::Quarantine).await?,
    }
    Ok(true)
}

pub(crate) async fn quantum(
    store: &CrudStore,
    service: &ArtifactService,
    progress: &mut Progress,
) -> anyhow::Result<bool> {
    let store = store.with_maintenance_access();
    // Bounded queue recovery runs even after a lost local wake or restart.
    if progress.blocked_upper.is_none() {
        progress.blocked_upper = Some(store.html_preview_io_recovery_frontier().await?);
    }
    let blocked_next = store
        .recover_html_preview_io_blocked_through(
            &progress.blocked_after,
            progress.blocked_upper.as_ref().expect("captured frontier"),
        )
        .await?;
    if blocked_next.is_none() {
        progress.blocked_upper = None;
    }
    let blocked_more = blocked_next.is_some();
    progress.blocked_after = blocked_next.unwrap_or_default();
    let io_result = io_quantum(&store, service).await;

    let mut deferred = false;
    if !progress.previews_finished {
        match service
            .html_preview_maintenance_outcome(&mut progress.previews)
            .await?
        {
            HtmlPreviewMaintenanceOutcome::More => {}
            HtmlPreviewMaintenanceOutcome::Idle => progress.previews_finished = true,
            HtmlPreviewMaintenanceOutcome::Deferred => deferred = true,
        }
    }
    // One poison IO row cannot prevent progress of retirement and BlobGC.
    let io_more = io_result?;
    if progress.previews_finished && !blocked_more && progress.revisit {
        *progress = Progress::default();
        return Ok(true);
    }
    Ok(blocked_more || io_more || (!deferred && !progress.previews_finished))
}

pub(super) async fn run(
    store: Arc<CrudStore>,
    service: Arc<ArtifactService>,
    cancellation: CancellationToken,
) {
    let mut progress = Progress::default();
    let io_notification = store.html_preview_io_work_notification();
    let database = store.database_connection();
    let mut wake = database.work_signal("maintenance.html").subscribe();
    let mut released = database.work_signal("maintenance.html.release").subscribe();
    let mut begin = true;
    loop {
        if begin {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                result = wake.begin_pass(&database) => if result.is_err() {
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        _ = tokio::time::sleep(RETRY) => {},
                    }
                    continue;
                },
            }
            begin = false;
        }
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = crate::database::attribution::scope_database_workload_result(pioneer_observability::DatabaseWorkload::HtmlPreviewMaintenance, pioneer_observability::DatabaseOperationSite::HtmlPreviewQuantum, quantum(&store, &service, &mut progress)) => result,
        };
        if matches!(&result, Ok(false)) {
            let deadline = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                result = service.html_preview_maintenance_deadline(&progress.previews) => result,
            };
            if let Ok(deadline) = deadline {
                let release_only = tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = service.html_preview_release_notified() => true,
                    _ = released.changed() => true,
                    _ = io_notification.notified() => false,
                    _ = wake.wait(deadline) => false,
                };
                if release_only {
                    progress.previews.invalidate_completed();
                    progress.previews_finished = false;
                    continue;
                }
                if progress.previews_finished {
                    progress = Progress::default();
                } else {
                    progress.previews.invalidate_completed();
                    progress.revisit = true;
                }
                begin = true;
                continue;
            }
        }
        if result.is_err() || matches!(&result, Ok(false)) {
            // Error-owned backoff cannot be consumed by our own UNKNOWN wake
            // after a failed write. Signals remain pending for the next pass.
            tracing::warn!("HTML preview maintenance deferred; durable work retained");
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                _ = pioneer_observability::live::sleeping(RETRY, tokio::time::sleep(RETRY)) => {},
            }
            progress.revisit = true;
            begin = true;
            continue;
        }
        let pause = Duration::from_millis(10);
        let invalidated = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = wake.changed() => true,
            _ = io_notification.notified() => true,
            _ = pioneer_observability::live::sleeping(pause, tokio::time::sleep(pause)) => false,
        };
        if invalidated {
            // A continuous IO backlog must not hide changes behind a retained
            // cursor. Finish its fixed frontier, then run another finite pass.
            progress.revisit = true;
            begin = true;
        }
    }
}
