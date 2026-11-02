use pioneer_artifacts::{
    ArtifactService, HtmlPreviewMaintenanceOutcome, HtmlPreviewMaintenanceProgress,
};
use pioneer_crud::{CrudStore, HtmlPreviewIoClaim, HtmlPreviewIoOutcome, HtmlPreviewIoSource};
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
    Ok(true)
}

pub(crate) async fn quantum(
    store: &CrudStore,
    service: &ArtifactService,
    progress: &mut Progress,
) -> anyhow::Result<bool> {
    let store = store.with_maintenance_access();
    // Bounded queue recovery runs even after a lost local wake or restart.
    progress.blocked_after = store
        .recover_html_preview_io_blocked(&progress.blocked_after)
        .await?
        .unwrap_or_default();
    let io_result = io_quantum(&store, service).await;
    if progress.finished.iter().all(|done| *done) {
        progress.finished = [false; 2];
    }
    if !progress.io_seeded {
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
            .html_preview_maintenance_outcome(&mut progress.previews)
            .await?
        {
            HtmlPreviewMaintenanceOutcome::More => {}
            HtmlPreviewMaintenanceOutcome::Idle => progress.finished[1] = true,
            HtmlPreviewMaintenanceOutcome::Deferred => deferred = true,
        }
    }
    // One poison IO row cannot prevent progress of retirement and BlobGC.
    let io_more = io_result?;
    if let Some(error) = witness_error {
        return Err(error);
    }
    Ok(io_more || !progress.finished[0] || (!deferred && !progress.finished[1]))
}

pub(super) async fn run(
    store: Arc<CrudStore>,
    service: Arc<ArtifactService>,
    cancellation: CancellationToken,
) {
    let mut progress = Progress::default();
    let io_notification = store.html_preview_io_work_notification();
    loop {
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = crate::database::attribution::scope_database_workload_result(pioneer_observability::DatabaseWorkload::HtmlPreviewMaintenance, pioneer_observability::DatabaseOperationSite::HtmlPreviewQuantum, quantum(&store, &service, &mut progress)) => result,
        };
        let pause = match result {
            Ok(true) => Duration::from_millis(10),
            Ok(false) => RETRY,
            Err(_) => {
                tracing::warn!("HTML preview maintenance deferred; durable work retained");
                RETRY
            }
        };
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = service.html_preview_release_notified() => {},
            _ = io_notification.notified() => {},
            _ = pioneer_observability::live::sleeping(pause, tokio::time::sleep(pause)) => {},
        }
    }
}
