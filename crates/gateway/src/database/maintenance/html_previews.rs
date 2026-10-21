use pioneer_artifacts::ArtifactService;
use pioneer_artifacts::HtmlPreviewMaintenanceProgress;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// The existing post-startup maintenance owner joins this worker on shutdown.
/// Progress is bounded; durable index/orphan rows resume after process restart.
#[derive(Default)]
pub(crate) struct Progress {
    previews: HtmlPreviewMaintenanceProgress,
    witness_after: (String, i64),
    finished: [bool; 2],
}

pub(crate) async fn quantum(
    store: &pioneer_crud::CrudStore,
    service: &ArtifactService,
    progress: &mut Progress,
) -> anyhow::Result<bool> {
    let store = store.with_maintenance_access();
    if progress.finished.iter().all(|done| *done) {
        progress.finished = [false; 2];
    }
    let mut witness_error = None;
    if !progress.finished[0] {
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
    // A poison/temporary witness failure does not stop the other traversals.
    if !progress.finished[1] {
        progress.finished[1] = !service
            .html_preview_maintenance_quantum(&mut progress.previews)
            .await?;
    }
    if let Some(error) = witness_error {
        return Err(error);
    }
    Ok(!progress.finished.iter().all(|done| *done))
}

pub(super) async fn run(
    store: Arc<pioneer_crud::CrudStore>,
    service: Arc<ArtifactService>,
    cancellation: CancellationToken,
) {
    let mut progress = Progress::default();
    loop {
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = crate::database::attribution::scope_database_workload_result(pioneer_observability::DatabaseWorkload::HtmlPreviewMaintenance,quantum(&store, &service, &mut progress)) => result,
        };
        let pause = match result {
            Ok(true) => Duration::from_millis(10),
            Ok(false) => Duration::from_secs(60),
            Err(_) => {
                tracing::warn!(
                    "HTML preview maintenance quantum failed; retained data will be revisited"
                );
                Duration::from_secs(60)
            }
        };
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = pioneer_observability::live::sleeping(pause,tokio::time::sleep(pause)) => {},
        }
    }
}
