//! Temporary first-party HTML media. Frozen inputs keep their exact source;
//! only unowned canonical preview results may become compact diagnostics.
use crate::CrudStore;
use anyhow::Result;
use pioneer_crud_artifacts::html_preview_receipts::{
    HtmlPreviewArchiveWitness, HtmlPreviewReceipts,
};
use pioneer_crud_artifacts::preview_retirement::{
    PreviewArtifactScan, PreviewCandidate, PreviewRetirementGuard, retire_preview,
};
use pioneer_crud_compaction::preview_frozen_index;
use pioneer_crud_compaction::preview_frozen_index::{PreviewImportIndex, unowned};
use sea_orm::ConnectionTrait;
use std::{future::Future, pin::Pin};

async fn index_quantum(
    store: &CrudStore,
    after: &mut String,
    imports: &mut Option<PreviewImportIndex>,
    upper: &str,
) -> Result<bool> {
    pioneer_crud_compaction::preview_frozen_index::index_quantum(
        &store.compaction_store(),
        &store.html_preview_io_work,
        after,
        imports,
        upper,
    )
    .await
}

#[derive(Default)]
pub struct HtmlPreviewRetentionProgress {
    artifacts: PreviewArtifactScan,
    index_after: String,
    imports: Option<PreviewImportIndex>,
    phase: usize,
    idle: [bool; 2],
    // Frozen index work and artifact discovery each own a finite pass.
    upper: [Option<String>; 2],
}
impl HtmlPreviewRetentionProgress {
    /// Returns false at discovery EOF. Errors do not pin the cursor to poison
    /// data; the maintenance owner must retain retry debt through pass EOF.
    pub async fn quantum(&mut self, store: &CrudStore) -> Result<bool> {
        while self.idle[self.phase] {
            self.phase = (self.phase + 1) % 2;
        }
        let path = self.phase;
        self.phase = (self.phase + 1) % 2;
        if self.upper[path].is_none() {
            self.upper[path] = Some(match path {
                0 => preview_frozen_index::frontier(&store.connection).await?,
                _ => store.html_preview_artifact_frontier().await?,
            });
        }
        let more = match path {
            0 => {
                index_quantum(
                    store,
                    &mut self.index_after,
                    &mut self.imports,
                    self.upper[0].as_deref().expect("captured frontier"),
                )
                .await?
            }
            _ => self.artifact_quantum(store).await?,
        };
        self.idle[path] = !more;
        if self.idle.iter().all(|idle| *idle) {
            self.idle = [false; 2];
            self.upper = Default::default();
            Ok(false)
        } else {
            Ok(true)
        }
    }
    async fn artifact_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        match self
            .artifacts
            .quantum(
                &store.connection,
                self.upper[1].as_deref().expect("captured frontier"),
            )
            .await?
        {
            PreviewCandidate::Exhausted => Ok(false),
            PreviewCandidate::Skipped => Ok(true),
            PreviewCandidate::Ready(candidate) => {
                retire_preview(
                    &store.connection,
                    &store.html_preview_io_work,
                    &FrozenPreviewGuard(&store.frozen_readers),
                    candidate,
                )
                .await?;
                Ok(true)
            }
        }
    }
}

struct FrozenPreviewGuard<'a>(&'a pioneer_crud_compaction::frozen_lifetime::ReaderCounts);

impl PreviewRetirementGuard for FrozenPreviewGuard<'_> {
    fn unowned<'a, C: ConnectionTrait + 'a>(
        &'a self,
        db: &'a C,
        workspace: &'a str,
        thread: &'a str,
        turn: &'a str,
        release: &'a pioneer_postgres_pools::WorkSignal,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            let root_execution = pioneer_crud_execution::canonical_id::canonical_agent_id(
                'E',
                &format!("root-agent-turn\0{turn}"),
            );
            unowned(
                db,
                workspace,
                thread,
                turn,
                &root_execution,
                self.0,
                release,
            )
            .await
        })
    }
}

impl CrudStore {
    /// Rechecked under the external storage mutation guard by every production
    /// unlink path. Check all owners of the physical key, including projections.
    pub async fn artifact_storage_key_is_unreferenced(
        &self,
        workspace: &str,
        key: &str,
    ) -> Result<bool> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .artifact_storage_key_is_unreferenced(workspace, key)
            .await
    }

    /// Bounded keyset discovery. Advance after a failed filesystem deletion too;
    /// a fresh pass/restart revisits its still-durable orphan row.
    pub async fn html_preview_gc_blob_page(
        &self,
        after: &str,
        cutoff_ms: i64,
    ) -> Result<(
        String,
        Vec<(
            String,
            pioneer_crud_artifacts::artifact::ArtifactGcBlobCandidateRecord,
        )>,
    )> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_gc_blob_page(after, cutoff_ms)
            .await
    }

    pub async fn html_preview_gc_blob_page_through(
        &self,
        after: &str,
        cutoff_ms: i64,
        upper: &str,
    ) -> Result<(
        String,
        Vec<(
            String,
            pioneer_crud_artifacts::artifact::ArtifactGcBlobCandidateRecord,
        )>,
    )> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_gc_blob_page_through(after, cutoff_ms, upper)
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_released_copies_page(
        &self,
        after: &str,
    ) -> Result<(String, Option<(String, String)>)> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_released_copies_page(after)
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_released_copies_page_through(
        &self,
        after: &str,
        upper: &str,
    ) -> Result<(String, Option<(String, String)>)> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_released_copies_page_through(after, upper)
            .await
    }

    pub async fn finish_html_preview_copy_cleanup(
        &self,
        id: &str,
        previous: &str,
        metadata: &str,
    ) -> Result<()> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .finish_html_preview_copy_cleanup(id, previous, metadata)
            .await
    }

    pub async fn finish_html_preview_copy_cleanup_claimed(
        &self,
        claim: &crate::HtmlPreviewIoClaim,
        previous: &str,
        metadata: &str,
    ) -> Result<bool> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .finish_html_preview_copy_cleanup_claimed(claim, previous, metadata)
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_archive_witness_page(
        &self,
        after: &(String, i64),
    ) -> Result<(Option<(String, i64)>, Option<HtmlPreviewArchiveWitness>)> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_archive_witness_page(after)
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_archive_witness_page_through(
        &self,
        after: &(String, i64),
        upper: &(String, i64),
    ) -> Result<(Option<(String, i64)>, Option<HtmlPreviewArchiveWitness>)> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_archive_witness_page_through(after, upper)
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn record_html_preview_archive_witness(
        &self,
        witness: &HtmlPreviewArchiveWitness,
        archive_hash: &str,
    ) -> Result<()> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .record_html_preview_archive_witness(witness, archive_hash)
            .await
    }

    pub async fn record_html_preview_archive_witness_claimed(
        &self,
        claim: &crate::HtmlPreviewIoClaim,
        witness: &HtmlPreviewArchiveWitness,
        archive_hash: &str,
    ) -> Result<bool> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .record_html_preview_archive_witness_claimed(claim, witness, archive_hash)
            .await
    }

    /// Only an exact, previously authenticated frozen preview can use this
    /// projection. Retirement's single UPDATE advances the existing source
    /// trigger exactly once. Require that adjacent revision, a release marker
    /// and the verified diagnostic hash; a later edit invalidates the receipt.
    /// An already archived diagnostic can also have an indexing receipt, but
    /// its unchanged wire hash cannot authorize another revision transition.
    pub async fn html_preview_archived_frozen_result(
        &self,
        workspace: &str,
        reference: &pioneer_compaction::frozen::FrozenMessageRef,
    ) -> Result<Option<(String, String, pioneer_compaction::SourceRef)>> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_archived_frozen_result(workspace, reference)
            .await
    }

    pub async fn load_html_preview_io_witness(
        &self,
        claim: &crate::HtmlPreviewIoClaim,
    ) -> Result<crate::HtmlPreviewIoSource<HtmlPreviewArchiveWitness>> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .load_html_preview_io_witness(claim)
            .await
    }

    pub async fn load_html_preview_io_copies(
        &self,
        claim: &crate::HtmlPreviewIoClaim,
    ) -> Result<crate::HtmlPreviewIoSource<(String, String)>> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .load_html_preview_io_copies(claim)
            .await
    }

    /// Future GC eligibility for currently unowned temporary blobs. Source
    /// changes can invalidate ownership and must wake maintenance separately.
    pub async fn html_preview_gc_deadline(
        &self,
        grace_ms: i64,
        pass_cutoff_ms: i64,
    ) -> Result<Option<i64>> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_gc_deadline(grace_ms, pass_cutoff_ms)
            .await
    }

    /// Capture once per keyset pass. Later source commits remain invalidations
    /// for the following pass, including changes behind its current cursor.
    pub async fn html_preview_artifact_frontier(&self) -> Result<String> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_artifact_frontier()
            .await
    }

    pub async fn html_preview_blob_frontier(&self) -> Result<String> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_blob_frontier()
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn html_preview_witness_frontier(&self) -> Result<(String, i64)> {
        HtmlPreviewReceipts::new(&self.connection, &self.html_preview_io_work)
            .html_preview_witness_frontier()
            .await
    }
}

#[cfg(test)]
#[path = "preview_retention_tests.rs"]
mod tests;
