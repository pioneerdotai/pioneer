use crate::CrudStore;
use anyhow::Result;
use sea_orm::ConnectionTrait;

pub use pioneer_crud_artifacts::html_preview_io::{
    HtmlPreviewIoClaim, HtmlPreviewIoErrorClass, HtmlPreviewIoFamily, HtmlPreviewIoLocator,
    HtmlPreviewIoOutcome, HtmlPreviewIoSource, HtmlPreviewIoState,
};
pub(crate) use pioneer_crud_artifacts::html_preview_io::{
    HtmlPreviewIoQueue, ack, callback_ack, callback_ticket, fence, find_work, into_database_error,
    is_conflict, request_at, request_prepared,
};

impl CrudStore {
    fn html_preview_io_queue(&self) -> HtmlPreviewIoQueue<'_> {
        HtmlPreviewIoQueue::new(&self.connection)
    }

    pub async fn claim_html_preview_io_work(
        &self,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<HtmlPreviewIoClaim>> {
        self.html_preview_io_queue()
            .claim_html_preview_io_work(now_ms, lease_ms)
            .await
    }

    pub async fn defer_html_preview_io_work(
        &self,
        claim: &HtmlPreviewIoClaim,
        outcome: HtmlPreviewIoOutcome,
        now_ms: i64,
    ) -> Result<bool> {
        self.html_preview_io_queue()
            .defer_html_preview_io_work(claim, outcome, now_ms)
            .await
    }

    pub async fn request_html_preview_io_resync<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace: &HtmlPreviewIoLocator,
        families: &[HtmlPreviewIoFamily],
    ) -> Result<()> {
        self.html_preview_io_queue()
            .request_html_preview_io_resync(db, workspace, families)
            .await
    }

    pub async fn recover_html_preview_io_blocked(
        &self,
        after: &(String, String, i64),
    ) -> Result<Option<(String, String, i64)>> {
        self.html_preview_io_queue()
            .recover_html_preview_io_blocked(after)
            .await
    }

    pub async fn html_preview_io_recovery_frontier(&self) -> Result<(String, String, i64)> {
        self.html_preview_io_queue()
            .html_preview_io_recovery_frontier()
            .await
    }

    pub async fn recover_html_preview_io_blocked_through(
        &self,
        after: &(String, String, i64),
        upper: &(String, String, i64),
    ) -> Result<Option<(String, String, i64)>> {
        self.html_preview_io_queue()
            .recover_html_preview_io_blocked_through(after, upper)
            .await
    }

    pub async fn html_preview_io_seed_quantum(&self, claim: &HtmlPreviewIoClaim) -> Result<bool> {
        self.html_preview_io_queue()
            .html_preview_io_seed_quantum(claim)
            .await
    }

    pub async fn html_preview_io_deadline(&self) -> Result<Option<i64>> {
        self.html_preview_io_queue()
            .html_preview_io_deadline()
            .await
    }

    #[cfg(test)]
    pub(super) async fn html_preview_test_repair_quantum(&self) -> Result<bool> {
        if let Some(claim) = self
            .html_preview_io_queue()
            .claim_html_preview_io_work_kind(chrono::Utc::now().timestamp_millis(), 300_000, true)
            .await?
        {
            return self.html_preview_io_seed_quantum(&claim).await;
        }
        Ok(false)
    }
    /// Retained coalesced hint; callers of transactional repair helpers invoke
    /// notify only after their business transaction has committed and dropped.
    pub fn html_preview_io_work_notification(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.html_preview_io_work.clone()
    }
    pub fn notify_html_preview_io_work(&self) {
        self.html_preview_io_work.notify_one();
        self.connection.work_signal("maintenance.html").notify();
    }
}

#[cfg(test)]
pub(crate) async fn request<C: ConnectionTrait>(
    db: &C,
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
