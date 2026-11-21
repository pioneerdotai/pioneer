use crate::CrudStore;
use anyhow::Result;
use pioneer_crud_timeline::codex_retry_notice::CodexRetryNoticeRepository;
pub use pioneer_crud_timeline::codex_retry_notice::{
    CodexRetryNoticeRead, CodexRetryNoticeTarget, codex_retry_notice_item_id,
};
use pioneer_protocol::{CodexRetryNotice, TaskTurnItem};
use std::collections::HashMap;

impl CrudStore {
    pub async fn codex_retry_notice_reads(
        &self,
        source_turn_ids: &[String],
    ) -> Result<HashMap<String, CodexRetryNoticeRead>> {
        CodexRetryNoticeRepository::new(&self.connection)
            .codex_retry_notice_reads(source_turn_ids)
            .await
    }

    pub async fn codex_retry_notice_target(
        &self,
        source_thread_id: &str,
        source_turn_id: &str,
    ) -> Result<Option<CodexRetryNoticeTarget>> {
        CodexRetryNoticeRepository::new(&self.connection)
            .codex_retry_notice_target(source_thread_id, source_turn_id)
            .await
    }

    pub async fn codex_retry_notices_for_task(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        task: &TaskTurnItem,
    ) -> Result<Vec<CodexRetryNotice>> {
        CodexRetryNoticeRepository::new(&self.connection)
            .codex_retry_notices_for_task(workspace_id, thread_id, turn_id, task)
            .await
    }

    pub async fn codex_retry_notices_for_task_read(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        task: &TaskTurnItem,
        read: &CodexRetryNoticeRead,
    ) -> Result<Vec<CodexRetryNotice>> {
        CodexRetryNoticeRepository::new(&self.connection)
            .codex_retry_notices_for_task_read(workspace_id, thread_id, turn_id, task, read)
            .await
    }

    pub async fn codex_retry_notices_for_tasks(
        &self,
        targets: &[CodexRetryNoticeTarget],
        reads: &HashMap<String, CodexRetryNoticeRead>,
    ) -> Result<Vec<Vec<CodexRetryNotice>>> {
        CodexRetryNoticeRepository::new(&self.connection)
            .codex_retry_notices_for_tasks(targets, reads)
            .await
    }
}
