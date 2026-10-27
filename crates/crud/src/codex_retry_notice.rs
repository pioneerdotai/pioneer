//! Narrow, read-only presentation links for Codex retry notices.
use crate::CrudStore;
use anyhow::{Result, bail};
use pioneer_protocol::{
    CodexRetryNotice, SystemEventLevel, TaskAttachmentMode, TaskTurnItem, TurnItem, TurnKind,
};

const NOTICE_READ_BATCH_SIZE: u64 = 64;

/// Shared by canonical projection and exact historical lookup.
pub fn codex_retry_notice_item_id(turn_id: &str, attempt_id: &str) -> String {
    crate::canonical_agent_id(
        'I',
        &format!("codex-request-retried:{turn_id}:{attempt_id}"),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRetryNoticeTarget {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub task: TaskTurnItem,
}

impl CrudStore {
    /// Discover only an exact primary execution -> run -> occurrence card link.
    /// Each point read releases database capacity before the next operation;
    /// callers reauthorize the source child before disclosing its notice body.
    pub async fn codex_retry_notice_target(
        &self,
        source_thread_id: &str,
        source_turn_id: &str,
    ) -> Result<Option<CodexRetryNoticeTarget>> {
        let Some(child) = self.get_thread_by_id(source_thread_id).await? else {
            return Ok(None);
        };
        if child.access_class != "internal" || child.sidebar_visibility != "hidden" {
            return Ok(None);
        }
        let Some(binding) = self.get_cli_runtime_turn_binding(source_turn_id).await? else {
            return Ok(None);
        };
        if binding.thread_id != source_thread_id
            || binding.workspace_id != child.workspace_id
            || binding.runtime_kind != "codex"
        {
            return Ok(None);
        }
        let Some(turn) = self
            .get_task_run_turn_by_turn(source_thread_id, source_turn_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(execution_id) = turn.execution_id.as_deref() else {
            return Ok(None);
        };
        let Some(execution) = crate::repositories::task_run_execution::find_execution_by_id(
            &self.connection,
            execution_id,
        )
        .await?
        else {
            return Ok(None);
        };
        if execution.task_id != turn.task_id || execution.task_run_id != turn.run_id {
            return Ok(None);
        }
        let Some(primary) = self
            .get_task_run_primary_thread_binding(&turn.run_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(lineage) = self.get_task_thread_lineage(source_thread_id).await? else {
            return Ok(None);
        };
        let Some(run) = self.get_task_run(&turn.run_id).await? else {
            return Ok(None);
        };
        let Some(task) = self.get_task_record(&turn.task_id).await? else {
            return Ok(None);
        };
        let Some(occurrence) = self
            .get_task_occurrence_contract_by_run(&turn.run_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(parent_thread_id) = lineage.created_by_thread_id.as_deref() else {
            return Ok(None);
        };
        if primary.thread_id != source_thread_id
            || primary.task_id != turn.task_id
            || primary.execution_id.is_none()
            || primary.execution_id != turn.execution_id
            || run.task_id != turn.task_id
            || task.workspace_id != child.workspace_id
            || occurrence.task_id != turn.task_id
            || occurrence.run_id != turn.run_id
            || occurrence
                .delivery_plan
                .as_ref()
                .and_then(|plan| plan.presentation_thread_id.as_deref())
                != Some(parent_thread_id)
            || lineage.child_thread_id != source_thread_id
            || lineage.depth <= 0
            || lineage.origin_kind.as_deref() != Some("task_run")
            || lineage.created_by_turn_id.as_deref() != Some(turn.run_id.as_str())
        {
            return Ok(None);
        }
        let Some((workspace, parent_turn)) = self.get_turn(parent_thread_id, &turn.run_id).await?
        else {
            return Ok(None);
        };
        let Some(root) = self.get_thread_by_id(&lineage.root_thread_id).await? else {
            return Ok(None);
        };
        if workspace != child.workspace_id
            || root.workspace_id != workspace
            || root.access_class == "internal"
            || parent_turn.turn_kind != TurnKind::TaskRun
        {
            return Ok(None);
        }
        let anchor = self
            .list_turn_items_by_type(&turn.run_id, "task")
            .await?
            .into_iter()
            .find_map(|item| match item {
                TurnItem::Task { item }
                    if item.task_id == turn.task_id
                        && item.run_id.as_deref() == Some(turn.run_id.as_str())
                        && item.attachment == TaskAttachmentMode::Detached
                        && item.child_thread_id.as_deref() == Some(source_thread_id)
                        && item.child_turn_id.as_deref() == Some(source_turn_id) =>
                {
                    Some(item)
                }
                _ => None,
            });
        Ok(anchor.map(|task| CodexRetryNoticeTarget {
            workspace_id: workspace,
            thread_id: parent_thread_id.to_owned(),
            turn_id: turn.run_id,
            task,
        }))
    }

    pub async fn codex_retry_notices_for_task(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        task: &TaskTurnItem,
    ) -> Result<Vec<CodexRetryNotice>> {
        let (Some(source_thread), Some(source_turn)) = (
            task.child_thread_id.as_deref(),
            task.child_turn_id.as_deref(),
        ) else {
            return Ok(Vec::new());
        };
        let Some(target) = self
            .codex_retry_notice_target(source_thread, source_turn)
            .await?
        else {
            return Ok(Vec::new());
        };
        if target.workspace_id != workspace_id
            || target.thread_id != thread_id
            || target.turn_id != turn_id
            || target.task.id != task.id
            || target.task.task_id != task.task_id
            || target.task.run_id != task.run_id
            || target.task.attachment != task.attachment
            || target.task.child_thread_id != task.child_thread_id
            || target.task.child_turn_id != task.child_turn_id
        {
            return Ok(Vec::new());
        }
        // Read only the exact IDs issued for durable Codex attempts. Bound each
        // payload read, release capacity before decoding, and walk all batches
        // through a fixed high-water mark rather than silently truncating notices.
        let Some(latest) = crate::repositories::cli_runtime_binding::latest_turn_attempt(
            &self.connection,
            source_turn,
        )
        .await?
        else {
            return Ok(Vec::new());
        };
        let mut after_index = 0;
        let mut notices = Vec::new();
        loop {
            let attempts =
                crate::repositories::cli_runtime_binding::list_codex_turn_attempt_id_batch(
                    &self.connection,
                    source_turn,
                    after_index,
                    i64::from(latest.attempt_index),
                    NOTICE_READ_BATCH_SIZE,
                )
                .await?;
            let Some((_, last_index)) = attempts.last() else {
                break;
            };
            after_index = *last_index;
            let item_ids: Vec<_> = attempts
                .iter()
                .map(|(id, _)| codex_retry_notice_item_id(source_turn, id))
                .collect();
            let rows = crate::repositories::turn::list_turn_items_by_ids(
                &self.connection,
                source_turn,
                &item_ids,
            )
            .await?;
            for row in rows {
                match serde_json::from_str::<TurnItem>(&row.payload)? {
                    TurnItem::SystemEvent {
                        id,
                        level: SystemEventLevel::Warning,
                        code: Some(code),
                        message,
                        details: None,
                    } if code == "codex_request_retried"
                        && id == row.item_id
                        && row.item_type == "system_event" =>
                    {
                        notices.push(CodexRetryNotice {
                            source_thread_id: source_thread.to_owned(),
                            source_turn_id: source_turn.to_owned(),
                            item_id: id,
                            message,
                            occurred_at_unix_ms: row.created_at.timestamp_millis(),
                        });
                    }
                    _ => bail!("invalid canonical Codex retry notice"),
                }
            }
        }
        notices.sort_by(|left, right| {
            (left.occurred_at_unix_ms, &left.item_id)
                .cmp(&(right.occurred_at_unix_ms, &right.item_id))
        });
        Ok(notices)
    }
}
