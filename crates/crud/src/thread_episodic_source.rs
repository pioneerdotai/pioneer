//! Pure source selection and version preparation shared by canonical saves and ingestion.
use crate::{
    NewThreadEpisodicItemRecord, ThreadEpisodicItemStatus, ThreadEpisodicItemVisibility,
    ThreadEpisodicSourceRuntimeKind,
};
use pioneer_protocol::{
    AgentMessagePhase, TaskStatus, TaskTurnItem, ThreadEpisodicSourceActorRole,
    ThreadEpisodicSourceContext, TurnItem, TurnItemType,
};
use sha2::{Digest, Sha256};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicCommittedItem {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub source_actor_role: Option<ThreadEpisodicSourceActorRole>,
    pub source_context: ThreadEpisodicSourceContext,
    pub item: TurnItem,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadEpisodicIngestionSkipReason {
    EmptyText,
    HiddenPrompt,
    SystemPrompt,
    DeveloperPrompt,
    ReasoningTrace,
    RawToolOutput,
    ToolItemsDisabled,
    AgentCommentary,
    InternalHookRuntime,
    TaskRuntimePrivate,
    UnsupportedSourceContext,
    IngestionNotConfigured,
}

impl ThreadEpisodicIngestionSkipReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EmptyText => "empty_text",
            Self::HiddenPrompt => "hidden_prompt",
            Self::SystemPrompt => "system_prompt",
            Self::DeveloperPrompt => "developer_prompt",
            Self::ReasoningTrace => "reasoning_trace",
            Self::RawToolOutput => "raw_tool_output",
            Self::ToolItemsDisabled => "tool_items_disabled",
            Self::AgentCommentary => "agent_commentary",
            Self::InternalHookRuntime => "internal_hook_runtime",
            Self::TaskRuntimePrivate => "task_runtime_private",
            Self::UnsupportedSourceContext => "unsupported_source_context",
            Self::IngestionNotConfigured => "thread_episodic_ingestion_not_configured",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicIndexableSource {
    pub text: String,
    pub source_actor_role: ThreadEpisodicSourceActorRole,
    pub source_context: ThreadEpisodicSourceContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadEpisodicSourceSelection {
    Indexable(ThreadEpisodicIndexableSource),
    Rejected {
        reason: ThreadEpisodicIngestionSkipReason,
    },
}

pub fn store_source_actor_role(
    role: ThreadEpisodicSourceActorRole,
) -> crate::ThreadEpisodicSourceActorRole {
    match role {
        ThreadEpisodicSourceActorRole::User => crate::ThreadEpisodicSourceActorRole::User,
        ThreadEpisodicSourceActorRole::Assistant => crate::ThreadEpisodicSourceActorRole::Assistant,
        ThreadEpisodicSourceActorRole::TaskSummary => crate::ThreadEpisodicSourceActorRole::Task,
        ThreadEpisodicSourceActorRole::GeneratedSummary => {
            crate::ThreadEpisodicSourceActorRole::SystemVisible
        }
    }
}

pub fn store_source_runtime_kind(item_type: TurnItemType) -> ThreadEpisodicSourceRuntimeKind {
    match item_type {
        TurnItemType::UserMessage => ThreadEpisodicSourceRuntimeKind::UserTurn,
        TurnItemType::AgentMessage => ThreadEpisodicSourceRuntimeKind::AssistantTurn,
        TurnItemType::Task => ThreadEpisodicSourceRuntimeKind::TaskResult,
        TurnItemType::CommandExecution
        | TurnItemType::FileChange
        | TurnItemType::WebSearch
        | TurnItemType::WebFetch
        | TurnItemType::Download
        | TurnItemType::DynamicToolCall => {
            unreachable!("tool turn items are not accepted by thread episodic indexing")
        }
        TurnItemType::Reasoning | TurnItemType::SystemEvent => {
            ThreadEpisodicSourceRuntimeKind::CompactionSummary
        }
    }
}

pub fn sha256_hex(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

pub fn source_text_hash(text: &str) -> String {
    sha256_hex(normalize_for_thread_episodic_hash(text).as_str())
}

pub fn occurrence_projection_group_id(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    source_text_hash: &str,
) -> String {
    sha256_hex(
        format!(
            "thread_episodic_projection_v1\noccurrence\nworkspace={}\nthread={}\nturn={}\nitem={}\nsource_text_hash={}\n",
            workspace_id.trim(),
            thread_id.trim(),
            turn_id.trim(),
            item_id.trim(),
            source_text_hash.trim()
        )
        .as_str(),
    )
}

pub fn task_result_projection_group_id(
    workspace_id: &str,
    run_id: &str,
    source_text_hash: &str,
) -> String {
    sha256_hex(
        format!(
            "thread_episodic_projection_v1\ntask_result\nworkspace={}\nrun={}\nsource_text_hash={}\n",
            workspace_id.trim(),
            run_id.trim(),
            source_text_hash.trim()
        )
        .as_str(),
    )
}

pub fn item_text_hash(item: &ThreadEpisodicCommittedItem, item_text: &str) -> String {
    let source_id = normalized_item_source_id(item);
    let normalized_item_text = normalize_for_thread_episodic_hash(item_text);
    sha256_hex(format!("{source_id}\n{normalized_item_text}").as_str())
}

pub fn normalized_item_source_id(item: &ThreadEpisodicCommittedItem) -> String {
    format!(
        "workspace:{}/thread:{}/turn:{}/item:{}",
        item.workspace_id.trim(),
        item.thread_id.trim(),
        item.turn_id.trim(),
        item.item_id.trim()
    )
}

pub fn normalize_for_thread_episodic_hash(text: &str) -> String {
    text.replace("\r\n", "\n")
        .replace('\r', "\n")
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

pub fn committed_item_source_actor_role(item: &TurnItem) -> Option<ThreadEpisodicSourceActorRole> {
    match item {
        TurnItem::UserMessage { .. } => Some(ThreadEpisodicSourceActorRole::User),
        TurnItem::AgentMessage {
            phase: AgentMessagePhase::FinalAnswer,
            ..
        } => Some(ThreadEpisodicSourceActorRole::Assistant),
        TurnItem::AgentMessage {
            phase: AgentMessagePhase::Commentary,
            ..
        } => None,
        TurnItem::CommandExecution { .. }
        | TurnItem::FileChange { .. }
        | TurnItem::WebSearch { .. }
        | TurnItem::WebFetch { .. }
        | TurnItem::Download { .. }
        | TurnItem::DynamicToolCall { .. } => None,
        TurnItem::Task { .. } => Some(ThreadEpisodicSourceActorRole::TaskSummary),
        TurnItem::Reasoning { .. } | TurnItem::SystemEvent { .. } => None,
    }
}

pub fn committed_item_source_context(item: &TurnItem) -> ThreadEpisodicSourceContext {
    match item {
        TurnItem::UserMessage { .. }
        | TurnItem::AgentMessage {
            phase: AgentMessagePhase::FinalAnswer,
            ..
        } => ThreadEpisodicSourceContext::UserVisibleThreadItem,
        TurnItem::AgentMessage {
            phase: AgentMessagePhase::Commentary,
            ..
        } => ThreadEpisodicSourceContext::InternalHookRuntime,
        TurnItem::CommandExecution { .. }
        | TurnItem::FileChange { .. }
        | TurnItem::WebSearch { .. }
        | TurnItem::WebFetch { .. }
        | TurnItem::Download { .. }
        | TurnItem::DynamicToolCall { .. } => ThreadEpisodicSourceContext::RawToolOutput,
        TurnItem::Task { .. } => ThreadEpisodicSourceContext::UserVisibleTaskSummary,
        TurnItem::Reasoning { .. } => ThreadEpisodicSourceContext::ReasoningTrace,
        TurnItem::SystemEvent { .. } => ThreadEpisodicSourceContext::InternalHookRuntime,
    }
}

pub fn committed_item_ingestion_input(
    notification: &pioneer_protocol::ItemCompletedNotification,
) -> Option<ThreadEpisodicCommittedItem> {
    committed_item_ingestion_input_from_parts(
        notification.workspace_id.as_str(),
        notification.thread_id.as_str(),
        notification.turn_id.as_str(),
        notification.item.clone(),
    )
}

pub fn committed_item_ingestion_input_from_parts(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item: TurnItem,
) -> Option<ThreadEpisodicCommittedItem> {
    let item_id = item.item_id().trim().to_owned();
    if workspace_id.trim().is_empty()
        || thread_id.trim().is_empty()
        || turn_id.trim().is_empty()
        || item_id.is_empty()
    {
        return None;
    }
    Some(ThreadEpisodicCommittedItem {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        item_id,
        item_type: item.item_type(),
        source_actor_role: committed_item_source_actor_role(&item),
        source_context: committed_item_source_context(&item),
        item,
    })
}

pub fn select_committed_item_source(
    item: &ThreadEpisodicCommittedItem,
) -> ThreadEpisodicSourceSelection {
    if matches!(
        item.item,
        TurnItem::CommandExecution { .. }
            | TurnItem::FileChange { .. }
            | TurnItem::WebSearch { .. }
            | TurnItem::WebFetch { .. }
            | TurnItem::Download { .. }
            | TurnItem::DynamicToolCall { .. }
    ) {
        return ThreadEpisodicSourceSelection::Rejected {
            reason: ThreadEpisodicIngestionSkipReason::ToolItemsDisabled,
        };
    }
    if matches!(
        item.item,
        TurnItem::AgentMessage {
            phase: AgentMessagePhase::Commentary,
            ..
        }
    ) {
        return ThreadEpisodicSourceSelection::Rejected {
            reason: ThreadEpisodicIngestionSkipReason::AgentCommentary,
        };
    }

    if let Some(reason) = hard_reject_source_context(&item.source_context) {
        return ThreadEpisodicSourceSelection::Rejected { reason };
    }

    match &item.item {
        TurnItem::UserMessage { text, .. } => indexable_text(
            text,
            ThreadEpisodicSourceActorRole::User,
            item.source_context.clone(),
        ),
        TurnItem::AgentMessage {
            text,
            phase: AgentMessagePhase::FinalAnswer,
            ..
        } => indexable_text(
            text,
            ThreadEpisodicSourceActorRole::Assistant,
            item.source_context.clone(),
        ),
        TurnItem::AgentMessage {
            phase: AgentMessagePhase::Commentary,
            ..
        } => ThreadEpisodicSourceSelection::Rejected {
            reason: ThreadEpisodicIngestionSkipReason::AgentCommentary,
        },
        TurnItem::Reasoning { .. } => ThreadEpisodicSourceSelection::Rejected {
            reason: ThreadEpisodicIngestionSkipReason::ReasoningTrace,
        },
        TurnItem::SystemEvent { .. } => ThreadEpisodicSourceSelection::Rejected {
            reason: ThreadEpisodicIngestionSkipReason::InternalHookRuntime,
        },
        TurnItem::CommandExecution { .. }
        | TurnItem::FileChange { .. }
        | TurnItem::WebSearch { .. }
        | TurnItem::WebFetch { .. }
        | TurnItem::Download { .. }
        | TurnItem::DynamicToolCall { .. } => unreachable!("tool items are rejected above"),
        TurnItem::Task { item } => {
            select_task_summary_source(item, ThreadEpisodicSourceContext::UserVisibleTaskSummary)
        }
    }
}

pub fn select_task_summary_source(
    item: &TaskTurnItem,
    source_context: ThreadEpisodicSourceContext,
) -> ThreadEpisodicSourceSelection {
    let Some(text) = task_summary_text(item) else {
        return ThreadEpisodicSourceSelection::Rejected {
            reason: ThreadEpisodicIngestionSkipReason::TaskRuntimePrivate,
        };
    };
    indexable_text(
        text.as_str(),
        ThreadEpisodicSourceActorRole::TaskSummary,
        source_context,
    )
}

pub fn task_summary_text(item: &TaskTurnItem) -> Option<String> {
    let title = item.title.trim();
    let result_preview = item.result_preview.as_deref().map(str::trim);
    if let Some(result_preview) = result_preview.filter(|preview| !preview.is_empty()) {
        return Some(format_task_summary_line(title, item.status, result_preview));
    }

    let error_preview = item.error_preview.as_deref().map(str::trim);
    if let Some(error_preview) = error_preview.filter(|preview| !preview.is_empty()) {
        return Some(format_task_summary_line(title, item.status, error_preview));
    }

    None
}

pub fn format_task_summary_line(title: &str, status: TaskStatus, preview: &str) -> String {
    if title.is_empty() {
        return preview.to_owned();
    }
    format!("{title}: {preview} ({})", task_status_label(status))
}

const fn task_status_label(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Draft => "draft",
        TaskStatus::Scheduled => "scheduled",
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Waiting => "waiting",
        TaskStatus::WaitingReview => "waiting_review",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Blocked => "blocked",
        TaskStatus::Cancelled => "cancelled",
    }
}

pub fn indexable_text(
    text: &str,
    source_actor_role: ThreadEpisodicSourceActorRole,
    source_context: ThreadEpisodicSourceContext,
) -> ThreadEpisodicSourceSelection {
    if text.trim().is_empty() {
        return ThreadEpisodicSourceSelection::Rejected {
            reason: ThreadEpisodicIngestionSkipReason::EmptyText,
        };
    }
    ThreadEpisodicSourceSelection::Indexable(ThreadEpisodicIndexableSource {
        text: text.to_owned(),
        source_actor_role,
        source_context,
    })
}

pub fn estimate_tokens(text: &str) -> i64 {
    let char_count = text.chars().count();
    std::cmp::max(1, char_count.div_ceil(4)) as i64
}

pub fn hard_reject_source_context(
    source_context: &ThreadEpisodicSourceContext,
) -> Option<ThreadEpisodicIngestionSkipReason> {
    match source_context {
        ThreadEpisodicSourceContext::UserVisibleThreadItem
        | ThreadEpisodicSourceContext::UserVisibleTaskSummary
        | ThreadEpisodicSourceContext::ThreadCompactionSummary => None,
        ThreadEpisodicSourceContext::HiddenPrompt => {
            Some(ThreadEpisodicIngestionSkipReason::HiddenPrompt)
        }
        ThreadEpisodicSourceContext::SystemPrompt => {
            Some(ThreadEpisodicIngestionSkipReason::SystemPrompt)
        }
        ThreadEpisodicSourceContext::DeveloperPrompt => {
            Some(ThreadEpisodicIngestionSkipReason::DeveloperPrompt)
        }
        ThreadEpisodicSourceContext::ReasoningTrace => {
            Some(ThreadEpisodicIngestionSkipReason::ReasoningTrace)
        }
        ThreadEpisodicSourceContext::RawToolOutput => {
            Some(ThreadEpisodicIngestionSkipReason::RawToolOutput)
        }
        ThreadEpisodicSourceContext::RawTaskRuntime => {
            Some(ThreadEpisodicIngestionSkipReason::TaskRuntimePrivate)
        }
        ThreadEpisodicSourceContext::InternalHookRuntime => {
            Some(ThreadEpisodicIngestionSkipReason::InternalHookRuntime)
        }
        ThreadEpisodicSourceContext::Unknown => {
            Some(ThreadEpisodicIngestionSkipReason::UnsupportedSourceContext)
        }
    }
}

/// Hash only the indexed content and occurrence, preserving the existing version contract.
pub fn prepare_source_record(
    item: &ThreadEpisodicCommittedItem,
    source: ThreadEpisodicIndexableSource,
    projection_group_id: String,
) -> NewThreadEpisodicItemRecord {
    let text = source.text.trim();
    NewThreadEpisodicItemRecord {
        id: None,
        workspace_id: item.workspace_id.clone(),
        thread_id: item.thread_id.clone(),
        turn_id: item.turn_id.clone(),
        item_id: item.item_id.clone(),
        source_actor_role: store_source_actor_role(source.source_actor_role),
        source_runtime_kind: store_source_runtime_kind(item.item_type),
        source_context: source.source_context,
        visibility: ThreadEpisodicItemVisibility::UserVisible,
        status: ThreadEpisodicItemStatus::PendingIndex,
        text_hash: item_text_hash(item, text),
        source_text_hash: source_text_hash(text),
        projection_group_id,
        language_hint: None,
        token_estimate: estimate_tokens(text),
        capsule_id: None,
        capsule_ref: None,
        segment_index: None,
        frame_id: None,
        frame_uri: None,
        indexed_at: None,
        deleted_at: None,
    }
}

pub fn source_status_is_committed(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("completed" | "failed" | "timed_out" | "cancelled")
    )
}
