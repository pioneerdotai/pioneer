use super::*;

#[cfg(test)]
use chrono::{DateTime, Utc};

#[cfg(test)]
pub(super) fn date_label(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| timestamp.to_string())
}

pub(super) fn scope_label(scope: &MemoryScope) -> String {
    match scope.kind {
        MemoryScopeKind::User => "user".to_owned(),
        MemoryScopeKind::Workspace => format!("workspace:{}", scope.key),
        MemoryScopeKind::Thread => format!("thread:{}", scope.key),
        MemoryScopeKind::Agent => format!("agent:{}", scope.key),
        MemoryScopeKind::Task => format!("task:{}", scope.key),
    }
}

pub(super) fn category_label(category: MemoryCategory) -> &'static str {
    match category {
        MemoryCategory::Identity => "identity",
        MemoryCategory::Preference => "preference",
        MemoryCategory::Biography => "biography",
        MemoryCategory::Relationship => "relationship",
        MemoryCategory::RecurringInstruction => "recurring_instruction",
        MemoryCategory::ProjectPolicy => "project_policy",
        MemoryCategory::ProjectFact => "project_fact",
        MemoryCategory::ProjectDecision => "project_decision",
        MemoryCategory::Procedure => "procedure",
        MemoryCategory::Todo => "todo",
        MemoryCategory::Constraint => "constraint",
        MemoryCategory::CommunicationStyle => "communication_style",
        MemoryCategory::Custom => "custom",
    }
}

pub(super) fn memory_status_label(status: MemoryStatus) -> &'static str {
    match status {
        MemoryStatus::Active => "active",
        MemoryStatus::Superseded => "superseded",
        MemoryStatus::Deleted => "deleted",
        MemoryStatus::Expired => "expired",
    }
}

pub(super) fn candidate_status_label(status: MemoryCandidateStatus) -> &'static str {
    match status {
        MemoryCandidateStatus::Pending => "pending",
        MemoryCandidateStatus::PendingSilent => "pending_silent",
        MemoryCandidateStatus::AskOnUse => "ask_on_use",
        MemoryCandidateStatus::NeedsReview => "needs_review",
        MemoryCandidateStatus::Approved => "approved",
        MemoryCandidateStatus::Rejected => "rejected",
        MemoryCandidateStatus::AutoRejected => "auto_rejected",
        MemoryCandidateStatus::ReviewDisabledRejected => "review_disabled_rejected",
        MemoryCandidateStatus::Superseded => "superseded",
        MemoryCandidateStatus::MergedDuplicate => "merged_duplicate",
        MemoryCandidateStatus::Expired => "expired",
    }
}

pub(super) fn turn_post_turn_status_label(status: TurnPostTurnStatus) -> &'static str {
    match status {
        TurnPostTurnStatus::Succeeded => "succeeded",
        TurnPostTurnStatus::Failed => "failed",
        TurnPostTurnStatus::ProviderFailure => "provider_failure",
        TurnPostTurnStatus::Interrupted => "interrupted",
        TurnPostTurnStatus::Blocked => "blocked",
    }
}
