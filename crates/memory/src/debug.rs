use pioneer_crud::{
    AgentMemoryCandidateRecord, AgentMemoryControlRecord, AgentMemoryEventRecord,
    AgentMemoryQualityDecisionRecord, AgentMemoryQuarantineRecord, AgentMemoryRepairJobRecord,
    HookRunRecord,
};
use pioneer_hooks::{HookDiagnosticSeverity, HookPhase, HookRunStatus};
use pioneer_protocol::{
    MemoryCandidateScore, MemoryCandidateStatus, MemoryCategory, MemoryEvidenceClass,
    MemoryFactClass, MemoryLifecycleReasonCode, MemoryLifetimeClass, MemoryOwnershipClass,
    MemoryQualityAction, MemoryQualityReasonCode, MemoryScope, MemorySemanticWriteRoute,
    MemorySensitivity, MemorySourceContextKind, MemoryStatus, MemoryWriteRelation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const MEMORY_DEBUG_TRACE_MAX_EVENTS: u64 = 50;
pub const MEMORY_DEBUG_TRACE_MAX_QUALITY_DECISIONS: u64 = 20;
pub const MEMORY_DEBUG_TRACE_MAX_QUARANTINE_HISTORY: u64 = 20;
pub const MEMORY_DEBUG_TRACE_MAX_REPAIR_JOBS: u64 = 20;
pub const MEMORY_DEBUG_TRACE_MAX_HOOK_RUNS: u64 = 20;
pub const MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS: usize = 240;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDebugEntityKind {
    Memory,
    Candidate,
    Turn,
    HookRun,
    Workspace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDebugLifecycleState {
    Active,
    Deleted,
    Superseded,
    Expired,
    Quarantined,
    CandidatePending,
    CandidateApproved,
    CandidateRejected,
    CandidateSuperseded,
    Missing,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDebugDecisionOutcome {
    Written,
    Updated,
    Duplicate,
    Rejected,
    Deferred,
    Routed,
    Quarantined,
    Suppressed,
    Recalled,
    Missing,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDebugRecallPlannerKind {
    Deterministic,
    Provider,
    Fallback,
    Skipped,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDebugSuppressionReason {
    Deleted,
    Superseded,
    Expired,
    Quarantined,
    WorkspaceMismatch,
    ScopeMismatch,
    SensitivityFiltered,
    QualityPenalty,
    RejectedRelated,
    LowSourceContext,
    Duplicate,
    StaleBackend,
    EmptyContent,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDebugMissingDataKind {
    MemoryRecord,
    CandidateRecord,
    QualityDecision,
    CandidateScore,
    SourceContext,
    RecallTrace,
    HookRun,
    QuarantineState,
    RepairJob,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugTraceTarget {
    pub kind: MemoryDebugEntityKind,
    pub id: Option<String>,
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
}

impl MemoryDebugTraceTarget {
    pub fn memory(memory_id: impl Into<String>) -> Self {
        Self {
            kind: MemoryDebugEntityKind::Memory,
            id: Some(memory_id.into()),
            workspace_id: None,
            thread_id: None,
            turn_id: None,
        }
    }

    pub fn candidate(candidate_id: impl Into<String>) -> Self {
        Self {
            kind: MemoryDebugEntityKind::Candidate,
            id: Some(candidate_id.into()),
            workspace_id: None,
            thread_id: None,
            turn_id: None,
        }
    }

    pub fn hook_run(hook_run_id: impl Into<String>) -> Self {
        Self {
            kind: MemoryDebugEntityKind::HookRun,
            id: Some(hook_run_id.into()),
            workspace_id: None,
            thread_id: None,
            turn_id: None,
        }
    }

    pub fn turn(turn_id: impl Into<String>, workspace_id: Option<String>) -> Self {
        let turn_id = turn_id.into();
        Self {
            kind: MemoryDebugEntityKind::Turn,
            id: Some(turn_id.clone()),
            workspace_id,
            thread_id: None,
            turn_id: Some(turn_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugMissingData {
    pub kind: MemoryDebugMissingDataKind,
    pub reason: String,
}

impl MemoryDebugMissingData {
    pub fn new(kind: MemoryDebugMissingDataKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: bounded_debug_text(reason.into().as_str(), MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryDebugItemSummary {
    pub id: String,
    pub lifecycle_state: MemoryDebugLifecycleState,
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub active_key: Option<String>,
    pub sensitivity: Option<MemorySensitivity>,
    pub confidence: f32,
    pub importance: f32,
    pub source_context_kind: Option<MemorySourceContextKind>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
    pub content_preview: Option<String>,
    pub created_at_unix: i64,
    pub updated_at_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugQualityTrace {
    pub id: String,
    pub action: MemoryQualityAction,
    pub target_ownership: MemoryOwnershipClass,
    pub source_context_kind: MemorySourceContextKind,
    pub fact_class: MemoryFactClass,
    pub lifetime_class: MemoryLifetimeClass,
    pub ownership_class: MemoryOwnershipClass,
    pub evidence_class: MemoryEvidenceClass,
    pub relation: MemoryWriteRelation,
    pub canonical_key: Option<String>,
    pub reason_codes: Vec<MemoryQualityReasonCode>,
    pub memory_id: Option<String>,
    pub candidate_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryDebugScoreTrace {
    pub score_version: Option<String>,
    pub total_score: Option<f32>,
    pub bucket: Option<String>,
    pub components: BTreeMap<String, f32>,
    pub reasons: Vec<String>,
    pub missing: bool,
}

impl MemoryDebugScoreTrace {
    pub fn missing() -> Self {
        Self {
            score_version: None,
            total_score: None,
            bucket: None,
            components: BTreeMap::new(),
            reasons: Vec::new(),
            missing: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugEventTrace {
    pub event_id: String,
    pub event_kind: String,
    pub memory_id: Option<String>,
    pub candidate_id: Option<String>,
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub details_preview: Option<String>,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugQuarantineTrace {
    pub id: String,
    pub memory_id: String,
    pub workspace_id: Option<String>,
    pub reason_code: MemoryLifecycleReasonCode,
    pub actor_kind: String,
    pub actor_id: Option<String>,
    pub created_at_unix: i64,
    pub resolved_at_unix: Option<i64>,
    pub resolved_reason_code: Option<MemoryLifecycleReasonCode>,
    pub resolved_actor_kind: Option<String>,
    pub resolved_actor_id: Option<String>,
    pub details_preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugRepairTrace {
    pub id: String,
    pub job_kind: String,
    pub status: String,
    pub memory_id: Option<String>,
    pub capsule_id: Option<String>,
    pub attempts: i64,
    pub max_attempts: i64,
    pub scheduled_at_unix: i64,
    pub completed_at_unix: Option<i64>,
    pub last_error_preview: Option<String>,
    pub result_preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugSourceContextTrace {
    pub source_context_kind: Option<MemorySourceContextKind>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_item_id: Option<String>,
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryDebugWriteTrace {
    pub outcome: MemoryDebugDecisionOutcome,
    pub relation: Option<MemoryWriteRelation>,
    pub semantic_route: Option<MemorySemanticWriteRoute>,
    pub latest_quality: Option<MemoryDebugQualityTrace>,
    pub score: Option<MemoryDebugScoreTrace>,
    pub source_context: Option<MemoryDebugSourceContextTrace>,
    pub events: Vec<MemoryDebugEventTrace>,
    pub reason: Option<String>,
}

impl Default for MemoryDebugWriteTrace {
    fn default() -> Self {
        Self {
            outcome: MemoryDebugDecisionOutcome::Missing,
            relation: None,
            semantic_route: None,
            latest_quality: None,
            score: None,
            source_context: None,
            events: Vec::new(),
            reason: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryDebugRecallModeTrace {
    pub mode: String,
    pub hit_count: usize,
    pub skipped_reason: Option<String>,
    pub truncated: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryDebugRecallTrace {
    pub planner_kind: MemoryDebugRecallPlannerKind,
    pub planner_status: Option<String>,
    pub planner_reason: Option<String>,
    pub provider_used: Option<bool>,
    pub provider_fallback_used: Option<bool>,
    pub deterministic_sufficient: Option<bool>,
    pub selected_modes: Vec<String>,
    pub dropped_modes: Vec<String>,
    pub mode_traces: Vec<MemoryDebugRecallModeTrace>,
    pub suppression_counts: BTreeMap<MemoryDebugSuppressionReason, usize>,
    pub suppressed_ids: Vec<String>,
    pub synthesized_count: Option<usize>,
    pub prompt_contribution_chars: Option<usize>,
    pub diagnostics: Vec<MemoryDebugDiagnosticPreview>,
}

impl Default for MemoryDebugRecallTrace {
    fn default() -> Self {
        Self {
            planner_kind: MemoryDebugRecallPlannerKind::Unknown,
            planner_status: None,
            planner_reason: None,
            provider_used: None,
            provider_fallback_used: None,
            deterministic_sufficient: None,
            selected_modes: Vec::new(),
            dropped_modes: Vec::new(),
            mode_traces: Vec::new(),
            suppression_counts: BTreeMap::new(),
            suppressed_ids: Vec::new(),
            synthesized_count: None,
            prompt_contribution_chars: None,
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugDiagnosticPreview {
    pub code: String,
    pub message: String,
    pub severity: HookDiagnosticSeverity,
    pub safe_for_user: bool,
    pub redacted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryDebugTrace {
    pub target: MemoryDebugTraceTarget,
    pub found: bool,
    pub lifecycle_state: MemoryDebugLifecycleState,
    pub item: Option<MemoryDebugItemSummary>,
    pub write: Option<MemoryDebugWriteTrace>,
    pub recall: Option<MemoryDebugRecallTrace>,
    pub quarantine_history: Vec<MemoryDebugQuarantineTrace>,
    pub repair_jobs: Vec<MemoryDebugRepairTrace>,
    pub missing: Vec<MemoryDebugMissingData>,
}

impl MemoryDebugTrace {
    pub fn missing(target: MemoryDebugTraceTarget, kind: MemoryDebugMissingDataKind) -> Self {
        Self {
            target,
            found: false,
            lifecycle_state: MemoryDebugLifecycleState::Missing,
            item: None,
            write: None,
            recall: None,
            quarantine_history: Vec::new(),
            repair_jobs: Vec::new(),
            missing: vec![MemoryDebugMissingData::new(kind, "target was not found")],
        }
    }

    pub fn developer_report(&self) -> String {
        format_memory_debug_trace(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryDebugInventoryItem {
    pub field: &'static str,
    pub available: bool,
    pub source: &'static str,
    pub gap: Option<&'static str>,
}

pub fn memory_debug_inventory() -> Vec<MemoryDebugInventoryItem> {
    vec![
        inventory(
            "quality_decisions",
            true,
            "agent_memory_quality_decision",
            None,
        ),
        inventory(
            "score_components",
            true,
            "candidate metadata candidate_score",
            None,
        ),
        inventory(
            "source_context",
            true,
            "agent_memory/source_context_kind",
            None,
        ),
        inventory(
            "recall_plans",
            true,
            "hook_run diagnostics/audit events",
            None,
        ),
        inventory(
            "recall_modes",
            true,
            "hook_run diagnostics/audit events",
            None,
        ),
        inventory(
            "suppressed_ids",
            true,
            "recall debug audit events",
            Some("only available for hooks that emit memory debug audit contributions"),
        ),
        inventory("quarantine_state", true, "agent_memory_quarantine", None),
        inventory("repair_jobs", true, "agent_memory_repair_job", None),
    ]
}

fn inventory(
    field: &'static str,
    available: bool,
    source: &'static str,
    gap: Option<&'static str>,
) -> MemoryDebugInventoryItem {
    MemoryDebugInventoryItem {
        field,
        available,
        source,
        gap,
    }
}

pub(crate) fn memory_debug_item_from_record(
    record: &AgentMemoryControlRecord,
    quarantined: bool,
) -> MemoryDebugItemSummary {
    MemoryDebugItemSummary {
        id: record.id.clone(),
        lifecycle_state: memory_lifecycle_state(record, quarantined),
        scope: record.scope.clone(),
        category: record.category,
        key: record.key.clone(),
        active_key: record.active_key.clone(),
        sensitivity: Some(record.sensitivity),
        confidence: record.confidence as f32,
        importance: record.importance as f32,
        source_context_kind: record.source_context_kind,
        source_thread_id: record.source_thread_id.clone(),
        source_turn_id: record.source_turn_id.clone(),
        source_item_id: record.source_item_id.clone(),
        content_preview: record
            .content_preview
            .as_deref()
            .map(|value| bounded_debug_text(value, MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS)),
        created_at_unix: record.created_at_unix,
        updated_at_unix: Some(record.updated_at_unix),
    }
}

pub(crate) fn memory_debug_item_from_candidate(
    candidate: &AgentMemoryCandidateRecord,
) -> MemoryDebugItemSummary {
    MemoryDebugItemSummary {
        id: candidate.id.clone(),
        lifecycle_state: candidate_lifecycle_state(candidate.status),
        scope: candidate.scope.clone(),
        category: candidate.category,
        key: candidate.key.clone(),
        active_key: candidate.key.clone(),
        sensitivity: None,
        confidence: candidate.confidence as f32,
        importance: 0.0,
        source_context_kind: candidate.source_context_kind,
        source_thread_id: candidate.source_thread_id.clone(),
        source_turn_id: candidate.source_turn_id.clone(),
        source_item_id: candidate.source_item_id.clone(),
        content_preview: Some(bounded_debug_text(
            candidate.candidate_text.as_str(),
            MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS,
        )),
        created_at_unix: candidate.created_at_unix,
        updated_at_unix: candidate.decided_at_unix,
    }
}

pub(crate) fn memory_debug_quality_trace(
    decision: &AgentMemoryQualityDecisionRecord,
) -> MemoryDebugQualityTrace {
    MemoryDebugQualityTrace {
        id: decision.id.clone(),
        action: decision.action,
        target_ownership: decision.target_ownership,
        source_context_kind: decision.source_context_kind,
        fact_class: decision.fact_class,
        lifetime_class: decision.lifetime_class,
        ownership_class: decision.ownership_class,
        evidence_class: decision.evidence_class,
        relation: decision.relation,
        canonical_key: decision.canonical_key.clone(),
        reason_codes: decision.reason_codes.clone(),
        memory_id: decision.memory_id.clone(),
        candidate_id: decision.candidate_id.clone(),
        thread_id: decision.thread_id.clone(),
        turn_id: decision.turn_id.clone(),
        item_id: decision.item_id.clone(),
        created_at_unix: decision.created_at_unix,
    }
}

pub(crate) fn memory_debug_event_trace(event: &AgentMemoryEventRecord) -> MemoryDebugEventTrace {
    MemoryDebugEventTrace {
        event_id: event.id.clone(),
        event_kind: event.event_kind.clone(),
        memory_id: event.memory_id.clone(),
        candidate_id: event.candidate_id.clone(),
        workspace_id: event.workspace_id.clone(),
        thread_id: event.thread_id.clone(),
        turn_id: event.turn_id.clone(),
        item_id: event.item_id.clone(),
        details_preview: event
            .details_json
            .as_deref()
            .map(|value| bounded_debug_text(value, MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS)),
        created_at_unix: event.created_at_unix,
    }
}

pub(crate) fn memory_debug_quarantine_trace(
    quarantine: &AgentMemoryQuarantineRecord,
) -> MemoryDebugQuarantineTrace {
    MemoryDebugQuarantineTrace {
        id: quarantine.id.clone(),
        memory_id: quarantine.memory_id.clone(),
        workspace_id: quarantine.workspace_id.clone(),
        reason_code: quarantine.reason_code,
        actor_kind: format!("{:?}", quarantine.actor.kind),
        actor_id: quarantine.actor.id.clone(),
        created_at_unix: quarantine.created_at_unix,
        resolved_at_unix: quarantine.resolved_at_unix,
        resolved_reason_code: quarantine.resolved_reason_code,
        resolved_actor_kind: quarantine
            .resolved_actor
            .as_ref()
            .map(|actor| format!("{:?}", actor.kind)),
        resolved_actor_id: quarantine
            .resolved_actor
            .as_ref()
            .and_then(|actor| actor.id.clone()),
        details_preview: quarantine
            .details_json
            .as_deref()
            .map(|value| bounded_debug_text(value, MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS)),
    }
}

pub(crate) fn memory_debug_repair_trace(
    job: &AgentMemoryRepairJobRecord,
) -> MemoryDebugRepairTrace {
    MemoryDebugRepairTrace {
        id: job.id.clone(),
        job_kind: job.job_kind.clone(),
        status: job.status.clone(),
        memory_id: job.memory_id.clone(),
        capsule_id: job.capsule_id.clone(),
        attempts: job.attempts,
        max_attempts: job.max_attempts,
        scheduled_at_unix: job.scheduled_at_unix,
        completed_at_unix: job.completed_at_unix,
        last_error_preview: job
            .last_error
            .as_deref()
            .map(|value| bounded_debug_text(value, MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS)),
        result_preview: job
            .result_json
            .as_deref()
            .map(|value| bounded_debug_text(value, MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS)),
    }
}

pub(crate) fn memory_debug_score_from_metadata(
    metadata_json: Option<&str>,
) -> Option<MemoryDebugScoreTrace> {
    let metadata = metadata_json
        .and_then(|json| serde_json::from_str::<BTreeMap<String, Value>>(json).ok())?;
    let score_value = metadata.get("candidate_score").or_else(|| {
        metadata
            .get("candidate_policy")
            .and_then(|value| value.get("score"))
    })?;
    let score = serde_json::from_value::<MemoryCandidateScore>(score_value.clone()).ok()?;
    let mut components = BTreeMap::new();
    components.insert("explicitness".to_owned(), score.explicitness_score);
    components.insert("durability".to_owned(), score.durability_score);
    components.insert("source_trust".to_owned(), score.source_trust_score);
    components.insert("fact_class".to_owned(), score.fact_class_score);
    components.insert("lifetime_fit".to_owned(), score.lifetime_fit_score);
    components.insert("scope".to_owned(), score.scope_score);
    components.insert("ownership_fit".to_owned(), score.ownership_fit_score);
    components.insert("evidence".to_owned(), score.evidence_score);
    components.insert("certainty".to_owned(), score.certainty_score);
    components.insert("sensitivity".to_owned(), score.sensitivity_score);
    components.insert("relation".to_owned(), score.relation_score);
    components.insert("penalty".to_owned(), score.penalty_score);
    Some(MemoryDebugScoreTrace {
        score_version: Some(score.score_version),
        total_score: Some(score.total_score),
        bucket: Some(format!("{:?}", score.bucket)),
        components,
        reasons: score
            .reasons
            .into_iter()
            .map(|reason| bounded_debug_text(reason.as_str(), MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS))
            .collect(),
        missing: false,
    })
}

pub(crate) fn memory_debug_source_context_from_record(
    record: &AgentMemoryControlRecord,
) -> MemoryDebugSourceContextTrace {
    MemoryDebugSourceContextTrace {
        source_context_kind: record.source_context_kind,
        source_thread_id: record.source_thread_id.clone(),
        source_turn_id: record.source_turn_id.clone(),
        source_item_id: record.source_item_id.clone(),
        workspace_id: record.workspace_id.clone(),
    }
}

pub(crate) fn memory_debug_source_context_from_candidate(
    candidate: &AgentMemoryCandidateRecord,
) -> MemoryDebugSourceContextTrace {
    MemoryDebugSourceContextTrace {
        source_context_kind: candidate.source_context_kind,
        source_thread_id: candidate.source_thread_id.clone(),
        source_turn_id: candidate.source_turn_id.clone(),
        source_item_id: candidate.source_item_id.clone(),
        workspace_id: candidate.workspace_id.clone(),
    }
}

pub(crate) fn memory_debug_recall_trace_from_hook_run(
    run: &HookRunRecord,
    audit_events: &[pioneer_crud::HookAuditEventRecord],
) -> MemoryDebugRecallTrace {
    let mut trace = MemoryDebugRecallTrace {
        planner_kind: planner_kind_from_hook_run(run),
        planner_status: Some(format!("{:?}", run.status)),
        diagnostics: run
            .diagnostic_previews
            .iter()
            .map(|diagnostic| MemoryDebugDiagnosticPreview {
                code: diagnostic.code.as_str().to_owned(),
                message: bounded_debug_text(
                    diagnostic.message.as_str(),
                    MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS,
                ),
                severity: diagnostic.severity,
                safe_for_user: diagnostic.safe_for_user,
                redacted: diagnostic.redacted,
            })
            .collect(),
        ..MemoryDebugRecallTrace::default()
    };
    trace.selected_modes = diagnostic_value_list(&trace.diagnostics, "executed_modes");
    trace.dropped_modes = diagnostic_value_list(&trace.diagnostics, "skipped_modes");
    trace.deterministic_sufficient =
        diagnostic_bool(&trace.diagnostics, "deterministic_sufficient");
    for event in audit_events {
        apply_recall_audit_event(&mut trace, event);
    }
    trace
}

pub(crate) fn memory_debug_recall_trace_from_hook_runs(
    runs: &[HookRunRecord],
    audit_events_by_run: &BTreeMap<String, Vec<pioneer_crud::HookAuditEventRecord>>,
) -> MemoryDebugRecallTrace {
    let Some(run) = runs
        .iter()
        .find(|run| is_memory_recall_hook_id(run.hook_id.as_str()))
    else {
        return MemoryDebugRecallTrace::default();
    };
    memory_debug_recall_trace_from_hook_run(
        run,
        audit_events_by_run
            .get(run.id.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]),
    )
}

pub(crate) fn write_outcome_for_memory(
    record: &AgentMemoryControlRecord,
    latest_quality: Option<&AgentMemoryQualityDecisionRecord>,
    active_quarantine: bool,
    events: &[AgentMemoryEventRecord],
) -> MemoryDebugDecisionOutcome {
    if active_quarantine {
        return MemoryDebugDecisionOutcome::Quarantined;
    }
    if matches!(record.status, MemoryStatus::Deleted | MemoryStatus::Expired) {
        return MemoryDebugDecisionOutcome::Suppressed;
    }
    if matches!(record.status, MemoryStatus::Superseded) {
        return MemoryDebugDecisionOutcome::Updated;
    }
    if let Some(decision) = latest_quality {
        return outcome_from_quality_action(decision.action, decision.relation);
    }
    if events.iter().any(|event| event.event_kind == "updated") {
        MemoryDebugDecisionOutcome::Updated
    } else {
        MemoryDebugDecisionOutcome::Written
    }
}

pub(crate) fn write_outcome_for_candidate(
    candidate: &AgentMemoryCandidateRecord,
    latest_quality: Option<&AgentMemoryQualityDecisionRecord>,
) -> MemoryDebugDecisionOutcome {
    match candidate.status {
        MemoryCandidateStatus::Approved => MemoryDebugDecisionOutcome::Written,
        MemoryCandidateStatus::Rejected
        | MemoryCandidateStatus::AutoRejected
        | MemoryCandidateStatus::ReviewDisabledRejected => MemoryDebugDecisionOutcome::Rejected,
        MemoryCandidateStatus::Superseded | MemoryCandidateStatus::MergedDuplicate => {
            MemoryDebugDecisionOutcome::Duplicate
        }
        MemoryCandidateStatus::Expired => MemoryDebugDecisionOutcome::Suppressed,
        _ => latest_quality
            .map(|quality| outcome_from_quality_action(quality.action, quality.relation))
            .unwrap_or(MemoryDebugDecisionOutcome::Deferred),
    }
}

pub(crate) fn write_outcome_for_quality_decision(
    decision: &AgentMemoryQualityDecisionRecord,
) -> MemoryDebugDecisionOutcome {
    outcome_from_quality_action(decision.action, decision.relation)
}

pub(crate) fn semantic_route_from_quality(
    decision: Option<&AgentMemoryQualityDecisionRecord>,
) -> Option<MemorySemanticWriteRoute> {
    let decision = decision?;
    Some(match decision.action {
        MemoryQualityAction::CandidatePolicy => match decision.target_ownership {
            MemoryOwnershipClass::DurableUserMemory
            | MemoryOwnershipClass::DurableWorkspaceMemory
            | MemoryOwnershipClass::DurableAgentMemory => {
                MemorySemanticWriteRoute::DurableControlPlane
            }
            MemoryOwnershipClass::ThreadEpisodicContext => {
                MemorySemanticWriteRoute::ThreadEpisodicDeferred
            }
            MemoryOwnershipClass::TaskRuntimeState => MemorySemanticWriteRoute::TaskStateDeferred,
            MemoryOwnershipClass::DomainRuntimeState => {
                MemorySemanticWriteRoute::DomainStateDeferred
            }
            MemoryOwnershipClass::AuditOnly => MemorySemanticWriteRoute::AuditOnly,
            MemoryOwnershipClass::Reject => MemorySemanticWriteRoute::Rejected,
        },
        MemoryQualityAction::ForceReject => MemorySemanticWriteRoute::Rejected,
        MemoryQualityAction::Quarantine => MemorySemanticWriteRoute::AuditOnly,
        MemoryQualityAction::RouteToThreadEpisodic => {
            MemorySemanticWriteRoute::ThreadEpisodicDeferred
        }
        MemoryQualityAction::RouteToTaskState => MemorySemanticWriteRoute::TaskStateDeferred,
        MemoryQualityAction::RouteToDomainState => MemorySemanticWriteRoute::DomainStateDeferred,
    })
}

pub(crate) fn bounded_debug_text(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let limit = max_chars.max(3);
    if normalized.chars().count() <= limit {
        normalized
    } else {
        normalized
            .chars()
            .take(limit.saturating_sub(3))
            .chain("...".chars())
            .collect()
    }
}

pub fn format_memory_debug_trace(trace: &MemoryDebugTrace) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "Memory Debug Trace: {:?} {}",
        trace.target.kind,
        trace.target.id.as_deref().unwrap_or("<none>")
    ));
    lines.push(format!("found: {}", trace.found));
    lines.push(format!("lifecycle: {:?}", trace.lifecycle_state));
    if let Some(item) = &trace.item {
        lines.push(format!("scope: {:?}:{}", item.scope.kind, item.scope.key));
        lines.push(format!("category: {:?}", item.category));
        if let Some(key) = &item.key {
            lines.push(format!("key: {key}"));
        }
        if let Some(source_context_kind) = item.source_context_kind {
            lines.push(format!("source_context: {:?}", source_context_kind));
        }
    }
    if let Some(write) = &trace.write {
        lines.push(format!("write_outcome: {:?}", write.outcome));
        if let Some(quality) = &write.latest_quality {
            lines.push(format!("quality_action: {:?}", quality.action));
            lines.push(format!("quality_target: {:?}", quality.target_ownership));
            lines.push(format!("quality_reasons: {:?}", quality.reason_codes));
        }
        if let Some(score) = &write.score
            && let Some(total) = score.total_score
        {
            lines.push(format!("candidate_score: {:.3}", total));
        }
    }
    if let Some(recall) = &trace.recall {
        lines.push(format!("recall_planner: {:?}", recall.planner_kind));
        if !recall.selected_modes.is_empty() {
            lines.push(format!(
                "selected_modes: {}",
                recall.selected_modes.join(",")
            ));
        }
        if !recall.suppression_counts.is_empty() {
            lines.push(format!(
                "suppression_counts: {:?}",
                recall.suppression_counts
            ));
        }
    }
    if !trace.quarantine_history.is_empty() {
        lines.push(format!(
            "quarantine_events: {}",
            trace.quarantine_history.len()
        ));
    }
    if !trace.repair_jobs.is_empty() {
        lines.push(format!("repair_jobs: {}", trace.repair_jobs.len()));
    }
    if !trace.missing.is_empty() {
        lines.push(format!(
            "missing: {}",
            trace
                .missing
                .iter()
                .map(|missing| format!("{:?}:{}", missing.kind, missing.reason))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.join("\n")
}

fn memory_lifecycle_state(
    record: &AgentMemoryControlRecord,
    quarantined: bool,
) -> MemoryDebugLifecycleState {
    if quarantined {
        return MemoryDebugLifecycleState::Quarantined;
    }
    match record.status {
        MemoryStatus::Active => MemoryDebugLifecycleState::Active,
        MemoryStatus::Deleted => MemoryDebugLifecycleState::Deleted,
        MemoryStatus::Superseded => MemoryDebugLifecycleState::Superseded,
        MemoryStatus::Expired => MemoryDebugLifecycleState::Expired,
    }
}

fn candidate_lifecycle_state(status: MemoryCandidateStatus) -> MemoryDebugLifecycleState {
    match status {
        MemoryCandidateStatus::Approved => MemoryDebugLifecycleState::CandidateApproved,
        MemoryCandidateStatus::Rejected
        | MemoryCandidateStatus::AutoRejected
        | MemoryCandidateStatus::ReviewDisabledRejected => {
            MemoryDebugLifecycleState::CandidateRejected
        }
        MemoryCandidateStatus::Superseded | MemoryCandidateStatus::MergedDuplicate => {
            MemoryDebugLifecycleState::CandidateSuperseded
        }
        _ => MemoryDebugLifecycleState::CandidatePending,
    }
}

fn outcome_from_quality_action(
    action: MemoryQualityAction,
    relation: MemoryWriteRelation,
) -> MemoryDebugDecisionOutcome {
    match action {
        MemoryQualityAction::CandidatePolicy => match relation {
            MemoryWriteRelation::Duplicate => MemoryDebugDecisionOutcome::Duplicate,
            MemoryWriteRelation::CompatibleUpdate => MemoryDebugDecisionOutcome::Updated,
            MemoryWriteRelation::SuppressedByRejection => MemoryDebugDecisionOutcome::Suppressed,
            MemoryWriteRelation::Contradiction => MemoryDebugDecisionOutcome::Deferred,
            MemoryWriteRelation::Novel => MemoryDebugDecisionOutcome::Written,
        },
        MemoryQualityAction::ForceReject => MemoryDebugDecisionOutcome::Rejected,
        MemoryQualityAction::Quarantine => MemoryDebugDecisionOutcome::Quarantined,
        MemoryQualityAction::RouteToThreadEpisodic
        | MemoryQualityAction::RouteToTaskState
        | MemoryQualityAction::RouteToDomainState => MemoryDebugDecisionOutcome::Routed,
    }
}

fn planner_kind_from_hook_run(run: &HookRunRecord) -> MemoryDebugRecallPlannerKind {
    if run.hook_id.as_str().contains("active") {
        if run
            .diagnostic_previews
            .iter()
            .any(|diagnostic| diagnostic.code.as_str().contains("provider"))
        {
            MemoryDebugRecallPlannerKind::Provider
        } else {
            MemoryDebugRecallPlannerKind::Deterministic
        }
    } else if run.hook_id.as_str().contains("deterministic") {
        MemoryDebugRecallPlannerKind::Deterministic
    } else if run.status == HookRunStatus::Skipped {
        MemoryDebugRecallPlannerKind::Skipped
    } else {
        MemoryDebugRecallPlannerKind::Unknown
    }
}

fn is_memory_recall_hook_id(hook_id: &str) -> bool {
    hook_id.contains("memory.deterministic_recall") || hook_id.contains("memory.active_recall")
}

fn diagnostic_value_list(diagnostics: &[MemoryDebugDiagnosticPreview], key: &str) -> Vec<String> {
    diagnostics
        .iter()
        .find_map(|diagnostic| {
            diagnostic
                .message
                .split_whitespace()
                .find_map(|token| token.strip_prefix(format!("{key}=").as_str()))
        })
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn diagnostic_bool(diagnostics: &[MemoryDebugDiagnosticPreview], key: &str) -> Option<bool> {
    diagnostics.iter().find_map(|diagnostic| {
        diagnostic.message.split_whitespace().find_map(|token| {
            let value = token.strip_prefix(format!("{key}=").as_str())?;
            match value {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            }
        })
    })
}

fn apply_recall_audit_event(
    trace: &mut MemoryDebugRecallTrace,
    event: &pioneer_crud::HookAuditEventRecord,
) {
    if !event.event_kind.as_str().starts_with("memory.recall.") {
        return;
    }
    let details = hook_value_to_json(&event.details);
    if let Some(kind) = details.get("planner_kind").and_then(Value::as_str) {
        trace.planner_kind = match kind {
            "deterministic" => MemoryDebugRecallPlannerKind::Deterministic,
            "provider" => MemoryDebugRecallPlannerKind::Provider,
            "fallback" => MemoryDebugRecallPlannerKind::Fallback,
            "skipped" => MemoryDebugRecallPlannerKind::Skipped,
            _ => MemoryDebugRecallPlannerKind::Unknown,
        };
    }
    trace.planner_status = details
        .get("planner_status")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| trace.planner_status.clone());
    trace.planner_reason = details
        .get("planner_reason")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| trace.planner_reason.clone());
    trace.provider_used = details
        .get("provider_used")
        .and_then(Value::as_bool)
        .or(trace.provider_used);
    trace.provider_fallback_used = details
        .get("provider_fallback_used")
        .and_then(Value::as_bool)
        .or(trace.provider_fallback_used);
    trace.deterministic_sufficient = details
        .get("deterministic_sufficient")
        .and_then(Value::as_bool)
        .or(trace.deterministic_sufficient);
    append_string_array(&mut trace.selected_modes, details.get("selected_modes"));
    append_string_array(&mut trace.dropped_modes, details.get("dropped_modes"));
    append_string_array(&mut trace.suppressed_ids, details.get("suppressed_ids"));
    if let Some(count) = details.get("synthesized_count").and_then(Value::as_u64) {
        trace.synthesized_count = Some(count as usize);
    }
    if let Some(chars) = details
        .get("prompt_contribution_chars")
        .and_then(Value::as_u64)
    {
        trace.prompt_contribution_chars = Some(chars as usize);
    }
    if let Some(modes) = details.get("modes").and_then(Value::as_array) {
        for mode in modes {
            let Some(mode_name) = mode.get("mode").and_then(Value::as_str) else {
                continue;
            };
            trace.mode_traces.push(MemoryDebugRecallModeTrace {
                mode: mode_name.to_owned(),
                hit_count: mode.get("hit_count").and_then(Value::as_u64).unwrap_or(0) as usize,
                skipped_reason: mode
                    .get("skipped_reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                truncated: mode.get("truncated").and_then(Value::as_bool),
            });
        }
    }
    if let Some(suppression_counts) = details.get("suppression_counts").and_then(Value::as_object) {
        for (reason, count) in suppression_counts {
            let reason = suppression_reason_from_str(reason);
            let count = count.as_u64().unwrap_or(0) as usize;
            if count > 0 {
                *trace.suppression_counts.entry(reason).or_insert(0) += count;
            }
        }
    }
}

fn append_string_array(target: &mut Vec<String>, value: Option<&Value>) {
    if let Some(values) = value.and_then(Value::as_array) {
        for value in values {
            if let Some(value) = value.as_str()
                && !target.iter().any(|existing| existing == value)
            {
                target.push(value.to_owned());
            }
        }
    }
}

fn suppression_reason_from_str(value: &str) -> MemoryDebugSuppressionReason {
    match value {
        "deleted" => MemoryDebugSuppressionReason::Deleted,
        "superseded" => MemoryDebugSuppressionReason::Superseded,
        "expired" => MemoryDebugSuppressionReason::Expired,
        "quarantined" => MemoryDebugSuppressionReason::Quarantined,
        "workspace_mismatch" => MemoryDebugSuppressionReason::WorkspaceMismatch,
        "scope_mismatch" => MemoryDebugSuppressionReason::ScopeMismatch,
        "sensitivity_filtered" => MemoryDebugSuppressionReason::SensitivityFiltered,
        "quality_penalty" | "quality_penalty_applied" => {
            MemoryDebugSuppressionReason::QualityPenalty
        }
        "rejected_related" => MemoryDebugSuppressionReason::RejectedRelated,
        "low_source_context" => MemoryDebugSuppressionReason::LowSourceContext,
        "duplicate" => MemoryDebugSuppressionReason::Duplicate,
        "stale_backend" => MemoryDebugSuppressionReason::StaleBackend,
        "empty_content" => MemoryDebugSuppressionReason::EmptyContent,
        _ => MemoryDebugSuppressionReason::Unknown,
    }
}

fn hook_value_to_json(value: &pioneer_hooks::HookValue) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

#[allow(dead_code)]
pub(crate) fn memory_recall_hook_phase() -> HookPhase {
    HookPhase::TurnPrePromptContext
}
