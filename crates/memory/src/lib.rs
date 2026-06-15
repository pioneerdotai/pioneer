mod backend;
mod candidate_policy;
mod config;
mod context;
mod convert;
mod debug;
mod extractor_ontology;
pub mod hooks;
mod lifecycle;
mod memvid;
mod ownership_route;
mod policy;
mod quality;
mod quality_gate;
mod ranking;
mod recall;
mod recall_visibility;
mod service;
mod thread_episodic;
mod write;

pub use backend::{
    BackendDeleteRequest, BackendDeleteResult, BackendGetRequest, BackendPayload,
    BackendPutRequest, BackendPutResult, BackendSearchHit, BackendSearchRequest,
    BackendSearchScope, InMemoryMemoryBackend, MemoryBackend, NoopMemoryBackend,
};
pub use config::{
    MemoryCandidatePolicyConfig, MemoryRankingConfig, MemoryReadPolicy, MemoryRecallConfig,
    MemoryServiceConfig,
};
pub use context::{
    MemoryActiveScopes, MemoryOperationContext, MemoryResolvedScopes, MemoryScopePriority,
};
pub use debug::{
    MEMORY_DEBUG_TEXT_PREVIEW_MAX_CHARS, MEMORY_DEBUG_TRACE_MAX_EVENTS,
    MEMORY_DEBUG_TRACE_MAX_HOOK_RUNS, MEMORY_DEBUG_TRACE_MAX_QUALITY_DECISIONS,
    MEMORY_DEBUG_TRACE_MAX_QUARANTINE_HISTORY, MEMORY_DEBUG_TRACE_MAX_REPAIR_JOBS,
    MemoryDebugDecisionOutcome, MemoryDebugDiagnosticPreview, MemoryDebugEntityKind,
    MemoryDebugEventTrace, MemoryDebugItemSummary, MemoryDebugLifecycleState,
    MemoryDebugMissingData, MemoryDebugMissingDataKind, MemoryDebugQualityTrace,
    MemoryDebugQuarantineTrace, MemoryDebugRecallModeTrace, MemoryDebugRecallPlannerKind,
    MemoryDebugRecallTrace, MemoryDebugRepairTrace, MemoryDebugScoreTrace,
    MemoryDebugSourceContextTrace, MemoryDebugSuppressionReason, MemoryDebugTrace,
    MemoryDebugTraceTarget, MemoryDebugWriteTrace, format_memory_debug_trace,
    memory_debug_inventory,
};
pub use lifecycle::{
    MemoryLifecycleInvariantError, MemoryLifecycleSubjectKind, MemoryLifecycleTransition,
    MemoryQuarantineRequest, MemoryQuarantineResponse, MemoryRestoreRequest, MemoryRestoreResponse,
    candidate_transition_is_candidate_scoped, memory_transition_removes_product_visibility,
    memory_transition_restores_product_eligibility, validate_lifecycle_transition,
};
pub use memvid::{MemvidMemoryBackend, MemvidMemoryBackendConfig, memvid_search_request};
pub use policy::{MemoryPolicyDecision, MemoryPolicyEngine};
pub use quality::{
    MemoryOntologyClassification, MemoryQualityAuditItemKind, MemoryQualityAuditRecord,
    MemoryQualityAuditStatus, MemorySourceContextClassification, MemorySourceContextInput,
    audit_memory_candidate_quality, audit_memory_record_quality, classify_memory_source_context,
    classify_semantic_memory_fact, resolve_semantic_write_source_context,
};
pub use recall::{
    MemoryModeRecallParams, MemoryModeRecallResponse, MemoryRecallItem, MemoryRecallMode,
    MemoryRecallParams, MemoryRecallResponse, MemoryRecallTarget,
};
pub use service::MemoryService;
pub use thread_episodic::{
    MemvidThreadEpisodicBackend, PioneerAdaptiveCutoff, PioneerAdaptiveCutoffConfig,
    PioneerAdaptiveCutoffDiagnostics, PioneerAdaptiveCutoffReason,
    ThreadEpisodicAdaptiveRetrievalImplementation, ThreadEpisodicCandidateSuppression,
    ThreadEpisodicCandidateSuppressionReason, ThreadEpisodicExactSourceTarget,
    ThreadEpisodicFilteredSearchCandidates, ThreadEpisodicMemvidBackend,
    ThreadEpisodicMemvidBackendCapabilities, ThreadEpisodicMemvidCapabilityState,
    ThreadEpisodicMemvidError, ThreadEpisodicMemvidFailureKind, ThreadEpisodicMemvidFeatureAudit,
    ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidIndexRequest,
    ThreadEpisodicMemvidSearchHit, ThreadEpisodicMemvidSearchOutput,
    ThreadEpisodicMemvidSearchRequest, ThreadEpisodicMemvidSearchSegment,
    ThreadEpisodicMemvidStats, ThreadEpisodicRankedSearchHit, ThreadEpisodicRankingContext,
    ThreadEpisodicSearchDiagnostics, ThreadEpisodicSearchProfile, ThreadEpisodicSearchProfileKind,
    filter_thread_episodic_search_candidates, rank_thread_episodic_search_hits,
    thread_episodic_memvid_metadata, thread_episodic_storage_uri_from_path,
};
pub use write::{build_memory_canonical_key, semantic_memory_fingerprint};
