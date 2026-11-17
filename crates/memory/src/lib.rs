mod backend;
mod candidate_policy;
mod config;
mod context;
mod convert;
mod debug;
mod extractor_ontology;
pub mod hooks;
mod lifecycle;
mod manifest_failure;
pub use manifest_failure::{
    MemoryManifestFailure, MemoryManifestFailureClass, MemoryManifestFailureStage,
    is_invalid_stored_memory_data,
};
mod ownership_route;
mod policy;
mod quality;
mod quality_gate;
mod ranking;
mod recall_visibility;
mod service;
mod sqlite;
mod thread_episodic;
mod thread_episodic_embedding;
mod write;
mod write_failure;
pub use write_failure::MemoryWriteFailure;

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
    MemoryActiveScopes, MemoryMutationBoundary, MemoryOperationContext, MemoryResolvedScopes,
    MemoryScopePriority, MemorySourceAccessPolicy,
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
pub use policy::{MemoryPolicyDecision, MemoryPolicyEngine};
pub use quality::{
    MemoryOntologyClassification, MemoryQualityAuditItemKind, MemoryQualityAuditRecord,
    MemoryQualityAuditStatus, MemorySourceContextClassification, MemorySourceContextInput,
    audit_memory_candidate_quality, audit_memory_record_quality, classify_memory_source_context,
    classify_semantic_memory_fact, resolve_semantic_write_source_context,
};
pub use service::MemoryService;
pub use sqlite::SqliteMemoryBackend;
pub use thread_episodic::{
    PioneerAdaptiveCutoff, PioneerAdaptiveCutoffConfig, PioneerAdaptiveCutoffDiagnostics,
    PioneerAdaptiveCutoffReason, SqliteThreadEpisodicBackend, THREAD_EPISODIC_INDEX_BATCH_BYTES,
    THREAD_EPISODIC_INDEX_BATCH_ITEMS, ThreadEpisodicCandidateSuppression,
    ThreadEpisodicCandidateSuppressionReason, ThreadEpisodicFilteredSearchCandidates,
    ThreadEpisodicIndexDocument, ThreadEpisodicIndexEmbedding, ThreadEpisodicRankedSearchHit,
    ThreadEpisodicRetrievalMode, ThreadEpisodicSearchBackend, ThreadEpisodicSearchDiagnostics,
    ThreadEpisodicSearchError, ThreadEpisodicSearchFailureKind, ThreadEpisodicSearchHit,
    ThreadEpisodicSearchOutput, ThreadEpisodicSearchPage, ThreadEpisodicSearchProfile,
    ThreadEpisodicSearchProfileKind, ThreadEpisodicSearchRequest, ThreadEpisodicValidationResult,
    ThreadEpisodicWorkspaceOwnership, filter_thread_episodic_search_candidates,
    lock_thread_episodic_workspace, rank_thread_episodic_search_hits,
    thread_episodic_source_metadata, thread_episodic_workspace_releases,
    try_lock_thread_episodic_workspace,
};
pub use thread_episodic_embedding::{
    ThreadEpisodicEmbeddingDiagnostic, ThreadEpisodicEmbeddingError,
    ThreadEpisodicEmbeddingErrorKind, ThreadEpisodicEmbeddingFailureClass,
    ThreadEpisodicEmbeddingIdentity, ThreadEpisodicEmbeddingProvider,
    ThreadEpisodicEmbeddingSafeLogFields,
};
pub use write::{build_memory_canonical_key, semantic_memory_fingerprint};
