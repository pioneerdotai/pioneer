mod backend;
mod candidate_policy;
mod config;
mod context;
mod convert;
pub mod hooks;
mod memvid;
mod policy;
mod quality;
mod ranking;
mod recall;
mod service;
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
pub use memvid::{MemvidMemoryBackend, MemvidMemoryBackendConfig, memvid_search_request};
pub use policy::{MemoryPolicyDecision, MemoryPolicyEngine};
pub use quality::{
    MemoryOntologyClassification, MemoryQualityAuditItemKind, MemoryQualityAuditRecord,
    MemoryQualityAuditStatus, MemorySourceContextClassification, MemorySourceContextInput,
    audit_memory_candidate_quality, audit_memory_record_quality, classify_memory_source_context,
    classify_semantic_memory_fact,
};
pub use recall::{MemoryRecallItem, MemoryRecallParams, MemoryRecallResponse};
pub use service::MemoryService;
pub use write::{build_memory_canonical_key, semantic_memory_fingerprint};
