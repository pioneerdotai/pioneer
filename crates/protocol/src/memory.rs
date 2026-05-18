use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScopeKind {
    User,
    Workspace,
    Thread,
    Agent,
    Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryScope {
    pub kind: MemoryScopeKind,
    pub key: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCategory {
    Identity,
    Preference,
    Biography,
    Relationship,
    RecurringInstruction,
    ProjectPolicy,
    ProjectFact,
    ProjectDecision,
    Procedure,
    Todo,
    Constraint,
    CommunicationStyle,
    Custom,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStatus {
    Active,
    Superseded,
    Deleted,
    Expired,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySensitivity {
    Normal,
    Personal,
    SecretLike,
    Regulated,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySourceKind {
    ExplicitUserRequest,
    UserCorrection,
    AssistantInference,
    BackgroundExtractor,
    ToolObservation,
    Import,
    System,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySourceContextKind {
    DirectUserConversation,
    AssistantResponse,
    ToolResult,
    TaskRuntime,
    SystemRuntime,
    DeveloperInstruction,
    ConnectorContent,
    ImportedDocument,
    GeneratedSummary,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEvidenceActorRole {
    User,
    Assistant,
    Tool,
    Task,
    System,
    Developer,
    Connector,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryFactClass {
    UserIdentity,
    UserBiography,
    UserRelationship,
    StableUserPreference,
    CommunicationPreference,
    RecurringUserInstruction,
    ProjectPolicy,
    ProjectDecision,
    ProjectProcedure,
    ProjectConstraint,
    TaskLifecycleState,
    OperationalObservation,
    ThreadLocalState,
    ToolResultFact,
    AssistantSelfDescription,
    GeneratedSummaryFact,
    DomainOwnedState,
    SecretOrCredential,
    RegulatedSensitiveFact,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryLifetimeClass {
    LongLived,
    ProjectLifetime,
    TaskLifetime,
    ThreadLifetime,
    SessionOnly,
    NaturallyExpiring,
    Instantaneous,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryOwnershipClass {
    DurableUserMemory,
    DurableWorkspaceMemory,
    DurableAgentMemory,
    ThreadEpisodicContext,
    TaskRuntimeState,
    DomainRuntimeState,
    AuditOnly,
    Reject,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEvidenceClass {
    DirectUserAssertion,
    UserCorrection,
    UserApproval,
    AssistantInference,
    ToolObservation,
    TaskRuntimeObservation,
    SystemObservation,
    GeneratedSummary,
    MissingOrWeak,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryQualityAction {
    CandidatePolicy,
    ForceReject,
    Quarantine,
    RouteToThreadEpisodic,
    RouteToTaskState,
    RouteToDomainState,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryQualityReasonCode {
    SourceEligible,
    SourceIneligible,
    DirectUserEvidence,
    WeakEvidence,
    DurableLifetime,
    NonDurableLifetime,
    OwnershipFit,
    OwnershipMismatch,
    CategoryEligible,
    CategoryRestricted,
    SensitivityAllowed,
    SensitivityRestricted,
    Duplicate,
    Contradiction,
    MemoryWriteDisabledForTurn,
    SecretOrCredential,
    RegulatedSensitiveWithoutUserApproval,
    SystemOwnedStateNotMemory,
    SourceNotAuthoritativeForDurableMemory,
    TaskStateNotUserMemory,
    ToolResultNotUserMemory,
    AssistantInferenceNotDurableEvidence,
    WeakOrMissingEvidence,
    DuplicateExistingMemory,
    RouteThreadEpisodic,
    RouteTaskState,
    RouteDomainState,
    TaskLifetime,
    ToolOwnedState,
    GeneratedSummaryNotDurableMemory,
    CandidatePolicyAllowed,
    DurableUserIdentity,
    DurableUserProfile,
    DurableUserPreference,
    DurableRecurringInstruction,
    DurableProjectMemory,
    UserConfirmedAgentMemory,
    CompatibleUpdate,
    ContradictsExistingMemory,
    RequiresResolution,
    NovelCandidate,
    UnknownSourceContext,
    UnknownFactClass,
    UnknownLifetime,
    SourcePolicyMissing,
    NoQualityAllowRule,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryQualityDecision {
    pub action: MemoryQualityAction,
    pub target_ownership: MemoryOwnershipClass,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reason_codes: Vec<MemoryQualityReasonCode>,
    #[serde(default)]
    pub candidate_auto_approve_allowed: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryActorKind {
    User,
    Assistant,
    Extractor,
    System,
    Tool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryActor {
    pub kind: MemoryActorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryProvenance {
    pub source_kind: MemorySourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryIntent {
    ExplicitStore,
    ExplicitForget,
    ExplicitNoMemory,
    ImplicitCandidate,
    None,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryExplicitness {
    Explicit,
    Implicit,
    None,
    Unclear,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySubject {
    CurrentUser,
    CurrentAgent,
    Workspace,
    Project,
    Person,
    Organization,
    Artifact,
    Custom,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryAttribute {
    Name,
    Birthday,
    PreferredLanguage,
    CommunicationStyle,
    MigrationPolicy,
    ReviewStyle,
    PhaseNaming,
    Custom,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScopeHint {
    UserGlobal,
    UserWorkspace,
    AgentGlobal,
    AgentWorkspace,
    ProjectWorkspace,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDurability {
    LongLived,
    ProjectLifetime,
    SessionOnly,
    Transient,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySensitivityHint {
    None,
    Low,
    Personal,
    Regulated,
    Secret,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryExtractorCertainty {
    High,
    Medium,
    Low,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryAttributeCardinality {
    SingleValue,
    MultiValue,
    SetMembership,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryWriteRelation {
    Duplicate,
    CompatibleUpdate,
    Contradiction,
    Novel,
    SuppressedByRejection,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySemanticWriteDisposition {
    AcceptActive,
    CreatePendingCandidate,
    RejectSuppressed,
    RouteToCandidatePolicy,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryWriteEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_or_span: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extractor_reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemorySemanticFields {
    pub intent: MemoryIntent,
    pub explicitness: MemoryExplicitness,
    pub category: MemoryCategory,
    pub subject: MemorySubject,
    pub attribute: MemoryAttribute,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_attribute: Option<String>,
    pub scope_hint: MemoryScopeHint,
    pub durability: MemoryDurability,
    pub sensitivity: MemorySensitivityHint,
    pub certainty: MemoryExtractorCertainty,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCanonicalKey {
    pub key: String,
    pub scope: MemoryScope,
    pub namespace: String,
    pub category: MemoryCategory,
    pub cardinality: MemoryAttributeCardinality,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemorySemanticWriteParams {
    pub scope: MemoryScope,
    pub semantic: MemorySemanticFields,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<MemoryWriteEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<MemoryProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_context_kind: Option<MemorySourceContextKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposition: Option<MemorySemanticWriteDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_provided_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub importance: Option<f32>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemorySemanticWriteResponse {
    pub relation: MemoryWriteRelation,
    pub canonical_key: MemoryCanonicalKey,
    pub semantic_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<MemoryRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<MemoryCandidate>,
    #[serde(default)]
    pub created: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_memory_id: Option<String>,
    #[serde(default)]
    pub evidence_merged: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryRecord {
    pub id: String,
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub category: MemoryCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub content: String,
    pub status: MemoryStatus,
    pub confidence: f32,
    pub importance: f32,
    pub sensitivity: MemorySensitivity,
    pub provenance: MemoryProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_context_kind: Option<MemorySourceContextKind>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_accessed_at: Option<i64>,
    #[serde(default)]
    pub access_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_reason: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemorySearchParams {
    pub query: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<MemoryScope>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<MemoryCategory>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<MemoryStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub include_provenance: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemorySearchHit {
    pub record: MemoryRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matched_terms: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemorySearchResponse {
    #[serde(default)]
    pub hits: Vec<MemorySearchHit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryGetParams {
    pub memory_id: String,
    #[serde(default)]
    pub include_deleted: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryGetResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<MemoryRecord>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryListParams {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<MemoryScope>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<MemoryCategory>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<MemoryStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryListResponse {
    #[serde(default)]
    pub records: Vec<MemoryRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryRememberParams {
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensitivity: Option<MemorySensitivity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub importance: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<MemoryProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryRememberResponse {
    pub record: MemoryRecord,
    #[serde(default)]
    pub created: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_memory_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MemoryForgetTarget {
    #[serde(rename_all = "snake_case")]
    Id { memory_id: String },
    #[serde(rename_all = "snake_case")]
    ScopedKey {
        scope: MemoryScope,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
        key: String,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryForgetParams {
    pub target: MemoryForgetTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryForgetResponse {
    #[serde(default)]
    pub forgotten_memory_ids: Vec<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidateStatus {
    Pending,
    PendingSilent,
    AskOnUse,
    NeedsReview,
    Approved,
    Rejected,
    AutoRejected,
    ReviewDisabledRejected,
    Superseded,
    MergedDuplicate,
    Expired,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidatePolicyDecision {
    AutoApprove,
    PendingSilent,
    AskOnUse,
    NeedsReview,
    AutoReject,
    RejectReviewDisabled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidateScoreBucket {
    High,
    Middle,
    ExtremelyLow,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidateScore {
    pub total_score: f32,
    pub bucket: MemoryCandidateScoreBucket,
    pub explicitness_score: f32,
    pub durability_score: f32,
    pub scope_score: f32,
    pub evidence_score: f32,
    pub certainty_score: f32,
    pub sensitivity_score: f32,
    pub relation_score: f32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScopeClarity {
    Clear,
    Inferred,
    Unclear,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatePolicyInput {
    pub semantic: MemorySemanticFields,
    pub relation: MemoryWriteRelation,
    pub scope: MemoryScope,
    pub scope_clarity: MemoryScopeClarity,
    pub evidence_count: u32,
    pub has_contradiction: bool,
    pub has_duplicate: bool,
    pub has_rejected_duplicate: bool,
    pub sensitivity: MemorySensitivity,
    pub active_no_memory_policy: bool,
    pub source_kind: MemorySourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_run_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatePolicyOutput {
    pub input: MemoryCandidatePolicyInput,
    pub score: MemoryCandidateScore,
    pub decision: MemoryCandidatePolicyDecision,
    pub status: MemoryCandidateStatus,
    pub reason_code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidate {
    pub id: String,
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub candidate_text: String,
    pub confidence: f32,
    pub reason: String,
    pub provenance: MemoryProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_context_kind: Option<MemorySourceContextKind>,
    pub status: MemoryCandidateStatus,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_reason: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidateDecision {
    Approve,
    Reject,
    Expire,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryCandidatesListParams {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<MemoryScope>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<MemoryCategory>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<MemoryCandidateStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCandidatesGetParams {
    pub candidate_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryCandidatesGetResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<MemoryCandidate>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
pub struct MemoryCandidatesListResponse {
    #[serde(default)]
    pub candidates: Vec<MemoryCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCandidatesDecideParams {
    pub candidate_id: String,
    pub decision: MemoryCandidateDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatesDecideResponse {
    pub candidate: MemoryCandidate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<MemoryRecord>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCandidatesApproveParams {
    pub candidate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatesApproveResponse {
    pub candidate: MemoryCandidate,
    pub record: MemoryRecord,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCandidatesRejectParams {
    pub candidate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatesRejectResponse {
    pub candidate: MemoryCandidate,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCandidatesEditAndApproveParams {
    pub candidate_id: String,
    pub edited_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatesEditAndApproveResponse {
    pub candidate: MemoryCandidate,
    pub record: MemoryRecord,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCandidatesMergeParams {
    pub candidate_id: String,
    pub target_candidate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatesMergeResponse {
    pub candidate: MemoryCandidate,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryCandidatesSuppressSimilarParams {
    pub candidate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<MemoryActor>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidatesSuppressSimilarResponse {
    pub candidate: MemoryCandidate,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryChangeKind {
    Created,
    Updated,
    Superseded,
    Deleted,
    Restored,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryChangedNotification {
    pub memory_id: String,
    pub scope: MemoryScope,
    pub change_kind: MemoryChangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<MemoryRecord>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct MemoryCandidateCreatedNotification {
    pub candidate: MemoryCandidate,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MemoryForgottenNotification {
    #[serde(default)]
    pub memory_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        MemoryAttribute, MemoryCandidateDecision, MemoryCandidatePolicyDecision,
        MemoryCandidateScore, MemoryCandidateScoreBucket, MemoryCandidateStatus, MemoryCategory,
        MemoryDurability, MemoryEvidenceActorRole, MemoryEvidenceClass, MemoryExplicitness,
        MemoryExtractorCertainty, MemoryFactClass, MemoryForgetTarget, MemoryIntent,
        MemoryLifetimeClass, MemoryOwnershipClass, MemoryQualityAction, MemoryQualityDecision,
        MemoryQualityReasonCode, MemoryRememberParams, MemoryScope, MemoryScopeHint,
        MemoryScopeKind, MemorySearchParams, MemorySemanticFields, MemorySemanticWriteDisposition,
        MemorySemanticWriteParams, MemorySensitivityHint, MemorySourceContextKind, MemorySubject,
        MemoryWriteEvidence, MemoryWriteRelation,
    };
    use crate::constants;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn memory_scope_kind_uses_snake_case() {
        let encoded = serde_json::to_value(MemoryScopeKind::Workspace).expect("scope kind encode");
        assert_eq!(encoded, json!("workspace"));
    }

    #[test]
    fn memory_category_uses_snake_case() {
        let encoded =
            serde_json::to_value(MemoryCategory::ProjectDecision).expect("category encode");
        assert_eq!(encoded, json!("project_decision"));
    }

    #[test]
    fn remember_params_skip_absent_optional_fields() {
        let encoded = serde_json::to_value(MemoryRememberParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            category: MemoryCategory::Identity,
            namespace: None,
            key: None,
            content: "User birthday is September 12.".to_owned(),
            sensitivity: None,
            confidence: None,
            importance: None,
            provenance: None,
            idempotency_key: None,
            supersedes: None,
            metadata: BTreeMap::new(),
        })
        .expect("remember params encode");

        assert_eq!(
            encoded,
            json!({
                "scope": {
                    "kind": "user",
                    "key": "default"
                },
                "category": "identity",
                "content": "User birthday is September 12."
            })
        );
    }

    #[test]
    fn search_params_default_collections_are_empty() {
        let params: MemorySearchParams =
            serde_json::from_value(json!({"query": "birthday"})).expect("search params decode");

        assert_eq!(params.query, "birthday");
        assert!(params.scopes.is_empty());
        assert!(params.categories.is_empty());
        assert!(params.statuses.is_empty());
        assert_eq!(params.limit, None);
        assert_eq!(params.cursor, None);
        assert!(!params.include_provenance);
    }

    #[test]
    fn forget_target_scoped_key_roundtrips() {
        let target = MemoryForgetTarget::ScopedKey {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            namespace: None,
            key: "user.birthday".to_owned(),
        };

        let encoded = serde_json::to_value(&target).expect("forget target encode");
        assert_eq!(
            encoded,
            json!({
                "kind": "scoped_key",
                "scope": {
                    "kind": "user",
                    "key": "default"
                },
                "key": "user.birthday"
            })
        );

        let decoded: MemoryForgetTarget =
            serde_json::from_value(encoded).expect("forget target decode");
        assert_eq!(decoded, target);
    }

    #[test]
    fn candidate_decision_roundtrips() {
        let encoded =
            serde_json::to_value(MemoryCandidateDecision::Approve).expect("decision encode");
        assert_eq!(encoded, json!("approve"));

        let decoded: MemoryCandidateDecision =
            serde_json::from_value(encoded).expect("decision decode");
        assert_eq!(decoded, MemoryCandidateDecision::Approve);
    }

    #[test]
    fn candidate_policy_contract_uses_typed_snake_case_fields() {
        assert_eq!(
            serde_json::to_value(MemoryCandidateStatus::ReviewDisabledRejected)
                .expect("candidate status encode"),
            json!("review_disabled_rejected")
        );
        assert_eq!(
            serde_json::to_value(MemoryCandidatePolicyDecision::RejectReviewDisabled)
                .expect("policy decision encode"),
            json!("reject_review_disabled")
        );
        assert_eq!(
            serde_json::to_value(MemoryCandidateScoreBucket::ExtremelyLow)
                .expect("score bucket encode"),
            json!("extremely_low")
        );

        let score = MemoryCandidateScore {
            total_score: 0.92,
            bucket: MemoryCandidateScoreBucket::High,
            explicitness_score: 0.25,
            durability_score: 0.2,
            scope_score: 0.15,
            evidence_score: 0.1,
            certainty_score: 0.15,
            sensitivity_score: 0.1,
            relation_score: 0.05,
            reasons: vec!["explicit".to_owned()],
        };
        let encoded = serde_json::to_value(score).expect("score encode");
        assert_eq!(encoded["bucket"], json!("high"));
        assert_eq!(encoded["reasons"], json!(["explicit"]));
    }

    #[test]
    fn memory_quality_ontology_uses_typed_snake_case_fields() {
        assert_eq!(
            serde_json::to_value(MemorySourceContextKind::DirectUserConversation)
                .expect("source context kind encode"),
            json!("direct_user_conversation")
        );
        assert_eq!(
            serde_json::to_value(MemoryEvidenceActorRole::Developer)
                .expect("evidence actor role encode"),
            json!("developer")
        );
        assert_eq!(
            serde_json::to_value(MemoryFactClass::StableUserPreference).expect("fact class encode"),
            json!("stable_user_preference")
        );
        assert_eq!(
            serde_json::to_value(MemoryLifetimeClass::NaturallyExpiring)
                .expect("lifetime class encode"),
            json!("naturally_expiring")
        );
        assert_eq!(
            serde_json::to_value(MemoryOwnershipClass::ThreadEpisodicContext)
                .expect("ownership class encode"),
            json!("thread_episodic_context")
        );
        assert_eq!(
            serde_json::to_value(MemoryEvidenceClass::DirectUserAssertion)
                .expect("evidence class encode"),
            json!("direct_user_assertion")
        );
        assert_eq!(
            serde_json::to_value(MemoryQualityReasonCode::OwnershipMismatch)
                .expect("quality reason code encode"),
            json!("ownership_mismatch")
        );
        assert_eq!(
            serde_json::to_value(MemoryQualityAction::CandidatePolicy)
                .expect("quality action encode"),
            json!("candidate_policy")
        );
        let decision = MemoryQualityDecision {
            action: MemoryQualityAction::Quarantine,
            target_ownership: MemoryOwnershipClass::AuditOnly,
            reason_codes: vec![MemoryQualityReasonCode::NoQualityAllowRule],
            candidate_auto_approve_allowed: false,
        };
        let encoded_decision =
            serde_json::to_value(decision).expect("quality decision should encode");
        assert_eq!(encoded_decision["action"], json!("quarantine"));
        assert_eq!(
            encoded_decision["reason_codes"],
            json!(["no_quality_allow_rule"])
        );

        let decoded: MemoryFactClass =
            serde_json::from_value(json!("assistant_self_description")).expect("fact class decode");
        assert_eq!(decoded, MemoryFactClass::AssistantSelfDescription);

        let unknown: MemorySourceContextKind =
            serde_json::from_value(json!("future_source_context")).expect("unknown decode");
        assert_eq!(unknown, MemorySourceContextKind::Unknown);
    }

    #[test]
    fn semantic_write_contract_uses_typed_snake_case_fields() {
        assert_eq!(
            serde_json::to_value(MemoryIntent::ExplicitStore).expect("intent encode"),
            json!("explicit_store")
        );
        assert_eq!(
            serde_json::to_value(MemoryWriteRelation::SuppressedByRejection)
                .expect("relation encode"),
            json!("suppressed_by_rejection")
        );

        let params = MemorySemanticWriteParams {
            scope: MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
            semantic: MemorySemanticFields {
                intent: MemoryIntent::ExplicitStore,
                explicitness: MemoryExplicitness::Explicit,
                category: MemoryCategory::Identity,
                subject: MemorySubject::CurrentUser,
                attribute: MemoryAttribute::Name,
                subject_key: None,
                custom_subject: None,
                custom_attribute: None,
                scope_hint: MemoryScopeHint::UserGlobal,
                durability: MemoryDurability::LongLived,
                sensitivity: MemorySensitivityHint::None,
                certainty: MemoryExtractorCertainty::High,
            },
            content: "Меня зовут Александр.".to_owned(),
            value: Some("Александр".to_owned()),
            evidence: Some(MemoryWriteEvidence {
                source_thread_id: Some("thread_1".to_owned()),
                source_turn_id: Some("turn_1".to_owned()),
                source_item_id: None,
                source_ref: Some("turn:turn_1".to_owned()),
                quote_or_span: Some("Меня зовут Александр.".to_owned()),
                extractor_reason: None,
            }),
            provenance: None,
            source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
            disposition: Some(MemorySemanticWriteDisposition::AcceptActive),
            client_provided_key: Some("llm/freeform/key".to_owned()),
            confidence: Some(0.95),
            importance: Some(0.7),
            metadata: BTreeMap::new(),
        };

        let encoded = serde_json::to_value(params).expect("semantic write encode");
        assert_eq!(encoded["semantic"]["intent"], json!("explicit_store"));
        assert_eq!(encoded["semantic"]["subject"], json!("current_user"));
        assert_eq!(encoded["semantic"]["attribute"], json!("name"));
        assert_eq!(
            encoded["source_context_kind"],
            json!("direct_user_conversation")
        );
        assert_eq!(encoded["disposition"], json!("accept_active"));
        assert_eq!(encoded["client_provided_key"], json!("llm/freeform/key"));
    }

    #[test]
    fn constants_include_memory_methods_and_events() {
        assert_eq!(constants::methods::MEMORY_SEARCH, "memory/search");
        assert_eq!(
            constants::methods::MEMORY_CANDIDATES_DECIDE,
            "memory/candidates/decide"
        );
        assert_eq!(constants::events::MEMORY_CHANGED, "memory/changed");
        assert_eq!(
            constants::events::MEMORY_CANDIDATE_CREATED,
            "memory/candidate_created"
        );
    }

    #[test]
    fn schema_documents_include_memory_contracts() {
        let schema_names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "memory_record.json",
            "memory_search_params.json",
            "memory_search_response.json",
            "memory_remember_params.json",
            "memory_forget_params.json",
            "memory_semantic_fields.json",
            "memory_semantic_write_params.json",
            "memory_semantic_write_response.json",
            "memory_candidate.json",
            "memory_candidate_score.json",
            "memory_source_context_kind.json",
            "memory_evidence_actor_role.json",
            "memory_fact_class.json",
            "memory_lifetime_class.json",
            "memory_ownership_class.json",
            "memory_evidence_class.json",
            "memory_quality_reason_code.json",
            "memory_candidate_policy_decision.json",
            "memory_candidates_decide_params.json",
            "memory_candidates_approve_params.json",
            "memory_changed_notification.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }
    }
}
