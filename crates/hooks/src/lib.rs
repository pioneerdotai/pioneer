mod config;
mod context;
mod contribution;
mod diagnostic;
mod error;
mod handler;
mod id;
mod package;
mod phase;
mod policy;
mod policy_set;
mod prompt_context_set;
mod prompt_section_set;
mod registry;
mod request;
mod runtime;
mod store;
mod subscription;
mod text;
mod tool_bundle_set;
mod value;

pub use config::{
    HookConfigLayer, HookConfigLayerKind, HookPhaseConfig, HookRuntimeConfig,
    HookSubscriptionConfig,
};
pub use context::{HookActor, HookActorKind, HookContext, HookContextMode};
pub use contribution::{
    AuditContribution, BackgroundJobContribution, HookContribution, HookSourceKind, HookSourceRef,
    PolicyContribution, PromptContextContribution, PromptManifestDiagnosticContribution,
    PromptSectionContribution, ToolBundleContribution,
};
pub use diagnostic::{
    HookDiagnostic, HookDiagnosticPreview, HookDiagnosticRedactionPolicy, HookDiagnosticSeverity,
};
pub use error::{HookError, HookRegistryError, HookResult};
pub use handler::{HookCapabilities, HookHandler, HookHandlerDescriptor};
pub use id::{
    HookActorId, HookAgentId, HookAuditEventKind, HookBackgroundJobId, HookCapability,
    HookCompactionId, HookContributionHash, HookContributionId, HookDiagnosticCode, HookDomain,
    HookFeatureFlag, HookFilterKey, HookId, HookIdError, HookKind, HookMetadataKey, HookPolicyKey,
    HookRunAttemptId, HookRunId, HookRunIdempotencyKey, HookRunScopeId, HookSectionId,
    HookSourceId, HookSubscriptionId, HookTaskId, HookThreadId, HookToolBundleId, HookToolName,
    HookTurnId, HookWorkspaceId,
};
pub use package::{HookDefinition, HookPackage, HookRuntimeBuilder};
pub use phase::{HookPhase, ParseHookPhaseError};
pub use policy::{
    HookAwaitPolicy, HookExecutionPolicy, HookFailurePolicy, HookRetryBackoff, HookRetryPolicy,
};
pub use policy_set::{HookPolicyEntry, HookPolicyKeyRef, HookPolicySet};
pub use prompt_context_set::{
    DEFAULT_PROMPT_CONTEXT_MAX_ENTRIES, DEFAULT_PROMPT_CONTEXT_MAX_TOTAL_CHARS,
    HookPromptContextEntry, HookPromptContextLimits, HookPromptContextSet,
};
pub use prompt_section_set::{
    DEFAULT_PROMPT_SECTION_MAX_CHARS_PER_SECTION, DEFAULT_PROMPT_SECTION_MAX_SECTIONS,
    DEFAULT_PROMPT_SECTION_MAX_TOTAL_CHARS, HookPromptSectionEntry, HookPromptSectionLimits,
    HookPromptSectionSet,
};
pub use registry::{HookRegistry, HookSubscriptionRegistry};
pub use request::{
    DEFAULT_POST_TURN_ASSISTANT_TEXT_PREVIEW_MAX_CHARS, DEFAULT_POST_TURN_DOMAIN_EVENT_MAX_COUNT,
    DEFAULT_POST_TURN_DOMAIN_EVENT_MESSAGE_MAX_CHARS, DEFAULT_POST_TURN_ERROR_PREVIEW_MAX_CHARS,
    DEFAULT_POST_TURN_TOOL_EVENT_MAX_COUNT, DEFAULT_POST_TURN_USER_TEXT_PREVIEW_MAX_CHARS,
    DEFAULT_PRE_COMPACTION_EXISTING_SUMMARY_PREVIEW_MAX_CHARS, HookHandlerRequest,
    HookHandlerResponse, HookInput, HookInputKind, HookInputPayload, HookTextPreview,
    TurnPostPreflightPromptContextHookInput, TurnPostTurnDomain, TurnPostTurnDomainEventSummary,
    TurnPostTurnHookInput, TurnPostTurnHookInputLimits, TurnPostTurnStatus,
    TurnPostTurnToolErrorClass, TurnPostTurnToolEventSummary, TurnPostTurnToolOutcomeStatus,
    TurnPostTurnToolStatus, TurnPreCompactionHookInput, TurnPreCompactionHookInputLimits,
    TurnPreCompactionRawTurnRetention, TurnPreCompactionRetentionPolicy,
    TurnPreCompactionSourceKind, TurnPreCompactionSourceRange, TurnPreCompactionSummaryPolicy,
    TurnPreCompactionSummaryStorage, TurnPreCompactionSummaryStrategy,
    TurnPreCompactionTokenBudget, TurnPreCompactionTrigger, TurnPrePolicyHookInput,
    TurnPrePromptCompileHookInput, TurnPrePromptContextHookInput,
    TurnPreToolMaterializationHookInput,
};
pub use runtime::{
    HookAttemptSummary, HookBackgroundDrainSummary, HookBackgroundRunSummary, HookPhaseRequest,
    HookPhaseResponse, HookRecoveredRunSummary, HookRecoveryOptions, HookRecoverySummary,
    HookRunErrorSummary, HookRunStatus, HookRunSummary, HookRuntime, HookRuntimeError,
    HookRuntimeOptions, HookRuntimeResult,
};
pub use store::{
    HOOK_RUN_RESUME_SCHEMA_VERSION, HookAuditEventStoreRecord, HookRecoverableRunRecord,
    HookRecoveryScan, HookRetrySchedule, HookRunAttemptStoreCompletion, HookRunAttemptStoreRecord,
    HookRunInputSnapshot, HookRunResumePayload, HookRunResumeReference, HookRunResumeState,
    HookRunScope, HookRunScopeKind, HookRunStore, HookRunStoreCompletion, HookRunStoreError,
    HookRunStoreRecord, HookRunStoreResult, NewHookAuditEventStoreRecord,
    NewHookRunAttemptStoreRecord, NewHookRunStoreRecord,
};
pub use subscription::{
    HookFilterSet, HookSubscription, HookSubscriptionDependencies, HookSubscriptionVisibility,
};
pub use text::{
    HookDiagnosticMessage, HookPromptContent, HookPromptSectionTitle, HookSourceLabel,
    HookTextError,
};
pub use tool_bundle_set::{HookToolBundleEntry, HookToolBundleSet};
pub use value::{HookMetadata, HookValue};
