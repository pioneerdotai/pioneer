mod context;
mod contribution;
mod diagnostic;
mod error;
mod handler;
mod id;
mod phase;
mod policy;
mod policy_set;
mod prompt_context_set;
mod registry;
mod request;
mod runtime;
mod subscription;
mod text;
mod value;

pub use context::{HookActor, HookActorKind, HookContext, HookContextMode};
pub use contribution::{
    AuditContribution, HookContribution, HookSourceKind, HookSourceRef, PolicyContribution,
    PromptContextContribution, PromptManifestDiagnosticContribution, PromptSectionContribution,
};
pub use diagnostic::{
    HookDiagnostic, HookDiagnosticPreview, HookDiagnosticRedactionPolicy, HookDiagnosticSeverity,
};
pub use error::{HookError, HookRegistryError, HookResult};
pub use handler::{HookCapabilities, HookHandler, HookHandlerDescriptor};
pub use id::{
    HookActorId, HookAgentId, HookAuditEventKind, HookCapability, HookContributionHash,
    HookContributionId, HookDiagnosticCode, HookDomain, HookFeatureFlag, HookFilterKey, HookId,
    HookIdError, HookKind, HookMetadataKey, HookPolicyKey, HookRunId, HookSectionId, HookSourceId,
    HookSubscriptionId, HookTaskId, HookThreadId, HookTurnId, HookWorkspaceId,
};
pub use phase::{HookPhase, ParseHookPhaseError};
pub use policy::{
    HookAwaitPolicy, HookExecutionPolicy, HookFailurePolicy, HookRetryBackoff, HookRetryPolicy,
};
pub use policy_set::{HookPolicyEntry, HookPolicyKeyRef, HookPolicySet};
pub use prompt_context_set::{
    DEFAULT_PROMPT_CONTEXT_MAX_ENTRIES, DEFAULT_PROMPT_CONTEXT_MAX_TOTAL_CHARS,
    HookPromptContextEntry, HookPromptContextLimits, HookPromptContextSet,
};
pub use registry::{HookRegistry, HookSubscriptionRegistry};
pub use request::{HookHandlerRequest, HookHandlerResponse, HookInput, HookInputKind};
pub use runtime::{
    HookAttemptSummary, HookPhaseRequest, HookPhaseResponse, HookRunErrorSummary, HookRunStatus,
    HookRunSummary, HookRuntime, HookRuntimeError, HookRuntimeOptions, HookRuntimeResult,
};
pub use subscription::{
    HookFilterSet, HookSubscription, HookSubscriptionDependencies, HookSubscriptionVisibility,
};
pub use text::{
    HookDiagnosticMessage, HookPromptContent, HookPromptSectionTitle, HookSourceLabel,
    HookTextError,
};
pub use value::{HookMetadata, HookValue};
