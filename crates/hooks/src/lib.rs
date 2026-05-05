mod context;
mod contribution;
mod diagnostic;
mod error;
mod handler;
mod id;
mod phase;
mod policy;
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
pub use diagnostic::{HookDiagnostic, HookDiagnosticSeverity};
pub use error::{HookError, HookRegistryError, HookResult};
pub use handler::{HookCapabilities, HookHandler, HookHandlerDescriptor};
pub use id::{
    HookActorId, HookAgentId, HookAuditEventKind, HookCapability, HookContributionId,
    HookDiagnosticCode, HookDomain, HookFeatureFlag, HookFilterKey, HookId, HookIdError, HookKind,
    HookMetadataKey, HookPolicyKey, HookRunId, HookSectionId, HookSourceId, HookSubscriptionId,
    HookTaskId, HookThreadId, HookTurnId, HookWorkspaceId,
};
pub use phase::{HookPhase, ParseHookPhaseError};
pub use policy::{
    HookAwaitPolicy, HookExecutionPolicy, HookFailurePolicy, HookRetryBackoff, HookRetryPolicy,
};
pub use registry::{HookRegistry, HookSubscriptionRegistry};
pub use request::{HookHandlerRequest, HookHandlerResponse, HookInput, HookInputKind};
pub use runtime::{
    HookPhaseRequest, HookPhaseResponse, HookRunErrorSummary, HookRunStatus, HookRunSummary,
    HookRuntime, HookRuntimeError, HookRuntimeOptions, HookRuntimeResult,
};
pub use subscription::{
    HookFilterSet, HookSubscription, HookSubscriptionDependencies, HookSubscriptionVisibility,
};
pub use text::{
    HookDiagnosticMessage, HookPromptContent, HookPromptSectionTitle, HookSourceLabel,
    HookTextError,
};
pub use value::{HookMetadata, HookValue};
