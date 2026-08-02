mod admission;
mod domain;
#[cfg(test)]
mod epic5_contract;
mod execution;
mod invalidation;
mod registry;
mod resolver;
mod service;

pub(crate) use admission::{
    AuthorizationExternalError, external_error_for_decision, record_authorization_unavailable,
    record_binary_decision, record_method_decision, record_method_decision_for_action,
    record_private_self_improvement_source_rejection, record_stale_policy_revision,
    record_subscription_evictions, record_task_notification_decision, record_task_tool_decision,
    record_thread_notification_decision, record_tool_decision,
    record_workspace_notification_decision,
};
pub(crate) use domain::{
    ActionGateDecision, AllowReason, ArtifactResourceId, AuthorizationDecision,
    AuthorizationResource, CapabilityKind, CapabilityResourceId, DenyReason, DisclosurePolicy,
    ResourceAction, ResourceIdError, TaskResourceId, ThreadAccessClass, ThreadResourceId,
    TurnResourceId, WorkspaceResourceId,
};
pub(crate) use execution::{
    ExecutionAuthorizationAdmission, ExecutionAuthorizationContext,
    RevalidatedExecutionAuthorization, RuntimeDraftCreator, RuntimeDraftMaterialization,
    ensure_contextless_execution_is_trusted,
};
pub(crate) use invalidation::{AccessChangeKind, AccessChangeSignal, AuthorizationInvalidationHub};
pub(crate) use registry::{
    BinaryAuthorizationEntry, BinaryIngressKind, MethodAuthorizationEntry, RegistryLookupError,
    ResourceResolverKind, binary_ingress_entry, normal_method_entry,
};
pub(crate) use resolver::{
    AuthorizationResolver, AuthorizedArtifact, AuthorizedInvitation,
    AuthorizedInvitationCollection, AuthorizedInvitationGrants, AuthorizedMemberDirectory,
    AuthorizedMemberPrincipal, AuthorizedSession, AuthorizedTask,
    AuthorizedThread, AuthorizedTurn, AuthorizedWorkspace, AuthorizedWorkspaceCollection,
    ProofResolution, persisted_actor_is_current,
};
#[cfg(test)]
pub(crate) use resolver::AuthorizedMemberAvatar;
pub(crate) use service::{
    AuthorizationService, ResolvedResourceAccess, ThreadAccessFacts, WorkspaceAccessFacts,
};
