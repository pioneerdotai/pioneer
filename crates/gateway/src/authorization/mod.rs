mod admission;
mod agent_action_service;
#[cfg(test)]
mod agent_authored_operations;
mod agent_execution;
mod agent_facts;
mod agent_liveness;
mod agent_route_management;
mod agent_route_service;
mod agent_runtime_adapter;
#[cfg(test)]
mod catalog;
mod domain;
#[cfg(test)]
mod epic5_contract;
mod execution;
mod governor;
mod invalidation;
mod lease;
mod registry;
mod resolver;
mod role_registry;
mod service;
mod snapshot;

pub(crate) use admission::{
    AuthorizationExternalError, external_error_for_decision, record_authorization_unavailable,
    record_binary_decision, record_method_decision, record_method_decision_for_action,
    record_private_self_improvement_source_rejection, record_stale_policy_revision,
    record_subscription_evictions, record_task_notification_decision, record_task_tool_decision,
    record_thread_notification_decision, record_tool_decision,
    record_workspace_notification_decision,
};
pub(crate) use agent_action_service::{
    AgentActionCommitPlan, AgentActionCommitProjection, AgentActionKindName,
    AgentActionServiceError, CanonicalAgentActionService, PreparedAgentAction,
};
pub(crate) use agent_execution::{
    ChildAgentLaunchGrant, ExecutionAttemptState, ExecutionMaterializationError,
    MaterializedChildAgentStart, RootExecutionBinding, RunningPermit,
    materialize_child_agent_start, resolve_agent_launch_selection,
    resolve_ephemeral_agent_launch_selection,
};
pub(crate) use agent_facts::{
    AgentAuthorizationFacts, AgentRouteFacts, AgentSecurityEnvelope, AgentStartFacts,
    AgentWorkResourcePolicy, project_bounded_start_options, validate_agent_start_facts,
};
pub(crate) use agent_liveness::{
    ExecutionLivenessAdapter, ExecutionLivenessDecision, ExecutionObservation,
};
pub(crate) use agent_route_management::AgentRouteManagementService;
pub(crate) use agent_route_service::{
    RouteAuthorizationRequest, authorize_route, safe_route_receipt,
};
pub(crate) use agent_runtime_adapter::{
    AgentExecutionPersistenceFacts, AgentToolAdapterError, BoundAgentActionAdapter,
    derive_task_agent_authorization_grant_seed, materialize_child_agent_action_binding,
    materialize_persisted_selected_task_agent_action_binding,
    materialize_persisted_task_agent_action_binding,
    materialize_selected_task_agent_action_binding,
};
pub(crate) use domain::{
    ActionGateDecision, AgentsDocumentResourceId, AllowReason, ArtifactResourceId,
    AuthorizationDecision, AuthorizationResource, CapabilityKind, CapabilityResourceId, DenyReason,
    DisclosurePolicy, ResourceAction, ResourceIdError, TaskResourceId, ThreadAccessClass,
    ThreadResourceId, TurnResourceId, WorkspaceResourceId, execution_child_policy_action,
};
pub(crate) use execution::{
    ExecutionAdmissionEntryPoint, ExecutionAdmissionRequest, ExecutionAdmissionService,
    ExecutionAuthorizationAdmission, ExecutionAuthorizationContext, ExecutionContinuityPolicy,
    ExecutionResourceBoundary, RevalidatedExecutionAuthorization, RootAgentRouteGrant,
    RuntimeDraftCreator, RuntimeDraftMaterialization,
    execution_grant_capabilities_with_agent_skills,
};
pub(crate) use governor::{
    ExecutionAdmissionGovernor, ObservationAdmissionGovernor, ObservationAdmissionPermit,
};
pub(crate) use invalidation::{
    AccessChangeKind, AccessChangeSignal, AuthorizationInvalidationHub, observed_policy_generation,
};
pub(crate) use lease::{ExecutionLeaseGuard, ExecutionLeaseRegistry};
pub(crate) use registry::{
    BinaryAuthorizationEntry, BinaryIngressKind, MethodAuthorizationEntry, RegistryLookupError,
    ResourceResolverKind, binary_ingress_entry, normal_method_entry,
};
#[cfg(test)]
pub(crate) use resolver::AuthorizedMemberAvatar;
pub(crate) use resolver::{
    AuthorizationResolver, AuthorizedAgentsDocument, AuthorizedArtifact, AuthorizedCapability,
    AuthorizedInvitation, AuthorizedInvitationCollection, AuthorizedInvitationGrants,
    AuthorizedMemberDirectory, AuthorizedMemberPrincipal, AuthorizedSession, AuthorizedTask,
    AuthorizedThread, AuthorizedTurn, AuthorizedWorkspace, AuthorizedWorkspaceCollection,
    CapabilityThreadFacts, ProofResolution, persisted_actor_is_current,
};
pub(crate) use role_registry::{
    ObservationResourcePolicy, RoleDefinitionRegistry, RoleDisclosurePolicy, RoleResourcePolicy,
    RuntimePrincipalPolicy,
};
pub(crate) use service::{
    AuthorizationService, CliThreadForkExportFacts, CliThreadForkExportProjection,
    ResolvedResourceAccess, ThreadAccessFacts, ThreadResourceClass, WorkspaceAccessFacts,
};
pub(crate) use snapshot::AuthorizationCapabilitySnapshotService;
