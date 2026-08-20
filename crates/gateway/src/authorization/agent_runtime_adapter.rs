//! Runtime-neutral adapter for model-facing agent actions.
//!
//! Every runtime enters the same adapter with a pinned execution binding.  The
//! adapter derives action/idempotency identifiers from the provider call ID,
//! converts typed model input into an opaque protocol intent, and delegates
//! authorization/resource/commit semantics to `CanonicalAgentActionService`.
//! Provider sessions are intentionally absent from this type.

use super::{
    AgentActionCommitPlan, AgentActionCommitProjection, AgentActionServiceError,
    AgentAuthorizationFacts, AgentRouteFacts, AgentSecurityEnvelope, AgentWorkResourcePolicy,
    CanonicalAgentActionService, PreparedAgentAction, ResourceAction, RoleDefinitionRegistry,
    RootExecutionBinding,
};
use pioneer_protocol::{
    AgentActionId, AgentActionIntent, AgentControlTaskToolInput, AgentCreateThreadToolInput,
    AgentExecutionId, AgentExecutionProfileBackend, AgentExecutionProfileProjection,
    AgentIdentityId, AgentIdentityProjection, AgentIdentitySourceKind, AgentModelToolName,
    AgentPublicOutcome, AgentReviewTaskToolInput, AgentScheduleTaskToolInput,
    AgentSendMessageToolInput, AgentStartToolInput, AgentTaskActionSelection, AgentTaskToolInput,
    AgentThreadAudienceTemplate, AgentThreadCreationOption, AgentToolCapability,
    AgentToolIdentityChoice, AgentToolLaunchSelection, AgentToolOptionsProjection,
    AgentToolProfileChoice, AgentToolResultStatus, AgentToolSafeResult, AgentToolTargetOption,
    AgentToolThreadCreationOption,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AgentToolAdapterError {
    InvalidInput(String),
    Action(AgentActionServiceError),
    OptionsUnavailable,
}

impl From<AgentActionServiceError> for AgentToolAdapterError {
    fn from(error: AgentActionServiceError) -> Self {
        Self::Action(error)
    }
}

/// A single execution-bound entry point shared by API, native and CLI
/// runtimes.  It is deliberately not `Clone`: resource admission state must
/// not be copied into a second adapter.
pub(crate) struct BoundAgentActionAdapter {
    service: CanonicalAgentActionService,
    root: RootExecutionBinding,
    start_options: pioneer_protocol::AgentStartOptionsProjection,
    envelope: AgentSecurityEnvelope,
    routes: Vec<AgentRouteFacts>,
    same_capsule_targets: Vec<(String, String)>,
    policy: AgentWorkResourcePolicy,
    branch_key: String,
    attempt_generation: u64,
    current_policy_generation: u64,
    execution_grant_fingerprint: String,
    execution_grant_policy_generation: u64,
    depth: u16,
}

/// Immutable facts needed to persist the execution graph.  Runtime adapters
/// may hold provider-specific state, but these values are server-derived from
/// the same pinned binding and therefore remain stable across reconnects.
#[derive(Clone, Debug)]
pub(crate) struct AgentExecutionPersistenceFacts {
    pub(crate) execution_id: AgentExecutionId,
    pub(crate) root_execution_id: AgentExecutionId,
    pub(crate) identity: AgentIdentityProjection,
    pub(crate) profile: AgentExecutionProfileProjection,
    pub(crate) identity_source_revision: u64,
    pub(crate) identity_source_fingerprint: String,
    pub(crate) home_root_thread_id: String,
    pub(crate) agent_authorization_fingerprint: String,
    pub(crate) agent_authorization_allowed_actions: Vec<String>,
    pub(crate) agent_authorization_role_key: String,
    pub(crate) agent_authorization_policy_generation: u64,
    pub(crate) execution_generation: u64,
}

pub(crate) fn derive_task_agent_authorization_grant_seed(
    identity_id: AgentIdentityId,
    root_capsule_id: &str,
    role_key: &str,
    policy_generation: u64,
    child_launch_grant: pioneer_protocol::ChildAgentLaunchGrantSet,
) -> Result<pioneer_tasks::TaskAgentAuthorizationGrantSeed, AgentToolAdapterError> {
    let envelope = AgentAuthorizationFacts {
        identity_id,
        identity_status: pioneer_protocol::AgentIdentityStatus::Active,
        role_key: role_key.to_owned(),
        root_capsule_id: root_capsule_id.to_owned(),
        parent_envelope: None,
        policy_generation,
    }
    .derive_envelope(RoleDefinitionRegistry::new())
    .ok_or(AgentToolAdapterError::Action(
        AgentActionServiceError::NotAuthorized("Task agent role is not registered"),
    ))?;
    if envelope.allowed_actions.is_empty() {
        return Err(AgentToolAdapterError::Action(
            AgentActionServiceError::NotAuthorized("Task agent role has no allowed actions"),
        ));
    }
    child_launch_grant.validate().map_err(|_| {
        AgentToolAdapterError::InvalidInput("Task child launch grant is invalid".to_owned())
    })?;
    Ok(task_authorization_grant_seed_from_envelope(
        envelope,
        child_launch_grant,
    ))
}

fn task_authorization_grant_seed_from_envelope(
    envelope: AgentSecurityEnvelope,
    child_launch_grant: pioneer_protocol::ChildAgentLaunchGrantSet,
) -> pioneer_tasks::TaskAgentAuthorizationGrantSeed {
    let allowed_actions = envelope.allowed_action_names();
    pioneer_tasks::TaskAgentAuthorizationGrantSeed {
        role_key: envelope.role_key,
        policy_generation: envelope.policy_generation,
        allowed_actions,
        fingerprint: envelope.fingerprint,
        child_launch_grant,
    }
}

impl BoundAgentActionAdapter {
    pub(crate) fn new(
        service: CanonicalAgentActionService,
        mut root: RootExecutionBinding,
        envelope: AgentSecurityEnvelope,
        route: Option<AgentRouteFacts>,
        policy: AgentWorkResourcePolicy,
        branch_key: impl Into<String>,
        attempt_generation: u64,
        current_policy_generation: u64,
        depth: u16,
    ) -> Result<Self, AgentToolAdapterError> {
        if envelope.fingerprint != root.envelope.fingerprint {
            return Err(AgentToolAdapterError::Action(
                AgentActionServiceError::NotAuthorized("authorization envelope is stale"),
            ));
        }
        let start_options = super::project_bounded_start_options(
            vec![root.identity.clone()],
            vec![root.profile.clone()],
            &envelope,
            current_policy_generation,
        );
        root.options_generation_fingerprint = start_options.generation_fingerprint.clone();
        let execution_grant_fingerprint = envelope.fingerprint.clone();
        let execution_grant_policy_generation = envelope.policy_generation;
        Ok(Self {
            service,
            root,
            start_options,
            envelope,
            routes: route.into_iter().collect(),
            same_capsule_targets: Vec::new(),
            policy,
            branch_key: branch_key.into(),
            attempt_generation,
            current_policy_generation,
            execution_grant_fingerprint,
            execution_grant_policy_generation,
            depth,
        })
    }

    pub(crate) fn execution_id(&self) -> &AgentExecutionId {
        &self.root.execution_id
    }

    pub(crate) fn work_graph_root_execution_id(&self) -> &AgentExecutionId {
        &self.root.work_graph_root_execution_id
    }

    pub(crate) fn policy_fingerprint(&self) -> &str {
        self.envelope.fingerprint.as_str()
    }

    pub(crate) fn current_policy_generation(&self) -> u64 {
        self.current_policy_generation
    }

    /// Replace the launch catalog with a current, server-resolved workspace
    /// projection. The model receives only its safe opaque projection; the
    /// full identity/profile facts remain inside this execution-bound adapter
    /// and are the facts used when a launch selection is materialized.
    pub(crate) fn install_start_options_catalog(
        &mut self,
        identities: Vec<AgentIdentityProjection>,
        profiles: Vec<AgentExecutionProfileProjection>,
        inherit_parent_identity_available: bool,
        derived_ephemeral_available: bool,
        inherit_parent_profile_available: bool,
        allowed_skill_ids: Vec<pioneer_protocol::SkillId>,
        allowed_mcp_server_ids: Vec<String>,
        max_permission_profile: pioneer_protocol::TurnPermissionProfileCap,
    ) -> AgentToolOptionsProjection {
        let mut start_options = super::project_bounded_start_options(
            identities,
            profiles,
            &self.envelope,
            self.current_policy_generation,
        );
        start_options.inherit_parent_agent_available = inherit_parent_identity_available;
        start_options.derived_ephemeral_available = derived_ephemeral_available;
        start_options.inherit_parent_profile_available = inherit_parent_profile_available;
        start_options.allowed_skill_ids = allowed_skill_ids;
        start_options.allowed_mcp_server_ids = allowed_mcp_server_ids;
        start_options.max_permission_profile = max_permission_profile;
        let skill_fingerprint_input =
            serde_json::to_string(&start_options.allowed_skill_ids).unwrap_or_default();
        let mcp_fingerprint_input =
            serde_json::to_string(&start_options.allowed_mcp_server_ids).unwrap_or_default();
        let permission_cap_fingerprint_input =
            serde_json::to_string(&start_options.max_permission_profile).unwrap_or_default();
        let capability_fingerprint = fingerprint(&[
            start_options.generation_fingerprint.as_str(),
            skill_fingerprint_input.as_str(),
            mcp_fingerprint_input.as_str(),
            permission_cap_fingerprint_input.as_str(),
        ]);
        start_options.generation_fingerprint = capability_fingerprint;
        self.root.options_generation_fingerprint = start_options.generation_fingerprint.clone();
        self.start_options = start_options;
        let mut safe = self.start_options.clone().safe_tool_projection();
        let capabilities = task_agent_capabilities(&self.envelope);
        populate_binding_options(
            &self.root,
            self.same_capsule_targets.as_slice(),
            self.routes.as_slice(),
            &capabilities,
            &mut safe,
        );
        safe
    }

    pub(crate) fn resolve_task_launch(
        &self,
        selection: &pioneer_protocol::AgentLaunchSelection,
    ) -> Result<(AgentIdentityProjection, AgentExecutionProfileProjection), AgentToolAdapterError>
    {
        super::resolve_agent_launch_selection(
            selection,
            &self.start_options,
            Some(&self.root.identity),
            Some(&self.root.profile),
        )
        .map_err(|error| {
            AgentToolAdapterError::InvalidInput(format!(
                "task launch selection could not be resolved: {error:?}"
            ))
        })
    }

    pub(crate) fn resolve_ephemeral_task_launch(
        &self,
        selection: &pioneer_protocol::AgentLaunchSelection,
        stable_seed: &str,
    ) -> Result<(AgentIdentityProjection, AgentExecutionProfileProjection), AgentToolAdapterError>
    {
        super::resolve_ephemeral_agent_launch_selection(
            selection,
            stable_seed,
            &self.start_options,
            Some(&self.root.profile),
        )
        .map_err(|error| {
            AgentToolAdapterError::InvalidInput(format!(
                "ephemeral Task launch could not be resolved: {error:?}"
            ))
        })
    }

    pub(crate) fn resolve_named_task_reviewer_launch(
        &self,
        nickname: &str,
    ) -> Result<(AgentIdentityProjection, AgentExecutionProfileProjection), AgentToolAdapterError>
    {
        let nickname = nickname.trim();
        if nickname.is_empty() {
            return Err(AgentToolAdapterError::InvalidInput(
                "reviewer agent nickname is required".to_owned(),
            ));
        }
        let mut identities = self
            .start_options
            .agents
            .iter()
            .filter(|identity| identity.nickname.eq_ignore_ascii_case(nickname));
        let identity = identities.next().cloned().ok_or_else(|| {
            AgentToolAdapterError::InvalidInput(
                "reviewer agent identity is stale or unavailable".to_owned(),
            )
        })?;
        if identities.next().is_some() {
            return Err(AgentToolAdapterError::InvalidInput(
                "reviewer agent nickname is ambiguous".to_owned(),
            ));
        }
        let mut profiles = self.start_options.profiles.iter().filter(|profile| {
            profile
                .compatible_agent_identity_ids
                .iter()
                .any(|candidate| candidate == &identity.id)
        });
        let profile = profiles.next().cloned().ok_or_else(|| {
            AgentToolAdapterError::InvalidInput(
                "reviewer execution profile is stale or unavailable".to_owned(),
            )
        })?;
        if profiles.next().is_some() {
            return Err(AgentToolAdapterError::InvalidInput(
                "reviewer execution profile must be selected exactly".to_owned(),
            ));
        }
        Ok((identity, profile))
    }

    pub(crate) fn bind_persisted_work_graph_root(
        &mut self,
        root_execution_id: &str,
    ) -> Result<(), AgentToolAdapterError> {
        let root_execution_id =
            AgentExecutionId::new(root_execution_id.to_owned()).map_err(|_| {
                AgentToolAdapterError::InvalidInput(
                    "persisted work-graph root has an invalid execution id".to_owned(),
                )
            })?;
        self.root.work_graph_root_execution_id = root_execution_id;
        Ok(())
    }

    /// Attach an exact durable route while restoring an already-admitted
    /// execution for a later side effect such as Task result delivery.  The
    /// route remains subject to the canonical service's current generation,
    /// expiry, disclosure and destination checks at prepare/commit time.
    pub(crate) fn bind_persisted_route(
        &mut self,
        route: AgentRouteFacts,
    ) -> Result<(), AgentToolAdapterError> {
        let source_binding_matches = match route.kind {
            pioneer_protocol::AgentRouteKind::ExecutionBound => {
                route.source_execution_id == self.root.execution_id.as_str()
            }
            pioneer_protocol::AgentRouteKind::IdentityBound => true,
        };
        if !source_binding_matches
            || route.source_identity_id.as_ref() != Some(&self.root.identity.id)
        {
            return Err(AgentToolAdapterError::InvalidInput(
                "persisted route differs from its source binding".to_owned(),
            ));
        }
        self.routes
            .retain(|existing| existing.route_id != route.route_id);
        self.routes.push(route);
        self.routes
            .sort_by(|left, right| left.route_id.cmp(&right.route_id));
        Ok(())
    }

    pub(crate) fn replace_persisted_routes(
        &mut self,
        routes: Vec<AgentRouteFacts>,
    ) -> Result<(), AgentToolAdapterError> {
        self.routes.clear();
        for route in routes {
            self.bind_persisted_route(route)?;
        }
        Ok(())
    }

    pub(crate) fn replace_same_capsule_targets(
        &mut self,
        mut targets: Vec<(String, String)>,
    ) -> Result<(), AgentToolAdapterError> {
        if targets
            .iter()
            .any(|(thread_id, _)| thread_id.trim().is_empty())
        {
            return Err(AgentToolAdapterError::InvalidInput(
                "same-capsule target has an invalid thread binding".to_owned(),
            ));
        }
        targets.retain(|(thread_id, _)| thread_id != &self.root.home_root_thread_id);
        targets.sort_by(|left, right| left.0.cmp(&right.0));
        targets.dedup_by(|left, right| left.0 == right.0);
        self.same_capsule_targets = targets;
        Ok(())
    }

    pub(crate) fn presentation_snapshot(&self) -> pioneer_protocol::AgentPresentationSnapshot {
        pioneer_protocol::AgentPresentationSnapshot {
            agent_identity_id: self.root.identity.id.clone(),
            agent_execution_id: self.root.execution_id.clone(),
            identity_source_kind: self.root.identity.source_kind.clone(),
            identity_source_revision: self.root.identity.source_revision,
            display_name: self.root.identity.display_name.clone(),
            nickname: self.root.identity.nickname.clone(),
            avatar_revision: self.root.identity.avatar_revision.clone(),
            role_label: self.root.identity.role_label.clone(),
        }
    }

    pub(crate) fn persistence_facts(&self) -> AgentExecutionPersistenceFacts {
        AgentExecutionPersistenceFacts {
            execution_id: self.root.execution_id.clone(),
            root_execution_id: self.root.work_graph_root_execution_id.clone(),
            identity: self.root.identity.clone(),
            profile: self.root.profile.clone(),
            identity_source_revision: self.root.identity_source_revision,
            identity_source_fingerprint: self.root.identity_source_fingerprint.clone(),
            home_root_thread_id: self.root.home_root_thread_id.clone(),
            agent_authorization_fingerprint: self.envelope.fingerprint.clone(),
            agent_authorization_allowed_actions: self.envelope.allowed_action_names(),
            agent_authorization_role_key: self.envelope.role_key.clone(),
            agent_authorization_policy_generation: self.envelope.policy_generation,
            execution_generation: self.root.execution_generation,
        }
    }

    pub(crate) fn task_authorization_grant_seed(
        &self,
        prepared: &PreparedAgentAction,
        identity: &AgentIdentityProjection,
        profile: &AgentExecutionProfileProjection,
    ) -> Result<pioneer_tasks::TaskAgentAuthorizationGrantSeed, AgentToolAdapterError> {
        let root_capsule_id = prepared
            .route
            .as_ref()
            .filter(|route| !route.same_capsule)
            .map(|route| route.destination_capsule_id.as_str())
            .unwrap_or(self.root.home_root_thread_id.as_str());
        let envelope = AgentAuthorizationFacts {
            identity_id: identity.id.clone(),
            identity_status: pioneer_protocol::AgentIdentityStatus::Active,
            role_key: self.envelope.role_key.clone(),
            root_capsule_id: root_capsule_id.to_owned(),
            parent_envelope: Some(self.envelope.clone()),
            policy_generation: self.envelope.policy_generation,
        }
        .derive_envelope(RoleDefinitionRegistry::new())
        .ok_or(AgentToolAdapterError::Action(
            AgentActionServiceError::NotAuthorized("Task child agent role is not registered"),
        ))?;
        if envelope.allowed_actions.is_empty() {
            return Err(AgentToolAdapterError::Action(
                AgentActionServiceError::NotAuthorized(
                    "Task child agent has no inherited allowed actions",
                ),
            ));
        }
        let mut identities = self.start_options.agents.clone();
        if !identities.iter().any(|candidate| candidate == identity) {
            identities.push(identity.clone());
        }
        let mut profiles = self.start_options.profiles.clone();
        if !profiles.iter().any(|candidate| candidate == profile) {
            profiles.push(profile.clone());
        }
        let child_launch_grant =
            pioneer_protocol::ChildAgentLaunchGrantSet::new(identities, profiles)
                .and_then(|grant| {
                    grant.with_policy(
                        self.start_options.inherit_parent_agent_available,
                        self.start_options.derived_ephemeral_available,
                        self.start_options.inherit_parent_profile_available,
                        self.start_options.allowed_skill_ids.clone(),
                        self.start_options.allowed_mcp_server_ids.clone(),
                        self.start_options.max_permission_profile.clone(),
                    )
                })
                .map_err(|_| {
                    AgentToolAdapterError::InvalidInput(
                        "Task child launch grant is invalid".to_owned(),
                    )
                })?;
        Ok(task_authorization_grant_seed_from_envelope(
            envelope,
            child_launch_grant,
        ))
    }

    /// Normalize and admit an already-built protocol intent.  This method is
    /// the common boundary used by all runtime adapters.
    pub(crate) fn prepare(
        &mut self,
        intent: &AgentActionIntent,
    ) -> Result<PreparedAgentAction, AgentToolAdapterError> {
        let route = self.route_for_intent(intent)?.cloned();
        let same_capsule_thread_ids = self
            .same_capsule_targets
            .iter()
            .map(|(thread_id, _)| thread_id.clone())
            .collect::<Vec<_>>();
        let mut prepared = self
            .service
            .prepare(
                intent,
                &self.envelope,
                &self.root,
                route.as_ref(),
                same_capsule_thread_ids.as_slice(),
                &self.policy,
                self.branch_key.clone(),
                self.attempt_generation,
                self.current_policy_generation,
                self.depth.saturating_add(1),
            )
            .map_err(AgentToolAdapterError::from)?;
        prepared.execution_grant_fingerprint = self.execution_grant_fingerprint.clone();
        prepared.execution_grant_policy_generation = self.execution_grant_policy_generation;
        Ok(prepared)
    }

    pub(crate) fn require_source_actions(
        &self,
        actions: impl IntoIterator<Item = ResourceAction>,
    ) -> Result<(), AgentToolAdapterError> {
        self.service
            .require_source_actions(&self.envelope, actions)
            .map_err(AgentToolAdapterError::from)
    }

    /// Produce the exact normalized fingerprint used to detect a committed
    /// replay before mutating the live admission hint. No durable state or
    /// live sibling is changed.
    pub(crate) fn preview_for_replay(
        &self,
        intent: &AgentActionIntent,
    ) -> Result<(pioneer_protocol::NormalizedAgentAction, String), AgentToolAdapterError> {
        let normalized = self.service.normalize(intent)?;
        let request_fingerprint = super::agent_action_service::request_fingerprint(&normalized);
        Ok((normalized, request_fingerprint))
    }

    /// Build the immutable persistence write-set for a domain service that
    /// owns a larger transaction (for example Task creation). The caller may
    /// not edit this plan; CRUD revalidates current authorization facts when
    /// it commits the plan beside the domain aggregate.
    pub(crate) fn prepare_commit(
        &self,
        prepared: &PreparedAgentAction,
        response_json: Option<String>,
        policy_fingerprint: &str,
        current_destination_policy_generation: u64,
    ) -> Result<AgentActionCommitPlan, AgentToolAdapterError> {
        self.service
            .prepare_commit(
                prepared,
                response_json,
                policy_fingerprint,
                self.current_policy_generation,
                current_destination_policy_generation,
            )
            .map_err(Into::into)
    }

    /// Resolve the child identity/profile and immutable visible authorship
    /// from the same bound catalog that authorized a typed StartAgent action.
    pub(crate) fn materialize_start_agent(
        &self,
        prepared: &PreparedAgentAction,
        start: &pioneer_protocol::StartAgentIntent,
        options: &AgentToolOptionsProjection,
        controller_principal_id: Option<pioneer_protocol::PrincipalId>,
    ) -> Result<super::MaterializedChildAgentStart, AgentToolAdapterError> {
        let execution_id = prepared.spawned_execution_id.clone().ok_or_else(|| {
            AgentToolAdapterError::InvalidInput(
                "StartAgent action has no server-allocated child execution".to_owned(),
            )
        })?;
        if options.generation_fingerprint != self.start_options.generation_fingerprint {
            return Err(AgentToolAdapterError::InvalidInput(
                "agent launch catalog is stale".to_owned(),
            ));
        }
        let branch_key = match prepared.resource.as_ref() {
            Some(permit) => permit.branch_key.as_str(),
            None => {
                return Err(AgentToolAdapterError::InvalidInput(
                    "StartAgent action has no branch admission".to_owned(),
                ));
            }
        };
        super::materialize_child_agent_start(
            prepared.normalized.action_id.clone(),
            execution_id,
            &self.root,
            start,
            &self.start_options,
            prepared.route.clone(),
            self.depth.saturating_add(1),
            branch_key,
            &self.policy,
            RoleDefinitionRegistry::new(),
            controller_principal_id,
        )
        .map_err(|error| {
            AgentToolAdapterError::InvalidInput(format!(
                "child Agent materialization failed: {error:?}"
            ))
        })
    }

    /// Decode one typed model call and create a protocol intent.  The call ID
    /// is the only provider-specific input retained, and it is converted to a
    /// deterministic opaque action/idempotency pair.
    pub(crate) fn intent_from_model_call(
        &self,
        call_id: &str,
        tool: AgentModelToolName,
        arguments: serde_json::Value,
        options: Option<&AgentToolOptionsProjection>,
    ) -> Result<Option<AgentActionIntent>, AgentToolAdapterError> {
        let action_id = action_id_for_call(call_id);
        let idempotency_key = idempotency_key_for_call(call_id, tool);
        let execution_id = self.root.execution_id.clone();
        let intent = match tool {
            AgentModelToolName::AgentStartOptions => return Ok(None),
            AgentModelToolName::SendMessage => {
                let input: AgentSendMessageToolInput = decode(arguments)?;
                AgentActionIntent::SendMessage {
                    action_id,
                    execution_id,
                    target: self.resolve_target_option(
                        options.ok_or(AgentToolAdapterError::OptionsUnavailable)?,
                        input.target_option_id.as_str(),
                    )?,
                    input: input.input,
                    idempotency_key,
                }
            }
            AgentModelToolName::CreateThread => {
                let input: AgentCreateThreadToolInput = decode(arguments)?;
                let audience = self.resolve_thread_creation_option(
                    options.ok_or(AgentToolAdapterError::OptionsUnavailable)?,
                    input.option_id.as_str(),
                )?;
                AgentActionIntent::CreateThread {
                    action_id,
                    execution_id,
                    option: AgentThreadCreationOption {
                        option_id: input.option_id,
                        audience,
                    },
                    idempotency_key,
                }
            }
            AgentModelToolName::StartAgent => {
                let input: AgentStartToolInput = decode(arguments)?;
                validate_tool_launch(
                    &input.launch,
                    options.ok_or(AgentToolAdapterError::OptionsUnavailable)?,
                )?;
                AgentActionIntent::StartAgent {
                    action_id,
                    execution_id,
                    start: pioneer_protocol::StartAgentIntent {
                        target: self.resolve_target_option(
                            options.ok_or(AgentToolAdapterError::OptionsUnavailable)?,
                            input.target_option_id.as_str(),
                        )?,
                        input: input.input,
                        launch: input.launch.into_server_selection(),
                    },
                    idempotency_key,
                }
            }
            AgentModelToolName::CreateTask => {
                let input: AgentTaskToolInput = decode(arguments)?;
                validate_tool_launch(
                    &input.launch,
                    options.ok_or(AgentToolAdapterError::OptionsUnavailable)?,
                )?;
                let selection = AgentTaskActionSelection {
                    task_template_id: input.task_template_id,
                    launch: input.launch.into_server_selection(),
                };
                AgentActionIntent::CreateTask {
                    action_id,
                    execution_id,
                    target: self.resolve_target_option(
                        options.ok_or(AgentToolAdapterError::OptionsUnavailable)?,
                        input.target_option_id.as_str(),
                    )?,
                    selection,
                    idempotency_key,
                }
            }
            AgentModelToolName::ScheduleTask => {
                let input: AgentScheduleTaskToolInput = decode(arguments)?;
                validate_tool_launch(
                    &input.launch,
                    options.ok_or(AgentToolAdapterError::OptionsUnavailable)?,
                )?;
                AgentActionIntent::ScheduleTask {
                    action_id,
                    execution_id,
                    target: self.resolve_target_option(
                        options.ok_or(AgentToolAdapterError::OptionsUnavailable)?,
                        input.target_option_id.as_str(),
                    )?,
                    selection: AgentTaskActionSelection {
                        task_template_id: input.task_template_id,
                        launch: input.launch.into_server_selection(),
                    },
                    schedule_option_id: input.schedule_option_id,
                    idempotency_key,
                }
            }
            AgentModelToolName::ReviewTask => {
                let input: AgentReviewTaskToolInput = decode(arguments)?;
                AgentActionIntent::ReviewTaskResult {
                    action_id,
                    execution_id,
                    task_id: input.task_id,
                    decision: input.decision,
                    idempotency_key,
                }
            }
            AgentModelToolName::ControlTask => {
                let input: AgentControlTaskToolInput = decode(arguments)?;
                AgentActionIntent::ControlTask {
                    action_id,
                    execution_id,
                    task_id: input.task_id,
                    control: input.control,
                    idempotency_key,
                }
            }
            AgentModelToolName::Wait | AgentModelToolName::Result => {
                // Observation is intentionally typed but not a mutation.  It
                // is handled by the runtime's task observation provider and
                // must never be converted into a fake mutation receipt.
                return Ok(None);
            }
        };
        Ok(Some(intent))
    }

    fn resolve_thread_creation_option(
        &self,
        options: &AgentToolOptionsProjection,
        option_id: &str,
    ) -> Result<AgentThreadAudienceTemplate, AgentToolAdapterError> {
        if !options
            .thread_creation_options
            .iter()
            .any(|option| option.id == option_id)
        {
            return Err(AgentToolAdapterError::InvalidInput(
                "thread creation option is stale or unavailable".to_owned(),
            ));
        }
        for audience in [
            AgentThreadAudienceTemplate::HomeCapsule,
            AgentThreadAudienceTemplate::RootDelegation,
        ] {
            if thread_creation_option_id(&self.root, &audience) == option_id {
                return Ok(audience);
            }
        }
        Err(AgentToolAdapterError::InvalidInput(
            "thread creation option is stale or unavailable".to_owned(),
        ))
    }

    pub(crate) fn current_target_option_id(&self) -> String {
        target_option_id(&self.root, None, "current")
    }

    fn resolve_target_option(
        &self,
        options: &AgentToolOptionsProjection,
        option_id: &str,
    ) -> Result<pioneer_protocol::AgentStartTarget, AgentToolAdapterError> {
        if !options
            .target_options
            .iter()
            .any(|option| option.id == option_id)
        {
            return Err(AgentToolAdapterError::InvalidInput(
                "action target option is stale or unavailable".to_owned(),
            ));
        }
        if self.current_target_option_id() == option_id {
            return Ok(pioneer_protocol::AgentStartTarget::CurrentThread);
        }
        if target_option_id(&self.root, None, "home") == option_id {
            return Ok(pioneer_protocol::AgentStartTarget::SameCapsuleThread {
                thread_id: self.root.home_root_thread_id.clone(),
            });
        }
        if let Some((thread_id, _)) = self.same_capsule_targets.iter().find(|(thread_id, _)| {
            same_capsule_target_option_id(&self.root, thread_id.as_str()) == option_id
        }) {
            return Ok(pioneer_protocol::AgentStartTarget::SameCapsuleThread {
                thread_id: thread_id.clone(),
            });
        }
        if let Some(route) = self
            .routes
            .iter()
            .find(|route| target_option_id(&self.root, Some(route), "route") == option_id)
        {
            return Ok(pioneer_protocol::AgentStartTarget::RoutedThread {
                route_id: pioneer_protocol::AgentDelegationRouteId::new(route.route_id.clone())
                    .map_err(|_| {
                        AgentToolAdapterError::InvalidInput(
                            "projected action route is invalid".to_owned(),
                        )
                    })?,
                thread_id: route.destination_thread_id.clone(),
            });
        }
        Err(AgentToolAdapterError::InvalidInput(
            "action target option is stale or unavailable".to_owned(),
        ))
    }

    fn route_for_intent(
        &self,
        intent: &AgentActionIntent,
    ) -> Result<Option<&AgentRouteFacts>, AgentToolAdapterError> {
        let target = match intent {
            AgentActionIntent::SendMessage { target, .. }
            | AgentActionIntent::CreateTask { target, .. }
            | AgentActionIntent::ScheduleTask { target, .. }
            | AgentActionIntent::DeliverResult { target, .. } => Some(target),
            AgentActionIntent::StartAgent { start, .. } => Some(&start.target),
            AgentActionIntent::CreateThread { .. }
            | AgentActionIntent::ReviewTaskResult { .. }
            | AgentActionIntent::ControlTask { .. } => None,
        };
        let Some(pioneer_protocol::AgentStartTarget::RoutedThread { route_id, .. }) = target else {
            return Ok(None);
        };
        self.routes
            .iter()
            .find(|route| route.route_id == route_id.as_str())
            .map(Some)
            .ok_or_else(|| {
                AgentToolAdapterError::InvalidInput(
                    "action target route is stale or unavailable".to_owned(),
                )
            })
    }

    pub(crate) fn safe_result(projection: &AgentActionCommitProjection) -> AgentToolSafeResult {
        AgentToolSafeResult {
            status: if projection.queued {
                AgentToolResultStatus::Queued
            } else {
                AgentToolResultStatus::Accepted
            },
            outcome: projection
                .queued
                .then_some(AgentPublicOutcome::AgentWorkQueued),
            receipt_id: Some(projection.receipt_id.clone()),
            outbox_id: Some(projection.outbox_id.clone()),
            public_message: None,
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(
    arguments: serde_json::Value,
) -> Result<T, AgentToolAdapterError> {
    serde_json::from_value(arguments)
        .map_err(|error| AgentToolAdapterError::InvalidInput(error.to_string()))
}

fn validate_tool_launch(
    selection: &AgentToolLaunchSelection,
    options: &AgentToolOptionsProjection,
) -> Result<(), AgentToolAdapterError> {
    if selection.skill_ids.iter().collect::<BTreeSet<_>>().len() != selection.skill_ids.len()
        || selection
            .mcp_server_ids
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != selection.mcp_server_ids.len()
    {
        return Err(AgentToolAdapterError::InvalidInput(
            "skill and MCP selections must not contain duplicates".to_owned(),
        ));
    }
    let selected_identity = match &selection.identity {
        AgentToolIdentityChoice::InheritParent if options.inherit_parent_identity_available => None,
        AgentToolIdentityChoice::DefaultPioneer if options.default_pioneer_identity_available => {
            options
                .identities
                .iter()
                .find(|identity| identity.nickname.eq_ignore_ascii_case("pioneer"))
                .map(|identity| &identity.id)
        }
        AgentToolIdentityChoice::Exact { id } if options.contains_identity(id) => Some(id),
        AgentToolIdentityChoice::ServerDerivedEphemeral { .. }
            if options.derived_ephemeral_identity_available =>
        {
            None
        }
        _ => {
            return Err(AgentToolAdapterError::InvalidInput(
                "agent identity option is stale or unavailable".to_owned(),
            ));
        }
    };
    let profile = match &selection.profile {
        AgentToolProfileChoice::InheritParent if options.inherit_parent_profile_available => None,
        AgentToolProfileChoice::Exact(id) => {
            let profile = options
                .profiles
                .iter()
                .find(|profile| &profile.id == id)
                .ok_or_else(|| {
                    AgentToolAdapterError::InvalidInput(
                        "execution profile option is stale or unavailable".to_owned(),
                    )
                })?;
            if selected_identity.is_some_and(|identity| {
                !profile
                    .compatible_identity_ids
                    .iter()
                    .any(|candidate| candidate == identity)
            }) {
                return Err(AgentToolAdapterError::InvalidInput(
                    "execution profile is incompatible with the selected identity".to_owned(),
                ));
            }
            Some(profile)
        }
        _ => {
            return Err(AgentToolAdapterError::InvalidInput(
                "execution profile option is stale or unavailable".to_owned(),
            ));
        }
    };
    if let Some(reasoning) = selection.reasoning.as_ref()
        && profile.is_some_and(|profile| !profile.allowed_reasoning.contains(reasoning))
    {
        return Err(AgentToolAdapterError::InvalidInput(
            "reasoning selection exceeds the projected profile".to_owned(),
        ));
    }
    if let Some(permission) = selection.permission_profile.as_ref()
        && (profile.is_some_and(|profile| {
            !profile
                .allowed_permission_profiles
                .contains(&permission.mode)
        }) || {
            let selected = pioneer_protocol::task_permission_cap_snapshot(
                &pioneer_protocol::task_permission_cap_for_mode(permission.mode),
            );
            let ceiling =
                pioneer_protocol::task_permission_cap_snapshot(&options.max_permission_profile);
            pioneer_protocol::intersect_turn_permission_profiles(
                &selected,
                &ceiling,
                pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
            ) != selected
        })
    {
        return Err(AgentToolAdapterError::InvalidInput(
            "permission profile exceeds the projected profile".to_owned(),
        ));
    }
    if selection
        .skill_ids
        .iter()
        .any(|id| !options.allowed_skill_ids.contains(id))
        || selection
            .mcp_server_ids
            .iter()
            .any(|id| !options.allowed_mcp_server_ids.contains(id))
    {
        return Err(AgentToolAdapterError::InvalidInput(
            "skill or MCP selection exceeds the projected launch grant".to_owned(),
        ));
    }
    Ok(())
}

fn thread_creation_option_id(
    root: &RootExecutionBinding,
    audience: &AgentThreadAudienceTemplate,
) -> String {
    let audience = match audience {
        AgentThreadAudienceTemplate::HomeCapsule => "home_capsule",
        AgentThreadAudienceTemplate::RootDelegation => "root_delegation",
    };
    format!(
        "O{}",
        &fingerprint(&[
            "agent-thread-creation-option",
            root.execution_id.as_str(),
            root.options_generation_fingerprint.as_str(),
            audience,
        ])[..20]
    )
}

fn target_option_id(
    root: &RootExecutionBinding,
    route: Option<&AgentRouteFacts>,
    kind: &str,
) -> String {
    format!(
        "O{}",
        &fingerprint(&[
            "agent-action-target-option",
            root.execution_id.as_str(),
            root.options_generation_fingerprint.as_str(),
            route
                .map(|route| route.grant_fingerprint.as_str())
                .unwrap_or(""),
            kind,
        ])[..20]
    )
}

pub(crate) fn action_id_for_call(call_id: &str) -> AgentActionId {
    let digest = Sha256::digest(call_id.as_bytes());
    let value = format!("A{}", hex::encode(digest)[..20].to_owned());
    AgentActionId::new(value).expect("hashed action id is exactly 21 alphanumeric characters")
}

pub(crate) fn idempotency_key_for_call(call_id: &str, tool: AgentModelToolName) -> String {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:model-tool:v1\0");
    digest.update(tool.as_str().as_bytes());
    digest.update([0]);
    digest.update(call_id.as_bytes());
    hex::encode(digest.finalize())
}

pub(crate) fn materialize_child_agent_action_binding(
    grant: &super::ChildAgentLaunchGrant,
    identity_source_revision: u64,
    identity_source_fingerprint: &str,
    execution_generation: u64,
    policy_generation: u64,
) -> Result<
    (
        BoundAgentActionAdapter,
        AgentToolOptionsProjection,
        BTreeSet<AgentToolCapability>,
    ),
    AgentToolAdapterError,
> {
    let root = RootExecutionBinding {
        execution_id: grant.execution_id.clone(),
        identity: grant.identity.clone(),
        identity_source_revision,
        identity_source_fingerprint: identity_source_fingerprint.to_owned(),
        profile: grant.profile.clone(),
        execution_generation,
        home_root_thread_id: grant.home_root_thread_id.clone(),
        work_graph_root_execution_id: grant.root_execution_id.clone(),
        authorization_context_fingerprint: grant.envelope.fingerprint.clone(),
        envelope: grant.envelope.clone(),
        options_generation_fingerprint: fingerprint(&[
            "agent-child-options",
            grant.execution_id.as_str(),
            grant.root_execution_id.as_str(),
            grant.identity.source_fingerprint.as_str(),
            grant.profile.fingerprint.as_str(),
        ]),
    };
    let capabilities = task_agent_capabilities(&grant.envelope);
    let mut options = super::project_bounded_start_options(
        vec![grant.identity.clone()],
        vec![grant.profile.clone()],
        &grant.envelope,
        policy_generation,
    )
    .safe_tool_projection();
    // `grant.route` authorizes the parent's ingress operation. Once the child
    // is running in the destination capsule it is provenance, not an
    // outgoing bearer grant: its source execution is the parent and cannot
    // authorize child mutations. A separately issued return/outbound route
    // must be bound explicitly when that operation is prepared.
    populate_binding_options(&root, &[], &[], &capabilities, &mut options);
    let adapter = BoundAgentActionAdapter::new(
        CanonicalAgentActionService::default(),
        root,
        grant.envelope.clone(),
        None,
        AgentWorkResourcePolicy::default(),
        grant.branch_key.clone(),
        1,
        policy_generation,
        grant.depth,
    )?;
    Ok((adapter, options, capabilities))
}

/// Rebuild a Task runtime binding from the immutable identity/profile facts
/// already stored with its execution. Recovery must use this path: current
/// Task configuration is not an authority to replace a pinned actor or
/// execution profile after admission.
pub(crate) fn materialize_persisted_task_agent_action_binding(
    execution_id: AgentExecutionId,
    root_capsule_id: &str,
    work_graph_root_execution_id: AgentExecutionId,
    identity: AgentIdentityProjection,
    profile: AgentExecutionProfileProjection,
    execution_generation: u64,
    attempt_generation: u64,
    depth: u16,
    branch_key: &str,
    role_key: &str,
    persisted_policy_generation: u64,
    current_policy_generation: u64,
    agent_authorization_fingerprint: &str,
    allowed_action_names: &[String],
) -> Result<
    (
        BoundAgentActionAdapter,
        AgentToolOptionsProjection,
        BTreeSet<AgentToolCapability>,
    ),
    AgentToolAdapterError,
> {
    if root_capsule_id.trim().is_empty()
        || branch_key.trim().is_empty()
        || role_key.trim().is_empty()
    {
        return Err(AgentToolAdapterError::InvalidInput(
            "persisted task binding requires non-empty root, branch and role facts".to_owned(),
        ));
    }
    if execution_generation == 0
        || attempt_generation == 0
        || identity.source_revision == 0
        || persisted_policy_generation == 0
        || current_policy_generation == 0
    {
        return Err(AgentToolAdapterError::InvalidInput(
            "persisted task binding has an invalid generation".to_owned(),
        ));
    }
    if identity.source_fingerprint.trim().is_empty() || profile.fingerprint.trim().is_empty() {
        return Err(AgentToolAdapterError::InvalidInput(
            "persisted task binding has incomplete immutable fingerprints".to_owned(),
        ));
    }
    if identity.source_kind != AgentIdentitySourceKind::Ephemeral
        && !profile
            .compatible_agent_identity_ids
            .iter()
            .any(|candidate| candidate == &identity.id)
    {
        return Err(AgentToolAdapterError::InvalidInput(
            "persisted task identity and profile are incompatible".to_owned(),
        ));
    }
    match (&identity.source_kind, &profile.backend) {
        (AgentIdentitySourceKind::NativeAgent, AgentExecutionProfileBackend::ApiProvider)
        | (AgentIdentitySourceKind::Ephemeral, _)
        | (
            AgentIdentitySourceKind::CliRuntimeInstance,
            AgentExecutionProfileBackend::CliRuntime { .. },
        ) => {}
        _ => {
            return Err(AgentToolAdapterError::InvalidInput(
                "persisted task identity/backend binding is invalid".to_owned(),
            ));
        }
    }

    let facts = AgentAuthorizationFacts {
        identity_id: identity.id.clone(),
        identity_status: pioneer_protocol::AgentIdentityStatus::Active,
        role_key: role_key.to_owned(),
        root_capsule_id: root_capsule_id.to_owned(),
        parent_envelope: None,
        policy_generation: current_policy_generation,
    };
    let envelope = facts
        .derive_envelope(RoleDefinitionRegistry::new())
        .and_then(|envelope| {
            envelope.constrain_to_persisted_actions(
                allowed_action_names,
                persisted_policy_generation,
                agent_authorization_fingerprint,
            )
        })
        .ok_or(AgentToolAdapterError::Action(
            AgentActionServiceError::NotAuthorized(
                "persisted task agent authorization grant is stale or invalid",
            ),
        ))?;
    let identity_source_revision = identity.source_revision;
    let identity_source_fingerprint = identity.source_fingerprint.clone();
    let options_generation_fingerprint = fingerprint(&[
        "agent-persisted-task-options",
        execution_id.as_str(),
        work_graph_root_execution_id.as_str(),
        identity_source_fingerprint.as_str(),
        profile.fingerprint.as_str(),
        &current_policy_generation.to_string(),
    ]);
    let root = RootExecutionBinding {
        execution_id,
        identity,
        identity_source_revision,
        identity_source_fingerprint,
        profile,
        execution_generation,
        home_root_thread_id: root_capsule_id.to_owned(),
        work_graph_root_execution_id,
        authorization_context_fingerprint: envelope.fingerprint.clone(),
        envelope: envelope.clone(),
        options_generation_fingerprint,
    };
    let capabilities = task_agent_capabilities(&envelope);
    let mut options = super::project_bounded_start_options(
        vec![root.identity.clone()],
        vec![root.profile.clone()],
        &envelope,
        current_policy_generation,
    )
    .safe_tool_projection();
    populate_binding_options(&root, &[], &[], &capabilities, &mut options);
    let mut adapter = BoundAgentActionAdapter::new(
        CanonicalAgentActionService::default(),
        root,
        envelope,
        None,
        AgentWorkResourcePolicy::default(),
        branch_key.to_owned(),
        attempt_generation,
        current_policy_generation,
        depth,
    )?;
    adapter.execution_grant_fingerprint = agent_authorization_fingerprint.to_owned();
    adapter.execution_grant_policy_generation = persisted_policy_generation;
    Ok((adapter, options, capabilities))
}

pub(crate) fn materialize_selected_task_agent_action_binding(
    execution_id: AgentExecutionId,
    root_capsule_id: &str,
    work_graph_root_execution_id: AgentExecutionId,
    agent_spec_id: &str,
    identity: AgentIdentityProjection,
    profile: AgentExecutionProfileProjection,
    execution_generation: u64,
    attempt_generation: u64,
    depth: u16,
    role_key: &str,
    policy_generation: u64,
) -> Result<
    (
        BoundAgentActionAdapter,
        AgentToolOptionsProjection,
        BTreeSet<AgentToolCapability>,
    ),
    AgentToolAdapterError,
> {
    let envelope = AgentAuthorizationFacts {
        identity_id: identity.id.clone(),
        identity_status: pioneer_protocol::AgentIdentityStatus::Active,
        role_key: role_key.to_owned(),
        root_capsule_id: root_capsule_id.to_owned(),
        parent_envelope: None,
        policy_generation,
    }
    .derive_envelope(RoleDefinitionRegistry::new())
    .ok_or(AgentToolAdapterError::Action(
        AgentActionServiceError::NotAuthorized("selected task agent role is not registered"),
    ))?;
    let agent_authorization_fingerprint = envelope.fingerprint.clone();
    let allowed_action_names = envelope.allowed_action_names();
    materialize_persisted_task_agent_action_binding(
        execution_id,
        root_capsule_id,
        work_graph_root_execution_id,
        identity,
        profile,
        execution_generation,
        attempt_generation,
        depth,
        &format!("task:{agent_spec_id}"),
        role_key,
        policy_generation,
        policy_generation,
        agent_authorization_fingerprint.as_str(),
        allowed_action_names.as_slice(),
    )
}

pub(crate) fn materialize_persisted_selected_task_agent_action_binding(
    execution_id: AgentExecutionId,
    root_capsule_id: &str,
    work_graph_root_execution_id: AgentExecutionId,
    agent_spec_id: &str,
    identity: AgentIdentityProjection,
    profile: AgentExecutionProfileProjection,
    execution_generation: u64,
    attempt_generation: u64,
    depth: u16,
    role_key: &str,
    persisted_policy_generation: u64,
    current_policy_generation: u64,
    agent_authorization_fingerprint: &str,
    allowed_action_names: &[String],
) -> Result<
    (
        BoundAgentActionAdapter,
        AgentToolOptionsProjection,
        BTreeSet<AgentToolCapability>,
    ),
    AgentToolAdapterError,
> {
    materialize_persisted_task_agent_action_binding(
        execution_id,
        root_capsule_id,
        work_graph_root_execution_id,
        identity,
        profile,
        execution_generation,
        attempt_generation,
        depth,
        &format!("task:{agent_spec_id}"),
        role_key,
        persisted_policy_generation,
        current_policy_generation,
        agent_authorization_fingerprint,
        allowed_action_names,
    )
}

fn populate_binding_options(
    root: &RootExecutionBinding,
    same_capsule_targets: &[(String, String)],
    routes: &[AgentRouteFacts],
    capabilities: &BTreeSet<AgentToolCapability>,
    options: &mut AgentToolOptionsProjection,
) {
    options.target_options = vec![
        AgentToolTargetOption {
            id: target_option_id(root, None, "current"),
            label: "Current thread".to_owned(),
        },
        AgentToolTargetOption {
            id: target_option_id(root, None, "home"),
            label: "Current collaboration".to_owned(),
        },
    ];
    options
        .target_options
        .extend(
            same_capsule_targets
                .iter()
                .map(|(thread_id, label)| AgentToolTargetOption {
                    id: same_capsule_target_option_id(root, thread_id),
                    label: label.clone(),
                }),
        );
    options
        .target_options
        .extend(routes.iter().map(|route| AgentToolTargetOption {
            id: target_option_id(root, Some(route), "route"),
            label: "Approved routed destination".to_owned(),
        }));
    if capabilities.contains(&AgentToolCapability::ThreadCreate) {
        let mut audiences = vec![(
            AgentThreadAudienceTemplate::HomeCapsule,
            "Current collaboration",
        )];
        if root.envelope.allows(ResourceAction::ThreadCreateWorkspace) {
            audiences.push((
                AgentThreadAudienceTemplate::RootDelegation,
                "New delegated root",
            ));
        }
        options.thread_creation_options = audiences
            .into_iter()
            .map(|(audience, label)| AgentToolThreadCreationOption {
                id: thread_creation_option_id(root, &audience),
                label: label.to_owned(),
            })
            .collect();
    }
}

fn same_capsule_target_option_id(root: &RootExecutionBinding, thread_id: &str) -> String {
    format!(
        "O{}",
        &fingerprint(&[
            "agent-same-capsule-target-option",
            root.execution_id.as_str(),
            root.options_generation_fingerprint.as_str(),
            thread_id,
        ])[..20]
    )
}

fn task_agent_capabilities(envelope: &AgentSecurityEnvelope) -> BTreeSet<AgentToolCapability> {
    let mut capabilities = [
        (
            AgentToolCapability::MessageCreate,
            ResourceAction::MessageCreate,
        ),
        (
            AgentToolCapability::ThreadCreate,
            ResourceAction::ThreadCreatePrivate,
        ),
        (AgentToolCapability::TaskReview, ResourceAction::TaskReview),
        (AgentToolCapability::TaskObserve, ResourceAction::TaskRead),
        (AgentToolCapability::ResultRead, ResourceAction::TaskRead),
    ]
    .into_iter()
    .filter_map(|(capability, action)| envelope.allows(action).then_some(capability))
    .collect::<BTreeSet<_>>();
    if envelope.allows(ResourceAction::ChildStart)
        && envelope.allows(ResourceAction::AgentTurnStart)
    {
        capabilities.insert(AgentToolCapability::ChildStart);
    }
    if envelope.allows(ResourceAction::TaskCreate) {
        capabilities.insert(AgentToolCapability::TaskCreate);
    }
    if envelope.allows(ResourceAction::TaskCreate)
        && envelope.allows(ResourceAction::TaskScheduleManage)
    {
        capabilities.insert(AgentToolCapability::TaskSchedule);
    }
    if [
        ResourceAction::TaskCancel,
        ResourceAction::TaskDetach,
        ResourceAction::TaskScheduleManage,
    ]
    .into_iter()
    .any(|action| envelope.allows(action))
    {
        capabilities.insert(AgentToolCapability::TaskControl);
    }
    capabilities
}

fn fingerprint(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AgentAuthoredInput, AgentExecutionProfileBackend, AgentExecutionProfileId,
        AgentExecutionProfileProjection, AgentIdentityId, AgentIdentityProjection,
        AgentIdentitySourceKind, AgentToolIdentityChoice, AgentToolLaunchSelection,
        AgentToolProfileChoice, McpScopeKind, SkillId, TurnPermissionMode,
        TurnPermissionProfileSelection, TurnReasoningSelection,
    };

    #[test]
    fn call_derivation_is_stable_and_opaque() {
        let first = action_id_for_call("provider-call-1");
        assert_eq!(first, action_id_for_call("provider-call-1"));
        assert_eq!(first.as_str().len(), 21);
        let key = idempotency_key_for_call("provider-call-1", AgentModelToolName::StartAgent);
        assert_eq!(key.len(), 64);
        assert!(!key.contains("provider-call-1"));
    }

    #[test]
    fn safe_result_distinguishes_queued_work_with_a_stable_outcome() {
        let projection = AgentActionCommitProjection {
            action_id: AgentActionId::new("C12345678901234567890").unwrap(),
            execution_id: AgentExecutionId::new("E12345678901234567890").unwrap(),
            kind: pioneer_protocol::AgentActionKind::StartAgent,
            queued: true,
            receipt_id: "R12345678901234567890".to_owned(),
            outbox_id: "O12345678901234567890".to_owned(),
        };
        let result = BoundAgentActionAdapter::safe_result(&projection);
        assert_eq!(result.status, AgentToolResultStatus::Queued);
        assert_eq!(result.outcome, Some(AgentPublicOutcome::AgentWorkQueued));
    }

    #[test]
    fn typed_start_call_cannot_supply_actor_or_provider_configuration() {
        let mut adapter = test_adapter();
        let options = test_options(&mut adapter);
        let mut input = serde_json::to_value(AgentStartToolInput {
            target_option_id: adapter.current_target_option_id(),
            input: AgentAuthoredInput::default(),
            launch: exact_tool_launch(),
        })
        .unwrap();
        input
            .get_mut("launch")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .insert("actor".to_owned(), serde_json::json!("attacker"));
        assert!(
            adapter
                .intent_from_model_call(
                    "call-1",
                    AgentModelToolName::StartAgent,
                    input,
                    Some(&options),
                )
                .is_err()
        );
    }

    #[test]
    fn observation_tools_do_not_create_mutation_intents() {
        let adapter = test_adapter();
        for tool in [
            AgentModelToolName::AgentStartOptions,
            AgentModelToolName::Wait,
            AgentModelToolName::Result,
        ] {
            assert!(
                adapter
                    .intent_from_model_call("call-1", tool, serde_json::json!({}), None)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn safe_start_options_hide_backend_configuration_and_keep_exact_caps() {
        let mut adapter = test_adapter();
        let options = test_options(&mut adapter);
        let encoded = serde_json::to_string(&options).unwrap();

        assert!(!encoded.contains("secret-provider"));
        assert!(!encoded.contains("secret-model"));
        assert!(!encoded.contains("credentials"));
        assert_eq!(
            options.allowed_mcp_server_ids,
            vec![pioneer_protocol::mcp_server_capability_key(
                McpScopeKind::Workspace,
                "allowed-mcp",
            )]
        );
        assert_eq!(options.allowed_skill_ids, vec![test_skill_id()]);
    }

    #[test]
    fn projected_launch_ceiling_rejects_reasoning_permission_and_capability_widening() {
        let mut adapter = test_adapter();
        let options = test_options(&mut adapter);

        let mut reasoning = exact_tool_launch();
        reasoning.reasoning = Some(TurnReasoningSelection {
            effort: "high".to_owned(),
        });
        assert!(validate_tool_launch(&reasoning, &options).is_err());

        let mut permission = exact_tool_launch();
        permission.permission_profile = Some(TurnPermissionProfileSelection {
            mode: TurnPermissionMode::FullAccess,
        });
        assert!(validate_tool_launch(&permission, &options).is_err());

        let mut skill = exact_tool_launch();
        skill.skill_ids = vec![SkillId::new("K99999999999999999999").unwrap()];
        assert!(validate_tool_launch(&skill, &options).is_err());

        let mut mcp = exact_tool_launch();
        mcp.mcp_server_ids = vec![pioneer_protocol::mcp_server_capability_key(
            McpScopeKind::User,
            "allowed-mcp",
        )];
        assert!(validate_tool_launch(&mcp, &options).is_err());

        let mut duplicate_skill = exact_tool_launch();
        duplicate_skill.skill_ids.push(test_skill_id());
        assert!(validate_tool_launch(&duplicate_skill, &options).is_err());

        let mut duplicate_mcp = exact_tool_launch();
        duplicate_mcp
            .mcp_server_ids
            .push(pioneer_protocol::mcp_server_capability_key(
                McpScopeKind::Workspace,
                "allowed-mcp",
            ));
        assert!(validate_tool_launch(&duplicate_mcp, &options).is_err());
    }

    #[test]
    fn persisted_work_graph_root_is_exact_and_never_synthesized() {
        let mut adapter = test_adapter();
        let persisted = "E99999999999999999999";
        adapter.bind_persisted_work_graph_root(persisted).unwrap();
        assert_eq!(adapter.work_graph_root_execution_id().as_str(), persisted);
        assert!(
            adapter
                .bind_persisted_work_graph_root("opaque-root")
                .is_err()
        );
    }

    #[test]
    fn role_capabilities_follow_the_registered_scalability_matrix() {
        let thread_agent = envelope_for_role("thread_agent");
        let runner = envelope_for_role("agent_runner");
        let reviewer = envelope_for_role("agent_reviewer");

        assert!(
            task_agent_capabilities(&thread_agent).contains(&AgentToolCapability::TaskSchedule)
        );
        let runner_caps = task_agent_capabilities(&runner);
        assert!(runner_caps.contains(&AgentToolCapability::ChildStart));
        assert!(!runner_caps.contains(&AgentToolCapability::TaskSchedule));
        assert!(!runner_caps.contains(&AgentToolCapability::TaskReview));
        let reviewer_caps = task_agent_capabilities(&reviewer);
        assert!(reviewer_caps.contains(&AgentToolCapability::TaskReview));
        assert!(!reviewer_caps.contains(&AgentToolCapability::ChildStart));
    }

    #[test]
    fn task_capabilities_use_root_authority_and_exact_intersections() {
        let mut envelope = envelope_for_role("thread_agent");
        envelope
            .allowed_actions
            .remove(&ResourceAction::ChildTaskCreate);
        let capabilities = task_agent_capabilities(&envelope);
        assert!(capabilities.contains(&AgentToolCapability::TaskCreate));
        assert!(capabilities.contains(&AgentToolCapability::TaskSchedule));

        envelope.allowed_actions.remove(&ResourceAction::TaskCreate);
        let capabilities = task_agent_capabilities(&envelope);
        assert!(!capabilities.contains(&AgentToolCapability::TaskCreate));
        assert!(!capabilities.contains(&AgentToolCapability::TaskSchedule));

        let mut envelope = envelope_for_role("thread_agent");
        envelope
            .allowed_actions
            .remove(&ResourceAction::AgentTurnStart);
        assert!(
            !task_agent_capabilities(&envelope).contains(&AgentToolCapability::ChildStart),
            "child start must not be projected when half of its source intersection is missing"
        );
    }

    fn exact_tool_launch() -> AgentToolLaunchSelection {
        AgentToolLaunchSelection {
            identity: AgentToolIdentityChoice::Exact {
                id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            },
            profile: AgentToolProfileChoice::Exact(
                AgentExecutionProfileId::new("P12345678901234567890").unwrap(),
            ),
            reasoning: Some(TurnReasoningSelection {
                effort: "medium".to_owned(),
            }),
            permission_profile: Some(TurnPermissionProfileSelection {
                mode: TurnPermissionMode::Supervised,
            }),
            skill_ids: vec![test_skill_id()],
            mcp_server_ids: vec![pioneer_protocol::mcp_server_capability_key(
                McpScopeKind::Workspace,
                "allowed-mcp",
            )],
        }
    }

    fn test_skill_id() -> SkillId {
        SkillId::new("K12345678901234567890").unwrap()
    }

    fn test_options(adapter: &mut BoundAgentActionAdapter) -> AgentToolOptionsProjection {
        let identity = adapter.root.identity.clone();
        let profile = adapter.root.profile.clone();
        adapter.install_start_options_catalog(
            vec![identity],
            vec![profile],
            false,
            true,
            false,
            vec![test_skill_id()],
            vec![pioneer_protocol::mcp_server_capability_key(
                McpScopeKind::Workspace,
                "allowed-mcp",
            )],
            pioneer_protocol::task_permission_cap_for_mode(TurnPermissionMode::Supervised),
        )
    }

    fn envelope_for_role(role_key: &str) -> AgentSecurityEnvelope {
        AgentAuthorizationFacts {
            identity_id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            identity_status: pioneer_protocol::AgentIdentityStatus::Active,
            role_key: role_key.to_owned(),
            root_capsule_id: "T12345678901234567890".to_owned(),
            parent_envelope: None,
            policy_generation: 1,
        }
        .derive_envelope(RoleDefinitionRegistry::new())
        .unwrap()
    }

    fn test_adapter() -> BoundAgentActionAdapter {
        let identity = AgentIdentityProjection::new(
            AgentIdentityId::new("A12345678901234567890").unwrap(),
            AgentIdentitySourceKind::NativeAgent,
            "Agent",
            "agent",
            None,
            None,
            1,
            "source",
        )
        .unwrap();
        let profile = AgentExecutionProfileProjection {
            id: AgentExecutionProfileId::new("P12345678901234567890").unwrap(),
            compatible_agent_identity_ids: vec![identity.id.clone()],
            backend: AgentExecutionProfileBackend::ApiProvider,
            provider_id: "secret-provider".to_owned(),
            model_id: "secret-model".to_owned(),
            provider_display_name: "P".to_owned(),
            model_display_name: "M".to_owned(),
            allowed_reasoning: vec![TurnReasoningSelection {
                effort: "medium".to_owned(),
            }],
            allowed_permission_profiles: vec![TurnPermissionMode::Supervised],
            catalog_generation: 1,
            policy_generation: 1,
            fingerprint: "profile".to_owned(),
        };
        let envelope = super::super::AgentAuthorizationFacts {
            identity_id: identity.id.clone(),
            identity_status: pioneer_protocol::AgentIdentityStatus::Active,
            role_key: "thread_agent".to_owned(),
            root_capsule_id: "T12345678901234567890".to_owned(),
            parent_envelope: None,
            policy_generation: 1,
        }
        .derive_envelope(super::super::RoleDefinitionRegistry::new())
        .unwrap();
        let root_id = AgentExecutionId::new("E12345678901234567890").unwrap();
        let root = RootExecutionBinding {
            execution_id: root_id.clone(),
            identity,
            identity_source_revision: 1,
            identity_source_fingerprint: "source".to_owned(),
            profile,
            execution_generation: 1,
            home_root_thread_id: "T12345678901234567890".to_owned(),
            work_graph_root_execution_id: root_id,
            authorization_context_fingerprint: "auth".to_owned(),
            envelope: envelope.clone(),
            options_generation_fingerprint: "options".to_owned(),
        };
        BoundAgentActionAdapter::new(
            CanonicalAgentActionService::default(),
            root,
            envelope,
            None,
            AgentWorkResourcePolicy::default(),
            "branch",
            1,
            1,
            1,
        )
        .unwrap()
    }
}
