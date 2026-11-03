//! Root/child execution materialization and fair root-scoped resource control.
//!
//! The coordinator owns capacity only.  Identity, profile and authorization
//! facts are pinned before it is called, so a queued execution has the same
//! security decision as a running one.

use chrono::{DateTime, FixedOffset};
use sha2::Digest;

use pioneer_protocol::{
    AgentActionId, AgentAuthoredTurnProjection, AgentExecutionId, AgentExecutionProfileBackend,
    AgentExecutionProfileProjection, AgentIdentityId, AgentIdentityProjection,
    AgentIdentitySelection, AgentIdentitySourceKind, AgentLaunchSelection,
    AgentPresentationSnapshot, AgentStartOptionsProjection, AgentStartTarget, PrincipalId,
    StartAgentIntent,
};

use super::{
    AgentAuthorizationFacts, AgentRouteFacts, AgentSecurityEnvelope, AgentStartFacts,
    AgentWorkResourcePolicy, ResourceAction, RoleDefinitionRegistry, validate_agent_start_facts,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RootExecutionBinding {
    pub(crate) execution_id: AgentExecutionId,
    pub(crate) identity: AgentIdentityProjection,
    pub(crate) identity_source_revision: u64,
    pub(crate) identity_source_fingerprint: String,
    pub(crate) profile: AgentExecutionProfileProjection,
    pub(crate) execution_generation: u64,
    pub(crate) home_root_thread_id: String,
    pub(crate) work_graph_root_execution_id: AgentExecutionId,
    pub(crate) authorization_context_fingerprint: String,
    pub(crate) envelope: AgentSecurityEnvelope,
    pub(crate) options_generation_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChildAgentLaunchGrant {
    pub(crate) execution_id: AgentExecutionId,
    pub(crate) parent_execution_id: AgentExecutionId,
    pub(crate) identity: AgentIdentityProjection,
    pub(crate) profile: AgentExecutionProfileProjection,
    pub(crate) root_execution_id: AgentExecutionId,
    pub(crate) home_root_thread_id: String,
    pub(crate) depth: u16,
    pub(crate) independent_root: bool,
    pub(crate) branch_key: String,
    pub(crate) envelope: AgentSecurityEnvelope,
    pub(crate) route: Option<AgentRouteFacts>,
    pub(crate) child_launch_ceiling: pioneer_protocol::ChildAgentLaunchGrantSet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaterializedChildAgentStart {
    pub(crate) grant: ChildAgentLaunchGrant,
    pub(crate) authored_turn: AgentAuthoredTurnProjection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionMaterializationError {
    MissingIdentity,
    MissingProfile,
    IncompatibleProfile,
    CliRuntimeMismatch,
    NativeAgentRequiresApiProfile,
    InvalidLaunchSelection(&'static str),
    BoundaryExceeded,
    PolicyGenerationStale,
}

fn select_identity(
    selection: &AgentIdentitySelection,
    options: &AgentStartOptionsProjection,
    inherited_identity: Option<&AgentIdentityProjection>,
) -> Result<AgentIdentityProjection, ExecutionMaterializationError> {
    match selection {
        AgentIdentitySelection::InheritParent if options.inherit_parent_agent_available => {
            inherited_identity
                .and_then(|inherited| {
                    options.agents.iter().find(|current| {
                        pioneer_protocol::agent_identity_has_same_launch_source(current, inherited)
                    })
                })
                .cloned()
                .ok_or(ExecutionMaterializationError::MissingIdentity)
        }
        AgentIdentitySelection::InheritParent => {
            Err(ExecutionMaterializationError::MissingIdentity)
        }
        AgentIdentitySelection::DefaultPioneer => options
            .agents
            .iter()
            .find(|identity| identity.nickname == "pioneer")
            .cloned()
            .ok_or(ExecutionMaterializationError::MissingIdentity),
        AgentIdentitySelection::Exact { agent_identity_id } => options
            .agents
            .iter()
            .find(|identity| &identity.id == agent_identity_id)
            .cloned()
            .ok_or(ExecutionMaterializationError::MissingIdentity),
        AgentIdentitySelection::ServerDerivedEphemeral { .. }
            if options.derived_ephemeral_available =>
        {
            options
                .agents
                .iter()
                .find(|identity| identity.source_kind == AgentIdentitySourceKind::Ephemeral)
                .cloned()
                .ok_or(ExecutionMaterializationError::MissingIdentity)
        }
        AgentIdentitySelection::ServerDerivedEphemeral { .. } => {
            Err(ExecutionMaterializationError::MissingIdentity)
        }
    }
}

fn select_profile(
    selection: &AgentLaunchSelection,
    identity: &AgentIdentityProjection,
    options: &AgentStartOptionsProjection,
    inherited_profile: Option<&AgentExecutionProfileProjection>,
) -> Result<AgentExecutionProfileProjection, ExecutionMaterializationError> {
    let selected = match &selection.execution.profile {
        pioneer_protocol::AgentExecutionProfileSelection::InheritParent
            if options.inherit_parent_profile_available =>
        {
            let inherited =
                inherited_profile.ok_or(ExecutionMaterializationError::MissingProfile)?;
            if let Some(profile) = options
                .profiles
                .iter()
                .filter(|current| {
                    (current.id == inherited.id
                        || (current.backend == inherited.backend
                            && current.provider_id == inherited.provider_id
                            && current.model_id == inherited.model_id))
                        && (identity.source_kind == AgentIdentitySourceKind::Ephemeral
                            || current.compatible_agent_identity_ids.contains(&identity.id))
                })
                .min_by_key(|current| current.id != inherited.id)
            {
                profile.clone()
            } else {
                // Changing identity must never carry a model from an unrelated
                // executor. A unique compatible choice is the only safe default.
                let mut compatible = options
                    .profiles
                    .iter()
                    .filter(|profile| profile.compatible_agent_identity_ids.contains(&identity.id));
                let profile = compatible
                    .next()
                    .ok_or(ExecutionMaterializationError::MissingProfile)?;
                if compatible.next().is_some() {
                    return Err(ExecutionMaterializationError::InvalidLaunchSelection(
                        "identity requires an explicit model; inspect threads_identity_models_list",
                    ));
                }
                profile.clone()
            }
        }
        pioneer_protocol::AgentExecutionProfileSelection::InheritParent => {
            return Err(ExecutionMaterializationError::MissingProfile);
        }
        pioneer_protocol::AgentExecutionProfileSelection::Exact { profile_id } => options
            .profiles
            .iter()
            .find(|profile| &profile.id == profile_id)
            .cloned()
            .ok_or(ExecutionMaterializationError::MissingProfile)?,
        pioneer_protocol::AgentExecutionProfileSelection::Model {
            provider_id,
            model_id,
        } => {
            let inherited_provider = inherited_profile
                .filter(|profile| profile.compatible_agent_identity_ids.contains(&identity.id))
                .map(|profile| profile.provider_id.as_str());
            let selected_provider = provider_id.as_deref().or(inherited_provider);
            let mut candidates = options.profiles.iter().filter(|profile| {
                (profile.compatible_agent_identity_ids.contains(&identity.id)
                    || (identity.source_kind == AgentIdentitySourceKind::Ephemeral
                        && options.agents.iter().any(|source| {
                            profile.compatible_agent_identity_ids.contains(&source.id)
                        })))
                    && profile.model_id == *model_id
                    && selected_provider.is_none_or(|provider| profile.provider_id == provider)
            });
            let profile = candidates
                .next()
                .ok_or(ExecutionMaterializationError::MissingProfile)?;
            if candidates.next().is_some() {
                return Err(ExecutionMaterializationError::InvalidLaunchSelection(
                    "model requires an explicit providerId",
                ));
            }
            profile.clone()
        }
    };
    if identity.source_kind != AgentIdentitySourceKind::Ephemeral
        && !selected
            .compatible_agent_identity_ids
            .iter()
            .any(|candidate| candidate == &identity.id)
    {
        return Err(ExecutionMaterializationError::IncompatibleProfile);
    }
    match (&identity.source_kind, &selected.backend) {
        (
            AgentIdentitySourceKind::CliRuntimeInstance,
            AgentExecutionProfileBackend::CliRuntime {
                runtime_instance_id: _,
            },
        ) => {
            // The server-owned profile projection already contains the exact
            // compatible identity IDs.  Runtime instance IDs intentionally
            // stay out of the safe identity projection, so never try to infer
            // them from the opaque source fingerprint.
        }
        (AgentIdentitySourceKind::NativeAgent, AgentExecutionProfileBackend::ApiProvider) => {}
        (AgentIdentitySourceKind::NativeAgent, _) => {
            return Err(ExecutionMaterializationError::NativeAgentRequiresApiProfile);
        }
        (AgentIdentitySourceKind::Ephemeral, _) => {}
        (AgentIdentitySourceKind::CliRuntimeInstance, _) => {
            return Err(ExecutionMaterializationError::CliRuntimeMismatch);
        }
    }
    if selection
        .execution
        .reasoning
        .as_ref()
        .is_some_and(|reasoning| !selected.allowed_reasoning.contains(reasoning))
    {
        return Err(ExecutionMaterializationError::InvalidLaunchSelection(
            "reasoning is not available for the selected model",
        ));
    }
    Ok(selected)
}

pub(crate) fn resolve_agent_launch_selection(
    selection: &AgentLaunchSelection,
    options: &AgentStartOptionsProjection,
    inherited_identity: Option<&AgentIdentityProjection>,
    inherited_profile: Option<&AgentExecutionProfileProjection>,
) -> Result<(AgentIdentityProjection, AgentExecutionProfileProjection), ExecutionMaterializationError>
{
    let identity = select_identity(&selection.agent, options, inherited_identity)?;
    let profile = select_profile(selection, &identity, options, inherited_profile)?;
    Ok((identity, profile))
}

pub(crate) fn resolve_ephemeral_agent_launch_selection(
    selection: &AgentLaunchSelection,
    stable_seed: &str,
    options: &AgentStartOptionsProjection,
    inherited_profile: Option<&AgentExecutionProfileProjection>,
) -> Result<(AgentIdentityProjection, AgentExecutionProfileProjection), ExecutionMaterializationError>
{
    let AgentIdentitySelection::ServerDerivedEphemeral {
        display_name_hint,
        role_label,
    } = &selection.agent
    else {
        return Err(ExecutionMaterializationError::InvalidLaunchSelection(
            "ephemeral launch resolver requires an ephemeral selection",
        ));
    };
    if !options.derived_ephemeral_available || stable_seed.trim().is_empty() {
        return Err(ExecutionMaterializationError::MissingIdentity);
    }
    let display_name = display_name_hint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Delegated agent")
        .chars()
        .take(80)
        .collect::<String>();
    let nickname_stem = display_name
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(12)
        .collect::<String>()
        .to_ascii_lowercase();
    let identity_digest =
        sha2::Sha256::digest(format!("agent-ephemeral-identity\0{stable_seed}").as_bytes());
    let digest_hex = hex::encode(identity_digest);
    let identity_id = AgentIdentityId::new(format!("A{}", &digest_hex[..20]))
        .expect("derived ephemeral identity id is valid");
    let nickname = format!(
        "{}-{}",
        if nickname_stem.is_empty() {
            "agent"
        } else {
            nickname_stem.as_str()
        },
        &digest_hex[20..26],
    );
    let identity = AgentIdentityProjection::new(
        identity_id,
        AgentIdentitySourceKind::Ephemeral,
        display_name,
        nickname,
        None,
        role_label.clone(),
        1,
        hex::encode(sha2::Sha256::digest(
            format!("agent-ephemeral-source\0{stable_seed}").as_bytes(),
        )),
    )
    .map_err(|_| {
        ExecutionMaterializationError::InvalidLaunchSelection(
            "derived ephemeral identity is invalid",
        )
    })?;
    let mut profile = select_profile(selection, &identity, options, inherited_profile)?;
    if !profile.compatible_agent_identity_ids.contains(&identity.id) {
        profile
            .compatible_agent_identity_ids
            .push(identity.id.clone());
        profile.compatible_agent_identity_ids.sort();
        profile.fingerprint = hex::encode(sha2::Sha256::digest(
            format!(
                "agent-derived-profile\0{}\0{}",
                profile.fingerprint, identity.id
            )
            .as_bytes(),
        ));
    }
    Ok((identity, profile))
}

pub(crate) fn derive_child_launch_grant(
    parent: &RootExecutionBinding,
    execution_id: AgentExecutionId,
    selection: &AgentLaunchSelection,
    options: &AgentStartOptionsProjection,
    target: AgentStartTarget,
    route: Option<AgentRouteFacts>,
    depth: u16,
    independent_root: bool,
    branch_key: impl Into<String>,
    policy: &AgentWorkResourcePolicy,
    registry: RoleDefinitionRegistry,
) -> Result<ChildAgentLaunchGrant, ExecutionMaterializationError> {
    if options.generation_fingerprint != parent.options_generation_fingerprint {
        return Err(ExecutionMaterializationError::PolicyGenerationStale);
    }
    if (independent_root && depth != 0)
        || (!independent_root && (depth == 0 || depth > policy.max_depth))
        || (independent_root
            && !route.as_ref().is_some_and(|route| {
                !route.same_capsule
                    && route.destination_thread_id == route.destination_capsule_id
                    && route.destination_capsule_id != parent.home_root_thread_id
            }))
    {
        return Err(ExecutionMaterializationError::BoundaryExceeded);
    }
    if !parent.envelope.allows(ResourceAction::ChildStart) {
        return Err(ExecutionMaterializationError::InvalidLaunchSelection(
            "parent lacks child-start authority",
        ));
    }
    let identity = match &selection.agent {
        AgentIdentitySelection::InheritParent => {
            select_identity(&selection.agent, options, Some(&parent.identity))?
        }
        AgentIdentitySelection::ServerDerivedEphemeral { .. } => {
            resolve_ephemeral_agent_launch_selection(
                selection,
                execution_id.as_str(),
                options,
                Some(&parent.profile),
            )?
            .0
        }
        _ => select_identity(&selection.agent, options, Some(&parent.identity))?,
    };
    let mut profile = select_profile(selection, &identity, options, Some(&parent.profile))?;
    if identity.source_kind == AgentIdentitySourceKind::Ephemeral
        && !profile.compatible_agent_identity_ids.contains(&identity.id)
    {
        profile
            .compatible_agent_identity_ids
            .push(identity.id.clone());
        profile.compatible_agent_identity_ids.sort();
        profile.fingerprint = hex::encode(sha2::Sha256::digest(
            format!(
                "agent-derived-profile\0{}\0{}",
                profile.fingerprint, identity.id
            )
            .as_bytes(),
        ));
    }
    let target_facts = AgentStartFacts {
        target: target.clone(),
        route: route.clone(),
        envelope: parent.envelope.clone(),
        inherited_profile: Some(parent.profile.id.clone()),
    };
    validate_agent_start_facts(selection, &target_facts)
        .map_err(ExecutionMaterializationError::InvalidLaunchSelection)?;
    let child_identity = identity.id.clone();
    let child_home_root_thread_id = route
        .as_ref()
        .filter(|route| !route.same_capsule)
        .map(|route| route.destination_capsule_id.clone())
        .unwrap_or_else(|| parent.home_root_thread_id.clone());
    let child_facts = AgentAuthorizationFacts {
        identity_id: child_identity,
        identity_status: pioneer_protocol::AgentIdentityStatus::Active,
        role_key: parent.envelope.role_key.clone(),
        root_capsule_id: child_home_root_thread_id.clone(),
        parent_envelope: Some(parent.envelope.clone()),
        policy_generation: parent.envelope.policy_generation,
    };
    let envelope = child_facts.derive_envelope(registry).ok_or(
        ExecutionMaterializationError::InvalidLaunchSelection("child role is not registered"),
    )?;
    let mut child_identities = options.agents.clone();
    if !child_identities
        .iter()
        .any(|candidate| candidate == &identity)
    {
        child_identities.push(identity.clone());
    }
    let child_launch_ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(child_identities)
        .and_then(|grant| {
            grant.with_policy(
                options.inherit_parent_agent_available,
                options.derived_ephemeral_available,
                options.inherit_parent_profile_available,
                options.allowed_skill_ids.clone(),
                options.allowed_mcp_server_ids.clone(),
                options.max_permission_profile.clone(),
            )
        })
        .map_err(|_| {
            ExecutionMaterializationError::InvalidLaunchSelection(
                "child launch grant ceiling is invalid",
            )
        })?;
    Ok(ChildAgentLaunchGrant {
        root_execution_id: if independent_root {
            execution_id.clone()
        } else {
            parent.work_graph_root_execution_id.clone()
        },
        execution_id,
        parent_execution_id: parent.execution_id.clone(),
        identity,
        profile,
        home_root_thread_id: child_home_root_thread_id,
        depth,
        independent_root,
        branch_key: branch_key.into(),
        envelope,
        route,
        child_launch_ceiling,
    })
}

/// Materialize the child execution and its visible authored input together.
/// Runtime/provider prompt compilation is intentionally not part of this
/// function; the returned projection is the only content exposed to the
/// conversation timeline.
pub(crate) fn materialize_child_agent_start(
    action_id: AgentActionId,
    execution_id: AgentExecutionId,
    parent: &RootExecutionBinding,
    intent: &StartAgentIntent,
    options: &AgentStartOptionsProjection,
    route: Option<AgentRouteFacts>,
    depth: u16,
    independent_root: bool,
    branch_key: &str,
    policy: &AgentWorkResourcePolicy,
    registry: RoleDefinitionRegistry,
    controller_principal_id: Option<PrincipalId>,
) -> Result<MaterializedChildAgentStart, ExecutionMaterializationError> {
    let grant = derive_child_launch_grant(
        parent,
        execution_id,
        &intent.launch,
        options,
        intent.target.clone(),
        route,
        depth,
        independent_root,
        branch_key.to_owned(),
        policy,
        registry,
    )?;
    let snapshot = AgentPresentationSnapshot {
        agent_identity_id: parent.identity.id.clone(),
        agent_execution_id: parent.execution_id.clone(),
        identity_source_kind: parent.identity.source_kind,
        identity_source_revision: parent.identity_source_revision,
        display_name: parent.identity.display_name.clone(),
        nickname: parent.identity.nickname.clone(),
        avatar_revision: parent.identity.avatar_revision.clone(),
        role_label: parent.identity.role_label.clone(),
    };
    let authored_turn = AgentAuthoredTurnProjection::new(
        action_id,
        &snapshot,
        intent.thread_mode(),
        intent.input.clone(),
        controller_principal_id,
    )
    .map_err(|_| {
        ExecutionMaterializationError::InvalidLaunchSelection(
            "agent authored input is not clean visible content",
        )
    })?;
    Ok(MaterializedChildAgentStart {
        grant,
        authored_turn,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunningPermit {
    pub(crate) permit_id: u64,
    pub(crate) root_execution_id: AgentExecutionId,
    pub(crate) execution_id: AgentExecutionId,
    pub(crate) attempt_generation: u64,
    pub(crate) branch_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionAttemptState {
    pub(crate) execution_id: AgentExecutionId,
    pub(crate) attempt_generation: u64,
    pub(crate) progress_sequence: u64,
    pub(crate) last_progress_at: Option<DateTime<FixedOffset>>,
    pub(crate) last_heartbeat_at: Option<DateTime<FixedOffset>>,
    pub(crate) idle_deadline: Option<DateTime<FixedOffset>>,
    pub(crate) hard_deadline: Option<DateTime<FixedOffset>>,
    pub(crate) fenced: bool,
}

impl ExecutionAttemptState {
    pub(crate) fn new(
        execution_id: AgentExecutionId,
        attempt_generation: u64,
        idle_deadline: Option<DateTime<FixedOffset>>,
        hard_deadline: Option<DateTime<FixedOffset>>,
    ) -> Result<Self, ExecutionMaterializationError> {
        if attempt_generation == 0 {
            return Err(ExecutionMaterializationError::BoundaryExceeded);
        }
        Ok(Self {
            execution_id,
            attempt_generation,
            progress_sequence: 0,
            last_progress_at: None,
            last_heartbeat_at: None,
            idle_deadline,
            hard_deadline,
            fenced: false,
        })
    }

    pub(crate) fn record_heartbeat(
        &mut self,
        now: DateTime<FixedOffset>,
    ) -> Result<(), ExecutionMaterializationError> {
        if self.fenced {
            return Err(ExecutionMaterializationError::PolicyGenerationStale);
        }
        self.last_heartbeat_at = Some(now);
        Ok(())
    }
}

#[cfg(test)]
impl ExecutionAttemptState {
    pub(crate) fn record_progress(
        &mut self,
        now: DateTime<FixedOffset>,
    ) -> Result<(), ExecutionMaterializationError> {
        if self.fenced {
            return Err(ExecutionMaterializationError::PolicyGenerationStale);
        }
        self.progress_sequence = self
            .progress_sequence
            .checked_add(1)
            .ok_or(ExecutionMaterializationError::BoundaryExceeded)?;
        self.last_progress_at = Some(now);
        self.idle_deadline = self
            .idle_deadline
            .map(|deadline| if deadline < now { now } else { deadline });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AgentExecutionProfileId, AgentExecutionProfileSelection, AgentExecutionSelection,
        AgentIdentityId, AgentIdentitySelection,
    };

    fn materialize_root_execution(
        selection: &AgentLaunchSelection,
        options: &AgentStartOptionsProjection,
        home_root_thread_id: impl Into<String>,
        role_key: impl Into<String>,
        policy_generation: u64,
        registry: RoleDefinitionRegistry,
    ) -> Result<RootExecutionBinding, ExecutionMaterializationError> {
        if options.generation_fingerprint.is_empty() {
            return Err(ExecutionMaterializationError::PolicyGenerationStale);
        }
        let identity = select_identity(&selection.agent, options, None)?;
        let profile = select_profile(selection, &identity, options, None)?;
        let facts = AgentAuthorizationFacts {
            identity_id: identity.id.clone(),
            identity_status: pioneer_protocol::AgentIdentityStatus::Active,
            role_key: role_key.into(),
            root_capsule_id: home_root_thread_id.into(),
            parent_envelope: None,
            policy_generation,
        };
        let envelope = facts.derive_envelope(registry).ok_or(
            ExecutionMaterializationError::InvalidLaunchSelection("agent role is not registered"),
        )?;
        if !envelope.allows(ResourceAction::AgentTurnStart) {
            return Err(ExecutionMaterializationError::InvalidLaunchSelection(
                "agent role cannot start execution",
            ));
        }
        let execution_id = AgentExecutionId::new(pioneer_protocol::generate_id(21))
            .expect("generated execution ID is valid");
        Ok(RootExecutionBinding {
            execution_id: execution_id.clone(),
            identity_source_revision: identity.source_revision,
            identity_source_fingerprint: identity.source_fingerprint.clone(),
            identity,
            profile,
            execution_generation: 1,
            home_root_thread_id: facts.root_capsule_id,
            work_graph_root_execution_id: execution_id,
            authorization_context_fingerprint: envelope.fingerprint.clone(),
            envelope,
            options_generation_fingerprint: options.generation_fingerprint.clone(),
        })
    }

    fn identity(id: &str, source_kind: AgentIdentitySourceKind) -> AgentIdentityProjection {
        AgentIdentityProjection::new(
            AgentIdentityId::new(id.to_owned()).unwrap(),
            source_kind,
            "Agent",
            "agent",
            None,
            None,
            1,
            "runtime-instance-1",
        )
        .unwrap()
    }

    fn profile(id: &str, identity: &AgentIdentityProjection) -> AgentExecutionProfileProjection {
        AgentExecutionProfileProjection {
            id: AgentExecutionProfileId::new(id.to_owned()).unwrap(),
            compatible_agent_identity_ids: vec![identity.id.clone()],
            backend: AgentExecutionProfileBackend::ApiProvider,
            provider_id: "provider".to_owned(),
            model_id: "model".to_owned(),
            provider_display_name: "Provider".to_owned(),
            model_display_name: "Model".to_owned(),
            allowed_reasoning: Vec::new(),
            allowed_permission_profiles: Vec::new(),
            catalog_generation: 1,
            policy_generation: 1,
            fingerprint: format!("fp-{id}"),
        }
    }

    fn selection() -> AgentLaunchSelection {
        AgentLaunchSelection {
            agent: AgentIdentitySelection::Exact {
                agent_identity_id: AgentIdentityId::new("A12345678901234567890".to_owned())
                    .unwrap(),
            },
            execution: AgentExecutionSelection {
                profile: AgentExecutionProfileSelection::Exact {
                    profile_id: AgentExecutionProfileId::new("P12345678901234567890".to_owned())
                        .unwrap(),
                },
                reasoning: None,
                permission_profile: None,
                skill_ids: Vec::new(),
                mcp_server_ids: Vec::new(),
                selected_capabilities: Vec::new(),
            },
        }
    }

    #[test]
    fn explicit_models_resolve_within_identity_and_model_reasoning_grants() {
        let api = identity(
            "A12345678901234567890",
            AgentIdentitySourceKind::NativeAgent,
        );
        let cli = identity(
            "A12345678901234567891",
            AgentIdentitySourceKind::CliRuntimeInstance,
        );
        let mut original = profile("P12345678901234567890", &api);
        original.allowed_reasoning = vec![pioneer_protocol::TurnReasoningSelection {
            effort: "low".into(),
        }];
        let mut other = profile("P12345678901234567891", &api);
        other.model_id = "other-model".into();
        other.allowed_reasoning = vec![pioneer_protocol::TurnReasoningSelection {
            effort: "high".into(),
        }];
        let mut cli_profile = profile("P12345678901234567892", &cli);
        cli_profile.backend = AgentExecutionProfileBackend::CliRuntime {
            runtime_instance_id: "configured-cli".into(),
        };
        cli_profile.provider_id = "cli_runtime:configured-cli".into();
        cli_profile.model_id = "cli-model".into();
        cli_profile.allowed_reasoning = vec![pioneer_protocol::TurnReasoningSelection {
            effort: "runtime-effort".into(),
        }];
        let options = AgentStartOptionsProjection {
            agents: vec![api.clone(), cli.clone()],
            profiles: vec![original.clone(), other.clone(), cli_profile.clone()],
            inherit_parent_agent_available: true,
            inherit_parent_profile_available: true,
            derived_ephemeral_available: false,
            allowed_skill_ids: vec![],
            allowed_mcp_server_ids: vec![],
            max_permission_profile: pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
            ),
            generation_fingerprint: "selection-options".into(),
        };
        for (identity, selected, effort) in [
            (&api, &other, "high"),
            (&cli, &cli_profile, "runtime-effort"),
        ] {
            let mut launch = selection();
            launch.agent = AgentIdentitySelection::Exact {
                agent_identity_id: identity.id.clone(),
            };
            launch.execution.profile = AgentExecutionProfileSelection::Model {
                provider_id: Some(selected.provider_id.clone()),
                model_id: selected.model_id.clone(),
            };
            launch.execution.reasoning = Some(pioneer_protocol::TurnReasoningSelection {
                effort: effort.into(),
            });
            let resolved =
                resolve_agent_launch_selection(&launch, &options, Some(&api), Some(&original))
                    .unwrap();
            assert_eq!(resolved, (identity.clone(), selected.clone()));
            launch.execution.reasoning = Some(pioneer_protocol::TurnReasoningSelection {
                effort: "unsupported".into(),
            });
            assert!(
                resolve_agent_launch_selection(&launch, &options, Some(&api), Some(&original))
                    .is_err()
            );
            launch.execution.reasoning = None;
            launch.execution.profile = AgentExecutionProfileSelection::Model {
                provider_id: Some("unavailable-executor".into()),
                model_id: selected.model_id.clone(),
            };
            assert!(
                resolve_agent_launch_selection(&launch, &options, Some(&api), Some(&original))
                    .is_err()
            );
            launch.execution.profile = AgentExecutionProfileSelection::Model {
                provider_id: None,
                model_id: "unavailable-model".into(),
            };
            assert!(
                resolve_agent_launch_selection(&launch, &options, Some(&api), Some(&original))
                    .is_err()
            );
        }
        let mut launch = selection();
        launch.agent = AgentIdentitySelection::Exact {
            agent_identity_id: cli.id.clone(),
        };
        launch.execution.profile = AgentExecutionProfileSelection::InheritParent;
        assert_eq!(
            resolve_agent_launch_selection(&launch, &options, Some(&api), Some(&original))
                .unwrap()
                .1,
            cli_profile
        );
        let mut ambiguous = options.clone();
        let mut another_cli = cli_profile.clone();
        another_cli.id = AgentExecutionProfileId::new("P12345678901234567893").unwrap();
        another_cli.model_id = "another-cli-model".into();
        ambiguous.profiles.push(another_cli);
        assert!(
            resolve_agent_launch_selection(&launch, &ambiguous, Some(&api), Some(&original))
                .is_err()
        );
        launch.agent = AgentIdentitySelection::Exact {
            agent_identity_id: AgentIdentityId::new("A12345678901234567899").unwrap(),
        };
        assert!(
            resolve_agent_launch_selection(&launch, &options, Some(&api), Some(&original)).is_err()
        );
    }

    #[test]
    fn ephemeral_child_replaces_the_same_profile_projection_without_duplicate_ids() {
        let root_identity = identity(
            "A12345678901234567890",
            AgentIdentitySourceKind::NativeAgent,
        );
        let root_profile = profile("P12345678901234567890", &root_identity);
        let options = AgentStartOptionsProjection {
            agents: vec![root_identity],
            inherit_parent_agent_available: true,
            derived_ephemeral_available: true,
            profiles: vec![root_profile],
            inherit_parent_profile_available: true,
            allowed_skill_ids: Vec::new(),
            allowed_mcp_server_ids: Vec::new(),
            max_permission_profile: pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
            ),
            generation_fingerprint: "generation-1".to_owned(),
        };
        let parent = materialize_root_execution(
            &selection(),
            &options,
            "thread-root",
            "thread_agent",
            1,
            RoleDefinitionRegistry::new(),
        )
        .unwrap();
        let ephemeral = AgentLaunchSelection {
            agent: AgentIdentitySelection::ServerDerivedEphemeral {
                display_name_hint: Some("Researcher".to_owned()),
                role_label: Some("Researcher".to_owned()),
            },
            execution: AgentExecutionSelection {
                profile: AgentExecutionProfileSelection::Exact {
                    profile_id: AgentExecutionProfileId::new("P12345678901234567890").unwrap(),
                },
                reasoning: None,
                permission_profile: None,
                skill_ids: Vec::new(),
                mcp_server_ids: Vec::new(),
                selected_capabilities: Vec::new(),
            },
        };

        let child = derive_child_launch_grant(
            &parent,
            AgentExecutionId::new("E12345678901234567891").unwrap(),
            &ephemeral,
            &options,
            AgentStartTarget::CurrentThread,
            None,
            1,
            false,
            "branch-ephemeral",
            &AgentWorkResourcePolicy::default(),
            RoleDefinitionRegistry::new(),
        )
        .unwrap();

        child.child_launch_ceiling.validate().unwrap();
        assert!(
            child
                .profile
                .compatible_agent_identity_ids
                .contains(&child.identity.id)
        );
    }

    #[test]
    fn inherited_child_resolves_current_source_without_mutating_parent_snapshot() {
        let original = identity(
            "A12345678901234567890",
            AgentIdentitySourceKind::NativeAgent,
        );
        let profile = profile("P12345678901234567890", &original);
        let mut options = AgentStartOptionsProjection {
            agents: vec![original.clone()],
            profiles: vec![profile.clone()],
            inherit_parent_agent_available: true,
            derived_ephemeral_available: false,
            inherit_parent_profile_available: true,
            allowed_skill_ids: Vec::new(),
            allowed_mcp_server_ids: Vec::new(),
            max_permission_profile: pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
            ),
            generation_fingerprint: "generation".to_owned(),
        };
        let parent = materialize_root_execution(
            &selection(),
            &options,
            "thread-root",
            "thread_agent",
            1,
            RoleDefinitionRegistry::new(),
        )
        .unwrap();
        let mut current = original.clone();
        current.source_revision = 9;
        current.source_fingerprint = "current-settings".to_owned();
        options.agents = vec![current.clone()];
        let mut equivalent = profile.clone();
        equivalent.id = AgentExecutionProfileId::new("P12345678901234567889").unwrap();
        options.profiles.insert(0, equivalent);
        let mut inherited = selection();
        inherited.agent = AgentIdentitySelection::InheritParent;
        inherited.execution.profile = AgentExecutionProfileSelection::InheritParent;
        let child = derive_child_launch_grant(
            &parent,
            AgentExecutionId::new("E12345678901234567891").unwrap(),
            &inherited,
            &options,
            AgentStartTarget::CurrentThread,
            None,
            1,
            false,
            "branch",
            &AgentWorkResourcePolicy::default(),
            RoleDefinitionRegistry::new(),
        )
        .unwrap();
        assert_eq!(child.identity, current);
        assert_eq!(child.profile, profile);
        assert_eq!(parent.identity, original);
        child.child_launch_ceiling.validate().unwrap();
        options.agents.clear();
        assert!(
            derive_child_launch_grant(
                &parent,
                AgentExecutionId::new("E12345678901234567891").unwrap(),
                &inherited,
                &options,
                AgentStartTarget::CurrentThread,
                None,
                1,
                false,
                "branch",
                &AgentWorkResourcePolicy::default(),
                RoleDefinitionRegistry::new(),
            )
            .is_err()
        );
    }
}
