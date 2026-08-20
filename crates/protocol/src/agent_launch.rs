use crate::{
    AgentDelegationRouteId, AgentExecutionProfileId, AgentIdentityId, AgentIdentityProjection,
    SkillId, ThreadMode, TurnPermissionMode, TurnPermissionProfileCap,
    TurnPermissionProfileSelection, TurnReasoningSelection, UserInput,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// Input authored by an agent for the child turn. Configuration and actor
/// identity are deliberately not part of this value.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(transparent)]
pub struct AgentAuthoredInput(pub Vec<UserInput>);

impl AgentAuthoredInput {
    pub fn new(input: Vec<UserInput>) -> Self {
        Self(input)
    }

    pub fn as_slice(&self) -> &[UserInput] {
        self.0.as_slice()
    }

    /// Only clean authored content may cross the agent action boundary. Raw
    /// host paths, URLs, mentions and runtime attachments are resolved by the
    /// server before an intent is built and therefore cannot be smuggled in
    /// as model-authored content.
    pub fn validate_visible(&self) -> Result<(), AgentAuthoredInputError> {
        if self.0.len() > crate::TURN_EXECUTION_INPUT_MAX_ITEMS {
            return Err(AgentAuthoredInputError::PayloadLimitExceeded);
        }
        let mut text_elements = 0usize;
        let mut attachment_references = 0usize;
        for input in &self.0 {
            match input {
                UserInput::Text {
                    text,
                    text_elements: elements,
                } => {
                    if text.len() > crate::TURN_EXECUTION_INPUT_MAX_BYTES {
                        return Err(AgentAuthoredInputError::PayloadLimitExceeded);
                    }
                    text_elements = text_elements.saturating_add(elements.len());
                }
                UserInput::Artifact {
                    artifact_id,
                    version_id: Some(version_id),
                } => {
                    if artifact_id.is_empty() || version_id.is_empty() {
                        return Err(AgentAuthoredInputError::ArtifactVersionRequired);
                    }
                    if artifact_id.len() > 255 || version_id.len() > 255 {
                        return Err(AgentAuthoredInputError::PayloadLimitExceeded);
                    }
                    if artifact_id.trim() != artifact_id
                        || version_id.trim() != version_id
                        || artifact_id.chars().any(char::is_control)
                        || version_id.chars().any(char::is_control)
                    {
                        return Err(AgentAuthoredInputError::RuntimeOrUnresolvedInput);
                    }
                    attachment_references = attachment_references.saturating_add(1);
                }
                UserInput::Artifact { .. } => {
                    return Err(AgentAuthoredInputError::ArtifactVersionRequired);
                }
                UserInput::Image { .. }
                | UserInput::LocalImage { .. }
                | UserInput::File { .. }
                | UserInput::LocalFile { .. }
                | UserInput::Audio { .. }
                | UserInput::LocalAudio { .. }
                | UserInput::Video { .. }
                | UserInput::LocalVideo { .. }
                | UserInput::Mention { .. } => {
                    return Err(AgentAuthoredInputError::RuntimeOrUnresolvedInput);
                }
            }
        }
        if text_elements > crate::TURN_EXECUTION_TEXT_ELEMENT_MAX_COUNT
            || attachment_references > crate::TURN_EXECUTION_ATTACHMENT_REFERENCE_MAX_COUNT
        {
            return Err(AgentAuthoredInputError::PayloadLimitExceeded);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentAuthoredInputError {
    ArtifactVersionRequired,
    RuntimeOrUnresolvedInput,
    PayloadLimitExceeded,
}

impl From<Vec<UserInput>> for AgentAuthoredInput {
    fn from(input: Vec<UserInput>) -> Self {
        Self::new(input)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentIdentitySelection {
    InheritParent,
    DefaultPioneer,
    Exact {
        agent_identity_id: AgentIdentityId,
    },
    ServerDerivedEphemeral {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_name_hint: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role_label: Option<String>,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentExecutionProfileBackend {
    ApiProvider,
    CliRuntime { runtime_instance_id: String },
    AcpAgentRuntime { runtime_id: String },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentExecutionProfileProjection {
    pub id: AgentExecutionProfileId,
    pub compatible_agent_identity_ids: Vec<AgentIdentityId>,
    pub backend: AgentExecutionProfileBackend,
    pub provider_id: String,
    pub model_id: String,
    pub provider_display_name: String,
    pub model_display_name: String,
    #[serde(default)]
    pub allowed_reasoning: Vec<TurnReasoningSelection>,
    #[serde(default)]
    pub allowed_permission_profiles: Vec<TurnPermissionMode>,
    pub catalog_generation: u64,
    pub policy_generation: u64,
    pub fingerprint: String,
}

/// Canonical exact reasoning selections that remain delegable by an admitted
/// execution. Effort labels are backend-owned opaque values, so ordering them
/// by presumed strength would be unsafe; the ceiling is an allow-set.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReasoningCeiling {
    pub allowed: Vec<TurnReasoningSelection>,
}

/// Immutable upper bound for identities and execution profiles that an
/// admitted AgentExecution may delegate to. Current source/policy state may
/// narrow this set, but a later workspace catalog addition can never widen it.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChildAgentLaunchGrantSet {
    pub version: u32,
    pub identities: Vec<AgentIdentityProjection>,
    pub allow_inherit_parent_identity: bool,
    pub allow_server_derived_ephemeral: bool,
    pub profiles: Vec<AgentExecutionProfileProjection>,
    pub allow_inherit_parent_profile: bool,
    #[serde(default)]
    pub skill_ids: Vec<SkillId>,
    #[serde(default)]
    pub mcp_server_ids: Vec<String>,
    pub max_permission_profile: TurnPermissionProfileCap,
    pub max_reasoning: ReasoningCeiling,
    pub fingerprint: String,
}

impl ChildAgentLaunchGrantSet {
    pub const VERSION: u32 = 1;
    pub const MAX_IDENTITIES: usize = 128;
    pub const MAX_PROFILES: usize = 256;
    pub const MAX_SKILLS: usize = 256;
    pub const MAX_MCP_SERVERS: usize = 256;

    pub fn new(
        mut identities: Vec<AgentIdentityProjection>,
        mut profiles: Vec<AgentExecutionProfileProjection>,
    ) -> Result<Self, ChildAgentLaunchGrantSetError> {
        identities.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        profiles.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        validate_child_launch_grant_entries(&identities, &profiles)?;
        let max_reasoning = reasoning_ceiling_for_profiles(&profiles);
        let mut grant = Self {
            version: Self::VERSION,
            identities,
            allow_inherit_parent_identity: false,
            allow_server_derived_ephemeral: false,
            profiles,
            allow_inherit_parent_profile: false,
            skill_ids: Vec::new(),
            mcp_server_ids: Vec::new(),
            max_permission_profile: crate::task_permission_cap_for_mode(
                TurnPermissionMode::Supervised,
            ),
            max_reasoning,
            fingerprint: String::new(),
        };
        grant.fingerprint = child_launch_grant_fingerprint(&grant)?;
        Ok(grant)
    }

    pub fn with_policy(
        mut self,
        allow_inherit_parent_identity: bool,
        allow_server_derived_ephemeral: bool,
        allow_inherit_parent_profile: bool,
        mut skill_ids: Vec<SkillId>,
        mut mcp_server_ids: Vec<String>,
        max_permission_profile: TurnPermissionProfileCap,
    ) -> Result<Self, ChildAgentLaunchGrantSetError> {
        skill_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        mcp_server_ids.sort();
        self.allow_inherit_parent_identity = allow_inherit_parent_identity;
        self.allow_server_derived_ephemeral = allow_server_derived_ephemeral;
        self.allow_inherit_parent_profile = allow_inherit_parent_profile;
        self.skill_ids = skill_ids;
        self.mcp_server_ids = mcp_server_ids;
        self.max_permission_profile = max_permission_profile;
        validate_child_launch_grant(&self)?;
        self.fingerprint = child_launch_grant_fingerprint(&self)?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), ChildAgentLaunchGrantSetError> {
        validate_child_launch_grant(self)?;
        if self.fingerprint != child_launch_grant_fingerprint(self)? {
            return Err(ChildAgentLaunchGrantSetError::FingerprintMismatch);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChildAgentLaunchGrantSetError {
    UnsupportedVersion,
    EmptyIdentitySet,
    CapacityExceeded,
    DuplicateIdentity,
    DuplicateProfile,
    InvalidIdentity,
    InvalidProfile,
    ProfileOutsideIdentitySet,
    DuplicateCapability,
    InvalidCapability,
    Serialization,
    FingerprintMismatch,
}

fn validate_child_launch_grant(
    grant: &ChildAgentLaunchGrantSet,
) -> Result<(), ChildAgentLaunchGrantSetError> {
    if grant.version != ChildAgentLaunchGrantSet::VERSION {
        return Err(ChildAgentLaunchGrantSetError::UnsupportedVersion);
    }
    validate_child_launch_grant_entries(&grant.identities, &grant.profiles)?;
    if grant.skill_ids.len() > ChildAgentLaunchGrantSet::MAX_SKILLS
        || grant.mcp_server_ids.len() > ChildAgentLaunchGrantSet::MAX_MCP_SERVERS
    {
        return Err(ChildAgentLaunchGrantSetError::CapacityExceeded);
    }
    if grant
        .skill_ids
        .windows(2)
        .any(|pair| pair[0].as_str() >= pair[1].as_str())
        || grant
            .mcp_server_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(ChildAgentLaunchGrantSetError::DuplicateCapability);
    }
    if grant
        .mcp_server_ids
        .iter()
        .any(|id| id.trim().is_empty() || id.len() > 255)
    {
        return Err(ChildAgentLaunchGrantSetError::InvalidCapability);
    }
    if grant.max_reasoning != reasoning_ceiling_for_profiles(&grant.profiles) {
        return Err(ChildAgentLaunchGrantSetError::InvalidProfile);
    }
    Ok(())
}

fn reasoning_ceiling_for_profiles(
    profiles: &[AgentExecutionProfileProjection],
) -> ReasoningCeiling {
    let mut allowed = profiles
        .iter()
        .flat_map(|profile| profile.allowed_reasoning.iter().cloned())
        .collect::<Vec<_>>();
    allowed.sort_by(|left, right| left.effort.cmp(&right.effort));
    allowed.dedup_by(|left, right| left.effort == right.effort);
    ReasoningCeiling { allowed }
}

fn validate_child_launch_grant_entries(
    identities: &[AgentIdentityProjection],
    profiles: &[AgentExecutionProfileProjection],
) -> Result<(), ChildAgentLaunchGrantSetError> {
    if identities.is_empty() {
        return Err(ChildAgentLaunchGrantSetError::EmptyIdentitySet);
    }
    if identities.len() > ChildAgentLaunchGrantSet::MAX_IDENTITIES
        || profiles.len() > ChildAgentLaunchGrantSet::MAX_PROFILES
    {
        return Err(ChildAgentLaunchGrantSetError::CapacityExceeded);
    }
    if identities
        .windows(2)
        .any(|pair| pair[0].id.as_str() >= pair[1].id.as_str())
    {
        return Err(ChildAgentLaunchGrantSetError::DuplicateIdentity);
    }
    if profiles
        .windows(2)
        .any(|pair| pair[0].id.as_str() >= pair[1].id.as_str())
    {
        return Err(ChildAgentLaunchGrantSetError::DuplicateProfile);
    }
    if identities.iter().any(|identity| {
        identity.source_revision == 0
            || identity.source_fingerprint.trim().is_empty()
            || identity.display_name.trim().is_empty()
            || identity.nickname.trim().is_empty()
    }) {
        return Err(ChildAgentLaunchGrantSetError::InvalidIdentity);
    }
    let identity_ids = identities
        .iter()
        .map(|identity| &identity.id)
        .collect::<std::collections::BTreeSet<_>>();
    for profile in profiles {
        if profile.catalog_generation == 0
            || profile.policy_generation == 0
            || profile.fingerprint.trim().is_empty()
            || profile.provider_id.trim().is_empty()
            || profile.model_id.trim().is_empty()
            || profile.compatible_agent_identity_ids.is_empty()
            || profile
                .compatible_agent_identity_ids
                .windows(2)
                .any(|pair| pair[0].as_str() >= pair[1].as_str())
        {
            return Err(ChildAgentLaunchGrantSetError::InvalidProfile);
        }
        if profile
            .compatible_agent_identity_ids
            .iter()
            .any(|identity_id| !identity_ids.contains(identity_id))
        {
            return Err(ChildAgentLaunchGrantSetError::ProfileOutsideIdentitySet);
        }
    }
    Ok(())
}

fn child_launch_grant_fingerprint(
    grant: &ChildAgentLaunchGrantSet,
) -> Result<String, ChildAgentLaunchGrantSetError> {
    let canonical = serde_json::to_vec(&(
        grant.version,
        &grant.identities,
        grant.allow_inherit_parent_identity,
        grant.allow_server_derived_ephemeral,
        &grant.profiles,
        grant.allow_inherit_parent_profile,
        &grant.skill_ids,
        &grant.mcp_server_ids,
        &grant.max_permission_profile,
        &grant.max_reasoning,
    ))
    .map_err(|_| ChildAgentLaunchGrantSetError::Serialization)?;
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:child-launch-grant:v1\0");
    digest.update(canonical);
    Ok(hex::encode(digest.finalize()))
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentExecutionProfileSelection {
    InheritParent,
    Exact { profile_id: AgentExecutionProfileId },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentExecutionSelection {
    pub profile: AgentExecutionProfileSelection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<TurnReasoningSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_profile: Option<TurnPermissionProfileSelection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skill_ids: Vec<SkillId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp_server_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentLaunchSelection {
    pub agent: AgentIdentitySelection,
    pub execution: AgentExecutionSelection,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentStartTarget {
    CurrentThread,
    SameCapsuleThread {
        thread_id: String,
    },
    RoutedThread {
        route_id: AgentDelegationRouteId,
        thread_id: String,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartAgentIntent {
    pub target: AgentStartTarget,
    pub input: AgentAuthoredInput,
    pub launch: AgentLaunchSelection,
}

impl StartAgentIntent {
    pub const fn thread_mode(&self) -> ThreadMode {
        ThreadMode::Agent
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentStartOptionsProjection {
    pub agents: Vec<AgentIdentityProjection>,
    pub inherit_parent_agent_available: bool,
    pub derived_ephemeral_available: bool,
    #[serde(default)]
    pub profiles: Vec<AgentExecutionProfileProjection>,
    pub inherit_parent_profile_available: bool,
    #[serde(default)]
    pub allowed_skill_ids: Vec<SkillId>,
    #[serde(default)]
    pub allowed_mcp_server_ids: Vec<String>,
    pub max_permission_profile: TurnPermissionProfileCap,
    pub generation_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentLaunchSelectionError {
    ExactProfileRequired,
}

impl fmt::Display for AgentLaunchSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExactProfileRequired => formatter
                .write_str("an exact execution profile is required without an inherited profile"),
        }
    }
}

impl std::error::Error for AgentLaunchSelectionError {}

impl AgentLaunchSelection {
    /// Validate the only context-dependent rule in the wire contract:
    /// InheritParent is legal only when an exact parent profile is available.
    pub fn validate(
        &self,
        inherited_exact_profile: Option<&AgentExecutionProfileId>,
    ) -> Result<(), AgentLaunchSelectionError> {
        if matches!(
            self.execution.profile,
            AgentExecutionProfileSelection::InheritParent
        ) && inherited_exact_profile.is_none()
        {
            return Err(AgentLaunchSelectionError::ExactProfileRequired);
        }
        Ok(())
    }
}

impl AgentStartOptionsProjection {
    pub fn has_agent(&self, id: &AgentIdentityId) -> bool {
        self.agents.iter().any(|agent| &agent.id == id)
    }

    pub fn profile_is_compatible(
        &self,
        profile_id: &AgentExecutionProfileId,
        identity_id: &AgentIdentityId,
    ) -> bool {
        self.profiles.iter().any(|profile| {
            &profile.id == profile_id
                && profile
                    .compatible_agent_identity_ids
                    .iter()
                    .any(|candidate| candidate == identity_id)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn identity_id(value: &str) -> AgentIdentityId {
        AgentIdentityId::new(value.to_owned()).expect("valid identity id")
    }

    fn profile_id(value: &str) -> AgentExecutionProfileId {
        AgentExecutionProfileId::new(value.to_owned()).expect("valid profile id")
    }

    fn launch(profile: AgentExecutionProfileSelection) -> AgentLaunchSelection {
        AgentLaunchSelection {
            agent: AgentIdentitySelection::DefaultPioneer,
            execution: AgentExecutionSelection {
                profile,
                reasoning: None,
                permission_profile: None,
                skill_ids: Vec::new(),
                mcp_server_ids: Vec::new(),
            },
        }
    }

    #[test]
    fn start_agent_intent_is_agent_mode_and_has_no_raw_execution_fields() {
        let intent = StartAgentIntent {
            target: AgentStartTarget::CurrentThread,
            input: AgentAuthoredInput::from(vec![UserInput::Text {
                text: "delegate this".to_owned(),
                text_elements: Vec::new(),
            }]),
            launch: launch(AgentExecutionProfileSelection::Exact {
                profile_id: profile_id("P00000000000000000001"),
            }),
        };
        assert_eq!(intent.thread_mode(), ThreadMode::Agent);
        let encoded = serde_json::to_value(&intent).expect("intent should encode");
        let object = encoded.as_object().expect("intent object");
        for forbidden in [
            "actor",
            "username",
            "principal_id",
            "provider",
            "provider_id",
            "model",
            "model_id",
            "credentials",
            "config",
            "mode",
        ] {
            assert!(
                !object.contains_key(forbidden),
                "raw field {forbidden} must not be accepted"
            );
        }
        assert!(
            serde_json::from_value::<StartAgentIntent>(json!({
                "target": "current_thread",
                "input": [],
                "launch": {
                    "agent": { "exact": { "agent_identity_id": "P00000000000000000001" } },
                    "execution": {
                        "profile": { "exact": { "profile_id": "P00000000000000000001" } },
                        "provider": "openai",
                        "model": "o4-mini"
                    }
                }
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<StartAgentIntent>(json!({
                "target": {
                    "same_capsule_thread": {
                        "thread_id": "thread-opaque",
                        "actor": "forged"
                    }
                },
                "input": [],
                "launch": {
                    "agent": "default_pioneer",
                    "execution": {
                        "profile": { "exact": { "profile_id": "P00000000000000000001" } }
                    }
                }
            }))
            .is_err()
        );
    }

    #[test]
    fn launch_selection_requires_exact_profile_without_parent() {
        let inherited = launch(AgentExecutionProfileSelection::InheritParent);
        assert!(matches!(
            inherited.validate(None),
            Err(AgentLaunchSelectionError::ExactProfileRequired)
        ));
        assert!(
            inherited
                .validate(Some(&profile_id("P00000000000000000001")))
                .is_ok()
        );

        let exact = launch(AgentExecutionProfileSelection::Exact {
            profile_id: profile_id("P00000000000000000002"),
        });
        assert!(exact.validate(None).is_ok());
    }

    #[test]
    fn options_projection_uses_opaque_ids_and_checks_compatibility() {
        let identity = AgentIdentityProjection::pioneer(
            identity_id("A00000000000000000001"),
            3,
            "generation-a",
        );
        let profile = AgentExecutionProfileProjection {
            id: profile_id("P00000000000000000003"),
            compatible_agent_identity_ids: vec![identity.id.clone()],
            backend: AgentExecutionProfileBackend::ApiProvider,
            provider_id: "provider-opaque".to_owned(),
            model_id: "model-opaque".to_owned(),
            provider_display_name: "Provider".to_owned(),
            model_display_name: "Model".to_owned(),
            allowed_reasoning: vec![TurnReasoningSelection {
                effort: "medium".to_owned(),
            }],
            allowed_permission_profiles: vec![TurnPermissionMode::FullAccess],
            catalog_generation: 4,
            policy_generation: 5,
            fingerprint: "profile-fingerprint".to_owned(),
        };
        let options = AgentStartOptionsProjection {
            agents: vec![identity.clone()],
            inherit_parent_agent_available: true,
            derived_ephemeral_available: true,
            profiles: vec![profile.clone()],
            inherit_parent_profile_available: false,
            allowed_skill_ids: Vec::new(),
            allowed_mcp_server_ids: vec!["mcp-opaque".to_owned()],
            max_permission_profile: crate::task_permission_cap_for_mode(
                TurnPermissionMode::FullAccess,
            ),
            generation_fingerprint: "options-fingerprint".to_owned(),
        };
        assert!(options.has_agent(&identity.id));
        assert!(options.profile_is_compatible(&profile.id, &identity.id));
        let json = serde_json::to_value(options).expect("options should encode");
        let text = serde_json::to_string(&json).expect("options json");
        assert!(!text.contains("credentials"));
        assert!(!text.contains("system_prompt"));
        assert!(!text.contains("binary_path"));
        assert!(matches!(json, Value::Object(_)));
    }

    #[test]
    fn child_launch_grant_is_canonical_and_detects_tampering() {
        let identity = AgentIdentityProjection::pioneer(
            identity_id("A00000000000000000001"),
            3,
            "generation-a",
        );
        let profile = AgentExecutionProfileProjection {
            id: profile_id("P00000000000000000003"),
            compatible_agent_identity_ids: vec![identity.id.clone()],
            backend: AgentExecutionProfileBackend::ApiProvider,
            provider_id: "provider-opaque".to_owned(),
            model_id: "model-opaque".to_owned(),
            provider_display_name: "Provider".to_owned(),
            model_display_name: "Model".to_owned(),
            allowed_reasoning: vec![TurnReasoningSelection {
                effort: "medium".to_owned(),
            }],
            allowed_permission_profiles: vec![TurnPermissionMode::FullAccess],
            catalog_generation: 4,
            policy_generation: 5,
            fingerprint: "profile-fingerprint".to_owned(),
        };
        let grant = ChildAgentLaunchGrantSet::new(vec![identity], vec![profile]).unwrap();
        assert!(grant.validate().is_ok());
        assert_eq!(
            grant.max_reasoning.allowed,
            vec![TurnReasoningSelection {
                effort: "medium".to_owned(),
            }]
        );

        let mut tampered_reasoning = grant.clone();
        tampered_reasoning.max_reasoning.allowed = vec![TurnReasoningSelection {
            effort: "high".to_owned(),
        }];
        assert_eq!(
            tampered_reasoning.validate(),
            Err(ChildAgentLaunchGrantSetError::InvalidProfile)
        );

        let mut tampered = grant;
        tampered.profiles[0].model_id = "other-model".to_owned();
        assert_eq!(
            tampered.validate(),
            Err(ChildAgentLaunchGrantSetError::FingerprintMismatch)
        );
    }

    #[test]
    fn launch_contracts_are_exposed_by_authoritative_schema_registry() {
        let names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<std::collections::HashSet<_>>();
        for required in [
            "agent_launch_selection.json",
            "agent_start_target.json",
            "agent_start_options_projection.json",
            "agent_execution_profile_projection.json",
            "child_agent_launch_grant_set.json",
            "start_agent_intent.json",
        ] {
            assert!(names.contains(required), "schema missing {required}");
        }
    }
}
