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
    pub const VERSION: u32 = CHILD_LAUNCH_GRANT_VERSION_V2;
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
        validate_child_launch_grant_v2(self)
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
    validate_child_launch_grant_shape(grant)
}

fn validate_child_launch_grant_shape(
    grant: &ChildAgentLaunchGrantSet,
) -> Result<(), ChildAgentLaunchGrantSetError> {
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

fn validate_child_launch_grant_v2(
    grant: &ChildAgentLaunchGrantSet,
) -> Result<(), ChildAgentLaunchGrantSetError> {
    if grant.version != CHILD_LAUNCH_GRANT_VERSION_V2 {
        return Err(ChildAgentLaunchGrantSetError::UnsupportedVersion);
    }
    validate_child_launch_grant_shape(grant)?;
    if grant.fingerprint != child_launch_grant_v2_fingerprint(grant)? {
        return Err(ChildAgentLaunchGrantSetError::FingerprintMismatch);
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
    child_launch_grant_v2_fingerprint(grant)
}

const CHILD_LAUNCH_GRANT_VERSION_V1: u32 = 1;
const CHILD_LAUNCH_GRANT_VERSION_V2: u32 = 2;

// Versioned fingerprint projections are immutable. A semantic field addition
// requires a new version, canonicalizer and adjacent migration step; editing a
// released projection would invalidate durable grants again.
fn child_launch_grant_v2_fingerprint(
    grant: &ChildAgentLaunchGrantSet,
) -> Result<String, ChildAgentLaunchGrantSetError> {
    let value =
        serde_json::to_value(grant).map_err(|_| ChildAgentLaunchGrantSetError::Serialization)?;
    // Decode through the frozen V2 wire schema before hashing. Any future
    // field added to the moving runtime type is rejected here and therefore
    // requires V3 instead of silently changing the V2 fingerprint contract.
    let wire = serde_json::from_value::<ChildLaunchGrantWire<PermissionPolicyV2Wire>>(value)
        .map_err(|_| ChildAgentLaunchGrantSetError::Serialization)?;
    child_launch_grant_wire_fingerprint(&wire, b"pioneer:agent-runtime:child-launch-grant:v2\0")
        .map_err(|_| ChildAgentLaunchGrantSetError::Serialization)
}

fn is_false(value: &bool) -> bool {
    !*value
}

// Released migration steps decode their own immutable wire schema. They must
// never deserialize through `ChildAgentLaunchGrantSet`, because that type is
// the moving current schema and a future V3 field must not change how V1 is
// authenticated or transformed into V2.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ChildLaunchGrantWire<P> {
    version: u32,
    identities: Vec<IdentityWire>,
    allow_inherit_parent_identity: bool,
    allow_server_derived_ephemeral: bool,
    profiles: Vec<ProfileWire>,
    allow_inherit_parent_profile: bool,
    #[serde(default)]
    skill_ids: Vec<String>,
    #[serde(default)]
    mcp_server_ids: Vec<String>,
    max_permission_profile: PermissionCapWire<P>,
    max_reasoning: ReasoningCeilingWire,
    fingerprint: String,
}

impl<P> ChildLaunchGrantWire<P> {
    fn try_map_permission_policy<Q>(
        self,
        map: impl FnOnce(PermissionCapWire<P>) -> Result<PermissionCapWire<Q>, String>,
    ) -> Result<ChildLaunchGrantWire<Q>, String> {
        Ok(ChildLaunchGrantWire {
            version: self.version,
            identities: self.identities,
            allow_inherit_parent_identity: self.allow_inherit_parent_identity,
            allow_server_derived_ephemeral: self.allow_server_derived_ephemeral,
            profiles: self.profiles,
            allow_inherit_parent_profile: self.allow_inherit_parent_profile,
            skill_ids: self.skill_ids,
            mcp_server_ids: self.mcp_server_ids,
            max_permission_profile: map(self.max_permission_profile)?,
            max_reasoning: self.max_reasoning,
            fingerprint: self.fingerprint,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityWire {
    id: String,
    source_kind: String,
    display_name: String,
    nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    avatar_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    role_label: Option<String>,
    source_revision: u64,
    source_fingerprint: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum ProfileBackendWire {
    ApiProvider,
    CliRuntime { runtime_instance_id: String },
    AcpAgentRuntime { runtime_id: String },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReasoningWire {
    effort: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProfileWire {
    id: String,
    compatible_agent_identity_ids: Vec<String>,
    backend: ProfileBackendWire,
    provider_id: String,
    model_id: String,
    provider_display_name: String,
    model_display_name: String,
    #[serde(default)]
    allowed_reasoning: Vec<ReasoningWire>,
    #[serde(default)]
    allowed_permission_profiles: Vec<String>,
    catalog_generation: u64,
    policy_generation: u64,
    fingerprint: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PermissionCapWire<P> {
    mode: String,
    effective_policy: P,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReasoningCeilingWire {
    allowed: Vec<ReasoningWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PermissionPolicyV1Wire {
    default_behavior: String,
    file_read: String,
    file_write: String,
    shell_command: String,
    network: String,
    mcp_read: String,
    mcp_write_or_unknown: String,
    dynamic_skill_tool: String,
    computer_use: String,
    task_subagent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_write: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_action: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    allowed_tools_restricted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    denied_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    allowed_paths_restricted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    allowed_paths: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PermissionPolicyV2Wire {
    default_behavior: String,
    file_read: String,
    file_write: String,
    shell_command: String,
    network: String,
    mcp_read: String,
    mcp_write_or_unknown: String,
    dynamic_skill_tool: String,
    computer_use: String,
    task_subagent: String,
    memory_write: String,
    agent_action: String,
    #[serde(default, skip_serializing_if = "is_false")]
    allowed_tools_restricted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    denied_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    allowed_paths_restricted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    allowed_paths: Vec<String>,
}

impl PermissionPolicyV2Wire {
    fn normalize_version_one_restrictions(&mut self) {
        // V1 readers treated every non-empty legacy allow-list as restricted,
        // even when the explicit restriction bit was absent or false. Preserve
        // that effective V1 meaning before assigning a V2 fingerprint.
        self.allowed_tools_restricted |= !self.allowed_tools.is_empty();
        self.allowed_paths_restricted |= !self.allowed_paths.is_empty();
    }
}

impl TryFrom<PermissionCapWire<PermissionPolicyV1Wire>>
    for PermissionCapWire<PermissionPolicyV2Wire>
{
    type Error = String;

    fn try_from(cap: PermissionCapWire<PermissionPolicyV1Wire>) -> Result<Self, Self::Error> {
        let policy = cap.effective_policy;
        let inherited_behavior = policy.default_behavior.clone();
        let (memory_write, agent_action) = match (policy.memory_write, policy.agent_action) {
            (Some(memory_write), Some(agent_action)) => (memory_write, agent_action),
            (None, None) => (inherited_behavior.clone(), inherited_behavior.clone()),
            _ => {
                return Err("version 1 permission policy is only partially populated".to_owned());
            }
        };
        Ok(Self {
            mode: cap.mode,
            effective_policy: PermissionPolicyV2Wire {
                default_behavior: policy.default_behavior,
                file_read: policy.file_read,
                file_write: policy.file_write,
                shell_command: policy.shell_command,
                network: policy.network,
                mcp_read: policy.mcp_read,
                mcp_write_or_unknown: policy.mcp_write_or_unknown,
                dynamic_skill_tool: policy.dynamic_skill_tool,
                computer_use: policy.computer_use,
                task_subagent: policy.task_subagent,
                memory_write,
                agent_action,
                allowed_tools_restricted: policy.allowed_tools_restricted
                    || !policy.allowed_tools.is_empty(),
                allowed_tools: policy.allowed_tools,
                denied_tools: policy.denied_tools,
                allowed_paths_restricted: policy.allowed_paths_restricted
                    || !policy.allowed_paths.is_empty(),
                allowed_paths: policy.allowed_paths,
            },
        })
    }
}

fn child_launch_grant_wire_fingerprint<P: Serialize>(
    grant: &ChildLaunchGrantWire<P>,
    domain: &[u8],
) -> Result<String, String> {
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
    .map_err(|error| format!("could not canonicalize child launch grant: {error}"))?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(canonical);
    Ok(hex::encode(digest.finalize()))
}

fn verify_child_launch_grant_v1_wire<P: Serialize>(
    grant: &ChildLaunchGrantWire<P>,
) -> Result<(), String> {
    if grant.version != CHILD_LAUNCH_GRANT_VERSION_V1 {
        return Err(format!(
            "expected child launch grant version 1, found {}",
            grant.version
        ));
    }
    let expected = child_launch_grant_wire_fingerprint(
        grant,
        b"pioneer:agent-runtime:child-launch-grant:v1\0",
    )?;
    if grant.fingerprint != expected {
        return Err("version 1 child launch grant fingerprint does not match".to_owned());
    }
    Ok(())
}

fn promote_child_launch_grant_v1_wire(
    mut grant: ChildLaunchGrantWire<PermissionPolicyV2Wire>,
) -> Result<serde_json::Value, String> {
    grant
        .max_permission_profile
        .effective_policy
        .normalize_version_one_restrictions();
    grant.version = CHILD_LAUNCH_GRANT_VERSION_V2;
    grant.fingerprint = child_launch_grant_wire_fingerprint(
        &grant,
        b"pioneer:agent-runtime:child-launch-grant:v2\0",
    )?;
    serde_json::to_value(grant)
        .map_err(|error| format!("could not encode migrated child launch grant: {error}"))
}

const CHILD_LAUNCH_GRANT_MIGRATIONS: [crate::versioned_contract::JsonContractMigration; 1] =
    [crate::versioned_contract::JsonContractMigration {
        from_version: CHILD_LAUNCH_GRANT_VERSION_V1,
        to_version: CHILD_LAUNCH_GRANT_VERSION_V2,
        migrate: migrate_child_launch_grant_v1_to_v2,
    }];

/// Upcasts the `child_launch_grant` member of any durable outer contract.
///
/// The outer document is deliberately opaque here: Task actor contracts and
/// AgentExecution grants share the same versioned child ceiling. Persistence
/// adapters validate their own outer contract after this function returns.
/// Current documents are returned byte-for-byte so an on-read compatibility
/// path does not disturb an enclosing fingerprint.
pub fn migrate_embedded_child_launch_grant_json_to_current(
    json: &str,
) -> Result<String, ChildAgentLaunchGrantMigrationError> {
    let mut outer = serde_json::from_str::<serde_json::Value>(json)
        .map_err(|error| ChildAgentLaunchGrantMigrationError::InvalidJson(error.to_string()))?;
    let child = outer
        .get_mut("child_launch_grant")
        .ok_or(ChildAgentLaunchGrantMigrationError::MissingChildGrant)?;
    let source_version = child_launch_grant_version(child)
        .map_err(ChildAgentLaunchGrantMigrationError::InvalidPayload)?;
    let migrator = crate::versioned_contract::JsonContractMigrator::new(
        ChildAgentLaunchGrantSet::VERSION,
        child_launch_grant_version,
        &CHILD_LAUNCH_GRANT_MIGRATIONS,
    );
    *child = migrator
        .migrate_to_current(child.take())
        .map_err(|error| ChildAgentLaunchGrantMigrationError::Migration(error.to_string()))?;

    let current =
        serde_json::from_value::<ChildAgentLaunchGrantSet>(child.clone()).map_err(|error| {
            ChildAgentLaunchGrantMigrationError::InvalidCurrentContract(error.to_string())
        })?;
    current.validate().map_err(|error| {
        ChildAgentLaunchGrantMigrationError::InvalidCurrentContract(format!("{error:?}"))
    })?;

    if source_version == ChildAgentLaunchGrantSet::VERSION {
        return Ok(json.to_owned());
    }
    serde_json::to_string(&outer)
        .map_err(|error| ChildAgentLaunchGrantMigrationError::InvalidJson(error.to_string()))
}

fn child_launch_grant_version(value: &serde_json::Value) -> Result<u32, String> {
    value
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
        .ok_or_else(|| "child launch grant version must be a u32".to_owned())
}

#[derive(Debug)]
pub enum ChildAgentLaunchGrantMigrationError {
    InvalidJson(String),
    MissingChildGrant,
    InvalidPayload(String),
    Migration(String),
    InvalidCurrentContract(String),
}

impl fmt::Display for ChildAgentLaunchGrantMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson(message) => {
                write!(formatter, "invalid durable grant JSON: {message}")
            }
            Self::MissingChildGrant => formatter.write_str("durable grant has no child ceiling"),
            Self::InvalidPayload(message) => write!(formatter, "invalid child ceiling: {message}"),
            Self::Migration(error) => write!(formatter, "{error}"),
            Self::InvalidCurrentContract(message) => {
                write!(formatter, "invalid migrated child ceiling: {message}")
            }
        }
    }
}

impl std::error::Error for ChildAgentLaunchGrantMigrationError {}

pub(crate) fn migrate_child_launch_grant_v1_to_v2(
    value: serde_json::Value,
) -> Result<serde_json::Value, String> {
    // V1 was released both before and after optional permission fields were
    // appended without a version bump. The frozen V1 wire schema represents
    // that one public version directly; it is not split into invented
    // pseudo-versions. Serialization preserves the exact historical shape, so
    // the stored V1 fingerprint is still verified before any defaults apply.
    let grant = serde_json::from_value::<ChildLaunchGrantWire<PermissionPolicyV1Wire>>(value)
        .map_err(|error| format!("version 1 child launch grant is invalid: {error}"))?;
    verify_child_launch_grant_v1_wire(&grant)?;
    let grant = grant.try_map_permission_policy(|cap| {
        PermissionCapWire::<PermissionPolicyV2Wire>::try_from(cap)
    })?;
    promote_child_launch_grant_v1_wire(grant)
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

    fn child_grant() -> ChildAgentLaunchGrantSet {
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
        ChildAgentLaunchGrantSet::new(vec![identity], vec![profile]).unwrap()
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
        let grant = child_grant();
        assert!(grant.validate().is_ok());
        assert_eq!(
            grant.fingerprint,
            "1620c8eca0ca3eace03dc7d614e3af348b2f77224cde1064bcf7f492c3177047"
        );
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

    fn version_one_grant(original_policy_shape: bool) -> Value {
        let mut policy = json!({
            "default_behavior": "ask",
            "file_read": "allow",
            "file_write": "ask",
            "shell_command": "ask",
            "network": "ask",
            "mcp_read": "allow",
            "mcp_write_or_unknown": "ask",
            "dynamic_skill_tool": "ask",
            "computer_use": "ask",
            "task_subagent": "ask"
        });
        let fingerprint = if original_policy_shape {
            "fc747ed91c91bd27b7ed9378961fedb2c57de5eba831ae5bfd5e3745e29e0af3"
        } else {
            let policy = policy
                .as_object_mut()
                .expect("permission fixture should be an object");
            policy.insert("memory_write".to_owned(), json!("ask"));
            policy.insert("agent_action".to_owned(), json!("ask"));
            "3ea9513e6206d823b6988c7f258869b425c3249a5b76c8672c86454c19615c24"
        };
        json!({
            "version": 1,
            "identities": [{
                "id": "A00000000000000000001",
                "source_kind": "native_agent",
                "display_name": "Pioneer",
                "nickname": "pioneer",
                "source_revision": 3,
                "source_fingerprint": "generation-a"
            }],
            "allowInheritParentIdentity": false,
            "allowServerDerivedEphemeral": false,
            "profiles": [{
                "id": "P00000000000000000003",
                "compatibleAgentIdentityIds": ["A00000000000000000001"],
                "backend": "api_provider",
                "providerId": "provider-opaque",
                "modelId": "model-opaque",
                "providerDisplayName": "Provider",
                "modelDisplayName": "Model",
                "allowedReasoning": [{ "effort": "medium" }],
                "allowedPermissionProfiles": ["full_access"],
                "catalogGeneration": 4,
                "policyGeneration": 5,
                "fingerprint": "profile-fingerprint"
            }],
            "allowInheritParentProfile": false,
            "skillIds": [],
            "mcpServerIds": [],
            "maxPermissionProfile": {
                "mode": "supervised",
                "effective_policy": policy
            },
            "maxReasoning": { "allowed": [{ "effort": "medium" }] },
            "fingerprint": fingerprint
        })
    }

    #[test]
    fn child_launch_grant_migrates_original_version_one_to_version_two() {
        let source = version_one_grant(true);
        assert_eq!(
            source["fingerprint"],
            json!("fc747ed91c91bd27b7ed9378961fedb2c57de5eba831ae5bfd5e3745e29e0af3")
        );
        let migrated = migrate_child_launch_grant_v1_to_v2(source)
            .expect("original version 1 grant should migrate");
        let migrated: ChildAgentLaunchGrantSet =
            serde_json::from_value(migrated).expect("migrated grant should decode");
        assert_eq!(migrated.version, 2);
        assert!(migrated.validate().is_ok());
    }

    #[test]
    fn child_launch_grant_migrates_version_one_with_optional_permission_fields() {
        let source = version_one_grant(false);
        assert_eq!(
            source["fingerprint"],
            json!("3ea9513e6206d823b6988c7f258869b425c3249a5b76c8672c86454c19615c24")
        );
        let migrated = migrate_child_launch_grant_v1_to_v2(source)
            .expect("version 1 grant with optional permission fields should migrate");
        let migrated: ChildAgentLaunchGrantSet =
            serde_json::from_value(migrated).expect("migrated grant should decode");
        assert_eq!(migrated.version, 2);
        assert!(migrated.validate().is_ok());
    }

    #[test]
    fn child_launch_grant_migration_preserves_version_one_implicit_allow_list_restrictions() {
        let mut source = version_one_grant(false);
        source["maxPermissionProfile"]["effective_policy"]["allowed_tools"] = json!(["read_file"]);
        source["maxPermissionProfile"]["effective_policy"]["allowed_paths"] = json!(["/workspace"]);
        source["fingerprint"] =
            json!("f71fe2e275437c5ac4a01850a4c0f132ca725e26499d268fef19e98757f06c7d");

        let migrated = migrate_child_launch_grant_v1_to_v2(source)
            .expect("version 1 implicit restrictions should migrate");
        let migrated: ChildAgentLaunchGrantSet =
            serde_json::from_value(migrated).expect("migrated grant should decode");

        assert!(
            migrated
                .max_permission_profile
                .effective_policy
                .allowed_tools_restricted
        );
        assert!(
            migrated
                .max_permission_profile
                .effective_policy
                .allowed_paths_restricted
        );
        assert!(migrated.validate().is_ok());
    }

    #[test]
    fn child_launch_grant_migration_rejects_tampered_version_one() {
        let mut value = version_one_grant(true);
        value["profiles"][0]["modelId"] = json!("tampered-model");
        assert!(migrate_child_launch_grant_v1_to_v2(value).is_err());
    }

    #[test]
    fn child_launch_grant_migrates_version_one_cli_runtime_projection() {
        let source = json!({
            "version": 1,
            "identities": [{
                "id": "C00000000000000000001",
                "source_kind": "cli_runtime_instance",
                "display_name": "Codex",
                "nickname": "codex",
                "role_label": "codex",
                "source_revision": 1,
                "source_fingerprint": "cli-fingerprint"
            }],
            "allowInheritParentIdentity": true,
            "allowServerDerivedEphemeral": true,
            "profiles": [{
                "id": "P00000000000000000004",
                "compatibleAgentIdentityIds": ["C00000000000000000001"],
                "backend": { "cli_runtime": { "runtime_instance_id": "codex" } },
                "providerId": "cli_runtime:codex",
                "modelId": "model",
                "providerDisplayName": "Codex",
                "modelDisplayName": "Model",
                "allowedReasoning": [],
                "allowedPermissionProfiles": [
                    "full_access",
                    "auto_accept_edits",
                    "supervised"
                ],
                "catalogGeneration": 62,
                "policyGeneration": 62,
                "fingerprint": "profile-fingerprint-cli"
            }],
            "allowInheritParentProfile": true,
            "skillIds": [],
            "mcpServerIds": [],
            "maxPermissionProfile": {
                "mode": "full_access",
                "effective_policy": {
                    "default_behavior": "allow",
                    "file_read": "allow",
                    "file_write": "allow",
                    "shell_command": "allow",
                    "network": "allow",
                    "mcp_read": "allow",
                    "mcp_write_or_unknown": "allow",
                    "dynamic_skill_tool": "allow",
                    "computer_use": "allow",
                    "task_subagent": "allow"
                }
            },
            "maxReasoning": { "allowed": [] },
            "fingerprint": "f8f6969b263e02a18e6ccb6ef813a1c0f003cd32293d4d138970a9f939b44aba"
        });

        let migrated = migrate_child_launch_grant_v1_to_v2(source)
            .expect("CLI runtime version 1 grant should migrate");
        let migrated: ChildAgentLaunchGrantSet =
            serde_json::from_value(migrated).expect("migrated CLI grant should decode");
        assert_eq!(migrated.version, 2);
        assert!(migrated.validate().is_ok());
    }

    #[test]
    fn task_child_launch_migrator_is_idempotent_at_current_version() {
        let child = version_one_grant(true);
        let outer = json!({
            "kind": "resolved_task_launch",
            "identity": child["identities"][0].clone(),
            "profile": child["profiles"][0].clone(),
            "role_key": "member",
            "agent_policy_generation": 1,
            "allowed_actions": ["create_task"],
            "agent_authorization_fingerprint": "a".repeat(64),
            "child_launch_grant": child,
        });
        let migrated =
            crate::migrate_task_derived_child_launch_grant_json_to_current(&outer.to_string())
                .expect("task child launch should migrate");
        let migrated_value: Value =
            serde_json::from_str(&migrated).expect("migrated task launch should decode");
        assert_eq!(migrated_value["child_launch_grant"]["version"], json!(2));

        let migrated_again =
            crate::migrate_task_derived_child_launch_grant_json_to_current(&migrated)
                .expect("current task child launch should be accepted");
        assert_eq!(migrated_again, migrated);
    }

    #[test]
    fn task_child_launch_migration_rejects_outer_identity_outside_authenticated_ceiling() {
        let child = version_one_grant(true);
        let mut identity = child["identities"][0].clone();
        identity["display_name"] = json!("Tampered");
        let outer = json!({
            "kind": "resolved_task_launch",
            "identity": identity,
            "profile": child["profiles"][0].clone(),
            "role_key": "member",
            "agent_policy_generation": 1,
            "allowed_actions": ["create_task"],
            "agent_authorization_fingerprint": "a".repeat(64),
            "child_launch_grant": child,
        });

        assert!(
            crate::migrate_task_derived_child_launch_grant_json_to_current(&outer.to_string())
                .is_err()
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
