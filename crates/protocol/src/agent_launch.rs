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

/// Compare durable source membership, not a particular configuration snapshot.
/// Workspace/source-id ownership is additionally checked against the identity
/// repository at admission. CLI role_label records the runtime kind, so a
/// different engine cannot be smuggled through an existing runtime instance.
pub fn agent_identity_has_same_launch_source(
    candidate: &AgentIdentityProjection,
    ceiling: &AgentIdentityProjection,
) -> bool {
    candidate.id == ceiling.id
        && candidate.source_kind == ceiling.source_kind
        && match candidate.source_kind {
            crate::AgentIdentitySourceKind::Ephemeral => candidate == ceiling,
            crate::AgentIdentitySourceKind::CliRuntimeInstance => {
                candidate.role_label == ceiling.role_label
            }
            crate::AgentIdentitySourceKind::NativeAgent => true,
        }
}

/// Real launch capabilities may only narrow across catalog generations. Profile
/// ids, generations, labels and fingerprints identify snapshots, not authority.
pub fn agent_execution_profile_is_no_wider(
    candidate: &AgentExecutionProfileProjection,
    ceiling: &AgentExecutionProfileProjection,
) -> bool {
    candidate.backend == ceiling.backend
        && candidate.provider_id == ceiling.provider_id
        && candidate.model_id == ceiling.model_id
        && !candidate.compatible_agent_identity_ids.is_empty()
        && candidate
            .compatible_agent_identity_ids
            .iter()
            .all(|id| ceiling.compatible_agent_identity_ids.contains(id))
        && candidate
            .allowed_reasoning
            .iter()
            .all(|reasoning| ceiling.allowed_reasoning.contains(reasoning))
        && candidate
            .allowed_permission_profiles
            .iter()
            .all(|mode| ceiling.allowed_permission_profiles.contains(mode))
}

/// Immutable delegation boundary for identities and capabilities. Model catalogs
/// are current metadata, not durable authority; each launch stores its exact
/// execution profile separately.
#[derive(JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChildAgentLaunchGrantSet {
    pub version: u32,
    pub identities: Vec<AgentIdentityProjection>,
    pub allow_inherit_parent_identity: bool,
    pub allow_server_derived_ephemeral: bool,
    pub allow_inherit_parent_profile: bool,
    #[serde(default)]
    pub skill_ids: Vec<SkillId>,
    #[serde(default)]
    pub mcp_server_ids: Vec<String>,
    pub max_permission_profile: TurnPermissionProfileCap,
    pub fingerprint: String,
    #[schemars(skip)]
    original: Option<serde_json::Value>,
}

// Only the behavioral projection follows the current DTO. Authentication uses
// the original Value and the frozen wire types below, before projection.
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CurrentChildLaunchGrantFields {
    pub version: u32,
    pub identities: Vec<AgentIdentityProjection>,
    pub allow_inherit_parent_identity: bool,
    pub allow_server_derived_ephemeral: bool,
    pub allow_inherit_parent_profile: bool,
    #[serde(default)]
    pub skill_ids: Vec<SkillId>,
    #[serde(default)]
    pub mcp_server_ids: Vec<String>,
    pub max_permission_profile: TurnPermissionProfileCap,
    pub fingerprint: String,
}

impl ChildAgentLaunchGrantSet {
    fn fields(&self) -> CurrentChildLaunchGrantFields {
        CurrentChildLaunchGrantFields {
            version: self.version,
            identities: self.identities.clone(),
            allow_inherit_parent_identity: self.allow_inherit_parent_identity,
            allow_server_derived_ephemeral: self.allow_server_derived_ephemeral,
            allow_inherit_parent_profile: self.allow_inherit_parent_profile,
            skill_ids: self.skill_ids.clone(),
            mcp_server_ids: self.mcp_server_ids.clone(),
            max_permission_profile: self.max_permission_profile.clone(),
            fingerprint: self.fingerprint.clone(),
        }
    }

    fn seal(&mut self) -> Result<(), ChildAgentLaunchGrantSetError> {
        let mut value = serde_json::to_value(self.fields())
            .map_err(|_| ChildAgentLaunchGrantSetError::Serialization)?;
        self.fingerprint = full_json_child_grant_fingerprint(&value)
            .map_err(|_| ChildAgentLaunchGrantSetError::Serialization)?;
        value["fingerprint"] = self.fingerprint.clone().into();
        let mut sealed = project_verified_child_grant(&value)
            .map_err(|_| ChildAgentLaunchGrantSetError::Serialization)?;
        sealed.original = Some(value);
        *self = sealed;
        Ok(())
    }

    /// The selected snapshot must fit both the current identity boundary and
    /// any authenticated historical catalog ceiling. This check is also safe
    /// for history readers; it does not authorize resuming an execution.
    pub fn validate_selected_profile(
        &self,
        identity: &AgentIdentityProjection,
        profile: &AgentExecutionProfileProjection,
    ) -> Result<(), ChildAgentLaunchGrantSetError> {
        self.validate()?;
        if let Some(profiles) = self
            .original
            .as_ref()
            .and_then(|value| value.get("profiles"))
        {
            let profiles: Vec<AgentExecutionProfileProjection> =
                serde_json::from_value(profiles.clone())
                    .map_err(|_| ChildAgentLaunchGrantSetError::InvalidProfile)?;
            // Released enclosing documents pinned the selected snapshot by
            // equality. Preserve that historical rule, including backends that
            // the current executor no longer supports, for history reads.
            if !self.identities.contains(identity)
                || !profiles.contains(profile)
                || (identity.source_kind != crate::AgentIdentitySourceKind::Ephemeral
                    && !profile.compatible_agent_identity_ids.contains(&identity.id))
            {
                return Err(ChildAgentLaunchGrantSetError::ProfileOutsideIdentitySet);
            }
            let allowed = self
                .original
                .as_ref()
                .and_then(|value| value.pointer("/maxReasoning/allowed"))
                .ok_or(ChildAgentLaunchGrantSetError::InvalidProfile)?;
            let allowed: Vec<TurnReasoningSelection> = serde_json::from_value(allowed.clone())
                .map_err(|_| ChildAgentLaunchGrantSetError::InvalidProfile)?;
            if profile
                .allowed_reasoning
                .iter()
                .any(|reasoning| !allowed.contains(reasoning))
            {
                return Err(ChildAgentLaunchGrantSetError::InvalidProfile);
            }
        } else {
            validate_selected_agent_launch_profile(identity, profile, &self.identities)?;
        }
        Ok(())
    }

    /// Historical catalog ceilings remain readable, but the current executor
    /// cannot enforce them. Never turn their projection into broader authority.
    pub fn validate_for_execution(&self) -> Result<(), ChildAgentLaunchGrantSetError> {
        self.validate()?;
        if self.original.as_ref().is_some_and(|value| {
            value.get("profiles").is_some() || value.get("maxReasoning").is_some()
        }) {
            return Err(ChildAgentLaunchGrantSetError::InvalidProfile);
        }
        Ok(())
    }
}

impl Serialize for ChildAgentLaunchGrantSet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate()
            .map_err(|error| <S::Error as serde::ser::Error>::custom(format!("{error:?}")))?;
        self.original
            .as_ref()
            .ok_or_else(|| <S::Error as serde::ser::Error>::custom("unsealed child grant"))?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ChildAgentLaunchGrantSet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let mut grant =
            project_verified_child_grant(&value).map_err(<D::Error as serde::de::Error>::custom)?;
        grant.original = Some(value);
        Ok(grant)
    }
}

impl ChildAgentLaunchGrantSet {
    pub const VERSION: u32 = CHILD_LAUNCH_GRANT_VERSION_V2;
    pub const MAX_SKILLS: usize = 256;
    pub const MAX_MCP_SERVERS: usize = 256;

    pub fn new(
        mut identities: Vec<AgentIdentityProjection>,
    ) -> Result<Self, ChildAgentLaunchGrantSetError> {
        identities.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        validate_child_launch_grant_entries(&identities)?;
        let mut grant = Self {
            version: Self::VERSION,
            identities,
            allow_inherit_parent_identity: false,
            allow_server_derived_ephemeral: false,
            allow_inherit_parent_profile: false,
            skill_ids: Vec::new(),
            mcp_server_ids: Vec::new(),
            max_permission_profile: crate::task_permission_cap_for_mode(
                TurnPermissionMode::Supervised,
            ),
            fingerprint: String::new(),
            original: None,
        };
        grant.seal()?;
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
        self.validate_for_execution()?;
        skill_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        mcp_server_ids.sort();
        self.allow_inherit_parent_identity = allow_inherit_parent_identity;
        self.allow_server_derived_ephemeral = allow_server_derived_ephemeral;
        self.allow_inherit_parent_profile = allow_inherit_parent_profile;
        self.skill_ids = skill_ids;
        self.mcp_server_ids = mcp_server_ids;
        self.max_permission_profile = max_permission_profile;
        validate_child_launch_grant(&self)?;
        self.seal()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), ChildAgentLaunchGrantSetError> {
        validate_child_launch_grant(self)?;
        let original = self
            .original
            .as_ref()
            .ok_or(ChildAgentLaunchGrantSetError::FingerprintMismatch)?;
        let expected = project_verified_child_grant(original)
            .map_err(|_| ChildAgentLaunchGrantSetError::FingerprintMismatch)?;
        if self.fields() != expected.fields() {
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
    validate_child_launch_grant_shape(grant)
}

fn validate_child_launch_grant_shape(
    grant: &ChildAgentLaunchGrantSet,
) -> Result<(), ChildAgentLaunchGrantSetError> {
    validate_child_launch_grant_entries(&grant.identities)?;
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
    Ok(())
}

fn validate_child_launch_grant_entries(
    identities: &[AgentIdentityProjection],
) -> Result<(), ChildAgentLaunchGrantSetError> {
    if identities.is_empty() {
        return Err(ChildAgentLaunchGrantSetError::EmptyIdentitySet);
    }
    if identities
        .windows(2)
        .any(|pair| pair[0].id.as_str() >= pair[1].id.as_str())
    {
        return Err(ChildAgentLaunchGrantSetError::DuplicateIdentity);
    }
    if identities.iter().any(|identity| {
        identity.source_revision == 0
            || identity.source_fingerprint.trim().is_empty()
            || identity.display_name.trim().is_empty()
            || identity.nickname.trim().is_empty()
    }) {
        return Err(ChildAgentLaunchGrantSetError::InvalidIdentity);
    }
    Ok(())
}

/// Validate the exact selected snapshot independently of the delegation boundary.
/// Every compatible source must still belong to that boundary, including the
/// registered backing identity of a server-derived CLI identity.
pub fn validate_selected_agent_launch_profile(
    identity: &AgentIdentityProjection,
    profile: &AgentExecutionProfileProjection,
    identities: &[AgentIdentityProjection],
) -> Result<(), ChildAgentLaunchGrantSetError> {
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
    if !identities.contains(identity)
        || !profile.compatible_agent_identity_ids.contains(&identity.id)
        || profile
            .compatible_agent_identity_ids
            .iter()
            .any(|id| !identities.iter().any(|candidate| &candidate.id == id))
    {
        return Err(ChildAgentLaunchGrantSetError::ProfileOutsideIdentitySet);
    }
    use crate::AgentIdentitySourceKind;
    let compatible = match (&identity.source_kind, &profile.backend) {
        (AgentIdentitySourceKind::NativeAgent, AgentExecutionProfileBackend::ApiProvider) => true,
        (AgentIdentitySourceKind::Ephemeral, AgentExecutionProfileBackend::ApiProvider) => true,
        (
            AgentIdentitySourceKind::CliRuntimeInstance | AgentIdentitySourceKind::Ephemeral,
            AgentExecutionProfileBackend::CliRuntime {
                runtime_instance_id,
            },
        ) => {
            !runtime_instance_id.trim().is_empty()
                && profile.provider_id == format!("cli_runtime:{runtime_instance_id}")
                && identities.iter().any(|source| {
                    source.source_kind == AgentIdentitySourceKind::CliRuntimeInstance
                        && profile.compatible_agent_identity_ids.contains(&source.id)
                })
        }
        _ => false,
    };
    if !compatible {
        return Err(ChildAgentLaunchGrantSetError::InvalidProfile);
    }
    Ok(())
}

const CHILD_LAUNCH_GRANT_VERSION_V1: u32 = 1;
const CHILD_LAUNCH_GRANT_VERSION_V2: u32 = 2;
const FULL_JSON_CHILD_GRANT_PREFIX: &str = "json-sha256-v1:";

/// Full parsed JSON, recursively lexicographically sorted object keys; array
/// order is significant. Scalars use serde_json's compact UTF-8 encoding (no
/// Unicode normalization). Exclude ONLY the root `fingerprint` member. Nested
/// fingerprints and every unknown field are covered. The prefix selects this
/// algorithm unambiguously; it is not a grant/application version.
fn full_json_child_grant_fingerprint(value: &serde_json::Value) -> Result<String, String> {
    fn canonical(value: &serde_json::Value, bytes: &mut Vec<u8>) -> Result<(), String> {
        match value {
            serde_json::Value::Object(object) => {
                bytes.push(b'{');
                let mut keys = object.keys().collect::<Vec<_>>();
                keys.sort();
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        bytes.push(b',');
                    }
                    bytes.extend(serde_json::to_vec(key).map_err(|e| e.to_string())?);
                    bytes.push(b':');
                    canonical(&object[key], bytes)?;
                }
                bytes.push(b'}');
            }
            serde_json::Value::Array(array) => {
                bytes.push(b'[');
                for (index, item) in array.iter().enumerate() {
                    if index > 0 {
                        bytes.push(b',');
                    }
                    canonical(item, bytes)?;
                }
                bytes.push(b']');
            }
            scalar => bytes.extend(serde_json::to_vec(scalar).map_err(|e| e.to_string())?),
        }
        Ok(())
    }
    let mut value = value.clone();
    value
        .as_object_mut()
        .ok_or("child grant must be an object")?
        .remove("fingerprint");
    let mut bytes = Vec::new();
    canonical(&value, &mut bytes)?;
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:child-launch-grant:full-json:v1\0");
    digest.update(bytes);
    Ok(format!(
        "{FULL_JSON_CHILD_GRANT_PREFIX}{}",
        hex::encode(digest.finalize())
    ))
}

// Compact v2 was also released. These wire fields and tuple order are frozen,
// including the nested identity and permission serializers; never use the DTO.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompactChildLaunchGrantWire {
    version: u32,
    identities: Vec<IdentityWire>,
    allow_inherit_parent_identity: bool,
    allow_server_derived_ephemeral: bool,
    allow_inherit_parent_profile: bool,
    #[serde(default)]
    skill_ids: Vec<String>,
    #[serde(default)]
    mcp_server_ids: Vec<String>,
    max_permission_profile: PermissionCapWire<PermissionPolicyV2Wire>,
    fingerprint: String,
}

fn verify_compact_child_grant(value: &serde_json::Value) -> Result<(), String> {
    let grant: CompactChildLaunchGrantWire =
        serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
    let bytes = serde_json::to_vec(&(
        grant.version,
        &grant.identities,
        grant.allow_inherit_parent_identity,
        grant.allow_server_derived_ephemeral,
        grant.allow_inherit_parent_profile,
        &grant.skill_ids,
        &grant.mcp_server_ids,
        &grant.max_permission_profile,
    ))
    .map_err(|error| error.to_string())?;
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:child-launch-grant:v2\0");
    digest.update(bytes);
    if grant.fingerprint != hex::encode(digest.finalize()) {
        return Err("compact v2 child grant fingerprint mismatch".into());
    }
    Ok(())
}

fn project_verified_child_grant(
    value: &serde_json::Value,
) -> Result<ChildAgentLaunchGrantSet, String> {
    let version = child_launch_grant_version(value)?;
    let fingerprint = value
        .get("fingerprint")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing child grant fingerprint")?;
    let mut projection = value.clone();
    if fingerprint.starts_with(FULL_JSON_CHILD_GRANT_PREFIX) {
        if version != CHILD_LAUNCH_GRANT_VERSION_V2 {
            return Err("unsupported full JSON child grant version".into());
        }
        if fingerprint != full_json_child_grant_fingerprint(value)? {
            return Err("full JSON child grant fingerprint mismatch".into());
        }
        // Only these fields influence current behavior. Everything else remains
        // in the authenticated original, including future reference metadata.
        let object = projection
            .as_object_mut()
            .ok_or("child grant must be an object")?;
        object.retain(|key, _| {
            matches!(
                key.as_str(),
                "version"
                    | "identities"
                    | "allowInheritParentIdentity"
                    | "allowServerDerivedEphemeral"
                    | "allowInheritParentProfile"
                    | "skillIds"
                    | "mcpServerIds"
                    | "maxPermissionProfile"
                    | "fingerprint"
            )
        });
    } else {
        if fingerprint.len() != 64
            || !fingerprint
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("unknown child grant fingerprint algorithm".into());
        }
        match version {
            CHILD_LAUNCH_GRANT_VERSION_V1 => {
                let wire: ChildLaunchGrantWire<PermissionPolicyV1Wire> =
                    serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
                verify_child_launch_grant_v1_wire(&wire)?;
                let mut wire = wire.try_map_permission_policy(
                    PermissionCapWire::<PermissionPolicyV2Wire>::try_from,
                )?;
                wire.max_permission_profile
                    .effective_policy
                    .normalize_version_one_restrictions();
                projection = serde_json::to_value(wire).map_err(|e| e.to_string())?;
                projection["version"] = CHILD_LAUNCH_GRANT_VERSION_V2.into();
            }
            CHILD_LAUNCH_GRANT_VERSION_V2 => {
                if value.get("profiles").is_some() || value.get("maxReasoning").is_some() {
                    let wire: ChildLaunchGrantWire<PermissionPolicyV2Wire> =
                        serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
                    let expected = child_launch_grant_wire_fingerprint(
                        &wire,
                        b"pioneer:agent-runtime:child-launch-grant:v2\0",
                    )?;
                    if fingerprint != expected {
                        return Err("historical v2 child grant fingerprint mismatch".into());
                    }
                } else {
                    verify_compact_child_grant(value)?;
                }
            }
            _ => return Err("unsupported child grant version".into()),
        }
        let object = projection
            .as_object_mut()
            .ok_or("child grant must be an object")?;
        object.remove("profiles");
        object.remove("maxReasoning");
    }
    let fields: CurrentChildLaunchGrantFields =
        serde_json::from_value(projection).map_err(|e| e.to_string())?;
    let grant = ChildAgentLaunchGrantSet {
        version: fields.version,
        identities: fields.identities,
        allow_inherit_parent_identity: fields.allow_inherit_parent_identity,
        allow_server_derived_ephemeral: fields.allow_server_derived_ephemeral,
        allow_inherit_parent_profile: fields.allow_inherit_parent_profile,
        skill_ids: fields.skill_ids,
        mcp_server_ids: fields.mcp_server_ids,
        max_permission_profile: fields.max_permission_profile,
        fingerprint: fields.fingerprint,
        original: None,
    };
    validate_child_launch_grant(&grant).map_err(|e| format!("{e:?}"))?;
    Ok(grant)
}

fn is_false(value: &bool) -> bool {
    !*value
}

// Historical authentication uses immutable wire schemas. Never serialize the
// moving runtime DTO to verify an old checksum: the field order, defaults and
// omission rules below are part of the released hash algorithms.
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

/// Compatibility entry point retained for persistence callers. Verify the
/// nested original, but return the outer bytes unchanged. Projection belongs
/// to ChildAgentLaunchGrantSet's durable deserializer, never to a DB rewrite.
pub fn migrate_embedded_child_launch_grant_json_to_current(
    json: &str,
) -> Result<String, ChildAgentLaunchGrantMigrationError> {
    let outer: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| ChildAgentLaunchGrantMigrationError::InvalidJson(e.to_string()))?;
    let child = outer
        .get("child_launch_grant")
        .ok_or(ChildAgentLaunchGrantMigrationError::MissingChildGrant)?;
    project_verified_child_grant(child)
        .map_err(ChildAgentLaunchGrantMigrationError::InvalidCurrentContract)?;
    Ok(json.to_owned())
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

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentExecutionProfileSelection {
    InheritParent,
    Exact {
        profile_id: AgentExecutionProfileId,
    },
    Model {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_id: Option<String>,
        model_id: String,
    },
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
    /// Exact normalized capabilities for a durable Task launch. Legacy launches
    /// use skill_ids and mcp_server_ids when this field is empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selected_capabilities: Vec<crate::TurnCapability>,
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
        ChildAgentLaunchGrantSet::new(vec![identity]).unwrap()
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
                selected_capabilities: Vec::new(),
            },
        }
    }

    #[test]
    fn serialized_launch_capabilities_fit_the_closed_launch_schema() {
        let mut launch = launch(AgentExecutionProfileSelection::InheritParent);
        launch.execution.selected_capabilities = vec![crate::TurnCapability {
            id: crate::mcp_tool_capability_key(crate::McpScopeKind::Workspace, "resend", "send"),
            kind: crate::TurnCapabilityKind::McpTool {
                server_name: "resend".to_owned(),
                raw_tool_name: "send".to_owned(),
                scope_kind: crate::McpScopeKind::Workspace,
            },
            label: None,
        }];
        let serialized = serde_json::to_value(&launch).expect("launch should serialize");
        let schema = serde_json::to_value(schemars::schema_for!(AgentLaunchSelection))
            .expect("launch schema should serialize");
        let checked_in: Value =
            serde_json::from_str(include_str!("../../../schemas/agent_launch_selection.json"))
                .expect("checked-in launch schema should parse");
        assert_eq!(
            schema, checked_in,
            "checked-in launch schema must be current"
        );
        let execution_document =
            serde_json::to_value(schemars::schema_for!(AgentExecutionSelection)).unwrap();
        let checked_in_execution: Value = serde_json::from_str(include_str!(
            "../../../schemas/agent_execution_selection.json"
        ))
        .expect("checked-in execution schema should parse");
        assert_eq!(execution_document, checked_in_execution);

        let execution_schema = &schema["$defs"]["AgentExecutionSelection"];
        assert_eq!(execution_schema["additionalProperties"], false);
        for key in serialized["execution"]
            .as_object()
            .expect("serialized execution should be an object")
            .keys()
        {
            assert!(
                execution_schema["properties"].get(key).is_some(),
                "serialized execution field `{key}` is absent from its closed schema"
            );
        }
        assert_eq!(
            execution_schema["properties"]["selectedCapabilities"]["items"]["$ref"],
            "#/$defs/TurnCapability"
        );
        let capability = &serialized["execution"]["selectedCapabilities"][0];
        for key in capability
            .as_object()
            .expect("capability should be an object")
            .keys()
        {
            assert!(
                schema["$defs"]["TurnCapability"]["properties"]
                    .get(key)
                    .is_some()
            );
        }
        let kind = &capability["kind"];
        assert!(
            schema["$defs"]["TurnCapabilityKind"]["oneOf"]
                .as_array()
                .expect("capability kind variants")
                .iter()
                .any(
                    |variant| variant["properties"]["type"]["const"] == kind["type"]
                        && kind
                            .as_object()
                            .expect("capability kind should be an object")
                            .keys()
                            .all(|key| variant["properties"].get(key).is_some())
                )
        );
        assert_eq!(kind["type"], "mcpTool");
        assert_eq!(
            serde_json::from_value::<AgentLaunchSelection>(serialized).unwrap(),
            launch
        );
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
        assert!(grant.fingerprint.starts_with(FULL_JSON_CHILD_GRANT_PREFIX));
        let encoded = serde_json::to_value(&grant).unwrap();
        assert!(encoded.get("profiles").is_none());
        assert!(encoded.get("maxReasoning").is_none());
        let mut tampered = grant;
        tampered.identities[0].source_fingerprint = "other-source".to_owned();
        assert_eq!(
            tampered.validate(),
            Err(ChildAgentLaunchGrantSetError::FingerprintMismatch)
        );
    }

    #[test]
    fn task_grant_validates_its_exact_profile_without_a_model_catalog() {
        let seed = version_one_grant(true);
        let grant = json!({
            "kind": "resolved_task_launch",
            "identity": seed["identities"][0],
            "profile": seed["profiles"][0],
            "role_key": "member", "agent_policy_generation": 1,
            "allowed_actions": ["create_task"],
            "agent_authorization_fingerprint": "a".repeat(64),
            "child_launch_grant": child_grant(),
        });
        crate::migrate_task_derived_child_launch_grant_json_to_current(&grant.to_string()).unwrap();
        for change in 0..7 {
            let mut invalid = grant.clone();
            match change {
                0 => invalid["profile"]["catalogGeneration"] = json!(0),
                1 => invalid["profile"]["policyGeneration"] = json!(0),
                2 => invalid["profile"]["fingerprint"] = json!(""),
                3 => invalid["profile"]["compatibleAgentIdentityIds"] = json!([]),
                4 => {
                    invalid["profile"]["compatibleAgentIdentityIds"] =
                        json!(["A00000000000000000001", "A00000000000000000001"])
                }
                5 => {
                    invalid["profile"]["compatibleAgentIdentityIds"] =
                        json!(["A00000000000000000002"])
                }
                _ => {
                    invalid["profile"]["backend"] =
                        json!({"cli_runtime":{"runtime_instance_id":"codex"}});
                    invalid["profile"]["providerId"] = json!("cli_runtime:codex");
                }
            }
            assert!(
                crate::migrate_task_derived_child_launch_grant_json_to_current(
                    &invalid.to_string()
                )
                .is_err(),
                "invalid exact profile {change}"
            );
        }
    }

    #[test]
    fn large_admitted_grants_round_trip_without_capacity_loss_or_widening() {
        let identities = (0..140)
            .map(|n| {
                AgentIdentityProjection::pioneer(identity_id(&format!("A{n:020}")), 3, "source")
            })
            .collect::<Vec<_>>();
        let admitted = ChildAgentLaunchGrantSet::new(identities).unwrap();
        let stored = serde_json::to_string(&admitted).unwrap();
        let restored: ChildAgentLaunchGrantSet = serde_json::from_str(&stored).unwrap();
        restored.validate().unwrap();
        assert_eq!(restored, admitted);
        assert_eq!(restored.identities.len(), 140);
        let mut widened = restored;
        widened.identities.last_mut().unwrap().source_fingerprint = "not-admitted".into();
        assert_eq!(
            widened.validate(),
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
    fn child_launch_grant_migration_preserves_version_one_implicit_allow_list_restrictions() {
        let mut source = version_one_grant(false);
        source["maxPermissionProfile"]["effective_policy"]["allowed_tools"] = json!(["read_file"]);
        source["maxPermissionProfile"]["effective_policy"]["allowed_paths"] = json!(["/workspace"]);
        source["fingerprint"] =
            json!("f71fe2e275437c5ac4a01850a4c0f132ca725e26499d268fef19e98757f06c7d");

        let migrated: ChildAgentLaunchGrantSet = serde_json::from_value(source)
            .expect("version 1 implicit restrictions should project safely");

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
        assert!(serde_json::from_value::<ChildAgentLaunchGrantSet>(value).is_err());
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

        let migrated: ChildAgentLaunchGrantSet = serde_json::from_value(source)
            .expect("CLI runtime version 1 grant should project safely");
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
        assert_eq!(migrated_value["child_launch_grant"]["version"], json!(1));
        assert_eq!(
            migrated,
            outer.to_string(),
            "read must preserve the complete original"
        );

        let migrated_again =
            crate::migrate_task_derived_child_launch_grant_json_to_current(&migrated)
                .expect("current task child launch should be accepted");
        assert_eq!(migrated_again, migrated);
        let crate::TaskDerivedChildLaunchGrant::ResolvedTaskLaunch {
            identity,
            profile,
            child_launch_grant,
            ..
        } = serde_json::from_str(&migrated).unwrap();
        let mut current = identity.clone();
        current.source_revision += 1;
        current.source_fingerprint = "updated-connection".to_owned();
        assert!(agent_identity_has_same_launch_source(&current, &identity));
        validate_selected_agent_launch_profile(&identity, &profile, &child_launch_grant.identities)
            .unwrap();
        assert!(
            child_launch_grant.validate().is_ok(),
            "reading legacy allowances never rewrites their authenticated snapshot"
        );
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

    #[test]
    fn released_grant_formats_authenticate_before_projection_and_preserve_originals() {
        // 1620... is pinned in d907644f's released V2 writer regression;
        // cdafe... is pinned in the compact writer at our base SHA. Neither
        // expected checksum is generated by the verifier under test.
        let mut historical_v2 = version_one_grant(false);
        historical_v2["version"] = json!(2);
        historical_v2["fingerprint"] =
            json!("1620c8eca0ca3eace03dc7d614e3af348b2f77224cde1064bcf7f492c3177047");
        let mut compact_v2 = historical_v2.clone();
        compact_v2.as_object_mut().unwrap().remove("profiles");
        compact_v2.as_object_mut().unwrap().remove("maxReasoning");
        compact_v2["fingerprint"] =
            json!("cdafe1ec7b698b312a327d4d7907f2c0c346cd8d859bcf54076f40a920c25953");
        for original in [
            version_one_grant(true),
            version_one_grant(false),
            historical_v2,
            compact_v2,
        ] {
            let grant: ChildAgentLaunchGrantSet = serde_json::from_value(original.clone()).unwrap();
            grant.validate().unwrap();
            assert_eq!(serde_json::to_value(&grant).unwrap(), original);
            assert_eq!(grant.fingerprint, original["fingerprint"].as_str().unwrap());
            assert_eq!(
                grant.validate_for_execution().is_ok(),
                original.get("profiles").is_none()
            );
            if original.get("profiles").is_some() {
                let identity = grant.identities[0].clone();
                let profile: AgentExecutionProfileProjection =
                    serde_json::from_value(original["profiles"][0].clone()).unwrap();
                grant
                    .validate_selected_profile(&identity, &profile)
                    .unwrap();
                let mut wider = profile.clone();
                wider.model_id = "outside-saved-profile".into();
                assert!(grant.validate_selected_profile(&identity, &wider).is_err());
                let mut wider = profile;
                wider.allowed_reasoning.push(TurnReasoningSelection {
                    effort: "high".into(),
                });
                assert!(grant.validate_selected_profile(&identity, &wider).is_err());
                assert!(
                    grant
                        .clone()
                        .with_policy(
                            false,
                            false,
                            false,
                            vec![],
                            vec![],
                            crate::task_permission_cap_for_mode(TurnPermissionMode::FullAccess)
                        )
                        .is_err()
                );
                let mut tampered_reference = original.clone();
                tampered_reference["profiles"][0]["modelDisplayName"] = json!("changed");
                assert!(
                    serde_json::from_value::<ChildAgentLaunchGrantSet>(tampered_reference).is_err()
                );
                let mut tampered_reasoning = original.clone();
                tampered_reasoning["maxReasoning"]["allowed"] = json!([]);
                assert!(
                    serde_json::from_value::<ChildAgentLaunchGrantSet>(tampered_reasoning).is_err()
                );
            }
            let mut tampered = original;
            tampered["allowServerDerivedEphemeral"] = json!(true);
            assert!(serde_json::from_value::<ChildAgentLaunchGrantSet>(tampered).is_err());
        }
    }

    // Independently specified canonical bytes are the checksum oracle. This
    // fixture deliberately includes metadata absent from the Rust DTO, a nested
    // fingerprint, ordered arrays, and non-ASCII text. No production serializer
    // or verifier constructs these bytes.
    const NEW_JSON_CANONICAL: &str = r#"{"allowInheritParentIdentity":false,"allowInheritParentProfile":false,"allowServerDerivedEphemeral":false,"futureCatalog":{"labels":["я","a"],"nested":{"fingerprint":"reference"}},"identities":[{"display_name":"Pioneer","id":"A00000000000000000001","nickname":"pioneer","source_fingerprint":"generation-a","source_kind":"native_agent","source_revision":3}],"maxPermissionProfile":{"effective_policy":{"agent_action":"ask","computer_use":"ask","default_behavior":"ask","dynamic_skill_tool":"ask","file_read":"allow","file_write":"ask","mcp_read":"allow","mcp_write_or_unknown":"ask","memory_write":"ask","network":"ask","shell_command":"ask","task_subagent":"ask"},"mode":"supervised"},"mcpServerIds":[],"skillIds":[],"version":2}"#;

    fn full_json_fixture(canonical: &str) -> Value {
        let mut value: Value = serde_json::from_str(canonical).unwrap();
        let mut digest = Sha256::new();
        digest.update(b"pioneer:agent-runtime:child-launch-grant:full-json:v1\0");
        digest.update(canonical.as_bytes());
        value["fingerprint"] = json!(format!("json-sha256-v1:{}", hex::encode(digest.finalize())));
        value
    }

    #[test]
    fn full_json_checksum_survives_projection_and_covers_all_original_fields() {
        let original = full_json_fixture(NEW_JSON_CANONICAL);
        let grant: ChildAgentLaunchGrantSet = serde_json::from_value(original.clone()).unwrap();
        grant.validate_for_execution().unwrap();
        assert_eq!(serde_json::to_value(&grant).unwrap(), original);
        assert_eq!(grant.fingerprint, original["fingerprint"].as_str().unwrap());
        // The writer must agree with independently specified complete JSON;
        // the extra catalog is absent from today's DTO but retained on reads.
        let current_canonical = NEW_JSON_CANONICAL.replace(
            r#""futureCatalog":{"labels":["я","a"],"nested":{"fingerprint":"reference"}},"#,
            "",
        );
        assert_eq!(
            serde_json::to_value(child_grant()).unwrap(),
            full_json_fixture(&current_canonical)
        );
        for pointer in [
            "/allowInheritParentIdentity",
            "/identities/0/source_fingerprint",
            "/maxPermissionProfile/effective_policy/network",
            "/futureCatalog/nested/fingerprint",
            "/futureCatalog/labels/0",
        ] {
            let mut tampered = original.clone();
            let target = tampered.pointer_mut(pointer).unwrap();
            *target = if pointer.ends_with("/network") {
                json!("allow")
            } else {
                match target.as_bool() {
                    Some(value) => json!(!value),
                    None => json!("changed"),
                }
            };
            assert!(
                serde_json::from_value::<ChildAgentLaunchGrantSet>(tampered).is_err(),
                "{pointer}"
            );
        }
        for (field, value) in [
            ("version", json!(3)),
            ("fingerprint", json!("json-sha256-v9:unknown")),
            ("fingerprint", json!("0".repeat(64))),
        ] {
            let mut unknown = original.clone();
            unknown[field] = value;
            assert!(serde_json::from_value::<ChildAgentLaunchGrantSet>(unknown).is_err());
        }
        let mut lost_reference = original.clone();
        lost_reference
            .as_object_mut()
            .unwrap()
            .remove("futureCatalog");
        assert!(serde_json::from_value::<ChildAgentLaunchGrantSet>(lost_reference).is_err());
        let wrong_version =
            full_json_fixture(&NEW_JSON_CANONICAL.replace("\"version\":2", "\"version\":3"));
        assert!(serde_json::from_value::<ChildAgentLaunchGrantSet>(wrong_version).is_err());
        let missing_field = full_json_fixture(
            &NEW_JSON_CANONICAL.replace("\"allowInheritParentIdentity\":false,", ""),
        );
        assert!(serde_json::from_value::<ChildAgentLaunchGrantSet>(missing_field).is_err());
        let mut mutated_projection = grant;
        mutated_projection.allow_inherit_parent_profile = true;
        assert!(mutated_projection.validate().is_err());
        assert!(serde_json::to_value(mutated_projection).is_err());
    }

    #[test]
    fn full_json_canonicalization_sorts_nested_keys_but_preserves_array_order() {
        let original = full_json_fixture(NEW_JSON_CANONICAL);
        let checksum = original["fingerprint"].as_str().unwrap();
        // Move the root fingerprint first and add insignificant
        // whitespace. Also reverse the nested identity's member order.
        let reordered = format!(
            "{{ \"fingerprint\":{}, {} }}",
            serde_json::to_string(checksum).unwrap(),
            NEW_JSON_CANONICAL
                .trim_start_matches('{')
                .trim_end_matches('}')
                .replace(
                    "\"display_name\":\"Pioneer\",\"id\":\"A00000000000000000001\"",
                    "\"id\":\"A00000000000000000001\",\"display_name\":\"Pioneer\""
                )
        );
        let restored: ChildAgentLaunchGrantSet = serde_json::from_str(&reordered).unwrap();
        assert_eq!(restored.fingerprint, checksum);
        let mut changed_order = original;
        changed_order["futureCatalog"]["labels"]
            .as_array_mut()
            .unwrap()
            .reverse();
        assert!(serde_json::from_value::<ChildAgentLaunchGrantSet>(changed_order).is_err());
    }
}
