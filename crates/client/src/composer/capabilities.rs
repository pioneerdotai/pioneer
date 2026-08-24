//! Composer capability selection helpers.

use crate::{
    providers::list::runtime_id_from_cli_runtime_provider_key,
    skills::{catalog as skill_catalog, presentation as skill_presentation},
};

use pioneer_protocol::{
    McpListItem, McpListResponse, McpRuntimeState, McpScopeKind, McpServerDetailsResponse,
    RuntimeCapabilities, RuntimeStatus, RuntimeSummary, SkillId, SkillListItem, SkillListResponse,
    TurnCapability, TurnCapabilityKind, TurnMcpServerCapabilitySummary,
    TurnMcpToolCapabilitySummary, TurnSkillCapabilitySummary, UserMessageAttachment,
};
use std::collections::HashSet;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct ComposerCapability {
    pub id: String,
    pub label: String,
    pub kind: ComposerCapabilityKind,
}

#[derive(serde::Deserialize)]
struct ComposerCapabilityWire {
    id: String,
    label: String,
    kind: ComposerCapabilityKind,
}

impl<'de> serde::Deserialize<'de> for ComposerCapability {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = <ComposerCapabilityWire as serde::Deserialize>::deserialize(deserializer)?;
        let expected = wire.kind.key();
        if wire.id != expected {
            return Err(serde::de::Error::custom(format!(
                "composer capability id must be {expected}"
            )));
        }
        Ok(Self {
            id: wire.id,
            label: wire.label,
            kind: wire.kind,
        })
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ComposerCapabilityKind {
    Skill {
        skill_id: SkillId,
        owner: Option<String>,
        slug: String,
        source_kind: String,
    },
    McpServer {
        name: String,
        scope_kind: McpScopeKind,
    },
    McpTool {
        server_name: String,
        raw_tool_name: String,
        scope_kind: McpScopeKind,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerCapabilityPolicy {
    pub supports_skills: bool,
    pub supports_mcp_tools: bool,
}

impl ComposerCapabilityPolicy {
    pub const fn native() -> Self {
        Self {
            supports_skills: true,
            supports_mcp_tools: true,
        }
    }

    pub const fn unsupported_cli() -> Self {
        Self {
            supports_skills: false,
            supports_mcp_tools: false,
        }
    }

    pub const fn cli(supports_skills: bool, supports_mcp_tools: bool) -> Self {
        Self {
            supports_skills,
            supports_mcp_tools,
        }
    }

    pub fn from_cli_runtime_capabilities(capabilities: &RuntimeCapabilities) -> Self {
        Self::cli(
            capabilities.supports_skills,
            capabilities.supports_mcp_tools,
        )
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComposerCapabilityTargetKind {
    Native,
    Cli,
}

/// Capability eligibility context.
///
/// The target kind exists only because native skills retain their current
/// source policy while CLI skills must be exportable. Capability support is
/// represented exclusively by [`ComposerCapabilityPolicy`].
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerCapabilityTarget {
    kind: ComposerCapabilityTargetKind,
    supports_skills: bool,
    supports_mcp_tools: bool,
}

impl ComposerCapabilityTarget {
    pub const fn native() -> Self {
        Self {
            kind: ComposerCapabilityTargetKind::Native,
            supports_skills: true,
            supports_mcp_tools: true,
        }
    }

    pub const fn cli(policy: ComposerCapabilityPolicy) -> Self {
        Self {
            kind: ComposerCapabilityTargetKind::Cli,
            supports_skills: policy.supports_skills,
            supports_mcp_tools: policy.supports_mcp_tools,
        }
    }

    /// Structural capability target used at the CLI turn-submission boundary.
    ///
    /// Runtime summaries are suitable for presenting the composer picker, but
    /// they are not authoritative at submission time. The Gateway owns and
    /// validates its current readiness snapshot while preparing the turn, so
    /// clients must preserve an explicit selection on the wire instead of
    /// silently reducing it from a stale local snapshot. CLI-only source
    /// restrictions still apply to skills.
    pub const fn cli_submission() -> Self {
        Self::cli(ComposerCapabilityPolicy::cli(true, true))
    }

    pub fn from_cli_runtime_capabilities(capabilities: &RuntimeCapabilities) -> Self {
        Self::cli(ComposerCapabilityPolicy::from_cli_runtime_capabilities(
            capabilities,
        ))
    }

    pub const fn policy(self) -> ComposerCapabilityPolicy {
        ComposerCapabilityPolicy {
            supports_skills: self.supports_skills,
            supports_mcp_tools: self.supports_mcp_tools,
        }
    }

    pub const fn kind(self) -> ComposerCapabilityTargetKind {
        self.kind
    }

    pub const fn is_native(self) -> bool {
        matches!(self.kind, ComposerCapabilityTargetKind::Native)
    }

    pub const fn is_cli(self) -> bool {
        matches!(self.kind, ComposerCapabilityTargetKind::Cli)
    }
}

/// Canonical capability matrix shared by Rust clients and their focused tests.
///
/// Stale, disabled, or missing CLI summaries resolve to the `cli_neither`
/// policy; provider-switch behavior is a reduction from one row to another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ComposerCapabilityMatrixCase {
    pub id: &'static str,
    pub target: ComposerCapabilityTarget,
    pub supports_skills: bool,
    pub supports_mcp_tools: bool,
}

pub const COMPOSER_CAPABILITY_MATRIX: [ComposerCapabilityMatrixCase; 5] = [
    ComposerCapabilityMatrixCase {
        id: "native",
        target: ComposerCapabilityTarget::native(),
        supports_skills: true,
        supports_mcp_tools: true,
    },
    ComposerCapabilityMatrixCase {
        id: "cli_neither",
        target: ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(false, false)),
        supports_skills: false,
        supports_mcp_tools: false,
    },
    ComposerCapabilityMatrixCase {
        id: "cli_skills_only",
        target: ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, false)),
        supports_skills: true,
        supports_mcp_tools: false,
    },
    ComposerCapabilityMatrixCase {
        id: "cli_mcp_only",
        target: ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(false, true)),
        supports_skills: false,
        supports_mcp_tools: true,
    },
    ComposerCapabilityMatrixCase {
        id: "cli_both",
        target: ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, true)),
        supports_skills: true,
        supports_mcp_tools: true,
    },
];

pub fn composer_capability_target_for_provider(
    provider: Option<&str>,
    runtimes: &[RuntimeSummary],
) -> ComposerCapabilityTarget {
    let Some(runtime_id) = provider
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
        .and_then(runtime_id_from_cli_runtime_provider_key)
        .map(str::trim)
        .filter(|runtime_id| !runtime_id.is_empty())
    else {
        return ComposerCapabilityTarget::native();
    };

    let Some(runtime) = runtimes.iter().find(|runtime| {
        runtime.runtime_id == runtime_id
            && runtime.enabled
            && matches!(runtime.status, RuntimeStatus::Ready)
    }) else {
        return ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::unsupported_cli());
    };

    ComposerCapabilityTarget::from_cli_runtime_capabilities(&runtime.capabilities)
}

/// Structural capability target used when a turn is submitted.
///
/// Gateway runtime summaries are presentation evidence only. Submission keeps
/// an explicit selection intact and lets Gateway perform the authoritative
/// cached-readiness and projection validation. The only client-side reduction
/// here is the stable source rule for skills exported to CLI runtimes.
pub fn composer_submission_target_for_provider(provider: Option<&str>) -> ComposerCapabilityTarget {
    if provider
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
        .and_then(runtime_id_from_cli_runtime_provider_key)
        .is_some()
    {
        ComposerCapabilityTarget::cli_submission()
    } else {
        ComposerCapabilityTarget::native()
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComposerCapabilityRemovalReason {
    SkillsUnsupported,
    SkillSourceNotExportable,
    McpToolsUnsupported,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemovedComposerCapability {
    pub capability: ComposerCapability,
    pub reason: ComposerCapabilityRemovalReason,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerCapabilityPolicyReduction {
    pub capabilities: Vec<ComposerCapability>,
    pub removed: Vec<RemovedComposerCapability>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerCapabilityMenuVisibility {
    pub skills: bool,
    pub mcp: bool,
    pub any: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerCapabilityPresentation {
    pub target: ComposerCapabilityTarget,
    pub menu_visibility: ComposerCapabilityMenuVisibility,
    pub capabilities: Vec<ComposerCapability>,
    pub removed: Vec<RemovedComposerCapability>,
    pub has_composer_payload: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposerSubmissionPlan {
    pub target: ComposerCapabilityTarget,
    pub capabilities: Vec<ComposerCapability>,
    pub removed: Vec<RemovedComposerCapability>,
    pub has_composer_payload: bool,
}

/// Picker entry points are structural composer actions, not catalog or
/// provider-readiness indicators. Empty and unavailable states belong inside
/// the picker so installing a capability never requires restarting a shell.
pub const fn composer_capability_menu_visibility(
    _target: ComposerCapabilityTarget,
) -> ComposerCapabilityMenuVisibility {
    ComposerCapabilityMenuVisibility {
        skills: true,
        mcp: true,
        any: true,
    }
}

pub fn project_composer_capability_presentation(
    provider: Option<&str>,
    runtimes: &[RuntimeSummary],
    text: &str,
    has_attachments: bool,
    capabilities: &[ComposerCapability],
) -> ComposerCapabilityPresentation {
    let target = composer_capability_target_for_provider(provider, runtimes);
    let reduction = reduce_composer_capabilities_for_target(capabilities, target);
    let has_composer_payload = super::turn_prepare::composer_has_sendable_content(
        text,
        has_attachments,
        !reduction.capabilities.is_empty(),
    );
    ComposerCapabilityPresentation {
        target,
        menu_visibility: composer_capability_menu_visibility(target),
        capabilities: reduction.capabilities,
        removed: reduction.removed,
        has_composer_payload,
    }
}

pub fn plan_composer_submission(
    provider: Option<&str>,
    text: &str,
    has_attachments: bool,
    capabilities: &[ComposerCapability],
) -> ComposerSubmissionPlan {
    let target = composer_submission_target_for_provider(provider);
    let reduction = reduce_composer_capabilities_for_target(capabilities, target);
    let has_composer_payload = super::turn_prepare::composer_has_sendable_content(
        text,
        has_attachments,
        !reduction.capabilities.is_empty(),
    );
    ComposerSubmissionPlan {
        target,
        capabilities: reduction.capabilities,
        removed: reduction.removed,
        has_composer_payload,
    }
}

fn is_cli_exportable_skill_source(source_kind: &str) -> bool {
    matches!(source_kind, "system" | "user" | "registry")
}

pub fn composer_capability_is_eligible_for_target(
    capability: &ComposerCapability,
    target: ComposerCapabilityTarget,
) -> bool {
    composer_capability_removal_reason(capability, target).is_none()
}

pub fn composer_capability_removal_reason(
    capability: &ComposerCapability,
    target: ComposerCapabilityTarget,
) -> Option<ComposerCapabilityRemovalReason> {
    let policy = target.policy();
    match &capability.kind {
        ComposerCapabilityKind::Skill { source_kind, .. } => {
            if !policy.supports_skills {
                Some(ComposerCapabilityRemovalReason::SkillsUnsupported)
            } else if target.is_cli() && !is_cli_exportable_skill_source(source_kind) {
                Some(ComposerCapabilityRemovalReason::SkillSourceNotExportable)
            } else {
                None
            }
        }
        ComposerCapabilityKind::McpServer { .. } | ComposerCapabilityKind::McpTool { .. } => {
            (!policy.supports_mcp_tools)
                .then_some(ComposerCapabilityRemovalReason::McpToolsUnsupported)
        }
    }
}

pub fn reduce_composer_capabilities_for_target(
    capabilities: &[ComposerCapability],
    target: ComposerCapabilityTarget,
) -> ComposerCapabilityPolicyReduction {
    let mut reduction = ComposerCapabilityPolicyReduction {
        capabilities: Vec::with_capacity(capabilities.len()),
        removed: Vec::new(),
    };

    for capability in capabilities {
        if let Some(reason) = composer_capability_removal_reason(capability, target) {
            reduction.removed.push(RemovedComposerCapability {
                capability: capability.clone(),
                reason,
            });
        } else {
            reduction.capabilities.push(capability.clone());
        }
    }

    reduction
}

pub fn filter_composer_capabilities_for_target(
    capabilities: &[ComposerCapability],
    target: ComposerCapabilityTarget,
) -> Vec<ComposerCapability> {
    reduce_composer_capabilities_for_target(capabilities, target).capabilities
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SelectableSkillCapability {
    pub key: String,
    pub skill_id: SkillId,
    pub label: String,
    pub display_name: String,
    pub description: String,
    pub owner: Option<String>,
    pub slug: String,
    pub source_kind: String,
    pub selectable: bool,
    pub unavailable_reason: Option<SkillCapabilityUnavailableReason>,
}

pub fn selectable_skill_capability_is_eligible_for_target(
    row: &SelectableSkillCapability,
    target: ComposerCapabilityTarget,
) -> bool {
    let capability = ComposerCapability {
        id: pioneer_protocol::skill_capability_key(&row.skill_id),
        label: row.label.clone(),
        kind: ComposerCapabilityKind::Skill {
            skill_id: row.skill_id.clone(),
            owner: row.owner.clone(),
            slug: row.slug.clone(),
            source_kind: row.source_kind.clone(),
        },
    };
    composer_capability_is_eligible_for_target(&capability, target)
}

pub fn filter_selectable_skill_capabilities_for_target(
    rows: &[SelectableSkillCapability],
    target: ComposerCapabilityTarget,
) -> Vec<SelectableSkillCapability> {
    rows.iter()
        .filter(|row| selectable_skill_capability_is_eligible_for_target(row, target))
        .cloned()
        .collect()
}

pub fn selectable_mcp_capability_is_eligible_for_target(
    row: &SelectableMcpCapability,
    target: ComposerCapabilityTarget,
) -> bool {
    let kind = match &row.raw_tool_name {
        Some(raw_tool_name) => ComposerCapabilityKind::McpTool {
            server_name: row.server_name.clone(),
            raw_tool_name: raw_tool_name.clone(),
            scope_kind: row.scope_kind,
        },
        None => ComposerCapabilityKind::McpServer {
            name: row.server_name.clone(),
            scope_kind: row.scope_kind,
        },
    };
    composer_capability_is_eligible_for_target(
        &ComposerCapability {
            id: row.key.clone(),
            label: row.label.clone(),
            kind,
        },
        target,
    )
}

pub fn filter_selectable_mcp_capabilities_for_target(
    rows: &[SelectableMcpCapability],
    target: ComposerCapabilityTarget,
) -> Vec<SelectableMcpCapability> {
    rows.iter()
        .filter(|row| selectable_mcp_capability_is_eligible_for_target(row, target))
        .cloned()
        .collect()
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SkillCapabilityUnavailableReason {
    DisabledByPolicy,
    Inactive { status_reason: Option<String> },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SelectableMcpCapability {
    pub key: String,
    pub label: String,
    pub description: String,
    pub server_id: String,
    pub server_name: String,
    pub raw_tool_name: Option<String>,
    pub scope_kind: McpScopeKind,
    pub tools_count: Option<usize>,
    pub selectable: bool,
    pub unavailable_reason: Option<McpCapabilityUnavailableReason>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpCapabilityUnavailableReason {
    DisabledByPolicy,
    RuntimeUnavailable,
    RuntimeNotReady,
    NoToolCatalog,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpCapabilitySelectionToggle {
    pub collapse_active_server: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerSkillPickerRowsReduction {
    pub rows: Vec<SelectableSkillCapability>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerMcpServerPickerRowsReduction {
    pub rows: Vec<SelectableMcpCapability>,
    pub prefetch_server_ids: Vec<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerMcpToolPickerRowsReduction {
    pub rows: Vec<SelectableMcpCapability>,
}

impl ComposerCapability {
    pub fn key(&self) -> String {
        self.kind.key()
    }

    pub fn to_turn_capability(&self) -> TurnCapability {
        TurnCapability {
            id: self.key(),
            kind: match &self.kind {
                ComposerCapabilityKind::Skill { skill_id, .. } => TurnCapabilityKind::Skill {
                    skill_id: skill_id.clone(),
                    pack_id: None,
                },
                ComposerCapabilityKind::McpServer { name, scope_kind } => {
                    TurnCapabilityKind::McpServer {
                        name: name.clone(),
                        scope_kind: *scope_kind,
                    }
                }
                ComposerCapabilityKind::McpTool {
                    server_name,
                    raw_tool_name,
                    scope_kind,
                } => TurnCapabilityKind::McpTool {
                    server_name: server_name.clone(),
                    raw_tool_name: raw_tool_name.clone(),
                    scope_kind: *scope_kind,
                },
            },
            label: (!self.label.trim().is_empty()).then(|| self.label.clone()),
        }
    }

    pub fn to_user_message_attachment(&self) -> UserMessageAttachment {
        match &self.kind {
            ComposerCapabilityKind::Skill {
                skill_id,
                owner,
                slug,
                source_kind,
            } => UserMessageAttachment::Skill {
                capability: TurnSkillCapabilitySummary {
                    skill_id: skill_id.clone(),
                    label: self.label.clone(),
                    owner: owner.clone(),
                    slug: slug.clone(),
                    source_kind: source_kind.clone(),
                    pack: None,
                },
            },
            ComposerCapabilityKind::McpServer { name, scope_kind } => {
                UserMessageAttachment::McpServer {
                    capability: TurnMcpServerCapabilitySummary {
                        id: self.id.clone(),
                        label: self.label.clone(),
                        name: name.clone(),
                        scope_kind: *scope_kind,
                    },
                }
            }
            ComposerCapabilityKind::McpTool {
                server_name,
                raw_tool_name,
                scope_kind,
            } => UserMessageAttachment::McpTool {
                capability: TurnMcpToolCapabilitySummary {
                    id: self.id.clone(),
                    label: self.label.clone(),
                    server_name: server_name.clone(),
                    raw_tool_name: raw_tool_name.clone(),
                    scope_kind: *scope_kind,
                },
            },
        }
    }
}

impl ComposerCapabilityKind {
    pub fn key(&self) -> String {
        match self {
            Self::Skill { skill_id, .. } => pioneer_protocol::skill_capability_key(skill_id),
            Self::McpServer { name, scope_kind } => {
                pioneer_protocol::mcp_server_capability_key(*scope_kind, name)
            }
            Self::McpTool {
                server_name,
                raw_tool_name,
                scope_kind,
            } => pioneer_protocol::mcp_tool_capability_key(*scope_kind, server_name, raw_tool_name),
        }
    }
}

pub fn add_composer_capability(
    composer_capabilities: &mut Vec<ComposerCapability>,
    capability: ComposerCapability,
) -> bool {
    let initial_len = composer_capabilities.len();
    composer_capabilities.retain(|existing| !composer_capabilities_conflict(existing, &capability));

    let key = capability.key();
    if composer_capabilities
        .iter()
        .any(|existing| existing.key() == key)
    {
        return composer_capabilities.len() != initial_len;
    }

    composer_capabilities.push(capability);
    true
}

pub fn remove_composer_capability_at(
    composer_capabilities: &mut Vec<ComposerCapability>,
    index: usize,
) -> bool {
    if index >= composer_capabilities.len() {
        return false;
    }
    composer_capabilities.remove(index);
    true
}

pub fn composer_capabilities_conflict(
    existing: &ComposerCapability,
    incoming: &ComposerCapability,
) -> bool {
    composer_capability_kinds_conflict(&existing.kind, &incoming.kind)
}

pub fn composer_capability_kinds_conflict(
    existing: &ComposerCapabilityKind,
    incoming: &ComposerCapabilityKind,
) -> bool {
    match (existing, incoming) {
        (
            ComposerCapabilityKind::McpServer { name, scope_kind },
            ComposerCapabilityKind::McpTool {
                server_name,
                scope_kind: tool_scope_kind,
                ..
            },
        ) => name == server_name && scope_kind == tool_scope_kind,
        (
            ComposerCapabilityKind::McpTool {
                server_name,
                scope_kind,
                ..
            },
            ComposerCapabilityKind::McpServer {
                name,
                scope_kind: server_scope_kind,
            },
        ) => server_name == name && scope_kind == server_scope_kind,
        _ => false,
    }
}

pub fn turn_capabilities_from_composer_capabilities(
    capabilities: &[ComposerCapability],
) -> Vec<TurnCapability> {
    capabilities
        .iter()
        .map(ComposerCapability::to_turn_capability)
        .collect()
}

pub fn user_message_attachments_from_composer_capabilities(
    capabilities: &[ComposerCapability],
) -> Vec<UserMessageAttachment> {
    capabilities
        .iter()
        .map(ComposerCapability::to_user_message_attachment)
        .collect()
}

pub fn toggle_selected_capability_key(selected: &mut HashSet<String>, key: &str) {
    if !selected.remove(key) {
        selected.insert(key.to_owned());
    }
}

pub fn toggle_mcp_capability_selection(
    selected: &mut HashSet<String>,
    server_rows: &[SelectableMcpCapability],
    tool_rows: &[SelectableMcpCapability],
    row: &SelectableMcpCapability,
) -> McpCapabilitySelectionToggle {
    if row.raw_tool_name.is_none() {
        if selected.remove(row.key.as_str()) {
            return McpCapabilitySelectionToggle {
                collapse_active_server: false,
            };
        }

        selected.insert(row.key.clone());
        for tool_row in tool_rows
            .iter()
            .filter(|tool_row| tool_row.server_id == row.server_id)
        {
            selected.remove(tool_row.key.as_str());
        }
        return McpCapabilitySelectionToggle {
            collapse_active_server: true,
        };
    }

    if !selected.remove(row.key.as_str()) {
        selected.insert(row.key.clone());
        if let Some(server_row) = server_rows
            .iter()
            .find(|server_row| server_row.server_id == row.server_id)
        {
            selected.remove(server_row.key.as_str());
        }
    }

    McpCapabilitySelectionToggle {
        collapse_active_server: false,
    }
}

pub fn selected_mcp_server_ids(
    server_rows: &[SelectableMcpCapability],
    selected: &HashSet<String>,
) -> HashSet<String> {
    server_rows
        .iter()
        .filter(|row| selected.contains(row.key.as_str()) && row.selectable)
        .map(|row| row.server_id.clone())
        .collect()
}

pub fn selectable_mcp_server_ids(rows: &[SelectableMcpCapability]) -> Vec<String> {
    rows.iter()
        .filter(|row| row.raw_tool_name.is_none() && row.selectable)
        .map(|row| row.server_id.clone())
        .collect()
}

pub fn loaded_mcp_tool_server_ids(rows: &[SelectableMcpCapability]) -> HashSet<String> {
    rows.iter()
        .filter(|row| row.raw_tool_name.is_some())
        .map(|row| row.server_id.clone())
        .collect()
}

pub fn replace_skill_capability_rows(
    skill_rows: &mut Vec<SelectableSkillCapability>,
    selected: &mut HashSet<String>,
    rows: Vec<SelectableSkillCapability>,
) {
    *skill_rows = rows;
    retain_selected_skill_capability_keys(selected, skill_rows.as_slice());
}

pub fn replace_mcp_server_capability_rows(
    server_rows: &mut Vec<SelectableMcpCapability>,
    tool_rows: &[SelectableMcpCapability],
    selected: &mut HashSet<String>,
    rows: Vec<SelectableMcpCapability>,
) {
    *server_rows = rows;
    retain_selected_mcp_capability_keys(selected, server_rows.as_slice(), tool_rows);
}

pub fn mcp_tool_capability_rows_loaded(
    tool_rows: &[SelectableMcpCapability],
    server_id: &str,
) -> bool {
    tool_rows
        .iter()
        .any(|row| row.server_id.as_str() == server_id)
}

pub fn toggle_mcp_tool_capability_panel(
    selected: &HashSet<String>,
    server_rows: &[SelectableMcpCapability],
    tool_rows: &[SelectableMcpCapability],
    active_server_id: &mut Option<String>,
    tool_error: &mut Option<String>,
    loading_server_id: Option<&str>,
    server_id: &str,
) -> bool {
    if server_rows
        .iter()
        .any(|row| row.server_id.as_str() == server_id && selected.contains(&row.key))
    {
        *active_server_id = None;
        return false;
    }

    if active_server_id.as_deref() == Some(server_id) {
        *active_server_id = None;
        return false;
    }

    if mcp_tool_capability_rows_loaded(tool_rows, server_id) || loading_server_id == Some(server_id)
    {
        *active_server_id = Some(server_id.to_owned());
        *tool_error = None;
        return false;
    }

    true
}

pub fn replace_mcp_tool_capability_rows_for_server(
    tool_rows: &mut Vec<SelectableMcpCapability>,
    server_rows: &[SelectableMcpCapability],
    selected: &mut HashSet<String>,
    server_id: &str,
    rows: Vec<SelectableMcpCapability>,
) {
    tool_rows.retain(|row| row.server_id.as_str() != server_id);
    tool_rows.extend(rows);
    retain_selected_mcp_capability_keys(selected, server_rows, tool_rows.as_slice());
}

pub fn merge_mcp_tool_capability_rows(
    tool_rows: &mut Vec<SelectableMcpCapability>,
    server_rows: &[SelectableMcpCapability],
    selected: &mut HashSet<String>,
    rows: Vec<SelectableMcpCapability>,
) -> bool {
    let server_ids = rows
        .iter()
        .map(|row| row.server_id.clone())
        .collect::<HashSet<_>>();
    if server_ids.is_empty() {
        return false;
    }

    tool_rows.retain(|row| !server_ids.contains(row.server_id.as_str()));
    tool_rows.extend(rows);
    retain_selected_mcp_capability_keys(selected, server_rows, tool_rows.as_slice());
    true
}

pub fn retain_selected_skill_capability_keys(
    selected: &mut HashSet<String>,
    rows: &[SelectableSkillCapability],
) {
    let valid_keys = rows
        .iter()
        .filter(|row| row.selectable)
        .map(|row| row.key.as_str())
        .collect::<HashSet<_>>();
    selected.retain(|key| valid_keys.contains(key.as_str()));
}

pub fn retain_selected_mcp_capability_keys(
    selected: &mut HashSet<String>,
    server_rows: &[SelectableMcpCapability],
    tool_rows: &[SelectableMcpCapability],
) {
    let valid_keys = server_rows
        .iter()
        .chain(tool_rows.iter())
        .filter(|row| row.selectable)
        .map(|row| row.key.as_str())
        .collect::<HashSet<_>>();
    selected.retain(|key| valid_keys.contains(key.as_str()));
}

pub fn selected_skill_composer_capabilities_from_rows(
    rows: &[SelectableSkillCapability],
    selected: &HashSet<String>,
) -> Vec<ComposerCapability> {
    rows.iter()
        .filter(|row| selected.contains(row.key.as_str()) && row.selectable)
        .map(|row| ComposerCapability {
            id: pioneer_protocol::skill_capability_key(&row.skill_id),
            label: row.label.clone(),
            kind: ComposerCapabilityKind::Skill {
                skill_id: row.skill_id.clone(),
                owner: row.owner.clone(),
                slug: row.slug.clone(),
                source_kind: row.source_kind.clone(),
            },
        })
        .collect()
}

pub fn selected_mcp_composer_capabilities_from_rows(
    server_rows: &[SelectableMcpCapability],
    tool_rows: &[SelectableMcpCapability],
    selected: &HashSet<String>,
) -> Vec<ComposerCapability> {
    let selected_server_ids = selected_mcp_server_ids(server_rows, selected);

    server_rows
        .iter()
        .chain(tool_rows.iter())
        .filter(|row| selected.contains(row.key.as_str()) && row.selectable)
        .filter(|row| {
            row.raw_tool_name.is_none() || !selected_server_ids.contains(row.server_id.as_str())
        })
        .cloned()
        .map(mcp_row_to_composer_capability)
        .collect()
}

/// Replaces the MCP portion of a composer selection from canonical picker
/// rows while preserving unrelated capabilities. Whole-server selection wins
/// over individual tools for the same server through
/// [`selected_mcp_composer_capabilities_from_rows`].
pub fn replace_selected_mcp_composer_capabilities(
    current: &[ComposerCapability],
    server_rows: &[SelectableMcpCapability],
    tool_rows: &[SelectableMcpCapability],
    selected: &HashSet<String>,
) -> Vec<ComposerCapability> {
    let mut capabilities = current
        .iter()
        .filter(|capability| {
            !matches!(
                &capability.kind,
                ComposerCapabilityKind::McpServer { .. } | ComposerCapabilityKind::McpTool { .. }
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    for capability in selected_mcp_composer_capabilities_from_rows(server_rows, tool_rows, selected)
    {
        add_composer_capability(&mut capabilities, capability);
    }
    capabilities
}

pub fn filter_selectable_skill_capability_rows(
    rows: &[SelectableSkillCapability],
    query: &str,
) -> Vec<SelectableSkillCapability> {
    let query = normalize_capability_query(query);
    rows.iter()
        .filter(|row| {
            query.is_empty()
                || row.label.to_ascii_lowercase().contains(query.as_str())
                || row
                    .display_name
                    .to_ascii_lowercase()
                    .contains(query.as_str())
                || row
                    .owner
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains(query.as_str())
                || row.slug.to_ascii_lowercase().contains(query.as_str())
                || row
                    .description
                    .to_ascii_lowercase()
                    .contains(query.as_str())
        })
        .cloned()
        .collect()
}

pub fn filter_selectable_mcp_capability_rows(
    rows: &[SelectableMcpCapability],
    query: &str,
) -> Vec<SelectableMcpCapability> {
    let query = normalize_capability_query(query);
    rows.iter()
        .filter(|row| {
            row.selectable && selectable_mcp_capability_matches_query(row, query.as_str())
        })
        .cloned()
        .collect()
}

pub fn filter_active_mcp_tool_capability_rows(
    rows: &[SelectableMcpCapability],
    active_server_id: Option<&str>,
    query: &str,
) -> Vec<SelectableMcpCapability> {
    let Some(active_server_id) = active_server_id else {
        return Vec::new();
    };
    let active_rows = rows
        .iter()
        .filter(|row| row.server_id.as_str() == active_server_id)
        .cloned()
        .collect::<Vec<_>>();
    filter_selectable_mcp_capability_rows(active_rows.as_slice(), query)
}

pub fn filter_search_mcp_tool_capability_rows(
    rows: &[SelectableMcpCapability],
    selected_server_ids: &HashSet<String>,
    query: &str,
) -> Vec<SelectableMcpCapability> {
    filter_selectable_mcp_capability_rows(rows, query)
        .into_iter()
        .filter(|row| {
            row.raw_tool_name.is_some() && !selected_server_ids.contains(row.server_id.as_str())
        })
        .collect()
}

pub fn selectable_skill_from_item(skill: &SkillListItem) -> SelectableSkillCapability {
    let unavailable_reason = if !skill.policy.enabled {
        Some(SkillCapabilityUnavailableReason::DisabledByPolicy)
    } else if skill.status != "active" {
        Some(SkillCapabilityUnavailableReason::Inactive {
            status_reason: skill.status_reason.clone(),
        })
    } else {
        None
    };
    let key = pioneer_protocol::skill_capability_key(&skill.skill_id);
    let label =
        skill_presentation::compact_skill_label(skill.owner.as_deref(), skill.slug.as_str());

    SelectableSkillCapability {
        key,
        skill_id: skill.skill_id.clone(),
        label,
        display_name: skill.display_name.clone(),
        description: skill.description.clone(),
        owner: skill.owner.clone(),
        slug: skill.slug.clone(),
        source_kind: skill.source_kind.clone(),
        selectable: unavailable_reason.is_none(),
        unavailable_reason,
    }
}

pub fn filter_skill_capability_rows(
    skills: &[SkillListItem],
    query: &str,
) -> Vec<SelectableSkillCapability> {
    let mut rows = skills
        .iter()
        .filter(|skill| skill_catalog::skill_is_user_selectable(skill))
        .map(selectable_skill_from_item)
        .collect::<Vec<_>>();
    rows = filter_selectable_skill_capability_rows(rows.as_slice(), query);
    sort_selectable_skill_capability_rows(&mut rows);
    rows
}

pub fn filter_installed_skill_capability_rows(
    skills: &[SkillListItem],
    query: &str,
) -> Vec<SelectableSkillCapability> {
    let installed = skills
        .iter()
        .filter(|skill| skill.install.installed)
        .cloned()
        .collect::<Vec<_>>();
    filter_skill_capability_rows(installed.as_slice(), query)
}

pub fn reduce_composer_skill_picker_rows_response(
    response: SkillListResponse,
    query: &str,
) -> ComposerSkillPickerRowsReduction {
    ComposerSkillPickerRowsReduction {
        rows: filter_installed_skill_capability_rows(response.skills.as_slice(), query),
    }
}

pub fn selectable_mcp_server_from_item(server: &McpListItem) -> SelectableMcpCapability {
    let unavailable_reason = mcp_server_unavailable_reason(server);
    let label = server
        .display_name
        .clone()
        .unwrap_or_else(|| server.name.clone());
    let key = ComposerCapabilityKind::McpServer {
        name: server.name.clone(),
        scope_kind: server.scope,
    }
    .key();

    SelectableMcpCapability {
        key,
        label,
        description: String::new(),
        server_id: server.id.clone(),
        server_name: server.name.clone(),
        raw_tool_name: None,
        scope_kind: server.scope,
        tools_count: Some(server.tools_count),
        selectable: unavailable_reason.is_none(),
        unavailable_reason,
    }
}

pub fn mcp_server_unavailable_reason(
    server: &McpListItem,
) -> Option<McpCapabilityUnavailableReason> {
    if !server.policy.enabled {
        return Some(McpCapabilityUnavailableReason::DisabledByPolicy);
    }
    if !server.runtime.live {
        return Some(McpCapabilityUnavailableReason::RuntimeUnavailable);
    }
    if !matches!(
        server.runtime.state,
        McpRuntimeState::Ready | McpRuntimeState::Degraded
    ) {
        return Some(McpCapabilityUnavailableReason::RuntimeNotReady);
    }
    if server.tools_count == 0 {
        return Some(McpCapabilityUnavailableReason::NoToolCatalog);
    }
    None
}

pub fn filter_mcp_server_capability_rows(
    servers: &[McpListItem],
    query: &str,
) -> Vec<SelectableMcpCapability> {
    let mut rows = servers
        .iter()
        .map(selectable_mcp_server_from_item)
        .collect::<Vec<_>>();
    rows = filter_selectable_mcp_capability_rows(rows.as_slice(), query);
    sort_selectable_mcp_capability_rows(&mut rows);
    rows
}

pub fn reduce_composer_mcp_server_picker_rows_response(
    response: McpListResponse,
    query: &str,
) -> ComposerMcpServerPickerRowsReduction {
    let rows = filter_mcp_server_capability_rows(response.servers.as_slice(), query);
    let prefetch_server_ids = selectable_mcp_server_ids(rows.as_slice());

    ComposerMcpServerPickerRowsReduction {
        rows,
        prefetch_server_ids,
    }
}

pub fn filter_mcp_tool_capability_rows(
    details: &McpServerDetailsResponse,
    query: &str,
) -> Vec<SelectableMcpCapability> {
    let query = normalize_capability_query(query);
    let server = &details.server;
    let server_label = server
        .display_name
        .clone()
        .unwrap_or_else(|| server.name.clone());
    let server_unavailable_reason = mcp_server_unavailable_reason(server);
    let server_selectable = server_unavailable_reason.is_none();
    let mut rows = details
        .catalog
        .tools
        .iter()
        .map(|tool| {
            let label = tool
                .title
                .clone()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| tool.name.clone());
            let description = tool.description.clone().unwrap_or_default();
            let unavailable_reason = if server_selectable {
                None
            } else {
                server_unavailable_reason
            };
            let key = ComposerCapabilityKind::McpTool {
                server_name: server.name.clone(),
                raw_tool_name: tool.name.clone(),
                scope_kind: server.scope,
            }
            .key();
            SelectableMcpCapability {
                key,
                label: format!("{server_label}/{label}"),
                description,
                server_id: server.id.clone(),
                server_name: server.name.clone(),
                raw_tool_name: Some(tool.name.clone()),
                scope_kind: server.scope,
                tools_count: None,
                selectable: unavailable_reason.is_none(),
                unavailable_reason,
            }
        })
        .filter(|row| {
            row.selectable && selectable_mcp_capability_matches_query(row, query.as_str())
        })
        .collect::<Vec<_>>();
    sort_selectable_mcp_capability_rows(&mut rows);
    rows
}

pub fn reduce_composer_mcp_tool_picker_rows_response(
    response: McpServerDetailsResponse,
    query: &str,
) -> ComposerMcpToolPickerRowsReduction {
    ComposerMcpToolPickerRowsReduction {
        rows: filter_mcp_tool_capability_rows(&response, query),
    }
}

pub fn mcp_row_to_composer_capability(row: SelectableMcpCapability) -> ComposerCapability {
    let kind = match row.raw_tool_name {
        Some(raw_tool_name) => ComposerCapabilityKind::McpTool {
            server_name: row.server_name,
            raw_tool_name,
            scope_kind: row.scope_kind,
        },
        None => ComposerCapabilityKind::McpServer {
            name: row.server_name,
            scope_kind: row.scope_kind,
        },
    };
    ComposerCapability {
        id: row.key,
        label: row.label,
        kind,
    }
}

pub fn has_capability_query(query: &str) -> bool {
    !normalize_capability_query(query).is_empty()
}

fn sort_selectable_skill_capability_rows(rows: &mut [SelectableSkillCapability]) {
    rows.sort_by(|left, right| {
        left.label
            .to_ascii_lowercase()
            .cmp(&right.label.to_ascii_lowercase())
            .then_with(|| left.key.cmp(&right.key))
    });
}

fn sort_selectable_mcp_capability_rows(rows: &mut [SelectableMcpCapability]) {
    rows.sort_by(|left, right| {
        left.label
            .to_ascii_lowercase()
            .cmp(&right.label.to_ascii_lowercase())
            .then_with(|| left.key.cmp(&right.key))
    });
}

fn selectable_mcp_capability_matches_query(row: &SelectableMcpCapability, query: &str) -> bool {
    query.is_empty()
        || row.label.to_ascii_lowercase().contains(query)
        || row.server_name.to_ascii_lowercase().contains(query)
        || row
            .raw_tool_name
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .contains(query)
        || row.description.to_ascii_lowercase().contains(query)
        || row
            .tools_count
            .is_some_and(|count| count.to_string().contains(query))
}

fn normalize_capability_query(query: &str) -> String {
    query.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        McpManagementDetails, McpPolicyState, McpRuntimeStatus, McpServerCatalogDetails,
        McpServerHealthDetails, McpServerStatus, McpSourceKind, McpToolCatalogItem,
        McpTransportSummary, SkillHealthSummary, SkillInstallState, SkillPolicyState,
    };

    fn skill_id(seed: &str) -> SkillId {
        let mut value = seed
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .take(21)
            .collect::<String>();
        while value.len() < 21 {
            value.push('X');
        }
        SkillId::new(value).expect("test skill id")
    }

    fn skill_key(seed: &str) -> String {
        pioneer_protocol::skill_capability_key(&skill_id(seed))
    }

    #[test]
    fn canonical_capability_matrix_covers_every_supported_policy_shape() {
        assert_eq!(
            COMPOSER_CAPABILITY_MATRIX
                .iter()
                .map(|case| case.id)
                .collect::<Vec<_>>(),
            vec![
                "native",
                "cli_neither",
                "cli_skills_only",
                "cli_mcp_only",
                "cli_both",
            ]
        );
        for case in COMPOSER_CAPABILITY_MATRIX {
            assert_eq!(case.target.policy().supports_skills, case.supports_skills);
            assert_eq!(
                case.target.policy().supports_mcp_tools,
                case.supports_mcp_tools
            );
        }
    }

    #[test]
    fn capability_picker_entry_points_are_visible_for_every_policy_shape() {
        for case in COMPOSER_CAPABILITY_MATRIX {
            assert_eq!(
                composer_capability_menu_visibility(case.target),
                ComposerCapabilityMenuVisibility {
                    skills: true,
                    mcp: true,
                    any: true,
                },
                "{}",
                case.id
            );
        }
    }

    fn skill_capability(slug: &str) -> ComposerCapability {
        skill_capability_from_source(slug, "user")
    }

    fn skill_capability_from_source(slug: &str, source_kind: &str) -> ComposerCapability {
        let skill_id = skill_id(format!("{source_kind}{slug}").as_str());
        ComposerCapability {
            id: pioneer_protocol::skill_capability_key(&skill_id),
            label: slug.to_owned(),
            kind: ComposerCapabilityKind::Skill {
                skill_id,
                owner: None,
                slug: slug.to_owned(),
                source_kind: source_kind.to_owned(),
            },
        }
    }

    fn mcp_tool_capability(server_name: &str, raw_tool_name: &str) -> ComposerCapability {
        ComposerCapability {
            id: format!("mcp-tool:workspace:{server_name}:{raw_tool_name}"),
            label: format!("{server_name} / {raw_tool_name}"),
            kind: ComposerCapabilityKind::McpTool {
                server_name: server_name.to_owned(),
                raw_tool_name: raw_tool_name.to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    fn mcp_server_capability(server_name: &str) -> ComposerCapability {
        ComposerCapability {
            id: format!("mcp-server:workspace:{server_name}"),
            label: server_name.to_owned(),
            kind: ComposerCapabilityKind::McpServer {
                name: server_name.to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        }
    }

    fn skill_item(slug: &str) -> SkillListItem {
        SkillListItem {
            skill_id: skill_id(slug),
            pack: None,
            owner: None,
            slug: slug.to_owned(),
            source_kind: "user".to_owned(),
            display_name: "Example".to_owned(),
            description: "Example skill".to_owned(),
            version: None,
            fingerprint: "fingerprint".to_owned(),
            trust_level: "community".to_owned(),
            install: SkillInstallState {
                managed: false,
                installed: true,
                lifecycle_editable: true,
                install_path: None,
                updated_at: None,
            },
            policy: SkillPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
                allow_implicit_invocation_editable: true,
            },
            health: SkillHealthSummary {
                status: "ok".to_owned(),
                dependency_failures: Vec::new(),
                security_blocks: Vec::new(),
                validation_issues: Vec::new(),
            },
            status: "active".to_owned(),
            status_reason: None,
        }
    }

    fn mcp_server(name: &str) -> McpListItem {
        McpListItem {
            id: format!("mcp:{name}"),
            name: name.to_owned(),
            display_name: None,
            scope: McpScopeKind::Workspace,
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            runtime: McpRuntimeStatus {
                state: McpRuntimeState::Ready,
                live: true,
                last_seen_at: None,
            },
            tools_count: 1,
            resources_count: 0,
            resource_templates_count: 0,
            prompts_count: 0,
            status: McpServerStatus::Ready,
        }
    }

    fn mcp_tool(name: &str, description: &str) -> McpToolCatalogItem {
        McpToolCatalogItem {
            name: name.to_owned(),
            title: None,
            description: Some(description.to_owned()),
            input_schema_summary: None,
            annotations: None,
        }
    }

    #[test]
    fn capability_policy_filters_native_and_all_cli_capability_matrices_independently() {
        let input = vec![
            skill_capability_from_source("user-first", "user"),
            mcp_server_capability("docs"),
            skill_capability_from_source("registry-second", "registry"),
            skill_capability_from_source("browser", "system"),
            mcp_tool_capability("docs", "search"),
            skill_capability_from_source("unknown", "future"),
        ];
        let original = input.clone();
        let user_first = input[0].id.as_str();
        let registry_second = input[2].id.as_str();
        let browser = input[3].id.as_str();
        let unknown = input[5].id.as_str();

        let cases = [
            (
                "native",
                ComposerCapabilityTarget::native(),
                vec![
                    user_first,
                    "mcp-server:workspace:docs",
                    registry_second,
                    browser,
                    "mcp-tool:workspace:docs:search",
                    unknown,
                ],
            ),
            (
                "unsupported CLI",
                ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(false, false)),
                vec![],
            ),
            (
                "skills-only CLI",
                ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, false)),
                vec![user_first, registry_second, browser],
            ),
            (
                "MCP-only CLI",
                ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(false, true)),
                vec![
                    "mcp-server:workspace:docs",
                    "mcp-tool:workspace:docs:search",
                ],
            ),
            (
                "combined CLI",
                ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, true)),
                vec![
                    user_first,
                    "mcp-server:workspace:docs",
                    registry_second,
                    browser,
                    "mcp-tool:workspace:docs:search",
                ],
            ),
        ];

        for (case, target, expected_ids) in cases {
            let reduction = reduce_composer_capabilities_for_target(&input, target);
            assert_eq!(
                reduction
                    .capabilities
                    .iter()
                    .map(|capability| capability.id.as_str())
                    .collect::<Vec<_>>(),
                expected_ids,
                "{case}"
            );
            assert_eq!(
                reduction.capabilities.len() + reduction.removed.len(),
                input.len(),
                "{case} must account for every input capability"
            );
        }

        assert_eq!(input, original);
    }

    #[test]
    fn capability_policy_reports_stable_kind_and_source_removal_reasons() {
        let input = vec![
            skill_capability_from_source("user", "user"),
            skill_capability_from_source("browser", "system"),
            skill_capability_from_source("unknown", "future"),
            mcp_server_capability("docs"),
            mcp_tool_capability("docs", "search"),
        ];

        let unsupported = reduce_composer_capabilities_for_target(
            &input,
            ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::unsupported_cli()),
        );
        assert_eq!(
            unsupported
                .removed
                .iter()
                .map(|removed| removed.reason)
                .collect::<Vec<_>>(),
            vec![
                ComposerCapabilityRemovalReason::SkillsUnsupported,
                ComposerCapabilityRemovalReason::SkillsUnsupported,
                ComposerCapabilityRemovalReason::SkillsUnsupported,
                ComposerCapabilityRemovalReason::McpToolsUnsupported,
                ComposerCapabilityRemovalReason::McpToolsUnsupported,
            ]
        );

        let combined = reduce_composer_capabilities_for_target(
            &input,
            ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, true)),
        );
        assert_eq!(
            combined
                .removed
                .iter()
                .map(|removed| (removed.capability.id.as_str(), removed.reason))
                .collect::<Vec<_>>(),
            vec![(
                input[2].id.as_str(),
                ComposerCapabilityRemovalReason::SkillSourceNotExportable,
            )]
        );
    }

    #[test]
    fn cli_runtime_capabilities_map_directly_to_policy_and_default_mcp_closed() {
        let missing_or_old = RuntimeCapabilities::default();
        assert_eq!(
            ComposerCapabilityPolicy::from_cli_runtime_capabilities(&missing_or_old),
            ComposerCapabilityPolicy::unsupported_cli()
        );

        let proven = RuntimeCapabilities {
            supports_skills: true,
            supports_mcp_tools: true,
            ..Default::default()
        };
        let target = ComposerCapabilityTarget::from_cli_runtime_capabilities(&proven);
        assert!(target.is_cli());
        assert_eq!(target.policy(), ComposerCapabilityPolicy::cli(true, true));
    }

    #[test]
    fn cli_submission_preserves_explicit_capabilities_for_gateway_live_validation() {
        let input = vec![
            skill_capability_from_source("user", "user"),
            skill_capability_from_source("registry", "registry"),
            skill_capability_from_source("browser", "system"),
            skill_capability_from_source("unknown", "future"),
            mcp_server_capability("docs"),
            mcp_tool_capability("docs", "search"),
        ];

        let reduction = reduce_composer_capabilities_for_target(
            &input,
            ComposerCapabilityTarget::cli_submission(),
        );

        assert_eq!(
            reduction
                .capabilities
                .iter()
                .map(|capability| capability.id.as_str())
                .collect::<Vec<_>>(),
            vec![
                input[0].id.as_str(),
                input[1].id.as_str(),
                input[2].id.as_str(),
                "mcp-server:workspace:docs",
                "mcp-tool:workspace:docs:search",
            ]
        );
        assert_eq!(
            reduction
                .removed
                .iter()
                .map(|removed| (removed.capability.id.as_str(), removed.reason))
                .collect::<Vec<_>>(),
            vec![(
                input[3].id.as_str(),
                ComposerCapabilityRemovalReason::SkillSourceNotExportable,
            )]
        );
    }

    #[test]
    fn presentation_readiness_cannot_hide_pickers_or_strip_the_structural_submission_plan() {
        let input = vec![
            skill_capability_from_source("user", "user"),
            skill_capability_from_source("browser", "system"),
            skill_capability_from_source("unknown", "future"),
            mcp_server_capability("docs"),
            mcp_tool_capability("docs", "search"),
        ];
        let provider = Some("cli_runtime:codex");

        let presentation = project_composer_capability_presentation(
            provider,
            &[],
            "send",
            false,
            input.as_slice(),
        );
        assert!(presentation.capabilities.is_empty());
        assert_eq!(
            presentation.menu_visibility,
            ComposerCapabilityMenuVisibility {
                skills: true,
                mcp: true,
                any: true,
            }
        );

        let submission = plan_composer_submission(provider, "send", false, input.as_slice());
        assert_eq!(
            submission
                .capabilities
                .iter()
                .map(|capability| capability.id.as_str())
                .collect::<Vec<_>>(),
            vec![
                input[0].id.as_str(),
                input[1].id.as_str(),
                "mcp-server:workspace:docs",
                "mcp-tool:workspace:docs:search",
            ]
        );
        assert_eq!(
            submission
                .removed
                .iter()
                .map(|removed| (removed.capability.id.as_str(), removed.reason))
                .collect::<Vec<_>>(),
            vec![(
                input[2].id.as_str(),
                ComposerCapabilityRemovalReason::SkillSourceNotExportable,
            )]
        );
        assert!(submission.has_composer_payload);
    }

    #[test]
    fn text_and_voice_build_the_same_cli_capability_submission() {
        let input = vec![
            skill_capability_from_source("registry", "registry"),
            mcp_server_capability("appstoreconnect"),
            mcp_tool_capability("appstoreconnect", "list_apps"),
        ];
        let text = plan_composer_submission(
            Some("cli_runtime:codex"),
            "inspect releases",
            false,
            input.as_slice(),
        );
        let voice = plan_composer_submission(Some("cli_runtime:codex"), "", true, input.as_slice());

        assert_eq!(text.target, voice.target);
        assert_eq!(text.capabilities, voice.capabilities);
        assert_eq!(text.removed, voice.removed);
        assert!(text.has_composer_payload);
        assert!(voice.has_composer_payload);
    }

    #[test]
    fn selectable_skill_target_filter_allows_system_browser_and_rejects_unknown_cli_sources() {
        let rows = ["user", "registry", "system", "future"]
            .into_iter()
            .map(|source_kind| {
                let mut item = skill_item(source_kind);
                item.source_kind = source_kind.to_owned();
                selectable_skill_from_item(&item)
            })
            .collect::<Vec<_>>();
        let original = rows.clone();

        assert_eq!(
            filter_selectable_skill_capabilities_for_target(
                &rows,
                ComposerCapabilityTarget::native()
            )
            .iter()
            .map(|row| row.source_kind.as_str())
            .collect::<Vec<_>>(),
            vec!["user", "registry", "system", "future"]
        );
        assert!(
            filter_selectable_skill_capabilities_for_target(
                &rows,
                ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::unsupported_cli())
            )
            .is_empty()
        );

        let cli_rows = filter_selectable_skill_capabilities_for_target(
            &rows,
            ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, false)),
        );
        assert_eq!(
            cli_rows
                .iter()
                .map(|row| row.source_kind.as_str())
                .collect::<Vec<_>>(),
            vec!["user", "registry", "system"]
        );
        assert!(cli_rows.iter().all(|row| {
            selectable_skill_capability_is_eligible_for_target(
                row,
                ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, false)),
            )
        }));
        assert_eq!(rows, original);
    }

    #[test]
    fn selectable_mcp_rows_use_the_same_server_and_tool_policy_gate() {
        let server_row = selectable_mcp_server_from_item(&mcp_server("docs"));
        let tool_row = filter_mcp_tool_capability_rows(
            &mcp_details("docs", vec![mcp_tool("search", "Search docs")]),
            "",
        )
        .remove(0);
        let rows = vec![server_row, tool_row];

        let skills_only = ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, false));
        assert!(filter_selectable_mcp_capabilities_for_target(&rows, skills_only).is_empty());

        for target in [
            ComposerCapabilityTarget::native(),
            ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(false, true)),
            ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::cli(true, true)),
        ] {
            assert_eq!(
                filter_selectable_mcp_capabilities_for_target(&rows, target),
                rows
            );
        }
    }

    fn mcp_details(server_name: &str, tools: Vec<McpToolCatalogItem>) -> McpServerDetailsResponse {
        let mut server = mcp_server(server_name);
        server.tools_count = tools.len();
        McpServerDetailsResponse {
            snapshot_version: 1,
            generated_at: 1_700_000_000,
            server: server.clone(),
            catalog: McpServerCatalogDetails {
                catalog_version: Some("catalog-v1".to_owned()),
                generated_at: Some(1_700_000_000),
                server_info: serde_json::json!({ "name": server.name }),
                server_instructions_hash: None,
                tools,
                resources: Vec::new(),
                resource_templates: Vec::new(),
                prompts: Vec::new(),
            },
            management: Some(McpManagementDetails {
                scope: McpScopeKind::Workspace,
                source_kind: McpSourceKind::Config,
                transport: McpTransportSummary::Stdio {
                    command: "server".to_owned(),
                },
                fingerprint: "fingerprint".to_owned(),
                health: McpServerHealthDetails {
                    runtime: server.runtime.clone(),
                    status: server.status,
                    status_reason: None,
                    last_error: None,
                    retry_attempt: None,
                    next_retry_at: None,
                    catalog_version: Some("catalog-v1".to_owned()),
                    stderr_tail: None,
                },
                audit: Vec::new(),
                recent_bindings: Vec::new(),
            }),
        }
    }

    #[test]
    fn composer_capability_key_is_canonical_by_kind() {
        let skill_id = skill_id("imagegen");
        assert_eq!(
            ComposerCapabilityKind::Skill {
                skill_id: skill_id.clone(),
                owner: Some("owner".to_owned()),
                slug: "imagegen".to_owned(),
                source_kind: "user".to_owned(),
            }
            .key(),
            pioneer_protocol::skill_capability_key(&skill_id)
        );
        assert_eq!(
            ComposerCapabilityKind::McpServer {
                name: "browser".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            }
            .key(),
            "mcp-server:workspace:browser"
        );
        assert_eq!(
            ComposerCapabilityKind::McpTool {
                server_name: "browser".to_owned(),
                raw_tool_name: "open".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            }
            .key(),
            "mcp-tool:workspace:browser:open"
        );
    }

    #[test]
    fn composer_capability_converts_to_turn_capability_without_text_tokens() {
        let capability = skill_capability("imagegen");
        let expected_skill_id = match &capability.kind {
            ComposerCapabilityKind::Skill { skill_id, .. } => skill_id.clone(),
            _ => unreachable!(),
        };
        let turn_capability = capability.to_turn_capability();

        assert_eq!(
            turn_capability.id,
            pioneer_protocol::skill_capability_key(&expected_skill_id)
        );
        assert_eq!(turn_capability.label.as_deref(), Some("imagegen"));
        assert_eq!(
            turn_capability.kind,
            TurnCapabilityKind::Skill {
                skill_id: expected_skill_id,
                pack_id: None,
            }
        );
    }

    #[test]
    fn skill_capability_without_exact_id_is_rejected() {
        let mut without_id =
            serde_json::to_value(skill_capability("docs")).expect("capability should encode");
        without_id["kind"]["Skill"]
            .as_object_mut()
            .expect("skill kind must encode as object")
            .remove("skill_id");

        assert!(serde_json::from_value::<ComposerCapability>(without_id).is_err());

        let mismatched = serde_json::json!({
            "id": "skill:AAAAAAAAAAAAAAAAAAAAA",
            "label": "docs",
            "kind": {
                "Skill": {
                    "skill_id": "BBBBBBBBBBBBBBBBBBBBB",
                    "owner": null,
                    "slug": "docs",
                    "source_kind": "user"
                }
            }
        });
        assert!(serde_json::from_value::<ComposerCapability>(mismatched).is_err());
    }

    #[test]
    fn composer_capability_omits_blank_turn_label() {
        let mut capability = skill_capability("docs");
        capability.label = "  ".to_owned();

        assert_eq!(capability.to_turn_capability().label, None);
    }

    #[test]
    fn add_composer_capability_replaces_server_with_tool_for_same_mcp_server() {
        let mut capabilities = vec![mcp_server_capability("resend")];

        assert!(add_composer_capability(
            &mut capabilities,
            mcp_tool_capability("resend", "send_email"),
        ));

        assert_eq!(capabilities.len(), 1);
        assert!(matches!(
            capabilities[0].kind,
            ComposerCapabilityKind::McpTool {
                ref server_name,
                ref raw_tool_name,
                ..
            } if server_name == "resend" && raw_tool_name == "send_email"
        ));
    }

    #[test]
    fn add_composer_capability_replaces_tools_with_server_for_same_mcp_server() {
        let mut capabilities = vec![
            mcp_tool_capability("resend", "send_email"),
            mcp_tool_capability("browser", "open"),
        ];

        assert!(add_composer_capability(
            &mut capabilities,
            mcp_server_capability("resend"),
        ));

        assert_eq!(capabilities.len(), 2);
        assert!(capabilities.iter().any(|capability| matches!(
            capability.kind,
            ComposerCapabilityKind::McpServer { ref name, .. } if name == "resend"
        )));
        assert!(capabilities.iter().any(|capability| matches!(
            capability.kind,
            ComposerCapabilityKind::McpTool {
                ref server_name,
                ref raw_tool_name,
                ..
            } if server_name == "browser" && raw_tool_name == "open"
        )));
    }

    #[test]
    fn add_composer_capability_skips_duplicate_key() {
        let mut capabilities = vec![skill_capability("docs")];

        assert!(!add_composer_capability(
            &mut capabilities,
            skill_capability("docs"),
        ));

        assert_eq!(capabilities.len(), 1);
    }

    #[test]
    fn remove_composer_capability_at_reports_bounds() {
        let mut capabilities = vec![skill_capability("docs"), skill_capability("imagegen")];

        assert!(remove_composer_capability_at(&mut capabilities, 0));
        assert_eq!(capabilities.len(), 1);
        assert_eq!(capabilities[0].key(), skill_key("userimagegen"));
        assert!(!remove_composer_capability_at(&mut capabilities, 9));
    }

    #[test]
    fn user_message_attachments_from_capabilities_preserve_label() {
        let attachments = user_message_attachments_from_composer_capabilities(&[
            skill_capability("docs"),
            mcp_tool_capability("browser", "open"),
        ]);

        assert_eq!(attachments.len(), 2);
        assert!(matches!(
            attachments[0],
            UserMessageAttachment::Skill { ref capability }
                if capability.slug == "docs" && capability.label == "docs"
        ));
        assert!(matches!(
            attachments[1],
            UserMessageAttachment::McpTool { ref capability }
                if capability.server_name == "browser" && capability.raw_tool_name == "open"
        ));
    }

    #[test]
    fn selectable_skill_preserves_disabled_reason() {
        let mut item = skill_item("tests/example");
        item.policy.enabled = false;

        let row = selectable_skill_from_item(&item);

        assert!(!row.selectable);
        assert_eq!(
            row.unavailable_reason,
            Some(SkillCapabilityUnavailableReason::DisabledByPolicy)
        );
    }

    #[test]
    fn selectable_skill_rows_filter_by_label_slug_and_description() {
        let mut docs = skill_item("tests/docs-writer");
        docs.display_name = "Docs Writer".to_owned();
        docs.description = "Creates release notes".to_owned();
        let mut image = skill_item("tests/imagegen");
        image.display_name = "Imagegen".to_owned();
        image.description = "Generate bitmap assets".to_owned();

        let rows = filter_skill_capability_rows(&[image.clone(), docs.clone()], "release");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].slug, "tests/docs-writer");

        let rows = filter_skill_capability_rows(&[image, docs], "imagegen");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, skill_key("tests/imagegen"));
        assert!(rows[0].selectable);
    }

    #[test]
    fn duplicate_skill_labels_remain_distinct_by_exact_id() {
        let mut first = skill_item("humanizer");
        first.skill_id = SkillId::new("A".repeat(21)).unwrap();
        first.owner = Some("alexander".to_owned());
        first.display_name = "Humanizer".to_owned();
        let mut second = first.clone();
        second.skill_id = SkillId::new("B".repeat(21)).unwrap();

        let rows = filter_skill_capability_rows(&[first, second], "alexander/humanizer");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].label, "alexander/humanizer");
        assert_eq!(rows[1].label, "alexander/humanizer");
        assert_ne!(rows[0].key, rows[1].key);

        let selected = rows
            .iter()
            .map(|row| row.key.clone())
            .collect::<HashSet<_>>();
        let capabilities = selected_skill_composer_capabilities_from_rows(&rows, &selected);
        assert_eq!(capabilities.len(), 2);
        assert_ne!(capabilities[0].key(), capabilities[1].key());
    }

    #[test]
    fn skill_attachment_keeps_exact_id_and_presentation_snapshot_separate() {
        let mut item = skill_item("docs");
        item.skill_id = SkillId::new("Q".repeat(21)).unwrap();
        item.owner = Some("owner".to_owned());
        item.display_name = "Documentation".to_owned();
        let row = selectable_skill_from_item(&item);
        let selected = HashSet::from([row.key.clone()]);
        let capability =
            selected_skill_composer_capabilities_from_rows(std::slice::from_ref(&row), &selected)
                .remove(0);

        item.owner = Some("changed".to_owned());
        item.slug = "renamed".to_owned();

        assert!(matches!(
            capability.to_user_message_attachment(),
            UserMessageAttachment::Skill { capability }
                if capability.skill_id == SkillId::new("Q".repeat(21)).unwrap()
                    && capability.owner.as_deref() == Some("owner")
                    && capability.slug == "docs"
                    && capability.label == "owner/docs"
        ));
    }

    #[test]
    fn capability_query_presence_uses_normalized_query() {
        assert!(!has_capability_query(" \t\n "));
        assert!(has_capability_query(" Docs "));
    }

    #[test]
    fn installed_skill_rows_keep_system_browser_and_hide_required_system_skills() {
        let mut installed = skill_item("tests/installed");
        installed.display_name = "Installed".to_owned();
        let mut uninstalled = skill_item("tests/uninstalled");
        uninstalled.display_name = "Uninstalled".to_owned();
        uninstalled.install.installed = false;
        let mut browser = skill_item("browser");
        browser.source_kind = "system".to_owned();
        browser.display_name = "Browser".to_owned();
        let mut memory = skill_item("memory");
        memory.source_kind = "system".to_owned();
        memory.policy.allow_implicit_invocation = true;
        memory.policy.allow_implicit_invocation_editable = false;

        let rows =
            filter_installed_skill_capability_rows(&[uninstalled, memory, installed, browser], "");

        assert_eq!(
            rows.iter().map(|row| row.slug.as_str()).collect::<Vec<_>>(),
            vec!["browser", "tests/installed"]
        );
    }

    #[test]
    fn skill_picker_response_reduction_returns_installed_rows() {
        let mut installed = skill_item("tests/installed");
        installed.display_name = "Installed".to_owned();
        let mut uninstalled = skill_item("tests/uninstalled");
        uninstalled.install.installed = false;
        let mut browser = skill_item("browser");
        browser.source_kind = "system".to_owned();
        browser.display_name = "Browser".to_owned();
        let mut tasks = skill_item("tasks");
        tasks.source_kind = "system".to_owned();
        tasks.policy.allow_implicit_invocation = true;
        tasks.policy.allow_implicit_invocation_editable = false;

        let reduction = reduce_composer_skill_picker_rows_response(
            SkillListResponse {
                snapshot_version: 1,
                generated_at: 1,
                skills: vec![uninstalled, tasks, browser, installed],
                packs: Vec::new(),
            },
            "",
        );

        assert_eq!(
            reduction
                .rows
                .iter()
                .map(|row| row.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["browser", "tests/installed"]
        );
    }

    #[test]
    fn selected_skill_capabilities_use_only_selectable_selected_rows() {
        let mut selected = HashSet::new();
        let mut docs = selectable_skill_from_item(&skill_item("tests/docs"));
        let image = selectable_skill_from_item(&skill_item("tests/imagegen"));
        docs.selectable = false;
        selected.insert(docs.key.clone());
        selected.insert(image.key.clone());

        retain_selected_skill_capability_keys(&mut selected, &[docs.clone(), image.clone()]);
        let capabilities =
            selected_skill_composer_capabilities_from_rows(&[docs, image], &selected);

        assert_eq!(selected.len(), 1);
        assert_eq!(capabilities.len(), 1);
        assert_eq!(capabilities[0].id, skill_key("tests/imagegen"));
    }

    #[test]
    fn replace_skill_rows_prunes_stale_selection() {
        let docs = selectable_skill_from_item(&skill_item("tests/docs"));
        let image = selectable_skill_from_item(&skill_item("tests/imagegen"));
        let mut rows = vec![docs.clone()];
        let mut selected = HashSet::from([docs.key.clone(), image.key.clone()]);

        replace_skill_capability_rows(&mut rows, &mut selected, vec![image.clone()]);

        assert_eq!(rows, vec![image.clone()]);
        assert_eq!(selected, HashSet::from([image.key]));
    }

    #[test]
    fn mcp_server_requires_live_runtime_and_catalog() {
        let mut server = mcp_server("server-a");
        server.runtime.live = false;
        assert_eq!(
            mcp_server_unavailable_reason(&server),
            Some(McpCapabilityUnavailableReason::RuntimeUnavailable)
        );

        server.runtime.live = true;
        server.tools_count = 0;
        assert_eq!(
            mcp_server_unavailable_reason(&server),
            Some(McpCapabilityUnavailableReason::NoToolCatalog)
        );
    }

    #[test]
    fn mcp_server_rows_filter_and_sort_by_label() {
        let mut resend = mcp_server("resend");
        resend.display_name = Some("Resend".to_owned());
        let mut browser = mcp_server("browser");
        browser.display_name = Some("Browser".to_owned());

        let rows = filter_mcp_server_capability_rows(&[resend, browser], "bro");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, "mcp-server:workspace:browser");
        assert_eq!(rows[0].tools_count, Some(1));
    }

    #[test]
    fn mcp_server_rows_hide_unavailable_servers() {
        let mut unavailable = mcp_server("unavailable");
        unavailable.runtime.live = false;
        let available = mcp_server("available");

        let rows = filter_mcp_server_capability_rows(&[unavailable, available], "");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].server_name, "available");
        assert!(rows[0].selectable);
    }

    #[test]
    fn mcp_server_picker_response_reduction_returns_rows_and_prefetch_ids() {
        let browser = mcp_server("browser");

        let reduction = reduce_composer_mcp_server_picker_rows_response(
            McpListResponse {
                snapshot_version: 1,
                generated_at: 1,
                servers: vec![browser],
            },
            "",
        );

        assert_eq!(reduction.rows.len(), 1);
        assert_eq!(reduction.prefetch_server_ids, vec!["mcp:browser"]);
    }

    #[test]
    fn mcp_server_id_helpers_track_selectable_servers_and_loaded_tool_servers() {
        let mut browser = selectable_mcp_server_from_item(&mcp_server("browser"));
        let resend = selectable_mcp_server_from_item(&mcp_server("resend"));
        browser.selectable = false;
        let tool = filter_mcp_tool_capability_rows(
            &mcp_details("resend", vec![mcp_tool("add_contact", "Add contact")]),
            "",
        )
        .into_iter()
        .next()
        .expect("tool row");

        assert_eq!(
            selectable_mcp_server_ids(&[browser, resend.clone(), tool.clone()]),
            vec![resend.server_id.clone()]
        );
        assert_eq!(
            loaded_mcp_tool_server_ids(&[resend, tool]),
            HashSet::from(["mcp:resend".to_owned()])
        );
    }

    #[test]
    fn mcp_tool_rows_filter_and_convert_to_tool_capability() {
        let details = mcp_details("browser", vec![mcp_tool("open", "Open page")]);

        let rows = filter_mcp_tool_capability_rows(&details, "page");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, "mcp-tool:workspace:browser:open");
        assert_eq!(rows[0].raw_tool_name.as_deref(), Some("open"));
        assert!(rows[0].selectable);

        let capability = mcp_row_to_composer_capability(rows[0].clone());
        assert_eq!(capability.id, "mcp-tool:workspace:browser:open");
    }

    #[test]
    fn mcp_tool_rows_hide_unavailable_tools() {
        let mut details = mcp_details("browser", vec![mcp_tool("open", "Open page")]);
        details.server.runtime.live = false;

        let rows = filter_mcp_tool_capability_rows(&details, "");

        assert!(rows.is_empty());
    }

    #[test]
    fn mcp_tool_picker_response_reduction_returns_tool_rows() {
        let details = mcp_details("browser", vec![mcp_tool("open", "Open page")]);

        let reduction = reduce_composer_mcp_tool_picker_rows_response(details, "");

        assert_eq!(reduction.rows.len(), 1);
        assert_eq!(reduction.rows[0].server_name, "browser");
        assert_eq!(reduction.rows[0].raw_tool_name.as_deref(), Some("open"));
    }

    #[test]
    fn mcp_search_can_match_tools_from_unopened_servers() {
        let browser = mcp_details("browser", vec![mcp_tool("open", "Open page")]);
        let resend = mcp_details("resend", vec![mcp_tool("add_contact", "Add contact")]);
        let mut rows = filter_mcp_tool_capability_rows(&browser, "");
        rows.extend(filter_mcp_tool_capability_rows(&resend, ""));

        let filtered = filter_search_mcp_tool_capability_rows(&rows, &HashSet::new(), "contact");

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].server_name, "resend");
        assert_eq!(filtered[0].raw_tool_name.as_deref(), Some("add_contact"));
    }

    #[test]
    fn mcp_selection_toggle_keeps_server_and_tool_mutually_exclusive() {
        let server = selectable_mcp_server_from_item(&mcp_server("resend"));
        let details = mcp_details(
            "resend",
            vec![mcp_tool("add_contact", "Add contact to audience")],
        );
        let tool = filter_mcp_tool_capability_rows(&details, "contact")
            .into_iter()
            .next()
            .expect("test tool should match query");
        let mut selected = HashSet::from([server.key.clone()]);

        let update = toggle_mcp_capability_selection(
            &mut selected,
            &[server.clone()],
            &[tool.clone()],
            &tool,
        );

        assert!(!update.collapse_active_server);
        assert!(selected.contains(&tool.key));
        assert!(!selected.contains(&server.key));

        let update =
            toggle_mcp_capability_selection(&mut selected, &[server.clone()], &[tool], &server);

        assert!(update.collapse_active_server);
        assert!(selected.contains(&server.key));
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn selected_mcp_capabilities_skip_tools_when_server_is_selected() {
        let server = selectable_mcp_server_from_item(&mcp_server("resend"));
        let details = mcp_details(
            "resend",
            vec![mcp_tool("add_contact", "Add contact to audience")],
        );
        let tool = filter_mcp_tool_capability_rows(&details, "contact")
            .into_iter()
            .next()
            .expect("test tool should match query");
        let mut selected = HashSet::new();
        selected.insert(server.key.clone());
        selected.insert(tool.key.clone());

        retain_selected_mcp_capability_keys(
            &mut selected,
            std::slice::from_ref(&server),
            std::slice::from_ref(&tool),
        );
        let capabilities =
            selected_mcp_composer_capabilities_from_rows(&[server], &[tool], &selected);

        assert_eq!(capabilities.len(), 1);
        assert_eq!(capabilities[0].id, "mcp-server:workspace:resend");
    }

    #[test]
    fn replacing_mcp_picker_selection_preserves_skills_and_removes_stale_mcp_rows() {
        let old_server = mcp_server_capability("old");
        let skill = skill_capability("docs");
        let server = selectable_mcp_server_from_item(&mcp_server("resend"));
        let tool = filter_mcp_tool_capability_rows(
            &mcp_details("resend", vec![mcp_tool("send", "Send email")]),
            "",
        )
        .into_iter()
        .next()
        .expect("tool row");
        let selected = HashSet::from([tool.key.clone()]);

        let capabilities = replace_selected_mcp_composer_capabilities(
            &[skill.clone(), old_server],
            std::slice::from_ref(&server),
            std::slice::from_ref(&tool),
            &selected,
        );

        assert_eq!(capabilities.len(), 2);
        assert_eq!(capabilities[0], skill);
        assert_eq!(capabilities[1].id, "mcp-tool:workspace:resend:send");
    }

    #[test]
    fn toggle_mcp_tool_panel_reuses_loaded_or_loading_rows() {
        let server = selectable_mcp_server_from_item(&mcp_server("resend"));
        let details = mcp_details(
            "resend",
            vec![mcp_tool("add_contact", "Add contact to audience")],
        );
        let tool = filter_mcp_tool_capability_rows(&details, "")
            .into_iter()
            .next()
            .expect("test tool should exist");
        let mut selected = HashSet::new();
        let mut active_server_id = None;
        let mut tool_error = Some("stale".to_owned());

        assert!(!toggle_mcp_tool_capability_panel(
            &selected,
            std::slice::from_ref(&server),
            std::slice::from_ref(&tool),
            &mut active_server_id,
            &mut tool_error,
            None,
            server.server_id.as_str(),
        ));
        assert_eq!(active_server_id.as_deref(), Some(server.server_id.as_str()));
        assert_eq!(tool_error, None);

        assert!(!toggle_mcp_tool_capability_panel(
            &selected,
            std::slice::from_ref(&server),
            &[],
            &mut active_server_id,
            &mut tool_error,
            None,
            server.server_id.as_str(),
        ));
        assert_eq!(active_server_id, None);

        selected.insert(server.key.clone());
        tool_error = Some("keep".to_owned());
        assert!(!toggle_mcp_tool_capability_panel(
            &selected,
            std::slice::from_ref(&server),
            &[],
            &mut active_server_id,
            &mut tool_error,
            Some(server.server_id.as_str()),
            server.server_id.as_str(),
        ));
        assert_eq!(active_server_id, None);
        assert_eq!(tool_error.as_deref(), Some("keep"));

        selected.clear();
        assert!(toggle_mcp_tool_capability_panel(
            &selected,
            &[server],
            &[],
            &mut active_server_id,
            &mut tool_error,
            None,
            "mcp:resend",
        ));
        assert_eq!(active_server_id, None);
    }

    #[test]
    fn mcp_tool_row_replace_and_merge_prune_stale_selection() {
        let browser_server = selectable_mcp_server_from_item(&mcp_server("browser"));
        let resend_server = selectable_mcp_server_from_item(&mcp_server("resend"));
        let browser_tool = filter_mcp_tool_capability_rows(
            &mcp_details("browser", vec![mcp_tool("open", "Open page")]),
            "",
        )
        .into_iter()
        .next()
        .expect("browser tool should exist");
        let resend_tool = filter_mcp_tool_capability_rows(
            &mcp_details("resend", vec![mcp_tool("add_contact", "Add contact")]),
            "",
        )
        .into_iter()
        .next()
        .expect("resend tool should exist");
        let mut tool_rows = vec![browser_tool.clone()];
        let mut selected = HashSet::from([browser_tool.key.clone(), resend_tool.key.clone()]);

        replace_mcp_tool_capability_rows_for_server(
            &mut tool_rows,
            &[browser_server.clone(), resend_server.clone()],
            &mut selected,
            browser_tool.server_id.as_str(),
            Vec::new(),
        );

        assert!(tool_rows.is_empty());
        assert!(selected.is_empty());

        assert!(merge_mcp_tool_capability_rows(
            &mut tool_rows,
            &[browser_server, resend_server],
            &mut selected,
            vec![resend_tool.clone()],
        ));
        assert_eq!(tool_rows, vec![resend_tool]);
        assert!(selected.is_empty());
        assert!(!merge_mcp_tool_capability_rows(
            &mut tool_rows,
            &[],
            &mut selected,
            Vec::new(),
        ));
    }
}
