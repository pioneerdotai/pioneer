//! Composer capability selection helpers.

use pioneer_protocol::{
    McpListItem, McpRuntimeState, McpScopeKind, McpServerDetailsResponse, SkillListItem,
    TurnCapability, TurnCapabilityKind, TurnMcpServerCapabilitySummary,
    TurnMcpToolCapabilitySummary, TurnSkillCapabilitySummary, UserMessageAttachment,
};
use std::collections::HashSet;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerCapability {
    pub id: String,
    pub label: String,
    pub kind: ComposerCapabilityKind,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ComposerCapabilityKind {
    Skill {
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SelectableSkillCapability {
    pub key: String,
    pub label: String,
    pub description: String,
    pub slug: String,
    pub source_kind: String,
    pub selectable: bool,
    pub unavailable_reason: Option<SkillCapabilityUnavailableReason>,
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

impl ComposerCapability {
    pub fn key(&self) -> String {
        self.kind.key()
    }

    pub fn to_turn_capability(&self) -> TurnCapability {
        TurnCapability {
            id: self.id.clone(),
            kind: match &self.kind {
                ComposerCapabilityKind::Skill { slug, source_kind } => TurnCapabilityKind::Skill {
                    slug: slug.clone(),
                    source_kind: source_kind.clone(),
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
            ComposerCapabilityKind::Skill { slug, source_kind } => UserMessageAttachment::Skill {
                capability: TurnSkillCapabilitySummary {
                    id: self.id.clone(),
                    label: self.label.clone(),
                    slug: slug.clone(),
                    source_kind: source_kind.clone(),
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
            Self::Skill { slug, source_kind } => format!("skill:{source_kind}:{slug}"),
            Self::McpServer { name, scope_kind } => {
                format!("mcp-server:{}:{name}", scope_kind.as_str())
            }
            Self::McpTool {
                server_name,
                raw_tool_name,
                scope_kind,
            } => format!(
                "mcp-tool:{}:{server_name}:{raw_tool_name}",
                scope_kind.as_str()
            ),
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
            id: row.key.clone(),
            label: row.label.clone(),
            kind: ComposerCapabilityKind::Skill {
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

pub fn filter_selectable_skill_capability_rows(
    rows: &[SelectableSkillCapability],
    query: &str,
) -> Vec<SelectableSkillCapability> {
    let query = normalize_capability_query(query);
    rows.iter()
        .filter(|row| {
            query.is_empty()
                || row.label.to_ascii_lowercase().contains(query.as_str())
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
        .filter(|row| selectable_mcp_capability_matches_query(row, query.as_str()))
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
    let key = ComposerCapabilityKind::Skill {
        slug: skill.slug.clone(),
        source_kind: skill.source_kind.clone(),
    }
    .key();

    SelectableSkillCapability {
        key,
        label: skill.display_name.clone(),
        description: skill.description.clone(),
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
        .map(selectable_skill_from_item)
        .collect::<Vec<_>>();
    rows = filter_selectable_skill_capability_rows(rows.as_slice(), query);
    sort_selectable_skill_capability_rows(&mut rows);
    rows
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
        .filter(|row| selectable_mcp_capability_matches_query(row, query.as_str()))
        .collect::<Vec<_>>();
    sort_selectable_mcp_capability_rows(&mut rows);
    rows
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
        McpPolicyState, McpRuntimeStatus, McpServerCatalogDetails, McpServerHealthDetails,
        McpServerStatus, McpSourceKind, McpToolCatalogItem, McpTransportSummary,
        SkillHealthSummary, SkillInstallState, SkillPolicyState,
    };

    fn skill_capability(slug: &str) -> ComposerCapability {
        ComposerCapability {
            id: format!("skill:user:{slug}"),
            label: slug.to_owned(),
            kind: ComposerCapabilityKind::Skill {
                slug: slug.to_owned(),
                source_kind: "user".to_owned(),
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
            source_kind: McpSourceKind::Config,
            transport: McpTransportSummary::Stdio {
                command: "server".to_owned(),
            },
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            fingerprint: "fingerprint".to_owned(),
            runtime: McpRuntimeStatus {
                state: McpRuntimeState::Ready,
                live: true,
                last_seen_at: None,
                last_error: None,
            },
            tools_count: 1,
            resources_count: 0,
            resource_templates_count: 0,
            prompts_count: 0,
            status: McpServerStatus::Ready,
            status_reason: None,
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
            health: McpServerHealthDetails {
                runtime: server.runtime.clone(),
                status: server.status,
                status_reason: server.status_reason.clone(),
                last_error: None,
                retry_attempt: None,
                next_retry_at: None,
                catalog_version: Some("catalog-v1".to_owned()),
                stderr_tail: None,
            },
            audit: Vec::new(),
            recent_bindings: Vec::new(),
        }
    }

    #[test]
    fn composer_capability_key_is_canonical_by_kind() {
        assert_eq!(
            ComposerCapabilityKind::Skill {
                slug: "imagegen".to_owned(),
                source_kind: "user".to_owned(),
            }
            .key(),
            "skill:user:imagegen"
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
        let turn_capability = skill_capability("imagegen").to_turn_capability();

        assert_eq!(turn_capability.id, "skill:user:imagegen");
        assert_eq!(turn_capability.label.as_deref(), Some("imagegen"));
        assert_eq!(
            turn_capability.kind,
            TurnCapabilityKind::Skill {
                slug: "imagegen".to_owned(),
                source_kind: "user".to_owned(),
            }
        );
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
        assert_eq!(capabilities[0].key(), "skill:user:imagegen");
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
        assert_eq!(rows[0].key, "skill:user:tests/imagegen");
        assert!(rows[0].selectable);
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
        assert_eq!(capabilities[0].id, "skill:user:tests/imagegen");
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
