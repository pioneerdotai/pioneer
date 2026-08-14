//! UI-neutral MCP presentation rows.

use pioneer_protocol::{
    McpAuditEventSummary, McpListItem, McpPromptCatalogItem, McpResourceCatalogItem,
    McpResourceTemplateCatalogItem, McpRuntimeState, McpScopeKind, McpServerDetailsResponse,
    McpServerStatus, McpSourceKind, McpToolCatalogItem, McpTransportSummary,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpPresentationTone {
    Default,
    Muted,
    Success,
    Warning,
    Danger,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpStatusLabel {
    NotStarted,
    Disabled,
    Starting,
    Ready,
    Degraded,
    AuthRequired,
    Failed,
    Stopping,
    Stopped,
    Restarting,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpSourceLabel {
    Config,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpScopeLabel {
    Workspace,
    User,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpTransportPresentation {
    Stdio { command: String },
    StreamableHttp { url: String },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpCapabilityKind {
    Tools,
    Resources,
    ResourceTemplates,
    Prompts,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpCapabilityCount {
    pub kind: McpCapabilityKind,
    pub count: usize,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpDetailMetaKind {
    Source,
    Scope,
    Transport,
    Loaded,
    Runtime,
    Live,
    LastSeen,
    RetryAttempt,
    NextRetry,
    LastError,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpDetailValue {
    Empty,
    Text(String),
    Timestamp(i64),
    Count(u32),
    Bool(bool),
    Status(McpStatusLabel),
    Source(McpSourceLabel),
    Scope(McpScopeLabel),
    Transport(McpTransportPresentation),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpDetailMetaRow {
    pub kind: McpDetailMetaKind,
    pub value: McpDetailValue,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpAuditAction {
    Install,
    Update,
    Uninstall,
    Policy,
    Start,
    Started,
    StartFailed,
    Stop,
    Stopped,
    Restart,
    CatalogRefreshed,
    Call,
    CallCompleted,
    CallFailed,
    Other(String),
    None,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpAuditDecision {
    Allowed,
    Blocked,
    Warning,
    Other(String),
    None,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpAuditDetailsSummary {
    Empty,
    ObjectPairs(Vec<(String, McpJsonValuePreview)>),
    ArrayLen(usize),
    Value(McpJsonValuePreview),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum McpJsonValuePreview {
    Text(String),
    Bool(bool),
    Number(String),
    None,
    EmptyArray,
    ArrayLen(usize),
    EmptyObject,
    ObjectKeys(usize),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpAuditRow {
    pub created_at: i64,
    pub action: McpAuditAction,
    pub raw_tool_name: Option<String>,
    pub decision: McpAuditDecision,
    pub decision_tone: McpPresentationTone,
    pub reason_code: Option<String>,
    pub details_summary: McpAuditDetailsSummary,
}

pub fn mcp_display_name(server: &McpListItem) -> String {
    optional_non_empty_text(server.display_name.as_deref()).unwrap_or_else(|| server.name.clone())
}

pub fn mcp_status_label(status: McpServerStatus) -> McpStatusLabel {
    match status {
        McpServerStatus::NotStarted => McpStatusLabel::NotStarted,
        McpServerStatus::Disabled => McpStatusLabel::Disabled,
        McpServerStatus::Starting => McpStatusLabel::Starting,
        McpServerStatus::Ready => McpStatusLabel::Ready,
        McpServerStatus::Degraded => McpStatusLabel::Degraded,
        McpServerStatus::AuthRequired => McpStatusLabel::AuthRequired,
        McpServerStatus::Failed => McpStatusLabel::Failed,
        McpServerStatus::Stopping => McpStatusLabel::Stopping,
        McpServerStatus::Stopped => McpStatusLabel::Stopped,
        McpServerStatus::Restarting => McpStatusLabel::Restarting,
    }
}

pub fn mcp_status_tone(status: McpServerStatus) -> McpPresentationTone {
    match status {
        McpServerStatus::Ready => McpPresentationTone::Success,
        McpServerStatus::Degraded | McpServerStatus::Starting | McpServerStatus::Restarting => {
            McpPresentationTone::Warning
        }
        McpServerStatus::Failed | McpServerStatus::AuthRequired => McpPresentationTone::Danger,
        McpServerStatus::Disabled | McpServerStatus::Stopped | McpServerStatus::Stopping => {
            McpPresentationTone::Muted
        }
        McpServerStatus::NotStarted => McpPresentationTone::Muted,
    }
}

pub fn mcp_runtime_label(status: McpRuntimeState) -> McpStatusLabel {
    match status {
        McpRuntimeState::NotStarted => McpStatusLabel::NotStarted,
        McpRuntimeState::Disabled => McpStatusLabel::Disabled,
        McpRuntimeState::Starting => McpStatusLabel::Starting,
        McpRuntimeState::Ready => McpStatusLabel::Ready,
        McpRuntimeState::Degraded => McpStatusLabel::Degraded,
        McpRuntimeState::AuthRequired => McpStatusLabel::AuthRequired,
        McpRuntimeState::Failed => McpStatusLabel::Failed,
        McpRuntimeState::Stopping => McpStatusLabel::Stopping,
        McpRuntimeState::Stopped => McpStatusLabel::Stopped,
        McpRuntimeState::Restarting => McpStatusLabel::Restarting,
    }
}

pub fn mcp_runtime_tone(status: McpRuntimeState) -> McpPresentationTone {
    match status {
        McpRuntimeState::Ready => McpPresentationTone::Success,
        McpRuntimeState::Degraded | McpRuntimeState::Starting | McpRuntimeState::Restarting => {
            McpPresentationTone::Warning
        }
        McpRuntimeState::Failed | McpRuntimeState::AuthRequired => McpPresentationTone::Danger,
        McpRuntimeState::Disabled | McpRuntimeState::Stopped | McpRuntimeState::Stopping => {
            McpPresentationTone::Muted
        }
        McpRuntimeState::NotStarted => McpPresentationTone::Muted,
    }
}

pub fn mcp_source_label(source: McpSourceKind) -> McpSourceLabel {
    match source {
        McpSourceKind::Config => McpSourceLabel::Config,
    }
}

pub fn mcp_scope_label(scope: McpScopeKind) -> McpScopeLabel {
    match scope {
        McpScopeKind::Workspace => McpScopeLabel::Workspace,
        McpScopeKind::User => McpScopeLabel::User,
    }
}

pub fn mcp_transport_presentation(transport: &McpTransportSummary) -> McpTransportPresentation {
    match transport {
        McpTransportSummary::Stdio { command } => McpTransportPresentation::Stdio {
            command: command.clone(),
        },
        McpTransportSummary::StreamableHttp { url } => {
            McpTransportPresentation::StreamableHttp { url: url.clone() }
        }
    }
}

pub fn mcp_capability_counts(server: &McpListItem) -> Vec<McpCapabilityCount> {
    mcp_capability_counts_from_counts(
        server.tools_count,
        server.resources_count,
        server.resource_templates_count,
        server.prompts_count,
    )
}

pub fn mcp_capability_counts_from_counts(
    tools_count: usize,
    resources_count: usize,
    resource_templates_count: usize,
    prompts_count: usize,
) -> Vec<McpCapabilityCount> {
    [
        (McpCapabilityKind::Tools, tools_count),
        (McpCapabilityKind::Resources, resources_count),
        (
            McpCapabilityKind::ResourceTemplates,
            resource_templates_count,
        ),
        (McpCapabilityKind::Prompts, prompts_count),
    ]
    .into_iter()
    .filter(|(_, count)| *count > 0)
    .map(|(kind, count)| McpCapabilityCount { kind, count })
    .collect()
}

pub fn mcp_overview_rows(
    _server: &McpListItem,
    details: Option<&McpServerDetailsResponse>,
) -> Vec<McpDetailMetaRow> {
    let management = details.and_then(|details| details.management.as_ref());
    vec![
        McpDetailMetaRow {
            kind: McpDetailMetaKind::Source,
            value: management
                .map(|management| McpDetailValue::Source(mcp_source_label(management.source_kind)))
                .unwrap_or(McpDetailValue::Empty),
        },
        McpDetailMetaRow {
            kind: McpDetailMetaKind::Scope,
            value: management
                .map(|management| McpDetailValue::Scope(mcp_scope_label(management.scope)))
                .unwrap_or(McpDetailValue::Empty),
        },
        McpDetailMetaRow {
            kind: McpDetailMetaKind::Transport,
            value: management
                .map(|management| {
                    McpDetailValue::Transport(mcp_transport_presentation(&management.transport))
                })
                .unwrap_or(McpDetailValue::Empty),
        },
        McpDetailMetaRow {
            kind: McpDetailMetaKind::Loaded,
            value: details
                .map(|details| McpDetailValue::Timestamp(details.generated_at))
                .unwrap_or(McpDetailValue::Empty),
        },
    ]
}

pub fn mcp_health_rows(
    server: &McpListItem,
    details: Option<&McpServerDetailsResponse>,
) -> Vec<McpDetailMetaRow> {
    let health = details
        .and_then(|details| details.management.as_ref())
        .map(|management| &management.health);
    vec![
        McpDetailMetaRow {
            kind: McpDetailMetaKind::Runtime,
            value: McpDetailValue::Status(mcp_runtime_label(server.runtime.state)),
        },
        McpDetailMetaRow {
            kind: McpDetailMetaKind::Live,
            value: McpDetailValue::Bool(server.runtime.live),
        },
        McpDetailMetaRow {
            kind: McpDetailMetaKind::LastSeen,
            value: server
                .runtime
                .last_seen_at
                .map(McpDetailValue::Timestamp)
                .unwrap_or(McpDetailValue::Empty),
        },
        McpDetailMetaRow {
            kind: McpDetailMetaKind::RetryAttempt,
            value: health
                .and_then(|health| health.retry_attempt)
                .map(McpDetailValue::Count)
                .unwrap_or(McpDetailValue::Empty),
        },
        McpDetailMetaRow {
            kind: McpDetailMetaKind::NextRetry,
            value: health
                .and_then(|health| health.next_retry_at)
                .map(McpDetailValue::Timestamp)
                .unwrap_or(McpDetailValue::Empty),
        },
        McpDetailMetaRow {
            kind: McpDetailMetaKind::LastError,
            value: health
                .and_then(|health| optional_non_empty_text(health.last_error.as_deref()))
                .map(McpDetailValue::Text)
                .unwrap_or(McpDetailValue::Empty),
        },
    ]
}

pub fn mcp_tool_title(tool: &McpToolCatalogItem) -> String {
    optional_non_empty_text(tool.title.as_deref()).unwrap_or_else(|| tool.name.clone())
}

pub fn mcp_resource_title(resource: &McpResourceCatalogItem) -> Option<String> {
    optional_non_empty_text(resource.title.as_deref())
        .or_else(|| optional_non_empty_text(resource.name.as_deref()))
        .or_else(|| optional_non_empty_text(resource.uri.as_deref()))
}

pub fn mcp_resource_template_title(template: &McpResourceTemplateCatalogItem) -> Option<String> {
    optional_non_empty_text(template.title.as_deref())
        .or_else(|| optional_non_empty_text(template.name.as_deref()))
        .or_else(|| optional_non_empty_text(template.uri_template.as_deref()))
}

pub fn mcp_prompt_title(prompt: &McpPromptCatalogItem) -> String {
    optional_non_empty_text(prompt.title.as_deref()).unwrap_or_else(|| prompt.name.clone())
}

pub fn mcp_audit_rows(audit: &[McpAuditEventSummary], limit: usize) -> Vec<McpAuditRow> {
    audit
        .iter()
        .take(limit)
        .map(|item| {
            let decision = mcp_audit_decision(item.decision.as_str());
            McpAuditRow {
                created_at: item.created_at,
                action: mcp_audit_action(item.action.as_str()),
                raw_tool_name: optional_non_empty_text(item.raw_tool_name.as_deref()),
                decision: decision.clone(),
                decision_tone: mcp_audit_decision_tone(&decision),
                reason_code: optional_non_empty_text(item.reason_code.as_deref()),
                details_summary: summarize_mcp_audit_details(&item.details),
            }
        })
        .collect()
}

pub fn mcp_audit_action(action: &str) -> McpAuditAction {
    match action.trim() {
        "install" => McpAuditAction::Install,
        "update" => McpAuditAction::Update,
        "uninstall" => McpAuditAction::Uninstall,
        "policy" => McpAuditAction::Policy,
        "start" => McpAuditAction::Start,
        "started" => McpAuditAction::Started,
        "start_failed" => McpAuditAction::StartFailed,
        "stop" => McpAuditAction::Stop,
        "stopped" => McpAuditAction::Stopped,
        "restart" => McpAuditAction::Restart,
        "catalog_refreshed" => McpAuditAction::CatalogRefreshed,
        "call" => McpAuditAction::Call,
        "call_completed" => McpAuditAction::CallCompleted,
        "call_failed" => McpAuditAction::CallFailed,
        other if !other.is_empty() => McpAuditAction::Other(other.to_owned()),
        _ => McpAuditAction::None,
    }
}

pub fn mcp_audit_decision(decision: &str) -> McpAuditDecision {
    match decision.trim() {
        "allowed" => McpAuditDecision::Allowed,
        "blocked" => McpAuditDecision::Blocked,
        "warning" => McpAuditDecision::Warning,
        other if !other.is_empty() => McpAuditDecision::Other(other.to_owned()),
        _ => McpAuditDecision::None,
    }
}

pub fn mcp_audit_decision_tone(decision: &McpAuditDecision) -> McpPresentationTone {
    match decision {
        McpAuditDecision::Allowed => McpPresentationTone::Success,
        McpAuditDecision::Blocked => McpPresentationTone::Danger,
        McpAuditDecision::Warning => McpPresentationTone::Warning,
        McpAuditDecision::Other(_) | McpAuditDecision::None => McpPresentationTone::Muted,
    }
}

pub fn summarize_mcp_audit_details(details: &serde_json::Value) -> McpAuditDetailsSummary {
    match details {
        serde_json::Value::Null => McpAuditDetailsSummary::Empty,
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                return McpAuditDetailsSummary::Empty;
            }

            McpAuditDetailsSummary::ObjectPairs(
                map.iter()
                    .take(2)
                    .map(|(key, value)| (key.clone(), mcp_json_value_preview(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => McpAuditDetailsSummary::ArrayLen(values.len()),
        other => McpAuditDetailsSummary::Value(mcp_json_value_preview(other)),
    }
}

pub fn mcp_json_value_preview(value: &serde_json::Value) -> McpJsonValuePreview {
    match value {
        serde_json::Value::String(value) => {
            McpJsonValuePreview::Text(truncate_for_mcp_table(value, 48))
        }
        serde_json::Value::Bool(value) => McpJsonValuePreview::Bool(*value),
        serde_json::Value::Number(value) => McpJsonValuePreview::Number(value.to_string()),
        serde_json::Value::Null => McpJsonValuePreview::None,
        serde_json::Value::Array(values) => {
            if values.is_empty() {
                McpJsonValuePreview::EmptyArray
            } else {
                McpJsonValuePreview::ArrayLen(values.len())
            }
        }
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                McpJsonValuePreview::EmptyObject
            } else {
                McpJsonValuePreview::ObjectKeys(map.len())
            }
        }
    }
}

pub fn truncate_for_mcp_table(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }

    let shortened = value.chars().take(max_chars).collect::<String>();
    format!("{shortened}...")
}

pub fn non_empty_text(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

pub fn optional_non_empty_text(value: Option<&str>) -> Option<String> {
    value.and_then(non_empty_text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        McpManagementDetails, McpPolicyState, McpRuntimeStatus, McpServerCatalogDetails,
        McpServerHealthDetails,
    };

    fn runtime(state: McpRuntimeState, live: bool) -> McpRuntimeStatus {
        McpRuntimeStatus {
            state,
            live,
            last_seen_at: None,
        }
    }

    fn server(id: &str) -> McpListItem {
        McpListItem {
            id: id.to_owned(),
            name: id.to_owned(),
            display_name: None,
            scope: McpScopeKind::Workspace,
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            runtime: runtime(McpRuntimeState::Ready, true),
            tools_count: 1,
            resources_count: 2,
            resource_templates_count: 0,
            prompts_count: 4,
            status: McpServerStatus::Ready,
        }
    }

    fn details(server: McpListItem) -> McpServerDetailsResponse {
        McpServerDetailsResponse {
            snapshot_version: 1,
            generated_at: 1_717_000_000,
            management: Some(McpManagementDetails {
                scope: McpScopeKind::Workspace,
                source_kind: McpSourceKind::Config,
                transport: McpTransportSummary::Stdio {
                    command: "node".to_owned(),
                },
                fingerprint: "fingerprint".to_owned(),
                health: McpServerHealthDetails {
                    runtime: server.runtime.clone(),
                    status: server.status,
                    status_reason: None,
                    last_error: None,
                    retry_attempt: Some(3),
                    next_retry_at: Some(1_717_000_500),
                    catalog_version: None,
                    stderr_tail: None,
                },
                audit: Vec::new(),
                recent_bindings: Vec::new(),
            }),
            server,
            catalog: McpServerCatalogDetails {
                catalog_version: None,
                generated_at: None,
                server_info: serde_json::Value::Null,
                server_instructions_hash: None,
                tools: Vec::new(),
                resources: Vec::new(),
                resource_templates: Vec::new(),
                prompts: Vec::new(),
            },
        }
    }

    #[test]
    fn status_runtime_transport_and_capability_selectors_are_semantic() {
        let mut server = server("server_a");
        server.display_name = Some("Server A".to_owned());
        assert_eq!(mcp_display_name(&server), "Server A");
        assert_eq!(mcp_status_label(server.status), McpStatusLabel::Ready);
        assert_eq!(mcp_status_tone(server.status), McpPresentationTone::Success);
        assert_eq!(
            mcp_runtime_label(server.runtime.state),
            McpStatusLabel::Ready
        );
        assert_eq!(
            mcp_runtime_tone(server.runtime.state),
            McpPresentationTone::Success
        );
        assert_eq!(
            mcp_transport_presentation(
                &details(server.clone())
                    .management
                    .expect("management")
                    .transport,
            ),
            McpTransportPresentation::Stdio {
                command: "node".to_owned()
            }
        );
        assert_eq!(
            mcp_capability_counts(&server),
            vec![
                McpCapabilityCount {
                    kind: McpCapabilityKind::Tools,
                    count: 1
                },
                McpCapabilityCount {
                    kind: McpCapabilityKind::Resources,
                    count: 2
                },
                McpCapabilityCount {
                    kind: McpCapabilityKind::Prompts,
                    count: 4
                }
            ]
        );
    }

    #[test]
    fn overview_and_health_rows_preserve_raw_values_for_shell_formatting() {
        let mut server = server("server_a");
        server.runtime.last_seen_at = Some(1_717_000_100);
        let mut details = details(server.clone());
        details
            .management
            .as_mut()
            .expect("management")
            .health
            .last_error = Some("runtime failed".to_owned());

        let overview = mcp_overview_rows(&server, Some(&details));
        assert_eq!(
            overview[0].value,
            McpDetailValue::Source(McpSourceLabel::Config)
        );
        assert_eq!(
            overview[1].value,
            McpDetailValue::Scope(McpScopeLabel::Workspace)
        );
        assert_eq!(overview[3].value, McpDetailValue::Timestamp(1_717_000_000));

        let health = mcp_health_rows(&server, Some(&details));
        assert_eq!(
            health[0].value,
            McpDetailValue::Status(McpStatusLabel::Ready)
        );
        assert_eq!(health[1].value, McpDetailValue::Bool(true));
        assert_eq!(health[2].value, McpDetailValue::Timestamp(1_717_000_100));
        assert_eq!(health[3].value, McpDetailValue::Count(3));
        assert_eq!(
            health[5].value,
            McpDetailValue::Text("runtime failed".to_owned())
        );
    }

    #[test]
    fn catalog_title_selectors_use_display_fallback_order() {
        let tool = McpToolCatalogItem {
            name: "raw_tool".to_owned(),
            title: Some(" Tool Title ".to_owned()),
            description: None,
            input_schema_summary: None,
            annotations: None,
        };
        assert_eq!(mcp_tool_title(&tool), "Tool Title");

        let resource = McpResourceCatalogItem {
            uri: Some("file:///readme".to_owned()),
            name: Some("readme".to_owned()),
            title: None,
            mime_type: None,
            description: None,
        };
        assert_eq!(mcp_resource_title(&resource).as_deref(), Some("readme"));

        let template = McpResourceTemplateCatalogItem {
            uri_template: Some("file:///{name}".to_owned()),
            name: None,
            title: None,
            mime_type: None,
            description: None,
        };
        assert_eq!(
            mcp_resource_template_title(&template).as_deref(),
            Some("file:///{name}")
        );

        let prompt = McpPromptCatalogItem {
            name: "raw_prompt".to_owned(),
            title: Some("Prompt Title".to_owned()),
            description: None,
            arguments_count: 0,
        };
        assert_eq!(mcp_prompt_title(&prompt), "Prompt Title");
    }

    #[test]
    fn audit_rows_classify_actions_decisions_and_summarize_details() {
        let rows = mcp_audit_rows(
            &[McpAuditEventSummary {
                turn_id: None,
                server_installation_id: None,
                server_name: "server".to_owned(),
                raw_tool_name: Some("tool".to_owned()),
                callable_name: None,
                catalog_version: None,
                action: "call_failed".to_owned(),
                decision: "blocked".to_owned(),
                reason_code: Some("policy".to_owned()),
                details: serde_json::json!({
                    "message": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
                    "retryable": false,
                    "ignored": 1
                }),
                created_at: 1_717_000_000,
            }],
            8,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].action, McpAuditAction::CallFailed);
        assert_eq!(rows[0].raw_tool_name.as_deref(), Some("tool"));
        assert_eq!(rows[0].decision, McpAuditDecision::Blocked);
        assert_eq!(rows[0].decision_tone, McpPresentationTone::Danger);
        assert_eq!(rows[0].reason_code.as_deref(), Some("policy"));
        assert!(matches!(
            rows[0].details_summary,
            McpAuditDetailsSummary::ObjectPairs(_)
        ));
    }
}
