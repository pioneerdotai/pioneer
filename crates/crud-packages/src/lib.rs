//! Package repositories share the caller’s connection or transaction.

pub mod agent_skill;
pub mod mcp_audit_event;
pub mod mcp_server_catalog_snapshot;
pub mod mcp_server_installation;
pub mod package_record;
pub mod plugin_metadata;
pub mod plugins;
pub mod skill_audit_event;
pub mod skill_dependency_snapshot;
pub mod skill_pack_installation;

pub use package_record::{PackageKind, PackageRecord, PreparedAppMetadata};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerInstallationRecord {
    pub id: Option<String>,
    pub scope_kind: String,
    pub scope_key: String,
    pub name: String,
    pub display_name: Option<String>,
    pub source_kind: String,
    pub source_ref: String,
    pub transport_kind: String,
    pub transport_json: String,
    pub auth_json: String,
    pub secret_refs_json: String,
    pub enabled: bool,
    pub allow_implicit_invocation: bool,
    pub required: bool,
    pub fingerprint: String,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerCatalogSnapshotRecord {
    pub server_installation_id: String,
    pub catalog_version: String,
    pub server_info_json: String,
    pub server_instructions_hash: Option<String>,
    pub tools_json: String,
    pub resources_json: String,
    pub resource_templates_json: String,
    pub prompts_json: String,
    pub generated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuditEventRecord {
    pub turn_id: Option<String>,
    pub server_installation_id: Option<String>,
    pub server_name: String,
    pub raw_tool_name: Option<String>,
    pub callable_name: Option<String>,
    pub catalog_version: Option<String>,
    pub action: String,
    pub decision: String,
    pub reason_code: Option<String>,
    pub details_json: String,
    pub created_at_unix: i64,
}

impl From<pioneer_entity::mcp_server_catalog_snapshot::Model> for McpServerCatalogSnapshotRecord {
    fn from(model: pioneer_entity::mcp_server_catalog_snapshot::Model) -> Self {
        Self {
            server_installation_id: model.server_installation_id,
            catalog_version: model.catalog_version,
            server_info_json: model.server_info_json.to_string(),
            server_instructions_hash: model.server_instructions_hash,
            tools_json: model.tools_json.to_string(),
            resources_json: model.resources_json.to_string(),
            resource_templates_json: model.resource_templates_json.to_string(),
            prompts_json: model.prompts_json.to_string(),
            generated_at_unix: model.generated_at.timestamp(),
        }
    }
}
impl From<pioneer_entity::mcp_audit_event::Model> for McpAuditEventRecord {
    fn from(model: pioneer_entity::mcp_audit_event::Model) -> Self {
        Self {
            turn_id: model.turn_id,
            server_installation_id: model.server_installation_id,
            server_name: model.server_name,
            raw_tool_name: model.raw_tool_name,
            callable_name: model.callable_name,
            catalog_version: model.catalog_version,
            action: model.action,
            decision: model.decision,
            reason_code: model.reason_code,
            details_json: model.details_json.to_string(),
            created_at_unix: model.created_at.timestamp(),
        }
    }
}
impl From<pioneer_entity::mcp_server_installation::Model> for McpServerInstallationRecord {
    fn from(model: pioneer_entity::mcp_server_installation::Model) -> Self {
        Self {
            id: Some(model.id),
            scope_kind: model.scope_kind,
            scope_key: model.scope_key,
            name: model.name,
            display_name: model.display_name,
            source_kind: model.source_kind,
            source_ref: model.source_ref.to_string(),
            transport_kind: model.transport_kind,
            transport_json: model.transport_json.to_string(),
            auth_json: model.auth_json.to_string(),
            secret_refs_json: model.secret_refs_json.to_string(),
            enabled: model.enabled,
            allow_implicit_invocation: model.allow_implicit_invocation,
            required: model.required,
            fingerprint: model.fingerprint,
            updated_at_unix: model.updated_at.timestamp(),
        }
    }
}

pub mod skill_installation;
pub mod skill_records;
pub mod skill_upload_session;
pub mod skill_workspace_policy;
