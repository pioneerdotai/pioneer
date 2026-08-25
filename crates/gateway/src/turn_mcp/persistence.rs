use super::projection::{
    ResolvedMcpTurnProjection, canonical_annotations_identity, canonical_schema_identity,
};
use pioneer_agent::{
    AgentMcpPersistedProjection, AgentMcpProjectionBinding, AgentMcpProjectionPersistenceError,
    AgentMcpProjectionPersistenceRequest,
};
use pioneer_crud::{
    CrudStore, TurnMcpBindingRecord, TurnMcpProjectionRecord, TurnMcpProjectionReplacement,
};
use std::sync::Arc;

const FIRST_PARTY_FILE_SERVER_ID: &str = "pioneer-file-tools-v1";
const FIRST_PARTY_FILE_SERVER_NAME: &str = "pioneer_files";
const FIRST_PARTY_FILE_CATALOG: &str = "pioneer-file-tools-v1";
const FIRST_PARTY_FILE_SELECTION: &str = "first_party_filesystem";
const FIRST_PARTY_READ_FILE: &str = "read_file";
const FIRST_PARTY_APPLY_PATCH: &str = "apply_patch";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnMcpProviderBindingIdentity {
    pub(crate) canonical_callable_name: String,
    pub(crate) provider_callable_name: String,
    pub(crate) provider_schema_fingerprint: String,
}

#[derive(Clone)]
pub(crate) struct TurnMcpPersistenceCoordinator {
    crud_store: Arc<CrudStore>,
}

impl TurnMcpPersistenceCoordinator {
    pub(crate) fn new(crud_store: Arc<CrudStore>) -> Self {
        Self { crud_store }
    }

    pub(crate) async fn persist(
        &self,
        request: &AgentMcpProjectionPersistenceRequest,
    ) -> Result<AgentMcpPersistedProjection, AgentMcpProjectionPersistenceError> {
        let projection_version = i32::try_from(request.projection_version)
            .map_err(|_| persistence_error("projection version exceeds durable integer range"))?;
        let tool_count = i32::try_from(request.bindings.len()).map_err(|_| {
            persistence_error("projection tool count exceeds durable integer range")
        })?;
        let bindings = request
            .bindings
            .iter()
            .map(durable_binding)
            .collect::<Result<Vec<_>, _>>()?;
        let replacement = TurnMcpProjectionReplacement {
            projection: TurnMcpProjectionRecord {
                turn_id: request.turn_id.clone(),
                workspace_id: request.workspace_id.clone(),
                projection_version,
                manifest_hash: request.manifest_hash.clone(),
                resolution_status: request.resolution_status.clone(),
                tool_count,
                created_at_unix: chrono::Utc::now().timestamp(),
            },
            bindings,
        };
        let authorization_context = self.bound_execution_authorization_context(request).await?;
        let outcome = self
            .crud_store
            .replace_turn_mcp_projection_with_authorization_context(
                &replacement,
                authorization_context.as_str(),
            )
            .await
            .map_err(|error| persistence_error(format!("{error}")))?;
        Ok(AgentMcpPersistedProjection {
            turn_id: outcome.turn_id,
            manifest_hash: outcome.manifest_hash,
            tool_count: usize::try_from(outcome.tool_count).map_err(|_| {
                persistence_error("persisted projection returned a negative tool count")
            })?,
        })
    }

    async fn bound_execution_authorization_context(
        &self,
        request: &AgentMcpProjectionPersistenceRequest,
    ) -> Result<String, AgentMcpProjectionPersistenceError> {
        let mut context = crate::authorization::ExecutionAuthorizationContext::load_for_turn(
            self.crud_store.as_ref(),
            request.turn_id.as_str(),
        )
        .await
        .map_err(|error| {
            persistence_error(format!(
                "failed to restore execution authorization context for MCP projection: {error:#}"
            ))
        })?;
        let server_names = request
            .bindings
            .iter()
            // The managed Claude filesystem facade is a Pioneer-owned
            // capability. It is persisted as a frozen binding so invocation
            // validation and drift checks remain exact, but it is not an
            // external MCP server that must appear in the user's immutable
            // MCP server admission. External MCP names remain the only names
            // bound to role/capability grants here.
            .filter(|binding| binding.server_installation_id != FIRST_PARTY_FILE_SERVER_ID)
            .map(|binding| binding.server_name.clone())
            .collect::<Vec<_>>();
        context
            .bind_mcp_projection(
                request.workspace_id.as_str(),
                request.projection_version,
                request.manifest_hash.as_str(),
                server_names.as_slice(),
            )
            .map_err(|error| {
                persistence_error(format!(
                    "failed to bind MCP projection to execution authorization: {error:#}"
                ))
            })?;
        let bound_json = context.to_persisted_json().map_err(|error| {
            persistence_error(format!(
                "failed to serialize MCP-bound execution authorization context: {error:#}"
            ))
        })?;
        Ok(bound_json)
    }
}

pub(crate) fn persistence_request_from_projection(
    projection: &ResolvedMcpTurnProjection,
) -> Result<AgentMcpProjectionPersistenceRequest, String> {
    persistence_request_from_projection_with_provider(projection, &[])
}

pub(crate) fn persistence_request_from_projection_with_provider(
    projection: &ResolvedMcpTurnProjection,
    provider_bindings: &[TurnMcpProviderBindingIdentity],
) -> Result<AgentMcpProjectionPersistenceRequest, String> {
    let mut provider_bindings_by_name = std::collections::HashMap::new();
    for binding in provider_bindings {
        if provider_bindings_by_name
            .insert(binding.canonical_callable_name.as_str(), binding)
            .is_some()
        {
            return Err(format!(
                "provider MCP binding projection repeats canonical callable `{}`",
                binding.canonical_callable_name
            ));
        }
    }
    let provider_bindings = provider_bindings_by_name;
    let projection_names = projection
        .tools
        .iter()
        .map(|tool| tool.canonical_callable_name.as_str())
        .collect::<std::collections::HashSet<_>>();
    if !provider_bindings.is_empty()
        && projection
            .tools
            .iter()
            .any(|tool| !provider_bindings.contains_key(tool.canonical_callable_name.as_str()))
    {
        return Err("provider MCP binding projection is missing a canonical callable".to_owned());
    }
    if provider_bindings.values().any(|binding| {
        !projection_names.contains(binding.canonical_callable_name.as_str())
            && !matches!(
                binding.canonical_callable_name.as_str(),
                FIRST_PARTY_READ_FILE | FIRST_PARTY_APPLY_PATCH
            )
    }) {
        return Err(
            "provider MCP binding projection contains an unknown extra callable".to_owned(),
        );
    }
    let bindings = projection
        .tools
        .iter()
        .map(|tool| {
            let provider = provider_bindings.get(tool.canonical_callable_name.as_str());
            if !provider_bindings.is_empty() && provider.is_none() {
                return Err(format!(
                    "provider MCP binding is missing canonical callable `{}`",
                    tool.canonical_callable_name
                ));
            }
            let (annotations_json, annotations_digest) =
                canonical_annotations_identity(&tool.annotations.clone().unwrap_or_default())?;
            Ok(AgentMcpProjectionBinding {
                server_installation_id: tool.server_installation_id.clone(),
                server_name: tool.server_name.clone(),
                raw_tool_name: tool.raw_tool_name.clone(),
                callable_name: tool.canonical_callable_name.clone(),
                canonical_callable_name: tool.canonical_callable_name.clone(),
                provider_callable_name: provider
                    .map(|binding| binding.provider_callable_name.clone())
                    .unwrap_or_else(|| tool.canonical_callable_name.clone()),
                catalog_version: tool.catalog_version.clone(),
                installation_fingerprint: tool.installation_fingerprint.clone(),
                canonical_schema_fingerprint: tool.schema_fingerprint.clone(),
                provider_schema_fingerprint: provider
                    .map(|binding| binding.provider_schema_fingerprint.clone())
                    .unwrap_or_else(|| tool.schema_fingerprint.clone()),
                annotations_json,
                annotations_digest,
                effective_timeout_ms: tool.timeout_ms,
                runtime_generation: tool.runtime_generation,
                projection_activation_generation: 0,
                selection_reason: tool.selection_reason.legacy_binding_value().to_owned(),
                capability_id: tool.capability_id.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut bindings = bindings;
    for name in [FIRST_PARTY_READ_FILE, FIRST_PARTY_APPLY_PATCH] {
        let Some(provider) = provider_bindings.get(name) else {
            continue;
        };
        if projection_names.contains(name) {
            return Err(format!(
                "first-party callable `{name}` collides with a canonical MCP projection tool"
            ));
        }
        let schema = first_party_file_schema(name)?;
        let (_, canonical_schema_fingerprint, _) = canonical_schema_identity(&schema)?;
        bindings.push(AgentMcpProjectionBinding {
            server_installation_id: FIRST_PARTY_FILE_SERVER_ID.to_owned(),
            server_name: FIRST_PARTY_FILE_SERVER_NAME.to_owned(),
            raw_tool_name: name.to_owned(),
            callable_name: name.to_owned(),
            canonical_callable_name: name.to_owned(),
            provider_callable_name: provider.provider_callable_name.clone(),
            catalog_version: FIRST_PARTY_FILE_CATALOG.to_owned(),
            installation_fingerprint: FIRST_PARTY_FILE_SERVER_ID.to_owned(),
            canonical_schema_fingerprint,
            provider_schema_fingerprint: provider.provider_schema_fingerprint.clone(),
            annotations_json: "{}".to_owned(),
            annotations_digest: crate::turn_mcp::projection::sha256_hex(b"{}"),
            effective_timeout_ms: crate::turn_mcp::DEFAULT_MCP_TURN_TOOL_TIMEOUT_MS,
            runtime_generation: 1,
            projection_activation_generation: 0,
            selection_reason: FIRST_PARTY_FILE_SELECTION.to_owned(),
            capability_id: Some(FIRST_PARTY_FILE_CATALOG.to_owned()),
        });
    }
    Ok(AgentMcpProjectionPersistenceRequest {
        workspace_id: projection.workspace_id.clone(),
        turn_id: projection.turn_id.clone(),
        projection_version: projection.projection_version,
        manifest_hash: projection.manifest_hash.clone(),
        resolution_status: if projection.diagnostics.is_empty() {
            "resolved".to_owned()
        } else {
            "resolved_degraded".to_owned()
        },
        bindings,
    })
}

fn first_party_file_schema(name: &str) -> Result<serde_json::Value, String> {
    match name {
        FIRST_PARTY_READ_FILE => Ok(pioneer_provider::read_file_tool_schema()
            .get("input")
            .cloned()
            .ok_or_else(|| "read_file schema has no input object".to_owned())?),
        FIRST_PARTY_APPLY_PATCH => Ok(serde_json::json!({
            "type": "object",
            "properties": {"patch": {"type": "string"}},
            "required": ["patch"],
            "additionalProperties": false
        })),
        _ => Err(format!("unknown first-party file callable `{name}`")),
    }
}

fn durable_binding(
    binding: &AgentMcpProjectionBinding,
) -> Result<TurnMcpBindingRecord, AgentMcpProjectionPersistenceError> {
    Ok(TurnMcpBindingRecord {
        server_installation_id: binding.server_installation_id.clone(),
        server_name: binding.server_name.clone(),
        raw_tool_name: binding.raw_tool_name.clone(),
        callable_name: binding.callable_name.clone(),
        canonical_callable_name: binding.canonical_callable_name.clone(),
        provider_callable_name: binding.provider_callable_name.clone(),
        catalog_version: binding.catalog_version.clone(),
        fingerprint: binding.installation_fingerprint.clone(),
        canonical_schema_fingerprint: binding.canonical_schema_fingerprint.clone(),
        provider_schema_fingerprint: binding.provider_schema_fingerprint.clone(),
        annotations_json: binding.annotations_json.clone(),
        annotations_digest: binding.annotations_digest.clone(),
        effective_timeout_ms: i64::try_from(binding.effective_timeout_ms).map_err(|_| {
            persistence_error("effective MCP timeout exceeds durable integer range")
        })?,
        runtime_generation: i64::try_from(binding.runtime_generation).map_err(|_| {
            persistence_error("MCP runtime generation exceeds durable integer range")
        })?,
        projection_activation_generation: i64::try_from(binding.projection_activation_generation)
            .map_err(|_| {
            persistence_error("MCP projection activation generation exceeds durable integer range")
        })?,
        selection_reason: binding.selection_reason.clone(),
        capability_id: binding.capability_id.clone(),
    })
}

fn persistence_error(message: impl Into<String>) -> AgentMcpProjectionPersistenceError {
    AgentMcpProjectionPersistenceError {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_mcp::projection::{
        McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnTool,
    };
    use serde_json::json;

    #[test]
    fn cli_persistence_keeps_exact_provider_name_and_transformed_schema_fingerprint() {
        let mut projection = ResolvedMcpTurnProjection::empty("workspace", "turn");
        projection.tools.push(ResolvedMcpTurnTool {
            canonical_callable_name: String::new(),
            workspace_id: "workspace".to_owned(),
            server_installation_id: "installation".to_owned(),
            server_name: "server".to_owned(),
            raw_tool_name: "send".to_owned(),
            description: None,
            input_schema: json!({"type": "object"}),
            annotations: None,
            timeout_ms: 20_000,
            catalog_version: "catalog".to_owned(),
            installation_fingerprint: "installation-fingerprint".to_owned(),
            schema_fingerprint: String::new(),
            runtime_generation: 3,
            selection_reason: McpSelectionReason::ExplicitTool,
            capability_id: Some("mcp-tool:send".to_owned()),
        });
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("projection identity");
        let canonical = projection.tools[0].canonical_callable_name.clone();
        let request = persistence_request_from_projection_with_provider(
            &projection,
            &[TurnMcpProviderBindingIdentity {
                canonical_callable_name: canonical.clone(),
                provider_callable_name: format!("mcp__pioneer__{canonical}"),
                provider_schema_fingerprint: "transformed-schema".to_owned(),
            }],
        )
        .expect("provider persistence request");

        assert_eq!(
            request.bindings[0].provider_callable_name,
            format!("mcp__pioneer__{canonical}")
        );
        assert_eq!(
            request.bindings[0].provider_schema_fingerprint,
            "transformed-schema"
        );
    }
}
