use super::projection::{ResolvedMcpTurnProjection, canonical_annotations_identity};
use pioneer_agent::{
    AgentMcpPersistedProjection, AgentMcpProjectionBinding, AgentMcpProjectionPersistenceError,
    AgentMcpProjectionPersistenceRequest,
};
use pioneer_crud::{
    CrudStore, TurnMcpBindingRecord, TurnMcpProjectionRecord, TurnMcpProjectionReplacement,
};
use std::sync::Arc;

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
    let provider_bindings = provider_bindings
        .iter()
        .map(|binding| (binding.canonical_callable_name.as_str(), binding))
        .collect::<std::collections::HashMap<_, _>>();
    if !provider_bindings.is_empty() && provider_bindings.len() != projection.tools.len() {
        return Err(
            "provider MCP binding projection is not an exact canonical projection".to_owned(),
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
