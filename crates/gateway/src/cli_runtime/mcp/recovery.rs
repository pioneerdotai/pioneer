//! Exact reconstruction of a frozen turn MCP projection for native recovery.

use crate::cli_runtime::claude_mcp::build_claude_mcp_session_launch_projection;
use crate::cli_runtime::codex_mcp::build_codex_mcp_session_launch_projection;
use crate::cli_runtime::continuation::CliMcpSessionLaunch;
use crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits;
use crate::turn_mcp::invoker::{
    TurnMcpInvocationErrorCode, TurnMcpRuntimeView, validate_frozen_identity,
};
use crate::turn_mcp::{
    MCP_TURN_PROJECTION_VERSION, McpProjectionLimits, McpSelectionReason,
    ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
};
use anyhow::{Context, Result, anyhow, bail};
use pioneer_crud::{CliRuntimeTurnBindingRecord, CrudStore, TurnMcpBindingRecord};
use pioneer_protocol::CLIAgentRuntimeKind;

const CODEX_MCP_ADAPTER_KIND: &str = "codex_synthetic_mcp";
const CLAUDE_MCP_ADAPTER_KIND: &str = "claude_strict_mcp";
const FIRST_PARTY_FILE_SERVER_ID: &str = "pioneer-file-tools-v1";

#[derive(Debug)]
pub(crate) enum CliMcpSessionLaunchRestoreError {
    Unavailable(anyhow::Error),
    Invalid(anyhow::Error),
}

impl std::fmt::Display for CliMcpSessionLaunchRestoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(error) | Self::Invalid(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CliMcpSessionLaunchRestoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unavailable(error) | Self::Invalid(error) => error.source(),
        }
    }
}

fn unavailable(error: anyhow::Error) -> CliMcpSessionLaunchRestoreError {
    CliMcpSessionLaunchRestoreError::Unavailable(error)
}

fn invalid(error: anyhow::Error) -> CliMcpSessionLaunchRestoreError {
    CliMcpSessionLaunchRestoreError::Invalid(error)
}

/// Restore the provider launch contract from the immutable turn projection.
///
/// Recovery must never re-resolve the user's MCP selection. It may only rebuild
/// the exact provider facade that was persisted for the interrupted turn, and
/// it fails closed if the current runtime tool has drifted.
pub(crate) async fn restore_cli_mcp_session_launch(
    crud_store: &CrudStore,
    runtime_view: &dyn TurnMcpRuntimeView,
    turn_binding: &CliRuntimeTurnBindingRecord,
    runtime_kind: CLIAgentRuntimeKind,
    limits: McpProjectionLimits,
) -> std::result::Result<CliMcpSessionLaunch, CliMcpSessionLaunchRestoreError> {
    let projection_record = crud_store
        .get_turn_mcp_projection(turn_binding.turn_id.as_str())
        .await
        .context("failed to load frozen MCP projection for CLI recovery")
        .map_err(unavailable)?;
    let bindings = crud_store
        .list_turn_mcp_bindings(turn_binding.turn_id.as_str())
        .await
        .context("failed to load frozen MCP bindings for CLI recovery")
        .map_err(unavailable)?;

    let Some(metadata) = turn_binding.mcp.as_ref() else {
        if projection_record
            .as_ref()
            .is_some_and(|projection| projection.tool_count != 0)
            || !bindings.is_empty()
        {
            return Err(invalid(anyhow!(
                "CLI recovery has frozen MCP tools but no activated MCP session contract"
            )));
        }
        return Ok(CliMcpSessionLaunch::Disabled);
    };
    let projection_record = projection_record
        .context("CLI recovery MCP session contract has no frozen turn projection")
        .map_err(invalid)?;
    if projection_record.turn_id != turn_binding.turn_id
        || projection_record.workspace_id != turn_binding.workspace_id
        || projection_record.projection_version
            != i32::try_from(MCP_TURN_PROJECTION_VERSION)
                .expect("MCP projection version fits in i32")
        || projection_record.manifest_hash != metadata.manifest_hash
        || usize::try_from(projection_record.tool_count).ok() != Some(bindings.len())
        || bindings.iter().any(|binding| {
            binding.projection_activation_generation != metadata.projection_activation_generation
        })
    {
        return Err(invalid(anyhow!(
            "CLI recovery MCP projection header does not match its immutable turn binding"
        )));
    }

    let mut projection = ResolvedMcpTurnProjection::empty(
        turn_binding.workspace_id.clone(),
        turn_binding.turn_id.clone(),
    );
    for binding in &bindings {
        // First-party Claude file tools are provider facade entries, not
        // upstream MCP installations.  They are re-added by the Claude
        // adapter from its canonical reserved catalog below; reconstructing
        // them as ordinary runtime tools would make the recovery manifest
        // ambiguous and would ask the MCP runtime to execute them upstream.
        if binding.server_installation_id == FIRST_PARTY_FILE_SERVER_ID {
            continue;
        }
        let current = runtime_view
            .current_tool_identity(turn_binding.workspace_id.as_str(), binding)
            .await
            .map_err(|error| {
                let temporarily_unavailable = matches!(
                    error.code,
                    TurnMcpInvocationErrorCode::RuntimeNotLive
                        | TurnMcpInvocationErrorCode::ResourceExhausted
                        | TurnMcpInvocationErrorCode::Cancelled
                        | TurnMcpInvocationErrorCode::TimedOut
                        | TurnMcpInvocationErrorCode::ExecutionFailed
                        | TurnMcpInvocationErrorCode::Internal
                );
                let error = anyhow!(error).context(format!(
                    "failed to restore frozen MCP tool `{}` for CLI recovery",
                    binding.canonical_callable_name
                ));
                if temporarily_unavailable {
                    unavailable(error)
                } else {
                    invalid(error)
                }
            })?;
        validate_frozen_identity(binding, &current).map_err(|error| invalid(anyhow!(error)))?;
        let annotations = serde_json::from_str(binding.annotations_json.as_str())
            .context("frozen MCP annotations are invalid during CLI recovery")
            .map_err(invalid)?;
        let selection_reason = match binding.selection_reason.as_str() {
            "implicit_policy" => McpSelectionReason::ImplicitPolicy,
            "explicit_composer_capability" => McpSelectionReason::ExplicitTool,
            other => {
                return Err(invalid(anyhow!(
                    "unsupported frozen MCP selection reason `{other}`"
                )));
            }
        };
        projection.tools.push(ResolvedMcpTurnTool {
            canonical_callable_name: binding.canonical_callable_name.clone(),
            workspace_id: turn_binding.workspace_id.clone(),
            server_installation_id: current.server_installation_id,
            server_name: current.server_name,
            raw_tool_name: current.raw_tool_name,
            description: current.description,
            input_schema: current.canonical_schema,
            annotations: Some(annotations),
            timeout_ms: current.effective_timeout_ms,
            catalog_version: current.catalog_version,
            installation_fingerprint: current.installation_fingerprint,
            schema_fingerprint: current.canonical_schema_fingerprint,
            runtime_generation: current.runtime_generation,
            selection_reason,
            capability_id: binding.capability_id.clone(),
        });
    }
    projection
        .finalize_identity(limits)
        .context("failed to finalize frozen MCP projection for CLI recovery")
        .map_err(invalid)?;
    if projection.manifest_hash != projection_record.manifest_hash {
        return Err(invalid(anyhow!(
            "reconstructed CLI recovery MCP manifest differs from the frozen projection"
        )));
    }
    validate_canonical_callable_names(projection.tools.as_slice(), bindings.as_slice())
        .map_err(invalid)?;

    let facade_limits = CliMcpFacadeProjectionLimits::transport_bounded(limits.max_tools);
    match runtime_kind {
        CLIAgentRuntimeKind::Codex => {
            if metadata.adapter_kind != CODEX_MCP_ADAPTER_KIND {
                return Err(invalid(anyhow!(
                    "frozen MCP adapter is not a Codex adapter"
                )));
            }
            let launch = build_codex_mcp_session_launch_projection(
                projection,
                metadata.provider_contract_fingerprint.clone(),
            )
            .context("failed to rebuild frozen Codex MCP launch projection")
            .map_err(invalid)?;
            let facade = launch
                .facade_projection(facade_limits)
                .map_err(|error| invalid(anyhow!(error)))?;
            let facade_fingerprint = facade.fingerprint();
            validate_provider_projection(
                launch.preflight.canonical_manifest_hash.as_str(),
                launch.preflight.provider_contract_fingerprint.as_str(),
                launch.semantic_restart_fingerprint(),
                facade_fingerprint.as_str(),
                metadata,
            )
            .map_err(invalid)?;
            validate_provider_bindings(
                bindings.as_slice(),
                launch.preflight.tools.iter().map(|tool| {
                    (
                        tool.canonical_callable_name.as_str(),
                        tool.transformed_schema_fingerprint.as_str(),
                    )
                }),
            )
            .map_err(invalid)?;
            Ok(CliMcpSessionLaunch::Codex(launch))
        }
        CLIAgentRuntimeKind::Claude => {
            if metadata.adapter_kind != CLAUDE_MCP_ADAPTER_KIND {
                return Err(invalid(anyhow!(
                    "frozen MCP adapter is not a Claude adapter"
                )));
            }
            let launch = build_claude_mcp_session_launch_projection(
                projection,
                metadata.provider_contract_fingerprint.clone(),
            )
            .context("failed to rebuild frozen Claude MCP launch projection")
            .map_err(invalid)?;
            let facade = launch
                .facade_projection(facade_limits)
                .map_err(|error| invalid(anyhow!(error)))?;
            let facade_fingerprint = facade.fingerprint();
            validate_provider_projection(
                launch.preflight.canonical_manifest_hash.as_str(),
                launch.preflight.provider_contract_fingerprint.as_str(),
                launch.semantic_restart_fingerprint(),
                facade_fingerprint.as_str(),
                metadata,
            )
            .map_err(invalid)?;
            validate_provider_bindings(
                bindings.as_slice(),
                launch.preflight.tools.iter().map(|tool| {
                    (
                        tool.canonical_callable_name.as_str(),
                        tool.transformed_schema_fingerprint.as_str(),
                    )
                }),
            )
            .map_err(invalid)?;
            Ok(CliMcpSessionLaunch::Claude(launch))
        }
    }
}

fn validate_canonical_callable_names(
    tools: &[ResolvedMcpTurnTool],
    bindings: &[TurnMcpBindingRecord],
) -> Result<()> {
    for tool in tools {
        if tool.server_installation_id == FIRST_PARTY_FILE_SERVER_ID {
            continue;
        }
        let binding = bindings
            .iter()
            .find(|binding| {
                binding.server_installation_id == tool.server_installation_id
                    && binding.raw_tool_name == tool.raw_tool_name
            })
            .context("reconstructed MCP tool has no frozen binding")?;
        if binding.canonical_callable_name != tool.canonical_callable_name
            || binding.callable_name != tool.canonical_callable_name
        {
            bail!("reconstructed MCP callable name differs from its frozen binding");
        }
    }
    Ok(())
}

fn validate_provider_projection(
    manifest_hash: &str,
    provider_contract_fingerprint: &str,
    isolation_contract_fingerprint: &str,
    projection_fingerprint: &str,
    metadata: &pioneer_crud::CliRuntimeTurnMcpMetadata,
) -> Result<()> {
    if manifest_hash != metadata.manifest_hash
        || provider_contract_fingerprint != metadata.provider_contract_fingerprint
        || isolation_contract_fingerprint != metadata.isolation_contract_fingerprint
        || projection_fingerprint != metadata.projection_fingerprint
    {
        bail!("reconstructed CLI recovery MCP provider contract differs from the frozen session");
    }
    Ok(())
}

fn validate_provider_bindings<'a>(
    bindings: &[TurnMcpBindingRecord],
    transformed: impl Iterator<Item = (&'a str, &'a str)>,
) -> Result<()> {
    let transformed = transformed.collect::<std::collections::HashMap<_, _>>();
    if transformed.len() != bindings.len() {
        bail!("reconstructed MCP provider projection is not exact");
    }
    for binding in bindings {
        let expected_provider_name = format!("mcp__pioneer__{}", binding.canonical_callable_name);
        if binding.provider_callable_name != expected_provider_name
            || transformed
                .get(binding.canonical_callable_name.as_str())
                .copied()
                != Some(binding.provider_schema_fingerprint.as_str())
        {
            bail!("reconstructed MCP provider binding differs from its frozen identity");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_mcp::invoker::{
        CurrentMcpToolIdentity, TurnMcpInvocationError, TurnMcpInvocationErrorCode,
    };
    use crate::turn_mcp::projection::{canonical_annotations_identity, canonical_schema_identity};
    use async_trait::async_trait;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        CliRuntimeTurnMcpMetadata, TurnMcpProjectionRecord, TurnMcpProjectionReplacement,
    };
    use pioneer_protocol::{
        SandboxMode, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
        Turn, TurnStatus,
    };
    use sea_orm::Database;
    use serde_json::json;

    #[derive(Clone)]
    struct StaticRuntimeView {
        current: CurrentMcpToolIdentity,
    }

    #[async_trait]
    impl TurnMcpRuntimeView for StaticRuntimeView {
        async fn current_tool_identity(
            &self,
            _workspace_id: &str,
            _binding: &TurnMcpBindingRecord,
        ) -> Result<CurrentMcpToolIdentity, TurnMcpInvocationError> {
            Ok(self.current.clone())
        }
    }

    async fn test_store() -> CrudStore {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        CrudStore::new(connection)
    }

    async fn persist_test_turn(
        store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        let now = chrono::Utc::now().timestamp();
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: Some("MCP recovery".to_owned()),
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5".to_owned(),
            model_provider: "cli_runtime:codex".to_owned(),
            reasoning_effort: None,
            created_at: now,
            updated_at: now,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .upsert_thread_model(&thread, pioneer_protocol::PersistedActorRef::System)
            .await
            .expect("thread should persist");
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn should persist");
    }

    #[tokio::test]
    async fn codex_recovery_restores_exact_frozen_mcp_contract_and_rejects_runtime_drift() {
        let store = test_store().await;
        let workspace_id = "workspace_mcp_recovery";
        let thread_id = "thread_mcp_recovery";
        let turn_id = "turn_mcp_recovery";
        persist_test_turn(&store, workspace_id, thread_id, turn_id).await;

        let annotations = pioneer_tools::McpDynamicToolAnnotations::default();
        let mut projection = ResolvedMcpTurnProjection::empty(workspace_id, turn_id);
        projection.tools.push(ResolvedMcpTurnTool {
            canonical_callable_name: String::new(),
            workspace_id: workspace_id.to_owned(),
            server_installation_id: "mcp-installation".to_owned(),
            server_name: "analytics".to_owned(),
            raw_tool_name: "query".to_owned(),
            description: Some("Query analytics".to_owned()),
            input_schema: json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"]
            }),
            annotations: Some(annotations.clone()),
            timeout_ms: 20_000,
            catalog_version: "catalog-v1".to_owned(),
            installation_fingerprint: "installation-fingerprint".to_owned(),
            schema_fingerprint: String::new(),
            runtime_generation: 7,
            selection_reason: McpSelectionReason::ExplicitTool,
            capability_id: Some("mcp-tool:query".to_owned()),
        });
        projection
            .finalize_identity(McpProjectionLimits::default())
            .expect("projection should finalize");
        let provider_contract_fingerprint = "a".repeat(64);
        let original_launch = build_codex_mcp_session_launch_projection(
            projection.clone(),
            provider_contract_fingerprint.clone(),
        )
        .expect("Codex MCP projection should build");
        let facade_fingerprint = original_launch
            .facade_projection(CliMcpFacadeProjectionLimits::transport_bounded(128))
            .expect("facade should build")
            .fingerprint()
            .as_str()
            .to_owned();
        let tool = projection.tools.first().expect("tool should exist");
        let transformed = original_launch
            .preflight
            .tools
            .first()
            .expect("transformed tool should exist");
        let (annotations_json, annotations_digest) =
            canonical_annotations_identity(&annotations).expect("annotations should canonicalize");
        let durable_binding = TurnMcpBindingRecord {
            server_installation_id: tool.server_installation_id.clone(),
            server_name: tool.server_name.clone(),
            raw_tool_name: tool.raw_tool_name.clone(),
            callable_name: tool.canonical_callable_name.clone(),
            canonical_callable_name: tool.canonical_callable_name.clone(),
            provider_callable_name: format!("mcp__pioneer__{}", tool.canonical_callable_name),
            catalog_version: tool.catalog_version.clone(),
            fingerprint: tool.installation_fingerprint.clone(),
            canonical_schema_fingerprint: tool.schema_fingerprint.clone(),
            provider_schema_fingerprint: transformed.transformed_schema_fingerprint.clone(),
            annotations_json: annotations_json.clone(),
            annotations_digest: annotations_digest.clone(),
            effective_timeout_ms: 20_000,
            runtime_generation: 7,
            projection_activation_generation: 1,
            selection_reason: "explicit_composer_capability".to_owned(),
            capability_id: tool.capability_id.clone(),
        };
        store
            .replace_turn_mcp_projection(&TurnMcpProjectionReplacement {
                projection: TurnMcpProjectionRecord {
                    turn_id: turn_id.to_owned(),
                    workspace_id: workspace_id.to_owned(),
                    projection_version: MCP_TURN_PROJECTION_VERSION as i32,
                    manifest_hash: projection.manifest_hash.clone(),
                    resolution_status: "resolved".to_owned(),
                    tool_count: 1,
                    created_at_unix: chrono::Utc::now().timestamp(),
                },
                bindings: vec![durable_binding.clone()],
            })
            .await
            .expect("projection should persist");
        let metadata = CliRuntimeTurnMcpMetadata {
            adapter_kind: CODEX_MCP_ADAPTER_KIND.to_owned(),
            manifest_hash: projection.manifest_hash.clone(),
            projection_fingerprint: facade_fingerprint,
            provider_contract_fingerprint,
            isolation_contract_fingerprint: original_launch
                .semantic_restart_fingerprint()
                .to_owned(),
            session_generation: 1,
            projection_activation_generation: 1,
        };
        let now = chrono::Utc::now().fixed_offset();
        let turn_binding = CliRuntimeTurnBindingRecord {
            turn_id: turn_id.to_owned(),
            thread_id: thread_id.to_owned(),
            continuation_thread_id: thread_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_thread_id: "native-thread".to_owned(),
            native_turn_id: Some("native-turn".to_owned()),
            request_id: None,
            status: "running".to_owned(),
            model: Some("gpt-5".to_owned()),
            cwd: Some("/tmp/project".to_owned()),
            sandbox_json: None,
            approval_policy: None,
            input_mapping_json: "{}".to_owned(),
            mcp: Some(metadata),
            native_goal_status: None,
            native_goal_turn_id: None,
            native_goal_observed_at: None,
            created_at: now,
            updated_at: now,
        };
        let (canonical_schema, canonical_schema_fingerprint, _) =
            canonical_schema_identity(&tool.input_schema).expect("schema should canonicalize");
        let current = CurrentMcpToolIdentity {
            server_installation_id: tool.server_installation_id.clone(),
            server_name: tool.server_name.clone(),
            raw_tool_name: tool.raw_tool_name.clone(),
            description: tool.description.clone(),
            catalog_version: tool.catalog_version.clone(),
            installation_fingerprint: tool.installation_fingerprint.clone(),
            canonical_schema_fingerprint,
            canonical_schema,
            annotations_json,
            annotations_digest,
            effective_timeout_ms: tool.timeout_ms,
            runtime_generation: tool.runtime_generation,
        };

        let restored = restore_cli_mcp_session_launch(
            &store,
            &StaticRuntimeView {
                current: current.clone(),
            },
            &turn_binding,
            CLIAgentRuntimeKind::Codex,
            McpProjectionLimits::default(),
        )
        .await
        .expect("exact frozen projection should restore");
        assert!(matches!(restored, CliMcpSessionLaunch::Codex(_)));

        let error = restore_cli_mcp_session_launch(
            &store,
            &StaticRuntimeView {
                current: CurrentMcpToolIdentity {
                    runtime_generation: current.runtime_generation + 1,
                    ..current
                },
            },
            &turn_binding,
            CLIAgentRuntimeKind::Codex,
            McpProjectionLimits::default(),
        )
        .await
        .expect_err("runtime drift must fail closed");
        assert!(
            error
                .to_string()
                .contains(TurnMcpInvocationErrorCode::ToolDrift.as_str())
        );
    }
}
