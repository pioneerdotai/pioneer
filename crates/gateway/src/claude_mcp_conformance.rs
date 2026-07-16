//! Deterministic local Claude MCP adapter checks.
//!
//! The checks in this module exercise production projection, restart identity,
//! exact preallow, permission parsing, and native-item binding without starting
//! a provider process.

use crate::cli_runtime::claude_mcp::{
    ClaudeManagedMcpLaunchMode, ClaudeNativeMcpEventError, ClaudeNativeMcpPermissionParseError,
    append_claude_exact_allowed_tools, build_claude_mcp_session_launch_projection,
    materialize_claude_mcp_config, parse_claude_native_mcp_permission_request,
};
use crate::cli_runtime::claude_session::claim_claude_mcp_permission_request;
use crate::cli_runtime::continuation::{
    CliMcpSessionLaunch, CliSessionLaunchSpec, requires_restart,
};
use crate::cli_runtime::manager::CLIAgentRuntimeSessionStartOptions;
use crate::cli_runtime::mcp::conformance::run_cli_mcp_bridge_conformance;
use crate::cli_runtime::permissions::{
    ClaudeMcpPermissionFallbackDecision, claude_mcp_permission_fallback_response,
};
use crate::cli_runtime::skills::partition_cli_runtime_capabilities;
use crate::turn_mcp::projection::{
    McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
};
use anyhow::{Context, Result, ensure};
use pioneer_cli_agent_runtime::claude::{
    ClaudeManagedMcpConfigIdentity, cleanup_claude_managed_mcp_config,
};
use pioneer_protocol::{McpScopeKind, TurnCapability, TurnCapabilityKind};
use pioneer_tools::McpDynamicToolAnnotations;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use uuid::Uuid;

const WORKSPACE_ID: &str = "claude-deterministic-workspace";
const PROVIDER_SESSION_ID: &str = "01900000-0000-7000-8000-000000000031";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeMcpDeterministicEvidence {
    pub callable_a: String,
    pub callable_b: String,
    pub qualified_a: String,
    pub qualified_b: String,
    pub manifest_a: String,
    pub manifest_b: String,
    pub same_projection_reused: bool,
    pub changed_projection_requires_restart: bool,
    pub provider_session_identity_preserved: bool,
    pub concurrent_projections_isolated: bool,
    pub empty_projection_is_empty: bool,
    pub strict_managed_config_isolated: bool,
    pub mixed_skill_server_preflight_preserved: bool,
    pub mixed_skill_tool_preflight_preserved: bool,
    pub exact_native_item_bound: bool,
    pub unselected_native_item_rejected: bool,
    pub exact_permission_request_parsed: bool,
    pub wildcard_permission_request_rejected: bool,
    pub exact_permission_fallback_allowed: bool,
    pub permission_callback_deduplicated: bool,
    pub native_timeline_deduplicated: bool,
    pub initial_turn_blocked_until_exact_list: bool,
    pub helper_attached: bool,
    pub bridge_call_succeeded: bool,
    pub bridge_cancellation_propagated: bool,
    pub bridge_cleanup_complete: bool,
    pub secret_canary_absent: bool,
    pub recorded_scenarios: usize,
    pub recorded_tool_uses: usize,
    pub recorded_unique_call_ids: usize,
}

pub async fn run_claude_mcp_deterministic_conformance() -> Result<ClaudeMcpDeterministicEvidence> {
    let provider_contract = sha256("claude-adapter-contract-v1");
    let projection_a = projection("turn-a", "tool_a", true)?;
    let projection_a_next = projection("turn-a-next", "tool_a", true)?;
    let projection_b = projection("turn-b", "tool_b", false)?;
    let empty_projection = empty_projection("turn-empty")?;

    let callable_a = projection_a.tools[0].canonical_callable_name.clone();
    let callable_b = projection_b.tools[0].canonical_callable_name.clone();
    ensure!(
        callable_a != callable_b,
        "fixture tools must remain distinct"
    );

    let launch_a = build_claude_mcp_session_launch_projection(
        projection_a.clone(),
        provider_contract.clone(),
    )?;
    let launch_a_next =
        build_claude_mcp_session_launch_projection(projection_a_next, provider_contract.clone())?;
    let launch_b = build_claude_mcp_session_launch_projection(
        projection_b.clone(),
        provider_contract.clone(),
    )?;
    let empty_launch =
        build_claude_mcp_session_launch_projection(empty_projection, provider_contract.clone())?;

    let qualified_a = launch_a.preflight.allowed_tool_names[0].clone();
    let qualified_b = launch_b.preflight.allowed_tool_names[0].clone();
    ensure!(
        qualified_a == format!("mcp__pioneer__{callable_a}")
            && qualified_b == format!("mcp__pioneer__{callable_b}"),
        "Claude exact preallow names drifted"
    );

    let facade_limits =
        crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits::transport_bounded(1);
    let facade_a = launch_a.facade_projection(facade_limits)?;
    let facade_b = launch_b.facade_projection(facade_limits)?;
    let empty_facade = empty_launch.facade_projection(facade_limits)?;
    ensure!(
        facade_a.tools().len() == 1 && facade_a.contains_tool(callable_a.as_str()),
        "Claude A projection must expose exactly A"
    );
    ensure!(
        facade_b.tools().len() == 1 && facade_b.contains_tool(callable_b.as_str()),
        "Claude B projection must expose exactly B"
    );
    let concurrent_projections_isolated = !facade_a.contains_tool(callable_b.as_str())
        && !facade_b.contains_tool(callable_a.as_str());
    ensure!(
        concurrent_projections_isolated,
        "parallel Claude projections must remain disjoint"
    );
    let empty_projection_is_empty =
        empty_facade.tools().is_empty() && empty_launch.preflight.allowed_tool_names.is_empty();
    ensure!(
        empty_projection_is_empty,
        "empty Claude projection exposed a tool"
    );

    let temporary = tempfile::tempdir().context("create Claude conformance root")?;
    let managed_root = temporary.path().join("managed");
    let bootstrap_path = temporary.path().join("bootstrap.json");
    std::fs::write(&bootstrap_path, b"{}\n").context("write Claude bootstrap fixture")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&bootstrap_path, std::fs::Permissions::from_mode(0o600))
            .context("protect Claude bootstrap fixture")?;
    }
    let exact_descriptor = materialize_claude_mcp_config(
        managed_root.as_path(),
        claude_config_identity(1)?,
        ClaudeManagedMcpLaunchMode::Pioneer {
            bootstrap_path: bootstrap_path.clone(),
        },
    )?;
    let empty_descriptor = materialize_claude_mcp_config(
        managed_root.as_path(),
        claude_config_identity(2)?,
        ClaudeManagedMcpLaunchMode::Empty,
    )?;
    let exact_document: JsonValue = serde_json::from_slice(
        &std::fs::read(&exact_descriptor.config_path).context("read exact Claude config")?,
    )
    .context("decode exact Claude config")?;
    let empty_document: JsonValue = serde_json::from_slice(
        &std::fs::read(&empty_descriptor.config_path).context("read empty Claude config")?,
    )
    .context("decode empty Claude config")?;
    let mut exact_args = Vec::new();
    append_claude_exact_allowed_tools(
        &mut exact_args,
        &exact_descriptor,
        &launch_a.preflight.allowed_tool_names,
    )?;
    let strict_managed_config_isolated =
        exact_document["mcpServers"]
            .as_object()
            .is_some_and(|servers| {
                servers.len() == 1
                    && servers.contains_key("pioneer")
                    && !servers.contains_key("unmanaged_pioneer_sentinel")
            })
            && empty_document == json!({"mcpServers": {}})
            && exact_args == ["--allowedTools", qualified_a.as_str()]
            && !exact_document.to_string().contains("malicious_sentinel");
    ensure!(
        strict_managed_config_isolated,
        "Claude strict managed config or exact preallow drifted"
    );
    cleanup_claude_managed_mcp_config(&exact_descriptor)?;
    cleanup_claude_managed_mcp_config(&empty_descriptor)?;

    let mixed_server = partition_cli_runtime_capabilities(&[
        skill_capability("server-skill"),
        TurnCapability {
            id: "server-a".to_owned(),
            kind: TurnCapabilityKind::McpServer {
                name: "server".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
            label: Some("Server A".to_owned()),
        },
    ]);
    let mixed_tool = partition_cli_runtime_capabilities(&[
        skill_capability("tool-skill"),
        TurnCapability {
            id: "tool-b".to_owned(),
            kind: TurnCapabilityKind::McpTool {
                server_name: "server".to_owned(),
                raw_tool_name: "tool_b".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
            label: Some("Tool B".to_owned()),
        },
    ]);
    let mixed_skill_server_preflight_preserved =
        mixed_server.skills.len() == 1 && mixed_server.mcp_servers.len() == 1;
    let mixed_skill_tool_preflight_preserved =
        mixed_tool.skills.len() == 1 && mixed_tool.mcp_tools.len() == 1;
    ensure!(
        mixed_skill_server_preflight_preserved && mixed_skill_tool_preflight_preserved,
        "combined Claude skill/MCP preflight dropped a capability kind"
    );

    let provider_session_id = Uuid::parse_str(PROVIDER_SESSION_ID)?;
    let spec_a = CliSessionLaunchSpec::claude_new(
        CLIAgentRuntimeSessionStartOptions::default(),
        CliMcpSessionLaunch::Claude(launch_a.clone()),
        provider_session_id,
    );
    let spec_a_next = CliSessionLaunchSpec::claude_resume(
        CLIAgentRuntimeSessionStartOptions::default(),
        CliMcpSessionLaunch::Claude(launch_a_next),
        provider_session_id,
    );
    let spec_b = CliSessionLaunchSpec::claude_resume(
        CLIAgentRuntimeSessionStartOptions::default(),
        CliMcpSessionLaunch::Claude(launch_b.clone()),
        provider_session_id,
    );
    let same_projection_reused = !requires_restart(&spec_a, &spec_a_next);
    let changed_projection_requires_restart = requires_restart(&spec_a, &spec_b);
    let provider_session_identity_preserved = spec_a
        .continuation
        .claude_provider_session_id()
        .is_some_and(|identity| identity == provider_session_id)
        && spec_a_next
            .continuation
            .claude_provider_session_id()
            .is_some_and(|identity| identity == provider_session_id)
        && spec_b
            .continuation
            .claude_provider_session_id()
            .is_some_and(|identity| identity == provider_session_id);
    ensure!(
        same_projection_reused,
        "turn-local identity triggered a restart"
    );
    ensure!(
        changed_projection_requires_restart,
        "A to B Claude projection change did not trigger a restart"
    );
    ensure!(
        provider_session_identity_preserved,
        "Claude restart lost the stable provider session identity"
    );

    let arguments = json!({"value": "A"});
    let exact_binding = launch_a.bind_native_tool_use(
        "claude",
        7,
        PROVIDER_SESSION_ID,
        "native-turn-a",
        "native-item-a",
        qualified_a.as_str(),
        &arguments,
    )?;
    let exact_native_item_bound = exact_binding.canonical_callable_name == callable_a
        && exact_binding.native_thread_id == PROVIDER_SESSION_ID
        && exact_binding.native_turn_id == "native-turn-a"
        && exact_binding.native_item_id == "native-item-a";
    ensure!(
        exact_native_item_bound,
        "managed Claude item binding drifted"
    );

    let unselected_native_item_rejected = matches!(
        launch_a.bind_native_tool_use(
            "claude",
            7,
            PROVIDER_SESSION_ID,
            "native-turn-a",
            "native-item-b",
            qualified_b.as_str(),
            &arguments,
        ),
        Err(ClaudeNativeMcpEventError::ToolOutsideProjection)
    );
    ensure!(
        unselected_native_item_rejected,
        "Claude accepted an item outside the frozen projection"
    );

    let exact_permission = json!({
        "subtype": "can_use_tool",
        "tool_name": qualified_a,
        "tool_use_id": "permission-a",
        "input": arguments,
    });
    let parsed_permission = parse_claude_native_mcp_permission_request(
        &exact_permission,
        "claude",
        7,
        PROVIDER_SESSION_ID,
        "native-turn-a",
        projection_a.manifest_hash.as_str(),
        provider_contract.as_str(),
    )?;
    let exact_permission_request_parsed = parsed_permission.qualified_tool_name == qualified_a
        && parsed_permission.canonical_callable_name == callable_a
        && parsed_permission.native_item_id == "permission-a";
    ensure!(
        exact_permission_request_parsed,
        "exact Claude permission request binding drifted"
    );

    let wildcard_permission = json!({
        "subtype": "can_use_tool",
        "tool_name": "mcp__pioneer__*",
        "tool_use_id": "permission-wildcard",
        "input": {},
    });
    let wildcard_permission_request_rejected = matches!(
        parse_claude_native_mcp_permission_request(
            &wildcard_permission,
            "claude",
            7,
            PROVIDER_SESSION_ID,
            "native-turn-a",
            projection_a.manifest_hash.as_str(),
            provider_contract.as_str(),
        ),
        Err(ClaudeNativeMcpPermissionParseError::WildcardOrInvalidName)
    );
    ensure!(
        wildcard_permission_request_rejected,
        "Claude accepted a wildcard permission request"
    );

    let exact_permission_fallback_allowed = claude_mcp_permission_fallback_response(
        ClaudeMcpPermissionFallbackDecision::AllowExact,
        &parsed_permission.arguments,
    ) == json!({
        "behavior": "allow",
        "updatedInput": parsed_permission.arguments,
    });
    let mut completed_permission_requests = HashSet::new();
    let permission_callback_deduplicated = claim_claude_mcp_permission_request(
        &mut completed_permission_requests,
        parsed_permission.native_item_id.as_str(),
    ) && !claim_claude_mcp_permission_request(
        &mut completed_permission_requests,
        parsed_permission.native_item_id.as_str(),
    );
    ensure!(
        exact_permission_fallback_allowed && permission_callback_deduplicated,
        "Claude exact approval fallback or replay suppression drifted"
    );

    let (recorded_scenarios, recorded_tool_uses, recorded_unique_call_ids) =
        recorded_fixture_counts(callable_a.as_str(), callable_b.as_str())?;
    let native_timeline_deduplicated = recorded_unique_call_ids < recorded_tool_uses;
    ensure!(
        native_timeline_deduplicated,
        "Claude replay fixture no longer proves canonical item deduplication"
    );
    let bridge = run_cli_mcp_bridge_conformance("claude").await?;
    let bridge_cleanup_complete = bridge.grant_revoked_on_eof && bridge.artifacts_removed;

    Ok(ClaudeMcpDeterministicEvidence {
        callable_a,
        callable_b,
        qualified_a,
        qualified_b,
        manifest_a: projection_a.manifest_hash,
        manifest_b: projection_b.manifest_hash,
        same_projection_reused,
        changed_projection_requires_restart,
        provider_session_identity_preserved,
        concurrent_projections_isolated: concurrent_projections_isolated
            && bridge.concurrent_isolation_observed,
        empty_projection_is_empty,
        strict_managed_config_isolated,
        mixed_skill_server_preflight_preserved,
        mixed_skill_tool_preflight_preserved,
        exact_native_item_bound,
        unselected_native_item_rejected,
        exact_permission_request_parsed,
        wildcard_permission_request_rejected,
        exact_permission_fallback_allowed,
        permission_callback_deduplicated,
        native_timeline_deduplicated,
        initial_turn_blocked_until_exact_list: bridge.turn_blocked_before_list
            && bridge.bootstrap_consumed_before_list
            && bridge.exact_list_observed,
        helper_attached: bridge.helper_attached,
        bridge_call_succeeded: bridge.successful_call_observed,
        bridge_cancellation_propagated: bridge.cancellation_propagated,
        bridge_cleanup_complete,
        secret_canary_absent: bridge.secret_canary_absent
            && !exact_document
                .to_string()
                .contains("pioneer-bridge-secret-canary-53"),
        recorded_scenarios,
        recorded_tool_uses,
        recorded_unique_call_ids,
    })
}

fn claude_config_identity(generation: u64) -> Result<ClaudeManagedMcpConfigIdentity> {
    ClaudeManagedMcpConfigIdentity::new(
        WORKSPACE_ID,
        "claude",
        format!("conformance-thread-{generation}"),
        "conformance-boot",
        generation,
    )
    .map_err(|error| anyhow::anyhow!("invalid Claude conformance config identity: {error}"))
}

fn skill_capability(id: &str) -> TurnCapability {
    TurnCapability {
        id: id.to_owned(),
        kind: TurnCapabilityKind::Skill {
            slug: format!("workspace/{id}"),
            source_kind: "workspace".to_owned(),
        },
        label: Some(format!("Skill {id}")),
    }
}

fn projection(
    turn_id: &str,
    raw_tool_name: &str,
    risky: bool,
) -> Result<ResolvedMcpTurnProjection> {
    let mut projection = ResolvedMcpTurnProjection::empty(WORKSPACE_ID, turn_id);
    projection.tools.push(ResolvedMcpTurnTool {
        canonical_callable_name: String::new(),
        workspace_id: WORKSPACE_ID.to_owned(),
        server_installation_id: format!("installation-{raw_tool_name}"),
        server_name: "server".to_owned(),
        raw_tool_name: raw_tool_name.to_owned(),
        description: Some("Deterministic Claude MCP fixture tool".to_owned()),
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {"value": {"type": "string"}}
        }),
        annotations: Some(McpDynamicToolAnnotations {
            read_only_hint: Some(!risky),
            destructive_hint: Some(risky),
            idempotent_hint: Some(!risky),
            open_world_hint: Some(risky),
            ..Default::default()
        }),
        timeout_ms: 30_000,
        catalog_version: "claude-fixture-v1".to_owned(),
        installation_fingerprint: sha256(raw_tool_name),
        schema_fingerprint: String::new(),
        runtime_generation: 1,
        selection_reason: McpSelectionReason::ExplicitTool,
        capability_id: Some(format!("capability-{raw_tool_name}")),
    });
    projection
        .finalize_identity(McpProjectionLimits::default())
        .map_err(|error| {
            anyhow::anyhow!("failed to finalize Claude fixture projection: {error}")
        })?;
    Ok(projection)
}

fn empty_projection(turn_id: &str) -> Result<ResolvedMcpTurnProjection> {
    let mut projection = ResolvedMcpTurnProjection::empty(WORKSPACE_ID, turn_id);
    projection
        .finalize_identity(McpProjectionLimits::default())
        .map_err(|error| anyhow::anyhow!("failed to finalize empty Claude projection: {error}"))?;
    Ok(projection)
}

fn recorded_fixture_counts(callable_a: &str, callable_b: &str) -> Result<(usize, usize, usize)> {
    let matrix: JsonValue =
        serde_json::from_str(include_str!("../tests/fixtures/cli_mcp_claude/matrix.json"))
            .context("decode deterministic Claude scenario matrix")?;
    let scenarios = matrix["scenarios"]
        .as_array()
        .context("Claude scenario matrix omitted scenarios")?;
    ensure!(scenarios.len() == 6, "Claude scenario matrix drifted");

    let skill = include_str!("../tests/fixtures/cli_mcp_claude/skill/SKILL.md");
    for marker in [
        matrix["serverSelectionMarker"].as_str(),
        matrix["individualToolMarker"].as_str(),
    ] {
        let marker = marker.context("Claude scenario matrix omitted a skill marker")?;
        ensure!(
            skill.contains(marker),
            "Claude skill fixture omitted `{marker}`"
        );
    }

    let lifecycle: JsonValue = serde_json::from_str(include_str!(
        "../../cli-agent-runtime/tests/fixtures/claude_mcp/lifecycle.json"
    ))
    .context("decode recorded Claude lifecycle fixture")?;
    let messages = lifecycle["messages"]
        .as_array()
        .context("Claude lifecycle fixture omitted messages")?;
    let mut tool_uses = 0;
    let mut call_ids = HashSet::new();
    let mut tool_names = HashSet::new();
    for message in messages {
        ensure!(
            message["session_id"].as_str() == Some(PROVIDER_SESSION_ID),
            "Claude lifecycle fixture changed provider session identity"
        );
        let Some(content) = message["message"]["content"].as_array() else {
            continue;
        };
        for item in content {
            if item["type"].as_str() == Some("tool_use") {
                tool_uses += 1;
                call_ids.insert(
                    item["id"]
                        .as_str()
                        .context("Claude tool use omitted its call id")?
                        .to_owned(),
                );
                tool_names.insert(
                    item["name"]
                        .as_str()
                        .context("Claude tool use omitted its exact name")?
                        .to_owned(),
                );
            }
        }
    }
    ensure!(tool_uses == 3, "Claude lifecycle tool-use count drifted");
    ensure!(
        call_ids.len() == 2,
        "Claude lifecycle call correlation drifted"
    );
    ensure!(
        tool_names
            == HashSet::from([
                format!("mcp__pioneer__{callable_a}"),
                format!("mcp__pioneer__{callable_b}"),
            ]),
        "Claude lifecycle exact tool surface drifted"
    );
    Ok((scenarios.len(), tool_uses, call_ids.len()))
}

fn sha256(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}
