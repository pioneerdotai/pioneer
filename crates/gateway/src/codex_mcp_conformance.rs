//! Deterministic local Codex MCP adapter checks.
//!
//! The checks in this module exercise production projection, restart identity,
//! facade, and native-event binding code without starting a provider process.

use crate::cli_runtime::codex_mcp::{
    CodexNativeMcpEventError, build_codex_managed_mcp_config,
    build_codex_mcp_session_launch_projection,
};
use crate::cli_runtime::codex_session::codex_native_approval_fallback_response;
use crate::cli_runtime::continuation::{
    CliMcpSessionLaunch, CliProviderContinuation, CliSessionLaunchSpec, requires_restart,
};
use crate::cli_runtime::manager::CLIAgentRuntimeSessionStartOptions;
use crate::cli_runtime::mcp::conformance::run_cli_mcp_bridge_conformance;
use crate::turn_mcp::projection::{
    McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
};
use anyhow::{Context, Result, ensure};
use pioneer_cli_agent_runtime::codex::CodexJsonlRpcNotificationEvent;
use pioneer_cli_agent_runtime::event::{
    RuntimeEvent, RuntimeEventMappingOptions, RuntimeItemStarted, map_codex_notification_event,
};
use pioneer_tools::McpDynamicToolAnnotations;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};

const WORKSPACE_ID: &str = "codex-deterministic-workspace";
const NATIVE_THREAD_ID: &str = "codex-native-thread-53";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexMcpDeterministicEvidence {
    pub callable_a: String,
    pub callable_b: String,
    pub manifest_a: String,
    pub manifest_b: String,
    pub same_projection_reused: bool,
    pub changed_projection_requires_restart: bool,
    pub native_thread_identity_preserved: bool,
    pub concurrent_projections_isolated: bool,
    pub empty_projection_is_empty: bool,
    pub exact_managed_config_isolated: bool,
    pub exact_approval_fallback_allowed: bool,
    pub stale_approval_fallback_denied: bool,
    pub exact_native_item_bound: bool,
    pub unselected_native_item_rejected: bool,
    pub native_timeline_deduplicated: bool,
    pub initial_turn_blocked_until_exact_list: bool,
    pub helper_attached: bool,
    pub bridge_call_succeeded: bool,
    pub bridge_cancellation_propagated: bool,
    pub bridge_cleanup_complete: bool,
    pub secret_canary_absent: bool,
    pub recorded_started_items: usize,
    pub recorded_progress_items: usize,
    pub recorded_completed_items: usize,
    pub recorded_permission_requests: usize,
}

pub async fn run_codex_mcp_deterministic_conformance() -> Result<CodexMcpDeterministicEvidence> {
    let provider_contract = sha256("codex-adapter-contract-v1");
    let projection_a = projection("turn-a", "tool_a", true)?;
    let projection_a_next = projection("turn-a-next", "tool_a", true)?;
    let projection_b = projection("turn-b", "tool_b", true)?;
    let empty_projection = empty_projection("turn-empty")?;

    let callable_a = projection_a.tools[0].canonical_callable_name.clone();
    let callable_b = projection_b.tools[0].canonical_callable_name.clone();
    ensure!(
        callable_a != callable_b,
        "fixture tools must remain distinct"
    );

    let launch_a =
        build_codex_mcp_session_launch_projection(projection_a.clone(), provider_contract.clone())?;
    let launch_a_next =
        build_codex_mcp_session_launch_projection(projection_a_next, provider_contract.clone())?;
    let launch_b =
        build_codex_mcp_session_launch_projection(projection_b.clone(), provider_contract.clone())?;
    let empty_launch =
        build_codex_mcp_session_launch_projection(empty_projection, provider_contract)?;

    let facade_limits =
        crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits::transport_bounded(1);
    let facade_a = launch_a.facade_projection(facade_limits)?;
    let facade_b = launch_b.facade_projection(facade_limits)?;
    let empty_facade = empty_launch.facade_projection(facade_limits)?;
    ensure!(
        facade_a.tools().len() == 1 && facade_a.contains_tool(callable_a.as_str()),
        "Codex A projection must expose exactly A"
    );
    ensure!(
        facade_b.tools().len() == 1 && facade_b.contains_tool(callable_b.as_str()),
        "Codex B projection must expose exactly B"
    );
    let concurrent_projections_isolated = !facade_a.contains_tool(callable_b.as_str())
        && !facade_b.contains_tool(callable_a.as_str());
    ensure!(
        concurrent_projections_isolated,
        "parallel Codex projections must remain disjoint"
    );
    let empty_projection_is_empty = empty_facade.tools().is_empty();
    ensure!(empty_projection_is_empty, "empty projection exposed a tool");

    let temporary = tempfile::tempdir().context("create Codex conformance root")?;
    let bootstrap_path = temporary.path().join("bootstrap.json");
    let managed_a =
        build_codex_managed_mcp_config(&launch_a.preflight, Some(&bootstrap_path), 512)?;
    let managed_empty = build_codex_managed_mcp_config(&empty_launch.preflight, None, 512)?;
    let exact_managed_config_isolated = managed_a.enabled_tools == [callable_a.as_str()]
        && managed_a.config_toml.contains("[mcp_servers.pioneer]")
        && managed_a.config_toml.contains("required = true")
        && managed_a
            .config_toml
            .contains("approval_mode = \"approve\"")
        && !managed_a.config_toml.contains(callable_b.as_str())
        && !managed_a.config_toml.contains("malicious_sentinel")
        && managed_empty.enabled_tools.is_empty()
        && !managed_empty.config_toml.contains("mcp_servers")
        && !managed_empty.config_toml.contains("__cli-mcp-stdio");
    ensure!(
        exact_managed_config_isolated,
        "Codex managed config was not exact pioneer-only/strict-empty"
    );

    let spec_a = CliSessionLaunchSpec::codex(
        CLIAgentRuntimeSessionStartOptions::default(),
        CliMcpSessionLaunch::Codex(launch_a.clone()),
        Some(NATIVE_THREAD_ID.to_owned()),
    );
    let spec_a_next = CliSessionLaunchSpec::codex(
        CLIAgentRuntimeSessionStartOptions::default(),
        CliMcpSessionLaunch::Codex(launch_a_next),
        Some(NATIVE_THREAD_ID.to_owned()),
    );
    let spec_b = CliSessionLaunchSpec::codex(
        CLIAgentRuntimeSessionStartOptions::default(),
        CliMcpSessionLaunch::Codex(launch_b.clone()),
        Some(NATIVE_THREAD_ID.to_owned()),
    );
    let same_projection_reused = !requires_restart(&spec_a, &spec_a_next);
    let changed_projection_requires_restart = requires_restart(&spec_a, &spec_b);
    let native_thread_identity_preserved = matches!(
        &spec_b.continuation,
        CliProviderContinuation::CodexRpcThread {
            native_thread_id: Some(native_thread_id)
        } if native_thread_id == NATIVE_THREAD_ID
    );
    ensure!(
        same_projection_reused,
        "turn-local identity triggered a restart"
    );
    ensure!(
        changed_projection_requires_restart,
        "A to B projection change did not trigger a restart"
    );
    ensure!(
        native_thread_identity_preserved,
        "restart lost the native thread continuation identity"
    );

    let mut exact_event = native_item_event(callable_a.as_str());
    let exact_binding = launch_a
        .enrich_native_event("codex", 7, &mut exact_event)?
        .context("managed Codex item did not produce a binding")?;
    let exact_native_item_bound = exact_binding.canonical_callable_name == callable_a
        && exact_binding.native_thread_id == NATIVE_THREAD_ID
        && exact_binding.native_turn_id == "native-turn-a"
        && exact_binding.native_item_id == "native-item-a";
    ensure!(
        exact_native_item_bound,
        "managed Codex item binding drifted"
    );

    let mut unselected_event = native_item_event(callable_b.as_str());
    let unselected_native_item_rejected = matches!(
        launch_a.enrich_native_event("codex", 7, &mut unselected_event),
        Err(CodexNativeMcpEventError::ToolOutsideProjection)
    );
    ensure!(
        unselected_native_item_rejected,
        "Codex accepted an item outside the frozen projection"
    );

    let requested_permissions = json!({"network": ["example.com"]});
    let exact_approval_fallback_allowed =
        codex_native_approval_fallback_response(requested_permissions.clone(), true)
            == Some(json!({
                "permissions": requested_permissions,
                "scope": "turn",
                "strictAutoReview": false,
            }));
    let stale_approval_fallback_denied =
        codex_native_approval_fallback_response(json!({}), false).is_none();
    ensure!(
        exact_approval_fallback_allowed && stale_approval_fallback_denied,
        "Codex exact/stale native approval fallback drifted"
    );

    let (
        recorded_started_items,
        recorded_progress_items,
        recorded_completed_items,
        recorded_permission_requests,
        native_timeline_deduplicated,
    ) = recorded_lifecycle_counts()?;
    ensure!(
        (
            recorded_started_items,
            recorded_progress_items,
            recorded_completed_items
        ) == (1, 1, 1),
        "recorded Codex lifecycle is not one start/progress/completion sequence"
    );
    ensure!(
        recorded_permission_requests == 1,
        "recorded Codex permission callback fixture drifted"
    );
    ensure!(
        native_timeline_deduplicated,
        "recorded Codex lifecycle does not bind to one native item"
    );

    let bridge = run_cli_mcp_bridge_conformance("codex").await?;
    let bridge_cleanup_complete = bridge.grant_revoked_on_eof && bridge.artifacts_removed;

    Ok(CodexMcpDeterministicEvidence {
        callable_a,
        callable_b,
        manifest_a: projection_a.manifest_hash,
        manifest_b: projection_b.manifest_hash,
        same_projection_reused,
        changed_projection_requires_restart,
        native_thread_identity_preserved,
        concurrent_projections_isolated: concurrent_projections_isolated
            && bridge.concurrent_isolation_observed,
        empty_projection_is_empty,
        exact_managed_config_isolated,
        exact_approval_fallback_allowed,
        stale_approval_fallback_denied,
        exact_native_item_bound,
        unselected_native_item_rejected,
        native_timeline_deduplicated,
        initial_turn_blocked_until_exact_list: bridge.turn_blocked_before_list
            && bridge.bootstrap_consumed_before_list
            && bridge.exact_list_observed,
        helper_attached: bridge.helper_attached,
        bridge_call_succeeded: bridge.successful_call_observed,
        bridge_cancellation_propagated: bridge.cancellation_propagated,
        bridge_cleanup_complete,
        secret_canary_absent: bridge.secret_canary_absent
            && !managed_a
                .config_toml
                .contains("pioneer-bridge-secret-canary-53"),
        recorded_started_items,
        recorded_progress_items,
        recorded_completed_items,
        recorded_permission_requests,
    })
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
        server_name: "fixture".to_owned(),
        raw_tool_name: raw_tool_name.to_owned(),
        description: Some("Deterministic Codex MCP fixture tool".to_owned()),
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
        catalog_version: "codex-fixture-v1".to_owned(),
        installation_fingerprint: sha256(raw_tool_name),
        schema_fingerprint: String::new(),
        runtime_generation: 1,
        selection_reason: McpSelectionReason::ExplicitTool,
        capability_id: Some(format!("capability-{raw_tool_name}")),
    });
    projection
        .finalize_identity(McpProjectionLimits::default())
        .map_err(|error| anyhow::anyhow!("failed to finalize Codex fixture projection: {error}"))?;
    Ok(projection)
}

fn empty_projection(turn_id: &str) -> Result<ResolvedMcpTurnProjection> {
    let mut projection = ResolvedMcpTurnProjection::empty(WORKSPACE_ID, turn_id);
    projection
        .finalize_identity(McpProjectionLimits::default())
        .map_err(|error| anyhow::anyhow!("failed to finalize empty Codex projection: {error}"))?;
    Ok(projection)
}

fn native_item_event(callable_name: &str) -> RuntimeEvent {
    RuntimeEvent::ItemStarted(RuntimeItemStarted {
        native_thread_id: Some(NATIVE_THREAD_ID.to_owned()),
        native_turn_id: "native-turn-a".to_owned(),
        native_item_id: "native-item-a".to_owned(),
        item_kind: "mcpToolCall".to_owned(),
        title: None,
        phase: Default::default(),
        metadata: Some(json!({
            "server": "pioneer",
            "tool": callable_name,
            "arguments": {"value": "A"},
            "status": "inProgress"
        })),
        native_item_redacted: None,
        native: None,
    })
}

fn recorded_lifecycle_counts() -> Result<(usize, usize, usize, usize, bool)> {
    let fixture: JsonValue = serde_json::from_str(include_str!(
        "../../cli-agent-runtime/tests/fixtures/codex_mcp_lifecycle_0_144_1.json"
    ))
    .context("decode recorded Codex lifecycle fixture")?;
    let notifications = fixture["notifications"]
        .as_array()
        .context("Codex lifecycle fixture omitted notifications")?;
    let mut started = 0;
    let mut progress = 0;
    let mut completed = 0;
    let mut native_item_ids = std::collections::HashSet::new();
    for value in notifications {
        let method = value["method"]
            .as_str()
            .context("Codex fixture notification omitted method")?;
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: method.to_owned(),
                params: value.get("params").cloned(),
                raw: value.clone(),
            },
            RuntimeEventMappingOptions::default(),
        );
        match event {
            RuntimeEvent::ItemStarted(_) => started += 1,
            RuntimeEvent::ItemDelta(_) => progress += 1,
            RuntimeEvent::ItemCompleted(_) => completed += 1,
            _ => anyhow::bail!("recorded Codex MCP notification mapped to an unexpected event"),
        }
        if let Some(item_id) = value["params"]["item"]["id"]
            .as_str()
            .or_else(|| value["params"]["itemId"].as_str())
        {
            native_item_ids.insert(item_id.to_owned());
        }
    }
    let permission_requests = fixture["requests"]
        .as_array()
        .context("Codex lifecycle fixture omitted requests")?
        .iter()
        .filter(|request| request["method"].as_str() == Some("item/permissions/requestApproval"))
        .count();
    for request in fixture["requests"]
        .as_array()
        .context("Codex lifecycle fixture omitted requests")?
    {
        if let Some(item_id) = request["params"]["itemId"].as_str() {
            native_item_ids.insert(item_id.to_owned());
        }
    }
    Ok((
        started,
        progress,
        completed,
        permission_requests,
        native_item_ids.len() == 1,
    ))
}

fn sha256(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}
