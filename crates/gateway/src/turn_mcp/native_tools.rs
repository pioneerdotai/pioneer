//! Internal definitions shared by CLI projection, validation, quota and dispatch.
//! Registration is not native model visibility: request_tools owns that policy.
use crate::authorization::ResourceAction;
use crate::turn_mcp::invoker::{
    CurrentMcpToolIdentity, TurnMcpInvocationError, TurnMcpInvocationErrorCode,
};
use crate::turn_mcp::projection::{
    McpProjectionLimits, ResolvedMcpTurnTool, canonical_schema_identity, sha256_hex,
};
use pioneer_crud::TurnMcpBindingRecord;
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolSpec, dynamic_unknown_output_policy,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeToolFamily {
    Html,
    Tasks,
    Threads,
}

impl NativeToolFamily {
    pub(crate) fn from_server_id(id: &str) -> Option<Self> {
        match id {
            pioneer_tools::html::HTML_SERVER_ID => Some(Self::Html),
            "pioneer-task-tools-v1" => Some(Self::Tasks),
            "pioneer-thread-tools-v1" => Some(Self::Threads),
            _ => None,
        }
    }

    pub(crate) fn server_id(self) -> &'static str {
        match self {
            Self::Html => pioneer_tools::html::HTML_SERVER_ID,
            Self::Tasks => "pioneer-task-tools-v1",
            Self::Threads => "pioneer-thread-tools-v1",
        }
    }

    pub(crate) fn server_name(self) -> &'static str {
        match self {
            Self::Html => pioneer_tools::html::HTML_SERVER_NAME,
            Self::Tasks => "pioneer_tasks",
            Self::Threads => "pioneer_threads",
        }
    }

    pub(crate) fn definitions(self) -> Vec<ConfiguredToolSpec> {
        self.catalog().to_vec()
    }

    fn catalog(self) -> &'static [ConfiguredToolSpec] {
        static HTML: std::sync::OnceLock<Vec<ConfiguredToolSpec>> = std::sync::OnceLock::new();
        static THREADS: std::sync::OnceLock<Vec<ConfiguredToolSpec>> = std::sync::OnceLock::new();
        static TASKS: std::sync::OnceLock<Vec<ConfiguredToolSpec>> = std::sync::OnceLock::new();
        match self {
            Self::Html => HTML.get_or_init(pioneer_tools::html::html_tool_specs),
            Self::Threads => THREADS.get_or_init(|| {
                let mut specs = crate::message::thread_read_tools::thread_read_tool_specs();
                specs.extend(crate::message::thread_search_tools::thread_search_tool_specs());
                specs.extend(crate::message::thread_tool_results::thread_result_tool_specs());
                let capabilities = [
                    pioneer_protocol::AgentToolCapability::ChildStart,
                    pioneer_protocol::AgentToolCapability::ThreadCreate,
                    pioneer_protocol::AgentToolCapability::MessageCreate,
                ]
                .into_iter()
                .collect();
                // Share the protocol definitions without embedding execution
                // facts in the fixed catalog.
                let entry = pioneer_protocol::agent_identity_models_catalog_entry();
                let mut entries = vec![entry];
                entries.extend(pioneer_protocol::project_agent_model_tool_catalog(
                    &capabilities,
                    None,
                ));
                entries.push(pioneer_protocol::agent_turn_start_catalog_entry());
                specs.extend(entries.into_iter().map(|entry| {
                    ConfiguredToolSpec::new(
                        ToolSpec::new(
                            entry.name.as_str(),
                            entry.description,
                            entry.parameters,
                            PayloadKind::Function,
                        ),
                        ExecutionClass::Shared,
                        dynamic_unknown_output_policy(),
                    )
                }));
                specs
            }),
            Self::Tasks => TASKS.get_or_init(|| {
                let mut specs = crate::task_tools::task_tool_specs();
                let options = pioneer_protocol::agent_start_options_catalog_entry();
                specs.push(ConfiguredToolSpec::new(
                    ToolSpec::new(
                        options.name.as_str(),
                        options.description,
                        options.parameters,
                        PayloadKind::Function,
                    ),
                    ExecutionClass::Shared,
                    dynamic_unknown_output_policy(),
                ));
                specs
            }),
        }
    }

    pub(crate) fn action(self, name: &str) -> Result<ResourceAction, pioneer_tools::ToolError> {
        match self {
            Self::Html => Ok(ResourceAction::McpUse),
            Self::Threads
                if self.is_thread_reader(name) || name == "threads_identity_models_list" =>
            {
                Ok(ResourceAction::ThreadRead)
            }
            Self::Threads => match name {
                "thread_message_send" => Ok(ResourceAction::MessageCreate),
                "thread_create" => Ok(ResourceAction::ThreadCreateWorkspace),
                "threads_turn_start" => Ok(ResourceAction::AgentTurnStart),
                _ => Err(pioneer_tools::ToolError::NotFound(
                    "unknown threads tool".to_owned(),
                )),
            },
            // Reading the already capability-filtered launch catalog is not a
            // child-start mutation. ChildStart belongs to the Agent action
            // envelope, not the Turn execution's resource-action grant.
            Self::Tasks if name == "threads_start_options" => Ok(ResourceAction::ThreadRead),
            Self::Tasks => crate::task_tools::task_tool_resource_action(name),
        }
    }

    pub(crate) fn is_thread_reader(self, name: &str) -> bool {
        self == Self::Threads
            && (crate::message::thread_read_tools::THREAD_READ_TOOL_NAMES.contains(&name)
                || name == "threads_tools_result_read"
                || name == "threads_search")
    }

    pub(crate) fn timeout_ms(self, name: &str) -> u64 {
        // Preserve the native specification's wall-clock policy. Zero means
        // no extra transport deadline; Task Service owns DurableWait activity.
        self.catalog()
            .iter()
            .find(|spec| spec.spec.name == name)
            .and_then(|spec| spec.spec.recovery.max_wall_clock_secs)
            .unwrap_or(0)
            .saturating_mul(1_000)
    }
}

pub(crate) fn exact_definition(tool: &ResolvedMcpTurnTool) -> Option<NativeToolFamily> {
    let family = NativeToolFamily::from_server_id(&tool.server_installation_id)?;
    (tool.server_name == family.server_name()
        && tool.catalog_version == family.server_id()
        && tool.installation_fingerprint == family.server_id()
        && tool.runtime_generation == 1
        && tool.timeout_ms == family.timeout_ms(&tool.raw_tool_name)
        && tool.annotations.is_none()
        && family.catalog().iter().any(|spec| {
            spec.spec.name == tool.raw_tool_name
                && spec.spec.parameters == tool.input_schema
                && tool.description.as_deref() == Some(spec.spec.description.as_str())
        }))
    .then_some(family)
}

pub(crate) fn requires_extended_provider_deadline(tool: &ResolvedMcpTurnTool) -> bool {
    exact_definition(tool).is_some() && tool.timeout_ms == 0
}

pub(crate) fn is_task_operation(tool: &ResolvedMcpTurnTool) -> bool {
    exact_definition(tool) == Some(NativeToolFamily::Tasks)
        && tool.raw_tool_name != "threads_start_options"
}

pub(crate) fn projection_allowance(
    tools: &[ResolvedMcpTurnTool],
    mut limits: McpProjectionLimits,
) -> McpProjectionLimits {
    for tool in tools.iter().filter(|tool| exact_definition(tool).is_some()) {
        limits.max_tools += 1;
        if let Ok((_, _, bytes)) = canonical_schema_identity(&tool.input_schema) {
            limits.max_total_schema_bytes += bytes.len();
        }
    }
    limits
}

pub(crate) fn current_identity(
    workspace_id: &str,
    binding: &TurnMcpBindingRecord,
) -> Result<CurrentMcpToolIdentity, TurnMcpInvocationError> {
    let drift = || {
        TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::ToolDrift,
            "native binding differs from the canonical catalog",
        )
    };
    let family =
        NativeToolFamily::from_server_id(&binding.server_installation_id).ok_or_else(drift)?;
    let spec = family
        .catalog()
        .iter()
        .find(|spec| spec.spec.name == binding.raw_tool_name)
        .ok_or_else(drift)?
        .spec
        .clone();
    let (schema, fingerprint, _) =
        canonical_schema_identity(&spec.parameters).map_err(|_| drift())?;
    if workspace_id.is_empty()
        || binding.server_name != family.server_name()
        || binding.catalog_version != family.server_id()
        || binding.fingerprint != family.server_id()
        || binding.canonical_callable_name != binding.raw_tool_name
        || binding.canonical_schema_fingerprint != fingerprint
        || binding.annotations_json != "{}"
        || binding.annotations_digest != sha256_hex(b"{}")
        || binding.runtime_generation != 1
        || binding.effective_timeout_ms != family.timeout_ms(&binding.raw_tool_name) as i64
    {
        return Err(drift());
    }
    Ok(CurrentMcpToolIdentity {
        server_installation_id: family.server_id().into(),
        server_name: family.server_name().into(),
        raw_tool_name: spec.name,
        description: Some(spec.description),
        catalog_version: family.server_id().into(),
        installation_fingerprint: family.server_id().into(),
        canonical_schema_fingerprint: fingerprint,
        canonical_schema: schema,
        annotations_json: "{}".into(),
        annotations_digest: sha256_hex(b"{}"),
        effective_timeout_ms: family.timeout_ms(&binding.raw_tool_name),
        runtime_generation: 1,
    })
}

/// A logical CLI call is stable across reconnect, with explicit mutation keys
/// also surviving a changed provider call ID. History keeps the native item ID.
pub(crate) fn cli_task_call_id(
    turn_id: &str,
    tool: &str,
    native_item_id: &str,
    arguments: &serde_json::Value,
) -> String {
    let key = arguments
        .get("idempotencyKey")
        .or_else(|| arguments.get("idempotency_key"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|key| !key.is_empty());
    let identity = match key {
        Some(key) => format!("mutation:{key}"),
        None => format!("native:{native_item_id}"),
    };
    format!(
        "pioneer-cli:{turn_id}:{tool}:{}",
        sha256_hex(identity.as_bytes())
    )
}

pub(crate) async fn materialize_task_tools(
    processor: std::sync::Arc<crate::message::MessageProcessor>,
    context: pioneer_agent::TurnToolContext,
) -> Result<Vec<pioneer_tools::ToolExtensionBundle>, String> {
    use pioneer_agent::TaskToolProvider;
    let mut actions =
        crate::message::agent_action_tools::materialize(processor.clone(), context.clone()).await?;
    for bundle in &mut actions.bundles {
        bundle.specs.retain(|spec| {
            pioneer_tools::THREADS_DOMAIN_TOOL_NAMES.contains(&spec.spec.name.as_str())
        });
        bundle
            .handlers
            .retain(|(name, _)| pioneer_tools::THREADS_DOMAIN_TOOL_NAMES.contains(&name.as_str()));
    }
    let provider =
        crate::task_tools::GatewayTaskToolProvider::for_cli(std::sync::Arc::downgrade(&processor));
    let tasks = provider
        .materialize_task_tools(pioneer_agent::TaskTurnContext {
            workspace_id: context.workspace_id,
            thread_id: context.thread_id,
            turn_id: context.turn_id,
        })
        .await?;
    actions.bundles.extend(tasks.bundles);
    Ok(actions.bundles)
}

pub(crate) fn materialize_thread_tools(
    processor: std::sync::Arc<crate::message::MessageProcessor>,
    context: pioneer_agent::TurnToolContext,
) -> Vec<pioneer_tools::ToolExtensionBundle> {
    vec![
        processor.thread_read_tools(context.clone()),
        processor.thread_search_tools(context.clone()),
        processor.thread_result_tools(context),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_mcp::{McpSelectionReason, ResolvedMcpTurnProjection};
    use serde_json::json;

    #[test]
    fn search_is_a_thread_reader_with_a_bounded_page_and_unbounded_query_schema() {
        let spec = NativeToolFamily::Threads
            .definitions()
            .into_iter()
            .find(|spec| spec.spec.name == "threads_search")
            .unwrap();
        assert_eq!(
            NativeToolFamily::Threads.action("threads_search").unwrap(),
            ResourceAction::ThreadRead
        );
        let properties = &spec.spec.parameters["properties"];
        assert_eq!(properties["limit"]["maximum"], 100);
        assert!(properties["query"].get("maxLength").is_none());
    }

    #[test]
    fn every_provider_preserves_native_catalog_with_full_external_allowance() {
        for external in [8, 128, 512] {
            let limits = McpProjectionLimits {
                max_tools: external,
                ..Default::default()
            };
            let mut projection = ResolvedMcpTurnProjection::empty("workspace", "turn");
            for index in 0..external {
                projection.tools.push(ResolvedMcpTurnTool {
                    canonical_callable_name: String::new(),
                    workspace_id: "workspace".into(),
                    server_installation_id: "external".into(),
                    server_name: "upstream".into(),
                    raw_tool_name: format!("external_{index}"),
                    description: None,
                    input_schema: json!({"type":"object"}),
                    annotations: None,
                    timeout_ms: 20_000,
                    catalog_version: "v1".into(),
                    installation_fingerprint: "external".into(),
                    schema_fingerprint: String::new(),
                    runtime_generation: 1,
                    selection_reason: McpSelectionReason::ImplicitPolicy,
                    capability_id: None,
                });
            }
            let definitions = [
                NativeToolFamily::Html,
                NativeToolFamily::Tasks,
                NativeToolFamily::Threads,
            ]
            .into_iter()
            .flat_map(|family| {
                family
                    .definitions()
                    .into_iter()
                    .map(move |spec| (family, spec))
            })
            .collect::<Vec<_>>();
            let native_count = definitions.len();
            let projection = projection.with_native_tools(definitions, limits).unwrap();
            assert_eq!(projection.native_tool_count(), native_count);
            let budget =
                crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits::transport_bounded(
                    external,
                );
            let codex = crate::cli_runtime::codex_mcp::build_codex_mcp_session_launch_projection(
                projection.clone(),
                "a".repeat(64),
            )
            .unwrap();
            assert_eq!(
                codex.preflight.tool_timeout_secs,
                Some(crate::cli_runtime::mcp::limits::MANAGED_DURABLE_TOOL_TIMEOUT_SECS)
            );
            let claude =
                crate::cli_runtime::claude_mcp::build_claude_mcp_session_launch_projection(
                    projection.clone(),
                    "a".repeat(64),
                )
                .unwrap();
            let opencode =
                crate::cli_runtime::opencode_mcp::build_opencode_mcp_session_launch_projection(
                    projection.clone(),
                    "a".repeat(64),
                )
                .unwrap();
            assert_eq!(
                codex.facade_projection(budget).unwrap().tools().len(),
                external + native_count
            );
            assert_eq!(
                claude.facade_projection(budget).unwrap().tools().len(),
                external + native_count + 2
            );
            assert_eq!(
                opencode.facade_projection(budget).unwrap().tools().len(),
                external + native_count
            );
            for family in [
                NativeToolFamily::Html,
                NativeToolFamily::Tasks,
                NativeToolFamily::Threads,
            ] {
                for definition in family.definitions() {
                    let actual = projection
                        .tools
                        .iter()
                        .find(|tool| tool.canonical_callable_name == definition.spec.name)
                        .unwrap();
                    assert_eq!(actual.input_schema, definition.spec.parameters);
                }
            }
            let smaller =
                crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits::transport_bounded(
                    external - 1,
                );
            assert!(codex.facade_projection(smaller).is_err());
            assert!(claude.facade_projection(smaller).is_err());
            assert!(opencode.facade_projection(smaller).is_err());
            let mut forged = projection.clone();
            let wait = forged
                .tools
                .iter_mut()
                .find(|tool| tool.raw_tool_name == "task_wait")
                .unwrap();
            wait.server_installation_id = "external-task-wait".into();
            assert!(
                !requires_extended_provider_deadline(wait),
                "a matching external name cannot select native deadline policy"
            );
            assert_eq!(
                forged.native_tool_count(),
                native_count - 1,
                "a matching name/schema cannot borrow internal allowance"
            );
        }
    }

    #[test]
    fn canonical_call_identity_survives_response_loss_without_cross_turn_collisions() {
        let first = cli_task_call_id("turn-a", "task_create", "native-a", &json!({}));
        assert_eq!(
            first,
            cli_task_call_id("turn-a", "task_create", "native-a", &json!({}))
        );
        assert_ne!(
            first,
            cli_task_call_id("turn-b", "task_create", "native-a", &json!({}))
        );
        let keyed = json!({"idempotencyKey":"durable-operation"});
        assert_eq!(
            cli_task_call_id("turn-a", "task_create", "native-a", &keyed),
            cli_task_call_id("turn-a", "task_create", "native-b", &keyed)
        );
        assert_ne!(
            cli_task_call_id("turn-a", "task_create", "native-a", &keyed),
            cli_task_call_id("turn-a", "task_cancel", "native-a", &keyed)
        );
    }
}
