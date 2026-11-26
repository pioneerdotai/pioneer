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
use pioneer_protocol::{AgentModelToolCatalogEntry, AgentModelToolName};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, PayloadKind, TASK_IDENTITY_MODELS_LIST_TOOL,
    TASK_START_OPTIONS_TOOL, ToolSpec, dynamic_unknown_output_policy, task_launch_catalog_alias,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeToolFamily {
    Html,
    ComputerUse,
    Tasks,
    Threads,
}

impl NativeToolFamily {
    pub(crate) fn from_server_id(id: &str) -> Option<Self> {
        match id {
            pioneer_tools::html::HTML_SERVER_ID => Some(Self::Html),
            #[cfg(feature = "computer-use")]
            "pioneer-cua-688c5fd62a9f-v1" => Some(Self::ComputerUse),
            "pioneer-task-tools-v1" => Some(Self::Tasks),
            "pioneer-thread-tools-v1" => Some(Self::Threads),
            _ => None,
        }
    }

    pub(crate) fn server_id(self) -> &'static str {
        match self {
            Self::Html => pioneer_tools::html::HTML_SERVER_ID,
            Self::ComputerUse => "pioneer-cua-688c5fd62a9f-v1",
            Self::Tasks => "pioneer-task-tools-v1",
            Self::Threads => "pioneer-thread-tools-v1",
        }
    }

    pub(crate) fn server_name(self) -> &'static str {
        match self {
            Self::Html => pioneer_tools::html::HTML_SERVER_NAME,
            Self::ComputerUse => "pioneer_cua",
            Self::Tasks => "pioneer_tasks",
            Self::Threads => "pioneer_threads",
        }
    }

    pub(crate) fn definitions(self) -> Vec<ConfiguredToolSpec> {
        self.catalog().to_vec()
    }

    fn catalog(self) -> &'static [ConfiguredToolSpec] {
        static HTML: std::sync::OnceLock<Vec<ConfiguredToolSpec>> = std::sync::OnceLock::new();
        #[cfg(feature = "computer-use")]
        static CUA: std::sync::OnceLock<Vec<ConfiguredToolSpec>> = std::sync::OnceLock::new();
        static THREADS: std::sync::OnceLock<Vec<ConfiguredToolSpec>> = std::sync::OnceLock::new();
        static TASKS: std::sync::OnceLock<Vec<ConfiguredToolSpec>> = std::sync::OnceLock::new();
        match self {
            Self::Html => HTML.get_or_init(pioneer_tools::html::html_tool_specs),
            #[cfg(feature = "computer-use")]
            Self::ComputerUse => CUA.get_or_init(pioneer_tools::computer_use_tool_specs),
            #[cfg(not(feature = "computer-use"))]
            Self::ComputerUse => &[],
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
                let mut entries = vec![
                    pioneer_protocol::agent_start_options_catalog_entry(),
                    pioneer_protocol::agent_identity_models_catalog_entry(),
                ];
                entries.extend(pioneer_protocol::project_agent_model_tool_catalog(
                    &capabilities,
                    None,
                ));
                entries.push(pioneer_protocol::agent_turn_start_catalog_entry());
                specs.extend(entries.into_iter().map(|entry| {
                    let name = entry.name.as_str();
                    agent_action_spec(name, entry)
                }));
                specs
            }),
            Self::Tasks => TASKS.get_or_init(|| {
                let mut specs = crate::task_tools::task_tool_specs();
                specs.extend(
                    [
                        (
                            TASK_START_OPTIONS_TOOL,
                            pioneer_protocol::agent_start_options_catalog_entry(),
                        ),
                        (
                            TASK_IDENTITY_MODELS_LIST_TOOL,
                            pioneer_protocol::agent_identity_models_catalog_entry(),
                        ),
                    ]
                    .into_iter()
                    .map(|(alias, entry)| agent_action_spec(alias, entry)),
                );
                specs
            }),
        }
    }

    // Creation has two existing workspace-scoped variants. Catalog projection
    // accepts either; the handler rechecks the exact server-issued option.
    pub(crate) fn actions(
        self,
        name: &str,
    ) -> Result<Vec<ResourceAction>, pioneer_tools::ToolError> {
        if self == Self::Threads && name == "thread_create" {
            Ok(vec![
                ResourceAction::ThreadCreatePrivate,
                ResourceAction::ThreadCreateWorkspace,
            ])
        } else {
            self.action(name).map(|action| vec![action])
        }
    }

    pub(crate) fn action(self, name: &str) -> Result<ResourceAction, pioneer_tools::ToolError> {
        match self {
            Self::Html => Ok(ResourceAction::McpUse),
            Self::ComputerUse if pioneer_tools::is_computer_use_tool(name) => {
                Ok(ResourceAction::McpUse)
            }
            Self::ComputerUse => Err(pioneer_tools::ToolError::NotFound(
                "unknown computer-use operation".into(),
            )),
            // ChildStart filters the Agent catalog. Reading the catalog uses
            // the Turn execution's ThreadRead grant.
            _ if self.is_launch_catalog(name) || self.is_thread_reader(name) => {
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
            Self::Tasks => crate::task_tools::task_tool_resource_action(name),
        }
    }

    pub(crate) fn is_thread_reader(self, name: &str) -> bool {
        self == Self::Threads
            && (crate::message::thread_read_tools::THREAD_READ_TOOL_NAMES.contains(&name)
                || name == "threads_tools_result_read"
                || name == "threads_search")
    }

    pub(crate) fn is_launch_catalog(self, name: &str) -> bool {
        match self {
            Self::Threads => task_launch_catalog_alias(name).is_some(),
            Self::Tasks => {
                matches!(
                    name,
                    TASK_START_OPTIONS_TOOL | TASK_IDENTITY_MODELS_LIST_TOOL
                ) || name == AgentModelToolName::AgentStartOptions.as_str()
            }
            _ => false,
        }
    }

    fn legacy_definitions(self, name: &str) -> impl Iterator<Item = &'static ToolSpec> {
        static TASK_OPTIONS: std::sync::OnceLock<ToolSpec> = std::sync::OnceLock::new();
        static TASK_CREATE: std::sync::OnceLock<[ToolSpec; 2]> = std::sync::OnceLock::new();
        static TASK_UPDATE: std::sync::OnceLock<ToolSpec> = std::sync::OnceLock::new();
        static TASK_RESULT: std::sync::OnceLock<ToolSpec> = std::sync::OnceLock::new();
        let current_spec = || {
            self.catalog()
                .iter()
                .find(|spec| spec.spec.name == name)
                .expect("current Task tool definition")
                .spec
                .clone()
        };
        let previous_budget_spec = || {
            let mut spec = current_spec();
            crate::task_tools::restore_pre_revision_budget_catalog(&mut spec);
            spec
        };
        let definitions = match (self, name) {
            (Self::Html, pioneer_tools::html::HTML_PREVIEW) => {
                [Some(legacy_html_preview_spec()), None]
            }
            (Self::Tasks, "threads_start_options") => [
                Some(TASK_OPTIONS.get_or_init(|| {
                    let entry = pioneer_protocol::agent_start_options_catalog_entry();
                    agent_action_spec(entry.name.as_str(), entry).spec
                })),
                None,
            ],
            (Self::Tasks, "task_create") => {
                let [previous, old_launch] = TASK_CREATE.get_or_init(|| {
                    let previous = previous_budget_spec();
                    let mut old_launch = previous.clone();
                    crate::task_tools::restore_legacy_task_create_catalog(&mut old_launch);
                    [previous, old_launch]
                });
                [Some(previous), Some(old_launch)]
            }
            (Self::Tasks, "task_update") => {
                [Some(TASK_UPDATE.get_or_init(previous_budget_spec)), None]
            }
            (Self::Tasks, "task_result") => [
                Some(TASK_RESULT.get_or_init(|| {
                    let mut spec = current_spec();
                    crate::task_tools::restore_legacy_task_result_catalog(&mut spec);
                    spec
                })),
                None,
            ],
            _ => [None, None],
        };
        definitions.into_iter().flatten()
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

fn agent_action_spec(name: &str, entry: AgentModelToolCatalogEntry) -> ConfiguredToolSpec {
    ConfiguredToolSpec::new(
        ToolSpec::new(
            name,
            entry.description,
            entry.parameters,
            PayloadKind::Function,
        ),
        ExecutionClass::Shared,
        dynamic_unknown_output_policy(),
    )
}

// A frozen binding identifies the immutable schema, not the latest catalog.
// Keep its description too: exact_definition controls the unprefixed callable.
fn legacy_html_preview_spec() -> &'static ToolSpec {
    static LEGACY: std::sync::OnceLock<ToolSpec> = std::sync::OnceLock::new();
    LEGACY.get_or_init(|| {
        let definition = pioneer_tools::html::legacy_preview_definition();
        ToolSpec::new(
            pioneer_tools::html::HTML_PREVIEW,
            definition["description"]
                .as_str()
                .expect("recorded description"),
            definition["inputSchema"].clone(),
            PayloadKind::Function,
        )
    })
}

pub(crate) fn exact_definition(tool: &ResolvedMcpTurnTool) -> Option<NativeToolFamily> {
    let family = NativeToolFamily::from_server_id(&tool.server_installation_id)?;
    (tool.server_name == family.server_name()
        && tool.catalog_version == family.server_id()
        && tool.installation_fingerprint == family.server_id()
        && tool.runtime_generation == 1
        && tool.timeout_ms == family.timeout_ms(&tool.raw_tool_name)
        && tool.annotations.is_none()
        && family
            .catalog()
            .iter()
            .map(|spec| &spec.spec)
            .chain(family.legacy_definitions(&tool.raw_tool_name))
            .any(|spec| {
                spec.name == tool.raw_tool_name
                    && spec.parameters == tool.input_schema
                    && tool.description.as_deref() == Some(spec.description.as_str())
            }))
    .then_some(family)
}

pub(crate) fn requires_extended_provider_deadline(tool: &ResolvedMcpTurnTool) -> bool {
    exact_definition(tool).is_some() && tool.timeout_ms == 0
}

pub(crate) fn is_task_operation(tool: &ResolvedMcpTurnTool) -> bool {
    exact_definition(tool) == Some(NativeToolFamily::Tasks)
        && !NativeToolFamily::Tasks.is_launch_catalog(&tool.raw_tool_name)
}

/// The options tool alone is not evidence of available Threads operations.
pub(crate) fn is_threads_operation(tool: &ResolvedMcpTurnTool) -> bool {
    exact_definition(tool) == Some(NativeToolFamily::Threads)
        && tool.raw_tool_name != "threads_start_options"
        && pioneer_tools::THREADS_DOMAIN_TOOL_NAMES.contains(&tool.raw_tool_name.as_str())
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
    let original = family
        .catalog()
        .iter()
        .find(|spec| spec.spec.name == binding.raw_tool_name)
        .map(|spec| &spec.spec);
    let (spec, schema, fingerprint) = original
        .into_iter()
        .chain(family.legacy_definitions(&binding.raw_tool_name))
        .find_map(|spec| {
            let (schema, fingerprint, _) = canonical_schema_identity(&spec.parameters).ok()?;
            (binding.canonical_schema_fingerprint == fingerprint).then_some((
                spec,
                schema,
                fingerprint,
            ))
        })
        .ok_or_else(drift)?;
    if workspace_id.is_empty()
        || binding.server_name != family.server_name()
        || binding.catalog_version != family.server_id()
        || binding.fingerprint != family.server_id()
        || binding.canonical_callable_name != binding.raw_tool_name
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
        raw_tool_name: spec.name.clone(),
        description: Some(spec.description.clone()),
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
    materialize_task_tools_internal(processor, context, false)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn materialize_task_tools_with_proof(
    processor: std::sync::Arc<crate::message::MessageProcessor>,
    context: pioneer_agent::TurnToolContext,
) -> anyhow::Result<Vec<pioneer_tools::ToolExtensionBundle>> {
    materialize_task_tools_internal(processor, context, true).await
}

async fn materialize_task_tools_internal(
    processor: std::sync::Arc<crate::message::MessageProcessor>,
    context: pioneer_agent::TurnToolContext,
    retain_proof_errors: bool,
) -> anyhow::Result<Vec<pioneer_tools::ToolExtensionBundle>> {
    use pioneer_agent::TaskToolProvider;
    let mut actions = if retain_proof_errors {
        crate::message::agent_action_tools::materialize_with_proof(
            processor.clone(),
            context.clone(),
        )
        .await?
    } else {
        crate::message::agent_action_tools::materialize(processor.clone(), context.clone())
            .await
            .map_err(anyhow::Error::msg)?
    };
    for bundle in &mut actions.bundles {
        bundle.specs.retain(|spec| {
            pioneer_tools::THREADS_DOMAIN_TOOL_NAMES.contains(&spec.spec.name.as_str())
                || matches!(
                    spec.spec.name.as_str(),
                    TASK_START_OPTIONS_TOOL | TASK_IDENTITY_MODELS_LIST_TOOL
                )
        });
        bundle
            .handlers
            .retain(|(name, _)| bundle.specs.iter().any(|spec| spec.spec.name == *name));
    }
    let provider =
        crate::task_tools::GatewayTaskToolProvider::for_cli(std::sync::Arc::downgrade(&processor));
    let task_context = pioneer_agent::TaskTurnContext {
        workspace_id: context.workspace_id,
        thread_id: context.thread_id,
        turn_id: context.turn_id,
    };
    let tasks = if retain_proof_errors {
        provider.materialize_cli_task_tools(task_context).await?
    } else {
        provider
            .materialize_task_tools(task_context)
            .await
            .map_err(anyhow::Error::msg)?
    };
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
    fn frozen_task_bindings_keep_their_original_identities() {
        let family = NativeToolFamily::Tasks;
        for name in ["threads_start_options", "task_create", "task_result"] {
            let legacy = family.legacy_definitions(name).next().unwrap();
            let (_, fingerprint, _) = canonical_schema_identity(&legacy.parameters).unwrap();
            let binding = TurnMcpBindingRecord {
                server_installation_id: family.server_id().into(),
                server_name: family.server_name().into(),
                raw_tool_name: name.into(),
                callable_name: name.into(),
                canonical_callable_name: name.into(),
                provider_callable_name: format!("mcp__pioneer__{name}"),
                catalog_version: family.server_id().into(),
                fingerprint: family.server_id().into(),
                canonical_schema_fingerprint: fingerprint,
                provider_schema_fingerprint: String::new(),
                annotations_json: "{}".into(),
                annotations_digest: sha256_hex(b"{}"),
                effective_timeout_ms: family.timeout_ms(name) as i64,
                runtime_generation: 1,
                projection_activation_generation: 1,
                selection_reason: "implicit_policy".into(),
                capability_id: None,
            };
            let restored = current_identity("workspace", &binding).unwrap();
            assert_eq!(restored.canonical_schema, legacy.parameters);
            assert_eq!(
                restored.description.as_deref(),
                Some(legacy.description.as_str())
            );
        }
    }

    #[test]
    fn frozen_revision_budget_bindings_keep_their_original_identities() {
        let family = NativeToolFamily::Tasks;
        // Fingerprints captured from persisted bindings written before this change.
        for (name, fingerprint, description_hash) in [
            (
                "task_create",
                "88cceda07844305ebe651db0d080fa0fe85d5b2f23140b5d82ae698bbebf56a4",
                "881ae3ed5312b4fb8fa2b7fe137060a5d3d342c328621700b1f936f300b694f8",
            ),
            (
                "task_create",
                "78a551ec818dcb7d6920cceae81b1750526c6eaee08b2b0ed4f688ce945c0963",
                "a2be726710dd60c23490231914167c823ac5e0d1046acd9a18de70b8ea9442b2",
            ),
            (
                "task_update",
                "4b9ce2db1452435f02944ff3bc696fd52c2b28f376c8428e07fd8e676afc30e1",
                "ea4fdb1cd049e27287aaf174791160adff441627749fd1e7a64daebd84ed625f",
            ),
            (
                "task_get",
                "61aabd61e22105d22e6f407224df2cda262457cc450759ade385ba10c7b6c626",
                "fc4538a2d8cd39e73a7fd1b16e0887b4a68787c817ce7feb47bd8ad4573d95a9",
            ),
            (
                "task_wait",
                "033f9228b2298bafab92b423624f8e4d5fea7a9ecc2ba7f0373379abfb58fc00",
                "0b9711667b6f2940e2be36272ffae939cc454d7d66117f1656ac5c8047c070f7",
            ),
        ] {
            let binding = TurnMcpBindingRecord {
                server_installation_id: family.server_id().into(),
                server_name: family.server_name().into(),
                raw_tool_name: name.into(),
                callable_name: name.into(),
                canonical_callable_name: name.into(),
                provider_callable_name: format!("mcp__pioneer__{name}"),
                catalog_version: family.server_id().into(),
                fingerprint: family.server_id().into(),
                canonical_schema_fingerprint: fingerprint.to_owned(),
                provider_schema_fingerprint: fingerprint.to_owned(),
                annotations_json: "{}".into(),
                annotations_digest: sha256_hex(b"{}"),
                effective_timeout_ms: family.timeout_ms(name) as i64,
                runtime_generation: 1,
                projection_activation_generation: 1,
                selection_reason: "implicit_policy".into(),
                capability_id: None,
            };
            let restored = current_identity("workspace", &binding).unwrap();
            assert!(
                restored.canonical_schema["properties"]
                    .get("revisionBudget")
                    .is_none()
            );
            assert!(
                restored.canonical_schema["properties"]
                    .get("maxRevisionRounds")
                    .is_none()
            );
            let description = restored.description.unwrap();
            assert_eq!(sha256_hex(description.as_bytes()), description_hash);
            let mut projection = ResolvedMcpTurnProjection::empty("workspace", "turn");
            projection.tools.push(ResolvedMcpTurnTool {
                canonical_callable_name: binding.canonical_callable_name.clone(),
                workspace_id: "workspace".into(),
                server_installation_id: restored.server_installation_id,
                server_name: restored.server_name,
                raw_tool_name: restored.raw_tool_name,
                description: Some(description),
                input_schema: restored.canonical_schema,
                annotations: None,
                timeout_ms: restored.effective_timeout_ms,
                catalog_version: restored.catalog_version,
                installation_fingerprint: restored.installation_fingerprint,
                schema_fingerprint: restored.canonical_schema_fingerprint,
                runtime_generation: restored.runtime_generation,
                selection_reason: McpSelectionReason::ImplicitPolicy,
                capability_id: None,
            });
            assert_eq!(exact_definition(&projection.tools[0]), Some(family));
            projection
                .finalize_identity(McpProjectionLimits::default())
                .unwrap();
            let provider = crate::cli_runtime::codex_mcp::preflight_codex_mcp_schemas(
                &projection,
                "a".repeat(64),
            )
            .unwrap();
            assert_eq!(
                provider.tools[0].transformed_schema_fingerprint,
                binding.provider_schema_fingerprint
            );
            let mut unknown = binding;
            unknown.canonical_schema_fingerprint = "0".repeat(64);
            assert_eq!(
                current_identity("workspace", &unknown).unwrap_err().code,
                TurnMcpInvocationErrorCode::ToolDrift
            );
        }
        assert_eq!(
            family.action("threads_start_options").unwrap(),
            ResourceAction::ThreadRead
        );
        assert!(
            !family
                .definitions()
                .iter()
                .any(|spec| spec.spec.name == "threads_start_options")
        );
    }

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
                NativeToolFamily::ComputerUse,
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
            assert!(
                projection
                    .tools
                    .iter()
                    .any(|tool| tool.canonical_callable_name == "html_preview")
            );
            assert!(
                projection
                    .tools
                    .iter()
                    .any(|tool| tool.canonical_callable_name == "html_render")
            );
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
                NativeToolFamily::ComputerUse,
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
