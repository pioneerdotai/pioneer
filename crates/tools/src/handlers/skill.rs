use super::http::{BufferedHttpRequest, execute_buffered_http_request};
use crate::ToolExtensionBundle;
use crate::context::{AnyToolResult, FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::network_policy::enforce_network_url;
use crate::output_dynamic_policy::{
    DynamicToolKind, DynamicToolOutputPolicyCaps, DynamicToolPolicyContext,
    DynamicToolPolicyDiagnostic, resolve_dynamic_tool_output_policy,
};
use crate::output_policy::{ToolOutputPolicySnapshot, ToolOutputProjectionKind};
use crate::registry::ToolHandler;
use crate::router::{RawToolCall, ToolRouter};
use crate::runtime::ToolCallRuntime;
use crate::spec::{ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolSpec};
use crate::spec::{
    DynamicSkillPermissionKind, DynamicSkillPermissionMetadata, ToolPermissionMetadata,
};
use async_trait::async_trait;
use pioneer_protocol::SkillId;
use pioneer_skills::{DynamicToolOutputPolicyDeclaration, SkillSourceKind, SkillTrustLevel};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const READ_SKILL_TOOL_NAME: &str = "read_skill";
const DEFAULT_HTTP_TIMEOUT_MS: u64 = 20_000;
const DEFAULT_SHELL_TIMEOUT_MS: u64 = 20_000;
const MAX_HTTP_BODY_CHARS: usize = 64 * 1024;
const MAX_HTTP_BODY_BYTES: usize = MAX_HTTP_BODY_CHARS * 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillDynamicToolKind {
    Http,
    Shell,
    FunctionProxy,
}

#[derive(Debug, Clone)]
pub struct SkillDynamicToolDescriptor {
    pub canonical_tool_name: String,
    pub skill_id: SkillId,
    pub skill_owner: Option<String>,
    pub skill_slug: String,
    pub skill_asset_root: String,
    pub skill_fingerprint: String,
    pub source_kind: SkillSourceKind,
    pub trust_level: SkillTrustLevel,
    pub description: String,
    pub parameters: JsonValue,
    pub execution_class: ExecutionClass,
    pub kind: SkillDynamicToolKind,
    pub config: JsonValue,
    pub requested_output_policy: Option<DynamicToolOutputPolicyDeclaration>,
}

#[derive(Debug, Clone)]
pub struct SkillReadToolEntry {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
    pub name: String,
    pub description: String,
    pub body: String,
    pub skill_asset_root: String,
    pub fingerprint: String,
    pub source_kind: String,
}

#[derive(Debug, Clone)]
pub struct SkillReadToolConfig {
    pub index: HashMap<String, SkillReadToolEntry>,
    pub default_max_chars: usize,
}

#[derive(Clone, Default)]
struct FunctionProxyRuntimeBridge {
    inner: Arc<RwLock<Option<BoundFunctionProxyRuntime>>>,
}

#[derive(Clone)]
struct BoundFunctionProxyRuntime {
    router: Arc<ToolRouter>,
    runtime: ToolCallRuntime,
}

impl FunctionProxyRuntimeBridge {
    async fn bind(&self, router: Arc<ToolRouter>, runtime: ToolCallRuntime) {
        let mut state = self.inner.write().await;
        *state = Some(BoundFunctionProxyRuntime { router, runtime });
    }

    async fn clear(&self) {
        let mut state = self.inner.write().await;
        *state = None;
    }

    async fn get(&self) -> Option<BoundFunctionProxyRuntime> {
        self.inner.read().await.clone()
    }
}

pub struct SkillRuntimeToolMaterialization {
    pub bundles: Vec<ToolExtensionBundle>,
    pub excluded_tools: Vec<ExcludedSkillRuntimeTool>,
    pub policy_diagnostics: Vec<SkillRuntimeToolPolicyDiagnostic>,
    function_proxy_bridges: Vec<FunctionProxyRuntimeBridge>,
}

#[derive(Debug, Clone)]
pub struct ExcludedSkillRuntimeTool {
    pub canonical_tool_name: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct SkillRuntimeToolPolicyDiagnostic {
    pub canonical_tool_name: String,
    pub diagnostics: Vec<DynamicToolPolicyDiagnostic>,
}

impl SkillRuntimeToolMaterialization {
    pub async fn bind_function_proxy_runtime(
        &self,
        router: Arc<ToolRouter>,
        runtime: ToolCallRuntime,
    ) {
        for bridge in &self.function_proxy_bridges {
            bridge.bind(router.clone(), runtime.clone()).await;
        }
    }

    pub async fn clear_function_proxy_runtime(&self) {
        for bridge in &self.function_proxy_bridges {
            bridge.clear().await;
        }
    }
}

pub fn materialize_skill_runtime_tools(
    descriptors: &[SkillDynamicToolDescriptor],
    read_skill: Option<SkillReadToolConfig>,
    output_policy_caps: DynamicToolOutputPolicyCaps,
) -> SkillRuntimeToolMaterialization {
    let mut bundle = ToolExtensionBundle::default();
    let mut bridges = Vec::new();
    let mut excluded_tools = Vec::new();
    let mut policy_diagnostics = Vec::new();

    for descriptor in descriptors {
        let resolved_spec = match runtime_descriptor_to_spec(descriptor, &output_policy_caps) {
            Ok(spec) => spec,
            Err(error) => {
                excluded_tools.push(ExcludedSkillRuntimeTool {
                    canonical_tool_name: descriptor.canonical_tool_name.clone(),
                    reason: error,
                });
                continue;
            }
        };
        if !resolved_spec.diagnostics.is_empty() {
            policy_diagnostics.push(SkillRuntimeToolPolicyDiagnostic {
                canonical_tool_name: descriptor.canonical_tool_name.clone(),
                diagnostics: resolved_spec.diagnostics.clone(),
            });
        }

        let handler = match runtime_descriptor_to_handler(descriptor, &mut bridges) {
            Ok(handler) => handler,
            Err(error) => {
                excluded_tools.push(ExcludedSkillRuntimeTool {
                    canonical_tool_name: descriptor.canonical_tool_name.clone(),
                    reason: error,
                });
                continue;
            }
        };

        bundle.specs.push(resolved_spec.configured_spec);

        bundle
            .handlers
            .push((descriptor.canonical_tool_name.clone(), handler));
    }

    if let Some(read_skill) = read_skill {
        bundle.specs.push(read_skill_spec());
        bundle.handlers.push((
            READ_SKILL_TOOL_NAME.to_owned(),
            Arc::new(ReadSkillHandler {
                read_skill_index: read_skill.index,
                default_max_chars: read_skill.default_max_chars.max(1),
            }),
        ));
    }

    let bundles = if bundle.specs.is_empty() {
        Vec::new()
    } else {
        vec![bundle]
    };

    SkillRuntimeToolMaterialization {
        bundles,
        excluded_tools,
        policy_diagnostics,
        function_proxy_bridges: bridges,
    }
}

struct ResolvedRuntimeToolSpec {
    configured_spec: ConfiguredToolSpec,
    diagnostics: Vec<DynamicToolPolicyDiagnostic>,
}

fn runtime_descriptor_to_spec(
    descriptor: &SkillDynamicToolDescriptor,
    output_policy_caps: &DynamicToolOutputPolicyCaps,
) -> Result<ResolvedRuntimeToolSpec, String> {
    if descriptor.canonical_tool_name.trim().is_empty() {
        return Err("dynamic skill tool has empty canonical tool name".to_owned());
    }

    let parameters = if descriptor.parameters.is_object() {
        descriptor.parameters.clone()
    } else {
        serde_json::json!({
            "type": "object",
            "additionalProperties": true
        })
    };

    let spec = ToolSpec::new(
        descriptor.canonical_tool_name.clone(),
        descriptor.description.clone(),
        parameters,
        PayloadKind::Function,
    )
    .with_permission_metadata(permission_metadata_for_descriptor(descriptor));
    let resolution = resolve_dynamic_tool_output_policy(
        dynamic_policy_context(descriptor, output_policy_caps),
        descriptor.requested_output_policy.clone(),
    );
    Ok(ResolvedRuntimeToolSpec {
        configured_spec: ConfiguredToolSpec::with_output_projection(
            spec,
            descriptor.execution_class,
            resolution.effective_policy,
            resolution.projection_kind,
        ),
        diagnostics: resolution.diagnostics,
    })
}

fn permission_metadata_for_descriptor(
    descriptor: &SkillDynamicToolDescriptor,
) -> ToolPermissionMetadata {
    let config = descriptor.config.as_object();
    ToolPermissionMetadata {
        dynamic_skill: Some(DynamicSkillPermissionMetadata {
            kind: match descriptor.kind {
                SkillDynamicToolKind::Http => DynamicSkillPermissionKind::Http,
                SkillDynamicToolKind::Shell => DynamicSkillPermissionKind::Shell,
                SkillDynamicToolKind::FunctionProxy => DynamicSkillPermissionKind::FunctionProxy,
            },
            skill_id: descriptor.skill_id.clone(),
            skill_owner: descriptor.skill_owner.clone(),
            skill_slug: descriptor.skill_slug.clone(),
            source_kind: format!("{:?}", descriptor.source_kind),
            trust_level: format!("{:?}", descriptor.trust_level),
            target_tool: config
                .and_then(|config| config.get("target_tool"))
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
            configured_method: config
                .and_then(|config| config.get("method"))
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
            configured_url: config
                .and_then(|config| config.get("url"))
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
        }),
    }
}

fn dynamic_policy_context(
    descriptor: &SkillDynamicToolDescriptor,
    output_policy_caps: &DynamicToolOutputPolicyCaps,
) -> DynamicToolPolicyContext {
    let target_tool_name = descriptor
        .config
        .get("target_tool")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let (target_output_policy, target_projection_kind) = target_tool_name
        .as_deref()
        .and_then(builtin_target_policy)
        .map(|policy| (Some(policy), Some(ToolOutputProjectionKind::Builtin)))
        .unwrap_or((None, None));

    DynamicToolPolicyContext {
        canonical_tool_name: descriptor.canonical_tool_name.clone(),
        skill_id: descriptor.skill_id.clone(),
        skill_slug: descriptor.skill_slug.clone(),
        skill_fingerprint: descriptor.skill_fingerprint.clone(),
        source_kind: descriptor.source_kind.clone(),
        trust_level: descriptor.trust_level.clone(),
        kind: match descriptor.kind {
            SkillDynamicToolKind::Http => DynamicToolKind::Http,
            SkillDynamicToolKind::Shell => DynamicToolKind::Shell,
            SkillDynamicToolKind::FunctionProxy => DynamicToolKind::FunctionProxy,
        },
        target_tool_name,
        target_output_policy,
        target_projection_kind,
        host_caps: output_policy_caps.clone(),
    }
}

fn builtin_target_policy(tool_name: &str) -> Option<ToolOutputPolicySnapshot> {
    if matches!(
        tool_name,
        "exec_command"
            | "write_stdin"
            | "read_file"
            | "read_skill"
            | "list_dir"
            | "grep_files"
            | "apply_patch"
            | "write_file"
            | "web_fetch"
            | "web_search"
            | "download_url"
            | "download"
            | "computer_use"
    ) {
        Some(ToolOutputPolicySnapshot::for_tool_name(tool_name))
    } else {
        None
    }
}

fn runtime_descriptor_to_handler(
    descriptor: &SkillDynamicToolDescriptor,
    bridges: &mut Vec<FunctionProxyRuntimeBridge>,
) -> Result<Arc<dyn ToolHandler>, String> {
    match descriptor.kind {
        SkillDynamicToolKind::Http => Ok(Arc::new(SkillHttpToolHandler {
            descriptor: descriptor.clone(),
        })),
        SkillDynamicToolKind::Shell => {
            let bridge = FunctionProxyRuntimeBridge::default();
            bridges.push(bridge.clone());
            Ok(Arc::new(SkillShellToolHandler {
                descriptor: descriptor.clone(),
                bridge,
            }))
        }
        SkillDynamicToolKind::FunctionProxy => {
            let target_tool = descriptor
                .config
                .get("target_tool")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    format!(
                        "function_proxy tool `{}` missing `config.target_tool`",
                        descriptor.canonical_tool_name
                    )
                })?;
            let bridge = FunctionProxyRuntimeBridge::default();
            bridges.push(bridge.clone());
            Ok(Arc::new(SkillFunctionProxyHandler {
                descriptor: descriptor.clone(),
                bridge,
                target_tool,
            }))
        }
    }
}

fn read_skill_spec() -> ConfiguredToolSpec {
    ConfiguredToolSpec::new(
        ToolSpec::new(
            READ_SKILL_TOOL_NAME,
            "Read full instructions and skill_asset_root for an active skill by its exact skill:<skill_id> reference. Relative paths in the returned skill body resolve under skill_asset_root.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "skill_id": {
                        "type": "string",
                        "description": "Exact opaque skill reference from the Skills prompt, for example \"skill:mvg02zVNGWuw5z5C4nYDo\". Never reconstruct it from owner, slug, name, or display label.",
                        "pattern": "^skill:[A-Za-z0-9]{21}$"
                    },
                    "max_chars": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Optional maximum number of skill body characters to return."
                    },
                    "include_metadata": {
                        "type": "boolean",
                        "description": "Whether to include skill metadata before the body."
                    }
                },
                "required": ["skill_id"],
                "additionalProperties": false
            }),
            PayloadKind::Function,
        ),
        ExecutionClass::Shared,
        crate::model_only_metadata_policy(),
    )
}

struct ReadSkillHandler {
    read_skill_index: HashMap<String, SkillReadToolEntry>,
    default_max_chars: usize,
}

impl ReadSkillHandler {
    fn resolve_requested_skill_id(&self, requested: &str) -> Result<String, ToolError> {
        let Some(raw_id) = requested.strip_prefix("skill:") else {
            return Err(ToolError::invalid_arguments(
                "`skill_id` must be the exact `skill:<skill_id>` reference from the Skills prompt",
            ));
        };
        let skill_id = SkillId::new(raw_id).map_err(|_| {
            ToolError::invalid_arguments(
                "`skill_id` must be the exact `skill:<skill_id>` reference from the Skills prompt",
            )
        })?;
        Ok(format!("skill:{skill_id}"))
    }
}

#[async_trait]
impl ToolHandler for ReadSkillHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let arguments = function_arguments(&invocation)?;

        let skill_id = arguments
            .get("skill_id")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.strip_prefix('$').unwrap_or(value))
            .map(str::to_owned)
            .ok_or_else(|| ToolError::invalid_arguments("`skill_id` is required"))?;

        let skill_id = self.resolve_requested_skill_id(skill_id.as_str())?;

        let include_metadata = arguments
            .get("include_metadata")
            .and_then(JsonValue::as_bool)
            .unwrap_or(true);

        let max_chars = arguments
            .get("max_chars")
            .and_then(JsonValue::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(self.default_max_chars)
            .clamp(1, self.default_max_chars.max(1));

        let Some(entry) = self.read_skill_index.get(skill_id.as_str()) else {
            return Err(ToolError::Rejected(format!(
                "skill `{skill_id}` is not active for this turn"
            )));
        };

        let mut body = entry.body.clone();
        let mut truncated = false;
        if body.chars().count() > max_chars {
            body = body.chars().take(max_chars).collect::<String>();
            truncated = true;
        }

        let payload = if include_metadata {
            serde_json::json!({
                "skill_id": entry.skill_id,
                "owner": entry.owner,
                "slug": entry.slug,
                "name": entry.name,
                "description": entry.description,
                "skill_asset_root": entry.skill_asset_root,
                "relative_path_resolution": "Resolve relative file paths mentioned by this skill under skill_asset_root. Prefer absolute paths built from skill_asset_root for commands and file operations.",
                "body": body,
                "truncated": truncated,
                "fingerprint": entry.fingerprint,
                "source_kind": entry.source_kind
            })
        } else {
            serde_json::json!({
                "skill_id": entry.skill_id,
                "slug": entry.slug,
                "skill_asset_root": entry.skill_asset_root,
                "relative_path_resolution": "Resolve relative file paths mentioned by this skill under skill_asset_root. Prefer absolute paths built from skill_asset_root for commands and file operations.",
                "body": body,
                "truncated": truncated
            })
        };

        let text = serde_json::to_string(&payload).unwrap_or_else(|_| payload.to_string());
        Ok(Box::new(FunctionToolOutput::with_payload(
            text, true, payload,
        )))
    }
}

struct SkillHttpToolHandler {
    descriptor: SkillDynamicToolDescriptor,
}

#[async_trait]
impl ToolHandler for SkillHttpToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let arguments = function_arguments(&invocation)?;
        let config = self
            .descriptor
            .config
            .as_object()
            .cloned()
            .unwrap_or_default();

        let method = arguments
            .get("method")
            .and_then(JsonValue::as_str)
            .or_else(|| config.get("method").and_then(JsonValue::as_str))
            .unwrap_or("GET")
            .to_uppercase();

        let url = arguments
            .get("url")
            .and_then(JsonValue::as_str)
            .or_else(|| config.get("url").and_then(JsonValue::as_str))
            .ok_or_else(|| ToolError::invalid_arguments("http tool requires `url`"))?
            .to_owned();
        enforce_network_url(
            invocation.execution_security_snapshot.as_ref(),
            url.as_str(),
            "dynamic_skill_http",
        )?;

        let timeout_ms = arguments
            .get("timeout_ms")
            .and_then(JsonValue::as_u64)
            .or_else(|| config.get("timeout_ms").and_then(JsonValue::as_u64))
            .unwrap_or(DEFAULT_HTTP_TIMEOUT_MS)
            .clamp(1, 5 * 60_000);

        let mut merged_headers = HashMap::new();
        if let Some(headers) = config.get("headers").and_then(JsonValue::as_object) {
            for (name, value) in headers {
                if let Some(value) = value.as_str() {
                    merged_headers.insert(name.clone(), value.to_owned());
                }
            }
        }
        if let Some(headers) = arguments.get("headers").and_then(JsonValue::as_object) {
            for (name, value) in headers {
                if let Some(value) = value.as_str() {
                    merged_headers.insert(name.clone(), value.to_owned());
                }
            }
        }

        let query = arguments
            .get("query")
            .and_then(JsonValue::as_object)
            .cloned();
        let body = arguments.get("body").cloned();

        let response = execute_buffered_http_request(BufferedHttpRequest {
            method,
            url,
            timeout_ms,
            follow_redirects: true,
            user_agent: None,
            headers: merged_headers,
            query,
            body,
            max_bytes: MAX_HTTP_BODY_BYTES,
        })
        .await
        .map_err(|error| match error {
            ToolError::ExecutionFailed(message) => ToolError::execution_failed(format!(
                "HTTP skill tool `{}` request failed: {message}",
                self.descriptor.canonical_tool_name
            )),
            ToolError::Internal(message) => ToolError::internal(format!(
                "HTTP skill tool `{}` internal error: {message}",
                self.descriptor.canonical_tool_name
            )),
            other => other,
        })?;

        let mut body = String::from_utf8_lossy(&response.body).to_string();

        let mut truncated_by_chars = false;
        if body.chars().count() > MAX_HTTP_BODY_CHARS {
            body = body.chars().take(MAX_HTTP_BODY_CHARS).collect::<String>();
            truncated_by_chars = true;
        }

        let success = response.status_code < 400;
        let payload = serde_json::json!({
            "status_code": response.status_code,
            "success": success,
            "url": response.final_url,
            "body": body,
            "truncated": response.truncated || truncated_by_chars
        });
        let rendered = serde_json::to_string(&payload).unwrap_or_else(|_| payload.to_string());

        Ok(Box::new(FunctionToolOutput::with_payload(
            rendered, success, payload,
        )))
    }
}

struct SkillShellToolHandler {
    descriptor: SkillDynamicToolDescriptor,
    bridge: FunctionProxyRuntimeBridge,
}

#[async_trait]
impl ToolHandler for SkillShellToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let arguments = function_arguments(&invocation)?;
        let config = self
            .descriptor
            .config
            .as_object()
            .cloned()
            .unwrap_or_default();

        let command = if let Some(command) = arguments.get("command").and_then(JsonValue::as_array)
        {
            command
                .iter()
                .filter_map(JsonValue::as_str)
                .map(|value| {
                    expand_skill_asset_placeholders(
                        value,
                        self.descriptor.skill_asset_root.as_str(),
                    )
                })
                .collect::<Vec<_>>()
        } else if let Some(command) = config.get("command").and_then(JsonValue::as_array) {
            command
                .iter()
                .filter_map(JsonValue::as_str)
                .map(|value| {
                    expand_skill_asset_placeholders(
                        value,
                        self.descriptor.skill_asset_root.as_str(),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            return Err(ToolError::invalid_arguments(
                "shell skill tool requires `command` array",
            ));
        };

        if command.is_empty() {
            return Err(ToolError::invalid_arguments(
                "shell command must not be empty",
            ));
        }

        let timeout_ms = arguments
            .get("timeout_ms")
            .and_then(JsonValue::as_u64)
            .or_else(|| config.get("timeout_ms").and_then(JsonValue::as_u64))
            .unwrap_or(DEFAULT_SHELL_TIMEOUT_MS)
            .clamp(1, 5 * 60_000);

        let Some(runtime) = self.bridge.get().await else {
            return Err(ToolError::internal(format!(
                "shell bridge for `{}` is not bound",
                self.descriptor.canonical_tool_name
            )));
        };

        let nested_arguments = serde_json::json!({
            "command": command,
            "timeout_ms": timeout_ms,
            "tty": false
        });

        let nested =
            execute_nested_tool_call(&runtime, &invocation, "exec_command", nested_arguments)
                .await
                .map_err(|error| {
                    ToolError::execution_failed(format!(
                        "shell skill tool `{}` execution failed: {error}",
                        self.descriptor.canonical_tool_name
                    ))
                })?;

        let raw_payload = nested.raw_output_json();
        let stdout = raw_payload
            .get("stdout")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_owned();
        let stderr = raw_payload
            .get("stderr")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_owned();
        let merged = format!("{stdout}{stderr}");
        let truncated = raw_payload
            .get("truncated")
            .and_then(|value| {
                value
                    .get("aggregated_output")
                    .and_then(JsonValue::as_bool)
                    .or_else(|| value.as_bool())
            })
            .unwrap_or(false);
        let payload = serde_json::json!({
            "command": raw_payload.get("command").cloned().unwrap_or_else(|| serde_json::json!([])),
            "exit_code": raw_payload.get("exit_code").cloned().unwrap_or(JsonValue::Null),
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": raw_payload.get("timed_out").cloned().unwrap_or_else(|| serde_json::json!(false)),
            "duration_ms": raw_payload.get("duration_ms").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "truncated": truncated
        });

        Ok(Box::new(FunctionToolOutput::with_payload(
            merged,
            nested.success(),
            payload,
        )))
    }
}

fn expand_skill_asset_placeholders(value: &str, skill_asset_root: &str) -> String {
    value
        .replace("${skill_asset_root}", skill_asset_root)
        .replace("${skill_dir}", skill_asset_root)
}

struct SkillFunctionProxyHandler {
    descriptor: SkillDynamicToolDescriptor,
    bridge: FunctionProxyRuntimeBridge,
    target_tool: String,
}

#[async_trait]
impl ToolHandler for SkillFunctionProxyHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        if self.target_tool == invocation.tool_name {
            return Err(ToolError::Rejected(
                "function_proxy target cannot call itself".to_owned(),
            ));
        }

        let Some(runtime) = self.bridge.get().await else {
            return Err(ToolError::internal(format!(
                "function_proxy bridge for `{}` is not bound",
                self.descriptor.canonical_tool_name
            )));
        };

        let arguments = function_arguments(&invocation)?;
        let nested_arguments = arguments
            .get("arguments")
            .cloned()
            .unwrap_or(arguments.clone());

        let nested = execute_nested_tool_call(
            &runtime,
            &invocation,
            self.target_tool.as_str(),
            nested_arguments,
        )
        .await?;

        let payload = nested.raw_output_json();
        Ok(Box::new(FunctionToolOutput::with_payload(
            nested.raw_output_text(),
            nested.success(),
            payload,
        )))
    }
}

fn function_arguments(invocation: &ToolInvocation) -> Result<JsonValue, ToolError> {
    match &invocation.payload {
        ToolPayload::Function { arguments } => Ok(arguments.clone()),
        _ => Err(ToolError::invalid_arguments(
            "skill runtime tools require function payload arguments",
        )),
    }
}

async fn execute_nested_tool_call(
    runtime: &BoundFunctionProxyRuntime,
    invocation: &ToolInvocation,
    target_tool: &str,
    arguments: JsonValue,
) -> Result<AnyToolResult, ToolError> {
    let raw_call = RawToolCall {
        call_id: format!("{}::{}", invocation.call_id, target_tool),
        tool_name: target_tool.to_owned(),
        arguments: arguments.to_string(),
    };

    let call = runtime
        .router
        .build_tool_call(raw_call)
        .map_err(|error| ToolError::invalid_arguments(error.to_string()))?;

    runtime
        .runtime
        .execute_nested_tool_call(call, invocation.workdir.clone())
        .await
        .map_err(|error| ToolError::execution_failed(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        ReadSkillHandler, SkillDynamicToolDescriptor, SkillDynamicToolKind, SkillHttpToolHandler,
        SkillReadToolEntry, read_skill_spec,
    };
    use crate::{
        ExecutionClass, ToolCallSource, ToolHandler, ToolInvocation, ToolPayload,
        ToolRecoveryMetadata,
    };
    use pioneer_protocol::{
        SkillId, TurnExecutionSecuritySnapshot, TurnPermissionMode, TurnPermissionProfileSnapshot,
        TurnPermissionProfileSource,
    };
    use pioneer_skills::{SkillSourceKind, SkillTrustLevel};
    use serde_json::json;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn read_skill_entry(skill_id: &str, slug: &str, name: &str) -> SkillReadToolEntry {
        let skill_id = SkillId::new(skill_id).expect("valid read-skill fixture ID");
        SkillReadToolEntry {
            skill_id,
            owner: Some("workspace".to_owned()),
            slug: slug.to_owned(),
            name: name.to_owned(),
            description: "Skill description".to_owned(),
            body: "Skill body".to_owned(),
            skill_asset_root: "/tmp/pioneer-skills/user/workspace/weather".to_owned(),
            fingerprint: "fingerprint".to_owned(),
            source_kind: "user".to_owned(),
        }
    }

    fn read_skill_invocation(skill_ref: &str) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_1".to_owned(),
            tool_name: "read_skill".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function {
                arguments: json!({ "skill_id": skill_ref }),
            },
            workdir: PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn skill_http_handler(url: &str) -> SkillHttpToolHandler {
        SkillHttpToolHandler {
            descriptor: SkillDynamicToolDescriptor {
                canonical_tool_name: "skill_weather_http".to_owned(),
                skill_id: SkillId::new("HHHHHHHHHHHHHHHHHHHHH").expect("valid HTTP test SkillId"),
                skill_owner: Some("workspace".to_owned()),
                skill_slug: "user:workspace/weather".to_owned(),
                skill_asset_root: "/tmp/skill-weather".to_owned(),
                skill_fingerprint: "fingerprint".to_owned(),
                source_kind: SkillSourceKind::User,
                trust_level: SkillTrustLevel::Community,
                description: "Fetch weather".to_owned(),
                parameters: json!({
                    "type": "object",
                    "additionalProperties": true
                }),
                execution_class: ExecutionClass::Shared,
                kind: SkillDynamicToolKind::Http,
                config: json!({
                    "method": "GET",
                    "url": url
                }),
                requested_output_policy: None,
            },
        }
    }

    fn skill_http_invocation(
        arguments: serde_json::Value,
        execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
    ) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_skill_http".to_owned(),
            tool_name: "skill_weather_http".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function { arguments },
            workdir: PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn read_only_snapshot() -> TurnExecutionSecuritySnapshot {
        TurnExecutionSecuritySnapshot::read_only(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            "/tmp/workspace",
            Vec::new(),
            1_700_000_000_000,
        )
    }

    fn full_access_snapshot() -> TurnExecutionSecuritySnapshot {
        TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp/workspace", 1_700_000_000_000)
    }

    #[test]
    fn read_skill_schema_points_model_to_exact_skill_id() {
        let spec = read_skill_spec();
        let skill_id = &spec.spec.parameters["properties"]["skill_id"];

        assert_eq!(skill_id["pattern"], r"^skill:[A-Za-z0-9]{21}$");
        assert!(
            skill_id["description"]
                .as_str()
                .unwrap_or_default()
                .contains("skill:mvg02zVNGWuw5z5C4nYDo")
        );
        assert!(
            skill_id["description"]
                .as_str()
                .unwrap_or_default()
                .contains("Never reconstruct")
        );
        assert!(spec.spec.description.contains("skill_asset_root"));
    }

    #[tokio::test]
    async fn read_skill_handler_rejects_presentation_slug() {
        let mut index = HashMap::<String, SkillReadToolEntry>::new();
        index.insert(
            "skill:TTTTTTTTTTTTTTTTTTTTT".to_owned(),
            read_skill_entry("TTTTTTTTTTTTTTTTTTTTT", "weather", "weather"),
        );
        let handler = ReadSkillHandler {
            read_skill_index: index,
            default_max_chars: 1024,
        };

        let result = handler
            .handle(
                read_skill_invocation("weather"),
                crate::ToolEventBus::default().start_trace("turn", "call_1", "read_skill"),
            )
            .await;

        let error = match result {
            Ok(_) => panic!("presentation slug should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("skill:<skill_id>"));
    }

    #[tokio::test]
    async fn skill_policy_http_tool_inherits_disabled_network_snapshot() {
        let handler = skill_http_handler("https://example.com");
        let result = handler
            .handle(
                skill_http_invocation(json!({}), Some(read_only_snapshot())),
                crate::ToolEventBus::default().start_trace(
                    "turn",
                    "call_skill_http",
                    "skill_weather_http",
                ),
            )
            .await;

        let error = match result {
            Ok(_) => panic!("disabled network should reject skill HTTP"),
            Err(error) => error,
        };
        assert!(
            matches!(error, crate::ToolError::Rejected(ref message) if message.contains("network access is disabled")),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn skill_policy_http_tool_full_access_reaches_http_execution_path() {
        let handler = skill_http_handler("https://example.com");
        let result = handler
            .handle(
                skill_http_invocation(
                    json!({ "method": "BAD METHOD" }),
                    Some(full_access_snapshot()),
                ),
                crate::ToolEventBus::default().start_trace(
                    "turn",
                    "call_skill_http",
                    "skill_weather_http",
                ),
            )
            .await;

        let error = match result {
            Ok(_) => panic!("invalid method should fail inside HTTP execution path"),
            Err(error) => error,
        };
        assert!(
            !matches!(error, crate::ToolError::Rejected(message) if message.contains("network access is disabled")),
            "full access must not be blocked by skill network policy"
        );
    }

    #[test]
    fn expands_skill_asset_placeholders() {
        assert_eq!(
            super::expand_skill_asset_placeholders(
                "${skill_asset_root}/scripts/run.py:${skill_dir}/data",
                "/tmp/skill-root",
            ),
            "/tmp/skill-root/scripts/run.py:/tmp/skill-root/data"
        );
    }

    #[tokio::test]
    async fn read_skill_handler_rejects_owner_slug() {
        let mut index = HashMap::<String, SkillReadToolEntry>::new();
        index.insert(
            "skill:SSSSSSSSSSSSSSSSSSSSS".to_owned(),
            read_skill_entry("SSSSSSSSSSSSSSSSSSSSS", "weather", "weather"),
        );
        index.insert(
            "skill:UUUUUUUUUUUUUUUUUUUUU".to_owned(),
            read_skill_entry("UUUUUUUUUUUUUUUUUUUUU", "weather", "weather"),
        );
        let handler = ReadSkillHandler {
            read_skill_index: index,
            default_max_chars: 1024,
        };

        let result = handler
            .handle(
                read_skill_invocation("workspace/weather"),
                crate::ToolEventBus::default().start_trace("turn", "call_1", "read_skill"),
            )
            .await;

        let error = match result {
            Ok(_) => panic!("owner/slug should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("skill:<skill_id>"));
    }

    #[tokio::test]
    async fn read_skill_handler_accepts_exact_id_among_duplicate_labels() {
        let mut index = HashMap::<String, SkillReadToolEntry>::new();
        index.insert(
            "skill:SSSSSSSSSSSSSSSSSSSSS".to_owned(),
            read_skill_entry("SSSSSSSSSSSSSSSSSSSSS", "weather", "weather"),
        );
        index.insert(
            "skill:UUUUUUUUUUUUUUUUUUUUU".to_owned(),
            read_skill_entry("UUUUUUUUUUUUUUUUUUUUU", "weather", "weather"),
        );
        let handler = ReadSkillHandler {
            read_skill_index: index,
            default_max_chars: 1024,
        };

        let output = handler
            .handle(
                read_skill_invocation("skill:SSSSSSSSSSSSSSSSSSSSS"),
                crate::ToolEventBus::default().start_trace("turn", "call_1", "read_skill"),
            )
            .await
            .expect("exact SkillId should resolve");

        assert_eq!(output.raw_json()["skill_id"], "SSSSSSSSSSSSSSSSSSSSS");
        assert_eq!(output.raw_json()["slug"], "weather");
        assert_eq!(
            output.raw_json()["skill_asset_root"],
            "/tmp/pioneer-skills/user/workspace/weather"
        );
        assert_eq!(output.raw_json()["source_kind"], "user");
    }

    #[tokio::test]
    async fn read_skill_handler_rejects_unknown_id() {
        let handler = ReadSkillHandler {
            read_skill_index: HashMap::<String, SkillReadToolEntry>::new(),
            default_max_chars: 1024,
        };

        let result = handler
            .handle(
                read_skill_invocation("skill:MMMMMMMMMMMMMMMMMMMMM"),
                crate::ToolEventBus::default().start_trace("turn", "call_1", "read_skill"),
            )
            .await;

        let error = match result {
            Ok(_) => panic!("inactive exact SkillId should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("not active"));
    }
}
