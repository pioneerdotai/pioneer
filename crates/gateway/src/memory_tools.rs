use crate::message::MessageProcessor;
use async_trait::async_trait;
use pioneer_agent::{
    AgentMemoryProvider, MemoryRecallItem as AgentMemoryRecallItem, MemoryRecallRequest,
    MemoryRecallSnapshot, MemoryToolMaterialization, MemoryTurnContext,
};
use pioneer_crud::workspace_agent_memory_scope_key;
use pioneer_memory::{MemoryOperationContext, MemoryRecallParams};
use pioneer_protocol::{
    MemoryActor, MemoryActorKind, MemoryCategory, MemoryForgetParams, MemoryForgetTarget,
    MemoryGetParams, MemoryProvenance, MemoryRecord, MemoryRememberParams, MemoryScope,
    MemoryScopeKind, MemorySearchHit, MemorySearchParams, MemorySensitivity, MemorySourceKind,
};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolError,
    ToolExtensionBundle, ToolHandler, ToolIdempotencyMode, ToolInvocation, ToolOutput, ToolPayload,
    ToolRecoveryMetadata, ToolRetryClass, ToolSpec, dynamic_unknown_output_policy,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

const MEMORY_SEARCH_TOOL: &str = "memory_search";
const MEMORY_GET_TOOL: &str = "memory_get";
const MEMORY_REMEMBER_TOOL: &str = "memory_remember";
const MEMORY_FORGET_TOOL: &str = "memory_forget";
const DEFAULT_SEARCH_LIMIT: u32 = 8;
const MAX_SEARCH_LIMIT: u32 = 20;
const SNIPPET_MAX_CHARS: usize = 280;

#[derive(Clone)]
pub(crate) struct GatewayMemoryProvider {
    processor: Weak<MessageProcessor>,
}

impl GatewayMemoryProvider {
    pub(crate) fn new(processor: Weak<MessageProcessor>) -> Self {
        Self { processor }
    }

    fn processor(&self) -> Result<Arc<MessageProcessor>, String> {
        self.processor
            .upgrade()
            .ok_or_else(|| "message processor is no longer available".to_owned())
    }
}

#[async_trait]
impl AgentMemoryProvider for GatewayMemoryProvider {
    async fn recall_memory(
        &self,
        context: MemoryTurnContext,
        request: MemoryRecallRequest,
    ) -> Result<MemoryRecallSnapshot, String> {
        let processor = self.processor()?;
        let runtime = processor.memory_runtime();
        if let Err(error) = runtime.ensure_enabled() {
            return Ok(MemoryRecallSnapshot {
                items: Vec::new(),
                diagnostics: vec![format!("memory runtime unavailable: {error:#}")],
                truncated: false,
            });
        }

        let response = runtime
            .service()
            .recall_for_prompt(
                runtime.operation_context_for_turn(&context, None),
                MemoryRecallParams {
                    query: request.query,
                    scopes: Vec::new(),
                    categories: request.categories,
                    top_k: request.top_k,
                    max_chars: request.max_chars,
                },
            )
            .await
            .map_err(|error| format!("{error:#}"))?;

        Ok(MemoryRecallSnapshot {
            items: response
                .items
                .into_iter()
                .map(|item| AgentMemoryRecallItem {
                    memory_id: item.memory_id,
                    scope: item.scope,
                    category: item.category,
                    key: item.key,
                    content: item.content,
                    score: item.score,
                    updated_at: item.updated_at,
                })
                .collect(),
            diagnostics: Vec::new(),
            truncated: false,
        })
    }

    async fn materialize_memory_tools(
        &self,
        context: MemoryTurnContext,
    ) -> Result<MemoryToolMaterialization, String> {
        let processor = self.processor()?;
        let runtime = processor.memory_runtime();
        if let Err(error) = runtime.ensure_enabled() {
            return Ok(MemoryToolMaterialization {
                bundles: Vec::new(),
                diagnostics: vec![format!("memory runtime unavailable: {error:#}")],
            });
        }

        let handler = Arc::new(MemoryToolHandler { processor, context });
        let mut bundle = ToolExtensionBundle::default();
        for configured in memory_tool_specs() {
            let name = configured.spec.name.clone();
            bundle.specs.push(configured);
            bundle.handlers.push((name, handler.clone()));
        }

        Ok(MemoryToolMaterialization {
            bundles: vec![bundle],
            diagnostics: Vec::new(),
        })
    }
}

#[derive(Clone)]
struct MemoryToolHandler {
    processor: Arc<MessageProcessor>,
    context: MemoryTurnContext,
}

#[async_trait]
impl ToolHandler for MemoryToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        match invocation.tool_name.as_str() {
            MEMORY_SEARCH_TOOL => self.handle_search(invocation).await,
            MEMORY_GET_TOOL => self.handle_get(invocation).await,
            MEMORY_REMEMBER_TOOL => self.handle_remember(invocation).await,
            MEMORY_FORGET_TOOL => self.handle_forget(invocation).await,
            other => Err(ToolError::NotFound(other.to_owned())),
        }
    }
}

impl MemoryToolHandler {
    async fn handle_search(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: MemorySearchToolInput = decode_tool_args(invocation)?;
        let query = required_string(input.query.as_deref(), "query")?;
        let limit = input
            .limit
            .unwrap_or(DEFAULT_SEARCH_LIMIT)
            .clamp(1, MAX_SEARCH_LIMIT);
        let scopes = self.scopes_for_kinds(&input.scopes)?;
        let context = self.operation_context(None)?;

        let response = self
            .processor
            .memory_runtime()
            .service()
            .search(
                context,
                MemorySearchParams {
                    query,
                    scopes,
                    categories: input.categories,
                    statuses: Vec::new(),
                    limit: Some(limit),
                    cursor: None,
                    include_provenance: input.include_provenance,
                },
            )
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

        Ok(function_output(search_output(
            &response.hits,
            response.next_cursor.as_deref(),
            input.include_provenance,
        )))
    }

    async fn handle_get(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: MemoryGetToolInput = decode_tool_args(invocation)?;
        let memory_id = optional_trimmed(input.memory_id);
        let key = optional_trimmed(input.key);
        if memory_id.is_some() == key.is_some() {
            return Err(ToolError::invalid_arguments(
                "exactly one of memoryId or key is required",
            ));
        }

        let context = self.operation_context(None)?;
        let response = if let Some(memory_id) = memory_id {
            self.processor
                .memory_runtime()
                .service()
                .get(
                    context,
                    MemoryGetParams {
                        memory_id,
                        include_deleted: false,
                    },
                )
                .await
        } else {
            let key = key.expect("checked above");
            let scope_kind = input.scope.ok_or_else(|| {
                ToolError::invalid_arguments("scope is required for key lookup in Phase 09")
            })?;
            let scope = self.scope_for_kind(scope_kind)?;
            self.processor
                .memory_runtime()
                .service()
                .get_by_key(context, scope, None, key)
                .await
        }
        .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

        Ok(function_output(json!({
            "record": response.record.as_ref().map(|record| record_output(record, true, true)),
        })))
    }

    async fn handle_remember(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let idempotency_key = invocation.idempotency_key.clone();
        let input: MemoryRememberToolInput = decode_tool_args(invocation)?;
        let content = required_string(Some(input.content.as_str()), "content")?;
        let scope = match input.scope {
            Some(kind) => self.scope_for_kind(kind)?,
            None => self.default_scope_for_category(input.category)?,
        };
        let key = optional_trimmed(input.key)
            .or_else(|| Some(stable_memory_key(input.category, content.as_str())));
        let actor = assistant_actor(&self.context);
        let context = self.operation_context(Some(actor.clone()))?;
        let provenance = MemoryProvenance {
            source_kind: input
                .source
                .unwrap_or(MemoryToolSource::ExplicitUserRequest)
                .source_kind(),
            source_thread_id: Some(self.context.thread_id.clone()),
            source_turn_id: Some(self.context.turn_id.clone()),
            source_item_id: None,
            created_by: Some(actor),
        };

        let response = self
            .processor
            .memory_runtime()
            .service()
            .remember(
                context.clone(),
                MemoryRememberParams {
                    scope,
                    category: input.category,
                    namespace: None,
                    key,
                    content,
                    sensitivity: input.sensitivity,
                    confidence: input.confidence,
                    importance: input.importance,
                    provenance: Some(provenance),
                    idempotency_key,
                    supersedes: None,
                    metadata: BTreeMap::new(),
                },
            )
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

        self.processor
            .send_memory_changed_after_tool_remember(&context, &response)
            .await;

        Ok(function_output(remember_output(&response)))
    }

    async fn handle_forget(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: MemoryForgetToolInput = decode_tool_args(invocation)?;
        let memory_id = optional_trimmed(input.memory_id);
        let key = optional_trimmed(input.key);
        if memory_id.is_some() == key.is_some() {
            return Err(ToolError::invalid_arguments(
                "exactly one of memoryId or key is required",
            ));
        }

        let target = if let Some(memory_id) = memory_id {
            MemoryForgetTarget::Id { memory_id }
        } else {
            let key = key.expect("checked above");
            let scope_kind = input.scope.ok_or_else(|| {
                ToolError::invalid_arguments("scope is required for key forget in Phase 09")
            })?;
            MemoryForgetTarget::ScopedKey {
                scope: self.scope_for_kind(scope_kind)?,
                namespace: None,
                key,
            }
        };
        let reason = optional_trimmed(input.reason);
        let actor = assistant_actor(&self.context);
        let context = self.operation_context(Some(actor.clone()))?;
        let response = self
            .processor
            .memory_runtime()
            .service()
            .forget(
                context.clone(),
                MemoryForgetParams {
                    target,
                    reason: reason.clone(),
                    actor: Some(actor),
                    dry_run: input.dry_run,
                },
            )
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

        self.processor
            .send_memory_forgotten_after_tool_forget(
                &context,
                reason.clone(),
                input.dry_run,
                &response,
            )
            .await;

        Ok(function_output(json!({
            "forgottenMemoryIds": response.forgotten_memory_ids,
            "dryRun": response.dry_run,
            "reason": reason,
        })))
    }

    fn operation_context(
        &self,
        actor: Option<MemoryActor>,
    ) -> Result<MemoryOperationContext, ToolError> {
        let runtime = self.processor.memory_runtime();
        runtime
            .ensure_enabled()
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        Ok(runtime.operation_context_for_turn(&self.context, actor))
    }

    fn scopes_for_kinds(&self, kinds: &[MemoryScopeKind]) -> Result<Vec<MemoryScope>, ToolError> {
        kinds
            .iter()
            .copied()
            .map(|kind| self.scope_for_kind(kind))
            .collect()
    }

    fn scope_for_kind(&self, kind: MemoryScopeKind) -> Result<MemoryScope, ToolError> {
        match kind {
            MemoryScopeKind::User => Ok(MemoryScope {
                kind,
                key: "default".to_owned(),
            }),
            MemoryScopeKind::Workspace => Ok(MemoryScope {
                kind,
                key: self.context.workspace_id.clone(),
            }),
            MemoryScopeKind::Thread => Ok(MemoryScope {
                kind,
                key: self.context.thread_id.clone(),
            }),
            MemoryScopeKind::Task => {
                let Some(task_id) = self.context.task_id.as_deref() else {
                    return Err(ToolError::invalid_arguments(
                        "task scope requires an active task id",
                    ));
                };
                Ok(MemoryScope {
                    kind,
                    key: task_id.to_owned(),
                })
            }
            MemoryScopeKind::Agent => {
                let Some(agent_id) = self.context.agent_id.as_deref() else {
                    return Err(ToolError::invalid_arguments(
                        "agent scope requires an active agent id",
                    ));
                };
                Ok(MemoryScope {
                    kind,
                    key: workspace_agent_memory_scope_key(
                        self.context.workspace_id.as_str(),
                        agent_id,
                    ),
                })
            }
        }
    }

    fn default_scope_for_category(
        &self,
        category: MemoryCategory,
    ) -> Result<MemoryScope, ToolError> {
        match category {
            MemoryCategory::Identity
            | MemoryCategory::Preference
            | MemoryCategory::Biography
            | MemoryCategory::Relationship
            | MemoryCategory::CommunicationStyle => self.scope_for_kind(MemoryScopeKind::User),
            MemoryCategory::ProjectFact
            | MemoryCategory::ProjectDecision
            | MemoryCategory::Procedure
            | MemoryCategory::Constraint => self.scope_for_kind(MemoryScopeKind::Workspace),
            MemoryCategory::Todo => self.scope_for_kind(MemoryScopeKind::Thread),
            MemoryCategory::Custom => Err(ToolError::invalid_arguments(
                "custom memory requires an explicit scope",
            )),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MemorySearchToolInput {
    query: Option<String>,
    #[serde(default)]
    scopes: Vec<MemoryScopeKind>,
    #[serde(default)]
    categories: Vec<MemoryCategory>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    include_provenance: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MemoryGetToolInput {
    #[serde(default)]
    memory_id: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    scope: Option<MemoryScopeKind>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MemoryRememberToolInput {
    content: String,
    category: MemoryCategory,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    scope: Option<MemoryScopeKind>,
    #[serde(default)]
    sensitivity: Option<MemorySensitivity>,
    #[serde(default)]
    confidence: Option<f32>,
    #[serde(default)]
    importance: Option<f32>,
    #[serde(default)]
    source: Option<MemoryToolSource>,
    #[serde(default, rename = "idempotency_key")]
    _idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MemoryForgetToolInput {
    #[serde(default)]
    memory_id: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    scope: Option<MemoryScopeKind>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    dry_run: bool,
    #[serde(default, rename = "idempotency_key")]
    _idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MemoryToolSource {
    ExplicitUserRequest,
    AgentInferred,
}

impl MemoryToolSource {
    fn source_kind(self) -> MemorySourceKind {
        match self {
            Self::ExplicitUserRequest => MemorySourceKind::ExplicitUserRequest,
            Self::AgentInferred => MemorySourceKind::AssistantInference,
        }
    }
}

fn memory_tool_specs() -> Vec<ConfiguredToolSpec> {
    vec![
        memory_tool_spec(
            MEMORY_SEARCH_TOOL,
            "Search durable memory in the current active scopes. Use for prior user preferences, identity, dates, relationships, project facts, project decisions, durable procedures, constraints and remembered communication style.",
            memory_search_schema(),
            safe_read_recovery(),
        ),
        memory_tool_spec(
            MEMORY_GET_TOOL,
            "Get exact durable memory details by memory id, or by scoped key when scope is known.",
            memory_get_schema(),
            safe_read_recovery(),
        ),
        memory_tool_spec(
            MEMORY_REMEMBER_TOOL,
            "Store durable memory only when the user explicitly asks or when memory policy allows. Do not use for one-off commands, temporary debugging state, raw logs or secrets unless explicitly requested and policy allows.",
            memory_remember_schema(),
            safe_mutation_recovery(),
        ),
        memory_tool_spec(
            MEMORY_FORGET_TOOL,
            "Forget or suppress durable memory by id or scoped key. This creates a control-plane tombstone and excludes matching records from future recall.",
            memory_forget_schema(),
            safe_mutation_recovery(),
        ),
    ]
}

fn memory_tool_spec(
    name: &str,
    description: &str,
    parameters: JsonValue,
    recovery: ToolRecoveryMetadata,
) -> ConfiguredToolSpec {
    ConfiguredToolSpec::with_output_projection(
        ToolSpec::new(name, description, parameters, PayloadKind::Function).with_recovery(recovery),
        ExecutionClass::Shared,
        dynamic_unknown_output_policy(),
        pioneer_tools::ToolOutputProjectionKind::DynamicGeneric,
    )
}

fn safe_read_recovery() -> ToolRecoveryMetadata {
    ToolRecoveryMetadata {
        retry_class: ToolRetryClass::Transient,
        idempotency_mode: ToolIdempotencyMode::Safe,
        max_attempts: 2,
        can_resume: true,
    }
}

fn safe_mutation_recovery() -> ToolRecoveryMetadata {
    ToolRecoveryMetadata {
        retry_class: ToolRetryClass::Transient,
        idempotency_mode: ToolIdempotencyMode::RequiresKey,
        max_attempts: 1,
        can_resume: false,
    }
}

fn memory_search_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "query": { "type": "string", "minLength": 1 },
            "scopes": {
                "type": "array",
                "items": { "type": "string", "enum": scope_kind_values() }
            },
            "categories": {
                "type": "array",
                "items": { "type": "string", "enum": category_values() }
            },
            "limit": { "type": "integer", "minimum": 1, "maximum": MAX_SEARCH_LIMIT },
            "includeProvenance": { "type": "boolean" }
        },
        "required": ["query"],
        "additionalProperties": false
    })
}

fn memory_get_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "memoryId": { "type": "string" },
            "key": { "type": "string" },
            "scope": { "type": "string", "enum": scope_kind_values() }
        },
        "additionalProperties": false
    })
}

fn memory_remember_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "content": { "type": "string", "minLength": 1 },
            "category": { "type": "string", "enum": category_values() },
            "key": { "type": "string" },
            "scope": { "type": "string", "enum": scope_kind_values() },
            "sensitivity": { "type": "string", "enum": sensitivity_values() },
            "confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
            "importance": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
            "source": { "type": "string", "enum": ["explicit_user_request", "agent_inferred"] },
            "idempotency_key": { "type": "string" }
        },
        "required": ["content", "category"],
        "additionalProperties": false
    })
}

fn memory_forget_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "memoryId": { "type": "string" },
            "key": { "type": "string" },
            "scope": { "type": "string", "enum": scope_kind_values() },
            "reason": { "type": "string" },
            "dryRun": { "type": "boolean" },
            "idempotency_key": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn scope_kind_values() -> Vec<&'static str> {
    vec!["user", "workspace", "thread", "agent", "task"]
}

fn category_values() -> Vec<&'static str> {
    vec![
        "identity",
        "preference",
        "biography",
        "relationship",
        "project_fact",
        "project_decision",
        "procedure",
        "todo",
        "constraint",
        "communication_style",
        "custom",
    ]
}

fn sensitivity_values() -> Vec<&'static str> {
    vec!["normal", "personal", "secret_like", "regulated"]
}

fn decode_tool_args<T>(invocation: ToolInvocation) -> Result<T, ToolError>
where
    T: DeserializeOwned,
{
    let arguments = match invocation.payload {
        ToolPayload::Function { arguments } => arguments,
        other => {
            return Err(ToolError::invalid_arguments(format!(
                "memory tools require function arguments, got {}",
                other.log_payload()
            )));
        }
    };
    serde_json::from_value(arguments).map_err(|error| {
        ToolError::invalid_arguments(format!("invalid memory tool arguments: {error}"))
    })
}

fn required_string(value: Option<&str>, field: &str) -> Result<String, ToolError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ToolError::invalid_arguments(format!(
            "`{field}` must not be empty"
        )));
    };
    Ok(value.to_owned())
}

fn optional_trimmed(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn assistant_actor(context: &MemoryTurnContext) -> MemoryActor {
    MemoryActor {
        kind: MemoryActorKind::Assistant,
        id: context.agent_id.clone(),
    }
}

fn stable_memory_key(category: MemoryCategory, content: &str) -> String {
    let normalized = content.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut hasher = Sha256::new();
    hasher.update(category_fragment(category).as_bytes());
    hasher.update([0]);
    hasher.update(normalized.to_ascii_lowercase().as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("auto:{}:{}", category_fragment(category), &digest[..16])
}

fn category_fragment(category: MemoryCategory) -> &'static str {
    match category {
        MemoryCategory::Identity => "identity",
        MemoryCategory::Preference => "preference",
        MemoryCategory::Biography => "biography",
        MemoryCategory::Relationship => "relationship",
        MemoryCategory::ProjectFact => "project_fact",
        MemoryCategory::ProjectDecision => "project_decision",
        MemoryCategory::Procedure => "procedure",
        MemoryCategory::Todo => "todo",
        MemoryCategory::Constraint => "constraint",
        MemoryCategory::CommunicationStyle => "communication_style",
        MemoryCategory::Custom => "custom",
    }
}

fn search_output(
    hits: &[MemorySearchHit],
    next_cursor: Option<&str>,
    include_provenance: bool,
) -> JsonValue {
    json!({
        "hits": hits
            .iter()
            .map(|hit| search_hit_output(hit, include_provenance))
            .collect::<Vec<_>>(),
        "nextCursor": next_cursor,
    })
}

fn search_hit_output(hit: &MemorySearchHit, include_provenance: bool) -> JsonValue {
    let record = &hit.record;
    let mut object = JsonMap::new();
    object.insert("memoryId".to_owned(), JsonValue::String(record.id.clone()));
    object.insert("scope".to_owned(), to_json_value(&record.scope));
    object.insert("category".to_owned(), to_json_value(&record.category));
    object.insert(
        "key".to_owned(),
        optional_string_json(record.key.as_deref()),
    );
    object.insert(
        "snippet".to_owned(),
        JsonValue::String(
            hit.snippet
                .clone()
                .unwrap_or_else(|| truncate_chars(record.content.as_str(), SNIPPET_MAX_CHARS)),
        ),
    );
    object.insert("score".to_owned(), to_json_value(&hit.score));
    object.insert("matchedTerms".to_owned(), to_json_value(&hit.matched_terms));
    object.insert("updatedAt".to_owned(), JsonValue::from(record.updated_at));
    if include_provenance {
        object.insert("provenance".to_owned(), to_json_value(&record.provenance));
    }
    JsonValue::Object(object)
}

fn remember_output(response: &pioneer_protocol::MemoryRememberResponse) -> JsonValue {
    let mut output = record_output(&response.record, true, true);
    let JsonValue::Object(ref mut object) = output else {
        return output;
    };
    object.insert("created".to_owned(), JsonValue::Bool(response.created));
    object.insert(
        "supersededMemoryId".to_owned(),
        optional_string_json(response.superseded_memory_id.as_deref()),
    );
    output
}

fn record_output(
    record: &MemoryRecord,
    include_content: bool,
    include_provenance: bool,
) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert("memoryId".to_owned(), JsonValue::String(record.id.clone()));
    object.insert("scope".to_owned(), to_json_value(&record.scope));
    object.insert("category".to_owned(), to_json_value(&record.category));
    object.insert(
        "key".to_owned(),
        optional_string_json(record.key.as_deref()),
    );
    if include_content {
        object.insert(
            "content".to_owned(),
            JsonValue::String(record.content.clone()),
        );
    }
    object.insert("status".to_owned(), to_json_value(&record.status));
    object.insert("confidence".to_owned(), json!(record.confidence));
    object.insert("importance".to_owned(), json!(record.importance));
    object.insert("sensitivity".to_owned(), to_json_value(&record.sensitivity));
    object.insert("createdAt".to_owned(), JsonValue::from(record.created_at));
    object.insert("updatedAt".to_owned(), JsonValue::from(record.updated_at));
    if include_provenance {
        object.insert("provenance".to_owned(), to_json_value(&record.provenance));
    }
    JsonValue::Object(object)
}

fn optional_string_json(value: Option<&str>) -> JsonValue {
    value
        .map(|value| JsonValue::String(value.to_owned()))
        .unwrap_or(JsonValue::Null)
}

fn to_json_value<T: Serialize>(value: &T) -> JsonValue {
    serde_json::to_value(value).unwrap_or(JsonValue::Null)
}

fn function_output(payload: JsonValue) -> Box<dyn ToolOutput> {
    let text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string());
    Box::new(FunctionToolOutput::with_payload(text, true, payload))
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for ch in value.chars().take(max_chars) {
        output.push(ch);
    }
    if value.chars().count() > max_chars {
        output.push_str("...");
    }
    output
}
