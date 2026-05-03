use crate::context::{
    ToolInvocation, ToolOutput, ToolPayload, ToolSearchOutput, ToolSearchResultTool,
    ToolSuggestOutput,
};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use crate::spec::ToolSpec;
use crate::visibility::ToolVisibilitySnapshot;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Clone)]
pub struct ToolSearchHandler {
    visibility: ToolVisibilitySnapshot,
    policy: ToolDiscoveryPolicy,
}

#[derive(Clone)]
pub struct ToolSuggestHandler {
    visibility: ToolVisibilitySnapshot,
    policy: ToolDiscoveryPolicy,
}

#[derive(Debug, Clone, Copy)]
pub struct ToolDiscoveryPolicy {
    pub allow_hidden: bool,
}

impl Default for ToolDiscoveryPolicy {
    fn default() -> Self {
        Self { allow_hidden: true }
    }
}

#[derive(Debug, Deserialize)]
struct ToolSearchArgs {
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    intent: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_hidden: Option<bool>,
}

impl ToolSearchHandler {
    pub fn new(visibility: ToolVisibilitySnapshot, policy: ToolDiscoveryPolicy) -> Self {
        Self { visibility, policy }
    }
}

impl ToolSuggestHandler {
    pub fn new(visibility: ToolVisibilitySnapshot, policy: ToolDiscoveryPolicy) -> Self {
        Self { visibility, policy }
    }
}

#[async_trait]
impl ToolHandler for ToolSearchHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let (query, limit, include_hidden) = parse_query_and_limit(invocation.payload)?;
        let specs = select_specs(&self.visibility, self.policy, include_hidden).await;
        let matched = search_specs(specs.as_slice(), query.as_str(), limit.unwrap_or(8));

        let output = ToolSearchOutput {
            tools: matched
                .into_iter()
                .map(|spec| ToolSearchResultTool {
                    name: spec.name.clone(),
                    description: spec.description.clone(),
                })
                .collect(),
        };

        Ok(Box::new(output))
    }
}

#[async_trait]
impl ToolHandler for ToolSuggestHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let (query, limit, include_hidden) = parse_query_and_limit(invocation.payload)?;
        let specs = select_specs(&self.visibility, self.policy, include_hidden).await;
        let selected = suggest_specs(specs.as_slice(), query.as_str(), limit.unwrap_or(5));
        Ok(Box::new(ToolSuggestOutput { tools: selected }))
    }
}

fn parse_query_and_limit(payload: ToolPayload) -> Result<(String, Option<usize>, bool), ToolError> {
    match payload {
        ToolPayload::ToolSearch {
            query,
            limit,
            include_hidden,
        } => Ok((query, limit, include_hidden.unwrap_or(false))),
        ToolPayload::Function { arguments } => {
            let parsed = serde_json::from_value::<ToolSearchArgs>(arguments).map_err(|error| {
                ToolError::invalid_arguments(format!(
                    "failed to parse tool_search arguments: {error}"
                ))
            })?;
            let query = parsed
                .query
                .or(parsed.q)
                .or(parsed.intent)
                .unwrap_or_default();
            Ok((query, parsed.limit, parsed.include_hidden.unwrap_or(false)))
        }
        ToolPayload::Custom { input } => {
            let parsed = serde_json::from_str::<JsonValue>(input.as_str()).ok();
            if let Some(value) = parsed {
                let query = value
                    .get("query")
                    .and_then(JsonValue::as_str)
                    .or_else(|| value.get("q").and_then(JsonValue::as_str))
                    .or_else(|| value.get("intent").and_then(JsonValue::as_str))
                    .unwrap_or_default()
                    .to_owned();
                let limit = value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .and_then(|value| usize::try_from(value).ok());
                let include_hidden = value
                    .get("include_hidden")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                return Ok((query, limit, include_hidden));
            }
            Ok((input, None, false))
        }
        other => Err(ToolError::invalid_arguments(format!(
            "unsupported payload for tool discovery: {}",
            other.log_payload()
        ))),
    }
}

async fn select_specs(
    visibility: &ToolVisibilitySnapshot,
    policy: ToolDiscoveryPolicy,
    include_hidden: bool,
) -> Vec<ToolSpec> {
    if include_hidden && policy.allow_hidden {
        return visibility.all_specs().to_vec();
    }
    visibility.get().await
}

fn search_specs<'a>(specs: &'a [ToolSpec], query: &str, limit: usize) -> Vec<&'a ToolSpec> {
    if query.trim().is_empty() {
        return specs.iter().take(limit).collect();
    }

    let q = query.to_lowercase();
    let mut scored: Vec<(&ToolSpec, i32)> = specs
        .iter()
        .map(|spec| {
            let name = spec.name.to_lowercase();
            let description = spec.description.to_lowercase();

            let mut score = 0;
            if name == q {
                score += 100;
            }
            if name.contains(q.as_str()) {
                score += 30;
            }
            if description.contains(q.as_str()) {
                score += 20;
            }

            for token in q.split_whitespace() {
                if name.contains(token) {
                    score += 15;
                }
                if description.contains(token) {
                    score += 8;
                }
            }

            (spec, score)
        })
        .collect();

    scored.sort_by(|left, right| right.1.cmp(&left.1));
    scored
        .into_iter()
        .filter(|(_, score)| *score > 0)
        .take(limit)
        .map(|(spec, _)| spec)
        .collect()
}

fn suggest_specs(specs: &[ToolSpec], query: &str, limit: usize) -> Vec<ToolSearchResultTool> {
    let mut selected = search_specs(specs, query, limit);

    if selected.is_empty() {
        let keywords = query.to_lowercase();
        let fallback = specs.iter().filter(|spec| {
            if keywords.contains("patch") || keywords.contains("edit") {
                return spec.name == "apply_patch" || spec.name == "read_file";
            }
            if keywords.contains("search") || keywords.contains("find") {
                return spec.name == "grep_files"
                    || spec.name == "tool_search"
                    || spec.name == "web_search";
            }
            if keywords.contains("command") || keywords.contains("shell") {
                return spec.name == "exec_command" || spec.name == "write_stdin";
            }
            if keywords.contains("web")
                || keywords.contains("internet")
                || keywords.contains("site")
                || keywords.contains("url")
            {
                return spec.name == "web_search"
                    || spec.name == "web_fetch"
                    || spec.name == "download_url";
            }
            if keywords.contains("download") {
                return spec.name == "download_url";
            }
            false
        });
        selected.extend(fallback.take(limit));
    }

    if selected.is_empty() {
        selected.extend(specs.iter().take(limit));
    }

    selected
        .into_iter()
        .take(limit)
        .map(|spec| ToolSearchResultTool {
            name: spec.name.clone(),
            description: spec.description.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{ToolCallSource, ToolInvocation, ToolPayload};
    use crate::events::ToolEventBus;
    use crate::spec::PayloadKind;
    use std::path::PathBuf;

    fn invocation(query: &str) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_1".to_owned(),
            tool_name: "tool_suggest".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::ToolSearch {
                query: query.to_owned(),
                limit: Some(3),
                include_hidden: Some(false),
            },
            workdir: PathBuf::from("."),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn trace(tool_name: &str) -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn_test", "call_1", tool_name)
    }

    #[tokio::test]
    async fn tool_suggest_returns_structured_output() {
        let specs = vec![ToolSpec::new(
            "read_file",
            "Read file content",
            serde_json::json!({ "type": "object" }),
            PayloadKind::Function,
        )];
        let visibility = ToolVisibilitySnapshot::new(specs);
        let handler = ToolSuggestHandler::new(visibility, ToolDiscoveryPolicy::default());

        let output = handler
            .handle(invocation("read"), trace("tool_suggest"))
            .await
            .expect("tool suggestion should succeed");

        let json = output.raw_json();
        let tools = json
            .get("tools")
            .and_then(JsonValue::as_array)
            .expect("output must include tools array");
        assert!(!tools.is_empty());
    }
}
