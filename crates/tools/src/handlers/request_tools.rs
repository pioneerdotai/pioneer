use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::domain::{BuiltinToolDomain, dedupe_request_tools_domains, parse_request_tools_domains};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use crate::visibility::ToolVisibilitySnapshot;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestToolsResult {
    pub added: BTreeMap<String, Vec<String>>,
    pub already_visible: BTreeMap<String, Vec<String>>,
    pub blocked: Vec<RequestToolsDomainDiagnostic>,
    pub unknown_or_unavailable: Vec<RequestToolsDomainDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestToolsDomainDiagnostic {
    pub domain: String,
    pub tools: Vec<String>,
    pub reason: String,
}

#[derive(Clone)]
pub struct RequestToolsHandler {
    visibility: ToolVisibilitySnapshot,
    registered_tool_names: Arc<BTreeSet<String>>,
    blocked_tool_names: Arc<RwLock<BTreeMap<String, String>>>,
}

impl RequestToolsHandler {
    pub fn new(
        visibility: ToolVisibilitySnapshot,
        registered_tool_names: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            visibility,
            registered_tool_names: Arc::new(registered_tool_names.into_iter().collect()),
            blocked_tool_names: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn with_blocked_tool_names(
        mut self,
        blocked_tool_names: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        self.blocked_tool_names = Arc::new(RwLock::new(blocked_tool_names.into_iter().collect()));
        self
    }

    pub fn with_shared_blocked_tool_names(
        mut self,
        blocked_tool_names: Arc<RwLock<BTreeMap<String, String>>>,
    ) -> Self {
        self.blocked_tool_names = blocked_tool_names;
        self
    }

    async fn resolve(
        &self,
        arguments: &serde_json::Value,
    ) -> Result<RequestToolsResult, ToolError> {
        let requested =
            parse_request_tools_domains(arguments).map_err(ToolError::invalid_arguments)?;
        let requested = dedupe_request_tools_domains(requested);
        let visible_tool_names = self
            .visibility
            .get()
            .await
            .into_iter()
            .map(|spec| spec.name)
            .collect::<BTreeSet<_>>();

        Ok(self.resolve_domains(requested, &visible_tool_names))
    }

    fn resolve_domains(
        &self,
        requested: Vec<BuiltinToolDomain>,
        visible_tool_names: &BTreeSet<String>,
    ) -> RequestToolsResult {
        let mut added = BTreeMap::new();
        let mut already_visible = BTreeMap::new();
        let mut blocked = Vec::new();
        let mut unknown_or_unavailable = Vec::new();
        let blocked_tool_names = self
            .blocked_tool_names
            .read()
            .expect("request_tools blocked tool map lock poisoned")
            .clone();

        for domain in requested {
            let domain_name = domain.as_str().to_owned();
            let mut domain_added = Vec::new();
            let mut domain_already_visible = Vec::new();
            let mut domain_blocked = Vec::new();
            let mut domain_unknown_or_unavailable = Vec::new();

            for tool_name in domain.tool_names() {
                if blocked_tool_names.contains_key(*tool_name) {
                    domain_blocked.push((*tool_name).to_owned());
                    continue;
                }

                if !self.registered_tool_names.contains(*tool_name) {
                    domain_unknown_or_unavailable.push((*tool_name).to_owned());
                    continue;
                }

                if visible_tool_names.contains(*tool_name) {
                    domain_already_visible.push((*tool_name).to_owned());
                } else {
                    domain_added.push((*tool_name).to_owned());
                }
            }

            if !domain_added.is_empty() {
                added.insert(domain_name.clone(), domain_added);
            }
            if !domain_already_visible.is_empty() {
                already_visible.insert(domain_name.clone(), domain_already_visible);
            }
            if !domain_blocked.is_empty() {
                let reason = domain_blocked
                    .iter()
                    .filter_map(|tool_name| blocked_tool_names.get(tool_name))
                    .next()
                    .cloned()
                    .unwrap_or_else(|| "blocked_by_host_policy".to_owned());
                blocked.push(RequestToolsDomainDiagnostic {
                    domain: domain_name.clone(),
                    tools: domain_blocked,
                    reason,
                });
            }
            if !domain_unknown_or_unavailable.is_empty() {
                unknown_or_unavailable.push(RequestToolsDomainDiagnostic {
                    domain: domain_name,
                    tools: domain_unknown_or_unavailable,
                    reason: "not_registered_or_unavailable".to_owned(),
                });
            }
        }

        RequestToolsResult {
            added,
            already_visible,
            blocked,
            unknown_or_unavailable,
        }
    }
}

#[async_trait]
impl ToolHandler for RequestToolsHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(ToolError::invalid_arguments(
                "expected function payload for `request_tools`",
            ));
        };
        let result = self.resolve(&arguments).await?;
        let payload = serde_json::to_value(&result).map_err(|error| {
            ToolError::internal(format!("failed to encode request_tools result: {error}"))
        })?;
        let text = serde_json::to_string_pretty(&payload).map_err(|error| {
            ToolError::internal(format!("failed to render request_tools result: {error}"))
        })?;

        Ok(Box::new(FunctionToolOutput::with_payload(
            text, true, payload,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{PayloadKind, ToolSpec};

    fn visibility(names: &[&str]) -> ToolVisibilitySnapshot {
        ToolVisibilitySnapshot::new(
            names
                .iter()
                .map(|name| {
                    ToolSpec::new(*name, "test", serde_json::json!({}), PayloadKind::Function)
                })
                .collect(),
        )
    }

    #[tokio::test]
    async fn request_tools_result_reports_added_visible_and_unavailable_tools() {
        let visibility = visibility(&["request_tools", "artifact_prepare"]);
        visibility
            .set_visible_by_name(&["request_tools".to_owned(), "artifact_prepare".to_owned()])
            .await;
        let handler = RequestToolsHandler::new(
            visibility,
            [
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
                "artifact_read".to_owned(),
            ],
        );

        let result = handler
            .resolve(&serde_json::json!({
                "domains": ["artifact", "memory", "artifact"],
                "reason": "Need files and memory."
            }))
            .await
            .expect("valid request_tools arguments should resolve");

        assert_eq!(
            result.added.get("artifact"),
            Some(&vec![
                "artifact_register".to_owned(),
                "artifact_read".to_owned()
            ])
        );
        assert_eq!(
            result.already_visible.get("artifact"),
            Some(&vec!["artifact_prepare".to_owned()])
        );
        assert_eq!(result.unknown_or_unavailable.len(), 1);
        assert_eq!(result.unknown_or_unavailable[0].domain, "memory");
        assert_eq!(result.unknown_or_unavailable[0].tools.len(), 5);
        assert!(result.blocked.is_empty());
    }

    #[tokio::test]
    async fn request_tools_result_reports_host_policy_blocked_tools() {
        let visibility = visibility(&["request_tools"]);
        let handler = RequestToolsHandler::new(
            visibility,
            [
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
                "artifact_read".to_owned(),
            ],
        )
        .with_blocked_tool_names([(
            "artifact_register".to_owned(),
            "blocked_by_host_policy".to_owned(),
        )]);

        let result = handler
            .resolve(&serde_json::json!({
                "domains": ["artifact"],
                "reason": "Need file artifact tools."
            }))
            .await
            .expect("valid request_tools arguments should resolve");

        assert_eq!(
            result.added.get("artifact"),
            Some(&vec![
                "artifact_prepare".to_owned(),
                "artifact_read".to_owned()
            ])
        );
        assert_eq!(result.blocked.len(), 1);
        assert_eq!(result.blocked[0].tools, vec!["artifact_register"]);
        assert!(result.unknown_or_unavailable.is_empty());
    }

    #[tokio::test]
    async fn request_tools_result_reports_repeated_visible_domain_as_already_visible() {
        let visibility = visibility(&[
            "request_tools",
            "artifact_prepare",
            "artifact_register",
            "artifact_read",
        ]);
        visibility
            .set_visible_by_name(&[
                "request_tools".to_owned(),
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
                "artifact_read".to_owned(),
            ])
            .await;
        let handler = RequestToolsHandler::new(
            visibility,
            [
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
                "artifact_read".to_owned(),
            ],
        );

        let result = handler
            .resolve(&serde_json::json!({
                "domains": ["artifact", "artifact"],
                "reason": "Need artifact tools again."
            }))
            .await
            .expect("valid request_tools arguments should resolve");

        assert!(result.added.is_empty());
        assert_eq!(
            result.already_visible.get("artifact"),
            Some(&vec![
                "artifact_prepare".to_owned(),
                "artifact_register".to_owned(),
                "artifact_read".to_owned()
            ])
        );
        assert!(result.blocked.is_empty());
        assert!(result.unknown_or_unavailable.is_empty());
    }

    #[tokio::test]
    async fn request_tools_result_reports_computer_use_only_when_registered() {
        let visibility = visibility(&["request_tools"]);
        let available_handler =
            RequestToolsHandler::new(visibility.clone(), ["computer_use".to_owned()]);

        let available = available_handler
            .resolve(&serde_json::json!({
                "domains": ["computer_use"],
                "reason": "Need GUI tools."
            }))
            .await
            .expect("registered computer_use should resolve");

        assert_eq!(
            available.added.get("computer_use"),
            Some(&vec!["computer_use".to_owned()])
        );
        assert!(available.already_visible.is_empty());
        assert!(available.blocked.is_empty());
        assert!(available.unknown_or_unavailable.is_empty());

        let unavailable_handler = RequestToolsHandler::new(visibility, Vec::<String>::new());
        let unavailable = unavailable_handler
            .resolve(&serde_json::json!({
                "domains": ["computer_use"],
                "reason": "Need GUI tools."
            }))
            .await
            .expect("unregistered computer_use should resolve as unavailable");

        assert!(unavailable.added.is_empty());
        assert!(unavailable.already_visible.is_empty());
        assert!(unavailable.blocked.is_empty());
        assert_eq!(unavailable.unknown_or_unavailable.len(), 1);
        assert_eq!(unavailable.unknown_or_unavailable[0].domain, "computer_use");
        assert_eq!(
            unavailable.unknown_or_unavailable[0].tools,
            vec!["computer_use".to_owned()]
        );
    }

    #[tokio::test]
    async fn request_tools_result_does_not_embed_hidden_schemas() {
        let visibility = visibility(&["request_tools"]);
        let handler = RequestToolsHandler::new(
            visibility,
            ["task_create".to_owned(), "task_wait".to_owned()],
        );

        let result = handler
            .handle(
                ToolInvocation {
                    call_id: "call_request_tools".to_owned(),
                    tool_name: "request_tools".to_owned(),
                    source: crate::context::ToolCallSource::Model,
                    payload: ToolPayload::Function {
                        arguments: serde_json::json!({
                            "domains": ["task"],
                            "reason": "Need task tools."
                        }),
                    },
                    workdir: std::env::current_dir().expect("cwd should be available"),
                    environment: BTreeMap::new(),
                    attempt_id: 1,
                    idempotency_key: None,
                    recovery: crate::spec::ToolRecoveryMetadata::default(),
                    cancellation: tokio_util::sync::CancellationToken::new(),
                },
                crate::events::ToolEventBus::default().start_trace(
                    "turn_request_tools",
                    "call_request_tools",
                    "request_tools",
                ),
            )
            .await
            .expect("request_tools should return compact result");

        let text = result.raw_text();
        assert!(text.contains("\"added\""));
        assert!(!text.contains("\"parameters\""));
        assert!(!text.contains("\"properties\""));
        assert!(!text.contains("\"additionalProperties\""));
    }
}
