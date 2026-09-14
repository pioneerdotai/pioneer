use super::MessageProcessor;
use crate::authorization::{
    AuthorizationDecision, AuthorizationResolver, AuthorizationService, DenyReason,
    ProofResolution, ResourceAction,
};
use anyhow::Result;
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_protocol::{
    ThreadToolResultCursor, ThreadToolResultReadParams, ThreadToolResultReadResponse,
};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolError,
    ToolExtensionBundle, ToolHandler, ToolInvocation, ToolOutput, ToolPayload, ToolSpec,
};
use std::sync::Arc;

pub(super) const RESULT_READ_TOOL: &str = "threads_tools_result_read";

impl MessageProcessor {
    /// Caller supplies a centrally-authorized thread proof or revalidated tool authority.
    pub(super) async fn read_thread_tool_result(
        &self,
        params: ThreadToolResultReadParams,
    ) -> Result<ThreadToolResultReadResponse> {
        if params.output_log {
            return self.read_thread_tool_output_log(params).await;
        }
        let offset = params.cursor.as_ref().map_or(0, |c| c.offset);
        let fragment = self
            .crud_store
            .compaction_tool_result_fragment(
                &params.workspace_id,
                &params.thread_id,
                &params.turn_id,
                &params.item_id,
                params.cursor.as_ref().map(|c| c.version.as_str()),
                offset,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("tool result source is unavailable"))?;
        let max_tokens = params.max_tokens.unwrap_or(4096).min(4096);
        let max_bytes = params.max_bytes.unwrap_or(32768).min(32768) as usize;
        let mut token_budget = max_tokens;
        let mut byte_budget = max_bytes;
        loop {
            let page = pioneer_compaction::results::result_page(
                &fragment.text,
                &fragment.reference.version,
                None,
                token_budget,
                byte_budget,
            )?;
            let eof = page.eof && fragment.next_character.is_none();
            let next_offset = offset
                .checked_add(page.text.chars().count() as u64)
                .ok_or_else(|| anyhow::anyhow!("cursor overflow"))?;
            let result = ThreadToolResultReadResponse {
                text: page.text,
                next: (!eof).then(|| ThreadToolResultCursor {
                    version: fragment.reference.version.clone(),
                    offset: next_offset,
                }),
                eof,
            };
            let encoded = serde_json::to_string(&result)?;
            let token_excess = pioneer_compaction::text_tokens(&encoded).saturating_sub(max_tokens);
            let byte_excess = encoded.len().saturating_sub(max_bytes);
            if token_excess == 0 && byte_excess == 0 {
                return Ok(result);
            }
            token_budget = token_budget.saturating_sub(token_excess);
            byte_budget = byte_budget.saturating_sub(byte_excess);
        }
    }

    async fn read_thread_tool_output_log(
        &self,
        params: ThreadToolResultReadParams,
    ) -> Result<ThreadToolResultReadResponse> {
        let ordinal = params
            .cursor
            .as_ref()
            .map(|cursor| {
                cursor
                    .version
                    .strip_prefix("output:")
                    .and_then(|s| s.parse::<i64>().ok())
                    .filter(|n| *n > 0)
                    .ok_or_else(|| anyhow::anyhow!("invalid output cursor"))
            })
            .transpose()?;
        let rows = self
            .crud_store
            .tool_output_page(
                &params.workspace_id,
                &params.thread_id,
                &params.turn_id,
                &params.item_id,
                ordinal.map_or(0, |n| n - 1),
            )
            .await?;
        let first = rows
            .first()
            .ok_or_else(|| anyhow::anyhow!("tool output log is unavailable"))?;
        anyhow::ensure!(
            ordinal.is_none_or(|n| n == first.0),
            "output cursor is stale"
        );
        let text = serde_json::to_string(&serde_json::json!({
            "stream": serde_json::from_str::<serde_json::Value>(&first.1.stream)?,
            "text": first.1.text,
            "metadata": first.1.metadata.as_ref().map(|m| serde_json::from_str::<serde_json::Value>(m)).transpose()?,
        }))?;
        let version = format!("output:{}", first.0);
        let offset = params.cursor.as_ref().map_or(0, |c| c.offset);
        anyhow::ensure!(
            offset <= text.chars().count() as u64,
            "output cursor is stale"
        );
        let remainder = text.chars().skip(offset as usize).collect::<String>();
        let max_tokens = params.max_tokens.unwrap_or(4096).min(4096);
        let max_bytes = params.max_bytes.unwrap_or(32768).min(32768) as usize;
        let mut token_budget = max_tokens;
        let mut byte_budget = max_bytes;
        loop {
            let page = pioneer_compaction::results::result_page(
                &remainder,
                &version,
                None,
                token_budget,
                byte_budget,
            )?;
            let next = if !page.eof {
                Some(ThreadToolResultCursor {
                    version: version.clone(),
                    offset: offset + page.text.chars().count() as u64,
                })
            } else {
                rows.get(1).map(|row| ThreadToolResultCursor {
                    version: format!("output:{}", row.0),
                    offset: 0,
                })
            };
            let response = ThreadToolResultReadResponse {
                text: page.text,
                eof: next.is_none(),
                next,
            };
            let encoded = serde_json::to_string(&response)?;
            let token_excess = pioneer_compaction::text_tokens(&encoded).saturating_sub(max_tokens);
            let byte_excess = encoded.len().saturating_sub(max_bytes);
            if token_excess == 0 && byte_excess == 0 {
                return Ok(response);
            }
            token_budget = token_budget.saturating_sub(token_excess);
            byte_budget = byte_budget.saturating_sub(byte_excess);
        }
    }

    pub(super) fn thread_result_tools(
        self: &Arc<Self>,
        context: TurnToolContext,
    ) -> ToolExtensionBundle {
        let spec = ToolSpec::new(
            RESULT_READ_TOOL,
            "Read a saved full tool result as bounded text pages of canonical JSON. Follow the version-bound cursor until eof. Attachment references remain references. Set output_log=true to read saved stdout/stderr/progress chunks (including interrupted commands); each chunk is a separate JSON object, and eof refers to the currently saved log.",
            serde_json::json!({"type":"object","properties":{
                "workspace_id":{"type":"string"},"thread_id":{"type":"string"},"turn_id":{"type":"string"},"item_id":{"type":"string"},
                "output_log":{"type":"boolean","default":false},
                "cursor":{"type":"object","properties":{"version":{"type":"string"},"offset":{"type":"integer","minimum":0}},"required":["version","offset"]},
                "max_tokens":{"type":"integer","minimum":1,"maximum":4096},"max_bytes":{"type":"integer","minimum":1,"maximum":32768}},
                "required":["workspace_id","thread_id","turn_id","item_id"],"additionalProperties":false}),
            PayloadKind::Function,
        );
        ToolExtensionBundle {
            specs: vec![ConfiguredToolSpec::new(
                spec,
                ExecutionClass::Shared,
                pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(RESULT_READ_TOOL),
            )],
            handlers: vec![(
                RESULT_READ_TOOL.into(),
                Arc::new(ResultReadHandler {
                    processor: self.clone(),
                    context,
                }),
            )],
        }
    }
}
struct ResultReadHandler {
    processor: Arc<MessageProcessor>,
    context: TurnToolContext,
}
#[async_trait]
impl ToolHandler for ResultReadHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(ToolError::invalid_arguments("expected function arguments"));
        };
        let params: ThreadToolResultReadParams = serde_json::from_value(arguments)
            .map_err(|_| ToolError::invalid_arguments("invalid result/read parameters"))?;
        let action = ResourceAction::ThreadRead;
        let current = self
            .processor
            .revalidate_tool_execution_authorization(
                &self.context.workspace_id,
                &self.context.thread_id,
                &self.context.turn_id,
                None,
                action,
            )
            .await
            .map_err(|_| ToolError::invalid_arguments("source unavailable or access denied"))?;
        if params.workspace_id != self.context.workspace_id {
            return Err(ToolError::invalid_arguments(
                "source unavailable or access denied",
            ));
        }
        let gate = AuthorizationService::new().authorize_action(
            current.principal().kind,
            current.principal().role_key.as_ref(),
            action,
        );
        let resolver = AuthorizationResolver::new(self.processor.crud_store.as_ref().clone());
        let mut proof = resolver
            .authorize_thread(
                current.principal(),
                &gate,
                action,
                &params.thread_id,
                Some(&params.workspace_id),
            )
            .await;
        if matches!(
            proof.as_ref().ok().and_then(|p| p.denial()),
            Some(AuthorizationDecision::Deny {
                reason: DenyReason::MissingAuthoritativeResource,
                ..
            })
        ) {
            proof = resolver
                .authorize_internal_thread_via_root(
                    current.principal(),
                    &gate,
                    action,
                    &params.thread_id,
                    Some(&params.workspace_id),
                )
                .await;
        }
        if !matches!(proof, Ok(ProofResolution::Authorized(_))) {
            return Err(ToolError::invalid_arguments(
                "source unavailable or access denied",
            ));
        }
        let result = self
            .processor
            .read_thread_tool_result(params)
            .await
            .map_err(|_| {
                ToolError::invalid_arguments(
                    "result unavailable, cursor stale, or page budget too small",
                )
            })?;
        let text = serde_json::to_string(&result)
            .map_err(|_| ToolError::invalid_arguments("result encoding failed"))?;
        Ok(Box::new(FunctionToolOutput::new(text, true)))
    }
}
