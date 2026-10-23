use super::MessageProcessor;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_protocol::{
    ThreadToolResultCursor, ThreadToolResultReadParams, ThreadToolResultReadResponse,
};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolError,
    ToolExtensionBundle, ToolHandler, ToolInvocation, ToolOutput, ToolPayload, ToolSpec,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub(super) const RESULT_READ_TOOL: &str = "threads_tools_result_read";

impl MessageProcessor {
    /// Caller supplies a centrally-authorized thread proof or revalidated tool authority.
    pub(super) async fn read_thread_tool_result(
        &self,
        params: ThreadToolResultReadParams,
    ) -> Result<ThreadToolResultReadResponse> {
        let source = self
            .crud_store
            .get_thread_by_id(&params.thread_id)
            .await?
            .filter(|thread| thread.workspace_id == params.workspace_id)
            .ok_or_else(|| anyhow::anyhow!("tool result source is unavailable"))?;
        let turns = self
            .crud_store
            .get_turns_by_thread_and_ids(&source.id, std::slice::from_ref(&params.turn_id))
            .await?;
        ensure!(
            turns
                .get(&params.turn_id)
                .is_some_and(|turn| !turn.message_deleted),
            "tool result source is unavailable"
        );
        if params.output_log {
            return self.read_thread_tool_output_log(params).await;
        }
        let offset = params.cursor.as_ref().map_or(0, |c| c.offset);
        let item_reference = self
            .crud_store
            .compaction_tool_item_reference(
                &params.workspace_id,
                &params.thread_id,
                &params.turn_id,
                &params.item_id,
            )
            .await?;
        let context_reference = self
            .crud_store
            .compaction_context_reference_for_item(
                &params.workspace_id,
                &params.thread_id,
                &params.turn_id,
                &params.item_id,
                "tool_result_v2",
            )
            .await?;
        let references = item_reference
            .into_iter()
            .chain(context_reference)
            .collect::<Vec<_>>();
        if !references.is_empty() {
            self.crud_store
                .compaction_prepare_references(&params.workspace_id, &params.thread_id, &references)
                .await?;
        }
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
            .await?;
        let Some(fragment) = fragment else {
            return self.read_saved_tool_item_fallback(params).await;
        };
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

    // Saved observation results carry ACL-protected metadata of OTHER sources.
    // Replaying their byte cursor must not bypass those sources' current ACLs.
    // Reader responses are bounded to 32KiB; search pages to 2MiB. Only their
    // known public envelopes are decoded, never arbitrary user text/receipts.
    pub(super) async fn saved_thread_read_result_disclosable(
        &self,
        authority: &super::thread_read_tools::ThreadReadAuthority,
        params: &ThreadToolResultReadParams,
        item: &pioneer_protocol::TurnItem,
    ) -> Result<bool> {
        let mut target = params.clone();
        let mut value = serde_json::to_value(item)?;
        let mut seen = std::collections::HashSet::new();
        // A saved result-reader call may itself replay an earlier reader. Follow
        // exact stored parameter references, reauthorizing every source, so a
        // cache of a cache cannot turn a revoked source into readable content.
        while value["toolName"].as_str() == Some(RESULT_READ_TOOL) {
            let nested: ThreadToolResultReadParams =
                serde_json::from_value(value["arguments"].clone())?;
            if nested.workspace_id != target.workspace_id
                || !self
                    .thread_source_readable(authority, &nested.thread_id)
                    .await?
            {
                return Ok(false);
            }
            if !seen.insert((
                nested.thread_id.clone(),
                nested.turn_id.clone(),
                nested.item_id.clone(),
            )) {
                return Ok(false);
            }
            let turns = self
                .crud_store
                .get_turns_by_thread_and_ids(
                    &nested.thread_id,
                    std::slice::from_ref(&nested.turn_id),
                )
                .await?;
            if !turns
                .get(&nested.turn_id)
                .is_some_and(|turn| !turn.message_deleted)
            {
                return Ok(false);
            }
            let Some(nested_item) = self
                .crud_store
                .get_turn_item(&nested.turn_id, &nested.item_id)
                .await?
            else {
                return Ok(false);
            };
            if !nested_item.is_tool_item()
                || pioneer_crud::classify_turn_item_with_db_status(&nested_item, None).visibility
                    != pioneer_crud::ProjectionVisibility::Visible
            {
                return Ok(false);
            }
            value = serde_json::to_value(nested_item)?;
            target = nested;
        }
        let params = &target;
        let Some(name) = value["toolName"].as_str().filter(|name| {
            super::thread_read_tools::THREAD_READ_TOOL_NAMES.contains(name)
                || *name == super::thread_search_tools::SEARCH_TOOL
        }) else {
            return Ok(true);
        };
        if params.output_log {
            return Ok(false);
        }
        if let Some(target) = value["arguments"]["threadId"].as_str() {
            if !self.thread_source_readable(authority, target).await? {
                return Ok(false);
            }
        }
        let mut source = params.clone();
        source.cursor = None;
        source.max_bytes = Some(32768);
        source.max_tokens = Some(4096);
        let mut encoded = String::new();
        let mut complete = false;
        let search_result = name == super::thread_search_tools::SEARCH_TOOL;
        let mut page_count = 0;
        loop {
            if !search_result && page_count == 32 {
                break;
            }
            page_count += 1;
            let page = self.read_thread_tool_result(source.clone()).await?;
            encoded.push_str(&page.text);
            if !search_result && encoded.len() > 256 * 1024 {
                return Ok(false);
            }
            if page.eof {
                complete = true;
                break;
            }
            ensure!(
                page.next.is_some()
                    && page.next.as_ref().map(|c| (&c.version, c.offset))
                        != source.cursor.as_ref().map(|c| (&c.version, c.offset)),
                "saved result continuation did not advance"
            );
            source.cursor = page.next;
        }
        if !complete {
            return Ok(false);
        }
        let root: serde_json::Value = serde_json::from_str(&encoded)?;
        let Some(response) = saved_read_response(root, name) else {
            return Ok(false);
        };
        if search_result {
            return self.search_response_readable(authority, &response).await;
        }
        let mut references = Vec::new();
        if let Some(thread) = response["threadId"].as_str() {
            references.push(thread.to_owned());
        }
        if let Some(rows) = response["threads"].as_array() {
            for row in rows {
                for field in ["threadId", "parentThreadId", "rootThreadId"] {
                    if let Some(thread) = row[field].as_str() {
                        references.push(thread.to_owned());
                    }
                }
            }
        }
        if let Some(events) = response["events"].as_array() {
            for event in events {
                let reference = event
                    .get("sourceThreadReference")
                    .or_else(|| event["content"].get("sourceThreadReference"));
                if let Some(thread) = reference.and_then(|r| r["threadId"].as_str()) {
                    references.push(thread.to_owned());
                }
                if let Some(thread) = event["childReference"]["threadId"].as_str() {
                    references.push(thread.to_owned());
                }
            }
        }
        references.sort();
        references.dedup();
        for reference in references {
            if !self.thread_source_readable(authority, &reference).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Legacy/native tools can retain a typed item without tool_result_v2 or
    /// shell storage. Page that existing saved view rather than publish a dead
    /// reference or claim an unsaved provider result can be reconstructed.
    async fn read_saved_tool_item_fallback(
        &self,
        params: ThreadToolResultReadParams,
    ) -> Result<ThreadToolResultReadResponse> {
        let item = self
            .crud_store
            .get_turn_item(&params.turn_id, &params.item_id)
            .await?
            .filter(|item| item.is_tool_item())
            .ok_or_else(|| anyhow::anyhow!("tool result source is unavailable"))?;
        let mut saved = serde_json::to_value(item)?;
        if let Some(object) = saved.as_object_mut() {
            for key in ["recovery", "recoveryPolicy", "outputPolicy", "observation"] {
                object.remove(key);
            }
        }
        let text = serde_json::to_string(
            &serde_json::json!({"kind":"saved_item", "fullResultSaved":false, "item":saved}),
        )?;
        let version = format!(
            "saved-item:{}",
            hex::encode(Sha256::digest(text.as_bytes()))
        );
        if let Some(cursor) = &params.cursor {
            ensure!(cursor.version == version, "tool result cursor is stale");
        }
        let offset = params.cursor.as_ref().map_or(0, |c| c.offset);
        let count = text.chars().count() as u64;
        ensure!(offset <= count, "invalid tool result cursor");
        let remaining = text
            .chars()
            .skip(usize::try_from(offset)?)
            .collect::<String>();
        let max_tokens = params.max_tokens.unwrap_or(4096).min(4096);
        let max_bytes = params.max_bytes.unwrap_or(32768).min(32768) as usize;
        let mut tokens = max_tokens;
        let mut bytes = max_bytes;
        loop {
            let page = pioneer_compaction::results::result_page(
                &remaining, &version, None, tokens, bytes,
            )?;
            let end = offset + page.text.chars().count() as u64;
            let response = ThreadToolResultReadResponse {
                text: page.text,
                eof: end == count,
                next: (end < count).then(|| ThreadToolResultCursor {
                    version: version.clone(),
                    offset: end,
                }),
            };
            let encoded = serde_json::to_string(&response)?;
            let token_excess = pioneer_compaction::text_tokens(&encoded).saturating_sub(max_tokens);
            let byte_excess = encoded.len().saturating_sub(max_bytes);
            if token_excess == 0 && byte_excess == 0 {
                return Ok(response);
            }
            tokens = tokens.saturating_sub(token_excess);
            bytes = bytes.saturating_sub(byte_excess);
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

    pub(crate) fn thread_result_tools(
        self: &Arc<Self>,
        context: TurnToolContext,
    ) -> ToolExtensionBundle {
        ToolExtensionBundle {
            specs: thread_result_tool_specs(),
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
        let denied = || ToolError::invalid_arguments("source unavailable or access denied");
        if params.workspace_id != self.context.workspace_id {
            return Err(denied());
        }
        let authority = self
            .processor
            .thread_read_authority(&self.context)
            .await
            .map_err(|_| denied())?;
        if !self
            .processor
            .thread_source_readable(&authority, &params.thread_id)
            .await
            .map_err(|_| denied())?
        {
            return Err(denied());
        }
        let item = self
            .processor
            .crud_store
            .get_turn_item(&params.turn_id, &params.item_id)
            .await
            .map_err(|_| denied())?
            .ok_or_else(denied)?;
        let turns = self
            .processor
            .crud_store
            .get_turns_by_thread_and_ids(&params.thread_id, std::slice::from_ref(&params.turn_id))
            .await
            .map_err(|_| denied())?;
        if !turns
            .get(&params.turn_id)
            .is_some_and(|turn| !turn.message_deleted)
            || !item.is_tool_item()
            || pioneer_crud::classify_turn_item_with_db_status(&item, None).visibility
                != pioneer_crud::ProjectionVisibility::Visible
        {
            return Err(denied());
        }
        if !self
            .processor
            .saved_thread_read_result_disclosable(&authority, &params, &item)
            .await
            .map_err(|_| denied())?
        {
            return Err(denied());
        }
        let source_thread_id = params.thread_id.clone();
        let result = self
            .processor
            .read_thread_tool_result(params.clone())
            .await
            .map_err(|_| {
                ToolError::invalid_arguments(
                    "result unavailable, cursor stale, or page budget too small",
                )
            })?;
        let current = self
            .processor
            .thread_read_authority(&self.context)
            .await
            .map_err(|_| denied())?;
        if !self
            .processor
            .thread_source_readable(&current, &source_thread_id)
            .await
            .map_err(|_| denied())?
            || !self
                .processor
                .saved_thread_read_result_disclosable(&current, &params, &item)
                .await
                .map_err(|_| denied())?
        {
            return Err(denied());
        }
        let text = serde_json::to_string(&result)
            .map_err(|_| ToolError::invalid_arguments("result encoding failed"))?;
        Ok(Box::new(FunctionToolOutput::new(text, true)))
    }
}

pub(crate) fn thread_result_tool_specs() -> Vec<ConfiguredToolSpec> {
    let spec = ToolSpec::new(
        RESULT_READ_TOOL,
        "Read a saved full tool result as bounded text pages of canonical JSON. Follow the version-bound cursor until eof. Attachment references remain references. Legacy saved-item views explicitly report fullResultSaved=false when a separate full provider result was never retained. Set output_log=true to read saved stdout/stderr/progress chunks (including interrupted commands); each chunk is a separate JSON object, and eof refers to the currently saved log.",
        serde_json::json!({"type":"object","properties":{
                "workspace_id":{"type":"string"},"thread_id":{"type":"string"},"turn_id":{"type":"string"},"item_id":{"type":"string"},
                "output_log":{"type":"boolean","default":false},
                "cursor":{"type":"object","properties":{"version":{"type":"string"},"offset":{"type":"integer","minimum":0}},"required":["version","offset"]},
                "max_tokens":{"type":"integer","minimum":1,"maximum":4096},"max_bytes":{"type":"integer","minimum":1,"maximum":32768}},
                "required":["workspace_id","thread_id","turn_id","item_id"],"additionalProperties":false}),
        PayloadKind::Function,
    );
    vec![ConfiguredToolSpec::new(
        spec,
        ExecutionClass::Shared,
        pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(RESULT_READ_TOOL),
    )]
}

// Search only the canonical tool-result wrapper vocabulary. A matched public
// response terminates traversal before its user-message contents are inspected.
fn saved_read_response(root: serde_json::Value, name: &str) -> Option<serde_json::Value> {
    use serde_json::Value;
    let mut pending = vec![(root, 0_u8)];
    while let Some((value, depth)) = pending.pop() {
        if depth > 16 {
            continue;
        }
        if (name == "threads_history_read" && value["events"].is_array())
            || (name != "threads_history_read" && value["threads"].is_array())
        {
            return Some(value);
        }
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    if [
                        "value",
                        "output",
                        "result",
                        "structuredContent",
                        "content",
                        "text",
                        "item",
                        "storage",
                        "metadata",
                        "stdout",
                        "aggregated_output",
                    ]
                    .contains(&key.as_str())
                    {
                        pending.push((value, depth + 1));
                    }
                }
            }
            Value::Array(array) => {
                for value in array {
                    pending.push((value, depth + 1));
                }
            }
            Value::String(text) => {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
    }
    None
}
