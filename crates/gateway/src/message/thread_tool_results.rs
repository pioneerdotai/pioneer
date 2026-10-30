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

#[derive(Clone, Copy)]
struct ResultCursorProtection<'a> {
    codec: &'a super::thread_read_cursor::ThreadReadCursorCodec,
    scope: &'a str,
}

// Encode the delivered representation and its protected cursor. Cursor ciphertext
// is URL-safe ASCII: its length, rather than its random contents, determines the
// shared UTF-16 estimate. Keep the independently checked hard byte limit.
fn result_delivery_encoding(
    mut response: ThreadToolResultReadResponse,
    protection: Option<ResultCursorProtection<'_>>,
) -> Result<(ThreadToolResultReadResponse, String)> {
    let encoded = if let Some(protection) = protection {
        if let Some(next) = &mut response.next {
            next.version = protection.codec.encode(next, protection.scope)?;
        }
        serde_json::to_string(&serde_json::to_value(&response)?)?
    } else {
        // Internal/direct consumers retain their raw source cursor contract.
        serde_json::to_string(&response)?
    };
    Ok((response, encoded))
}
/// Fit text using prepared empty envelopes, then serialize the selected page
/// once. Cursor offsets use source Unicode scalars; budgets use escaped UTF-16
/// units and bytes. The upper-offset envelope bounds every partial candidate.
fn result_delivery_page(
    text: &str,
    max_tokens: u64,
    max_bytes: usize,
    protection: Option<ResultCursorProtection<'_>>,
    make: impl Fn(u64, bool) -> Result<ThreadToolResultReadResponse>,
) -> Result<ThreadToolResultReadResponse> {
    use pioneer_compaction::estimator::{TextSize, json_string_content_size};
    let measure = |response| -> Result<_> {
        let (response, encoded) = result_delivery_encoding(response, protection)?;
        let value = serde_json::from_str(&encoded)?;
        let projection = pioneer_tools::thread_read_page_bytes(&value);
        Ok((
            response,
            TextSize::of(&encoded),
            projection.saturating_sub(encoded.len()),
        ))
    };
    let finish = |mut response: ThreadToolResultReadResponse, end: usize| -> Result<_> {
        response.text = text[..end].into();
        let encoded = serde_json::to_string(&serde_json::to_value(&response)?)?;
        ensure!(
            encoded.len() <= max_bytes
                && pioneer_compaction::text_tokens(&encoded) <= max_tokens
                && pioneer_tools::thread_read_page_fits(&serde_json::from_str(&encoded)?),
            "result page does not fit its delivery budget"
        );
        Ok(response)
    };
    if text.len() <= max_bytes {
        let count = text.chars().count() as u64;
        let (response, frame, overhead) = measure(make(count, true)?)?;
        let size = frame.saturating_add(json_string_content_size(text));
        if size.bytes <= max_bytes
            && size.tokens() <= max_tokens
            && size.bytes.saturating_add(overhead) <= pioneer_protocol::TOOL_RESPONSE_MAX_BYTES
        {
            return finish(response, text.len());
        }
    }
    let (_, frame, overhead) = measure(make(max_bytes as u64, false)?)?;
    let byte_limit =
        max_bytes.min(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES.saturating_sub(overhead));
    let mut content = TextSize::default();
    let mut count = 0u64;
    let mut end = 0;
    for (offset, ch) in text.char_indices() {
        let next_end = offset + ch.len_utf8();
        if next_end == text.len() {
            break;
        } // complete candidate handled above
        let next = content.saturating_add(json_string_content_size(&text[offset..next_end]));
        let size = frame.saturating_add(next);
        if size.bytes > byte_limit || size.tokens() > max_tokens {
            break;
        }
        content = next;
        count += 1;
        end = next_end;
    }
    ensure!(end > 0, "result page cannot make progress within budget");
    let (response, _, _) = measure(make(count, false)?)?;
    finish(response, end)
}

impl MessageProcessor {
    /// Caller supplies a centrally-authorized thread proof or revalidated tool authority.
    pub(super) async fn read_thread_tool_result(
        &self,
        params: ThreadToolResultReadParams,
    ) -> Result<ThreadToolResultReadResponse> {
        self.read_thread_tool_result_with_cursor(params, None).await
    }

    async fn read_thread_tool_result_with_cursor(
        &self,
        params: ThreadToolResultReadParams,
        protection: Option<ResultCursorProtection<'_>>,
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
            return self.read_thread_tool_output_log(params, protection).await;
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
            return self.read_saved_tool_item_fallback(params, protection).await;
        };
        // Source reads remain bounded quanta. Fill at most one public page,
        // instead of exposing the internal 16K-character read as a hidden cap.
        let mut text = fragment.text;
        let version = fragment.reference.version;
        let mut more = fragment.next_character;
        let max_tokens = params.max_tokens.unwrap_or(u64::MAX);
        let max_bytes = params
            .max_bytes
            .unwrap_or(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES as u64)
            .min(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES as u64) as usize;
        let mut text_size = pioneer_compaction::estimator::json_string_content_size(&text);
        while let Some(next_offset) = more {
            let probe = ThreadToolResultReadResponse {
                text: String::new(),
                next: Some(ThreadToolResultCursor {
                    version: version.clone(),
                    offset: next_offset,
                }),
                eof: false,
            };
            let (_, encoded) = result_delivery_encoding(probe, protection)?;
            let frame = pioneer_compaction::estimator::TextSize::of(&encoded);
            let size = frame.saturating_add(text_size);
            let projection =
                pioneer_tools::thread_read_page_bytes(&serde_json::from_str(&encoded)?);
            if size.bytes >= max_bytes
                || projection.saturating_add(text_size.bytes)
                    >= pioneer_protocol::TOOL_RESPONSE_MAX_BYTES
                || size.tokens() >= max_tokens
            {
                break;
            }
            let next = self
                .crud_store
                .compaction_tool_result_fragment(
                    &params.workspace_id,
                    &params.thread_id,
                    &params.turn_id,
                    &params.item_id,
                    Some(&version),
                    next_offset,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("tool result source changed"))?;
            text_size = text_size.saturating_add(
                pioneer_compaction::estimator::json_string_content_size(&next.text),
            );
            text.push_str(&next.text);
            more = next.next_character;
        }
        let max_tokens = params.max_tokens.unwrap_or(u64::MAX);
        let max_bytes = params
            .max_bytes
            .unwrap_or(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES as u64)
            .min(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES as u64) as usize;
        result_delivery_page(
            &text,
            max_tokens,
            max_bytes,
            protection,
            |count, complete| {
                let next_offset = offset
                    .checked_add(count)
                    .ok_or_else(|| anyhow::anyhow!("cursor overflow"))?;
                let eof = complete && more.is_none();
                ensure!(
                    eof || count > 0,
                    "result page cannot advance its source cursor"
                );
                Ok(ThreadToolResultReadResponse {
                    text: String::new(),
                    eof,
                    next: (!eof).then(|| ThreadToolResultCursor {
                        version: version.clone(),
                        offset: next_offset,
                    }),
                })
            },
        )
    }

    // Saved observation results carry ACL-protected metadata of OTHER sources.
    // Replaying their byte cursor must not bypass those sources' current ACLs.
    // Canonical ownership is resolved by exact workspace/thread/turn/item joins.
    // Referenced sources are inspected incrementally, independently of result
    // size and wrapper depth; arbitrary history message text is not traversed.
    pub(super) async fn saved_thread_read_result_disclosable(
        &self,
        authority: &super::thread_read_tools::ThreadReadAuthority,
        params: &ThreadToolResultReadParams,
        item: &serde_json::Value,
    ) -> Result<bool> {
        let mut target = params.clone();
        let mut value = item.clone();
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
            // A saved reader page is authority only for the exact bytes it read.
            // Compare its decoded text incrementally with the canonical source
            // at the original offset. A changed source must not launder stale
            // cached metadata through a currently accessible different result.
            if !nested.output_log && !self.saved_reader_page_matches(&target, &nested).await? {
                return Ok(false);
            }
            if !seen.insert((
                nested.thread_id.clone(),
                nested.turn_id.clone(),
                nested.item_id.clone(),
            )) {
                return Ok(false);
            }
            let Some(nested_item) = self
                .crud_store
                .tool_result_access_source(
                    &nested.workspace_id,
                    &nested.thread_id,
                    &nested.turn_id,
                    &nested.item_id,
                )
                .await?
            else {
                return Ok(false);
            };
            value = nested_item;
            target = nested;
        }
        if value["toolName"].as_str() == Some(super::thread_search_tools::SEARCH_TOOL) {
            if target.output_log {
                return Ok(false);
            }
            let mut source = target.clone();
            source.cursor = None;
            source.max_bytes = Some(32768);
            source.max_tokens = Some(4096);
            let mut encoded = String::new();
            loop {
                let page = self.read_thread_tool_result(source.clone()).await?;
                encoded.push_str(&page.text);
                if page.eof {
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
            let root = serde_json::from_str(&encoded)?;
            let Some(response) = saved_read_response(root, super::thread_search_tools::SEARCH_TOOL)
            else {
                return Ok(false);
            };
            return self.search_response_readable(authority, &response).await;
        }
        let params = &target;
        let Some(_) = value["toolName"]
            .as_str()
            .filter(|name| super::thread_read_tools::THREAD_READ_TOOL_NAMES.contains(name))
        else {
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
        source.max_bytes = None;
        source.max_tokens = None;
        let mut references = super::thread_result_sources::SourceReferences::default();
        loop {
            let page = self.read_thread_tool_result(source.clone()).await?;
            references.feed(&page.text)?;
            for reference in references.take_references() {
                if !self.thread_source_readable(authority, &reference).await? {
                    return Ok(false);
                }
            }
            if page.eof {
                break;
            }
            ensure!(page.next.is_some(), "saved observer source cannot continue");
            source.cursor = page.next;
        }
        if !references.finish()? {
            return Ok(false);
        }
        Ok(true)
    }

    async fn saved_reader_page_matches(
        &self,
        cached: &ThreadToolResultReadParams,
        original: &ThreadToolResultReadParams,
    ) -> Result<bool> {
        let mut cached = cached.clone();
        cached.cursor = None;
        cached.max_tokens = None;
        cached.max_bytes = None;
        let mut expected = original.clone();
        let mut skip = expected.cursor.as_ref().map_or(0, |cursor| cursor.offset);
        expected.cursor = None;
        expected.max_tokens = None;
        expected.max_bytes = None;
        let mut page = self.read_thread_tool_result(expected.clone()).await?;
        let mut position = 0;
        // Legacy/public cursors may have different envelopes. Only the saved,
        // server-validated scalar offset is used; ownership and current ACLs
        // are independently proved, and subsequent reads pin the live revision.
        while skip > 0 {
            if position == page.text.len() {
                if page.eof {
                    return Ok(false);
                }
                expected.cursor = page.next;
                page = self.read_thread_tool_result(expected.clone()).await?;
                position = 0;
            }
            let ch = page.text[position..]
                .chars()
                .next()
                .ok_or_else(|| anyhow::anyhow!("source cannot advance"))?;
            position += ch.len_utf8();
            skip -= 1;
        }
        let mut scanner = super::thread_result_sources::SourceReferences::result_text();
        loop {
            let saved = self.read_thread_tool_result(cached.clone()).await?;
            scanner.feed(&saved.text)?;
            let text = scanner.take_result_text();
            for actual in text.chars() {
                if position == page.text.len() {
                    if page.eof {
                        return Ok(false);
                    }
                    expected.cursor = page.next;
                    page = self.read_thread_tool_result(expected.clone()).await?;
                    position = 0;
                }
                let expected = page.text[position..]
                    .chars()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("source cannot advance"))?;
                if actual != expected {
                    return Ok(false);
                }
                position += expected.len_utf8();
            }
            if saved.eof {
                return scanner.finish();
            }
            ensure!(saved.next.is_some(), "saved reader source cannot continue");
            cached.cursor = saved.next;
        }
    }

    // Bind disclosure validation and the public read to the same canonical
    // revisions, including saved reader chains. No result ID alone is authority.
    async fn saved_result_source_stamp(
        &self,
        params: &ThreadToolResultReadParams,
    ) -> Result<String> {
        let mut target = params.clone();
        let mut digest = Sha256::new();
        let mut seen = std::collections::HashSet::new();
        loop {
            ensure!(
                seen.insert((
                    target.thread_id.clone(),
                    target.turn_id.clone(),
                    target.item_id.clone()
                )),
                "cyclic saved reader source"
            );
            let value = self
                .crud_store
                .tool_result_access_source(
                    &target.workspace_id,
                    &target.thread_id,
                    &target.turn_id,
                    &target.item_id,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("tool result source unavailable"))?;
            digest.update(serde_json::to_vec(&value)?);
            let references = [
                self.crud_store
                    .compaction_tool_item_reference(
                        &target.workspace_id,
                        &target.thread_id,
                        &target.turn_id,
                        &target.item_id,
                    )
                    .await?,
                self.crud_store
                    .compaction_context_reference_for_item(
                        &target.workspace_id,
                        &target.thread_id,
                        &target.turn_id,
                        &target.item_id,
                        "tool_result_v2",
                    )
                    .await?,
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            if !references.is_empty() {
                self.crud_store
                    .compaction_prepare_references(
                        &target.workspace_id,
                        &target.thread_id,
                        &references,
                    )
                    .await?;
            }
            if !target.output_log {
                if let Some(fragment) = self
                    .crud_store
                    .compaction_tool_result_fragment(
                        &target.workspace_id,
                        &target.thread_id,
                        &target.turn_id,
                        &target.item_id,
                        None,
                        0,
                    )
                    .await?
                {
                    digest.update(serde_json::to_vec(&fragment.reference)?);
                }
            }
            if value["toolName"].as_str() != Some(RESULT_READ_TOOL) {
                break;
            }
            let nested: ThreadToolResultReadParams =
                serde_json::from_value(value["arguments"].clone())?;
            ensure!(
                nested.workspace_id == params.workspace_id,
                "tool result source unavailable"
            );
            target = nested;
        }
        Ok(hex::encode(digest.finalize()))
    }

    /// Legacy/native tools can retain a typed item without tool_result_v2 or
    /// shell storage. Page that existing saved view rather than publish a dead
    /// reference or claim an unsaved provider result can be reconstructed.
    async fn read_saved_tool_item_fallback(
        &self,
        params: ThreadToolResultReadParams,
        protection: Option<ResultCursorProtection<'_>>,
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
        let start = text
            .char_indices()
            .nth(usize::try_from(offset)?)
            .map_or(text.len(), |(byte, _)| byte);
        let remaining = &text[start..];
        let max_tokens = params.max_tokens.unwrap_or(u64::MAX);
        let max_bytes = params
            .max_bytes
            .unwrap_or(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES as u64)
            .min(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES as u64) as usize;
        result_delivery_page(
            remaining,
            max_tokens,
            max_bytes,
            protection,
            |count_read, complete| {
                let end = offset
                    .checked_add(count_read)
                    .ok_or_else(|| anyhow::anyhow!("cursor overflow"))?;
                Ok(ThreadToolResultReadResponse {
                    text: String::new(),
                    eof: complete,
                    next: (!complete).then(|| ThreadToolResultCursor {
                        version: version.clone(),
                        offset: end,
                    }),
                })
            },
        )
    }

    async fn read_thread_tool_output_log(
        &self,
        params: ThreadToolResultReadParams,
        protection: Option<ResultCursorProtection<'_>>,
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
        let start = text
            .char_indices()
            .nth(usize::try_from(offset)?)
            .map_or(text.len(), |(byte, _)| byte);
        let remainder = &text[start..];
        let max_tokens = params.max_tokens.unwrap_or(u64::MAX);
        let max_bytes = params
            .max_bytes
            .unwrap_or(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES as u64)
            .min(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES as u64) as usize;
        result_delivery_page(
            remainder,
            max_tokens,
            max_bytes,
            protection,
            |count, complete| {
                let next = if !complete {
                    Some(ThreadToolResultCursor {
                        version: version.clone(),
                        offset: offset
                            .checked_add(count)
                            .ok_or_else(|| anyhow::anyhow!("cursor overflow"))?,
                    })
                } else {
                    rows.get(1).map(|row| ThreadToolResultCursor {
                        version: format!("output:{}", row.0),
                        offset: 0,
                    })
                };
                Ok(ThreadToolResultReadResponse {
                    text: String::new(),
                    eof: next.is_none(),
                    next,
                })
            },
        )
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
        let mut params: ThreadToolResultReadParams = serde_json::from_value(arguments)
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
        let codec = self
            .processor
            .thread_read_cursor_codec()
            .map_err(|_| denied())?;
        let scope = serde_json::json!([
            authority.scope("result", &params.thread_id, false, 0),
            params.turn_id,
            params.item_id,
            params.output_log,
            params.max_tokens,
            params.max_bytes
        ])
        .to_string();
        if let Some(cursor) = &params.cursor {
            if serde_json::to_vec(cursor).map_err(|_| denied())?.len() > 16384 {
                return Err(ToolError::invalid_arguments(
                    "result cursor exceeds 16 KiB; restart reading",
                ));
            }
            let decoded: ThreadToolResultCursor =
                codec.decode(&cursor.version, &scope).map_err(|_| {
                    ToolError::invalid_arguments("invalid or old result cursor; restart reading")
                })?;
            if cursor.offset != decoded.offset {
                return Err(ToolError::invalid_arguments("invalid result cursor"));
            }
            params.cursor = Some(decoded);
        }
        let item = self
            .processor
            .crud_store
            .tool_result_access_source(
                &params.workspace_id,
                &params.thread_id,
                &params.turn_id,
                &params.item_id,
            )
            .await
            .map_err(|_| denied())?
            .ok_or_else(denied)?;
        let stamp = self
            .processor
            .saved_result_source_stamp(&params)
            .await
            .map_err(|_| denied())?;
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
            .read_thread_tool_result_with_cursor(
                params.clone(),
                Some(ResultCursorProtection {
                    codec: &codec,
                    scope: &scope,
                }),
            )
            .await
            .map_err(|_| {
                ToolError::invalid_arguments(
                    "result unavailable, cursor stale, or requested page budget cannot fit framing and source text",
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
        if !self
            .processor
            .saved_thread_read_result_disclosable(&current, &params, &item)
            .await
            .map_err(|_| denied())?
        {
            return Err(denied());
        }
        if self
            .processor
            .saved_result_source_stamp(&params)
            .await
            .map_err(|_| denied())?
            != stamp
        {
            return Err(ToolError::invalid_arguments(
                "result cursor is stale; restart reading",
            ));
        }
        let payload = serde_json::to_value(result)
            .map_err(|_| ToolError::invalid_arguments("result encoding failed"))?;
        if !pioneer_tools::thread_read_page_fits(&payload) {
            return Err(ToolError::execution_failed(
                "result page exceeds projection limit",
            ));
        }
        let text = serde_json::to_string(&payload)
            .map_err(|_| ToolError::invalid_arguments("result encoding failed"))?;
        if params
            .max_tokens
            .is_some_and(|budget| pioneer_compaction::text_tokens(&text) > budget)
        {
            return Err(ToolError::invalid_arguments(
                "result page cannot fit the requested max_tokens budget",
            ));
        }
        Ok(Box::new(FunctionToolOutput::with_payload(
            text, true, payload,
        )))
    }
}

pub(crate) fn thread_result_tool_specs() -> Vec<ConfiguredToolSpec> {
    let spec = ToolSpec::new(
        RESULT_READ_TOOL,
        "Read a saved full tool result as bounded text pages of canonical JSON. Follow the opaque version-bound cursor until eof, keeping the exact source, output_log, max_tokens and max_bytes unchanged. Old cursors require restarting. Attachment references remain references. Legacy saved-item views explicitly report fullResultSaved=false when a separate full provider result was never retained. Set output_log=true to read saved stdout/stderr/progress chunks (including interrupted commands); each chunk is a separate JSON object, and eof refers to the currently saved log.",
        serde_json::json!({"type":"object","properties":{
                "workspace_id":{"type":"string"},"thread_id":{"type":"string"},"turn_id":{"type":"string"},"item_id":{"type":"string"},
                "output_log":{"type":"boolean","default":false},
                "cursor":{"type":"object","properties":{"version":{"type":"string"},"offset":{"type":"integer","minimum":0}},"required":["version","offset"]},
                "max_tokens":{"type":"integer","minimum":1,"description":"Optional estimated token budget for the complete returned JSON, including the protected cursor; omitted means no token ceiling."},"max_bytes":{"type":"integer","minimum":1,"description":"Optional caller page budget; the complete page always fits Pioneer’s 2 MiB response projection."}},
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

#[cfg(test)]
mod estimation_tests {
    use super::*;

    #[test]
    fn protected_pages_preserve_unicode_source_offsets_and_hard_delivery_bytes() {
        let codec = super::super::thread_read_cursor::ThreadReadCursorCodec::new(&[9; 64]).unwrap();
        let text = "漢🌍\n\"\\\u{0}e\u{301}".repeat(100);
        let mut rebuilt = String::new();
        let mut offset = 0u64;
        let mut remaining = text.as_str();
        for _ in 0..100 {
            let page = result_delivery_page(
                remaining,
                100,
                400,
                Some(ResultCursorProtection {
                    codec: &codec,
                    scope: "fixture",
                }),
                |count, complete| {
                    Ok(ThreadToolResultReadResponse {
                        text: String::new(),
                        eof: complete,
                        next: (!complete).then(|| ThreadToolResultCursor {
                            version: "source-v1".into(),
                            offset: offset + count,
                        }),
                    })
                },
            )
            .unwrap();
            let encoded = serde_json::to_string(&serde_json::to_value(&page).unwrap()).unwrap();
            assert!(encoded.len() <= 400 && pioneer_compaction::text_tokens(&encoded) <= 100);
            rebuilt.push_str(&page.text);
            offset += page.text.chars().count() as u64;
            remaining = &remaining[page.text.len()..];
            if page.eof {
                break;
            }
            assert!(!page.text.is_empty());
            let raw: ThreadToolResultCursor = codec
                .decode(&page.next.unwrap().version, "fixture")
                .unwrap();
            assert_eq!(raw.version, "source-v1");
            assert_eq!(raw.offset, offset);
        }
        assert!(remaining.is_empty());
        assert_eq!(rebuilt, text);
        assert!(
            result_delivery_page("🌍", 1, 1, None, |_, _| Ok(
                ThreadToolResultReadResponse {
                    text: String::new(),
                    next: None,
                    eof: true,
                }
            ))
            .is_err()
        );
    }

    #[test]
    fn completed_output_chunk_can_keep_a_next_row_cursor_without_losing_text() {
        let page = result_delivery_page("last 🌍", 100, 400, None, |count, complete| {
            Ok(ThreadToolResultReadResponse {
                text: String::new(),
                eof: false,
                next: Some(ThreadToolResultCursor {
                    version: if complete { "output:2" } else { "output:1" }.into(),
                    offset: if complete { 0 } else { count },
                }),
            })
        })
        .unwrap();
        assert_eq!(page.text, "last 🌍");
        assert!(!page.eof);
        assert_eq!(page.next.unwrap().version, "output:2");
    }
}
