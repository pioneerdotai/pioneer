//! Authorized indexed conversation search for the Threads tool.
use super::{MessageProcessor, thread_read_tools::ThreadReadAuthority};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_memory::{
    ThreadEpisodicSearchProfile, ThreadEpisodicSearchProfileKind, ThreadEpisodicSearchRequest,
};
use pioneer_protocol::ThreadsSearchParams;
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolError,
    ToolExtensionBundle, ToolHandler, ToolInvocation, ToolOutput, ToolPayload, ToolSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

pub(super) const SEARCH_TOOL: &str = "threads_search";
const RETRIEVAL_QUANTUM: usize = 100;
const PAGE_BYTES: usize = 2 * 1024 * 1024;

pub(crate) fn thread_search_tool_specs() -> Vec<ConfiguredToolSpec> {
    vec![ConfiguredToolSpec::new(
        ToolSpec::new(
            SEARCH_TOOL,
            "Search all accessible indexed conversations in this workspace. Results group matching fragments by conversation, with full current titles and real source references. limit counts fragments (1–100). Follow nextCursor until eof. Use source.turnId and source.itemId as a threads_history_read anchor. A pageFragment continues an oversized JSON card; concatenate its text until complete. Combines word and semantic search; query punctuation and quotes are ordinary text.",
            json!({"type":"object","properties":{"query":{"type":"string","minLength":1},"limit":{"type":"integer","minimum":1,"maximum":100},"cursor":{"type":["string","null"]}},"required":["query","limit"],"additionalProperties":false}),
            PayloadKind::Function,
        ),
        ExecutionClass::Shared,
        pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(SEARCH_TOOL),
    )]
}

/// Backend-neutral query and range. Retrieval positions stay inside the search service.
struct ThreadSearchRequest<'a> {
    query: &'a str,
    position: usize,
    limit: usize,
}

#[derive(Clone)]
struct FoundSource {
    thread_id: String,
    title: Option<String>,
    fragment: Value,
    score: f32,
    position: usize,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchCursor {
    position: usize,
    #[serde(default)]
    fragment: Option<CardFragmentCursor>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CardFragmentCursor {
    index_item_id: String,
    version: String,
    byte_offset: usize,
    // The index snippet is small; bind the continuation to the already selected
    // canonical source instead of repeating retrieval to reconstruct a large title.
    text: String,
    score: f32,
    #[serde(default)]
    coverage_incomplete: bool,
}

impl MessageProcessor {
    pub(crate) fn thread_search_tools(
        self: &Arc<Self>,
        context: TurnToolContext,
    ) -> ToolExtensionBundle {
        ToolExtensionBundle {
            specs: thread_search_tool_specs(),
            handlers: vec![(
                SEARCH_TOOL.into(),
                Arc::new(ThreadSearchHandler {
                    processor: self.clone(),
                    context,
                }) as Arc<dyn ToolHandler>,
            )],
        }
    }

    async fn search_thread_sources(
        &self,
        authority: &ThreadReadAuthority,
        request: ThreadSearchRequest<'_>,
    ) -> Result<(Vec<FoundSource>, bool, bool, usize)> {
        let service = &self.thread_episodic_recall_service;
        let (embedding, mut incomplete) = match service
            .prepare_thread_search_embedding(&authority.workspace_id, request.query)
            .await
        {
            Ok(embedding) => (embedding, false),
            Err(error) => {
                tracing::warn!(
                    failure_class = "embedding_provider",
                    "thread search semantic branch unavailable"
                );
                let _ = error;
                (None, true)
            }
        };
        let search = ThreadEpisodicSearchRequest {
            workspace_id: authority.workspace_id.clone(),
            thread_id: String::new(),
            query: request.query.to_owned(),
            profile: ThreadEpisodicSearchProfile::for_kind(
                ThreadEpisodicSearchProfileKind::DefaultContext,
            ),
            access: authority.search_access(),
        };
        let mut position = request.position;
        let mut found = Vec::with_capacity(request.limit + 1);
        let mut seen = HashSet::new();
        // Bound source validation work even if concurrent changes invalidate an
        // entire candidate portion. Empty nonterminal pages still advance.
        for _ in 0..4 {
            let page = service
                .thread_search_backend()
                .search_page(
                    search.clone(),
                    RETRIEVAL_QUANTUM,
                    Some(position.to_string()),
                    embedding.clone(),
                )
                .await
                .map_err(|error| anyhow!("thread search failed: {error}"))?;
            incomplete |= !page.warnings.is_empty();
            let sources = service
                .resolve_thread_search_sources(&authority.workspace_id, &page.hits)
                .await?;
            let mut readable = BTreeMap::new();
            for hit in page.hits {
                let candidate_position = position;
                position = position
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("invalid retrieval position"))?;
                let Some((item, title)) = sources.get(&hit.index_item_id) else {
                    // A candidate passed SQL eligibility but lost its current
                    // canonical source/version before disclosure. Do not report
                    // complete coverage while reconciliation catches up.
                    incomplete = true;
                    continue;
                };
                if item.thread_id != hit.thread_id
                    || item.turn_id != hit.turn_id
                    || item.item_id != hit.item_id
                    || hit.metadata.get("pioneer.thread_episodic.source_text_hash")
                        != Some(&item.source_text_hash)
                {
                    incomplete = true;
                    continue;
                }
                let allowed = match readable.get(&item.thread_id) {
                    Some(allowed) => *allowed,
                    None => {
                        let allowed = self
                            .thread_source_readable(authority, &item.thread_id)
                            .await?;
                        readable.insert(item.thread_id.clone(), allowed);
                        allowed
                    }
                };
                if !allowed {
                    continue;
                }
                let key = (
                    item.thread_id.clone(),
                    item.turn_id.clone(),
                    item.item_id.clone(),
                );
                if !seen.insert(key) {
                    continue;
                }
                let score = hit.rank_score();
                found.push(FoundSource {
                    thread_id: item.thread_id.clone(), title: title.clone(),
                    fragment: json!({"text":hit.text,"score":score,"source":{
                        "workspaceId":item.workspace_id,"threadId":item.thread_id,"turnId":item.turn_id,
                        "itemId":item.item_id,"indexItemId":item.id,"sourceKind":item.source_context,
                        "sourceVersion":item.source_text_hash}}),
                    score, position: candidate_position,
                });
                if found.len() > request.limit {
                    let continuation = found.pop().unwrap().position;
                    return Ok((found, true, incomplete, continuation));
                }
            }
            if page.next_cursor.is_none() {
                return Ok((found, false, incomplete, position));
            }
        }
        Ok((found, true, incomplete, position))
    }

    pub(super) async fn search_threads_page(
        &self,
        context: &TurnToolContext,
        params: ThreadsSearchParams,
    ) -> Result<Value> {
        let result = self.search_threads_page_inner(context, params).await?;
        let current = self.thread_read_authority(context).await?;
        ensure!(
            self.search_response_readable(&current, &result).await?,
            "source unavailable or access denied"
        );
        Ok(result)
    }

    async fn search_threads_page_inner(
        &self,
        context: &TurnToolContext,
        params: ThreadsSearchParams,
    ) -> Result<Value> {
        params.validate().map_err(anyhow::Error::msg)?;
        let authority = self.thread_read_authority(context).await?;
        let codec = self.thread_read_cursor_codec()?;
        let query_hash = hex::encode(Sha256::digest(params.query.as_bytes()));
        let scope = json!([
            authority.scope("search", &query_hash, false, params.limit),
            context.thread_id,
            context.turn_id
        ])
        .to_string();
        let state = params
            .cursor
            .as_deref()
            .map(|c| codec.decode::<SearchCursor>(c, &scope))
            .transpose()?
            .unwrap_or_default();
        if let Some(fragment) = state.fragment {
            return self
                .continue_search_card(&authority, state.position, fragment, &codec, &scope)
                .await;
        }
        let (found, has_more, incomplete, next_position) = self
            .search_thread_sources(
                &authority,
                ThreadSearchRequest {
                    query: &params.query,
                    position: state.position,
                    limit: params.limit as usize,
                },
            )
            .await?;
        let mut accepted = Vec::new();
        for source in found {
            let mut proposed = accepted.clone();
            proposed.push(source.clone());
            let next_position = source
                .position
                .checked_add(1)
                .ok_or_else(|| anyhow!("invalid cursor"))?;
            let cursor = codec.encode(
                &SearchCursor {
                    position: next_position,
                    fragment: None,
                },
                &scope,
            )?;
            let candidate = search_response(&proposed, Some(cursor), false, incomplete);
            if !search_page_fits_output(&candidate)? {
                if accepted.is_empty() {
                    let fragment = CardFragmentCursor {
                        index_item_id: source.fragment["source"]["indexItemId"]
                            .as_str()
                            .ok_or_else(|| anyhow!("source unavailable"))?
                            .into(),
                        version: String::new(),
                        byte_offset: 0,
                        coverage_incomplete: incomplete,
                        text: source.fragment["text"].as_str().unwrap_or_default().into(),
                        score: source.score,
                    };
                    return self
                        .continue_search_card(&authority, source.position, fragment, &codec, &scope)
                        .await;
                }
                return Ok(search_response(
                    &accepted,
                    Some(
                        codec.encode(
                            &SearchCursor {
                                position: accepted
                                    .last()
                                    .map_or(next_position, |source: &FoundSource| {
                                        source.position + 1
                                    }),
                                fragment: None,
                            },
                            &scope,
                        )?,
                    ),
                    false,
                    incomplete,
                ));
            }
            accepted.push(source);
        }
        let next = has_more
            .then(|| {
                codec.encode(
                    &SearchCursor {
                        position: next_position,
                        fragment: None,
                    },
                    &scope,
                )
            })
            .transpose()?;
        Ok(search_response(&accepted, next, !has_more, incomplete))
    }

    pub(super) async fn search_fragment_readable(
        &self,
        authority: &ThreadReadAuthority,
        fragment: &Value,
    ) -> Result<bool> {
        let source = &fragment["source"];
        let Some(id) = source["indexItemId"].as_str() else {
            return Ok(false);
        };
        let Some(thread) = source["threadId"].as_str() else {
            return Ok(false);
        };
        if source["workspaceId"].as_str() != Some(authority.workspace_id.as_str())
            || !self.thread_source_readable(authority, thread).await?
        {
            return Ok(false);
        }
        let Some(item) = self
            .thread_episodic_recall_service
            .current_thread_search_source(&authority.workspace_id, id)
            .await?
        else {
            return Ok(false);
        };
        Ok(item.thread_id == thread
            && source["turnId"].as_str() == Some(item.turn_id.as_str())
            && source["itemId"].as_str() == Some(item.item_id.as_str())
            && source["sourceVersion"].as_str() == Some(item.source_text_hash.as_str())
            && source["sourceKind"] == serde_json::to_value(item.source_context)?)
    }

    pub(super) async fn search_response_readable(
        &self,
        authority: &ThreadReadAuthority,
        response: &Value,
    ) -> Result<bool> {
        let Some(threads) = response["threads"].as_array() else {
            return Ok(false);
        };
        for thread in threads {
            let Some(id) = thread["threadId"].as_str() else {
                return Ok(false);
            };
            if !self.thread_source_readable(authority, id).await? {
                return Ok(false);
            }
            let Some(fragments) = thread["fragments"].as_array() else {
                return Ok(false);
            };
            for fragment in fragments {
                if fragment["source"]["threadId"].as_str() != Some(id)
                    || !self.search_fragment_readable(authority, fragment).await?
                {
                    return Ok(false);
                }
            }
        }
        if let Some(fragment) = response.get("pageFragment") {
            if !self.search_fragment_readable(authority, fragment).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn continue_search_card(
        &self,
        authority: &ThreadReadAuthority,
        position: usize,
        mut state: CardFragmentCursor,
        codec: &super::thread_read_cursor::ThreadReadCursorCodec,
        scope: &str,
    ) -> Result<Value> {
        let item = self
            .thread_episodic_recall_service
            .current_thread_search_source(&authority.workspace_id, &state.index_item_id)
            .await?
            .ok_or_else(|| anyhow!("cursor source unavailable"))?;
        ensure!(
            self.thread_source_readable(authority, &item.thread_id)
                .await?,
            "source unavailable or access denied"
        );
        let thread = self
            .crud_store
            .get_thread_by_id(&item.thread_id)
            .await?
            .filter(|t| t.workspace_id == authority.workspace_id)
            .ok_or_else(|| anyhow!("cursor source unavailable"))?;
        let source = json!({"workspaceId":item.workspace_id,"threadId":item.thread_id,"turnId":item.turn_id,
            "itemId":item.item_id,"indexItemId":item.id,"sourceKind":item.source_context,"sourceVersion":item.source_text_hash});
        let card = json!({"threadId":thread.id,"title":thread.name,"fragments":[{"text":state.text,"score":state.score,"source":source}]});
        let encoded = serde_json::to_string(&card)?;
        let version = hex::encode(Sha256::digest(encoded.as_bytes()));
        ensure!(
            state.version.is_empty() || state.version == version,
            "cursor source changed; restart search"
        );
        state.version = version.clone();
        ensure!(
            state.byte_offset < encoded.len() && encoded.is_char_boundary(state.byte_offset),
            "invalid cursor fragment offset"
        );
        // The existing result pager continues exact serialized records. Its
        // current 32KiB quantum is internal to this exceptional oversized card;
        // it never truncates or discards the remaining title or source data.
        let page = pioneer_compaction::results::result_page(
            &encoded,
            &version,
            Some(&pioneer_compaction::results::ResultCursor {
                version: version.clone(),
                offset: state.byte_offset,
            }),
            u64::MAX,
            PAGE_BYTES,
        )?;
        state.byte_offset += page.text.len();
        let incomplete = state.coverage_incomplete;
        let next_state = if page.eof {
            SearchCursor {
                position: position
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("invalid cursor"))?,
                fragment: None,
            }
        } else {
            SearchCursor {
                position,
                fragment: Some(state),
            }
        };
        let next = codec.encode(&next_state, scope)?;
        let response = json!({"threads":[],"pageFragment":{"text":page.text,"source":source,"version":version,"complete":page.eof},
            "nextCursor":next,"eof":false,"coverageIncomplete":incomplete,
            "warnings":if incomplete {vec!["Some indexes or semantic retrieval were unavailable; coverage is incomplete."]} else {vec![]}});
        ensure!(
            search_page_fits_output(&response)?,
            "search page exceeds byte limit"
        );
        Ok(response)
    }
}

/// Measure the representation used by shared native/CLI output projection,
/// including its JSON view wrapper and the metadata delivered to the CLI.
/// A raw response that fits alone can otherwise be replaced by a preview.
fn search_page_fits_output(response: &Value) -> Result<bool> {
    let view = pioneer_tools::ToolResultView::Json {
        value: response.clone(),
        truncated: false,
    };
    if view.serialized_size_bytes() > PAGE_BYTES {
        return Ok(false);
    }
    Ok(pioneer_tools::builtin_json_llm_payload_size(SEARCH_TOOL, response)? <= PAGE_BYTES)
}

fn search_response(
    sources: &[FoundSource],
    cursor: Option<String>,
    eof: bool,
    incomplete: bool,
) -> Value {
    let mut threads: Vec<Value> = Vec::new();
    for source in sources {
        if let Some(card) = threads
            .iter_mut()
            .find(|t| t["threadId"].as_str() == Some(source.thread_id.as_str()))
        {
            card["fragments"]
                .as_array_mut()
                .unwrap()
                .push(source.fragment.clone());
        } else {
            threads.push(json!({"threadId":source.thread_id,"title":source.title,"fragments":[source.fragment]}));
        }
    }
    json!({"threads":threads,"nextCursor":cursor,"eof":eof,"coverageIncomplete":incomplete,
        "warnings":if incomplete {vec!["Some indexes or semantic retrieval were unavailable; coverage is incomplete."]} else {vec![]}})
}

struct ThreadSearchHandler {
    processor: Arc<MessageProcessor>,
    context: TurnToolContext,
}
#[async_trait]
impl ToolHandler for ThreadSearchHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(ToolError::invalid_arguments("expected function arguments"));
        };
        let params = serde_json::from_value(arguments)
            .map_err(|_| ToolError::invalid_arguments("invalid search parameters"))?;
        let result = self
            .processor
            .search_threads_page(&self.context, params)
            .await
            .map_err(|e| {
                let message = e.to_string();
                if message.contains("cursor") {
                    ToolError::invalid_arguments(
                        "invalid or stale search cursor; restart from first page",
                    )
                } else if message.starts_with("query must") || message.starts_with("limit must") {
                    ToolError::invalid_arguments(message)
                } else {
                    ToolError::execution_failed("search unavailable or access denied")
                }
            })?;
        let text = serde_json::to_string(&result)
            .map_err(|_| ToolError::execution_failed("response encoding failed"))?;
        if !search_page_fits_output(&result)
            .map_err(|_| ToolError::execution_failed("response encoding failed"))?
        {
            return Err(ToolError::execution_failed(
                "search page exceeds byte limit",
            ));
        }
        Ok(Box::new(FunctionToolOutput::with_payload(
            text, true, result,
        )))
    }
}

#[cfg(test)]
#[path = "thread_search_tools/tests.rs"]
pub(in crate::message) mod tests;
