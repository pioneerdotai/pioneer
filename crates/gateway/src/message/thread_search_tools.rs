//! Indexed conversation search shared by the tool and production episodic recall.
use super::{MessageProcessor, thread_read_tools::ThreadReadAuthority};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_crud::{
    THREAD_EPISODIC_WORKSPACE_CAPSULE_THREAD_ID, ThreadEpisodicCapsuleStatus,
    ThreadEpisodicRepairStatus,
};
use pioneer_memory::{
    ThreadEpisodicMemvidSearchRequest, ThreadEpisodicMemvidSearchSegment,
    ThreadEpisodicSearchProfile, ThreadEpisodicSearchProfileKind,
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
const CATALOG_QUANTUM: u64 = 100;
const RETRIEVAL_QUANTUM: usize = 100;
const PAGE_BYTES: usize = 2 * 1024 * 1024;

pub(crate) fn thread_search_tool_specs() -> Vec<ConfiguredToolSpec> {
    vec![ConfiguredToolSpec::new(
        ToolSpec::new(
            SEARCH_TOOL,
            "Search all accessible indexed conversations in this workspace. Results group matching fragments by conversation, with full current titles and real source references. limit counts fragments (1–100). Follow nextCursor until eof. Use source.turnId and source.itemId as a threads_history_read anchor. A pageFragment continues an oversized JSON card; concatenate its text until complete. Lexical and semantic retrieval are selected automatically.",
            json!({"type":"object","properties":{"query":{"type":"string","minLength":1},"limit":{"type":"integer","minimum":1,"maximum":100},"cursor":{"type":["string","null"]}},"required":["query","limit"],"additionalProperties":false}),
            PayloadKind::Function,
        ),
        ExecutionClass::Shared,
        pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(SEARCH_TOOL),
    )]
}

/// Backend-neutral query and range. Engine cursors stay inside the Memvid adapter.
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
    canonical_key: (String, String, String),
    score: f32,
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
    // canonical source instead of re-running ANN to reconstruct a large title.
    text: String,
    score: f32,
    #[serde(default)]
    coverage_incomplete: bool,
}

impl MessageProcessor {
    pub(crate) fn thread_search_recall_enabled(&self) -> bool {
        self.thread_episodic_runtime_config
            .read()
            .map(|c| c.enabled && c.recall_enabled)
            .unwrap_or(false)
    }

    pub(crate) async fn recall_thread_search(
        &self,
        request: pioneer_memory::hooks::MemoryCurrentThreadRecallRequest,
    ) -> Result<pioneer_memory::hooks::MemoryEpisodicRecallResponse> {
        use pioneer_memory::hooks::*;
        if !self.thread_search_recall_enabled() || request.query.trim().is_empty() {
            return Ok(MemoryEpisodicRecallResponse::default());
        }
        // These IDs come from the hook context. The principal and admitted
        // grants are independently resolved from the persisted execution.
        let context = TurnToolContext {
            workspace_id: request.workspace_id,
            thread_id: request.thread_id,
            turn_id: request.turn_id,
        };
        let authority = self.thread_read_authority(&context).await?;
        let (found, has_more, incomplete) = self
            .search_thread_sources(
                &authority,
                ThreadSearchRequest {
                    query: &request.query,
                    position: 0,
                    limit: request.top_k.max(1) as usize,
                },
            )
            .await?;
        let current = self.thread_read_authority(&context).await?;
        let mut remaining = request.max_chars;
        let mut items = Vec::new();
        let mut truncated = has_more;
        for found in found {
            ensure!(
                self.search_fragment_readable(&current, &found.fragment)
                    .await?,
                "source unavailable or access denied"
            );
            let source = &found.fragment["source"];
            let text = found.fragment["text"].as_str().unwrap_or_default();
            let content = text.chars().take(remaining).collect::<String>();
            truncated |= content.len() < text.len();
            remaining = remaining.saturating_sub(content.chars().count());
            if content.is_empty() {
                truncated = true;
                break;
            }
            items.push(MemoryEpisodicRecallItem {
                id: serde_json::to_string(&found.canonical_key)?,
                content,
                title: found.title,
                provenance: MemoryEpisodicRecallProvenance {
                    workspace_id: authority.workspace_id.clone(),
                    thread_id: Some(found.thread_id.clone()),
                    turn_id: source["turnId"].as_str().map(str::to_owned),
                    item_id: source["itemId"].as_str().map(str::to_owned),
                    index_item_id: source["indexItemId"].as_str().map(str::to_owned),
                    source_kind: source["sourceKind"].as_str().map(str::to_owned),
                    task_id: None,
                    timestamp_unix: None,
                    source: if found.thread_id == context.thread_id {
                        MemoryEpisodicRecallSourceKind::CurrentThread
                    } else {
                        MemoryEpisodicRecallSourceKind::WorkspaceThread
                    },
                    retrieval_score: Some(found.score),
                    boundary: MemoryEpisodicRecallBoundary::Snippet,
                },
                score: Some(found.score),
                updated_at_unix: None,
                visibility: MemoryEpisodicRecallVisibility::Public,
            });
        }
        Ok(MemoryEpisodicRecallResponse {
            items,
            diagnostics: if incomplete {
                vec!["memory.episodic_recall.coverage_incomplete".into()]
            } else {
                Vec::new()
            },
            truncated,
        })
    }

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
    ) -> Result<(Vec<FoundSource>, bool, bool)> {
        let required = request
            .position
            .checked_add(request.limit)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| anyhow!("invalid cursor position"))?;
        let service = &self.thread_episodic_recall_service;
        let (embedding, mut incomplete) = match service
            .prepare_thread_search_embedding(&authority.workspace_id, request.query)
            .await
        {
            Ok(embedding) => (embedding, false),
            Err(_) => (None, true),
        };
        let mut merged = BTreeMap::<(String, String, String), FoundSource>::new();
        let mut after = None;
        loop {
            let capsules = self
                .crud_store
                .list_thread_episodic_capsules_for_workspace_after(
                    &authority.workspace_id,
                    after.as_deref(),
                    CATALOG_QUANTUM,
                )
                .await?;
            let catalog_end = capsules.len() < CATALOG_QUANTUM as usize;
            for capsule in capsules {
                after = Some(capsule.id.clone());
                if capsule.status == ThreadEpisodicCapsuleStatus::Deleted {
                    continue;
                }
                if capsule.thread_id != THREAD_EPISODIC_WORKSPACE_CAPSULE_THREAD_ID
                    && !self
                        .thread_source_readable(authority, &capsule.thread_id)
                        .await?
                {
                    continue;
                }
                if capsule.status != ThreadEpisodicCapsuleStatus::Active
                    || capsule.repair_status != ThreadEpisodicRepairStatus::Ok
                {
                    incomplete = true;
                    continue;
                }
                let search = ThreadEpisodicMemvidSearchRequest {
                    workspace_id: authority.workspace_id.clone(),
                    thread_id: String::new(),
                    query: request.query.to_owned(),
                    scope: None,
                    profile: ThreadEpisodicSearchProfile::for_kind(
                        ThreadEpisodicSearchProfileKind::DefaultContext,
                    ),
                    segments: vec![ThreadEpisodicMemvidSearchSegment {
                        capsule_id: capsule.id,
                        capsule_ref: capsule.capsule_ref,
                        storage_uri: capsule.storage_uri,
                        segment_index: capsule.segment_index,
                    }],
                    exact_source: None,
                };
                let mut cursor = None;
                let mut seen = HashSet::new();
                let mut local_count = 0usize;
                let mut segment_embedding = embedding.clone();
                loop {
                    let page = match service
                        .thread_search_backend()
                        .search_segment_page(
                            search.clone(),
                            RETRIEVAL_QUANTUM,
                            cursor.clone(),
                            segment_embedding.clone(),
                        )
                        .await
                    {
                        Ok(page) => page,
                        Err(_) if segment_embedding.is_some() => {
                            // A healthy lexical index remains searchable when
                            // its vector projection is absent or incompatible.
                            // Restart this file's retrieval in lexical mode;
                            // canonical deduplication handles any earlier hits.
                            incomplete = true;
                            segment_embedding = None;
                            cursor = None;
                            continue;
                        }
                        Err(_) => {
                            incomplete = true;
                            break;
                        }
                    };
                    incomplete |=
                        !page.unavailable_segment_ids.is_empty() || !page.warnings.is_empty();
                    for hit in page.hits {
                        if !self
                            .thread_source_readable(authority, &hit.thread_id)
                            .await?
                        {
                            continue;
                        }
                        let Some(item) = service
                            .resolve_thread_search_source(&authority.workspace_id, &hit)
                            .await?
                        else {
                            continue;
                        };
                        let Some(thread) = self
                            .crud_store
                            .get_thread_by_id(&item.thread_id)
                            .await?
                            .filter(|t| t.workspace_id == authority.workspace_id)
                        else {
                            continue;
                        };
                        let key = (
                            item.thread_id.clone(),
                            item.turn_id.clone(),
                            item.item_id.clone(),
                        );
                        if !seen.insert(key.clone()) {
                            continue;
                        }
                        let score = hit
                            .memvid_score
                            .filter(|s| s.is_finite())
                            .unwrap_or_default();
                        let found = FoundSource {
                            thread_id: item.thread_id.clone(),
                            title: thread.name,
                            fragment: json!({"text":hit.text,"score":score,"source":{
                                "workspaceId":item.workspace_id,"threadId":item.thread_id,
                                "turnId":item.turn_id,"itemId":item.item_id,"indexItemId":item.id,
                                "sourceKind":item.source_context,"sourceVersion":item.source_text_hash}}),
                            canonical_key: key.clone(),
                            score,
                        };
                        if merged.get(&key).is_none_or(|old| score > old.score) {
                            merged.insert(key, found);
                        }
                        local_count += 1;
                        if local_count == required {
                            break;
                        }
                    }
                    // Keep only the globally necessary range, never corpus data.
                    if merged.len() > required {
                        let mut ranked = merged.into_values().collect::<Vec<_>>();
                        ranked.sort_by(source_order);
                        merged = ranked
                            .into_iter()
                            .take(required)
                            .map(|f| (f.canonical_key.clone(), f))
                            .collect();
                    }
                    if local_count >= required || page.next_cursor.is_none() {
                        break;
                    }
                    ensure!(
                        page.next_cursor != cursor,
                        "index continuation did not advance"
                    );
                    cursor = page.next_cursor;
                }
            }
            if catalog_end {
                break;
            }
        }
        let mut found = merged.into_values().collect::<Vec<_>>();
        found.sort_by(source_order);
        let has_more = found.len() > request.position.saturating_add(request.limit);
        Ok((
            found
                .into_iter()
                .skip(request.position)
                .take(request.limit)
                .collect(),
            has_more,
            incomplete,
        ))
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
        let (found, has_more, incomplete) = self
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
            let next_position = state
                .position
                .checked_add(proposed.len())
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
                        .continue_search_card(&authority, state.position, fragment, &codec, &scope)
                        .await;
                }
                return Ok(search_response(
                    &accepted,
                    Some(codec.encode(
                        &SearchCursor {
                            position: state.position + accepted.len(),
                            fragment: None,
                        },
                        &scope,
                    )?),
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
                        position: state.position + accepted.len(),
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
    let projection = pioneer_tools::project_tool_result(pioneer_tools::ToolProjectionInput {
        call_id: "",
        tool_name: SEARCH_TOOL,
        arguments: &Value::Null,
        raw_output_text: "",
        raw_output_json: response,
        success: true,
        outcome: &pioneer_tools::ToolOutcome::ok(),
        output_policy: &pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(SEARCH_TOOL),
        output_projection: &pioneer_tools::ToolOutputProjectionKind::Builtin,
    });
    Ok(serde_json::to_vec(&projection.llm_payload())?.len() <= PAGE_BYTES)
}

fn source_order(a: &FoundSource, b: &FoundSource) -> std::cmp::Ordering {
    b.score
        .total_cmp(&a.score)
        .then_with(|| a.canonical_key.cmp(&b.canonical_key))
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
