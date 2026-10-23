//! Observation-only tools, independent of UI reads/subscriptions and write routes.
use super::{MessageProcessor, thread_read_cursor::ThreadReadCursorCodec};
use crate::{
    auth::AuthenticatedSessionPrincipal,
    authorization::{
        AuthorizationResolver, AuthorizationService, ProofResolution, ResourceAction,
        RoleDefinitionRegistry,
    },
};
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_protocol::{
    ThreadHistoryDirection, ThreadsChildrenListParams, ThreadsHistoryReadParams, ThreadsListParams,
};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolError,
    ToolExtensionBundle, ToolHandler, ToolInvocation, ToolOutput, ToolPayload, ToolSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::Arc};

pub(crate) const THREAD_READ_TOOL_NAMES: [&str; 4] = [
    "threads_search",
    "threads_list",
    "threads_history_read",
    "threads_children_list",
];
const CANDIDATE_QUANTUM: u64 = 200;
const RESPONSE_BYTES: usize = 32768;

pub(crate) fn thread_read_tool_specs() -> Vec<ConfiguredToolSpec> {
    let limit = json!({"type":"integer","minimum":1,"maximum":100});
    let cursor = json!({"type":["string","null"],"description":"Opaque nextCursor from the same operation, resource and parameters; omit/null for first page."});
    let definitions = [
        (
            "threads_search",
            "Search accessible indexed conversations in the current workspace. Automatically combines lexical and compatible semantic retrieval. Returns bounded thread cards and exact sources usable with threads_history_read. limit counts fragments (1..20). Continue with the same query, limit and nextCursor within five minutes; pages use a fixed selection and recheck current access/content. eof ends this selection only; limitations describes incomplete coverage, capped selection, approximate semantic retrieval and lexical fallback. Start a new search to refresh.",
            json!({"query":{"type":"string","minLength":1,"maxLength":4096},"limit":{"type":"integer","minimum":1,"maximum":20},"cursor":cursor}),
            vec!["query", "limit"],
        ),
        (
            "threads_list",
            "List accessible saved conversations in the current workspace, including hidden internal children and archived conversations. Bounded pages ordered by immutable thread ID. Follow nextCursor until eof; an empty nonterminal page is possible. No search. Read a selected conversation with threads_history_read.",
            json!({"limit":limit,"cursor":cursor}),
            vec!["limit"],
        ),
        (
            "threads_history_read",
            "Read one conversation's saved history, newest to oldest by default. Optionally open near an exact saved anchor {turnId,itemId?}; omit itemId to anchor the turn. Initial pages include bounded neighboring context. Use direction=older (default) or newer with nextCursor, or reverseCursor and its reverseDirection to continue within the same snapshot; omit anchor on continuation and retain limit. nextCursor/eof apply to the requested direction. Every page is displayed newest to oldest. Follow nextCursor to finish JSON fragments of oversized records before changing direction. Child references identify separate histories: open a child with another threads_history_read call, then resume the parent with its original cursor. Children are never loaded automatically; threads_children_list is optional. Read large saved tool results with threads_tools_result_read using the exact supplied reference; output_log=true reads saved logs.",
            json!({"threadId":{"type":"string"},"limit":limit,"cursor":cursor,
                "anchor":{"type":["object","null"],"properties":{"turnId":{"type":"string"},"itemId":{"type":["string","null"]}},"required":["turnId"],"additionalProperties":false,"description":"Exact saved source in this thread; omit on cursor continuation."},
                "direction":{"type":"string","enum":["older","newer"],"default":"older"}}),
            vec!["threadId", "limit"],
        ),
        (
            "threads_children_list",
            "Optionally list direct children (recursive=false) or every descendant of this specific conversation (recursive=true). Does not read child history. Follow nextCursor until eof even if a page is empty. Open selected children with threads_history_read; this list is not a prerequisite for reading history.",
            json!({"threadId":{"type":"string"},"recursive":{"type":"boolean"},"limit":limit,"cursor":cursor}),
            vec!["threadId", "recursive", "limit"],
        ),
    ];
    definitions.into_iter().map(|(name, description, properties, required)| ConfiguredToolSpec::new(
        ToolSpec::new(name, description, json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}), PayloadKind::Function),
        ExecutionClass::Shared, pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(name),
    )).collect()
}

pub(super) struct ThreadReadAuthority {
    pub(super) principal: AuthenticatedSessionPrincipal,
    pub(super) workspace_id: String,
    pub(super) root_thread_id: String,
    pub(super) current_thread_id: String,
    pub(super) observe_children: bool,
    pub(super) observe_workspace: bool,
}
impl ThreadReadAuthority {
    pub(super) fn scope(
        &self,
        operation: &str,
        resource: &str,
        recursive: bool,
        limit: u32,
    ) -> String {
        // JSON encoding avoids delimiter ambiguity in imported legacy IDs.
        json!([
            "thread-read-v1",
            self.principal.principal_id.as_str(),
            self.workspace_id,
            operation,
            resource,
            recursive,
            limit
        ])
        .to_string()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListCursor {
    after: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildrenCursor {
    node: String,
    after: Option<String>,
    path_fingerprint: String,
}

impl MessageProcessor {
    pub(crate) fn thread_read_tools(
        self: &Arc<Self>,
        context: TurnToolContext,
    ) -> ToolExtensionBundle {
        let handler = Arc::new(ThreadReadHandler {
            processor: self.clone(),
            context,
        });
        ToolExtensionBundle {
            specs: thread_read_tool_specs(),
            handlers: THREAD_READ_TOOL_NAMES
                .into_iter()
                .map(|name| (name.to_owned(), handler.clone() as Arc<dyn ToolHandler>))
                .collect(),
        }
    }

    pub(super) async fn thread_read_authority(
        &self,
        context: &TurnToolContext,
    ) -> Result<ThreadReadAuthority> {
        let current = self
            .revalidate_tool_execution_authorization(
                &context.workspace_id,
                &context.thread_id,
                &context.turn_id,
                None,
                ResourceAction::ThreadRead,
            )
            .await?;
        let principal = current
            .initiating_principal(self.crud_store.as_ref())
            .await?;
        let envelope = self
            .load_turn_execution_authorization_context(&context.turn_id)
            .await?;
        // Recall may run before tool materialization creates a runtime binding.
        // The persisted execution envelope is authoritative in both phases.
        let mut agent_observe = envelope.grants_action(ResourceAction::ThreadObserveWorkspace);
        let mut observe_children = false;
        if let Some((role, grants)) = self.thread_read_agent_ceiling(context).await? {
            let policy = RoleDefinitionRegistry::new();
            let permits = |action: ResourceAction| {
                grants.iter().any(|name| name == action.safe_name())
                    && policy.agent_policy_allows(&role, action)
            };
            ensure!(
                permits(ResourceAction::ThreadRead),
                "source unavailable or access denied"
            );
            observe_children = permits(ResourceAction::ChildObserve);
            agent_observe &= permits(ResourceAction::ThreadObserveWorkspace);
        }
        let mut authority = ThreadReadAuthority {
            principal,
            workspace_id: context.workspace_id.clone(),
            root_thread_id: envelope.root_thread_id().to_owned(),
            current_thread_id: context.thread_id.clone(),
            observe_children,
            observe_workspace: false,
        };
        ensure!(
            self.thread_source_readable(&authority, &authority.root_thread_id)
                .await?,
            "source unavailable or access denied"
        );
        if agent_observe && envelope.grants_action(ResourceAction::ThreadObserveWorkspace) {
            // Revalidate this separate immutable grant too. Broader current user
            // rights cannot add an action absent from the admitted execution.
            let action = ResourceAction::ThreadObserveWorkspace;
            let gate = AuthorizationService::new().authorize_action(
                authority.principal.kind,
                authority.principal.role_key.as_ref(),
                action,
            );
            if matches!(
                AuthorizationResolver::new(self.crud_store.as_ref().clone())
                    .authorize_thread(
                        &authority.principal,
                        &gate,
                        action,
                        &authority.root_thread_id,
                        Some(&authority.workspace_id)
                    )
                    .await?,
                ProofResolution::Authorized(_)
            ) && self
                .revalidate_tool_execution_authorization(
                    &context.workspace_id,
                    &context.thread_id,
                    &context.turn_id,
                    None,
                    action,
                )
                .await
                .is_ok()
            {
                authority.observe_workspace = true;
            }
        }
        Ok(authority)
    }

    // Resolve the same immutable Agent ceiling used by tool materialization.
    // Hooks can precede the in-memory binding; loading a persisted grant never
    // creates an execution, expands grants or admits another Agent.
    async fn thread_read_agent_ceiling(
        &self,
        context: &TurnToolContext,
    ) -> Result<Option<(String, Vec<String>)>> {
        if let Some(binding) = self.agent_action_binding(&context.turn_id).await {
            let facts = binding.adapter.lock().await.persistence_facts();
            return Ok(Some((
                facts.agent_authorization_role_key,
                facts.agent_authorization_allowed_actions,
            )));
        }
        let db = self.crud_store.database_connection();
        let execution = if let Some(response) =
            pioneer_crud::load_agent_turn_response(&db, &context.turn_id).await?
        {
            Some(response.execution_id)
        } else {
            self.crud_store
                .get_turn(&context.thread_id, &context.turn_id)
                .await?
                .and_then(|(_, turn)| turn.author)
                .and_then(|author| match author.actor {
                    pioneer_protocol::PersistedActorRef::AgentExecution(id) => Some(id.to_string()),
                    _ => None,
                })
        };
        let Some(execution) = execution else {
            return Ok(None);
        };
        let grant = pioneer_crud::load_agent_execution_grant(&db, &execution)
            .await?
            .ok_or_else(|| anyhow!("source unavailable or access denied"))?;
        let grant: Value = serde_json::from_str(&grant.grant_json)?;
        let role = grant
            .get("role_key")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("source unavailable or access denied"))?
            .to_owned();
        let actions = serde_json::from_value(
            grant
                .get("allowed_actions")
                .cloned()
                .ok_or_else(|| anyhow!("source unavailable or access denied"))?,
        )?;
        Ok(Some((role, actions)))
    }

    pub(super) async fn thread_source_readable(
        &self,
        authority: &ThreadReadAuthority,
        thread_id: &str,
    ) -> Result<bool> {
        let action = ResourceAction::ThreadRead;
        let gate = AuthorizationService::new().authorize_action(
            authority.principal.kind,
            authority.principal.role_key.as_ref(),
            action,
        );
        let resolver = AuthorizationResolver::new(self.crud_store.as_ref().clone());
        let mut proof = resolver
            .authorize_thread(
                &authority.principal,
                &gate,
                action,
                thread_id,
                Some(&authority.workspace_id),
            )
            .await?;
        if !matches!(proof, ProofResolution::Authorized(_)) {
            proof = resolver
                .authorize_internal_thread_via_root(
                    &authority.principal,
                    &gate,
                    action,
                    thread_id,
                    Some(&authority.workspace_id),
                )
                .await?;
        }
        Ok(match proof {
            ProofResolution::Authorized(proof) => {
                let internal = proof.collaboration_root_thread_id() != proof.thread_id();
                (!internal
                    || authority.observe_children
                    || thread_id == authority.current_thread_id)
                    && (authority.observe_workspace
                        || proof.collaboration_root_thread_id() == authority.root_thread_id)
            }
            _ => false,
        })
    }

    fn thread_read_cursor_codec(&self) -> Result<ThreadReadCursorCodec> {
        let secret = self
            .gateway_secrets
            .load_or_create_auth_credential_hmac_key(64)?;
        ThreadReadCursorCodec::new(secret.as_bytes())
    }

    async fn thread_list_page(
        &self,
        authority: &ThreadReadAuthority,
        params: ThreadsListParams,
        codec: &ThreadReadCursorCodec,
    ) -> Result<Value> {
        let scope = authority.scope("list", "", false, params.limit);
        let state: Option<ListCursor> = params
            .cursor
            .as_deref()
            .map(|c| codec.decode(c, &scope))
            .transpose()?;
        let rows = self
            .crud_store
            .list_thread_read_candidates_page(
                &authority.workspace_id,
                state.as_ref().map(|s| s.after.as_str()),
                CANDIDATE_QUANTUM,
            )
            .await?;
        let mut threads = Vec::new();
        let mut after = None;
        let mut done = rows.len() < CANDIDATE_QUANTUM as usize;
        for (index, row) in rows.iter().enumerate() {
            if self.thread_source_readable(authority, &row).await? {
                let metadata = self.thread_read_metadata(authority, &row).await?;
                let mut proposed = threads.clone();
                proposed.push(metadata.clone());
                if !thread_metadata_page_fits(&proposed)? {
                    done = false;
                    break;
                }
                threads.push(metadata);
            }
            after = Some(row.clone());
            if threads.len() == params.limit as usize {
                done = index + 1 == rows.len() && rows.len() < CANDIDATE_QUANTUM as usize;
                break;
            }
        }
        let next = if done {
            None
        } else {
            Some(codec.encode(
                &ListCursor {
                    after: after.ok_or_else(|| anyhow!("thread metadata exceeds page budget"))?,
                },
                &scope,
            )?)
        };
        Ok(json!({"threads":threads,"nextCursor":next,"eof":done}))
    }

    async fn thread_read_metadata(
        &self,
        authority: &ThreadReadAuthority,
        thread_id: &str,
    ) -> Result<Value> {
        let row = self
            .crud_store
            .get_thread_by_id(thread_id)
            .await?
            .ok_or_else(|| anyhow!("source unavailable or access denied"))?;
        ensure!(
            row.workspace_id == authority.workspace_id,
            "source unavailable or access denied"
        );
        let title = row
            .name
            .as_ref()
            .map(|s| s.chars().take(256).collect::<String>());
        let mut value = json!({"threadId":row.id,"title":title,"titleTruncated":row.name.as_ref().is_some_and(|s| s.chars().count()>256),"preview":row.preview.chars().take(160).collect::<String>(),"createdAt":row.created_at.timestamp(),"updatedAt":row.updated_at.timestamp(),"visibility":row.sidebar_visibility,"status":row.status,"archived":row.status == "closed"});
        if let Some(lineage) = self.crud_store.get_task_thread_lineage(thread_id).await? {
            if self
                .thread_source_readable(authority, &lineage.parent_thread_id)
                .await?
            {
                value["parentThreadId"] = json!(lineage.parent_thread_id);
            }
            if self
                .thread_source_readable(authority, &lineage.root_thread_id)
                .await?
            {
                value["rootThreadId"] = json!(lineage.root_thread_id);
            }
        }
        Ok(value)
    }

    // Recover only the current ancestor path from durable single-parent edges.
    // The stateless cursor stores a constant-sized DFS position, not the tree
    // or an ever-growing frontier. Reparenting/deletion invalidates the cursor.
    async fn thread_children_path(&self, root: &str, node: &str) -> Result<Vec<String>> {
        let mut path = vec![node.to_owned()];
        let mut seen = HashSet::from([node.to_owned()]);
        while path.last().is_some_and(|n| n != root) {
            let lineage = self
                .crud_store
                .get_task_thread_lineage(path.last().unwrap())
                .await?
                .ok_or_else(|| anyhow!("children cursor is stale; restart listing"))?;
            ensure!(
                seen.insert(lineage.parent_thread_id.clone()),
                "children cursor is stale; cyclic lineage"
            );
            path.push(lineage.parent_thread_id);
        }
        path.reverse();
        Ok(path)
    }

    async fn thread_children_page(
        &self,
        authority: &ThreadReadAuthority,
        params: ThreadsChildrenListParams,
        codec: &ThreadReadCursorCodec,
    ) -> Result<Value> {
        ensure!(
            self.thread_source_readable(authority, &params.thread_id)
                .await?,
            "source unavailable or access denied"
        );
        let scope = authority.scope(
            "children",
            &params.thread_id,
            params.recursive,
            params.limit,
        );
        let mut state = params
            .cursor
            .as_deref()
            .map(|c| codec.decode::<ChildrenCursor>(c, &scope))
            .transpose()?
            .unwrap_or(ChildrenCursor {
                node: params.thread_id.clone(),
                after: None,
                path_fingerprint: children_path_fingerprint(std::slice::from_ref(
                    &params.thread_id,
                ))?,
            });
        ensure!(
            params.recursive || state.node == params.thread_id,
            "invalid children cursor"
        );
        let mut path = self
            .thread_children_path(&params.thread_id, &state.node)
            .await?;
        ensure!(
            state.path_fingerprint == children_path_fingerprint(&path)?,
            "children cursor is stale; lineage changed"
        );
        let mut threads = Vec::new();
        let mut done = false;
        for _ in 0..CANDIDATE_QUANTUM {
            let rows = self
                .crud_store
                .list_thread_child_lineage_page(&state.node, state.after.as_deref(), 1)
                .await?;
            let Some(edge) = rows.first() else {
                if path.len() == 1 {
                    done = true;
                    break;
                }
                state.after = path.pop();
                state.node = path.last().unwrap().clone();
                continue;
            };
            // Self edges and cycles cannot trap traversal or include the root.
            if path.contains(&edge.child_thread_id) {
                state.after = Some(edge.child_thread_id.clone());
                continue;
            }
            if self
                .thread_source_readable(authority, &edge.child_thread_id)
                .await?
            {
                let mut metadata = self
                    .thread_read_metadata(authority, &edge.child_thread_id)
                    .await?;
                // Parent may be unavailable even when a descendant is readable.
                metadata.as_object_mut().unwrap().remove("parentThreadId");
                metadata.as_object_mut().unwrap().remove("rootThreadId");
                for field in ["preview", "createdAt", "updatedAt", "archived"] {
                    metadata.as_object_mut().unwrap().remove(field);
                }
                if self
                    .thread_source_readable(authority, &edge.parent_thread_id)
                    .await?
                {
                    metadata["parentThreadId"] = json!(edge.parent_thread_id);
                }
                metadata["depth"] = json!(path.len());
                let mut proposed = threads.clone();
                proposed.push(metadata.clone());
                if !thread_metadata_page_fits(&proposed)? {
                    break;
                }
                threads.push(metadata);
            }
            if params.recursive {
                path.push(edge.child_thread_id.clone());
                state.node = edge.child_thread_id.clone();
                state.after = None;
            } else {
                state.after = Some(edge.child_thread_id.clone());
            }
            if threads.len() == params.limit as usize {
                break;
            }
        }
        state.path_fingerprint = children_path_fingerprint(&path)?;
        let next = if done {
            None
        } else {
            Some(codec.encode(&state, &scope)?)
        };
        Ok(json!({"threads":threads,"nextCursor":next,"eof":done}))
    }
}

struct ThreadReadHandler {
    processor: Arc<MessageProcessor>,
    context: TurnToolContext,
}
#[async_trait]
impl ToolHandler for ThreadReadHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let name = invocation.tool_name.clone();
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(ToolError::invalid_arguments("expected function arguments"));
        };
        let result = self
            .read(&name, arguments)
            .await
            .map_err(|e| safe_read_error(&e))?;
        let text = serde_json::to_string(&result)
            .map_err(|_| ToolError::execution_failed("response encoding failed"))?;
        if text.len() > RESPONSE_BYTES || pioneer_compaction::text_tokens(&text) > 4096 {
            return Err(ToolError::execution_failed(
                "read response exceeds page budget",
            ));
        }
        Ok(Box::new(FunctionToolOutput::with_payload(
            text, true, result,
        )))
    }
}
impl ThreadReadHandler {
    async fn read(&self, name: &str, arguments: Value) -> Result<Value> {
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("limit must be between 1 and 100"))?;
        ensure!(
            (1..=100).contains(&limit),
            "limit must be between 1 and 100"
        );
        let requested_thread_id = arguments
            .get("threadId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let authority = self
            .processor
            .thread_read_authority(&self.context)
            .await
            .map_err(|_| anyhow!("source unavailable or access denied"))?;
        let codec = self.processor.thread_read_cursor_codec()?;
        let mut result = match name {
            "threads_search" => {
                self.processor
                    .thread_search_page(
                        &self.context,
                        &authority,
                        serde_json::from_value(arguments)
                            .map_err(|_| anyhow!("invalid search parameters"))?,
                        &codec,
                    )
                    .await
            }
            "threads_list" => {
                self.processor
                    .thread_list_page(
                        &authority,
                        serde_json::from_value(arguments)
                            .map_err(|_| anyhow!("invalid list parameters"))?,
                        &codec,
                    )
                    .await
            }
            "threads_children_list" => {
                self.processor
                    .thread_children_page(
                        &authority,
                        serde_json::from_value(arguments)
                            .map_err(|_| anyhow!("invalid children parameters"))?,
                        &codec,
                    )
                    .await
            }
            "threads_history_read" => {
                let params: ThreadsHistoryReadParams = serde_json::from_value(arguments)
                    .map_err(|_| anyhow!("invalid history parameters"))?;
                ensure!(
                    self.processor
                        .thread_source_readable(&authority, &params.thread_id)
                        .await?,
                    "source unavailable or access denied"
                );
                let scope_for = |direction: ThreadHistoryDirection| {
                    json!([
                        authority.scope("history", &params.thread_id, false, params.limit),
                        direction
                    ])
                    .to_string()
                };
                let scope = scope_for(params.direction);
                let cursor = params
                    .cursor
                    .as_deref()
                    .map(|c| codec.decode::<Value>(c, &scope))
                    .transpose()?;
                let page = self
                    .processor
                    .read_saved_thread_history_near(
                        &authority.workspace_id,
                        &params.thread_id,
                        params.limit,
                        cursor,
                        params.anchor,
                        params.direction,
                        |child| {
                            let authority = &authority;
                            async move {
                                self.processor
                                    .thread_source_readable(authority, &child)
                                    .await
                            }
                        },
                    )
                    .await?;
                let older = page
                    .older_cursor
                    .as_ref()
                    .map(|state| codec.encode(state, &scope_for(ThreadHistoryDirection::Older)))
                    .transpose()?;
                let newer = page
                    .newer_cursor
                    .as_ref()
                    .map(|state| codec.encode(state, &scope_for(ThreadHistoryDirection::Newer)))
                    .transpose()?;
                let next = if params.direction == ThreadHistoryDirection::Older {
                    older.clone()
                } else {
                    newer.clone()
                };
                let (reverse, reverse_direction) =
                    if params.direction == ThreadHistoryDirection::Older {
                        (newer, ThreadHistoryDirection::Newer)
                    } else {
                        (older, ThreadHistoryDirection::Older)
                    };
                Ok(
                    json!({"threadId":params.thread_id,"events":page.events,"nextCursor":next,
                    "eof":page.eof,"reverseCursor":reverse,"reverseDirection":reverse_direction,"direction":params.direction}),
                )
            }
            _ => Err(anyhow!("unknown thread read operation")),
        }?;
        let current = self
            .processor
            .thread_read_authority(&self.context)
            .await
            .map_err(|_| anyhow!("source unavailable or access denied"))?;
        if let Some(thread) = requested_thread_id {
            ensure!(
                self.processor
                    .thread_source_readable(&current, &thread)
                    .await?,
                "source unavailable or access denied"
            );
        }
        if let Some(rows) = result.get_mut("threads").and_then(Value::as_array_mut) {
            for row in rows {
                let id = row["threadId"]
                    .as_str()
                    .ok_or_else(|| anyhow!("source unavailable or access denied"))?;
                ensure!(
                    self.processor.thread_source_readable(&current, id).await?,
                    "source unavailable or access denied"
                );
                for field in ["parentThreadId", "rootThreadId"] {
                    if let Some(reference) = row.get(field).and_then(Value::as_str) {
                        if !self
                            .processor
                            .thread_source_readable(&current, reference)
                            .await?
                        {
                            row.as_object_mut().unwrap().remove(field);
                        }
                    }
                }
            }
        }
        if let Some(events) = result.get("events").and_then(Value::as_array) {
            for event in events {
                for path in [
                    "/childReference/threadId",
                    "/sourceThreadReference/threadId",
                    "/content/sourceThreadReference/threadId",
                ] {
                    if let Some(source) = event.pointer(path).and_then(Value::as_str) {
                        ensure!(
                            self.processor
                                .thread_source_readable(&current, source)
                                .await?,
                            "source unavailable or access denied"
                        );
                    }
                }
            }
        }
        Ok(result)
    }
}

// Storage errors may contain SQL/resource identities. Never expose them through
// an agent tool; only the bounded public error vocabulary crosses this boundary.
fn safe_read_error(error: &anyhow::Error) -> ToolError {
    let message = error.to_string();
    let safe = if message.starts_with("search session expired") {
        "search session expired or evicted; restart search without cursor"
    } else if message.starts_with("search limit must") {
        "search limit must be between 1 and 20"
    } else if message.starts_with("query must") {
        "query must contain 1..4096 bytes"
    } else if message.contains("search is busy") {
        "thread search is busy; retry later"
    } else if message.contains("cursor is stale") || message.contains("source changed; restart") {
        "cursor is stale; restart reading from the first page"
    } else if message.contains("cursor") {
        "invalid cursor or cursor scope/parameters mismatch"
    } else if message.starts_with("limit must") {
        "limit must be between 1 and 100"
    } else if message.starts_with("invalid ") && message.ends_with(" parameters") {
        "invalid read parameters"
    } else {
        "source unavailable or access denied"
    };
    ToolError::invalid_arguments(safe)
}

#[cfg(test)]
#[path = "thread_read_tools/tests.rs"]
mod tests;

fn thread_metadata_page_fits(threads: &[Value]) -> Result<bool> {
    let encoded = serde_json::to_string(threads)?;
    Ok(encoded.len() <= 28 * 1024 && pioneer_compaction::text_tokens(&encoded) <= 2048)
}

fn children_path_fingerprint(path: &[String]) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(path)?)))
}
