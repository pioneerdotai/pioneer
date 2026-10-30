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

pub(crate) const THREAD_READ_TOOL_NAMES: [&str; 3] = [
    "threads_list",
    "threads_history_read",
    "threads_children_list",
];
const CANDIDATE_QUANTUM: u64 = 200;

pub(crate) fn thread_read_tool_specs() -> Vec<ConfiguredToolSpec> {
    let limit = json!({"type":"integer","minimum":1,"maximum":100});
    let cursor = json!({"type":["string","null"],"description":"Opaque nextCursor from the same operation, resource and parameters; omit/null for first page."});
    let definitions = [
        (
            "threads_list",
            "List accessible saved conversations in the current workspace, including hidden internal children and archived conversations. Bounded pages ordered by created_at DESC, id DESC (newest creation first, not latest activity). Follow nextCursor until eof; an empty nonterminal page is possible. Continuation retains the last candidate's full creation date and ID; unrelated activity does not invalidate it. New threads before that position appear on a fresh first page. Access is rechecked on every page. Oversized metadata uses UTF-8 JSON fragments: concatenate text by byte offsets before decoding; changes to the continued card invalidate its fragments. Invalid or incompatible cursors require restarting the list. No search. Read a selected conversation with threads_history_read.",
            json!({"limit":limit,"cursor":cursor}),
            vec!["limit"],
        ),
        (
            "threads_history_read",
            "Read one conversation's saved history, newest to oldest by default. Optionally open near an exact saved anchor {turnId,itemId?}; omit itemId to anchor the turn. Initial pages include bounded neighboring context. Use direction=older (default) or newer with nextCursor, or reverseCursor and its reverseDirection to continue within the same snapshot; omit anchor on continuation and retain limit. nextCursor/eof apply to the requested direction. Every page is displayed newest to oldest. Follow nextCursor to finish JSON fragments of oversized records before changing direction. List metadata fragments contain canonical JSON: concatenate fragment.text by byte offsets before decoding. Child references identify separate histories: open a child with another threads_history_read call, then resume the parent with its original cursor. Children are never loaded automatically; threads_children_list is optional. Read large saved tool results with threads_tools_result_read using the exact supplied reference; output_log=true reads saved logs.",
            json!({"threadId":{"type":"string"},"limit":limit,"cursor":cursor,
                "anchor":{"type":["object","null"],"properties":{"turnId":{"type":"string"},"itemId":{"type":["string","null"]}},"required":["turnId"],"additionalProperties":false,"description":"Exact saved source in this thread; omit on cursor continuation."},
                "direction":{"type":"string","enum":["older","newer"],"default":"older"}}),
            vec!["threadId", "limit"],
        ),
        (
            "threads_children_list",
            "Optionally list direct children (recursive=false) or every descendant of this specific conversation (recursive=true). Does not read child history. Follow nextCursor until eof even if a page is empty. Changes to traversed parent edges or returned child metadata invalidate cursors; restart listing. Unrelated conversations and activity do not invalidate this traversal. Oversized metadata uses UTF-8 JSON fragments: concatenate text by byte offsets before decoding. Open selected children with threads_history_read; this list is not a prerequisite for reading history.",
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
    principal: AuthenticatedSessionPrincipal,
    pub(super) workspace_id: String,
    root_thread_id: String,
    current_thread_id: String,
    current_turn_id: String,
    observe_children: bool,
    observe_workspace: bool,
}
impl ThreadReadAuthority {
    fn list_scope(&self, limit: u32) -> String {
        json!([
            self.scope("list-created-desc-v1", "", false, limit),
            self.current_turn_id
        ])
        .to_string()
    }

    pub(super) fn scope(
        &self,
        operation: &str,
        resource: &str,
        recursive: bool,
        limit: u32,
    ) -> String {
        // JSON encoding avoids delimiter ambiguity in imported legacy IDs.
        json!([
            "thread-read-v2",
            self.root_thread_id,
            self.current_thread_id,
            self.observe_children,
            self.observe_workspace,
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
    after: Option<pioneer_crud::ThreadReadPosition>,
    fragment: Option<MetadataFragment>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataFragment {
    thread_id: String,
    digest: String,
    offset: usize,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildrenCursor {
    revision: String,
    node: String,
    after: Option<String>,
    path_fingerprint: String,
    fragment: Option<MetadataFragment>,
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
        let mut agent_observe = false;
        let mut observe_children = false;
        if let Some(binding) = self.agent_action_binding(&context.turn_id).await {
            let facts = binding.adapter.lock().await.persistence_facts();
            let grants = &facts.agent_authorization_allowed_actions;
            let policy = RoleDefinitionRegistry::new();
            ensure!(
                grants
                    .iter()
                    .any(|name| name == ResourceAction::ThreadRead.safe_name())
                    && policy.agent_policy_allows(
                        &facts.agent_authorization_role_key,
                        ResourceAction::ThreadRead
                    ),
                "source unavailable or access denied"
            );
            observe_children = grants
                .iter()
                .any(|name| name == ResourceAction::ChildObserve.safe_name())
                && policy.agent_policy_allows(
                    &facts.agent_authorization_role_key,
                    ResourceAction::ChildObserve,
                );
            agent_observe = grants
                .iter()
                .any(|name| name == ResourceAction::ThreadObserveWorkspace.safe_name())
                && policy.agent_policy_allows(
                    &facts.agent_authorization_role_key,
                    ResourceAction::ThreadObserveWorkspace,
                );
        }
        let mut authority = ThreadReadAuthority {
            principal,
            workspace_id: context.workspace_id.clone(),
            root_thread_id: envelope.root_thread_id().to_owned(),
            current_thread_id: context.thread_id.clone(),
            current_turn_id: context.turn_id.clone(),
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

    pub(super) fn thread_read_cursor_codec(&self) -> Result<ThreadReadCursorCodec> {
        let secret = self
            .gateway_secrets
            .load_or_create_auth_credential_hmac_key(64)?;
        ThreadReadCursorCodec::new(secret.as_bytes())
    }

    // Children depend on actual parent edges within the requested traversal and
    // the fields they return, not unrelated workspace activity. Recomputing the
    // subtree also detects reparenting/deletion ahead of the current position.
    // Each read releases DB capacity before hashing; only the DFS ancestor path
    // is retained, never a complete tree or an accumulated result list.
    async fn thread_children_revision(
        &self,
        workspace: &str,
        root: &str,
        recursive: bool,
    ) -> Result<String> {
        let mut digest = Sha256::new();
        let root_workspace = self
            .crud_store
            .get_thread_by_id(root)
            .await?
            .map(|row| row.workspace_id);
        digest.update(serde_json::to_vec(&json!([
            "children-tree-v1",
            workspace,
            root,
            recursive,
            root_workspace
        ]))?);
        let mut path = vec![root.to_owned()];
        let mut after = None;
        loop {
            let rows = self
                .crud_store
                .list_thread_child_lineage_page(path.last().unwrap(), after.as_deref(), 1)
                .await?;
            let Some(edge) = rows.first() else {
                if path.len() == 1 {
                    break;
                }
                after = path.pop();
                continue;
            };
            digest.update(serde_json::to_vec(&json!([
                edge.parent_thread_id,
                edge.child_thread_id
            ]))?);
            if path.contains(&edge.child_thread_id) {
                after = Some(edge.child_thread_id.clone());
                continue;
            }
            let row = self
                .crud_store
                .get_thread_by_id(&edge.child_thread_id)
                .await?;
            digest.update(serde_json::to_vec(&row.map(|row| {
                json!([
                    row.workspace_id,
                    row.name,
                    row.status,
                    row.sidebar_visibility
                ])
            }))?);
            if recursive {
                path.push(edge.child_thread_id.clone());
                after = None;
            } else {
                after = Some(edge.child_thread_id.clone());
            }
        }
        Ok(hex::encode(digest.finalize()))
    }

    async fn thread_list_page(
        &self,
        authority: &ThreadReadAuthority,
        params: ThreadsListParams,
        codec: &ThreadReadCursorCodec,
    ) -> Result<Value> {
        // Changing only the list operation's authenticated scope rejects legacy
        // ID-ascending/revision cursors without changing history or children.
        let scope = authority.list_scope(params.limit);
        let state: Option<ListCursor> = params
            .cursor
            .as_deref()
            .map(|c| codec.decode(c, &scope))
            .transpose()?;
        let mut state = state.unwrap_or(ListCursor {
            after: None,
            fragment: None,
        });
        let continuing_fragment = state.fragment.is_some();
        let rows = if let Some(fragment) = &state.fragment {
            let position = state
                .after
                .as_ref()
                .filter(|position| position.id == fragment.thread_id)
                .ok_or_else(|| anyhow!("invalid list cursor"))?;
            ensure!(
                self.thread_source_readable(authority, &fragment.thread_id)
                    .await?,
                "source unavailable or access denied"
            );
            // Resume this exact card, regardless of new candidates around it.
            // Its digest is checked by metadata_page_record below. Ordinary
            // completed candidates never need to be looked up again.
            vec![position.clone()]
        } else {
            self.crud_store
                .list_thread_read_candidates_page(
                    &authority.workspace_id,
                    state.after.as_ref(),
                    CANDIDATE_QUANTUM,
                )
                .await?
        };
        let mut threads = Vec::new();
        let mut done = !continuing_fragment && rows.len() < CANDIDATE_QUANTUM as usize;
        for (index, row) in rows.iter().enumerate() {
            if self.thread_source_readable(authority, &row.id).await? {
                let metadata = self.thread_read_metadata(authority, &row.id).await?;
                let (metadata, fragment) = metadata_page_record(
                    metadata,
                    state.fragment.as_ref(),
                    |candidate, fragment| {
                        let next = codec.encode(
                            &ListCursor {
                                after: Some(row.clone()),
                                fragment: fragment.cloned(),
                            },
                            &scope,
                        )?;
                        let done = !continuing_fragment
                            && fragment.is_none()
                            && index + 1 == rows.len()
                            && rows.len() < CANDIDATE_QUANTUM as usize;
                        let next = if done { None } else { Some(next) };
                        let mut proposed = threads.clone();
                        proposed.push(candidate.clone());
                        Ok(pioneer_tools::thread_read_page_fits(
                            &json!({"threads":proposed,"nextCursor":next,"eof":done}),
                        ))
                    },
                    threads.is_empty(),
                )?;
                let Some(metadata) = metadata else {
                    done = false;
                    break;
                };
                threads.push(metadata);
                state.after = Some(row.clone());
                state.fragment = fragment;
                if state.fragment.is_some() {
                    done = false;
                    break;
                }
            } else {
                ensure!(
                    state.fragment.is_none(),
                    "source unavailable or access denied"
                );
            }
            state.after = Some(row.clone());
            if threads.len() == params.limit as usize {
                done = !continuing_fragment
                    && index + 1 == rows.len()
                    && rows.len() < CANDIDATE_QUANTUM as usize;
                break;
            }
        }
        ensure!(
            state.fragment.is_none() || !rows.is_empty(),
            "cursor is stale; restart reading"
        );
        let next = if done {
            None
        } else {
            Some(codec.encode(&state, &scope)?)
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
        let title = row.name.clone();
        let mut value = json!({"threadId":row.id,"title":title,"titleTruncated":false,"preview":row.preview.chars().take(160).collect::<String>(),"previewTruncated":row.preview.chars().count()>160,"createdAt":row.created_at.timestamp(),"updatedAt":row.updated_at.timestamp(),"visibility":row.sidebar_visibility,"status":row.status,"archived":row.status == "closed"});
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
            "children-tree-v1",
            &params.thread_id,
            params.recursive,
            params.limit,
        );
        let revision = self
            .thread_children_revision(&authority.workspace_id, &params.thread_id, params.recursive)
            .await?;
        let mut state = params
            .cursor
            .as_deref()
            .map(|c| codec.decode::<ChildrenCursor>(c, &scope))
            .transpose()?
            .unwrap_or(ChildrenCursor {
                revision: revision.clone(),
                node: params.thread_id.clone(),
                after: None,
                path_fingerprint: children_path_fingerprint(std::slice::from_ref(
                    &params.thread_id,
                ))?,
                fragment: None,
            });
        ensure!(
            state.revision == revision,
            "children cursor is stale; restart reading"
        );
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
        let mut checkpoint = (state.clone(), path.clone());
        for _ in 0..CANDIDATE_QUANTUM {
            if !threads.is_empty() && !children_page_fits(&threads, &state, codec, &scope)? {
                (state, path) = checkpoint.clone();
                break;
            }
            checkpoint = (state.clone(), path.clone());
            let rows = self
                .crud_store
                .list_thread_child_lineage_page(&state.node, state.after.as_deref(), 1)
                .await?;
            let Some(edge) = rows.first() else {
                ensure!(state.fragment.is_none(), "cursor is stale; restart reading");
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
                for field in [
                    "preview",
                    "previewTruncated",
                    "createdAt",
                    "updatedAt",
                    "archived",
                ] {
                    metadata.as_object_mut().unwrap().remove(field);
                }
                if self
                    .thread_source_readable(authority, &edge.parent_thread_id)
                    .await?
                {
                    metadata["parentThreadId"] = json!(edge.parent_thread_id);
                }
                metadata["depth"] = json!(path.len());
                let (metadata, fragment) = metadata_page_record(
                    metadata,
                    state.fragment.as_ref(),
                    |candidate, fragment| {
                        let mut next_state = state.clone();
                        let mut next_path = path.clone();
                        next_state.fragment = fragment.cloned();
                        if fragment.is_none() {
                            if params.recursive {
                                next_path.push(edge.child_thread_id.clone());
                                next_state.node = edge.child_thread_id.clone();
                                next_state.after = None;
                            } else {
                                next_state.after = Some(edge.child_thread_id.clone());
                            }
                        }
                        next_state.path_fingerprint = children_path_fingerprint(&next_path)?;
                        let next = codec.encode(&next_state, &scope)?;
                        let mut proposed = threads.clone();
                        proposed.push(candidate.clone());
                        Ok(pioneer_tools::thread_read_page_fits(
                            &json!({"threads":proposed,"nextCursor":next,"eof":false}),
                        ))
                    },
                    threads.is_empty(),
                )?;
                let Some(metadata) = metadata else {
                    break;
                };
                threads.push(metadata);
                state.fragment = fragment;
                if state.fragment.is_some() {
                    break;
                }
            }
            ensure!(state.fragment.is_none(), "cursor is stale; restart reading");
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
        if !done && !threads.is_empty() && !children_page_fits(&threads, &state, codec, &scope)? {
            (state, path) = checkpoint.clone();
        }
        state.path_fingerprint = children_path_fingerprint(&path)?;
        let next = if done {
            None
        } else {
            Some(codec.encode(&state, &scope)?)
        };
        ensure!(
            self.thread_children_revision(
                &authority.workspace_id,
                &params.thread_id,
                params.recursive,
            )
            .await?
                == revision,
            "cursor is stale; restart reading"
        );
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
        if !pioneer_tools::thread_read_page_fits(&result) {
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
const PUBLIC_READ_ERRORS: [(&str, &str); 5] = [
    (
        "cursor is stale; restart reading from the first page",
        "read_source_changed",
    ),
    (
        "invalid or incompatible cursor; restart reading with the same resource and parameters",
        "read_cursor_incompatible",
    ),
    ("limit must be between 1 and 100", "read_invalid_parameters"),
    ("invalid read parameters", "read_invalid_parameters"),
    (
        "source unavailable or access denied",
        "read_source_unavailable",
    ),
];

/// Only exact native public messages can cross the CLI adapter. Never publish
/// arbitrary ToolError diagnostics, including router/runtime failures.
pub(crate) fn public_read_error(error: &ToolError) -> (&'static str, &'static str) {
    if let ToolError::InvalidArguments(message) = error {
        if let Some(public) = PUBLIC_READ_ERRORS
            .iter()
            .find(|(text, _)| *text == message.as_str())
        {
            return *public;
        }
    }
    PUBLIC_READ_ERRORS[4]
}

fn safe_read_error(error: &anyhow::Error) -> ToolError {
    let message = error.to_string();
    let safe = if message.contains("cursor is stale") || message.contains("source changed; restart")
    {
        PUBLIC_READ_ERRORS[0].0
    } else if message.contains("cursor") {
        PUBLIC_READ_ERRORS[1].0
    } else if message.starts_with("limit must") {
        PUBLIC_READ_ERRORS[2].0
    } else if message.starts_with("invalid ") && message.ends_with(" parameters") {
        PUBLIC_READ_ERRORS[3].0
    } else {
        PUBLIC_READ_ERRORS[4].0
    };
    ToolError::invalid_arguments(safe)
}

#[cfg(test)]
#[path = "thread_read_tools/tests.rs"]
pub(in crate::message) mod tests;

// Only one metadata record is fragmented. No accumulated rows enter the cursor.
fn metadata_page_record<F>(
    metadata: Value,
    fragment: Option<&MetadataFragment>,
    mut fits: F,
    empty: bool,
) -> Result<(Option<Value>, Option<MetadataFragment>)>
where
    F: FnMut(&Value, Option<&MetadataFragment>) -> Result<bool>,
{
    if fragment.is_none() && fits(&metadata, None)? {
        return Ok((Some(metadata), None));
    }
    if !empty {
        return Ok((None, fragment.cloned()));
    }
    let encoded = serde_json::to_string(&metadata)?;
    let digest = hex::encode(Sha256::digest(encoded.as_bytes()));
    let thread_id = metadata["threadId"]
        .as_str()
        .ok_or_else(|| anyhow!("metadata source unavailable"))?
        .to_owned();
    let offset = fragment.map_or(0, |f| f.offset);
    if let Some(f) = fragment {
        ensure!(
            f.thread_id == thread_id
                && f.digest == digest
                && offset < encoded.len()
                && encoded.is_char_boundary(offset),
            "cursor is stale; restart reading"
        );
    }
    let make = |end| {
        let mut record = json!({"threadId":thread_id,"fragment":{"encoding":"json","version":digest,"offset":offset,"nextOffset":end,"eof":end == encoded.len(),"text":&encoded[offset..end]}});
        for field in ["parentThreadId", "rootThreadId"] {
            if let Some(id) = metadata.get(field) {
                record[field] = id.clone();
            }
        }
        let next = (end < encoded.len()).then(|| MetadataFragment {
            thread_id: thread_id.clone(),
            digest: digest.clone(),
            offset: end,
        });
        (record, next)
    };
    let mut low = offset;
    let mut high = encoded
        .len()
        .min(offset.saturating_add(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES));
    let mut best = None;
    while low <= high {
        let mid = low + (high - low) / 2;
        let mut end = mid;
        while !encoded.is_char_boundary(end) {
            end -= 1;
        }
        let (record, next) = make(end);
        if fits(&record, next.as_ref())? {
            best = Some((end, record, next));
            low = mid + 1;
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
    if encoded.len() - offset <= pioneer_protocol::TOOL_RESPONSE_MAX_BYTES {
        let (record, next) = make(encoded.len());
        if fits(&record, next.as_ref())? {
            best = Some((encoded.len(), record, next));
        }
    }
    let (end, record, next) =
        best.ok_or_else(|| anyhow!("metadata framing exceeds page budget"))?;
    ensure!(end > offset, "metadata framing exceeds page budget");
    Ok((Some(record), next))
}

fn children_page_fits(
    threads: &[Value],
    state: &ChildrenCursor,
    codec: &ThreadReadCursorCodec,
    scope: &str,
) -> Result<bool> {
    Ok(pioneer_tools::thread_read_page_fits(
        &json!({"threads":threads,"nextCursor":codec.encode(state, scope)?,"eof":false}),
    ))
}

fn children_path_fingerprint(path: &[String]) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(path)?)))
}
