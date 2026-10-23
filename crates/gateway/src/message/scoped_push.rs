//! Connection-owned, bounded derived-value delivery. No offline subscribers.
use super::*;
use crate::authorization::{
    AuthorizationResolver, AuthorizationService, ProofResolution, ResourceAction,
};
use anyhow::anyhow;
use axum::extract::ws::Message;
use pioneer_protocol::{
    TaskInboxPatch, ThreadIndicatorPatch, WorkspacePushNotification, WorkspacePushParams,
    WorkspacePushResponse,
};
use std::collections::BTreeSet;

const MAX_SCOPES: usize = 256;
const MAX_METADATA_IDS: usize = 4096;
const MAX_DIRTY_IDS: usize = 100;
const MAX_PACKET_BYTES: usize = 1024 * 1024;

#[derive(Default)]
pub(super) struct PushScopes(std::sync::Mutex<HashMap<ConnectionId, Arc<PushScope>>>);
struct PushScope {
    params: WorkspacePushParams,
    principal: crate::auth::AuthenticatedSessionPrincipal,
    pending: std::sync::Mutex<Pending>,
    wake: tokio::sync::Notify,
    cancel: tokio_util::sync::CancellationToken,
}
#[derive(Default)]
struct Pending {
    generation: u64,
    unread: BTreeSet<String>,
    activity: BTreeSet<String>,
    inbox: BTreeSet<String>,
    metadata: BTreeSet<String>,
    metadata_seen: BTreeSet<String>,
    known_inbox: BTreeSet<String>,
    retained_cursor: Option<String>,
    retained_initialized: bool,
    recovery: bool,
    inbox_reset: bool,
    mutation_holds: usize,
    ready: Option<ReadyValues>,
}
impl Pending {
    fn enforce_limits(&mut self) {
        if self.inbox.len() > MAX_DIRTY_IDS {
            self.inbox.clear();
            self.inbox_reset = true;
            if let Some(ready) = &mut self.ready {
                ready.inbox = None;
            }
        }
        self.recovery |=
            self.unread.len() + self.activity.len() + self.metadata.len() > MAX_DIRTY_IDS;
        if self.recovery {
            self.unread.clear();
            self.activity.clear();
            self.metadata.clear();
            self.inbox.clear();
            self.ready = None;
        }
    }
}
#[derive(Default, Clone)]
struct ReadyValues {
    generation: u64,
    unread: HashMap<String, u64>,
    inbox: Option<TaskInboxPatch>,
}
/// Holds only a source scope, never DB capacity. Drop safely recovers a failed RPC.
pub(super) struct PushMutation {
    scope: Arc<PushScope>,
    generation: u64,
    finished: bool,
}
impl PushMutation {
    pub(super) fn finish(mut self, unread: Option<(String, u64)>, inbox: Option<TaskInboxPatch>) {
        let bounded_inbox = inbox.as_ref().is_none_or(|patch| {
            patch.upserts.len() + patch.removes.len() <= MAX_DIRTY_IDS
                && serde_json::to_vec(patch).is_ok_and(|bytes| bytes.len() <= MAX_PACKET_BYTES)
        });
        let mut pending = self.scope.pending.lock().expect("push pending poisoned");
        let unchanged = pending.generation == self.generation;
        pending.mutation_holds = pending.mutation_holds.saturating_sub(1);
        pending.generation = pending.generation.wrapping_add(1);
        let mut ready = ReadyValues {
            generation: pending.generation,
            ..Default::default()
        };
        if let Some((id, count)) = unread {
            pending.unread.insert(id.clone());
            if unchanged {
                ready.unread.insert(id, count);
            }
        }
        if let Some(patch) = inbox {
            let covered = patch.replace
                || (!pending.inbox_reset
                    && pending.inbox.iter().all(|id| {
                        patch.removes.contains(id)
                            || patch.upserts.iter().any(|row| &row.run_id == id)
                    }));
            if patch.replace {
                pending.inbox_reset = true;
            }
            pending.inbox.extend(patch.removes.iter().cloned());
            pending
                .inbox
                .extend(patch.upserts.iter().map(|row| row.run_id.clone()));
            if unchanged && covered && bounded_inbox {
                ready.inbox = Some(patch);
            }
        }
        pending.ready = unchanged.then_some(ready);
        pending.enforce_limits();
        self.finished = true;
        self.scope.wake.notify_one();
    }
}
impl Drop for PushMutation {
    fn drop(&mut self) {
        if !self.finished {
            let mut pending = self.scope.pending.lock().expect("push pending poisoned");
            pending.mutation_holds = pending.mutation_holds.saturating_sub(1);
            pending.generation = pending.generation.wrapping_add(1);
            pending.recovery = true;
            pending.ready = None;
            self.scope.wake.notify_one();
        }
    }
}

impl PushScopes {
    fn remove(&self, connection: ConnectionId, scope: &Arc<PushScope>) {
        let mut scopes = self.0.lock().expect("push scopes poisoned");
        if scopes
            .get(&connection)
            .is_some_and(|current| Arc::ptr_eq(current, scope))
        {
            scopes.remove(&connection);
        }
    }
}

impl MessageProcessor {
    pub(super) async fn workspace_push(
        &self,
        context: &RequestContext,
        id: RequestId,
        params: WorkspacePushParams,
    ) {
        if params.epoch.is_empty()
            || params.epoch.len() > 128
            || params.thread_ids.len() > MAX_METADATA_IDS
            || params.run_ids.len() > MAX_METADATA_IDS
            || params.inbox_cursor.as_deref().is_some_and(|cursor| {
                super::task_completion_inbox::decode_inbox_cursor(cursor).is_err()
            })
            || params
                .thread_ids
                .iter()
                .chain(&params.run_ids)
                .any(|id| id.is_empty() || id.len() > 128)
        {
            self.send_error(
                context.connection_id(),
                JsonRpcErrorResponse::new(Some(id), INVALID_PARAMS_CODE, "invalid push scope"),
            )
            .await;
            return;
        }
        let pending = Pending {
            known_inbox: params.run_ids.iter().cloned().collect(),
            retained_cursor: params.inbox_cursor.clone(),
            ..Default::default()
        };
        let scope = Arc::new(PushScope {
            params,
            principal: context.principal().clone(),
            pending: std::sync::Mutex::new(pending),
            wake: Default::default(),
            cancel: Default::default(),
        });
        let supported = {
            let mut scopes = self.push_scopes.0.lock().expect("push scopes poisoned");
            let unsubscribe = !scope.params.tree && !scope.params.inbox;
            let matches_workspace = scopes
                .get(&context.connection_id())
                .is_some_and(|old| old.params.workspace_id == scope.params.workspace_id);
            if !unsubscribe || matches_workspace {
                if let Some(old) = scopes.remove(&context.connection_id()) {
                    old.cancel.cancel();
                }
            }
            if !scope.params.tree && !scope.params.inbox {
                true
            } else if scopes.len() >= MAX_SCOPES {
                false
            } else {
                scopes.insert(context.connection_id(), scope.clone());
                true
            }
        };
        self.send_task_response(
            context.connection_id(),
            id,
            &WorkspacePushResponse { supported },
        )
        .await;
        if supported && (scope.params.tree || scope.params.inbox) {
            let processor = self.clone();
            let connection = context.connection_id();
            tokio::spawn(async move {
                processor.run_push_scope(connection, scope).await;
            });
        }
    }

    pub(super) fn begin_push_mutation(
        &self,
        connection: ConnectionId,
        workspace: &str,
        tree: bool,
    ) -> Option<PushMutation> {
        let scopes = self.push_scopes.0.lock().expect("push scopes poisoned");
        let scope = scopes.get(&connection)?.clone();
        if scope.params.workspace_id != workspace
            || (tree && !scope.params.tree)
            || (!tree && !scope.params.inbox)
        {
            return None;
        }
        let mut pending = scope.pending.lock().expect("push pending poisoned");
        pending.generation = pending.generation.wrapping_add(1);
        // Existing WebSocket domain dispatch serializes source RPCs; this is
        // a scalar operation hold, not a queue or an independently scheduled job.
        pending.mutation_holds += 1;
        pending.ready = None;
        let generation = pending.generation;
        drop(pending);
        Some(PushMutation {
            scope,
            generation,
            finished: false,
        })
    }

    pub(super) fn dirty_push_inbox(
        &self,
        workspace: &str,
        principal: Option<&pioneer_protocol::PrincipalId>,
        exclude: Option<ConnectionId>,
    ) {
        let scopes = self.push_scopes.0.lock().expect("push scopes poisoned");
        for (connection, scope) in scopes.iter() {
            if Some(*connection) == exclude
                || !scope.params.inbox
                || scope.params.workspace_id != workspace
                || principal.is_some_and(|id| id != &scope.principal.principal_id)
            {
                continue;
            }
            let mut pending = scope.pending.lock().expect("push pending poisoned");
            pending.generation = pending.generation.wrapping_add(1);
            pending.ready = None;
            pending.inbox_reset = true;
            pending.inbox.clear();
            scope.wake.notify_one();
        }
    }

    pub(super) fn dirty_push_peers(
        &self,
        context: &RequestContext,
        workspace: &str,
        unread: Option<&str>,
        run: Option<&str>,
    ) {
        self.dirty_push_except(
            workspace,
            unread,
            None,
            run,
            false,
            Some(&context.principal().principal_id),
            Some(context.connection_id()),
            false,
        );
    }

    /// Called after durable domain commits. Repetition unions keys; no event queue.
    pub(super) fn dirty_push(
        &self,
        workspace: &str,
        unread: Option<&str>,
        activity: Option<&str>,
        run: Option<&str>,
        recovery: bool,
        principal: Option<&pioneer_protocol::PrincipalId>,
    ) {
        self.dirty_push_except(
            workspace,
            unread,
            activity,
            run,
            recovery,
            principal,
            None,
            unread.is_some(),
        );
    }

    fn dirty_push_except(
        &self,
        workspace: &str,
        unread: Option<&str>,
        activity: Option<&str>,
        run: Option<&str>,
        recovery: bool,
        principal: Option<&pioneer_protocol::PrincipalId>,
        exclude: Option<ConnectionId>,
        metadata: bool,
    ) {
        let scopes = self.push_scopes.0.lock().expect("push scopes poisoned");
        for (connection, scope) in scopes.iter() {
            if Some(*connection) == exclude
                || scope.params.workspace_id != workspace
                || principal.is_some_and(|p| p != &scope.principal.principal_id)
            {
                continue;
            }
            if !recovery
                && !(scope.params.tree && (unread.is_some() || activity.is_some()))
                && !(scope.params.inbox && run.is_some())
            {
                continue;
            }
            let mut pending = scope.pending.lock().expect("push pending poisoned");
            pending.generation = pending.generation.wrapping_add(1);
            pending.ready = None;
            if scope.params.tree {
                if let Some(id) = unread {
                    pending.unread.insert(id.to_owned());
                    if metadata {
                        pending.metadata.insert(id.to_owned());
                        if pending.metadata_seen.len() < MAX_METADATA_IDS {
                            pending.metadata_seen.insert(id.to_owned());
                        } else if !pending.metadata_seen.contains(id) {
                            pending.recovery = true;
                        }
                    }
                }
                if let Some(id) = activity {
                    pending.activity.insert(id.to_owned());
                }
            }
            if scope.params.inbox {
                if let Some(id) = run {
                    pending.inbox.insert(id.to_owned());
                }
            }
            pending.recovery |= recovery;
            pending.enforce_limits();
            scope.wake.notify_one();
        }
    }

    pub(super) fn retire_push_workspace(
        &self,
        workspace: &str,
        principal: Option<&pioneer_protocol::PrincipalId>,
    ) {
        self.push_scopes
            .0
            .lock()
            .expect("push scopes poisoned")
            .retain(|_, scope| {
                if scope.params.workspace_id == workspace
                    && principal.is_none_or(|id| id == &scope.principal.principal_id)
                {
                    scope.cancel.cancel();
                    false
                } else {
                    true
                }
            });
    }

    pub(super) async fn notify_thread_unread_changed(&self, workspace: &str, thread: &str) {
        // Reuse loaded materialized metadata without cloning its timeline.
        // Only an unloaded completion needs one persisted row, never per-peer
        // metadata/history. Authorization remains the publisher's own gate.
        let visible = match self
            .thread_manager
            .loaded_thread_sidebar_visibility(workspace, thread)
            .await
        {
            Some(visibility) => visibility == pioneer_protocol::ThreadSidebarVisibility::Visible,
            None => match self.crud_store.get_thread_by_id(thread).await {
                Ok(Some(row)) if row.workspace_id == workspace => {
                    row.sidebar_visibility == "visible"
                }
                _ => {
                    // Missing/failed metadata cannot establish eligibility.
                    // Recover without accumulating an unproven sidebar ID.
                    self.dirty_push(workspace, None, None, None, true, None);
                    self.notify_thread_unread_legacy(workspace, thread).await;
                    return;
                }
            },
        };
        if !visible {
            return;
        }
        self.dirty_push(workspace, Some(thread), None, None, false, None);
        self.notify_thread_unread_legacy(workspace, thread).await;
    }

    pub(super) async fn notify_thread_unread_legacy(&self, workspace: &str, thread: &str) {
        let notification = ThreadTreeChangedNotification {
            workspace_id: workspace.to_owned(),
        };
        let connections = self
            .session_manager
            .connection_ids_for_workspace(workspace)
            .await;
        // A commit can precede registration while its legacy fanout awaits.
        // Suppress its hint only if this owner actually received its dirty key.
        let connections = {
            let scopes = self.push_scopes.0.lock().expect("push scopes poisoned");
            connections
                .into_iter()
                .filter(|connection| {
                    !scopes.get(connection).is_some_and(|scope| {
                        scope.params.tree
                            && scope
                                .pending
                                .lock()
                                .expect("push pending poisoned")
                                .metadata_seen
                                .contains(thread)
                    })
                })
                .collect()
        };
        self.send_notification_to_reauthorized_workspace_connections(
            workspace,
            events::THREAD_TREE_CHANGED,
            &notification,
            connections,
        )
        .await;
    }

    pub(super) fn legacy_push_connections(
        &self,
        connections: Vec<ConnectionId>,
        tree: bool,
        inbox: bool,
    ) -> Vec<ConnectionId> {
        let scopes = self.push_scopes.0.lock().expect("push scopes poisoned");
        connections
            .into_iter()
            .filter(|id| {
                !scopes
                    .get(id)
                    .is_some_and(|s| (tree && s.params.tree) || (inbox && s.params.inbox))
            })
            .collect()
    }

    async fn run_push_scope(&self, connection: ConnectionId, scope: Arc<PushScope>) {
        let Ok(mut termination) = self
            .session_manager
            .connection_termination_receiver(connection)
            .await
        else {
            self.push_scopes.remove(connection, &scope);
            return;
        };
        let Ok(mut scope_changes) = self
            .session_manager
            .connection_scope_receiver(connection)
            .await
        else {
            self.push_scopes.remove(connection, &scope);
            return;
        };
        // Every helper below inherits Maintenance, including authorization SQL.
        let mut processor = self.clone();
        processor.crud_store = Arc::new(self.crud_store.with_maintenance_access());
        let mut revision = 0_u64;
        let mut retry_dirty = false;
        let mut wait_for_mutation = false;
        loop {
            if (revision != 0 && !retry_dirty) || wait_for_mutation {
                tokio::select! {
                    _ = scope.cancel.cancelled() => break,
                    _ = termination.changed() => break,
                    _ = scope_changes.changed() => break,
                    _ = scope.wake.notified() => {},
                }
            }
            if scope.cancel.is_cancelled() {
                break;
            }
            let (generation, unread, activity, runs, metadata_ids, recovery, inbox_reset, ready) = {
                let mut pending = scope.pending.lock().expect("push pending poisoned");
                if pending.mutation_holds != 0 {
                    wait_for_mutation = true;
                    continue;
                }
                wait_for_mutation = false;
                let values = (
                    pending.generation,
                    std::mem::take(&mut pending.unread),
                    std::mem::take(&mut pending.activity),
                    std::mem::take(&mut pending.inbox),
                    std::mem::take(&mut pending.metadata),
                    pending.recovery,
                    std::mem::take(&mut pending.inbox_reset),
                    pending.ready.take(),
                );
                pending.recovery = false;
                values
            };
            let snapshot = revision == 0;
            if !snapshot
                && !recovery
                && !inbox_reset
                && unread.is_empty()
                && activity.is_empty()
                && runs.is_empty()
                && metadata_ids.is_empty()
            {
                continue;
            }
            let result = if recovery {
                Err(anyhow!("push scope requires recovery"))
            } else {
                tokio::select! {
                    _ = scope.cancel.cancelled() => break,
                    _ = termination.changed() => break,
                    _ = scope_changes.changed() => break,
                    result = processor.compute_push(connection, &scope, snapshot, unread.clone(), activity.clone(), runs.clone(), metadata_ids.clone(), inbox_reset, ready.filter(|ready| ready.generation == generation)) => result,
                }
            };
            // An event during any read/auth stage invalidates the entire result.
            // One coalesced follow-up; repeated dirty/overflow retires the epoch.
            let changed = scope
                .pending
                .lock()
                .expect("push pending poisoned")
                .generation
                != generation;
            if changed && !recovery && !retry_dirty {
                // Preserve the in-flight keys too: a stale read of A followed by
                // a dirty B must reconcile both. Only one follow-up is allowed.
                let mut pending = scope.pending.lock().expect("push pending poisoned");
                pending.unread.extend(unread);
                pending.activity.extend(activity);
                pending.inbox.extend(runs);
                pending.metadata.extend(metadata_ids);
                pending.inbox_reset |= inbox_reset;
                pending.enforce_limits();
                retry_dirty = true;
                continue;
            }
            retry_dirty = false;
            let mut packet = match result {
                Ok(packet) if !changed => packet,
                _ => WorkspacePushNotification {
                    workspace_id: scope.params.workspace_id.clone(),
                    epoch: scope.params.epoch.clone(),
                    revision: 0,
                    snapshot: false,
                    resync_required: true,
                    thread_summaries: Vec::new(),
                    indicators: Vec::new(),
                    inbox: None,
                },
            };
            if let Some(inbox) = packet.inbox.as_mut() {
                let mut pending = scope.pending.lock().expect("push pending poisoned");
                // A workspace hint never disclosed another task's run ID.
                // Removes may therefore name only identities this scope knew.
                inbox.removes.retain(|id| pending.known_inbox.contains(id));
                if !inbox.replace && !packet.snapshot {
                    let additional = inbox
                        .upserts
                        .iter()
                        .filter(|entry| !pending.known_inbox.contains(&entry.run_id))
                        .count();
                    let retained = pending
                        .known_inbox
                        .iter()
                        .filter(|id| !inbox.removes.contains(id))
                        .count();
                    if retained + additional > MAX_METADATA_IDS {
                        pending.inbox_reset = true;
                        pending.inbox.clear();
                        pending.unread.extend(unread);
                        pending.activity.extend(activity);
                        pending.metadata.extend(metadata_ids);
                        pending.generation = pending.generation.wrapping_add(1);
                        pending.ready = None;
                        pending.enforce_limits();
                        retry_dirty = true;
                        continue;
                    }
                }
            }
            revision += 1;
            packet.revision = revision;
            if scope.cancel.is_cancelled() {
                break;
            }
            let Some(text) =
                processor.serialize_notification(events::WORKSPACE_PUSH_CHANGED, &packet)
            else {
                break;
            };
            let mut text = text;
            // A legitimate large bootstrap can trim only at an explicit cap,
            // preserving the exact keyset frontier for normal pagination.
            while text.len() > MAX_PACKET_BYTES
                && (packet.snapshot || packet.inbox.as_ref().is_some_and(|inbox| inbox.replace))
                && packet
                    .inbox
                    .as_ref()
                    .is_some_and(|inbox| inbox.upserts.len() > 1)
            {
                let inbox = packet.inbox.as_mut().expect("checked inbox");
                inbox.upserts.truncate(inbox.upserts.len() / 2);
                inbox.next_cursor = inbox.upserts.last().and_then(|entry| {
                    serde_json::to_string(&(&entry.completed_key, &entry.run_id)).ok()
                });
                let Some(smaller) =
                    processor.serialize_notification(events::WORKSPACE_PUSH_CHANGED, &packet)
                else {
                    break;
                };
                text = smaller;
            }
            if text.len() > MAX_PACKET_BYTES {
                packet.thread_summaries.clear();
                packet.indicators.clear();
                packet.inbox = None;
                packet.resync_required = true;
                let Some(smaller) =
                    processor.serialize_notification(events::WORKSPACE_PUSH_CHANGED, &packet)
                else {
                    break;
                };
                text = smaller;
            }
            let recovery_text = processor.serialize_notification(
                events::WORKSPACE_PUSH_CHANGED,
                &WorkspacePushNotification {
                    workspace_id: scope.params.workspace_id.clone(),
                    epoch: scope.params.epoch.clone(),
                    revision,
                    snapshot: false,
                    resync_required: true,
                    thread_summaries: Vec::new(),
                    indicators: Vec::new(),
                    inbox: None,
                },
            );
            let enqueued = processor
                .session_manager
                .with_notification_outbound(
                    connection,
                    &scope.principal,
                    &scope.params.workspace_id,
                    |outbound| {
                        // No await from the last generation/access gate to try_enqueue.
                        // Commit producers and ACL retirement take this same scope lock.
                        let scopes = self.push_scopes.0.lock().expect("push scopes poisoned");
                        if !scopes
                            .get(&connection)
                            .is_some_and(|current| Arc::ptr_eq(current, &scope))
                            || scope.cancel.is_cancelled()
                        {
                            return Ok(true);
                        }
                        let mut pending = scope.pending.lock().expect("push pending poisoned");
                        let recover = packet.resync_required || pending.generation != generation;
                        let payload = if recover {
                            recovery_text.ok_or_else(|| anyhow!("cannot serialize recovery"))?
                        } else {
                            text
                        };
                        outbound
                            .try_send(Message::Text(payload.into()))
                            .map_err(|_| anyhow!("push outbound full or closed"))?;
                        if !recover {
                            if let Some(inbox) = &packet.inbox {
                                if inbox.replace || packet.snapshot {
                                    pending.known_inbox.clear();
                                    pending.retained_cursor = inbox.next_cursor.clone();
                                    pending.retained_initialized = true;
                                }
                                for id in &inbox.removes {
                                    pending.known_inbox.remove(id);
                                }
                                pending
                                    .known_inbox
                                    .extend(inbox.upserts.iter().map(|entry| entry.run_id.clone()));
                            }
                        }
                        Ok(recover)
                    },
                )
                .await;
            match enqueued {
                Ok(Some(false)) => {}
                Ok(Some(true)) | Ok(None) => break,
                Err(_) => {
                    // Match the existing bounded notification overload policy.
                    processor
                        .session_manager
                        .unregister_connection(connection)
                        .await;
                    break;
                }
            }
        }
        self.push_scopes.remove(connection, &scope);
    }

    async fn compute_push(
        &self,
        connection: ConnectionId,
        scope: &PushScope,
        snapshot: bool,
        unread: BTreeSet<String>,
        activity: BTreeSet<String>,
        runs: BTreeSet<String>,
        metadata_ids: BTreeSet<String>,
        inbox_reset: bool,
        ready: Option<ReadyValues>,
    ) -> anyhow::Result<WorkspacePushNotification> {
        let principal = self
            .session_manager
            .connection_principal(connection)
            .await?;
        anyhow::ensure!(
            principal.as_ref() == &scope.principal,
            "push identity changed"
        );
        if let Some(auth) = &self.auth_service {
            auth.validate_session_lease_with_database(
                &principal,
                &self.crud_store.database_connection(),
            )
            .await?;
        }
        let workspace = &scope.params.workspace_id;
        anyhow::ensure!(
            self.session_manager
                .connection_workspace_id(connection)
                .await
                .as_deref()
                == Some(workspace.as_str()),
            "push workspace changed"
        );
        let resolver = AuthorizationResolver::new((*self.crud_store).clone());
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::WorkspaceRead,
        );
        anyhow::ensure!(
            matches!(
                resolver
                    .authorize_workspace(
                        &principal,
                        &gate,
                        ResourceAction::WorkspaceRead,
                        workspace
                    )
                    .await?,
                ProofResolution::Authorized(_)
            ),
            "push workspace revoked"
        );
        let unread = if snapshot && scope.params.tree {
            scope.params.thread_ids.iter().cloned().collect()
        } else {
            unread
        };
        let activity = if snapshot && scope.params.tree {
            scope.params.thread_ids.iter().cloned().collect()
        } else {
            activity
        };
        let candidates: BTreeSet<_> = unread
            .union(&activity)
            .chain(metadata_ids.iter())
            .cloned()
            .collect();
        let thread_gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        let mut authorized = Vec::new();
        for thread in candidates {
            // Resolve an explicit proof, retaining unavailable as an error:
            // a transient SQL/auth failure is not a safely irrelevant denial.
            let mut resolution = resolver
                .authorize_thread(
                    &principal,
                    &thread_gate,
                    ResourceAction::ThreadRead,
                    &thread,
                    Some(workspace),
                )
                .await?;
            if matches!(
                resolution.denial(),
                Some(crate::authorization::AuthorizationDecision::Deny {
                    reason: crate::authorization::DenyReason::MissingAuthoritativeResource,
                    ..
                })
            ) {
                resolution = resolver
                    .authorize_internal_thread_via_root(
                        &principal,
                        &thread_gate,
                        ResourceAction::ThreadRead,
                        &thread,
                        Some(workspace),
                    )
                    .await?;
            }
            if !matches!(resolution, ProofResolution::Authorized(_)) {
                if scope.params.thread_ids.contains(&thread) {
                    anyhow::bail!("push thread scope changed");
                }
                continue;
            }
            authorized.push(thread);
        }
        let mut metadata = Vec::new();
        for thread_id in metadata_ids.iter().filter(|id| authorized.contains(id)) {
            if let Some(thread) = self.crud_store.get_thread_model(thread_id).await? {
                anyhow::ensure!(
                    thread.workspace_id == *workspace,
                    "metadata workspace changed"
                );
                let original_preview_len = thread.preview.len();
                let thread = thread.into_directory_summary();
                metadata.push(pioneer_protocol::ThreadMetadataPatch {
                    preview_truncated: thread.preview.len() < original_preview_len,
                    thread,
                });
            } else {
                anyhow::bail!("metadata removed");
            }
        }
        let ready = ready.unwrap_or_default();
        let unread_ids: Vec<_> = authorized
            .iter()
            .filter(|id| unread.contains(*id) && !ready.unread.contains_key(*id))
            .cloned()
            .collect();
        let activity_ids: Vec<_> = authorized
            .iter()
            .filter(|id| activity.contains(*id))
            .cloned()
            .collect();
        let mut counts = if unread_ids.is_empty() {
            HashMap::new()
        } else {
            self.crud_store
                .unread_counts_for_threads(&principal.principal_id, &unread_ids)
                .await?
        };
        counts.extend(ready.unread);
        let active = if activity_ids.is_empty() {
            Vec::new()
        } else {
            resolver
                .running_task_thread_ids(&principal, workspace, &activity_ids)
                .await?
        };
        let indicators = authorized
            .into_iter()
            .filter(|id| unread.contains(id) || activity.contains(id))
            .map(|thread_id| ThreadIndicatorPatch {
                unread_count: unread
                    .contains(&thread_id)
                    .then(|| counts.get(&thread_id).copied().unwrap_or(0)),
                has_running_tasks: activity
                    .contains(&thread_id)
                    .then(|| active.contains(&thread_id)),
                thread_id,
            })
            .collect();
        // Cached values come only from this exact authorized source RPC. The
        // captured scope/generation must survive the whole operation; any ACL
        // signal retires the scope, any dirty event invalidates the cache, and
        // the final enqueue checks current principal/workspace/lease expiry.
        // This cache is never reused across connections or authorization scopes.
        let inbox = if scope.params.inbox
            && ready
                .inbox
                .as_ref()
                .is_some_and(|patch| (!(snapshot || inbox_reset)) || patch.replace)
        {
            ready.inbox
        } else if scope.params.inbox && (snapshot || inbox_reset || !runs.is_empty()) {
            let gate = AuthorizationService::new().authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::TaskRead,
            );
            anyhow::ensure!(
                !matches!(gate, crate::authorization::ActionGateDecision::Deny { .. }),
                "push inbox denied"
            );
            let access = pioneer_crud::TaskCompletionInboxAccess {
                workspace_id: workspace,
                principal_id: principal.principal_id.as_str(),
                absolute: matches!(
                    gate,
                    crate::authorization::ActionGateDecision::AllowAbsolute
                ),
            };
            let db = self.crud_store.database_connection();
            let ids: Vec<_> = if snapshot {
                scope.params.run_ids.clone()
            } else {
                runs.into_iter().collect()
            };
            let reset = snapshot || inbox_reset;
            let retained_cursor = scope
                .pending
                .lock()
                .expect("push pending poisoned")
                .retained_cursor
                .clone();
            let (rows, total) = if !reset {
                self.crud_store
                    .task_completion_inbox_patch(&access, &ids)
                    .await?
            } else {
                let (retained_count, initialized) = {
                    let pending = scope.pending.lock().expect("push pending poisoned");
                    (pending.known_inbox.len(), pending.retained_initialized)
                };
                let frontier = retained_cursor
                    .as_deref()
                    .map(super::task_completion_inbox::decode_inbox_cursor)
                    .transpose()?;
                let cap = if !initialized && retained_count == 0 && frontier.is_none() {
                    30
                } else {
                    MAX_METADATA_IDS
                };
                let read = db.begin_read().await?;
                let mut rows = Vec::new();
                {
                    let mut before: Option<(String, String)> = None;
                    while rows.len() < cap {
                        let limit = (cap - rows.len()).min(100);
                        let page = pioneer_crud::list_task_completion_inbox(
                            &read,
                            &access,
                            before.as_ref().map(|(key, id)| (key.as_str(), id.as_str())),
                            limit,
                        )
                        .await?;
                        let count = page.len();
                        before = page
                            .last()
                            .map(|row| (row.completed_key.clone(), row.run_id.clone()));
                        let crossed_frontier = frontier.as_ref().is_some_and(|frontier| {
                            before.as_ref().is_some_and(|key| key <= frontier)
                        });
                        rows.extend(page.into_iter().filter(|row| {
                            frontier.as_ref().is_none_or(|(key, id)| {
                                (&row.completed_key, &row.run_id) >= (key, id)
                            })
                        }));
                        if count < limit || crossed_frontier {
                            break;
                        }
                    }
                }
                let total =
                    pioneer_crud::count_unread_task_completion_inbox(&read, &access).await?;
                read.commit().await?;
                (rows, total)
            };
            let next_cursor = if reset && total > rows.len() as u64 {
                if rows.len() < MAX_METADATA_IDS && retained_cursor.is_some() {
                    retained_cursor.clone()
                } else {
                    rows.last()
                        .map(|row| serde_json::to_string(&(&row.completed_key, &row.run_id)))
                        .transpose()?
                }
            } else {
                None
            };
            let members: HashSet<_> = rows.iter().map(|row| row.run_id.clone()).collect();
            let removes = ids.into_iter().filter(|id| !members.contains(id)).collect();
            let mut seen = HashSet::new();
            let mut upserts = Vec::new();
            for row in rows {
                if !seen.insert(row.run_id.clone()) {
                    continue;
                }
                anyhow::ensure!(
                    !self
                        .authorized_task_notification_recipients(
                            &row.task_id,
                            workspace,
                            vec![connection]
                        )
                        .await
                        .is_empty(),
                    "push task scope changed"
                );
                upserts.push(super::task_completion_inbox::completion_inbox_entry(
                    workspace, row,
                )?);
            }
            Some(TaskInboxPatch {
                replace: reset,
                upserts,
                removes,
                unread_count: total,
                next_cursor,
            })
        } else {
            None
        };
        anyhow::ensure!(
            self.session_manager
                .connection_principal(connection)
                .await?
                .as_ref()
                == &scope.principal,
            "push identity changed"
        );
        Ok(WorkspacePushNotification {
            workspace_id: workspace.clone(),
            epoch: scope.params.epoch.clone(),
            revision: 0,
            snapshot,
            resync_required: false,
            thread_summaries: metadata,
            indicators,
            inbox,
        })
    }
}
