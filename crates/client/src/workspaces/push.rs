//! Connection-owned push demand, using the existing workspace worker and socket.
use crate::core::ClientCore;
use pioneer_protocol::{WorkspacePushNotification, WorkspacePushParams, WorkspacePushResponse};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(super) struct PushController {
    pub(super) pending: BTreeSet<String>,
    scopes: BTreeMap<String, Scope>,
    serial: u64,
    unsupported_identity: Option<(u64, (u64, u64), String)>,
}
struct Scope {
    identity: (u64, (u64, u64), String),
    params: WorkspacePushParams,
    revision: u64,
    // None until registration completes; packets may precede the RPC reply.
    supported: Option<bool>,
    recovering: bool,
    bootstrap_retry: bool,
}
impl ClientCore {
    pub(crate) fn retire_workspace_push(&self) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        owner.push.pending.clear();
        owner.push.scopes.clear();
        owner.push.unsupported_identity = None;
    }
    /// Gateway retires the old principal's publisher on access-token renewal.
    /// Keep domain operation epochs intact, but hand this one stream back to
    /// the existing workspace worker for a fresh, current-principal snapshot.
    pub(crate) fn renew_workspace_push(&self) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        let workspaces = owner.push.scopes.keys().cloned().collect::<Vec<_>>();
        let mut inbox_owner = self.task_notifications.lock().expect("task inbox poisoned");
        for workspace in workspaces {
            if let Some(scope) = owner.push.scopes.get_mut(&workspace) {
                scope.recovering = true;
                scope.supported = Some(false);
            }
            // Retain the last tree stamp until replacement registration. Late
            // legacy read replies must not regain count authority at renewal.
            Self::pause_inbox_push_epoch(&mut inbox_owner.store, &workspace);
            owner.push.pending.insert(workspace);
        }
        owner.wake();
    }
    fn push_identity(&self) -> Option<(u64, (u64, u64), String)> {
        Some((
            self.gateway_http_generation()?,
            self.authorization_operation_epoch(),
            self.current_auth()?.principal.id.to_string(),
        ))
    }
    pub(crate) fn queue_workspace_push(&self, workspace: &str) {
        if self.is_stopped() {
            return;
        }
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        // Workspace demand is already bounded by retained subscriptions. Never
        // turn malformed/unknown IDs into an unbounded controller queue.
        if owner.push.pending.len() >= 256 && !owner.push.pending.contains(workspace) {
            // One actual connection scope: overflow collapses queued identities
            // to reconciliation of that scope and the newest demand, not backlog.
            let active = owner.push.scopes.keys().next().cloned();
            owner.push.pending.clear();
            owner.push.pending.extend(active);
        }
        owner.push.pending.insert(workspace.to_owned());
        owner.wake();
    }
    pub(crate) fn pause_workspace_push_for_page(&self, workspace: &str) {
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        if let Some(scope) = owner.push.scopes.get_mut(workspace) {
            scope.recovering = true;
            scope.supported = Some(false);
            let mut inbox_owner = self.task_notifications.lock().expect("task inbox poisoned");
            Self::pause_inbox_push_epoch(&mut inbox_owner.store, workspace);
            // Existing tree counts remain protected from legacy acks. Packets
            // of the retired epoch are ignored until the canonical page arrives;
            // completion registers an authoritative new snapshot closing the gap.
        }
    }
    pub(crate) fn workspace_push_inbox_active(&self, workspace: &str) -> bool {
        let identity = self.push_identity();
        let owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        owner.push.scopes.get(workspace).is_some_and(|scope| {
            Some(&scope.identity) == identity.as_ref()
                && scope.supported != Some(false)
                && scope.params.inbox
        })
    }
    pub(crate) fn reconcile_workspace_push(&self, workspace: &str) {
        if self.inbox_push_page_pending(workspace) {
            // push_paused retains this obligation after the worker consumes its
            // wake. Inbox completion resumes it once all deferred actions drain.
            return;
        }
        let Some(identity) = self.push_identity() else {
            return;
        };
        if self.authorization_snapshot(Some(workspace), None).is_none() {
            return;
        }
        let mut thread_ids = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .directory
            .push_demand(workspace);
        let mut inbox = self.inbox_push_demand(workspace);
        if self
            .navigation_snapshot()
            .workspace_id()
            .is_some_and(|selected| selected != workspace)
        {
            thread_ids = None;
            inbox = None;
        }
        if thread_ids.as_ref().is_some_and(|ids| ids.len() > 4096) {
            thread_ids = None;
        }
        if inbox.as_ref().is_some_and(|(ids, _)| ids.len() > 4096) {
            inbox = None;
        }
        let mut tree = thread_ids.is_some();
        let mut wants_inbox = inbox.is_some();
        let mut thread_ids = thread_ids.unwrap_or_default();
        thread_ids.sort();
        let (mut run_ids, mut inbox_cursor) = inbox.unwrap_or_default();
        run_ids.sort();
        let params = {
            let mut owner = self
                .workspace_controller
                .lock()
                .expect("workspace controller poisoned");
            if owner.push.unsupported_identity.as_ref() == Some(&identity) {
                // A persistently failing bootstrap falls back once per identity.
                // Retire a possibly live server registration on this worker,
                // never by blocking the notification ingress on an RPC.
                if !owner
                    .push
                    .scopes
                    .get(workspace)
                    .is_some_and(|scope| scope.params.tree || scope.params.inbox)
                {
                    return;
                }
                tree = false;
                wants_inbox = false;
                thread_ids.clear();
                run_ids.clear();
                inbox_cursor = None;
            }
            if let Some(scope) = owner.push.scopes.get(workspace) {
                if !scope.recovering
                    && scope.identity == identity
                    && scope.params.tree == tree
                    && scope.params.inbox == wants_inbox
                    && scope.params.thread_ids == thread_ids
                    && scope.params.run_ids == run_ids
                    && scope.params.inbox_cursor == inbox_cursor
                {
                    return;
                }
            } else if (!tree && !wants_inbox) || owner.push.scopes.len() >= 256 {
                return;
            }
            let bootstrap_retry = owner.push.scopes.get(workspace).is_some_and(|scope| {
                scope.identity == identity && scope.recovering && scope.revision == 0
            });
            owner.push.serial = owner.push.serial.wrapping_add(1);
            let epoch = format!(
                "{}:{}:{}:{}:{}",
                identity.0,
                identity.1.0,
                identity.1.1,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
                owner.push.serial
            );
            let params = WorkspacePushParams {
                workspace_id: workspace.to_owned(),
                epoch,
                tree,
                inbox: wants_inbox,
                thread_ids,
                run_ids,
                inbox_cursor,
            };
            // Gateway owns one workspace scope per physical connection. Switching
            // workspace replaces it; an old queued unsubscribe must not retire the
            // new scope. Keep the same invariant in the shared client.
            let retired = owner
                .push
                .scopes
                .keys()
                .filter(|id| id.as_str() != workspace)
                .cloned()
                .collect::<Vec<_>>();
            owner.push.scopes.retain(|id, _| id == workspace);
            owner.push.scopes.insert(
                workspace.to_owned(),
                Scope {
                    identity: identity.clone(),
                    params: params.clone(),
                    revision: 0,
                    supported: None,
                    recovering: false,
                    bootstrap_retry,
                },
            );
            // Install before sending: the authoritative snapshot may arrive
            // before the register RPC response on the same socket.
            let mut inbox_owner = self.task_notifications.lock().expect("task inbox poisoned");
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            for old in retired {
                registry.directory.push_revisions.remove(&old);
                Self::set_inbox_push_epoch(&mut inbox_owner.store, &old, None);
            }
            if tree {
                registry
                    .directory
                    .push_revisions
                    .insert(workspace.to_owned(), (params.epoch.clone(), 0));
            } else {
                registry.directory.push_revisions.remove(workspace);
            }
            Self::set_inbox_push_epoch(
                &mut inbox_owner.store,
                workspace,
                wants_inbox.then_some(params.epoch.as_str()),
            );
            params
        };
        let result: anyhow::Result<WorkspacePushResponse> = crate::rpc::send_json_rpc_request_typed(
            &self.transport_runtime().ws_command_sender(),
            pioneer_protocol::constants::methods::WORKSPACE_PUSH,
            &params,
            crate::rpc::RPC_REQUEST_TIMEOUT,
        );
        let current_identity = self.push_identity();
        let mut owner = self
            .workspace_controller
            .lock()
            .expect("workspace controller poisoned");
        let Some(scope) = owner.push.scopes.get_mut(workspace) else {
            return;
        };
        if scope.params.epoch != params.epoch
            || scope.recovering
            || current_identity.as_ref() != Some(&identity)
        {
            return;
        }
        let failed = result.is_err();
        let supported = result.is_ok_and(|response| response.supported);
        scope.supported = Some(supported);
        if !supported || (!tree && !wants_inbox) {
            self.thread_registry
                .lock()
                .expect("thread registry poisoned")
                .directory
                .push_revisions
                .remove(workspace);
            let mut inbox_owner = self.task_notifications.lock().expect("task inbox poisoned");
            Self::set_inbox_push_epoch(&mut inbox_owner.store, workspace, None);
            if !tree && !wants_inbox {
                owner.push.scopes.remove(workspace);
            }
        }
        if !supported {
            owner.push.unsupported_identity = Some(identity);
        }
        drop(owner);
        if !supported && (tree || wants_inbox) {
            if failed {
                // A lost reply can follow successful registration. One bounded
                // cleanup attempt restores legacy hints on the same connection.
                let mut unsubscribe = params;
                unsubscribe.tree = false;
                unsubscribe.inbox = false;
                let _: anyhow::Result<WorkspacePushResponse> =
                    crate::rpc::send_json_rpc_request_typed(
                        &self.transport_runtime().ws_command_sender(),
                        pioneer_protocol::constants::methods::WORKSPACE_PUSH,
                        &unsubscribe,
                        crate::rpc::RPC_REQUEST_TIMEOUT,
                    );
            }
            if tree {
                self.queue_directory_refresh(workspace);
            }
            if wants_inbox {
                self.task_notification_intent(
                    crate::tasks::notifications::TaskNotificationIntent::Refresh {
                        workspace_id: workspace.to_owned(),
                    },
                );
            }
        }
    }
    pub(crate) fn recover_malformed_workspace_push(&self) {
        let packet = {
            let owner = self
                .workspace_controller
                .lock()
                .expect("workspace controller poisoned");
            owner
                .push
                .scopes
                .iter()
                // A malformed frame cannot supply a trusted scope identity.
                // There is exactly one negotiated scope on this connection.
                .next()
                .map(|(workspace, scope)| WorkspacePushNotification {
                    workspace_id: workspace.clone(),
                    epoch: scope.params.epoch.clone(),
                    revision: 0,
                    snapshot: false,
                    resync_required: true,
                    thread_summaries: vec![],
                    indicators: vec![],
                    inbox: None,
                })
        };
        if let Some(packet) = packet {
            self.observe_workspace_push(&packet);
        }
    }
    pub(crate) fn observe_workspace_push(&self, packet: &WorkspacePushNotification) {
        let identity = self.push_identity();
        let recover = {
            let mut owner = self
                .workspace_controller
                .lock()
                .expect("workspace controller poisoned");
            let Some(scope) = owner.push.scopes.get_mut(&packet.workspace_id) else {
                return;
            };
            if self.is_stopped()
                || Some(&scope.identity) != identity.as_ref()
                || scope.params.epoch != packet.epoch
                || scope.supported == Some(false)
            {
                return;
            }
            if packet.revision <= scope.revision && !packet.resync_required {
                return;
            }
            let valid_revision = if scope.revision == 0 {
                packet.snapshot && packet.revision == 1
            } else {
                !packet.snapshot && scope.revision.checked_add(1) == Some(packet.revision)
            };
            let mut inbox_owner = self.task_notifications.lock().expect("task inbox poisoned");
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let valid_tree = !scope.params.tree
                || (registry
                    .directory
                    .push_epoch_is_current(&packet.workspace_id, &packet.epoch)
                    && packet.thread_summaries.iter().all(|patch| {
                        let thread = &patch.thread;
                        thread.workspace_id == packet.workspace_id
                            && registry
                                .directory
                                .threads
                                .get(&thread.id)
                                .is_some_and(|known| known.workspace_id == packet.workspace_id)
                    })
                    && packet.indicators.iter().all(|patch| {
                        registry
                            .directory
                            .threads
                            .get(&patch.thread_id)
                            .is_some_and(|thread| thread.workspace_id == packet.workspace_id)
                    }));
            let valid = valid_revision
                && !packet.resync_required
                && packet.indicators.len() <= 4096
                && packet.thread_summaries.len() <= 4096
                && packet
                    .thread_summaries
                    .iter()
                    .map(|p| p.thread.id.as_str())
                    .collect::<BTreeSet<_>>()
                    .len()
                    == packet.thread_summaries.len()
                && packet
                    .indicators
                    .iter()
                    .map(|p| p.thread_id.as_str())
                    .chain(packet.thread_summaries.iter().map(|t| t.thread.id.as_str()))
                    .collect::<BTreeSet<_>>()
                    .len()
                    <= 4096
                && valid_tree
                && (scope.params.tree
                    || (packet.indicators.is_empty() && packet.thread_summaries.is_empty()))
                && (scope.params.inbox || packet.inbox.is_none())
                && (!packet.snapshot || !scope.params.inbox || packet.inbox.is_some())
                && Self::validate_inbox_push(&inbox_owner.store, packet);
            if valid {
                let mut drafts = registry.apply_push_summaries(&packet.thread_summaries);
                if scope.params.tree {
                    // Metadata-only packets also advance the late-RPC fence;
                    // empty indicators never trigger a separate tree projection.
                    registry.directory.apply_push_indicators(packet);
                }
                if !self.apply_inbox_push(&mut inbox_owner.store, packet) {
                    return;
                }
                scope.revision = packet.revision;
                scope.bootstrap_retry = false;
                let authority = crate::core::ClientMutationAuthority { _private: () };
                if packet.inbox.is_some() {
                    drafts.extend(self.task_inbox_publication_draft(
                        &mut inbox_owner.store,
                        &packet.workspace_id,
                    ));
                }
                if scope.params.tree
                    && (!packet.indicators.is_empty() || !packet.thread_summaries.is_empty())
                {
                    registry
                        .directory
                        .dirty_workspaces
                        .remove(&packet.workspace_id);
                    let draft = registry
                        .navigation
                        .drafts
                        .get(&packet.workspace_id)
                        .cloned();
                    // Keep bootstrap receipt and loading state owned by metadata
                    // request; indicator delivery never makes metadata Ready.
                    let previous = registry.directory.snapshot(&packet.workspace_id);
                    let loading = previous.as_ref().is_some_and(|p| p.is_loading());
                    let error = previous.as_ref().and_then(|p| p.error().map(str::to_owned));
                    if let Some(publication) = registry.directory.project(
                        &packet.workspace_id,
                        draft.as_deref(),
                        loading,
                        error,
                    ) {
                        drafts.push(authority.publication(
                            crate::core::ClientScope::WorkspaceTree {
                                workspace_id: Some(packet.workspace_id.clone()),
                            },
                            crate::threads::registry::revisions(publication.revision()),
                            publication,
                        ));
                    }
                }
                self.transition(&authority, drafts, vec![]);
                None
            } else {
                registry
                    .directory
                    .push_revisions
                    .remove(&packet.workspace_id);
                Self::set_inbox_push_epoch(&mut inbox_owner.store, &packet.workspace_id, None);
                let domains = (scope.params.tree, scope.params.inbox);
                let disable =
                    (scope.revision == 0 && scope.bootstrap_retry).then(|| scope.identity.clone());
                scope.recovering = true;
                scope.supported = Some(false);
                if let Some(identity) = disable {
                    owner.push.unsupported_identity = Some(identity);
                }
                Some(domains)
            }
        };
        if let Some((tree, inbox)) = recover {
            // Retire once, no packet backlog/retry timer. Existing owners perform
            // authoritative metadata/prefix recovery then register a fresh epoch.
            if tree {
                self.queue_directory_refresh(&packet.workspace_id);
            }
            if inbox {
                self.task_notification_intent(
                    crate::tasks::notifications::TaskNotificationIntent::Refresh {
                        workspace_id: packet.workspace_id.clone(),
                    },
                );
            }
            self.queue_workspace_push(&packet.workspace_id);
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::{core::ClientScope, rpc::JsonRpcRequestTransport};
    use pioneer_protocol::*;
    use std::sync::{Arc, Mutex, Weak};

    struct RegisterReply {
        core: Weak<ClientCore>,
        first: Mutex<Option<WorkspacePushNotification>>,
        fail_bootstrap: Mutex<bool>,
    }
    fn deliver(core: &ClientCore, packet: &WorkspacePushNotification) {
        let wire =
            JsonRpcNotification::from_params(constants::events::WORKSPACE_PUSH_CHANGED, packet)
                .unwrap();
        let event = GatewayNotification::from_jsonrpc(wire).unwrap();
        assert!(core.observe_workspace_notification(&event));
    }
    impl JsonRpcRequestTransport for RegisterReply {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            tx: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(request["method"], constants::methods::WORKSPACE_PUSH);
            let params: WorkspacePushParams =
                serde_json::from_value(request["params"].clone()).unwrap();
            if !params.tree && !params.inbox {
                tx.send(Ok(serde_json::json!({"supported":true}))).unwrap();
                return Ok(());
            }
            let packet = WorkspacePushNotification {
                workspace_id: params.workspace_id,
                epoch: params.epoch,
                revision: 1,
                snapshot: true,
                resync_required: *self.fail_bootstrap.lock().unwrap(),
                thread_summaries: vec![],
                indicators: ["a", "b"]
                    .into_iter()
                    .map(|id| ThreadIndicatorPatch {
                        thread_id: id.into(),
                        unread_count: Some(3),
                        has_running_tasks: Some(true),
                    })
                    .collect(),
                inbox: None,
            };
            // Real socket ordering permits this before the register RPC reply.
            deliver(&self.core.upgrade().unwrap(), &packet);
            *self.first.lock().unwrap() = Some(packet);
            tx.send(Ok(serde_json::json!({"supported":true}))).unwrap();
            Ok(())
        }
    }
    struct TreeReply<'a> {
        core: &'a ClientCore,
        during: Option<WorkspacePushNotification>,
    }
    impl JsonRpcRequestTransport for TreeReply<'_> {
        fn send_json_rpc_request(
            &self,
            _: String,
            _: String,
            tx: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            if let Some(packet) = &self.during {
                deliver(self.core, packet);
            }
            let threads = ["a", "b"]
                .into_iter()
                .map(|id| {
                    serde_json::from_value(
                        serde_json::json!({"id":id,"workspace_id":"ws","preview":"",
                    "mode":"Chat","model":"model","model_provider":"provider","created_at":1,
                    "updated_at":1,"status":"Idle","turns":[]}),
                    )
                    .unwrap()
                })
                .collect();
            tx.send(Ok(serde_json::to_value(ThreadTreeResponse {
                workspace_id: "ws".into(),
                threads,
                unread: vec![],
                running_task_thread_ids: vec![],
                folders: [("first", "second"), ("second", "first")]
                    .into_iter()
                    .map(|(id, parent)| ThreadFolder {
                        id: id.into(),
                        workspace_id: "ws".into(),
                        parent_folder_id: Some(parent.into()),
                        name: id.into(),
                        created_at: 1,
                        updated_at: 1,
                    })
                    .collect(),
                placements: [("a", "first"), ("b", "second")]
                    .into_iter()
                    .map(|(thread, folder)| ThreadPlacement {
                        thread_id: thread.into(),
                        workspace_id: "ws".into(),
                        folder_id: Some(folder.into()),
                    })
                    .collect(),
                agents_docs: vec![],
            })
            .unwrap()))
                .unwrap();
            Ok(())
        }
    }
    fn push_fixture() -> Arc<ClientCore> {
        let core = crate::catalog_test_support::settings_client();
        let mut authorization = core.authorization_snapshot(None, None).unwrap();
        let resources = AuthorizationOperationalResourceProjection {
            fingerprint: "tree-test-resources".into(),
            ..Default::default()
        };
        authorization.workspace = Some(AuthorizationWorkspaceCapabilitySnapshot {
            workspace_id: "ws".into(),
            capabilities: AuthorizationWorkspaceCapabilities {
                can_read_own_notifications: true,
                can_acknowledge_own_notifications: true,
                ..Default::default()
            },
            operational_resources: resources.clone(),
            execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                fingerprint: "tree-test".into(),
                resources,
                permission_options: vec![],
                can_attach_artifacts: false,
                mcp_invocation_limits: Default::default(),
            },
        });
        let (generation, connection) = core.current_auth_ticket();
        assert_eq!(
            core.accept_authorization_projection(generation, connection, authorization),
            crate::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        core.transport_runtime()
            .ws_command_sender()
            .set_http_session_access_for_test(Some(crate::transport::http::GatewayHttpAccess {
                gateway_base_url: GatewayBaseUrl::parse_presentation("https://example.test")
                    .unwrap(),
                gateway_id: GatewayId::new("GAAAAAAAAAAAAAAAAAAAA").unwrap(),
                session_id: AuthSessionId::new("SAAAAAAAAAAAAAAAAAAAA").unwrap(),
                generation: 7,
                access_expires_at_unix: 1000,
                access_token: AuthSecretString::new("synthetic-token"),
            }));
        crate::catalog_test_support::replace_connection(&core, 7);
        core
    }

    fn handoff_entry(id: &str) -> TaskCompletionInboxEntry {
        let mut entry: TaskCompletionInboxEntry = serde_json::from_value(serde_json::json!({
            "workspaceId":"ws", "taskId":"task", "runId":id, "title":"Task",
            "threadId":"a", "threadTitle":"Thread", "status":"succeeded", "completedAt":1,
            "completedKey":"2026-10-22 09:00:00.000000"
        }))
        .unwrap();
        if id == "after" {
            entry.completed_key = "2026-10-22 09:01:00.000000".into();
        }
        entry
    }
    struct HandoffTransport {
        core: Weak<ClientCore>,
        entries: Mutex<Vec<TaskCompletionInboxEntry>>,
        requests: std::sync::mpsc::Sender<(String, crate::rpc::JsonRpcResponseSender)>,
        registrations: std::sync::mpsc::Sender<WorkspacePushParams>,
    }
    impl JsonRpcRequestTransport for HandoffTransport {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let wire: serde_json::Value = serde_json::from_str(&payload).unwrap();
            let method = wire["method"].as_str().unwrap();
            if method != constants::methods::WORKSPACE_PUSH {
                self.requests.send((method.into(), response)).unwrap();
                return Ok(());
            }
            let params: WorkspacePushParams =
                serde_json::from_value(wire["params"].clone()).unwrap();
            assert!(
                params.tree && params.inbox,
                "combined demand survives handoff"
            );
            let entries = self.entries.lock().unwrap().clone();
            deliver(
                &self.core.upgrade().unwrap(),
                &WorkspacePushNotification {
                    workspace_id: params.workspace_id.clone(),
                    epoch: params.epoch.clone(),
                    revision: 1,
                    snapshot: true,
                    resync_required: false,
                    thread_summaries: vec![],
                    indicators: params
                        .thread_ids
                        .iter()
                        .map(|id| ThreadIndicatorPatch {
                            thread_id: id.clone(),
                            unread_count: Some(0),
                            has_running_tasks: Some(false),
                        })
                        .collect(),
                    inbox: Some(TaskInboxPatch {
                        replace: true,
                        unread_count: entries.len() as u64,
                        upserts: entries,
                        removes: vec![],
                        next_cursor: params.inbox_cursor.clone(),
                    }),
                },
            );
            response
                .send(Ok(serde_json::json!({"supported":true})))
                .unwrap();
            self.registrations.send(params).unwrap();
            Ok(())
        }
    }
    fn handoff_wait(mut condition: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !condition() {
            assert!(
                std::time::Instant::now() < deadline,
                "bounded worker handoff"
            );
            std::thread::yield_now();
        }
    }
    fn handoff_response(
        requests: &std::sync::mpsc::Receiver<(String, crate::rpc::JsonRpcResponseSender)>,
        expected: &str,
    ) -> crate::rpc::JsonRpcResponseSender {
        let (method, response) = requests
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_eq!(method, expected, "no unrelated recovery RPC");
        response
    }

    pub(in crate::workspaces) fn paused_combined_scope_resumes_after_renewal_or_page_deferred_action(
        drain_workspace_cycle: impl Fn(&Arc<ClientCore>),
    ) {
        use crate::tasks::notifications::TaskNotificationIntent;
        // One worker/transport matrix owns the lost-continuation boundary.
        // Ordinary acknowledgements are the negative control: no fresh epoch.
        for cause in ["ordinary", "renewal", "page"] {
            for action in ["dismiss", "read-all", "viewed", "open"] {
                let core = push_fixture();
                struct Stop(Arc<ClientCore>);
                impl Drop for Stop {
                    fn drop(&mut self) {
                        self.0.shutdown();
                    }
                }
                let _stop = Stop(core.clone());
                let (requests_tx, requests) = std::sync::mpsc::channel();
                let (registered_tx, registered) = std::sync::mpsc::channel();
                let transport = Arc::new(HandoffTransport {
                    core: Arc::downgrade(&core),
                    entries: Mutex::new(vec![handoff_entry("run")]),
                    requests: requests_tx,
                    registrations: registered_tx,
                });
                *core
                    .transport_runtime()
                    .ws_command_sender()
                    .test_requests
                    .lock()
                    .unwrap() = Some(transport.clone());
                core.start_task_notification_controller();
                core.task_notification_intent(TaskNotificationIntent::Refresh {
                    workspace_id: "ws".into(),
                });
                let initial_cursor =
                    serde_json::to_string(&(handoff_entry("run").completed_key, "run")).unwrap();
                handoff_response(&requests, constants::methods::TASK_COMPLETION_INBOX_LIST)
                    .send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
                        entries: vec![handoff_entry("run")],
                        unread_count: 1,
                        next_cursor: Some(initial_cursor),
                    })
                    .unwrap()))
                    .unwrap();
                handoff_wait(|| core.task_inbox("ws").is_some_and(|p| !p.is_loading()));
                let _inbox = core.subscribe(
                    ClientScope::TaskInbox {
                        workspace_id: "ws".into(),
                    },
                    std::num::NonZeroUsize::new(8).unwrap(),
                );
                let _tree = core.subscribe(
                    ClientScope::WorkspaceTree {
                        workspace_id: Some("ws".into()),
                    },
                    std::num::NonZeroUsize::new(8).unwrap(),
                );
                core.refresh_workspace_tree_with_transport(
                    "ws",
                    &TreeReply {
                        core: &core,
                        during: None,
                    },
                )
                .unwrap();
                core.reconcile_workspace_push("ws");
                let first = registered
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                drain_workspace_cycle(&core);
                let drained = || {
                    core.workspace_controller
                        .lock()
                        .unwrap()
                        .push
                        .pending
                        .is_empty()
                };
                let revision = core.task_inbox("ws").unwrap().items()[0].revision();
                let intent = match action {
                    "dismiss" => TaskNotificationIntent::Dismiss {
                        workspace_id: "ws".into(),
                        notification_id: "run".into(),
                        revision,
                    },
                    "read-all" => TaskNotificationIntent::ReadAll {
                        workspace_id: "ws".into(),
                    },
                    "viewed" => TaskNotificationIntent::Viewed {
                        params: TaskCompletionInboxViewedParams {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            run_ids: vec!["run".into()],
                            result_turn_ids: vec![],
                            result_items: vec![],
                        },
                    },
                    _ => TaskNotificationIntent::Open {
                        workspace_id: "ws".into(),
                        notification_id: "run".into(),
                        revision,
                    },
                };
                if cause == "page" {
                    core.task_notification_intent(TaskNotificationIntent::LoadMore {
                        workspace_id: "ws".into(),
                    });
                    let page =
                        handoff_response(&requests, constants::methods::TASK_COMPLETION_INBOX_LIST);
                    core.task_notification_intent(intent); // truly deferred behind the real page RPC
                    transport
                        .entries
                        .lock()
                        .unwrap()
                        .push(handoff_entry("older"));
                    page.send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
                        entries: vec![handoff_entry("older")],
                        unread_count: 2,
                        next_cursor: None,
                    })
                    .unwrap()))
                        .unwrap();
                } else {
                    core.task_notification_intent(intent);
                }
                let method = match action {
                    "dismiss" => constants::methods::TASK_COMPLETION_INBOX_READ,
                    "read-all" => constants::methods::TASK_COMPLETION_INBOX_READ_ALL,
                    "viewed" => constants::methods::TASK_COMPLETION_INBOX_VIEWED,
                    _ => constants::methods::TASK_COMPLETION_INBOX_GET,
                };
                let action_response = handoff_response(&requests, method);
                if cause == "renewal" {
                    let auth = core.current_auth().unwrap();
                    let (generation, connection) = core.current_auth_ticket();
                    core.finish_gateway_access_renewal(
                        generation,
                        connection,
                        &crate::gateway::session_lifecycle::GatewaySessionMetadata {
                            gateway_id: auth.gateway.id.clone(),
                            device_id: auth.device.id.clone(),
                            session_id: auth.session.id.clone(),
                            refresh_generation: auth.session.refresh_generation + 1,
                            refresh_expires_at_unix: auth.session.refresh_expires_at_unix + 100,
                        },
                    )
                    .unwrap();
                }
                // Run the actual post-wake quantum to completion while the RPC
                // is held: observing pop_first alone would leave a race before
                // its blocked reconcile returns. Then attach the native worker;
                // completion must supply its sole new wake without test hints.
                drain_workspace_cycle(&core);
                core.start_workspace_controller();
                assert!(registered.try_recv().is_err());
                if cause != "ordinary" {
                    assert!(!core.workspace_push_inbox_active("ws"));
                }
                let value = match action {
                    "dismiss" => {
                        transport
                            .entries
                            .lock()
                            .unwrap()
                            .retain(|e| e.run_id != "run");
                        let mut entry = handoff_entry("run");
                        entry.read_at = Some(2);
                        serde_json::to_value(TaskCompletionInboxEntryResponse {
                            entry,
                            unread_count: transport.entries.lock().unwrap().len() as u64,
                        })
                        .unwrap()
                    }
                    "read-all" => {
                        transport.entries.lock().unwrap().clear();
                        serde_json::to_value(TaskCompletionInboxListResponse {
                            entries: vec![],
                            unread_count: 0,
                            next_cursor: None,
                        })
                        .unwrap()
                    }
                    "viewed" => serde_json::json!([]),
                    _ => serde_json::to_value(TaskCompletionInboxEntryResponse {
                        entry: handoff_entry("run"),
                        unread_count: 1,
                    })
                    .unwrap(),
                };
                action_response.send(Ok(value)).unwrap();
                if action == "open" {
                    let thread =
                        core.workspace_tree("ws").unwrap().snapshot().threads_by_id["a"].clone();
                    handoff_response(&requests, constants::methods::THREAD_GET)
                        .send(Ok(serde_json::json!({"thread":thread})))
                        .unwrap();
                }
                handoff_wait(|| core.task_inbox("ws").is_some_and(|p| !p.is_loading()));
                let epoch = if cause == "ordinary" {
                    handoff_wait(drained);
                    assert!(
                        registered.try_recv().is_err(),
                        "ordinary ack must not renegotiate"
                    );
                    first.epoch
                } else {
                    let resumed = registered
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    assert_ne!(resumed.epoch, first.epoch);
                    handoff_wait(|| core.workspace_push_inbox_active("ws"));
                    resumed.epoch
                };
                deliver(
                    &core,
                    &WorkspacePushNotification {
                        workspace_id: "ws".into(),
                        epoch,
                        revision: 2,
                        snapshot: false,
                        resync_required: false,
                        thread_summaries: vec![],
                        indicators: vec![ThreadIndicatorPatch {
                            thread_id: "a".into(),
                            unread_count: Some(4),
                            has_running_tasks: Some(true),
                        }],
                        inbox: Some(TaskInboxPatch {
                            replace: false,
                            upserts: vec![handoff_entry("after")],
                            removes: vec![],
                            unread_count: 88,
                            next_cursor: None,
                        }),
                    },
                );
                let tree = core.workspace_tree("ws").unwrap();
                assert_eq!(
                    tree.snapshot()
                        .unread
                        .iter()
                        .find(|u| u.thread_id == "a")
                        .unwrap()
                        .unread_count,
                    4
                );
                assert!(tree.snapshot().running_task_thread_ids.contains("a"));
                let inbox = core.task_inbox("ws").unwrap();
                assert_eq!(inbox.unread_count(), 88);
                assert!(
                    inbox
                        .items()
                        .iter()
                        .any(|i| i.notification().run_id == "after")
                );
                assert!(
                    requests.try_recv().is_err(),
                    "accepted push needs no recovery RPC"
                );
                core.shutdown();
            }
        }
    }
    #[test]
    fn registered_batch_precedes_ack_and_fences_late_snapshot_duplicates_and_gaps() {
        let core = push_fixture();
        let scope = ClientScope::WorkspaceTree {
            workspace_id: Some("ws".into()),
        };
        let binding = core.subscribe(scope, std::num::NonZeroUsize::new(8).unwrap());
        core.refresh_workspace_tree_with_transport(
            "ws",
            &TreeReply {
                core: &core,
                during: None,
            },
        )
        .unwrap();
        let before = core.workspace_tree("ws").unwrap().revision();
        let navigation = core.navigation_snapshot();
        let transport = Arc::new(RegisterReply {
            core: Arc::downgrade(&core),
            first: Mutex::new(None),
            fail_bootstrap: Mutex::new(false),
        });
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(transport.clone());
        core.reconcile_workspace_push("ws");
        let first = transport.first.lock().unwrap().clone().unwrap();
        let after = core.workspace_tree("ws").unwrap();
        assert_eq!(after.revision(), before + 1);
        assert_eq!(
            after
                .snapshot()
                .unread
                .iter()
                .map(|u| u.unread_count)
                .sum::<u64>(),
            6
        );
        assert_eq!(after.snapshot().running_task_thread_ids.len(), 2);
        for folder in ["first", "second"] {
            assert_eq!(after.snapshot().unread_by_folder_id[folder], 6);
            assert!(after.snapshot().running_task_folder_ids.contains(folder));
        }
        assert_eq!(core.navigation_snapshot(), navigation);
        deliver(&core, &first);
        assert_eq!(
            core.workspace_tree("ws").unwrap().revision(),
            after.revision()
        );
        let mut next = first.clone();
        next.revision = 2;
        next.snapshot = false;
        next.indicators[0].unread_count = Some(9);
        core.refresh_workspace_tree_with_transport(
            "ws",
            &TreeReply {
                core: &core,
                during: Some(next.clone()),
            },
        )
        .unwrap();
        let current = core.workspace_tree("ws").unwrap();
        assert_eq!(
            current
                .snapshot()
                .unread
                .iter()
                .find(|u| u.thread_id == "a")
                .unwrap()
                .unread_count,
            9
        );
        assert!(current.snapshot().running_task_thread_ids.contains("a"));
        assert_eq!(current.snapshot().unread_by_folder_id["first"], 12);
        assert_eq!(current.snapshot().unread_by_folder_id["second"], 12);
        // Compact summaries and numeric values share one accepted publication;
        // an existing detailed store retains only an explicitly truncated prefix.
        let mut full = current.snapshot().threads_by_id["a"].clone();
        full.name = Some("Named".into());
        full.preview = "Длинное 😀 сообщение ".repeat(30);
        full.updated_at = 2;
        core.upsert_thread(full.clone());
        let compact = full.clone().into_directory_summary();
        next.revision = 3;
        next.indicators[0].unread_count = Some(10);
        let mut other = current.snapshot().threads_by_id["b"].clone();
        other.updated_at = 3;
        other.name = Some("Second changed summary".into());
        let mut changed = compact.clone();
        changed.updated_at = 3;
        changed.status = ThreadStatus::Active;
        changed.turns =
            vec![serde_json::from_value(serde_json::json!({
            "id":"latest-scoped-turn", "status":"Completed",
            "permission_profile":pioneer_protocol::default_turn_permission_profile_snapshot(),
        })).unwrap()];
        next.thread_summaries = vec![
            ThreadMetadataPatch {
                thread: changed.clone(),
                preview_truncated: true,
            },
            ThreadMetadataPatch {
                thread: other,
                preview_truncated: false,
            },
        ];
        while binding.try_next().is_some() {}
        let before_metadata = core.workspace_tree("ws").unwrap().revision();
        deliver(&core, &next);
        let Some(crate::core::ClientSubscriptionEvent::Publication { publication, .. }) =
            binding.try_next()
        else {
            panic!("accepted metadata and indicators must publish together");
        };
        let observed = publication
            .typed::<crate::workspaces::directory::ThreadTreePublication>()
            .unwrap()
            .payload();
        assert_eq!(
            observed.snapshot().threads_by_id["a"].status,
            ThreadStatus::Active
        );
        assert_eq!(
            observed
                .snapshot()
                .unread
                .iter()
                .find(|u| u.thread_id == "a")
                .unwrap()
                .unread_count,
            10
        );
        assert!(binding.try_next().is_none());
        let batch = core.workspace_tree("ws").unwrap();
        assert_eq!(batch.revision(), before_metadata + 1);
        assert_eq!(batch.snapshot().threads_by_id["a"], changed);
        assert_eq!(
            batch.snapshot().threads_by_id["b"].name.as_deref(),
            Some("Second changed summary")
        );
        assert!(core.thread_snapshot("b").is_none()); // Sidebar-only update creates no detailed store.
        let detailed = core.thread_snapshot("a").unwrap();
        assert_eq!(
            detailed.coordinator().thread().unwrap().preview,
            full.preview
        );
        assert_eq!(
            detailed.coordinator().thread().unwrap().status,
            ThreadStatus::Active
        );
        assert_eq!(
            detailed
                .coordinator()
                .thread()
                .unwrap()
                .turns
                .last()
                .unwrap()
                .id,
            "latest-scoped-turn"
        );
        // Even a real edit equal to the old 160-character prefix must be accepted.
        for preview in [compact.preview.clone(), String::new()] {
            next.revision += 1;
            changed.updated_at += 1;
            changed.preview = preview.clone();
            next.thread_summaries = vec![ThreadMetadataPatch {
                thread: changed.clone(),
                preview_truncated: false,
            }];
            deliver(&core, &next);
            assert_eq!(
                core.thread_snapshot("a")
                    .unwrap()
                    .coordinator()
                    .thread()
                    .unwrap()
                    .preview,
                preview
            );
        }
        // The production renewal acceptance routine keeps ordinary operation
        // epochs but must negotiate a fresh publisher epoch on the same socket.
        let operation_epoch = core.authorization_operation_epoch();
        let auth = core.current_auth().unwrap();
        let (generation, connection) = core.current_auth_ticket();
        core.finish_gateway_access_renewal(
            generation,
            connection,
            &crate::gateway::session_lifecycle::GatewaySessionMetadata {
                gateway_id: auth.gateway.id.clone(),
                device_id: auth.device.id.clone(),
                session_id: auth.session.id.clone(),
                refresh_generation: auth.session.refresh_generation + 1,
                refresh_expires_at_unix: auth.session.refresh_expires_at_unix + 100,
            },
        )
        .unwrap();
        assert_eq!(core.authorization_operation_epoch(), operation_epoch);
        let before_stale = core.workspace_tree("ws").unwrap().revision();
        core.apply_directory_read(
            "ws",
            "a",
            &ThreadReadCursor {
                through_turn_id: "late-read-before-renewal".into(),
                sort_key: "002".into(),
            },
            777,
        );
        assert_eq!(core.workspace_tree("ws").unwrap().revision(), before_stale);
        next.revision += 1;
        next.indicators[0].unread_count = Some(999);
        deliver(&core, &next); // retired pre-renewal epoch
        assert_eq!(core.workspace_tree("ws").unwrap().revision(), before_stale);
        core.reconcile_workspace_push("ws");
        let renewed = transport.first.lock().unwrap().clone().unwrap();
        assert_ne!(renewed.epoch, next.epoch);
        next = renewed;
        next.snapshot = false;
        next.revision = 2;
        next.indicators[0].unread_count = Some(0); // post-renewal read
        deliver(&core, &next);
        let current = core.workspace_tree("ws").unwrap();
        assert_eq!(
            current
                .snapshot()
                .unread
                .iter()
                .find(|u| u.thread_id == "a")
                .unwrap()
                .unread_count,
            0
        );
        next.revision = 4;
        next.indicators[0].unread_count = Some(100);
        deliver(&core, &next);
        assert_eq!(
            core.workspace_tree("ws").unwrap().revision(),
            current.revision()
        );
        next.revision = 3;
        deliver(&core, &next); // retired epoch cannot fill a gap later
        assert_eq!(
            core.workspace_tree("ws").unwrap().revision(),
            current.revision()
        );
        for frame in [
            serde_json::json!({"jsonrpc":"2.0", "method":constants::events::WORKSPACE_PUSH_CHANGED,
                "params":{"workspaceId":"untrusted-other-workspace","revision":"invalid"}}),
            serde_json::json!({"jsonrpc":"2.0", "method":constants::events::WORKSPACE_PUSH_CHANGED}),
        ] {
            core.reconcile_workspace_push("ws");
            let restarted = transport.first.lock().unwrap().clone().unwrap();
            let before_malformed = core.workspace_tree("ws").unwrap().revision();
            let malformed = GatewayNotification::from_jsonrpc(
                serde_json::from_value::<JsonRpcNotification>(frame).unwrap(),
            )
            .unwrap();
            assert!(core.observe_workspace_notification(&malformed));
            let mut stale = restarted;
            stale.snapshot = false;
            stale.revision = 2;
            stale.indicators[0].unread_count = Some(555);
            deliver(&core, &stale);
            assert_eq!(
                core.workspace_tree("ws").unwrap().revision(),
                before_malformed
            );
        }
        // Two failed bootstraps retire rich delivery for this identity rather
        // than repeatedly querying an oversized/unavailable authoritative scope.
        *transport.fail_bootstrap.lock().unwrap() = true;
        core.reconcile_workspace_push("ws");
        core.reconcile_workspace_push("ws");
        core.reconcile_workspace_push("ws");
        core.refresh_workspace_tree_with_transport(
            "ws",
            &TreeReply {
                core: &core,
                during: None,
            },
        )
        .unwrap();
        assert!(
            core.workspace_tree("ws")
                .unwrap()
                .snapshot()
                .unread
                .is_empty()
        );
    }
}
