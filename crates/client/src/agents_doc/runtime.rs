//! Process-owned document executor; no editor or platform runtime is involved.
use super::{content::*, controller::*, scope::AgentsDocEditorScope};
use crate::core::*;
use std::{
    collections::HashSet,
    sync::{Arc, Weak, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentsDocumentIntent {
    Scoped {
        scope: AgentsDocEditorScope,
        expected_owner: u64,
        action: AgentsDocumentAction,
    },
    Edit {
        scope: AgentsDocEditorScope,
        content: String,
    },
    Save {
        scope: AgentsDocEditorScope,
    },
    Reload {
        scope: AgentsDocEditorScope,
    },
    ReloadRemote {
        scope: AgentsDocEditorScope,
    },
    OverwriteRemote {
        scope: AgentsDocEditorScope,
    },
    Close {
        scope: AgentsDocEditorScope,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentsDocumentAction {
    Edit { content: String },
    Save,
    Reload,
    ReloadRemote,
    OverwriteRemote,
    Close,
}
impl AgentsDocumentAction {
    fn into_intent(self, scope: AgentsDocEditorScope) -> AgentsDocumentIntent {
        match self {
            Self::Edit { content } => AgentsDocumentIntent::Edit { scope, content },
            Self::Save => AgentsDocumentIntent::Save { scope },
            Self::Reload => AgentsDocumentIntent::Reload { scope },
            Self::ReloadRemote => AgentsDocumentIntent::ReloadRemote { scope },
            Self::OverwriteRemote => AgentsDocumentIntent::OverwriteRemote { scope },
            Self::Close => AgentsDocumentIntent::Close { scope },
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub struct AgentsDocumentTestRequest(DocumentRequest);

#[cfg(any(test, feature = "test-support"))]
impl ClientCore {
    pub fn next_agents_document_request_for_test(&self) -> Option<AgentsDocumentTestRequest> {
        let mut runtime = self
            .agents_documents
            .lock()
            .expect("document owner poisoned");
        let request = runtime
            .controller
            .next_request(self.timeline_started.elapsed())?;
        self.publish_document(&mut runtime.controller, &request.scope);
        Some(AgentsDocumentTestRequest(request))
    }
    pub fn complete_agents_document_load_for_test(
        &self,
        request: AgentsDocumentTestRequest,
        response: pioneer_protocol::ThreadAgentsDocGetResponse,
    ) -> bool {
        let mut runtime = self
            .agents_documents
            .lock()
            .expect("document owner poisoned");
        let accepted = runtime.controller.complete(
            &request.0,
            DocumentCompletion::Loaded(response),
            self.timeline_started.elapsed(),
        );
        self.publish_document(&mut runtime.controller, &request.0.scope);
        accepted
    }
    pub fn complete_agents_document_save_for_test(
        &self,
        request: AgentsDocumentTestRequest,
        response: Result<pioneer_protocol::ThreadAgentsDocPayload, String>,
    ) -> bool {
        let mut runtime = self
            .agents_documents
            .lock()
            .expect("document owner poisoned");
        let completion = match response {
            Ok(doc) => DocumentCompletion::Saved(doc, 7),
            Err(message) => DocumentCompletion::Failed(message),
        };
        let accepted =
            runtime
                .controller
                .complete(&request.0, completion, self.timeline_started.elapsed());
        self.publish_document(&mut runtime.controller, &request.0.scope);
        accepted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn released_loading_draft_finishes_and_old_widget_cannot_edit_reopened_scope() {
        let core = crate::catalog_test_support::client();
        let scope = AgentsDocEditorScope::root("workspace");
        let demand = core.acquire_agents_document(scope.clone());
        let old_owner = core
            .agents_document_snapshot(&scope)
            .unwrap()
            .owner_generation();
        let load = core.next_agents_document_request_for_test().unwrap();
        core.agents_document_intent(AgentsDocumentIntent::Scoped {
            scope: scope.clone(),
            expected_owner: old_owner,
            action: AgentsDocumentAction::Edit {
                content: "typed while refreshing".into(),
            },
        });
        drop(demand);
        assert!(
            core.complete_agents_document_load_for_test(load, empty_document_response_for_test())
        );
        assert_eq!(
            core.agents_document_snapshot(&scope).unwrap().content(),
            "typed while refreshing"
        );
        assert!(
            core.next_agents_document_request_for_test().is_some(),
            "released dirty scope must finish its save"
        );
        let _reopened = core.acquire_agents_document(scope.clone());
        let current = core.agents_document_snapshot(&scope).unwrap();
        assert_ne!(current.owner_generation(), old_owner);
        let result = core.agents_document_intent(AgentsDocumentIntent::Scoped {
            scope: scope.clone(),
            expected_owner: old_owner,
            action: AgentsDocumentAction::Edit {
                content: "late widget event".into(),
            },
        });
        assert_eq!(result.outcome(), ClientTransitionOutcome::Rejected);
        assert!(Arc::ptr_eq(
            &current,
            &core.agents_document_snapshot(&scope).unwrap()
        ));
    }

    #[tokio::test]
    async fn close_waiter_tracks_new_edits_reports_failure_and_wakes_on_shutdown() {
        let core = crate::catalog_test_support::client();
        let scope = AgentsDocEditorScope::root("workspace");
        let _demand = core.acquire_agents_document(scope.clone());
        let load = core.next_agents_document_request_for_test().unwrap();
        core.complete_agents_document_load_for_test(load, empty_document_response_for_test());
        core.agents_document_intent(AgentsDocumentIntent::Edit {
            scope: scope.clone(),
            content: "first".into(),
        });
        let closing = core.flush_agents_documents_before_close(None);
        tokio::pin!(closing);
        assert!(futures_util::poll!(&mut closing).is_pending());
        let first = core.next_agents_document_request_for_test().unwrap();
        core.agents_document_intent(AgentsDocumentIntent::Edit {
            scope: scope.clone(),
            content: "second".into(),
        });
        core.complete_agents_document_save_for_test(
            first,
            Ok(pioneer_protocol::ThreadAgentsDocPayload {
                id: "document".into(),
                workspace_id: "workspace".into(),
                folder_id: None,
                status: pioneer_protocol::ThreadAgentsDocStatus::Active,
                title: "AGENTS.md".into(),
                content: "first".into(),
                content_sha256: agents_doc_content_hash("first"),
                version: 1,
                created_at: 1,
                updated_at: 1,
            }),
        );
        assert!(futures_util::poll!(&mut closing).is_pending());
        let second = core.next_agents_document_request_for_test().unwrap();
        core.complete_agents_document_save_for_test(second, Err("synthetic offline".into()));
        assert_eq!(
            closing.await,
            Err(AgentsDocumentCloseError::SaveFailed(scope.clone()))
        );
        assert_eq!(
            core.agents_document_snapshot(&scope).unwrap().content(),
            "second"
        );
        let retry = core.flush_agents_documents_before_close(None);
        tokio::pin!(retry);
        assert!(futures_util::poll!(&mut retry).is_pending());
        assert!(core.next_agents_document_request_for_test().is_some());
        core.shutdown();
        assert_eq!(retry.await, Err(AgentsDocumentCloseError::ClientStopped));
    }

    #[test]
    fn retained_identity_cannot_restore_a_draft_before_policy_revalidation() {
        let core = crate::catalog_test_support::settings_client();
        let scope = AgentsDocEditorScope::root("workspace");
        let demand = core.acquire_agents_document(scope.clone());
        let load = core.next_agents_document_request_for_test().unwrap();
        core.complete_agents_document_load_for_test(load, empty_document_response_for_test());
        core.agents_document_intent(AgentsDocumentIntent::Edit {
            scope: scope.clone(),
            content: "private".into(),
        });
        core.invalidate_authorization_revision(2);
        assert!(core.current_auth().is_some());
        core.agents_document_intent(AgentsDocumentIntent::Reload {
            scope: scope.clone(),
        });
        drop(demand);
        let _reentry = core.acquire_agents_document(scope.clone());
        assert!(!core.agents_document_snapshot(&scope).unwrap().has_access());
        assert_eq!(core.agents_document_snapshot(&scope).unwrap().content(), "");
        assert!(core.next_agents_document_request_for_test().is_none());
    }

    #[test]
    fn authorization_fence_publishes_redacted_editor_in_the_identity_transaction() {
        let core = crate::catalog_test_support::client();
        let scope = AgentsDocEditorScope::root("workspace");
        let _demand = core.acquire_agents_document(scope.clone());
        let request = core.next_agents_document_request_for_test().unwrap();
        assert!(core.complete_agents_document_load_for_test(
            request,
            pioneer_protocol::ThreadAgentsDocGetResponse {
                explicit: None,
                effective: None
            }
        ));
        core.agents_document_intent(AgentsDocumentIntent::Edit {
            scope: scope.clone(),
            content: "private draft".into(),
        });
        core.invalidate_authorization_revision(2);
        let document = core.snapshot(&document_scope(&scope)).unwrap();
        let identity = core
            .snapshot(&ClientScope::Administration { workspace_id: None })
            .unwrap();
        assert_eq!(
            document.snapshot().sequence(),
            identity.snapshot().sequence()
        );
        let publication = core.agents_document_snapshot(&scope).unwrap();
        assert!(!publication.has_access());
        assert_eq!(publication.content(), "");
        core.agents_document_intent(AgentsDocumentIntent::Edit {
            scope: scope.clone(),
            content: "late edit".into(),
        });
        assert!(Arc::ptr_eq(
            &publication,
            &core.agents_document_snapshot(&scope).unwrap()
        ));
        core.agents_document_intent(AgentsDocumentIntent::Close {
            scope: scope.clone(),
        });
        assert!(Arc::ptr_eq(
            &publication,
            &core.agents_document_snapshot(&scope).unwrap()
        ));
    }
}
impl AgentsDocumentIntent {
    fn scope(&self) -> &AgentsDocEditorScope {
        match self {
            Self::Scoped { scope, .. }
            | Self::Edit { scope, .. }
            | Self::Save { scope }
            | Self::Reload { scope }
            | Self::ReloadRemote { scope }
            | Self::OverwriteRemote { scope }
            | Self::Close { scope } => scope,
        }
    }
}
pub fn document_scope(scope: &AgentsDocEditorScope) -> ClientScope {
    ClientScope::AgentsDocumentContent {
        workspace_id: scope.workspace_id().into(),
        folder_id: scope.folder_id().map(str::to_owned),
    }
}
#[derive(Default)]
pub(crate) struct DocumentRuntime {
    controller: AgentsDocController,
    bindings: HashSet<AgentsDocEditorScope>,
    wake: Option<mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl DocumentRuntime {
    pub(crate) fn stop(&mut self) {
        self.wake.take();
        self.controller.invalidate();
    }
    fn wake(&self) {
        if let Some(wake) = &self.wake {
            let _ = wake.try_send(());
        }
    }
}
impl Drop for DocumentRuntime {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
pub struct AgentsDocumentDemand {
    core: Weak<ClientCore>,
    scope: AgentsDocEditorScope,
    authority: Option<String>,
}
impl Drop for AgentsDocumentDemand {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            core.release_document(&self.scope, self.authority.as_deref());
        }
    }
}
impl ClientCore {
    pub fn agents_documents_close_status(
        &self,
        scope: Option<&AgentsDocEditorScope>,
    ) -> Result<bool, AgentsDocumentCloseError> {
        if self.is_stopped() {
            return Err(AgentsDocumentCloseError::ClientStopped);
        }
        self.agents_documents
            .lock()
            .expect("document owner poisoned")
            .controller
            .close_status(scope)
    }
    fn document_authority(&self) -> Option<String> {
        let identity = self
            .snapshot(&ClientScope::Administration { workspace_id: None })?
            .snapshot()
            .payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>(
            )?;
        // A retained identity is not authorization while its policy is invalidated.
        let capabilities = identity.capabilities.snapshot(None, None)?;
        let principal = identity
            .current_auth
            .as_ref()
            .map(|auth| auth.principal.id.clone())
            .unwrap_or(capabilities.principal_id);
        serde_json::to_string(&(identity.endpoint_id.as_deref(), principal)).ok()
    }
    /// Flushes through the same executor as autosave. The caller retains this
    /// future until its window/process close is accepted or cancelled. Failed
    /// drafts stay in the controller for the existing editor's retry UI.
    pub async fn flush_agents_documents_before_close(
        &self,
        scope: Option<AgentsDocEditorScope>,
    ) -> Result<(), AgentsDocumentCloseError> {
        let mut changed = self.watch_publications();
        {
            let mut runtime = self
                .agents_documents
                .lock()
                .expect("document owner poisoned");
            let scopes = scope
                .clone()
                .map(|scope| vec![scope])
                .unwrap_or_else(|| runtime.controller.scopes());
            for scope in scopes {
                runtime
                    .controller
                    .close(&scope, self.timeline_started.elapsed());
                self.publish_document(&mut runtime.controller, &scope);
            }
            runtime.wake();
        }
        loop {
            if self.is_stopped() {
                return Err(AgentsDocumentCloseError::ClientStopped);
            }
            if self.agents_documents_close_status(scope.as_ref())? {
                return Ok(());
            }
            changed
                .changed()
                .await
                .map_err(|_| AgentsDocumentCloseError::ClientStopped)?;
        }
    }
    pub fn agents_document_snapshot(
        &self,
        scope: &AgentsDocEditorScope,
    ) -> Option<Arc<AgentsDocumentPublication>> {
        self.snapshot(&document_scope(scope))
            .and_then(|p| p.snapshot().payload())
    }
    pub fn acquire_agents_document(
        self: &Arc<Self>,
        scope: AgentsDocEditorScope,
    ) -> AgentsDocumentDemand {
        let authority = self.acquire_document(scope.clone());
        AgentsDocumentDemand {
            core: Arc::downgrade(self),
            scope,
            authority,
        }
    }
    fn acquire_document(&self, scope: AgentsDocEditorScope) -> Option<String> {
        let _identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() {
            return None;
        }
        let authority = self.document_authority();
        let mut runtime = self
            .agents_documents
            .lock()
            .expect("document owner poisoned");
        runtime
            .controller
            .acquire_as(scope.clone(), authority.clone().unwrap_or_default());
        if authority.is_none() {
            runtime.controller.invalidate();
        }
        self.publish_document(&mut runtime.controller, &scope);
        runtime.wake();
        authority
    }
    fn release_document(&self, scope: &AgentsDocEditorScope, authority: Option<&str>) {
        let mut runtime = self
            .agents_documents
            .lock()
            .expect("document owner poisoned");
        if runtime.controller.authority(scope) != Some(authority.unwrap_or_default()) {
            return;
        }
        runtime
            .controller
            .release(scope, self.timeline_started.elapsed());
        self.publish_document(&mut runtime.controller, scope);
        runtime.wake();
    }
    pub(crate) fn document_demand_changed(&self, scope: &ClientScope, demand: ClientDemand) {
        let _identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let ClientScope::AgentsDocumentContent {
            workspace_id,
            folder_id,
        } = scope
        else {
            return;
        };
        let key = match folder_id {
            Some(folder) => AgentsDocEditorScope::folder(workspace_id, folder),
            None => AgentsDocEditorScope::root(workspace_id),
        };
        let demand = self.current_scope_demand(scope).unwrap_or(demand);
        let authority = self.document_authority();
        let mut runtime = self
            .agents_documents
            .lock()
            .expect("document owner poisoned");
        let active = demand != ClientDemand::Suspended;
        if active && runtime.bindings.insert(key.clone()) {
            runtime
                .controller
                .acquire_as(key.clone(), authority.clone().unwrap_or_default());
            if authority.is_none() {
                runtime.controller.invalidate();
            }
        } else if !active && runtime.bindings.remove(&key) {
            runtime
                .controller
                .release(&key, self.timeline_started.elapsed());
        }
        self.publish_document(&mut runtime.controller, &key);
        runtime.wake();
    }
    pub fn agents_document_intent(&self, intent: AgentsDocumentIntent) -> ClientTransition {
        let _identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() {
            return self.reject_intent();
        }
        let scope = intent.scope().clone();
        let now = self.timeline_started.elapsed();
        let authority = self.document_authority();
        let mut runtime = self
            .agents_documents
            .lock()
            .expect("document owner poisoned");
        let intent = match intent {
            AgentsDocumentIntent::Scoped {
                expected_owner,
                action,
                ..
            } => {
                if runtime
                    .controller
                    .snapshot(&scope)
                    .is_none_or(|value| value.owner_generation() != expected_owner)
                {
                    return self.reject_intent();
                }
                action.into_intent(scope.clone())
            }
            other => other,
        };
        match intent {
            AgentsDocumentIntent::Scoped { .. } => {
                unreachable!("scoped editor command was unwrapped")
            }
            AgentsDocumentIntent::Edit { content, .. } => {
                runtime.controller.edit(&scope, content, now);
            }
            AgentsDocumentIntent::Save { .. } => runtime.controller.save(&scope, now),
            AgentsDocumentIntent::Reload { .. } => {
                if let Some(authority) = authority {
                    runtime.controller.revalidate_as(&scope, authority);
                    runtime.controller.reload(&scope);
                }
            }
            AgentsDocumentIntent::ReloadRemote { .. } => runtime.controller.reload_remote(&scope),
            AgentsDocumentIntent::OverwriteRemote { .. } => {
                runtime.controller.overwrite_remote(&scope, now)
            }
            AgentsDocumentIntent::Close { .. } => runtime.controller.close(&scope, now),
        }
        let transition = self.publish_document(&mut runtime.controller, &scope);
        runtime.wake();
        transition
    }
    fn publish_document(
        &self,
        controller: &mut AgentsDocController,
        scope: &AgentsDocEditorScope,
    ) -> ClientTransition {
        let current = self.snapshot(&document_scope(scope));
        let floor = current.as_ref().map_or(0, |p| p.revisions().scoped().get());
        let value = current.and_then(|p| p.snapshot().payload());
        let Some(publication) = controller.prepare_publication(scope, value, floor) else {
            return self.reject_intent();
        };
        self.publish(
            &ClientMutationAuthority { _private: () },
            document_scope(scope),
            crate::threads::registry::revisions(publication.revision()),
            publication,
            vec![],
        )
    }
    pub(crate) fn fence_agents_documents(
        &self,
        runtime: &mut DocumentRuntime,
    ) -> Vec<ClientPublicationDraft> {
        runtime.controller.invalidate();
        let drafts = runtime
            .controller
            .scopes()
            .into_iter()
            .filter_map(|scope| {
                let current = self.snapshot(&document_scope(&scope));
                let floor = current.as_ref().map_or(0, |p| p.revisions().scoped().get());
                let value = current.and_then(|p| p.snapshot().payload());
                let publication = runtime
                    .controller
                    .prepare_publication(&scope, value, floor)?;
                Some(ClientMutationAuthority { _private: () }.publication(
                    document_scope(&scope),
                    crate::threads::registry::revisions(publication.revision()),
                    publication,
                ))
            })
            .collect();
        runtime.wake();
        drafts
    }
    pub(crate) fn start_agents_document_controller(self: &Arc<Self>) {
        let (wake, receiver) = mpsc::sync_channel(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-agents-document".into())
            .spawn(move || {
                loop {
                    let Some(core) = weak.upgrade() else { return };
                    if core.is_stopped() {
                        return;
                    }
                    let identity = core.identity_authorization.lock().expect("identity owner poisoned");
                    let connection = core.gateway_http_generation();
                    let connection = connection.filter(|_| identity.connection_matches(connection));
                    let mut runtime = core
                        .agents_documents
                        .lock()
                        .expect("document owner poisoned");
                    let request = runtime
                        .controller
                        .next_request(core.timeline_started.elapsed());
                    if let Some(request) = &request {
                        core.publish_document(&mut runtime.controller, &request.scope);
                    }
                    let wait = runtime
                        .controller
                        .wait_duration(core.timeline_started.elapsed());
                    drop(runtime);
                    drop(identity);
                    let sender = core.transport_runtime().ws_command_sender();
                    drop(core);
                    if let Some(request) = request {
                        let scope = &request.scope;
                        let completion = match connection {
                            None => DocumentCompletion::Failed("agents_doc_connection_unavailable".into()),
                            Some(connection) => {
                            let transport = sender.requests_for_connection(connection);
                            match &request.operation {
                            DocumentOperation::Load | DocumentOperation::Conflict => {
                                match crate::transport::ws::command_sender::thread_agents_doc_get(&transport, agents_doc_get_params(
                                    scope.workspace_id(),
                                    scope.folder_id(),
                                )) {
                                    Ok(response) => DocumentCompletion::Loaded(response),
                                    Err(error) => DocumentCompletion::Failed(format!("{error:#}")),
                                }
                            }
                            DocumentOperation::Save {
                                content,
                                expected_version,
                            } => {
                                let params = agents_doc_save_params(
                                    scope.workspace_id(),
                                    scope.folder_id(),
                                    content,
                                    *expected_version,
                                    pioneer_protocol::ThreadAgentsDocSaveReason::Autosave,
                                );
                                match crate::transport::ws::command_sender::thread_agents_doc_save(&transport, params) {
                                    Ok(response) => DocumentCompletion::Saved(
                                        response.doc,
                                        agents_doc_saved_at_now(),
                                    ),
                                    Err(error) => {
                                        let message = format!("{error:#}");
                                        if agents_doc_is_version_conflict_error_message(&message) {
                                            DocumentCompletion::Conflict
                                        } else {
                                            DocumentCompletion::Failed(message)
                                        }
                                    }
                                }
                            }
                            }
                            }
                        };
                        if let Some(core) = weak.upgrade() {
                            let mut runtime = core
                                .agents_documents
                                .lock()
                                .expect("document owner poisoned");
                            runtime.controller.complete(
                                &request,
                                completion,
                                core.timeline_started.elapsed(),
                            );
                            core.publish_document(&mut runtime.controller, &request.scope);
                        }
                        continue;
                    }
                    match wait {
                        Some(wait) => {
                            if matches!(
                                receiver.recv_timeout(wait),
                                Err(mpsc::RecvTimeoutError::Disconnected)
                            ) {
                                return;
                            }
                        }
                        None => {
                            if receiver.recv().is_err() {
                                return;
                            }
                        }
                    }
                }
            })
            .expect("document controller could not start");
        let mut runtime = self
            .agents_documents
            .lock()
            .expect("document owner poisoned");
        runtime.wake = Some(wake);
        runtime.task = Some(task);
    }
}
