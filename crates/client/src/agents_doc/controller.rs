//! Canonical document drafts and generation-fenced load/save policy.
use super::{autosave::*, content::*, scope::AgentsDocEditorScope};
use pioneer_protocol::{ThreadAgentsDocGetResponse, ThreadAgentsDocPayload};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[cfg(any(test, feature = "test-support"))]
pub fn empty_document_response_for_test() -> ThreadAgentsDocGetResponse {
    ThreadAgentsDocGetResponse {
        explicit: None,
        effective: None,
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AgentsDocumentPublication {
    scope: AgentsDocEditorScope,
    revision: u64,
    edit_revision: u64,
    owner_generation: u64,
    content: String,
    load: AgentsDocEditorLoadState,
    save: AgentsDocEditorSaveState,
    access: bool,
    close_ready: bool,
}
impl AgentsDocumentPublication {
    pub fn scope(&self) -> &AgentsDocEditorScope {
        &self.scope
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn edit_revision(&self) -> u64 {
        self.edit_revision
    }
    pub fn owner_generation(&self) -> u64 {
        self.owner_generation
    }
    pub fn content(&self) -> &str {
        &self.content
    }
    pub fn load(&self) -> &AgentsDocEditorLoadState {
        &self.load
    }
    pub fn save(&self) -> &AgentsDocEditorSaveState {
        &self.save
    }
    pub fn has_access(&self) -> bool {
        self.access
    }
    pub fn is_close_ready(&self) -> bool {
        self.close_ready
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DocumentRequest {
    pub scope: AgentsDocEditorScope,
    generation: u64,
    edit_revision: u64,
    pub operation: DocumentOperation,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DocumentOperation {
    Load,
    Save {
        content: String,
        expected_version: Option<i64>,
    },
    Conflict,
}
pub(crate) enum DocumentCompletion {
    Loaded(ThreadAgentsDocGetResponse),
    Saved(ThreadAgentsDocPayload, i64),
    Conflict,
    Failed(String),
}
struct Document {
    authority: String,
    publication: Arc<AgentsDocumentPublication>,
    autosave: AgentsDocAutosaveState,
    request: Option<DocumentRequest>,
    queued: Option<DocumentOperation>,
    deadline: Option<Duration>,
    consumers: usize,
    closing: bool,
    suspended_draft: Option<String>,
    restore_draft: bool,
}

/// One owner per process; editors retain only immutable values and widget state.
#[derive(Default)]
pub struct AgentsDocController {
    documents: BTreeMap<AgentsDocEditorScope, Document>,
    recovery: BTreeMap<(String, AgentsDocEditorScope), (String, AgentsDocAutosaveState)>,
    generation: u64,
    owner_generation: u64,
}

/// A close request never treats retained recovery or a conflict as saved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentsDocumentCloseError {
    SaveFailed(AgentsDocEditorScope),
    Conflict(AgentsDocEditorScope),
    AccessLost(AgentsDocEditorScope),
    ClientStopped,
}

impl AgentsDocController {
    pub(crate) fn close_status(
        &self,
        scope: Option<&AgentsDocEditorScope>,
    ) -> Result<bool, AgentsDocumentCloseError> {
        for ((_, key), _) in &self.recovery {
            if scope.is_none_or(|scope| scope == key) {
                return Err(AgentsDocumentCloseError::AccessLost(key.clone()));
            }
        }
        let mut ready = true;
        for (key, doc) in &self.documents {
            if scope.is_some_and(|scope| scope != key) {
                continue;
            }
            if doc.autosave.pending_hash.is_none() && !doc.autosave.save_in_flight {
                continue;
            }
            if !doc.publication.access {
                return Err(AgentsDocumentCloseError::AccessLost(key.clone()));
            }
            match doc.publication.save() {
                AgentsDocEditorSaveState::Conflict { .. } => {
                    return Err(AgentsDocumentCloseError::Conflict(key.clone()));
                }
                AgentsDocEditorSaveState::Error { .. } => {
                    return Err(AgentsDocumentCloseError::SaveFailed(key.clone()));
                }
                _ if matches!(doc.publication.load(), AgentsDocEditorLoadState::Failed(_)) => {
                    return Err(AgentsDocumentCloseError::SaveFailed(key.clone()));
                }
                _ => ready = false,
            }
        }
        Ok(ready)
    }
    pub(crate) fn prepare_publication(
        &mut self,
        scope: &AgentsDocEditorScope,
        current: Option<Arc<AgentsDocumentPublication>>,
        floor: u64,
    ) -> Option<Arc<AgentsDocumentPublication>> {
        let doc = self.documents.get_mut(scope)?;
        let mut next = (*doc.publication).clone();
        next.revision = floor;
        if current.as_ref().is_some_and(|current| **current == next) {
            doc.publication = current.expect("equal publication exists");
        } else {
            next.revision = floor
                .checked_add(1)
                .expect("document publication revision exhausted");
            doc.publication = Arc::new(next);
        }
        Some(doc.publication.clone())
    }
    pub(crate) fn authority(&self, scope: &AgentsDocEditorScope) -> Option<&str> {
        self.documents.get(scope).map(|doc| doc.authority.as_str())
    }
    pub(crate) fn revalidate_as(&mut self, scope: &AgentsDocEditorScope, authority: String) {
        let Some(doc) = self.documents.get(scope) else {
            return;
        };
        if doc.publication.access && doc.authority == authority {
            return;
        }
        let consumers = doc.consumers;
        self.acquire_as(scope.clone(), authority);
        self.documents
            .get_mut(scope)
            .expect("document revalidated")
            .consumers = consumers;
    }
    pub(crate) fn scopes(&self) -> Vec<AgentsDocEditorScope> {
        self.documents.keys().cloned().collect()
    }
    pub fn snapshot(&self, scope: &AgentsDocEditorScope) -> Option<Arc<AgentsDocumentPublication>> {
        self.documents.get(scope).map(|d| d.publication.clone())
    }
    #[cfg(test)]
    pub(crate) fn acquire(&mut self, scope: AgentsDocEditorScope) {
        self.acquire_as(scope, String::new());
    }
    pub(crate) fn acquire_as(&mut self, scope: AgentsDocEditorScope, authority: String) {
        let mut revision_floor = 0;
        let new_owner = self.documents.get(&scope).is_none_or(|doc| {
            doc.consumers == 0 || !doc.publication.access || doc.authority != authority
        });
        if new_owner {
            self.owner_generation = self
                .owner_generation
                .checked_add(1)
                .expect("document owner generation exhausted");
        }
        if self
            .documents
            .get(&scope)
            .is_some_and(|d| d.authority != authority)
        {
            let old = self.documents.remove(&scope).expect("document exists");
            revision_floor = old.publication.revision;
            if old.autosave.pending_hash.is_some() {
                self.recovery.insert(
                    (old.authority, scope.clone()),
                    (
                        old.suspended_draft
                            .unwrap_or_else(|| old.publication.content.clone()),
                        old.autosave,
                    ),
                );
            }
        }
        let recovery = self.recovery.remove(&(authority.clone(), scope.clone()));
        let doc = self
            .documents
            .entry(scope.clone())
            .or_insert_with(|| Document {
                authority,
                publication: Arc::new(AgentsDocumentPublication {
                    scope,
                    revision: revision_floor + 1,
                    edit_revision: 0,
                    owner_generation: self.owner_generation,
                    content: String::new(),
                    load: AgentsDocEditorLoadState::Loading,
                    save: AgentsDocEditorSaveState::Clean,
                    access: true,
                    close_ready: false,
                }),
                autosave: AgentsDocAutosaveState::default(),
                request: None,
                queued: Some(DocumentOperation::Load),
                deadline: None,
                consumers: 0,
                closing: false,
                suspended_draft: None,
                restore_draft: false,
            });
        if new_owner {
            doc.update(|p| p.owner_generation = self.owner_generation);
        }
        if let Some((content, autosave)) = recovery {
            doc.suspended_draft = Some(content);
            doc.autosave = autosave;
        }
        if !doc.publication.access || doc.suspended_draft.is_some() {
            doc.restore_draft = doc.suspended_draft.is_some();
            // Keep recovery private until a scoped load proves current server access.
            let content = String::new();
            doc.autosave.save_in_flight = false;
            doc.request = None;
            doc.queued = Some(DocumentOperation::Load);
            doc.update(|p| {
                p.access = true;
                p.content = content;
                p.load = AgentsDocEditorLoadState::Loading;
            });
        }
        doc.consumers += 1;
        doc.closing = false;
        doc.update(|p| p.close_ready = false);
        if doc.publication.access
            && doc.request.is_none()
            && doc.queued.is_none()
            && matches!(doc.publication.load, AgentsDocEditorLoadState::Loading)
        {
            doc.queued = Some(DocumentOperation::Load);
        }
    }
    /// Route release flushes a dirty draft. Failed/conflicting drafts remain owned
    /// by Client for reentry, including edits made during an in-flight save.
    pub(crate) fn release(&mut self, scope: &AgentsDocEditorScope, now: Duration) {
        if let Some(doc) = self.documents.get_mut(scope) {
            doc.consumers = doc.consumers.saturating_sub(1);
            if doc.consumers == 0 {
                doc.closing = true;
                doc.flush(now);
                if doc.autosave.pending_hash.is_none()
                    && matches!(
                        doc.request.as_ref().map(|r| &r.operation),
                        Some(DocumentOperation::Load)
                    )
                {
                    doc.request = None;
                    doc.queued = None;
                }
            }
        }
    }
    pub fn edit(&mut self, scope: &AgentsDocEditorScope, content: String, now: Duration) -> bool {
        let Some(doc) = self.documents.get_mut(scope) else {
            return false;
        };
        if !doc.publication.access
            || doc.restore_draft
            || doc.publication.content == content
            || doc.consumers == 0
        {
            return false;
        }
        // A refresh may run alongside editing; its response cannot replace this draft.
        doc.update(|p| {
            p.content = content;
            p.edit_revision += 1;
            p.close_ready = false;
        });
        let decision = doc.autosave.mark_changed(&doc.publication.content);
        doc.deadline = matches!(decision, AgentsDocAutosaveDecision::Schedule { .. })
            .then_some(now + AGENTS_DOC_AUTOSAVE_DELAY);
        doc.sync_save();
        true
    }
    pub fn save(&mut self, scope: &AgentsDocEditorScope, now: Duration) {
        if let Some(doc) = self
            .documents
            .get_mut(scope)
            .filter(|d| d.publication.access)
        {
            doc.flush(now);
        }
    }
    pub fn close(&mut self, scope: &AgentsDocEditorScope, now: Duration) {
        if let Some(doc) = self.documents.get_mut(scope) {
            doc.closing = true;
            doc.flush(now);
            doc.sync_save();
        }
    }
    pub fn reload(&mut self, scope: &AgentsDocEditorScope) {
        let Some(doc) = self
            .documents
            .get_mut(scope)
            .filter(|d| d.publication.access)
        else {
            return;
        };
        if doc.request.is_some()
            || (doc.autosave.pending_hash.is_some()
                && !doc.restore_draft
                && !matches!(doc.publication.load, AgentsDocEditorLoadState::Failed(_)))
        {
            return;
        }
        doc.queued = Some(DocumentOperation::Load);
        doc.update(|p| p.load = AgentsDocEditorLoadState::Loading);
    }
    pub fn reload_remote(&mut self, scope: &AgentsDocEditorScope) {
        let Some(doc) = self
            .documents
            .get_mut(scope)
            .filter(|d| d.publication.access)
        else {
            return;
        };
        let AgentsDocEditorSaveState::Conflict { remote_doc, .. } = &doc.autosave.save_state else {
            return;
        };
        let remote = remote_doc.clone();
        doc.autosave.reload_remote(&remote);
        doc.deadline = None;
        doc.update(|p| {
            p.content = remote.content;
            p.edit_revision += 1;
        });
        doc.sync_save();
    }
    pub fn overwrite_remote(&mut self, scope: &AgentsDocEditorScope, now: Duration) {
        let Some(doc) = self
            .documents
            .get_mut(scope)
            .filter(|d| d.publication.access)
        else {
            return;
        };
        if !matches!(
            doc.autosave.save_state,
            AgentsDocEditorSaveState::Conflict { .. }
        ) {
            return;
        }
        doc.autosave
            .prepare_conflict_overwrite(&doc.publication.content);
        doc.deadline = Some(now);
        doc.sync_save();
    }
    pub(crate) fn next_request(&mut self, now: Duration) -> Option<DocumentRequest> {
        for (scope, doc) in &mut self.documents {
            if !doc.publication.access || doc.request.is_some() {
                continue;
            }
            let operation = doc.queued.take().or_else(|| {
                if doc.deadline.is_some_and(|deadline| now >= deadline)
                    && doc.autosave.debounce_due(doc.autosave.generation)
                {
                    doc.deadline = None;
                    Some(DocumentOperation::Save {
                        content: doc.publication.content.clone(),
                        expected_version: doc.autosave.last_saved_version,
                    })
                } else {
                    None
                }
            });
            let Some(operation) = operation else { continue };
            self.generation = self
                .generation
                .checked_add(1)
                .expect("document generation exhausted");
            let request = DocumentRequest {
                scope: scope.clone(),
                generation: self.generation,
                edit_revision: doc.publication.edit_revision,
                operation,
            };
            if matches!(request.operation, DocumentOperation::Save { .. }) {
                doc.autosave.mark_saving();
                doc.sync_save();
            }
            doc.request = Some(request.clone());
            return Some(request);
        }
        None
    }
    pub(crate) fn complete(
        &mut self,
        request: &DocumentRequest,
        completion: DocumentCompletion,
        now: Duration,
    ) -> bool {
        let Some(doc) = self.documents.get_mut(&request.scope) else {
            return false;
        };
        if doc.request.as_ref() != Some(request) || !doc.publication.access {
            return false;
        }
        match (&request.operation, &completion) {
            (
                DocumentOperation::Load | DocumentOperation::Conflict,
                DocumentCompletion::Loaded(response),
            ) => {
                if response.explicit.as_ref().is_some_and(|p| {
                    p.workspace_id != request.scope.workspace_id()
                        || p.folder_id.as_deref() != request.scope.folder_id()
                }) || response
                    .effective
                    .as_ref()
                    .is_some_and(|p| p.doc.workspace_id != request.scope.workspace_id())
                {
                    return false;
                }
            }
            (DocumentOperation::Save { content, .. }, DocumentCompletion::Saved(saved, _)) => {
                if saved.workspace_id != request.scope.workspace_id()
                    || saved.folder_id.as_deref() != request.scope.folder_id()
                    || saved.content_sha256 != agents_doc_content_hash(content)
                {
                    return false;
                }
            }
            (_, DocumentCompletion::Failed(_))
            | (DocumentOperation::Save { .. }, DocumentCompletion::Conflict) => {}
            _ => return false,
        }
        doc.request = None;
        match completion {
            DocumentCompletion::Loaded(response) => {
                if request.operation == DocumentOperation::Conflict {
                    doc.deadline = None;
                    if let Some(remote) = response.explicit {
                        doc.autosave
                            .enter_conflict(doc.publication.content.clone(), remote);
                    } else {
                        doc.autosave
                            .finish_error("agents_doc_conflict_missing_remote".into());
                    }
                } else {
                    let projection = agents_doc_load_projection(response);
                    if let Some(content) = doc.suspended_draft.take() {
                        doc.update(|p| p.content = content);
                    }
                    let restored_conflict = doc.restore_draft
                        && projection.explicit_doc.as_ref().is_some_and(|remote| {
                            doc.autosave.last_saved_version != Some(remote.version)
                                && agents_doc_content_hash(&doc.publication.content)
                                    != remote.content_sha256
                        });
                    doc.autosave.reset_from_loaded(
                        projection.explicit_doc.as_ref(),
                        projection.effective_doc.as_ref(),
                    );
                    if restored_conflict {
                        doc.autosave.enter_conflict(
                            doc.publication.content.clone(),
                            projection
                                .explicit_doc
                                .expect("restored conflict has remote"),
                        );
                        doc.deadline = None;
                    } else if request.edit_revision == doc.publication.edit_revision
                        && !doc.restore_draft
                    {
                        doc.update(|p| p.content = projection.buffer);
                    } else {
                        let decision = doc.autosave.mark_changed(&doc.publication.content);
                        doc.deadline =
                            matches!(decision, AgentsDocAutosaveDecision::Schedule { .. })
                                .then_some(if doc.closing {
                                    now
                                } else {
                                    now + AGENTS_DOC_AUTOSAVE_DELAY
                                });
                    }
                    doc.restore_draft = false;
                    doc.update(|p| p.load = AgentsDocEditorLoadState::Loaded);
                }
            }
            DocumentCompletion::Saved(saved, saved_at) => {
                let hash = agents_doc_content_hash(&doc.publication.content);
                let decision = doc.autosave.finish_success(&saved, &hash, saved_at);
                doc.deadline = matches!(decision, AgentsDocAutosaveDecision::Schedule { .. })
                    .then_some(if doc.closing {
                        now
                    } else {
                        now + AGENTS_DOC_AUTOSAVE_DELAY
                    });
            }
            DocumentCompletion::Conflict => {
                doc.deadline = None;
                doc.queued = Some(DocumentOperation::Conflict);
            }
            DocumentCompletion::Failed(message) => {
                doc.deadline = None;
                if request.operation == DocumentOperation::Load {
                    doc.update(|p| p.load = AgentsDocEditorLoadState::Failed(message));
                } else {
                    doc.autosave.finish_error(message);
                }
            }
        }
        doc.sync_save();
        true
    }
    /// Retains unsaved bytes privately until process teardown; protected values
    /// disappear from publications immediately and late responses are fenced.
    pub(crate) fn invalidate(&mut self) {
        for doc in self.documents.values_mut() {
            doc.request = None;
            doc.queued = None;
            doc.deadline = None;
            if doc.publication.access
                && doc.suspended_draft.is_none()
                && doc.autosave.pending_hash.is_some()
            {
                doc.suspended_draft = Some(doc.publication.content.clone());
            }
            doc.update(|p| {
                p.access = false;
                p.content.clear();
                p.close_ready = false;
                p.load = AgentsDocEditorLoadState::Failed("agents_doc_access_lost".into());
                p.save = AgentsDocEditorSaveState::Error {
                    message: "agents_doc_access_lost".into(),
                };
            });
        }
    }
    pub(crate) fn wait_duration(&self, now: Duration) -> Option<Duration> {
        self.documents
            .values()
            .filter(|d| d.publication.access && d.request.is_none())
            .filter_map(|d| {
                if d.queued.is_some() {
                    Some(Duration::ZERO)
                } else {
                    d.deadline.map(|deadline| deadline.saturating_sub(now))
                }
            })
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn close_rejects_conflicts_and_private_recovery_without_creating_an_empty_file() {
        let mut owner = AgentsDocController::default();
        let scope = AgentsDocEditorScope::root("workspace");
        owner.acquire(scope.clone());
        let load = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(
            &load,
            DocumentCompletion::Loaded(empty_document_response_for_test()),
            Duration::ZERO,
        );
        owner.close(&scope, Duration::ZERO);
        assert_eq!(owner.close_status(None), Ok(true));
        assert!(owner.next_request(Duration::ZERO).is_none());
        owner.edit(&scope, "local".into(), Duration::ZERO);
        owner.close(&scope, Duration::ZERO);
        assert_eq!(owner.close_status(None), Ok(false));
        let save = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(&save, DocumentCompletion::Conflict, Duration::ZERO);
        let refresh = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(
            &refresh,
            loaded(payload(&scope, "remote", 3)),
            Duration::ZERO,
        );
        assert!(!owner.snapshot(&scope).unwrap().is_close_ready());
        assert_eq!(
            owner.close_status(None),
            Err(AgentsDocumentCloseError::Conflict(scope.clone()))
        );
        owner.invalidate();
        assert_eq!(
            owner.close_status(None),
            Err(AgentsDocumentCloseError::AccessLost(scope.clone()))
        );
        owner.acquire_as(scope.clone(), "another principal".into());
        assert_eq!(
            owner.close_status(None),
            Err(AgentsDocumentCloseError::AccessLost(scope))
        );
    }

    fn payload(
        scope: &AgentsDocEditorScope,
        content: &str,
        version: i64,
    ) -> ThreadAgentsDocPayload {
        ThreadAgentsDocPayload {
            id: "document".into(),
            workspace_id: scope.workspace_id().into(),
            folder_id: scope.folder_id().map(str::to_owned),
            status: pioneer_protocol::ThreadAgentsDocStatus::Active,
            title: "AGENTS.md".into(),
            content: content.into(),
            content_sha256: agents_doc_content_hash(content),
            version,
            created_at: 0,
            updated_at: version,
        }
    }
    fn loaded(doc: ThreadAgentsDocPayload) -> DocumentCompletion {
        DocumentCompletion::Loaded(ThreadAgentsDocGetResponse {
            explicit: Some(doc),
            effective: None,
        })
    }
    fn ready() -> (AgentsDocController, AgentsDocEditorScope) {
        let mut owner = AgentsDocController::default();
        let scope = AgentsDocEditorScope::root("workspace");
        owner.acquire(scope.clone());
        let load = owner.next_request(Duration::ZERO).unwrap();
        assert!(owner.complete(&load, loaded(payload(&scope, "initial", 1)), Duration::ZERO));
        (owner, scope)
    }
    #[test]
    fn delayed_load_preserves_newer_draft_and_uses_loaded_version() {
        let (mut owner, scope) = ready();
        owner.reload(&scope);
        let load = owner.next_request(Duration::ZERO).unwrap();
        owner.edit(&scope, "new edit".into(), Duration::ZERO);
        assert!(owner.complete(&load, loaded(payload(&scope, "remote", 2)), Duration::ZERO));
        assert_eq!(owner.snapshot(&scope).unwrap().content(), "new edit");
        let save = owner.next_request(AGENTS_DOC_AUTOSAVE_DELAY).unwrap();
        assert_eq!(
            save.operation,
            DocumentOperation::Save {
                content: "new edit".into(),
                expected_version: Some(2)
            }
        );
    }
    #[test]
    fn save_echo_does_not_acknowledge_newer_edits_or_emit_duplicate_completion() {
        let (mut owner, scope) = ready();
        owner.edit(&scope, "first".into(), Duration::ZERO);
        let save = owner.next_request(AGENTS_DOC_AUTOSAVE_DELAY).unwrap();
        owner.edit(&scope, "second".into(), AGENTS_DOC_AUTOSAVE_DELAY);
        assert!(owner.complete(
            &save,
            DocumentCompletion::Saved(payload(&scope, "first", 2), 1),
            AGENTS_DOC_AUTOSAVE_DELAY
        ));
        let first = owner.snapshot(&scope).unwrap();
        assert_eq!(first.content(), "second");
        assert_eq!(first.save(), &AgentsDocEditorSaveState::Dirty);
        assert!(!owner.complete(
            &save,
            DocumentCompletion::Saved(payload(&scope, "first", 2), 1),
            AGENTS_DOC_AUTOSAVE_DELAY
        ));
        assert!(Arc::ptr_eq(&first, &owner.snapshot(&scope).unwrap()));
        assert!(owner.next_request(AGENTS_DOC_AUTOSAVE_DELAY * 2).is_some());
    }
    #[test]
    fn reverting_during_save_still_saves_the_reverted_draft_after_completion() {
        let (mut owner, scope) = ready();
        owner.edit(&scope, "changed".into(), Duration::ZERO);
        let save = owner.next_request(AGENTS_DOC_AUTOSAVE_DELAY).unwrap();
        owner.edit(&scope, "initial".into(), AGENTS_DOC_AUTOSAVE_DELAY);
        owner.complete(
            &save,
            DocumentCompletion::Saved(payload(&scope, "changed", 2), 1),
            AGENTS_DOC_AUTOSAVE_DELAY,
        );
        assert_eq!(
            owner.snapshot(&scope).unwrap().save(),
            &AgentsDocEditorSaveState::Dirty
        );
        let next = owner.next_request(AGENTS_DOC_AUTOSAVE_DELAY * 2).unwrap();
        assert_eq!(
            next.operation,
            DocumentOperation::Save {
                content: "initial".into(),
                expected_version: Some(2)
            }
        );
    }
    #[test]
    fn failed_save_keeps_draft_and_requires_explicit_retry() {
        let (mut owner, scope) = ready();
        owner.edit(&scope, "draft".into(), Duration::ZERO);
        owner.save(&scope, Duration::ZERO);
        let request = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(
            &request,
            DocumentCompletion::Failed("offline".into()),
            Duration::ZERO,
        );
        assert!(owner.next_request(Duration::from_secs(100)).is_none());
        assert_eq!(owner.wait_duration(Duration::ZERO), None);
        assert_eq!(owner.snapshot(&scope).unwrap().content(), "draft");
        owner.save(&scope, Duration::ZERO);
        assert!(owner.next_request(Duration::ZERO).is_some());
    }
    #[test]
    fn debounce_coalesces_equal_input_without_echo_or_new_revision() {
        let (mut owner, scope) = ready();
        let before = owner.snapshot(&scope).unwrap();
        assert!(!owner.edit(&scope, "initial".into(), Duration::ZERO));
        assert!(Arc::ptr_eq(&before, &owner.snapshot(&scope).unwrap()));
        owner.edit(&scope, "a".into(), Duration::ZERO);
        owner.edit(&scope, "b".into(), Duration::from_millis(500));
        assert!(owner.next_request(Duration::from_millis(700)).is_none());
        assert!(owner.next_request(Duration::from_millis(1200)).is_some());
        assert!(owner.next_request(Duration::from_millis(1200)).is_none());
    }
    #[test]
    fn conflict_refresh_uses_latest_draft_and_overwrite_uses_remote_version() {
        let (mut owner, scope) = ready();
        owner.edit(&scope, "first".into(), Duration::ZERO);
        owner.save(&scope, Duration::ZERO);
        let request = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(&request, DocumentCompletion::Conflict, Duration::ZERO);
        let refresh = owner.next_request(Duration::ZERO).unwrap();
        owner.edit(&scope, "second".into(), Duration::ZERO);
        owner.complete(
            &refresh,
            loaded(payload(&scope, "remote", 3)),
            Duration::ZERO,
        );
        assert!(
            matches!(owner.snapshot(&scope).unwrap().save(), AgentsDocEditorSaveState::Conflict { local_content, .. } if local_content == "second")
        );
        assert!(owner.next_request(Duration::from_secs(1)).is_none());
        owner.overwrite_remote(&scope, Duration::ZERO);
        assert_eq!(
            owner.next_request(Duration::ZERO).unwrap().operation,
            DocumentOperation::Save {
                content: "second".into(),
                expected_version: Some(3)
            }
        );
    }
    #[test]
    fn scope_and_access_loss_fence_late_completions_and_keep_unsaved_bytes_private() {
        let (mut owner, scope) = ready();
        let other = AgentsDocEditorScope::folder("workspace", "folder");
        owner.acquire(other.clone());
        let load = owner.next_request(Duration::ZERO).unwrap();
        let before = owner.snapshot(&scope).unwrap();
        assert!(!owner.complete(
            &load,
            loaded(payload(&scope, "wrong scope", 2)),
            Duration::ZERO
        ));
        assert!(Arc::ptr_eq(&before, &owner.snapshot(&scope).unwrap()));
        owner.edit(&scope, "unsaved".into(), Duration::ZERO);
        owner.invalidate();
        assert!(!owner.complete(&load, loaded(payload(&other, "late", 2)), Duration::ZERO));
        assert!(!owner.snapshot(&scope).unwrap().has_access());
        assert_eq!(owner.snapshot(&scope).unwrap().content(), "");
        assert_eq!(
            owner.documents[&scope].suspended_draft.as_deref(),
            Some("unsaved")
        );
        assert!(owner.next_request(Duration::from_secs(100)).is_none());
    }
    #[test]
    fn close_waits_for_all_edits_and_release_keeps_failed_draft_for_reentry() {
        let (mut owner, scope) = ready();
        owner.edit(&scope, "first".into(), Duration::ZERO);
        owner.close(&scope, Duration::ZERO);
        let save = owner.next_request(Duration::ZERO).unwrap();
        owner.edit(&scope, "second".into(), Duration::ZERO);
        owner.complete(
            &save,
            DocumentCompletion::Saved(payload(&scope, "first", 2), 1),
            Duration::ZERO,
        );
        assert!(!owner.snapshot(&scope).unwrap().is_close_ready());
        let second = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(
            &second,
            DocumentCompletion::Failed("offline".into()),
            Duration::ZERO,
        );
        owner.release(&scope, Duration::ZERO);
        owner.acquire(scope.clone());
        assert_eq!(owner.snapshot(&scope).unwrap().content(), "second");
        assert!(!owner.snapshot(&scope).unwrap().is_close_ready());
    }

    #[test]
    fn recovery_stays_private_until_successful_load_and_failed_load_can_retry() {
        let (mut owner, scope) = ready();
        owner.edit(&scope, "private unsaved".into(), Duration::ZERO);
        owner.invalidate();
        owner.revalidate_as(&scope, String::new());
        assert_eq!(owner.snapshot(&scope).unwrap().content(), "");
        assert!(!owner.edit(&scope, "stale widget".into(), Duration::ZERO));
        let load = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(
            &load,
            DocumentCompletion::Failed("denied".into()),
            Duration::ZERO,
        );
        assert_eq!(owner.snapshot(&scope).unwrap().content(), "");
        assert_eq!(
            owner.documents[&scope].suspended_draft.as_deref(),
            Some("private unsaved")
        );
        owner.reload(&scope);
        let retry = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(
            &retry,
            loaded(payload(&scope, "initial", 1)),
            Duration::ZERO,
        );
        assert_eq!(owner.snapshot(&scope).unwrap().content(), "private unsaved");
        assert!(owner.next_request(AGENTS_DOC_AUTOSAVE_DELAY).is_some());
    }

    #[test]
    fn revalidation_restores_only_same_principal_draft_and_detects_remote_conflict() {
        let (mut owner, scope) = ready();
        owner.edit(&scope, "unsaved".into(), Duration::ZERO);
        owner.invalidate();
        owner.revalidate_as(&scope, "another-account".into());
        let load = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(
            &load,
            loaded(payload(&scope, "other-account-document", 2)),
            Duration::ZERO,
        );
        assert_eq!(
            owner.snapshot(&scope).unwrap().content(),
            "other-account-document"
        );
        owner.invalidate();
        owner.revalidate_as(&scope, String::new());
        let load = owner.next_request(Duration::ZERO).unwrap();
        owner.complete(
            &load,
            loaded(payload(&scope, "remote-change", 3)),
            Duration::ZERO,
        );
        assert_eq!(owner.snapshot(&scope).unwrap().content(), "unsaved");
        assert!(matches!(
            owner.snapshot(&scope).unwrap().save(),
            AgentsDocEditorSaveState::Conflict { .. }
        ));
        assert_eq!(owner.wait_duration(Duration::ZERO), None);
    }
}
impl Document {
    fn update(&mut self, apply: impl FnOnce(&mut AgentsDocumentPublication)) {
        let mut next = (*self.publication).clone();
        apply(&mut next);
        if next != *self.publication {
            next.revision = next
                .revision
                .checked_add(1)
                .expect("document revision exhausted");
            self.publication = Arc::new(next);
        }
    }
    fn sync_save(&mut self) {
        if !self.publication.access || self.restore_draft {
            return;
        }
        let save = self.autosave.save_state.clone();
        let ready = self.closing
            && self.request.is_none()
            && self.autosave.pending_hash.is_none()
            && !matches!(
                save,
                AgentsDocEditorSaveState::Error { .. } | AgentsDocEditorSaveState::Conflict { .. }
            );
        self.update(|p| {
            p.save = save;
            p.close_ready = ready;
        });
    }
    fn flush(&mut self, now: Duration) {
        if !self.publication.access
            || !matches!(self.publication.load, AgentsDocEditorLoadState::Loaded)
            || (self.autosave.pending_hash.is_none() && !self.autosave.save_in_flight)
        {
            return;
        }
        let decision = self.autosave.flush(&self.publication.content);
        self.deadline =
            matches!(decision, AgentsDocAutosaveDecision::SaveNow { .. }).then_some(now);
        self.sync_save();
    }
}
