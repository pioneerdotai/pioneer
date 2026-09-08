//! Process-local draft ownership. Publications are immutable; editor events are intents.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use super::{
    draft::ComposerDomainDraft,
    state_machine::{ComposerDomainAction, ComposerDomainState, reduce_composer_domain_state},
};
use crate::core::{
    ClientCore, ClientMutationAuthority, ClientRevisions, ClientScope, ClientTransition,
    ContentRevision, DomainRevision, PresentationRevision, ScopedRevision,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DraftId(u64);

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComposerOperationKind {
    Send,
    EditMessage,
    Steer,
    Voice,
    PickFiles,
    PickMedia,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerOperationIdentity {
    pub thread_id: String,
    pub draft_id: DraftId,
    pub generation: u64,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerOperationPlan {
    pub steer_target: Option<super::steer::ComposerSteerTarget>,
    pub message_edit: Option<super::message_edit::ComposerMessageEditTarget>,
    pub voice_start: Option<pioneer_protocol::VoiceSessionStartContext>,
    pub authorization_fingerprint: Option<String>,
    pub identity: ComposerOperationIdentity,
    pub kind: ComposerOperationKind,
    pub draft: ComposerDomainDraft,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerOperationStatus {
    StartingCapture,
    Pending,
    Preparing,
    Uploading,
    Prepared,
    Sending,
    Failed { message: String },
    Cancelled,
    Completed,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ComposerOperationPublication {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice_capture_preflight: Option<super::voice::ComposerVoiceCapturePreflightState>,
    pub voice_turn_id: Option<String>,
    pub voice_committing: bool,
    pub voice_finalize: Option<crate::voice::VoiceFinalizeResponseReduction>,
    pub voice_result: Option<crate::voice::VoiceSessionResultReduction>,
    pub voice_session_id: Option<String>,
    pub voice_context: Option<pioneer_protocol::VoiceTurnContext>,
    pub identity: ComposerOperationIdentity,
    pub kind: ComposerOperationKind,
    pub plan: Option<ComposerOperationPlan>,
    pub status: ComposerOperationStatus,
}

impl ComposerOperationPublication {
    pub fn pending(&self) -> bool {
        matches!(
            self.status,
            ComposerOperationStatus::StartingCapture
                | ComposerOperationStatus::Pending
                | ComposerOperationStatus::Preparing
                | ComposerOperationStatus::Uploading
                | ComposerOperationStatus::Prepared
                | ComposerOperationStatus::Sending
        )
    }
}

fn cancel_operation(input: &mut ComposerPublication) {
    let Some(operation) = input
        .operation
        .as_mut()
        .filter(|operation| operation.pending())
    else {
        return;
    };
    if let Some(plan) = operation.plan.take() {
        for attachment in &mut input.draft.domain.attachments {
            if matches!(
                attachment.upload_state,
                super::attachments::ComposerAttachmentUploadState::Uploading
            ) && let Some(original) = plan
                .draft
                .domain
                .attachments
                .iter()
                .find(|original| original.path == attachment.path)
            {
                attachment.upload_state = original.upload_state.clone();
            }
        }
    }
    operation.voice_context = None;
    operation.voice_committing = false;
    operation.status = ComposerOperationStatus::Cancelled;
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerOperationCompletion {
    VoicePrepared {
        snapshot: super::turn_prepare::PreparedVoiceComposerSnapshot,
    },
    Uploaded {
        artifacts: Vec<Option<pioneer_protocol::ArtifactRef>>,
    },
    FilesSelected {
        attachments: Vec<super::attachments::ComposerAttachment>,
    },
    Sent,
    MessageEditFailed {
        conflicted: bool,
    },
    Failed {
        message: String,
    },
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ComposerPublication {
    pub(super) voice_readiness: Option<super::voice_readiness::ComposerVoiceReadinessPublication>,
    selected_provider_ready: bool,
    permission_options: Vec<super::permissions::ComposerPermissionModeOption>,
    selected_permission_mode_allowed: bool,
    pub(super) runtime_selection: Option<super::runtime_selection::ComposerRuntimePublication>,
    pub(super) model_display: Option<super::model_display::ComposerModelDisplayPublication>,
    thread_id: String,
    draft_id: DraftId,
    pub(super) revision: u64,
    draft: ComposerDomainDraft,
    message_edit: Option<super::message_edit::ComposerMessageEditTarget>,
    authorization_fingerprint: Option<String>,
    reconciliation: Option<super::reconciliation::ExecutionDraftReconciliation>,
    execution_capabilities_removed: bool,
    pub(super) operation: Option<ComposerOperationPublication>,
}

impl ComposerPublication {
    pub fn voice_readiness(
        &self,
    ) -> Option<&super::voice_readiness::ComposerVoiceReadinessPublication> {
        self.voice_readiness.as_ref()
    }
    pub fn runtime_selection(
        &self,
    ) -> Option<&super::runtime_selection::ComposerRuntimePublication> {
        self.runtime_selection.as_ref()
    }
    pub fn permission_options(&self) -> &[super::permissions::ComposerPermissionModeOption] {
        &self.permission_options
    }
    pub fn selected_permission_mode_allowed(&self) -> bool {
        self.selected_permission_mode_allowed
    }
    pub fn selected_provider_ready(&self) -> bool {
        self.selected_provider_ready
    }
    pub(super) fn refresh_runtime_readiness(&mut self) {
        self.selected_provider_ready = self.compute_provider_ready();
        self.selected_permission_mode_allowed = self
            .permission_options
            .iter()
            .any(|option| option.mode == self.domain().selected_permission_mode);
    }
    fn compute_provider_ready(&self) -> bool {
        if self
            .domain()
            .selected_provider
            .as_deref()
            .and_then(crate::providers::list::runtime_id_from_cli_runtime_provider_key)
            .is_none()
        {
            return true;
        }
        self.runtime_selection.as_ref().is_some_and(|p| {
            p.identity.draft_id == self.draft_id
                && p.selected_provider == self.domain().selected_provider
                && p.selected_provider_ready
        })
    }
    pub fn model_display(&self) -> Option<&super::model_display::ComposerModelDisplayPublication> {
        self.model_display.as_ref()
    }
    pub fn message_edit(&self) -> Option<&super::message_edit::ComposerMessageEditTarget> {
        self.message_edit.as_ref()
    }
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }
    pub fn draft_id(&self) -> DraftId {
        self.draft_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn draft(&self) -> &ComposerDomainDraft {
        &self.draft
    }
    pub fn domain(&self) -> &ComposerDomainState {
        &self.draft.domain
    }
    pub fn authorization_fingerprint(&self) -> Option<&str> {
        self.authorization_fingerprint.as_deref()
    }
    pub fn reconciliation(&self) -> Option<&super::reconciliation::ExecutionDraftReconciliation> {
        self.reconciliation.as_ref()
    }
    pub fn operation(&self) -> Option<&ComposerOperationPublication> {
        self.operation.as_ref()
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerIntent {
    Activate {
        thread_id: String,
    },
    SetVoiceReadinessDemand {
        thread_id: String,
        draft_id: DraftId,
        demand: super::voice_readiness::ComposerVoiceReadinessDemand,
    },
    SyncModelSelection {
        thread_id: String,
        draft_id: DraftId,
        reset: bool,
    },
    RetryRuntimeSelection {
        thread_id: String,
        draft_id: DraftId,
    },
    RetryModelDisplay {
        thread_id: String,
        draft_id: DraftId,
    },
    CommitVoiceCapture {
        identity: ComposerOperationIdentity,
    },
    VoiceFinalized {
        identity: ComposerOperationIdentity,
        response: pioneer_protocol::VoiceSessionFinalizeResponse,
    },
    StartMessageEdit {
        thread_id: String,
        draft_id: DraftId,
        turn_id: String,
    },
    SubmitSteer {
        thread_id: String,
        draft_id: DraftId,
    },
    SubmitMessageEdit {
        thread_id: String,
        draft_id: DraftId,
    },
    StartVoiceCapture {
        identity: ComposerOperationIdentity,
    },
    FinalizeVoiceCapture {
        identity: ComposerOperationIdentity,
    },
    VoiceSessionStarted {
        identity: ComposerOperationIdentity,
        session_id: String,
    },
    ClearAll,
    PrepareOperation {
        identity: ComposerOperationIdentity,
    },
    UploadOperation {
        identity: ComposerOperationIdentity,
    },
    BeginOperation {
        thread_id: String,
        draft_id: DraftId,
        operation: ComposerOperationKind,
    },
    CompleteOperation {
        identity: ComposerOperationIdentity,
        completion: ComposerOperationCompletion,
    },
    Open {
        thread_id: String,
        defaults: ComposerDomainState,
    },
    EditText {
        thread_id: String,
        draft_id: DraftId,
        text: String,
    },
    Domain {
        thread_id: String,
        draft_id: DraftId,
        action: ComposerDomainAction,
    },
    Clear {
        thread_id: String,
        draft_id: DraftId,
    },
}

#[derive(Default)]
pub(crate) struct ComposerStore {
    pub(super) voice_sessions: BTreeMap<u64, super::voice::VoiceSessionCleanup>,
    pub(super) voice_readiness: BTreeMap<String, super::voice_readiness::VoiceReadinessRequest>,
    pub(super) runtimes: BTreeMap<String, super::runtime_selection::ComposerRuntimeState>,
    pub(super) model_pickers:
        BTreeMap<String, Arc<super::model_picker::ComposerModelPickerPublication>>,
    pub(super) model_picker_subscriptions: BTreeMap<String, usize>,
    pub(super) model_picker_suspended: BTreeSet<String>,
    pub(super) catalogs: BTreeMap<String, Arc<super::catalog::ComposerCatalogPublication>>,
    pub(super) catalog_suspended: BTreeSet<String>,
    pub(super) catalog_subscriptions: BTreeMap<String, usize>,
    pub(super) drafts: BTreeMap<String, Arc<ComposerPublication>>,
    last_opened_thread: Option<String>,
    subscriptions: BTreeMap<String, usize>,
    pub(super) suspended: BTreeSet<String>,
    policies: BTreeMap<String, pioneer_protocol::AuthorizationExecutionDraftPolicyProjection>,
    next_identity: u64,
    pub(super) next_operation: u64,
}

impl ComposerStore {
    pub(crate) fn fence_authorization(&mut self) -> Vec<crate::core::ClientPublicationDraft> {
        self.policies.clear();
        self.runtimes.clear();
        self.catalogs.clear();
        self.model_pickers.clear();
        self.voice_readiness.clear();
        self.drafts
            .values_mut()
            .map(|current| {
                let mut next = (**current).clone();
                cancel_operation(&mut next);
                next.authorization_fingerprint = None;
                next.reconciliation = None;
                next.permission_options.clear();
                next.selected_permission_mode_allowed = false;
                next.selected_provider_ready = false;
                next.runtime_selection = None;
                next.model_display = None;
                next.voice_readiness = None;
                next.revision = next
                    .revision
                    .checked_add(1)
                    .expect("composer revision exhausted");
                *current = Arc::new(next);
                ClientMutationAuthority { _private: () }.publication(
                    ClientScope::Composer {
                        thread_id: current.thread_id().into(),
                    },
                    ClientRevisions::new(
                        DomainRevision::new(current.revision()),
                        PresentationRevision::new(current.revision()),
                        ContentRevision::ZERO,
                        ScopedRevision::new(current.revision()),
                    ),
                    current.clone(),
                )
            })
            .collect()
    }

    pub(crate) fn clear(&mut self) {
        self.drafts.clear();
        self.last_opened_thread = None;
        self.runtimes.clear();
        self.voice_readiness.clear();
        self.voice_sessions.clear();
        self.catalogs.clear();
        self.model_pickers.clear();
        self.policies.clear();
        // Identity allocation survives logout, so a late callback cannot name a new draft.
    }

    fn identity(&mut self) -> DraftId {
        self.next_identity = self
            .next_identity
            .checked_add(1)
            .expect("draft identity exhausted");
        DraftId(self.next_identity)
    }
}

impl ClientCore {
    pub(crate) fn forget_revoked_composer(&self, thread: &str) {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let Some(draft) = store.drafts.remove(thread) else {
            return;
        };
        store.policies.remove(thread);
        if store.last_opened_thread.as_deref() == Some(thread) {
            store.last_opened_thread = None;
        }
        let revision = draft
            .revision()
            .checked_add(1)
            .expect("composer revision exhausted");
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::Composer {
                thread_id: thread.into(),
            },
            ClientRevisions::new(
                DomainRevision::new(revision),
                PresentationRevision::new(revision),
                ContentRevision::ZERO,
                ScopedRevision::new(revision),
            ),
            Arc::new(serde_json::Value::Null),
            vec![],
        );
    }

    pub(super) fn reconcile_composer_authorization(
        &self,
        thread: &str,
        draft: DraftId,
    ) -> Option<(ClientTransition, bool)> {
        let workspace = self
            .thread_coordinator_snapshot(thread)?
            .workspace_id
            .clone();
        let snapshot = self
            .thread_capability_snapshot(thread)
            .and_then(|p| p.snapshot.clone())
            .or_else(|| self.authorization_snapshot(Some(&workspace), None))?;
        let workspace = snapshot
            .workspace
            .as_ref()
            .filter(|p| p.workspace_id == workspace)?;
        let policy = workspace.execution_draft_policy.clone();
        let options = super::permissions::authorized_composer_permission_mode_options(
            &workspace.capabilities.agent_permission_options,
        );
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if self.is_stopped() || store.suspended.contains(thread) {
            return None;
        }
        let current = store
            .drafts
            .get(thread)
            .filter(|p| p.draft_id() == draft)?
            .clone();
        if current.authorization_fingerprint() == Some(policy.fingerprint.as_str())
            && current.permission_options == options
        {
            return None;
        }
        let mut next = (*current).clone();
        next.permission_options = options;
        let result = super::reconciliation::reconcile_composer_policy(
            &mut next.draft.domain,
            &mut next.authorization_fingerprint,
            &policy,
        );
        let selections_changed = result.reasons.iter().any(|r| {
            r.kind != super::reconciliation::ExecutionDraftReconciliationKind::PolicyGeneration
        });
        next.reconciliation = Some(result);
        store.policies.insert(thread.into(), policy);
        if next.authorization_fingerprint != current.authorization_fingerprint
            || next.draft.domain != current.draft.domain
        {
            cancel_operation(&mut next);
        }
        Some((
            self.publish_composer_model_display(&mut store, next),
            selections_changed,
        ))
    }

    pub(super) fn apply_composer_runtime_target(
        &self,
        next: &mut ComposerPublication,
        target: super::capabilities::ComposerCapabilityTarget,
    ) {
        let reduction = reduce_composer_domain_state(
            &next.draft.domain,
            ComposerDomainAction::SyncCapabilityTarget {
                provider: next.draft.domain.selected_provider.clone(),
                target,
            },
        );
        if reduction.changed {
            cancel_operation(next);
            next.draft.domain = reduction.state;
            next.execution_capabilities_removed = reduction.execution_capabilities_removed;
        }
    }

    pub(super) fn apply_composer_model_picker_selection(
        &self,
        store: &mut ComposerStore,
        identity: &ComposerOperationIdentity,
        selection: super::model_selection::ModelSelectorSelection,
        target: super::capabilities::ComposerCapabilityTarget,
    ) {
        let Some(current) = store
            .drafts
            .get(&identity.thread_id)
            .filter(|p| p.draft_id() == identity.draft_id)
            .cloned()
        else {
            return;
        };
        let mut next = (*current).clone();
        let reduction = reduce_composer_domain_state(
            &next.draft.domain,
            ComposerDomainAction::SetModelSelectionFromUser {
                provider: selection.provider,
                model: selection.model,
                capability_target: Some(target),
            },
        );
        next.draft.domain = reduce_composer_domain_state(
            &reduction.state,
            ComposerDomainAction::SetReasoningEffortFromUser {
                effort: selection.selected_reasoning_effort,
            },
        )
        .state;
        next.execution_capabilities_removed = reduction.execution_capabilities_removed;
        if let Some(policy) = store.policies.get(&identity.thread_id) {
            next.reconciliation = Some(super::reconciliation::reconcile_composer_policy(
                &mut next.draft.domain,
                &mut next.authorization_fingerprint,
                policy,
            ));
        }
        if next.draft.domain == current.draft.domain {
            return;
        }
        cancel_operation(&mut next);
        next.revision = next
            .revision
            .checked_add(1)
            .expect("composer revision exhausted");
        let revision = next.revision;
        next.refresh_runtime_readiness();
        let next = Arc::new(next);
        store
            .drafts
            .insert(identity.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::Composer {
                thread_id: identity.thread_id.clone(),
            },
            ClientRevisions::new(
                DomainRevision::new(revision),
                PresentationRevision::new(revision),
                ContentRevision::ZERO,
                ScopedRevision::new(revision),
            ),
            next,
            vec![],
        );
    }

    pub(crate) fn composer_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::Composer { thread_id } = scope else {
            return;
        };
        let identity = {
            let mut owner = self.composer_store.lock().expect("composer store poisoned");
            let count = owner.subscriptions.entry(thread_id.clone()).or_default();
            if added {
                *count += 1;
                owner.suspended.remove(thread_id);
                drop(owner);
                self.observe_composer_runtime(thread_id, None, false);
                self.observe_composer_model_display(thread_id, None);
                return;
            }
            *count = count.saturating_sub(1);
            if *count > 0 {
                return;
            }
            owner.subscriptions.remove(thread_id);
            owner.suspended.insert(thread_id.clone());
            owner.drafts.get(thread_id).and_then(|input| {
                input
                    .operation()
                    .filter(|operation| operation.pending())
                    .map(|operation| operation.identity.clone())
            })
        };
        self.cancel_composer_voice_readiness(thread_id, None);
        self.cancel_composer_runtime(thread_id);
        self.cancel_composer_model_display(thread_id);
        self.cancel_composer_catalog(thread_id);
        self.cancel_composer_model_picker(thread_id);
        if let Some(identity) = identity {
            self.complete_composer_operation(identity, ComposerOperationCompletion::Cancelled);
        }
    }

    pub(crate) fn composer_demand_changed(
        &self,
        scope: &ClientScope,
        demand: crate::core::ClientDemand,
    ) {
        let ClientScope::Composer { thread_id } = scope else {
            return;
        };
        let identity = {
            let mut owner = self.composer_store.lock().expect("composer store poisoned");
            if demand != crate::core::ClientDemand::Suspended {
                owner.suspended.remove(thread_id);
                drop(owner);
                self.observe_composer_runtime(thread_id, None, false);
                self.observe_composer_model_display(thread_id, None);
                return;
            }
            owner.suspended.insert(thread_id.clone());
            owner.drafts.get(thread_id).and_then(|input| {
                input
                    .operation()
                    .filter(|operation| operation.pending())
                    .map(|operation| operation.identity.clone())
            })
        };
        self.cancel_composer_voice_readiness(thread_id, None);
        self.cancel_composer_runtime(thread_id);
        self.cancel_composer_model_display(thread_id);
        self.cancel_composer_catalog(thread_id);
        self.cancel_composer_model_picker(thread_id);
        if let Some(identity) = identity {
            self.complete_composer_operation(identity, ComposerOperationCompletion::Cancelled);
        }
    }

    pub(crate) fn cancel_composer_requests_for_thread(&self, thread_id: &str) {
        self.cancel_composer_voice_readiness(thread_id, None);
        self.cancel_composer_runtime(thread_id);
        self.cancel_composer_model_display(thread_id);
        self.cancel_composer_catalog(thread_id);
        self.cancel_composer_model_picker(thread_id);
        if let Some(identity) = self.composer_snapshot(thread_id).and_then(|input| {
            input
                .operation()
                .filter(|operation| operation.pending())
                .map(|operation| operation.identity.clone())
        }) {
            self.complete_composer_operation(identity, ComposerOperationCompletion::Cancelled);
        }
    }

    pub(crate) fn cancel_composer_requests(&self) {
        let threads = self
            .composer_store
            .lock()
            .expect("composer store poisoned")
            .drafts
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for thread in threads {
            self.cancel_composer_requests_for_thread(&thread);
        }
    }

    pub(crate) fn commit_composer_send(
        &self,
        identity: &ComposerOperationIdentity,
        workspace_id: &str,
        reduction: &super::turn_prepare::PreparedComposerTurnSubmitReduction,
    ) -> Option<crate::timeline::semantic::SemanticTimelineCachePatch> {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let current = store.drafts.get(&identity.thread_id)?;
        let operation = current.operation.as_ref()?;
        if self.is_stopped()
            || current.draft_id != identity.draft_id
            || operation.identity != *identity
            || operation.kind != ComposerOperationKind::Send
            || operation.status != ComposerOperationStatus::Prepared
            || reduction.send_context.thread_id != identity.thread_id
            || self
                .thread_coordinator_snapshot(&identity.thread_id)
                .is_none_or(|thread| thread.workspace_id != workspace_id)
        {
            return None;
        }
        let mut next = (**current).clone();
        next.operation.as_mut()?.status = ComposerOperationStatus::Sending;
        next.revision = next
            .revision
            .checked_add(1)
            .expect("composer revision exhausted");
        // Draft cancellation and optimistic thread commit share this mutation boundary.
        let patch = self.commit_prepared_thread_turn(
            &identity.thread_id,
            workspace_id,
            next.draft.domain.selected_mode,
            &reduction.thread_snapshot_update,
            reduction.local_turn_start_requested_event.clone(),
            reduction.composer_execution_mode,
        );
        let revision = next.revision;
        next.refresh_runtime_readiness();
        let next = Arc::new(next);
        store
            .drafts
            .insert(identity.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::Composer {
                thread_id: identity.thread_id.clone(),
            },
            ClientRevisions::new(
                DomainRevision::new(revision),
                PresentationRevision::new(revision),
                ContentRevision::ZERO,
                ScopedRevision::new(revision),
            ),
            next,
            vec![],
        );
        Some(patch)
    }

    pub fn clear_composer_drafts(&self) -> ClientTransition {
        self.cancel_composer_requests();
        let authority = ClientMutationAuthority { _private: () };
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if self.is_stopped() {
            return self.reject_intent();
        }
        let drafts = store.drafts.values().cloned().collect::<Vec<_>>();
        let mut publications = Vec::with_capacity(drafts.len());
        for draft in drafts {
            let mut next = (*draft).clone();
            next.draft_id = store.identity();
            next.voice_readiness = None;
            next.draft = ComposerDomainDraft::default();
            next.authorization_fingerprint = None;
            next.reconciliation = None;
            next.execution_capabilities_removed = false;
            next.operation = None;
            next.message_edit = None;
            next.model_display = None;
            next.runtime_selection = None;
            next.voice_readiness = None;
            next.permission_options.clear();
            next.revision = next
                .revision
                .checked_add(1)
                .expect("composer revision exhausted");
            let revision = next.revision;
            let thread_id = next.thread_id.clone();
            next.refresh_runtime_readiness();
            let next = Arc::new(next);
            publications.push(authority.publication(
                ClientScope::Composer {
                    thread_id: thread_id.clone(),
                },
                ClientRevisions::new(
                    DomainRevision::new(revision),
                    PresentationRevision::new(revision),
                    ContentRevision::ZERO,
                    ScopedRevision::new(revision),
                ),
                next.clone(),
            ));
            store.drafts.insert(thread_id, next);
        }
        store.policies.clear();
        store.catalogs.clear();
        store.model_pickers.clear();
        store.runtimes.clear();
        store.voice_readiness.clear();
        store.voice_sessions.clear();
        self.transition(&authority, publications, vec![])
    }
    pub(crate) fn observe_composer_voice_notification(
        &self,
        notification: &pioneer_protocol::GatewayNotification,
    ) {
        use pioneer_protocol::{GatewayNotification, VoiceSessionOutcome};
        if let GatewayNotification::VoiceSessionResult(result) = notification {
            self.forget_completed_voice_session(&result.session_id);
        }
        let operations = self
            .composer_store
            .lock()
            .expect("composer store poisoned")
            .drafts
            .values()
            .filter_map(|draft| draft.operation.as_ref())
            .filter(|operation| {
                operation.kind == ComposerOperationKind::Voice && operation.pending()
            })
            .cloned()
            .collect::<Vec<_>>();
        for operation in operations {
            let completion = match notification {
                GatewayNotification::VoiceSessionResult(result)
                    if operation.voice_session_id.as_deref()
                        == Some(result.session_id.as_str()) =>
                {
                    let matching_turn = result.turn_id.as_ref().is_none_or(|turn| {
                        operation
                            .plan
                            .as_ref()
                            .and_then(|p| p.voice_start.as_ref())
                            .is_some_and(|start| &start.turn_id == turn)
                    });
                    if !matching_turn {
                        continue;
                    }
                    match result.outcome {
                        VoiceSessionOutcome::TurnStarted => ComposerOperationCompletion::Sent,
                        VoiceSessionOutcome::Cancelled => ComposerOperationCompletion::Cancelled,
                        VoiceSessionOutcome::NoSpeech | VoiceSessionOutcome::Failed => {
                            ComposerOperationCompletion::Failed {
                                message: result
                                    .error
                                    .as_ref()
                                    .map(|e| e.message.clone())
                                    .unwrap_or_else(|| "Voice session failed".into()),
                            }
                        }
                    }
                }
                GatewayNotification::TurnStarted(result)
                    if operation
                        .plan
                        .as_ref()
                        .and_then(|p| p.voice_start.as_ref())
                        .is_some_and(|start| {
                            start.turn_id == result.turn.id && start.thread_id == result.thread_id
                        }) =>
                {
                    ComposerOperationCompletion::Sent
                }
                _ => continue,
            };
            let thread = operation.identity.thread_id.clone();
            self.retire_voice_operation(&operation.identity, true);
            let transition = self.composer_intent_with_voice_result(
                ComposerIntent::CompleteOperation {
                    identity: operation.identity,
                    completion,
                },
                match notification {
                    GatewayNotification::VoiceSessionResult(result) => Some(result),
                    _ => None,
                },
            );
            if transition.outcome() == crate::core::ClientTransitionOutcome::Changed {
                self.refresh_composer_voice_readiness(&thread);
            }
        }
    }

    pub fn composer_snapshot(&self, thread_id: &str) -> Option<Arc<ComposerPublication>> {
        self.composer_store
            .lock()
            .expect("composer store poisoned")
            .drafts
            .get(thread_id)
            .cloned()
    }

    pub fn composer_intent(&self, intent: ComposerIntent) -> ClientTransition {
        let intent = match intent {
            ComposerIntent::Activate { thread_id } => {
                let store = self.composer_store.lock().expect("composer store poisoned");
                let defaults = store
                    .last_opened_thread
                    .as_ref()
                    .and_then(|id| store.drafts.get(id))
                    .map(|publication| publication.domain().clone())
                    .unwrap_or_default();
                ComposerIntent::Open {
                    thread_id,
                    defaults,
                }
            }
            intent => intent,
        };
        let authorization_scope = match &intent {
            ComposerIntent::Domain {
                thread_id,
                draft_id,
                ..
            }
            | ComposerIntent::BeginOperation {
                thread_id,
                draft_id,
                ..
            } => Some((thread_id.as_str(), *draft_id)),
            _ => None,
        };
        if let Some((thread, draft)) = authorization_scope {
            if let Some((transition, selections_changed)) =
                self.reconcile_composer_authorization(thread, draft)
            {
                if selections_changed
                    && matches!(
                        intent,
                        ComposerIntent::BeginOperation {
                            operation: ComposerOperationKind::Send | ComposerOperationKind::Voice,
                            ..
                        }
                    )
                {
                    return transition;
                }
            }
        }

        if let ComposerIntent::SyncModelSelection {
            thread_id,
            draft_id,
            reset,
        } = intent
        {
            if self.thread_coordinator_snapshot(&thread_id).is_none() {
                return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
            }
            let selection = self.resolved_composer_model_selection(&thread_id);
            let action = if reset {
                ComposerDomainAction::ResetModelSelection {
                    selection,
                    capability_target: None,
                }
            } else {
                ComposerDomainAction::SyncResolvedModelSelection {
                    selection,
                    capability_target: None,
                }
            };
            return self.composer_intent(ComposerIntent::Domain {
                thread_id,
                draft_id,
                action,
            });
        }
        if let ComposerIntent::SetVoiceReadinessDemand {
            thread_id,
            draft_id,
            demand,
        } = intent
        {
            return self.set_composer_voice_readiness_demand(&thread_id, draft_id, demand);
        }
        if let ComposerIntent::RetryRuntimeSelection {
            thread_id,
            draft_id,
        } = intent
        {
            return self
                .observe_composer_runtime(&thread_id, Some(draft_id), true)
                .unwrap_or_else(|| {
                    self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![])
                });
        }
        if let ComposerIntent::RetryModelDisplay {
            thread_id,
            draft_id,
        } = intent
        {
            return self
                .observe_composer_model_display(&thread_id, Some(draft_id))
                .unwrap_or_else(|| {
                    self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![])
                });
        }
        let thread_id = match &intent {
            ComposerIntent::Open { thread_id, .. }
            | ComposerIntent::Domain { thread_id, .. }
            | ComposerIntent::StartMessageEdit { thread_id, .. }
            | ComposerIntent::Clear { thread_id, .. } => Some(thread_id.clone()),
            ComposerIntent::CompleteOperation {
                identity,
                completion: ComposerOperationCompletion::Sent,
            } => Some(identity.thread_id.clone()),
            _ => None,
        };
        let voice_completion = match &intent {
            ComposerIntent::CompleteOperation {
                identity,
                completion:
                    ComposerOperationCompletion::Sent
                    | ComposerOperationCompletion::Failed { .. }
                    | ComposerOperationCompletion::Cancelled,
            } => self
                .composer_snapshot(&identity.thread_id)
                .and_then(|p| p.operation().cloned())
                .filter(|p| p.identity == *identity && p.kind == ComposerOperationKind::Voice)
                .map(|_| identity.thread_id.clone()),
            _ => None,
        };
        let retiring_voice = match &intent {
            ComposerIntent::CompleteOperation {
                identity,
                completion,
            } if voice_completion.is_some() => Some((
                identity.clone(),
                matches!(completion, ComposerOperationCompletion::Sent),
            )),
            ComposerIntent::Clear { thread_id, .. } => self
                .composer_snapshot(thread_id)
                .and_then(|p| p.operation().cloned())
                .filter(|op| op.kind == ComposerOperationKind::Voice)
                .map(|op| (op.identity, false)),
            _ => None,
        };
        let transition = self.composer_intent_with_voice_result(intent, None);
        if transition.outcome() == crate::core::ClientTransitionOutcome::Changed {
            if let Some((identity, sent)) = retiring_voice {
                self.retire_voice_operation(&identity, sent);
            }
        }
        if let Some(thread_id) = thread_id {
            if self
                .composer_catalog_snapshot(&thread_id)
                .is_some_and(|catalog| {
                    self.composer_snapshot(&thread_id)
                        .is_none_or(|draft| draft.draft_id() != catalog.draft_id)
                })
            {
                self.cancel_composer_catalog(&thread_id);
            }
            if self
                .composer_model_picker_snapshot(&thread_id)
                .is_some_and(|picker| {
                    self.composer_snapshot(&thread_id)
                        .is_none_or(|draft| draft.draft_id() != picker.identity.draft_id)
                })
            {
                self.cancel_composer_model_picker(&thread_id);
            }
            if let Some(draft) = self.composer_snapshot(&thread_id) {
                self.reconcile_composer_authorization(&thread_id, draft.draft_id());
            }
            self.observe_composer_runtime(&thread_id, None, false);
            self.observe_composer_model_display(&thread_id, None);
            self.reconcile_composer_voice_readiness_draft(&thread_id);
        }
        if transition.outcome() == crate::core::ClientTransitionOutcome::Changed {
            if let Some(thread) = voice_completion {
                self.refresh_composer_voice_readiness(&thread);
            }
        }
        transition
    }

    fn composer_intent_with_voice_result(
        &self,
        intent: ComposerIntent,
        voice_result: Option<&pioneer_protocol::VoiceSessionResultNotification>,
    ) -> ClientTransition {
        if let ComposerIntent::SubmitMessageEdit {
            thread_id,
            draft_id,
        } = intent
        {
            return self.submit_composer_message_edit(thread_id, draft_id);
        }
        if let ComposerIntent::SubmitSteer {
            thread_id,
            draft_id,
        } = intent
        {
            return self.submit_composer_steer(thread_id, draft_id);
        }
        let steer_target = if let ComposerIntent::BeginOperation {
            thread_id,
            operation: ComposerOperationKind::Steer,
            ..
        } = &intent
        {
            self.composer_steer_target(thread_id)
        } else {
            None
        };
        let edit_target = if let ComposerIntent::StartMessageEdit {
            thread_id, turn_id, ..
        } = &intent
        {
            self.composer_message_edit_target(thread_id, turn_id)
        } else {
            None
        };
        if matches!(intent, ComposerIntent::ClearAll) {
            return self.clear_composer_drafts();
        }
        let voice_start = if let ComposerIntent::BeginOperation {
            thread_id,
            operation: ComposerOperationKind::Voice,
            ..
        } = &intent
        {
            self.thread_coordinator_snapshot(thread_id).map(|thread| {
                pioneer_protocol::VoiceSessionStartContext {
                    thread_id: thread_id.clone(),
                    workspace_id: thread.workspace_id.clone(),
                    turn_id: crate::turns::start::plan_turn_start_ids().turn_id,
                }
            })
        } else {
            None
        };
        let resolved_defaults = match &intent {
            ComposerIntent::Open { thread_id, .. }
            | ComposerIntent::Domain {
                thread_id,
                action:
                    ComposerDomainAction::SetModeFromUser { .. } | ComposerDomainAction::Reset { .. },
                ..
            } => self
                .thread_coordinator_snapshot(thread_id)
                .map(|_| self.resolved_composer_model_selection(thread_id)),
            _ => None,
        };
        let authority = ClientMutationAuthority { _private: () };
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if self.is_stopped() {
            return self.reject_intent();
        }
        let (thread_id, mut next) = match intent {
            ComposerIntent::Open {
                thread_id,
                defaults,
            } => {
                if thread_id.is_empty() {
                    return self.reject_intent();
                }
                store.last_opened_thread = Some(thread_id.clone());
                if store.drafts.contains_key(&thread_id) {
                    return self.transition(&authority, vec![], vec![]);
                }
                let revision = self
                    .snapshot(&ClientScope::Composer {
                        thread_id: thread_id.clone(),
                    })
                    .map_or(1, |publication| {
                        publication
                            .snapshot()
                            .revisions()
                            .scoped()
                            .get()
                            .checked_add(1)
                            .expect("composer revision exhausted")
                    });
                let defaults = if let Some(selection) = resolved_defaults {
                    reduce_composer_domain_state(
                        &defaults,
                        ComposerDomainAction::SyncResolvedModelSelection {
                            selection,
                            capability_target: None,
                        },
                    )
                    .state
                } else {
                    defaults
                };
                let next = ComposerPublication {
                    thread_id: thread_id.clone(),
                    draft_id: store.identity(),
                    revision,
                    draft: super::draft::composer_thread_switch_fallback(defaults),
                    authorization_fingerprint: None,
                    reconciliation: None,
                    execution_capabilities_removed: false,
                    operation: None,
                    message_edit: None,
                    model_display: None,
                    runtime_selection: None,
                    voice_readiness: None,
                    selected_provider_ready: false,
                    permission_options: vec![],
                    selected_permission_mode_allowed: false,
                };
                (thread_id, next)
            }
            intent => {
                let (thread_id, draft_id) = match &intent {
                    ComposerIntent::StartMessageEdit {
                        thread_id,
                        draft_id,
                        ..
                    }
                    | ComposerIntent::SubmitSteer {
                        thread_id,
                        draft_id,
                    }
                    | ComposerIntent::SubmitMessageEdit {
                        thread_id,
                        draft_id,
                    }
                    | ComposerIntent::EditText {
                        thread_id,
                        draft_id,
                        ..
                    }
                    | ComposerIntent::Domain {
                        thread_id,
                        draft_id,
                        ..
                    }
                    | ComposerIntent::Clear {
                        thread_id,
                        draft_id,
                    } => (thread_id, *draft_id),
                    ComposerIntent::BeginOperation {
                        thread_id,
                        draft_id,
                        ..
                    } => (thread_id, *draft_id),
                    ComposerIntent::CommitVoiceCapture { identity }
                    | ComposerIntent::VoiceFinalized { identity, .. }
                    | ComposerIntent::StartVoiceCapture { identity }
                    | ComposerIntent::FinalizeVoiceCapture { identity }
                    | ComposerIntent::VoiceSessionStarted { identity, .. }
                    | ComposerIntent::PrepareOperation { identity }
                    | ComposerIntent::UploadOperation { identity }
                    | ComposerIntent::CompleteOperation { identity, .. } => {
                        (&identity.thread_id, identity.draft_id)
                    }
                    ComposerIntent::Open { .. }
                    | ComposerIntent::Activate { .. }
                    | ComposerIntent::ClearAll
                    | ComposerIntent::RetryRuntimeSelection { .. }
                    | ComposerIntent::SetVoiceReadinessDemand { .. }
                    | ComposerIntent::RetryModelDisplay { .. }
                    | ComposerIntent::SyncModelSelection { .. } => unreachable!(),
                };
                let Some(current) = store.drafts.get(thread_id).cloned() else {
                    return self.reject_intent();
                };
                if current.draft_id != draft_id {
                    return self.transition(&authority, vec![], vec![]);
                }
                let mut next = (*current).clone();
                match intent {
                    ComposerIntent::SubmitMessageEdit { .. }
                    | ComposerIntent::SubmitSteer { .. } => unreachable!(),
                    ComposerIntent::StartMessageEdit { .. } => {
                        if store.suspended.contains(&next.thread_id) {
                            return self.reject_intent();
                        }
                        let Some(target) = edit_target else {
                            return self.reject_intent();
                        };
                        if next
                            .operation
                            .as_ref()
                            .is_some_and(|operation| operation.pending())
                        {
                            return self.transition(&authority, vec![], vec![]);
                        }
                        next.draft =
                            super::draft::composer_thread_switch_fallback(next.draft.domain);
                        next.draft.domain = reduce_composer_domain_state(
                            &next.draft.domain,
                            ComposerDomainAction::SetModeFromUser {
                                mode: pioneer_protocol::ThreadMode::Message,
                            },
                        )
                        .state;
                        next.draft.text = target.preview.clone();
                        next.message_edit = Some(target);
                        next.draft_id = store.identity();
                        next.voice_readiness = None;
                        next.operation = None;
                    }
                    ComposerIntent::CommitVoiceCapture { identity } => {
                        let Some(operation) = next.operation.as_mut().filter(|operation| {
                            operation.identity == identity
                                && operation.kind == ComposerOperationKind::Voice
                                && operation.status == ComposerOperationStatus::Pending
                                && operation.voice_session_id.is_some()
                                && !operation.voice_committing
                        }) else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        operation.voice_committing = true;
                    }
                    ComposerIntent::VoiceFinalized { identity, response } => {
                        let Some(operation) = next.operation.as_mut().filter(|operation| {
                            operation.identity == identity
                                && operation.kind == ComposerOperationKind::Voice
                                && operation.status == ComposerOperationStatus::Sending
                                && operation.voice_finalize.is_none()
                        }) else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        let Some(session_id) = operation.voice_session_id.clone() else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        operation.voice_finalize =
                            Some(crate::voice::reduce_voice_session_finalize_response(
                                session_id, &response,
                            ));
                    }
                    ComposerIntent::StartVoiceCapture { identity } => {
                        let Some(operation) = next.operation.as_mut().filter(|operation| {
                            operation.identity == identity
                                && operation.kind == ComposerOperationKind::Voice
                                && operation.status == ComposerOperationStatus::Pending
                                && operation.voice_session_id.is_none()
                        }) else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        operation.status = ComposerOperationStatus::StartingCapture;
                    }
                    ComposerIntent::FinalizeVoiceCapture { identity } => {
                        let Some(operation) = next.operation.as_mut().filter(|operation| {
                            operation.identity == identity
                                && operation.kind == ComposerOperationKind::Voice
                                && operation.status == ComposerOperationStatus::Prepared
                                && operation.voice_session_id.is_some()
                                && operation.voice_context.is_some()
                        }) else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        operation.status = ComposerOperationStatus::Sending;
                        operation.voice_committing = true;
                    }

                    ComposerIntent::VoiceSessionStarted {
                        identity,
                        session_id,
                    } => {
                        let Some(operation) = next.operation.as_mut().filter(|operation| {
                            operation.identity == identity
                                && operation.kind == ComposerOperationKind::Voice
                                && operation.pending()
                                && operation.voice_session_id.is_none()
                                && !session_id.trim().is_empty()
                        }) else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        operation.voice_session_id = Some(session_id);
                        if operation.status == ComposerOperationStatus::StartingCapture {
                            operation.status = ComposerOperationStatus::Pending;
                        }
                    }

                    ComposerIntent::UploadOperation { identity } => {
                        let Some(operation) = next.operation.as_mut().filter(|operation| {
                            operation.identity == identity
                                && matches!(
                                    operation.kind,
                                    ComposerOperationKind::Send | ComposerOperationKind::Voice
                                )
                                && matches!(
                                    operation.status,
                                    ComposerOperationStatus::Pending
                                        | ComposerOperationStatus::Preparing
                                )
                        }) else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        operation.status = ComposerOperationStatus::Uploading;
                        super::turn_prepare::mark_pending_composer_attachments_uploading(
                            &mut next.draft.domain.attachments,
                        );
                    }
                    ComposerIntent::PrepareOperation { identity } => {
                        let Some(operation) = next.operation.as_mut().filter(|operation| {
                            operation.identity == identity
                                && matches!(
                                    operation.kind,
                                    ComposerOperationKind::Send
                                        | ComposerOperationKind::Voice
                                        | ComposerOperationKind::EditMessage
                                        | ComposerOperationKind::Steer
                                )
                                && operation.status == ComposerOperationStatus::Pending
                        }) else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        operation.status = ComposerOperationStatus::Preparing;
                        if operation.kind == ComposerOperationKind::Voice {
                            super::turn_prepare::mark_pending_composer_attachments_uploading(
                                &mut next.draft.domain.attachments,
                            );
                        }
                    }
                    ComposerIntent::BeginOperation { operation, .. } => {
                        if store.suspended.contains(&next.thread_id) {
                            return self.reject_intent();
                        }
                        if operation == ComposerOperationKind::Steer
                            && (steer_target.is_none() || next.draft.text.trim().is_empty())
                        {
                            return self.reject_intent();
                        }
                        if operation == ComposerOperationKind::EditMessage
                            && next.message_edit.as_ref().is_none_or(|target| {
                                target.conflicted
                                    || (next.draft.text.trim().is_empty()
                                        && target.artifacts.is_empty())
                            })
                        {
                            return self.transition(&authority, vec![], vec![]);
                        }
                        if next.message_edit.is_some()
                            && operation != ComposerOperationKind::EditMessage
                        {
                            return self.transition(&authority, vec![], vec![]);
                        }
                        if next
                            .operation
                            .as_ref()
                            .is_some_and(|operation| operation.pending())
                        {
                            return self.transition(&authority, vec![], vec![]);
                        }
                        store.next_operation = store
                            .next_operation
                            .checked_add(1)
                            .expect("composer operation generation exhausted");
                        let identity = ComposerOperationIdentity {
                            thread_id: next.thread_id.clone(),
                            draft_id: next.draft_id,
                            generation: store.next_operation,
                        };
                        if let Some(target) = &mut next.message_edit {
                            target.failed = false;
                        }
                        let plan = ComposerOperationPlan {
                            steer_target,
                            message_edit: next.message_edit.clone(),
                            voice_start,
                            authorization_fingerprint: next.authorization_fingerprint.clone(),
                            identity: identity.clone(),
                            kind: operation,
                            draft: next.draft.clone(),
                        };
                        if operation == ComposerOperationKind::Send {
                            super::turn_prepare::mark_pending_composer_attachments_uploading(
                                &mut next.draft.domain.attachments,
                            );
                        }
                        next.operation = Some(ComposerOperationPublication {
                            voice_capture_preflight: None,
                            voice_turn_id: plan
                                .voice_start
                                .as_ref()
                                .map(|start| start.turn_id.clone()),
                            voice_committing: false,
                            voice_finalize: None,
                            voice_result: None,
                            voice_session_id: None,
                            voice_context: None,
                            identity,
                            kind: operation,
                            plan: Some(plan),
                            status: ComposerOperationStatus::Pending,
                        });
                    }
                    ComposerIntent::CompleteOperation {
                        identity,
                        completion,
                    } => {
                        let Some(operation) = next
                            .operation
                            .as_ref()
                            .filter(|operation| {
                                operation.identity == identity && operation.pending()
                            })
                            .cloned()
                        else {
                            return self.transition(&authority, vec![], vec![]);
                        };
                        let plan = operation
                            .plan
                            .as_ref()
                            .expect("pending operation owns plan");
                        if let Some(result) = voice_result {
                            if operation.kind != ComposerOperationKind::Voice
                                || operation.voice_session_id.as_deref()
                                    != Some(result.session_id.as_str())
                                || result.turn_id.as_ref().is_some_and(|turn| {
                                    operation.voice_turn_id.as_ref() != Some(turn)
                                })
                            {
                                return self.transition(&authority, vec![], vec![]);
                            }
                            next.operation.as_mut().unwrap().voice_result = Some(
                                crate::voice::reduce_voice_session_result_notification(result),
                            );
                        }

                        let completion =
                            if let ComposerOperationCompletion::VoicePrepared { snapshot } =
                                completion
                            {
                                if operation.kind != ComposerOperationKind::Voice
                                    || operation.status != ComposerOperationStatus::Uploading
                                    || !plan.voice_start.as_ref().is_some_and(|start| {
                                        start.thread_id == snapshot.context.thread_id
                                            && start.workspace_id == snapshot.context.workspace_id
                                            && start.turn_id == snapshot.context.turn_id
                                    })
                                {
                                    return self.transition(&authority, vec![], vec![]);
                                }
                                next.operation.as_mut().unwrap().voice_context =
                                    Some(snapshot.context);
                                ComposerOperationCompletion::Uploaded {
                                    artifacts: snapshot.uploaded_attachment_artifacts,
                                }
                            } else {
                                completion
                            };
                        let status = match completion {
                            ComposerOperationCompletion::VoicePrepared { .. } => unreachable!(),
                            ComposerOperationCompletion::Uploaded { artifacts } => {
                                if !matches!(
                                    operation.status,
                                    ComposerOperationStatus::Pending
                                        | ComposerOperationStatus::Uploading
                                ) || !matches!(
                                    operation.kind,
                                    ComposerOperationKind::Send | ComposerOperationKind::Voice
                                ) || artifacts.len() != plan.draft.domain.attachments.len()
                                {
                                    return self.transition(&authority, vec![], vec![]);
                                }
                                for (original, artifact) in
                                    plan.draft.domain.attachments.iter().zip(artifacts)
                                {
                                    if let Some(artifact) = artifact
                                        && let Some(attachment) = next
                                            .draft
                                            .domain
                                            .attachments
                                            .iter_mut()
                                            .find(|attachment| attachment.path == original.path)
                                    {
                                        attachment.upload_state = super::attachments::ComposerAttachmentUploadState::Uploaded { artifact };
                                    }
                                }
                                ComposerOperationStatus::Prepared
                            }
                            ComposerOperationCompletion::FilesSelected { attachments } => {
                                if !matches!(
                                    operation.kind,
                                    ComposerOperationKind::PickFiles
                                        | ComposerOperationKind::PickMedia
                                ) {
                                    return self.transition(&authority, vec![], vec![]);
                                }
                                for attachment in attachments {
                                    next.draft.domain = reduce_composer_domain_state(
                                        &next.draft.domain,
                                        ComposerDomainAction::AddAttachment { attachment },
                                    )
                                    .state;
                                }
                                ComposerOperationStatus::Completed
                            }
                            ComposerOperationCompletion::MessageEditFailed { conflicted } => {
                                if operation.kind != ComposerOperationKind::EditMessage
                                    || operation.status != ComposerOperationStatus::Preparing
                                {
                                    return self.transition(&authority, vec![], vec![]);
                                }
                                if let Some(target) = &mut next.message_edit {
                                    target.failed = true;
                                    target.conflicted = conflicted;
                                }
                                ComposerOperationStatus::Failed {
                                    message: "message_edit_failed".into(),
                                }
                            }
                            ComposerOperationCompletion::Sent => {
                                if matches!(
                                    operation.kind,
                                    ComposerOperationKind::EditMessage
                                        | ComposerOperationKind::Steer
                                ) && operation.status != ComposerOperationStatus::Preparing
                                {
                                    return self.transition(&authority, vec![], vec![]);
                                }
                                next.message_edit = None;
                                if matches!(
                                    operation.kind,
                                    ComposerOperationKind::PickFiles
                                        | ComposerOperationKind::PickMedia
                                ) {
                                    return self.transition(&authority, vec![], vec![]);
                                }
                                next.draft = super::draft::composer_thread_switch_fallback(
                                    next.draft.domain,
                                );
                                next.draft_id = store.identity();
                                next.voice_readiness = None;
                                ComposerOperationStatus::Completed
                            }
                            ComposerOperationCompletion::Failed { message } => {
                                super::turn_prepare::mark_uploading_composer_attachments_failed(
                                    &mut next.draft.domain.attachments,
                                    &message,
                                );
                                ComposerOperationStatus::Failed { message }
                            }
                            ComposerOperationCompletion::Cancelled => {
                                cancel_operation(&mut next);
                                ComposerOperationStatus::Cancelled
                            }
                        };
                        if let Some(operation) = &mut next.operation {
                            if matches!(
                                status,
                                ComposerOperationStatus::Cancelled
                                    | ComposerOperationStatus::Failed { .. }
                            ) {
                                operation.voice_committing = false;
                            }
                            operation.status = status;
                            if !operation.pending() {
                                operation.plan = None;
                                operation.voice_context = None;
                            }
                        }
                    }
                    ComposerIntent::EditText { text, .. } => {
                        if next.draft.text == text {
                            return self.transition(&authority, vec![], vec![]);
                        }
                        next.draft.domain = reduce_composer_domain_state(
                            &next.draft.domain,
                            ComposerDomainAction::ReconcileMentionsWithText { text: text.clone() },
                        )
                        .state;
                        next.draft.text = text;
                        cancel_operation(&mut next);
                    }
                    ComposerIntent::Domain { action, .. } => {
                        if let ComposerDomainAction::SelectMention { candidate } = &action {
                            if candidate.nickname.trim().is_empty() {
                                return self.transition(&authority, vec![], vec![]);
                            }
                            let token = format!("@{}", candidate.nickname.trim());
                            let text = &next.draft.text;
                            if text.trim().is_empty() {
                                next.draft.text = format!("{token} ");
                            } else if !text.contains(&token) {
                                next.draft.text = format!("{} {token} ", text.trim_end());
                            }
                        }
                        let transition = reduce_composer_domain_state(&next.draft.domain, action);
                        if !transition.changed && next.draft.text == current.draft.text {
                            return self.transition(&authority, vec![], vec![]);
                        }
                        next.draft.domain = transition.state;
                        if let Some(selection) = resolved_defaults {
                            next.draft.domain = reduce_composer_domain_state(
                                &next.draft.domain,
                                ComposerDomainAction::SyncResolvedModelSelection {
                                    selection,
                                    capability_target: None,
                                },
                            )
                            .state;
                        }
                        next.execution_capabilities_removed =
                            transition.execution_capabilities_removed;
                        if let Some(policy) = store.policies.get(&next.thread_id) {
                            next.reconciliation =
                                Some(super::reconciliation::reconcile_composer_policy(
                                    &mut next.draft.domain,
                                    &mut next.authorization_fingerprint,
                                    policy,
                                ));
                        }
                        cancel_operation(&mut next);
                    }
                    ComposerIntent::Clear { .. } => {
                        next.message_edit = None;
                        next.draft =
                            super::draft::composer_thread_switch_fallback(next.draft.domain);
                        next.draft_id = store.identity();
                        next.voice_readiness = None;
                        next.authorization_fingerprint = None;
                        next.reconciliation = None;
                        next.execution_capabilities_removed = false;
                        next.operation = None;
                    }
                    ComposerIntent::Open { .. }
                    | ComposerIntent::Activate { .. }
                    | ComposerIntent::ClearAll
                    | ComposerIntent::RetryRuntimeSelection { .. }
                    | ComposerIntent::SetVoiceReadinessDemand { .. }
                    | ComposerIntent::RetryModelDisplay { .. }
                    | ComposerIntent::SyncModelSelection { .. } => unreachable!(),
                }
                if *current == next {
                    return self.transition(&authority, vec![], vec![]);
                }
                next.revision = next
                    .revision
                    .checked_add(1)
                    .expect("composer revision exhausted");
                (next.thread_id.clone(), next)
            }
        };
        next.refresh_runtime_readiness();
        let next = Arc::new(next);
        store.drafts.insert(thread_id.clone(), next.clone());
        let revision = next.revision;
        // Serialize mutation and publication under the same owner lock.
        self.publish(
            &authority,
            ClientScope::Composer { thread_id },
            ClientRevisions::new(
                DomainRevision::new(revision),
                PresentationRevision::new(revision),
                ContentRevision::ZERO,
                ScopedRevision::new(revision),
            ),
            next,
            vec![],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ClientIntent, ClientTransitionOutcome};

    fn open(core: &ClientCore, thread: &str) -> Arc<ComposerPublication> {
        core.dispatch(ClientIntent::Composer {
            intent: ComposerIntent::Open {
                thread_id: thread.into(),
                defaults: ComposerDomainState::default(),
            },
        });
        core.composer_snapshot(thread).unwrap()
    }
    fn edit(core: &ClientCore, input: &ComposerPublication, text: &str) -> ClientTransition {
        core.composer_intent(ComposerIntent::EditText {
            thread_id: input.thread_id.clone(),
            draft_id: input.draft_id,
            text: text.into(),
        })
    }
    fn begin(
        core: &ClientCore,
        input: &ComposerPublication,
        operation: ComposerOperationKind,
    ) -> ComposerOperationPlan {
        assert_eq!(
            core.composer_intent(ComposerIntent::BeginOperation {
                thread_id: input.thread_id.clone(),
                draft_id: input.draft_id,
                operation
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        core.composer_snapshot(input.thread_id())
            .unwrap()
            .operation()
            .unwrap()
            .plan
            .clone()
            .unwrap()
    }
    fn complete(
        core: &ClientCore,
        plan: &ComposerOperationPlan,
        completion: ComposerOperationCompletion,
    ) -> ClientTransitionOutcome {
        core.composer_intent(ComposerIntent::CompleteOperation {
            identity: plan.identity.clone(),
            completion,
        })
        .outcome()
    }
    fn attachment(path: &str) -> super::super::attachments::ComposerAttachment {
        super::super::attachments::ComposerAttachment {
            path: path.into(),
            file_name: path.into(),
            kind: super::super::attachments::ComposerAttachmentKind::File,
            upload_state: super::super::attachments::ComposerAttachmentUploadState::Local,
        }
    }
    #[test]
    fn activation_inherits_client_owned_selection_without_copying_another_draft_payload() {
        let core = ClientCore::new();
        let defaults = ComposerDomainState {
            selected_mode: pioneer_protocol::ThreadMode::Message,
            selected_provider: Some("provider".into()),
            selected_model: Some("model".into()),
            ..Default::default()
        };
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: defaults.clone(),
        });
        let a = core.composer_snapshot("a").unwrap();
        edit(&core, &a, "private draft A");
        let files = begin(&core, &a, ComposerOperationKind::PickFiles);
        complete(
            &core,
            &files,
            ComposerOperationCompletion::FilesSelected {
                attachments: vec![attachment("a.txt")],
            },
        );
        let a = core.composer_snapshot("a").unwrap();
        core.dispatch(ClientIntent::Composer {
            intent: ComposerIntent::Activate {
                thread_id: "b".into(),
            },
        });
        let b = core.composer_snapshot("b").unwrap();
        assert_eq!(b.domain().selected_provider, a.domain().selected_provider);
        assert_eq!(b.domain().selected_model, a.domain().selected_model);
        assert_eq!(
            b.domain().selected_mode,
            pioneer_protocol::ThreadMode::Message
        );
        assert!(b.draft().text.is_empty());
        assert!(b.domain().attachments.is_empty());
        assert_ne!(a.draft_id(), b.draft_id());
        assert_eq!(
            core.composer_intent(ComposerIntent::Activate {
                thread_id: "a".into()
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(&a, &core.composer_snapshot("a").unwrap()));
        core.composer_intent(ComposerIntent::ClearAll);
        core.composer_intent(ComposerIntent::Activate {
            thread_id: "c".into(),
        });
        assert_eq!(
            core.composer_snapshot("c").unwrap().domain(),
            &ComposerDomainState::default()
        );
    }
    #[test]
    fn picker_completion_is_fenced_by_draft_generation_and_cancellation() {
        let core = ClientCore::new();
        let input = open(&core, "a");
        let first = begin(&core, &input, ComposerOperationKind::PickFiles);
        assert_eq!(
            complete(&core, &first, ComposerOperationCompletion::Cancelled),
            ClientTransitionOutcome::Changed
        );
        let second = begin(&core, &input, ComposerOperationKind::PickFiles);
        assert_ne!(first.identity.generation, second.identity.generation);
        assert_eq!(
            complete(
                &core,
                &first,
                ComposerOperationCompletion::FilesSelected {
                    attachments: vec![attachment("late")]
                }
            ),
            ClientTransitionOutcome::Noop
        );
        let selected = ComposerOperationCompletion::FilesSelected {
            attachments: vec![
                attachment("target"),
                attachment("target"),
                attachment("other"),
            ],
        };
        assert_eq!(
            complete(&core, &second, selected.clone()),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            complete(&core, &second, selected),
            ClientTransitionOutcome::Noop
        );
        let current = core.composer_snapshot("a").unwrap();
        assert_eq!(
            current
                .domain()
                .attachments
                .iter()
                .map(|item| item.path.as_str())
                .collect::<Vec<_>>(),
            ["target", "other"]
        );
        assert!(
            current.operation().unwrap().plan.is_none(),
            "terminal operation must release the captured draft"
        );
    }
    #[test]
    fn a_user_edit_cancels_the_captured_operation_without_losing_the_new_text() {
        let core = ClientCore::new();
        let input = open(&core, "a");
        edit(&core, &input, "original");
        let plan = begin(&core, &input, ComposerOperationKind::Send);
        assert_eq!(plan.draft.text, "original");
        edit(&core, &input, "new draft text");
        assert_eq!(
            complete(&core, &plan, ComposerOperationCompletion::Sent),
            ClientTransitionOutcome::Noop
        );
        let current = core.composer_snapshot("a").unwrap();
        assert_eq!(current.draft().text, "new draft text");
        assert_eq!(
            current.operation().unwrap().status,
            ComposerOperationStatus::Cancelled
        );
        assert!(current.operation().unwrap().plan.is_none());
    }
    #[test]
    fn failure_preserves_draft_retry_allocates_generation_and_sent_replaces_identity() {
        let core = ClientCore::new();
        let input = open(&core, "a");
        edit(&core, &input, "draft text");
        core.composer_intent(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: input.draft_id,
            action: ComposerDomainAction::AddAttachment {
                attachment: attachment("file"),
            },
        });
        let first = begin(&core, &input, ComposerOperationKind::Send);
        assert_eq!(
            complete(
                &core,
                &first,
                ComposerOperationCompletion::Failed {
                    message: "upload failed".into()
                }
            ),
            ClientTransitionOutcome::Changed
        );
        let failed = core.composer_snapshot("a").unwrap();
        assert_eq!(failed.draft().text, "draft text");
        assert!(matches!(
            failed.domain().attachments[0].upload_state,
            super::super::attachments::ComposerAttachmentUploadState::Failed { .. }
        ));
        let retry = begin(&core, &failed, ComposerOperationKind::Send);
        assert_ne!(first.identity.generation, retry.identity.generation);
        assert_eq!(
            complete(&core, &first, ComposerOperationCompletion::Sent),
            ClientTransitionOutcome::Noop
        );
        assert_eq!(
            complete(&core, &retry, ComposerOperationCompletion::Sent),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            complete(&core, &retry, ComposerOperationCompletion::Sent),
            ClientTransitionOutcome::Noop
        );
        let sent = core.composer_snapshot("a").unwrap();
        assert_ne!(sent.draft_id(), input.draft_id());
        assert!(sent.draft().text.is_empty());
        assert!(sent.domain().attachments.is_empty());
        assert!(sent.operation().unwrap().plan.is_none());
    }
    #[test]
    fn voice_session_result_only_clears_the_matching_live_operation() {
        use pioneer_protocol::{
            GatewayNotification, VoiceSessionOutcome, VoiceSessionResultNotification,
        };
        let core = ClientCore::new();
        let a = open(&core, "a");
        let b = open(&core, "b");
        let first = begin(&core, &a, ComposerOperationKind::Voice);
        core.composer_intent(ComposerIntent::VoiceSessionStarted {
            identity: first.identity.clone(),
            session_id: "old-session".into(),
        });
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: a.draft_id(),
            text: "new draft".into(),
        });
        let updated = core.composer_snapshot("a").unwrap();
        let current = begin(&core, &updated, ComposerOperationKind::Voice);
        core.composer_intent(ComposerIntent::VoiceSessionStarted {
            identity: current.identity.clone(),
            session_id: "current-session".into(),
        });
        let before = core.composer_snapshot("a").unwrap();
        core.composer_intent(ComposerIntent::VoiceSessionStarted {
            identity: current.identity.clone(),
            session_id: "other-session".into(),
        });
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        let result = |session: &str| {
            GatewayNotification::VoiceSessionResult(VoiceSessionResultNotification {
                session_id: session.into(),
                outcome: VoiceSessionOutcome::TurnStarted,
                turn_id: None,
                error: None,
            })
        };
        core.observe_composer_voice_notification(&result("old-session"));
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        core.observe_composer_voice_notification(&result("current-session"));
        let sent = core.composer_snapshot("a").unwrap();
        assert_ne!(sent.draft_id(), a.draft_id());
        assert!(sent.draft().text.is_empty());
        assert!(Arc::ptr_eq(&b, &core.composer_snapshot("b").unwrap()));
        core.observe_composer_voice_notification(&result("current-session"));
        assert!(Arc::ptr_eq(&sent, &core.composer_snapshot("a").unwrap()));
    }

    #[test]
    fn voice_capture_does_not_mark_files_uploading_until_preparation_starts() {
        let core = ClientCore::new();
        let input = open(&core, "a");
        core.composer_intent(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            action: ComposerDomainAction::AddAttachment {
                attachment: super::super::attachments::ComposerAttachment {
                    path: "synthetic-file".into(),
                    file_name: "file".into(),
                    kind: super::super::attachments::ComposerAttachmentKind::File,
                    upload_state: super::super::attachments::ComposerAttachmentUploadState::Local,
                },
            },
        });
        let input = core.composer_snapshot("a").unwrap();
        let plan = begin(&core, &input, ComposerOperationKind::Voice);
        assert_eq!(
            core.composer_snapshot("a").unwrap().domain().attachments[0].upload_state,
            super::super::attachments::ComposerAttachmentUploadState::Local
        );
        core.composer_intent(ComposerIntent::UploadOperation {
            identity: plan.identity.clone(),
        });
        assert_eq!(
            core.composer_snapshot("a").unwrap().domain().attachments[0].upload_state,
            super::super::attachments::ComposerAttachmentUploadState::Uploading
        );
        complete(&core, &plan, ComposerOperationCompletion::Cancelled);
        let cancelled = core.composer_snapshot("a").unwrap();
        assert_eq!(
            complete(&core, &plan, ComposerOperationCompletion::Sent),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.composer_snapshot("a").unwrap()
        ));
    }

    #[test]
    fn voice_preparation_accepts_one_completion_and_cancel_rejects_late_send() {
        let core = ClientCore::new();
        let input = open(&core, "a");
        let plan = begin(&core, &input, ComposerOperationKind::Voice);
        let completion = ComposerOperationCompletion::Uploaded { artifacts: vec![] };
        assert_eq!(
            complete(&core, &plan, completion.clone()),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            complete(&core, &plan, completion),
            ClientTransitionOutcome::Noop
        );
        assert_eq!(
            complete(&core, &plan, ComposerOperationCompletion::Cancelled),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            complete(&core, &plan, ComposerOperationCompletion::Sent),
            ClientTransitionOutcome::Noop
        );
    }

    #[test]
    fn controlled_text_echo_does_not_publish_or_replace_snapshot() {
        let core = ClientCore::new();
        let input = open(&core, "a");
        assert_eq!(
            edit(&core, &input, "draft").outcome(),
            ClientTransitionOutcome::Changed
        );
        let input = core.composer_snapshot("a").unwrap();
        assert_eq!(
            edit(&core, &input, "draft").outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(&input, &core.composer_snapshot("a").unwrap()));
        assert_eq!(input.draft.text, "draft");
    }
    #[test]
    fn drafts_survive_switch_and_other_threads_do_not_publish() {
        let core = ClientCore::new();
        let a = open(&core, "a");
        edit(&core, &a, "A");
        let a = core.composer_snapshot("a").unwrap();
        let b = open(&core, "b");
        edit(&core, &b, "B");
        assert!(Arc::ptr_eq(&a, &open(&core, "a")));
        assert_eq!(core.composer_snapshot("b").unwrap().draft.text, "B");
        let delivered = core
            .snapshot(&ClientScope::Composer {
                thread_id: "a".into(),
            })
            .unwrap()
            .typed::<ComposerPublication>()
            .unwrap()
            .payload();
        assert!(Arc::ptr_eq(&a, &delivered));
    }
    #[test]
    fn stale_and_wrong_draft_edits_cannot_mutate_replacement() {
        let core = ClientCore::new();
        let a = open(&core, "a");
        let b = open(&core, "b");
        let clear = ComposerIntent::Clear {
            thread_id: "a".into(),
            draft_id: a.draft_id,
        };
        core.composer_intent(clear.clone());
        let replacement = core.composer_snapshot("a").unwrap();
        assert_ne!(a.draft_id, replacement.draft_id);
        assert_eq!(
            core.composer_intent(clear).outcome(),
            ClientTransitionOutcome::Noop
        );
        assert_eq!(
            edit(&core, &a, "late").outcome(),
            ClientTransitionOutcome::Noop
        );
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: b.draft_id,
            text: "wrong draft".into(),
        });
        assert!(Arc::ptr_eq(
            &replacement,
            &core.composer_snapshot("a").unwrap()
        ));
    }
    #[test]
    fn clear_all_retires_every_identity_and_clears_all_publications_atomically() {
        let core = ClientCore::new();
        let a = open(&core, "a");
        let b = open(&core, "b");
        edit(&core, &a, "secret A");
        edit(&core, &b, "secret B");
        core.clear_composer_drafts();
        for input in [a, b] {
            assert_eq!(
                edit(&core, &input, "late").outcome(),
                ClientTransitionOutcome::Noop
            );
            let current = core.composer_snapshot(input.thread_id()).unwrap();
            assert_eq!(current.draft, ComposerDomainDraft::default());
        }
        let a = core
            .snapshot(&ClientScope::Composer {
                thread_id: "a".into(),
            })
            .unwrap();
        let b = core
            .snapshot(&ClientScope::Composer {
                thread_id: "b".into(),
            })
            .unwrap();
        assert_eq!(a.snapshot().sequence(), b.snapshot().sequence());
    }
    #[test]
    fn shutdown_drops_drafts_and_rejects_late_edit_and_open() {
        let core = ClientCore::new();
        let input = open(&core, "a");
        core.shutdown();
        assert!(core.composer_snapshot("a").is_none());
        assert_eq!(
            edit(&core, &input, "late").outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert_eq!(
            core.composer_intent(ComposerIntent::Open {
                thread_id: "a".into(),
                defaults: Default::default()
            })
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
    }
}
