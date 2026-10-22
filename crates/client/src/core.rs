//! Process-local client mutation and publication contracts.
//!
//! The core is partitioned by [`ClientScope`]. It serializes mutations, owns
//! scope revisions and request/effect generations, and publishes immutable
//! snapshot handles. Shell adapters may retain those handles, but cannot mint
//! revisions or feed publications back as commands.

use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
};

use serde::{Deserialize, Serialize};

use crate::{notifications::effects::ClientEffect, runtime::ClientRuntime};

macro_rules! counter_type {
    ($name:ident) => {
        #[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            Deserialize,
            Eq,
            Hash,
            Ord,
            PartialEq,
            PartialOrd,
            Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            pub const ZERO: Self = Self(0);

            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

counter_type!(DomainRevision);
counter_type!(PresentationRevision);
counter_type!(ContentRevision);
counter_type!(ScopedRevision);
counter_type!(ClientGeneration);
counter_type!(ClientTransitionSequence);
counter_type!(ClientChangeSequence);

impl ClientTransitionSequence {
    fn advance(&mut self) {
        self.0 = self
            .0
            .checked_add(1)
            .expect("Client transition sequence exhausted");
    }
}

impl ClientChangeSequence {
    fn advance(&mut self) {
        self.0 = self
            .0
            .checked_add(1)
            .expect("Client change sequence exhausted");
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientRevisions {
    domain: DomainRevision,
    presentation: PresentationRevision,
    content: ContentRevision,
    scoped: ScopedRevision,
}

impl ClientRevisions {
    pub const fn new(
        domain: DomainRevision,
        presentation: PresentationRevision,
        content: ContentRevision,
        scoped: ScopedRevision,
    ) -> Self {
        Self {
            domain,
            presentation,
            content,
            scoped,
        }
    }

    pub const fn domain(self) -> DomainRevision {
        self.domain
    }

    pub const fn presentation(self) -> PresentationRevision {
        self.presentation
    }

    pub const fn content(self) -> ContentRevision {
        self.content
    }

    pub const fn scoped(self) -> ScopedRevision {
        self.scoped
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientScope {
    Session,
    Navigation,
    SidebarSummary {
        workspace_id: String,
        thread_id: String,
    },
    WorkspaceTree {
        workspace_id: Option<String>,
    },
    TaskInbox {
        workspace_id: String,
    },
    Task {
        task_id: Option<String>,
    },
    Thread {
        thread_id: String,
    },
    Timeline {
        thread_id: String,
    },
    Composer {
        thread_id: String,
    },
    ThreadMember {
        thread_id: String,
    },
    ThreadCapability {
        thread_id: String,
    },
    TurnCancellation {
        thread_id: String,
    },
    ComposerModelPicker {
        thread_id: String,
    },
    ComposerCatalog {
        thread_id: String,
    },
    MessageDeletion {
        thread_id: String,
    },
    MessageRevisions {
        thread_id: String,
    },
    ApprovalAction {
        thread_id: String,
        request_id: String,
    },
    TaskReview {
        thread_id: String,
        candidate_id: String,
    },
    PendingRequest {
        workspace_id: Option<String>,
        thread_id: Option<String>,
    },
    Artifact {
        thread_id: String,
    },
    Avatar {
        principal_id: String,
    },
    AdministrationOperation,
    AdministrationPage {
        page: crate::administration::pages::AdministrationPage,
    },
    Provider,
    ProviderOperation {
        workspace_id: String,
    },
    ProviderCollection {
        key: crate::providers::store::ProviderCollectionKey,
    },
    ProviderRuntime {
        workspace_id: String,
    },
    Administration {
        workspace_id: Option<String>,
    },
    Mcp {
        workspace_id: Option<String>,
    },
    SkillsDetails {
        workspace_id: String,
        skill_id: pioneer_protocol::SkillId,
    },
    McpAction {
        workspace_id: String,
        target: String,
    },
    SkillsAction {
        workspace_id: String,
        target: String,
    },
    SkillsUpload {
        workspace_id: String,
        operation_id: u64,
    },
    McpDetails {
        workspace_id: String,
        server_id: String,
    },
    Skills {
        workspace_id: Option<String>,
    },
    Settings,
    AuthSessions,
    DeviceActivation,
    Profile,
    SettingsModelPicker {
        picker_id: String,
    },
    SettingsPage {
        page: crate::settings::runtime::SettingsPage,
    },
    OnboardingInvitation,
    GatewaySetup,
    GatewayDestinations,
    AgentsDocumentContent {
        workspace_id: String,
        folder_id: Option<String>,
    },
    AgentsDocument {
        workspace_id: String,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientDemand {
    Suspended,
    Visible,
    Prefetch,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientIntent {
    SessionDemand {
        demand: crate::gateway::session_driver::SessionDemand,
    },
    SettingsModelPicker {
        intent: crate::settings::model_picker::SettingsModelPickerIntent,
    },
    Onboarding {
        intent: crate::gateway::onboarding_runtime::OnboardingIntent,
    },
    Settings {
        intent: crate::settings::runtime::SettingsIntent,
    },
    Profile {
        intent: crate::settings::profile::ProfileIntent,
    },
    AgentsDocument {
        intent: crate::agents_doc::runtime::AgentsDocumentIntent,
    },
    AdministrationPresentation {
        intent: crate::administration::operations::AdministrationPresentationIntent,
    },
    AdministrationCommand {
        command: crate::administration::operations::AdministrationCommand,
    },
    AdministrationPage {
        intent: crate::administration::pages::AdministrationPageIntent,
    },
    ProviderPresentation {
        intent: crate::providers::effects::ProviderPresentationIntent,
    },
    ProviderCommand {
        command: crate::providers::operations::ProviderCommand,
    },
    ProviderCollection {
        intent: crate::providers::store::ProviderCollectionIntent,
    },
    ProviderRuntime {
        intent: crate::providers::runtime::ProviderRuntimeIntent,
    },
    Artifact {
        intent: crate::artifacts::store::ArtifactIntent,
    },
    ThreadMember {
        intent: crate::threads::members::ThreadMemberIntent,
    },
    ThreadCapability {
        intent: crate::threads::capabilities::ThreadCapabilityIntent,
    },
    TurnCancellation {
        intent: crate::turns::cancellation::TurnCancellationIntent,
    },
    ComposerModelPicker {
        intent: crate::composer::model_picker::ComposerModelPickerIntent,
    },
    ComposerCatalog {
        intent: crate::composer::catalog::ComposerCatalogIntent,
    },
    MessageRevisions {
        intent: crate::threads::message_revisions::MessageRevisionIntent,
    },
    MessageDeletion {
        intent: crate::threads::message_deletion::MessageDeletionIntent,
    },
    ApprovalAction {
        intent: crate::cli_runtime::approval_actions::ApprovalActionIntent,
    },
    TaskReview {
        intent: crate::tasks::review_controller::TaskReviewIntent,
    },
    Composer {
        intent: crate::composer::store::ComposerIntent,
    },
    Workspace {
        intent: crate::workspaces::intents::WorkspaceIntent,
    },
    TaskNotification {
        intent: crate::tasks::notifications::TaskNotificationIntent,
    },
    Navigation {
        intent: crate::navigation::NavigationIntent,
        expected_revision: Option<u64>,
    },
    RefreshTimeline {
        thread_id: String,
    },
    SetTimelineExpansion {
        thread_id: String,
        row_ids: Vec<String>,
        collapsed_row_ids: Vec<String>,
    },
    Timeline {
        intent: crate::timeline::controller::TimelineIntent,
    },
    SetScopeDemand {
        scope: ClientScope,
        demand: ClientDemand,
        generation: ClientGeneration,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ClientOperationId(String);

impl ClientOperationId {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ClientPlannedEffect {
    Notification(ClientEffect),
    AdministrationPresentation(crate::administration::operations::AdministrationPresentationEffect),
    ProviderPresentation(crate::providers::effects::ProviderPresentationEffect),
    GatewaySessionStorage(crate::gateway::session_refresh::GatewaySessionStorageEffect),
    OnboardingPlatform(crate::gateway::onboarding_effects::OnboardingPlatformEffect),
}

impl From<ClientEffect> for ClientPlannedEffect {
    fn from(effect: ClientEffect) -> Self {
        Self::Notification(effect)
    }
}
impl From<crate::gateway::session_refresh::GatewaySessionStorageEffect> for ClientPlannedEffect {
    fn from(effect: crate::gateway::session_refresh::GatewaySessionStorageEffect) -> Self {
        Self::GatewaySessionStorage(effect)
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientEffectPlan {
    operation_id: ClientOperationId,
    generation: ClientGeneration,
    effect: ClientPlannedEffect,
}

impl ClientEffectPlan {
    pub fn new(
        operation_id: ClientOperationId,
        generation: ClientGeneration,
        effect: impl Into<ClientPlannedEffect>,
    ) -> Self {
        Self {
            operation_id,
            generation,
            effect: effect.into(),
        }
    }

    pub fn operation_id(&self) -> &ClientOperationId {
        &self.operation_id
    }

    pub const fn generation(&self) -> ClientGeneration {
        self.generation
    }

    pub const fn effect(&self) -> &ClientPlannedEffect {
        &self.effect
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientEffectResult {
    GatewayEnvironmentLoaded {
        environment: crate::gateway::onboarding_effects::OnboardingEnvironment,
    },
    LocalGatewayPrepared {
        prepared: crate::gateway::onboarding_effects::LocalGatewayPreparation,
    },
    LocalDeviceActivationCreated {
        activation: pioneer_protocol::AuthSecretString,
    },
    GatewaySessionEnvelopeLoaded {
        envelope: Option<crate::gateway::session_envelope::GatewaySessionEnvelope>,
    },
    Completed,
    Failed {
        code: String,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientEffectCompletion {
    operation_id: ClientOperationId,
    generation: ClientGeneration,
    result: ClientEffectResult,
}

impl ClientEffectCompletion {
    pub fn new(
        operation_id: ClientOperationId,
        generation: ClientGeneration,
        result: ClientEffectResult,
    ) -> Self {
        Self {
            operation_id,
            generation,
            result,
        }
    }

    pub fn operation_id(&self) -> &ClientOperationId {
        &self.operation_id
    }

    pub const fn generation(&self) -> ClientGeneration {
        self.generation
    }

    pub const fn result(&self) -> &ClientEffectResult {
        &self.result
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientEffectCancellation {
    operation_id: ClientOperationId,
    generation: ClientGeneration,
}

impl ClientEffectCancellation {
    pub fn new(operation_id: ClientOperationId, generation: ClientGeneration) -> Self {
        Self {
            operation_id,
            generation,
        }
    }

    pub fn operation_id(&self) -> &ClientOperationId {
        &self.operation_id
    }

    pub const fn generation(&self) -> ClientGeneration {
        self.generation
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientTransitionOutcome {
    Changed,
    Noop,
    Stale,
    Rejected,
}

struct SerializedClientPayload {
    value: std::sync::OnceLock<Arc<serde_json::Value>>,
    encode: Option<Box<dyn Fn() -> serde_json::Value + Send + Sync>>,
}
impl SerializedClientPayload {
    fn deferred<T: Serialize + Send + Sync + 'static>(payload: Arc<T>) -> Self {
        Self {
            value: std::sync::OnceLock::new(),
            encode: Some(Box::new(move || {
                serde_json::to_value(payload.as_ref())
                    .expect("Client publication payloads must be serializable")
            })),
        }
    }
    fn eager(value: serde_json::Value) -> Self {
        Self {
            value: std::sync::OnceLock::from(Arc::new(value)),
            encode: None,
        }
    }
    fn get(&self) -> Arc<serde_json::Value> {
        self.value
            .get_or_init(|| Arc::new(self.encode.as_ref().expect("deferred payload encoder")()))
            .clone()
    }
}

pub struct ClientSnapshot {
    scope: ClientScope,
    revisions: ClientRevisions,
    sequence: ClientChangeSequence,
    payload: Arc<dyn Any + Send + Sync>,
    serialized_payload: Arc<SerializedClientPayload>,
}

impl ClientSnapshot {
    fn from_draft(draft: ClientPublicationDraft, sequence: ClientChangeSequence) -> Arc<Self> {
        Arc::new(Self {
            scope: draft.scope,
            revisions: draft.revisions,
            sequence,
            payload: draft.payload,
            serialized_payload: draft.serialized_payload,
        })
    }

    pub fn scope(&self) -> &ClientScope {
        &self.scope
    }

    pub fn revisions(&self) -> ClientRevisions {
        self.revisions
    }

    pub const fn sequence(&self) -> ClientChangeSequence {
        self.sequence
    }

    pub fn serialized_payload(&self) -> Arc<serde_json::Value> {
        self.serialized_payload.get()
    }

    pub fn payload<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        Arc::clone(&self.payload).downcast().ok()
    }
}

#[derive(Clone)]
pub struct ClientPublicationReference(
    Arc<ClientSnapshot>,
    Option<Arc<crate::timeline::presentation::TimelineChangeSet>>,
);

impl ClientPublicationReference {
    /// Transaction delta carried by delivery, never retained in a scoped snapshot.
    pub fn timeline_change(&self) -> Option<Arc<crate::timeline::presentation::TimelineChangeSet>> {
        self.1.clone()
    }

    pub fn scope(&self) -> &ClientScope {
        self.0.scope()
    }

    pub fn revisions(&self) -> ClientRevisions {
        self.0.revisions()
    }

    pub fn snapshot(&self) -> Arc<ClientSnapshot> {
        Arc::clone(&self.0)
    }

    pub fn typed<T>(&self) -> Option<ScopedPublication<T>>
    where
        T: Any + Send + Sync,
    {
        let payload = self.0.payload::<T>()?;
        Some(ScopedPublication {
            snapshot: Arc::clone(&self.0),
            payload,
        })
    }
}

pub struct ScopedPublication<T> {
    snapshot: Arc<ClientSnapshot>,
    payload: Arc<T>,
}

impl<T> Clone for ScopedPublication<T> {
    fn clone(&self) -> Self {
        Self {
            snapshot: Arc::clone(&self.snapshot),
            payload: Arc::clone(&self.payload),
        }
    }
}

impl<T> ScopedPublication<T> {
    pub fn scope(&self) -> &ClientScope {
        self.snapshot.scope()
    }

    pub fn revisions(&self) -> ClientRevisions {
        self.snapshot.revisions()
    }

    pub fn payload(&self) -> Arc<T> {
        Arc::clone(&self.payload)
    }

    pub fn reference(&self) -> ClientPublicationReference {
        ClientPublicationReference(Arc::clone(&self.snapshot), None)
    }
}

#[derive(Clone)]
pub struct ClientChangeSet {
    sequence: ClientChangeSequence,
    predecessor: Option<ClientChangeSequence>,
    publications: Arc<[ClientPublicationReference]>,
    timeline_changes: Arc<[crate::timeline::presentation::TimelineChangeSet]>,
}

/// Catch-up metadata must not keep obsolete full histories alive. Current
/// snapshots and in-flight deliveries own payloads; an expired reference is a
/// delivery gap, handled by the same coherent resnapshot as journal overflow.
struct JournalChangeSet {
    sequence: ClientChangeSequence,
    predecessor: Option<ClientChangeSequence>,
    publications: Vec<JournalPublication>,
    timeline_changes: Arc<[crate::timeline::presentation::TimelineChangeSet]>,
}

struct JournalPublication {
    snapshot: Weak<ClientSnapshot>,
    timeline_change: Option<Arc<crate::timeline::presentation::TimelineChangeSet>>,
}

impl JournalChangeSet {
    fn new(change: &ClientChangeSet) -> Self {
        Self {
            sequence: change.sequence,
            predecessor: change.predecessor,
            publications: change
                .publications
                .iter()
                .map(|reference| JournalPublication {
                    snapshot: Arc::downgrade(&reference.0),
                    timeline_change: reference.1.clone(),
                })
                .collect(),
            timeline_changes: change.timeline_changes.clone(),
        }
    }

    fn is_obsolete(&self) -> bool {
        self.publications
            .iter()
            .any(|publication| publication.snapshot.strong_count() == 0)
    }

    fn upgrade(&self) -> Option<Arc<ClientChangeSet>> {
        let publications = self
            .publications
            .iter()
            .map(|publication| {
                Some(ClientPublicationReference(
                    publication.snapshot.upgrade()?,
                    publication.timeline_change.clone(),
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Arc::new(ClientChangeSet {
            sequence: self.sequence,
            predecessor: self.predecessor,
            publications: Arc::from(publications),
            timeline_changes: self.timeline_changes.clone(),
        }))
    }
}

impl ClientChangeSet {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            sequence: ClientChangeSequence::ZERO,
            predecessor: None,
            publications: Arc::from([]),
            timeline_changes: Arc::from([]),
        })
    }

    pub const fn sequence(&self) -> ClientChangeSequence {
        self.sequence
    }

    pub const fn predecessor(&self) -> Option<ClientChangeSequence> {
        self.predecessor
    }

    pub fn timeline_changes(&self) -> &[crate::timeline::presentation::TimelineChangeSet] {
        &self.timeline_changes
    }

    pub fn publications(&self) -> &[ClientPublicationReference] {
        &self.publications
    }
}

#[derive(Clone)]
pub struct ClientTransition {
    sequence: ClientTransitionSequence,
    outcome: ClientTransitionOutcome,
    changes: Arc<ClientChangeSet>,
    effects: Arc<[ClientEffectPlan]>,
}

impl ClientTransition {
    pub const fn sequence(&self) -> ClientTransitionSequence {
        self.sequence
    }

    pub const fn outcome(&self) -> ClientTransitionOutcome {
        self.outcome
    }

    pub fn changes(&self) -> Arc<ClientChangeSet> {
        Arc::clone(&self.changes)
    }

    pub fn effects(&self) -> &[ClientEffectPlan] {
        &self.effects
    }
}

#[derive(Clone)]
pub enum ClientSubscriptionEvent {
    Publication {
        sequence: ClientChangeSequence,
        predecessor: Option<ClientChangeSequence>,
        publication: ClientPublicationReference,
    },
    ResnapshotRequired {
        scope: ClientScope,
        latest_sequence: ClientChangeSequence,
    },
}

struct SubscriptionQueue {
    scope: ClientScope,
    capacity: NonZeroUsize,
    observer: pioneer_observability::queues::QueueObserver,
    events: Mutex<VecDeque<pioneer_observability::work::WorkItem<ClientSubscriptionEvent>>>,
    latest_sequence: Mutex<Option<ClientChangeSequence>>,
    closed: std::sync::atomic::AtomicBool,
}

impl SubscriptionQueue {
    fn push(&self, sequence: ClientChangeSequence, publication: ClientPublicationReference) {
        use pioneer_observability::{
            queues::{QueueBytes, QueueOutcome},
            work::{Boundary, ExecutorRole, WorkItem, WorkKind},
        };
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            self.observer.outcome(QueueOutcome::Rejected, 1);
            return;
        }
        let predecessor = self
            .latest_sequence
            .lock()
            .expect("client subscriber sequence poisoned")
            .replace(sequence);
        let mut events = self
            .events
            .lock()
            .expect("client subscriber queue poisoned");
        if events.len() >= self.capacity.get() {
            self.observer
                .outcome(QueueOutcome::Dropped, events.len() as u64);
            events.clear();
            self.observer.current(0, QueueBytes::Unsupported);
            events.push_back(
                WorkItem::new(
                    ClientSubscriptionEvent::ResnapshotRequired {
                        scope: self.scope.clone(),
                        latest_sequence: sequence,
                    },
                    Boundary::ClientPublication,
                    WorkKind::ClientPublication,
                    ExecutorRole::ClientController,
                )
                .observed(self.observer.clone(), QueueBytes::Unsupported),
            );
            self.observer.outcome(QueueOutcome::Resnapshot, 1);
            self.observer.native_item_head(events.len(), events.front());
            return;
        }
        if let Some(item) = events.back_mut() {
            if let ClientSubscriptionEvent::ResnapshotRequired {
                latest_sequence, ..
            } = &mut item.value
            {
                *latest_sequence = sequence;
                self.observer.outcome(QueueOutcome::Coalesced, 1);
                return;
            }
        }
        events.push_back(
            WorkItem::new(
                ClientSubscriptionEvent::Publication {
                    sequence,
                    predecessor,
                    publication,
                },
                Boundary::ClientPublication,
                WorkKind::ClientPublication,
                ExecutorRole::ClientController,
            )
            .observed(self.observer.clone(), QueueBytes::Unsupported),
        );
        self.observer.native_item_head(events.len(), events.front());
    }
}

pub struct ClientSubscription {
    pending_work: Mutex<pioneer_observability::work::WorkCarrier>,
    id: u64,
    queue: Arc<SubscriptionQueue>,
    core: Weak<ClientCore>,
}

impl ClientSubscription {
    pub fn scope(&self) -> &ClientScope {
        &self.queue.scope
    }

    /// Local causal metadata for the last delivery, consumed by Desktop's
    /// binding router. It never enters the wire/FFI payload schema.
    pub fn pending_work(&self) -> pioneer_observability::work::WorkCarrier {
        self.pending_work
            .lock()
            .expect("subscription work poisoned")
            .clone()
    }
    pub fn try_next(&self) -> Option<ClientSubscriptionEvent> {
        let (event, work, observer) = {
            let mut events = self
                .queue
                .events
                .lock()
                .expect("client subscriber queue poisoned");
            let item = events.pop_front()?;
            let observer = item.queue.as_ref().map(|(queue, _)| queue.clone());
            let received = item.received(
                pioneer_observability::work::Boundary::ClientPublication,
                pioneer_observability::work::WorkKind::ClientPublication,
                pioneer_observability::work::ExecutorRole::ClientController,
            );
            self.queue
                .observer
                .native_item_head(events.len(), events.front());
            (received.0, received.1, observer)
        };
        *self
            .pending_work
            .lock()
            .expect("subscription work poisoned") = work.complete_handoff();
        if let Some(observer) = observer {
            observer.outcome(pioneer_observability::queues::QueueOutcome::Processed, 1);
        }
        Some(event)
    }
}

impl Drop for ClientSubscription {
    fn drop(&mut self) {
        self.queue
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(core) = self.core.upgrade() {
            core.subscribers
                .lock()
                .expect("client subscriber registry poisoned")
                .remove(&self.id);
            core.thread_subscription_changed(&self.queue.scope, false);
            core.composer_subscription_changed(&self.queue.scope, false);
            core.avatar_subscription_changed(&self.queue.scope, false);
            core.task_inbox_subscription_changed(&self.queue.scope, false);
            core.task_review_subscription_changed(&self.queue.scope, false);
            core.approval_action_subscription_changed(&self.queue.scope, false);
            core.message_revision_subscription_changed(&self.queue.scope, false);
            core.message_deletion_subscription_changed(&self.queue.scope, false);
            core.composer_catalog_subscription_changed(&self.queue.scope, false);
            core.composer_model_picker_subscription_changed(&self.queue.scope, false);
            core.settings_model_picker_subscription_changed(&self.queue.scope, false);
            core.turn_cancellation_subscription_changed(&self.queue.scope, false);
            core.thread_capability_subscription_changed(&self.queue.scope, false);
            core.thread_member_subscription_changed(&self.queue.scope, false);
            core.artifact_subscription_changed(&self.queue.scope, false);
            core.workspace_subscription_changed(&self.queue.scope, false);
        }
        let mut events = self
            .queue
            .events
            .lock()
            .expect("client subscriber queue poisoned");
        self.queue.observer.outcome(
            pioneer_observability::queues::QueueOutcome::Dropped,
            events.len() as u64,
        );
        self.queue
            .observer
            .current(0, pioneer_observability::queues::QueueBytes::Unsupported);
        events.clear();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationState {
    Pending(ClientGeneration),
    Completed(ClientGeneration),
    Cancelled(ClientGeneration),
}

#[derive(Clone, Copy, Debug)]
struct DemandState {
    demand: ClientDemand,
    generation: ClientGeneration,
}

struct PendingPlatformEffect {
    published_at: std::time::Instant,
    plan: ClientEffectPlan,
    reply: std::sync::mpsc::Sender<ClientEffectResult>,
}

#[derive(Default)]
struct ClientPartitions {
    next_platform_effect: u64,
    completed_platform_effects: VecDeque<ClientOperationId>,
    platform_effects: HashMap<ClientOperationId, PendingPlatformEffect>,
    transition_sequence: ClientTransitionSequence,
    change_sequence: ClientChangeSequence,
    latest_change_sequence: Option<ClientChangeSequence>,
    publications: HashMap<ClientScope, Arc<ClientSnapshot>>,
    demands: HashMap<ClientScope, DemandState>,
    operations: HashMap<ClientOperationId, OperationState>,
    journal_observer: Option<pioneer_observability::queues::QueueObserver>,
    change_journal: VecDeque<JournalChangeSet>,
}

impl ClientPartitions {
    fn journal_observer(&mut self) -> pioneer_observability::queues::QueueObserver {
        self.journal_observer.get_or_insert_with(||pioneer_observability::queues::QueueObserver::new(pioneer_observability::queues::QueueKind::ClientPublications,Some(128),"retained publication journal; bytes unsupported; reads are replay, not new input or dequeue")).clone()
    }

    fn prune_change_journal(&mut self) {
        // Deltas in an unusable prefix are obsolete too. Check on both commit
        // and polling, so draining the final delivery also releases old rows
        // when the application becomes idle.
        if let Some(index) = self
            .change_journal
            .iter()
            .rposition(JournalChangeSet::is_obsolete)
        {
            self.journal_observer().outcome(
                pioneer_observability::queues::QueueOutcome::Stale,
                (index + 1) as u64,
            );
            self.change_journal.drain(..=index);
            self.journal_observer().current(
                self.change_journal.len(),
                pioneer_observability::queues::QueueBytes::Unsupported,
            );
        }
    }
}

/// Ordered process delivery. A resnapshot contains one coherent set of the
/// currently retained publications after the consumer fell behind the queue.
pub struct ClientPublicationBatch {
    pub closed: bool,
    pub effects: Vec<ClientEffectPlan>,
    pub sequence: ClientChangeSequence,
    pub resnapshot: bool,
    pub changes: Vec<Arc<ClientChangeSet>>,
}

/// The one process-local mutable owner for newly shared client state.
pub struct ClientCore {
    pub(crate) session_driver: Mutex<crate::gateway::session_driver::SessionDriver>,
    pub(crate) invitation_commits: Mutex<crate::gateway::invitation_commits::InvitationCommits>,
    pub(crate) device_activation:
        Mutex<crate::settings::device_activation::DeviceActivationController>,
    pub(crate) onboarding: Mutex<crate::gateway::onboarding_runtime::OnboardingRuntime>,
    pub(crate) settings_model_pickers:
        Mutex<crate::settings::model_picker::SettingsModelPickerController>,
    pub(crate) settings_runtime: Mutex<crate::settings::runtime::SettingsRuntime>,
    pub(crate) agents_documents: Mutex<crate::agents_doc::runtime::DocumentRuntime>,
    pub(crate) skills_controller: Mutex<crate::skills::operations::SkillsController>,
    pub(crate) skills_store: Mutex<crate::skills::store::SkillsStore>,
    pub(crate) mcp_controller: Mutex<crate::mcp::operations::McpController>,
    pub(crate) mcp_oauth: Mutex<crate::mcp::oauth::OAuthController>,
    pub(crate) mcp_store: Mutex<crate::mcp::store::McpStore>,
    pub(crate) administration_operations:
        Mutex<crate::administration::operations::AdministrationOperationController>,
    pub(crate) administration_store: Mutex<crate::administration::pages::AdministrationStore>,
    pub(crate) provider_controller: Mutex<crate::providers::operations::ProviderController>,
    pub(crate) provider_store: Mutex<crate::providers::store::ProviderStore>,
    pub(crate) provider_runtimes: Mutex<crate::providers::runtime::ProviderRuntimeController>,
    pub(crate) artifact_store: Mutex<crate::artifacts::store::ArtifactStore>,
    pub(crate) artifact_downloads: Mutex<crate::artifacts::operations::ArtifactDownloadController>,
    pub(crate) thread_members: Mutex<crate::threads::members::ThreadMemberController>,
    pub(crate) thread_capabilities: Mutex<crate::threads::capabilities::ThreadCapabilityController>,
    pub(crate) composer_store: Mutex<crate::composer::store::ComposerStore>,
    pub(crate) composer_voice_cleanup:
        Mutex<crate::composer::voice::ComposerVoiceCleanupController>,
    pub(crate) composer_voice_readiness:
        Mutex<crate::composer::voice_readiness::ComposerVoiceReadinessController>,
    pub(crate) composer_models:
        Mutex<crate::composer::model_display::ComposerModelDisplayController>,
    pub(crate) composer_steers: Mutex<crate::composer::steer::ComposerSteerController>,
    pub(crate) composer_edits: Mutex<crate::composer::message_edit::ComposerMessageEditController>,
    pub(crate) composer_sends: Mutex<crate::composer::workflow::ComposerSendController>,
    pub(crate) turn_cancellations: Mutex<crate::turns::cancellation::TurnCancellationController>,
    pub(crate) composer_model_picker_requests:
        Mutex<crate::composer::model_picker::ComposerModelPickerController>,
    pub(crate) composer_catalog_requests:
        Mutex<crate::composer::catalog::ComposerCatalogController>,
    pub(crate) message_revisions:
        Mutex<crate::threads::message_revisions::MessageRevisionController>,
    pub(crate) message_deletions:
        Mutex<crate::threads::message_deletion::MessageDeletionController>,
    pub(crate) approval_actions:
        Mutex<crate::cli_runtime::approval_actions::ApprovalActionController>,
    pub(crate) task_reviews: Mutex<crate::tasks::review_controller::TaskReviewActionController>,
    pub(crate) avatar_store: Mutex<crate::avatars::AvatarStore>,
    pub(crate) workspace_catalog: Mutex<crate::workspaces::catalog::WorkspaceCatalogStore>,
    pub(crate) workspace_controller: Mutex<crate::workspaces::controller::WorkspaceController>,
    pub(crate) task_notifications: Mutex<crate::tasks::notifications::TaskNotificationController>,
    transport_runtime: ClientRuntime,
    pub(crate) thread_request_sender:
        Mutex<Option<crate::threads::registry::ThreadControllerSender>>,
    thread_request_task: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(crate) presentation_sender: Mutex<
        Option<
            std::sync::mpsc::SyncSender<(
                pioneer_observability::work::WorkCarrier,
                pioneer_observability::queues::QueueObserver,
            )>,
        >,
    >,
    pub(crate) presentation_observer: pioneer_observability::queues::QueueObserver,
    presentation_task: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(crate) thread_registry: Mutex<crate::threads::registry::ThreadRegistry>,
    pub(crate) timeline_started: std::time::Instant,
    pub(crate) identity_authorization:
        Mutex<crate::gateway::identity_authorization::IdentityAuthorizationStore>,
    pub(crate) capability_read_gate: Mutex<()>,
    pub(crate) session_refresh_slots: Mutex<HashMap<String, Weak<Mutex<bool>>>>,
    pub(crate) gateway_session: Mutex<crate::gateway::session_controller::GatewaySessionController>,
    pub(crate) session_transport: Mutex<Option<String>>,
    pub(crate) gateway_transport_leases:
        Mutex<crate::gateway::session_controller::GatewayTransportLeases>,
    pub(crate) gateway_transport_ready: std::sync::Condvar,
    gateway_task: Mutex<Option<std::thread::JoinHandle<()>>>,
    stopped: std::sync::atomic::AtomicBool,
    partitions: Mutex<ClientPartitions>,
    subscribers: Mutex<HashMap<u64, Weak<SubscriptionQueue>>>,
    next_subscription_id: Mutex<u64>,
    publication_signal: tokio::sync::watch::Sender<ClientChangeSequence>,
    publication_ready: std::sync::Condvar,
}

impl Drop for ClientCore {
    fn drop(&mut self) {
        self.shutdown();
        if let Ok(task) = self.presentation_task.get_mut() {
            if let Some(task) = task.take() {
                if task.thread().id() != std::thread::current().id() {
                    let _ = task.join();
                }
            }
        }
        if let Ok(task) = self.thread_request_task.get_mut() {
            if let Some(task) = task.take() {
                if task.thread().id() != std::thread::current().id() {
                    let _ = task.join();
                }
            }
        }
        if let Ok(task) = self.gateway_task.get_mut() {
            if let Some(task) = task.take() {
                if task.thread().id() != std::thread::current().id() {
                    let _ = task.join();
                }
            }
        }
    }
}

/// Capability-internal proof that a publication originated in Client code.
/// Shell crates can name this type but cannot construct it.
pub struct ClientMutationAuthority {
    pub(crate) _private: (),
}

type PayloadEquality = fn(&(dyn Any + Send + Sync), &(dyn Any + Send + Sync)) -> bool;

pub struct ClientPublicationDraft {
    timeline_change: Option<crate::timeline::presentation::TimelineChangeSet>,
    scope: ClientScope,
    revisions: ClientRevisions,
    payload: Arc<dyn Any + Send + Sync>,
    serialized_payload: Arc<SerializedClientPayload>,
    payload_eq: Option<PayloadEquality>,
}

impl ClientPublicationDraft {
    pub(crate) fn with_timeline_change(
        mut self,
        change: crate::timeline::presentation::TimelineChangeSet,
    ) -> Self {
        self.timeline_change = Some(change);
        self
    }
}

impl ClientMutationAuthority {
    /// Synthetic publication capability for independent boundary test harnesses.
    /// This constructor is absent from ordinary application builds.
    #[cfg(feature = "test-support")]
    pub fn for_test() -> Self {
        Self { _private: () }
    }

    /// Installs a synthetic identity through the same fenced completion as auth/me.
    #[cfg(feature = "test-support")]
    pub fn accept_identity_for_test(
        &self,
        core: &ClientCore,
        auth: pioneer_protocol::AuthMeResponse,
    ) -> anyhow::Result<pioneer_protocol::AuthMeResponse> {
        let (generation, connection) = core.current_auth_ticket();
        core.finish_current_auth(generation, connection, auth)
    }

    pub(crate) fn timeline_publication(
        &self,
        snapshot: Arc<crate::timeline::presentation::TimelineSnapshot>,
    ) -> ClientPublicationDraft {
        let payload = snapshot.clone();
        ClientPublicationDraft {
            payload_eq: None,
            timeline_change: None,
            scope: ClientScope::Timeline {
                thread_id: snapshot.thread_id().to_owned(),
            },
            revisions: crate::threads::registry::revisions(snapshot.revision()),
            payload: snapshot,
            serialized_payload: Arc::new(SerializedClientPayload {
                value: std::sync::OnceLock::new(),
                encode: Some(Box::new(move || {
                    serde_json::to_value(payload.as_ref()).expect("timeline snapshot serializes")
                })),
            }),
        }
    }

    pub fn publication<T>(
        &self,
        scope: ClientScope,
        revisions: ClientRevisions,
        payload: Arc<T>,
    ) -> ClientPublicationDraft
    where
        T: Any + Send + Sync + Serialize,
    {
        let serialized_payload = serde_json::to_value(payload.as_ref())
            .expect("Client publication payloads must be serializable");
        ClientPublicationDraft {
            payload_eq: None,
            timeline_change: None,
            scope,
            revisions,
            payload,
            serialized_payload: Arc::new(SerializedClientPayload::eager(serialized_payload)),
        }
    }

    /// Typed sources can compare their contents without allocating JSON copies.
    /// Encode only when a boundary consumer actually requests serialized data.
    pub(crate) fn typed_publication<T>(
        &self,
        scope: ClientScope,
        revisions: ClientRevisions,
        payload: Arc<T>,
    ) -> ClientPublicationDraft
    where
        T: Any + Send + Sync + Serialize + PartialEq,
    {
        ClientPublicationDraft {
            timeline_change: None,
            scope,
            revisions,
            serialized_payload: Arc::new(SerializedClientPayload::deferred(payload.clone())),
            payload,
            payload_eq: Some(|left, right| {
                left.downcast_ref::<T>()
                    .zip(right.downcast_ref::<T>())
                    .is_some_and(|(left, right)| left == right)
            }),
        }
    }
}

impl Default for ClientCore {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientCore {
    pub fn new() -> Self {
        Self {
            skills_controller: Mutex::new(crate::skills::operations::SkillsController::default()),
            agents_documents: Mutex::new(Default::default()),
            onboarding: Mutex::new(Default::default()),
            settings_runtime: Mutex::new(Default::default()),
            settings_model_pickers: Mutex::new(Default::default()),
            invitation_commits: Mutex::default(),
            device_activation: Mutex::new(Default::default()),
            skills_store: Mutex::new(crate::skills::store::SkillsStore::default()),
            mcp_controller: Mutex::new(crate::mcp::operations::McpController::default()),
            mcp_oauth: Mutex::new(crate::mcp::oauth::OAuthController::default()),
            mcp_store: Mutex::default(),
            administration_operations: Mutex::default(),
            administration_store: Mutex::default(),
            provider_controller: Mutex::default(),
            provider_store: Mutex::default(),
            provider_runtimes: Mutex::default(),
            artifact_store: Mutex::default(),
            artifact_downloads: Mutex::default(),
            thread_members: Mutex::default(),
            thread_capabilities: Mutex::default(),
            composer_store: Mutex::default(),
            composer_voice_cleanup: Mutex::default(),
            composer_voice_readiness: Mutex::default(),
            composer_models: Mutex::default(),
            composer_steers: Mutex::default(),
            composer_edits: Mutex::default(),
            composer_sends: Mutex::default(),
            message_revisions: Mutex::default(),
            message_deletions: Mutex::default(),
            turn_cancellations: Mutex::default(),
            composer_model_picker_requests: Mutex::default(),
            composer_catalog_requests: Mutex::default(),
            approval_actions: Mutex::default(),
            task_reviews: Mutex::default(),
            avatar_store: Mutex::default(),
            workspace_catalog: Mutex::default(),
            workspace_controller: Mutex::default(),
            task_notifications: Mutex::default(),
            transport_runtime: ClientRuntime::new(),
            thread_registry: Mutex::default(),
            timeline_started: std::time::Instant::now(),
            thread_request_sender: Mutex::default(),
            thread_request_task: Mutex::default(),
            presentation_sender: Mutex::default(),
            presentation_observer: pioneer_observability::queues::QueueObserver::new(
                pioneer_observability::queues::QueueKind::ClientController,
                Some(1),
                "coalesced wake tokens; zero payload bytes; processed=input handoff to coalescing, materialization cycle separate",
            ),
            presentation_task: Mutex::default(),
            identity_authorization: Mutex::default(),
            capability_read_gate: Mutex::default(),
            session_driver: Mutex::default(),
            session_refresh_slots: Mutex::default(),
            session_transport: Mutex::default(),
            gateway_transport_leases: Mutex::default(),
            gateway_transport_ready: std::sync::Condvar::new(),
            gateway_session: Mutex::default(),
            gateway_task: Mutex::default(),
            stopped: std::sync::atomic::AtomicBool::new(false),
            partitions: Mutex::new(ClientPartitions::default()),
            subscribers: Mutex::new(HashMap::new()),
            next_subscription_id: Mutex::new(0),
            publication_signal: tokio::sync::watch::channel(ClientChangeSequence::ZERO).0,
            publication_ready: std::sync::Condvar::new(),
        }
    }

    pub fn reduce_gateway_session_lifecycle(
        &self,
        endpoint_id: &str,
        event: crate::gateway::session_lifecycle::SessionLifecycleEvent,
    ) -> crate::gateway::session_controller::GatewaySessionTransition {
        self.reduce_gateway_session_lifecycle_with_identity_policy(endpoint_id, event, true)
    }

    pub(crate) fn reduce_gateway_session_lifecycle_with_identity_policy(
        &self,
        endpoint_id: &str,
        event: crate::gateway::session_lifecycle::SessionLifecycleEvent,
        invalidate_suspended_identity: bool,
    ) -> crate::gateway::session_controller::GatewaySessionTransition {
        self.reduce_gateway_session_lifecycle_with_diagnostic(
            endpoint_id,
            event,
            invalidate_suspended_identity,
            None,
        )
    }

    pub(crate) fn reduce_gateway_session_lifecycle_with_diagnostic(
        &self,
        endpoint_id: &str,
        event: crate::gateway::session_lifecycle::SessionLifecycleEvent,
        invalidate_suspended_identity: bool,
        terminal_outcome: Option<
            &mut Option<crate::gateway::session_controller::SessionDiagnosticTerminalOutcome>,
        >,
    ) -> crate::gateway::session_controller::GatewaySessionTransition {
        if self.is_stopped() {
            return crate::gateway::session_controller::GatewaySessionTransition::stopped();
        }
        if matches!(
            &event,
            crate::gateway::session_lifecycle::SessionLifecycleEvent::AuthFailed { .. }
                | crate::gateway::session_lifecycle::SessionLifecycleEvent::NoStoredSession
        ) || invalidate_suspended_identity
            && matches!(
                &event,
                crate::gateway::session_lifecycle::SessionLifecycleEvent::Suspend
            )
        {
            self.invalidate_session_authorization(endpoint_id);
        }
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() {
            return crate::gateway::session_controller::GatewaySessionTransition::stopped();
        }
        let before = owner.publication();
        let diagnostic_before = terminal_outcome
            .as_ref()
            .map(|_| owner.diagnostic_identity(endpoint_id));
        let result = owner.reduce(endpoint_id, event);
        if let (Some(receipt), Some(diagnostic_before)) = (terminal_outcome, diagnostic_before) {
            crate::gateway::session_controller::SessionDiagnosticTerminalOutcome::record(
                receipt,
                diagnostic_before,
                owner.diagnostic_identity(endpoint_id),
            );
        }
        if before == owner.publication() {
            return result;
        }
        self.publish_gateway_session(&owner);
        result
    }

    pub fn update_startup_stage(
        &self,
        stage: crate::gateway::session_controller::StartupStage,
        state: crate::gateway::session_controller::StartupStageState,
    ) -> ClientTransitionOutcome {
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() {
            return ClientTransitionOutcome::Rejected;
        }
        let outcome = owner.update_startup(stage, state);
        if outcome == ClientTransitionOutcome::Changed {
            self.publish_gateway_session(&owner);
        }
        outcome
    }

    pub fn project_gateway_status(
        &self,
        input: crate::state::reducers::GatewayStatusInput,
    ) -> crate::state::reducers::GatewayStatusProjection {
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() {
            return owner
                .publication()
                .status
                .unwrap_or_else(|| crate::state::reducers::project_gateway_status(input));
        }
        let before = owner.publication();
        let result = owner.project_status(input);
        if before != owner.publication() {
            self.publish_gateway_session(&owner);
        }
        result
    }

    fn observe_gateway_transport(&self, event: &crate::transport::ws::GatewayWsEvent) {
        if self.is_stopped()
            || !self
                .transport_runtime
                .ws_command_sender()
                .gateway_state_event_is_current(event)
        {
            return;
        }
        if matches!(
            event,
            crate::transport::ws::GatewayWsEvent::Disconnected { .. }
                | crate::transport::ws::GatewayWsEvent::Reconnecting { .. }
                | crate::transport::ws::GatewayWsEvent::ConnectFailed { .. }
        ) {
            self.fence_mcp_oauth();
        }
        self.observe_session_connection(event);
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() {
            return;
        }
        let before = owner.publication();
        owner.observe_transport(event);
        if before != owner.publication() {
            self.publish_gateway_session(&owner);
        }
    }

    fn observe_gateway_authorization(&self, event: &crate::transport::ws::GatewayWsEvent) {
        use crate::transport::ws::GatewayWsEvent;
        use pioneer_protocol::GatewayNotification;
        if self.is_stopped()
            || !self
                .transport_runtime
                .ws_command_sender()
                .gateway_state_event_is_current(event)
        {
            return;
        }
        self.observe_authorization_connection(event);
        match event {
            GatewayWsEvent::Notification {
                notification: GatewayNotification::AccessChanged(change),
                ..
            } => self.observe_access_change(change),
            GatewayWsEvent::Notification {
                notification: GatewayNotification::AuthorizationProjectionChanged(change),
                ..
            } => self.observe_policy_change(change),
            _ => {}
        }
    }

    pub(crate) fn publish_gateway_session(
        &self,
        owner: &crate::gateway::session_controller::GatewaySessionController,
    ) {
        let publication = owner.publication();
        if self
            .snapshot(&ClientScope::Session)
            .and_then(|reference| {
                reference.typed::<crate::gateway::session_controller::GatewaySessionPublication>()
            })
            .is_some_and(|current| *current.payload() == publication)
        {
            return;
        }
        let revision = self.snapshot(&ClientScope::Session).map_or(1, |current| {
            current
                .revisions()
                .scoped()
                .get()
                .checked_add(1)
                .expect("session revision exhausted")
        });
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::Session,
            ClientRevisions::new(
                DomainRevision::new(revision),
                PresentationRevision::new(revision),
                ContentRevision::ZERO,
                ScopedRevision::new(revision),
            ),
            Arc::new(publication),
            Vec::new(),
        );
    }

    pub fn gateway_session(
        &self,
    ) -> Arc<crate::gateway::session_controller::GatewaySessionPublication> {
        self.snapshot(&ClientScope::Session)
            .and_then(|reference| {
                reference.typed::<crate::gateway::session_controller::GatewaySessionPublication>()
            })
            .map(|publication| publication.payload())
            .unwrap_or_default()
    }

    pub fn watch_publications(&self) -> tokio::sync::watch::Receiver<ClientChangeSequence> {
        self.publication_signal.subscribe()
    }

    /// Process-local enqueue time for native shell queue diagnostics. Not part of the wire plan.
    /// Repeated publications retain the original instant; retired plans return None.
    pub fn native_effect_published_at(
        &self,
        plan: &ClientEffectPlan,
    ) -> Option<std::time::Instant> {
        self.partitions
            .lock()
            .expect("client partitions poisoned")
            .platform_effects
            .get(plan.operation_id())
            .filter(|pending| pending.plan.generation() == plan.generation())
            .map(|pending| pending.published_at)
    }

    pub fn wait_for_publications(&self, after: ClientChangeSequence) -> ClientPublicationBatch {
        let partitions = self.partitions.lock().expect("client partitions poisoned");
        let mut partitions = if !self.is_stopped() && partitions.change_sequence <= after {
            self.publication_ready
                .wait_timeout(partitions, std::time::Duration::from_millis(250))
                .expect("client publication wait poisoned")
                .0
        } else {
            partitions
        };
        partitions.prune_change_journal();
        let mut effects: Vec<_> = partitions
            .platform_effects
            .values()
            .map(|pending| pending.plan.clone())
            .collect();
        effects.sort_by_key(|plan| plan.generation().get());
        let sequence = partitions.change_sequence;
        if sequence <= after {
            return ClientPublicationBatch {
                closed: self.is_stopped(),
                effects,
                sequence,
                resnapshot: false,
                changes: vec![],
            };
        }
        let gap = partitions
            .change_journal
            .front()
            .is_none_or(|first| first.predecessor.unwrap_or_default() > after);
        let changes = if gap {
            None
        } else {
            partitions
                .change_journal
                .iter()
                .filter(|change| change.sequence > after)
                .map(JournalChangeSet::upgrade)
                .collect::<Option<Vec<_>>>()
        };
        let gap = changes.is_none();
        let changes = changes.unwrap_or_else(|| {
            vec![Arc::new(ClientChangeSet {
                timeline_changes: Arc::from([]),
                sequence,
                predecessor: Some(after),
                publications: Arc::from(
                    partitions
                        .publications
                        .values()
                        .map(|snapshot| ClientPublicationReference(snapshot.clone(), None))
                        .collect::<Vec<_>>(),
                ),
            })]
        });
        partitions.journal_observer().outcome(
            pioneer_observability::queues::QueueOutcome::Replay,
            changes.len() as u64,
        );
        if gap {
            partitions
                .journal_observer()
                .outcome(pioneer_observability::queues::QueueOutcome::Resnapshot, 1);
        }
        ClientPublicationBatch {
            closed: self.is_stopped(),
            effects,
            sequence,
            resnapshot: gap,
            changes,
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn shutdown(&self) {
        if self.stopped.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        self.session_driver
            .lock()
            .expect("session driver poisoned")
            .stop();
        // Wake lifecycle waiters without inventing a visible publication revision.
        self.publication_signal.send_modify(|_| {});
        self.onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .stop();
        self.device_activation
            .lock()
            .expect("activation owner poisoned")
            .invalidate();
        self.invitation_commits
            .lock()
            .expect("invitation commits poisoned")
            .invalidate();
        self.agents_documents
            .lock()
            .expect("document owner poisoned")
            .stop();
        self.settings_model_pickers
            .lock()
            .expect("settings model picker poisoned")
            .stop();
        self.settings_runtime
            .lock()
            .expect("settings owner poisoned")
            .stop();
        self.skills_controller
            .lock()
            .expect("Skills controller poisoned")
            .stop();
        self.skills_store
            .lock()
            .expect("Skills store poisoned")
            .stop();
        self.mcp_controller
            .lock()
            .expect("MCP controller poisoned")
            .stop();
        self.mcp_store.lock().expect("MCP store poisoned").stop();
        self.administration_operations
            .lock()
            .expect("administration operations poisoned")
            .stop();
        self.administration_store
            .lock()
            .expect("administration store poisoned")
            .stop();
        self.provider_controller
            .lock()
            .expect("provider controller poisoned")
            .stop();
        self.provider_store
            .lock()
            .expect("provider store poisoned")
            .stop();
        self.provider_runtimes
            .lock()
            .expect("provider runtimes poisoned")
            .stop();
        self.composer_voice_cleanup
            .lock()
            .expect("voice cleanup poisoned")
            .stop();
        self.composer_voice_readiness
            .lock()
            .expect("voice readiness controller poisoned")
            .stop();
        self.composer_models
            .lock()
            .expect("composer models poisoned")
            .stop();
        self.composer_steers
            .lock()
            .expect("composer steer controller poisoned")
            .stop();
        self.composer_edits
            .lock()
            .expect("composer edit controller poisoned")
            .stop();
        self.composer_store
            .lock()
            .expect("composer store poisoned")
            .clear();
        self.avatar_store
            .lock()
            .expect("avatar store poisoned")
            .invalidate();
        self.task_notifications
            .lock()
            .expect("task inbox poisoned")
            .stop();
        self.turn_cancellations
            .lock()
            .expect("turn cancellations poisoned")
            .stop();
        self.composer_model_picker_requests
            .lock()
            .expect("composer model picker poisoned")
            .stop();
        self.composer_catalog_requests
            .lock()
            .expect("composer catalogs poisoned")
            .stop();
        self.message_revisions
            .lock()
            .expect("message revisions poisoned")
            .stop();
        self.message_deletions
            .lock()
            .expect("message deletion owner poisoned")
            .stop();
        self.approval_actions
            .lock()
            .expect("approval action owner poisoned")
            .stop();
        self.task_reviews
            .lock()
            .expect("task review owner poisoned")
            .stop();
        self.thread_capabilities
            .lock()
            .expect("thread capability owner poisoned")
            .stop();
        self.cancel_artifact_downloads(None);
        self.artifact_store
            .lock()
            .expect("artifact owner poisoned")
            .stop();
        self.thread_members
            .lock()
            .expect("thread member owner poisoned")
            .stop();
        self.workspace_controller
            .lock()
            .expect("workspace controller poisoned")
            .stop();
        self.presentation_sender
            .lock()
            .expect("presentation sender poisoned")
            .take();
        self.thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .take();
        *self
            .thread_registry
            .lock()
            .expect("thread registry poisoned") = Default::default();
        self.publication_ready.notify_all();
        {
            let mut partitions = self.partitions.lock().expect("client partitions poisoned");
            partitions.publications.clear();
            partitions.journal_observer().outcome(
                pioneer_observability::queues::QueueOutcome::Dropped,
                partitions.change_journal.len() as u64,
            );
            partitions.change_journal.clear();
            partitions
                .journal_observer()
                .current(0, pioneer_observability::queues::QueueBytes::Unsupported);
            let pending_effects = std::mem::take(&mut partitions.platform_effects);
            for (id, pending) in pending_effects {
                partitions
                    .operations
                    .insert(id, OperationState::Cancelled(pending.plan.generation()));
                let _ = pending.reply.send(ClientEffectResult::Failed {
                    code: "client_shutdown".into(),
                });
            }
        }
        self.close_gateway_transport_leases();
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .stop();
        self.gateway_session
            .lock()
            .expect("Gateway session owner poisoned")
            .stop();
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("client subscriber registry poisoned");
        for queue in subscribers.values().filter_map(Weak::upgrade) {
            queue
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
            let mut events = queue
                .events
                .lock()
                .expect("client subscriber queue poisoned");
            queue.observer.outcome(
                pioneer_observability::queues::QueueOutcome::Dropped,
                events.len() as u64,
            );
            events.clear();
            queue
                .observer
                .current(0, pioneer_observability::queues::QueueBytes::Unsupported);
        }
        subscribers.clear();
        self.stop_mcp_oauth();
        let _ = self.transport_runtime.ws_command_sender().shutdown();
        #[cfg(test)]
        self.transport_runtime.close_test_ingress();
    }

    pub(crate) fn current_scope_demand(&self, scope: &ClientScope) -> Option<ClientDemand> {
        self.partitions
            .lock()
            .expect("client partitions poisoned")
            .demands
            .get(scope)
            .map(|state| state.demand)
    }

    /// The single ingress dispatcher. All mutation terminates in Client owners.
    /// Unhandled classification is diagnostic only and is never delivered to a shell.
    pub(crate) fn route_gateway_event(
        &self,
        event: &crate::transport::ws::GatewayWsEvent,
    ) -> Option<crate::gateway::event_router::GatewayEventRoute> {
        use crate::gateway::event_router::GatewayEventRoute;
        if self.is_stopped()
            || !self
                .transport_runtime
                .ws_command_sender()
                .accepts_gateway_event(event)
        {
            return None;
        }
        // Accepted retiring-connection events still own exact Voice cleanup even
        // when they can no longer update the current authorization presentation.
        match event {
            crate::transport::ws::GatewayWsEvent::Connected { .. } => {
                self.retire_disconnected_composer_voice(None);
            }
            crate::transport::ws::GatewayWsEvent::Reconnecting { connection_id, .. }
            | crate::transport::ws::GatewayWsEvent::Disconnected { connection_id, .. } => {
                self.retire_disconnected_composer_voice(Some(*connection_id));
            }
            _ => {}
        }
        if let crate::transport::ws::GatewayWsEvent::Notification { notification, .. } = event {
            self.observe_administration_notification(notification);
            self.observe_provider_runtime_notification(notification);
            if self.observe_mcp_oauth_notification(notification) {
                return None;
            }
            if self.observe_mcp_notification(notification) {
                return None;
            }
            if let pioneer_protocol::GatewayNotification::SkillsChanged(notification) = notification
            {
                self.refresh_skills(&notification.workspace_id);
                return None;
            }
            self.observe_composer_voice_notification(notification);
            self.observe_composer_voice_readiness_notification(notification);
            self.observe_artifact_notification(notification);
            if self.observe_thread_member_notification(notification) {
                return None;
            }
            if self.observe_workspace_notification(notification) {
                return None;
            }
        }
        let route = GatewayEventRoute::classify(event);
        match route {
            GatewayEventRoute::Connection => {
                if !matches!(
                    event,
                    crate::transport::ws::GatewayWsEvent::Connected { .. }
                ) {
                    self.cancel_thread_requests();
                }
                self.observe_gateway_transport(event);
                self.observe_gateway_authorization(event);
            }
            GatewayEventRoute::Session => self.observe_gateway_transport(event),
            GatewayEventRoute::Authorization => self.observe_gateway_authorization(event),
            GatewayEventRoute::Settings => self.observe_gateway_settings(event),
            GatewayEventRoute::Thread | GatewayEventRoute::PendingRequest => {
                if let crate::transport::ws::GatewayWsEvent::Notification { notification, .. } =
                    event
                {
                    if self.apply_thread_notification(notification.clone()) {
                        return None;
                    }
                }
                return Some(route);
            }
            GatewayEventRoute::TaskNotification => {
                if let crate::transport::ws::GatewayWsEvent::Notification { notification, .. } =
                    event
                {
                    self.observe_task_notification(notification);
                }
            }
            GatewayEventRoute::Mcp | GatewayEventRoute::Skills => {}
            GatewayEventRoute::Administration
            | GatewayEventRoute::Workspace
            | GatewayEventRoute::Memory
            | GatewayEventRoute::Provider
            | GatewayEventRoute::Unknown => return Some(route),
        }
        None
    }

    pub fn shared() -> Arc<Self> {
        let core = Arc::new(Self::new());
        core.initialize_navigation();
        core.start_session_driver();
        core.start_administration_operation_controller();
        core.start_administration_controller();
        core.start_provider_runtime_controller();
        core.start_provider_controller();
        core.start_mcp_controller();
        core.start_mcp_operation_controller();
        core.start_agents_document_controller();
        core.start_settings_controller();
        core.start_settings_model_picker_controller();
        core.start_onboarding_controller();
        core.start_skills_controller();
        core.start_skills_operation_controller();
        core.start_provider_operation_controller();
        core.start_provider_credential_controller();
        core.start_task_notification_controller();
        core.start_task_review_controller();
        core.start_approval_action_controller();
        core.start_message_revision_controller();
        core.start_message_deletion_controller();
        core.start_composer_catalog_controller();
        core.start_composer_model_picker_controller();
        core.start_turn_cancellation_controller();
        core.start_composer_voice_cleanup_controller();
        core.start_composer_voice_readiness_controller();
        core.start_composer_model_display_controller();
        core.start_composer_message_edit_controller();
        core.start_composer_steer_controller();
        core.start_thread_capability_controller();
        core.start_thread_member_controller();
        core.start_artifact_controller();
        core.start_artifact_preview_controller();
        core.start_workspace_controller();
        let (presentation_sender, presentation_receiver) = std::sync::mpsc::sync_channel(1);
        *core
            .presentation_sender
            .lock()
            .expect("presentation sender poisoned") = Some(presentation_sender);
        let weak_presentation = Arc::downgrade(&core);
        *core
            .presentation_task
            .lock()
            .expect("presentation task poisoned") = Some(
            std::thread::Builder::new()
                .name("client-thread-presentation".into())
                .spawn(move || {
                    while let Ok(first_wake) = presentation_receiver.recv() {
                        let first_work = Self::receive_presentation_wake(first_wake);
                        // A fixed window has bounded latency even during a continuous stream.
                        std::thread::sleep(std::time::Duration::from_millis(8));
                        let mut links = first_work;
                        while let Ok(wake) = presentation_receiver.try_recv() {
                            links.merge_link(&Self::receive_presentation_wake(wake));
                        }
                        let Some(core) = weak_presentation.upgrade() else {
                            break;
                        };
                        if core.is_stopped() {
                            break;
                        }
                        core.materialize_presentation_cycle(links);
                    }
                })
                .expect("Client presentation task could not start"),
        );
        core.start_thread_request_controller(
            #[cfg(test)]
            None,
        );
        core.start_gateway_event_dispatcher();
        core
    }

    fn receive_presentation_wake(
        (work, queue): (
            pioneer_observability::work::WorkCarrier,
            pioneer_observability::queues::QueueObserver,
        ),
    ) -> pioneer_observability::work::WorkCarrier {
        queue.dequeued(pioneer_observability::queues::QueueBytes::Known(0));
        let mut processing = queue.processing();
        processing.processed(); // input consumed into coalescing, not UI/materialization completion
        work
    }
    fn materialize_presentation_cycle(&self, links: pioneer_observability::work::WorkCarrier) {
        use pioneer_observability::work::{Boundary, ExecutorRole, Trigger, WorkCarrier, WorkKind};
        let mut cycle = WorkCarrier::independent(
            Boundary::Timer,
            WorkKind::ClientPublication,
            Trigger::Reconcile,
            ExecutorRole::ClientController,
        );
        cycle.merge_link(&links);
        cycle.run(|| self.materialize_demanded_timelines());
    }

    pub(crate) fn start_thread_request_controller(
        self: &Arc<Self>,
        #[cfg(test)] waiting: Option<std::sync::mpsc::Sender<Option<std::time::Duration>>>,
    ) {
        let (sender, receiver) = std::sync::mpsc::channel();
        *self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned") = Some(sender.into());
        let weak_requests = Arc::downgrade(self);
        *self
            .thread_request_task
            .lock()
            .expect("thread request task poisoned") = Some(
            std::thread::Builder::new()
                .name("client-thread-requests".into())
                .spawn(move || {
                    loop {
                        let Some(core) = weak_requests.upgrade() else {
                            break;
                        };
                        if core.is_stopped() {
                            break;
                        }
                        let delay = [
                            core.next_thread_resume_delay(),
                            core.next_timeline_request_delay(),
                            core.next_thread_subscription_delay(),
                        ]
                        .into_iter()
                        .flatten()
                        .min();
                        #[cfg(test)]
                        if let Some(waiting) = &waiting {
                            let _ = waiting.send(delay);
                        }
                        drop(core);
                        let request = match delay {
                            Some(delay) => match receiver.recv_timeout(delay) {
                                Ok(r) => Some(r),
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                                Err(_) => break,
                            },
                            None => match receiver.recv() {
                                Ok(r) => Some(r),
                                Err(_) => break,
                            },
                        };
                        let Some(core) = weak_requests.upgrade() else {
                            break;
                        };
                        if core.is_stopped() {
                            break;
                        }
                        let timer = request.is_none();
                        if let Some(item) = request {
                            core.dispatch_thread_controller_item(item);
                        }
                        pioneer_observability::work::WorkCarrier::independent(
                            pioneer_observability::work::Boundary::Timer,
                            pioneer_observability::work::WorkKind::ClientCommand,
                            if timer {
                                pioneer_observability::work::Trigger::Timer
                            } else {
                                pioneer_observability::work::Trigger::Reconcile
                            },
                            pioneer_observability::work::ExecutorRole::ClientController,
                        )
                        .run(|| {
                            core.drive_due_thread_resumes();
                            core.drive_timeline_requests();
                            core.drive_thread_subscriptions();
                        });
                    }
                })
                .expect("Client thread request task could not start"),
        );
    }

    fn dispatch_thread_controller_item(
        &self,
        item: pioneer_observability::work::WorkItem<
            crate::threads::registry::ThreadControllerRequest,
        >,
    ) {
        let queue = item.queue.as_ref().map(|(q, _)| q.clone());
        let (request, work) = item.received(
            pioneer_observability::work::Boundary::StdThread,
            pioneer_observability::work::WorkKind::ClientCommand,
            pioneer_observability::work::ExecutorRole::ClientController,
        );
        let mut processing = queue.as_ref().map(|q| q.processing());
        work.run(|| self.execute_thread_controller_request(request));
        if let Some(processing) = &mut processing {
            processing.processed();
        }
    }

    fn execute_thread_controller_request(
        &self,
        request: crate::threads::registry::ThreadControllerRequest,
    ) {
        match request {
            crate::threads::registry::ThreadControllerRequest::Read(request) => {
                self.execute_timeline_read(request)
            }
            crate::threads::registry::ThreadControllerRequest::Semantic(request) => self
                .execute_thread_semantic_request(
                    &self.transport_runtime.ws_command_sender(),
                    request,
                ),
            crate::threads::registry::ThreadControllerRequest::Subscribe(request) => {
                self.execute_thread_subscription_request(request)
            }
            crate::threads::registry::ThreadControllerRequest::Binding {
                id,
                workspace,
                generation,
            } => self.execute_thread_cli_binding_request(&id, &workspace, generation),
            _ => {}
        }
    }

    pub(crate) fn start_gateway_event_dispatcher(self: &Arc<Self>) {
        let runtime = self.transport_runtime.clone();
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-gateway-events".into())
            .spawn(move || {
                while let Some(first) = runtime.recv_ws_event() {
                    for event in std::iter::once(first).chain(runtime.drain_ws_events()) {
                        let Some(core) = weak.upgrade() else {
                            return;
                        };
                        core.dispatch_local_gateway_event(event);
                    }
                }
            })
            .expect("Client Gateway event task could not start");
        *self
            .gateway_task
            .lock()
            .expect("Gateway task owner poisoned") = Some(task);
    }

    fn dispatch_local_gateway_event(
        &self,
        item: pioneer_observability::work::WorkItem<crate::transport::ws::GatewayWsEvent>,
    ) {
        let queue = item.queue.as_ref().map(|(q, _)| q.clone());
        let (event, work) = item.received(
            pioneer_observability::work::Boundary::EventLane,
            pioneer_observability::work::WorkKind::Event,
            pioneer_observability::work::ExecutorRole::ClientController,
        );
        let mut processing = queue.as_ref().map(|q| q.processing());
        work.run(|| {
            self.route_gateway_event(&event);
        });
        if let Some(processing) = &mut processing {
            processing.processed();
        }
    }

    /// Process-local Gateway transport handle; no domain state or event delivery to shells.
    pub fn transport_runtime(&self) -> &ClientRuntime {
        &self.transport_runtime
    }

    pub fn subscribe(
        self: &Arc<Self>,
        scope: ClientScope,
        capacity: NonZeroUsize,
    ) -> ClientSubscription {
        self.thread_subscription_changed(&scope, true);
        self.composer_subscription_changed(&scope, true);
        self.avatar_subscription_changed(&scope, true);
        self.task_inbox_subscription_changed(&scope, true);
        self.task_review_subscription_changed(&scope, true);
        self.approval_action_subscription_changed(&scope, true);
        self.message_revision_subscription_changed(&scope, true);
        self.message_deletion_subscription_changed(&scope, true);
        self.composer_catalog_subscription_changed(&scope, true);
        self.composer_model_picker_subscription_changed(&scope, true);
        self.settings_model_picker_subscription_changed(&scope, true);
        self.turn_cancellation_subscription_changed(&scope, true);
        self.thread_capability_subscription_changed(&scope, true);
        self.thread_member_subscription_changed(&scope, true);
        self.artifact_subscription_changed(&scope, true);
        self.workspace_subscription_changed(&scope, true);
        let partitions = self.partitions.lock().expect("client partitions poisoned");
        let latest_sequence = partitions
            .publications
            .get(&scope)
            .map(|snapshot| snapshot.sequence());
        let queue = Arc::new(SubscriptionQueue {
            observer: pioneer_observability::queues::QueueObserver::new(
                pioneer_observability::queues::QueueKind::ClientSubscriptions,
                Some(capacity.get()),
                "unsupported: shared publication retained bytes; processed=handoff to shell, not UI apply",
            ),
            scope,
            capacity,
            events: Mutex::new(VecDeque::with_capacity(capacity.get())),
            latest_sequence: Mutex::new(latest_sequence),
            closed: std::sync::atomic::AtomicBool::new(self.is_stopped()),
        });
        let id = {
            let mut next = self
                .next_subscription_id
                .lock()
                .expect("client subscriber sequence poisoned");
            *next = next
                .checked_add(1)
                .expect("Client subscription identity exhausted");
            *next
        };
        {
            let mut subscribers = self
                .subscribers
                .lock()
                .expect("client subscriber registry poisoned");
            if self.is_stopped() {
                queue
                    .closed
                    .store(true, std::sync::atomic::Ordering::Release);
            } else {
                subscribers.insert(id, Arc::downgrade(&queue));
            }
        }
        drop(partitions);
        ClientSubscription {
            pending_work: Mutex::new(Default::default()),
            id,
            queue,
            core: Arc::downgrade(self),
        }
    }

    pub fn snapshot(&self, scope: &ClientScope) -> Option<ClientPublicationReference> {
        self.partitions
            .lock()
            .expect("client partitions poisoned")
            .publications
            .get(scope)
            .map(|snapshot| ClientPublicationReference(Arc::clone(snapshot), None))
    }

    pub fn snapshot_if_newer(
        &self,
        scope: &ClientScope,
        revision: Option<ScopedRevision>,
    ) -> Option<ClientPublicationReference> {
        self.snapshot(scope).filter(|publication| {
            revision.is_none_or(|revision| publication.revisions().scoped() > revision)
        })
    }

    pub(crate) fn navigation_outcome(&self, outcome: ClientTransitionOutcome) -> ClientTransition {
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        partitions.transition_sequence.advance();
        Self::transition_without_publication(&partitions, outcome)
    }

    pub(crate) fn artifact_action_transition(&self, changed: bool) -> ClientTransition {
        let partitions = self.partitions.lock().expect("client partitions poisoned");
        Self::transition_without_publication(
            &partitions,
            if changed {
                ClientTransitionOutcome::Changed
            } else {
                ClientTransitionOutcome::Noop
            },
        )
    }

    pub(crate) fn reject_intent(&self) -> ClientTransition {
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        partitions.transition_sequence.advance();
        Self::transition_without_publication(&partitions, ClientTransitionOutcome::Rejected)
    }
    pub fn dispatch(&self, intent: ClientIntent) -> ClientTransition {
        if self.is_stopped() {
            let mut partitions = self.partitions.lock().expect("client partitions poisoned");
            partitions.transition_sequence.advance();
            return Self::transition_without_publication(
                &partitions,
                ClientTransitionOutcome::Rejected,
            );
        }
        match &intent {
            ClientIntent::SessionDemand { demand } => return self.session_demand(demand.clone()),
            ClientIntent::Onboarding { intent } => return self.onboarding_intent(intent.clone()),
            ClientIntent::Settings { intent } => return self.settings_intent(intent.clone()),
            ClientIntent::SettingsModelPicker { intent } => {
                return self.settings_model_picker_intent(intent.clone());
            }
            ClientIntent::Profile { intent } => return self.profile_intent(intent.clone()),
            ClientIntent::AgentsDocument { intent } => {
                return self.agents_document_intent(intent.clone());
            }
            ClientIntent::AdministrationPresentation { intent } => {
                return self.administration_presentation_intent(intent.clone());
            }
            ClientIntent::AdministrationCommand { command } => {
                return self.administration_command_intent(command.clone());
            }
            ClientIntent::AdministrationPage { intent } => {
                return self.administration_page_intent(intent.clone());
            }
            ClientIntent::ProviderPresentation { intent } => {
                return self.provider_presentation_intent(intent.clone());
            }
            ClientIntent::ProviderCommand { command } => {
                return self.provider_command_intent(command.clone());
            }
            ClientIntent::ProviderCollection { intent } => {
                return self.provider_collection_intent(intent.clone());
            }
            ClientIntent::ProviderRuntime { intent } => {
                return self.provider_runtime_intent(intent.clone());
            }
            ClientIntent::Composer { intent } => return self.composer_intent(intent.clone()),
            ClientIntent::Artifact { intent } => return self.artifact_intent(intent.clone()),
            ClientIntent::ThreadMember { intent } => {
                return self.thread_member_intent(intent.clone());
            }
            ClientIntent::ThreadCapability { intent } => {
                return self.thread_capability_intent(intent.clone());
            }
            ClientIntent::TurnCancellation { intent } => {
                return self.request_turn_cancellation(intent.clone());
            }
            ClientIntent::ComposerModelPicker { intent } => {
                return self.composer_model_picker_intent(intent.clone());
            }
            ClientIntent::ComposerCatalog { intent } => {
                return self.composer_catalog_intent(intent.clone());
            }
            ClientIntent::MessageRevisions { intent } => {
                return self.message_revision_intent(intent.clone());
            }
            ClientIntent::MessageDeletion { intent } => {
                return self.message_deletion_intent(intent.clone());
            }
            ClientIntent::ApprovalAction { intent } => {
                return self.approval_action_intent(intent.clone());
            }
            ClientIntent::TaskReview { intent } => return self.task_review_intent(intent.clone()),
            ClientIntent::Workspace { intent } => {
                return self.dispatch_workspace_intent(intent.clone());
            }
            ClientIntent::TaskNotification { intent } => {
                return self.task_notification_intent(intent.clone());
            }
            ClientIntent::Navigation {
                intent,
                expected_revision,
            } => return self.navigate(intent.clone(), *expected_revision),
            ClientIntent::RefreshTimeline { thread_id } => {
                self.refresh_thread_timeline(thread_id);
            }
            ClientIntent::SetTimelineExpansion {
                thread_id,
                row_ids,
                collapsed_row_ids,
            } => {
                self.set_thread_timeline_expansion(thread_id, row_ids, collapsed_row_ids);
            }
            ClientIntent::Timeline { intent } => {
                let plan = self.timeline_intent(intent.clone());
                let changed = plan != Default::default();
                let mut partitions = self.partitions.lock().expect("client partitions poisoned");
                if changed {
                    partitions.transition_sequence.advance();
                }
                return Self::transition_without_publication(
                    &partitions,
                    if changed {
                        ClientTransitionOutcome::Changed
                    } else {
                        ClientTransitionOutcome::Noop
                    },
                );
            }
            ClientIntent::SetScopeDemand { .. } => {}
        }
        let ClientIntent::SetScopeDemand {
            scope: thread_scope,
            demand: thread_demand,
            ..
        } = &intent
        else {
            let mut partitions = self.partitions.lock().expect("client partitions poisoned");
            partitions.transition_sequence.advance();
            return Self::transition_without_publication(
                &partitions,
                if self.is_stopped() {
                    ClientTransitionOutcome::Rejected
                } else {
                    ClientTransitionOutcome::Changed
                },
            );
        };
        let (thread_scope, thread_demand) = (thread_scope.clone(), *thread_demand);
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        partitions.transition_sequence.advance();
        if self.is_stopped() {
            return Self::transition_without_publication(
                &partitions,
                ClientTransitionOutcome::Rejected,
            );
        }
        let outcome = match intent {
            ClientIntent::SetScopeDemand {
                scope,
                demand,
                generation,
            } => match partitions.demands.get(&scope) {
                Some(current) if current.generation > generation => ClientTransitionOutcome::Stale,
                Some(current) if current.generation == generation && current.demand == demand => {
                    ClientTransitionOutcome::Noop
                }
                Some(current) if current.generation == generation => {
                    ClientTransitionOutcome::Rejected
                }
                _ => {
                    partitions
                        .demands
                        .insert(scope, DemandState { demand, generation });
                    ClientTransitionOutcome::Changed
                }
            },
            _ => unreachable!("timeline intent handled"),
        };
        let transition = Self::transition_without_publication(&partitions, outcome);
        drop(partitions);
        if outcome == ClientTransitionOutcome::Changed {
            self.thread_demand_changed(&thread_scope, thread_demand);
            self.composer_demand_changed(&thread_scope, thread_demand);
            self.avatar_demand_changed(&thread_scope, thread_demand);
            self.task_inbox_demand_changed(&thread_scope, thread_demand);
            self.task_review_demand_changed(&thread_scope, thread_demand);
            self.approval_action_demand_changed(&thread_scope, thread_demand);
            self.message_revision_demand_changed(&thread_scope, thread_demand);
            self.message_deletion_demand_changed(&thread_scope, thread_demand);
            self.composer_catalog_demand_changed(&thread_scope, thread_demand);
            self.mcp_binding_demand_changed(&thread_scope, thread_demand);
            self.skills_binding_demand_changed(&thread_scope, thread_demand);
            self.document_demand_changed(&thread_scope, thread_demand);
            self.settings_demand_changed(&thread_scope, thread_demand);
            self.composer_model_picker_demand_changed(&thread_scope, thread_demand);
            self.settings_model_picker_demand_changed(&thread_scope, thread_demand);
            self.turn_cancellation_demand_changed(&thread_scope, thread_demand);
            self.thread_capability_demand_changed(&thread_scope, thread_demand);
            self.thread_member_demand_changed(&thread_scope, thread_demand);
            self.artifact_demand_changed(&thread_scope, thread_demand);
            self.workspace_demand_changed(&thread_scope, thread_demand);
        }
        transition
    }

    pub(crate) fn request_platform_effect(
        &self,
        effect: crate::gateway::session_refresh::GatewaySessionStorageEffect,
    ) -> anyhow::Result<ClientEffectResult> {
        self.request_native_effect(effect.into())
    }
    pub(crate) fn request_native_effect(
        &self,
        effect: ClientPlannedEffect,
    ) -> anyhow::Result<ClientEffectResult> {
        let (reply, received) = std::sync::mpsc::channel();
        {
            let mut partitions = self.partitions.lock().expect("client partitions poisoned");
            anyhow::ensure!(!self.is_stopped(), "Client runtime is stopped");
            anyhow::ensure!(
                partitions.platform_effects.len() < 128,
                "Native effect capacity exhausted"
            );
            partitions.next_platform_effect = partitions
                .next_platform_effect
                .checked_add(1)
                .expect("native effect identity exhausted");
            let generation = ClientGeneration::new(partitions.next_platform_effect);
            let id =
                ClientOperationId::new(format!("gateway-session-storage/{}", generation.get()))
                    .expect("nonempty operation identity");
            let plan = ClientEffectPlan::new(id.clone(), generation, effect);
            let published_at = std::time::Instant::now();
            self.commit_publications(&mut partitions, vec![], vec![plan.clone()]);
            partitions.platform_effects.insert(
                id,
                PendingPlatformEffect {
                    plan,
                    reply,
                    published_at,
                },
            );
            self.publication_ready.notify_all();
        }
        received
            .recv()
            .map_err(|_| anyhow::anyhow!("Native effect owner was released"))
    }

    pub fn complete_effect(&self, completion: ClientEffectCompletion) -> ClientTransition {
        self.finish_effect(
            completion.operation_id(),
            completion.generation(),
            Some(completion.result().clone()),
        )
    }

    pub fn cancel_effect(&self, cancellation: ClientEffectCancellation) -> ClientTransition {
        self.finish_effect(cancellation.operation_id(), cancellation.generation(), None)
    }

    fn finish_effect(
        &self,
        operation_id: &ClientOperationId,
        generation: ClientGeneration,
        result: Option<ClientEffectResult>,
    ) -> ClientTransition {
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        partitions.transition_sequence.advance();
        if self.is_stopped() {
            return Self::transition_without_publication(
                &partitions,
                ClientTransitionOutcome::Rejected,
            );
        }
        let outcome = match partitions.operations.get(operation_id).copied() {
            None => ClientTransitionOutcome::Rejected,
            Some(OperationState::Pending(current)) if current != generation => {
                ClientTransitionOutcome::Stale
            }
            Some(OperationState::Pending(_)) => {
                if let (Some(pending), Some(result)) = (
                    partitions.platform_effects.get(operation_id),
                    result.as_ref(),
                ) {
                    let compatible = matches!(result, ClientEffectResult::Failed { .. }) || matches!((pending.plan.effect(), result),
                        (ClientPlannedEffect::OnboardingPlatform(crate::gateway::onboarding_effects::OnboardingPlatformEffect::LoadGatewayEnvironment),ClientEffectResult::GatewayEnvironmentLoaded{..}) |
                        (ClientPlannedEffect::OnboardingPlatform(crate::gateway::onboarding_effects::OnboardingPlatformEffect::PersistGatewayRegistry{..}|crate::gateway::onboarding_effects::OnboardingPlatformEffect::RemoveGatewayBindingJournal{..}),ClientEffectResult::Completed) |
                        (ClientPlannedEffect::OnboardingPlatform(crate::gateway::onboarding_effects::OnboardingPlatformEffect::PrepareLocalGateway{..}),ClientEffectResult::LocalGatewayPrepared{..}) |
                        (ClientPlannedEffect::OnboardingPlatform(crate::gateway::onboarding_effects::OnboardingPlatformEffect::CreateLocalDeviceActivation{..}),ClientEffectResult::LocalDeviceActivationCreated{..}) |
                        (ClientPlannedEffect::GatewaySessionStorage(crate::gateway::session_refresh::GatewaySessionStorageEffect::ReadGatewaySession { .. }), ClientEffectResult::GatewaySessionEnvelopeLoaded { .. }) |
                        (ClientPlannedEffect::GatewaySessionStorage(crate::gateway::session_refresh::GatewaySessionStorageEffect::PersistGatewaySession { .. } | crate::gateway::session_refresh::GatewaySessionStorageEffect::DeleteGatewaySession { .. }), ClientEffectResult::Completed));
                    if !compatible {
                        return Self::transition_without_publication(
                            &partitions,
                            ClientTransitionOutcome::Rejected,
                        );
                    }
                }
                partitions.operations.insert(
                    operation_id.clone(),
                    if result.is_none() {
                        OperationState::Cancelled(generation)
                    } else {
                        OperationState::Completed(generation)
                    },
                );
                if let Some(pending) = partitions.platform_effects.remove(operation_id) {
                    let _ = pending
                        .reply
                        .send(result.unwrap_or(ClientEffectResult::Failed {
                            code: "operation_cancelled".into(),
                        }));
                    partitions
                        .completed_platform_effects
                        .push_back(operation_id.clone());
                    while partitions.completed_platform_effects.len() > 128 {
                        if let Some(expired) = partitions.completed_platform_effects.pop_front() {
                            partitions.operations.remove(&expired);
                        }
                    }
                }
                ClientTransitionOutcome::Changed
            }
            Some(OperationState::Completed(current) | OperationState::Cancelled(current))
                if current == generation =>
            {
                ClientTransitionOutcome::Noop
            }
            Some(OperationState::Completed(_) | OperationState::Cancelled(_)) => {
                ClientTransitionOutcome::Stale
            }
        };
        Self::transition_without_publication(&partitions, outcome)
    }

    fn transition_without_publication(
        partitions: &ClientPartitions,
        outcome: ClientTransitionOutcome,
    ) -> ClientTransition {
        ClientTransition {
            sequence: partitions.transition_sequence,
            outcome,
            changes: ClientChangeSet::empty(),
            effects: Arc::from([]),
        }
    }

    pub fn publish<T>(
        &self,
        authority: &ClientMutationAuthority,
        scope: ClientScope,
        revisions: ClientRevisions,
        payload: Arc<T>,
        effects: Vec<ClientEffectPlan>,
    ) -> ClientTransition
    where
        T: Any + Send + Sync + Serialize,
    {
        self.transition(
            authority,
            vec![authority.publication(scope, revisions, payload)],
            effects,
        )
    }

    pub fn transition(
        &self,
        _authority: &ClientMutationAuthority,
        drafts: Vec<ClientPublicationDraft>,
        effects: Vec<ClientEffectPlan>,
    ) -> ClientTransition {
        let work = pioneer_observability::work::WorkCarrier::child(
            pioneer_observability::work::Boundary::ClientPublication,
            pioneer_observability::work::WorkKind::ClientPublication,
            pioneer_observability::work::Trigger::Event,
            pioneer_observability::work::ExecutorRole::ClientController,
        );
        let _context = work.enter();
        let mut observed = pioneer_observability::live::WorkGuard::start(work.context(), false);
        pioneer_observability::live::progress(pioneer_observability::live::Stage::Publication);
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        let wake_presentation = drafts
            .iter()
            .any(|draft| matches!(draft.scope, ClientScope::Thread { .. }));
        let wake_composer = drafts.iter().any(|draft| {
            matches!(
                draft.scope,
                ClientScope::Thread { .. }
                    | ClientScope::Navigation
                    | ClientScope::ThreadCapability { .. }
                    | ClientScope::Administration { .. }
            )
        });
        let wake_workspace_draft = drafts
            .iter()
            .any(|draft| matches!(draft.scope, ClientScope::Administration { .. }));
        let transition = self.commit_publications(&mut partitions, drafts, effects);
        drop(partitions);
        if wake_presentation {
            self.wake_thread_presentation();
        }
        if wake_composer {
            self.queue_composer_model_selection_refresh();
        }
        if wake_workspace_draft {
            self.queue_workspace_draft_reconciliation();
        }
        observed.finish(pioneer_observability::work::Outcome::Completed);
        transition
    }

    fn commit_publications(
        &self,
        partitions: &mut ClientPartitions,
        drafts: Vec<ClientPublicationDraft>,
        effects: Vec<ClientEffectPlan>,
    ) -> ClientTransition {
        {
            partitions.transition_sequence.advance();
            if self.is_stopped() {
                return Self::transition_without_publication(
                    &partitions,
                    ClientTransitionOutcome::Rejected,
                );
            }

            let mut seen_scopes = std::collections::HashSet::new();
            if drafts
                .iter()
                .any(|draft| !seen_scopes.insert(draft.scope.clone()))
            {
                return Self::transition_without_publication(
                    &partitions,
                    ClientTransitionOutcome::Rejected,
                );
            }

            let mut changed_drafts = Vec::new();
            for draft in drafts {
                match partitions.publications.get(&draft.scope) {
                    Some(current) => {
                        let current_revisions = current.revisions();
                        if draft.revisions.domain() < current_revisions.domain()
                            || draft.revisions.presentation() < current_revisions.presentation()
                            || draft.revisions.content() < current_revisions.content()
                            || draft.revisions.scoped() < current_revisions.scoped()
                        {
                            return Self::transition_without_publication(
                                &partitions,
                                ClientTransitionOutcome::Stale,
                            );
                        }
                        if (draft.timeline_change.is_none() || draft.revisions == current_revisions)
                            && draft.payload_eq.map_or_else(
                                || {
                                    draft.serialized_payload.get().as_ref()
                                        == current.serialized_payload().as_ref()
                                },
                                |equal| equal(draft.payload.as_ref(), current.payload.as_ref()),
                            )
                        {
                            continue;
                        }
                        if draft.revisions == current_revisions {
                            return Self::transition_without_publication(
                                &partitions,
                                ClientTransitionOutcome::Rejected,
                            );
                        }
                        let scoped_increment = current_revisions
                            .scoped()
                            .get()
                            .checked_add(1)
                            .map(ScopedRevision::new);
                        if scoped_increment != Some(draft.revisions.scoped()) {
                            return Self::transition_without_publication(
                                &partitions,
                                ClientTransitionOutcome::Rejected,
                            );
                        }
                    }
                    None if draft.revisions.scoped() != ScopedRevision::new(1) => {
                        return Self::transition_without_publication(
                            &partitions,
                            ClientTransitionOutcome::Rejected,
                        );
                    }
                    None => {}
                }
                changed_drafts.push(draft);
            }

            let mut unique_effects = HashMap::new();
            let mut accepted_effects = Vec::with_capacity(effects.len());
            for effect in effects {
                if let Some(seen_generation) =
                    unique_effects.insert(effect.operation_id().clone(), effect.generation())
                {
                    if seen_generation == effect.generation() {
                        continue;
                    }
                    return Self::transition_without_publication(
                        &partitions,
                        ClientTransitionOutcome::Rejected,
                    );
                }

                match partitions.operations.get(effect.operation_id()).copied() {
                    Some(OperationState::Pending(current)) if current < effect.generation() => {
                        return Self::transition_without_publication(
                            &partitions,
                            ClientTransitionOutcome::Rejected,
                        );
                    }
                    Some(
                        OperationState::Pending(current)
                        | OperationState::Completed(current)
                        | OperationState::Cancelled(current),
                    ) if current > effect.generation() => {
                        return Self::transition_without_publication(
                            &partitions,
                            ClientTransitionOutcome::Stale,
                        );
                    }
                    Some(
                        OperationState::Pending(current)
                        | OperationState::Completed(current)
                        | OperationState::Cancelled(current),
                    ) if current == effect.generation() => continue,
                    Some(OperationState::Completed(_) | OperationState::Cancelled(_)) | None => {}
                    Some(OperationState::Pending(_)) => unreachable!("pending generation covered"),
                }
                accepted_effects.push(effect);
            }
            let effects = accepted_effects;

            if changed_drafts.is_empty() && effects.is_empty() {
                return Self::transition_without_publication(
                    &partitions,
                    ClientTransitionOutcome::Noop,
                );
            }

            if changed_drafts.is_empty() {
                for effect in &effects {
                    partitions.operations.insert(
                        effect.operation_id().clone(),
                        OperationState::Pending(effect.generation()),
                    );
                }
                return ClientTransition {
                    sequence: partitions.transition_sequence,
                    outcome: ClientTransitionOutcome::Changed,
                    changes: ClientChangeSet::empty(),
                    effects: Arc::from(effects),
                };
            }

            partitions.change_sequence.advance();
            let predecessor = partitions.latest_change_sequence;
            let sequence = partitions.change_sequence;
            let timeline_changes = changed_drafts
                .iter()
                .filter_map(|d| d.timeline_change.clone())
                .collect::<Vec<_>>();
            let publications = changed_drafts
                .into_iter()
                .map(|draft| {
                    let timeline_change = draft.timeline_change.clone().map(Arc::new);
                    let snapshot = ClientSnapshot::from_draft(draft, sequence);
                    partitions
                        .publications
                        .insert(snapshot.scope().clone(), Arc::clone(&snapshot));
                    ClientPublicationReference(snapshot, timeline_change)
                })
                .collect::<Vec<_>>();
            for effect in &effects {
                partitions.operations.insert(
                    effect.operation_id().clone(),
                    OperationState::Pending(effect.generation()),
                );
            }
            partitions.latest_change_sequence = Some(sequence);
            let change_set = Arc::new(ClientChangeSet {
                sequence,
                predecessor,
                publications: Arc::from(publications),
                timeline_changes: Arc::from(timeline_changes),
            });
            let transition = ClientTransition {
                sequence: partitions.transition_sequence,
                outcome: ClientTransitionOutcome::Changed,
                changes: change_set,
                effects: Arc::from(effects),
            };
            partitions.prune_change_journal();
            if partitions.change_journal.len() == 128 {
                partitions.change_journal.pop_front();
                partitions
                    .journal_observer()
                    .outcome(pioneer_observability::queues::QueueOutcome::Dropped, 1);
            }
            partitions
                .change_journal
                .push_back(JournalChangeSet::new(transition.changes().as_ref()));
            partitions
                .journal_observer()
                .enqueued(pioneer_observability::queues::QueueBytes::Unsupported);
            let journal_depth = partitions.change_journal.len();
            partitions.journal_observer().current(
                journal_depth,
                pioneer_observability::queues::QueueBytes::Unsupported,
            );
            self.deliver_change_set(transition.changes().as_ref());
            self.publication_signal.send_replace(sequence);
            self.publication_ready.notify_all();
            transition
        }
    }

    pub(crate) fn publish_identity_authorization(
        &self,
        projections: &crate::gateway::identity_authorization::IdentityAuthorizationPublication,
        change: crate::gateway::identity_authorization::IdentityPublicationChange,
    ) -> ClientTransition {
        use crate::gateway::identity_authorization::IdentityPublicationChange;
        self.fence_onboarding_authorization(projections);
        if change == IdentityPublicationChange::Update {
            self.resume_settings_demand(projections);
        }
        let evict_protected = change != IdentityPublicationChange::Update;
        let reset_session = change == IdentityPublicationChange::ResetSession;
        let mut composer_publications = Vec::new();
        let mut document_fence = evict_protected.then(|| {
            self.agents_documents
                .lock()
                .expect("document owner poisoned")
        });
        let mut document_publications = document_fence
            .as_mut()
            .map(|owner| self.fence_agents_documents(owner))
            .unwrap_or_default();
        let principal = projections
            .current_auth
            .as_ref()
            .map(|auth| auth.principal.id.as_str().to_owned());
        if reset_session {
            self.invitation_commits
                .lock()
                .expect("invitation commits poisoned")
                .invalidate();
        }
        let remote_key_reset_generation = if evict_protected {
            let mut runtime = self
                .settings_runtime
                .lock()
                .expect("settings owner poisoned");
            runtime.invalidate();
            runtime.remote_key_reset_generation()
        } else {
            0
        };
        if evict_protected {
            self.device_activation
                .lock()
                .expect("activation owner poisoned")
                .invalidate();
            self.avatar_store
                .lock()
                .expect("avatar store poisoned")
                .invalidate_access();
        }
        if evict_protected {
            self.workspace_catalog
                .lock()
                .expect("workspace catalog poisoned")
                .invalidate();
        }
        if evict_protected {
            self.retire_workspace_push();
            self.task_notifications
                .lock()
                .expect("task inbox poisoned")
                .store
                .invalidate();
        }
        let retained_invitation_presentation =
            evict_protected && self.invalidate_administration_operations(!reset_session, None);
        if evict_protected {
            self.invalidate_administration(false);
            self.invalidate_provider_runtimes();
            self.invalidate_provider_collections();
            self.invalidate_mcp();
            self.invalidate_mcp_operations();
            self.fence_mcp_oauth();
            self.invalidate_skills();
            self.invalidate_skills_operations();
            self.invalidate_provider_operations();
            self.cancel_artifact_downloads(None);
            self.cancel_composer_requests();
            self.invalidate_task_reviews(None);
            self.invalidate_approval_actions(None);
            self.invalidate_message_revisions(None);
            self.invalidate_message_deletions(None);
            self.invalidate_turn_cancellations(None);
            self.invalidate_thread_capabilities(None);
            self.invalidate_thread_members(None);
            self.invalidate_artifacts(None);
        }
        // Keep edits serialized with the fence through publication commit. Otherwise
        // a concurrent keystroke could advance the composer revision between them.
        let mut composer_fence =
            evict_protected.then(|| self.composer_store.lock().expect("composer store poisoned"));
        if let Some(composers) = composer_fence.as_mut() {
            if reset_session {
                composers.clear();
            } else {
                composer_publications = composers.fence_authorization();
            }
        }
        let mut presentation_fence = evict_protected.then(|| {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            registry.fence_presentations(principal.clone());
            registry.restrict_settings_navigation(
                projections
                    .capabilities
                    .snapshot(None, None)
                    .is_some_and(|s| s.global.can_manage_gateway_settings),
            );
            if reset_session {
                registry.reset_navigation();
            }
            registry
        });
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        if !evict_protected && partitions.publications
            .get(&ClientScope::Administration { workspace_id: None })
            .and_then(|snapshot| snapshot.payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>())
            .is_some_and(|current| current.as_ref() == projections)
        {
            return self.commit_publications(&mut partitions, vec![], vec![]);
        }
        let authority = ClientMutationAuthority { _private: () };
        let next_revisions = |scope: &ClientScope| {
            let current = partitions
                .publications
                .get(scope)
                .map(|snapshot| snapshot.revisions())
                .unwrap_or_default();
            let next = current
                .scoped()
                .get()
                .checked_add(1)
                .expect("scope revision exhausted");
            ClientRevisions::new(
                DomainRevision::new(
                    current
                        .domain()
                        .get()
                        .checked_add(1)
                        .expect("domain revision exhausted"),
                ),
                PresentationRevision::new(
                    current
                        .presentation()
                        .get()
                        .checked_add(1)
                        .expect("presentation revision exhausted"),
                ),
                current.content(),
                ScopedRevision::new(next),
            )
        };
        let identity_scope = ClientScope::Administration { workspace_id: None };
        let mut drafts = vec![authority.publication(
            identity_scope.clone(),
            next_revisions(&identity_scope),
            Arc::new(projections.clone()),
        )];
        drafts.append(&mut composer_publications);
        drafts.append(&mut document_publications);
        if let Some(registry) = presentation_fence.as_mut() {
            if let Some(navigation) = registry.navigation_change() {
                drafts.push(navigation);
            }
        }
        if evict_protected {
            for scope in partitions.publications.keys() {
                if matches!(
                    scope,
                    ClientScope::Navigation
                        | ClientScope::Session
                        | ClientScope::OnboardingInvitation
                        | ClientScope::GatewaySetup
                        | ClientScope::GatewayDestinations
                ) || scope == &identity_scope
                    || (retained_invitation_presentation
                        && scope == &ClientScope::AdministrationOperation)
                    || (matches!(scope, ClientScope::AgentsDocumentContent { .. })
                        && drafts.iter().any(|draft| &draft.scope == scope))
                    || (!reset_session
                        && matches!(scope, ClientScope::Composer { .. })
                        && drafts.iter().any(|draft| &draft.scope == scope))
                {
                    continue;
                }
                if scope == &ClientScope::AuthSessions {
                    drafts.push(authority.publication(
                        scope.clone(),
                        next_revisions(scope),
                        Arc::new(
                            crate::gateway::identity_authorization::AuthSessionsStore::default(),
                        ),
                    ));
                } else if scope == &ClientScope::Profile {
                    drafts.push(authority.publication(
                        scope.clone(),
                        next_revisions(scope),
                        Arc::new(crate::settings::profile::ProfilePublication::default()),
                    ));
                } else if matches!(scope, ClientScope::SettingsPage { .. }) {
                    drafts.push(authority.publication(
                        scope.clone(),
                        next_revisions(scope),
                        Arc::new(crate::settings::runtime::SettingsPagePublication {
                            input_reset_generation: if matches!(
                                scope,
                                ClientScope::SettingsPage {
                                    page: crate::settings::runtime::SettingsPage::RemoteAccess
                                }
                            ) {
                                remote_key_reset_generation
                            } else {
                                0
                            },
                            ..Default::default()
                        }),
                    ));
                } else if scope == &ClientScope::Settings {
                    drafts.push(authority.publication(
                        scope.clone(),
                        next_revisions(scope),
                        Arc::new(crate::gateway::settings_store::GatewaySettingsStore::default()),
                    ));
                } else {
                    drafts.push(authority.publication(
                        scope.clone(),
                        next_revisions(scope),
                        Arc::new(serde_json::Value::Null),
                    ));
                }
            }
            // A slow consumer must resnapshot after the fence, rather than
            // receive historical protected values followed by their eviction.
            partitions.journal_observer().outcome(
                pioneer_observability::queues::QueueOutcome::Dropped,
                partitions.change_journal.len() as u64,
            );
            partitions.change_journal.clear();
            partitions
                .journal_observer()
                .current(0, pioneer_observability::queues::QueueBytes::Unsupported);
            let subscribers = self
                .subscribers
                .lock()
                .expect("client subscriber registry poisoned");
            for queue in subscribers.values().filter_map(Weak::upgrade) {
                if drafts.iter().any(|draft| draft.scope == queue.scope) {
                    let mut events = queue
                        .events
                        .lock()
                        .expect("client subscriber queue poisoned");
                    queue.observer.outcome(
                        pioneer_observability::queues::QueueOutcome::Dropped,
                        events.len() as u64,
                    );
                    events.clear();
                    queue
                        .observer
                        .current(0, pioneer_observability::queues::QueueBytes::Unsupported);
                }
            }
        }
        let transition = self.commit_publications(&mut partitions, drafts, vec![]);
        drop(partitions);
        if let Some(registry) = presentation_fence.as_mut() {
            registry.synchronize_publication_revisions(transition.changes().publications());
        }
        drop(presentation_fence);
        drop(composer_fence);
        drop(document_fence);
        self.update_thread_presentation_identity(principal);
        // Identity commits bypass transition() while applying the access fence.
        // Resume consumers waiting for workspace capabilities/model metadata too.
        self.queue_workspace_draft_reconciliation();
        self.queue_composer_model_selection_refresh();
        transition
    }

    fn deliver_change_set(&self, change_set: &ClientChangeSet) {
        self.subscribers
            .lock()
            .expect("client subscriber registry poisoned")
            .retain(|_, weak| {
                let Some(queue) = weak.upgrade() else {
                    return false;
                };
                if let Some(publication) = change_set
                    .publications()
                    .iter()
                    .find(|publication| publication.scope() == &queue.scope)
                {
                    queue.push(change_set.sequence(), publication.clone());
                }
                true
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_dispatch_publication_handoff_and_materialization_keep_causality() {
        use pioneer_observability::{
            live::{self, JournalPayload, WorkState},
            work::{
                self, Boundary, ExecutorRole, Outcome, Trigger, WorkCarrier, WorkItem, WorkKind,
            },
        };
        let _lock = crate::OBSERVABILITY_TEST_LOCK.lock().unwrap();
        let old = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        let mut fixture_core = ClientCore::new();
        fixture_core.presentation_observer = pioneer_observability::queues::QueueObserver::new(
            pioneer_observability::queues::QueueKind::ClientController,
            Some(1),
            "fixture common presentation lane",
        );
        let core = Arc::new(fixture_core);
        struct Cleanup {
            core: Arc<ClientCore>,
            enabled: bool,
        }
        impl Drop for Cleanup {
            fn drop(&mut self) {
                self.core.shutdown();
                pioneer_observability::set_telemetry_enabled(self.enabled);
            }
        }
        let _cleanup = Cleanup {
            core: core.clone(),
            enabled: old,
        };
        let parent = WorkCarrier::independent(
            Boundary::StdThread,
            WorkKind::ClientCommand,
            Trigger::Event,
            ExecutorRole::ClientController,
        );
        let outer = WorkCarrier::independent(
            Boundary::StdThread,
            WorkKind::ClientCommand,
            Trigger::Event,
            ExecutorRole::ClientController,
        );
        // Accepted local transport event must produce a real Session publication
        // under its carried owner, not just complete a wrapper around a reject.
        parent.run(|| {
            core.transport_runtime.inject_test_event(
                crate::transport::ws::GatewayWsEvent::Connecting {
                    connection_id: 0,
                    endpoint_id: "fixture".into(),
                    endpoint_name: "Fixture".into(),
                    endpoint_kind: crate::gateway::types::GatewayEndpointKind::Local,
                },
            )
        });
        let connecting = core
            .transport_runtime
            .drain_ws_events()
            .into_iter()
            .next()
            .expect("synchronous injected event");
        let connecting_context = connecting.carrier.context().unwrap();
        outer.run(|| {
            core.dispatch_local_gateway_event(connecting);
            assert_eq!(work::current(), outer.context());
        });
        assert!(core.snapshot(&ClientScope::Session).is_some());
        assert!(live::snapshot().unwrap().journal.iter().any(|record| matches!(&record.payload,
            JournalPayload::Work(w) if w.context.parent == Some(connecting_context.token) && w.context.kind == WorkKind::ClientPublication)));
        let mut fixture_thread = pioneer_protocol::Thread {
            workspace_id: "ws".into(),
            id: "thread".into(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: pioneer_protocol::ThreadMode::Chat,
            model: "fixture".into(),
            model_provider: "fixture".into(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 2,
            status: pioneer_protocol::ThreadStatus::Idle,
            origin_kind: pioneer_protocol::ThreadOriginKind::User,
            sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        core.upsert_thread(fixture_thread.clone());
        fixture_thread.name = Some("observed update".into());
        fixture_thread.updated_at = 3;
        let (tx, rx) = std::sync::mpsc::channel();
        let sender = crate::transport::ws::client::GatewayWsEventSender::from(tx);
        let payload = serde_json::json!({"jsonrpc":"2.0", "method":pioneer_protocol::constants::events::THREAD_UPDATED,
            "params":pioneer_protocol::ThreadUpdatedNotification {thread:fixture_thread.clone(),placement:None}}).to_string();
        parent.run(|| {
            assert!(matches!(
                crate::transport::ws::decode::process_text_payload(
                    &payload,
                    0,
                    &mut crate::rpc::PendingJsonRpcRequests::default(),
                    &mut HashMap::new(),
                    &mut HashMap::new(),
                    &sender,
                ),
                Some(pioneer_protocol::GatewayNotification::ThreadUpdated(_))
            ));
        });
        let item = rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("valid notification reaches local dispatch");
        let event_context = item.carrier.context().unwrap();
        assert_eq!(event_context.parent, parent.context().map(|c| c.token));
        outer.run(|| {
            core.dispatch_local_gateway_event(item);
            assert_eq!(work::current(), outer.context());
        });
        let observed = core.thread_coordinator_snapshot("thread").unwrap();
        assert_eq!(observed.thread().unwrap().name, fixture_thread.name);
        assert!(live::snapshot().unwrap().journal.iter().any(|record| matches!(&record.payload,
            JournalPayload::Work(w) if w.context.parent == Some(event_context.token) && w.context.kind == WorkKind::ClientPublication)));
        let (tx, rx) = std::sync::mpsc::channel();
        let sender = crate::threads::registry::ThreadControllerSender::from(tx);

        *core.thread_request_sender.lock().unwrap() = Some(sender.clone());
        struct BindingTransport(Arc<Mutex<Option<work::WorkContext>>>);
        impl crate::rpc::JsonRpcRequestTransport for BindingTransport {
            fn send_json_rpc_request(
                &self,
                _id: String,
                _payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                *self.0.lock().unwrap() = work::current();
                response
                    .send(Err(crate::rpc::JsonRpcResponseError::transport(
                        "fixture binding rejection",
                    )))
                    .unwrap();
                Ok(())
            }
        }
        let observed_rpc = Arc::new(Mutex::new(None));
        *core
            .transport_runtime
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(Arc::new(BindingTransport(observed_rpc.clone())));
        parent.run(|| core.schedule_thread_cli_binding("thread", "ws"));
        let item = rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("local request delivered");
        let request_context = item.carrier.context().unwrap();
        assert_eq!(request_context.parent, parent.context().map(|c| c.token));
        outer.run(|| {
            core.dispatch_thread_controller_item(item);
            assert_eq!(work::current(), outer.context());
        });
        let rpc = observed_rpc
            .lock()
            .unwrap()
            .expect("real Binding handler reached the RPC factory");
        assert_eq!(rpc.kind, WorkKind::ClientRpc);
        assert_eq!(rpc.parent, Some(request_context.token));
        assert!(live::snapshot().unwrap().journal.iter().any(|record| matches!(&record.payload,
            JournalPayload::Work(w) if w.context.token == rpc.token && w.state == WorkState::Terminal(Outcome::Failed))));
        // A following item without local metadata is a root, never the previous item.
        core.dispatch_thread_controller_item(WorkItem::from_carrier(
            crate::threads::registry::ThreadControllerRequest::Resume,
            WorkCarrier::default(),
            None,
        ));
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        let closed = crate::threads::registry::ThreadControllerSender::from(closed_tx);
        drop(closed_rx);
        parent.run(|| {
            assert!(
                closed
                    .send(crate::threads::registry::ThreadControllerRequest::Resume)
                    .is_err()
            )
        });
        assert!(live::snapshot().unwrap().journal.iter().any(|record| matches!(&record.payload,
            JournalPayload::Work(w) if w.context.parent == parent.context().map(|c| c.token) && w.state == WorkState::Terminal(Outcome::Cancelled))));
        let scope = ClientScope::Navigation;
        let subscription = core.subscribe(scope.clone(), NonZeroUsize::new(4).unwrap());
        parent.run(|| {
            core.publish(
                &authority(),
                scope.clone(),
                revisions(1),
                Arc::new(1u64),
                vec![],
            );
        });
        subscription.try_next().unwrap();
        let delivered = subscription.pending_work().context().unwrap();
        assert!(
            !live::snapshot()
                .unwrap()
                .active
                .iter()
                .any(|w| w.context.token == delivered.token)
        );
        let execution = subscription
            .pending_work()
            .execution_child(WorkKind::ClientPublication, ExecutorRole::DesktopForeground);
        assert_eq!(execution.context().unwrap().parent, Some(delivered.token));
        execution.run(|| {});
        core.publish(
            &authority(),
            scope.clone(),
            revisions(2),
            Arc::new(2u64),
            vec![],
        );
        let undelivered = subscription.queue.events.lock().unwrap()[0]
            .carrier
            .context()
            .unwrap()
            .token;
        drop(subscription);
        let snapshot = live::snapshot().unwrap();
        let terminals = |token| {
            snapshot
                .journal
                .iter()
                .filter_map(move |r| match &r.payload {
                    JournalPayload::Work(w)
                        if w.context.token == token
                            && matches!(w.state, WorkState::Terminal(_)) =>
                    {
                        Some(w.state)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            terminals(delivered.token),
            [WorkState::Terminal(Outcome::Completed)]
        );
        assert_eq!(
            terminals(undelivered),
            [WorkState::Terminal(Outcome::Cancelled)]
        );
        assert_eq!(
            terminals(event_context.token),
            [WorkState::Terminal(Outcome::Completed)]
        );
        assert_eq!(
            terminals(request_context.token),
            [WorkState::Terminal(Outcome::Completed)]
        );
        let (wake_tx, wake_rx) = std::sync::mpsc::sync_channel(1);
        *core.presentation_sender.lock().unwrap() = Some(wake_tx);
        parent.run(|| {
            core.wake_thread_presentation();
            core.wake_thread_presentation();
        });
        core.materialize_presentation_cycle(ClientCore::receive_presentation_wake(
            wake_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("local request delivered"),
        ));
        let snapshot = live::snapshot().unwrap();
        let wakes = snapshot
            .queues
            .iter()
            .find(|q| q.byte_definition == "fixture common presentation lane")
            .unwrap();
        assert_eq!(
            (
                wakes.offered,
                wakes.enqueued,
                wakes.dequeued,
                wakes.processed,
                wakes.coalesced,
                wakes.depth
            ),
            (2, 1, 1, 1, 1, 0)
        );
        assert!(live::snapshot().unwrap().journal.iter().any(|r| matches!(&r.payload, JournalPayload::Work(w) if w.context.kind == WorkKind::ClientPublication && w.context.trigger == Trigger::Reconcile && w.context.parent.is_none() && w.context.links.contains(&parent.context().map(|c| c.token)))));
        assert_eq!(work::current(), None);
        let old_item = parent.run(|| {
            WorkItem::new(
                crate::threads::registry::ThreadControllerRequest::Resume,
                Boundary::StdThread,
                WorkKind::ClientCommand,
                ExecutorRole::ClientController,
            )
        });
        let old_context = old_item.carrier.context().unwrap();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        core.dispatch_thread_controller_item(old_item);
        let snapshot = live::snapshot().unwrap();
        assert!(
            !snapshot
                .active
                .iter()
                .any(|w| w.context.token == old_context.token)
        );
        assert!(snapshot.journal.iter().any(|r| matches!(&r.payload, JournalPayload::Work(w) if w.context.parent.is_none() && w.context.consent_generation != old_context.consent_generation)));
    }
    #[test]
    fn publication_journal_releases_consumed_superseded_payloads() {
        let core = Arc::new(ClientCore::new());
        let scope = ClientScope::Thread {
            thread_id: "large-thread".into(),
        };
        let subscription = core.subscribe(scope.clone(), NonZeroUsize::new(4).unwrap());
        let mut payloads = Vec::new();
        for revision in 1..=96 {
            let payload = Arc::new(format!("{revision}:{}", "x".repeat(256 * 1024)));
            payloads.push(Arc::downgrade(&payload));
            core.publish(
                &authority(),
                scope.clone(),
                revisions(revision),
                payload,
                vec![],
            );
            drop(subscription.try_next().unwrap());
        }
        let retained = payloads
            .iter()
            .filter(|payload| payload.strong_count() > 0)
            .count();
        assert_eq!(
            retained, 1,
            "delivery history must not own obsolete full thread snapshots"
        );
        let batch = core.wait_for_publications(ClientChangeSequence::ZERO);
        assert!(batch.resnapshot);
        assert_eq!(batch.changes.len(), 1);
        assert_eq!(
            batch.changes[0].publications()[0].revisions(),
            revisions(96)
        );
    }

    #[test]
    fn current_publications_remain_incrementally_available_without_a_transition_owner() {
        let core = ClientCore::new();
        for (scope, value) in [
            (ClientScope::Settings, "settings"),
            (ClientScope::Navigation, "navigation"),
        ] {
            core.publish(&authority(), scope, revisions(1), Arc::new(value), vec![]);
        }
        let batch = core.wait_for_publications(ClientChangeSequence::ZERO);
        assert!(!batch.resnapshot);
        assert_eq!(batch.changes.len(), 2);
        assert_eq!(
            batch.changes[1].predecessor(),
            Some(batch.changes[0].sequence())
        );
    }

    #[test]
    fn typed_publications_compare_without_encoding_and_serialize_on_demand() {
        let core = ClientCore::new();
        let scope = ClientScope::Thread {
            thread_id: "typed".into(),
        };
        let publish = |revision, value| {
            core.transition(
                &authority(),
                vec![authority().typed_publication(
                    scope.clone(),
                    revisions(revision),
                    Arc::new(value),
                )],
                vec![],
            )
        };
        assert_eq!(
            publish(1, "first").outcome(),
            ClientTransitionOutcome::Changed
        );
        let first = core.snapshot(&scope).unwrap().snapshot();
        assert_eq!(publish(2, "first").outcome(), ClientTransitionOutcome::Noop);
        assert_eq!(
            publish(1, "different").outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert!(first.serialized_payload.value.get().is_none());
        assert_eq!(
            publish(2, "next").outcome(),
            ClientTransitionOutcome::Changed
        );
        let next = core.snapshot(&scope).unwrap().snapshot();
        assert!(next.serialized_payload.value.get().is_none());
        assert!(first.serialized_payload.value.get().is_none());
        let serialized = next.serialized_payload();
        assert_eq!(serialized.as_ref(), &serde_json::json!("next"));
        assert!(Arc::ptr_eq(&serialized, &next.serialized_payload()));
    }

    #[test]
    fn expired_journal_entry_returns_one_coherent_snapshot_across_scopes() {
        let core = ClientCore::new();
        core.publish(
            &authority(),
            ClientScope::Settings,
            revisions(1),
            Arc::new("old"),
            vec![],
        );
        let initial = core.publish(
            &authority(),
            ClientScope::Navigation,
            revisions(1),
            Arc::new("thread-a"),
            vec![],
        );
        let after = initial.changes().sequence();
        drop(initial);
        for revision in 2..=3 {
            core.transition(
                &authority(),
                vec![
                    authority().publication(
                        ClientScope::Settings,
                        revisions(revision),
                        Arc::new(revision),
                    ),
                    authority().publication(
                        ClientScope::Navigation,
                        revisions(revision),
                        Arc::new(revision),
                    ),
                ],
                vec![],
            );
        }
        let batch = core.wait_for_publications(after);
        assert!(batch.resnapshot);
        assert_eq!(batch.changes.len(), 1);
        let change = &batch.changes[0];
        assert_eq!(change.predecessor(), Some(after));
        assert_eq!(change.sequence(), batch.sequence);
        assert_eq!(change.publications().len(), 2);
        assert!(change.timeline_changes().is_empty());
        for publication in change.publications() {
            assert_eq!(publication.revisions(), revisions(3));
            assert_eq!(publication.snapshot().sequence(), batch.sequence);
            assert_eq!(*publication.typed::<u64>().unwrap().payload(), 3);
        }
    }

    #[derive(Debug)]
    struct SharedRow {
        id: &'static str,
        revision: u64,
    }

    #[derive(Debug)]
    struct SharedRows {
        rows: Vec<Arc<SharedRow>>,
    }

    impl Serialize for SharedRows {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            self.rows
                .iter()
                .map(|row| (row.id, row.revision))
                .collect::<Vec<_>>()
                .serialize(serializer)
        }
    }

    fn revisions(value: u64) -> ClientRevisions {
        ClientRevisions::new(
            DomainRevision::new(value),
            PresentationRevision::new(value),
            ContentRevision::new(value),
            ScopedRevision::new(value),
        )
    }

    fn authority() -> ClientMutationAuthority {
        ClientMutationAuthority { _private: () }
    }

    fn storage_read_effect() -> crate::gateway::session_refresh::GatewaySessionStorageEffect {
        crate::gateway::session_refresh::GatewaySessionStorageEffect::ReadGatewaySession {
            endpoint: crate::gateway::types::GatewayEndpoint {
                id: "synthetic".into(),
                name: "Synthetic".into(),
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                    "https://gateway.invalid",
                )
                .unwrap(),
                kind: crate::gateway::types::GatewayEndpointKind::Remote,
                session_ref: Some("synthetic-session".into()),
                server_gateway_id: None,
                workspace_id: None,
                service_name: None,
            },
        }
    }

    fn next_storage_effect(core: &ClientCore) -> ClientEffectPlan {
        for _ in 0..20 {
            let batch = core.wait_for_publications(ClientChangeSequence::ZERO);
            if let Some(effect) = batch.effects.into_iter().next() {
                return effect;
            }
        }
        panic!("storage effect was not delivered");
    }

    #[test]
    fn native_effect_enqueue_time_survives_republication_and_retires_with_plan() {
        let core = Arc::new(ClientCore::new());
        let worker_core = core.clone();
        let before = std::time::Instant::now();
        let worker =
            std::thread::spawn(move || worker_core.request_platform_effect(storage_read_effect()));
        let plan = next_storage_effect(&core);
        let published = core.native_effect_published_at(&plan).unwrap();
        assert!(published >= before && published <= std::time::Instant::now());
        let repeated = next_storage_effect(&core);
        assert_eq!(core.native_effect_published_at(&repeated), Some(published));
        let stale = ClientEffectPlan::new(
            plan.operation_id().clone(),
            ClientGeneration::new(plan.generation().get() + 1),
            storage_read_effect(),
        );
        assert_eq!(core.native_effect_published_at(&stale), None);
        core.complete_effect(ClientEffectCompletion::new(
            plan.operation_id().clone(),
            plan.generation(),
            ClientEffectResult::GatewaySessionEnvelopeLoaded { envelope: None },
        ));
        worker.join().unwrap().unwrap();
        assert_eq!(core.native_effect_published_at(&plan), None);
        core.shutdown();
    }

    #[test]
    fn storage_effect_completion_validates_shape_generation_and_duplicate_delivery() {
        let core = Arc::new(ClientCore::new());
        let worker_core = core.clone();
        let worker =
            std::thread::spawn(move || worker_core.request_platform_effect(storage_read_effect()));
        let effect = next_storage_effect(&core);
        let completion = |generation, result| {
            ClientEffectCompletion::new(effect.operation_id().clone(), generation, result)
        };
        assert_eq!(
            core.complete_effect(completion(
                ClientGeneration::new(effect.generation().get() + 1),
                ClientEffectResult::GatewaySessionEnvelopeLoaded { envelope: None }
            ))
            .outcome(),
            ClientTransitionOutcome::Stale
        );
        assert_eq!(
            core.complete_effect(completion(
                effect.generation(),
                ClientEffectResult::Completed
            ))
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert!(!worker.is_finished());
        let result = ClientEffectResult::GatewaySessionEnvelopeLoaded { envelope: None };
        assert_eq!(
            core.complete_effect(completion(effect.generation(), result.clone()))
                .outcome(),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(worker.join().unwrap().unwrap(), result);
        assert_eq!(
            core.complete_effect(completion(effect.generation(), result))
                .outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(core.partitions.lock().unwrap().platform_effects.is_empty());
    }

    #[test]
    fn storage_effect_shutdown_releases_waiter_and_rejects_late_completion() {
        let core = Arc::new(ClientCore::new());
        let worker_core = core.clone();
        let worker =
            std::thread::spawn(move || worker_core.request_platform_effect(storage_read_effect()));
        let effect = next_storage_effect(&core);
        core.shutdown();
        assert_eq!(
            worker.join().unwrap().unwrap(),
            ClientEffectResult::Failed {
                code: "client_shutdown".into()
            }
        );
        assert_eq!(
            core.complete_effect(ClientEffectCompletion::new(
                effect.operation_id().clone(),
                effect.generation(),
                ClientEffectResult::GatewaySessionEnvelopeLoaded { envelope: None }
            ))
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert!(core.request_platform_effect(storage_read_effect()).is_err());
    }

    #[test]
    fn equal_payload_preserves_revisions_snapshot_and_delivery() {
        let core = Arc::new(ClientCore::new());
        let scope = ClientScope::Settings;
        let subscription = core.subscribe(scope.clone(), NonZeroUsize::new(4).unwrap());
        core.publish(
            &authority(),
            scope.clone(),
            revisions(1),
            Arc::new("ready"),
            vec![],
        );
        let before = core.snapshot(&scope).unwrap().snapshot();
        assert!(subscription.try_next().is_some());
        let transition = core.publish(
            &authority(),
            scope.clone(),
            revisions(2),
            Arc::new("ready"),
            vec![],
        );
        assert_eq!(transition.outcome(), ClientTransitionOutcome::Noop);
        assert!(transition.changes().publications().is_empty());
        assert!(subscription.try_next().is_none());
        assert!(Arc::ptr_eq(
            &before,
            &core.snapshot(&scope).unwrap().snapshot()
        ));
        assert_eq!(core.snapshot(&scope).unwrap().revisions(), revisions(1));
    }

    #[test]
    fn revisions_and_publications_are_isolated_by_scope() {
        let core = Arc::new(ClientCore::new());
        let visible_scope = ClientScope::Timeline {
            thread_id: "thread-visible".to_owned(),
        };
        let other_scope = ClientScope::Timeline {
            thread_id: "thread-other".to_owned(),
        };
        let subscription = core.subscribe(
            visible_scope.clone(),
            NonZeroUsize::new(4).expect("non-zero capacity"),
        );

        core.publish(
            &authority(),
            other_scope,
            revisions(1),
            Arc::new(vec!["other"]),
            vec![],
        );
        assert!(subscription.try_next().is_none());

        core.publish(
            &authority(),
            visible_scope.clone(),
            revisions(1),
            Arc::new(vec!["first"]),
            vec![],
        );
        let first = subscription.try_next().expect("matching publication");
        let ClientSubscriptionEvent::Publication { publication, .. } = first else {
            panic!("unexpected resnapshot");
        };
        assert_eq!(publication.scope(), &visible_scope);
        assert_eq!(
            publication.typed::<Vec<&str>>().unwrap().payload()[0],
            "first"
        );

        let before = core.snapshot(&visible_scope).unwrap().snapshot();
        let noop = core.publish(
            &authority(),
            visible_scope.clone(),
            revisions(1),
            Arc::new(vec!["first"]),
            vec![],
        );
        assert_eq!(noop.outcome(), ClientTransitionOutcome::Noop);
        assert!(subscription.try_next().is_none());
        let after = core.snapshot(&visible_scope).unwrap().snapshot();
        assert!(Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn multi_scope_transition_is_atomic_and_preserves_shared_rows() {
        let core = Arc::new(ClientCore::new());
        let authority = authority();
        let thread_scope = ClientScope::Thread {
            thread_id: "thread-a".to_owned(),
        };
        let timeline_scope = ClientScope::Timeline {
            thread_id: "thread-a".to_owned(),
        };
        let thread_subscription = core.subscribe(
            thread_scope.clone(),
            NonZeroUsize::new(2).expect("non-zero capacity"),
        );
        let timeline_subscription = core.subscribe(
            timeline_scope.clone(),
            NonZeroUsize::new(2).expect("non-zero capacity"),
        );
        let stable_row = Arc::new(SharedRow {
            id: "row-stable",
            revision: 1,
        });
        let initial_rows = Arc::new(SharedRows {
            rows: vec![Arc::clone(&stable_row)],
        });
        core.publish(
            &authority,
            timeline_scope.clone(),
            revisions(1),
            initial_rows,
            vec![],
        );
        let _ = timeline_subscription.try_next();

        let changed_rows = Arc::new(SharedRows {
            rows: vec![
                Arc::clone(&stable_row),
                Arc::new(SharedRow {
                    id: "row-new",
                    revision: 1,
                }),
            ],
        });
        let transition = core.transition(
            &authority,
            vec![
                authority.publication(thread_scope.clone(), revisions(1), Arc::new("thread-ready")),
                authority.publication(timeline_scope.clone(), revisions(2), changed_rows),
            ],
            vec![],
        );
        assert_eq!(transition.outcome(), ClientTransitionOutcome::Changed);
        assert_eq!(transition.changes().publications().len(), 2);
        assert!(thread_subscription.try_next().is_some());
        assert!(timeline_subscription.try_next().is_some());

        let rows = core
            .snapshot(&timeline_scope)
            .unwrap()
            .typed::<SharedRows>()
            .unwrap()
            .payload();
        assert!(Arc::ptr_eq(&rows.rows[0], &stable_row));
    }

    #[test]
    fn invalid_multi_scope_draft_preserves_every_snapshot_and_effect() {
        let core = Arc::new(ClientCore::new());
        let authority = authority();
        let scope = ClientScope::Settings;
        core.publish(&authority, scope.clone(), revisions(1), Arc::new(1), vec![]);
        let before = core.snapshot(&scope).unwrap().snapshot();
        let subscription = core.subscribe(scope.clone(), NonZeroUsize::new(2).unwrap());
        for (invalid_revision, payload, expected) in [
            (0, 0, ClientTransitionOutcome::Stale),
            (1, 2, ClientTransitionOutcome::Rejected),
            (3, 3, ClientTransitionOutcome::Rejected),
        ] {
            let transition = core.transition(
                &authority,
                vec![
                    authority.publication(ClientScope::Provider, revisions(1), Arc::new("ready")),
                    authority.publication(
                        scope.clone(),
                        revisions(invalid_revision),
                        Arc::new(payload),
                    ),
                ],
                vec![ClientEffectPlan::new(
                    ClientOperationId::new("refresh").unwrap(),
                    ClientGeneration::new(1),
                    ClientEffect::RefreshProviderLists,
                )],
            );
            assert_eq!(transition.outcome(), expected);
            assert!(transition.changes().publications().is_empty());
            assert!(transition.effects().is_empty());
            assert!(core.snapshot(&ClientScope::Provider).is_none());
            assert!(Arc::ptr_eq(
                &before,
                &core.snapshot(&scope).unwrap().snapshot()
            ));
            assert!(subscription.try_next().is_none());
            assert!(core.partitions.lock().unwrap().operations.is_empty());
        }
    }

    #[test]
    fn bounded_queue_coalesces_to_scoped_resnapshot() {
        let core = Arc::new(ClientCore::new());
        let scope = ClientScope::Settings;
        let subscription = core.subscribe(
            scope.clone(),
            NonZeroUsize::new(1).expect("non-zero capacity"),
        );
        core.publish(
            &authority(),
            scope.clone(),
            revisions(1),
            Arc::new(1_u64),
            vec![],
        );
        core.publish(
            &authority(),
            scope.clone(),
            revisions(2),
            Arc::new(2_u64),
            vec![],
        );
        core.publish(
            &authority(),
            scope.clone(),
            revisions(3),
            Arc::new(3_u64),
            vec![],
        );

        let event = subscription.try_next().expect("resnapshot marker");
        let ClientSubscriptionEvent::ResnapshotRequired {
            scope: event_scope,
            latest_sequence,
        } = event
        else {
            panic!("queue did not coalesce");
        };
        assert_eq!(event_scope, scope);
        assert_eq!(latest_sequence.get(), 3);
        assert!(subscription.try_next().is_none());
        assert_eq!(
            core.snapshot(&scope)
                .unwrap()
                .typed::<u64>()
                .unwrap()
                .payload()
                .as_ref(),
            &3
        );
    }

    #[test]
    fn scoped_delivery_preserves_change_sequence_order() {
        let core = Arc::new(ClientCore::new());
        let scope = ClientScope::Settings;
        let subscription = core.subscribe(
            scope.clone(),
            NonZeroUsize::new(4).expect("non-zero capacity"),
        );
        for revision in 1..=3 {
            core.publish(
                &authority(),
                scope.clone(),
                revisions(revision),
                Arc::new(revision),
                vec![],
            );
        }

        let sequences = (0..3)
            .map(
                |_| match subscription.try_next().expect("ordered publication") {
                    ClientSubscriptionEvent::Publication { sequence, .. } => sequence.get(),
                    ClientSubscriptionEvent::ResnapshotRequired { .. } => {
                        panic!("queue should not coalesce below capacity")
                    }
                },
            )
            .collect::<Vec<_>>();
        assert_eq!(sequences, [1, 2, 3]);
    }

    #[test]
    fn demand_and_effect_generation_rules_are_explicit() {
        let core = ClientCore::new();
        let intent = ClientIntent::SetScopeDemand {
            scope: ClientScope::Provider,
            demand: ClientDemand::Visible,
            generation: ClientGeneration::new(2),
        };
        assert_eq!(
            core.dispatch(intent.clone()).outcome(),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            core.dispatch(intent).outcome(),
            ClientTransitionOutcome::Noop
        );
        assert_eq!(
            core.dispatch(ClientIntent::SetScopeDemand {
                scope: ClientScope::Provider,
                demand: ClientDemand::Suspended,
                generation: ClientGeneration::new(2),
            })
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert_eq!(
            core.dispatch(ClientIntent::SetScopeDemand {
                scope: ClientScope::Provider,
                demand: ClientDemand::Suspended,
                generation: ClientGeneration::new(1),
            })
            .outcome(),
            ClientTransitionOutcome::Stale
        );

        let operation = ClientOperationId::new("avatar-read").unwrap();
        core.publish(
            &authority(),
            ClientScope::Avatar {
                principal_id: "principal-a".to_owned(),
            },
            revisions(1),
            Arc::new("ready"),
            vec![ClientEffectPlan::new(
                operation.clone(),
                ClientGeneration::new(4),
                ClientEffect::RefreshProviderLists,
            )],
        );
        let completion = ClientEffectCompletion::new(
            operation,
            ClientGeneration::new(4),
            ClientEffectResult::Completed,
        );
        assert_eq!(
            core.complete_effect(completion.clone()).outcome(),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            core.complete_effect(completion).outcome(),
            ClientTransitionOutcome::Noop
        );

        let duplicate = core.publish(
            &authority(),
            ClientScope::Settings,
            revisions(1),
            Arc::new("settings-ready"),
            vec![ClientEffectPlan::new(
                ClientOperationId::new("avatar-read").unwrap(),
                ClientGeneration::new(4),
                ClientEffect::RefreshProviderLists,
            )],
        );
        assert_eq!(duplicate.outcome(), ClientTransitionOutcome::Changed);
        assert!(duplicate.effects().is_empty());

        let retry = core.publish(
            &authority(),
            ClientScope::Settings,
            revisions(2),
            Arc::new("settings-retrying"),
            vec![ClientEffectPlan::new(
                ClientOperationId::new("avatar-read").unwrap(),
                ClientGeneration::new(5),
                ClientEffect::RefreshProviderLists,
            )],
        );
        assert_eq!(retry.outcome(), ClientTransitionOutcome::Changed);
        assert_eq!(retry.effects().len(), 1);
    }

    #[test]
    fn dropping_subscription_unregisters_synchronously() {
        let core = Arc::new(ClientCore::new());
        let subscription = core.subscribe(
            ClientScope::Navigation,
            NonZeroUsize::new(2).expect("non-zero capacity"),
        );
        assert_eq!(core.subscribers.lock().unwrap().len(), 1);
        drop(subscription);
        assert!(core.subscribers.lock().unwrap().is_empty());
    }

    #[test]
    fn authorization_fence_evicts_protected_snapshots_in_one_change_set() {
        let core = Arc::new(ClientCore::new());
        let workspace = ClientScope::WorkspaceTree {
            workspace_id: Some("workspace".into()),
        };
        let thread = ClientScope::Thread {
            thread_id: "thread".into(),
        };
        let identity = ClientScope::Administration { workspace_id: None };
        for scope in [workspace.clone(), thread.clone()] {
            core.publish(
                &authority(),
                scope,
                revisions(1),
                Arc::new("protected"),
                vec![],
            );
        }
        let subscription = core.subscribe(identity.clone(), NonZeroUsize::new(4).unwrap());
        core.invalidate_authorization_revision(7);
        let ClientSubscriptionEvent::Publication { sequence, .. } =
            subscription.try_next().unwrap()
        else {
            panic!("identity publication");
        };
        for scope in [&workspace, &thread] {
            let snapshot = core.snapshot(scope).unwrap().snapshot();
            assert_eq!(snapshot.sequence(), sequence);
            assert!(snapshot.serialized_payload().is_null());
        }
        let lagging = core.wait_for_publications(ClientChangeSequence::ZERO);
        assert!(lagging.resnapshot);
        assert_eq!(lagging.changes.len(), 1);
        assert!(lagging.changes[0].publications().iter().all(|publication| {
            publication.scope() == &identity
                || publication.snapshot().serialized_payload().is_null()
                || (publication.scope() == &ClientScope::Navigation
                    && publication.snapshot().serialized_payload().as_ref()
                        == &serde_json::to_value(
                            crate::navigation::ClientNavigationState::default(),
                        )
                        .unwrap())
        }));
        let retained = core.snapshot(&identity).unwrap().snapshot();
        core.invalidate_authorization_revision(6);
        core.invalidate_authorization_revision(7);
        assert!(subscription.try_next().is_none());
        assert!(Arc::ptr_eq(
            &retained,
            &core.snapshot(&identity).unwrap().snapshot()
        ));
    }
}

impl From<crate::gateway::onboarding_effects::OnboardingPlatformEffect> for ClientPlannedEffect {
    fn from(effect: crate::gateway::onboarding_effects::OnboardingPlatformEffect) -> Self {
        Self::OnboardingPlatform(effect)
    }
}
