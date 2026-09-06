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
    Provider,
    Administration {
        workspace_id: Option<String>,
    },
    Mcp {
        workspace_id: Option<String>,
    },
    Skills {
        workspace_id: Option<String>,
    },
    Settings,
    OnboardingInvitation,
    AgentsDocument {
        workspace_id: String,
    },
    DesktopUpdate,
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientIntent {
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
    GatewaySessionStorage(crate::gateway::session_refresh::GatewaySessionStorageEffect),
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

pub struct ClientSnapshot {
    scope: ClientScope,
    revisions: ClientRevisions,
    sequence: ClientChangeSequence,
    payload: Arc<dyn Any + Send + Sync>,
    serialized_payload: Arc<serde_json::Value>,
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
        Arc::clone(&self.serialized_payload)
    }

    pub fn payload<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        Arc::clone(&self.payload).downcast().ok()
    }
}

#[derive(Clone)]
pub struct ClientPublicationReference(Arc<ClientSnapshot>);

impl ClientPublicationReference {
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
        ClientPublicationReference(Arc::clone(&self.snapshot))
    }
}

#[derive(Clone)]
pub struct ClientChangeSet {
    sequence: ClientChangeSequence,
    predecessor: Option<ClientChangeSequence>,
    publications: Arc<[ClientPublicationReference]>,
}

impl ClientChangeSet {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            sequence: ClientChangeSequence::ZERO,
            predecessor: None,
            publications: Arc::from([]),
        })
    }

    pub const fn sequence(&self) -> ClientChangeSequence {
        self.sequence
    }

    pub const fn predecessor(&self) -> Option<ClientChangeSequence> {
        self.predecessor
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
    events: Mutex<VecDeque<ClientSubscriptionEvent>>,
    latest_sequence: Mutex<Option<ClientChangeSequence>>,
    closed: std::sync::atomic::AtomicBool,
}

impl SubscriptionQueue {
    fn push(&self, sequence: ClientChangeSequence, publication: ClientPublicationReference) {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
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
            events.clear();
            events.push_back(ClientSubscriptionEvent::ResnapshotRequired {
                scope: self.scope.clone(),
                latest_sequence: sequence,
            });
            return;
        }

        if let Some(ClientSubscriptionEvent::ResnapshotRequired {
            latest_sequence, ..
        }) = events.back_mut()
        {
            *latest_sequence = sequence;
            return;
        }

        events.push_back(ClientSubscriptionEvent::Publication {
            sequence,
            predecessor,
            publication,
        });
    }
}

pub struct ClientSubscription {
    id: u64,
    queue: Arc<SubscriptionQueue>,
    core: Weak<ClientCore>,
}

impl ClientSubscription {
    pub fn scope(&self) -> &ClientScope {
        &self.queue.scope
    }

    pub fn try_next(&self) -> Option<ClientSubscriptionEvent> {
        self.queue
            .events
            .lock()
            .expect("client subscriber queue poisoned")
            .pop_front()
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
        }
        self.queue
            .events
            .lock()
            .expect("client subscriber queue poisoned")
            .clear();
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
    change_journal: VecDeque<Arc<ClientChangeSet>>,
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
    compatibility_runtime: ClientRuntime,
    pub(crate) thread_request_sender:
        Mutex<Option<std::sync::mpsc::Sender<crate::threads::registry::ThreadControllerRequest>>>,
    thread_request_task: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(crate) thread_registry: Mutex<crate::threads::registry::ThreadRegistry>,
    pub(crate) identity_authorization:
        Mutex<crate::gateway::identity_authorization::IdentityAuthorizationStore>,
    pub(crate) session_refresh_slots: Mutex<HashMap<String, Weak<Mutex<bool>>>>,
    pub(crate) gateway_session: Mutex<crate::gateway::session_controller::GatewaySessionController>,
    pub(crate) session_transport: Mutex<Option<String>>,
    pub(crate) gateway_transport_leases:
        Mutex<crate::gateway::session_controller::GatewayTransportLeases>,
    pub(crate) gateway_transport_ready: std::sync::Condvar,
    gateway_delivery: Arc<crate::gateway::event_router::GatewayCompatibilityQueue>,
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

pub struct ClientPublicationDraft {
    scope: ClientScope,
    revisions: ClientRevisions,
    payload: Arc<dyn Any + Send + Sync>,
    serialized_payload: Arc<serde_json::Value>,
}

impl ClientMutationAuthority {
    /// Synthetic publication capability for independent boundary test harnesses.
    /// This constructor is absent from ordinary application builds.
    #[cfg(feature = "test-support")]
    pub fn for_test() -> Self {
        Self { _private: () }
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
            scope,
            revisions,
            payload,
            serialized_payload: Arc::new(serialized_payload),
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
            compatibility_runtime: ClientRuntime::new(),
            thread_registry: Mutex::default(),
            thread_request_sender: Mutex::default(),
            thread_request_task: Mutex::default(),
            identity_authorization: Mutex::default(),
            session_refresh_slots: Mutex::default(),
            session_transport: Mutex::default(),
            gateway_transport_leases: Mutex::default(),
            gateway_transport_ready: std::sync::Condvar::new(),
            gateway_session: Mutex::default(),
            gateway_delivery: Arc::default(),
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
        if self.is_stopped() {
            return crate::gateway::session_controller::GatewaySessionTransition::stopped();
        }
        if matches!(
            &event,
            crate::gateway::session_lifecycle::SessionLifecycleEvent::AuthFailed { .. }
                | crate::gateway::session_lifecycle::SessionLifecycleEvent::NoStoredSession
                | crate::gateway::session_lifecycle::SessionLifecycleEvent::Suspend
        ) {
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
        let result = owner.reduce(endpoint_id, event);
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
                .compatibility_runtime
                .ws_command_sender()
                .gateway_state_event_is_current(event)
        {
            return;
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
                .compatibility_runtime
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

    pub fn wait_for_publications(&self, after: ClientChangeSequence) -> ClientPublicationBatch {
        let partitions = self.partitions.lock().expect("client partitions poisoned");
        let partitions = if !self.is_stopped() && partitions.change_sequence <= after {
            self.publication_ready
                .wait_timeout(partitions, std::time::Duration::from_millis(250))
                .expect("client publication wait poisoned")
                .0
        } else {
            partitions
        };
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
            .is_none_or(|first| first.predecessor().unwrap_or_default() > after);
        let changes = if gap {
            vec![Arc::new(ClientChangeSet {
                sequence,
                predecessor: Some(after),
                publications: Arc::from(
                    partitions
                        .publications
                        .values()
                        .map(|snapshot| ClientPublicationReference(snapshot.clone()))
                        .collect::<Vec<_>>(),
                ),
            })]
        } else {
            partitions
                .change_journal
                .iter()
                .filter(|change| change.sequence() > after)
                .cloned()
                .collect()
        };
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
            partitions.change_journal.clear();
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
        self.gateway_delivery.close();
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
            queue
                .events
                .lock()
                .expect("client subscriber queue poisoned")
                .clear();
        }
        subscribers.clear();
        let _ = self.compatibility_runtime.ws_command_sender().shutdown();
    }

    /// The single ingress dispatcher. Owned routes publish here; only unported
    /// feature routes cross the non-owning compatibility boundary.
    pub(crate) fn route_gateway_event(
        &self,
        event: &crate::transport::ws::GatewayWsEvent,
    ) -> Option<crate::gateway::event_router::GatewayEventRoute> {
        use crate::gateway::event_router::GatewayEventRoute;
        if self.is_stopped()
            || !self
                .compatibility_runtime
                .ws_command_sender()
                .accepts_gateway_event(event)
        {
            return None;
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
            GatewayEventRoute::Administration
            | GatewayEventRoute::Workspace
            | GatewayEventRoute::Memory
            | GatewayEventRoute::Provider
            | GatewayEventRoute::Mcp
            | GatewayEventRoute::Skills
            | GatewayEventRoute::TaskNotification
            | GatewayEventRoute::Unknown => return Some(route),
        }
        None
    }

    pub fn shared() -> Arc<Self> {
        let core = Arc::new(Self::new());
        let (sender, receiver) = std::sync::mpsc::channel();
        *core
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned") = Some(sender);
        let weak_requests = Arc::downgrade(&core);
        *core
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
                        let delay = core.next_thread_resume_delay();
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
                        match request {
                            Some(crate::threads::registry::ThreadControllerRequest::Semantic(
                                request,
                            )) => core.execute_thread_semantic_request(
                                &core.compatibility_runtime.ws_command_sender(),
                                request,
                            ),
                            Some(
                                crate::threads::registry::ThreadControllerRequest::Subscribe {
                                    id,
                                    workspace,
                                    generation,
                                },
                            ) => core.execute_thread_connection_request(
                                &id, &workspace, generation, false,
                            ),
                            Some(crate::threads::registry::ThreadControllerRequest::Binding {
                                id,
                                workspace,
                                generation,
                            }) => core.execute_thread_connection_request(
                                &id, &workspace, generation, true,
                            ),
                            _ => {}
                        }
                        core.drive_due_thread_resumes();
                    }
                })
                .expect("Client thread request task could not start"),
        );
        let runtime = core.compatibility_runtime.clone();
        let delivery = core.gateway_delivery.clone();
        let weak = Arc::downgrade(&core);
        let task = std::thread::Builder::new()
            .name("client-gateway-events".into())
            .spawn(move || {
                while let Some(first) = runtime.recv_ws_event() {
                    for event in std::iter::once(first).chain(runtime.drain_ws_events()) {
                        let Some(core) = weak.upgrade() else {
                            return;
                        };
                        let route = core.route_gateway_event(&event);
                        drop(core);
                        if let Some(route) = route {
                            if !delivery.push(route, event) {
                                return;
                            }
                        }
                    }
                }
                delivery.finish();
            })
            .expect("Client Gateway event task could not start");
        *core
            .gateway_task
            .lock()
            .expect("Gateway task owner poisoned") = Some(task);
        core
    }

    pub async fn next_gateway_compatibility_event(
        &self,
    ) -> Option<crate::gateway::event_router::GatewayCompatibilityEvent> {
        self.gateway_delivery.receive_async().await
    }

    pub fn receive_gateway_compatibility_event(
        &self,
    ) -> Option<crate::gateway::event_router::GatewayCompatibilityEvent> {
        self.gateway_delivery.receive()
    }

    pub fn drain_gateway_compatibility_events(
        &self,
    ) -> Vec<crate::gateway::event_router::GatewayCompatibilityEvent> {
        self.gateway_delivery.drain()
    }

    /// Named compatibility route for transport/reducer owners not migrated yet.
    pub fn compatibility_runtime(&self) -> &ClientRuntime {
        &self.compatibility_runtime
    }

    pub fn subscribe(
        self: &Arc<Self>,
        scope: ClientScope,
        capacity: NonZeroUsize,
    ) -> ClientSubscription {
        self.thread_subscription_changed(&scope, true);
        let partitions = self.partitions.lock().expect("client partitions poisoned");
        let latest_sequence = partitions
            .publications
            .get(&scope)
            .map(|snapshot| snapshot.sequence());
        let queue = Arc::new(SubscriptionQueue {
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
            .map(|snapshot| ClientPublicationReference(Arc::clone(snapshot)))
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

    pub fn dispatch(&self, intent: ClientIntent) -> ClientTransition {
        let ClientIntent::SetScopeDemand {
            scope: thread_scope,
            demand: thread_demand,
            ..
        } = &intent;
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
        };
        let transition = Self::transition_without_publication(&partitions, outcome);
        drop(partitions);
        if outcome == ClientTransitionOutcome::Changed {
            self.thread_demand_changed(&thread_scope, thread_demand);
        }
        transition
    }

    pub(crate) fn request_platform_effect(
        &self,
        effect: crate::gateway::session_refresh::GatewaySessionStorageEffect,
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
            self.commit_publications(&mut partitions, vec![], vec![plan.clone()]);
            partitions
                .platform_effects
                .insert(id, PendingPlatformEffect { plan, reply });
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
                        (ClientPlannedEffect::GatewaySessionStorage(crate::gateway::session_refresh::GatewaySessionStorageEffect::ReadGatewaySession { .. }), ClientEffectResult::GatewaySessionEnvelopeLoaded { .. }) |
                        (ClientPlannedEffect::GatewaySessionStorage(crate::gateway::session_refresh::GatewaySessionStorageEffect::PersistGatewaySession { .. }), ClientEffectResult::Completed));
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
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        self.commit_publications(&mut partitions, drafts, effects)
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
                        if draft.serialized_payload.as_ref()
                            == current.serialized_payload().as_ref()
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
            let publications = changed_drafts
                .into_iter()
                .map(|draft| {
                    let snapshot = ClientSnapshot::from_draft(draft, sequence);
                    partitions
                        .publications
                        .insert(snapshot.scope().clone(), Arc::clone(&snapshot));
                    ClientPublicationReference(snapshot)
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
            });
            let transition = ClientTransition {
                sequence: partitions.transition_sequence,
                outcome: ClientTransitionOutcome::Changed,
                changes: change_set,
                effects: Arc::from(effects),
            };
            if partitions.change_journal.len() == 128 {
                partitions.change_journal.pop_front();
            }
            partitions.change_journal.push_back(transition.changes());
            self.deliver_change_set(transition.changes().as_ref());
            self.publication_signal.send_replace(sequence);
            self.publication_ready.notify_all();
            transition
        }
    }

    pub(crate) fn publish_identity_authorization(
        &self,
        projections: &crate::gateway::identity_authorization::IdentityAuthorizationPublication,
        evict_protected: bool,
    ) -> ClientTransition {
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
        if evict_protected {
            for scope in partitions.publications.keys() {
                if matches!(
                    scope,
                    ClientScope::Session
                        | ClientScope::DesktopUpdate
                        | ClientScope::OnboardingInvitation
                ) || scope == &identity_scope
                {
                    continue;
                }
                if scope == &ClientScope::Settings {
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
            partitions.change_journal.clear();
            let subscribers = self
                .subscribers
                .lock()
                .expect("client subscriber registry poisoned");
            for queue in subscribers.values().filter_map(Weak::upgrade) {
                if drafts.iter().any(|draft| draft.scope == queue.scope) {
                    queue
                        .events
                        .lock()
                        .expect("client subscriber queue poisoned")
                        .clear();
                }
            }
        }
        self.commit_publications(&mut partitions, drafts, vec![])
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
