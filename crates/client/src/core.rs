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
    WorkspaceTree { workspace_id: Option<String> },
    Task { task_id: Option<String> },
    Thread { thread_id: String },
    Timeline { thread_id: String },
    Composer { thread_id: String },
    PendingRequest { thread_id: Option<String> },
    Artifact { thread_id: String },
    Avatar { principal_id: String },
    Provider,
    Administration { workspace_id: Option<String> },
    Mcp { workspace_id: Option<String> },
    Skills { workspace_id: Option<String> },
    Settings,
    OnboardingInvitation,
    AgentsDocument { workspace_id: String },
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
pub struct ClientEffectPlan {
    operation_id: ClientOperationId,
    generation: ClientGeneration,
    effect: ClientEffect,
}

impl ClientEffectPlan {
    pub fn new(
        operation_id: ClientOperationId,
        generation: ClientGeneration,
        effect: ClientEffect,
    ) -> Self {
        Self {
            operation_id,
            generation,
            effect,
        }
    }

    pub fn operation_id(&self) -> &ClientOperationId {
        &self.operation_id
    }

    pub const fn generation(&self) -> ClientGeneration {
        self.generation
    }

    pub const fn effect(&self) -> &ClientEffect {
        &self.effect
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientEffectResult {
    Completed,
    Failed { code: String },
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
}

impl SubscriptionQueue {
    fn push(&self, sequence: ClientChangeSequence, publication: ClientPublicationReference) {
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
        if let Some(core) = self.core.upgrade() {
            core.subscribers
                .lock()
                .expect("client subscriber registry poisoned")
                .remove(&self.id);
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

#[derive(Default)]
struct ClientPartitions {
    transition_sequence: ClientTransitionSequence,
    change_sequence: ClientChangeSequence,
    latest_change_sequence: Option<ClientChangeSequence>,
    publications: HashMap<ClientScope, Arc<ClientSnapshot>>,
    demands: HashMap<ClientScope, DemandState>,
    operations: HashMap<ClientOperationId, OperationState>,
}

/// The one process-local mutable owner for newly shared client state.
pub struct ClientCore {
    compatibility_runtime: ClientRuntime,
    partitions: Mutex<ClientPartitions>,
    subscribers: Mutex<HashMap<u64, Weak<SubscriptionQueue>>>,
    next_subscription_id: Mutex<u64>,
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
            partitions: Mutex::new(ClientPartitions::default()),
            subscribers: Mutex::new(HashMap::new()),
            next_subscription_id: Mutex::new(0),
        }
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
        self.subscribers
            .lock()
            .expect("client subscriber registry poisoned")
            .insert(id, Arc::downgrade(&queue));
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
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        partitions.transition_sequence.advance();
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
        Self::transition_without_publication(&partitions, outcome)
    }

    pub fn complete_effect(&self, completion: ClientEffectCompletion) -> ClientTransition {
        self.finish_effect(completion.operation_id(), completion.generation(), false)
    }

    pub fn cancel_effect(&self, cancellation: ClientEffectCancellation) -> ClientTransition {
        self.finish_effect(cancellation.operation_id(), cancellation.generation(), true)
    }

    fn finish_effect(
        &self,
        operation_id: &ClientOperationId,
        generation: ClientGeneration,
        cancelled: bool,
    ) -> ClientTransition {
        let mut partitions = self.partitions.lock().expect("client partitions poisoned");
        partitions.transition_sequence.advance();
        let outcome = match partitions.operations.get(operation_id).copied() {
            None => ClientTransitionOutcome::Rejected,
            Some(OperationState::Pending(current)) if current != generation => {
                ClientTransitionOutcome::Stale
            }
            Some(OperationState::Pending(_)) => {
                partitions.operations.insert(
                    operation_id.clone(),
                    if cancelled {
                        OperationState::Cancelled(generation)
                    } else {
                        OperationState::Completed(generation)
                    },
                );
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
        {
            let mut partitions = self.partitions.lock().expect("client partitions poisoned");
            partitions.transition_sequence.advance();

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
            self.deliver_change_set(transition.changes().as_ref());
            transition
        }
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
}
