//! Passive evidence attached to the existing single navigation/open-intent owner.
//! Tickets identify observations only; business fences remain authoritative.
use super::*;
use pioneer_observability::{
    live::WaitKind,
    opening::{Consent, Fact, Measure, Phase, Result as ObservedResult},
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct OpeningTicket {
    token: Arc<()>,
    correlation: Correlation,
}
impl OpeningTicket {
    pub fn correlation(&self) -> Correlation {
        self.correlation.clone()
    }
}
impl PartialEq for OpeningTicket {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.token, &other.token)
    }
}
impl Eq for OpeningTicket {}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitialHistory {
    Empty,
    Nonempty,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComposerReadiness {
    Ready,
    SetupRequired,
    ReadOnly,
    AuthRequired,
    Busy,
    VoiceDisabled,
    Degraded,
}
impl ComposerReadiness {
    fn fact(self) -> Fact {
        match self {
            Self::Ready => Fact::CoreReady,
            Self::SetupRequired => Fact::SetupRequired,
            Self::ReadOnly => Fact::ReadOnly,
            Self::AuthRequired => Fact::AuthRequired,
            Self::Busy => Fact::Busy,
            Self::VoiceDisabled => Fact::VoiceDisabled,
            Self::Degraded => Fact::Degraded,
        }
    }
}
#[derive(Clone, Debug)]
pub struct OpeningEvidence {
    pub ticket: OpeningTicket,
    pub navigation_revision: u64,
    pub store_generation: Option<u64>,
    pub transport_current: bool,
    pub subscription_acknowledged: bool,
    pub initial_history: Option<InitialHistory>,
    pub initial_history_request_generation: Option<u64>,
    pub composer_core: Option<ComposerReadiness>,
    pub native_inputs: Option<ComposerReadiness>,
    pub draft_id: Option<crate::composer::store::DraftId>,
    pub core_composer_revision: Option<u64>,
    pub composer_revision: Option<u64>,
    pub native_session_transport_revision: Option<u64>,
    pub capability_generation: Option<u64>,
    pub authorization_generation: (u64, u64),
    pub authorization_ticket: u64,
    pub transport_generation: Option<u64>,
    pub subscription_transport_revision: Option<u64>,
    /// Whole-screen presentation remains unknown; structure is a narrower fact.
    pub presentation_observed: Option<bool>,
    /// Exact accepted GPUI empty/list structure, never all row content or screen readiness.
    pub content_structure_observed: Option<(u64, u64)>,
}
struct Episode {
    token: Arc<()>,
    retired: bool,
    admitted: bool,
    core_input: std::sync::Weak<crate::composer::store::ComposerPublication>,
    core_identity:
        std::sync::Weak<crate::gateway::identity_authorization::IdentityAuthorizationPublication>,
    core_capability: std::sync::Weak<crate::threads::capabilities::ThreadCapabilityPublication>,
    core_session: std::sync::Weak<crate::gateway::session_controller::GatewaySessionPublication>,
    native_revision: Option<u64>,
    content_structure: Option<(u64, u64)>,
    capability_generation: Option<u64>,
    correlation: Correlation,
    consent: Consent,
    measure: Option<Measure>,
    subscription: Option<Measure>,
    history: Option<Measure>,
    core_wait: Option<Measure>,
    native_wait: Option<Measure>,
    acknowledged: bool,
    initial_history: Option<InitialHistory>,
    core: Option<ComposerReadiness>,
    native: Option<ComposerReadiness>,
}
#[derive(Clone)]
pub(crate) struct Handle(Arc<Mutex<Episode>>);
impl Handle {
    fn start(source: Fact) -> Self {
        let measure = Measure::start(Phase::Intent, &Correlation::capture(), &[]);
        measure.fact(source);
        measure.fact(Fact::PresentationUnknown);
        Self(Arc::new(Mutex::new(Episode {
            token: Arc::new(()),
            retired: false,
            admitted: false,
            core_input: std::sync::Weak::new(),
            core_identity: std::sync::Weak::new(),
            core_capability: std::sync::Weak::new(),
            core_session: std::sync::Weak::new(),
            native_revision: None,
            content_structure: None,
            capability_generation: None,
            correlation: measure.correlation(),
            consent: measure.consent(),
            measure: Some(measure),
            subscription: None,
            history: None,
            core_wait: None,
            native_wait: None,
            acknowledged: false,
            initial_history: None,
            core: None,
            native: None,
        })))
    }
    pub(crate) fn correlation(&self) -> Correlation {
        self.0
            .lock()
            .expect("opening observation poisoned")
            .correlation
            .clone()
    }
    pub(crate) fn finish(&self, result: ObservedResult) {
        let mut owner = self.0.lock().expect("opening observation poisoned");
        owner.retired = true;
        if let Some(mut measure) = owner.measure.take() {
            measure.finish(result);
        }
        fn finish(pending: &mut Option<Measure>, result: ObservedResult) {
            if let Some(mut measure) = pending.take() {
                measure.finish(result);
            }
        }
        finish(&mut owner.subscription, result);
        finish(&mut owner.history, result);
        finish(&mut owner.core_wait, result);
        finish(&mut owner.native_wait, result);
    }
    pub(crate) fn handed_off(&self) {
        self.0
            .lock()
            .expect("opening observation poisoned")
            .admitted = true;
    }
    pub(crate) fn finish_unadmitted(&self) {
        let admitted = self
            .0
            .lock()
            .expect("opening observation poisoned")
            .admitted;
        if !admitted {
            self.finish(ObservedResult::Rejected);
        }
    }
    pub(crate) fn fact(&self, fact: Fact) {
        if let Some(measure) = &self.0.lock().expect("opening observation poisoned").measure {
            measure.fact(fact);
        }
    }
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
pub(crate) struct Selection {
    handle: Handle,
    target: Option<String>,
    auth: (u64, Option<u64>),
    authority: (u64, u64),
    navigation: Option<u64>,
    generation: Option<u64>,
    history_request: Option<u64>,
}
impl Drop for Selection {
    fn drop(&mut self) {
        self.handle.finish(ObservedResult::Unconfirmed);
    }
}
#[derive(Clone)]
pub(super) struct HistoryEvidence {
    pub generation: u64,
    pub auth: (u64, Option<u64>),
    pub authority: (u64, u64),
    pub history: InitialHistory,
    pub request_generation: u64,
    pub socket: Option<crate::transport::ws::runtime::ConnectionScope>,
}

impl Episode {
    fn inputs_unknown(&mut self) {
        self.core = None;
        self.native = None;
        self.native_revision = None;
        self.content_structure = None;
    }
    fn settle(&mut self) {
        if self.acknowledged && self.initial_history.is_some() && self.core.is_some() {
            if let Some(mut measure) = self.measure.take() {
                measure.finish(if self.native.is_some() {
                    ObservedResult::NativeObserved
                } else {
                    ObservedResult::CoreObserved
                });
            }
        }
    }
}
impl ThreadRegistry {
    pub(crate) fn opening_target_retired(&mut self, id: &str) {
        if self
            .opening
            .as_ref()
            .is_some_and(|owner| owner.target.as_deref() == Some(id))
        {
            self.retire_opening(ObservedResult::Superseded);
        }
    }
    pub(crate) fn retire_opening(&mut self, result: ObservedResult) {
        if let Some(owner) = self.opening.take() {
            owner.handle.finish(result);
        }
    }
    pub(crate) fn opening_navigation(&mut self, preserving: Option<&Handle>) {
        if self.opening.as_ref().is_some_and(|owner| {
            preserving.is_none_or(|handle| !owner.handle.same(handle) && owner.navigation.is_some())
        }) {
            self.retire_opening(ObservedResult::Superseded);
        }
    }
    pub(crate) fn accept_opening(&mut self, handle: &Handle) {
        let Some(owner) = self
            .opening
            .as_mut()
            .filter(|owner| owner.handle.same(handle))
        else {
            return;
        };
        let Some(target) = self.navigation.active_thread_id() else {
            handle.finish(ObservedResult::Rejected);
            return;
        };
        if owner.target.as_deref().is_some_and(|id| id != target) {
            handle.finish(ObservedResult::Rejected);
            return;
        }
        if target.len() > 1024 {
            handle.fact(Fact::CoverageRetired);
            handle.finish(ObservedResult::Unconfirmed);
            return;
        }
        handle.handed_off();
        owner.target = Some(target.to_owned());
        owner.navigation = Some(self.navigation_revision);
        owner.generation = self.stores.get(target).map(|store| store.generation);
        let pending_history = self.stores.get(target).and_then(|store| {
            store
                .opening_history_flight
                .as_ref()
                .and_then(|(request, _)| {
                    self.timeline
                        .accepts(
                            &SemanticTimelineRequestKey::ThreadNewest {
                                thread_id: target.to_owned(),
                            },
                            *request,
                        )
                        .then_some((store.generation, *request))
                })
        });
        let receipt_ready = self.stores.get(target).is_some_and(|store| {
            store.subscription_receipt.as_ref().is_some_and(|receipt| {
                receipt.scope.generation == store.generation
                    && receipt.scope.auth == owner.auth
                    && receipt.scope.authority == owner.authority
                    && *receipt.live.lock().expect("subscription receipt poisoned")
            })
        });
        let cached_history = self.stores.get(target).and_then(|store| {
            let receipt = store.subscription_receipt.as_ref()?;
            let history = store.opening_history.as_ref()?;
            (receipt_ready
                && receipt.scope.auth == owner.auth
                && receipt.scope.authority == owner.authority
                && history.generation == store.generation
                && history.auth == owner.auth
                && history.authority == owner.authority
                && history.socket == Some(receipt.scope.socket)
                && *receipt.live.lock().expect("subscription receipt poisoned"))
            .then_some((history.history, history.request_generation))
        });
        let mut episode = handle.0.lock().expect("opening observation poisoned");
        if let Some(measure) = &episode.measure {
            let context = measure.correlation();
            if episode.initial_history.is_none() {
                let mut history = Measure::start(Phase::InitialHistory, &context, &[]);
                history.wait_for(WaitKind::Reply);
                episode.history = Some(history);
            }
            if episode.core.is_none() {
                let mut core = Measure::start(Phase::Core, &context, &[]);
                core.wait_for(WaitKind::UserOrProvider);
                episode.core_wait = Some(core);
            }
            if episode.native.is_none() {
                let mut native = Measure::start(Phase::Native, &context, &[]);
                native.wait_for(WaitKind::QueueItem);
                episode.native_wait = Some(native);
            }
        }
        if receipt_ready && !episode.acknowledged {
            if let Some(measure) = &episode.measure {
                measure.fact(Fact::ReadyReceipt);
                measure.milestone(Fact::SubscriptionAcknowledged);
            }
            episode.acknowledged = true;
            if let Some(mut wait) = episode.subscription.take() {
                wait.finish(ObservedResult::Confirmed);
            }
        }
        if let Some((history, request_generation)) =
            cached_history.filter(|_| episode.initial_history.is_none())
        {
            owner.history_request = Some(request_generation);
            if let Some(measure) = &episode.measure {
                measure.milestone(if history == InitialHistory::Empty {
                    Fact::HistoryEmpty
                } else {
                    Fact::HistoryNonempty
                });
            }
            episode.initial_history = Some(history);
            if let Some(mut wait) = episode.history.take() {
                wait.finish(ObservedResult::Confirmed);
            }
        }
        let needs_history = episode.initial_history.is_none();
        episode.settle();
        drop(episode);
        if let Some((generation, request)) = pending_history.filter(|_| needs_history) {
            let target = target.to_owned();
            self.opening_history_request(&target, generation, request, true);
        }
    }
    pub(super) fn opening_subscription_failed(
        &mut self,
        request: &ThreadSubscriptionRequest,
        result: ObservedResult,
    ) {
        if let Some(owner) = self.opening.as_ref().filter(|owner| {
            owner.target.as_deref() == Some(&request.id)
                && owner.auth == request.auth
                && owner.authority == request.authority
                && owner
                    .generation
                    .is_none_or(|generation| generation == request.generation)
        }) {
            owner.handle.finish(result);
        }
    }
    pub(super) fn opening_subscription(
        &mut self,
        request: &ThreadSubscriptionRequest,
        ready: bool,
        fact: Fact,
    ) {
        let Some(owner) = self.opening.as_mut().filter(|owner| {
            owner.target.as_deref() == Some(&request.id)
                && owner.auth == request.auth
                && owner.authority == request.authority
                && owner
                    .generation
                    .is_none_or(|generation| generation == request.generation)
        }) else {
            return;
        };
        owner.generation = Some(request.generation);
        let mut episode = owner.handle.0.lock().expect("opening observation poisoned");
        let Some(measure) = &episode.measure else {
            return;
        };
        measure.fact(fact);
        if ready {
            if !episode.acknowledged {
                measure.milestone(Fact::SubscriptionAcknowledged);
                episode.acknowledged = true;
                if let Some(mut wait) = episode.subscription.take() {
                    wait.finish(ObservedResult::Confirmed);
                }
                episode.settle();
            }
        } else if episode.subscription.is_none() {
            let mut wait = Measure::start(
                Phase::SubscriptionPrerequisite,
                &measure.correlation(),
                &[request.correlation.clone()],
            );
            wait.wait_for(WaitKind::Reply);
            episode.subscription = Some(wait);
        }
    }
    pub(super) fn opening_history_request(
        &mut self,
        id: &str,
        generation: u64,
        request_generation: u64,
        joined: bool,
    ) {
        let cause = self
            .stores
            .get(id)
            .and_then(|store| store.opening_history_flight.as_ref())
            .filter(|(current, _)| *current == request_generation)
            .map(|(_, cause)| cause.clone());
        if let Some(owner) = self.opening.as_mut().filter(|owner| {
            owner.target.as_deref() == Some(id) && owner.generation == Some(generation)
        }) {
            let pending = owner
                .handle
                .0
                .lock()
                .expect("opening observation poisoned")
                .initial_history
                .is_none();
            if pending {
                if owner.history_request != Some(request_generation) {
                    let episode = owner.handle.0.lock().expect("opening observation poisoned");
                    if let Some(cause) = &cause {
                        let mut dependency = Measure::start_with_consent(
                            Phase::HistoryDependency,
                            &episode.correlation,
                            &[cause.clone()],
                            episode.consent,
                        );
                        dependency.finish(ObservedResult::Returned);
                    }
                }
                owner.history_request = Some(request_generation);
                if joined {
                    owner.handle.fact(Fact::HistoryJoined);
                }
            }
        }
    }
    pub(super) fn opening_initial_rejected(&mut self, id: &str, generation: u64) {
        if let Some(owner) = self.opening.as_ref().filter(|owner| {
            owner.target.as_deref() == Some(id) && owner.generation == Some(generation)
        }) {
            let pending = owner
                .handle
                .0
                .lock()
                .expect("opening observation poisoned")
                .initial_history
                .is_none();
            if pending {
                owner.handle.fact(Fact::HistoryRejected);
                owner.handle.finish(ObservedResult::Rejected);
            }
        }
    }
    pub(super) fn opening_history_failed(
        &mut self,
        id: &str,
        generation: u64,
        request_generation: u64,
        result: ObservedResult,
    ) {
        if let Some(owner) = self.opening.as_ref().filter(|owner| {
            owner.target.as_deref() == Some(id)
                && owner.generation == Some(generation)
                && owner.history_request == Some(request_generation)
        }) {
            let pending = owner
                .handle
                .0
                .lock()
                .expect("opening observation poisoned")
                .initial_history
                .is_none();
            if pending {
                owner.handle.fact(Fact::HistoryRejected);
                owner.handle.finish(result);
            }
        }
    }
    pub(super) fn opening_history_applied(
        &mut self,
        id: &str,
        generation: u64,
        request_generation: u64,
        history: InitialHistory,
        auth: (u64, Option<u64>),
        authority: (u64, u64),
        socket: Option<crate::transport::ws::runtime::ConnectionScope>,
    ) {
        if let Some(store) = self
            .stores
            .get_mut(id)
            .filter(|store| store.generation == generation)
        {
            store.opening_history = Some(HistoryEvidence {
                generation,
                auth,
                authority,
                history,
                request_generation,
                socket,
            });
        }
        let Some(owner) = self.opening.as_mut().filter(|owner| {
            owner.target.as_deref() == Some(id)
                && owner.generation == Some(generation)
                && owner.auth == auth
                && owner.authority == authority
                && owner.history_request == Some(request_generation)
        }) else {
            return;
        };
        let mut episode = owner.handle.0.lock().expect("opening observation poisoned");
        if !episode.retired && episode.initial_history.is_none() {
            if let Some(measure) = &episode.measure {
                measure.milestone(if history == InitialHistory::Empty {
                    Fact::HistoryEmpty
                } else {
                    Fact::HistoryNonempty
                });
            }
            episode.initial_history = Some(history);
            if let Some(mut wait) = episode.history.take() {
                wait.finish(ObservedResult::Confirmed);
            }
            episode.settle();
        }
    }
}

pub(crate) fn failure(error: &anyhow::Error) -> ObservedResult {
    match crate::rpc::json_rpc_response_error(error) {
        Some(crate::rpc::JsonRpcResponseError::DeadlineExceeded) => ObservedResult::Deadline,
        Some(crate::rpc::JsonRpcResponseError::Transport { .. }) => ObservedResult::TransportLost,
        Some(error) if error.authorization_failure().is_some() => {
            ObservedResult::AuthorizationDenied
        }
        Some(
            crate::rpc::JsonRpcResponseError::Server { .. }
            | crate::rpc::JsonRpcResponseError::InvalidResponse,
        ) => ObservedResult::Failed,
        None => ObservedResult::Rejected,
    }
}
impl ClientCore {
    pub(crate) fn observe_rejected_opening_intent(&self, intent: &crate::core::ClientIntent) {
        use crate::{
            core::ClientIntent,
            navigation::NavigationIntent,
            tasks::notifications::{TaskNotificationCompletion, TaskNotificationIntent},
            workspaces::intents::WorkspaceIntent,
        };
        let source = match intent {
            ClientIntent::Workspace {
                intent: WorkspaceIntent::SelectThread { .. },
            } => Some(Fact::Workspace),
            ClientIntent::Navigation {
                intent:
                    NavigationIntent::SelectThread {
                        thread_id: Some(_), ..
                    }
                    | NavigationIntent::PushTaskThread { .. }
                    | NavigationIntent::PopTaskThread,
                ..
            } => Some(Fact::Navigation),
            ClientIntent::TaskNotification {
                intent:
                    TaskNotificationIntent::Open { .. }
                    | TaskNotificationIntent::NativeCompletion {
                        completion: TaskNotificationCompletion::Activated,
                        ..
                    },
            } => Some(Fact::Task),
            _ => None,
        };
        if let Some(source) = source {
            self.begin_opening(None, source)
                .finish(ObservedResult::Rejected);
        }
    }
    pub fn observe_unavailable_task_selection(&self) {
        self.begin_opening(None, Fact::Task)
            .finish(ObservedResult::Rejected);
    }
    pub fn observe_unavailable_thread_selection(&self) {
        self.begin_opening(None, Fact::Navigation)
            .finish(ObservedResult::Rejected);
    }
    pub(crate) fn finish_pending_opening(&self, handle: &Handle) {
        let pending = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .opening
            .as_ref()
            .is_some_and(|owner| owner.handle.same(handle) && owner.navigation.is_none());
        if pending {
            handle.finish(ObservedResult::Unconfirmed);
        }
    }
    pub(crate) fn begin_opening(&self, target: Option<&str>, source: Fact) -> Handle {
        let handle = Handle::start(source);
        if !handle
            .0
            .lock()
            .expect("opening observation poisoned")
            .consent
            .current()
        {
            self.thread_registry
                .lock()
                .expect("thread registry poisoned")
                .retire_opening(ObservedResult::Unconfirmed);
            return handle;
        }
        if self.current_auth().is_none() {
            handle.fact(Fact::AuthRequired);
        }
        let auth = self.current_auth_ticket();
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let authority = identity.authorization_epoch();
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.retire_opening(ObservedResult::Superseded);
        if !identity.history_ticket_matches(auth.0) {
            handle.finish(ObservedResult::Superseded);
            return handle;
        }
        if !handle
            .0
            .lock()
            .expect("opening observation poisoned")
            .consent
            .current()
        {
            return handle;
        }
        // IDs are normally short opaque keys. Refuse diagnostic retention above
        // 1 KiB without rejecting or altering the actual selection.
        if target.is_some_and(|id| id.len() > 1024) {
            handle.fact(Fact::CoverageRetired);
            handle.finish(ObservedResult::Unconfirmed);
            return handle;
        }
        registry.opening = Some(Selection {
            handle: handle.clone(),
            target: target.map(str::to_owned),
            auth,
            authority,
            navigation: None,
            generation: None,
            history_request: None,
        });
        handle
    }
    pub fn opening_evidence(&self, id: &str) -> Option<OpeningEvidence> {
        if !pioneer_observability::telemetry_enabled() {
            return None;
        }
        self.observe_opening_core();
        let transport = crate::core::HistoryAuthority::capture(self);
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let owner = registry.opening.as_ref().filter(|owner| {
            owner.target.as_deref() == Some(id)
                && owner.navigation == Some(registry.navigation_revision)
                && registry.navigation.active_thread_id() == Some(id)
                && owner.auth == transport.observation_scope().0
                && identity.history_ticket_matches(owner.auth.0)
                && owner.authority == identity.authorization_epoch()
        })?;
        let (_transport_guard, current) = transport.lock_transport();
        if self.is_stopped() {
            return None;
        }
        let episode = owner.handle.0.lock().expect("opening observation poisoned");
        if episode.retired || !episode.consent.current() {
            return None;
        }
        Some(OpeningEvidence {
            ticket: OpeningTicket {
                token: episode.token.clone(),
                correlation: episode.correlation.clone(),
            },
            navigation_revision: registry.navigation_revision,
            store_generation: owner.generation,
            transport_current: current,
            subscription_acknowledged: current && episode.acknowledged,
            initial_history: current.then_some(episode.initial_history).flatten(),
            initial_history_request_generation: owner.history_request,
            composer_core: current.then_some(episode.core).flatten(),
            native_inputs: current.then_some(episode.native).flatten(),
            draft_id: episode.core_input.upgrade().map(|input| input.draft_id()),
            core_composer_revision: episode.core_input.upgrade().map(|input| input.revision()),
            composer_revision: current.then_some(episode.native_revision).flatten(),
            native_session_transport_revision: (current && episode.native.is_some())
                .then(|| {
                    episode
                        .core_session
                        .upgrade()
                        .map(|session| session.startup.transport_revision)
                })
                .flatten(),
            capability_generation: episode.capability_generation,
            authorization_generation: owner.authority,
            authorization_ticket: owner.auth.0,
            transport_generation: owner.auth.1,
            subscription_transport_revision: registry
                .stores
                .get(id)
                .and_then(|store| store.subscription_receipt.as_ref())
                .map(|receipt| receipt.scope.transport_revision),
            presentation_observed: None,
            content_structure_observed: (current
                && episode.core == Some(ComposerReadiness::Ready)
                && episode.native == Some(ComposerReadiness::Ready))
            .then_some(episode.content_structure)
            .flatten(),
        })
    }
    /// Called only by the common renderer's exact accepted empty/structure
    /// branch. This associates a local content fact; it never finishes Intent
    /// as screen-ready and never substitutes a row for the complete screen.
    pub fn observe_opening_content_structure(
        &self,
        id: &str,
        generation: u64,
        revision: u64,
        content: &Correlation,
    ) {
        let Some(evidence) = self.opening_evidence(id) else {
            return;
        };
        if !evidence.transport_current
            || !evidence.subscription_acknowledged
            || evidence.store_generation != Some(generation)
            || evidence.initial_history.is_none()
            || evidence.initial_history_request_generation.is_none()
            || evidence.composer_core != Some(ComposerReadiness::Ready)
            || evidence.native_inputs != Some(ComposerReadiness::Ready)
            || evidence.core_composer_revision != evidence.composer_revision
            || evidence.draft_id.is_none()
            || evidence.native_session_transport_revision.is_none()
            || !content.is_current()
        {
            return;
        }
        let transport = crate::core::HistoryAuthority::capture(self);
        let identity_guard = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let (_transport_guard, current_transport) = transport.lock_transport();
        if !current_transport
            || self.is_stopped()
            || !identity_guard.history_ticket_matches(evidence.authorization_ticket)
            || identity_guard.authorization_epoch() != evidence.authorization_generation
            || transport.observation_scope().0
                != (evidence.authorization_ticket, evidence.transport_generation)
        {
            return;
        }
        let Some(owner) = registry.opening.as_ref().filter(|owner| {
            owner.target.as_deref() == Some(id)
                && owner.navigation == Some(evidence.navigation_revision)
                && registry.navigation_revision == evidence.navigation_revision
                && owner.generation == Some(generation)
                && owner.history_request == evidence.initial_history_request_generation
                && owner.auth == (evidence.authorization_ticket, evidence.transport_generation)
                && owner.authority == evidence.authorization_generation
        }) else {
            return;
        };
        let mut episode = owner.handle.0.lock().expect("opening observation poisoned");
        if episode.retired
            || !episode.consent.current()
            || !Arc::ptr_eq(&episode.token, &evidence.ticket.token)
            || episode.native_revision != evidence.composer_revision
            || episode.core != Some(ComposerReadiness::Ready)
            || episode.native != Some(ComposerReadiness::Ready)
            || episode.content_structure.is_some()
        {
            return;
        }
        // Partition identity is checked while holding the existing registry
        // fence, so an obsolete prepared publication cannot acquire this ticket.
        let Some(input) = episode.core_input.upgrade() else {
            return;
        };
        let Some(identity) = episode.core_identity.upgrade() else {
            return;
        };
        let Some(session) = episode.core_session.upgrade() else {
            return;
        };
        let capability = episode.core_capability.upgrade();
        let runtime = input
            .runtime_selection()
            .and_then(|selection| self.provider_runtime_snapshot(&selection.workspace_id));
        if !self.opening_publications_current(
            id,
            &input,
            &identity,
            capability.as_ref(),
            &session,
            runtime.as_ref(),
        ) || !self.opening_content_current(id, generation, revision)
        {
            return;
        }
        episode.content_structure = Some((generation, revision));
        let parent = episode.correlation.clone();
        drop(episode);
        drop(registry);
        drop(_transport_guard);
        drop(identity_guard);
        let mut fact = pioneer_observability::scenario::Observation::start(
            pioneer_observability::scenario::Scenario::Opening,
            pioneer_observability::scenario::Boundary::Presentation,
            &parent,
            &[content.clone()],
        );
        // Returned deliberately does not claim whole-screen presentation.
        fact.finish(pioneer_observability::scenario::Outcome::Returned);
    }
    pub(crate) fn observe_opening_core(&self) {
        if !pioneer_observability::telemetry_enabled() {
            return;
        }
        let target = {
            self.thread_registry
                .lock()
                .expect("thread registry poisoned")
                .opening
                .as_ref()
                .filter(|owner| {
                    let episode = owner.handle.0.lock().expect("opening observation poisoned");
                    !episode.retired && episode.consent.current()
                })
                .and_then(|owner| owner.target.clone())
        };
        if let Some(target) = target {
            self.observe_opening_inputs(&target, None);
        }
    }
    /// Native callers pass only already applied inputs. No requests, demands or
    /// redraws are produced. This also exposes a real evidence consumer to 2-Г.
    pub fn observe_opening_native(
        &self,
        id: &str,
        ticket: &OpeningTicket,
        input: &crate::composer::store::ComposerPublication,
        identity: &Arc<crate::gateway::identity_authorization::IdentityAuthorizationPublication>,
        capability: Option<&Arc<crate::threads::capabilities::ThreadCapabilityPublication>>,
        session: &Arc<crate::gateway::session_controller::GatewaySessionPublication>,
        state: ComposerReadiness,
    ) {
        self.observe_opening_inputs(
            id,
            Some((ticket, input, identity, capability, session, state)),
        );
    }
    fn opening_inputs_unknown(&self, id: &str) {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if let Some(owner) = registry
            .opening
            .as_ref()
            .filter(|owner| owner.target.as_deref() == Some(id))
        {
            owner
                .handle
                .0
                .lock()
                .expect("opening observation poisoned")
                .inputs_unknown();
        }
    }
    fn observe_opening_inputs(
        &self,
        id: &str,
        native: Option<(
            &OpeningTicket,
            &crate::composer::store::ComposerPublication,
            &Arc<crate::gateway::identity_authorization::IdentityAuthorizationPublication>,
            Option<&Arc<crate::threads::capabilities::ThreadCapabilityPublication>>,
            &Arc<crate::gateway::session_controller::GatewaySessionPublication>,
            ComposerReadiness,
        )>,
    ) {
        if !pioneer_observability::telemetry_enabled() {
            return;
        }
        // Read existing publications first; compare them again under the existing
        // identity -> registry -> partitions order at the confirmation boundary.
        let Some(input) = self.composer_snapshot(id) else {
            self.opening_inputs_unknown(id);
            return;
        };
        let Some(identity) = self.snapshot(&ClientScope::Administration { workspace_id: None }).and_then(|snapshot| snapshot.typed::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>()).map(|snapshot| snapshot.payload()) else { self.opening_inputs_unknown(id); return; };
        let capability = self.thread_capability_snapshot(id);
        let runtime = input
            .runtime_selection()
            .and_then(|selection| self.provider_runtime_snapshot(&selection.workspace_id));
        let runtime_authorized = input
            .runtime_selection()
            .is_none_or(|selection| self.provider_runtime_data_valid(&selection.workspace_id));
        let session = self.gateway_session();
        let auth = self.current_auth_ticket();
        let transport_authority = crate::core::HistoryAuthority::capture(self);
        let identity_guard = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let revision = registry.navigation_revision;
        let active = registry.navigation.active_thread_id() == Some(id);
        let workspace = registry.navigation.workspace_id().map(str::to_owned);
        let generation = registry.stores.get(id).map(|store| store.generation);
        let Some(owner) = registry.opening.as_ref() else {
            return;
        };
        if !active || owner.target.as_deref() != Some(id) || owner.navigation != Some(revision) {
            return;
        }
        if self.is_stopped()
            || owner.auth != auth
            || !identity_guard.history_ticket_matches(auth.0)
            || owner.authority != identity_guard.authorization_epoch()
            || owner.generation != generation
        {
            registry.retire_opening(ObservedResult::Superseded);
            return;
        }
        let (_transport_guard, transport_current) = transport_authority.lock_transport();
        if !transport_current
            || !self.opening_publications_current(
                id,
                &input,
                &identity,
                capability.as_ref(),
                &session,
                runtime.as_ref(),
            )
        {
            owner
                .handle
                .0
                .lock()
                .expect("opening observation poisoned")
                .inputs_unknown();
            return;
        }
        let scoped_policy = capability.as_ref().and_then(|p| p.snapshot.as_ref());
        let fallback_policy = scoped_policy
            .is_none()
            .then(|| identity.capabilities.snapshot(workspace.as_deref(), None))
            .flatten();
        let policy = scoped_policy.or(fallback_policy.as_ref());
        let state = if identity.current_auth.is_none() {
            Some(ComposerReadiness::AuthRequired)
        } else if !session.startup.transport_ready {
            None
        } else {
            policy.and_then(|policy| {
                let mode = input.domain().selected_mode;
                let draft = workspace
                    .as_deref()
                    .is_some_and(|workspace| registry.navigation.draft(workspace) == Some(id));
                let write = if draft {
                    policy.workspace.as_ref()?.capabilities.can_create_thread
                } else {
                    let scoped = capability.as_ref()?;
                    let snapshot = scoped.snapshot.as_ref()?;
                    let permissions = crate::authorization::thread_presentation_capabilities(
                        snapshot.thread.as_ref().map(|thread| &thread.capabilities),
                    );
                    if mode == pioneer_protocol::ThreadMode::Message {
                        permissions.can_write
                    } else {
                        permissions.can_start_turn
                    }
                };
                if !write {
                    return Some(ComposerReadiness::ReadOnly);
                }
                let workspace_policy = policy.workspace.as_ref()?;
                if Some(workspace_policy.workspace_id.as_str()) != workspace.as_deref()
                    || input.authorization_fingerprint()
                        != Some(workspace_policy.execution_draft_policy.fingerprint.as_str())
                {
                    return None;
                }
                if mode != pioneer_protocol::ThreadMode::Message
                    && !crate::composer::model_selection::has_complete_composer_model_selection(
                        input.domain().selected_provider.as_deref(),
                        input.domain().selected_model.as_deref(),
                    )
                {
                    return Some(ComposerReadiness::SetupRequired);
                }
                if mode != pioneer_protocol::ThreadMode::Message
                    && !input.selected_permission_mode_allowed()
                {
                    return Some(ComposerReadiness::SetupRequired);
                }
                if mode != pioneer_protocol::ThreadMode::Message {
                    let selected_cli =
                        input.domain().selected_provider.as_deref().and_then(
                            crate::providers::list::runtime_id_from_cli_runtime_provider_key,
                        );
                    if let Some(selected) = selected_cli {
                        if !runtime_authorized {
                            return None;
                        }
                        let selection = input.runtime_selection().filter(|selection| {
                            selection.identity.draft_id == input.draft_id()
                                && selection.selected_provider == input.domain().selected_provider
                                && Some(selection.workspace_id.as_str()) == workspace.as_deref()
                        })?;
                        match &selection.request.state {
                            crate::composer::catalog::ComposerCatalogRequestState::Idle
                            | crate::composer::catalog::ComposerCatalogRequestState::Loading => {
                                return None;
                            }
                            crate::composer::catalog::ComposerCatalogRequestState::Failed {
                                ..
                            }
                            | crate::composer::catalog::ComposerCatalogRequestState::Cancelled => {
                                return Some(ComposerReadiness::Degraded);
                            }
                            crate::composer::catalog::ComposerCatalogRequestState::Ready => {}
                        }
                        let discovery = runtime.as_ref().filter(|runtime| {
                            Some(runtime.workspace_id()) == workspace.as_deref()
                        })?;
                        if *discovery.request()
                            != crate::providers::runtime::ProviderRuntimeRequestState::Ready
                        {
                            return None;
                        }
                        let row = discovery
                            .runtimes()
                            .iter()
                            .find(|row| row.runtime().runtime_id == selected);
                        let ready = row.is_some_and(|row| {
                            crate::providers::list::provider_ready_for_model_selector(
                                input.domain().selected_provider.as_deref(),
                                std::slice::from_ref(row.runtime()),
                            )
                        });
                        if input.selected_provider_ready() != ready {
                            return None;
                        }
                        if row.is_some_and(|row| {
                            row.runtime().status
                                == crate::providers::types::RuntimeStatus::Initializing
                        }) {
                            return None;
                        }
                        if !ready {
                            return Some(ComposerReadiness::SetupRequired);
                        }
                    }
                }
                if input
                    .operation()
                    .is_some_and(|operation| operation.pending())
                    || registry
                        .stores
                        .get(id)
                        .is_some_and(|store| !store.coordinator.conversation.can_submit_message())
                {
                    return Some(ComposerReadiness::Busy);
                }
                if input
                    .voice_readiness()
                    .and_then(|voice| voice.response.as_ref())
                    .is_some_and(|voice| voice.status == pioneer_protocol::VoiceStatus::Disabled)
                {
                    return Some(ComposerReadiness::VoiceDisabled);
                }
                Some(ComposerReadiness::Ready)
            })
        };
        let mut episode = owner.handle.0.lock().expect("opening observation poisoned");
        if episode.retired || !episode.consent.current() {
            return;
        }
        let same_inputs = episode.core_input.upgrade().is_some_and(|previous| {
            previous.has_same_submission_prerequisites(&input)
                && previous.domain().selected_permission_mode
                    == input.domain().selected_permission_mode
                && previous.selected_permission_mode_allowed()
                    == input.selected_permission_mode_allowed()
        });
        let same_identity = episode
            .core_identity
            .upgrade()
            .is_some_and(|previous| Arc::ptr_eq(&previous, &identity));
        let same_capability = match (episode.core_capability.upgrade(), capability.as_ref()) {
            (Some(a), Some(b)) => Arc::ptr_eq(&a, b),
            (None, None) => true,
            _ => false,
        };
        let same_session = episode
            .core_session
            .upgrade()
            .is_some_and(|previous| Arc::ptr_eq(&previous, &session));
        if !same_inputs || !same_identity || !same_capability || !same_session {
            episode.native = None;
            episode.native_revision = None;
            episode.content_structure = None;
        }
        episode.core_input = Arc::downgrade(&input);
        episode.core_identity = Arc::downgrade(&identity);
        episode.core_session = Arc::downgrade(&session);
        episode.core_capability = capability.as_ref().map(Arc::downgrade).unwrap_or_default();
        episode.capability_generation = capability.as_ref().map(|capability| capability.generation);
        let Some(state) = state else {
            // Pending replacement is not the previously confirmed input set.
            episode.inputs_unknown();
            return;
        };
        if episode.core != Some(state) {
            if let Some(measure) = &episode.measure {
                measure.milestone(state.fact());
            }
            episode.core = Some(state);
            episode.native = None;
            episode.native_revision = None;
            episode.content_structure = None;
            if let Some(mut wait) = episode.core_wait.take() {
                wait.finish(ObservedResult::Confirmed);
            }
        }
        if let Some((
            ticket,
            applied,
            applied_identity,
            applied_capability,
            applied_session,
            native_state,
        )) = native
        {
            let current_ticket = OpeningTicket {
                token: episode.token.clone(),
                correlation: episode.correlation.clone(),
            };
            if ticket != &current_ticket
                || applied.revision() != input.revision()
                || !applied.has_same_submission_prerequisites(&input)
                || applied.domain().selected_permission_mode
                    != input.domain().selected_permission_mode
                || applied.selected_permission_mode_allowed()
                    != input.selected_permission_mode_allowed()
                || !Arc::ptr_eq(applied_identity, &identity)
                || !Arc::ptr_eq(applied_session, &session)
                || match (applied_capability, capability.as_ref()) {
                    (Some(a), Some(b)) => !Arc::ptr_eq(a, b),
                    (None, None) => false,
                    _ => true,
                }
            {
                return;
            }
            // A shell may report a stricter local busy/voice/read-only state, but
            // it may not upgrade a non-ready core to Ready.
            if native_state == ComposerReadiness::Ready && state != ComposerReadiness::Ready {
                return;
            }
            if episode.native != Some(native_state)
                || episode.native_revision != Some(applied.revision())
            {
                episode.content_structure = None;
                episode.native = Some(native_state);
                episode.native_revision = Some(applied.revision());
                if let Some(mut wait) = episode.native_wait.take() {
                    wait.milestone(Fact::NativeApplied);
                    wait.fact(native_state.fact());
                    wait.finish(ObservedResult::Confirmed);
                }
            }
        }
        episode.settle();
    }
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OpeningHandle")
    }
}
