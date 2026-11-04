//! Subscription-scoped post-open obligations and ephemeral pending-delivery order.
use crate::session::{ConnectionId, SessionOutbound};
use anyhow::{Result, anyhow};
use futures_util::FutureExt;
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

// Protective local budgets. No queue: exhaustion is an explicit pre-reply
// admission error. Completed joins are reaped on admission, never accumulated.
const MAX_REPLAYS_PER_CONNECTION: usize = 8;
const MAX_CAPTURE_SCOPES: usize = 64;
const MAX_ORDERED_REQUESTS: usize = 4096;

#[derive(Debug)]
struct SubscriptionState {
    live: bool,
    replay: bool,
    committed: bool,
    replayed: bool,
}
pub(crate) struct SubscriptionIncarnation {
    pub(crate) connection: ConnectionId,
    identity: super::ThreadSubscriptionIdentity,
    delivery_failure: Mutex<Option<SessionOutbound>>,
    state: Mutex<SubscriptionState>,
    cancel: CancellationToken,
}
impl std::fmt::Debug for SubscriptionIncarnation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionIncarnation")
            .field("connection", &self.connection)
            .finish_non_exhaustive()
    }
}
impl SubscriptionIncarnation {
    pub(crate) fn new(
        connection: ConnectionId,
        identity: super::ThreadSubscriptionIdentity,
    ) -> Arc<Self> {
        Arc::new(Self {
            connection,
            identity,
            delivery_failure: Mutex::new(None),
            state: Mutex::new(SubscriptionState {
                live: true,
                replay: false,
                committed: false,
                replayed: false,
            }),
            cancel: CancellationToken::new(),
        })
    }
    pub(crate) async fn run<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        REPLAY_SCOPE.scope(self.clone(), future).await
    }
    pub(crate) fn retire(&self) {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).live = false;
        self.cancel.cancel();
    }
    fn subject_matches(&self, principal: &crate::auth::AuthenticatedSessionPrincipal) -> bool {
        self.identity.principal_id == principal.principal_id
            && self.identity.session_id == principal.session_id
    }
    fn fail(&self) {
        // Retirement and the terminal outbound decision share this lock. Late
        // failure of a retired token cannot close a replacement on the socket.
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.live {
            state.live = false;
            if let Some(outbound) = self
                .delivery_failure
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
            {
                outbound.abort();
            }
        }
        drop(state);
        self.cancel.cancel();
    }
    pub(crate) fn admit_reply<T>(
        &self,
        principal: &crate::auth::AuthenticatedSessionPrincipal,
        enqueue: impl FnOnce() -> T,
    ) -> Option<T> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.live || !self.subject_matches(principal) {
            return None;
        }
        Some(enqueue())
    }
    fn claim(&self) -> Result<bool> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        anyhow::ensure!(state.live, "subscription retired");
        if state.replayed {
            return Ok(false);
        }
        if state.replay {
            anyhow::ensure!(state.committed, "post-open reply handoff is not committed");
            return Ok(false);
        }
        state.replay = true;
        state.committed = false;
        Ok(true)
    }
    fn release(&self, complete: bool) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.replay = false;
        state.committed = false;
        state.replayed |= complete;
    }
}

tokio::task_local! {
    static REPLAY_SCOPE: Arc<SubscriptionIncarnation>;
    static OPENED_SCOPE: OpenedProducer;
    static RESOLVED_SCOPE: ResolvedProducer;
}
/// All gates are synchronous and retained through the actual nonblocking enqueue.
/// LIVE collaborator deliveries deliberately need no selected-workspace gate.
pub(crate) fn admit_delivery<T>(
    connection: ConnectionId,
    method: &str,
    principal: &crate::auth::AuthenticatedSessionPrincipal,
    enqueue: impl FnOnce() -> T,
) -> Option<T> {
    let subscription = REPLAY_SCOPE.try_with(Arc::clone).ok();
    let subscription_state = subscription
        .as_ref()
        .map(|token| token.state.lock().unwrap_or_else(|p| p.into_inner()));
    if subscription
        .as_ref()
        .is_some_and(|token| token.connection != connection || !token.subject_matches(principal))
        || subscription_state.as_ref().is_some_and(|state| !state.live)
    {
        return None;
    }
    let producer = if matches!(
        method,
        pioneer_protocol::constants::events::TURN_PERMISSION_REQUEST_OPENED
            | pioneer_protocol::constants::events::CLI_RUNTIME_REQUEST_OPENED
    ) {
        OPENED_SCOPE.try_with(Clone::clone).ok()
    } else {
        None
    };
    let terminal = producer
        .as_ref()
        .map(|producer| producer.stamp.lock().unwrap_or_else(|p| p.into_inner()));
    if terminal.as_ref().is_some_and(|stamp| stamp.terminal)
        || producer
            .as_ref()
            .and_then(|producer| producer.expires_at_unix_ms)
            .is_some_and(|deadline| chrono::Utc::now().timestamp_millis() >= deadline)
    {
        return None;
    }
    let resolved = if matches!(
        method,
        pioneer_protocol::constants::events::TURN_PERMISSION_REQUEST_RESOLVED
            | pioneer_protocol::constants::events::CLI_RUNTIME_REQUEST_RESOLVED
    ) {
        RESOLVED_SCOPE.try_with(Clone::clone).ok()
    } else {
        None
    };
    let mut resolved_stamp = resolved
        .as_ref()
        .map(|producer| producer.stamp.lock().unwrap_or_else(|p| p.into_inner()));
    if resolved
        .as_ref()
        .zip(resolved_stamp.as_ref())
        .is_some_and(|(producer, stamp)| !Arc::ptr_eq(&producer.revision, &stamp.revision))
    {
        return None;
    }
    if let Some(stamp) = resolved_stamp.as_mut() {
        stamp.terminal = true;
    }
    Some(enqueue())
}

pub(crate) fn fail_current() {
    let _ = REPLAY_SCOPE.try_with(|token| token.fail());
}

#[derive(Default)]
struct DeliveryStamp {
    terminal: bool,
    revision: Arc<()>,
}
#[derive(Clone)]
pub(crate) struct OpenedProducer {
    stamp: Arc<Mutex<DeliveryStamp>>,
    expires_at_unix_ms: Option<i64>,
}
#[derive(Clone)]
pub(crate) struct ResolvedProducer {
    stamp: Arc<Mutex<DeliveryStamp>>,
    revision: Arc<()>,
}
impl ResolvedProducer {
    pub(crate) async fn run<F: Future>(&self, future: F) -> F::Output {
        RESOLVED_SCOPE.scope(self.clone(), future).await
    }
}
impl OpenedProducer {
    pub(crate) fn expires_at(mut self, deadline: i64) -> Self {
        self.expires_at_unix_ms = Some(deadline);
        self
    }
    /// Capture BEFORE the durable read used for a replay correction. A later
    /// LIVE terminal revision invalidates that read at actual enqueue.
    pub(crate) fn correction(&self) -> ResolvedProducer {
        let revision = self
            .stamp
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .revision
            .clone();
        ResolvedProducer {
            stamp: self.stamp.clone(),
            revision,
        }
    }
    pub(crate) async fn run<F: Future>(&self, future: F) -> F::Output {
        OPENED_SCOPE.scope(self.clone(), future).await
    }
}
struct OrderEntry {
    weak: Weak<Mutex<DeliveryStamp>>,
    capture_hold: Option<Arc<Mutex<DeliveryStamp>>>,
}
#[derive(Default)]
struct OrderState {
    requests: HashMap<String, OrderEntry>,
    captures: HashMap<u64, Weak<SubscriptionIncarnation>>,
    next: u64,
}
#[derive(Default)]
pub(crate) struct PendingDeliveryOrder(Mutex<OrderState>);
impl PendingDeliveryOrder {
    fn request(&self, id: &str) -> Option<OpenedProducer> {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        state
            .requests
            .retain(|_, entry| entry.weak.strong_count() != 0);
        if let Some(existing) = state
            .requests
            .get(id)
            .and_then(|entry| entry.weak.upgrade())
        {
            return Some(OpenedProducer {
                stamp: existing,
                expires_at_unix_ms: None,
            });
        }
        if id.len() > 256 || state.requests.len() >= MAX_ORDERED_REQUESTS {
            // Captures may have observed any unknown ID. Retire exactly those
            // obligations instead of dropping their terminal fences and ACKing loss.
            for token in state.captures.values().filter_map(Weak::upgrade) {
                token.fail();
            }
            return None;
        }
        let value = Arc::new(Mutex::new(DeliveryStamp::default()));
        let capture_hold = (!state.captures.is_empty()).then(|| value.clone());
        state.requests.insert(
            id.to_owned(),
            OrderEntry {
                weak: Arc::downgrade(&value),
                capture_hold,
            },
        );
        Some(OpenedProducer {
            stamp: value,
            expires_at_unix_ms: None,
        })
    }
    pub(crate) fn opened(&self, id: &str) -> Option<OpenedProducer> {
        self.request(id)
    }
    pub(crate) fn terminal(&self, id: &str) -> Option<ResolvedProducer> {
        let producer = self.request(id)?;
        let mut stamp = producer.stamp.lock().unwrap_or_else(|p| p.into_inner());
        stamp.terminal = true;
        stamp.revision = Arc::new(());
        Some(ResolvedProducer {
            stamp: producer.stamp.clone(),
            revision: stamp.revision.clone(),
        })
    }
    pub(crate) fn capture(
        self: &Arc<Self>,
        token: &Arc<SubscriptionIncarnation>,
    ) -> Result<Capture> {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        anyhow::ensure!(
            state.captures.len() < MAX_CAPTURE_SCOPES,
            "post-open capture capacity exhausted"
        );
        state.next = state
            .next
            .checked_add(1)
            .ok_or_else(|| anyhow!("capture generation exhausted"))?;
        let id = state.next;
        state.captures.insert(id, Arc::downgrade(token));
        // Existing opened producers can outlive this capture and vice versa.
        for entry in state.requests.values_mut() {
            entry.capture_hold = entry.weak.upgrade();
        }
        Ok(Capture {
            order: self.clone(),
            id,
        })
    }
}
pub(crate) struct Capture {
    order: Arc<PendingDeliveryOrder>,
    id: u64,
}
impl Drop for Capture {
    fn drop(&mut self) {
        let mut state = self.order.0.lock().unwrap_or_else(|p| p.into_inner());
        state.captures.remove(&self.id);
        if state.captures.is_empty() {
            for entry in state.requests.values_mut() {
                entry.capture_hold = None;
            }
            state
                .requests
                .retain(|_, entry| entry.weak.strong_count() != 0);
        }
    }
}

#[derive(Default)]
struct TaskState {
    tasks: JoinSet<()>,
    reserved: usize,
    stopped: bool,
}
#[derive(Default)]
pub(crate) struct ReplayTasks(Mutex<TaskState>);
impl ReplayTasks {
    #[cfg(test)]
    pub(crate) async fn drain(&self) {
        let mut tasks = {
            let mut state = self.0.lock().unwrap();
            std::mem::take(&mut state.tasks)
        };
        while tasks.join_next().await.is_some() {}
    }
    pub(crate) fn stop(&self) {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        state.stopped = true;
        state.tasks.abort_all();
    }
    pub(crate) fn reserve(
        self: &Arc<Self>,
        token: Arc<SubscriptionIncarnation>,
        outbound: SessionOutbound,
        order: &Arc<PendingDeliveryOrder>,
    ) -> Result<ReplayReservation> {
        *token
            .delivery_failure
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(outbound.clone());
        let owns_replay = token.claim()?;
        let admission = (|| {
            let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
            while state.tasks.try_join_next().is_some() {}
            anyhow::ensure!(
                !state.stopped && state.tasks.len() + state.reserved < MAX_REPLAYS_PER_CONNECTION,
                "post-open replay capacity exhausted"
            );
            let capture = if owns_replay {
                Some(order.capture(&token)?)
            } else {
                None
            };
            state.reserved += 1;
            Ok(capture)
        })();
        match admission {
            Ok(capture) => Ok(ReplayReservation {
                owner: self.clone(),
                token,
                capture,
                owns_replay,
                acknowledged: false,
                started: false,
            }),
            Err(error) => {
                if owns_replay {
                    token.release(false);
                }
                Err(error)
            }
        }
    }
}
pub(crate) struct ReplayReservation {
    owner: Arc<ReplayTasks>,
    token: Arc<SubscriptionIncarnation>,
    capture: Option<Capture>,
    owns_replay: bool,
    acknowledged: bool,
    started: bool,
}
struct RunningReplay {
    owns_replay: bool,
    token: Arc<SubscriptionIncarnation>,
    complete: bool,
}
impl Drop for RunningReplay {
    fn drop(&mut self) {
        // A live obligation cannot disappear on panic/drop: terminal socket
        // teardown forces recovery from durable pending state on reconnect.
        if !self.complete {
            self.token.fail();
        }
        if self.owns_replay {
            self.token.release(self.complete);
        }
    }
}
impl ReplayReservation {
    pub(crate) fn includes_pending(&self) -> bool {
        self.owns_replay
    }
    pub(crate) fn acknowledge(&mut self) {
        self.acknowledged = true;
        if self.owns_replay {
            self.token
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .committed = true;
        }
    }
    pub(crate) fn start<F: Future<Output = ()> + Send + 'static>(mut self, future: F) {
        self.acknowledge();
        let mut state = self.owner.0.lock().unwrap_or_else(|p| p.into_inner());
        state.reserved -= 1;
        self.started = true;
        let token = self.token.clone();
        let capture = self.capture.take();
        let running = RunningReplay {
            owns_replay: self.owns_replay,
            token: token.clone(),
            complete: false,
        };
        if state.stopped {
            return;
        }
        state.tasks.spawn(async move {
            // Move the whole guard, not just its `complete` field. Otherwise
            // precise closure capture leaves the Drop owner outside the task.
            // Construct it before spawn so dropping an unpolled task recovers too.
            let mut running = running;
            let _capture = capture;
            let cancelled = token.cancel.clone();
            let work = REPLAY_SCOPE.scope(token, future);
            let result = std::panic::AssertUnwindSafe(async {
                tokio::select! { _ = cancelled.cancelled() => false, _ = work => true }
            })
            .catch_unwind()
            .await;
            running.complete = matches!(result, Ok(true));
            if result.is_err() {
                tracing::warn!("post-open replay panicked; socket recovery required");
            }
            drop(running);
        });
    }
}
impl Drop for ReplayReservation {
    fn drop(&mut self) {
        if !self.started {
            if self.acknowledged {
                self.token.fail();
            }
            self.owner
                .0
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .reserved -= 1;
            if self.owns_replay {
                self.token.release(false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::test_support::{authenticated_test_superuser, outbound_channel};
    use axum::extract::ws::Message;
    use pioneer_protocol::constants::events;

    fn token(connection: ConnectionId) -> Arc<SubscriptionIncarnation> {
        let principal = authenticated_test_superuser();
        SubscriptionIncarnation::new(
            connection,
            super::super::ThreadSubscriptionIdentity::new(
                principal.principal_id.clone(),
                principal.session_id.clone(),
            ),
        )
    }

    #[tokio::test]
    async fn terminal_and_incarnation_gates_linearize_at_real_fifo_admission() {
        let principal = authenticated_test_superuser();
        let old = token(1);
        let order = Arc::new(PendingDeliveryOrder::default());
        let capture = order.capture(&old).unwrap();
        let producer = order.opened("request").unwrap();
        let (outbound, mut receiver) = outbound_channel(8);
        // Captured OPENED survives a terminal publication and is then polled.
        let _ = order.terminal("request");
        old.run(producer.run(async {
            assert!(
                admit_delivery(
                    1,
                    events::TURN_PERMISSION_REQUEST_OPENED,
                    &principal,
                    || outbound.try_send_data(Message::Text("late-opened".into()))
                )
                .is_none()
            );
            admit_delivery(
                1,
                events::TURN_PERMISSION_REQUEST_RESOLVED,
                &principal,
                || outbound.try_send_data(Message::Text("allow_once".into())),
            )
            .unwrap()
            .unwrap();
        }))
        .await;
        drop(capture);
        old.retire();
        let replacement = token(1);
        old.run(async {
            assert!(
                admit_delivery(1, events::CLI_RUNTIME_REQUEST_OPENED, &principal, || {
                    outbound.try_send_data(Message::Text("old-subscription".into()))
                })
                .is_none()
            );
        })
        .await;
        let other_session =
            crate::session::test_support::authenticated_test_superuser_secondary_session();
        replacement
            .run(async {
                assert!(
                    admit_delivery(
                        1,
                        events::CLI_RUNTIME_REQUEST_OPENED,
                        &other_session,
                        || outbound.try_send_data(Message::Text("retired-subject".into()))
                    )
                    .is_none()
                );
            })
            .await;
        replacement
            .run(async {
                admit_delivery(1, events::CLI_RUNTIME_REQUEST_OPENED, &principal, || {
                    outbound.try_send_data(Message::Text("current-subscription".into()))
                })
                .unwrap()
                .unwrap();
            })
            .await;
        assert!(
            matches!(receiver.recv().await, Some(Message::Text(value)) if value == "allow_once")
        );
        assert!(
            matches!(receiver.recv().await, Some(Message::Text(value)) if value == "current-subscription")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn stale_terminal_correction_and_expired_opened_are_rejected_at_enqueue() {
        let principal = authenticated_test_superuser();
        let order = PendingDeliveryOrder::default();
        let opened = order.opened("request").unwrap();
        let correction = opened.correction(); // Before its durable read.
        let latest = order.terminal("request").unwrap();
        let (outbound, mut receiver) = outbound_channel(8);
        latest
            .run(async {
                admit_delivery(
                    1,
                    events::TURN_PERMISSION_REQUEST_RESOLVED,
                    &principal,
                    || outbound.try_send_data(Message::Text("actual-terminal".into())),
                )
                .unwrap()
                .unwrap();
            })
            .await;
        correction
            .run(async {
                assert!(
                    admit_delivery(
                        1,
                        events::TURN_PERMISSION_REQUEST_RESOLVED,
                        &principal,
                        || outbound.try_send_data(Message::Text("stale-terminal".into()))
                    )
                    .is_none()
                );
            })
            .await;
        let expired = order.opened("expired").unwrap().expires_at(0);
        expired
            .run(async {
                assert!(
                    admit_delivery(1, events::CLI_RUNTIME_REQUEST_OPENED, &principal, || {
                        outbound.try_send_data(Message::Text("expired-opened".into()))
                    })
                    .is_none()
                );
            })
            .await;
        assert!(
            matches!(receiver.recv().await, Some(Message::Text(value)) if value == "actual-terminal")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn ordered_capture_capacity_fails_explicitly_and_retired_capture_releases_holds() {
        let order = Arc::new(PendingDeliveryOrder::default());
        let owner = Arc::new(ReplayTasks::default());
        let (outbound, _receiver) = outbound_channel(8);
        let current = token(1);
        let reservation = owner.reserve(current, outbound.clone(), &order).unwrap();
        for index in 0..MAX_ORDERED_REQUESTS {
            let _ = order.terminal(&format!("terminal-{index}"));
        }
        assert!(order.opened("over-capacity").is_none());
        assert!(
            outbound
                .try_send_data(Message::Text("recover explicitly".into()))
                .is_err()
        );
        drop(reservation);
        assert!(
            order.opened("after-retirement").is_some(),
            "no permanent terminal tombstones"
        );
    }

    #[tokio::test]
    async fn global_capture_saturation_is_retryable_before_handoff() {
        let order = Arc::new(PendingDeliveryOrder::default());
        let membership = token(1);
        let mut captures = vec![];
        for _ in 0..MAX_CAPTURE_SCOPES {
            captures.push(order.capture(&membership).unwrap());
        }
        assert!(order.capture(&membership).is_err());
        drop(captures.pop());
        assert!(order.capture(&membership).is_ok());
    }

    #[tokio::test]
    async fn replay_reservation_is_bounded_deduplicated_and_drop_releases_capacity() {
        let owner = Arc::new(ReplayTasks::default());
        let order = Arc::new(PendingDeliveryOrder::default());
        let (outbound, _receiver) = outbound_channel(8);
        let first = token(1);
        let reservation = owner
            .reserve(first.clone(), outbound.clone(), &order)
            .unwrap();
        assert!(
            owner
                .reserve(first.clone(), outbound.clone(), &order)
                .is_err(),
            "an uncommitted pre-reply reservation cannot justify another success"
        );
        let mut reservations = vec![reservation];
        for _ in 2..=8 {
            reservations.push(owner.reserve(token(1), outbound.clone(), &order).unwrap());
        }
        let ninth = token(1);
        assert!(
            owner
                .reserve(ninth.clone(), outbound.clone(), &order)
                .is_err()
        );
        drop(reservations.pop());
        let admitted = owner.reserve(ninth, outbound.clone(), &order).unwrap();
        drop(admitted);
        drop(reservations);
        // Pre-response cancellation must permit the same membership to retry.
        assert!(owner.reserve(first, outbound, &order).is_ok());
    }

    #[tokio::test]
    async fn dropping_acknowledged_handoff_forces_recovery_but_retired_handoff_cannot_close_replacement()
     {
        for retired in [false, true] {
            let owner = Arc::new(ReplayTasks::default());
            let order = Arc::new(PendingDeliveryOrder::default());
            let (outbound, _receiver) = outbound_channel(8);
            let old = token(1);
            let mut reservation = owner
                .reserve(old.clone(), outbound.clone(), &order)
                .unwrap();
            reservation.acknowledge();
            if retired {
                old.retire();
            }
            drop(reservation);
            assert_eq!(
                outbound
                    .try_send_data(Message::Text("current socket".into()))
                    .is_ok(),
                retired
            );
        }
    }

    #[tokio::test]
    async fn panic_shutdown_and_old_cleanup_do_not_leave_successful_orphan_work() {
        for panic in [false, true] {
            let owner = Arc::new(ReplayTasks::default());
            let order = Arc::new(PendingDeliveryOrder::default());
            let (outbound, _receiver) = outbound_channel(8);
            let (entered, waiting) = tokio::sync::oneshot::channel();
            let reservation = owner.reserve(token(1), outbound.clone(), &order).unwrap();
            reservation.start(async move {
                let _ = entered.send(());
                if panic {
                    panic!("injected replay failure");
                }
                std::future::pending::<()>().await;
            });
            waiting.await.unwrap();
            if !panic {
                owner.stop();
            }
            owner.drain().await;
            assert!(
                outbound
                    .try_send_data(Message::Text("must reconnect".into()))
                    .is_err()
            );
        }
        let owner = Arc::new(ReplayTasks::default());
        let order = Arc::new(PendingDeliveryOrder::default());
        let (outbound, _receiver) = outbound_channel(8);
        let old = token(1);
        let (entered, waiting) = tokio::sync::oneshot::channel();
        owner
            .reserve(old.clone(), outbound.clone(), &order)
            .unwrap()
            .start(async move {
                let _ = entered.send(());
                std::future::pending::<()>().await;
            });
        waiting.await.unwrap();
        old.retire();
        let replacement = token(1);
        owner
            .reserve(replacement, outbound.clone(), &order)
            .unwrap()
            .start(async {});
        owner.drain().await;
        outbound
            .try_send_data(Message::Text("replacement is live".into()))
            .unwrap();
    }
}
