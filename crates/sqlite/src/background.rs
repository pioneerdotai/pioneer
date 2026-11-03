//! Memory-only coordination for independent durable owners. This module knows
//! nothing about SQL, domain eligibility, claims, or acknowledgement rights.
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::time::Instant;

// Only an owner executing its own bounded quantum may suppress its own hint.
// Cross-owner effects still invalidate their independent generations.
#[derive(Clone)]
struct Origin {
    wake: Arc<Wake>,
    lease: u64,
}
tokio::task_local! { static ORIGIN: Origin; }

/// Existing owned child-task boundaries carry the producer origin so their
/// successful own ACKs do not invalidate their parent drain. The lease fence
/// makes a late child from a replaced owner a new producer, never its ACK.
pub fn inherit_origin<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let origin = ORIGIN.try_with(Clone::clone).ok();
    async move {
        match origin {
            Some(origin) => ORIGIN.scope(origin, future).await,
            None => future.await,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Plan {
    Runnable,
    Dormant,
    WallDeadline(SystemTime),
    MonotonicDeadline(Instant),
    Deadlines {
        wall: SystemTime,
        monotonic: Instant,
    },
}
impl Plan {
    /// A real monotonic retry must not hide an independent wall-clock
    /// obligation. Mixed waits retain the SQL-free wall/resume sampling bound.
    pub fn with_monotonic_deadline(self, at: Instant) -> Self {
        match self {
            Self::Runnable => Self::Runnable,
            Self::Dormant => Self::MonotonicDeadline(at),
            Self::WallDeadline(wall) => Self::Deadlines {
                wall,
                monotonic: at,
            },
            Self::MonotonicDeadline(old) => Self::MonotonicDeadline(old.min(at)),
            Self::Deadlines { wall, monotonic } => Self::Deadlines {
                wall,
                monotonic: monotonic.min(at),
            },
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ready {
    Work,
    SettlementRequired,
    Closed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Observation(u64);

struct State {
    generation: u64,
    // Distinguish this owner's ambiguous mutation from a concurrent producer.
    // A failed own quantum must retain its bounded error retry even though
    // Change::drop advanced the settlement generation.
    external_generation: u64,
    in_flight: usize,
    unsettled: bool,
    dirty: bool,
    active: bool,
    owner_lease: u64,
    closed: bool,
    plan: Plan,
    // A completed read while a producer is still in flight may guide waiting,
    // but cannot confirm Empty or erase dirty/settlement debt. Retain at most
    // one such result so a continuous producer stream neither starves visible
    // backlog nor causes repeated Empty SQL for the same committed generation.
    provisional: Option<(u64, Plan)>,
    retry: Option<(u64, Instant)>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            generation: 0,
            external_generation: 0,
            in_flight: 0,
            unsettled: false,
            dirty: true,
            active: false,
            owner_lease: 0,
            closed: false,
            plan: Plan::Runnable,
            provisional: None,
            retry: None,
        }
    }
}
impl State {
    fn advance(&mut self) {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("background generation exhausted");
    }
    fn advance_external(&mut self) {
        self.advance();
        self.external_generation = self.generation;
    }
}

pub struct Wake {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
    drain: Arc<tokio::sync::Semaphore>,
    #[cfg(any(test, feature = "test-support"))]
    wall_clock: Mutex<Option<SystemTime>>,
}
impl Default for Wake {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            changed: watch::channel(0).0,
            drain: Arc::new(tokio::sync::Semaphore::new(1)),
            #[cfg(any(test, feature = "test-support"))]
            wall_clock: Mutex::new(None),
        }
    }
}
#[derive(Default)]
pub struct Registry(Mutex<HashMap<&'static str, Arc<Wake>>>);
impl Drop for Registry {
    fn drop(&mut self) {
        // Owners retain only Wake, not this physical-runtime registry. Closing
        // the runtime must therefore release dormant futures even without a
        // subsequent producer or SQL timer. No registry lock during fanout.
        let owners = std::mem::take(self.0.get_mut().unwrap());
        for (_, wake) in owners {
            wake.close();
        }
    }
}
impl Registry {
    /// Keys are static domain-owner names, never row IDs or payloads.
    pub fn owner(&self, name: &'static str) -> Arc<Wake> {
        self.0.lock().unwrap().entry(name).or_default().clone()
    }
}
impl Wake {
    /// A bounded quantum may also be requested by an existing foreground API.
    /// Its admission token shares the owner's one active drain, without holding
    /// the memory-state mutex or any database capacity across domain work.
    pub async fn drain(self: &Arc<Self>) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.drain.clone().acquire_owned().await.ok()
    }
    pub fn wall_now(&self) -> SystemTime {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(now) = *self.wall_clock.lock().unwrap() {
            return now;
        }
        SystemTime::now()
    }
    /// Test clock changes do not invalidate inputs: exercise real clock waits.
    #[cfg(feature = "test-support")]
    pub fn set_wall_clock_for_test(&self, at: SystemTime) {
        *self.wall_clock.lock().unwrap() = Some(at);
    }
    fn announce(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }
    fn origin_lease(&self) -> Option<u64> {
        ORIGIN
            .try_with(|origin| {
                (Arc::as_ptr(&origin.wake) == self as *const Self).then_some(origin.lease)
            })
            .ok()
            .flatten()
    }
    fn is_origin(&self) -> bool {
        let Some(lease) = self.origin_lease() else {
            return false;
        };
        let state = self.state.lock().unwrap();
        state.active && !state.closed && state.owner_lease == lease
    }
    /// Whether a long-lived consumer currently owns this addressed domain.
    /// A synchronous producer may use this to hand an explicit foreground kick
    /// to that owner instead of starting a competing drain.
    pub fn requires_settlement(&self) -> bool {
        self.state.lock().unwrap().unsettled
    }
    pub fn has_owner(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.active && !state.closed
    }
    pub fn invalidate(&self) {
        if self.is_origin() {
            return;
        }
        self.dependency_released();
    }
    /// An actual dependency release can enable an earlier blocked candidate
    /// even inside the owner's quantum. Unlike its own ACK, it must invalidate
    /// the observation; no DB work or capacity is held by this signal.
    pub fn dependency_released(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.advance_external();
            state.dirty = true;
        }
        self.announce();
    }
    /// Call before admission to a potentially mutating operation. Known outcomes
    /// call finish after DB capacity is released. Drop means ambiguous, not rollback.
    pub fn changing(self: &Arc<Self>) -> Change {
        let origin = self.origin_lease();
        self.state.lock().unwrap().in_flight += 1;
        Change {
            wake: self.clone(),
            origin,
            finished: false,
        }
    }
    /// One long-lived owner lease, not one task spawned per invalidation.
    pub fn acquire(self: &Arc<Self>) -> Option<Owner> {
        let receiver = self.changed.subscribe();
        let mut state = self.state.lock().unwrap();
        if state.closed || state.active {
            return None;
        }
        state.active = true;
        state.owner_lease = state
            .owner_lease
            .checked_add(1)
            .expect("owner lease exhausted");
        let lease = state.owner_lease;
        Some(Owner {
            wake: self.clone(),
            receiver,
            lease,
        })
    }
    /// A retained bounded domain traversal may fence its cursor/deadline cache
    /// to committed inputs without acquiring a second long-lived owner lease.
    /// This is only a hint observation; it grants no durable mutation rights.
    pub fn observe(&self) -> Observation {
        Observation(self.state.lock().unwrap().generation)
    }
    /// Retained cursors shared with foreground drains need the same producer
    /// fence as the owner, without acquiring another owner lease. Own ACKs
    /// suppressed by the existing origin lease do not request a rescan.
    pub fn producer_changed_since(&self, observation: Observation) -> bool {
        self.state.lock().unwrap().external_generation > observation.0
    }
    pub fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.drain.close();
        self.announce();
    }
}

/// Wait on existing process-local dependency locks, without spawning tasks or
/// retaining a DB handle/permit. Duplicate scopes coalesce. Acquiring a lock is
/// only a release hint; the domain must still revalidate durable authority.
pub async fn wait_for_mutex_release(
    gates: impl IntoIterator<Item = Arc<tokio::sync::Mutex<()>>>,
) -> Arc<tokio::sync::Mutex<()>> {
    use futures_util::{StreamExt, stream::FuturesUnordered};
    let mut identities = std::collections::HashSet::new();
    let mut waits = FuturesUnordered::new();
    for gate in gates {
        if identities.insert(Arc::as_ptr(&gate) as usize) {
            waits.push(async move {
                let guard = gate.clone().lock_owned().await;
                drop(guard);
                gate
            });
        }
    }
    match waits.next().await {
        Some(gate) => gate,
        None => std::future::pending().await,
    }
}

pub struct Change {
    wake: Arc<Wake>,
    origin: Option<u64>,
    finished: bool,
}
impl Change {
    /// `changed` is a domain outcome, not writer release or mere SQL success.
    /// Known rollback/no-change passes false. All calls occur after commit/rollback.
    pub fn finish(mut self, changed: bool) {
        self.finished = true;
        {
            let mut state = self.wake.state.lock().unwrap();
            state.in_flight -= 1;
            let own = self.origin == Some(state.owner_lease) && state.active && !state.closed;
            if changed && !own {
                state.advance_external();
                state.dirty = true;
            }
        }
        self.wake.announce();
    }
}
impl Drop for Change {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        {
            let mut state = self.wake.state.lock().unwrap();
            state.in_flight -= 1;
            let own = self.origin == Some(state.owner_lease) && state.active && !state.closed;
            if own {
                state.advance();
            } else {
                state.advance_external();
            }
            state.dirty = true;
            state.unsettled = true;
        }
        self.wake.announce();
    }
}

pub struct Owner {
    wake: Arc<Wake>,
    receiver: watch::Receiver<u64>,
    lease: u64,
}
impl Owner {
    pub fn requires_settlement(&self) -> bool {
        self.wake.state.lock().unwrap().unsettled
    }

    pub fn observe(&self) -> Observation {
        self.wake.observe()
    }
    /// The caller must first complete an authoritative settlement operation
    /// behind prior ambiguous writer work. A fresh read alone cannot establish it.
    pub fn settled(&self, observation: Observation) -> bool {
        let mut state = self.wake.state.lock().unwrap();
        if !state.active
            || state.closed
            || state.owner_lease != self.lease
            || state.generation != observation.0
            || state.in_flight != 0
        {
            return false;
        }
        state.unsettled = false;
        true
    }
    /// Retained domain cursors can distinguish their own failed operation from
    /// a new producer while preserving one bounded error cooldown. This is a
    /// process-local hint fence, never durable mutation/settlement authority.
    pub fn producer_changed_since(&self, observation: Observation) -> bool {
        self.wake.producer_changed_since(observation)
    }
    /// Snapshot publication cannot clear a concurrent commit or in-flight write.
    pub fn publish(&self, observation: Observation, plan: Plan) -> bool {
        let mut state = self.wake.state.lock().unwrap();
        if !state.active
            || state.closed
            || state.owner_lease != self.lease
            || state.generation != observation.0
        {
            return false;
        }
        if state.in_flight != 0 {
            state.provisional = Some((observation.0, plan));
            return false;
        }
        if state.unsettled {
            return false;
        }
        state.dirty = false;
        state.retry = None;
        state.provisional = None;
        state.plan = plan;
        true
    }
    /// Error recovery does not publish Empty or clear ambiguous debt. New
    /// producer generations interrupt this retry deadline immediately.
    pub fn retry_after(&self, observation: Observation, delay: Duration) -> bool {
        let mut state = self.wake.state.lock().unwrap();
        if !state.active || state.closed || state.owner_lease != self.lease {
            return false;
        }
        if state.generation != observation.0
            && !(state.unsettled && state.external_generation <= observation.0)
        {
            return false;
        }
        state.retry = Some((state.generation, Instant::now() + delay));
        true
    }

    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        ORIGIN
            .scope(
                Origin {
                    wake: self.wake.clone(),
                    lease: self.lease,
                },
                future,
            )
            .await
    }
    /// Dormant has no timer. The only clock sampling below belongs to an actual
    /// future wall-clock obligation. It never runs SQL or wakes dormant siblings.
    pub async fn ready(&mut self) -> Ready {
        loop {
            let retry = {
                let state = self.wake.state.lock().unwrap();
                if state.closed {
                    return Ready::Closed;
                }
                state.retry.filter(|(generation, at)| {
                    *generation == state.generation && *at > Instant::now()
                })
            };
            if let Some((_, at)) = retry {
                tokio::select! {
                    _ = self.receiver.changed() => {},
                    _ = tokio::time::sleep_until(at) => {},
                }
                continue;
            }
            let plan = {
                let state = self.wake.state.lock().unwrap();
                if state.closed {
                    return Ready::Closed;
                }
                if state.in_flight != 0 {
                    // One observed result per committed generation. Runnable
                    // continuation and an actual deadline remain serviceable
                    // while unrelated writes are pending. A provisional Empty
                    // waits for their completion, never authorizes parking.
                    state
                        .provisional
                        .filter(|(generation, _)| *generation == state.generation)
                        .map_or(Some(Plan::Runnable), |(_, plan)| Some(plan))
                } else if state.unsettled {
                    return Ready::SettlementRequired;
                } else if state.dirty {
                    Some(Plan::Runnable)
                } else {
                    Some(state.plan)
                }
            };
            match plan {
                Some(Plan::Runnable) => {
                    tokio::task::yield_now().await;
                    return Ready::Work;
                }
                Some(Plan::WallDeadline(at)) => {
                    let remaining = match at.duration_since(self.wake.wall_now()) {
                        Ok(remaining) if !remaining.is_zero() => remaining,
                        _ => {
                            tokio::task::yield_now().await;
                            return Ready::Work;
                        }
                    };
                    // SQL-free wall-clock/resume re-evaluation only while a real
                    // deadline exists; no periodic successful-Empty discovery.
                    let pause = remaining.min(Duration::from_secs(30));
                    tokio::select! {
                        _ = self.receiver.changed() => {},
                        _ = tokio::time::sleep(pause) => {},
                    }
                }
                Some(Plan::MonotonicDeadline(at)) => {
                    if at <= Instant::now() {
                        tokio::task::yield_now().await;
                        return Ready::Work;
                    }
                    tokio::select! {
                        _ = self.receiver.changed() => {},
                        _ = tokio::time::sleep_until(at) => {
                            tokio::task::yield_now().await;
                            return Ready::Work;
                        },
                    }
                }
                Some(Plan::Deadlines { wall, monotonic }) => {
                    let wall_remaining = wall
                        .duration_since(self.wake.wall_now())
                        .unwrap_or_default();
                    let mono_remaining = monotonic.saturating_duration_since(Instant::now());
                    if wall_remaining.is_zero() || mono_remaining.is_zero() {
                        tokio::task::yield_now().await;
                        return Ready::Work;
                    }
                    let pause = wall_remaining
                        .min(mono_remaining)
                        .min(Duration::from_secs(30));
                    tokio::select! {
                        _ = self.receiver.changed() => {},
                        _ = tokio::time::sleep(pause) => {},
                    }
                }
                None | Some(Plan::Dormant) => {
                    let _ = self.receiver.changed().await;
                }
            }
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        {
            let mut state = self.wake.state.lock().unwrap();
            state.active = false;
            // Cancellation/reacquisition reconstructs; no stale owner clears debt.
            state.advance_external();
            state.dirty = true;
        }
        self.wake.announce();
    }
}

/// Existing domain owners with retained Notify handoffs can reuse the same
/// wall/resume protocol without a second owner or a domain SQL polling timer.
/// Called only while a concrete deadline exists; cancellation drops this wait.
pub async fn wait_until_wall(at: SystemTime) {
    loop {
        let remaining = match at.duration_since(SystemTime::now()) {
            Ok(remaining) if !remaining.is_zero() => remaining,
            _ => return,
        };
        tokio::time::sleep(remaining.min(Duration::from_secs(30))).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn coalescing_generation_settlement_independence_and_shutdown() {
        let registry = Registry::default();
        let a = registry.owner("a");
        let b = registry.owner("b");
        let mut owner = a.acquire().unwrap();
        assert!(a.acquire().is_none());
        let mut sibling = b.acquire().unwrap();
        let quantum = a.drain().await.unwrap();
        assert!(a.drain().now_or_never().is_none());
        let independent_quantum = b.drain().await.unwrap();
        drop(independent_quantum);
        drop(quantum);
        assert!(sibling.publish(sibling.observe(), Plan::Dormant));
        let old = owner.observe();
        for _ in 0..1000 {
            a.changing().finish(true);
        }
        assert!(!owner.publish(old, Plan::Dormant));
        assert_eq!(owner.ready().await, Ready::Work);
        let mut in_flight: Vec<_> = (0..1000).map(|_| a.changing()).collect();
        in_flight.pop().unwrap().finish(true);
        assert_eq!(
            owner.ready().await,
            Ready::Work,
            "committed backlog is serviceable before all other producers finish"
        );
        let visible = owner.observe();
        assert!(!owner.publish(visible, Plan::Runnable));
        assert_eq!(
            owner.ready().await,
            Ready::Work,
            "bounded continuation is not starved by in-flight producers"
        );
        assert!(!owner.publish(visible, Plan::Dormant));
        assert!(
            owner.ready().now_or_never().is_none(),
            "provisional Empty never loops on SQL while the same generation is in flight"
        );
        in_flight.pop().unwrap().finish(true);
        assert_eq!(
            owner.ready().await,
            Ready::Work,
            "a concurrent actual commit supersedes provisional Empty"
        );
        for change in in_flight {
            change.finish(false);
        }
        assert!(owner.publish(owner.observe(), Plan::Dormant));
        let change = a.changing();
        assert!(!owner.publish(owner.observe(), Plan::Dormant));
        drop(change);
        assert_eq!(owner.ready().await, Ready::SettlementRequired);
        let pending = owner.observe();
        a.changing().finish(true);
        assert!(!owner.settled(pending));
        assert!(owner.settled(owner.observe()));
        assert!(owner.publish(owner.observe(), Plan::Dormant));
        assert!(owner.ready().now_or_never().is_none());
        assert!(sibling.ready().now_or_never().is_none());
        a.invalidate(); // commit-before-wait retained even without a waiting future
        assert_eq!(owner.ready().await, Ready::Work);
        owner
            .scope(async {
                a.changing().finish(true);
            })
            .await;
        assert!(owner.publish(owner.observe(), Plan::Dormant));
        assert!(owner.ready().now_or_never().is_none());
        let delayed_commit = owner
            .scope(async {
                inherit_origin(async {
                    a.changing().finish(true);
                })
            })
            .await;
        drop(owner);
        let mut replacement = a.acquire().unwrap();
        assert_eq!(replacement.ready().await, Ready::Work);
        let empty_replacement = replacement.observe();
        assert!(replacement.publish(empty_replacement, Plan::Dormant));
        delayed_commit.await;
        assert!(
            !replacement.publish(empty_replacement, Plan::Dormant),
            "old child commit is not the replacement owner's ACK"
        );
        assert_eq!(replacement.ready().await, Ready::Work);
        assert!(replacement.publish(
            replacement.observe(),
            Plan::MonotonicDeadline(Instant::now() + Duration::from_secs(3600))
        ));
        assert!(replacement.ready().now_or_never().is_none());
        a.invalidate();
        assert_eq!(replacement.ready().await, Ready::Work);
        let before_own_unknown = replacement.observe();
        let abandoned = replacement.scope(async { a.changing() }).await;
        drop(abandoned);
        assert_eq!(replacement.ready().await, Ready::SettlementRequired);
        assert!(
            replacement.retry_after(before_own_unknown, Duration::from_secs(3600)),
            "own ambiguous completion advances settlement but retains its error cooldown"
        );
        assert!(replacement.ready().now_or_never().is_none());
        a.invalidate(); // new work interrupts error retry but cannot erase settlement
        assert!(
            !replacement.retry_after(before_own_unknown, Duration::from_secs(3600)),
            "an old error cannot postpone a new producer generation"
        );
        assert_eq!(replacement.ready().await, Ready::SettlementRequired);
        a.close();
        assert_eq!(replacement.ready().await, Ready::Closed);
        drop(replacement);
        assert!(a.acquire().is_none());
        assert!(sibling.ready().now_or_never().is_none());
        // Control the sampled wall clock privately; leave the actual OS clock
        // untouched. An already-pending wait must resample after rollback and
        // forward/resume instead of trusting its original monotonic deadline.
        tokio::time::pause();
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        *b.wall_clock.lock().unwrap() = Some(wall);
        assert!(sibling.publish(
            sibling.observe(),
            Plan::WallDeadline(wall + Duration::from_secs(3600))
        ));
        let mut waiting = Box::pin(sibling.ready());
        assert!(waiting.as_mut().now_or_never().is_none());
        *b.wall_clock.lock().unwrap() = Some(wall - Duration::from_secs(3600));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(
            waiting.as_mut().now_or_never().is_none(),
            "rollback must not make work due"
        );
        *b.wall_clock.lock().unwrap() = Some(wall + Duration::from_secs(7200));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(
            waiting.await,
            Ready::Work,
            "forward jump/resume must interrupt the original distant deadline without a DB scan"
        );
        *b.wall_clock.lock().unwrap() = Some(wall - Duration::from_secs(3600));
        assert!(
            sibling.publish(
                sibling.observe(),
                Plan::WallDeadline(wall + Duration::from_secs(86_400))
                    .with_monotonic_deadline(Instant::now() + Duration::from_secs(2))
            )
        );
        let mut mixed = Box::pin(sibling.ready());
        assert!(mixed.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            mixed.await,
            Ready::Work,
            "rollback cannot postpone a real error cooldown"
        );
        // Companion to mixed rollback above: the later monotonic retry must
        // not conceal an independent wall-clock forward jump.
        *b.wall_clock.lock().unwrap() = Some(wall);
        assert!(
            sibling.publish(
                sibling.observe(),
                Plan::WallDeadline(wall + Duration::from_secs(3600))
                    .with_monotonic_deadline(Instant::now() + Duration::from_secs(7200))
            )
        );
        let mut mixed_forward = Box::pin(sibling.ready());
        assert!(mixed_forward.as_mut().now_or_never().is_none());
        *b.wall_clock.lock().unwrap() = Some(wall + Duration::from_secs(7200));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(
            mixed_forward.await,
            Ready::Work,
            "an independent wall forward must not wait for the monotonic retry"
        );
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let held = gate.clone().lock_owned().await;
        let mut release = Box::pin(wait_for_mutex_release([gate.clone(), gate.clone()]));
        assert!(release.as_mut().now_or_never().is_none());
        drop(held);
        let released = release.await;
        assert!(Arc::ptr_eq(&gate, &released));
        assert!(
            gate.try_lock().is_ok(),
            "release hint retains no dependency guard"
        );
        assert!(sibling.publish(sibling.observe(), Plan::Dormant));
        drop(registry);
        assert_eq!(
            sibling.ready().await,
            Ready::Closed,
            "physical runtime teardown closes dormant owners without polling"
        );
    }
}
