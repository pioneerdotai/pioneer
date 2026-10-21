//! Stable Tokio counters and independent responsiveness probes. No unstable
//! scheduling metrics and no inference that poll/busy wall time is CPU time.
use crate::identity::monotonic_ms;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
const MAX_RUNTIMES: usize = 32;
const PROBE_DEADLINE_MS: u64 = 10_000;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRole {
    Gateway,
    ClientWs,
    ClientHttp,
    ClientController,
    GatewayHook,
    GatewayPatch,
    DesktopHelper,
    CliHelper,
}
impl RuntimeRole {
    pub(crate) const ALL: [Self; 8] = [
        Self::Gateway,
        Self::ClientWs,
        Self::ClientHttp,
        Self::ClientController,
        Self::GatewayHook,
        Self::GatewayPatch,
        Self::DesktopHelper,
        Self::CliHelper,
    ];
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::ClientWs => "client_ws",
            Self::ClientHttp => "client_http",
            Self::ClientController => "client_controller",
            Self::GatewayHook => "gateway_hook",
            Self::GatewayPatch => "gateway_patch",
            Self::DesktopHelper => "desktop_helper",
            Self::CliHelper => "cli_helper",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    pub id: u64,
    pub role: RuntimeRole,
    pub snapshot_ms: u64,
    pub workers: usize,
    pub alive_tasks: usize,
    pub global_queue_depth: usize,
    pub probe_requested_ms: Option<u64>,
    pub probe_acknowledged_ms: Option<u64>,
    pub responsiveness: String,
    pub counter_scope: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForegroundSnapshot {
    pub supported: bool,
    pub sampled_ms: u64,
    pub requested_ms: Option<u64>,
    pub acknowledged_ms: Option<u64>,
    pub responsiveness: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeSnapshots {
    pub schema_version: u32,
    pub snapshot_ms: u64,
    pub runtimes: Vec<RuntimeSnapshot>,
    pub foreground: ForegroundSnapshot,
    pub registration_overflow: u64,
    pub watchdog_ticks: u64,
    pub watchdog_last_ms: Option<u64>,
    pub watchdog_late_ticks: u64,
    pub foreign_workers: String,
}
struct Probe {
    request: AtomicU64,
    ack: AtomicU64,
    ack_at: AtomicU64,
    epoch: AtomicU64,
}
impl Probe {
    fn new() -> Self {
        Self {
            request: AtomicU64::new(0),
            ack: AtomicU64::new(0),
            ack_at: AtomicU64::new(0),
            epoch: AtomicU64::new(u64::MAX),
        }
    }
    fn status(&self, now: u64, epoch: u64) -> (&'static str, Option<u64>, Option<u64>) {
        if self.epoch.load(Ordering::Acquire) != epoch {
            return ("not_recorded", None, None);
        }
        let request = self.request.load(Ordering::Acquire);
        let ack = self.ack.load(Ordering::Acquire);
        (
            if request == 0 {
                "idle_no_expected_probe"
            } else if ack >= request {
                "responsive"
            } else if now.saturating_sub(request) > PROBE_DEADLINE_MS {
                "expected_probe_overdue"
            } else {
                "probe_pending"
            },
            (request != 0).then_some(request),
            (ack != 0).then(|| self.ack_at.load(Ordering::Acquire)),
        )
    }
}
struct RuntimeEntry {
    role: RuntimeRole,
    metrics: tokio::runtime::RuntimeMetrics,
    handle: tokio::runtime::Handle,
    probe: Arc<Probe>,
    pending: Option<tokio::task::JoinHandle<()>>,
    continuously_driven: bool,
    drive_leases: usize,
}
struct ForegroundEntry {
    probe: Arc<Probe>,
    sender: tokio::sync::watch::Sender<u64>,
}
struct Registry {
    next: u64,
    runtimes: BTreeMap<u64, RuntimeEntry>,
    foreground: Option<ForegroundEntry>,
    overflow: u64,
    ticks: u64,
    last_tick: Option<u64>,
    late: u64,
}
static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
fn registry() -> std::sync::MutexGuard<'static, Registry> {
    REGISTRY
        .get_or_init(|| {
            Mutex::new(Registry {
                next: 1,
                runtimes: BTreeMap::new(),
                foreground: None,
                overflow: 0,
                ticks: 0,
                last_tick: None,
                late: 0,
            })
        })
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
pub struct RuntimeRegistration {
    id: Option<u64>,
}
/// Registration is constant startup metadata even with consent disabled;
/// counters/probes are read ONLY by the consent-gated independent sampler.
pub fn register(handle: &tokio::runtime::Handle, role: RuntimeRole) -> RuntimeRegistration {
    let mut state = registry();
    if state.runtimes.len() >= MAX_RUNTIMES || state.next == u64::MAX {
        state.overflow = state.overflow.saturating_add(1);
        return RuntimeRegistration { id: None };
    }
    let id = state.next;
    state.next = state.next.saturating_add(1);
    state.runtimes.insert(
        id,
        RuntimeEntry {
            role,
            metrics: handle.metrics(),
            handle: handle.clone(),
            probe: Arc::new(Probe::new()),
            pending: None,
            continuously_driven: handle.runtime_flavor()
                == tokio::runtime::RuntimeFlavor::MultiThread,
            drive_leases: 0,
        },
    );
    RuntimeRegistration { id: Some(id) }
}
/// Only current-thread block_on intervals require probe execution. Registration
/// alone supplies counters; idle synchronous owners have no responsiveness SLA.
struct DriveLease {
    id: Option<u64>,
}
impl RuntimeRegistration {
    fn drive(&self) -> DriveLease {
        if let Some(entry) = self.id.and_then(|id| {
            registry().runtimes.get_mut(&id).map(|e| {
                e.drive_leases += 1;
                id
            })
        }) {
            DriveLease { id: Some(entry) }
        } else {
            DriveLease { id: None }
        }
    }
    pub fn block_on<F: std::future::Future>(
        &self,
        runtime: &tokio::runtime::Runtime,
        future: F,
    ) -> F::Output {
        let _drive = self.drive();
        runtime.block_on(future)
    }
}
impl Drop for DriveLease {
    fn drop(&mut self) {
        let Some(id) = self.id else { return };
        let mut state = registry();
        let Some(entry) = state.runtimes.get_mut(&id) else {
            return;
        };
        entry.drive_leases = entry.drive_leases.saturating_sub(1);
        if !entry.continuously_driven && entry.drive_leases == 0 {
            if let Some(pending) = entry.pending.take() {
                pending.abort();
            }
            entry.probe = Arc::new(Probe::new());
        }
    }
}
impl Drop for RuntimeRegistration {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            if let Some(mut entry) = registry().runtimes.remove(&id) {
                if let Some(pending) = entry.pending.take() {
                    pending.abort();
                }
            }
        }
    }
}
pub struct ObservedRuntime {
    registration: RuntimeRegistration,
    runtime: tokio::runtime::Runtime,
}
impl ObservedRuntime {
    pub fn new(runtime: tokio::runtime::Runtime, role: RuntimeRole) -> Self {
        let registration = register(runtime.handle(), role);
        Self {
            registration,
            runtime,
        }
    }
    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.registration.block_on(&self.runtime, future)
    }
    pub fn shutdown_timeout(self, timeout: Duration) {
        drop(self.registration);
        self.runtime.shutdown_timeout(timeout);
    }
}
impl std::ops::Deref for ObservedRuntime {
    type Target = tokio::runtime::Runtime;
    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

/// The Desktop owns this lease and the GPUI foreground task that awaits probes.
/// Dropping it closes the receiver; no framework patch/profiler feature needed.
pub struct ForegroundRegistration {
    probe: Arc<Probe>,
}
pub struct ForegroundReceiver {
    probe: Arc<Probe>,
    receiver: tokio::sync::watch::Receiver<u64>,
}
pub fn register_foreground() -> (ForegroundRegistration, ForegroundReceiver) {
    let probe = Arc::new(Probe::new());
    let (sender, receiver) = tokio::sync::watch::channel(0);
    registry().foreground = Some(ForegroundEntry {
        probe: probe.clone(),
        sender,
    });
    (
        ForegroundRegistration {
            probe: probe.clone(),
        },
        ForegroundReceiver { probe, receiver },
    )
}
impl ForegroundReceiver {
    pub async fn acknowledge_next(&mut self) -> bool {
        if self.receiver.changed().await.is_err() {
            return false;
        }
        let requested = *self.receiver.borrow_and_update();
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        if enabled && self.probe.epoch.load(Ordering::Acquire) == epoch {
            self.probe.ack_at.store(monotonic_ms(), Ordering::Release);
            self.probe.ack.store(requested, Ordering::Release);
        }
        true
    }
}
impl Drop for ForegroundRegistration {
    fn drop(&mut self) {
        let mut state = registry();
        if state
            .foreground
            .as_ref()
            .is_some_and(|e| Arc::ptr_eq(&e.probe, &self.probe))
        {
            state.foreground = None;
        }
    }
}
pub(crate) fn tick(epoch: u64, period: Duration) {
    if !crate::telemetry_sample_allowed(Some(epoch)) {
        return;
    }
    let now = monotonic_ms().max(1);
    let mut state = registry();
    state.ticks = state.ticks.saturating_add(1);
    if state
        .last_tick
        .is_some_and(|last| now.saturating_sub(last) > period.as_millis() as u64 * 3)
    {
        state.late = state.late.saturating_add(1);
    }
    state.last_tick = Some(now);
    for entry in state.runtimes.values_mut() {
        if !entry.continuously_driven && entry.drive_leases == 0 {
            continue;
        }
        if entry.probe.epoch.load(Ordering::Acquire) != epoch {
            if let Some(pending) = entry.pending.take() {
                pending.abort();
            }
            entry.probe = Arc::new(Probe::new());
            entry.probe.epoch.store(epoch, Ordering::Release);
        }
        if entry.pending.is_none()
            || (entry.probe.ack.load(Ordering::Acquire)
                >= entry.probe.request.load(Ordering::Acquire)
                && entry.pending.as_ref().is_some_and(|p| p.is_finished()))
        {
            let probe = entry.probe.clone();
            probe.request.store(now, Ordering::Release);
            entry.pending = Some(entry.handle.spawn(async move {
                if crate::telemetry_sample_allowed(Some(epoch)) {
                    probe.ack_at.store(monotonic_ms(), Ordering::Release);
                    probe.ack.store(now, Ordering::Release);
                }
            }));
        }
    }
    if let Some(entry) = &state.foreground {
        if entry.probe.epoch.load(Ordering::Acquire) != epoch {
            entry.probe.epoch.store(epoch, Ordering::Release);
            entry.probe.request.store(0, Ordering::Release);
            entry.probe.ack.store(0, Ordering::Release);
        }
        let request = entry.probe.request.load(Ordering::Acquire);
        if request == 0 || entry.probe.ack.load(Ordering::Acquire) >= request {
            entry.probe.request.store(now, Ordering::Release);
            let _ = entry.sender.send(now);
        }
    }
}
pub(crate) fn discard() {
    if REGISTRY.get().is_none() {
        return;
    }
    let mut state = registry();
    for entry in state.runtimes.values_mut() {
        if let Some(pending) = entry.pending.take() {
            pending.abort();
        }
        entry.probe = Arc::new(Probe::new());
    }
    if let Some(entry) = &state.foreground {
        entry.probe.epoch.store(u64::MAX, Ordering::Release);
        entry.probe.request.store(0, Ordering::Release);
        entry.probe.ack.store(0, Ordering::Release);
    }
    state.ticks = 0;
    state.last_tick = None;
    state.late = 0;
}
pub fn snapshot() -> Option<RuntimeSnapshots> {
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return None;
    }
    let now = monotonic_ms();
    let state = registry();
    let runtimes=state.runtimes.iter().map(|(id,entry)|{let (status,request,ack)=if !entry.continuously_driven && entry.drive_leases == 0 { ("idle_undriven", None, None) } else { entry.probe.status(now,epoch) };RuntimeSnapshot{id:*id,role:entry.role,snapshot_ms:now,workers:entry.metrics.num_workers(),alive_tasks:entry.metrics.num_alive_tasks(),global_queue_depth:entry.metrics.global_queue_depth(),probe_requested_ms:request,probe_acknowledged_ms:ack,responsiveness:status.into(),counter_scope:"stable Tokio runtime counters; global queue excludes local worker queues; no CPU or poll-time inference".into()}}).collect();
    let foreground = if let Some(entry) = &state.foreground {
        let (status, request, ack) = entry.probe.status(now, epoch);
        ForegroundSnapshot {
            supported: true,
            sampled_ms: now,
            requested_ms: request,
            acknowledged_ms: ack,
            responsiveness: status.into(),
        }
    } else {
        ForegroundSnapshot {
            supported: false,
            sampled_ms: now,
            requested_ms: None,
            acknowledged_ms: None,
            responsiveness: "foreground_owner_not_registered".into(),
        }
    };
    Some(RuntimeSnapshots{schema_version:1,snapshot_ms:now,runtimes,foreground,registration_overflow:state.overflow,watchdog_ticks:state.ticks,watchdog_last_ms:state.last_tick,watchdog_late_ticks:state.late,foreign_workers:"std-thread/GPUI/SQLx: explicit work/wait owners + OS profiles; Tokio statistics do not describe them".into()})
}
pub(crate) fn shutdown() {
    discard();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_probe_clock_distinguishes_idle_pending_overdue_and_ack() {
        let p = Probe::new();
        assert_eq!(p.status(20000, 1).0, "not_recorded");
        p.epoch.store(1, Ordering::Release);
        assert_eq!(p.status(20000, 1).0, "idle_no_expected_probe");
        p.request.store(100, Ordering::Release);
        assert_eq!(p.status(500, 1).0, "probe_pending");
        assert_eq!(p.status(10101, 1).0, "expected_probe_overdue");
        p.ack_at.store(10500, Ordering::Release);
        p.ack.store(100, Ordering::Release);
        assert_eq!(p.status(11000, 1), ("responsive", Some(100), Some(10500)));
        assert_eq!(p.status(11000, 2).0, "not_recorded");
    }
    #[test]
    fn shared_runtime_is_registered_through_last_owner_drop() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let runtime = Arc::new(ObservedRuntime::new(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
            RuntimeRole::DesktopHelper,
        ));
        let id = runtime.registration.id.unwrap();
        let retained = runtime.clone();
        drop(runtime);
        assert!(
            snapshot()
                .unwrap()
                .runtimes
                .iter()
                .any(|entry| entry.id == id)
        );
        drop(retained);
        assert!(
            !snapshot()
                .unwrap()
                .runtimes
                .iter()
                .any(|entry| entry.id == id)
        );
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn blocked_runtime_does_not_block_native_watchdog_or_snapshot() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        // A current-thread runtime deliberately never enters block_on. A probe
        // cannot execute; native tick and snapshot still complete synchronously.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let lease = register(runtime.handle(), RuntimeRole::ClientWs);
        let epoch = crate::telemetry_consent_snapshot().1;
        tick(epoch, Duration::from_secs(5));
        assert!(
            registry()
                .runtimes
                .get(&lease.id.unwrap())
                .unwrap()
                .pending
                .is_none()
        );
        assert_eq!(
            snapshot()
                .unwrap()
                .runtimes
                .iter()
                .find(|e| e.id == lease.id.unwrap())
                .unwrap()
                .responsiveness,
            "idle_undriven"
        );
        let drive = lease.drive();
        tick(epoch, Duration::from_secs(5));
        {
            let mut state = registry();
            let entry = state.runtimes.get_mut(&lease.id.unwrap()).unwrap();
            entry.probe.request.store(1, Ordering::Release);
            assert_eq!(
                entry.probe.status(20_000, epoch).0,
                "expected_probe_overdue"
            );
        }
        // Executing this runtime's queued probe proves the active drive contract.
        runtime.block_on(async {
            tokio::task::yield_now().await;
        });
        assert_eq!(
            registry()
                .runtimes
                .get(&lease.id.unwrap())
                .unwrap()
                .probe
                .status(monotonic_ms(), epoch)
                .0,
            "responsive"
        );
        drop(drive);
        assert!(
            registry()
                .runtimes
                .get(&lease.id.unwrap())
                .unwrap()
                .pending
                .is_none()
        );
        let drive = lease.drive();
        tick(epoch, Duration::from_secs(5));
        let s = snapshot().unwrap();
        let entry = s
            .runtimes
            .iter()
            .find(|r| r.id == lease.id.unwrap())
            .unwrap();
        assert_eq!(entry.responsiveness, "probe_pending");
        assert!(s.watchdog_ticks >= 1);
        drop(drive);
        drop(lease);
        assert!(
            !snapshot()
                .unwrap()
                .runtimes
                .iter()
                .any(|r| r.role == RuntimeRole::ClientWs)
        );
        crate::set_telemetry_enabled(old);
    }
}
