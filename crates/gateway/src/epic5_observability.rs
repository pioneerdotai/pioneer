use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use pioneer_protocol::{GatewayId, PrincipalId};

const MAX_TRACKED_LIMIT_KEYS: usize = 4_096;
const MANAGEMENT_WINDOW: Duration = Duration::from_secs(60);
const RECOVERY_WINDOW: Duration = Duration::from_secs(60 * 60);
const RESTRICTED_FAILURE_WINDOW: Duration = Duration::from_secs(10 * 60);
const INVITATION_CREATE_PER_PRINCIPAL: u32 = 20;
const INVITATION_CREATE_PER_GATEWAY: u32 = 200;
const DIRECT_ADD_PER_ACTOR: u32 = 120;
const RECOVERY_CREATE_PER_TARGET: u32 = 10;
const RESTRICTED_FAILURES_PER_FINGERPRINT: u32 = 12;

pub(crate) const MAX_LIVE_PENDING_INVITATIONS_PER_CREATOR: u64 = 50;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LimitKey {
    InvitationCreatePrincipal(String, String),
    InvitationCreateGateway(String),
    DirectAddActor(String, String),
    RecoveryTarget(String, String),
    RestrictedFingerprint([u8; 8]),
}

#[derive(Debug, Clone, Copy)]
struct WindowEntry {
    count: u32,
    window: Duration,
    window_started: Instant,
    last_touched: Instant,
}

#[derive(Default)]
struct BoundedWindowLimiter {
    entries: Mutex<HashMap<LimitKey, WindowEntry>>,
}

impl BoundedWindowLimiter {
    fn consume(&self, key: LimitKey, limit: u32, window: Duration) -> bool {
        self.consume_at(key, limit, window, Instant::now())
    }

    fn consume_at(&self, key: LimitKey, limit: u32, window: Duration, now: Instant) -> bool {
        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };
        entries.retain(|_, entry| now.saturating_duration_since(entry.last_touched) < entry.window);
        if !entries.contains_key(&key) && entries.len() >= MAX_TRACKED_LIMIT_KEYS {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_touched)
                .map(|(key, _)| key.clone())
            {
                entries.remove(&oldest);
            }
        }
        let entry = entries.entry(key).or_insert(WindowEntry {
            count: 0,
            window,
            window_started: now,
            last_touched: now,
        });
        if now.saturating_duration_since(entry.window_started) >= window {
            entry.count = 0;
            entry.window = window;
            entry.window_started = now;
        }
        entry.last_touched = now;
        if entry.count >= limit {
            return false;
        }
        entry.count += 1;
        true
    }

    fn consume_pair(
        &self,
        first: (LimitKey, u32, Duration),
        second: (LimitKey, u32, Duration),
    ) -> bool {
        self.consume_pair_at(first, second, Instant::now())
    }

    fn consume_pair_at(
        &self,
        first: (LimitKey, u32, Duration),
        second: (LimitKey, u32, Duration),
        now: Instant,
    ) -> bool {
        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };
        entries.retain(|_, entry| now.saturating_duration_since(entry.last_touched) < entry.window);

        for (key, limit, window) in [&first, &second] {
            if entries.get(key).is_some_and(|entry| {
                now.saturating_duration_since(entry.window_started) < *window
                    && entry.count >= *limit
            }) {
                return false;
            }
        }

        let missing = usize::from(!entries.contains_key(&first.0))
            + usize::from(first.0 != second.0 && !entries.contains_key(&second.0));
        while entries.len().saturating_add(missing) > MAX_TRACKED_LIMIT_KEYS {
            let Some(oldest) = entries
                .iter()
                .filter(|(key, _)| *key != &first.0 && *key != &second.0)
                .min_by_key(|(_, entry)| entry.last_touched)
                .map(|(key, _)| key.clone())
            else {
                return false;
            };
            entries.remove(&oldest);
        }

        for (key, _, window) in [first, second] {
            let entry = entries.entry(key).or_insert(WindowEntry {
                count: 0,
                window,
                window_started: now,
                last_touched: now,
            });
            if now.saturating_duration_since(entry.window_started) >= window {
                entry.count = 0;
                entry.window = window;
                entry.window_started = now;
            }
            entry.last_touched = now;
            entry.count += 1;
        }
        true
    }

    fn clear(&self, key: &LimitKey) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(key);
        }
    }
}

fn restricted_key(fingerprint: &[u8; 32]) -> LimitKey {
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&fingerprint[..8]);
    LimitKey::RestrictedFingerprint(prefix)
}

#[derive(Default)]
pub(crate) struct Epic5RateLimits {
    limiter: BoundedWindowLimiter,
}

impl Epic5RateLimits {
    pub(crate) fn allow_invitation_create(
        &self,
        gateway_id: &GatewayId,
        actor: &PrincipalId,
    ) -> bool {
        self.limiter.consume_pair(
            (
                LimitKey::InvitationCreatePrincipal(gateway_id.to_string(), actor.to_string()),
                INVITATION_CREATE_PER_PRINCIPAL,
                MANAGEMENT_WINDOW,
            ),
            (
                LimitKey::InvitationCreateGateway(gateway_id.to_string()),
                INVITATION_CREATE_PER_GATEWAY,
                MANAGEMENT_WINDOW,
            ),
        )
    }

    pub(crate) fn allow_direct_add(&self, gateway_id: &GatewayId, actor: &PrincipalId) -> bool {
        self.limiter.consume(
            LimitKey::DirectAddActor(gateway_id.to_string(), actor.to_string()),
            DIRECT_ADD_PER_ACTOR,
            MANAGEMENT_WINDOW,
        )
    }

    pub(crate) fn allow_recovery_create(
        &self,
        gateway_id: &GatewayId,
        target: &PrincipalId,
    ) -> bool {
        self.limiter.consume(
            LimitKey::RecoveryTarget(gateway_id.to_string(), target.to_string()),
            RECOVERY_CREATE_PER_TARGET,
            RECOVERY_WINDOW,
        )
    }

    pub(crate) fn reserve_restricted_invitation_attempt(&self, fingerprint: &[u8; 32]) -> bool {
        self.limiter.consume(
            restricted_key(fingerprint),
            RESTRICTED_FAILURES_PER_FINGERPRINT,
            RESTRICTED_FAILURE_WINDOW,
        )
    }

    pub(crate) fn clear_restricted_invitation_failures(&self, fingerprint: &[u8; 32]) {
        self.limiter.clear(&restricted_key(fingerprint));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub(crate) enum Epic5Operation {
    InvitationCreate,
    InvitationPreview,
    InvitationAccept,
    InvitationRevoke,
    InvitationExpire,
    GrantReauthorization,
    NicknameConflict,
    WorkspaceMemberAdd,
    WorkspaceMemberRemove,
    MemberSuspend,
    MemberRestore,
    MemberRemove,
    SessionTerminationSuspended,
    SessionTerminationRemoved,
    RecoveryCreate,
    RecoveryActivate,
    AuditWrite,
    Notification,
    Invalidation,
    Count,
}

impl Epic5Operation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InvitationCreate => "invitation_create",
            Self::InvitationPreview => "invitation_preview",
            Self::InvitationAccept => "invitation_accept",
            Self::InvitationRevoke => "invitation_revoke",
            Self::InvitationExpire => "invitation_expire",
            Self::GrantReauthorization => "grant_reauthorization",
            Self::NicknameConflict => "nickname_conflict",
            Self::WorkspaceMemberAdd => "workspace_member_add",
            Self::WorkspaceMemberRemove => "workspace_member_remove",
            Self::MemberSuspend => "member_suspend",
            Self::MemberRestore => "member_restore",
            Self::MemberRemove => "member_remove",
            Self::SessionTerminationSuspended => "session_termination_suspended",
            Self::SessionTerminationRemoved => "session_termination_removed",
            Self::RecoveryCreate => "recovery_create",
            Self::RecoveryActivate => "recovery_activate",
            Self::AuditWrite => "audit_write",
            Self::Notification => "notification",
            Self::Invalidation => "invalidation",
            Self::Count => "count",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub(crate) enum Epic5Outcome {
    Success,
    Noop,
    Denied,
    Conflict,
    Unavailable,
    RateLimited,
    Contention,
    Invalid,
    Count,
}

impl Epic5Outcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Noop => "noop",
            Self::Denied => "denied",
            Self::Conflict => "conflict",
            Self::Unavailable => "unavailable",
            Self::RateLimited => "rate_limited",
            Self::Contention => "contention",
            Self::Invalid => "invalid",
            Self::Count => "count",
        }
    }
}

const LATENCY_BUCKETS: [Duration; 6] = [
    Duration::from_millis(1),
    Duration::from_millis(5),
    Duration::from_millis(25),
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(5),
];

struct Epic5Metrics {
    counters: Vec<Vec<AtomicU64>>,
    latency: Vec<Vec<AtomicU64>>,
}

impl Epic5Metrics {
    fn new() -> Self {
        Self {
            counters: (0..Epic5Operation::Count as usize)
                .map(|_| {
                    (0..Epic5Outcome::Count as usize)
                        .map(|_| AtomicU64::new(0))
                        .collect()
                })
                .collect(),
            latency: (0..Epic5Operation::Count as usize)
                .map(|_| {
                    (0..=LATENCY_BUCKETS.len())
                        .map(|_| AtomicU64::new(0))
                        .collect()
                })
                .collect(),
        }
    }
}

fn metrics() -> &'static Epic5Metrics {
    static METRICS: OnceLock<Epic5Metrics> = OnceLock::new();
    METRICS.get_or_init(Epic5Metrics::new)
}

pub(crate) fn record_outcome(operation: Epic5Operation, outcome: Epic5Outcome) {
    metrics().counters[operation as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        epic5_operation = operation.as_str(),
        epic5_outcome = outcome.as_str(),
        "Epic 5 operation metric"
    );
}

pub(crate) fn record_latency(operation: Epic5Operation, elapsed: Duration) {
    let bucket = LATENCY_BUCKETS
        .iter()
        .position(|upper| elapsed <= *upper)
        .unwrap_or(LATENCY_BUCKETS.len());
    metrics().latency[operation as usize][bucket].fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        epic5_operation = operation.as_str(),
        epic5_latency_bucket = bucket,
        "Epic 5 operation latency metric"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_window_denies_then_resets_without_raw_secret_key() {
        let limiter = BoundedWindowLimiter::default();
        let start = Instant::now();
        let key = LimitKey::RestrictedFingerprint([7; 8]);
        assert!(limiter.consume_at(key.clone(), 2, Duration::from_secs(10), start));
        assert!(limiter.consume_at(key.clone(), 2, Duration::from_secs(10), start));
        assert!(!limiter.consume_at(key.clone(), 2, Duration::from_secs(10), start));
        assert!(limiter.consume_at(
            key,
            2,
            Duration::from_secs(10),
            start + Duration::from_secs(10),
        ));
    }

    #[test]
    fn limiter_covers_each_epic5_key_class_and_keeps_bounded_state() {
        let limiter = BoundedWindowLimiter::default();
        let now = Instant::now();
        let keys = [
            LimitKey::InvitationCreatePrincipal("G1".to_owned(), "P1".to_owned()),
            LimitKey::InvitationCreateGateway("G1".to_owned()),
            LimitKey::DirectAddActor("G1".to_owned(), "P1".to_owned()),
            LimitKey::RecoveryTarget("G1".to_owned(), "P2".to_owned()),
            LimitKey::RestrictedFingerprint([3; 8]),
        ];

        for key in keys {
            assert!(limiter.consume_at(key, 1, MANAGEMENT_WINDOW, now));
        }

        for index in 0..=MAX_TRACKED_LIMIT_KEYS {
            assert!(limiter.consume_at(
                LimitKey::InvitationCreateGateway(format!("overflow-G{index}")),
                1,
                MANAGEMENT_WINDOW,
                now,
            ));
        }

        assert_eq!(
            limiter.entries.lock().unwrap().len(),
            MAX_TRACKED_LIMIT_KEYS
        );
    }

    #[test]
    fn paired_limit_is_atomic_when_either_dimension_is_exhausted() {
        let limiter = BoundedWindowLimiter::default();
        let now = Instant::now();
        let principal = LimitKey::InvitationCreatePrincipal("G1".to_owned(), "P1".to_owned());
        let gateway = LimitKey::InvitationCreateGateway("G1".to_owned());

        assert!(limiter.consume_at(principal.clone(), 1, MANAGEMENT_WINDOW, now));
        assert!(!limiter.consume_pair_at(
            (principal, 1, MANAGEMENT_WINDOW),
            (gateway.clone(), 2, MANAGEMENT_WINDOW),
            now,
        ));
        assert_eq!(
            limiter
                .entries
                .lock()
                .unwrap()
                .get(&gateway)
                .map(|entry| entry.count)
                .unwrap_or_default(),
            0,
            "a rejected principal must not consume Gateway-wide capacity"
        );
    }

    #[test]
    fn production_thresholds_reset_and_isolate_every_epic5_limit_key() {
        let limiter = BoundedWindowLimiter::default();
        let now = Instant::now();
        let gateway = LimitKey::InvitationCreateGateway("G1".to_owned());

        for principal_index in 1..=10 {
            let principal =
                LimitKey::InvitationCreatePrincipal("G1".to_owned(), format!("P{principal_index}"));
            for _ in 0..INVITATION_CREATE_PER_PRINCIPAL {
                assert!(limiter.consume_pair_at(
                    (
                        principal.clone(),
                        INVITATION_CREATE_PER_PRINCIPAL,
                        MANAGEMENT_WINDOW,
                    ),
                    (
                        gateway.clone(),
                        INVITATION_CREATE_PER_GATEWAY,
                        MANAGEMENT_WINDOW,
                    ),
                    now,
                ));
            }
            assert!(!limiter.consume_pair_at(
                (
                    principal,
                    INVITATION_CREATE_PER_PRINCIPAL,
                    MANAGEMENT_WINDOW,
                ),
                (
                    gateway.clone(),
                    INVITATION_CREATE_PER_GATEWAY,
                    MANAGEMENT_WINDOW,
                ),
                now,
            ));
        }
        assert!(!limiter.consume_pair_at(
            (
                LimitKey::InvitationCreatePrincipal("G1".to_owned(), "P11".to_owned()),
                INVITATION_CREATE_PER_PRINCIPAL,
                MANAGEMENT_WINDOW,
            ),
            (
                gateway.clone(),
                INVITATION_CREATE_PER_GATEWAY,
                MANAGEMENT_WINDOW,
            ),
            now,
        ));
        assert!(limiter.consume_pair_at(
            (
                LimitKey::InvitationCreatePrincipal("G1".to_owned(), "P1".to_owned()),
                INVITATION_CREATE_PER_PRINCIPAL,
                MANAGEMENT_WINDOW,
            ),
            (gateway, INVITATION_CREATE_PER_GATEWAY, MANAGEMENT_WINDOW,),
            now + MANAGEMENT_WINDOW,
        ));

        for (key, limit, window) in [
            (
                LimitKey::DirectAddActor("G2".to_owned(), "P1".to_owned()),
                DIRECT_ADD_PER_ACTOR,
                MANAGEMENT_WINDOW,
            ),
            (
                LimitKey::RecoveryTarget("G2".to_owned(), "P2".to_owned()),
                RECOVERY_CREATE_PER_TARGET,
                RECOVERY_WINDOW,
            ),
            (
                LimitKey::RestrictedFingerprint([9; 8]),
                RESTRICTED_FAILURES_PER_FINGERPRINT,
                RESTRICTED_FAILURE_WINDOW,
            ),
        ] {
            for _ in 0..limit {
                assert!(limiter.consume_at(key.clone(), limit, window, now));
            }
            assert!(!limiter.consume_at(key.clone(), limit, window, now));
            assert!(limiter.consume_at(key, limit, window, now + window));
        }

        assert!(limiter.consume_at(
            LimitKey::DirectAddActor("G2".to_owned(), "P3".to_owned()),
            DIRECT_ADD_PER_ACTOR,
            MANAGEMENT_WINDOW,
            now,
        ));
        assert!(limiter.consume_at(
            LimitKey::RecoveryTarget("G2".to_owned(), "P4".to_owned()),
            RECOVERY_CREATE_PER_TARGET,
            RECOVERY_WINDOW,
            now,
        ));
        assert!(limiter.consume_at(
            LimitKey::RestrictedFingerprint([10; 8]),
            RESTRICTED_FAILURES_PER_FINGERPRINT,
            RESTRICTED_FAILURE_WINDOW,
            now,
        ));
    }

    #[test]
    fn gateway_rate_limit_instances_do_not_share_quota() {
        let first_gateway = Epic5RateLimits::default();
        let second_gateway = Epic5RateLimits::default();
        let gateway_id = GatewayId::new("G00000000000000000001".to_owned()).unwrap();
        let principal_id = PrincipalId::new("P00000000000000000001".to_owned()).unwrap();

        for _ in 0..INVITATION_CREATE_PER_PRINCIPAL {
            assert!(first_gateway.allow_invitation_create(&gateway_id, &principal_id));
        }
        assert!(!first_gateway.allow_invitation_create(&gateway_id, &principal_id));
        assert!(second_gateway.allow_invitation_create(&gateway_id, &principal_id));
    }

    #[test]
    fn concurrent_restricted_attempts_cannot_overrun_the_failure_budget() {
        let limits = std::sync::Arc::new(Epic5RateLimits::default());
        let admitted = std::sync::Arc::new(AtomicU64::new(0));
        let fingerprint = [42_u8; 32];

        std::thread::scope(|scope| {
            for _ in 0..64 {
                let limits = limits.clone();
                let admitted = admitted.clone();
                scope.spawn(move || {
                    if limits.reserve_restricted_invitation_attempt(&fingerprint) {
                        admitted.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });

        assert_eq!(
            admitted.load(Ordering::SeqCst),
            u64::from(RESTRICTED_FAILURES_PER_FINGERPRINT),
        );
        assert!(!limits.reserve_restricted_invitation_attempt(&fingerprint));

        limits.clear_restricted_invitation_failures(&fingerprint);
        assert!(limits.reserve_restricted_invitation_attempt(&fingerprint));
    }

    #[test]
    fn metric_dimensions_are_fixed_low_cardinality_enums() {
        let before = metrics().counters[Epic5Operation::InvitationCreate as usize]
            [Epic5Outcome::Success as usize]
            .load(Ordering::Relaxed);
        record_outcome(Epic5Operation::InvitationCreate, Epic5Outcome::Success);
        record_latency(Epic5Operation::InvitationCreate, Duration::from_millis(3));
        assert_eq!(
            metrics().counters[Epic5Operation::InvitationCreate as usize]
                [Epic5Outcome::Success as usize]
                .load(Ordering::Relaxed),
            before + 1
        );
        assert_eq!(Epic5Operation::Invalidation.as_str(), "invalidation");
        assert_eq!(Epic5Outcome::Unavailable.as_str(), "unavailable");
    }
}
