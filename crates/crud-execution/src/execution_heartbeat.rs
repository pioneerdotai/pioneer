//! Process-local hints for the existing durable execution owner. This is not
//! a lease authority: only a fresh DB observation can establish Empty.
use crate::turn_execution::{self, TurnExecutionRecord};
use anyhow::{Context, Result};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Default)]
pub struct Registries(Arc<Mutex<HashMap<String, Arc<Registry>>>>);

impl Registries {
    pub fn owner(&self, owner: &str) -> Arc<Registry> {
        self.0
            .lock()
            .unwrap()
            .entry(owner.to_owned())
            .or_insert_with(|| {
                Arc::new(Registry {
                    owner_id: owner.to_owned(),
                    ..Default::default()
                })
            })
            .clone()
    }

    pub fn notification(&self, owner: &str) -> Arc<Notify> {
        self.owner(owner).notification.clone()
    }

    pub fn release_wake(&self) -> ReleaseWake {
        ReleaseWake(self.clone())
    }

    /// True means a fresh database observation permits the owner to park.
    pub async fn supervise(
        &self,
        db: &SqliteDatabase,
        owner_id: &str,
        lease_seconds: i64,
    ) -> Result<bool> {
        let registry = self.owner(owner_id);
        let (generation, unknown, empty) = registry.observation();
        if empty {
            return Ok(true);
        }

        let result = async {
            // A cancelled caller can leave a COMMIT in flight on another
            // pooled session. Finish dispatched writes before a fresh read
            // can publish Empty. The generation also fences new acquisitions.
            if unknown {
                db.settle_dispatched_writes()
                    .await
                    .context("settle outstanding execution ownership writes")?;
            }
            let affected = renew(db, owner_id, lease_seconds).await?;
            if unknown || affected == 0 {
                let active = turn_execution::list_owned_active(db, owner_id, 200).await?;
                registry.reconstruct(generation, active);
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            registry.unknown();
            return Err(error);
        }
        Ok(registry.observation().2)
    }

    pub fn changing(&self, owner: &str) -> OwnershipChange {
        let registry = self.owner(owner);
        {
            let mut state = registry.state.lock().unwrap();
            state.advance();
            state.in_flight += 1;
            state.ownership = Ownership::Unknown;
        }
        // Before database capacity is acquired. A parked owner must resume even
        // if the caller is cancelled at an ambiguous commit boundary.
        registry.notification.notify_one();
        OwnershipChange(registry, std::sync::atomic::AtomicBool::new(false))
    }
}

/// Extend active receipts using the PostgreSQL clock at each UPDATE attempt.
pub async fn renew(db: &SqliteDatabase, owner: &str, lease_seconds: i64) -> Result<u64> {
    retry_with_backoff(
        || turn_execution::heartbeat_owner(db, owner, lease_seconds),
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

/// Terminal outcomes wake heartbeat owners after their outer transaction has
/// released capacity. No witness is removed, so stale release cannot erase a
/// newer ownership fence. Empty owners simply remain parked without SQL.
pub struct ReleaseWake(Registries);
impl Drop for ReleaseWake {
    fn drop(&mut self) {
        let notifications = self
            .0
            .0
            .lock()
            .unwrap()
            .values()
            .map(|registry| registry.notification.clone())
            .collect::<Vec<_>>();
        for notification in notifications {
            notification.notify_one();
        }
    }
}

#[derive(Default)]
enum Ownership {
    #[default]
    Unknown,
    Empty,
    // Bounded bootstrap witnesses keyed by execution identity AND owner fence.
    // A full page is not an exhaustive inventory; it still proves Owned. We
    // renew ALL durable rows for the owner, never just this witness page.
    Owned(BTreeMap<String, u64>),
}

#[derive(Default)]
struct State {
    generation: u64,
    in_flight: usize,
    ownership: Ownership,
}
impl State {
    fn advance(&mut self) {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("execution heartbeat generation exhausted");
    }
}

#[derive(Default)]
pub struct Registry {
    owner_id: String,
    state: Mutex<State>,
    notification: Arc<Notify>,
}
impl Registry {
    pub fn observation(&self) -> (u64, bool, bool) {
        let state = self.state.lock().unwrap();
        if let Ownership::Owned(witnesses) = &state.ownership {
            debug_assert!(!witnesses.is_empty());
        }
        (
            state.generation,
            matches!(state.ownership, Ownership::Unknown),
            matches!(state.ownership, Ownership::Empty),
        )
    }

    fn reconstruct(&self, generation: u64, rows: Vec<TurnExecutionRecord>) {
        let mut state = self.state.lock().unwrap();
        if state.generation != generation || state.in_flight != 0 {
            return;
        }
        state.ownership = if rows.is_empty() {
            Ownership::Empty
        } else {
            Ownership::Owned(
                rows.into_iter()
                    .map(|row| (row.turn_id, row.owner_generation))
                    .collect(),
            )
        };
    }

    fn unknown(&self) {
        let mut state = self.state.lock().unwrap();
        state.advance();
        state.ownership = Ownership::Unknown;
    }
}

/// Covers the whole acquisition attempt, outside transaction/retry scope.
/// Success, rollback, error, panic and cancellation all retain an obligation to
/// revisit the durable owner. No terminal callback removes an identity: the
/// existing heartbeat's zero result triggers a fresh read to clear the registry, fenced
/// against every newer acquisition. A stale release cannot erase a reacquire.
pub struct OwnershipChange(Arc<Registry>, std::sync::atomic::AtomicBool);
impl OwnershipChange {
    /// Called synchronously after commit and before the producer can return or
    /// launch execution. The durable owner generation is the registration fence.
    pub fn register(&self, execution: &TurnExecutionRecord) {
        if execution.owner_id != self.0.owner_id || !execution.status.is_active() {
            return;
        }
        let mut state = self.0.state.lock().unwrap();
        state.advance();
        if !matches!(state.ownership, Ownership::Owned(_)) {
            state.ownership = Ownership::Owned(BTreeMap::new());
        }
        if let Ownership::Owned(witnesses) = &mut state.ownership {
            let generation = witnesses.entry(execution.turn_id.clone()).or_default();
            *generation = (*generation).max(execution.owner_generation);
        }
        self.1.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
impl Drop for OwnershipChange {
    fn drop(&mut self) {
        {
            let mut state = self.0.state.lock().unwrap();
            state.advance();
            state.in_flight -= 1;
            if !self.1.load(std::sync::atomic::Ordering::Relaxed) {
                state.ownership = Ownership::Unknown;
            }
        }
        self.0.notification.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquisition_during_empty_bootstrap_retains_wake_and_prevents_parking() {
        let registries = Registries::default();
        let owner = registries.owner("owner");
        let (before, _, _) = owner.observation();
        let acquisition = registries.changing("owner");
        owner.reconstruct(before, Vec::new());
        assert!(!owner.observation().2);
        // Even a read begun during the acquisition cannot establish Empty.
        owner.reconstruct(owner.observation().0, Vec::new());
        assert!(!owner.observation().2);
        drop(acquisition);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            owner.notification.notified(),
        )
        .await
        .unwrap();
        assert!(owner.observation().1);
    }

    #[tokio::test]
    async fn admission_while_parked_and_cancelled_commit_both_resume_owner() {
        let registries = Registries::default();
        let owner = registries.owner("owner");
        owner.reconstruct(owner.observation().0, Vec::new());
        assert!(owner.observation().2);
        // The waiter is deliberately installed AFTER the acquisition exits.
        // This also models a cancelled/unknown commit: Drop must retain resync.
        drop(registries.changing("owner"));
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            owner.notification.notified(),
        )
        .await
        .unwrap();
        assert!(owner.observation().1);
    }

    #[test]
    fn zero_result_from_before_reacquisition_cannot_clear_new_ownership() {
        let registries = Registries::default();
        let owner = registries.owner("owner");
        let (old_update, _, _) = owner.observation();
        drop(registries.changing("owner"));
        owner.reconstruct(old_update, Vec::new());
        assert!(!owner.observation().2);
        owner.reconstruct(owner.observation().0, Vec::new());
        assert!(owner.observation().2);
    }

    #[test]
    fn stale_terminal_release_does_not_remove_reacquired_identity() {
        let registries = Registries::default();
        let owner = registries.owner("owner");
        let stale_release = ReleaseWake(registries.clone());
        let acquisition = registries.changing("owner");
        let now = chrono::Utc::now().fixed_offset();
        acquisition.register(&TurnExecutionRecord {
            turn_id: "turn".into(),
            thread_id: "thread".into(),
            workspace_id: "workspace".into(),
            executor_kind: turn_execution::TurnExecutorKind::ApiProvider,
            executor_key: None,
            status: turn_execution::TurnExecutionStatus::Running,
            owner_id: "owner".into(),
            owner_generation: 2,
            lease_until: now,
            heartbeat_at: now,
            started_at: Some(now),
            completed_at: None,
            created_at: now,
            updated_at: now,
        });
        drop(acquisition);
        drop(stale_release);
        let state = owner.state.lock().unwrap();
        let Ownership::Owned(witnesses) = &state.ownership else {
            panic!("reacquire must remain owned");
        };
        assert_eq!(witnesses.get("turn"), Some(&2));
    }

    #[test]
    fn bootstrap_error_returns_unknown_and_other_owner_does_not_invalidate_empty() {
        let registries = Registries::default();
        let owner = registries.owner("owner");
        owner.reconstruct(owner.observation().0, Vec::new());
        drop(registries.changing("other-owner"));
        assert!(owner.observation().2);
        owner.unknown();
        assert!(owner.observation().1);
        assert!(!owner.observation().2);
    }
}
