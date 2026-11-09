//! Advisory readiness/deadlines for the existing control owner. Durable CAS,
//! claims and leases remain authoritative; overflow always restores discovery.
use pioneer_crud::{GatewayControlWake as Wake, TimeoutCandidateCursor};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::task::{Id, JoinError, JoinSet};

pub(super) enum CliTurnReadiness {
    Ready(Option<i64>),
    Waiting(Arc<tokio::sync::Mutex<()>>),
}

const CLI_TRANSITION_WAIT_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Family {
    Cli,
    Commands,
    Buffers,
    Timeouts,
    Supervision,
    Resources,
    Actions,
    Skills,
    Ledger,
    Terminalization,
    NativeRetention,
}
impl Family {
    pub(super) const ALL: [Self; 11] = [
        Self::Cli,
        Self::Commands,
        Self::Buffers,
        Self::Timeouts,
        Self::Supervision,
        Self::Resources,
        Self::Actions,
        Self::Skills,
        Self::Ledger,
        Self::Terminalization,
        Self::NativeRetention,
    ];
    pub(super) fn wake(self) -> Wake {
        match self {
            Self::Cli => Wake::Cli,
            Self::Commands => Wake::Commands,
            Self::Buffers => Wake::Buffers,
            Self::Timeouts => Wake::Timeouts,
            Self::Supervision => Wake::Supervision,
            Self::Resources => Wake::Resources,
            Self::Actions => Wake::Actions,
            Self::Skills => Wake::Skills,
            Self::Ledger => Wake::Ledger,
            Self::Terminalization => Wake::Terminalization,
            Self::NativeRetention => Wake::NativeRetention,
        }
    }
}

pub(super) struct TurnPass {
    pub check_state: bool,
    pub cli_done: bool,
    pub timeout_done: bool,
    pub timeout_after: Option<TimeoutCandidateCursor>,
    pub commands: bool,
    pub command_failed: bool,
    pub command_after: Option<String>,
    pub command_upper: Option<String>,
}

impl Default for TurnPass {
    fn default() -> Self {
        Self {
            check_state: true,
            cli_done: false,
            timeout_done: false,
            timeout_after: None,
            commands: false,
            command_failed: false,
            command_after: None,
            command_upper: None,
        }
    }
}

#[derive(Default)]
pub(super) struct ControlWork {
    ready: BTreeSet<Family>,
    deadlines: BTreeMap<Family, i64>,
    pub turns: BTreeMap<String, TurnPass>,
    cli_deadlines: BTreeMap<String, i64>,
    cli_overflow: bool,
    turn_after: Option<String>,
    retry_turns: BTreeMap<String, (TurnPass, i64)>,
    cli_transition_turns: BTreeMap<String, Id>,
    cli_transition_waits: JoinSet<String>,
    cli_transition_overflow_retry: Option<i64>,
    pub rebuild_commands: bool,
}
impl ControlWork {
    pub(super) fn full(&mut self) {
        self.ready.extend(Family::ALL);
        self.rebuild_commands = true;
        // A full source pass covers keys already captured for this pass.
        self.turns.clear();
        self.retry_turns.clear();
        self.deadlines.clear();
        self.cli_deadlines.clear();
        self.cli_overflow = false;
        self.turn_after = None;
        // Gate waits are live obligations, not discovery cursors. Retain them
        // across UNKNOWN/full hints until release, cancellation or shutdown.
    }
    pub(super) fn turn(&mut self, id: String, commands: bool) {
        let prior = self.retry_turns.remove(&id).map(|(pass, _)| pass);
        if self.turns.len() + self.retry_turns.len() >= 256 && !self.turns.contains_key(&id) {
            self.full();
            return;
        }
        let pass = self
            .turns
            .entry(id)
            .or_insert_with(|| prior.unwrap_or_default());
        pass.commands |= commands;
        pass.check_state = true;
        pass.cli_done = false;
        pass.timeout_done = false;
        pass.timeout_after = None;
    }
    fn activity(&mut self, id: String) {
        if self.turns.contains_key(&id) {
            return;
        }
        let prior = self.retry_turns.remove(&id).map(|(pass, _)| pass);
        if self.turns.len() + self.retry_turns.len() >= 256 {
            self.full();
            return;
        }
        let mut pass = prior.unwrap_or_else(|| TurnPass {
            check_state: false,
            ..Default::default()
        });
        pass.cli_done = false;
        pass.timeout_done = false;
        pass.timeout_after = None;
        self.turns.insert(id, pass);
    }
    pub(super) fn wake(&mut self, wake: Wake) {
        match wake {
            Wake::Turn(id) => self.turn(id, false),
            Wake::Activity(id) => self.activity(id),
            Wake::CommandTurn(id) => self.turn(id, true),
            Wake::Cli => {
                self.ready.insert(Family::Cli);
                self.ready.insert(Family::Commands);
                self.rebuild_commands = true;
            }
            Wake::Commands => {
                self.ready.insert(Family::Commands);
            }
            Wake::Buffers => {
                self.ready.insert(Family::Buffers);
            }
            Wake::Timeouts => {
                self.ready.insert(Family::Timeouts);
            }
            Wake::Supervision => {
                self.ready.insert(Family::Supervision);
            }
            Wake::Resources => {
                self.ready.insert(Family::Resources);
            }
            Wake::Actions => {
                self.ready.insert(Family::Actions);
            }
            Wake::Skills => {
                self.ready.insert(Family::Skills);
            }
            Wake::Ledger => {
                self.ready.insert(Family::Ledger);
            }
            Wake::Terminalization => {
                self.ready.insert(Family::Terminalization);
            }
            Wake::NativeRetention => {
                self.ready.insert(Family::NativeRetention);
            }
            Wake::Request(_) | Wake::Attempt(_) | Wake::Native(_, _) | Wake::Thread(_, _) => {
                self.full()
            }
        }
    }
    pub(super) fn due(&mut self, now: i64) {
        if self
            .cli_transition_overflow_retry
            .is_some_and(|at| at <= now)
        {
            self.cli_transition_overflow_retry = None;
            self.ready.insert(Family::Cli);
        }
        let retry_ids = self
            .retry_turns
            .iter()
            .filter(|(_, (_, at))| *at <= now)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in retry_ids {
            if let Some((pass, _)) = self.retry_turns.remove(&id) {
                self.turns.insert(id, pass);
            }
        }

        let due = self
            .deadlines
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(family, _)| *family)
            .collect::<Vec<_>>();
        for family in due {
            self.deadlines.remove(&family);
            if family == Family::Cli && !self.cli_overflow {
                let ids = self
                    .cli_deadlines
                    .iter()
                    .filter(|(_, at)| **at <= now)
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                for id in ids {
                    self.cli_deadlines.remove(&id);
                    self.turn(id, false);
                }
                self.refresh_cli_head();
            } else {
                self.ready.insert(family);
            }
        }
    }
    pub(super) fn ready(&self, family: Family) -> bool {
        self.ready.contains(&family)
    }
    pub(super) fn retry(&mut self, family: Family, at: i64) {
        if family == Family::Cli {
            self.cli_overflow = true;
            self.cli_deadlines.clear();
        }
        self.finish(family, Some(at));
    }
    pub(super) fn finish(&mut self, family: Family, deadline: Option<i64>) {
        self.ready.remove(&family);
        self.deadlines.remove(&family);
        if let Some(at) = deadline {
            self.deadlines.insert(family, at);
        }
    }
    pub(super) fn cli_snapshot(
        &mut self,
        deadlines: BTreeMap<String, i64>,
        head: Option<i64>,
        overflow: bool,
    ) {
        self.cli_deadlines = deadlines;
        self.cli_overflow = overflow;
        self.finish(Family::Cli, head);
        if !overflow {
            for id in self.cli_transition_turns.keys() {
                self.cli_deadlines.remove(id);
            }
            self.refresh_cli_head();
        }
    }
    pub(super) fn cli_turn(&mut self, id: String, deadline: Option<i64>) {
        if self.cli_overflow {
            self.ready.insert(Family::Cli);
            return;
        }
        self.cli_deadlines.remove(&id);
        if let Some(at) = deadline {
            self.cli_deadlines.insert(id, at);
        }
        if self.cli_deadlines.len() > 4096 {
            self.cli_deadlines.clear();
            self.cli_overflow = true;
            self.ready.insert(Family::Cli);
        }
        self.refresh_cli_head();
    }
    fn refresh_cli_head(&mut self) {
        if !self.cli_overflow {
            self.deadlines.remove(&Family::Cli);
            if let Some(at) = self.cli_deadlines.values().min() {
                self.deadlines.insert(Family::Cli, *at);
            }
        }
    }
    pub(super) fn take_turn_ids(&mut self, limit: usize) -> Vec<String> {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut ids = if let Some(after) = &self.turn_after {
            self.turns
                .range((Excluded(after.clone()), Unbounded))
                .take(limit)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if ids.is_empty() {
            ids = self.turns.keys().take(limit).cloned().collect();
        }
        self.turn_after = ids.last().cloned();
        ids
    }
    pub(super) fn retry_turn(&mut self, id: String, pass: TurnPass, at: i64) {
        self.retry_turns.insert(id, (pass, at));
    }
    /// One-shot waits run independently of the control owner's dispatch awaits:
    /// dispatch may itself need this gate. A waiter releases the acquired guard
    /// immediately, without DB work or effects, then reports readiness. JoinSet
    /// owns every task and aborts outstanding waits when this owner is dropped.
    pub(super) fn defer_cli_transition(
        &mut self,
        id: String,
        transition: Arc<tokio::sync::Mutex<()>>,
        now: i64,
    ) {
        self.cli_deadlines.remove(&id);
        self.refresh_cli_head();
        if self.cli_transition_turns.contains_key(&id) {
            return;
        }
        if self.cli_transition_turns.len() >= CLI_TRANSITION_WAIT_LIMIT {
            // Bounded overload fallback, serving a known deferred obligation.
            // Do not silently discard it or substitute the healthy stale TTL.
            let retry = now.saturating_add(2);
            self.cli_transition_overflow_retry = Some(
                self.cli_transition_overflow_retry
                    .map_or(retry, |at| at.min(retry)),
            );
            return;
        }
        let turn_id = id.clone();
        let task = self.cli_transition_waits.spawn(async move {
            drop(transition.lock_owned().await);
            turn_id
        });
        self.cli_transition_turns.insert(id, task.id());
    }
    fn cli_transition_released(&mut self, id: String) {
        self.cli_transition_turns.remove(&id);
        // Re-read current authority instead of applying a captured terminal
        // snapshot: resume/new execution may have won while the gate was held.
        self.turn(id, false);
    }
    fn cli_transition_joined(&mut self, result: Result<(Id, String), JoinError>) {
        let id = match result {
            Ok((_, id)) => id,
            Err(error) => {
                // An unexpected waiter cancellation/panic cannot consume the
                // owed retry or permanently occupy the bounded admission set.
                let Some(id) = self
                    .cli_transition_turns
                    .iter()
                    .find(|(_, task)| **task == error.id())
                    .map(|(id, _)| id.clone())
                else {
                    return;
                };
                id
            }
        };
        self.cli_transition_released(id);
    }
    pub(super) fn drain_cli_transition_retries(&mut self) {
        while let Some(result) = self.cli_transition_waits.try_join_next_with_id() {
            self.cli_transition_joined(result);
        }
    }
    pub(super) async fn wait_cli_transition_retry(&mut self) {
        if self.cli_transition_waits.is_empty() {
            std::future::pending::<()>().await;
        } else if let Some(result) = self.cli_transition_waits.join_next_with_id().await {
            self.cli_transition_joined(result);
        }
    }
    pub(super) fn deadline(&self) -> Option<i64> {
        self.deadlines
            .values()
            .copied()
            .chain(self.retry_turns.values().map(|(_, at)| *at))
            .chain(self.cli_transition_overflow_retry)
            .min()
    }
    pub(super) fn has_work(&self) -> bool {
        !self.ready.is_empty() || !self.turns.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::time::Duration;

    #[tokio::test]
    async fn cli_transition_wait_survives_select_cancellation_and_full_hints_without_blocking_peers()
     {
        let busy = Arc::new(tokio::sync::Mutex::new(()));
        let held = busy.clone().lock_owned().await;
        let free = Arc::new(tokio::sync::Mutex::new(()));
        let mut work = ControlWork::default();
        work.defer_cli_transition("busy".into(), busy.clone(), 100);
        assert!(work.wait_cli_transition_retry().now_or_never().is_none());
        work.full();
        work.defer_cli_transition("busy".into(), busy.clone(), 100);
        work.defer_cli_transition("peer".into(), free.clone(), 100);
        tokio::time::timeout(Duration::from_secs(1), work.wait_cli_transition_retry())
            .await
            .expect("free peer must progress independently of a busy gate");
        assert_eq!(work.take_turn_ids(16), ["peer"]);
        assert!(
            free.try_lock().is_ok(),
            "a completed waiter must release its acquired gate"
        );
        work.turns.clear();
        drop(held);
        tokio::time::timeout(Duration::from_secs(1), work.wait_cli_transition_retry())
            .await
            .expect("the retained gate release must reach its owner");
        assert_eq!(work.take_turn_ids(16), ["busy"]);
        assert!(
            work.turns["busy"].check_state,
            "release must revalidate authority"
        );
        work.turns.clear();
        assert!(
            work.wait_cli_transition_retry().now_or_never().is_none(),
            "no duplicate or empty wake"
        );
        assert!(busy.try_lock().is_ok());
    }

    #[tokio::test]
    async fn cli_transition_wait_overflow_retains_bounded_due_work_and_shutdown_releases_waiters() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let held = gate.clone().lock_owned().await;
        let mut work = ControlWork::default();
        for n in 0..=CLI_TRANSITION_WAIT_LIMIT {
            work.defer_cli_transition(format!("turn-{n}"), gate.clone(), 100);
        }
        assert!(!work.has_work());
        assert_eq!(work.deadline(), Some(102));
        work.due(101);
        assert!(!work.ready(Family::Cli));
        work.due(102);
        assert!(
            work.ready(Family::Cli),
            "overflow must rediscover its deferred obligation"
        );
        assert!(work.wait_cli_transition_retry().now_or_never().is_none());
        drop(work);
        drop(held);
        let _guard = tokio::time::timeout(Duration::from_secs(1), gate.lock_owned())
            .await
            .expect("shutdown must cancel owned waiters and release lock reservations");
    }

    // The owner stops polling retry completions while synchronously dispatching
    // the next CLI launch, whose preparation awaits the same transition gate.
    #[tokio::test]
    async fn cli_transition_waiter_releases_gate_while_control_owner_awaits_next_launch() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let previous_launch = gate.clone().lock_owned().await;
        let mut work = ControlWork::default();
        work.defer_cli_transition("previous-turn".into(), gate.clone(), 100);
        assert!(work.wait_cli_transition_retry().now_or_never().is_none());
        drop(previous_launch);

        // Do not poll ControlWork here. The previous FuturesUnordered waiter
        // reserved the permit but could not release it during this await.
        tokio::time::timeout(Duration::from_secs(1), async {
            let _next_launch = gate.clone().lock_owned().await;
        })
        .await
        .expect("CLI dispatch must not depend on its own caller polling a gate waiter");

        tokio::time::timeout(Duration::from_secs(1), work.wait_cli_transition_retry())
            .await
            .expect("the previous Turn must still be scheduled for reconciliation");
        assert_eq!(work.take_turn_ids(16), ["previous-turn"]);
    }

    #[tokio::test]
    async fn cancelled_cli_transition_waiter_retains_its_original_turn_retry() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let _held = gate.clone().lock_owned().await;
        let mut work = ControlWork::default();
        work.defer_cli_transition("cancelled-turn".into(), gate, 100);
        work.cli_transition_waits.abort_all();
        tokio::time::timeout(Duration::from_secs(1), work.wait_cli_transition_retry())
            .await
            .expect("unexpected cancellation cannot silently consume owed reconciliation");
        assert_eq!(work.take_turn_ids(16), ["cancelled-turn"]);
        assert!(work.turns["cancelled-turn"].check_state);
    }
}
