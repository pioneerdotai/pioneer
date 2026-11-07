//! Advisory readiness/deadlines for the existing control owner. Durable CAS,
//! claims and leases remain authoritative; overflow always restores discovery.
use pioneer_crud::{GatewayControlWake as Wake, TimeoutCandidateCursor};
use std::collections::{BTreeMap, BTreeSet};

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
    pub(super) fn deadline(&self) -> Option<i64> {
        self.deadlines
            .values()
            .copied()
            .chain(self.retry_turns.values().map(|(_, at)| *at))
            .min()
    }
    pub(super) fn has_work(&self) -> bool {
        !self.ready.is_empty() || !self.turns.is_empty()
    }
}
