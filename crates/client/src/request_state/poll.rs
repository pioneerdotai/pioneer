//! Demand and generation ownership for a recurring scoped read.

use std::{collections::BTreeSet, time::Duration};

/// Kept private to Client: consumers hold leases, never a timer or deadline.
#[derive(Debug)]
pub(crate) struct PollRequest {
    interval: Duration,
    consumers: BTreeSet<u64>,
    generation: u64,
    active: Option<u64>,
    due: Option<Duration>,
    tick: Option<Duration>,
    connected: bool,
    failed: bool,
}

impl PollRequest {
    pub(crate) fn new(interval: Duration) -> Self {
        assert!(!interval.is_zero());
        Self {
            interval,
            consumers: BTreeSet::new(),
            generation: 0,
            active: None,
            due: None,
            tick: None,
            connected: false,
            failed: false,
        }
    }

    pub(crate) fn acquire(&mut self, consumer: u64, now: Duration) {
        let was_empty = self.consumers.is_empty();
        if self.consumers.insert(consumer) && was_empty && self.connected {
            self.failed = false;
            self.due = Some(now);
            self.tick = Some(now.saturating_add(self.interval));
        }
    }

    pub(crate) fn release(&mut self, consumer: u64) {
        if self.consumers.remove(&consumer) && self.consumers.is_empty() {
            self.cancel();
        }
    }

    pub(crate) fn set_connected(&mut self, connected: bool, now: Duration) {
        if self.connected == connected {
            return;
        }
        self.connected = connected;
        self.cancel();
        if connected && !self.consumers.is_empty() {
            self.failed = false;
            self.due = Some(now);
            self.tick = Some(now.saturating_add(self.interval));
        }
    }

    /// Explicit refresh/retry coalesces with an already claimed request.
    pub(crate) fn refresh(&mut self, now: Duration) {
        if self.connected && !self.consumers.is_empty() && self.active.is_none() {
            self.failed = false;
            self.due = Some(now);
        }
    }

    pub(crate) fn claim(&mut self, now: Duration) -> Option<u64> {
        if !self.connected
            || self.consumers.is_empty()
            || self.failed
            || self.active.is_some()
            || self.due.is_none_or(|due| due > now)
        {
            return None;
        }
        self.generation = self
            .generation
            .checked_add(1)
            .expect("poll generation exhausted");
        self.active = Some(self.generation);
        self.due = None;
        self.active
    }

    pub(crate) fn is_current(&self, generation: u64) -> bool {
        self.connected && !self.consumers.is_empty() && self.active == Some(generation)
    }

    pub(crate) fn complete(&mut self, generation: u64, succeeded: bool, now: Duration) -> bool {
        if !self.is_current(generation) {
            return false;
        }
        self.active = None;
        self.failed = !succeeded;
        self.due = if succeeded {
            let tick = self
                .tick
                .unwrap_or_else(|| now.saturating_add(self.interval));
            let next = if tick <= now {
                let intervals = (now - tick).as_nanos() / self.interval.as_nanos() + 1;
                u32::try_from(intervals)
                    .ok()
                    .and_then(|count| self.interval.checked_mul(count))
                    .map_or(Duration::MAX, |elapsed| tick.saturating_add(elapsed))
            } else {
                tick
            };
            self.tick = Some(next);
            Some(next)
        } else {
            None
        };
        true
    }

    pub(crate) fn cancel(&mut self) {
        self.active = None;
        self.due = None;
        self.tick = None;
    }

    pub(crate) fn due(&self) -> Option<Duration> {
        self.due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seconds(value: u64) -> Duration {
        Duration::from_secs(value)
    }

    #[test]
    fn consumers_share_one_request_and_last_release_fences_completion() {
        let mut poll = PollRequest::new(seconds(20));
        poll.set_connected(true, seconds(0));
        poll.acquire(1, seconds(0));
        let first = poll.claim(seconds(0)).unwrap();
        poll.acquire(1, seconds(1));
        poll.acquire(2, seconds(1));
        poll.refresh(seconds(1));
        assert_eq!(poll.claim(seconds(1)), None);
        poll.release(1);
        assert!(poll.is_current(first));
        poll.release(2);
        assert!(!poll.complete(first, true, seconds(2)));
        assert_eq!(poll.due(), None);
        poll.acquire(3, seconds(3));
        let next = poll.claim(seconds(3)).unwrap();
        assert!(next > first);
        assert!(!poll.complete(first, true, seconds(4)));
        assert!(poll.complete(next, true, seconds(4)));
        assert!(!poll.complete(next, true, seconds(5)));
        assert_eq!(poll.claim(seconds(22)), None);
        assert!(poll.claim(seconds(23)).is_some());
    }

    #[test]
    fn disconnect_reconnect_and_failure_have_explicit_restart_rules() {
        let mut poll = PollRequest::new(seconds(20));
        poll.acquire(1, seconds(0));
        assert_eq!(poll.claim(seconds(0)), None);
        poll.set_connected(true, seconds(1));
        let first = poll.claim(seconds(1)).unwrap();
        poll.set_connected(false, seconds(2));
        assert!(!poll.complete(first, true, seconds(3)));
        poll.set_connected(true, seconds(4));
        let next = poll.claim(seconds(4)).unwrap();
        assert!(poll.complete(next, false, seconds(5)));
        poll.acquire(2, seconds(6));
        assert_eq!(poll.claim(seconds(1000)), None);
        poll.refresh(seconds(1001));
        assert!(poll.claim(seconds(1001)).is_some());
    }

    #[test]
    fn independent_controllers_do_not_share_deadlines_or_generations() {
        let mut mcp = PollRequest::new(seconds(20));
        let mut skills = PollRequest::new(seconds(20));
        for poll in [&mut mcp, &mut skills] {
            poll.set_connected(true, seconds(0));
            poll.acquire(1, seconds(0));
        }
        let request = mcp.claim(seconds(0)).unwrap();
        assert!(mcp.complete(request, true, seconds(1)));
        assert_eq!(mcp.due(), Some(seconds(20)));
        assert_eq!(skills.due(), Some(seconds(0)));
        mcp.release(1);
        assert!(skills.claim(seconds(0)).is_some());
    }
    #[test]
    fn response_latency_does_not_shift_the_recurring_cadence() {
        let mut poll = PollRequest::new(seconds(20));
        poll.set_connected(true, seconds(0));
        poll.acquire(1, seconds(0));
        let request = poll.claim(seconds(0)).unwrap();
        assert!(poll.complete(request, true, seconds(3)));
        assert_eq!(poll.due(), Some(seconds(20)));
        let request = poll.claim(seconds(20)).unwrap();
        assert!(poll.complete(request, true, seconds(65)));
        assert_eq!(poll.due(), Some(seconds(80)));
    }
}
