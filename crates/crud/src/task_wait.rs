//! Targeted, coalesced notifications for bounded foreground Task waits. These
//! are subscriptions, not jobs or durable queues. Each request reuses the common
//! generation/settlement owner; business event delivery remains independent.
use crate::{AppendedTaskEvent, CrudStore};
use pioneer_protocol::TaskEventPayload;
use pioneer_sqlite::background::{Change, Owner, Wake};
use std::sync::{Arc, Mutex, Weak};

pub struct TaskWaitSignal {
    task_ids: Vec<String>,
    run_ids: Vec<String>,
    wake: Arc<Wake>,
    committed_event: std::sync::atomic::AtomicBool,
}
impl TaskWaitSignal {
    pub fn acquire(&self) -> Option<Owner> {
        self.wake.acquire()
    }
    pub fn take_committed_event(&self) -> bool {
        self.committed_event
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    }
    fn matches_identity(&self, task: &str, run: Option<&str>) -> bool {
        (self.task_ids.is_empty() || self.task_ids.iter().any(|id| id == task))
            && (self.run_ids.is_empty()
                || run.is_none_or(|run| self.run_ids.iter().any(|id| id == run)))
    }
    fn matches(&self, payload: &TaskEventPayload) -> bool {
        (self.task_ids.is_empty() || self.task_ids.iter().any(|id| id == payload.task_id()))
            && (self.run_ids.is_empty()
                || payload
                    .run_id()
                    .is_none_or(|run| self.run_ids.iter().any(|id| id == run)))
    }
}
impl Drop for TaskWaitSignal {
    fn drop(&mut self) {
        self.wake.close();
    }
}
struct Listener {
    signal: Weak<TaskWaitSignal>,
    bootstrap: Option<Change>,
}
#[derive(Default)]
struct State {
    in_flight: usize,
    listeners: Vec<Listener>,
}
#[derive(Default)]
struct TaskWaitSignals {
    state: Mutex<State>,
    settlement: Arc<Wake>,
}
impl State {
    fn active(&mut self) -> (Vec<Arc<TaskWaitSignal>>, Vec<Change>) {
        let mut active = Vec::new();
        let mut retired = Vec::new();
        self.listeners
            .retain_mut(|entry| match entry.signal.upgrade() {
                Some(signal) => {
                    active.push(signal);
                    true
                }
                None => {
                    if let Some(change) = entry.bootstrap.take() {
                        retired.push(change);
                    }
                    false
                }
            });
        (active, retired)
    }
}
fn retire(changes: Vec<Change>) {
    for change in changes {
        change.finish(false);
    }
}

pub(crate) struct TaskWaitMutation {
    registry: Arc<TaskWaitSignals>,
    // Only identities are retained for cancellation/unknown routing. No event
    // payload or queue of hints is kept by this subscription protocol.
    inputs: Option<Vec<(String, Option<String>)>>,
    domain_change: Option<Change>,
    changes: Option<Vec<(Arc<TaskWaitSignal>, Change)>>,
}
impl TaskWaitMutation {
    fn input_matches(&self, signal: &TaskWaitSignal) -> bool {
        self.inputs.as_ref().is_none_or(|inputs| {
            inputs.iter().any(|(task, run)| {
                (signal.task_ids.is_empty() || signal.task_ids.contains(task))
                    && (signal.run_ids.is_empty()
                        || run.as_ref().is_none_or(|run| signal.run_ids.contains(run)))
            })
        })
    }
    // Subscribe-after-begin is covered too: callbacks route against the CURRENT
    // listeners, and a bootstrap guard keeps a newly registered wait unsettled
    // until pre-existing producer operations have actually completed.
    fn ended(&self) -> (Vec<Arc<TaskWaitSignal>>, Vec<Change>) {
        let mut state = self.registry.state.lock().unwrap();
        state.in_flight -= 1;
        let (active, mut retired) = state.active();
        if state.in_flight == 0 {
            for entry in &mut state.listeners {
                if let Some(change) = entry.bootstrap.take() {
                    retired.push(change);
                }
            }
        }
        (active, retired)
    }
    pub(crate) fn finish_events(mut self, events: &[AppendedTaskEvent]) {
        self.finish(|signal| {
            let changed = events
                .iter()
                .any(|event| event.append_status.is_inserted() && signal.matches(&event.payload));
            if changed {
                signal
                    .committed_event
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            changed
        });
    }
    pub(crate) fn finish_identity(mut self, identity: Option<(String, Option<String>)>) {
        self.finish(|signal| {
            identity
                .as_ref()
                .is_some_and(|(task, run)| signal.matches_identity(task, run.as_deref()))
        });
    }
    pub(crate) fn finish_changed(mut self, changed: bool) {
        self.finish(|_| changed);
    }
    fn finish(&mut self, changed: impl Fn(&TaskWaitSignal) -> bool) {
        let captured = self.changes.take().unwrap();
        let (active, retired) = self.ended();
        for signal in active {
            if !captured.iter().any(|(old, _)| Arc::ptr_eq(old, &signal)) && changed(&signal) {
                signal.wake.changing().finish(true);
            }
        }
        for (signal, change) in captured {
            change.finish(changed(&signal));
        }
        // Release all bootstrap guards only after publishing actual outcomes.
        // No registry/state mutex, DB resource or transaction is retained here.
        retire(retired);
        self.domain_change.take().unwrap().finish(false);
    }
}
impl Drop for TaskWaitMutation {
    fn drop(&mut self) {
        let Some(captured) = self.changes.take() else {
            return;
        };
        // Publish parent uncertainty BEFORE removing the last in-flight
        // registration. A subscriber born during this callback must fence the
        // cancelled/sent COMMIT too, even if it missed the listener snapshot.
        drop(self.domain_change.take());
        let (active, retired) = self.ended();
        for signal in active {
            if self.input_matches(&signal)
                && !captured.iter().any(|(old, _)| Arc::ptr_eq(old, &signal))
            {
                drop(signal.wake.changing());
            }
        }
        drop(captured); // Unknown; settlement is required, never false Empty.
        retire(retired);
    }
}
impl CrudStore {
    pub fn subscribe_task_wait(
        &self,
        task_ids: &[String],
        run_ids: &[String],
    ) -> Arc<TaskWaitSignal> {
        let registry = self
            .database_connection()
            .shared_resource::<TaskWaitSignals>();
        let signal = Arc::new(TaskWaitSignal {
            task_ids: task_ids.to_vec(),
            run_ids: run_ids.to_vec(),
            wake: Arc::default(),
            committed_event: std::sync::atomic::AtomicBool::new(false),
        });
        let (active, retired) = {
            let mut state = registry.state.lock().unwrap();
            let (active, retired) = state.active();
            let bootstrap = (state.in_flight != 0).then(|| signal.wake.changing());
            state.listeners.push(Listener {
                signal: Arc::downgrade(&signal),
                bootstrap,
            });
            (active, retired)
        };
        drop(active);
        retire(retired);
        if registry.settlement.requires_settlement() {
            drop(signal.wake.changing());
        }
        signal
    }
    /// One connection-order barrier for an actual ambiguous Task producer.
    /// Healthy waits and later subscriptions after settlement never take it.
    pub async fn settle_task_wait_changes(&self, owner: &Owner) -> anyhow::Result<bool> {
        let registry = self
            .database_connection()
            .shared_resource::<TaskWaitSignals>();
        let observed = owner.observe();
        let Some(_drain) = registry.settlement.drain().await else {
            return Ok(false);
        };
        let parent = registry
            .settlement
            .acquire()
            .ok_or_else(|| anyhow::anyhow!("Task wait settlement owner unavailable"))?;
        if registry.settlement.requires_settlement()
            && !self
                .database_connection()
                .settle_background_change(&parent)
                .await?
        {
            return Ok(false);
        }
        Ok(owner.settled(observed))
    }

    pub(crate) async fn track_task_wait_result<T>(
        &self,
        operation: impl std::future::Future<Output = anyhow::Result<T>>,
        identity: impl FnOnce(&T) -> Option<(String, Option<String>)>,
    ) -> anyhow::Result<T> {
        let change = self.task_wait_mutation(None);
        let result = operation.await?;
        change.finish_identity(identity(&result));
        Ok(result)
    }

    pub(crate) fn task_wait_mutation(
        &self,
        inputs: Option<&[TaskEventPayload]>,
    ) -> TaskWaitMutation {
        let registry = self
            .database_connection()
            .shared_resource::<TaskWaitSignals>();
        let domain_change = registry.settlement.changing();
        let (active, retired) = {
            let mut state = registry.state.lock().unwrap();
            state.in_flight = state
                .in_flight
                .checked_add(1)
                .expect("Task wait producer count exhausted");
            state.active()
        };
        retire(retired);
        let inputs = inputs.map(|events| {
            events
                .iter()
                .map(|event| {
                    (
                        event.task_id().to_owned(),
                        event.run_id().map(str::to_owned),
                    )
                })
                .collect()
        });
        let mut mutation = TaskWaitMutation {
            registry,
            inputs,
            domain_change: Some(domain_change),
            changes: None,
        };
        mutation.changes = Some(
            active
                .into_iter()
                .filter(|signal| mutation.input_matches(signal))
                .map(|signal| {
                    let change = signal.wake.changing();
                    (signal, change)
                })
                .collect(),
        );
        mutation
    }
}
