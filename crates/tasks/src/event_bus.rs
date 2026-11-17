use pioneer_crud::AppendedTaskEvent;
use pioneer_observability::{closing::Closing, scenario::Correlation};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, broadcast};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEventWake {
    pub event_id: String,
    pub task_id: String,
    pub run_id: Option<String>,
    pub workspace_id: Option<String>,
    pub root_task_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub sequence: i64,
}

impl TaskEventWake {
    fn from_committed_event(event: AppendedTaskEvent) -> Self {
        Self {
            event_id: event.id,
            task_id: event.task_id,
            run_id: event.run_id,
            workspace_id: event.workspace_id,
            root_task_id: event.root_task_id,
            parent_task_id: event.parent_task_id,
            sequence: event.sequence,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TaskEventFilter {
    pub workspace_id: Option<String>,
    pub task_ids: Vec<String>,
    pub run_ids: Vec<String>,
    pub root_task_id: Option<String>,
    pub parent_task_id: Option<String>,
}

impl TaskEventFilter {
    pub fn matches(&self, wake: &TaskEventWake) -> bool {
        if !self.task_ids.is_empty() && !self.task_ids.iter().any(|id| id == &wake.task_id) {
            return false;
        }
        if !self.run_ids.is_empty() {
            let Some(run_id) = wake.run_id.as_deref() else {
                return false;
            };
            if !self.run_ids.iter().any(|id| id == run_id) {
                return false;
            }
        }
        if let Some(workspace_id) = self.workspace_id.as_deref()
            && wake.workspace_id.as_deref() != Some(workspace_id)
        {
            return false;
        }
        if let Some(root_task_id) = self.root_task_id.as_deref()
            && wake
                .root_task_id
                .as_deref()
                .unwrap_or(wake.task_id.as_str())
                != root_task_id
        {
            return false;
        }
        if let Some(parent_task_id) = self.parent_task_id.as_deref()
            && wake.parent_task_id.as_deref() != Some(parent_task_id)
        {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskEventWakeDelivery {
    Wake(TaskEventWake),
    Lagged(u64),
    Closed,
}

/// Wake-only notification channel for committed task events.
///
/// This bus is intentionally not a source of truth. Subscribers must use each
/// wake as a hint to reload committed task state or event history from storage.
/// A lagged delivery means "rescan durable state", not "drop lifecycle state".
#[derive(Clone)]
pub struct TaskEventBus {
    sender: broadcast::Sender<TaskEventWake>,
    scheduler_notify: Arc<Notify>,
    closing: Arc<Mutex<crate::closing_observer::ClosingObserver>>,
}

impl Default for TaskEventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskEventBus {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self {
            sender,
            scheduler_notify: Arc::new(Notify::new()),
            closing: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub(crate) fn scheduler_notify(&self) -> Arc<Notify> {
        self.scheduler_notify.clone()
    }

    pub fn subscribe(&self, filter: TaskEventFilter) -> TaskEventSubscription {
        TaskEventSubscription {
            receiver: self.sender.subscribe(),
            filter,
        }
    }

    /// Exact local publication provenance; absence means recovery/missing,
    /// never permission to borrow the latest run or candidate parent.
    pub fn closing_publication_parent(&self, task: &str, sequence: i64) -> Option<Correlation> {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .publication_parent(task, sequence)
    }
    pub fn closing_lifecycle_ticket(&self) -> u64 {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ticket()
    }
    pub fn closing_run_generation(&self, run: &str) -> Option<u64> {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation(run)
    }
    /// Only accepted, inserted lifecycle transitions may provide this receipt.
    pub fn admit_closing_run(
        &self,
        ticket: u64,
        task: &str,
        run: &str,
        sequence: i64,
        resume: bool,
    ) {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .admit(ticket, task, run, sequence, resume);
    }
    pub fn handoff_result_closing(
        &self,
        run: &str,
        turn: &str,
        result: Closing,
        generation: Option<u64>,
    ) {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .handoff_result(run, turn, result, generation);
    }
    pub fn closing_run_parent(&self, run: &str) -> Correlation {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .run_parent(run)
    }
    pub fn closing_run_links(&self, run: &str) -> ([Correlation; 5], usize) {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .run_links(run)
    }
    pub fn closing_candidate_parent(&self, run: &str, candidate: &str) -> Correlation {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .candidate_parent(run, candidate)
    }
    pub(crate) fn closing_review_links(
        &self,
        run: &str,
        candidate: &str,
        source: Correlation,
    ) -> ([Correlation; 5], usize) {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .review_links(run, candidate, source)
    }
    pub fn closing_delivery_parent(&self, id: &str, attempt: u32) -> Correlation {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .delivery_parent(id, attempt)
    }
    pub fn observe_closing_delivery(&self, delivery: &pioneer_protocol::TaskDelivery) {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recover_delivery(delivery);
    }
    pub fn observe_closing_review(
        &self,
        candidate: &pioneer_protocol::TaskResultCandidate,
        run: &pioneer_protocol::TaskRun,
    ) {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recover_review(candidate, run);
    }
    pub(crate) fn retire_closing(&self) {
        self.closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retire();
    }
    pub async fn publish(&self, event: AppendedTaskEvent) {
        if !event.append_status.is_inserted() {
            return;
        }
        if pioneer_observability::telemetry_enabled()
            && crate::closing_observer::ClosingObserver::observes(&event.payload)
        {
            let parent = Closing::capture();
            let mut closing = self
                .closing
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            closing.publication(&event.task_id, event.sequence, parent.clone());
            closing.committed(&event.payload, event.sequence, parent);
        }
        let _ = self.sender.send(TaskEventWake::from_committed_event(event));
        // A permit survives publication between processing and registering a wait.
        self.scheduler_notify.notify_one();
    }

    pub async fn publish_many(&self, events: Vec<AppendedTaskEvent>) {
        for event in events {
            self.publish(event).await;
        }
    }
}

pub struct TaskEventSubscription {
    receiver: broadcast::Receiver<TaskEventWake>,
    filter: TaskEventFilter,
}

impl TaskEventSubscription {
    pub(crate) fn try_recv(&mut self) -> Option<TaskEventWakeDelivery> {
        loop {
            match self.receiver.try_recv() {
                Ok(wake) if self.filter.matches(&wake) => {
                    return Some(TaskEventWakeDelivery::Wake(wake));
                }
                Ok(_) => {}
                Err(broadcast::error::TryRecvError::Empty) => return None,
                Err(broadcast::error::TryRecvError::Lagged(count)) => {
                    return Some(TaskEventWakeDelivery::Lagged(count));
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Some(TaskEventWakeDelivery::Closed);
                }
            }
        }
    }

    pub async fn recv(&mut self) -> TaskEventWakeDelivery {
        loop {
            match self.receiver.recv().await {
                Ok(wake) if self.filter.matches(&wake) => {
                    return TaskEventWakeDelivery::Wake(wake);
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    return TaskEventWakeDelivery::Lagged(count);
                }
                Err(broadcast::error::RecvError::Closed) => return TaskEventWakeDelivery::Closed,
            }
        }
    }
}
