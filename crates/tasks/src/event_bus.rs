use pioneer_crud::AppendedTaskEvent;
use tokio::sync::broadcast;

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
}

impl Default for TaskEventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskEventBus {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self { sender }
    }

    pub fn subscribe(&self, filter: TaskEventFilter) -> TaskEventSubscription {
        TaskEventSubscription {
            receiver: self.sender.subscribe(),
            filter,
        }
    }

    pub async fn publish(&self, event: AppendedTaskEvent) {
        if !event.append_status.is_inserted() {
            return;
        }
        let _ = self.sender.send(TaskEventWake::from_committed_event(event));
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
