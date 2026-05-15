use pioneer_crud::AppendedTaskEvent;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Default)]
pub struct TaskEventFilter {
    pub workspace_id: Option<String>,
    pub task_ids: Vec<String>,
    pub run_ids: Vec<String>,
    pub root_task_id: Option<String>,
    pub parent_task_id: Option<String>,
}

impl TaskEventFilter {
    pub fn matches(&self, event: &AppendedTaskEvent) -> bool {
        if !self.task_ids.is_empty() && !self.task_ids.iter().any(|id| id == &event.task_id) {
            return false;
        }
        if !self.run_ids.is_empty() {
            let Some(run_id) = event.run_id.as_deref() else {
                return false;
            };
            if !self.run_ids.iter().any(|id| id == run_id) {
                return false;
            }
        }
        if let Some(workspace_id) = self.workspace_id.as_deref()
            && event.workspace_id.as_deref() != Some(workspace_id)
        {
            return false;
        }
        if let Some(root_task_id) = self.root_task_id.as_deref()
            && event
                .root_task_id
                .as_deref()
                .unwrap_or(event.task_id.as_str())
                != root_task_id
        {
            return false;
        }
        if let Some(parent_task_id) = self.parent_task_id.as_deref()
            && event.parent_task_id.as_deref() != Some(parent_task_id)
        {
            return false;
        }
        true
    }
}

#[derive(Clone)]
pub struct TaskEventBus {
    sender: broadcast::Sender<AppendedTaskEvent>,
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
        let _ = self.sender.send(event);
    }

    pub async fn publish_many(&self, events: Vec<AppendedTaskEvent>) {
        for event in events {
            self.publish(event).await;
        }
    }
}

pub struct TaskEventSubscription {
    receiver: broadcast::Receiver<AppendedTaskEvent>,
    filter: TaskEventFilter,
}

impl TaskEventSubscription {
    pub async fn recv(&mut self) -> Option<AppendedTaskEvent> {
        loop {
            match self.receiver.recv().await {
                Ok(event) if self.filter.matches(&event) => return Some(event),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}
