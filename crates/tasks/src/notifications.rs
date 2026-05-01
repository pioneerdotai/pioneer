use pioneer_crud::AppendedTaskEvent;
use pioneer_protocol::{TaskEventPayload, constants::events};

#[derive(Clone, Default)]
pub struct TaskNotificationMapper;

impl TaskNotificationMapper {
    pub fn event_method(event: &AppendedTaskEvent) -> &'static str {
        match &event.payload {
            TaskEventPayload::TaskCreated { .. } => events::TASK_CREATED,
            TaskEventPayload::TriggerCreated { trigger } => {
                if trigger.next_fire_at.is_some() {
                    events::TASK_SCHEDULED
                } else {
                    events::TASK_QUEUED
                }
            }
            TaskEventPayload::TaskScheduled { .. } => events::TASK_SCHEDULED,
            TaskEventPayload::TaskQueued { .. } => events::TASK_QUEUED,
            TaskEventPayload::RunCreated { .. } => events::TASK_RUN_CREATED,
            TaskEventPayload::RunStarted { .. } => events::TASK_RUN_STARTED,
            TaskEventPayload::Progress { .. } => events::TASK_PROGRESS,
            TaskEventPayload::RunCompleted { .. } => events::TASK_RUN_COMPLETED,
            TaskEventPayload::RunFailed { .. } => events::TASK_RUN_FAILED,
            TaskEventPayload::RunRetryScheduled { .. } => events::TASK_RUN_RETRY_SCHEDULED,
            TaskEventPayload::RunRetryExhausted { .. } => events::TASK_RUN_RETRY_EXHAUSTED,
            TaskEventPayload::RunCancelled { .. } => events::TASK_RUN_CANCELLED,
            TaskEventPayload::TaskCompleted { .. } => events::TASK_COMPLETED,
            TaskEventPayload::TaskFailed { .. } => events::TASK_FAILED,
            TaskEventPayload::TaskCancelled { .. } => events::TASK_CANCELLED,
            TaskEventPayload::TaskDetached { .. } => events::TASK_DETACHED,
            TaskEventPayload::TaskRescheduled { .. } => events::TASK_RESCHEDULED,
            TaskEventPayload::TaskPaused { .. } => events::TASK_PAUSED,
            TaskEventPayload::TaskResumed { .. } => events::TASK_RESUMED,
            TaskEventPayload::TaskRecovered { .. } => events::TASK_RECOVERED,
            TaskEventPayload::DeliveryQueued { .. } => events::TASK_DELIVERY_QUEUED,
            TaskEventPayload::DeliveryStarted { .. } => events::TASK_DELIVERY_STARTED,
            TaskEventPayload::DeliveryDelivered { .. } => events::TASK_DELIVERY_DELIVERED,
            TaskEventPayload::DeliveryFailed { .. } => events::TASK_DELIVERY_FAILED,
            TaskEventPayload::DeliveryCancelled { .. } => events::TASK_DELIVERY_CANCELLED,
            TaskEventPayload::WriteLockAcquired { .. } => events::TASK_WRITE_LOCK_ACQUIRED,
            TaskEventPayload::WriteLockReleased { .. } => events::TASK_WRITE_LOCK_RELEASED,
            TaskEventPayload::WriteLockBlocked { .. } => events::TASK_WRITE_LOCK_BLOCKED,
            TaskEventPayload::WriteLockExpired { .. } => events::TASK_WRITE_LOCK_EXPIRED,
            TaskEventPayload::DependencyCreated { .. }
            | TaskEventPayload::AgentSpecCreated { .. }
            | TaskEventPayload::ChildThreadLinked { .. }
            | TaskEventPayload::DepthLimitExceeded { .. } => events::TASK_TREE_CHANGED,
        }
    }
}
