//! UI-neutral task display models.

use pioneer_protocol::TaskStatus;

pub fn task_status_label(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Draft => "Draft",
        TaskStatus::Scheduled => "Scheduled",
        TaskStatus::Queued => "Queued",
        TaskStatus::Running => "Running",
        TaskStatus::Waiting => "Waiting",
        TaskStatus::WaitingReview => "Needs review",
        TaskStatus::Completed => "Completed",
        TaskStatus::Failed => "Failed",
        TaskStatus::Cancelled => "Cancelled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiting_review_task_status_label_requires_review() {
        assert_eq!(task_status_label(TaskStatus::WaitingReview), "Needs review");
        assert_eq!(task_status_label(TaskStatus::Running), "Running");
        assert_eq!(task_status_label(TaskStatus::Completed), "Completed");
    }
}
