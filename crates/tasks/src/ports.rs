use crate::TaskRuntimeResult;

#[allow(dead_code)]
pub trait TaskNotificationPort: Send + Sync {
    fn emit_committed_task_event(
        &self,
        _event_id: &str,
    ) -> impl std::future::Future<Output = TaskRuntimeResult<()>> + Send {
        async { Ok(()) }
    }
}
