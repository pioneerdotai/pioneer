use crate::TaskRuntimeResult;
use anyhow::Context;
use std::future::Future;
use tokio::task::JoinHandle;

struct AbortOnDropTask<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDropTask<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        let result = self
            .handle
            .as_mut()
            .expect("join handle should be present")
            .await;
        self.handle = None;
        result
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take()
            && !handle.is_finished()
        {
            handle.abort();
        }
    }
}

/// Run owned task orchestration without inheriting the caller's poll stack.
///
/// The child is awaited to preserve ordering and aborted if the caller is
/// cancelled, so this is an ownership boundary rather than detached work.
pub(crate) async fn task_fresh_task<F, T>(
    future: F,
    join_context: &'static str,
) -> TaskRuntimeResult<T>
where
    F: Future<Output = TaskRuntimeResult<T>> + Send + 'static,
    T: Send + 'static,
{
    AbortOnDropTask::new(tokio::spawn(future))
        .join()
        .await
        .context(join_context)?
}
