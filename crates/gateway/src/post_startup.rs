use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

struct PostStartupScopeInner {
    cancellation: CancellationToken,
    children: Mutex<JoinSet<()>>,
}

/// Cancellation and task ownership shared by every post-startup subsystem.
///
/// Child tasks are registered before they can run and are joined by the root
/// supervisor. This lets independent retry loops make progress without
/// becoming detached from Gateway shutdown.
#[derive(Clone)]
pub(crate) struct PostStartupScope {
    inner: Arc<PostStartupScopeInner>,
}

impl PostStartupScope {
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    pub(crate) fn spawn<Fut>(&self, future: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.inner
            .children
            .lock()
            .expect("post-startup child task registry")
            .spawn(future);
    }
}

/// Owns the post-startup pipeline and its bounded independent retry loops for
/// a Gateway process.
///
/// Database maintenance remains a single cooperative pipeline. Optional
/// subsystem retries are registered as children, share cancellation, and are
/// joined before shutdown instead of becoming detached tasks.
pub(crate) struct PostStartupSupervisor {
    scope: PostStartupScope,
    worker: Option<JoinHandle<()>>,
}

impl PostStartupSupervisor {
    pub(crate) fn start<F, Fut>(run: F) -> Self
    where
        F: FnOnce(PostStartupScope) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let scope = PostStartupScope {
            inner: Arc::new(PostStartupScopeInner {
                cancellation: CancellationToken::new(),
                children: Mutex::new(JoinSet::new()),
            }),
        };
        let worker = tokio::spawn(run(scope.clone()));
        Self {
            scope,
            worker: Some(worker),
        }
    }

    pub(crate) async fn shutdown(&mut self) {
        self.scope.inner.cancellation.cancel();
        if let Some(worker) = self.worker.take()
            && let Err(error) = worker.await
        {
            tracing::warn!(error = %error, "post-startup supervisor join failed");
        }

        // The root worker has stopped, so no new children can be registered by
        // the production pipeline. Take ownership of the set before awaiting
        // it; holding a synchronous mutex across `.await` would be unsound.
        let mut children = {
            let mut children = self
                .scope
                .inner
                .children
                .lock()
                .expect("post-startup child task registry");
            std::mem::take(&mut *children)
        };
        while let Some(result) = children.join_next().await {
            if let Err(error) = result {
                tracing::warn!(error = %error, "post-startup child task join failed");
            }
        }
    }
}

impl Drop for PostStartupSupervisor {
    fn drop(&mut self) {
        self.scope.inner.cancellation.cancel();
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
        if let Ok(mut children) = self.scope.inner.children.lock() {
            children.abort_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[tokio::test]
    async fn shutdown_cancels_and_joins_independent_children() {
        let child_stopped = Arc::new(AtomicBool::new(false));
        let observed = child_stopped.clone();
        let mut supervisor = PostStartupSupervisor::start(move |scope| async move {
            let cancellation = scope.cancellation();
            scope.spawn(async move {
                cancellation.cancelled().await;
                observed.store(true, Ordering::Release);
            });
            scope.cancellation().cancelled().await;
        });

        supervisor.shutdown().await;

        assert!(child_stopped.load(Ordering::Acquire));
    }
}
