use futures_util::Stream;
use pioneer_observability::pool_context::{self, PoolContext};
use pioneer_observability::query_cpu::QueryScope;
use sea_orm::{DbErr, QueryResult};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

// Structurally pins the borrowed creation future without a new allocation.
// No API exposes or moves inner after polling; Drop destroys it in place.
pub(crate) struct ScopedFuture<'a, F> {
    inner: std::mem::ManuallyDrop<F>,
    live: bool,
    scope: &'a QueryScope,
}
impl<'a, F> ScopedFuture<'a, F> {
    pub(crate) fn new(inner: F, scope: &'a QueryScope) -> Self {
        Self {
            inner: std::mem::ManuallyDrop::new(inner),
            live: true,
            scope,
        }
    }
}
impl<F: Future> Future for ScopedFuture<'_, F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: inner is structurally pinned and never moved, including on
        // completion. The only destruction paths drop it in place exactly once.
        let this = unsafe { self.get_unchecked_mut() };
        assert!(this.live, "polled completed stream creation");
        let _entered = this.scope.enter();
        let result = unsafe { Pin::new_unchecked(&mut *this.inner) }.poll(cx);
        if result.is_ready() {
            this.live = false;
            // SAFETY: the future is still pinned; this flag prevents double drop.
            unsafe {
                std::mem::ManuallyDrop::drop(&mut this.inner);
            }
        }
        result
    }
}
impl<F> Drop for ScopedFuture<'_, F> {
    fn drop(&mut self) {
        let _entered = self.scope.enter();
        if self.live {
            self.live = false;
            // SAFETY: destroy in place once, preserving structural pinning.
            unsafe {
                std::mem::ManuallyDrop::drop(&mut self.inner);
            }
        }
    }
}

/// Query lifetime, including every row poll and the inner stream destructor.
/// SeaORM 2.0.4 QueryStream and TransactionStream are Unpin (their own Stream
/// implementations use Pin::get_mut), so storage needs no additional Box.
pub struct DiagnosticStream<S> {
    inner: Option<S>,
    scope: QueryScope,
    failed: bool,
    pool: Option<PoolContext>,
    history_sql: pioneer_observability::turn_startup::HistorySqlContext,
}
impl<S> DiagnosticStream<S> {
    pub(crate) fn new(inner: S, scope: QueryScope, pool: Option<PoolContext>) -> Self {
        Self {
            inner: Some(inner),
            scope,
            failed: false,
            pool,
            history_sql: pioneer_observability::turn_startup::HistorySqlContext::capture(),
        }
    }
}
impl<S: Stream<Item = Result<QueryResult, DbErr>> + Unpin> Stream for DiagnosticStream<S> {
    type Item = Result<QueryResult, DbErr>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let _history_sql = this.history_sql.enter();
        let _pool = pool_context::enter(this.pool);
        let entered = this.scope.enter();
        let result = Pin::new(inner).poll_next(cx);
        match &result {
            Poll::Ready(Some(Err(_))) => {
                this.failed = true;
                this.scope.finish(false);
            }
            Poll::Ready(None) => {
                this.scope.finish(!this.failed);
                // MetricStream's drop callback must see this query's explicit TLS.
                drop(this.inner.take());
            }
            _ => {}
        }
        drop(entered);
        if matches!(&result, Poll::Ready(None)) {
            // EOF releases the caller reference even if this empty wrapper is
            // retained. Worker references still delay on_close until cleanup.
            drop(std::mem::take(&mut this.scope));
        }
        result
    }
}
impl<S> Drop for DiagnosticStream<S> {
    fn drop(&mut self) {
        let _history_sql = self.history_sql.enter();
        let _pool = pool_context::enter(self.pool);
        let _entered = self.scope.enter();
        drop(self.inner.take());
        // No EOF means cancellation, even if MetricStream reports failed=false.
    }
}

pub(crate) async fn open_stream<S, F>(future: F) -> Result<DiagnosticStream<S>, DbErr>
where
    F: Future<Output = Result<S, DbErr>>,
{
    let pool = pool_context::current();
    let scope = QueryScope::new();
    let result = ScopedFuture::new(future, &scope).await;
    match result {
        Ok(stream) => Ok(DiagnosticStream::new(stream, scope, pool)),
        Err(error) => {
            scope.finish(false);
            Err(error)
        }
    }
}

// Construct the private diagnostic span on first poll, under typed pool attribution.
pub(crate) async fn observe_control<F, T, E>(future: F, label: &'static str) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    pioneer_observability::query_cpu::ObservedFuture::new(future, QueryScope::control(label)).await
}

/// Fixed pool attribution around each poll and destructor; no TLS guard crosses await.
pub(crate) trait PoolFutureExt: Future + Sized {
    fn in_pool(self, context: PoolContext) -> PoolFuture<Self> {
        PoolFuture {
            inner: std::mem::ManuallyDrop::new(self),
            live: true,
            context,
            history_sql: pioneer_observability::turn_startup::HistorySqlContext::capture(),
        }
    }
}
impl<F: Future> PoolFutureExt for F {}
pub(crate) struct PoolFuture<F> {
    inner: std::mem::ManuallyDrop<F>,
    live: bool,
    context: PoolContext,
    history_sql: pioneer_observability::turn_startup::HistorySqlContext,
}
impl<F: Future> Future for PoolFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: inner is structurally pinned, never exposed or moved.
        let this = unsafe { self.get_unchecked_mut() };
        assert!(this.live, "polled completed pool future");
        let _history_sql = this.history_sql.enter();
        let _entered = this.context.enter();
        let result = unsafe { Pin::new_unchecked(&mut *this.inner) }.poll(cx);
        if result.is_ready() {
            this.live = false;
            // SAFETY: drop in place once, including if this destructor panics.
            unsafe {
                std::mem::ManuallyDrop::drop(&mut this.inner);
            }
        }
        result
    }
}
impl<F> Drop for PoolFuture<F> {
    fn drop(&mut self) {
        let _history_sql = self.history_sql.enter();
        let _entered = self.context.enter();
        if self.live {
            self.live = false;
            // SAFETY: pinned inner is destroyed in place exactly once.
            unsafe {
                std::mem::ManuallyDrop::drop(&mut self.inner);
            }
        }
    }
}
