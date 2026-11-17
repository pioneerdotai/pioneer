use crate::query_scope::{DiagnosticStream, PoolFutureExt};
use futures_util::Stream;
use pioneer_observability::pool_context::PoolContext::{ReaderMaintenance, WriterCritical};
use pioneer_observability::pool_context::{self, PoolContext};
use pioneer_observability::query_cpu::QueryScope;
use sea_orm::{DbErr, QueryResult};
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
struct Probe {
    seen: Arc<Mutex<Vec<Option<PoolContext>>>>,
    panic: bool,
}
impl Future for Probe {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.seen.lock().unwrap().push(pool_context::current());
        assert!(!self.panic, "probe panic");
        Poll::Pending
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.seen.lock().unwrap().push(pool_context::current());
    }
}

// Protects the adapter's actual poll/cancel/panic boundary, including moving
// a pending future to a thread with a different ambient context.
#[test]
fn pending_pool_future_restores_each_thread_and_scopes_cancellation() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut future = Box::pin(
        Probe {
            seen: seen.clone(),
            panic: false,
        }
        .in_pool(ReaderMaintenance),
    );
    let waker = Waker::from(Arc::new(Noop));
    {
        let _outer = WriterCritical.enter();
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(pool_context::current(), Some(WriterCritical));
    }
    assert_eq!(pool_context::current(), None);
    std::thread::spawn(move || {
        let _outer = WriterCritical.enter();
        let waker = Waker::from(Arc::new(Noop));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(pool_context::current(), Some(WriterCritical));
        drop(future);
        assert_eq!(pool_context::current(), Some(WriterCritical));
    })
    .join()
    .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![Some(ReaderMaintenance); 3]);
}

#[test]
fn panic_restores_pool_context_before_cancel_drop() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut future = Box::pin(
        Probe {
            seen: seen.clone(),
            panic: true,
        }
        .in_pool(ReaderMaintenance),
    );
    let _outer = WriterCritical.enter();
    let waker = Waker::from(Arc::new(Noop));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = future.as_mut().poll(&mut Context::from_waker(&waker));
        }))
        .is_err()
    );
    assert_eq!(pool_context::current(), Some(WriterCritical));
    drop(future);
    assert_eq!(pool_context::current(), Some(WriterCritical));
    assert_eq!(*seen.lock().unwrap(), vec![Some(ReaderMaintenance); 2]);
}

struct Rows {
    seen: Arc<Mutex<Vec<Option<PoolContext>>>>,
    ended: bool,
}
impl Stream for Rows {
    type Item = Result<QueryResult, DbErr>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.seen.lock().unwrap().push(pool_context::current());
        let ended = self.ended;
        self.ended = true;
        Poll::Ready(if ended {
            None
        } else {
            Some(Err(DbErr::Custom("fixture".into())))
        })
    }
}
impl Drop for Rows {
    fn drop(&mut self) {
        self.seen.lock().unwrap().push(pool_context::current());
    }
}
#[test]
fn stream_error_eof_and_early_drop_use_owned_context() {
    for eof in [false, true] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut stream = DiagnosticStream::new(
            Rows {
                seen: seen.clone(),
                ended: false,
            },
            QueryScope::default(),
            Some(ReaderMaintenance),
        );
        let _outer = WriterCritical.enter();
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(Some(Err(_)))
        ));
        assert_eq!(pool_context::current(), Some(WriterCritical));
        if eof {
            assert!(matches!(
                Pin::new(&mut stream).poll_next(&mut cx),
                Poll::Ready(None)
            ));
        }
        drop(stream);
        assert_eq!(pool_context::current(), Some(WriterCritical));
        assert_eq!(
            *seen.lock().unwrap(),
            vec![Some(ReaderMaintenance); if eof { 3 } else { 2 }]
        );
    }
}
