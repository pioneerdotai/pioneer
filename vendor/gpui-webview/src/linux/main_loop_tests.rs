use super::*;
use std::{
    future::Future,
    io::Write as _,
    pin::pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Wake},
    thread,
    time::{Duration, Instant},
};

struct WakeThread(thread::Thread);

impl Wake for WakeThread {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn drive_until<F: Future>(future: F, done: impl Fn() -> bool) {
    drive_until_started(future, || {}, done);
}

fn drive_until_started<F: Future>(future: F, start: impl FnOnce(), done: impl Fn() -> bool) {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(WakeThread(thread::current())));
    let mut context = Context::from_waker(&waker);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut start = Some(start);
    loop {
        assert!(future.as_mut().poll(&mut context).is_pending());
        if let Some(start) = start.take() {
            start();
        }
        if done() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "event loop did not make progress"
        );
        thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
    }
}

#[test]
fn source_added_from_another_thread_wakes_an_idle_context_on_its_owner_thread() {
    let context = glib::MainContext::new();
    let done = Arc::new(AtomicBool::new(false));
    let owner = thread::current().id();
    let sender_context = context.clone();
    let observed = done.clone();
    // The context is acquired before the producer is released, so invoke cannot
    // execute immediately on that producer instead of waking the real dispatcher.
    let ownership = context.acquire().unwrap();
    let (start, started) = mpsc::channel();
    let producer = thread::spawn(move || {
        started.recv().unwrap();
        sender_context.invoke(move || {
            assert_eq!(thread::current().id(), owner);
            observed.store(true, Ordering::SeqCst);
        });
    });
    drive_until_started(
        run(context.clone(), Poller::new().unwrap()),
        || start.send(()).unwrap(),
        || done.load(Ordering::SeqCst),
    );
    producer.join().unwrap();
    drop(ownership);
    assert!(
        !context.is_owner(),
        "cancelled dispatcher retained context ownership"
    );
}

#[test]
fn glib_deadlines_dispatch_and_a_ready_idle_source_still_yields() {
    let context = glib::MainContext::new();
    let ticks = Arc::new(AtomicUsize::new(0));
    let observed = ticks.clone();
    let timer = glib::timeout_source_new(
        Duration::from_millis(2),
        None,
        glib::Priority::DEFAULT,
        move || {
            if observed.fetch_add(1, Ordering::SeqCst) + 1 == 3 {
                glib::ControlFlow::Break
            } else {
                glib::ControlFlow::Continue
            }
        },
    );
    timer.attach(Some(&context));
    drive_until(run(context.clone(), Poller::new().unwrap()), || {
        ticks.load(Ordering::SeqCst) == 3
    });

    let idle_calls = Arc::new(AtomicUsize::new(0));
    let observed = idle_calls.clone();
    let idle = glib::idle_source_new(None, glib::Priority::DEFAULT_IDLE, move || {
        observed.fetch_add(1, Ordering::SeqCst);
        glib::ControlFlow::Continue
    });
    idle.attach(Some(&context));
    // A dispatcher that drains all pending events never returns from poll here.
    drive_until(run(context, Poller::new().unwrap()), || {
        idle_calls.load(Ordering::SeqCst) >= 3
    });
    idle.destroy();
}

#[test]
fn descriptor_readiness_is_delivered_without_a_timer_source() {
    let context = glib::MainContext::new();
    let (reader, mut writer) = UnixStream::pair().unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let observed = done.clone();
    let (start, started) = mpsc::channel();
    let source_context = context.clone();
    let fd = reader.as_raw_fd();
    let producer = thread::spawn(move || {
        started.recv().unwrap();
        // Add a descriptor after the first prepare/query. The context must rebuild
        // its poll set on wakeup, rather than dispatch using the obsolete snapshot.
        let source = glib::source::unix_fd_source_new(
            fd,
            glib::IOCondition::IN,
            None,
            glib::Priority::DEFAULT,
            move |_, condition| {
                assert!(condition.contains(glib::IOCondition::IN));
                observed.store(true, Ordering::SeqCst);
                glib::ControlFlow::Break
            },
        );
        source.attach(Some(&source_context));
        writer.write_all(&[1]).unwrap();
    });
    drive_until_started(
        run(context, Poller::new().unwrap()),
        || start.send(()).unwrap(),
        || done.load(Ordering::SeqCst),
    );
    producer.join().unwrap();
}

#[test]
fn cancelling_an_infinite_poll_releases_the_waiter() {
    let poller = Poller::new().unwrap();
    let requests = poller.requests.clone();
    let mut wait = Box::pin(poller.wait(Vec::new(), -1));
    let waker = Waker::from(Arc::new(WakeThread(thread::current())));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(wait);
    drop(poller);
    let deadline = Instant::now() + Duration::from_secs(3);
    // A late requester must observe the consumer disappear even though the poll had
    // no data descriptors or timeout. This also catches a cancellation lost before poll.
    loop {
        let probe = Request {
            descriptors: Vec::new(),
            timeout_ms: 0,
            completion: Arc::new(Mutex::new(Completion {
                result: None,
                waker: None,
            })),
        };
        if requests.send(probe).is_err() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cancelled poll waiter remained alive"
        );
        thread::sleep(Duration::from_millis(1));
    }
}
