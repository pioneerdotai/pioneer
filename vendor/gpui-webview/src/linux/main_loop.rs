//! GLib event-loop integration: prepare/check/dispatch on the UI thread, poll off-thread.
//! No periodic timer and no nested GTK iteration from GPUI layout or paint callbacks.

use std::{
    future::poll_fn,
    io,
    os::{fd::AsRawFd as _, unix::net::UnixStream},
    sync::{Arc, Mutex, mpsc},
    task::{Poll, Waker},
};

use gtk::glib::{self, ffi, translate::ToGlibPtr as _};

struct Completion {
    result: Option<io::Result<Vec<ffi::GPollFD>>>,
    waker: Option<Waker>,
}

#[cfg(test)]
#[path = "main_loop_tests.rs"]
mod tests;

struct Request {
    descriptors: Vec<ffi::GPollFD>,
    timeout_ms: i32,
    completion: Arc<Mutex<Completion>>,
}

/// One waiter per application. Closing the socket wakes an infinite poll at shutdown;
/// closing the channel wakes the waiter if it is between requests. Neither path joins
/// a thread on the UI thread or leaves a detached, permanently blocked poll behind.
pub(super) struct Poller {
    requests: mpsc::Sender<Request>,
    _cancel: UnixStream,
}

impl Poller {
    pub(super) fn new() -> io::Result<Self> {
        let (cancel, cancelled) = UnixStream::pair()?;
        let (requests, receiver) = mpsc::channel::<Request>();
        std::thread::Builder::new()
            .name("gpui-gtk-poll".into())
            .spawn(move || {
                while let Ok(mut request) = receiver.recv() {
                    request.descriptors.push(ffi::GPollFD {
                        fd: cancelled.as_raw_fd(),
                        events: ffi::G_IO_IN as u16,
                        revents: 0,
                    });
                    // SAFETY: The vector owns initialized GPollFDs for the entire wait.
                    // The UI task retains ownership of the GLib context and its sources.
                    // g_poll only waits on descriptors; no GTK callbacks run here.
                    let count = unsafe {
                        ffi::g_poll(
                            request.descriptors.as_mut_ptr(),
                            request.descriptors.len() as u32,
                            request.timeout_ms,
                        )
                    };
                    let error = (count < 0).then(io::Error::last_os_error);
                    if request.descriptors.pop().unwrap().revents != 0 {
                        break;
                    }
                    let waker = {
                        let mut completion = request.completion.lock().unwrap();
                        completion.result = Some(match error {
                            Some(error) => Err(error),
                            None => Ok(request.descriptors),
                        });
                        completion.waker.take()
                    };
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
            })?;
        Ok(Self {
            requests,
            _cancel: cancel,
        })
    }

    async fn wait(
        &self,
        descriptors: Vec<ffi::GPollFD>,
        timeout_ms: i32,
    ) -> io::Result<Vec<ffi::GPollFD>> {
        let completion = Arc::new(Mutex::new(Completion {
            result: None,
            waker: None,
        }));
        self.requests
            .send(Request {
                descriptors,
                timeout_ms,
                completion: completion.clone(),
            })
            .map_err(|_| io::Error::other("GLib poll waiter stopped"))?;
        poll_fn(|cx| {
            let mut completion = completion.lock().unwrap();
            if let Some(result) = completion.result.take() {
                Poll::Ready(result)
            } else {
                completion.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

pub(super) async fn run(context: glib::MainContext, poller: Poller) -> io::Result<()> {
    // Ownership is recursive on the UI thread: synchronous GTK calls may acquire it too.
    // Retain it across the wait, so a second thread cannot dispatch GTK callbacks.
    let _owner = context.acquire().map_err(io::Error::other)?;
    loop {
        let mut priority = 0;
        let mut timeout_ms = -1;
        let mut descriptors = Vec::new();
        // SAFETY: This task owns the context and runs on its UI thread. query reports
        // the required vector length; retry if sources added descriptors concurrently.
        let ready = unsafe {
            let ready = ffi::g_main_context_prepare(context.to_glib_none().0, &mut priority);
            loop {
                let count = ffi::g_main_context_query(
                    context.to_glib_none().0,
                    priority,
                    &mut timeout_ms,
                    descriptors.as_mut_ptr(),
                    descriptors.len() as i32,
                );
                if count as usize <= descriptors.len() {
                    descriptors.truncate(count as usize);
                    break;
                }
                descriptors.resize_with(count as usize, || ffi::GPollFD {
                    fd: -1,
                    events: 0,
                    revents: 0,
                });
            }
            ready != 0
        };
        // Immediate sources must not block on descriptor readiness or a future deadline.
        let mut descriptors = match poller
            .wait(descriptors, if ready { 0 } else { timeout_ms })
            .await
        {
            Ok(descriptors) => descriptors,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        // SAFETY: The descriptors and priority come from this context's last query.
        // check detects sources changed while waiting and rejects stale results.
        unsafe {
            if ffi::g_main_context_check(
                context.to_glib_none().0,
                priority,
                descriptors.as_mut_ptr(),
                descriptors.len() as i32,
            ) != 0
            {
                ffi::g_main_context_dispatch(context.to_glib_none().0);
            }
        }
        // A zero-timeout poll can complete before wait() is first polled. Explicitly
        // yield once even then, so continuously-ready GLib sources cannot starve GPUI.
        let mut yielded = false;
        poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }
}
