//! Behavioral regressions at the real WebSocket monitor boundary.
use super::*;
use crate::rpc::{JsonRpcRequestTransport, JsonRpcResponseResult};
use serde_json::{Value, json};
use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

#[derive(Default)]
struct GateState {
    blocked: bool,
    flush_blocked: bool,
    block_flush_after_write: bool,
    writes: u64,
    flushed_writes: u64,
    reported: bool,
    failed: bool,
    waker: Option<Waker>,
}

#[derive(Default)]
struct WriteGate {
    state: Mutex<GateState>,
    entered: tokio::sync::Notify,
    flushed: tokio::sync::Notify,
}

impl WriteGate {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.blocked = true;
        state.reported = false;
    }

    fn block_flush_after_write(&self) {
        let mut state = self.state.lock().unwrap();
        state.block_flush_after_write = true;
        state.reported = false;
    }

    async fn wait_entered(&self) {
        loop {
            let entered = self.entered.notified();
            if self.state.lock().unwrap().reported {
                return;
            }
            entered.await;
        }
    }

    async fn wait_flushed(&self) {
        loop {
            let flushed = self.flushed.notified();
            {
                let state = self.state.lock().unwrap();
                if state.writes > 0 && state.flushed_writes == state.writes {
                    return;
                }
            }
            flushed.await;
        }
    }

    fn release(&self, failed: bool) {
        let mut state = self.state.lock().unwrap();
        state.blocked = false;
        state.flush_blocked = false;
        state.block_flush_after_write = false;
        state.failed = failed;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

/// An inexhaustible source of valid, unmasked server Pong frames. While active,
/// every AsyncRead poll has bytes available: passing the progress regression
/// cannot depend on a producer task yielding or leaving the reader empty.
struct ReadyInbound {
    active: AtomicBool,
    read: AtomicBool,
}

struct StopInbound(Arc<ReadyInbound>);
impl Drop for StopInbound {
    fn drop(&mut self) {
        self.0.active.store(false, Ordering::Release);
    }
}

/// Synchronously refill the REAL bounded mailbox when its retired request
/// completes. Unlike a producer task, this never leaves an accidental empty
/// slot that could rescue a biased ingress loop. No frame reaches the writer.
struct ReadyCancels {
    receipt: SocketReceipt,
    active: AtomicBool,
    completed: AtomicBool,
}
impl ReadyCancels {
    fn queue(self: &Arc<Self>) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let weak = Arc::downgrade(self);
        let (tx, _rx) = mpsc::channel();
        let response = JsonRpcResponseSender::new_with_wake(
            tx,
            std::time::Instant::now() + Duration::from_secs(30),
            Arc::new(move || {
                if let Some(source) = weak.upgrade() {
                    source.completed.store(true, Ordering::Release);
                    source.queue();
                }
            }),
        );
        let request = crate::rpc::build_json_rpc_request_payload(
            pioneer_protocol::constants::methods::TURN_CANCEL,
            Value::Null,
        )
        .unwrap();
        self.receipt
            .admit_cancel(request.request_id, request.payload, response.clone())
            .unwrap();
        // Retirement after admission, before dequeue, exercises the canonical
        // cleanup branch that emits no frame and needs no inbound response.
        response.bind_subscription(Arc::new(Mutex::new(false)));
    }
}
struct StopCancels(Arc<ReadyCancels>);
impl Drop for StopCancels {
    fn drop(&mut self) {
        self.0.active.store(false, Ordering::Release);
    }
}

struct GatedSocket {
    inner: DuplexStream,
    gate: Arc<WriteGate>,
    ready_inbound: Option<Arc<ReadyInbound>>,
    inbound_offset: usize,
}

impl AsyncRead for GatedSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(inbound) = self.ready_inbound.as_ref()
            && inbound.active.load(Ordering::Acquire)
        {
            inbound.read.store(true, Ordering::Release);
            let pong = [0x8a, 0x00]; // FIN | Pong, zero payload, server is unmasked.
            let count = buf.remaining().min(pong.len() - self.inbound_offset);
            buf.put_slice(&pong[self.inbound_offset..self.inbound_offset + count]);
            self.inbound_offset = (self.inbound_offset + count) % pong.len();
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for GatedSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        {
            let mut state = self.gate.state.lock().unwrap();
            if state.failed {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            if state.blocked {
                state.waker = Some(cx.waker().clone());
                if !state.reported {
                    state.reported = true;
                    self.gate.entered.notify_one();
                }
                return Poll::Pending;
            }
        }
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(count)) = &result
            && *count > 0
        {
            let mut state = self.gate.state.lock().unwrap();
            state.writes += 1;
            if state.block_flush_after_write {
                state.block_flush_after_write = false;
                state.flush_blocked = true;
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        {
            let mut state = self.gate.state.lock().unwrap();
            if state.flush_blocked {
                state.waker = Some(cx.waker().clone());
                if !state.reported {
                    state.reported = true;
                    self.gate.entered.notify_one();
                }
                return Poll::Pending;
            }
        }
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            let mut state = self.gate.state.lock().unwrap();
            state.flushed_writes = state.writes;
            self.gate.flushed.notify_one();
        }
        result
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

struct Fixture {
    peer: WebSocketStream<DuplexStream>,
    commands: WsSender<ConnectionRpcCommand>,
    gate: Arc<WriteGate>,
    task: JoinHandle<String>,
    spec: GatewayWsConnectSpec,
    receipt: SocketReceipt,
}

impl Fixture {
    async fn start() -> Self {
        let (fixture, start) = Self::paused(None).await;
        start.send(()).unwrap();
        fixture
    }

    async fn paused(
        ready_inbound: Option<Arc<ReadyInbound>>,
    ) -> (Self, tokio::sync::oneshot::Sender<()>) {
        let (client, server) = tokio::io::duplex(4096);
        let gate = Arc::new(WriteGate::default());
        let stream = WebSocketStream::from_raw_socket(
            GatedSocket {
                inner: client,
                gate: gate.clone(),
                ready_inbound,
                inbound_offset: 0,
            },
            Role::Client,
            None,
        )
        .await;
        let peer = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut spec = super::tests::connect_spec("https://synthetic.invalid".into());
        spec.timings.ping_interval = Duration::from_secs(60);
        spec.timings.pong_timeout = Duration::from_secs(120);
        spec.auth_token = Some(AuthSecretString::new("synthetic-access"));
        spec.session = Some(super::super::GatewayWsSessionIdentity {
            server_gateway_id: pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
            session_id: pioneer_protocol::AuthSessionId::new("S00000000000000000001").unwrap(),
            device_id: pioneer_protocol::DeviceId::new("D00000000000000000001").unwrap(),
            access_expires_at_unix: unix_timestamp_secs() + 600,
            refresh_leeway_seconds: 10,
        });
        let mut active_spec = spec.clone();
        let (events, _events_rx) = mpsc::channel();
        let events = GatewayWsEventSender::from(events);
        let (commands, command_rx) = work_channel(Boundary::ClientRpc);
        let mut command_rx = ConnectionCommandReceiver(command_rx);
        let (receipt, cancels) = SocketReceipt::new(7);
        let monitor_receipt = receipt.clone();
        let (start, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            started.await.expect("start monitor fixture");
            monitor_connection(
                stream,
                &mut active_spec,
                7,
                &events,
                &mut command_rx.0,
                monitor_receipt,
                cancels,
            )
            .await
        });
        (
            Self {
                peer,
                commands,
                gate,
                task,
                spec,
                receipt,
            },
            start,
        )
    }

    async fn next_data(&mut self) -> Message {
        loop {
            match self.peer.next().await.unwrap().unwrap() {
                Message::Ping(payload) => self.peer.send(Message::Pong(payload)).await.unwrap(),
                Message::Text(payload) => return Message::Text(payload),
                Message::Binary(payload) => return Message::Binary(payload),
                other => panic!("unexpected frame: {other:?}"),
            }
        }
    }

    async fn next_request(&mut self) -> Value {
        let Message::Text(payload) = self.next_data().await else {
            panic!("expected request");
        };
        serde_json::from_str(&payload).unwrap()
    }

    async fn reply(&mut self, id: &str) {
        self.peer
            .send(Message::Text(
                json!({"jsonrpc":"2.0", "id":id, "result":{"ok":true}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
}

struct Request {
    id: String,
    response: JsonRpcResponseSender,
    rx: mpsc::Receiver<JsonRpcResponseResult>,
}

fn request(commands: &WsSender<ConnectionRpcCommand>, method: &str) -> Request {
    let request = crate::rpc::build_json_rpc_request_payload(method, Value::Null).unwrap();
    let (tx, rx) = mpsc::channel();
    let response =
        JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
    commands
        .send(ConnectionRpcCommand::Request {
            request_id: request.request_id.clone(),
            payload: request.payload,
            response_tx: response.clone(),
        })
        .unwrap();
    Request {
        id: request.request_id,
        response,
        rx,
    }
}

fn cancel_request(receipt: &SocketReceipt) -> Request {
    let request = crate::rpc::build_json_rpc_request_payload(
        pioneer_protocol::constants::methods::TURN_CANCEL,
        json!({"thread_id":"control-thread", "turn_id":"control-turn"}),
    )
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let response =
        JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
    // The real physical socket admission, not another queue/scheduler fixture.
    receipt
        .admit_cancel(
            request.request_id.clone(),
            request.payload,
            response.clone(),
        )
        .unwrap();
    Request {
        id: request.request_id,
        response,
        rx,
    }
}

struct ReceiptTransport {
    receipt: SocketReceipt,
    admitted: Arc<tokio::sync::Notify>,
}
impl JsonRpcRequestTransport for ReceiptTransport {
    fn send_json_rpc_request(
        &self,
        id: String,
        payload: String,
        response: JsonRpcResponseSender,
    ) -> std::result::Result<(), String> {
        self.receipt.admit_cancel(id, payload, response)?;
        self.admitted.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn cancel_bypasses_ordinary_backlog_without_displacing_pong_or_reader() {
    timeout(Duration::from_secs(10), async {
        let mut f = Fixture::start().await;
        let first = request(&f.commands, "synthetic/first");
        assert_eq!(f.next_request().await["id"], first.id);
        f.gate.wait_flushed().await;
        f.gate.block();
        let blocked = request(&f.commands, "synthetic/claimed");
        f.gate.wait_entered().await;
        let queued = request(&f.commands, "synthetic/queued");
        let overflow = request(&f.commands, "synthetic/overflow");
        assert!(receive(overflow.rx).await.is_err());
        let cancel = cancel_request(&f.receipt);
        let (rejected, rejected_rx) = mpsc::channel();
        let rejected = JsonRpcResponseSender::new(
            rejected,
            std::time::Instant::now() + Duration::from_secs(30),
        );
        assert!(
            f.receipt
                .admit_cancel("extra".into(), "{}".into(), rejected.clone())
                .is_err()
        );
        assert!(receive(rejected_rx).await.is_err());
        assert!(!rejected.is_active());
        // Processing the response establishes reader progress through the Ping
        // while poll_write is still blocked. Pong owns the transport reserve.
        f.peer.send(Message::Ping(vec![9].into())).await.unwrap();
        f.reply(&first.id).await;
        assert!(receive(first.rx).await.is_ok());
        assert!(f.gate.state.lock().unwrap().blocked);
        f.gate.release(false);
        let mut ids = Vec::new();
        let mut pong_before_cancel = false;
        while ids.len() < 3 {
            match f.peer.next().await.unwrap().unwrap() {
                Message::Pong(payload) => {
                    assert_eq!(payload.as_ref(), &[9]);
                    assert!(ids.len() <= 1);
                    pong_before_cancel = true;
                }
                Message::Ping(payload) => f.peer.send(Message::Pong(payload)).await.unwrap(),
                Message::Text(payload) => {
                    let value: Value = serde_json::from_str(&payload).unwrap();
                    if value["id"] == cancel.id {
                        assert!(pong_before_cancel);
                    }
                    ids.push(value["id"].as_str().unwrap().to_owned());
                }
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        assert_eq!(
            ids,
            vec![blocked.id.clone(), cancel.id.clone(), queued.id.clone()]
        );
        f.reply(&cancel.id).await;
        assert!(receive(cancel.rx).await.is_ok());
        let next = cancel_request(&f.receipt); // response completion frees its slot
        assert_eq!(f.next_request().await["id"], next.id);
        f.peer.send(Message::Close(None)).await.unwrap();
        f.task.await.unwrap();
        assert!(receive(next.rx).await.is_err());
        assert!(!next.response.is_active());
    })
    .await
    .expect("bounded Cancel/reader/Pong progress");
}

#[tokio::test]
async fn renewal_keeps_its_reserve_with_full_data_handoff_and_pending_cancel() {
    timeout(Duration::from_secs(10), async {
        let mut f = Fixture::start().await;
        let first = request(&f.commands, "synthetic/ready");
        assert_eq!(f.next_request().await["id"], first.id);
        f.gate.wait_flushed().await;
        f.gate.block();
        let blocked = request(&f.commands, "synthetic/claimed");
        f.gate.wait_entered().await;
        let queued = request(&f.commands, "synthetic/queued");
        let overflow = request(&f.commands, "synthetic/overflow");
        assert!(receive(overflow.rx).await.is_err());
        let cancel = cancel_request(&f.receipt);
        let mut next = f.spec.clone();
        next.auth_token = Some(AuthSecretString::new("renewed-synthetic-access"));
        next.session.as_mut().unwrap().access_expires_at_unix += 600;
        let expires = next.session.as_ref().unwrap().access_expires_at_unix;
        let (tx, rx) = mpsc::channel();
        let renewal = JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
        f.commands.send(ConnectionRpcCommand::RenewAccess {
            spec: next, refresh_generation: 2, deadline: Instant::now() + Duration::from_secs(30), response_tx: renewal.clone(),
        }).unwrap();
        // FIFO command marker proves renewal admission while data is still full.
        let marker = request(&f.commands, "synthetic/after-renewal-admission");
        assert!(receive(marker.rx).await.is_err());
        f.gate.release(false);
        assert_eq!(f.next_request().await["id"], blocked.id);
        let renewed = f.next_request().await;
        assert_eq!(renewed["method"], pioneer_protocol::constants::methods::AUTH_ACCESS_RENEW);
        assert_eq!(f.next_request().await["id"], cancel.id);
        assert_eq!(f.next_request().await["id"], queued.id);
        f.peer.send(Message::Text(json!({"jsonrpc":"2.0","id":renewed["id"],"result":{"access_expires_at_unix":expires}}).to_string().into())).await.unwrap();
        assert!(receive(rx).await.is_ok());
        assert!(!renewal.is_active());
        f.peer.send(Message::Close(None)).await.unwrap();
        f.task.await.unwrap();
        assert!(receive(cancel.rx).await.is_err());
    }).await.expect("renewal cannot share the full ordinary or Cancel handoff");
}

#[tokio::test]
async fn unpolled_socket_owner_completes_retained_cancel_and_closes_admission() {
    let (f, _start) = Fixture::paused(None).await;
    let cancel = cancel_request(&f.receipt);
    f.task.abort();
    let _ = f.task.await;
    assert!(receive(cancel.rx).await.is_err());
    assert!(!cancel.response.is_active());
    let (tx, rx) = mpsc::channel();
    let retained =
        JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
    assert!(
        f.receipt
            .admit_cancel("late".into(), "{}".into(), retained.clone())
            .is_err()
    );
    assert!(receive(rx).await.is_err());
}

#[tokio::test]
async fn queued_expired_cancel_never_writes_or_rebinds_to_replacement_socket() {
    timeout(Duration::from_secs(10), async {
        let mut f = Fixture::start().await;
        let ready = request(&f.commands, "synthetic/ready");
        assert_eq!(f.next_request().await["id"], ready.id);
        f.gate.wait_flushed().await;
        f.gate.block();
        let blocked = request(&f.commands, "synthetic/blocked");
        f.gate.wait_entered().await;
        let admitted = Arc::new(tokio::sync::Notify::new());
        let transport = ReceiptTransport {
            receipt: f.receipt.clone(),
            admitted: admitted.clone(),
        };
        // The canonical synchronous waiter owns timeout and exact pending
        // cleanup. Its timeout, not a sleep, establishes retirement before write.
        let expired = pioneer_observability::work::spawn_blocking(move || {
            crate::rpc::send_json_rpc_request_value(
                &transport,
                pioneer_protocol::constants::methods::TURN_CANCEL,
                json!({"thread_id":"control-thread", "turn_id":"control-turn"}),
                Duration::from_secs(1),
            )
        });
        admitted.notified().await;
        f.reply(&ready.id).await;
        assert!(receive(ready.rx).await.is_ok());
        assert_eq!(
            expired.await.unwrap().unwrap_err().to_string(),
            crate::rpc::request_timeout_message(pioneer_protocol::constants::methods::TURN_CANCEL)
        );
        f.gate.release(false);
        assert_eq!(f.next_request().await["id"], blocked.id);
        let marker = request(&f.commands, "synthetic/marker");
        assert_eq!(
            f.next_request().await["id"],
            marker.id,
            "expired Cancel must not appear before marker"
        );
        f.task.abort();
        let _ = f.task.await;
        let mut replacement = Fixture::start().await; // actual new socket, same logical ID 7
        let (tx, rx) = mpsc::channel();
        let retained =
            JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
        assert!(
            f.receipt
                .admit_cancel("old".into(), "{}".into(), retained.clone())
                .is_err()
        );
        assert!(receive(rx).await.is_err());
        assert!(!retained.is_active());
        let current = cancel_request(&replacement.receipt);
        assert_eq!(replacement.next_request().await["id"], current.id);
        replacement.reply(&current.id).await;
        assert!(receive(current.rx).await.is_ok());
        replacement.peer.send(Message::Close(None)).await.unwrap();
        replacement.task.await.unwrap();
    })
    .await
    .expect("queued Cancel timeout and replacement fence");
}

async fn receive<T: Send + 'static>(rx: mpsc::Receiver<T>) -> T {
    pioneer_observability::work::spawn_blocking(move || {
        rx.recv_timeout(Duration::from_secs(3))
            .expect("waiter must complete")
    })
    .await
    .unwrap()
}

struct QueueTransport {
    commands: WsSender<ConnectionRpcCommand>,
    admitted: mpsc::Sender<String>,
}
impl JsonRpcRequestTransport for QueueTransport {
    fn send_json_rpc_request(
        &self,
        id: String,
        payload: String,
        response_tx: JsonRpcResponseSender,
    ) -> std::result::Result<(), String> {
        self.commands
            .send(ConnectionRpcCommand::Request {
                request_id: id.clone(),
                payload,
                response_tx,
            })
            .unwrap();
        self.admitted.send(id).unwrap();
        Ok(())
    }
}

// A permanently ready reader can monopolize one worker on the old ordering.
// A second worker lets the peer/fuse stop that source even on regression failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_inbound_does_not_starve_outgoing_request_admission() {
    let inbound = Arc::new(ReadyInbound {
        active: AtomicBool::new(true),
        read: AtomicBool::new(false),
    });
    let stop = StopInbound(inbound.clone());
    let (mut fixture, start) = Fixture::paused(Some(inbound.clone())).await;
    let cancels = Arc::new(ReadyCancels {
        receipt: fixture.receipt.clone(),
        active: AtomicBool::new(true),
        completed: AtomicBool::new(false),
    });
    let stop_cancels = StopCancels(cancels.clone());
    cancels.queue();
    // Admission and incoming bytes are both ready before the monitor starts.
    let live = request(&fixture.commands, "synthetic/inbound-progress");
    start.send(()).unwrap();
    let observed = timeout(Duration::from_secs(5), fixture.next_request()).await;
    let still_active = inbound.active.load(Ordering::Acquire);
    let read = inbound.read.load(Ordering::Acquire);
    let cancelled = cancels.completed.load(Ordering::Acquire);
    // Stop only after observing the outgoing request (or the deadlock fuse),
    // before asserting, so the old busy reader can always leave its poll loop.
    drop(stop);
    drop(stop_cancels);
    fixture.task.abort();
    let _ = fixture.task.await;
    let observed = observed.expect("outgoing RPC must arrive while inbound stays ready");
    assert!(
        read,
        "actual WebSocket reader must have consumed ready inbound"
    );
    assert!(still_active, "source must not stop to admit the command");
    assert!(
        cancelled,
        "real Cancel mailbox cleanup must run during the flood"
    );
    assert_eq!(observed["id"], live.id);
}

#[tokio::test]
async fn blocked_write_keeps_reader_live_and_timeout_retires_queued_frame() {
    let phase = std::cell::Cell::new("starting fixture");
    timeout(Duration::from_secs(10), async {
        let mut f = Fixture::start().await;
        // Consume the monitor's immediate heartbeat before arming a gate for
        // application data, so a delayed first tick cannot own the blocked flush.
        phase.set("receiving initial heartbeat");
        let Message::Ping(payload) = f.peer.next().await.unwrap().unwrap() else {
            panic!("expected initial heartbeat");
        };
        f.peer.send(Message::Pong(payload)).await.unwrap();
        phase.set("receiving first request");
        let first = request(&f.commands, "synthetic/first");
        assert_eq!(f.next_request().await["id"], first.id);
        // Receiving bytes does not establish completion of their flush. Arm
        // the next gate only after the preceding write has actually flushed.
        phase.set("waiting for first request flush");
        f.gate.wait_flushed().await;
        // The peer can respond before send/flush completes. Registration must
        // already exist, and response processing must not depend on writer exit.
        phase.set("receiving request with flush blocked");
        // Repeated flush polls for the first request may still be in flight.
        // Block only after the next write has put its bytes on the socket.
        f.gate.block_flush_after_write();
        let fast = request(&f.commands, "synthetic/fast");
        assert_eq!(f.next_request().await["id"], fast.id);
        phase.set("waiting for blocked flush");
        f.gate.wait_entered().await;
        phase.set("receiving response while flush is blocked");
        f.reply(&fast.id).await;
        assert!(receive(fast.rx).await.is_ok());
        assert!(f.gate.state.lock().unwrap().flush_blocked);
        f.gate.release(false);
        // A peer reply can arrive while the writer still owns the previous
        // flush. Complete a round trip with the gate open before blocking the
        // next write, so that write owns the writer and leaves the handoff free.
        phase.set("completing round trip after flush release");
        let flushed = request(&f.commands, "synthetic/after-flush");
        assert_eq!(f.next_request().await["id"], flushed.id);
        f.reply(&flushed.id).await;
        assert!(receive(flushed.rx).await.is_ok());
        phase.set("waiting for round trip flush");
        f.gate.wait_flushed().await;
        phase.set("waiting for blocked write");
        f.gate.block();
        let blocked = request(&f.commands, "synthetic/blocked");
        f.gate.wait_entered().await;

        phase.set("filling writer handoff");
        let (admitted, admitted_rx) = mpsc::channel();
        let transport = QueueTransport {
            commands: f.commands.clone(),
            admitted,
        };
        let timed = pioneer_observability::work::spawn_blocking(move || {
            crate::rpc::send_json_rpc_request_value(
                &transport,
                "synthetic/timeout",
                Value::Null,
                Duration::from_secs(2),
            )
        });
        let timed_id = receive(admitted_rx).await;
        let overflow = request(&f.commands, "synthetic/overflow");
        assert_eq!(
            receive(overflow.rx).await.unwrap_err().to_string(),
            "websocket writer handoff full or closed"
        );

        // A ready response must complete while poll_write remains blocked.
        phase.set("receiving response while write is blocked");
        f.reply(&first.id).await;
        assert_eq!(receive(first.rx).await.unwrap(), json!({"ok":true}));
        assert!(f.gate.state.lock().unwrap().blocked);
        phase.set("waiting for queued request timeout");
        assert_eq!(
            timed.await.unwrap().unwrap_err().to_string(),
            crate::rpc::request_timeout_message("synthetic/timeout")
        );
        phase.set("receiving blocked request after write release");
        f.reply(&timed_id).await; // late response cannot recreate observation
        f.gate.release(false);
        assert_eq!(f.next_request().await["id"], blocked.id);
        f.reply(&blocked.id).await;
        assert!(receive(blocked.rx).await.is_ok());

        // Retired-before-dequeue and retired-in-handoff requests cannot precede
        // this marker on the socket. This also proves the connection stayed live.
        let retired =
            crate::rpc::build_json_rpc_request_payload("synthetic/cancelled", Value::Null).unwrap();
        let (tx, rx) = mpsc::channel();
        let response =
            JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
        response.cancel();
        f.commands
            .send(ConnectionRpcCommand::Request {
                request_id: retired.request_id,
                payload: retired.payload,
                response_tx: response,
            })
            .unwrap();
        assert!(receive(rx).await.is_err());
        phase.set("receiving marker after retired frames");
        let marker = request(&f.commands, "synthetic/marker");
        assert_eq!(f.next_request().await["id"], marker.id);
        f.reply(&marker.id).await;
        assert!(receive(marker.rx).await.is_ok());
        phase.set("closing connection");
        f.peer.send(Message::Close(None)).await.unwrap();
        assert_eq!(f.task.await.unwrap(), WEBSOCKET_CLOSED_BY_PEER_MESSAGE);
    })
    .await
    .unwrap_or_else(|_| panic!("monitor regression timed out while {}", phase.get()));
}

#[derive(Clone, Copy, Debug)]
enum Termination {
    WriteFailure,
    PeerClose,
    OwnerAbort,
    RenewalRejected,
    ControlFull,
}

async fn assert_teardown(termination: Termination) {
    let mut f = Fixture::start().await;
    let pending = request(&f.commands, "synthetic/pending");
    assert_eq!(f.next_request().await["id"], pending.id);
    let (skills, skills_rx) = mpsc::channel();
    f.commands
        .send(ConnectionRpcCommand::BinaryUploadChunk {
            upload_id: "skills".into(),
            offset: 0,
            payload: vec![1, 2],
            response_tx: skills,
        })
        .unwrap();
    assert_eq!(f.next_data().await, Message::Binary(vec![1, 2].into()));
    let (artifact, artifact_rx) = mpsc::channel();
    f.commands
        .send(ConnectionRpcCommand::ArtifactBinaryUploadChunk {
            upload_id: "artifact".into(),
            offset: 0,
            payload: vec![3, 4],
            response_tx: JsonRpcResponseSender::new(
                artifact,
                std::time::Instant::now() + Duration::from_secs(30),
            ),
        })
        .unwrap();
    assert_eq!(f.next_data().await, Message::Binary(vec![3, 4].into()));

    let mut next = f.spec.clone();
    next.auth_token = Some(AuthSecretString::new("next-synthetic-access"));
    next.session.as_mut().unwrap().access_expires_at_unix += 600;
    let (renewal_tx, renewal_rx) = mpsc::channel();
    let renewal = JsonRpcResponseSender::new(
        renewal_tx,
        std::time::Instant::now() + Duration::from_secs(30),
    );
    f.commands
        .send(ConnectionRpcCommand::RenewAccess {
            spec: next,
            refresh_generation: 2,
            deadline: Instant::now() + Duration::from_secs(30),
            response_tx: renewal.clone(),
        })
        .unwrap();
    let renewal_request = f.next_request().await;
    assert_eq!(
        renewal_request["method"],
        pioneer_protocol::constants::methods::AUTH_ACCESS_RENEW
    );
    let control = cancel_request(&f.receipt);
    assert_eq!(f.next_request().await["id"], control.id);
    f.gate.block();
    f.commands
        .send(ConnectionRpcCommand::VoiceBinaryChunk {
            payload: vec![5, 6],
        })
        .unwrap();
    f.gate.wait_entered().await;
    let queued = request(&f.commands, "synthetic/queued");
    let overflow = request(&f.commands, "synthetic/overflow");
    assert!(receive(overflow.rx).await.is_err()); // queued request reached the handoff

    match termination {
        Termination::WriteFailure => f.gate.release(true),
        Termination::PeerClose => f.peer.send(Message::Close(None)).await.unwrap(),
        Termination::OwnerAbort => f.task.abort(),
        Termination::RenewalRejected => {
            let rejection = json!({
                "jsonrpc": "2.0",
                "id": renewal_request["id"],
                "error": {
                    "code": -32040,
                    "message": "peer text",
                    "data": {"code": "session_revoked"}
                }
            });
            f.peer
                .send(Message::Text(rejection.to_string().into()))
                .await
                .unwrap();
        }
        Termination::ControlFull => {
            f.peer.send(Message::Ping(vec![1].into())).await.unwrap();
            f.peer.send(Message::Ping(vec![2].into())).await.unwrap();
        }
    }
    match termination {
        Termination::OwnerAbort => assert!(f.task.await.unwrap_err().is_cancelled()),
        Termination::ControlFull => assert_eq!(
            f.task.await.unwrap(),
            "websocket control writer handoff full or closed"
        ),
        _ => {
            f.task.await.unwrap();
        }
    }
    assert!(receive(pending.rx).await.is_err(), "{termination:?}: RPC");
    assert!(
        receive(queued.rx).await.is_err(),
        "{termination:?}: queued RPC"
    );
    assert!(
        receive(renewal_rx).await.is_err(),
        "{termination:?}: renewal"
    );
    assert!(
        receive(skills_rx).await.is_err(),
        "{termination:?}: skills ACK"
    );
    assert!(
        receive(artifact_rx).await.is_err(),
        "{termination:?}: artifact ACK"
    );
    // Keep lifecycle handles alive through teardown: completion must be
    // explicit, rather than relying on last-Sender channel disconnect.
    assert!(
        receive(control.rx).await.is_err(),
        "{termination:?}: Cancel"
    );
    assert!(!control.response.is_active());
    assert!(!pending.response.is_active());
    assert!(!queued.response.is_active());
    assert!(!renewal.is_active());
}

#[tokio::test]
async fn connection_teardown_completes_rpc_renewal_and_both_binary_ack_owners() {
    for termination in [
        Termination::WriteFailure,
        Termination::PeerClose,
        Termination::OwnerAbort,
        Termination::RenewalRejected,
        Termination::ControlFull,
    ] {
        timeout(Duration::from_secs(10), assert_teardown(termination))
            .await
            .unwrap_or_else(|_| panic!("teardown regression timed out: {termination:?}"));
    }
}

#[test]
fn dropping_unpolled_connection_completes_queued_waiters_with_retained_handles() {
    let (commands, command_rx) = work_channel(Boundary::ClientRpc);
    let spec = super::tests::connect_spec("https://synthetic.invalid".into());
    let (events, _event_rx) = mpsc::channel();
    let connection = run_connection_task(
        7,
        spec.clone(),
        None,
        false,
        events.into(),
        ConnectionCommandReceiver(command_rx),
        None,
        Arc::default(),
    );
    let queued = request(&commands, "synthetic/queued-before-connect");
    let (skills_tx, skills_rx) = mpsc::channel();
    commands
        .send(ConnectionRpcCommand::BinaryUploadChunk {
            upload_id: "skills".into(),
            offset: 0,
            payload: vec![1],
            response_tx: skills_tx,
        })
        .unwrap();
    let (artifact_tx, artifact_rx) = mpsc::channel();
    commands
        .send(ConnectionRpcCommand::ArtifactBinaryUploadChunk {
            upload_id: "artifact".into(),
            offset: 0,
            payload: vec![2],
            response_tx: JsonRpcResponseSender::new(
                artifact_tx,
                std::time::Instant::now() + Duration::from_secs(30),
            ),
        })
        .unwrap();
    let (renewal_tx, renewal_rx) = mpsc::channel();
    let renewal = JsonRpcResponseSender::new(
        renewal_tx,
        std::time::Instant::now() + Duration::from_secs(30),
    );
    commands
        .send(ConnectionRpcCommand::RenewAccess {
            spec,
            refresh_generation: 2,
            deadline: Instant::now() + Duration::from_secs(30),
            response_tx: renewal.clone(),
        })
        .unwrap();
    drop(connection);
    assert!(queued.rx.recv().unwrap().is_err());
    assert!(skills_rx.recv().unwrap().is_err());
    assert!(artifact_rx.recv().unwrap().is_err());
    assert!(renewal_rx.recv().unwrap().is_err());
    assert!(!queued.response.is_active());
    assert!(!renewal.is_active());
}

#[tokio::test]
async fn retired_socket_receipt_cannot_write_queued_request_or_new_socket_with_same_logical_id() {
    timeout(Duration::from_secs(10), async {
        let mut old = Fixture::start().await;
        old.gate.block();
        let written = request(&old.commands, "synthetic/claimed-before-retire");
        old.gate.wait_entered().await;

        let payload =
            crate::rpc::build_json_rpc_request_payload("synthetic/old-scope", Value::Null).unwrap();
        let (tx, rx) = mpsc::channel();
        let response =
            JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
        old.receipt.bind_request(&response);
        old.commands
            .send(ConnectionRpcCommand::Request {
                request_id: payload.request_id.clone(),
                payload: payload.payload.clone(),
                response_tx: response.clone(),
            })
            .unwrap();
        // FIFO command processing plus an overflow proves the scoped frame is
        // waiting behind the claimed write, rather than merely not admitted yet.
        let overflow = request(&old.commands, "synthetic/overflow");
        assert!(receive(overflow.rx).await.is_err());
        old.receipt.retire();
        old.gate.release(false);
        assert_eq!(old.next_request().await["id"], written.id);
        assert!(receive(rx).await.is_err());
        let marker = request(&old.commands, "synthetic/old-marker");
        assert_eq!(old.next_request().await["id"], marker.id);
        old.peer.send(Message::Close(None)).await.unwrap();
        old.task.await.unwrap();

        let mut next = Fixture::start().await; // fixture deliberately reuses logical ID 7
        let (tx, rx) = mpsc::channel();
        let late =
            JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
        old.receipt.bind_request(&late);
        // This represents a command admitted after the old monitor's drain,
        // before delayed core disconnect processing. It enters the real new monitor.
        next.commands
            .send(ConnectionRpcCommand::Request {
                request_id: payload.request_id,
                payload: payload.payload,
                response_tx: late,
            })
            .unwrap();
        assert!(receive(rx).await.is_err());
        let marker = request(&next.commands, "synthetic/new-marker");
        assert_eq!(next.next_request().await["id"], marker.id);
        next.reply(&marker.id).await;
        assert!(receive(marker.rx).await.is_ok());
        next.peer.send(Message::Close(None)).await.unwrap();
        next.task.await.unwrap();
    })
    .await
    .expect("scope regression must complete");
}

#[tokio::test]
async fn operation_cancel_retires_binary_chunk_before_write_without_closing_socket() {
    timeout(Duration::from_secs(10), async {
        let mut f = Fixture::start().await;
        f.gate.block();
        let claimed = request(&f.commands, "synthetic/claimed");
        f.gate.wait_entered().await;
        let budget =
            crate::rpc::RequestBudget::new(std::time::Instant::now() + Duration::from_secs(30));
        let (tx, rx) = mpsc::channel();
        let chunk =
            JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
        budget.register(&chunk).unwrap();
        f.receipt.bind_request(&chunk);
        f.commands
            .send(ConnectionRpcCommand::ArtifactBinaryUploadChunk {
                upload_id: "cancelled-artifact".into(),
                offset: 0,
                payload: vec![1, 2],
                response_tx: chunk,
            })
            .unwrap();
        let overflow = request(&f.commands, "synthetic/overflow");
        assert!(receive(overflow.rx).await.is_err());
        budget.cancel();
        assert!(receive(rx).await.is_err());
        f.gate.release(false);
        assert_eq!(f.next_request().await["id"], claimed.id);
        let marker = request(&f.commands, "synthetic/after-chunk-cancel");
        assert_eq!(f.next_request().await["id"], marker.id);
        f.reply(&marker.id).await;
        assert!(receive(marker.rx).await.is_ok());
        f.peer.send(Message::Close(None)).await.unwrap();
        f.task.await.unwrap();
    })
    .await
    .expect("binary cancellation regression must complete");
}

#[tokio::test]
async fn subscription_retirement_fences_queued_json_and_artifact_on_live_socket() {
    timeout(Duration::from_secs(10), async {
        for artifact in [false, true] {
            let mut f = Fixture::start().await;
            f.gate.block();
            let claimed = request(&f.commands, "synthetic/claimed");
            f.gate.wait_entered().await;
            let subscription = Arc::new(Mutex::new(true));
            let (tx, rx) = mpsc::channel();
            let queued =
                JsonRpcResponseSender::new(tx, std::time::Instant::now() + Duration::from_secs(30));
            f.receipt.bind_request(&queued);
            queued.bind_subscription(subscription.clone());
            let payload = crate::rpc::build_json_rpc_request_payload(
                "synthetic/retired-subscription",
                Value::Null,
            )
            .unwrap();
            let command = if artifact {
                ConnectionRpcCommand::ArtifactBinaryUploadChunk {
                    upload_id: "retired-subscription-artifact".into(),
                    offset: 0,
                    payload: vec![1, 2],
                    response_tx: queued,
                }
            } else {
                ConnectionRpcCommand::Request {
                    request_id: payload.request_id,
                    payload: payload.payload,
                    response_tx: queued,
                }
            };
            f.commands.send(command).unwrap();
            let overflow = request(&f.commands, "synthetic/overflow");
            assert!(
                receive(overflow.rx).await.is_err(),
                "scoped frame reached bounded writer handoff"
            );
            *subscription.lock().unwrap() = false;
            f.gate.release(false);
            assert_eq!(f.next_request().await["id"], claimed.id);
            assert!(receive(rx).await.is_err());
            let marker = request(&f.commands, "synthetic/after-subscription-retire");
            assert_eq!(
                f.next_request().await["id"],
                marker.id,
                "no retired JSON or binary frame reaches peer"
            );
            f.reply(&marker.id).await;
            assert!(receive(marker.rx).await.is_ok());
            f.peer.send(Message::Close(None)).await.unwrap();
            f.task.await.unwrap();
        }
    })
    .await
    .expect("subscription scope regression must complete");
}
