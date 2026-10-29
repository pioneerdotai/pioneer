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
    reported: bool,
    failed: bool,
    waker: Option<Waker>,
}

#[derive(Default)]
struct WriteGate {
    state: Mutex<GateState>,
    entered: tokio::sync::Notify,
}

impl WriteGate {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.blocked = true;
        state.reported = false;
    }

    fn block_flush(&self) {
        let mut state = self.state.lock().unwrap();
        state.flush_blocked = true;
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

    fn release(&self, failed: bool) {
        let mut state = self.state.lock().unwrap();
        state.blocked = false;
        state.flush_blocked = false;
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
        Pin::new(&mut self.inner).poll_write(cx, buf)
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
        Pin::new(&mut self.inner).poll_flush(cx)
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
        let (start, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            started.await.expect("start monitor fixture");
            monitor_connection(stream, &mut active_spec, 7, &events, &mut command_rx.0).await
        });
        (
            Self {
                peer,
                commands,
                gate,
                task,
                spec,
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

#[tokio::test]
async fn delayed_voice_tail_and_finalize_keep_socket_order_without_per_packet_ack() {
    timeout(Duration::from_secs(10), async {
        let mut fixture = Fixture::start().await;
        let pcm = crate::transport::ws::frames::encode_voice_audio_chunk_frame(
            "voice_session_integrity".into(),
            0,
            pioneer_protocol::VoiceAudioFormat::pioneer_streaming_target(),
            None,
            None,
            &[0; 14],
        )
        .unwrap();
        fixture.gate.block();
        let budget = Arc::new(super::super::voice_budget::VoiceTransportBudget::default());
        fixture
            .commands
            .send(ConnectionRpcCommand::VoiceBinaryChunk {
                payload: budget.admit(pcm.clone()).unwrap(),
            })
            .unwrap();
        fixture.gate.wait_entered().await;
        // The in-flight socket write still owns its capacity reservation.
        assert!(budget.admit(vec![0; 256 * 1024]).is_err());
        let finalize = crate::rpc::build_json_rpc_request_payload(
            "voice/session/finalize",
            json!({
                "session_id":"voice_session_integrity",
                "audio_integrity":{"next_sequence":1,"normalized_samples":7}
            }),
        )
        .unwrap();
        let (response_tx, response_rx) = mpsc::channel();
        fixture
            .commands
            .send(ConnectionRpcCommand::Request {
                request_id: finalize.request_id.clone(),
                payload: finalize.payload,
                response_tx: JsonRpcResponseSender::new(
                    response_tx,
                    std::time::Instant::now() + Duration::from_secs(30),
                ),
            })
            .unwrap();
        assert!(matches!(
            response_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        fixture.gate.release(false);
        assert_eq!(fixture.next_data().await, Message::Binary(pcm.into()));
        let observed = fixture.next_request().await;
        assert_eq!(observed["method"], "voice/session/finalize");
        assert_eq!(
            observed["params"]["audio_integrity"],
            json!({"next_sequence":1,"normalized_samples":7})
        );
        assert!(matches!(
            response_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        fixture.reply(&finalize.request_id).await;
        assert!(receive(response_rx).await.is_ok());
        fixture.task.abort();
        let _ = fixture.task.await;
    })
    .await
    .unwrap();
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
    // Admission and incoming bytes are both ready before the monitor starts.
    let live = request(&fixture.commands, "synthetic/inbound-progress");
    start.send(()).unwrap();
    let observed = timeout(Duration::from_secs(5), fixture.next_request()).await;
    let still_active = inbound.active.load(Ordering::Acquire);
    let read = inbound.read.load(Ordering::Acquire);
    // Stop only after observing the outgoing request (or the deadlock fuse),
    // before asserting, so the old busy reader can always leave its poll loop.
    drop(stop);
    fixture.task.abort();
    let _ = fixture.task.await;
    let observed = observed.expect("outgoing RPC must arrive while inbound stays ready");
    assert!(
        read,
        "actual WebSocket reader must have consumed ready inbound"
    );
    assert!(still_active, "source must not stop to admit the command");
    assert_eq!(observed["id"], live.id);
}

#[tokio::test]
async fn blocked_write_keeps_reader_live_and_timeout_retires_queued_frame() {
    let phase = std::cell::Cell::new("starting fixture");
    timeout(Duration::from_secs(10), async {
        let mut f = Fixture::start().await;
        phase.set("receiving first request");
        let first = request(&f.commands, "synthetic/first");
        assert_eq!(f.next_request().await["id"], first.id);
        // The peer can respond before send/flush completes. Registration must
        // already exist, and response processing must not depend on writer exit.
        phase.set("receiving request with flush blocked");
        f.gate.block_flush();
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
            response_tx: artifact,
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
    f.gate.block();
    f.commands
        .send(ConnectionRpcCommand::VoiceBinaryChunk {
            payload: Arc::new(super::super::voice_budget::VoiceTransportBudget::default())
                .admit(vec![5, 6])
                .unwrap(),
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
            response_tx: artifact_tx,
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
