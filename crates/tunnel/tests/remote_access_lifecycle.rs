//! Local wire-protocol fixtures exercise the actual embedded client and supervisor.
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};

use pioneer_config::GatewayRemoteAccessConfig;
use pioneer_protocol::{
    GatewayRemoteAccessErrorKind as ErrorKind, GatewayRemoteAccessSettings,
    GatewayRemoteAccessState as State, GatewayRemoteAccessStatusSnapshot,
};
use pioneer_tunnel::{RemoteAccessDesiredState, RemoteAccessSupervisor};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};

const KEY: &str = "local-fixture-key";
// Longer than the client's first retry (400–600ms) and the supervisor's 10ms restart.
const QUIET: Duration = Duration::from_secs(2);
const DEADLINE: Duration = Duration::from_secs(5);
const ACCEPT: u8 = 0;
const SERVICE_NOT_EXIST: u8 = 1;
const AUTH_FAILED: u8 = 2;
const STALL_HANDSHAKE: u8 = 3;
const NETWORK_FAILURE: u8 = 4;
const OPEN_DATA: u8 = 5;
const STALL_DATA: u8 = 6;

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[derive(Default)]
struct Counts {
    controls: AtomicUsize,
    data: AtomicUsize,
    active: AtomicUsize,
}

struct Active(Arc<Counts>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Fixture {
    addr: SocketAddr,
    mode: Arc<AtomicU8>,
    counts: Arc<Counts>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Fixture {
    async fn start(mode: u8) -> Self {
        Self::bind(mode, "127.0.0.1:0".parse().unwrap()).await
    }

    async fn bind(mode: u8, addr: SocketAddr) -> Self {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mode = Arc::new(AtomicU8::new(mode));
        let counts = Arc::new(Counts::default());
        let (stop, mut stopped) = oneshot::channel();
        let task_mode = mode.clone();
        let task_counts = counts.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let counts = task_counts.clone();
                        let mode = task_mode.clone();
                        counts.active.fetch_add(1, Ordering::SeqCst);
                        connections.spawn(async move {
                            let _active = Active(counts.clone());
                            // A canceled client may close at any protocol boundary.
                            let _ = serve(socket, mode, counts).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            addr,
            mode,
            counts,
            stop: Some(stop),
            task: Some(task),
        }
    }

    fn attempts(&self) -> usize {
        self.counts.controls.load(Ordering::SeqCst)
    }
    fn set_mode(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    async fn quiet(&self, expected: usize) {
        sleep(QUIET).await;
        assert_eq!(self.attempts(), expected, "old client connected again");
    }

    async fn closed(&self) {
        wait_until(|| self.counts.active.load(Ordering::SeqCst) == 0).await;
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.take().unwrap().await.unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn serve(
    mut socket: TcpStream,
    mode: Arc<AtomicU8>,
    counts: Arc<Counts>,
) -> std::io::Result<()> {
    let mut hello = [0; 37];
    socket.read_exact(&mut hello).await?;
    if hello[..4] == 1u32.to_le_bytes() {
        counts.data.fetch_add(1, Ordering::SeqCst);
        if mode.load(Ordering::SeqCst) != STALL_DATA {
            socket.write_all(&0u32.to_le_bytes()).await?; // StartForwardTcp
        }
    } else {
        assert_eq!(&hello[..5], &[0, 0, 0, 0, 1]);
        counts.controls.fetch_add(1, Ordering::SeqCst);
        let selected = mode.load(Ordering::SeqCst);
        if selected == NETWORK_FAILURE {
            return Ok(());
        }
        if selected != STALL_HANDSHAKE {
            let nonce = [7; 32];
            socket.write_all(&[0, 0, 0, 0, 1]).await?;
            socket.write_all(&nonce).await?;
            let mut proof = [0; 32];
            socket.read_exact(&mut proof).await?;
            let mut expected = hash(KEY.as_bytes()).to_vec();
            expected.extend(nonce);
            let ack = match selected {
                AUTH_FAILED | SERVICE_NOT_EXIST => selected,
                _ if proof != hash(&expected) => AUTH_FAILED,
                _ => ACCEPT,
            };
            socket.write_all(&u32::from(ack).to_le_bytes()).await?;
            if ack != ACCEPT {
                return Ok(());
            }
            if matches!(selected, OPEN_DATA | STALL_DATA) {
                socket.write_all(&0u32.to_le_bytes()).await?; // CreateDataChannel
            }
        }
    }
    // Detect closure of handshake, control and forwarding sockets.
    let mut bytes = [0; 128];
    while socket.read(&mut bytes).await? != 0 {}
    Ok(())
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    timeout(DEADLINE, async {
        while !condition() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

fn desired(enabled: bool, key: &str) -> RemoteAccessDesiredState {
    RemoteAccessDesiredState {
        settings: GatewayRemoteAccessSettings {
            enabled,
            has_key: true,
            ..Default::default()
        },
        key: Some(key.to_owned()),
    }
}

fn gateway(
    home: &tempfile::TempDir,
    fixture: &Fixture,
    max_restarts: u32,
) -> RemoteAccessSupervisor {
    RemoteAccessSupervisor::new(
        home.path(),
        GatewayRemoteAccessConfig {
            relay_addr: fixture.addr.to_string(),
            local_addr: "127.0.0.1:1".to_owned(),
            restart_initial_ms: 10,
            restart_max_ms: 10,
            restart_jitter_percent: 0,
            max_restarts,
            ..Default::default()
        },
    )
    .unwrap()
}

async fn status(
    rx: &mut watch::Receiver<GatewayRemoteAccessStatusSnapshot>,
    state: State,
    kind: Option<ErrorKind>,
) {
    timeout(DEADLINE, async {
        loop {
            let current = rx.borrow().clone();
            if current.state == state && current.error_kind == kind {
                break;
            }
            rx.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

async fn terminal_case(mode: u8, kind: ErrorKind, max_restarts: u32) {
    let fixture = Fixture::start(mode).await;
    let home = tempfile::tempdir().unwrap();
    let gateway = gateway(&home, &fixture, max_restarts);
    let mut rx = gateway.subscribe_status();
    gateway.apply(desired(true, KEY)).await.unwrap();
    status(&mut rx, State::Failed, Some(kind)).await;
    let failed = gateway.status_snapshot();
    fixture.closed().await;
    fixture.quiet(1).await;
    assert_eq!(
        gateway.status_snapshot(),
        failed,
        "cleanup or restart overwrote the rejection"
    );
    assert!(!failed.message.unwrap().contains(KEY));
    gateway.shutdown().await;
    fixture.stop().await;
}

#[tokio::test]
async fn authentication_rejection_stops_internal_and_external_retries() {
    // Both unlimited and bounded outer restart policies must treat rejection as terminal.
    terminal_case(AUTH_FAILED, ErrorKind::TunnelAuthFailed, 0).await;
    terminal_case(AUTH_FAILED, ErrorKind::TunnelAuthFailed, 1).await;
}

#[tokio::test]
async fn missing_service_is_terminal() {
    terminal_case(SERVICE_NOT_EXIST, ErrorKind::InvalidSettings, 0).await;
}

#[tokio::test]
async fn correcting_the_key_starts_a_new_connected_generation() {
    let fixture = Fixture::start(ACCEPT).await;
    let home = tempfile::tempdir().unwrap();
    let gateway = gateway(&home, &fixture, 0);
    let mut rx = gateway.subscribe_status();
    gateway
        .apply(desired(true, "wrong-local-key"))
        .await
        .unwrap();
    status(&mut rx, State::Failed, Some(ErrorKind::TunnelAuthFailed)).await;
    fixture.quiet(1).await;
    gateway.apply(desired(true, KEY)).await.unwrap();
    status(&mut rx, State::Connected, None).await;
    fixture.quiet(2).await;
    assert_eq!(gateway.status_snapshot().state, State::Connected);
    gateway.shutdown().await;
    fixture.closed().await;
    fixture.stop().await;
}

#[tokio::test]
async fn explicit_retry_via_existing_settings_switch_recovers_with_the_same_key() {
    for rejection in [AUTH_FAILED, SERVICE_NOT_EXIST] {
        let fixture = Fixture::start(rejection).await;
        let home = tempfile::tempdir().unwrap();
        let gateway = gateway(&home, &fixture, 0);
        let mut rx = gateway.subscribe_status();
        let kind = if rejection == AUTH_FAILED {
            ErrorKind::TunnelAuthFailed
        } else {
            ErrorKind::InvalidSettings
        };
        gateway.apply(desired(true, KEY)).await.unwrap();
        status(&mut rx, State::Failed, Some(kind)).await;
        fixture.set_mode(ACCEPT);
        fixture.quiet(1).await; // Server recovery alone cannot silently clear a terminal failure.
        gateway.apply(desired(false, KEY)).await.unwrap();
        assert_eq!(gateway.status_snapshot().state, State::Disabled);
        gateway.apply(desired(true, KEY)).await.unwrap();
        status(&mut rx, State::Connected, None).await;
        assert_eq!(fixture.attempts(), 2);
        gateway.shutdown().await;
        fixture.closed().await;
        fixture.stop().await;
    }
}

#[tokio::test]
async fn transient_network_failure_recovers_automatically() {
    let fixture = Fixture::start(NETWORK_FAILURE).await;
    let home = tempfile::tempdir().unwrap();
    // A live client recovers through its internal backoff.
    let gateway = gateway(&home, &fixture, 1);
    let mut rx = gateway.subscribe_status();
    gateway.apply(desired(true, KEY)).await.unwrap();
    status(&mut rx, State::Failed, Some(ErrorKind::RelayConnectFailed)).await;
    fixture.set_mode(ACCEPT);
    status(&mut rx, State::Connected, None).await;
    assert!(fixture.attempts() >= 2);
    // Also recover after loss of an already connected control channel.
    let addr = fixture.addr;
    fixture.stop().await;
    status(
        &mut rx,
        State::Reconnecting,
        Some(ErrorKind::RelayConnectFailed),
    )
    .await;
    let recovered = Fixture::bind(ACCEPT, addr).await;
    status(&mut rx, State::Connected, None).await;
    assert_eq!(recovered.attempts(), 1);
    gateway.shutdown().await;
    recovered.closed().await;
    recovered.stop().await;
}

#[tokio::test]
async fn disable_and_generation_change_cancel_handshake_and_backoff() {
    for mode in [STALL_HANDSHAKE, NETWORK_FAILURE] {
        for replace in [false, true] {
            let fixture = Fixture::start(mode).await;
            let home = tempfile::tempdir().unwrap();
            let gateway = gateway(&home, &fixture, 0);
            let mut rx = gateway.subscribe_status();
            gateway.apply(desired(true, "old-local-key")).await.unwrap();
            wait_until(|| fixture.attempts() == 1).await;
            if mode == NETWORK_FAILURE {
                status(&mut rx, State::Failed, Some(ErrorKind::RelayConnectFailed)).await;
            }
            fixture.set_mode(ACCEPT);
            timeout(Duration::from_secs(1), gateway.apply(desired(replace, KEY)))
                .await
                .unwrap()
                .unwrap();
            if replace {
                status(&mut rx, State::Connected, None).await;
                fixture.quiet(2).await;
                assert_eq!(gateway.status_snapshot().state, State::Connected);
                assert_eq!(fixture.counts.active.load(Ordering::SeqCst), 1);
            } else {
                fixture.closed().await;
                fixture.quiet(1).await;
                assert_eq!(gateway.status_snapshot().state, State::Disabled);
            }
            gateway.shutdown().await;
            fixture.closed().await;
            fixture.stop().await;
        }
    }
}

#[tokio::test]
async fn shutdown_joins_live_tcp_forwarding_tasks() {
    let fixture = Fixture::start(OPEN_DATA).await;
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let home = tempfile::tempdir().unwrap();
    let gateway = RemoteAccessSupervisor::new(
        home.path(),
        GatewayRemoteAccessConfig {
            relay_addr: fixture.addr.to_string(),
            local_addr: upstream.local_addr().unwrap().to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    gateway.apply(desired(true, KEY)).await.unwrap();
    let (mut local, _) = timeout(DEADLINE, upstream.accept()).await.unwrap().unwrap();
    assert_eq!(fixture.counts.data.load(Ordering::SeqCst), 1);
    gateway.shutdown().await;
    let mut byte = [0];
    assert_eq!(
        timeout(DEADLINE, local.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    fixture.closed().await;
    fixture.quiet(1).await;
    fixture.stop().await;
}

#[tokio::test]
async fn shutdown_cancels_a_pending_data_channel_handshake() {
    let fixture = Fixture::start(STALL_DATA).await;
    let home = tempfile::tempdir().unwrap();
    let gateway = gateway(&home, &fixture, 0);
    gateway.apply(desired(true, KEY)).await.unwrap();
    wait_until(|| fixture.counts.data.load(Ordering::SeqCst) == 1).await;
    timeout(Duration::from_secs(1), gateway.shutdown())
        .await
        .unwrap();
    fixture.closed().await;
    fixture.quiet(1).await;
    fixture.stop().await;
}

#[tokio::test]
async fn dropping_the_supervisor_stops_handshake_and_backoff_tasks() {
    for mode in [STALL_HANDSHAKE, NETWORK_FAILURE] {
        let fixture = Fixture::start(mode).await;
        let home = tempfile::tempdir().unwrap();
        let gateway = gateway(&home, &fixture, 0);
        let mut rx = gateway.subscribe_status();
        gateway.apply(desired(true, KEY)).await.unwrap();
        wait_until(|| fixture.attempts() == 1).await;
        if mode == NETWORK_FAILURE {
            status(&mut rx, State::Failed, Some(ErrorKind::RelayConnectFailed)).await;
        }
        drop(gateway);
        status(&mut rx, State::Stopped, None).await;
        fixture.closed().await;
        fixture.quiet(1).await;
        fixture.stop().await;
    }
}

#[test]
fn terminal_failures_emit_one_secret_free_sentry_error_per_generation() {
    use tracing_subscriber::prelude::*;
    // TestTransport captures locally. No production Sentry client or network transport is used.
    let events = sentry::test::with_captured_events(|| {
        let subscriber =
            tracing_subscriber::registry().with(sentry::integrations::tracing::layer());
        tracing::subscriber::with_default(subscriber, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    terminal_case(AUTH_FAILED, ErrorKind::TunnelAuthFailed, 0).await;
                    terminal_case(SERVICE_NOT_EXIST, ErrorKind::InvalidSettings, 0).await;
                });
        });
    });
    assert_eq!(
        events.len(),
        2,
        "one diagnostic for each terminal generation"
    );
    for event in events {
        assert_eq!(event.level, sentry::Level::Error);
        assert_eq!(
            event.message.as_deref(),
            Some("Remote access control channel rejected")
        );
        let serialized = serde_json::to_string(&event).unwrap();
        assert!(!serialized.contains(KEY));
        assert!(!serialized.contains(&hex::encode(hash(KEY.as_bytes()))));
        assert!(!serialized.contains("Retry in"));
    }
}
