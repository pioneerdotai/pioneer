//! Offline startup regressions. These fixtures never use an installed provider or database.
use super::*;
use axum::response::IntoResponse;
use axum::{Json, Router, extract::Request, routing::any};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupPhase {
    AddressWait,
    RetryDelay,
    PreparingPlugin,
    GroupCleanupConfirmed,
}

struct PhaseGate {
    phase: StartupPhase,
    occurrence: usize,
    visits: AtomicUsize,
    entered: Notify,
    release: Notify,
}

impl PhaseGate {
    fn new(phase: StartupPhase, occurrence: usize) -> Arc<Self> {
        Arc::new(Self {
            phase,
            occurrence,
            visits: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        })
    }
}

tokio::task_local! {
    // Scoped to one startup future; parallel fixtures never share a gate.
    static PHASE_GATE: Arc<PhaseGate>;
}

fn phase_gate(phase: StartupPhase) -> Option<Arc<PhaseGate>> {
    if let Ok(gate) = PHASE_GATE.try_with(Arc::clone)
        && gate.phase == phase
        && gate.visits.fetch_add(1, Ordering::SeqCst) + 1 == gate.occurrence
    {
        Some(gate)
    } else {
        None
    }
}

pub(super) async fn pause_at(phase: StartupPhase) {
    if let Some(gate) = phase_gate(phase) {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
}

pub(super) async fn observe_address_read<F: std::future::Future>(read: F) -> F::Output {
    tokio::pin!(read);
    let mut announced = false;
    std::future::poll_fn(|cx| {
        let result = read.as_mut().poll(cx);
        if result.is_pending() && !announced {
            if let Some(gate) = phase_gate(StartupPhase::AddressWait) {
                gate.entered.notify_one();
            }
            announced = true;
        }
        result
    })
    .await
}

// Reuses the shell-child + loopback HTTP fixture pattern used by the other CLI
// adapters. Kept here so readiness exercises exactly the same startup path.
pub(crate) struct StartupFixture {
    pub(crate) root: tempfile::TempDir,
    pub(crate) instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
    http: JoinHandle<()>,
}

impl Drop for StartupFixture {
    fn drop(&mut self) {
        self.http.abort();
    }
}

impl StartupFixture {
    pub(crate) async fn new(before_address: &str, catalog: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let script = root.path().join("fake-opencode");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
cd "$(dirname "$0")" || exit 98
previous=$(tail -n 1 starts 2>/dev/null)
if [ -n "$previous" ] && kill -0 "$previous" 2>/dev/null; then
  echo overlap >> overlap
  exit 99
fi
echo $$ >> starts
echo 'synthetic OpenCode startup stderr' >&2
attempt=$(wc -l < starts | tr -d ' ')
{before_address}
printf '%s\n' '{{"url":"http://{address}"}}'
while read line; do :; done
"#
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = root.path().to_path_buf();
        let catalog = catalog.to_owned();
        let router = Router::new().fallback(any(move |request: Request| {
            let path = path.clone();
            let catalog = catalog.clone();
            async move {
                use std::io::Write;
                let route = request.uri().path();
                writeln!(std::fs::OpenOptions::new().create(true).append(true).open(path.join("requests")).unwrap(), "{route}").unwrap();
                if catalog == "health-delay" && route == "/api/info" {
                    sleep(Duration::from_millis(100)).await;
                }
                if (catalog == "health-error" && route == "/api/info")
                    || (catalog == "catalog-error" && route == "/api/model")
                    || route.ends_with("/prompt")
                {
                    return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"_tag":"FixtureError","message":"SQLiteError: database is locked"}))).into_response();
                }
                let pid: u32 = std::fs::read_to_string(path.join("starts")).unwrap().lines().last().unwrap().parse().unwrap();
                let value = match route {
                    "/api/info" => json!({"pid":if catalog == "wrong-pid" { 0 } else { pid },"version":"fixture-v2","urls":[],"paths":{"tmp":"/tmp"}}),
                    "/api/provider" => json!({"data":[{"id":"local","name":"Fixture provider"}]}),
                    "/api/model" if catalog == "ready" => json!({"data":[{"providerID":"local","id":"model","name":"Fixture model","enabled":true,"canonical":"custom-provider","capabilities":{"input":["text","image"],"output":["text"]},"limit":{"context":8192},"variants":[{"id":"high"}]}]}),
                    "/api/model" => json!({"data":[]}),
                    "/api/skill" | "/api/mcp" => json!({"data":[]}),
                    "/api/session/active" => json!({"data":{}}),
                    path if path.ends_with("/catalog") => json!({"output":[]}),
                    _ => json!({"_tag":"FixtureError","message":"unexpected request"}),
                };
                Json(value).into_response()
            }
        }));
        let http = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let instance = EffectiveGatewayCliAgentRuntimeInstanceConfig {
            id: "opencode".into(),
            kind: GatewayCliAgentRuntimeKindConfig::OpenCode,
            display_name: "Fixture".into(),
            enabled: true,
            binary_path: script.to_string_lossy().into_owned(),
            home_path: root.path().join("config").to_string_lossy().into_owned(),
            shadow_home_path: None,
            custom_models: vec!["local/custom".into()],
            app_server_args: vec![],
            startup_probe_timeout_ms: 2000,
            request_timeout_ms: 1000,
            idle_session_ttl_secs: 60,
            event_channel_capacity: 64,
            stderr_ring_lines: 40,
            debug_native_events: false,
        };
        Self {
            root,
            instance,
            http,
        }
    }

    async fn start(&self) -> Result<OpenCodeServerProcess> {
        self.start_with_env(&SensitiveEnvironment::new()).await
    }

    async fn start_with_env(&self, env: &SensitiveEnvironment) -> Result<OpenCodeServerProcess> {
        spawn_opencode_server(
            &self.instance,
            self.root.path(),
            env,
            None,
            &self.root.path().join("data"),
        )
        .await
    }

    pub(crate) fn starts(&self) -> Vec<u32> {
        std::fs::read_to_string(self.root.path().join("starts"))
            .unwrap_or_default()
            .lines()
            .map(|line| line.parse().unwrap())
            .collect()
    }

    fn assert_stopped(&self) {
        for pid in self.starts() {
            // Signal zero only observes the fake child, never an installed CLI.
            assert_eq!(
                unsafe { libc::kill(pid as i32, 0) },
                -1,
                "owned child {pid} survived cleanup"
            );
        }
        assert!(!self.root.path().join("overlap").exists());
    }

    async fn wait_stopped(&self) {
        let groups = self.starts();
        let mut pids = groups.clone();
        if let Ok(descendant) = std::fs::read_to_string(self.root.path().join("descendant")) {
            pids.push(descendant.trim().parse().unwrap());
        }
        tokio_timeout(Duration::from_secs(2), async {
            while pids.iter().any(|pid| process_present(*pid))
                || groups.iter().any(|pgid| group_present(*pgid))
            {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("owned fake parent/descendant must disappear after cleanup");
        self.assert_stopped();
    }

    async fn wait_for_starts(&self, count: usize) {
        tokio_timeout(Duration::from_secs(2), async {
            while self.starts().len() < count {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}

fn process_present(pid: u32) -> bool {
    // SAFETY: signal zero only observes a PID recorded by this fixture.
    if unsafe { libc::kill(i32::try_from(pid).unwrap(), 0) } == 0 {
        return true;
    }
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    false
}

fn group_present(pgid: u32) -> bool {
    // SAFETY: each PGID is a fake group leader recorded by this fixture.
    if unsafe { libc::kill(-i32::try_from(pgid).unwrap(), 0) } == 0 {
        return true;
    }
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    false
}

const BUSY: &str = "exec 1>&-\nsleep 0.03\necho 'SQLiteError: database is locked' >&2\nexit 7";

#[tokio::test]
async fn sqlite_startup_conflict_retries_after_exit_and_returns_normal_server() {
    let fixture = StartupFixture::new(
        &format!("if [ \"$attempt\" = 1 ]; then\n{BUSY}\nfi"),
        "ready",
    )
    .await;
    let server = fixture.start().await.unwrap();
    assert_eq!(fixture.starts().len(), 2);
    assert_eq!(server.version.as_deref(), Some("fixture-v2"));
    assert!(
        server
            .api
            .models(Duration::from_secs(1))
            .await
            .unwrap()
            .is_array()
    );
    server.close().await.unwrap();
    fixture.assert_stopped();
}

#[tokio::test]
async fn sqlite_startup_conflict_exhausts_three_attempts_with_exit_and_late_stderr() {
    let fixture = StartupFixture::new(BUSY, "ready").await;
    let error = fixture.start().await.err().unwrap();
    let failure = error.downcast_ref::<OpenCodeStartupFailure>().unwrap();
    assert!(failure.sqlite_busy);
    assert_eq!(failure.status.unwrap().code(), Some(7));
    assert!(
        failure
            .stderr
            .iter()
            .any(|line| line == "SQLiteError: database is locked")
    );
    assert!(format!("{error:#}").contains("3 attempt(s)"));
    assert_eq!(fixture.starts().len(), 3);
    fixture.assert_stopped();
}

#[tokio::test]
async fn sqlite_startup_conflict_uses_one_deadline_including_backoff() {
    let mut fixture = StartupFixture::new(
        &format!(
            "if [ \"$attempt\" = 1 ]; then\nsleep 0.3\n{BUSY}\nfi\nwhile read line; do :; done"
        ),
        "ready",
    )
    .await;
    fixture.instance.startup_probe_timeout_ms = 1000;
    let started = Instant::now();
    let error = fixture.start().await.err().unwrap();
    assert!(format!("{error:#}").contains("timed out"));
    assert_eq!(fixture.starts().len(), 2);
    // A fresh 1s timeout on the second attempt would exceed 1.4s.
    assert!(started.elapsed() < Duration::from_millis(1250));
    fixture.wait_stopped().await;
}

#[tokio::test]
async fn non_sqlite_exit_eof_auth_and_invalid_address_never_retry() {
    for (body, stage) in [
        (
            "echo 'unrecognized option' >&2; exit 2",
            OpenCodeStartupStage::AddressEof,
        ),
        (
            "echo 'database is locked' >&2; exit 7",
            OpenCodeStartupStage::AddressEof,
        ),
        (
            "echo 'token rejected' >&2; exit 3",
            OpenCodeStartupStage::AddressEof,
        ),
        (
            "echo 'SQLiteError: database is locked' >&2; kill -TERM $$",
            OpenCodeStartupStage::AddressEof,
        ),
        ("exit 0", OpenCodeStartupStage::AddressEof),
        (
            "echo 'SQLiteError: database is locked' >&2; echo '{\"url\":\"http://example.com:1234\"}'; exit 7",
            OpenCodeStartupStage::AddressInvalid,
        ),
    ] {
        let fixture = StartupFixture::new(body, "ready").await;
        let error = fixture.start().await.err().unwrap();
        let failure = error.downcast_ref::<OpenCodeStartupFailure>().unwrap();
        assert_eq!(failure.stage, stage, "{error:#}");
        assert!(!failure.sqlite_busy);
        assert!(failure.status.is_some());
        assert_eq!(fixture.starts().len(), 1);
        fixture.assert_stopped();
    }
}

#[tokio::test]
async fn startup_timeout_never_retries_even_with_sqlite_stderr() {
    let mut fixture = StartupFixture::new(
        "echo 'SQLiteError: database is locked' >&2; while read line; do :; done",
        "ready",
    )
    .await;
    fixture.instance.startup_probe_timeout_ms = 100;
    let error = fixture.start().await.err().unwrap();
    let failure = error.downcast_ref::<OpenCodeStartupFailure>().unwrap();
    assert_eq!(failure.stage, OpenCodeStartupStage::Timeout);
    assert!(!failure.sqlite_busy);
    assert_eq!(fixture.starts().len(), 1);
    fixture.wait_stopped().await;
}

#[tokio::test]
async fn startup_missing_binary_and_reserved_args_never_spawn_retry() {
    let mut fixture = StartupFixture::new("", "ready").await;
    fixture.instance.app_server_args = vec!["--port=1234".into()];
    assert!(fixture.start().await.is_err());
    fixture.instance.app_server_args.clear();
    fixture.instance.binary_path = fixture
        .root
        .path()
        .join("absent")
        .to_string_lossy()
        .into_owned();
    let error = fixture.start().await.err().unwrap();
    assert!(error.chain().any(|source| {
        source
            .downcast_ref::<std::io::Error>()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
    }));
    assert!(fixture.starts().is_empty());
}

#[tokio::test]
async fn startup_diagnostics_redact_password_env_config_and_unlabelled_secret() {
    let fixture = StartupFixture::new("echo \"$OPENCODE_PASSWORD\" > password; echo \"$OPENCODE_PASSWORD $UNLABELLED_SECRET $OPENCODE_CONFIG_CONTENT $CUSTOM_FLAG\" >&2; echo 'authorization: Bearer private-canary' >&2; exit 9", "ready").await;
    let mut env = SensitiveEnvironment::new();
    env.insert_secret("UNLABELLED_SECRET", SecretString::new("environment-canary"));
    env.insert_plain("CUSTOM_FLAG", "unlabelled-canary");
    let error = fixture.start_with_env(&env).await.err().unwrap();
    let failure = error.downcast_ref::<OpenCodeStartupFailure>().unwrap();
    let published = format!("{error:#} {failure:?}");
    let password = std::fs::read_to_string(fixture.root.path().join("password")).unwrap();
    for secret in [
        password.trim(),
        "environment-canary",
        "unlabelled-canary",
        "private-canary",
        "\"plugins\"",
        "\"update\"",
    ] {
        assert!(!published.contains(secret), "{published}");
    }
    assert!(published.contains("[REDACTED]"));
    assert_eq!(failure.status.unwrap().code(), Some(9));
    assert_eq!(fixture.starts().len(), 1);
    fixture.assert_stopped();
}

#[tokio::test]
async fn startup_cancellation_during_address_wait_cleans_owned_children() {
    let fixture = StartupFixture::new("while read line; do :; done", "ready").await;
    let gate = PhaseGate::new(StartupPhase::AddressWait, 1);
    {
        let future = PHASE_GATE.scope(gate.clone(), fixture.start());
        tokio::pin!(future);
        tokio::select! {
            result = &mut future => panic!("startup unexpectedly finished: {}", result.is_ok()),
            entered = tokio_timeout(Duration::from_secs(2), gate.entered.notified()) => entered.unwrap()
        }
        fixture.wait_for_starts(1).await;
        assert!(process_present(fixture.starts()[0]));
    }
    fixture.wait_stopped().await;
    assert_eq!(fixture.starts().len(), 1);
}

#[tokio::test]
async fn startup_cancellation_during_confirmed_retry_delay_does_not_spawn_again() {
    let fixture = StartupFixture::new(BUSY, "ready").await;
    let gate = PhaseGate::new(StartupPhase::RetryDelay, 1);
    {
        let future = PHASE_GATE.scope(gate.clone(), fixture.start());
        tokio::pin!(future);
        tokio::select! {
            result = &mut future => panic!("startup unexpectedly finished: {}", result.is_ok()),
            entered = tokio_timeout(Duration::from_secs(2), gate.entered.notified()) => entered.unwrap()
        }
        // The backoff future is entered only after the cleanup barrier returned.
        fixture.assert_stopped();
        assert_eq!(fixture.starts().len(), 1);
    }
    fixture.wait_stopped().await;
    assert_eq!(fixture.starts().len(), 1);
}

#[tokio::test]
async fn startup_pid_health_failures_never_retry_and_health_shares_address_deadline() {
    for mode in ["wrong-pid", "health-error", "health-delay"] {
        let mut fixture = StartupFixture::new(
            "sleep 0.08; echo 'SQLiteError: database is locked' >&2",
            mode,
        )
        .await;
        fixture.instance.startup_probe_timeout_ms = if mode == "health-delay" { 150 } else { 2000 };
        let error = fixture.start().await.err().unwrap();
        let failure = error.downcast_ref::<OpenCodeStartupFailure>().unwrap();
        assert!(!failure.sqlite_busy);
        if mode == "health-delay" {
            assert_eq!(failure.stage, OpenCodeStartupStage::Timeout);
        } else {
            assert_eq!(failure.stage, OpenCodeStartupStage::Health);
        }
        assert_eq!(fixture.starts().len(), 1);
        fixture.wait_stopped().await;
    }
}

#[tokio::test]
async fn startup_success_owns_one_process_and_prompt_failure_does_not_respawn() {
    let fixture = StartupFixture::new("", "ready").await;
    let server = fixture.start().await.unwrap();
    assert_eq!(fixture.starts().len(), 1);
    assert!(
        server
            .api
            .prompt(
                "ses_test",
                &json!({"id":"msg_test","text":"one input"}),
                Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.starts().len(), 1);
    let requests = std::fs::read_to_string(fixture.root.path().join("requests")).unwrap();
    assert_eq!(
        requests
            .lines()
            .filter(|line| line.ends_with("/prompt"))
            .count(),
        1
    );
    server.close().await.unwrap();
    fixture.assert_stopped();
}

#[tokio::test]
async fn startup_cleanup_failure_stops_sqlite_retries_when_descendant_holds_stderr() {
    let mut fixture = StartupFixture::new(
        "(exec 1>&-; sleep 30) &\necho $! > descendant\necho 'SQLiteError: database is locked' >&2\nexit 7",
        "ready",
    )
    .await;
    fixture.instance.startup_probe_timeout_ms = 400;
    let error = fixture.start().await.err().unwrap();
    let failure = error.downcast_ref::<OpenCodeStartupFailure>().unwrap();
    assert!(failure.cleanup_error.is_some());
    assert_eq!(fixture.starts().len(), 1);
    fixture.wait_stopped().await;
    let descendant: i32 = std::fs::read_to_string(fixture.root.path().join("descendant"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        unsafe { libc::kill(descendant, 0) },
        -1,
        "descendant survived cleanup"
    );
}

#[tokio::test]
async fn startup_retry_requires_group_cleanup_even_when_descendant_closed_all_pipes() {
    let fixture = StartupFixture::new(
        r#"
if [ "$attempt" = 1 ]; then
  (exec </dev/null >/dev/null 2>/dev/null; echo ready > closed-ready; exec sleep 30) &
  echo $! > descendant
  while [ ! -f closed-ready ]; do sleep 0.005; done
  echo 'SQLiteError: database is locked' >&2
  exit 7
fi
if kill -0 "$(cat descendant)" 2>/dev/null; then
  echo descendant-overlap >> overlap
  exit 99
fi
"#,
        "ready",
    )
    .await;
    let gate = PhaseGate::new(StartupPhase::GroupCleanupConfirmed, 1);
    let future = PHASE_GATE.scope(gate.clone(), fixture.start());
    tokio::pin!(future);
    tokio::select! {
        entered = tokio_timeout(Duration::from_secs(3), gate.entered.notified()) => {
            entered.unwrap();
            // The owner is still alive here. Its Drop cannot mask a missing
            // cleanup barrier by killing the descendant during error return.
            assert_eq!(fixture.starts().len(), 1);
            let descendant = std::fs::read_to_string(fixture.root.path().join("descendant"))
                .unwrap().trim().parse().unwrap();
            assert!(!process_present(descendant));
            assert!(!group_present(fixture.starts()[0]));
            gate.release.notify_one();
            let server = future.await.unwrap();
            assert_eq!(fixture.starts().len(), 2);
            assert!(
                !process_present(descendant),
                "retry overlapped the previous attempt's descendant"
            );
            server.close().await.unwrap();
        },
        result = &mut future => {
            let error = result.err().expect("a successful retry must pass the cleanup barrier");
            // An orphaned zombie can retain the group until the OS reaps it.
            // Failure to confirm disappearance must stop, never allow a retry.
            let failure = error.downcast_ref::<OpenCodeStartupFailure>().unwrap();
            assert!(failure.sqlite_busy);
            assert!(failure.cleanup_error.is_some());
            assert!(!format!("{error:#}").contains("cleanup: complete"));
            assert_eq!(fixture.starts().len(), 1);
        },
    }
    fixture.wait_stopped().await;
}

#[tokio::test]
async fn startup_prepare_deadline_preserves_last_process_cause_without_another_spawn() {
    let fixture = StartupFixture::new(BUSY, "ready").await;
    let gate = PhaseGate::new(StartupPhase::PreparingPlugin, 2);
    let budget = Duration::from_millis(fixture.instance.startup_probe_timeout_ms);
    let env = SensitiveEnvironment::new();
    let future = PHASE_GATE.scope(
        gate.clone(),
        probe_opencode_instance(&fixture.instance, fixture.root.path(), &env),
    );
    tokio::pin!(future);
    tokio::select! {
        _ = &mut future => panic!("startup must reach preparation of its second attempt"),
        entered = tokio_timeout(Duration::from_secs(2), gate.entered.notified()) => entered.unwrap()
    }
    assert_eq!(fixture.starts().len(), 1);
    fixture.assert_stopped();
    // No fake process remains. Advance a full budget from this checkpoint so
    // the startup deadline expires regardless of time spent before it began.
    tokio::time::pause();
    tokio::time::advance(budget).await;
    let expired = Instant::now();
    let probe = future.await;
    assert_eq!(
        Instant::now(),
        expired,
        "preparation must already be expired"
    );
    assert_eq!(probe.status, OpenCodeProbeStatus::SpawnFailed);
    let message = probe.message.unwrap();
    assert!(message.contains("preparing catalog plugin"), "{message}");
    assert!(
        message.contains("no process was spawned for this attempt"),
        "{message}"
    );
    assert!(message.contains("attempt 2"), "{message}");
    assert!(
        message.contains("last completed process attempt 1 follows"),
        "{message}"
    );
    assert!(message.contains("exit status: 7"), "{message}");
    assert!(message.contains("SQLite database contention"), "{message}");
    assert!(
        probe
            .stderr
            .iter()
            .any(|line| line == "SQLiteError: database is locked")
    );
    assert!(
        probe
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message == message)
    );
    assert_eq!(fixture.starts().len(), 1);
    fixture.assert_stopped();
}

#[tokio::test]
async fn startup_failed_probe_keeps_bounded_attempt_stderr_and_actual_exit_status() {
    let fixture = StartupFixture::new(
        "exec 1>&-; sleep 0.03; echo 'ordinary initialization failed' >&2; exit 9",
        "ready",
    )
    .await;
    let probe = probe_opencode_instance(
        &fixture.instance,
        fixture.root.path(),
        &SensitiveEnvironment::new(),
    )
    .await;
    assert_eq!(probe.status, OpenCodeProbeStatus::SpawnFailed);
    assert!(probe.message.as_deref().unwrap().contains("exit status: 9"));
    assert!(
        probe
            .message
            .as_deref()
            .unwrap()
            .contains("ordinary initialization failed")
    );
    assert!(
        probe
            .stderr
            .iter()
            .any(|line| line == "ordinary initialization failed")
    );
    assert_eq!(fixture.starts().len(), 1);
    fixture.assert_stopped();
}

#[tokio::test]
async fn startup_public_diagnostics_bound_lines_and_strip_controls() {
    let fixture = StartupFixture::new(
        "i=0; while [ $i -lt 60 ]; do printf '\\033[31merror detail %0800d\\n' \"$i\" >&2; i=$((i+1)); done; exit 8",
        "ready",
    ).await;
    let error = fixture.start().await.err().unwrap();
    let failure = error.downcast_ref::<OpenCodeStartupFailure>().unwrap();
    assert_eq!(
        failure.stderr.len(),
        pioneer_protocol::RUNTIME_DIAGNOSTIC_MAX_LINES
    );
    // The shared sanitizer adds a short truncation-count suffix after its limit.
    assert!(failure.stderr.iter().all(|line| line.chars().count()
        <= pioneer_protocol::RUNTIME_DIAGNOSTIC_LINE_MAX_CHARS + 40
        && !line.contains('\u{1b}')));
    assert!(failure.stderr[0].contains("[truncated"));
    assert!(format!("{error:#}").len() < 2300);
    fixture.assert_stopped();
}
