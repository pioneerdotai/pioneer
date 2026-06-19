use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use pioneer_config::GatewayRemoteAccessConfig;
use pioneer_protocol::{
    GatewayRemoteAccessErrorKind, GatewayRemoteAccessSettings, GatewayRemoteAccessState,
    GatewayRemoteAccessStatusSnapshot, GatewayRemoteAccessTransport,
};
use rathole::{RatholeClientEvent, RatholeEvent};
use serde::Serialize;
use tokio::sync::{Mutex, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use url::{Host, Url};

#[derive(Debug, Clone)]
pub struct RemoteAccessDesiredState {
    pub settings: GatewayRemoteAccessSettings,
    pub key: Option<String>,
}

pub struct RemoteAccessSupervisor {
    runtime_dir: PathBuf,
    config: GatewayRemoteAccessConfig,
    status_tx: watch::Sender<GatewayRemoteAccessStatusSnapshot>,
    state: Mutex<RemoteAccessSupervisorState>,
}

struct RemoteAccessSupervisorState {
    generation: u64,
    shutdown_tx: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
struct RemoteAccessRunConfig {
    generation: u64,
    runtime_dir: PathBuf,
    remote_addr: String,
    local_addr: String,
    service_name: String,
    token: String,
    restart_initial_ms: u64,
    restart_max_ms: u64,
    restart_jitter_percent: u8,
    max_restarts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RatholeRunOutcome {
    Stopped,
    Exited,
}

#[derive(Default)]
struct RatholeEventState {
    connected_once: bool,
    suppress_connecting_status: bool,
}

#[derive(Serialize)]
struct RatholeClientConfig {
    client: RatholeClientSection,
}

#[derive(Serialize)]
struct RatholeClientSection {
    remote_addr: String,
    services: BTreeMap<String, RatholeClientService>,
}

#[derive(Serialize)]
struct RatholeClientService {
    token: String,
    local_addr: String,
}

impl RemoteAccessSupervisor {
    pub fn new(runtime_home: &Path, config: GatewayRemoteAccessConfig) -> Result<Self> {
        let runtime_dir = runtime_home.join(config.runtime_dir.as_str());
        std::fs::create_dir_all(runtime_dir.as_path()).with_context(|| {
            format!(
                "failed to create remote access runtime dir `{}`",
                runtime_dir.display()
            )
        })?;

        let (status_tx, _status_rx) = watch::channel(status_snapshot(
            GatewayRemoteAccessState::Disabled,
            None,
            None,
        ));

        Ok(Self {
            runtime_dir,
            config,
            status_tx,
            state: Mutex::new(RemoteAccessSupervisorState {
                generation: 0,
                shutdown_tx: None,
                task: None,
            }),
        })
    }

    pub fn status_snapshot(&self) -> GatewayRemoteAccessStatusSnapshot {
        self.status_tx.borrow().clone()
    }

    pub fn subscribe_status(&self) -> watch::Receiver<GatewayRemoteAccessStatusSnapshot> {
        self.status_tx.subscribe()
    }

    pub async fn apply(&self, desired: RemoteAccessDesiredState) -> Result<()> {
        let mut state = self.state.lock().await;
        state.generation = state.generation.saturating_add(1);
        stop_supervisor_task(state.shutdown_tx.take(), state.task.take()).await;

        let Some(run_config) = self.validate_run_config(state.generation, desired)? else {
            return Ok(());
        };

        publish_status(
            &self.status_tx,
            GatewayRemoteAccessState::Starting,
            None,
            Some("starting vendored rathole client".to_owned()),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let status_tx = self.status_tx.clone();
        let task = tokio::spawn(async move {
            supervise_rathole(run_config, status_tx, shutdown_rx).await;
        });
        state.shutdown_tx = Some(shutdown_tx);
        state.task = Some(task);
        Ok(())
    }

    pub async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        stop_supervisor_task(state.shutdown_tx.take(), state.task.take()).await;
        publish_status(
            &self.status_tx,
            GatewayRemoteAccessState::Stopped,
            None,
            Some("remote access tunnel stopped".to_owned()),
        );
    }

    fn validate_run_config(
        &self,
        generation: u64,
        desired: RemoteAccessDesiredState,
    ) -> Result<Option<RemoteAccessRunConfig>> {
        if !desired.settings.enabled {
            publish_status(
                &self.status_tx,
                GatewayRemoteAccessState::Disabled,
                None,
                Some("remote access tunnel is disabled".to_owned()),
            );
            return Ok(None);
        }

        if desired.settings.transport != GatewayRemoteAccessTransport::Tcp {
            publish_status(
                &self.status_tx,
                GatewayRemoteAccessState::Failed,
                Some(GatewayRemoteAccessErrorKind::UnsupportedTransport),
                Some("only tcp remote access transport is supported by this runtime".to_owned()),
            );
            return Ok(None);
        }

        let Some(remote_addr) = desired
            .settings
            .server
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
        else {
            publish_status(
                &self.status_tx,
                GatewayRemoteAccessState::Failed,
                Some(GatewayRemoteAccessErrorKind::InvalidSettings),
                Some("remote access server is not configured".to_owned()),
            );
            return Ok(None);
        };

        let remote_addr = match normalize_rathole_remote_addr(remote_addr.as_str()) {
            Ok(remote_addr) => remote_addr,
            Err(error) => {
                publish_status(
                    &self.status_tx,
                    GatewayRemoteAccessState::Failed,
                    Some(GatewayRemoteAccessErrorKind::InvalidSettings),
                    Some(format!("{error:#}")),
                );
                return Ok(None);
            }
        };

        let Some(token) = desired
            .key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
        else {
            publish_status(
                &self.status_tx,
                GatewayRemoteAccessState::Failed,
                Some(GatewayRemoteAccessErrorKind::MissingKey),
                Some("remote access key is not configured".to_owned()),
            );
            return Ok(None);
        };

        let Some(service_name) = desired
            .settings
            .service_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
        else {
            publish_status(
                &self.status_tx,
                GatewayRemoteAccessState::Failed,
                Some(GatewayRemoteAccessErrorKind::InvalidSettings),
                Some("remote access service name is not configured".to_owned()),
            );
            return Ok(None);
        };

        if let Err(error) = validate_rathole_service_name(service_name.as_str()) {
            publish_status(
                &self.status_tx,
                GatewayRemoteAccessState::Failed,
                Some(GatewayRemoteAccessErrorKind::InvalidSettings),
                Some(format!("{error:#}")),
            );
            return Ok(None);
        }

        Ok(Some(RemoteAccessRunConfig {
            generation,
            runtime_dir: self.runtime_dir.clone(),
            remote_addr,
            local_addr: self.config.local_addr.clone(),
            service_name,
            token,
            restart_initial_ms: self.config.restart_initial_ms.max(1),
            restart_max_ms: self
                .config
                .restart_max_ms
                .max(self.config.restart_initial_ms.max(1)),
            restart_jitter_percent: self.config.restart_jitter_percent.min(100),
            max_restarts: self.config.max_restarts,
        }))
    }
}

async fn stop_supervisor_task(
    shutdown_tx: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
) {
    if let Some(shutdown_tx) = shutdown_tx {
        let _ = shutdown_tx.send(true);
    }
    if let Some(mut task) = task {
        if tokio::time::timeout(Duration::from_secs(6), &mut task)
            .await
            .is_err()
        {
            task.abort();
        }
    }
}

async fn supervise_rathole(
    run_config: RemoteAccessRunConfig,
    status_tx: watch::Sender<GatewayRemoteAccessStatusSnapshot>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut restart_count = 0u32;
    let mut next_delay = Duration::from_millis(run_config.restart_initial_ms);

    loop {
        if restart_count > 0 {
            publish_status(
                &status_tx,
                GatewayRemoteAccessState::Reconnecting,
                None,
                Some("reconnecting vendored rathole client".to_owned()),
            );
        }

        match run_rathole_once(&run_config, &status_tx, &mut shutdown_rx).await {
            Ok(RatholeRunOutcome::Stopped) => {
                publish_status(
                    &status_tx,
                    GatewayRemoteAccessState::Stopped,
                    None,
                    Some("remote access tunnel stopped".to_owned()),
                );
                return;
            }
            Ok(RatholeRunOutcome::Exited) => {
                publish_status(
                    &status_tx,
                    GatewayRemoteAccessState::Reconnecting,
                    Some(GatewayRemoteAccessErrorKind::ProcessExited),
                    Some("vendored rathole client exited".to_owned()),
                );
            }
            Err(error) => {
                warn!(
                    generation = run_config.generation,
                    error = %format!("{error:#}"),
                    "vendored rathole client failed"
                );
                publish_status(
                    &status_tx,
                    GatewayRemoteAccessState::Reconnecting,
                    Some(GatewayRemoteAccessErrorKind::ProcessExited),
                    Some(format!("{error:#}")),
                );
            }
        }

        if !should_restart(run_config.max_restarts, restart_count) {
            publish_status(
                &status_tx,
                GatewayRemoteAccessState::Failed,
                Some(GatewayRemoteAccessErrorKind::RestartLimitReached),
                Some("remote access tunnel restart limit reached".to_owned()),
            );
            return;
        }

        restart_count = restart_count.saturating_add(1);
        sleep_or_shutdown(next_delay, &mut shutdown_rx).await;
        next_delay = next_restart_delay(&run_config, next_delay, restart_count);
        if *shutdown_rx.borrow() {
            publish_status(
                &status_tx,
                GatewayRemoteAccessState::Stopped,
                None,
                Some("remote access tunnel stopped".to_owned()),
            );
            return;
        }
    }
}

async fn run_rathole_once(
    run_config: &RemoteAccessRunConfig,
    status_tx: &watch::Sender<GatewayRemoteAccessStatusSnapshot>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<RatholeRunOutcome> {
    let config_path = write_rathole_client_config(run_config)?;
    let args = rathole::Cli {
        config_path: Some(config_path.clone()),
        client: true,
        server: false,
        ..rathole::Cli::default()
    };
    let (rathole_shutdown_tx, rathole_shutdown_rx) = broadcast::channel::<bool>(1);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let mut event_state = RatholeEventState::default();
    let mut task = tokio::spawn(async move {
        rathole::run_with_events(args, rathole_shutdown_rx, Some(event_tx)).await
    });

    loop {
        tokio::select! {
            result = &mut task => {
                remove_runtime_config(config_path.as_path());
                return match result {
                    Ok(Ok(())) => Ok(RatholeRunOutcome::Exited),
                    Ok(Err(error)) => Err(error).context("vendored rathole client returned an error"),
                    Err(error) => Err(error).context("vendored rathole client task failed"),
                };
            }
            Some(event) = event_rx.recv() => {
                publish_rathole_event(status_tx, event, &mut event_state);
            }
            changed = shutdown_rx.changed() => {
                remove_runtime_config(config_path.as_path());
                if changed.is_ok() && *shutdown_rx.borrow() {
                    let _ = rathole_shutdown_tx.send(true);
                    return match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                        Ok(Ok(Ok(()))) => Ok(RatholeRunOutcome::Stopped),
                        Ok(Ok(Err(error))) => Err(error).context("vendored rathole client failed during shutdown"),
                        Ok(Err(error)) => Err(error).context("vendored rathole client task failed during shutdown"),
                        Err(_) => {
                            task.abort();
                            Ok(RatholeRunOutcome::Stopped)
                        }
                    }
                } else {
                    return Ok(RatholeRunOutcome::Exited);
                }
            }
        }
    }
}

fn write_rathole_client_config(run_config: &RemoteAccessRunConfig) -> Result<PathBuf> {
    std::fs::create_dir_all(run_config.runtime_dir.as_path()).with_context(|| {
        format!(
            "failed to create remote access runtime dir `{}`",
            run_config.runtime_dir.display()
        )
    })?;

    let mut services = BTreeMap::new();
    services.insert(
        run_config.service_name.clone(),
        RatholeClientService {
            token: run_config.token.clone(),
            local_addr: run_config.local_addr.clone(),
        },
    );

    let config = RatholeClientConfig {
        client: RatholeClientSection {
            remote_addr: run_config.remote_addr.clone(),
            services,
        },
    };
    let content =
        toml::to_string_pretty(&config).context("failed to serialize rathole client config")?;
    let path = run_config
        .runtime_dir
        .join(format!("rathole-client-{}.toml", run_config.generation));
    write_private_file(path.as_path(), content.as_bytes())?;
    set_private_permissions(path.as_path())?;
    Ok(path)
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }

    let mut file = options.open(path).with_context(|| {
        format!(
            "failed to create transient rathole client config `{}`",
            path.display()
        )
    })?;
    file.write_all(bytes).with_context(|| {
        format!(
            "failed to write transient rathole client config `{}`",
            path.display()
        )
    })
}

fn remove_runtime_config(path: &Path) {
    if let Err(error) = std::fs::remove_file(path) {
        debug!(
            path = %path.display(),
            error = %format!("{error:#}"),
            "failed to remove transient rathole client config"
        );
    }
}

fn validate_rathole_service_name(service_name: &str) -> Result<()> {
    if service_name.is_empty() {
        bail!("remote access service name must not be empty");
    }
    if service_name.chars().count() > 80 {
        bail!("remote access service name must be at most 80 characters");
    }
    if service_name
        .chars()
        .any(|ch| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.'))
    {
        bail!("remote access service name contains unsupported characters");
    }
    Ok(())
}

fn normalize_rathole_remote_addr(remote_addr: &str) -> Result<String> {
    if remote_addr.is_empty() {
        bail!("remote access server must not be empty");
    }
    if remote_addr.trim() != remote_addr || remote_addr.chars().any(char::is_whitespace) {
        bail!("remote access server must not contain whitespace");
    }

    if remote_addr.contains("://") {
        return normalize_rathole_remote_url(remote_addr);
    }

    if let Ok(socket_addr) = remote_addr.parse::<SocketAddr>() {
        if socket_addr.port() == 0 {
            bail!("remote access server port must be between 1 and 65535");
        }
        return Ok(remote_addr.to_owned());
    }

    bail!("remote access server must be an https URL or an IP address with port")
}

fn normalize_rathole_remote_url(remote_addr: &str) -> Result<String> {
    let url = Url::parse(remote_addr).context("failed to parse remote access server URL")?;
    if url.scheme() != "https" {
        bail!("remote access server URL must start with https://");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("remote access server URL must not include credentials");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("remote access server URL must not include query or fragment");
    }
    if !matches!(url.path(), "" | "/") {
        bail!("remote access server URL must not include a path");
    }

    let port = url
        .port_or_known_default()
        .context("remote access server URL must include a port or use https")?;
    if port == 0 {
        bail!("remote access server port must be between 1 and 65535");
    }
    let host = url
        .host()
        .context("remote access server URL must include a host")?;

    Ok(match host {
        Host::Domain(host) => format!("{host}:{port}"),
        Host::Ipv4(host) => format!("{host}:{port}"),
        Host::Ipv6(host) => format!("[{host}]:{port}"),
    })
}

fn should_restart(max_restarts: u32, restart_count: u32) -> bool {
    max_restarts == 0 || restart_count < max_restarts
}

async fn sleep_or_shutdown(delay: Duration, shutdown_rx: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        _ = shutdown_rx.changed() => {}
    }
}

fn next_restart_delay(
    run_config: &RemoteAccessRunConfig,
    current_delay: Duration,
    attempt: u32,
) -> Duration {
    let doubled = current_delay
        .as_millis()
        .saturating_mul(2)
        .min(u128::from(run_config.restart_max_ms));
    let base_ms = u64::try_from(doubled).unwrap_or(run_config.restart_max_ms);
    let jittered = jittered_delay_ms(base_ms, run_config.restart_jitter_percent, attempt);
    Duration::from_millis(jittered.max(1))
}

fn jittered_delay_ms(base_ms: u64, jitter_percent: u8, attempt: u32) -> u64 {
    if jitter_percent == 0 || base_ms == 0 {
        return base_ms;
    }
    let span = base_ms.saturating_mul(u64::from(jitter_percent)) / 100;
    if span == 0 {
        return base_ms;
    }
    let width = span.saturating_mul(2).saturating_add(1);
    let pseudo = u64::from(attempt)
        .wrapping_mul(1_103_515_245)
        .wrapping_add(12_345);
    let offset = i128::from(pseudo % width) - i128::from(span);
    u64::try_from((i128::from(base_ms) + offset).max(1)).unwrap_or(base_ms)
}

fn publish_rathole_event(
    status_tx: &watch::Sender<GatewayRemoteAccessStatusSnapshot>,
    event: RatholeEvent,
    state: &mut RatholeEventState,
) {
    match event {
        RatholeEvent::Client(event) => match event {
            RatholeClientEvent::ControlChannelConnecting { .. } => {
                if state.suppress_connecting_status {
                    return;
                }

                let status = if state.connected_once {
                    GatewayRemoteAccessState::Reconnecting
                } else {
                    GatewayRemoteAccessState::Starting
                };
                publish_status(
                    status_tx,
                    status,
                    None,
                    Some("connecting to remote access relay".to_owned()),
                );
            }
            RatholeClientEvent::ControlChannelConnected { .. } => {
                state.connected_once = true;
                state.suppress_connecting_status = false;
                publish_status(
                    status_tx,
                    GatewayRemoteAccessState::Connected,
                    None,
                    Some("remote access tunnel is connected".to_owned()),
                );
            }
            RatholeClientEvent::ControlChannelReconnecting { error, .. } => {
                let status = if state.connected_once {
                    GatewayRemoteAccessState::Reconnecting
                } else {
                    state.suppress_connecting_status = true;
                    GatewayRemoteAccessState::Failed
                };
                publish_status(
                    status_tx,
                    status,
                    Some(GatewayRemoteAccessErrorKind::RelayConnectFailed),
                    Some(error),
                );
            }
            RatholeClientEvent::ControlChannelAuthFailed { error, .. } => {
                state.connected_once = false;
                state.suppress_connecting_status = true;
                publish_status(
                    status_tx,
                    GatewayRemoteAccessState::Failed,
                    Some(GatewayRemoteAccessErrorKind::TunnelAuthFailed),
                    Some(error),
                );
            }
            RatholeClientEvent::ControlChannelServiceNotExist { error, .. } => {
                state.connected_once = false;
                state.suppress_connecting_status = true;
                publish_status(
                    status_tx,
                    GatewayRemoteAccessState::Failed,
                    Some(GatewayRemoteAccessErrorKind::InvalidSettings),
                    Some(error),
                );
            }
            RatholeClientEvent::ControlChannelStopped { .. } => {
                state.connected_once = false;
                state.suppress_connecting_status = false;
                publish_status(
                    status_tx,
                    GatewayRemoteAccessState::Stopped,
                    None,
                    Some("remote access tunnel stopped".to_owned()),
                );
            }
        },
    }
}

fn publish_status(
    status_tx: &watch::Sender<GatewayRemoteAccessStatusSnapshot>,
    state: GatewayRemoteAccessState,
    error_kind: Option<GatewayRemoteAccessErrorKind>,
    message: Option<String>,
) {
    status_tx.send_replace(status_snapshot(state, error_kind, message));
}

fn status_snapshot(
    state: GatewayRemoteAccessState,
    error_kind: Option<GatewayRemoteAccessErrorKind>,
    message: Option<String>,
) -> GatewayRemoteAccessStatusSnapshot {
    GatewayRemoteAccessStatusSnapshot {
        state,
        error_kind,
        message,
        updated_at_unix: unix_timestamp_secs()
            .ok()
            .and_then(|value| i64::try_from(value).ok()),
    }
}

fn unix_timestamp_secs() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)
        .with_context(|| format!("failed to read metadata for `{}`", path.display()))?
        .permissions();
    permissions.set_mode(0o600);
    std::fs::set_permissions(path, permissions)
        .with_context(|| format!("failed to set permissions for `{}`", path.display()))
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

impl Drop for RemoteAccessSupervisor {
    fn drop(&mut self) {
        info!("remote access supervisor dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rathole_client_config_uses_service_token_and_local_gateway() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let run_config = RemoteAccessRunConfig {
            generation: 7,
            runtime_dir: temp_dir.path().to_path_buf(),
            remote_addr: "relay.example.com:2333".to_owned(),
            local_addr: "127.0.0.1:17878".to_owned(),
            service_name: "pioneer_gateway".to_owned(),
            token: "secret-token".to_owned(),
            restart_initial_ms: 1000,
            restart_max_ms: 30000,
            restart_jitter_percent: 20,
            max_restarts: 0,
        };

        let path = write_rathole_client_config(&run_config).expect("write config");
        let content = std::fs::read_to_string(path).expect("read config");

        assert!(content.contains("[client]"));
        assert!(content.contains("remote_addr = \"relay.example.com:2333\""));
        assert!(content.contains("[client.services.pioneer_gateway]"));
        assert!(content.contains("token = \"secret-token\""));
        assert!(content.contains("local_addr = \"127.0.0.1:17878\""));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = std::fs::metadata(temp_dir.path().join("rathole-client-7.toml"))
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn unsupported_service_name_is_rejected() {
        assert!(validate_rathole_service_name("pioneer_gateway").is_ok());
        assert!(validate_rathole_service_name("bad/service").is_err());
        assert!(validate_rathole_service_name("").is_err());
    }

    #[test]
    fn remote_addr_accepts_https_url_or_ip_with_port() {
        for value in ["https://getpioneer.dev", "127.0.0.1:2333", "[::1]:2333"] {
            assert!(
                normalize_rathole_remote_addr(value).is_ok(),
                "{value} should be valid"
            );
        }

        for value in [
            "getpioneer.dev",
            "relay.example.com:2333",
            "localhost",
            "127.0.0.1",
            "[::1]",
            "relay.example.com:0",
            "relay.example.com:notaport",
            ":2333",
            "relay.example.com:",
            "http://getpioneer.dev",
            "https://getpioneer.dev/path",
            "::1:2333",
        ] {
            assert!(
                normalize_rathole_remote_addr(value).is_err(),
                "{value} should be invalid"
            );
        }
    }

    #[test]
    fn https_remote_addr_uses_https_default_port() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let supervisor =
            RemoteAccessSupervisor::new(temp_dir.path(), GatewayRemoteAccessConfig::default())
                .expect("supervisor");
        let desired = RemoteAccessDesiredState {
            settings: GatewayRemoteAccessSettings {
                enabled: true,
                server: Some("https://getpioneer.dev".to_owned()),
                service_name: Some("pioneer_gateway".to_owned()),
                has_key: true,
                ..GatewayRemoteAccessSettings::default()
            },
            key: Some("secret-token".to_owned()),
        };

        let run_config = supervisor
            .validate_run_config(1, desired)
            .expect("validate run config");

        let run_config = run_config.expect("run config");
        assert_eq!(run_config.remote_addr, "getpioneer.dev:443");
    }

    #[test]
    fn invalid_remote_addr_publishes_failed_status() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let supervisor =
            RemoteAccessSupervisor::new(temp_dir.path(), GatewayRemoteAccessConfig::default())
                .expect("supervisor");
        let desired = RemoteAccessDesiredState {
            settings: GatewayRemoteAccessSettings {
                enabled: true,
                server: Some("getpioneer.dev".to_owned()),
                service_name: Some("pioneer_gateway".to_owned()),
                has_key: true,
                ..GatewayRemoteAccessSettings::default()
            },
            key: Some("secret-token".to_owned()),
        };

        let run_config = supervisor
            .validate_run_config(1, desired)
            .expect("validate run config");

        assert!(run_config.is_none());
        let status = supervisor.status_snapshot();
        assert_eq!(status.state, GatewayRemoteAccessState::Failed);
        assert_eq!(
            status.error_kind,
            Some(GatewayRemoteAccessErrorKind::InvalidSettings)
        );
    }

    #[test]
    fn rathole_events_publish_gateway_status() {
        let (status_tx, _status_rx) = watch::channel(GatewayRemoteAccessStatusSnapshot::default());
        let mut event_state = RatholeEventState::default();

        publish_rathole_event(
            &status_tx,
            RatholeEvent::Client(RatholeClientEvent::ControlChannelConnecting {
                service_name: "pioneer_gateway".to_owned(),
            }),
            &mut event_state,
        );

        let status = status_tx.borrow().clone();
        assert_eq!(status.state, GatewayRemoteAccessState::Starting);
        assert_eq!(status.error_kind, None);

        publish_rathole_event(
            &status_tx,
            RatholeEvent::Client(RatholeClientEvent::ControlChannelReconnecting {
                service_name: "pioneer_gateway".to_owned(),
                error: "connect failed".to_owned(),
                retry_after_millis: 1000,
            }),
            &mut event_state,
        );

        let status = status_tx.borrow().clone();
        assert_eq!(status.state, GatewayRemoteAccessState::Failed);
        assert_eq!(
            status.error_kind,
            Some(GatewayRemoteAccessErrorKind::RelayConnectFailed)
        );

        publish_rathole_event(
            &status_tx,
            RatholeEvent::Client(RatholeClientEvent::ControlChannelConnecting {
                service_name: "pioneer_gateway".to_owned(),
            }),
            &mut event_state,
        );

        let status = status_tx.borrow().clone();
        assert_eq!(status.state, GatewayRemoteAccessState::Failed);
        assert_eq!(
            status.error_kind,
            Some(GatewayRemoteAccessErrorKind::RelayConnectFailed)
        );

        publish_rathole_event(
            &status_tx,
            RatholeEvent::Client(RatholeClientEvent::ControlChannelConnected {
                service_name: "pioneer_gateway".to_owned(),
            }),
            &mut event_state,
        );

        let status = status_tx.borrow().clone();
        assert_eq!(status.state, GatewayRemoteAccessState::Connected);
        assert_eq!(status.error_kind, None);

        publish_rathole_event(
            &status_tx,
            RatholeEvent::Client(RatholeClientEvent::ControlChannelAuthFailed {
                service_name: "pioneer_gateway".to_owned(),
                error: "bad token".to_owned(),
            }),
            &mut event_state,
        );

        let status = status_tx.borrow().clone();
        assert_eq!(status.state, GatewayRemoteAccessState::Failed);
        assert_eq!(
            status.error_kind,
            Some(GatewayRemoteAccessErrorKind::TunnelAuthFailed)
        );

        publish_rathole_event(
            &status_tx,
            RatholeEvent::Client(RatholeClientEvent::ControlChannelReconnecting {
                service_name: "pioneer_gateway".to_owned(),
                error: "connect failed".to_owned(),
                retry_after_millis: 1000,
            }),
            &mut event_state,
        );

        let status = status_tx.borrow().clone();
        assert_eq!(status.state, GatewayRemoteAccessState::Failed);
        assert_eq!(
            status.error_kind,
            Some(GatewayRemoteAccessErrorKind::RelayConnectFailed)
        );

        publish_rathole_event(
            &status_tx,
            RatholeEvent::Client(RatholeClientEvent::ControlChannelConnected {
                service_name: "pioneer_gateway".to_owned(),
            }),
            &mut event_state,
        );

        publish_rathole_event(
            &status_tx,
            RatholeEvent::Client(RatholeClientEvent::ControlChannelReconnecting {
                service_name: "pioneer_gateway".to_owned(),
                error: "connect failed".to_owned(),
                retry_after_millis: 1000,
            }),
            &mut event_state,
        );

        let status = status_tx.borrow().clone();
        assert_eq!(status.state, GatewayRemoteAccessState::Reconnecting);
        assert_eq!(
            status.error_kind,
            Some(GatewayRemoteAccessErrorKind::RelayConnectFailed)
        );
    }

    #[test]
    fn remote_addr_normalization_maps_https_url_to_rathole_endpoint() {
        assert_eq!(
            normalize_rathole_remote_addr("https://getpioneer.dev").expect("normalize"),
            "getpioneer.dev:443"
        );
        assert_eq!(
            normalize_rathole_remote_addr("https://getpioneer.dev:8443").expect("normalize"),
            "getpioneer.dev:8443"
        );
        assert_eq!(
            normalize_rathole_remote_addr("https://[::1]").expect("normalize"),
            "[::1]:443"
        );
        assert_eq!(
            normalize_rathole_remote_addr("127.0.0.1:2333").expect("normalize"),
            "127.0.0.1:2333"
        );
    }
}
