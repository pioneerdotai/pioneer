use anyhow::{Context, Result, anyhow, ensure};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use pioneer_protocol::ThreadCallCredentials;
#[cfg(not(target_os = "macos"))]
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tempfile::NamedTempFile;
use tokio::{process::Command, sync::Mutex};
use url::Url;
use uuid::Uuid;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::dispatch as dispatch_supervisor;

#[cfg(target_os = "macos")]
type OwnedChild = tokio::process::Child;
#[cfg(not(target_os = "macos"))]
type OwnedChild = Box<dyn ChildWrapper>;

const TOKEN_SECONDS: i64 = 60;
pub const MAX_SIGNAL_BYTES: usize = 256 * 1024;

pub struct LiveKit {
    binary: PathBuf,
    config: NamedTempFile,
    #[cfg(target_os = "macos")]
    ownership_lock: PathBuf,
    process: Mutex<Option<OwnedChild>>,
    api: reqwest::Client,
    api_url: Url,
    signal_url: Option<Url>,
    udp_port: u16,
    key_id: String,
    encoding: EncodingKey,
    decoding: DecodingKey,
    relay_keys: Option<(EncodingKey, DecodingKey)>,
}

#[derive(Deserialize, Serialize)]
struct Claims {
    iss: String,
    sub: String,
    exp: i64,
    video: VideoGrants,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default)]
struct VideoGrants {
    room: String,
    room_join: bool,
    room_create: bool,
    room_list: bool,
    room_admin: bool,
    can_publish: Option<bool>,
    can_subscribe: Option<bool>,
    can_publish_data: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    can_publish_sources: Vec<String>,
}

#[derive(Deserialize)]
struct Participants {
    #[serde(default)]
    participants: Vec<Participant>,
}

#[derive(Deserialize)]
struct Participant {
    identity: String,
}

impl LiveKit {
    pub fn bundled_executable(gateway: &Path) -> Result<PathBuf> {
        let parent = gateway
            .parent()
            .context("Gateway executable has no directory")?;
        let runtime = if cfg!(target_os = "macos")
            && gateway.ends_with("Pioneer Gateway.app/Contents/MacOS/pioneer")
        {
            gateway
                .ancestors()
                .nth(4)
                .context("invalid Gateway bundle path")?
                .to_path_buf()
        } else if parent.join("runtime").exists() {
            parent.join("runtime")
        } else {
            parent.join("cua")
        };
        let program = if cfg!(windows) {
            "livekit-server.exe"
        } else {
            "livekit-server"
        };
        Ok(runtime.join("livekit").join(program))
    }

    pub fn new(
        binary: PathBuf,
        runtime_home: &Path,
        http_port: u16,
        udp_port: u16,
        node_ip: Option<IpAddr>,
        signal_url: Url,
    ) -> Result<Self> {
        Self::configure(
            binary,
            runtime_home,
            http_port,
            udp_port,
            node_ip,
            Some(signal_url),
        )
    }

    pub fn new_relay(
        binary: PathBuf,
        runtime_home: &Path,
        http_port: u16,
        udp_port: u16,
    ) -> Result<Self> {
        Self::configure(binary, runtime_home, http_port, udp_port, None, None)
    }

    pub fn relay_udp_addr(&self) -> Option<SocketAddrV4> {
        self.signal_url
            .is_none()
            .then_some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.udp_port))
    }

    fn configure(
        binary: PathBuf,
        runtime_home: &Path,
        http_port: u16,
        udp_port: u16,
        node_ip: Option<IpAddr>,
        signal_url: Option<Url>,
    ) -> Result<Self> {
        ensure!(
            binary.is_file() && binary.is_absolute(),
            "LiveKit executable unavailable"
        );
        ensure!(udp_port != 0, "LiveKit UDP port must be nonzero");
        let http_port = if http_port == 0 {
            std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?
                .local_addr()?
                .port()
        } else {
            http_port
        };
        ensure!(
            signal_url
                .as_ref()
                .is_none_or(|url| matches!(url.scheme(), "ws" | "wss")
                    && url.host().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()),
            "invalid LiveKit signaling URL"
        );
        let api_url = Url::parse(&format!("http://127.0.0.1:{http_port}/"))?;
        let key_id = format!("pioneer-{}", Uuid::new_v4());
        let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let mut rtc = json!({
            "udp_port": udp_port,
            "tcp_port": 0,
            "use_external_ip": false,
        });
        if signal_url.is_none() {
            // The UDP mux and HTTP listener are reachable only on loopback.
            rtc["use_ice_lite"] = json!(true);
            rtc["enable_loopback_candidate"] = json!(true);
            rtc["ips"] = json!({"includes": ["127.0.0.1/32"]});
        }
        if let Some(ip) = node_ip {
            rtc["node_ip"] = json!(ip);
        }
        let relay_keys = signal_url.is_none().then(|| {
            let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
            (
                EncodingKey::from_secret(secret.as_bytes()),
                DecodingKey::from_secret(secret.as_bytes()),
            )
        });
        // JSON is accepted by LiveKit's YAML parser. The temporary file is
        // created privately and owned for the lifetime of this process.
        let config_body = json!({
            "bind_addresses": ["127.0.0.1"],
            "port": http_port,
            "rtc": rtc,
            "keys": { (key_id.clone()): secret },
            // Keep the room through SDK reconnect and the Gateway membership grace.
            "room": { "auto_create": false, "empty_timeout": 150, "departure_timeout": 150 },
            "logging": { "level": "error" },
        });
        let mut config = tempfile::Builder::new()
            .prefix("livekit-")
            .tempfile_in(runtime_home)?;
        serde_json::to_writer(&mut config, &config_body)?;
        config.flush()?;

        Ok(Self {
            binary,
            config,
            #[cfg(target_os = "macos")]
            ownership_lock: runtime_home.join(macos::LOCK_FILE),
            process: Mutex::new(None),
            api: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(4))
                .build()?,
            api_url,
            signal_url,
            udp_port,
            key_id,
            encoding: EncodingKey::from_secret(secret.as_bytes()),
            decoding: DecodingKey::from_secret(secret.as_bytes()),
            relay_keys,
        })
    }

    pub async fn ensure_running(&self) -> Result<()> {
        let mut process = self.process.lock().await;
        if let Some(child) = process.as_mut()
            && child.try_wait()?.is_none()
        {
            return Ok(());
        }
        let mut child = self.spawn_owned()?;
        let ready = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                ensure!(
                    child.try_wait()?.is_none(),
                    "LiveKit exited before readiness"
                );
                if self
                    .room_service::<Value>(
                        "ListRooms",
                        VideoGrants {
                            room_list: true,
                            ..Default::default()
                        },
                        json!({}),
                    )
                    .await
                    .is_ok_and(|rooms| rooms.is_some())
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        match ready {
            Ok(Ok(())) => {
                *process = Some(child);
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(anyhow!("LiveKit did not become ready")),
        }
    }

    #[cfg(not(target_os = "macos"))]
    fn spawn_owned(&self) -> Result<OwnedChild> {
        let mut command = Command::new(&self.binary);
        command
            .arg("--config")
            .arg(self.config.path())
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(target_os = "linux")]
        {
            let parent = std::process::id();
            // The sidecar must release its bound ports if Gateway is killed.
            unsafe {
                command.pre_exec(move || {
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::getppid() as u32 != parent {
                        return Err(std::io::Error::other("Gateway exited before LiveKit"));
                    }
                    Ok(())
                });
            }
        }
        let mut command = CommandWrap::from(command);
        command.wrap(KillOnDrop);
        #[cfg(windows)]
        command.wrap(process_wrap::tokio::JobObject);
        command.spawn().context("cannot launch bundled LiveKit")
    }

    #[cfg(target_os = "macos")]
    fn spawn_owned(&self) -> Result<OwnedChild> {
        let mut command = Command::new(std::env::current_exe()?);
        command
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // Keep the watchdog alive if the Gateway's service group is stopped.
        command.process_group(0);
        #[cfg(not(test))]
        command
            .arg(macos::ARGUMENT)
            .arg(&self.binary)
            .arg(self.config.path())
            .arg(&self.ownership_lock);
        #[cfg(test)]
        command
            .args([
                "--exact",
                "livekit::tests::supervisor_test_entry",
                "--ignored",
            ])
            .env("PIONEER_TEST_SUPERVISOR_BINARY", &self.binary)
            .env("PIONEER_TEST_SUPERVISOR_CONFIG", self.config.path())
            .env("PIONEER_TEST_SUPERVISOR_LOCK", &self.ownership_lock);
        command.spawn().context("launch LiveKit supervisor")
    }

    pub async fn shutdown(&self) {
        if let Some(mut child) = self.process.lock().await.take() {
            #[cfg(target_os = "macos")]
            drop(child.stdin.take());
            #[cfg(not(target_os = "macos"))]
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }

    pub fn credentials(
        &self,
        room: &str,
        identity: &str,
        now: i64,
    ) -> Result<ThreadCallCredentials> {
        self.credentials_with_relay_url(room, identity, now, None)
    }

    pub fn credentials_with_relay_url(
        &self,
        room: &str,
        identity: &str,
        now: i64,
        relay_url: Option<&str>,
    ) -> Result<ThreadCallCredentials> {
        let url = match &self.signal_url {
            Some(url) => url.as_str(),
            None => relay_url.context("authenticated relay signaling is unavailable")?,
        };
        let expires_at_unix = now + TOKEN_SECONDS;
        let claims = Claims {
            iss: self.key_id.clone(),
            sub: identity.to_owned(),
            exp: expires_at_unix,
            video: VideoGrants {
                room: room.to_owned(),
                room_join: true,
                can_publish: Some(true),
                can_subscribe: Some(true),
                can_publish_data: Some(false),
                can_publish_sources: vec!["microphone".into(), "camera".into()],
                ..Default::default()
            },
        };
        let key = self
            .relay_keys
            .as_ref()
            .map_or(&self.encoding, |(key, _)| key);
        let token = encode(&Header::new(Algorithm::HS256), &claims, key)?;
        Ok(ThreadCallCredentials {
            url: url.to_owned(),
            token,
            expires_at_unix,
        })
    }

    pub fn admission(&self, token: &str) -> Result<(String, String)> {
        let key = self
            .relay_keys
            .as_ref()
            .map_or(&self.decoding, |(_, key)| key);
        let claims = self.decode_admission(token, key)?;
        Ok((claims.video.room, claims.sub))
    }

    pub fn upstream_token(&self, token: &str) -> Result<String> {
        let Some((_, key)) = &self.relay_keys else {
            return Ok(token.to_owned());
        };
        let claims = self.decode_admission(token, key)?;
        Ok(encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &self.encoding,
        )?)
    }

    pub(crate) fn relay_refresh_token(
        &self,
        token: &str,
        identity: &str,
        room: &str,
    ) -> Result<String> {
        let (key, _) = self
            .relay_keys
            .as_ref()
            .context("relay credentials unavailable")?;
        let claims = self.decode_admission(token, &self.decoding)?;
        ensure!(
            claims.sub == identity && claims.video.room == room,
            "LiveKit refresh scope changed"
        );
        Ok(encode(&Header::new(Algorithm::HS256), &claims, key)?)
    }

    fn decode_admission(&self, token: &str, key: &DecodingKey) -> Result<Claims> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[&self.key_id]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        let claims = decode::<Claims>(token, key, &validation)?.claims;
        ensure!(
            claims.video.room_join && !claims.video.room.is_empty() && !claims.sub.is_empty(),
            "invalid signaling claims"
        );
        Ok(claims)
    }

    pub fn local_signal_url(&self, path: &str, query: Option<&str>) -> Url {
        let mut url = self.api_url.clone();
        url.set_path(path);
        url.set_query(query);
        url
    }

    pub async fn validate_signal(&self, path: &str, query: &str, token: &str) -> Result<bool> {
        Ok(self
            .api
            .get(self.local_signal_url(path, Some(query)))
            .bearer_auth(token)
            .send()
            .await?
            .status()
            .is_success())
    }

    pub async fn create_room(&self, room: &str) -> Result<()> {
        let room = self
            .room_service::<Value>(
                "CreateRoom",
                VideoGrants {
                    room_create: true,
                    ..Default::default()
                },
                json!({"name": room}),
            )
            .await?;
        ensure!(room.is_some(), "LiveKit did not create the call room");
        Ok(())
    }

    pub async fn delete_room(&self, room: &str) -> Result<()> {
        self.room_service::<Value>(
            "DeleteRoom",
            VideoGrants {
                room_create: true,
                ..Default::default()
            },
            json!({"room": room}),
        )
        .await?;
        Ok(())
    }

    pub async fn participants(&self, room: &str) -> Result<Vec<String>> {
        Ok(self
            .room_service::<Participants>(
                "ListParticipants",
                room_admin(room),
                json!({"room": room}),
            )
            .await?
            .unwrap_or(Participants {
                participants: vec![],
            })
            .participants
            .into_iter()
            .map(|p| p.identity)
            .collect())
    }

    pub async fn eject(&self, room: &str, identity: &str) -> Result<()> {
        self.room_service::<Value>(
            "RemoveParticipant",
            room_admin(room),
            json!({
                "room": room, "identity": identity
            }),
        )
        .await?;
        Ok(())
    }

    fn sign(&self, identity: &str, exp: i64, video: VideoGrants) -> Result<String> {
        Ok(encode(
            &Header::new(Algorithm::HS256),
            &Claims {
                iss: self.key_id.clone(),
                sub: identity.to_owned(),
                exp,
                video,
            },
            &self.encoding,
        )?)
    }

    async fn room_service<T: DeserializeOwned>(
        &self,
        method: &str,
        video: VideoGrants,
        body: Value,
    ) -> Result<Option<T>> {
        let url = self
            .api_url
            .join(&format!("twirp/livekit.RoomService/{method}"))?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let token = self.sign("pioneer-gateway", now + TOKEN_SECONDS, video)?;
        let response = self
            .api
            .post(url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(Some(response.json().await?));
        }
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            let body: Value = response.json().await?;
            if body.get("code").and_then(Value::as_str) == Some("not_found") {
                return Ok(None);
            }
        }
        Err(anyhow!("LiveKit {method} rejected the request ({status})"))
    }
}

fn room_admin(room: &str) -> VideoGrants {
    VideoGrants {
        room: room.to_owned(),
        room_admin: true,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "subprocess entry for the sidecar smoke test"]
    fn supervisor_test_entry() {
        let Some(binary) = std::env::var_os("PIONEER_TEST_SUPERVISOR_BINARY") else {
            return;
        };
        macos::dispatch([
            macos::ARGUMENT.into(),
            binary,
            std::env::var_os("PIONEER_TEST_SUPERVISOR_CONFIG").unwrap(),
            std::env::var_os("PIONEER_TEST_SUPERVISOR_LOCK").unwrap(),
        ])
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires a locally prepared stock LiveKit server"]
    async fn livekit_sidecar_creates_rooms_and_admits_only_its_join_tokens() {
        let root = tempfile::tempdir().unwrap();
        let stock = std::env::var_os("PIONEER_TEST_LIVEKIT").unwrap().into();
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let udp_port = udp.local_addr().unwrap().port();
        let tcp_port = tcp.local_addr().unwrap().port();
        drop((udp, tcp));
        let runtime = LiveKit::new(
            stock,
            root.path(),
            tcp_port,
            udp_port,
            Some("127.0.0.1".parse().unwrap()),
            Url::parse("ws://127.0.0.1:17878").unwrap(),
        )
        .unwrap();
        runtime.ensure_running().await.unwrap();
        runtime.create_room("test-call").await.unwrap();
        assert!(runtime.participants("test-call").await.unwrap().is_empty());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let credentials = runtime
            .credentials("test-call", "participant", now)
            .unwrap();
        assert_eq!(
            runtime.admission(&credentials.token).unwrap(),
            ("test-call".to_owned(), "participant".to_owned())
        );
        assert!(
            runtime
                .validate_signal("/rtc/validate", "", &credentials.token)
                .await
                .unwrap()
        );
        let expired = runtime
            .credentials("test-call", "participant", now - 120)
            .unwrap();
        assert!(runtime.admission(&expired.token).is_err());
        runtime.delete_room("test-call").await.unwrap();
        assert!(
            !runtime
                .validate_signal("/rtc/validate", "", &credentials.token)
                .await
                .unwrap()
        );
        runtime.shutdown().await;
    }
    #[tokio::test]
    #[ignore = "requires a locally prepared stock LiveKit server"]
    async fn relayed_livekit_sidecar_binds_loopback_and_rejects_client_token() -> Result<()> {
        let root = tempfile::tempdir()?;
        let stock = std::env::var_os("PIONEER_TEST_LIVEKIT")
            .context("stock LiveKit test executable path")?
            .into();
        let reserved = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        let udp_port = reserved.local_addr()?.port();
        drop(reserved);
        let runtime = LiveKit::new_relay(stock, root.path(), 0, udp_port)?;
        runtime.ensure_running().await?;
        // Loopback is occupied by the SFU's UDP mux, while a separate physical
        // IPv4 interface remains available on the same port.
        assert!(std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, udp_port)).is_err());
        let probe = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        if probe.connect("192.0.2.1:9").is_ok() {
            let local = probe.local_addr()?;
            if !local.ip().is_loopback() && !local.ip().is_unspecified() {
                let _other = std::net::UdpSocket::bind((local.ip(), udp_port))?;
            }
        }
        runtime.create_room("test-relayed-call").await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let client = runtime.credentials_with_relay_url(
            "test-relayed-call",
            "member",
            now,
            Some("wss://gateway.example.invalid/pioneer/"),
        )?;
        let upstream = runtime.upstream_token(&client.token)?;
        assert!(
            !runtime
                .validate_signal("/rtc/validate", "", &client.token)
                .await?
        );
        assert!(
            runtime
                .validate_signal("/rtc/validate", "", &upstream)
                .await?
        );
        runtime.shutdown().await;
        Ok(())
    }
}
