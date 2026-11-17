//! Narrow loopback transport for operations whose authority is possession of
//! the private runtime directory. It is never mounted on the public router or
//! remote-access tunnel. There is no SQL/admin dispatch or automatic replay.
use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    routing::post,
};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub(crate) enum Operation {
    DeviceCreate,
    ArtifactGc {
        workspace_id: String,
        now_unix_ms: i64,
        policy: pioneer_artifacts::ArtifactGcPolicy,
    },
}

#[derive(Serialize, Deserialize)]
struct Endpoint {
    address: SocketAddr,
    token: String,
}

#[derive(Serialize, Deserialize)]
enum Response {
    Success(serde_json::Value),
    Failure(String),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    nonce: String,
    payload: String,
    mac: String,
}

fn authenticate(key: &str, direction: &[u8], nonce: &str, payload: &str) -> Hmac<Sha256> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts this key length");
    mac.update(direction);
    mac.update(nonce.as_bytes());
    mac.update(b"\0");
    mac.update(payload.as_bytes());
    mac
}

impl Envelope {
    fn signed(key: &str, direction: &[u8], nonce: String, payload: String) -> Self {
        let mac = hex::encode(
            authenticate(key, direction, &nonce, &payload)
                .finalize()
                .into_bytes(),
        );
        Self {
            nonce,
            payload,
            mac,
        }
    }
    fn verify(&self, key: &str, direction: &[u8]) -> Result<()> {
        anyhow::ensure!(self.nonce.len() == 64, "invalid local operation nonce");
        let tag = hex::decode(&self.mac).context("invalid local operation authentication")?;
        authenticate(key, direction, &self.nonce, &self.payload)
            .verify_slice(&tag)
            .map_err(|_| anyhow::anyhow!("local operation owner could not be authenticated"))
    }
}

struct Services {
    token: zeroize::Zeroizing<String>,
    auth: Arc<crate::auth::GatewayAuthService>,
    artifacts: Arc<pioneer_artifacts::ArtifactService>,
    admission: Arc<tokio::sync::Semaphore>,
}

fn endpoint_path(runtime_home: &Path, config: &pioneer_config::AppConfig) -> Result<PathBuf> {
    let database = crate::database::gateway_database_path(runtime_home, config)?;
    let mut path = pioneer_sqlite::WriterOwnership::database_identity(&database)?.into_os_string();
    path.push(".operator.json");
    Ok(PathBuf::from(path))
}

pub(crate) struct LocalOperations {
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    endpoint_path: PathBuf,
}

impl LocalOperations {
    pub(crate) async fn start(
        runtime_home: &Path,
        config: &pioneer_config::AppConfig,
        auth: Arc<crate::auth::GatewayAuthService>,
        artifacts: Arc<pioneer_artifacts::ArtifactService>,
    ) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let mut entropy = [0_u8; 32];
        rand::fill(&mut entropy);
        let token = zeroize::Zeroizing::new(crate::helpers::encode_hex(&entropy));
        let state = Arc::new(Services {
            token,
            auth,
            artifacts: Arc::new(artifacts.with_maintenance_access()),
            admission: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        let router = Router::new()
            .route("/operation", post(execute))
            .layer(DefaultBodyLimit::max(8192))
            .with_state(state.clone());
        let path = endpoint_path(runtime_home, config)?;
        // Harden before writing a capability, including when reusing a stale
        // endpoint after a crash. The runtime directory is already private.
        let mut options = std::fs::OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path)?;
        pioneer_keystore::ensure_private_file(&path)?;
        use std::io::Write;
        file.write_all(&serde_json::to_vec(&Endpoint {
            address,
            token: state.token.to_string(),
        })?)?;
        drop(file);
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        Ok(Self {
            cancellation,
            task: Some(task),
            endpoint_path: path,
        })
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.await.context("local operations owner failed")??;
        }
        match std::fs::remove_file(&self.endpoint_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for LocalOperations {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn execute(
    State(state): State<Arc<Services>>,
    Json(envelope): Json<Envelope>,
) -> Result<Json<Envelope>, StatusCode> {
    envelope
        .verify(&state.token, b"pioneer.operator.request\0")
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let operation = serde_json::from_str::<Operation>(&envelope.payload)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let _admission = state
        .admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let result: Result<serde_json::Value> = match operation {
        Operation::DeviceCreate => state
            .auth
            .create_local_device()
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| serde_json::to_value(result).map_err(Into::into)),
        Operation::ArtifactGc {
            workspace_id,
            now_unix_ms,
            policy,
        } => {
            if workspace_id.is_empty() || workspace_id.len() > 1024 {
                return Err(StatusCode::BAD_REQUEST);
            }
            state
                .artifacts
                .with_gc_policy(policy)
                .gc_execute(&workspace_id, now_unix_ms)
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| serde_json::to_value(result).map_err(Into::into))
        }
    };
    let response = match result {
        Ok(value) => Response::Success(value),
        Err(error) => Response::Failure(error.to_string()),
    };
    let payload =
        serde_json::to_string(&response).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(Envelope::signed(
        &state.token,
        b"pioneer.operator.response\0",
        envelope.nonce,
        payload,
    )))
}

pub(crate) async fn request<T: DeserializeOwned>(
    runtime_home: &Path,
    config: &pioneer_config::AppConfig,
    operation: Operation,
) -> Result<T> {
    let endpoint: Endpoint = serde_json::from_slice(&std::fs::read(endpoint_path(runtime_home, config)?)
        .context("database owner is starting, stopping, or is an offline operation; local operation unavailable")?)?;
    if endpoint.address.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST) || endpoint.token.len() != 64 {
        bail!("invalid local database-owner endpoint");
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    // Never retry a sent mutation or silently fall back to direct DB access.
    // A lost response is an unknown outcome and is reported as such.
    // A stale endpoint can outlive its listener and its port can be reused by
    // another local process. Authenticate both directions without disclosing
    // the private capability to that listener; a signed response proves owner
    // identity. The nonce binds this single request, without automatic replay.
    let mut entropy = [0_u8; 32];
    rand::fill(&mut entropy);
    let nonce = hex::encode(entropy);
    let envelope = Envelope::signed(
        &endpoint.token,
        b"pioneer.operator.request\0",
        nonce.clone(),
        serde_json::to_string(&operation)?,
    );
    let response = client
        .post(format!("http://{}/operation", endpoint.address))
        .json(&envelope)
        .send()
        .await
        .context("local operation outcome unconfirmed; request was not replayed")?
        .error_for_status()?
        .json::<Envelope>()
        .await
        .context("local operation response unavailable; outcome unconfirmed")?;
    response
        .verify(&endpoint.token, b"pioneer.operator.response\0")
        .context("local operation response untrusted; outcome unconfirmed")?;
    anyhow::ensure!(
        response.nonce == nonce,
        "local operation response does not match this request; outcome unconfirmed"
    );
    match serde_json::from_str::<Response>(&response.payload)? {
        Response::Success(value) => serde_json::from_value(value).map_err(Into::into),
        Response::Failure(error) => Err(anyhow::anyhow!(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Auth-service fixtures prove capability admission and first-session
    // bootstrap. This boundary protects honest results after a stale
    // endpoint's port is reused by a listener without the private capability.
    #[tokio::test]
    async fn reused_operator_port_cannot_forge_a_successful_result() {
        let home = tempfile::tempdir().unwrap();
        let config = pioneer_config::AppConfig::load().unwrap();
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let key = "c".repeat(64);
        std::fs::write(
            endpoint_path(home.path(), &config).unwrap(),
            serde_json::to_vec(&Endpoint {
                address,
                token: key.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        let (observed, observation) = tokio::sync::oneshot::channel();
        let observed = Arc::new(std::sync::Mutex::new(Some(observed)));
        let app = Router::new().route(
            "/operation",
            post(move |Json(request): Json<Envelope>| {
                let observed = observed.clone();
                async move {
                    if let Some(observed) = observed.lock().unwrap().take() {
                        let _ = observed.send(serde_json::to_string(&request).unwrap());
                    }
                    Json(Envelope {
                        nonce: request.nonce,
                        payload: serde_json::to_string(&Response::Success(serde_json::Value::Null))
                            .unwrap(),
                        // Reflection cannot authenticate the opposite direction,
                        // and the listener never learns the signing capability.
                        mac: request.mac,
                    })
                }
            }),
        );
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .unwrap();
        });
        let result =
            request::<serde_json::Value>(home.path(), &config, Operation::DeviceCreate).await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("response untrusted")
        );
        assert!(!observation.await.unwrap().contains(&key));
        shutdown.cancel();
        server.await.unwrap();
    }
}
