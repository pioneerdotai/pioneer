//! Narrow local-owner ingress. Possession of the existing private auth key is
//! the same OS authority as offline LocalCli. Loopback alone never grants it.
//! No SQL or arbitrary command surface is exposed here.
use crate::secrets::GatewaySecrets;
use anyhow::{Context, Result, ensure};
use hmac::{Hmac, KeyInit, Mac};
use pioneer_config::AppConfig;
use pioneer_protocol::{REQUEST_ID_LEN, RequestId, generate_id};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

type HmacSha256 = Hmac<Sha256>;
const ROUTE: &str = "/local/owner-operation";
const REQUEST_DOMAIN: &[u8] = b"pioneer.local-owner.request.v1\0";
const RESPONSE_DOMAIN: &[u8] = b"pioneer.local-owner.response.v1\0";
const ENDPOINT_DOMAIN: &[u8] = b"pioneer.local-owner.endpoint.v1\0";

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub(crate) enum Action {
    CreateDevice,
    SecretsGc,
    ArtifactGc {
        workspace_id: String,
        now_unix_ms: i64,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub database: String,
    pub id: RequestId,
    pub issued_at_unix: u64,
    pub action: Action,
}
#[derive(Serialize, Deserialize)]
struct Endpoint {
    database: String,
    address: SocketAddr,
    mac: String,
}

fn database_identity(config: &AppConfig, home: &Path) -> Result<String> {
    let path = std::fs::canonicalize(crate::database::gateway_database_path(home, config)?)?;
    // Never transmit the local path. Canonical path is the same file identity
    // used by cooperative ownership; unsupported hardlinks are refused there.
    Ok(hex::encode(Sha256::digest(
        path.to_string_lossy().as_bytes(),
    )))
}
fn key(secrets: &GatewaySecrets) -> Result<crate::secrets::AuthKeyMaterial> {
    secrets.load_or_create_auth_credential_hmac_key(64)
}
fn mac(key: &[u8], domain: &[u8], payload: &[u8]) -> HmacSha256 {
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(key).expect("HMAC accepts all key sizes");
    mac.update(domain);
    mac.update(payload);
    mac
}
fn verify(key: &[u8], domain: &[u8], payload: &[u8], signature: &str) -> Result<()> {
    let signature = hex::decode(signature)?;
    mac(key, domain, payload)
        .verify_slice(&signature)
        .context("invalid local capability")
}
fn endpoint_file(home: &Path) -> PathBuf {
    home.join("local-owner-endpoint.json")
}

pub(crate) fn publish(
    config: &AppConfig,
    home: &Path,
    secrets: &GatewaySecrets,
    mut address: SocketAddr,
) -> Result<()> {
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() {
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        } else {
            IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        });
    }
    // Local administration is deliberately unavailable over LAN or proxy peers.
    ensure!(
        address.ip().is_loopback(),
        "local owner ingress requires a loopback listener or wildcard bind"
    );
    let database = database_identity(config, home)?;
    let payload = serde_json::to_vec(&(database.as_str(), address))?;
    let endpoint = Endpoint {
        database,
        address,
        mac: hex::encode(
            mac(key(secrets)?.as_bytes(), ENDPOINT_DOMAIN, &payload)
                .finalize()
                .into_bytes(),
        ),
    };
    let temporary = home.join(format!(".owner-endpoint-{}", generate_id(REQUEST_ID_LEN)));
    write_private(&temporary, &serde_json::to_vec(&endpoint)?)?;
    publish_file(&temporary, &endpoint_file(home))?;
    Ok(())
}
fn publish_file(temporary: &Path, target: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn MoveFileExW(from: *const u16, to: *const u16, flags: u32) -> i32;
        }
        let from: Vec<u16> = temporary.as_os_str().encode_wide().chain(Some(0)).collect();
        let to: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
        // Same-volume atomic replacement, WRITE_THROUGH before dispatching a
        // durable business operation. Both paths are owned NUL-terminated UTF16.
        if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), 0x1 | 0x8) } == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(temporary, target)?;
    }
    sync_parent(target)?;
    Ok(())
}
fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(
            path.parent()
                .context("local operation path has no parent")?,
        )?
        .sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    pioneer_keystore::ensure_private_file(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Retain an uncertain operation until the caller confirms delivery. A confirmed
/// result permits the next ordinary creation. No plaintext result is stored;
/// the operation lockfile is never replaced/deleted.
#[derive(Serialize, Deserialize)]
struct PendingDeviceOperation {
    database: String,
    id: RequestId,
    #[serde(default)]
    confirmed: bool,
}

pub(crate) struct DeviceOperation {
    pub id: RequestId,
    home: PathBuf,
    database: String,
    _lock: std::fs::File,
}
impl DeviceOperation {
    pub fn open(config: &AppConfig, home: &Path) -> Result<Self> {
        Self::open_for_database(home, &database_identity(config, home)?, false)
    }
    pub fn start_new(config: &AppConfig, home: &Path) -> Result<Self> {
        Self::open_for_database(home, &database_identity(config, home)?, true)
    }
    fn open_for_database(home: &Path, database: &str, start_new: bool) -> Result<Self> {
        use fs4::FileExt;
        pioneer_keystore::ensure_private_runtime_dir(home)?;
        let lock_path = home.join("local-device-operation.lock");
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        pioneer_keystore::ensure_private_file(&lock_path)?;
        FileExt::try_lock(&lock).context("another local device operation is in progress")?;
        let pending = home.join("local-device-operation.json");
        let id = match std::fs::read(&pending) {
            Ok(bytes) => {
                let previous: PendingDeviceOperation = serde_json::from_slice(&bytes)?;
                ensure!(
                    previous.confirmed || previous.database == database,
                    "pending local operation belongs to another database; outcome must be recovered there"
                );
                if start_new || previous.confirmed {
                    Self::publish_identity(home, &pending, database)?
                } else {
                    previous.id
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Self::publish_identity(home, &pending, database)?
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            id,
            home: home.to_owned(),
            database: database.to_owned(),
            _lock: lock,
        })
    }
    pub fn confirm(&self) -> Result<()> {
        // Hold the same operation lock through durable acknowledgement. Failed
        // output/cancellation simply drops this guard and leaves the id pending.
        let receipt = PendingDeviceOperation {
            database: self.database.clone(),
            id: self.id.clone(),
            confirmed: true,
        };
        let temporary = self
            .home
            .join(format!(".local-device-{}", generate_id(REQUEST_ID_LEN)));
        write_private(&temporary, &serde_json::to_vec(&receipt)?)?;
        publish_file(&temporary, &self.home.join("local-device-operation.json"))
    }
    fn publish_identity(home: &Path, pending: &Path, database: &str) -> Result<RequestId> {
        let id = RequestId::new(generate_id(REQUEST_ID_LEN))?;
        let receipt = PendingDeviceOperation {
            database: database.to_owned(),
            id: id.clone(),
            confirmed: false,
        };
        let temporary = home.join(format!(".local-device-{}", generate_id(REQUEST_ID_LEN)));
        write_private(&temporary, &serde_json::to_vec(&receipt)?)?;
        publish_file(&temporary, pending)?;
        Ok(id)
    }
}

#[derive(Debug)]
pub(crate) struct DeviceOperationClosed;
impl std::fmt::Display for DeviceOperationClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "previous local device operation is consumed or expired; use `pioneer device create --new-operation` to create another",
        )
    }
}
impl std::error::Error for DeviceOperationClosed {}

/// A definite HTTP response is authoritative. Only connection failure permits
/// trying offline ownership; that lock must still be acquired normally. A lost
/// response is reported as unknown, never silently re-executed with a new id.
pub(crate) async fn try_online<T: serde::de::DeserializeOwned>(
    config: &AppConfig,
    home: &Path,
    id: RequestId,
    action: Action,
) -> Result<Option<T>> {
    let bytes = match std::fs::read(endpoint_file(home)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    ensure!(bytes.len() <= 4096, "invalid local endpoint");
    let endpoint: Endpoint = serde_json::from_slice(&bytes)?;
    ensure!(
        endpoint.address.ip().is_loopback(),
        "local endpoint must be loopback"
    );
    let secrets = GatewaySecrets::open(home)?;
    let key = key(&secrets)?;
    verify(
        key.as_bytes(),
        ENDPOINT_DOMAIN,
        &serde_json::to_vec(&(endpoint.database.as_str(), endpoint.address))?,
        &endpoint.mac,
    )?;
    let database = database_identity(config, home)?;
    if database != endpoint.database {
        // This authenticated rendezvous belongs to another file/runtime. Do
        // not dispatch there. The configured DB still requires its own normal
        // OS ownership before any offline mutation; a busy lock never falls back.
        return Ok(None);
    }
    let body = serde_json::to_vec(&Request {
        database,
        id,
        issued_at_unix: crate::helpers::unix_timestamp_secs()?,
        action,
    })?;
    let signature = hex::encode(
        mac(key.as_bytes(), REQUEST_DOMAIN, &body)
            .finalize()
            .into_bytes(),
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::time::Duration::from_secs(2))
        .build()?;
    let response = match client
        .post(format!("http://{}{ROUTE}", endpoint.address))
        .header("pioneer-local-mac", signature)
        .header("content-type", "application/json")
        .body(body.clone())
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) if error.is_connect() => return Ok(None),
        Err(_) => anyhow::bail!("local operation outcome unknown; retry the same operation"),
    };
    let status = response.status();
    let signature = response
        .headers()
        .get("pioneer-local-result-mac")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    ensure!(
        response
            .content_length()
            .is_none_or(|size| size <= 16 * 1024 * 1024),
        "oversized local result"
    );
    let mut response = response;
    let mut result = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("local result unknown; retry the same operation")?
    {
        ensure!(
            result.len().saturating_add(chunk.len()) <= 16 * 1024 * 1024,
            "oversized local result"
        );
        result.extend_from_slice(&chunk);
    }
    ensure!(result.len() <= 16 * 1024 * 1024, "oversized local result");
    let mut payload = body;
    payload.extend_from_slice(&status.as_u16().to_be_bytes());
    payload.extend_from_slice(&result);
    verify(key.as_bytes(), RESPONSE_DOMAIN, &payload, &signature)
        .context("unverified local result; outcome unknown")?;
    if status == reqwest::StatusCode::CONFLICT {
        return Err(DeviceOperationClosed.into());
    }
    ensure!(
        status.is_success(),
        "local owner rejected operation ({status}); no offline fallback"
    );
    Ok(Some(serde_json::from_slice(&result)?))
}

pub(crate) fn authenticate(
    config: &AppConfig,
    home: &Path,
    secrets: &GatewaySecrets,
    bytes: &[u8],
    signature: &str,
) -> Result<Request> {
    verify(key(secrets)?.as_bytes(), REQUEST_DOMAIN, bytes, signature)?;
    let request: Request = serde_json::from_slice(bytes)?;
    validate_request_time(
        request.issued_at_unix,
        crate::helpers::unix_timestamp_secs()?,
    )?;
    ensure!(
        request.database == database_identity(config, home)?,
        "wrong runtime"
    );
    ensure!(
        request
            .id
            .as_str()
            .bytes()
            .all(|b| b.is_ascii_alphanumeric()),
        "invalid operation identity"
    );
    Ok(request)
}

fn validate_request_time(issued: u64, now: u64) -> Result<()> {
    // Same-machine clock: a short transmission/admission window, not a domain
    // job deadline. Retrying a durable operation signs a fresh envelope with
    // the SAME operation id; a captured MAC is not permanent local authority.
    ensure!(
        issued <= now.saturating_add(5) && now <= issued.saturating_add(120),
        "expired local admission"
    );
    Ok(())
}

/// Bind the reply to the exact authenticated request and status, so a stale
/// rendezvous address occupied by another process cannot forge an outcome.
pub(crate) fn result_signature(
    secrets: &GatewaySecrets,
    request: &[u8],
    status: u16,
    result: &[u8],
) -> Result<String> {
    let mut payload = request.to_vec();
    payload.extend_from_slice(&status.to_be_bytes());
    payload.extend_from_slice(result);
    Ok(hex::encode(
        mac(key(secrets)?.as_bytes(), RESPONSE_DOMAIN, &payload)
            .finalize()
            .into_bytes(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_request_capability_rejects_changed_scope_operation_payload_and_other_purposes() {
        let key = [7u8; 64];
        let request = br#"{"database":"one","id":"L00000000000000000001","action":{"operation":"CreateDevice"}}"#;
        let signature = hex::encode(mac(&key, REQUEST_DOMAIN, request).finalize().into_bytes());
        verify(&key, REQUEST_DOMAIN, request, &signature).unwrap();
        for changed in [
            br#"{"database":"two","id":"L00000000000000000001","action":{"operation":"CreateDevice"}}"#.as_slice(),
            br#"{"database":"one","id":"L00000000000000000002","action":{"operation":"CreateDevice"}}"#.as_slice(),
            br#"{"database":"one","id":"L00000000000000000001","action":{"operation":"SecretsGc"}}"#.as_slice(),
        ] { assert!(verify(&key, REQUEST_DOMAIN, changed, &signature).is_err()); }
        assert!(verify(&[8; 64], REQUEST_DOMAIN, request, &signature).is_err());
        assert!(verify(&key, ENDPOINT_DOMAIN, request, &signature).is_err());
        assert!(verify(&key, RESPONSE_DOMAIN, request, &signature).is_err());
        for (issued, now, accepted) in [
            (100, 100, true),
            (100, 220, true),
            (100, 221, false),
            (105, 100, true),
            (106, 100, false),
        ] {
            assert_eq!(validate_request_time(issued, now).is_ok(), accepted);
        }
    }

    #[test]
    fn local_device_pending_identity_survives_caller_drop_and_is_exclusive() {
        let directory = tempfile::tempdir().unwrap();
        let first =
            DeviceOperation::open_for_database(directory.path(), "database-one", false).unwrap();
        let id = first.id.clone();
        assert!(
            DeviceOperation::open_for_database(directory.path(), "database-one", false).is_err()
        );
        drop(first); // lost response/caller cancellation: preserve pending id
        assert!(
            DeviceOperation::open_for_database(directory.path(), "database-two", false).is_err()
        );
        let retry =
            DeviceOperation::open_for_database(directory.path(), "database-one", false).unwrap();
        assert_eq!(retry.id, id);
        drop(retry);
        let replay =
            DeviceOperation::open_for_database(directory.path(), "database-one", false).unwrap();
        assert_eq!(replay.id, id);
        replay.confirm().unwrap();
        drop(replay);
        let confirmed_next =
            DeviceOperation::open_for_database(directory.path(), "database-one", false).unwrap();
        assert_ne!(
            confirmed_next.id, id,
            "ordinary confirmed call is new intent"
        );
        confirmed_next.confirm().unwrap();
        drop(confirmed_next);
        let other =
            DeviceOperation::open_for_database(directory.path(), "database-two", false).unwrap();
        assert_ne!(other.id, id);
        assert!(
            DeviceOperation::open_for_database(directory.path(), "database-one", true).is_err()
        );
        other.confirm().unwrap();
        drop(other);
        let next =
            DeviceOperation::open_for_database(directory.path(), "database-one", true).unwrap();
        assert_ne!(next.id, id);
        drop(next);
    }
}
