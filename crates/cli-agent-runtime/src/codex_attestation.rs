use crate::codex::{
    CodexJsonlRpcClient, CodexJsonlRpcNotificationEvent, CodexJsonlRpcServerRequest,
};
use crate::driver::JsonlRpcId;
use crate::event::{
    RuntimeEvent, RuntimeEventMappingOptions, map_codex_notification_event,
    map_codex_server_request_event,
};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};
use tokio::time::{Duration, timeout};

pub const CODEX_MCP_LOCAL_ATTESTATION_CONTRACT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexExecutableCacheIdentity {
    pub realpath: PathBuf,
    pub size: u64,
    pub modified_unix_nanos: u64,
    pub provider_version: String,
    pub probe_contract_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexExecutableAttestation {
    pub cache_identity: CodexExecutableCacheIdentity,
    pub binary_sha256: String,
    pub platform: String,
    pub architecture: String,
    pub local_executable_fingerprint: String,
}

pub fn attest_codex_executable(
    configured_executable: &str,
    provider_version: &str,
    probe_contract_hash: &str,
) -> Result<CodexExecutableAttestation> {
    let cache_identity = codex_executable_cache_identity(
        configured_executable,
        provider_version,
        probe_contract_hash,
    )?;
    attest_codex_executable_identity(cache_identity)
}

/// Resolve the cheap cache key before hashing the binary. The key is never a
/// security identity: a cache miss is followed by a full content hash.
pub fn codex_executable_cache_identity(
    configured_executable: &str,
    provider_version: &str,
    probe_contract_hash: &str,
) -> Result<CodexExecutableCacheIdentity> {
    validate_fingerprint("probe contract hash", probe_contract_hash)?;
    let provider_version = provider_version.trim();
    if provider_version.is_empty()
        || provider_version.len() > 256
        || provider_version.contains('\0')
    {
        bail!("Codex provider version is unavailable or invalid");
    }
    let realpath = resolve_executable(configured_executable)?;
    let metadata = fs::metadata(realpath.as_path()).with_context(|| {
        format!(
            "failed to inspect Codex executable `{}`",
            realpath.display()
        )
    })?;
    if !metadata.is_file() {
        bail!("configured Codex executable is not a regular file");
    }
    let modified = metadata
        .modified()
        .context("failed to read Codex executable modification time")?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow!("Codex executable modification time predates Unix epoch"))?;
    let modified_unix_nanos = modified.as_nanos().min(u64::MAX as u128) as u64;
    Ok(CodexExecutableCacheIdentity {
        realpath,
        size: metadata.len(),
        modified_unix_nanos,
        provider_version: provider_version.to_owned(),
        probe_contract_hash: probe_contract_hash.to_owned(),
    })
}

pub fn attest_codex_executable_identity(
    cache_identity: CodexExecutableCacheIdentity,
) -> Result<CodexExecutableAttestation> {
    let metadata = fs::metadata(cache_identity.realpath.as_path()).with_context(|| {
        format!(
            "failed to re-inspect Codex executable `{}`",
            cache_identity.realpath.display()
        )
    })?;
    let modified = metadata
        .modified()
        .context("failed to re-read Codex executable modification time")?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow!("Codex executable modification time predates Unix epoch"))?;
    let modified_unix_nanos = modified.as_nanos().min(u64::MAX as u128) as u64;
    if !metadata.is_file()
        || metadata.len() != cache_identity.size
        || modified_unix_nanos != cache_identity.modified_unix_nanos
    {
        bail!("Codex executable changed while it was being attested");
    }
    let binary_sha256 = sha256_file_contents(cache_identity.realpath.as_path())?;
    let platform = env::consts::OS.to_owned();
    let architecture = env::consts::ARCH.to_owned();
    let fingerprint_payload = serde_json::json!({
        "contractVersion": CODEX_MCP_LOCAL_ATTESTATION_CONTRACT_VERSION,
        "binarySha256": binary_sha256,
        "binarySize": cache_identity.size,
        "providerVersion": cache_identity.provider_version,
        "probeContractHash": cache_identity.probe_contract_hash,
        "platform": platform,
        "architecture": architecture,
    });
    let local_executable_fingerprint = sha256_json(&fingerprint_payload)?;
    Ok(CodexExecutableAttestation {
        cache_identity,
        binary_sha256,
        platform,
        architecture,
        local_executable_fingerprint,
    })
}

/// Exercise the real JSONL-RPC request path with an unknown provider request
/// and prove that it receives one bounded MethodNotFound response.
pub async fn validate_unknown_codex_request_response() -> Result<()> {
    let (client_stream, server_stream) = duplex(8 * 1024);
    let (client_read, client_write) = split(client_stream);
    let (server_read, mut server_write) = split(server_stream);
    let client = CodexJsonlRpcClient::new(BufReader::new(client_read), client_write);
    let mut requests = client
        .take_server_request_receiver()
        .ok_or_else(|| anyhow!("Codex server-request receiver is unavailable"))?;
    server_write
        .write_all(
            b"{\"method\":\"pioneer/readiness/unknown\",\"id\":\"readiness-1\",\"params\":{}}\n",
        )
        .await
        .context("failed to inject unknown Codex request")?;
    let request = timeout(Duration::from_secs(1), requests.recv())
        .await
        .context("unknown Codex request was not surfaced")?
        .ok_or_else(|| anyhow!("Codex server-request channel closed"))?;
    if request.method != "pioneer/readiness/unknown" {
        bail!("unknown Codex request method changed during dispatch");
    }
    client
        .fail_server_request(request.id, -32601, "unknown server request", None)
        .await
        .context("failed to answer unknown Codex request")?;
    let mut response_line = String::new();
    timeout(
        Duration::from_secs(1),
        BufReader::new(server_read).read_line(&mut response_line),
    )
    .await
    .context("unknown Codex request response timed out")??;
    if response_line.len() > 1024 {
        bail!("unknown Codex request response exceeded its bound");
    }
    let response: JsonValue =
        serde_json::from_str(response_line.as_str()).context("invalid unknown-request response")?;
    if response["id"] != serde_json::json!("readiness-1")
        || response["error"]["code"] != serde_json::json!(-32601)
    {
        bail!("unknown Codex request did not receive MethodNotFound");
    }
    let _ = client.shutdown().await;
    Ok(())
}

pub fn validate_recorded_codex_mcp_decoder_fixtures() -> Result<()> {
    let fixture: DecoderFixture = serde_json::from_str(include_str!(
        "../tests/fixtures/codex_mcp_lifecycle_0_144_1.json"
    ))
    .context("failed to decode recorded Codex MCP lifecycle fixture")?;
    if fixture.contract_version != 1 || fixture.codex_version.trim().is_empty() {
        bail!("recorded Codex MCP lifecycle fixture has unsupported identity");
    }
    for notification in fixture.notifications {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: notification.method,
                params: Some(notification.params),
                raw: JsonValue::Null,
            },
            RuntimeEventMappingOptions::default(),
        );
        let actual = runtime_event_fixture_kind(&event);
        if actual != notification.expected_event {
            bail!(
                "recorded Codex MCP notification decoded as `{actual}` instead of `{}`",
                notification.expected_event
            );
        }
    }
    for request in fixture.requests {
        let event = map_codex_server_request_event(
            &CodexJsonlRpcServerRequest {
                id: JsonlRpcId::Number(request.id),
                method: request.method,
                params: Some(request.params),
                raw: JsonValue::Null,
            },
            RuntimeEventMappingOptions::default(),
        );
        let RuntimeEvent::RequestOpened(opened) = event else {
            bail!("recorded Codex MCP server request did not decode as request_opened");
        };
        if opened.request_kind != request.expected_request_kind {
            bail!(
                "recorded Codex MCP request decoded as `{}` instead of `{}`",
                opened.request_kind,
                request.expected_request_kind
            );
        }
    }
    Ok(())
}

pub fn sha256_json(value: &JsonValue) -> Result<String> {
    let bytes = serde_json::to_vec(value).context("failed to serialize fingerprint payload")?;
    Ok(hex_digest(&Sha256::digest(bytes)))
}

fn resolve_executable(configured_executable: &str) -> Result<PathBuf> {
    let configured_executable = configured_executable.trim();
    if configured_executable.is_empty() || configured_executable.contains('\0') {
        bail!("configured Codex executable is empty or invalid");
    }
    let configured = Path::new(configured_executable);
    if configured.is_absolute() || configured.components().count() > 1 {
        return fs::canonicalize(configured).with_context(|| {
            format!("failed to resolve configured Codex executable `{configured_executable}`")
        });
    }
    let path = env::var_os("PATH").ok_or_else(|| anyhow!("PATH is unavailable"))?;
    for directory in env::split_paths(&path) {
        let candidate = directory.join(configured);
        if candidate.is_file() {
            return fs::canonicalize(candidate)
                .context("failed to canonicalize Codex executable from PATH");
        }
        #[cfg(windows)]
        for extension in ["exe", "cmd", "bat"] {
            let candidate = directory.join(format!("{configured_executable}.{extension}"));
            if candidate.is_file() {
                return fs::canonicalize(candidate)
                    .context("failed to canonicalize Codex executable from PATH");
            }
        }
    }
    bail!("configured Codex executable was not found")
}

pub fn sha256_file_contents(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("failed to open Codex executable `{}`", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .context("failed to hash Codex executable")?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex_digest(&digest.finalize()))
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn validate_fingerprint(label: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{label} must be a SHA-256 fingerprint");
    }
    Ok(())
}

fn runtime_event_fixture_kind(event: &RuntimeEvent) -> &'static str {
    match event {
        RuntimeEvent::ItemStarted(_) => "item_started",
        RuntimeEvent::ItemDelta(_) => "item_delta",
        RuntimeEvent::ItemCompleted(_) => "item_completed",
        RuntimeEvent::RequestOpened(_) => "request_opened",
        RuntimeEvent::Raw(_) => "raw",
        _ => "other",
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecoderFixture {
    contract_version: u32,
    codex_version: String,
    notifications: Vec<DecoderNotificationFixture>,
    requests: Vec<DecoderRequestFixture>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecoderNotificationFixture {
    method: String,
    params: JsonValue,
    expected_event: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecoderRequestFixture {
    id: i64,
    method: String,
    params: JsonValue,
    expected_request_kind: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_attestation_recorded_mcp_decoder_fixtures_match_current_adapter() {
        validate_recorded_codex_mcp_decoder_fixtures().expect("recorded decoder fixtures");
    }

    #[tokio::test]
    async fn codex_attestation_unknown_request_receives_bounded_terminal_error() {
        validate_unknown_codex_request_response()
            .await
            .expect("unknown request response contract");
    }

    #[cfg(unix)]
    #[test]
    fn codex_attestation_executable_fingerprint_changes_with_binary_or_contract() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let root =
            std::env::temp_dir().join(format!("pioneer-codex-attestation-{}", std::process::id()));
        let _ = fs::remove_dir_all(root.as_path());
        fs::create_dir_all(root.as_path()).expect("create fixture root");
        let executable = root.join("codex-fixture");
        let mut file = File::create(executable.as_path()).expect("create fixture executable");
        file.write_all(b"fixture-a")
            .expect("write fixture executable");
        fs::set_permissions(executable.as_path(), fs::Permissions::from_mode(0o700))
            .expect("fixture permissions");
        let first = attest_codex_executable(
            executable.to_str().expect("UTF-8 fixture path"),
            "0.144.1",
            &"a".repeat(64),
        )
        .expect("first attestation");
        let changed_contract = attest_codex_executable(
            executable.to_str().expect("UTF-8 fixture path"),
            "0.144.1",
            &"b".repeat(64),
        )
        .expect("changed-contract attestation");
        assert_ne!(
            first.local_executable_fingerprint,
            changed_contract.local_executable_fingerprint
        );
        fs::write(executable.as_path(), b"fixture-b").expect("replace fixture executable");
        let changed_binary = attest_codex_executable(
            executable.to_str().expect("UTF-8 fixture path"),
            "0.144.1",
            &"a".repeat(64),
        )
        .expect("changed-binary attestation");
        assert_ne!(first.binary_sha256, changed_binary.binary_sha256);
        assert_ne!(
            first.local_executable_fingerprint,
            changed_binary.local_executable_fingerprint
        );
        let _ = fs::remove_dir_all(root);
    }
}
