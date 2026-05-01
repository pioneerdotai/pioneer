use crate::domain::McpServerInstallation;
use sha2::{Digest, Sha256};

pub fn fingerprint_installation(installation: &McpServerInstallation) -> String {
    let value = serde_json::json!({
        "scope_kind": installation.scope_kind,
        "scope_key": installation.scope_key,
        "name": installation.name,
        "source_kind": installation.source_kind,
        "transport": installation.transport,
        "auth": installation.auth,
        "required": installation.required,
    });
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}
