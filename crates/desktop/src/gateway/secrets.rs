use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use pioneer_keystore::{
    DbKeyStore, DbKeyStoreConfig, SecretEntryMeta, SecretFilter, SecretId, SecretKind, SecretMeta,
    SecretStore,
};
use pioneer_protocol::{
    AuthSecretString, AuthSessionId, DeviceId, GatewayId, PrincipalId, REFRESH_CREDENTIAL_BODY_LEN,
    REFRESH_CREDENTIAL_PREFIX, TokenFamilyId,
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

pub(crate) const DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION: u32 = 2;
const RETIRED_DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION: u32 = 1;
const RETIRED_DESKTOP_GATEWAY_AUTH_TOKEN_SERVICE: &str = "pioneer.desktop.gateway_auth_token";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DesktopGatewaySessionSecret {
    pub schema_version: u32,
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub token_family_id: TokenFamilyId,
    pub installation_id: String,
    pub refresh_generation: u64,
    pub refresh_expires_at_unix: u64,
    pub refresh_token: AuthSecretString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_refresh_request_id: Option<String>,
}

#[derive(Deserialize)]
struct DesktopGatewaySessionEnvelopeVersion {
    schema_version: u32,
}

impl DesktopGatewaySessionSecret {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.schema_version != DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION {
            bail!("unsupported desktop Gateway session secret version");
        }
        if !is_valid_refresh_credential(self.refresh_token.expose_secret()) {
            bail!("invalid desktop Gateway refresh credential");
        }
        let installation_id = self.installation_id.trim();
        if installation_id != self.installation_id
            || installation_id.is_empty()
            || installation_id.chars().count() > 255
            || installation_id.chars().any(char::is_control)
        {
            bail!("invalid desktop Gateway installation id");
        }
        if self.refresh_expires_at_unix == 0 {
            bail!("invalid desktop Gateway refresh expiry");
        }
        if self
            .pending_refresh_request_id
            .as_ref()
            .is_some_and(|request_id| {
                pioneer_protocol::RequestId::new(request_id.clone()).is_err()
                    || !request_id.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
        {
            bail!("invalid desktop Gateway pending refresh request id");
        }
        Ok(())
    }
}

pub(crate) fn is_valid_refresh_credential(value: &str) -> bool {
    let Some(body) = value.strip_prefix(REFRESH_CREDENTIAL_PREFIX) else {
        return false;
    };
    body.len() == REFRESH_CREDENTIAL_BODY_LEN
        && body
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[derive(Clone)]
pub(crate) struct DesktopSecrets {
    store: Arc<dyn SecretStore>,
}

impl DesktopSecrets {
    pub(crate) fn open(runtime_home: &Path) -> Result<Self> {
        let store = DbKeyStore::open(DbKeyStoreConfig::for_runtime_home(runtime_home))
            .context("failed to open desktop keystore")?;
        Ok(Self::new(Arc::new(store)))
    }

    pub(crate) fn new(store: Arc<dyn SecretStore>) -> Self {
        Self { store }
    }

    pub(crate) fn get_gateway_session(
        &self,
        session_ref: &str,
    ) -> Result<Option<DesktopGatewaySessionSecret>> {
        let id = desktop_gateway_session_secret_id(session_ref)?;
        let Some(raw) = self
            .store
            .get_string(&id)
            .context("failed to read desktop Gateway session from keystore")?
        else {
            return Ok(None);
        };
        let raw = Zeroizing::new(raw);
        let version = serde_json::from_str::<DesktopGatewaySessionEnvelopeVersion>(raw.as_str())
            .context("failed to decode desktop Gateway session envelope version")?;
        if version.schema_version == RETIRED_DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION {
            self.store
                .delete(&id)
                .context("failed to delete retired desktop Gateway session")?;
            return Ok(None);
        }
        let session = serde_json::from_str::<DesktopGatewaySessionSecret>(raw.as_str())
            .context("failed to decode desktop Gateway session envelope")?;
        session.validate()?;
        Ok(Some(session))
    }

    pub(crate) fn put_gateway_session(
        &self,
        session_ref: &str,
        session: &DesktopGatewaySessionSecret,
        label: Option<String>,
    ) -> Result<()> {
        session.validate()?;
        let id = desktop_gateway_session_secret_id(session_ref)?;
        let serialized = Zeroizing::new(
            serde_json::to_string(session)
                .context("failed to encode desktop Gateway session envelope")?,
        );
        let now = current_unix_i64()?;
        let created_at = self
            .existing_secret_meta(&id, SecretKind::DesktopGatewaySession)?
            .and_then(|entry| entry.created_at_unix)
            .unwrap_or(now);
        let label = label
            .and_then(|label| {
                let trimmed = label.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_owned())
            })
            .unwrap_or_else(|| id.user().to_owned());
        self.store
            .put_string(
                &id,
                serialized.as_str(),
                SecretMeta {
                    kind: SecretKind::DesktopGatewaySession,
                    label: Some(label),
                    created_at_unix: created_at,
                    updated_at_unix: now.max(created_at),
                },
            )
            .context("failed to write desktop Gateway session to keystore")
    }

    pub(crate) fn delete_gateway_session(&self, session_ref: &str) -> Result<bool> {
        let id = desktop_gateway_session_secret_id(session_ref)?;
        self.store
            .delete(&id)
            .context("failed to delete desktop Gateway session from keystore")
    }

    pub(crate) fn has_gateway_session(&self, session_ref: &str) -> Result<bool> {
        let id = desktop_gateway_session_secret_id(session_ref)?;
        self.store
            .exists(&id)
            .context("failed to inspect desktop Gateway session keystore entry")
    }

    pub(crate) fn purge_retired_gateway_auth_tokens(&self) -> Result<usize> {
        let entries = self
            .store
            .list(SecretFilter::Service(
                RETIRED_DESKTOP_GATEWAY_AUTH_TOKEN_SERVICE.to_owned(),
            ))
            .context("failed to list retired desktop Gateway auth tokens")?;
        let mut deleted = 0usize;
        for entry in entries {
            if self
                .store
                .delete(&entry.id)
                .context("failed to delete retired desktop Gateway auth token")?
            {
                deleted = deleted.saturating_add(1);
            }
        }
        Ok(deleted)
    }

    fn existing_secret_meta(
        &self,
        id: &SecretId,
        kind: SecretKind,
    ) -> Result<Option<SecretEntryMeta>> {
        let entries = self
            .store
            .list(SecretFilter::Kind(kind))
            .context("failed to list desktop Gateway secrets from keystore")?;
        Ok(entries.into_iter().find(|entry| entry.id == *id))
    }
}

fn desktop_gateway_session_secret_id(session_ref: &str) -> Result<SecretId> {
    SecretId::desktop_gateway_session(session_ref).context("invalid desktop Gateway session ref")
}

fn current_unix_i64() -> Result<i64> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs();
    i64::try_from(timestamp).context("current unix timestamp does not fit in i64")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_keystore::MemorySecretStore;

    fn session_json(schema_version: u32) -> serde_json::Value {
        serde_json::json!({
            "schema_version": schema_version,
            "gateway_id": "G00000000000000000001",
            "principal_id": "P00000000000000000001",
            "device_id": "D00000000000000000001",
            "session_id": "S00000000000000000001",
            "token_family_id": "F00000000000000000001",
            "installation_id": "installation-desktop-1",
            "refresh_generation": 3,
            "refresh_expires_at_unix": 1_900_000_000_u64,
            "refresh_token": format!(
                "{}{}",
                REFRESH_CREDENTIAL_PREFIX,
                "r".repeat(REFRESH_CREDENTIAL_BODY_LEN)
            ),
        })
    }

    fn put_raw_session(store: &MemorySecretStore, value: serde_json::Value) {
        let id = SecretId::desktop_gateway_session("remote-1").unwrap();
        store
            .put_string(
                &id,
                &value.to_string(),
                SecretMeta {
                    kind: SecretKind::DesktopGatewaySession,
                    label: Some("Remote".to_owned()),
                    created_at_unix: 1,
                    updated_at_unix: 1,
                },
            )
            .unwrap();
    }

    #[test]
    fn retired_session_envelope_is_deleted_and_treated_as_missing() {
        let store = Arc::new(MemorySecretStore::new());
        let id = SecretId::desktop_gateway_session("remote-1").unwrap();
        put_raw_session(store.as_ref(), session_json(1));
        let secrets = DesktopSecrets::new(store.clone());

        assert!(secrets.get_gateway_session("remote-1").unwrap().is_none());
        assert!(!store.exists(&id).unwrap());
    }

    #[test]
    fn existing_v2_session_without_pending_refresh_intent_remains_readable() {
        let store = Arc::new(MemorySecretStore::new());
        put_raw_session(store.as_ref(), session_json(2));
        let secrets = DesktopSecrets::new(store);

        let session = secrets
            .get_gateway_session("remote-1")
            .unwrap()
            .expect("existing session");

        assert_eq!(session.refresh_generation, 3);
        assert!(session.pending_refresh_request_id.is_none());
    }

    #[test]
    fn malformed_pending_refresh_request_id_is_rejected() {
        let store = Arc::new(MemorySecretStore::new());
        let mut raw = session_json(2);
        raw["pending_refresh_request_id"] = serde_json::json!("!!!!!!!!!!!!!!!!!!!!!");
        put_raw_session(store.as_ref(), raw);
        let secrets = DesktopSecrets::new(store);

        let error = secrets
            .get_gateway_session("remote-1")
            .expect_err("malformed pending request id must fail closed");

        assert!(
            format!("{error:#}").contains("invalid desktop Gateway pending refresh request id")
        );
    }
}
