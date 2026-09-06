use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use pioneer_keystore::{
    DbKeyStore, DbKeyStoreConfig, SecretEntryMeta, SecretFilter, SecretId, SecretKind, SecretMeta,
    SecretStore,
};
#[cfg(test)]
use pioneer_protocol::{REFRESH_CREDENTIAL_BODY_LEN, REFRESH_CREDENTIAL_PREFIX};
use serde::Deserialize;
use zeroize::Zeroizing;

pub(crate) const DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION: u32 = 2;
const RETIRED_DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION: u32 = 1;
const RETIRED_DESKTOP_GATEWAY_AUTH_TOKEN_SERVICE: &str = "pioneer.desktop.gateway_auth_token";

pub(crate) use pioneer_client::gateway::session_envelope::GatewaySessionEnvelope as DesktopGatewaySessionSecret;

#[derive(Deserialize)]
struct DesktopGatewaySessionEnvelopeVersion {
    schema_version: u32,
}

pub(super) fn desktop_session_validation_error(
    error: pioneer_client::gateway::session_envelope::GatewaySessionEnvelopeError,
) -> anyhow::Error {
    use pioneer_client::gateway::session_envelope::GatewaySessionEnvelopeError::*;
    anyhow::anyhow!(match error {
        Version => "unsupported desktop Gateway session secret version",
        Credential => "invalid desktop Gateway refresh credential",
        Installation => "invalid desktop Gateway installation id",
        Expiry => "invalid desktop Gateway refresh expiry",
        RequestId => "invalid desktop Gateway pending refresh request id",
    })
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
        session
            .validate()
            .map_err(desktop_session_validation_error)?;
        Ok(Some(session))
    }

    pub(crate) fn put_gateway_session(
        &self,
        session_ref: &str,
        session: &DesktopGatewaySessionSecret,
        label: Option<String>,
    ) -> Result<()> {
        session
            .validate()
            .map_err(desktop_session_validation_error)?;
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

impl pioneer_client::gateway::session_refresh::GatewaySessionStorage for DesktopSecrets {
    fn load(
        &self,
        endpoint: &pioneer_client::gateway::types::GatewayEndpoint,
    ) -> Result<Option<DesktopGatewaySessionSecret>> {
        match endpoint.session_ref.as_deref() {
            Some(reference) => self.get_gateway_session(reference),
            None => Ok(None),
        }
    }

    fn persist(
        &self,
        endpoint: &pioneer_client::gateway::types::GatewayEndpoint,
        envelope: &DesktopGatewaySessionSecret,
    ) -> Result<()> {
        self.put_gateway_session(
            endpoint
                .session_ref
                .as_deref()
                .context("Gateway endpoint has no session reference")?,
            envelope,
            Some(format!("{} session", endpoint.name)),
        )
    }
}
