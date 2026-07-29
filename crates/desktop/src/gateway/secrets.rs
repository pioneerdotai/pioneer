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
    AuthSecretString, AuthSessionId, DeviceId, GatewayId, MAX_OPAQUE_CREDENTIAL_BODY_LEN,
    MIN_OPAQUE_CREDENTIAL_BODY_LEN, PrincipalId, TokenFamilyId,
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

pub(crate) const DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION: u32 = 1;
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
        Ok(())
    }
}

pub(crate) fn is_valid_refresh_credential(value: &str) -> bool {
    let Some(entropy) = value.strip_prefix("prf_") else {
        return false;
    };
    (MIN_OPAQUE_CREDENTIAL_BODY_LEN..=MAX_OPAQUE_CREDENTIAL_BODY_LEN).contains(&entropy.len())
        && entropy
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
