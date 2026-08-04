//! Desktop-owned device activation and durable session provisioning.

use std::{fmt, time::Duration};

use anyhow::{Context, Result, bail};
use pioneer_client::{
    gateway::{endpoint::GatewayBaseUrl, registry::commit_registry_v3_binding},
    transport::ws::auth_exchange::AuthExchangeClient,
};
use pioneer_protocol::{
    AuthDeviceActivateParams, AuthSecretString, AuthSessionGrant, ClientInstallationDescriptor,
    CredentialStorageOrder, GatewayId, normalize_device_activation_code,
};
use zeroize::Zeroizing;

use super::secrets::{
    DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION, DesktopGatewaySessionSecret, DesktopSecrets,
};
use pioneer_client::gateway::types::GatewayRegistry;

/// Safe, typed boundary for Desktop secure-storage failures.
///
/// Device activation credentials are one-shot. Callers must be able to distinguish a
/// keystore failure after successful activation from an ordinary transient
/// transport error and enter the explicit recovery flow. The fixed display
/// text also prevents a future keystore backend from leaking credential
/// material through an application error.
pub(crate) struct DesktopSessionSecureStorageError {
    _source: anyhow::Error,
}

impl DesktopSessionSecureStorageError {
    pub(crate) fn new(source: anyhow::Error) -> Self {
        Self { _source: source }
    }
}

impl fmt::Debug for DesktopSessionSecureStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DesktopSessionSecureStorageError")
            .finish_non_exhaustive()
    }
}

impl fmt::Display for DesktopSessionSecureStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("desktop Gateway secure storage operation failed")
    }
}

impl std::error::Error for DesktopSessionSecureStorageError {}

#[derive(Debug)]
pub(crate) struct DesktopSessionAccessGrant {
    pub access_token: AuthSecretString,
    pub access_expires_at_unix: u64,
}

pub(crate) fn provision_endpoint_session<F, C, S>(
    registry: &mut GatewayRegistry,
    endpoint_id: &str,
    activation_code: &str,
    secrets: &DesktopSecrets,
    activate: F,
    mut cleanup_session: C,
    mut save: S,
) -> Result<()>
where
    F: FnOnce(&GatewayBaseUrl, &str, AuthDeviceActivateParams) -> Result<AuthSessionGrant>,
    C: FnMut(&GatewayBaseUrl, &str, &pioneer_protocol::AuthSessionId) -> Result<()>,
    S: FnMut(&GatewayRegistry) -> Result<()>,
{
    let endpoint = registry_endpoint(registry, endpoint_id)?.clone();
    let installation_id = registry
        .installation_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("desktop Gateway registry has no installation id")?
        .to_owned();
    let session_ref = endpoint
        .session_ref
        .clone()
        .unwrap_or_else(|| endpoint.id.clone());

    if endpoint.session_ref.is_some() || endpoint.server_gateway_id.is_some() {
        bail!("desktop Gateway endpoint already has a device session");
    }

    let activation = Zeroizing::new(
        normalize_device_activation_code(activation_code.trim())
            .map_err(anyhow::Error::msg)
            .context("invalid desktop Gateway activation credential")?,
    );
    let expected_gateway_id = endpoint.server_gateway_id.clone();

    if let Some(session) = secrets
        .get_gateway_session(session_ref.as_str())
        .map_err(DesktopSessionSecureStorageError::new)?
    {
        if session.installation_id != installation_id {
            bail!("durable desktop Gateway session belongs to a different installation");
        }
        if expected_gateway_id
            .as_ref()
            .is_some_and(|expected| &session.gateway_id != expected)
        {
            bail!("durable desktop Gateway session belongs to a different Gateway");
        }
        let mut next = registry.clone();
        commit_registry_v3_binding(
            &mut next,
            endpoint_id,
            session_ref.as_str(),
            &session.gateway_id,
        )
        .map_err(anyhow::Error::new)?;
        save(&next)?;
        *registry = next;
        return Ok(());
    }

    let params = AuthDeviceActivateParams {
        installation: ClientInstallationDescriptor {
            installation_id: installation_id.clone(),
            display_name: "Pioneer Desktop".to_owned(),
            client_kind: pioneer_protocol::ClientKind::Desktop,
            platform: Some(std::env::consts::OS.to_owned()),
            client_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
        },
    };
    let grant = activate(&endpoint.gateway_base_url, activation.as_str(), params)
        .context("desktop Gateway device activation failed")?;
    let cleanup_access = grant.access_token.clone();
    let cleanup_session_id = grant.session.id.clone();
    let (session, access) = match session_from_grant(
        grant,
        installation_id.as_str(),
        expected_gateway_id.as_ref(),
    ) {
        Ok(result) => result,
        Err(error) => {
            let _ = cleanup_session(
                &endpoint.gateway_base_url,
                cleanup_access.expose_secret(),
                &cleanup_session_id,
            );
            return Err(error);
        }
    };
    if let Err(error) = secrets.put_gateway_session(
        session_ref.as_str(),
        &session,
        Some(format!("{} session", endpoint.name)),
    ) {
        let _ = cleanup_session(
            &endpoint.gateway_base_url,
            access.access_token.expose_secret(),
            &session.session_id,
        );
        return Err(DesktopSessionSecureStorageError::new(error).into());
    }
    let mut next = registry.clone();
    if let Err(error) = commit_registry_v3_binding(
        &mut next,
        endpoint_id,
        session_ref.as_str(),
        &session.gateway_id,
    )
    .map_err(anyhow::Error::new)
    .and_then(|()| save(&next))
    {
        // Keep the durable envelope under the deterministic session_ref. The
        // activation is one-shot; the next explicit provisioning attempt can
        // adopt this envelope without exchanging another credential.
        return Err(error);
    }
    *registry = next;
    Ok(())
}

pub(crate) fn activate_device_session(
    gateway_base_url: &GatewayBaseUrl,
    credential: &str,
    params: AuthDeviceActivateParams,
    timeout: Duration,
) -> Result<AuthSessionGrant> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialize desktop auth exchange runtime")?;
    runtime
        .block_on(AuthExchangeClient::new(timeout).activate_device(
            gateway_base_url,
            credential,
            params,
        ))
        .map_err(anyhow::Error::new)
}

pub(crate) fn revoke_session_best_effort(
    gateway_base_url: &GatewayBaseUrl,
    access_token: &str,
    session_id: &pioneer_protocol::AuthSessionId,
    timeout: Duration,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialize desktop session cleanup runtime")?;
    runtime
        .block_on(AuthExchangeClient::new(timeout).cleanup_session_once(
            gateway_base_url,
            access_token,
            session_id.clone(),
        ))
        .map(|_| ())
        .map_err(anyhow::Error::new)
}

fn session_from_grant(
    grant: AuthSessionGrant,
    expected_installation_id: &str,
    expected_gateway_id: Option<&GatewayId>,
) -> Result<(DesktopGatewaySessionSecret, DesktopSessionAccessGrant)> {
    if grant.auth_protocol_version != pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION
        || expected_gateway_id.is_some_and(|expected| &grant.gateway.id != expected)
        || grant.principal.kind != pioneer_protocol::PrincipalKind::Superuser
        || grant.device.installation_id != expected_installation_id
        || grant.device.client_kind != pioneer_protocol::ClientKind::Desktop
        || grant.device.status != pioneer_protocol::DeviceStatus::Active
        || grant.session.status != pioneer_protocol::AuthSessionStatus::Active
        || grant.session.device_id != grant.device.id
        || grant.refresh_generation != 0
        || grant.session.refresh_generation != grant.refresh_generation
        || grant.session.refresh_expires_at_unix != grant.refresh_expires_at_unix
        || grant.access_token.expose_secret().is_empty()
        || grant.access_expires_at_unix == 0
        || grant.credential_storage_order
            != CredentialStorageOrder::PersistRefreshBeforeActivatingAccess
    {
        bail!("inconsistent Gateway session grant");
    }
    let session = DesktopGatewaySessionSecret {
        schema_version: DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION,
        gateway_id: grant.gateway.id,
        principal_id: grant.principal.id,
        device_id: grant.device.id,
        session_id: grant.session.id,
        token_family_id: grant.session.token_family_id,
        installation_id: grant.device.installation_id,
        refresh_generation: grant.refresh_generation,
        refresh_expires_at_unix: grant.refresh_expires_at_unix,
        refresh_token: grant.refresh_token,
    };
    session.validate()?;
    Ok((
        session,
        DesktopSessionAccessGrant {
            access_token: grant.access_token,
            access_expires_at_unix: grant.access_expires_at_unix,
        },
    ))
}

fn registry_endpoint<'a>(
    registry: &'a GatewayRegistry,
    endpoint_id: &str,
) -> Result<&'a pioneer_client::gateway::types::GatewayEndpoint> {
    registry
        .local
        .as_ref()
        .filter(|endpoint| endpoint.id == endpoint_id)
        .or_else(|| {
            registry
                .remotes
                .iter()
                .find(|endpoint| endpoint.id == endpoint_id)
        })
        .with_context(|| format!("unknown desktop Gateway endpoint `{endpoint_id}`"))
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, sync::Arc};

    use anyhow::anyhow;
    use pioneer_client::gateway::types::{GatewayEndpoint, GatewayEndpointKind};
    use pioneer_keystore::MemorySecretStore;
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthGatewaySnapshot, AuthPrincipalSnapshot, AuthSecretString,
        AuthSessionSnapshot, AuthSessionStatus, ClientKind, CredentialStorageOrder, DeviceId,
        DeviceStatus, GatewayId, PrincipalId, PrincipalKind, TokenFamilyId,
    };

    use super::*;

    const ENDPOINT_ID: &str = "local";
    const INSTALLATION_ID: &str = "desktop-installation";
    const ACCESS_SECRET: &str = "access-secret";

    fn registry() -> GatewayRegistry {
        GatewayRegistry {
            version: pioneer_client::gateway::registry::CURRENT_GATEWAY_REGISTRY_VERSION,
            installation_id: Some(INSTALLATION_ID.to_owned()),
            active_gateway_id: None,
            local: Some(GatewayEndpoint {
                id: ENDPOINT_ID.to_owned(),
                name: "Local Gateway".to_owned(),
                gateway_base_url:
                    pioneer_client::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                        "127.0.0.1:17878",
                    )
                    .unwrap(),
                kind: GatewayEndpointKind::Local,
                session_ref: None,
                server_gateway_id: None,
                workspace_id: None,
                service_name: Some("com.pioneer.gateway".to_owned()),
            }),
            remotes: Vec::new(),
        }
    }

    fn grant() -> AuthSessionGrant {
        let device_id = DeviceId::new("D00000000000000000001").expect("device id");
        AuthSessionGrant {
            gateway: AuthGatewaySnapshot {
                id: GatewayId::new("G00000000000000000001").expect("Gateway id"),
            },
            principal: AuthPrincipalSnapshot {
                id: PrincipalId::new("P00000000000000000001").expect("principal id"),
                kind: PrincipalKind::Superuser,
                display_name: "Owner".to_owned(),
                nickname: "owner".to_owned(),
            },
            device: AuthDeviceSnapshot {
                id: device_id.clone(),
                installation_id: INSTALLATION_ID.to_owned(),
                display_name: "Pioneer Desktop".to_owned(),
                client_kind: ClientKind::Desktop,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: pioneer_protocol::AuthSessionId::new("S00000000000000000001")
                    .expect("session id"),
                device_id,
                token_family_id: TokenFamilyId::new("F00000000000000000001")
                    .expect("token family id"),
                status: AuthSessionStatus::Active,
                refresh_generation: 0,
                refresh_expires_at_unix: 2_000,
            },
            access_token: AuthSecretString::new(ACCESS_SECRET),
            access_expires_at_unix: 1_000,
            refresh_token: AuthSecretString::new(format!(
                "{}{}",
                pioneer_protocol::REFRESH_CREDENTIAL_PREFIX,
                "r".repeat(pioneer_protocol::REFRESH_CREDENTIAL_BODY_LEN)
            )),
            refresh_expires_at_unix: 2_000,
            refresh_generation: 0,
            auth_protocol_version: pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
            credential_storage_order: CredentialStorageOrder::PersistRefreshBeforeActivatingAccess,
        }
    }

    fn secrets() -> DesktopSecrets {
        DesktopSecrets::new(Arc::new(MemorySecretStore::new()))
    }

    fn activation_code() -> String {
        "K7M4-P9Q2".to_owned()
    }

    #[test]
    fn activation_persists_refresh_before_registry_binding_without_access_secret() {
        let mut registry = registry();
        let secrets = secrets();
        let exchange_called = Cell::new(false);
        let save_called = Cell::new(false);
        let activation_code = activation_code();

        provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            activation_code.as_str(),
            &secrets,
            |gateway_base_url, credential, params| {
                exchange_called.set(true);
                assert_eq!(gateway_base_url.as_str(), "http://127.0.0.1:17878/");
                assert_eq!(credential, "K7M4P9Q2");
                assert_eq!(params.installation.installation_id, INSTALLATION_ID);
                assert_eq!(params.installation.client_kind, ClientKind::Desktop);
                Ok(grant())
            },
            |_, _, _| panic!("valid grant must not be cleaned up"),
            |next| {
                let durable = secrets
                    .get_gateway_session(ENDPOINT_ID)?
                    .expect("refresh envelope must exist before registry save");
                assert_eq!(durable.installation_id, INSTALLATION_ID);
                assert_eq!(durable.gateway_id.as_str(), "G00000000000000000001");
                assert_eq!(durable.refresh_generation, 0);
                assert_eq!(
                    next.local
                        .as_ref()
                        .and_then(|endpoint| endpoint.session_ref.as_deref()),
                    Some(ENDPOINT_ID)
                );
                let serialized = serde_json::to_string(&durable)?;
                assert!(!serialized.contains(ACCESS_SECRET));
                save_called.set(true);
                Ok(())
            },
        )
        .expect("provision desktop session");

        assert!(exchange_called.get());
        assert!(save_called.get());
        let endpoint = registry.local.as_ref().expect("local endpoint");
        assert_eq!(endpoint.session_ref.as_deref(), Some(ENDPOINT_ID));
        assert_eq!(
            endpoint.server_gateway_id.as_ref().map(GatewayId::as_str),
            Some("G00000000000000000001")
        );
    }

    #[test]
    fn registry_save_retry_adopts_durable_envelope_without_redeeming_again() {
        let mut registry = registry();
        let secrets = secrets();
        let exchange_count = Cell::new(0_u32);
        let activation_code = activation_code();

        let error = provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            activation_code.as_str(),
            &secrets,
            |_, _, _| {
                exchange_count.set(exchange_count.get() + 1);
                Ok(grant())
            },
            |_, _, _| panic!("successful exchange must not be cleaned up"),
            |_| Err(anyhow!("injected registry save failure")),
        )
        .expect_err("registry save must fail");
        assert!(error.to_string().contains("injected registry save failure"));
        assert_eq!(exchange_count.get(), 1);
        assert!(
            secrets
                .has_gateway_session(ENDPOINT_ID)
                .expect("inspect durable envelope")
        );
        assert!(registry.local.as_ref().unwrap().session_ref.is_none());

        provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            activation_code.as_str(),
            &secrets,
            |_, _, _| -> Result<AuthSessionGrant> {
                panic!("retry must adopt the durable envelope without another exchange")
            },
            |_, _, _| panic!("adoption must not clean up the active session"),
            |_| Ok(()),
        )
        .expect("adopt durable envelope");

        assert_eq!(exchange_count.get(), 1);
        assert_eq!(
            registry.local.as_ref().unwrap().session_ref.as_deref(),
            Some(ENDPOINT_ID)
        );
    }

    #[test]
    fn first_activation_establishes_the_gateway_pin_from_the_grant() {
        let mut registry = registry();
        let secrets = secrets();
        let mut observed_grant = grant();
        observed_grant.gateway.id =
            GatewayId::new("G00000000000000000002").expect("other Gateway id");
        let activation_code = activation_code();

        provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            activation_code.as_str(),
            &secrets,
            |_, _, _| Ok(observed_grant),
            |_, _, _| panic!("a consistent initial grant must not be cleaned up"),
            |_| Ok(()),
        )
        .expect("initial activation should trust and persist the observed Gateway identity");

        assert!(
            secrets
                .has_gateway_session(ENDPOINT_ID)
                .expect("inspect durable envelope")
        );
        assert_eq!(
            registry
                .local
                .as_ref()
                .and_then(|endpoint| endpoint.server_gateway_id.as_ref())
                .map(GatewayId::as_str),
            Some("G00000000000000000002")
        );
    }

    #[test]
    fn provisioning_refuses_to_replace_an_already_bound_device_session() {
        let mut registry = registry();
        let secrets = secrets();
        let local = registry.local.as_mut().expect("local endpoint");
        local.session_ref = Some(ENDPOINT_ID.to_owned());
        local.server_gateway_id =
            Some(GatewayId::new("G00000000000000000001").expect("Gateway id"));

        let error = provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            activation_code().as_str(),
            &secrets,
            |_, _, _| -> Result<AuthSessionGrant> {
                panic!("an already bound endpoint must not perform an exchange")
            },
            |_, _, _| panic!("an already bound endpoint must not clean up another session"),
            |_| panic!("an already bound endpoint must not save another registry binding"),
        )
        .expect_err("an existing device session must be replaced through the recovery flow");

        assert!(
            error
                .to_string()
                .contains("desktop Gateway endpoint already has a device session")
        );
    }

    #[test]
    fn inconsistent_initial_grant_is_revoked_and_never_persisted() {
        let mut registry = registry();
        let secrets = secrets();
        let cleanup_called = Cell::new(false);
        let mut invalid_grant = grant();
        invalid_grant.refresh_generation = 1;
        invalid_grant.session.refresh_generation = 1;
        let activation_code = activation_code();

        let error = provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            activation_code.as_str(),
            &secrets,
            |_, _, _| Ok(invalid_grant),
            |gateway_base_url, access_token, session_id| {
                assert_eq!(gateway_base_url.as_str(), "http://127.0.0.1:17878/");
                assert_eq!(access_token, ACCESS_SECRET);
                assert_eq!(session_id.as_str(), "S00000000000000000001");
                cleanup_called.set(true);
                Ok(())
            },
            |_| panic!("invalid grant must not reach registry save"),
        )
        .expect_err("non-zero initial refresh generation must be rejected");

        assert!(
            error
                .to_string()
                .contains("inconsistent Gateway session grant")
        );
        assert!(cleanup_called.get());
        assert!(
            !secrets
                .has_gateway_session(ENDPOINT_ID)
                .expect("inspect missing envelope")
        );
        assert!(registry.local.as_ref().unwrap().session_ref.is_none());
    }
}
