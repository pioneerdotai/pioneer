//! Device-session provisioning with refresh-before-registry persistence and explicit recovery.
use super::{
    endpoint::GatewayBaseUrl,
    registry::commit_registry_v3_binding,
    session_envelope::{GATEWAY_SESSION_SCHEMA_VERSION, GatewaySessionEnvelope},
    session_refresh::{GatewaySessionAccessGrant, GatewaySessionStorage},
    types::GatewayRegistry,
};
use crate::transport::ws::auth_exchange::AuthExchangeClient;
use anyhow::{Context, Result, bail};
use pioneer_protocol::{
    AuthDeviceActivateParams, AuthSessionGrant, ClientInstallationDescriptor,
    CredentialStorageOrder, GatewayId, normalize_device_activation_code,
};
use std::{fmt, time::Duration};
use zeroize::Zeroizing;
pub struct GatewayProvisioningStorageError {
    _source: anyhow::Error,
}

impl GatewayProvisioningStorageError {
    pub fn new(source: anyhow::Error) -> Self {
        Self { _source: source }
    }
}

impl fmt::Debug for GatewayProvisioningStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayProvisioningStorageError")
            .finish_non_exhaustive()
    }
}

impl fmt::Display for GatewayProvisioningStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Gateway secure storage operation failed")
    }
}

impl std::error::Error for GatewayProvisioningStorageError {}

pub fn provision_endpoint_session<F, C, S>(
    registry: &mut GatewayRegistry,
    installation: &ClientInstallationDescriptor,
    endpoint_id: &str,
    activation_code: &str,
    secrets: &dyn GatewaySessionStorage,
    activate: F,
    cleanup_session: C,
    save: S,
) -> Result<()>
where
    F: FnOnce(&GatewayBaseUrl, &str, AuthDeviceActivateParams) -> Result<AuthSessionGrant>,
    C: FnMut(&GatewayBaseUrl, &str, &pioneer_protocol::AuthSessionId) -> Result<()>,
    S: FnMut(&GatewayRegistry) -> Result<()>,
{
    provision_endpoint_session_pinned(
        registry,
        installation,
        endpoint_id,
        activation_code,
        None,
        secrets,
        activate,
        cleanup_session,
        save,
    )
}

pub fn provision_endpoint_session_pinned<F, C, S>(
    registry: &mut GatewayRegistry,
    installation: &ClientInstallationDescriptor,
    endpoint_id: &str,
    activation_code: &str,
    expected_pin: Option<&GatewayId>,
    secrets: &dyn GatewaySessionStorage,
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
        .context("Gateway registry has no installation id")?
        .to_owned();
    anyhow::ensure!(
        installation.installation_id == installation_id,
        "Gateway installation identity mismatch"
    );
    let session_ref = endpoint
        .session_ref
        .clone()
        .unwrap_or_else(|| endpoint.id.clone());

    if endpoint.session_ref.is_some() || endpoint.server_gateway_id.is_some() {
        bail!("Gateway endpoint already has a device session");
    }

    let activation = Zeroizing::new(
        normalize_device_activation_code(activation_code.trim())
            .map_err(anyhow::Error::msg)
            .context("invalid Gateway activation credential")?,
    );
    let expected_gateway_id = expected_pin
        .cloned()
        .or_else(|| endpoint.server_gateway_id.clone());

    let mut storage_endpoint = endpoint.clone();
    storage_endpoint.session_ref = Some(session_ref.clone());
    if let Some(session) = secrets
        .load(&storage_endpoint)
        .map_err(GatewayProvisioningStorageError::new)?
    {
        session
            .validate()
            .map_err(|_| anyhow::anyhow!("invalid durable Gateway session"))?;
        if session.installation_id != installation_id {
            bail!("durable Gateway session belongs to a different installation");
        }
        if expected_gateway_id
            .as_ref()
            .is_some_and(|expected| &session.gateway_id != expected)
        {
            bail!("durable Gateway session belongs to a different Gateway");
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
        installation: installation.clone(),
    };
    let grant = activate(&endpoint.gateway_base_url, activation.as_str(), params)
        .context("Gateway device activation failed")?;
    let cleanup_access = grant.access_token.clone();
    let cleanup_session_id = grant.session.id.clone();
    let (session, access) = match session_from_grant(
        grant,
        installation_id.as_str(),
        installation.client_kind,
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
    if let Err(error) = secrets.persist(&storage_endpoint, &session) {
        let _ = cleanup_session(
            &endpoint.gateway_base_url,
            access.access_token.expose_secret(),
            &session.session_id,
        );
        return Err(GatewayProvisioningStorageError::new(error).into());
    }
    let mut next = registry.clone();
    // Keep a durable envelope if the registry write fails: activation is one-shot.
    commit_registry_v3_binding(
        &mut next,
        endpoint_id,
        session_ref.as_str(),
        &session.gateway_id,
    )
    .map_err(anyhow::Error::new)?;
    save(&next)?;
    *registry = next;
    Ok(())
}

/// Replace a terminal session without orphaning its durable endpoint reference.
/// The existing registry already points to this key; a failed credential write keeps the old value.
pub fn replace_endpoint_session<F, C>(
    endpoint: &super::types::GatewayEndpoint,
    installation: &ClientInstallationDescriptor,
    activation_code: &str,
    secrets: &dyn GatewaySessionStorage,
    activate: F,
    mut cleanup: C,
) -> Result<()>
where
    F: FnOnce(&GatewayBaseUrl, &str, AuthDeviceActivateParams) -> Result<AuthSessionGrant>,
    C: FnMut(&GatewayBaseUrl, &str, &pioneer_protocol::AuthSessionId) -> Result<()>,
{
    anyhow::ensure!(
        endpoint.session_ref.is_some() && endpoint.server_gateway_id.is_some(),
        "Gateway replacement requires a durable binding"
    );
    let activation = Zeroizing::new(
        normalize_device_activation_code(activation_code.trim()).map_err(anyhow::Error::msg)?,
    );
    let grant = activate(
        &endpoint.gateway_base_url,
        &activation,
        AuthDeviceActivateParams {
            installation: installation.clone(),
        },
    )?;
    let access = grant.access_token.clone();
    let session_id = grant.session.id.clone();
    let result = session_from_grant(
        grant,
        &installation.installation_id,
        installation.client_kind,
        endpoint.server_gateway_id.as_ref(),
    )
    .and_then(|(session, _)| {
        secrets
            .persist(endpoint, &session)
            .map_err(|error| GatewayProvisioningStorageError::new(error).into())
    });
    if result.is_err() {
        let _ = cleanup(
            &endpoint.gateway_base_url,
            access.expose_secret(),
            &session_id,
        );
    }
    result
}

pub(crate) fn rollback_unprovisioned_remote(
    registry: &mut GatewayRegistry,
    change: &super::setup::AddRemoteGatewayChange,
    commit: &super::setup::AddRemoteGatewayCommit,
    storage: &dyn GatewaySessionStorage,
    save: impl FnOnce(&GatewayRegistry) -> Result<()>,
) -> Result<()> {
    let mut endpoint = commit.endpoint.clone();
    endpoint.session_ref = Some(endpoint.id.clone());
    if storage.load(&endpoint)?.is_none() {
        let mut reverted = registry.clone();
        change.rollback_commit(&mut reverted, commit);
        save(&reverted)?;
        *registry = reverted;
    }
    Ok(())
}
pub(crate) fn clear_endpoint_session_binding_durably(
    registry: &mut GatewayRegistry,
    id: &str,
    storage: &dyn GatewaySessionStorage,
    save: impl FnOnce(&GatewayRegistry) -> Result<()>,
) -> Result<()> {
    let endpoint = registry_endpoint(registry, id)?.clone();
    let mut next = registry.clone();
    let reference = super::registry::clear_endpoint_session_binding(&mut next, id)?;
    if reference.is_some() {
        storage.delete(&endpoint)?;
    }
    save(&next)?;
    *registry = next;
    Ok(())
}

pub fn activate_device_session(
    gateway_base_url: &GatewayBaseUrl,
    credential: &str,
    params: AuthDeviceActivateParams,
    timeout: Duration,
) -> Result<AuthSessionGrant> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialize Gateway auth exchange runtime")?;
    runtime
        .block_on(AuthExchangeClient::new(timeout).activate_device(
            gateway_base_url,
            credential,
            params,
        ))
        .map_err(anyhow::Error::new)
}

pub fn revoke_session_best_effort(
    gateway_base_url: &GatewayBaseUrl,
    access_token: &str,
    session_id: &pioneer_protocol::AuthSessionId,
    timeout: Duration,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialize Gateway session cleanup runtime")?;
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
    expected_client_kind: pioneer_protocol::ClientKind,
    expected_gateway_id: Option<&GatewayId>,
) -> Result<(GatewaySessionEnvelope, GatewaySessionAccessGrant)> {
    if grant.auth_protocol_version != pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION
        || expected_gateway_id.is_some_and(|expected| &grant.gateway.id != expected)
        || !matches!(
            grant.principal.kind,
            pioneer_protocol::PrincipalKind::Superuser | pioneer_protocol::PrincipalKind::User
        )
        || grant.device.installation_id != expected_installation_id
        || grant.device.client_kind != expected_client_kind
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
    let session = GatewaySessionEnvelope {
        schema_version: GATEWAY_SESSION_SCHEMA_VERSION,
        gateway_id: grant.gateway.id,
        principal_id: grant.principal.id,
        device_id: grant.device.id,
        session_id: grant.session.id,
        token_family_id: grant.session.token_family_id,
        installation_id: grant.device.installation_id,
        refresh_generation: grant.refresh_generation,
        refresh_expires_at_unix: grant.refresh_expires_at_unix,
        refresh_token: grant.refresh_token,
        pending_refresh_request_id: None,
    };
    session
        .validate()
        .map_err(|_| anyhow::anyhow!("invalid Gateway session envelope"))?;
    Ok((
        session,
        GatewaySessionAccessGrant {
            access_token: grant.access_token,
            access_expires_at_unix: grant.access_expires_at_unix,
        },
    ))
}

fn registry_endpoint<'a>(
    registry: &'a GatewayRegistry,
    endpoint_id: &str,
) -> Result<&'a super::types::GatewayEndpoint> {
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
        .with_context(|| format!("unknown Gateway endpoint `{endpoint_id}`"))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::Cell;

    use crate::gateway::types::{GatewayEndpoint, GatewayEndpointKind};
    use anyhow::anyhow;
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthGatewaySnapshot, AuthPrincipalSnapshot, AuthSecretString,
        AuthSessionSnapshot, AuthSessionStatus, ClientKind, CredentialStorageOrder, DeviceId,
        DeviceStatus, GatewayId, PrincipalId, PrincipalKind, TokenFamilyId,
    };

    use super::*;

    const ENDPOINT_ID: &str = "local";
    const INSTALLATION_ID: &str = "desktop-installation";
    const ACCESS_SECRET: &str = "access-secret";

    pub(crate) fn registry() -> GatewayRegistry {
        GatewayRegistry {
            version: crate::gateway::registry::CURRENT_GATEWAY_REGISTRY_VERSION,
            installation_id: Some(INSTALLATION_ID.to_owned()),
            active_gateway_id: None,
            local: Some(GatewayEndpoint {
                id: ENDPOINT_ID.to_owned(),
                name: "Local Gateway".to_owned(),
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
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

    pub(crate) fn grant() -> AuthSessionGrant {
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
                avatar_revision: None,
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

    #[derive(Default)]
    pub(crate) struct MemoryStorage {
        values: std::sync::Mutex<std::collections::HashMap<String, super::GatewaySessionEnvelope>>,
        pub(crate) fail_write: Cell<bool>,
        pub(crate) fail_delete: Cell<bool>,
    }
    impl MemoryStorage {
        fn get_gateway_session(&self, id: &str) -> Result<Option<super::GatewaySessionEnvelope>> {
            Ok(self.values.lock().unwrap().get(id).cloned())
        }
        fn has_gateway_session(&self, id: &str) -> Result<bool> {
            Ok(self.values.lock().unwrap().contains_key(id))
        }
    }
    impl GatewaySessionStorage for MemoryStorage {
        fn load(
            &self,
            endpoint: &GatewayEndpoint,
        ) -> Result<Option<super::GatewaySessionEnvelope>> {
            Ok(endpoint
                .session_ref
                .as_ref()
                .and_then(|id| self.values.lock().unwrap().get(id).cloned()))
        }
        fn persist(
            &self,
            endpoint: &GatewayEndpoint,
            envelope: &super::GatewaySessionEnvelope,
        ) -> Result<()> {
            if self.fail_write.get() {
                anyhow::bail!("synthetic storage failure");
            }
            self.values
                .lock()
                .unwrap()
                .insert(endpoint.session_ref.clone().unwrap(), envelope.clone());
            Ok(())
        }
        fn delete(&self, endpoint: &GatewayEndpoint) -> Result<()> {
            if self.fail_delete.get() {
                anyhow::bail!("synthetic delete failure");
            }
            if let Some(id) = &endpoint.session_ref {
                self.values.lock().unwrap().remove(id);
            }
            Ok(())
        }
    }
    fn secrets() -> MemoryStorage {
        MemoryStorage::default()
    }
    fn provision_endpoint_session<F, C, S>(
        registry: &mut GatewayRegistry,
        id: &str,
        code: &str,
        storage: &dyn GatewaySessionStorage,
        activate: F,
        cleanup: C,
        save: S,
    ) -> Result<()>
    where
        F: FnOnce(&GatewayBaseUrl, &str, AuthDeviceActivateParams) -> Result<AuthSessionGrant>,
        C: FnMut(&GatewayBaseUrl, &str, &pioneer_protocol::AuthSessionId) -> Result<()>,
        S: FnMut(&GatewayRegistry) -> Result<()>,
    {
        let installation = ClientInstallationDescriptor {
            installation_id: INSTALLATION_ID.into(),
            display_name: "Synthetic".into(),
            client_kind: ClientKind::Desktop,
            platform: None,
            client_version: None,
        };
        super::provision_endpoint_session(
            registry,
            &installation,
            id,
            code,
            storage,
            activate,
            cleanup,
            save,
        )
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
    fn activation_accepts_an_invited_member_session() {
        let mut registry = registry();
        let secrets = secrets();
        let mut member_grant = grant();
        member_grant.principal.kind = PrincipalKind::User;
        member_grant.principal.display_name = "Invited Member".to_owned();
        member_grant.principal.nickname = "invited_member".to_owned();

        provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            activation_code().as_str(),
            &secrets,
            |_, _, _| Ok(member_grant),
            |_, _, _| panic!("valid member grant must not be cleaned up"),
            |_| Ok(()),
        )
        .expect("provision invited member session");

        assert!(
            secrets
                .has_gateway_session(ENDPOINT_ID)
                .expect("inspect durable member envelope")
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
                .contains("Gateway endpoint already has a device session")
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
    #[test]
    fn replacement_preserves_old_credential_on_failure_and_rejects_gateway_mismatch() {
        for mismatch in [false, true] {
            let mut registry = registry();
            let storage = secrets();
            provision_endpoint_session(
                &mut registry,
                ENDPOINT_ID,
                &activation_code(),
                &storage,
                |_, _, _| Ok(grant()),
                |_, _, _| panic!(),
                |_| Ok(()),
            )
            .unwrap();
            let endpoint = registry.local.as_ref().unwrap();
            let before = storage.load(endpoint).unwrap().unwrap();
            let mut replacement = grant();
            replacement.session.id =
                pioneer_protocol::AuthSessionId::new("S00000000000000000002").unwrap();
            if mismatch {
                replacement.gateway.id = GatewayId::new("G00000000000000000002").unwrap();
            } else {
                storage.fail_write.set(true);
            }
            let installation = ClientInstallationDescriptor {
                installation_id: INSTALLATION_ID.into(),
                display_name: "Synthetic".into(),
                client_kind: ClientKind::Desktop,
                platform: None,
                client_version: None,
            };
            let cleaned = Cell::new(false);
            assert!(
                super::replace_endpoint_session(
                    endpoint,
                    &installation,
                    &activation_code(),
                    &storage,
                    |_, _, _| Ok(replacement),
                    |_, _, id| {
                        assert_eq!(id.as_str(), "S00000000000000000002");
                        cleaned.set(true);
                        Ok(())
                    }
                )
                .is_err()
            );
            assert!(cleaned.get());
            assert_eq!(storage.load(endpoint).unwrap().unwrap(), before);
        }
    }
    #[test]
    fn replacement_reuses_durable_pointer_and_does_not_adopt_terminal_envelope() {
        let mut registry = registry();
        let storage = secrets();
        provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            &activation_code(),
            &storage,
            |_, _, _| Ok(grant()),
            |_, _, _| panic!(),
            |_| Ok(()),
        )
        .unwrap();
        let endpoint = registry.local.as_ref().unwrap();
        let old = storage.load(endpoint).unwrap().unwrap();
        let mut replacement = grant();
        replacement.session.id =
            pioneer_protocol::AuthSessionId::new("S00000000000000000002").unwrap();
        let installation = ClientInstallationDescriptor {
            installation_id: INSTALLATION_ID.into(),
            display_name: "Synthetic".into(),
            client_kind: ClientKind::Desktop,
            platform: None,
            client_version: None,
        };
        super::replace_endpoint_session(
            endpoint,
            &installation,
            &activation_code(),
            &storage,
            |_, _, _| Ok(replacement),
            |_, _, _| panic!(),
        )
        .unwrap();
        assert_ne!(
            storage.load(endpoint).unwrap().unwrap().session_id,
            old.session_id
        );
    }
    #[test]
    fn invalid_manual_code_never_exchanges_and_mobile_grant_supports_remote_transports() {
        for code in ["123456", "INVALID!"] {
            assert!(
                provision_endpoint_session(
                    &mut registry(),
                    ENDPOINT_ID,
                    code,
                    &secrets(),
                    |_, _, _| panic!(),
                    |_, _, _| panic!(),
                    |_| panic!()
                )
                .is_err()
            );
        }
        for address in [
            "gateway.invalid:17878",
            "https://gateway.invalid/",
            "http://gateway.invalid/",
            "127.0.0.1:17878",
        ] {
            let mut registry = registry();
            let endpoint = registry.local.take().unwrap();
            registry.remotes.push(GatewayEndpoint {
                kind: GatewayEndpointKind::Remote,
                gateway_base_url: GatewayBaseUrl::parse_presentation(address).unwrap(),
                ..endpoint
            });
            let mut response = grant();
            response.device.client_kind = ClientKind::Mobile;
            response.principal.kind = PrincipalKind::User;
            let installation = ClientInstallationDescriptor {
                installation_id: INSTALLATION_ID.into(),
                display_name: "Synthetic".into(),
                client_kind: ClientKind::Mobile,
                platform: None,
                client_version: None,
            };
            super::provision_endpoint_session(
                &mut registry,
                &installation,
                ENDPOINT_ID,
                "k7m4-p9q2",
                &secrets(),
                |_, code, params| {
                    assert_eq!(code, "K7M4P9Q2");
                    assert_eq!(params.installation.client_kind, ClientKind::Mobile);
                    Ok(response)
                },
                |_, _, _| panic!(),
                |_| Ok(()),
            )
            .unwrap();
            assert!(registry.remotes[0].server_gateway_id.is_some());
        }
    }

    #[test]
    fn failed_provisioning_rolls_back_only_when_no_durable_successor_exists() {
        for durable in [false, true] {
            let mut registry = registry();
            let storage = secrets();
            let change = super::super::setup::plan_add_remote_gateway(
                &registry,
                super::super::setup::AddRemoteGatewayInput {
                    name: "Remote",
                    gateway_base_url: "https://remote.invalid",
                    new_endpoint_id: "remote-synthetic".into(),
                    default_remote_name: "Remote".into(),
                },
            )
            .unwrap();
            let commit = change
                .apply_to_registry(
                    &mut registry,
                    super::super::setup::AddRemoteGatewayApplyMode::ProfileOnly,
                )
                .unwrap();
            if durable {
                let (session, _) =
                    super::session_from_grant(grant(), INSTALLATION_ID, ClientKind::Desktop, None)
                        .unwrap();
                let mut endpoint = commit.endpoint.clone();
                endpoint.session_ref = Some(endpoint.id.clone());
                storage.persist(&endpoint, &session).unwrap();
            }
            let before = registry.clone();
            let saved = Cell::new(false);
            super::rollback_unprovisioned_remote(
                &mut registry,
                &change,
                &commit,
                &storage,
                |next| {
                    assert!(next.remotes.is_empty());
                    saved.set(true);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(saved.get(), !durable);
            assert_eq!(registry.remotes.len(), usize::from(durable));
            if durable {
                assert_eq!(registry, before);
            }
        }
    }
    #[test]
    fn failed_credential_delete_retains_binding_for_explicit_recovery_retry() {
        let mut registry = registry();
        let storage = secrets();
        provision_endpoint_session(
            &mut registry,
            ENDPOINT_ID,
            &activation_code(),
            &storage,
            |_, _, _| Ok(grant()),
            |_, _, _| panic!(),
            |_| Ok(()),
        )
        .unwrap();
        let before = registry.clone();
        storage.fail_delete.set(true);
        assert!(
            super::clear_endpoint_session_binding_durably(
                &mut registry,
                ENDPOINT_ID,
                &storage,
                |_| panic!("must keep durable pointer")
            )
            .is_err()
        );
        assert_eq!(registry, before);
        assert!(
            storage
                .load(registry.local.as_ref().unwrap())
                .unwrap()
                .is_some()
        );
        storage.fail_delete.set(false);
        super::clear_endpoint_session_binding_durably(
            &mut registry,
            ENDPOINT_ID,
            &storage,
            |next| {
                assert!(next.local.as_ref().unwrap().session_ref.is_none());
                Ok(())
            },
        )
        .unwrap();
        assert!(registry.local.unwrap().server_gateway_id.is_none());
    }
}
