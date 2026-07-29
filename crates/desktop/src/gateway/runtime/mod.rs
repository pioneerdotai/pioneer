mod compat;
mod discovery;
mod recovery;
mod session;

pub(crate) use session::{DesktopSessionConnectionOutcome, DesktopSessionPreparation};

use crate::gateway::activation::{
    activate_device_session, provision_endpoint_session, revoke_session_best_effort,
};
use crate::gateway::connectivity::validate_remote_gateway_address;
use crate::gateway::control::{GatewayInstallWarning, create_local_pending_device_session};
use crate::gateway::registry::{
    complete_registry_upgrade, load_registry_for_runtime, save_registry, setup_required,
};
use crate::gateway::secrets::DesktopSecrets;
use crate::gateway::timings::{
    GatewayTimings, GatewayWsTimings, gateway_timings_from_config, gateway_ws_timings_from_config,
};
use anyhow::{Context, Result};
use pioneer_client::gateway::runtime as client_gateway_runtime;
use pioneer_client::gateway::setup as client_gateway_setup;
use pioneer_client::gateway::types::{GatewayEndpoint, GatewayRegistry};
use pioneer_config::AppConfig;
use std::{collections::HashMap, path::PathBuf};
use tracing::info;

use pioneer_client::gateway::runtime::ActiveGatewayState;

#[cfg(test)]
pub(crate) use compat::is_same_gateway_version;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalGatewayRecovery {
    NotNeeded,
    AlreadyRunning,
    Started,
}
pub struct LocalGatewayStartOutcome {
    pub endpoint: GatewayEndpoint,
    pub warnings: Vec<GatewayInstallWarning>,
}

pub struct GatewayDeleteOutcome {
    pub deleted_active: bool,
    pub fallback_endpoint: Option<GatewayEndpoint>,
}

pub struct GatewayRuntime {
    config: AppConfig,
    timings: GatewayTimings,
    ws_timings: GatewayWsTimings,
    registry_path: PathBuf,
    registry: GatewayRegistry,
    registry_upgrade_pending: bool,
    secrets: DesktopSecrets,
    terminal_sessions:
        HashMap<String, pioneer_client::gateway::session_lifecycle::SessionTerminalReason>,
    access_expiries: HashMap<String, u64>,
}

impl GatewayRuntime {
    pub fn load() -> Result<Self> {
        let config = AppConfig::load().context(t!("errors.config.load_app").to_string())?;
        let runtime_home = config
            .ensure_runtime_home_dir()
            .context(t!("errors.runtime.ensure_home").to_string())?;
        let registry_file_name = config.desktop.gateway.registry_file_name.trim();
        let registry_path = runtime_home.join(registry_file_name);
        let timings = gateway_timings_from_config(&config.desktop.gateway)?;
        let ws_timings = gateway_ws_timings_from_config(&config.desktop.gateway)?;
        let secrets = DesktopSecrets::open(&runtime_home)?;
        let loaded_registry = load_registry_for_runtime(&registry_path, &config)?;

        let mut runtime = Self {
            config,
            timings,
            ws_timings,
            registry_path,
            registry: loaded_registry.registry,
            registry_upgrade_pending: loaded_registry.upgrade_pending,
            secrets,
            terminal_sessions: HashMap::new(),
            access_expiries: HashMap::new(),
        };
        if runtime.registry_upgrade_pending {
            runtime.resume_registry_upgrade_from_durable_session()?;
        } else {
            runtime.purge_retired_desktop_credentials();
        }

        Ok(runtime)
    }

    pub fn setup_required(&self) -> bool {
        setup_required(&self.registry)
    }

    pub(crate) fn registry_upgrade_pending(&self) -> bool {
        self.registry_upgrade_pending
    }

    pub fn local_gateway_update_required() -> bool {
        discovery::managed_gateway_requires_update()
    }

    pub fn local_gateway_install_required() -> bool {
        discovery::managed_gateway_requires_install()
    }

    pub fn local_gateway_provisioned(&self) -> Result<bool> {
        if let Some(session_ref) = self.local_gateway()?.session_ref.as_deref()
            && self.secrets.has_gateway_session(session_ref)?
        {
            return Ok(true);
        }
        if !Self::local_gateway_install_required() {
            return Ok(true);
        }

        if crate::gateway::control::is_configured_service_active(
            self.config.gateway.service_name.as_str(),
        )? {
            return Ok(true);
        }

        crate::gateway::connectivity::is_gateway_reachable(
            self.config.gateway.listen_addr.as_str(),
            self.timings.connect_timeout,
        )
    }

    pub fn active_gateway_id(&self) -> Option<&str> {
        client_gateway_runtime::active_gateway_id(&self.registry)
    }

    pub fn endpoints(&self) -> Vec<GatewayEndpoint> {
        client_gateway_runtime::selectable_gateway_endpoints(
            &self.registry,
            self.local_gateway_id(),
        )
    }

    pub fn active_gateway(&self) -> Option<&GatewayEndpoint> {
        client_gateway_runtime::active_gateway(&self.registry)
    }

    pub fn active_workspace_id(&self) -> Option<&str> {
        client_gateway_runtime::active_workspace_id(&self.registry)
    }

    pub fn endpoint(&self, id: &str) -> Option<GatewayEndpoint> {
        self.endpoint_by_id(id).cloned()
    }

    #[cfg(test)]
    pub fn ws_timings(&self) -> GatewayWsTimings {
        self.ws_timings
    }

    pub(crate) fn provision_gateway_session(
        &mut self,
        endpoint_id: &str,
        activation_code: &str,
    ) -> Result<()> {
        let registry_path = self.registry_path.clone();
        let timeout = self.timings.startup_timeout;
        provision_endpoint_session(
            &mut self.registry,
            endpoint_id,
            activation_code,
            &self.secrets,
            |address, credential, params| {
                activate_device_session(address, credential, params, timeout)
            },
            |address, access_token, session_id| {
                revoke_session_best_effort(address, access_token, session_id, timeout)
            },
            |registry| save_registry(registry_path.as_path(), registry),
        )?;
        self.complete_registry_upgrade_after_session_cutover();
        Ok(())
    }

    pub(crate) fn stage_local_gateway_session_recovery(&mut self, endpoint_id: &str) -> Result<()> {
        let session_mutation = self.begin_session_mutation(endpoint_id)?;
        let local = self.local_gateway()?.clone();
        if local.id != endpoint_id {
            anyhow::bail!("local Gateway recovery was requested for a non-local endpoint");
        }

        // Create the replacement pending device session before removing the
        // terminal envelope. If local creation fails, the previous durable state
        // stays untouched.
        let activation_code = create_local_pending_device_session()?;
        self.clear_gateway_session_binding_durably(endpoint_id)?;
        self.provision_gateway_session(endpoint_id, activation_code.as_str())?;
        drop(session_mutation);
        self.clear_session_terminal_for_explicit_retry(endpoint_id);
        Ok(())
    }

    pub fn activate_gateway(&mut self, id: &str) -> Result<()> {
        let plan = client_gateway_setup::plan_activate_gateway_registry(&self.registry, id)
            .map_err(map_gateway_profile_error)?;

        save_registry(&self.registry_path, &plan.registry)?;
        self.registry = plan.registry;
        Ok(())
    }

    pub fn reauthenticate_remote_gateway(
        &mut self,
        endpoint_id: &str,
        activation_code: &str,
    ) -> Result<GatewayEndpoint> {
        let endpoint = self
            .endpoint(endpoint_id)
            .with_context(|| format!("unknown desktop Gateway endpoint `{endpoint_id}`"))?;
        if endpoint.kind != pioneer_client::gateway::types::GatewayEndpointKind::Remote {
            anyhow::bail!("only a remote Gateway can be reauthenticated with an activation code");
        }
        if endpoint.session_ref.is_some() || endpoint.server_gateway_id.is_some() {
            anyhow::bail!("remote Gateway endpoint already has a device session");
        }
        self.provision_gateway_session(endpoint_id, activation_code)?;
        self.endpoint(endpoint_id)
            .context("remote Gateway endpoint disappeared after reauthentication")
    }

    pub fn set_gateway_workspace_id(
        &mut self,
        gateway_id: &str,
        workspace_id: Option<String>,
    ) -> Result<()> {
        let plan = client_gateway_setup::plan_set_gateway_workspace_registry(
            &self.registry,
            gateway_id,
            workspace_id,
        )
        .map_err(map_gateway_profile_error)?;

        save_registry(&self.registry_path, &plan.registry)?;
        self.registry = plan.registry;
        Ok(())
    }

    pub fn add_remote_gateway(
        &mut self,
        name: &str,
        address: &str,
        activation_code: Option<&str>,
    ) -> Result<GatewayEndpoint> {
        let address = validate_remote_gateway_address(address, self.timings.connect_timeout)?;

        let change = client_gateway_setup::plan_add_remote_gateway(
            &self.registry,
            client_gateway_setup::AddRemoteGatewayInput {
                name,
                address: address.as_str(),
                new_endpoint_id: client_gateway_setup::generated_remote_gateway_endpoint_id(),
                default_remote_name: t!(
                    "gateway.endpoint.remote_name",
                    index = self.registry.remotes.len() + 1
                )
                .to_string(),
            },
        )
        .map_err(map_gateway_profile_error)?;

        let commit = change
            .apply_to_registry(
                &mut self.registry,
                client_gateway_setup::AddRemoteGatewayApplyMode::ProfileOnly,
            )
            .map_err(map_gateway_profile_error)?;
        if let Err(error) = save_registry(&self.registry_path, &self.registry) {
            change.rollback_commit(&mut self.registry, &commit);
            return Err(error);
        }
        if commit.endpoint.session_ref.is_none() {
            let activation_code = activation_code
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .context("a Superuser device activation code is required for a new Gateway")?;
            self.provision_gateway_session(commit.endpoint.id.as_str(), activation_code)?;
        }
        self.endpoint(commit.endpoint.id.as_str())
            .context("remote Gateway endpoint disappeared after provisioning")
    }

    pub fn update_remote_gateway(
        &mut self,
        id: &str,
        name: &str,
        address: &str,
    ) -> Result<GatewayEndpoint> {
        let _session_mutation = self.begin_session_mutation(id)?;
        let default_index = self
            .registry
            .remotes
            .iter()
            .position(|remote| remote.id == id)
            .map(|index| index + 1)
            .unwrap_or_else(|| self.registry.remotes.len() + 1);
        let plan = client_gateway_setup::plan_update_remote_gateway_registry(
            &self.registry,
            client_gateway_setup::UpdateRemoteGatewayRegistryInput {
                gateway_id: id,
                name,
                address,
                default_remote_name: t!("gateway.endpoint.remote_name", index = default_index)
                    .to_string(),
            },
        )
        .map_err(map_gateway_profile_error)?;
        save_registry(&self.registry_path, &plan.registry)?;

        self.registry = plan.registry;

        Ok(plan.endpoint)
    }

    pub fn delete_gateway(&mut self, id: &str) -> Result<GatewayDeleteOutcome> {
        let _session_mutation = self.begin_session_mutation(id)?;
        let plan = client_gateway_setup::plan_delete_remote_gateway_registry(
            &self.registry,
            client_gateway_setup::DeleteRemoteGatewayRegistryInput {
                gateway_id: id,
                local_gateway_id: Some(self.local_gateway_id()),
            },
        )
        .map_err(map_gateway_profile_error)?;

        // Keep the old registry binding durable until every referenced
        // credential has been deleted. Secret-store deletion is idempotent,
        // so any failure (including the registry write itself) leaves a
        // retryable operation instead of an untracked credential.
        if let Some(session_ref) = plan.endpoint.session_ref.as_deref() {
            self.secrets.delete_gateway_session(session_ref)?;
        }

        save_registry(&self.registry_path, &plan.registry)?;
        self.registry = plan.registry;
        self.discard_gateway_session_runtime_state(id);

        Ok(GatewayDeleteOutcome {
            deleted_active: plan.deleted_active,
            fallback_endpoint: plan.fallback_endpoint,
        })
    }

    fn endpoint_by_id(&self, id: &str) -> Option<&GatewayEndpoint> {
        client_gateway_runtime::endpoint_by_id(&self.registry, id)
    }

    fn clear_gateway_session_binding_durably(&mut self, endpoint_id: &str) -> Result<()> {
        let mut next_registry = self.registry.clone();
        let session_ref = pioneer_client::gateway::registry::clear_endpoint_session_binding(
            &mut next_registry,
            endpoint_id,
        )
        .map_err(anyhow::Error::new)?;

        // Delete the credential while the old durable registry still points
        // at it. A keystore failure therefore leaves a retryable binding and
        // can never turn a stale terminal envelope into an adoptable session.
        if let Some(session_ref) = session_ref.as_deref() {
            self.secrets.delete_gateway_session(session_ref)?;
        }
        save_registry(&self.registry_path, &next_registry)?;
        self.registry = next_registry;
        Ok(())
    }

    fn purge_retired_desktop_credentials(&self) {
        match self.secrets.purge_retired_gateway_auth_tokens() {
            Ok(0) => {}
            Ok(deleted) => {
                info!(
                    deleted_credentials = deleted,
                    "retired desktop Gateway JWT credentials removed"
                );
            }
            Err(error) => {
                tracing::warn!(
                    error = %format!("{error:#}"),
                    "failed to remove retired desktop Gateway JWT credentials; cleanup will retry after the next successful session cutover"
                );
            }
        }
    }

    fn complete_registry_upgrade_after_session_cutover(&mut self) {
        if !self.registry_upgrade_pending {
            self.purge_retired_desktop_credentials();
            return;
        }

        match complete_registry_upgrade(&self.registry_path) {
            Ok(()) => {
                self.registry_upgrade_pending = false;
                self.purge_retired_desktop_credentials();
            }
            Err(error) => {
                tracing::warn!(
                    error = %format!("{error:#}"),
                    "failed to finalize the Gateway registry upgrade; the durable device session will be adopted and cleanup retried on the next Desktop start"
                );
            }
        }
    }

    fn resume_registry_upgrade_from_durable_session(&mut self) -> Result<()> {
        if self
            .adopt_durable_registry_sessions()
            .context("failed to adopt a durable device session while resuming registry upgrade")?
        {
            self.complete_registry_upgrade_after_session_cutover();
        }
        Ok(())
    }

    fn adopt_durable_registry_sessions(&mut self) -> Result<bool> {
        let installation_id = self
            .registry
            .installation_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("pending Gateway registry upgrade has no installation id")?
            .to_owned();
        let endpoint_ids = self
            .registry
            .local
            .iter()
            .chain(self.registry.remotes.iter())
            .map(|endpoint| endpoint.id.clone())
            .collect::<Vec<_>>();
        let mut next_registry = self.registry.clone();
        let mut changed = false;
        let mut found_durable_session = false;

        for endpoint_id in endpoint_ids {
            let endpoint = client_gateway_runtime::endpoint_by_id(&next_registry, &endpoint_id)
                .with_context(|| {
                    format!(
                        "Gateway endpoint `{endpoint_id}` disappeared while resuming registry upgrade"
                    )
                })?
                .clone();
            let (session_ref, expected_gateway_id) =
                match (&endpoint.session_ref, &endpoint.server_gateway_id) {
                    (Some(session_ref), Some(gateway_id)) => {
                        (session_ref.clone(), Some(gateway_id.clone()))
                    }
                    (None, None) => (endpoint.id.clone(), None),
                    _ => anyhow::bail!(
                        "Gateway endpoint `{}` has a partial session binding",
                        endpoint.id
                    ),
                };
            let Some(session) = self.secrets.get_gateway_session(session_ref.as_str())? else {
                if expected_gateway_id.is_some() {
                    pioneer_client::gateway::registry::clear_endpoint_session_binding(
                        &mut next_registry,
                        endpoint.id.as_str(),
                    )
                    .map_err(anyhow::Error::new)?;
                    changed = true;
                }
                continue;
            };
            if session.installation_id != installation_id {
                anyhow::bail!(
                    "durable session `{session_ref}` belongs to a different Desktop installation"
                );
            }
            if expected_gateway_id
                .as_ref()
                .is_some_and(|gateway_id| gateway_id != &session.gateway_id)
            {
                anyhow::bail!("durable session `{session_ref}` belongs to a different Gateway");
            }
            found_durable_session = true;
            if expected_gateway_id.is_none() {
                pioneer_client::gateway::registry::commit_registry_v2_binding(
                    &mut next_registry,
                    endpoint.id.as_str(),
                    session_ref.as_str(),
                    &session.gateway_id,
                )
                .map_err(anyhow::Error::new)?;
                changed = true;
            }
        }

        if changed {
            save_registry(&self.registry_path, &next_registry)?;
            self.registry = next_registry;
        }
        Ok(found_durable_session)
    }

    pub(crate) fn local_gateway(&self) -> Result<&GatewayEndpoint> {
        self.registry
            .local
            .as_ref()
            .context("desktop gateway registry is missing local gateway")
    }

    fn local_gateway_id(&self) -> &str {
        self.config.desktop.gateway.local_gateway_id.trim()
    }
}

pub fn ensure_runtime_home_dir() -> Result<PathBuf> {
    let config = AppConfig::load().context(t!("errors.config.load_app").to_string())?;
    let runtime_home = config
        .ensure_runtime_home_dir()
        .context(t!("errors.runtime.ensure_home").to_string())?;

    info!(
        runtime_home = %runtime_home.display(),
        message = %t!("logs.runtime.home_ready")
    );

    Ok(runtime_home)
}

fn map_gateway_profile_error(error: client_gateway_runtime::GatewayProfileError) -> anyhow::Error {
    match error {
        client_gateway_runtime::GatewayProfileError::EndpointNotFound { id } => {
            anyhow::anyhow!("{}", t!("errors.gateway.id_not_found", id = id))
        }
        client_gateway_runtime::GatewayProfileError::LocalGatewayDeleteUnsupported => {
            anyhow::anyhow!("{}", t!("errors.gateway.local_delete_unsupported"))
        }
        client_gateway_runtime::GatewayProfileError::DuplicateRemoteAddress { address } => {
            anyhow::anyhow!(
                "{}",
                t!(
                    "errors.gateway.address_already_exists",
                    address = address.as_str()
                )
            )
        }
        client_gateway_runtime::GatewayProfileError::InvalidAddress { address, .. } => {
            anyhow::anyhow!(
                "{}",
                t!(
                    "errors.gateway.invalid_address",
                    normalized = address.as_str()
                )
            )
        }
        client_gateway_runtime::GatewayProfileError::SessionBoundAddressChange { endpoint_id } => {
            anyhow::anyhow!(
                "Gateway `{endpoint_id}` must be reauthenticated before changing its address"
            )
        }
    }
}

#[cfg(test)]
impl GatewayRuntime {
    pub(crate) fn for_ws_spec_tests() -> Self {
        use std::sync::Arc;

        use pioneer_keystore::MemorySecretStore;

        use crate::gateway::{
            registry::default_registry,
            tests::{test_config, unique_temp_dir},
        };

        let config = test_config();
        let timings =
            gateway_timings_from_config(&config.desktop.gateway).expect("gateway timings");
        let ws_timings =
            gateway_ws_timings_from_config(&config.desktop.gateway).expect("gateway ws timings");
        let registry = default_registry(&config).expect("default registry");
        let registry_path =
            unique_temp_dir().join(config.desktop.gateway.registry_file_name.as_str());
        let secrets = DesktopSecrets::new(Arc::new(MemorySecretStore::new()));

        Self {
            config,
            timings,
            ws_timings,
            registry_path,
            registry,
            registry_upgrade_pending: false,
            secrets,
            terminal_sessions: HashMap::new(),
            access_expiries: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::gateway::{
        registry::{load_registry, save_registry},
        secrets::{
            DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION, DesktopGatewaySessionSecret, DesktopSecrets,
        },
        test_support::FailingDesktopSecretStore,
        tests::unique_temp_dir,
    };
    use pioneer_protocol::{
        AuthSecretString, AuthSessionId, DeviceId, GatewayId, PrincipalId, TokenFamilyId,
    };

    use super::GatewayRuntime;

    #[test]
    fn failed_secret_delete_keeps_the_durable_session_binding_retryable() {
        let mut runtime = GatewayRuntime::for_ws_spec_tests();
        let endpoint_id = runtime
            .registry
            .local
            .as_ref()
            .expect("local endpoint")
            .id
            .clone();
        {
            let local = runtime.registry.local.as_mut().expect("local endpoint");
            local.session_ref = Some(endpoint_id.clone());
            local.server_gateway_id =
                Some(pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap());
        }
        runtime.registry_path = unique_temp_dir().join("gateway-registry.json");
        save_registry(&runtime.registry_path, &runtime.registry).expect("save initial registry");

        let store = FailingDesktopSecretStore::new();
        store.fail_next_delete();
        runtime.secrets = DesktopSecrets::new(store);

        runtime
            .clear_gateway_session_binding_durably(endpoint_id.as_str())
            .expect_err("injected secret deletion must fail");

        let in_memory = runtime.registry.local.as_ref().expect("local endpoint");
        assert_eq!(in_memory.session_ref.as_deref(), Some(endpoint_id.as_str()));
        assert!(in_memory.server_gateway_id.is_some());

        let durable =
            load_registry(&runtime.registry_path, &runtime.config).expect("load durable registry");
        let durable_local = durable.local.as_ref().expect("local endpoint");
        assert_eq!(
            durable_local.session_ref.as_deref(),
            Some(endpoint_id.as_str())
        );
        assert!(durable_local.server_gateway_id.is_some());
    }

    #[test]
    fn restart_adopts_the_durable_session_before_finalizing_the_upgrade() {
        let mut runtime = GatewayRuntime::for_ws_spec_tests();
        let endpoint_id = runtime
            .registry
            .local
            .as_ref()
            .expect("local endpoint")
            .id
            .clone();
        let installation_id = runtime
            .registry
            .installation_id
            .clone()
            .expect("installation id");
        let gateway_id = GatewayId::new("G00000000000000000001").expect("Gateway identity");

        let runtime_dir = unique_temp_dir();
        fs::create_dir_all(&runtime_dir).expect("create runtime dir");
        runtime.registry_path = runtime_dir.join("gateway-registry.toml");
        save_registry(&runtime.registry_path, &runtime.registry).expect("save unbound registry");
        fs::write(
            runtime.registry_path.with_extension("upgrade-v2"),
            format!("version = 1\ninstallation_id = \"{installation_id}\"\n"),
        )
        .expect("write pending upgrade state");
        runtime.registry_upgrade_pending = true;

        runtime
            .resume_registry_upgrade_from_durable_session()
            .expect("inspect missing durable session");
        assert!(runtime.registry_upgrade_pending);
        assert!(runtime.registry_path.with_extension("upgrade-v2").exists());

        runtime
            .secrets
            .put_gateway_session(
                endpoint_id.as_str(),
                &DesktopGatewaySessionSecret {
                    schema_version: DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION,
                    gateway_id: gateway_id.clone(),
                    principal_id: PrincipalId::new("P00000000000000000001").expect("principal id"),
                    device_id: DeviceId::new("D00000000000000000001").expect("device id"),
                    session_id: AuthSessionId::new("S00000000000000000001").expect("session id"),
                    token_family_id: TokenFamilyId::new("F00000000000000000001")
                        .expect("token family id"),
                    installation_id,
                    refresh_generation: 0,
                    refresh_expires_at_unix: 2_000,
                    refresh_token: AuthSecretString::new(format!("prf_{}", "r".repeat(43))),
                },
                Some("Local Gateway session".to_owned()),
            )
            .expect("persist session envelope");

        runtime
            .resume_registry_upgrade_from_durable_session()
            .expect("adopt durable session");

        assert!(!runtime.registry_upgrade_pending);
        assert!(!runtime.registry_path.with_extension("upgrade-v2").exists());
        let local = runtime.registry.local.as_ref().expect("local endpoint");
        assert_eq!(local.session_ref.as_deref(), Some(endpoint_id.as_str()));
        assert_eq!(local.server_gateway_id.as_ref(), Some(&gateway_id));
        let durable =
            load_registry(&runtime.registry_path, &runtime.config).expect("load durable registry");
        let durable_local = durable.local.as_ref().expect("durable local endpoint");
        assert_eq!(
            durable_local.session_ref.as_deref(),
            Some(endpoint_id.as_str())
        );
        assert_eq!(durable_local.server_gateway_id.as_ref(), Some(&gateway_id));
        let _ = fs::remove_dir_all(runtime_dir);
    }
}
