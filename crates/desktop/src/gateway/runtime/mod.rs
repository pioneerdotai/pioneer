mod compat;
mod discovery;
mod recovery;

use crate::gateway::connectivity::normalize_address;
use crate::gateway::control::{GatewayInstallWarning, request_local_superuser_token};
use crate::gateway::registry::{load_registry, save_registry, setup_required};
use crate::gateway::timings::{GatewayTimings, GatewayWsTimings};
use crate::gateway::types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry};
use anyhow::{Context, Result, bail};
use pioneer_config::AppConfig;
use pioneer_protocol::generate_id;
use std::path::PathBuf;
use tracing::info;

#[cfg(test)]
pub(crate) use compat::is_same_gateway_version;
#[cfg(test)]
pub(crate) use recovery::classify_local_gateway_state;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveGatewayState {
    NotConfigured,
    Connected,
    Unreachable,
    LocalAddressConflict,
}

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

pub struct GatewayRuntime {
    config: AppConfig,
    timings: GatewayTimings,
    ws_timings: GatewayWsTimings,
    registry_path: PathBuf,
    registry: GatewayRegistry,
}

impl GatewayRuntime {
    pub fn load() -> Result<Self> {
        let config = AppConfig::load().context(t!("errors.config.load_app").to_string())?;
        let runtime_home = config
            .ensure_runtime_home_dir()
            .context(t!("errors.runtime.ensure_home").to_string())?;
        let registry_file_name = config.desktop.gateway.registry_file_name.trim();
        let registry_path = runtime_home.join(registry_file_name);
        let timings = GatewayTimings::from_config(&config.desktop.gateway)?;
        let ws_timings = GatewayWsTimings::from_config(&config.desktop.gateway)?;
        let registry = load_registry(&registry_path, &config)?;

        let runtime = Self {
            config,
            timings,
            ws_timings,
            registry_path,
            registry,
        };

        Ok(runtime)
    }

    pub fn setup_required(&self) -> bool {
        setup_required(&self.registry)
    }

    pub fn local_gateway_update_required() -> bool {
        discovery::managed_gateway_requires_update()
    }

    pub fn local_gateway_install_required() -> bool {
        discovery::managed_gateway_requires_install()
    }

    pub fn active_gateway_id(&self) -> Option<&str> {
        self.registry.active_gateway_id.as_deref()
    }

    pub fn endpoints(&self) -> Vec<GatewayEndpoint> {
        let mut endpoints = Vec::with_capacity(self.registry.remotes.len() + 1);
        endpoints.push(self.registry.local.clone());
        endpoints.extend(self.registry.remotes.clone());
        endpoints
    }

    pub fn active_gateway(&self) -> Option<&GatewayEndpoint> {
        let active_id = self.registry.active_gateway_id.as_deref()?;
        self.endpoint_by_id(active_id)
    }

    pub fn active_workspace_id(&self) -> Option<&str> {
        self.active_gateway()
            .and_then(|endpoint| endpoint.workspace_id.as_deref())
    }

    pub fn endpoint(&self, id: &str) -> Option<GatewayEndpoint> {
        self.endpoint_by_id(id).cloned()
    }

    pub fn ws_timings(&self) -> GatewayWsTimings {
        self.ws_timings
    }

    pub fn activate_gateway(&mut self, id: &str) -> Result<()> {
        if self.endpoint_by_id(id).is_none() {
            bail!("{}", t!("errors.gateway.id_not_found", id = id));
        }

        self.registry.active_gateway_id = Some(id.to_owned());
        save_registry(&self.registry_path, &self.registry)
    }

    pub fn set_gateway_workspace_id(
        &mut self,
        gateway_id: &str,
        workspace_id: Option<String>,
    ) -> Result<()> {
        let Some(endpoint) = self.endpoint_by_id_mut(gateway_id) else {
            bail!("{}", t!("errors.gateway.id_not_found", id = gateway_id));
        };

        endpoint.workspace_id = workspace_id;
        save_registry(&self.registry_path, &self.registry)
    }

    pub fn add_remote_gateway(
        &mut self,
        name: &str,
        address: &str,
        auth_token: Option<&str>,
    ) -> Result<GatewayEndpoint> {
        let address = normalize_address(address)?;
        let auth_token = auth_token.and_then(|token| {
            let token = token.trim();
            (!token.is_empty()).then_some(token.to_owned())
        });

        if !crate::gateway::connectivity::is_gateway_reachable(
            &address,
            self.timings.connect_timeout,
        )? {
            bail!(
                "{}",
                t!(
                    "errors.gateway.unreachable_verify",
                    address = address.as_str()
                )
            );
        }

        if let Some(existing_index) = self
            .registry
            .remotes
            .iter()
            .position(|remote| remote.address == address)
        {
            let existing = self
                .registry
                .remotes
                .get_mut(existing_index)
                .expect("remote index should exist");
            if !name.trim().is_empty() {
                existing.name = name.trim().to_owned();
            }
            if auth_token.is_some() {
                existing.auth_token = auth_token;
            }

            let endpoint = existing.clone();
            save_registry(&self.registry_path, &self.registry)?;
            return Ok(endpoint);
        }

        let endpoint_id = format!("remote-{}", generate_id(8));
        let endpoint_name = if name.trim().is_empty() {
            t!(
                "gateway.endpoint.remote_name",
                index = self.registry.remotes.len() + 1
            )
            .to_string()
        } else {
            name.trim().to_owned()
        };

        let endpoint = GatewayEndpoint {
            id: endpoint_id.clone(),
            name: endpoint_name,
            address,
            kind: GatewayEndpointKind::Remote,
            auth_token,
            workspace_id: None,
            service_name: None,
        };

        self.registry.remotes.push(endpoint.clone());
        save_registry(&self.registry_path, &self.registry)?;
        Ok(endpoint)
    }

    fn sync_local_auth_token_from_gateway_request(&mut self, force_refresh: bool) -> Result<bool> {
        if !force_refresh
            && self
                .registry
                .local
                .auth_token
                .as_deref()
                .is_some_and(|token| !token.trim().is_empty())
        {
            return Ok(false);
        }

        let token = request_local_superuser_token()?;
        if self.registry.local.auth_token.as_deref() == Some(token.as_str()) {
            return Ok(false);
        }

        self.registry.local.auth_token = Some(token);
        Ok(true)
    }

    fn endpoint_by_id(&self, id: &str) -> Option<&GatewayEndpoint> {
        if self.registry.local.id == id {
            return Some(&self.registry.local);
        }

        self.registry
            .remotes
            .iter()
            .find(|endpoint| endpoint.id == id)
    }

    fn endpoint_by_id_mut(&mut self, id: &str) -> Option<&mut GatewayEndpoint> {
        if self.registry.local.id == id {
            return Some(&mut self.registry.local);
        }

        self.registry
            .remotes
            .iter_mut()
            .find(|endpoint| endpoint.id == id)
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
