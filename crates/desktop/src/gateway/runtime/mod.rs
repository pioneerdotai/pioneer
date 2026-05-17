mod compat;
mod discovery;
mod recovery;

use crate::gateway::connectivity::normalize_address;
use crate::gateway::control::{GatewayInstallWarning, request_local_superuser_token};
use crate::gateway::registry::{
    gateway_auth_token_ref, load_registry, save_registry, setup_required,
};
use crate::gateway::secrets::DesktopSecrets;
use crate::gateway::timings::{GatewayTimings, GatewayWsTimings};
use crate::gateway::types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry};
use anyhow::{Context, Result, bail};
use pioneer_config::AppConfig;
use pioneer_protocol::generate_id;
use std::path::PathBuf;
use tracing::{info, warn};

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
    secrets: DesktopSecrets,
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
        let secrets = DesktopSecrets::open(&runtime_home)?;
        let registry = load_registry(&registry_path, &config)?;

        let runtime = Self {
            config,
            timings,
            ws_timings,
            registry_path,
            registry,
            secrets,
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

    pub fn local_gateway_provisioned(&self) -> Result<bool> {
        if self
            .gateway_auth_token_for_endpoint(&self.registry.local)?
            .is_some()
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
        self.registry.active_gateway_id.as_deref()
    }

    pub fn endpoints(&self) -> Vec<GatewayEndpoint> {
        let mut endpoints = Vec::with_capacity(self.registry.remotes.len() + 1);
        if self.local_gateway_is_selectable() {
            endpoints.push(self.registry.local.clone());
        }
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

    pub(crate) fn gateway_auth_token_for_endpoint(
        &self,
        endpoint: &GatewayEndpoint,
    ) -> Result<Option<String>> {
        let Some(token_ref) = endpoint.auth_token_ref.as_deref() else {
            return Ok(None);
        };

        let token = self
            .secrets
            .get_gateway_auth_token(token_ref)
            .with_context(|| {
                format!(
                    "failed to resolve desktop gateway auth token for endpoint `{}`",
                    endpoint.id
                )
            })?
            .and_then(normalize_token);

        Ok(token)
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
            let existing_id = self.registry.remotes[existing_index].id.clone();
            let existing_name = if name.trim().is_empty() {
                self.registry.remotes[existing_index].name.clone()
            } else {
                name.trim().to_owned()
            };
            let had_token_ref = self.registry.remotes[existing_index]
                .auth_token_ref
                .as_deref()
                .is_some_and(|token_ref| !token_ref.trim().is_empty());
            let auth_token_update = if let Some(token) = auth_token {
                let token_ref = gateway_auth_token_ref(existing_id.as_str())?;
                self.secrets.put_gateway_auth_token(
                    token_ref.as_str(),
                    token.as_str(),
                    Some(gateway_token_label(
                        existing_name.as_str(),
                        address.as_str(),
                    )),
                )?;
                Some(token_ref)
            } else {
                None
            };

            let existing = self
                .registry
                .remotes
                .get_mut(existing_index)
                .expect("remote index should exist");
            existing.name = existing_name;
            if let Some(token_ref) = auth_token_update {
                existing.auth_token_ref = Some(token_ref);
            }

            let endpoint = existing.clone();
            if let Err(error) = save_registry(&self.registry_path, &self.registry) {
                if !had_token_ref && let Some(token_ref) = endpoint.auth_token_ref.as_deref() {
                    let _ = self.secrets.delete_gateway_auth_token(token_ref);
                }
                return Err(error);
            }
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

        let mut endpoint = GatewayEndpoint {
            id: endpoint_id.clone(),
            name: endpoint_name,
            address,
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: None,
            workspace_id: None,
            service_name: None,
        };

        let mut wrote_token_ref = None;
        if let Some(token) = auth_token {
            let token_ref = gateway_auth_token_ref(endpoint_id.as_str())?;
            self.secrets.put_gateway_auth_token(
                token_ref.as_str(),
                token.as_str(),
                Some(gateway_token_label(
                    endpoint.name.as_str(),
                    endpoint.address.as_str(),
                )),
            )?;
            endpoint.auth_token_ref = Some(token_ref.clone());
            wrote_token_ref = Some(token_ref);
        }

        self.registry.remotes.push(endpoint.clone());
        if let Err(error) = save_registry(&self.registry_path, &self.registry) {
            if let Some(token_ref) = wrote_token_ref.as_deref() {
                let _ = self.secrets.delete_gateway_auth_token(token_ref);
            }
            self.registry
                .remotes
                .retain(|remote| remote.id != endpoint_id);
            return Err(error);
        }
        Ok(endpoint)
    }

    pub fn update_remote_gateway(
        &mut self,
        id: &str,
        name: &str,
        address: &str,
        auth_token: Option<&str>,
    ) -> Result<GatewayEndpoint> {
        let address = normalize_address(address)?;
        let Some(existing_index) = self
            .registry
            .remotes
            .iter()
            .position(|remote| remote.id == id)
        else {
            bail!("{}", t!("errors.gateway.id_not_found", id = id));
        };

        if self
            .registry
            .remotes
            .iter()
            .enumerate()
            .any(|(index, remote)| index != existing_index && remote.address == address)
        {
            bail!(
                "{}",
                t!(
                    "errors.gateway.address_already_exists",
                    address = address.as_str()
                )
            );
        }

        let old_endpoint = self.registry.remotes[existing_index].clone();
        let old_token = self.gateway_auth_token_for_endpoint(&old_endpoint)?;
        let endpoint_name = if name.trim().is_empty() {
            t!("gateway.endpoint.remote_name", index = existing_index + 1).to_string()
        } else {
            name.trim().to_owned()
        };
        let auth_token = auth_token.and_then(|token| {
            let token = token.trim();
            (!token.is_empty()).then_some(token.to_owned())
        });
        let next_token_ref = if auth_token.is_some() {
            Some(gateway_auth_token_ref(id)?)
        } else {
            None
        };

        if let Some(token) = auth_token.as_deref() {
            let token_ref = next_token_ref.as_deref().expect("token ref exists");
            self.secrets.put_gateway_auth_token(
                token_ref,
                token,
                Some(gateway_token_label(
                    endpoint_name.as_str(),
                    address.as_str(),
                )),
            )?;
        }

        let existing = self
            .registry
            .remotes
            .get_mut(existing_index)
            .expect("remote index should exist");
        existing.name = endpoint_name;
        existing.address = address;
        existing.auth_token_ref = next_token_ref.clone();

        let endpoint = existing.clone();
        if let Err(error) = save_registry(&self.registry_path, &self.registry) {
            self.registry.remotes[existing_index] = old_endpoint.clone();
            if let Some(old_token) = old_token.as_deref() {
                if let Some(token_ref) = old_endpoint.auth_token_ref.as_deref() {
                    let _ = self.secrets.put_gateway_auth_token(
                        token_ref,
                        old_token,
                        Some(gateway_token_label(
                            old_endpoint.name.as_str(),
                            old_endpoint.address.as_str(),
                        )),
                    );
                }
            } else if let Some(token_ref) = next_token_ref.as_deref() {
                let _ = self.secrets.delete_gateway_auth_token(token_ref);
            }
            return Err(error);
        }

        if auth_token.is_none()
            && let Some(token_ref) = old_endpoint.auth_token_ref.as_deref()
            && let Err(error) = self.secrets.delete_gateway_auth_token(token_ref)
        {
            warn!(
                error = %format!("{error:#}"),
                token_ref,
                "failed to delete cleared gateway auth token"
            );
        }

        Ok(endpoint)
    }

    pub fn delete_gateway(&mut self, id: &str) -> Result<GatewayDeleteOutcome> {
        if self.registry.local.id == id {
            bail!("{}", t!("errors.gateway.local_delete_unsupported"));
        }

        let Some(existing_index) = self
            .registry
            .remotes
            .iter()
            .position(|remote| remote.id == id)
        else {
            bail!("{}", t!("errors.gateway.id_not_found", id = id));
        };

        let deleted_endpoint = self.registry.remotes.remove(existing_index);
        let deleted_active = self.registry.active_gateway_id.as_deref() == Some(id);
        let previous_active_gateway_id = self.registry.active_gateway_id.clone();
        let fallback_endpoint = if deleted_active {
            let fallback = self.endpoints().into_iter().next();
            self.registry.active_gateway_id = fallback.as_ref().map(|endpoint| endpoint.id.clone());
            fallback
        } else {
            None
        };

        if let Err(error) = save_registry(&self.registry_path, &self.registry) {
            self.registry
                .remotes
                .insert(existing_index, deleted_endpoint.clone());
            self.registry.active_gateway_id = previous_active_gateway_id;
            return Err(error);
        }

        if let Some(token_ref) = deleted_endpoint.auth_token_ref.as_deref()
            && let Err(error) = self.secrets.delete_gateway_auth_token(token_ref)
        {
            warn!(
                error = %format!("{error:#}"),
                token_ref,
                "failed to delete removed gateway auth token"
            );
        }

        Ok(GatewayDeleteOutcome {
            deleted_active,
            fallback_endpoint,
        })
    }

    fn sync_local_auth_token_from_gateway_request(&mut self, force_refresh: bool) -> Result<bool> {
        if !force_refresh
            && self
                .gateway_auth_token_for_endpoint(&self.registry.local)?
                .is_some()
        {
            return Ok(false);
        }

        let token = request_local_superuser_token()?;
        self.store_local_auth_token(token)
    }

    fn store_local_auth_token(&mut self, token: String) -> Result<bool> {
        let token_ref = gateway_auth_token_ref(self.registry.local.id.as_str())?;
        if self.registry.local.auth_token_ref.as_deref() == Some(token_ref.as_str())
            && self
                .secrets
                .get_gateway_auth_token(token_ref.as_str())?
                .as_deref()
                == Some(token.as_str())
        {
            return Ok(false);
        }

        self.secrets.put_gateway_auth_token(
            token_ref.as_str(),
            token.as_str(),
            Some(gateway_token_label(
                self.registry.local.name.as_str(),
                self.registry.local.address.as_str(),
            )),
        )?;
        self.registry.local.auth_token_ref = Some(token_ref);
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

    fn local_gateway_is_selectable(&self) -> bool {
        let local_gateway_id = self.local_gateway_id();
        if self.registry.active_gateway_id.as_deref() == Some(local_gateway_id) {
            return true;
        }

        match self.gateway_auth_token_for_endpoint(&self.registry.local) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to determine whether local gateway should be shown"
                );
                false
            }
        }
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

fn gateway_token_label(name: &str, address: &str) -> String {
    format!("{} ({})", name.trim(), address.trim())
}

fn normalize_token(token: String) -> Option<String> {
    let trimmed = token.trim();
    (!trimmed.is_empty()).then_some(trimmed.to_owned())
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
            GatewayTimings::from_config(&config.desktop.gateway).expect("gateway timings");
        let ws_timings =
            GatewayWsTimings::from_config(&config.desktop.gateway).expect("gateway ws timings");
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
            secrets,
        }
    }

    pub(crate) fn store_gateway_auth_token_for_tests(
        &self,
        token_ref: &str,
        token: &str,
    ) -> Result<()> {
        self.secrets
            .put_gateway_auth_token(token_ref, token, Some(token_ref.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, net::TcpListener, sync::Arc, thread};

    use pioneer_keystore::MemorySecretStore;

    use crate::gateway::{
        registry::{default_registry, save_registry},
        tests::{test_config, unique_temp_dir},
    };

    use super::*;

    fn test_desktop_secrets() -> DesktopSecrets {
        DesktopSecrets::new(Arc::new(MemorySecretStore::new()))
    }

    fn test_runtime(registry_path: PathBuf, secrets: DesktopSecrets) -> GatewayRuntime {
        let config = test_config();
        let timings =
            GatewayTimings::from_config(&config.desktop.gateway).expect("gateway timings");
        let ws_timings =
            GatewayWsTimings::from_config(&config.desktop.gateway).expect("gateway ws timings");
        let registry = default_registry(&config).expect("default registry");

        GatewayRuntime {
            config,
            timings,
            ws_timings,
            registry_path,
            registry,
            secrets,
        }
    }

    #[test]
    fn remote_gateway_token_is_written_to_keystore_and_saved_as_ref() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path.clone(), secrets.clone());
        let (address, accept_thread) = reachable_test_address(3);

        let endpoint = runtime
            .add_remote_gateway("Remote", address.as_str(), Some("remote-token-one"))
            .expect("add remote with token");

        assert_eq!(
            endpoint.auth_token_ref.as_deref(),
            Some(endpoint.id.as_str())
        );
        assert_eq!(
            runtime
                .gateway_auth_token_for_endpoint(&endpoint)
                .expect("resolve endpoint token")
                .as_deref(),
            Some("remote-token-one")
        );
        assert_eq!(
            secrets
                .get_gateway_auth_token(endpoint.id.as_str())
                .expect("read remote token"),
            Some("remote-token-one".to_owned())
        );

        let content = fs::read_to_string(&registry_path).expect("read registry");
        assert!(content.contains(format!("auth_token_ref = \"{}\"", endpoint.id).as_str()));
        assert!(!content.contains("remote-token-one"));
        assert!(!content.contains("auth_token ="));

        let renamed = runtime
            .add_remote_gateway("Renamed", address.as_str(), None)
            .expect("update remote without token");
        assert_eq!(renamed.name, "Renamed");
        assert_eq!(
            renamed.auth_token_ref.as_deref(),
            Some(endpoint.id.as_str())
        );
        assert_eq!(
            runtime
                .gateway_auth_token_for_endpoint(&renamed)
                .expect("resolve renamed token")
                .as_deref(),
            Some("remote-token-one")
        );
        assert_eq!(
            secrets
                .get_gateway_auth_token(endpoint.id.as_str())
                .expect("read preserved remote token"),
            Some("remote-token-one".to_owned())
        );

        let updated = runtime
            .add_remote_gateway("Renamed", address.as_str(), Some("remote-token-two"))
            .expect("update remote token");
        assert_eq!(
            updated.auth_token_ref.as_deref(),
            Some(endpoint.id.as_str())
        );
        assert_eq!(
            runtime
                .gateway_auth_token_for_endpoint(&updated)
                .expect("resolve updated token")
                .as_deref(),
            Some("remote-token-two")
        );
        assert_eq!(
            secrets
                .get_gateway_auth_token(endpoint.id.as_str())
                .expect("read updated remote token"),
            Some("remote-token-two".to_owned())
        );

        let content = fs::read_to_string(&registry_path).expect("read updated registry");
        assert!(content.contains(format!("auth_token_ref = \"{}\"", endpoint.id).as_str()));
        assert!(!content.contains("remote-token-one"));
        assert!(!content.contains("remote-token-two"));
        assert!(!content.contains("auth_token ="));

        accept_thread.join().expect("accept thread joins");
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn local_gateway_token_is_written_to_keystore_and_saved_as_ref() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path.clone(), secrets.clone());

        assert!(
            runtime
                .store_local_auth_token("local-token".to_owned())
                .expect("store local token")
        );
        assert_eq!(
            runtime.registry.local.auth_token_ref.as_deref(),
            Some("local")
        );
        assert_eq!(
            runtime
                .gateway_auth_token_for_endpoint(&runtime.registry.local)
                .expect("resolve local token")
                .as_deref(),
            Some("local-token")
        );
        assert_eq!(
            secrets
                .get_gateway_auth_token("local")
                .expect("read local token"),
            Some("local-token".to_owned())
        );
        save_registry(&runtime.registry_path, &runtime.registry).expect("save registry");

        let content = fs::read_to_string(&registry_path).expect("read registry");
        assert!(content.contains("auth_token_ref = \"local\""));
        assert!(!content.contains("local-token"));
        assert!(!content.contains("auth_token ="));

        assert!(
            !runtime
                .store_local_auth_token("local-token".to_owned())
                .expect("store same local token")
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn local_gateway_provisioned_uses_saved_local_token() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path, secrets);

        assert!(
            runtime
                .store_local_auth_token("local-token".to_owned())
                .expect("store local token")
        );

        assert!(
            runtime
                .local_gateway_provisioned()
                .expect("check local provisioned")
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn endpoints_hide_uncreated_local_gateway() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path, secrets);
        let remote = test_remote_endpoint("remote-main");
        runtime.registry.active_gateway_id = Some(remote.id.clone());
        runtime.registry.remotes.push(remote.clone());

        let endpoints = runtime.endpoints();

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].id, remote.id);
        assert!(
            endpoints
                .iter()
                .all(|endpoint| endpoint.kind == GatewayEndpointKind::Remote)
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn endpoints_include_local_gateway_after_local_token_is_saved() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path, secrets);
        let remote = test_remote_endpoint("remote-main");
        runtime.registry.active_gateway_id = Some(remote.id.clone());
        runtime.registry.remotes.push(remote);
        assert!(
            runtime
                .store_local_auth_token("local-token".to_owned())
                .expect("store local token")
        );

        let endpoints = runtime.endpoints();

        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].id, runtime.local_gateway_id());
        assert_eq!(endpoints[0].kind, GatewayEndpointKind::Local);
        assert!(
            endpoints
                .iter()
                .any(|endpoint| endpoint.kind == GatewayEndpointKind::Remote)
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn endpoints_keep_active_local_gateway_visible_without_saved_token() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path, secrets);
        runtime.registry.active_gateway_id = Some(runtime.local_gateway_id().to_owned());

        let endpoints = runtime.endpoints();

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].id, runtime.local_gateway_id());
        assert_eq!(endpoints[0].kind, GatewayEndpointKind::Local);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn update_remote_gateway_rewrites_secret_and_can_clear_token() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path.clone(), secrets.clone());
        let remote = test_remote_endpoint("remote-main");
        runtime.registry.remotes.push(remote.clone());
        runtime
            .store_gateway_auth_token_for_tests(remote.id.as_str(), "old-token")
            .expect("store old token");

        let updated = runtime
            .update_remote_gateway(
                remote.id.as_str(),
                "Renamed",
                "127.0.0.1:23000",
                Some("new-token"),
            )
            .expect("update remote");

        assert_eq!(updated.name, "Renamed");
        assert_eq!(updated.address, "127.0.0.1:23000");
        assert_eq!(updated.auth_token_ref.as_deref(), Some(remote.id.as_str()));
        assert_eq!(
            secrets
                .get_gateway_auth_token(remote.id.as_str())
                .expect("read updated token"),
            Some("new-token".to_owned())
        );
        let content = fs::read_to_string(&registry_path).expect("read registry");
        assert!(!content.contains("new-token"));
        assert!(!content.contains("old-token"));

        let cleared = runtime
            .update_remote_gateway(remote.id.as_str(), "Renamed", "127.0.0.1:23000", Some(""))
            .expect("clear token");

        assert!(cleared.auth_token_ref.is_none());
        assert_eq!(
            secrets
                .get_gateway_auth_token(remote.id.as_str())
                .expect("read cleared token"),
            None
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn update_remote_gateway_rejects_duplicate_address() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path, secrets);
        let mut first = test_remote_endpoint("remote-one");
        first.address = "127.0.0.1:23000".to_owned();
        let mut second = test_remote_endpoint("remote-two");
        second.address = "127.0.0.1:24000".to_owned();
        runtime.registry.remotes.push(first.clone());
        runtime.registry.remotes.push(second.clone());

        let error = runtime
            .update_remote_gateway(
                first.id.as_str(),
                "First",
                second.address.as_str(),
                Some("token"),
            )
            .expect_err("duplicate address should fail");

        assert!(
            format!("{error:#}").contains("already"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn delete_gateway_removes_remote_secret_and_selects_fallback() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let registry_path = temp_dir.join("gateway_registry.toml");
        let secrets = test_desktop_secrets();
        let mut runtime = test_runtime(registry_path.clone(), secrets.clone());
        let first = test_remote_endpoint("remote-one");
        let second = test_remote_endpoint("remote-two");
        runtime.registry.active_gateway_id = Some(first.id.clone());
        runtime.registry.remotes.push(first.clone());
        runtime.registry.remotes.push(second.clone());
        runtime
            .store_gateway_auth_token_for_tests(first.id.as_str(), "first-token")
            .expect("store first token");
        runtime
            .store_gateway_auth_token_for_tests(second.id.as_str(), "second-token")
            .expect("store second token");

        let outcome = runtime
            .delete_gateway(first.id.as_str())
            .expect("delete active remote");

        assert!(outcome.deleted_active);
        assert_eq!(
            outcome
                .fallback_endpoint
                .as_ref()
                .map(|endpoint| endpoint.id.as_str()),
            Some(second.id.as_str())
        );
        assert_eq!(runtime.active_gateway_id(), Some(second.id.as_str()));
        assert!(
            runtime
                .registry
                .remotes
                .iter()
                .all(|endpoint| endpoint.id != first.id)
        );
        assert_eq!(
            secrets
                .get_gateway_auth_token(first.id.as_str())
                .expect("read removed token"),
            None
        );
        assert_eq!(
            secrets
                .get_gateway_auth_token(second.id.as_str())
                .expect("read fallback token"),
            Some("second-token".to_owned())
        );
        let content = fs::read_to_string(&registry_path).expect("read registry");
        assert!(!content.contains(first.id.as_str()));
        assert!(content.contains(second.id.as_str()));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    fn reachable_test_address(connection_count: usize) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let address = listener.local_addr().expect("local address").to_string();
        let handle = thread::spawn(move || {
            for _ in 0..connection_count {
                let _ = listener.accept().expect("accept reachability probe");
            }
        });
        (address, handle)
    }

    fn test_remote_endpoint(id: &str) -> GatewayEndpoint {
        GatewayEndpoint {
            id: id.to_owned(),
            name: "Remote".to_owned(),
            address: "127.0.0.1:22000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: Some(id.to_owned()),
            workspace_id: None,
            service_name: None,
        }
    }
}
