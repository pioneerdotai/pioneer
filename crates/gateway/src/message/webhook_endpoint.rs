use anyhow::{Context, Result};
use pioneer_protocol::{
    GatewayBaseUrl, GatewayRemoteAccessState, GatewayRemoteAccessStatusSnapshot,
};

use super::MessageProcessor;

impl MessageProcessor {
    pub(crate) fn gateway_secrets(&self) -> &crate::secrets::GatewaySecrets {
        self.gateway_secrets.as_ref()
    }

    pub(crate) fn set_gateway_listen_addr(&self, address: std::net::SocketAddr) {
        *self
            .gateway_listen_addr
            .write()
            .expect("Gateway listen address lock") = Some(address);
    }

    pub(crate) async fn task_webhook_base_url(&self) -> Result<(GatewayBaseUrl, &'static str)> {
        let _settings_guard = self.gateway_settings_update_lock.lock().await;
        if let Some(supervisor) = &self.remote_access_supervisor {
            let status = supervisor.status_snapshot();
            if status.state == GatewayRemoteAccessState::Connected {
                if let Some(base) = status.public_base_url {
                    return Ok((base, "public"));
                }
            }
        }
        let bind = *self
            .gateway_listen_addr
            .read()
            .expect("Gateway listen address lock");
        let bind = match bind {
            Some(bind) => bind,
            None => self
                .invitation_gateway_base_url
                .socket_address_input()
                .parse()
                .context("Gateway listen address is not available")?,
        };
        crate::webhook_address::local_webhook_base_url(bind)
    }

    /// Serialize startup with settings updates and reload the current key/settings.
    /// A delayed post-startup job must never reconnect an obsolete key.
    pub(crate) async fn apply_initial_remote_access(
        &self,
        config: &pioneer_config::GatewayRemoteAccessConfig,
    ) -> Result<()> {
        let Some(supervisor) = &self.remote_access_supervisor else {
            return Ok(());
        };
        let _settings_guard = self.gateway_settings_update_lock.lock().await;
        let app_config = webhook_app_config()?;
        let settings = crate::load_gateway_settings(&self.artifact_runtime_home, &app_config)?;
        supervisor
            .apply(crate::remote_access_desired_state(
                config,
                &settings,
                self.gateway_secrets.as_ref(),
            )?)
            .await
    }

    /// The notification watch may be delayed. Persist the current authenticated
    /// snapshot while both settings reconfiguration and supervisor replacement
    /// are excluded; never copy a queued old connection URL into the file.
    pub(super) async fn persist_current_remote_access_public_base_url(
        &self,
    ) -> Result<GatewayRemoteAccessStatusSnapshot> {
        let Some(supervisor) = &self.remote_access_supervisor else {
            return Ok(Default::default());
        };
        let _settings_guard = self.gateway_settings_update_lock.lock().await;
        let config = webhook_app_config()?;
        let file_name =
            crate::settings::normalize_settings_file_name(&config.gateway.settings_file_name)?;
        let path = self.artifact_runtime_home.join(&file_name);
        let mut settings = crate::settings::load_or_create_gateway_settings(
            &path,
            config.gateway.settings_version,
            &file_name,
        )?;
        supervisor
            .with_current_status(|status| {
                let public_base_url = if status.state == GatewayRemoteAccessState::Connected {
                    status.public_base_url.clone()
                } else {
                    None
                };
                if settings.set_remote_access_public_base_url(public_base_url) {
                    crate::settings::save_gateway_settings(&path, &settings)?;
                }
                Ok(status.clone())
            })
            .await
    }
}

fn webhook_app_config() -> Result<pioneer_config::AppConfig> {
    #[cfg(not(test))]
    {
        pioneer_config::AppConfig::load().context("failed to load Gateway configuration")
    }
    #[cfg(test)]
    {
        crate::isolated_test_app_config()
    }
}
