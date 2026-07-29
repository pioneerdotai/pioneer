use crate::gateway::connectivity::is_gateway_reachable;
use crate::gateway::control::{
    GatewayInstallWarning, is_configured_service_active, managed_gateway_install,
    update_gateway_service_from_desktop_binary,
};
use crate::gateway::registry::save_registry;
use crate::gateway::timings::GatewayTimings;
use anyhow::{Context, Result, bail};
use tracing::info;

use super::GatewayRuntime;
use super::compat::{is_same_gateway_version, local_gateway_version, managed_by_label};

impl GatewayRuntime {
    pub fn discover_and_adopt_existing_local_gateway_once(&mut self) -> Result<()> {
        if self.registry.active_gateway_id.is_some() {
            return Ok(());
        }

        let Some(install) = managed_gateway_install() else {
            return Ok(());
        };

        let service_name = self.config.gateway.service_name.as_str();
        let listen_addr = self.config.gateway.listen_addr.as_str();

        let service_active = is_configured_service_active(service_name)?;
        let gateway_reachable = is_gateway_reachable(listen_addr, self.timings.connect_timeout)?;

        info!(
            managed_by = %managed_by_label(&install.managed_by),
            installed_version = %install.installed_version,
            service_active,
            gateway_reachable,
            "discovered managed local gateway install"
        );

        let warnings = ensure_managed_gateway_up_to_date(service_name, listen_addr, &self.timings)?;
        for warning in warnings {
            info!(
                warning_code = %warning.code,
                warning_message = %warning.message,
                "managed local gateway auto-update warning"
            );
        }

        self.registry.active_gateway_id = Some(self.local_gateway_id().to_owned());
        if !self.registry_upgrade_pending() {
            save_registry(&self.registry_path, &self.registry)?;
        }
        Ok(())
    }
}

pub(super) fn managed_gateway_requires_update() -> bool {
    let Some(install) = managed_gateway_install() else {
        return false;
    };

    !is_same_gateway_version(install.installed_version.as_str(), local_gateway_version())
}

pub(super) fn managed_gateway_requires_install() -> bool {
    managed_gateway_install().is_none()
}

pub(super) fn ensure_managed_gateway_up_to_date(
    service_name: &str,
    listen_addr: &str,
    timings: &GatewayTimings,
) -> Result<Vec<GatewayInstallWarning>> {
    let Some(install) = managed_gateway_install() else {
        return Ok(Vec::new());
    };

    let desktop_gateway_version = local_gateway_version();
    if is_same_gateway_version(install.installed_version.as_str(), desktop_gateway_version) {
        return Ok(Vec::new());
    }

    info!(
        installed_version = %install.installed_version,
        desktop_gateway_version = %desktop_gateway_version,
        "managed local gateway version differs from desktop; running auto-update"
    );

    let warnings = update_gateway_service_from_desktop_binary(service_name, listen_addr, timings)
        .with_context(|| {
        format!(
            "failed to auto-update managed gateway from version `{}` to match desktop version `{}`",
            install.installed_version, desktop_gateway_version
        )
    })?;

    let refreshed = managed_gateway_install()
        .context("managed gateway install-state disappeared after auto-update")?;
    if !is_same_gateway_version(
        refreshed.installed_version.as_str(),
        desktop_gateway_version,
    ) {
        bail!(
            "auto-update completed but gateway version `{}` still differs from desktop `{}`",
            refreshed.installed_version,
            desktop_gateway_version
        );
    }

    Ok(warnings)
}
