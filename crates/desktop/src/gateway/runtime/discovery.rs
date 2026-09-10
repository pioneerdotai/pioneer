use crate::gateway::control::{
    GatewayInstallWarning, managed_gateway_install, update_gateway_service_from_desktop_binary,
};
use crate::gateway::timings::GatewayTimings;
use anyhow::{Context, Result, bail};
use tracing::info;

use super::compat::{is_same_gateway_version, local_gateway_version, managed_by_label};
pub(in crate::gateway) fn managed_gateway_requires_update() -> bool {
    let Some(install) = managed_gateway_install() else {
        return false;
    };

    !is_same_gateway_version(install.installed_version.as_str(), local_gateway_version())
}

pub(in crate::gateway) fn managed_gateway_requires_install() -> bool {
    managed_gateway_install().is_none()
}

pub(in crate::gateway) fn ensure_managed_gateway_up_to_date(
    service_name: &str,
    listen_addr: &str,
    timings: &GatewayTimings,
    startup_trace: Option<&pioneer_observability::DesktopStartupTrace>,
) -> Result<Vec<GatewayInstallWarning>> {
    let version_check = startup_trace.and_then(|trace| {
        trace.post_update_stage(pioneer_observability::DesktopPostUpdateStage::GatewayVersionCheck)
    });
    let Some(install) = managed_gateway_install() else {
        if let Some(stage) = version_check {
            stage.succeed();
        }
        return Ok(Vec::new());
    };

    let desktop_gateway_version = local_gateway_version();
    if is_same_gateway_version(install.installed_version.as_str(), desktop_gateway_version) {
        if let Some(stage) = version_check {
            stage.succeed();
        }
        return Ok(Vec::new());
    }
    if let Some(stage) = version_check {
        stage.succeed();
    }

    info!(
        installed_version = %install.installed_version,
        desktop_gateway_version = %desktop_gateway_version,
        "managed local gateway version differs from desktop; running auto-update"
    );

    let installer_execute = startup_trace.and_then(|trace| {
        trace.post_update_stage(
            pioneer_observability::DesktopPostUpdateStage::GatewayInstallerExecute,
        )
    });
    let warnings = update_gateway_service_from_desktop_binary(
        service_name,
        listen_addr,
        timings,
        startup_trace,
    )
    .with_context(|| {
        format!(
            "failed to auto-update managed gateway from version `{}` to match desktop version `{}`",
            install.installed_version, desktop_gateway_version
        )
    });
    if warnings.is_ok()
        && let Some(stage) = installer_execute
    {
        stage.succeed();
    }
    let warnings = warnings?;

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
