//! Native files, installation identity, and local service effects for onboarding.
use super::{
    connectivity::*,
    control::*,
    registry::{load_registry_for_runtime, save_registry},
    timings::{gateway_timings_from_config, gateway_ws_timings_from_config},
};
use anyhow::{Context, Result, bail};
use pioneer_client::{
    core::ClientEffectResult,
    gateway::{onboarding_effects::*, session_refresh::GatewaySessionStorage},
};
use pioneer_protocol::{AuthSecretString, ClientInstallationDescriptor, ClientKind};

pub(crate) fn execute(
    effect: &OnboardingPlatformEffect,
    storage: &dyn GatewaySessionStorage,
) -> Result<ClientEffectResult> {
    let config = pioneer_config::AppConfig::load()?;
    let home = config.ensure_runtime_home_dir()?;
    let path = home.join(config.desktop.gateway.registry_file_name.trim());
    let timings = gateway_timings_from_config(&config.desktop.gateway)?;
    match effect {
        OnboardingPlatformEffect::RemoveGatewayBindingJournal { .. } => {
            anyhow::bail!("native binding journals are unsupported on Desktop")
        }
        OnboardingPlatformEffect::LoadGatewayEnvironment => {
            let registry = load_registry_for_runtime(&path, &config)?.registry;
            let local_install_required =
                super::runtime::discovery::managed_gateway_requires_install();
            let local_update_required =
                super::runtime::discovery::managed_gateway_requires_update();
            let local_provisioned = registry
                .local
                .as_ref()
                .map(|endpoint| storage.load(endpoint).map(|value| value.is_some()))
                .transpose()?
                .unwrap_or(false)
                || !local_install_required
                || is_configured_service_active(&config.gateway.service_name)?
                || is_local_gateway_reachable(
                    &config.gateway.listen_addr,
                    timings.connect_timeout,
                )?;
            let installation = ClientInstallationDescriptor {
                installation_id: registry
                    .installation_id
                    .clone()
                    .context("Gateway installation unavailable")?,
                display_name: "Pioneer Desktop".into(),
                client_kind: ClientKind::Desktop,
                platform: Some(std::env::consts::OS.into()),
                client_version: Some(env!("CARGO_PKG_VERSION").into()),
            };
            Ok(ClientEffectResult::GatewayEnvironmentLoaded {
                environment: OnboardingEnvironment {
                    remote_connect_timeout_min: std::time::Duration::from_secs(5),
                    default_remote_name: t!("gateway.endpoint.remote_name", index = "{index}")
                        .to_string(),
                    discard_unbound_remote_candidates: false,
                    registry,
                    binding_journals: vec![],
                    installation,
                    timings,
                    ws_timings: gateway_ws_timings_from_config(&config.desktop.gateway)?,
                    local_provisioned,
                    local_install_required,
                    local_update_required,
                },
            })
        }
        OnboardingPlatformEffect::PersistGatewayRegistry { registry } => {
            save_registry(&path, registry)?;
            Ok(ClientEffectResult::Completed)
        }
        OnboardingPlatformEffect::CreateLocalDeviceActivation { endpoint_id } => {
            anyhow::ensure!(
                endpoint_id == config.desktop.gateway.local_gateway_id.trim(),
                "local activation endpoint mismatch"
            );
            Ok(ClientEffectResult::LocalDeviceActivationCreated {
                activation: AuthSecretString::new(create_local_pending_device_session()?.as_str()),
            })
        }
        OnboardingPlatformEffect::PrepareLocalGateway { endpoint, recover } => {
            anyhow::ensure!(
                endpoint.id == config.desktop.gateway.local_gateway_id.trim(),
                "local preparation endpoint mismatch"
            );
            let service = &config.gateway.service_name;
            let address = &config.gateway.listen_addr;
            let mut warnings = super::runtime::discovery::ensure_managed_gateway_up_to_date(
                service, address, &timings, None,
            )?;
            let reachable = is_local_gateway_reachable(address, timings.connect_timeout)?;
            let service_active = is_configured_service_active(service)?;
            let readiness = if reachable {
                local_gateway_readiness(address, timings.connect_timeout)?
            } else {
                LocalGatewayReadiness::Unavailable
            };
            if readiness == LocalGatewayReadiness::IncompatibleService {
                bail!(
                    "{}",
                    t!(
                        "errors.gateway.address_conflict_inactive_service",
                        listen_addr = address.as_str(),
                        service_name = service.as_str()
                    )
                );
            }
            if !readiness
                .status()
                .is_some_and(|status| status.accepts_sessions())
            {
                if reachable || readiness.status().is_some() || service_active {
                    wait_for_gateway_service(address, &timings)?;
                } else {
                    warnings.extend(start_gateway_service(service, address, &timings, None)?);
                }
            }
            let provisioned = endpoint.session_ref.is_some()
                && endpoint.server_gateway_id.is_some()
                && storage.load(endpoint)?.is_some();
            let activation = if provisioned && !recover {
                None
            } else {
                Some(AuthSecretString::new(
                    create_local_pending_device_session()?.as_str(),
                ))
            };
            Ok(ClientEffectResult::LocalGatewayPrepared {
                prepared: LocalGatewayPreparation {
                    endpoint: endpoint.clone(),
                    activation,
                    warnings: warning_notification_messages(&warnings),
                },
            })
        }
    }
}

pub(crate) fn default_user_command_bin_dir_label() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        return r"%LOCALAPPDATA%\Pioneer\bin";
    }

    #[cfg(not(target_os = "windows"))]
    {
        "~/.local/bin"
    }
}

pub(crate) fn warning_notification_messages(
    warnings: &[super::control::GatewayInstallWarning],
) -> Vec<String> {
    warnings
        .iter()
        .filter_map(|warning| {
            let code = warning.code.trim();
            if code == "path_update_skipped" {
                return Some(
                    t!(
                        "gateway.notification.path_update_skipped",
                        bin_dir = default_user_command_bin_dir_label()
                    )
                    .to_string(),
                );
            }

            let message = warning.message.trim();
            if message.is_empty() {
                None
            } else {
                Some(message.to_owned())
            }
        })
        .collect()
}
