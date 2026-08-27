use crate::gateway::connectivity::{
    LocalGatewayReadiness, is_gateway_reachable, is_local_gateway_reachable,
    local_gateway_readiness,
};
use crate::gateway::control::{
    create_local_pending_device_session, is_configured_service_active, start_gateway_service,
    wait_for_gateway_service,
};
use crate::gateway::registry::save_registry;
use anyhow::{Result, bail};
use pioneer_client::gateway::runtime::{
    classify_local_gateway_state, normalize_local_service_active,
};
use pioneer_client::gateway::types::GatewayEndpointKind;
use tracing::info;

use super::discovery::ensure_managed_gateway_up_to_date;
use super::{
    ActiveGatewayState, GatewayRuntime, LocalGatewayRecovery, LocalGatewayStartOutcome,
    observe_startup_stage,
};

impl GatewayRuntime {
    pub fn active_gateway_state(&self) -> Result<ActiveGatewayState> {
        let Some(active) = self.active_gateway() else {
            return Ok(ActiveGatewayState::NotConfigured);
        };

        if active.kind == GatewayEndpointKind::Local {
            let tcp_reachable =
                is_gateway_reachable(&active.gateway_base_url, self.timings.connect_timeout)?;
            let readiness = if tcp_reachable {
                local_gateway_readiness(
                    self.config.gateway.listen_addr.as_str(),
                    self.timings.connect_timeout,
                )?
            } else {
                LocalGatewayReadiness::Unavailable
            };
            if readiness == LocalGatewayReadiness::IncompatibleService {
                return Ok(ActiveGatewayState::LocalAddressConflict);
            }
            let gateway_present = readiness.status().is_some();
            let service_active =
                is_configured_service_active(self.config.gateway.service_name.as_str())?;
            let service_active = normalize_local_service_active(gateway_present, service_active);
            return Ok(classify_local_gateway_state(
                readiness
                    .status()
                    .is_some_and(|status| status.accepts_sessions()),
                service_active,
            ));
        }

        let reachable =
            is_gateway_reachable(&active.gateway_base_url, self.timings.connect_timeout)?;
        Ok(if reachable {
            ActiveGatewayState::Connected
        } else {
            ActiveGatewayState::Unreachable
        })
    }

    pub fn try_recover_active_local_gateway_once(
        &mut self,
        trace: &pioneer_observability::DesktopStartupTrace,
    ) -> Result<LocalGatewayRecovery> {
        observe_startup_stage(
            trace,
            pioneer_observability::DesktopStartupStage::GatewayRuntimeLocalRecovery,
            || self.try_recover_active_local_gateway_once_inner(trace),
        )
    }

    fn try_recover_active_local_gateway_once_inner(
        &mut self,
        trace: &pioneer_observability::DesktopStartupTrace,
    ) -> Result<LocalGatewayRecovery> {
        let local_gateway_id = self.local_gateway_id().to_owned();

        if self.registry.active_gateway_id.as_deref() != Some(local_gateway_id.as_str()) {
            return Ok(LocalGatewayRecovery::NotNeeded);
        }

        let service_name = self.config.gateway.service_name.clone();
        let listen_addr = self.config.gateway.listen_addr.clone();
        let warnings = observe_startup_stage(
            trace,
            pioneer_observability::DesktopStartupStage::GatewayRuntimeVersionReconcile,
            || {
                ensure_managed_gateway_up_to_date(
                    service_name.as_str(),
                    listen_addr.as_str(),
                    &self.timings,
                )
            },
        )?;
        for warning in warnings {
            info!(
                warning_code = %warning.code,
                warning_message = %warning.message,
                "managed local gateway auto-update warning"
            );
        }
        let tcp_reachable = observe_startup_stage(
            trace,
            pioneer_observability::DesktopStartupStage::GatewayRuntimeReachabilityCheck,
            || is_local_gateway_reachable(listen_addr.as_str(), self.timings.connect_timeout),
        )?;
        let service_active = observe_startup_stage(
            trace,
            pioneer_observability::DesktopStartupStage::GatewayRuntimeServiceStatusCheck,
            || is_configured_service_active(service_name.as_str()),
        )?;
        let readiness = if tcp_reachable {
            local_gateway_readiness(listen_addr.as_str(), self.timings.connect_timeout)?
        } else {
            LocalGatewayReadiness::Unavailable
        };
        if readiness == LocalGatewayReadiness::IncompatibleService {
            bail!(
                "{}",
                t!(
                    "errors.gateway.address_conflict_inactive_service",
                    listen_addr = listen_addr.as_str(),
                    service_name = service_name.as_str()
                )
            );
        }
        let gateway_present = readiness.status().is_some();
        let service_active = normalize_local_service_active(gateway_present, service_active);
        let accepting_sessions = readiness
            .status()
            .is_some_and(|status| status.accepts_sessions());

        match classify_local_gateway_state(accepting_sessions, service_active) {
            ActiveGatewayState::Connected => {
                observe_startup_stage(
                    trace,
                    pioneer_observability::DesktopStartupStage::GatewayRuntimeSessionEnsure,
                    || self.ensure_local_gateway_session(),
                )?;
                Ok(LocalGatewayRecovery::AlreadyRunning)
            }
            ActiveGatewayState::LocalAddressConflict => bail!(
                "{}",
                t!(
                    "errors.gateway.address_conflict_inactive_service",
                    listen_addr = listen_addr.as_str(),
                    service_name = service_name.as_str()
                )
            ),
            ActiveGatewayState::Unreachable => {
                let warnings = if tcp_reachable || gateway_present || service_active {
                    observe_startup_stage(
                        trace,
                        pioneer_observability::DesktopStartupStage::GatewayRuntimeServiceStart,
                        || wait_for_gateway_service(listen_addr.as_str(), &self.timings),
                    )?;
                    Vec::new()
                } else {
                    observe_startup_stage(
                        trace,
                        pioneer_observability::DesktopStartupStage::GatewayRuntimeServiceStart,
                        || {
                            start_gateway_service(
                                service_name.as_str(),
                                listen_addr.as_str(),
                                &self.timings,
                            )
                        },
                    )?
                };
                for warning in warnings {
                    info!(
                        warning_code = %warning.code,
                        warning_message = %warning.message,
                        "local gateway start warning"
                    );
                }

                self.registry.active_gateway_id = Some(local_gateway_id);
                save_registry(&self.registry_path, &self.registry)?;
                observe_startup_stage(
                    trace,
                    pioneer_observability::DesktopStartupStage::GatewayRuntimeSessionEnsure,
                    || self.ensure_local_gateway_session(),
                )?;
                Ok(LocalGatewayRecovery::Started)
            }
            ActiveGatewayState::NotConfigured => Ok(LocalGatewayRecovery::NotNeeded),
        }
    }

    pub fn ensure_local_gateway_started(&mut self) -> Result<LocalGatewayStartOutcome> {
        let service_name = self.config.gateway.service_name.as_str();
        let listen_addr = self.config.gateway.listen_addr.as_str();
        let mut warnings =
            ensure_managed_gateway_up_to_date(service_name, listen_addr, &self.timings)?;

        let tcp_reachable = is_local_gateway_reachable(listen_addr, self.timings.connect_timeout)?;
        let service_active = is_configured_service_active(service_name)?;
        let readiness = if tcp_reachable {
            local_gateway_readiness(listen_addr, self.timings.connect_timeout)?
        } else {
            LocalGatewayReadiness::Unavailable
        };

        if readiness == LocalGatewayReadiness::IncompatibleService {
            bail!(
                "{}",
                t!(
                    "errors.gateway.address_conflict_inactive_service",
                    listen_addr = listen_addr,
                    service_name = service_name
                )
            );
        }

        if readiness
            .status()
            .is_some_and(|status| status.accepts_sessions())
        {
            self.ensure_local_gateway_session()?;
            return Ok(LocalGatewayStartOutcome {
                endpoint: self.local_gateway()?.clone(),
                warnings,
            });
        }

        if tcp_reachable || readiness.status().is_some() || service_active {
            wait_for_gateway_service(listen_addr, &self.timings)?;
        } else {
            warnings.extend(start_gateway_service(
                service_name,
                listen_addr,
                &self.timings,
            )?);
        }

        self.ensure_local_gateway_session()?;

        Ok(LocalGatewayStartOutcome {
            endpoint: self.local_gateway()?.clone(),
            warnings,
        })
    }

    fn ensure_local_gateway_session(&mut self) -> Result<()> {
        let endpoint = self.local_gateway()?.clone();
        if endpoint.session_ref.is_some()
            && endpoint.server_gateway_id.is_some()
            && let Some(session_ref) = endpoint.session_ref.as_deref()
            && self.secrets.get_gateway_session(session_ref)?.is_some()
        {
            return Ok(());
        }

        // Local creation is authorized by access to the Gateway host. The
        // one-time activation code remains only in this Zeroizing value and is
        // used immediately unless a crash-safe durable envelope is adopted. In
        // that recovery case its embedded GatewayId still proves that the
        // envelope belongs to the currently running local Gateway, while the
        // newly created pending session remains unused and expires normally.
        let activation_code = create_local_pending_device_session()?;
        if endpoint.session_ref.is_some() || endpoint.server_gateway_id.is_some() {
            self.clear_gateway_session_binding_durably(endpoint.id.as_str())?;
        }
        self.provision_gateway_session(endpoint.id.as_str(), activation_code.as_str())?;
        Ok(())
    }
}
