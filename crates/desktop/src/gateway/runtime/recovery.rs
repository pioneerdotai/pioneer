use crate::gateway::connectivity::is_gateway_reachable;
use crate::gateway::control::{
    create_local_pending_device_session, is_configured_service_active, start_gateway_service,
};
use crate::gateway::registry::save_registry;
use anyhow::{Result, bail};
use pioneer_client::gateway::runtime::{
    classify_local_gateway_state, normalize_local_service_active,
};
use pioneer_client::gateway::types::GatewayEndpointKind;
use tracing::info;

use super::discovery::ensure_managed_gateway_up_to_date;
use super::{ActiveGatewayState, GatewayRuntime, LocalGatewayRecovery, LocalGatewayStartOutcome};

impl GatewayRuntime {
    pub fn active_gateway_state(&self) -> Result<ActiveGatewayState> {
        let Some(active) = self.active_gateway() else {
            return Ok(ActiveGatewayState::NotConfigured);
        };

        let reachable =
            is_gateway_reachable(active.address.as_str(), self.timings.connect_timeout)?;

        if active.kind == GatewayEndpointKind::Local {
            let service_active =
                is_configured_service_active(self.config.gateway.service_name.as_str())?;
            let service_active = normalize_local_service_active(reachable, service_active);
            return Ok(classify_local_gateway_state(reachable, service_active));
        }

        Ok(if reachable {
            ActiveGatewayState::Connected
        } else {
            ActiveGatewayState::Unreachable
        })
    }

    pub fn try_recover_active_local_gateway_once(&mut self) -> Result<LocalGatewayRecovery> {
        let local_gateway_id = self.local_gateway_id();

        if self.registry.active_gateway_id.as_deref() != Some(local_gateway_id) {
            return Ok(LocalGatewayRecovery::NotNeeded);
        }

        let service_name = self.config.gateway.service_name.as_str();
        let listen_addr = self.config.gateway.listen_addr.as_str();
        let warnings = ensure_managed_gateway_up_to_date(service_name, listen_addr, &self.timings)?;
        for warning in warnings {
            info!(
                warning_code = %warning.code,
                warning_message = %warning.message,
                "managed local gateway auto-update warning"
            );
        }
        let reachable = is_gateway_reachable(listen_addr, self.timings.connect_timeout)?;
        let service_active = is_configured_service_active(service_name)?;
        let service_active = normalize_local_service_active(reachable, service_active);

        match classify_local_gateway_state(reachable, service_active) {
            ActiveGatewayState::Connected => {
                self.ensure_local_gateway_session()?;
                Ok(LocalGatewayRecovery::AlreadyRunning)
            }
            ActiveGatewayState::LocalAddressConflict => bail!(
                "{}",
                t!(
                    "errors.gateway.address_conflict_inactive_service",
                    listen_addr = listen_addr,
                    service_name = service_name
                )
            ),
            ActiveGatewayState::Unreachable => {
                let warnings = start_gateway_service(service_name, listen_addr, &self.timings)?;
                for warning in warnings {
                    info!(
                        warning_code = %warning.code,
                        warning_message = %warning.message,
                        "local gateway start warning"
                    );
                }

                self.registry.active_gateway_id = Some(local_gateway_id.to_owned());
                if !self.registry_upgrade_pending() {
                    save_registry(&self.registry_path, &self.registry)?;
                }
                self.ensure_local_gateway_session()?;
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

        let reachable = is_gateway_reachable(listen_addr, self.timings.connect_timeout)?;
        let service_active = is_configured_service_active(service_name)?;
        let service_active = normalize_local_service_active(reachable, service_active);

        if reachable && service_active {
            self.ensure_local_gateway_session()?;
            return Ok(LocalGatewayStartOutcome {
                endpoint: self.local_gateway()?.clone(),
                warnings,
            });
        }

        if reachable && !service_active {
            bail!(
                "{}",
                t!(
                    "errors.gateway.address_conflict_inactive_service",
                    listen_addr = listen_addr,
                    service_name = service_name
                )
            );
        }

        warnings.extend(start_gateway_service(
            service_name,
            listen_addr,
            &self.timings,
        )?);

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
