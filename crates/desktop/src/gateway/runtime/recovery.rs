use crate::gateway::connectivity::is_gateway_reachable;
use crate::gateway::control::{is_configured_service_active, start_gateway_service};
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
                if self.sync_local_auth_token_from_gateway_request(false)? {
                    save_registry(&self.registry_path, &self.registry)?;
                }
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
                let _ = self.sync_local_auth_token_from_gateway_request(true)?;
                save_registry(&self.registry_path, &self.registry)?;
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
            if self.sync_local_auth_token_from_gateway_request(false)? {
                save_registry(&self.registry_path, &self.registry)?;
            }
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

        if self.sync_local_auth_token_from_gateway_request(true)? {
            save_registry(&self.registry_path, &self.registry)?;
        }

        Ok(LocalGatewayStartOutcome {
            endpoint: self.local_gateway()?.clone(),
            warnings,
        })
    }
}
