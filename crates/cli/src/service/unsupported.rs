use anyhow::{Result, bail};

use super::{GatewayServiceStartReport, GatewayServiceWarning, ServiceSettings};

pub fn start_gateway_service(
    _settings: &ServiceSettings,
    _report: &mut GatewayServiceStartReport,
) -> Result<Vec<GatewayServiceWarning>> {
    bail!("`pioneer start` is not supported on this operating system");
}

pub fn stop_gateway_service(_settings: &ServiceSettings) -> Result<()> {
    bail!("`pioneer stop` is not supported on this operating system");
}

pub fn is_gateway_service_active(_settings: &ServiceSettings) -> Result<bool> {
    Ok(false)
}

pub(super) struct RegistrationSnapshot;
impl RegistrationSnapshot {
    pub(super) fn is_registered(&self) -> bool {
        false
    }
}
pub(super) fn capture_registration(_settings: &ServiceSettings) -> Result<RegistrationSnapshot> {
    Ok(RegistrationSnapshot)
}
pub(super) fn restore_registration(
    _settings: &ServiceSettings,
    _snapshot: &RegistrationSnapshot,
) -> Result<()> {
    Ok(())
}
