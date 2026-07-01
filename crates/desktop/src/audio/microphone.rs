use std::fmt;

/// Desktop microphone preflight state before any voice session is opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DesktopMicrophoneGateState {
    Unknown,
    Granted,
    DeniedRetryable,
    DeniedBlocked,
    NoDevice,
    DeviceBusy,
    UnsupportedFormat,
}

impl DesktopMicrophoneGateState {
    pub(crate) fn can_open_gateway_voice_session(self) -> bool {
        matches!(self, Self::Granted)
    }
}

impl fmt::Display for DesktopMicrophoneGateState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Unknown => "unknown",
            Self::Granted => "granted",
            Self::DeniedRetryable => "denied_retryable",
            Self::DeniedBlocked => "denied_blocked",
            Self::NoDevice => "no_device",
            Self::DeviceBusy => "device_busy",
            Self::UnsupportedFormat => "unsupported_format",
        };
        f.write_str(label)
    }
}

/// How the desktop layer expects platform microphone permission to be requested.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DesktopMicrophonePermissionRequestStrategy {
    /// macOS prompts on first attempt to open an input stream from a signed app.
    OpenInputStream,
    /// Windows/Linux usually expose failures through device enumeration/open errors.
    DeviceProbe,
}

impl DesktopMicrophonePermissionRequestStrategy {
    pub(crate) fn current_platform() -> Self {
        if cfg!(target_os = "macos") {
            Self::OpenInputStream
        } else {
            Self::DeviceProbe
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DesktopMicrophoneFormatRequest {
    pub(crate) sample_rate_hz: u32,
    pub(crate) channels: u16,
}

impl Default for DesktopMicrophoneFormatRequest {
    fn default() -> Self {
        Self {
            sample_rate_hz: 16_000,
            channels: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DesktopMicrophoneGateReport {
    pub(crate) state: DesktopMicrophoneGateState,
    pub(crate) strategy: DesktopMicrophonePermissionRequestStrategy,
    pub(crate) device_name: Option<String>,
    pub(crate) message: Option<String>,
}

impl DesktopMicrophoneGateReport {
    pub(crate) fn unknown() -> Self {
        Self {
            state: DesktopMicrophoneGateState::Unknown,
            strategy: DesktopMicrophonePermissionRequestStrategy::current_platform(),
            device_name: None,
            message: None,
        }
    }

    pub(crate) fn can_open_gateway_voice_session(&self) -> bool {
        self.state.can_open_gateway_voice_session()
    }

    pub(crate) fn composer_error_message(&self) -> Option<String> {
        if self.can_open_gateway_voice_session()
            || matches!(self.state, DesktopMicrophoneGateState::Unknown)
        {
            return None;
        }

        let message = match self.state {
            DesktopMicrophoneGateState::DeniedRetryable => {
                t!("chat.composer.voice.microphone_permission_retry").to_string()
            }
            DesktopMicrophoneGateState::DeniedBlocked => {
                t!("chat.composer.voice.microphone_permission_blocked").to_string()
            }
            DesktopMicrophoneGateState::NoDevice => {
                t!("chat.composer.voice.microphone_no_device").to_string()
            }
            DesktopMicrophoneGateState::DeviceBusy => {
                t!("chat.composer.voice.microphone_busy").to_string()
            }
            DesktopMicrophoneGateState::UnsupportedFormat => {
                t!("chat.composer.voice.microphone_unsupported_format").to_string()
            }
            DesktopMicrophoneGateState::Unknown | DesktopMicrophoneGateState::Granted => {
                return None;
            }
        };

        Some(message)
    }
}

impl Default for DesktopMicrophoneGateReport {
    fn default() -> Self {
        Self::unknown()
    }
}

pub(crate) trait DesktopMicrophoneDeviceProbe {
    fn probe(&self, format: DesktopMicrophoneFormatRequest) -> DesktopMicrophoneGateReport;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PlatformDesktopMicrophoneDeviceProbe;

impl DesktopMicrophoneDeviceProbe for PlatformDesktopMicrophoneDeviceProbe {
    fn probe(&self, format: DesktopMicrophoneFormatRequest) -> DesktopMicrophoneGateReport {
        if format.sample_rate_hz == 0 || format.channels == 0 {
            return DesktopMicrophoneGateReport {
                state: DesktopMicrophoneGateState::UnsupportedFormat,
                strategy: DesktopMicrophonePermissionRequestStrategy::current_platform(),
                device_name: None,
                message: Some("Microphone format is unsupported.".to_owned()),
            };
        }

        platform_microphone_gate_report(format)
    }
}

pub(crate) fn verify_desktop_microphone_ready(
    probe: &dyn DesktopMicrophoneDeviceProbe,
    format: DesktopMicrophoneFormatRequest,
) -> DesktopMicrophoneGateReport {
    probe.probe(format)
}

fn platform_microphone_gate_report(
    _format: DesktopMicrophoneFormatRequest,
) -> DesktopMicrophoneGateReport {
    use cpal::traits::{DeviceTrait, HostTrait};

    let strategy = DesktopMicrophonePermissionRequestStrategy::current_platform();
    let host = cpal::default_host();
    let Some(device) = host.default_input_device() else {
        return DesktopMicrophoneGateReport {
            state: DesktopMicrophoneGateState::NoDevice,
            strategy,
            device_name: None,
            message: Some("No input microphone is available.".to_owned()),
        };
    };
    let device_name = Some(device.to_string());
    match device.default_input_config() {
        Ok(config) if config.channels() > 0 && config.sample_rate() > 0 => {}
        Ok(_) => {
            return DesktopMicrophoneGateReport {
                state: DesktopMicrophoneGateState::UnsupportedFormat,
                strategy,
                device_name,
                message: Some("Default microphone input configuration is invalid.".to_owned()),
            };
        }
        Err(error) => {
            return DesktopMicrophoneGateReport {
                state: gate_state_for_cpal_error(&error),
                strategy,
                device_name,
                message: Some(format!("Microphone device probe failed: {error}")),
            };
        }
    }

    DesktopMicrophoneGateReport {
        state: DesktopMicrophoneGateState::Granted,
        strategy,
        device_name,
        message: None,
    }
}

fn gate_state_for_cpal_error(error: &cpal::Error) -> DesktopMicrophoneGateState {
    match error.kind() {
        cpal::ErrorKind::DeviceBusy => DesktopMicrophoneGateState::DeviceBusy,
        cpal::ErrorKind::DeviceNotAvailable | cpal::ErrorKind::HostUnavailable => {
            DesktopMicrophoneGateState::NoDevice
        }
        cpal::ErrorKind::PermissionDenied => DesktopMicrophoneGateState::DeniedBlocked,
        cpal::ErrorKind::UnsupportedConfig | cpal::ErrorKind::UnsupportedOperation => {
            DesktopMicrophoneGateState::UnsupportedFormat
        }
        _ => DesktopMicrophoneGateState::DeniedRetryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticProbe(DesktopMicrophoneGateReport);

    impl DesktopMicrophoneDeviceProbe for StaticProbe {
        fn probe(&self, _format: DesktopMicrophoneFormatRequest) -> DesktopMicrophoneGateReport {
            self.0.clone()
        }
    }

    #[test]
    fn granted_gate_allows_gateway_voice_session() {
        let report = verify_desktop_microphone_ready(
            &StaticProbe(DesktopMicrophoneGateReport {
                state: DesktopMicrophoneGateState::Granted,
                strategy: DesktopMicrophonePermissionRequestStrategy::DeviceProbe,
                device_name: Some("Built-in Microphone".to_owned()),
                message: None,
            }),
            DesktopMicrophoneFormatRequest::default(),
        );

        assert!(report.can_open_gateway_voice_session());
        assert_eq!(report.composer_error_message(), None);
    }

    #[test]
    fn failure_gate_blocks_gateway_voice_session_with_message() {
        let report = verify_desktop_microphone_ready(
            &StaticProbe(DesktopMicrophoneGateReport {
                state: DesktopMicrophoneGateState::DeniedBlocked,
                strategy: DesktopMicrophonePermissionRequestStrategy::DeviceProbe,
                device_name: None,
                message: Some("Microphone access is blocked.".to_owned()),
            }),
            DesktopMicrophoneFormatRequest::default(),
        );

        assert!(!report.can_open_gateway_voice_session());
        assert_eq!(
            report.composer_error_message(),
            Some(t!("chat.composer.voice.microphone_permission_blocked").to_string())
        );
    }
}
