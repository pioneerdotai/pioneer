//! Provider catalog, settings, and selectors.

pub mod actions;
pub mod catalog;
pub mod cli_runtime_settings;
pub mod diagnostics;
pub mod list;
pub mod presentation;
pub mod selectors;

pub mod runtime;

pub mod store;

pub mod operations;

/// Protocol data used by typed provider consumers.
pub mod types {
    pub use pioneer_protocol::{CLIAgentRuntimeKind, GatewayCliRuntimeInstanceSettings, GatewayCliRuntimeSettings, GatewaySettingsSnapshot, RuntimeCapabilities, RuntimeDiagnosticLevel, RuntimeStatus, RuntimeSummary, RuntimeAccountSnapshot, CLIRuntimeLoginStartType, CLIRuntimeLoginStartResponse, ProviderConfigureParams, ProviderDeleteApiKeyParams, CLIRuntimeLoginStartParams, CLIRuntimeProxySetParams, CLIRuntimeProxyDeleteParams};
}

pub mod effects;

pub mod credentials;
