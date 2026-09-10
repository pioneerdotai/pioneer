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
    pub use pioneer_protocol::{
        CLIAgentRuntimeKind, CLIRuntimeLoginStartParams, CLIRuntimeLoginStartResponse,
        CLIRuntimeLoginStartType, CLIRuntimeProxyDeleteParams, CLIRuntimeProxySetParams,
        GatewayCliRuntimeInstanceSettings, GatewayCliRuntimeSettings, GatewaySettingsSnapshot,
        ProviderConfigureParams, ProviderDeleteApiKeyParams, RuntimeAccountSnapshot,
        RuntimeCapabilities, RuntimeDiagnosticLevel, RuntimeStatus, RuntimeSummary,
    };
}

pub mod effects;

pub mod credentials;
