//! Gateway endpoint, profile, and lifecycle client logic.

pub mod connectivity;
pub mod device_activation;
pub mod endpoint;
pub mod event_router;
pub mod identity_authorization;
pub mod invitation;
pub mod invitation_commits;
pub mod invitation_controller;
pub mod invitation_persistence;
pub mod migration;
pub mod onboarding_effects;
mod onboarding_invitation;
pub mod onboarding_runtime;
pub mod provisioning;
pub mod registry;
pub mod registry_recovery;
pub mod runtime;
pub mod session_connection;
pub mod session_controller;
pub mod session_envelope;
pub mod session_lifecycle;
pub mod session_refresh;
pub mod settings_store;
pub mod setup;
pub mod setup_controller;
pub mod timings;
pub mod types;

pub mod session_driver;
