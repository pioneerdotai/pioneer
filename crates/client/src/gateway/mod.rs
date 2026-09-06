//! Gateway endpoint, profile, and lifecycle client logic.

pub mod connectivity;
pub mod device_activation;
pub mod endpoint;
pub mod event_router;
pub mod identity_authorization;
pub mod invitation;
pub mod migration;
pub mod registry;
pub mod runtime;
pub mod session_connection;
pub mod session_controller;
pub mod session_envelope;
pub mod session_lifecycle;
pub mod session_refresh;
pub mod settings_store;
pub mod setup;
pub mod timings;
pub mod types;
