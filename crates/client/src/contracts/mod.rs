//! Client DTO contracts intended for shell boundaries.
//!
//! These types describe the outer client contract a shell may consume. They are
//! kept separate from reducer internals so schema export stays limited to
//! explicit shell-facing DTOs.

use crate::{notifications::effects::ClientEffect, state::snapshot::ClientSnapshot};
use pioneer_protocol::GatewayNotification;

pub mod export;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum ClientEvent {
    SnapshotChanged(ClientSnapshot),
    GatewayNotification(GatewayNotification),
    EffectsPlanned(Vec<ClientEffect>),
    Error(ClientErrorEvent),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientErrorEvent {
    pub message: String,
    pub code: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ClientCommand {
    RefreshWorkspaceList,
    RefreshGatewaySettings,
    RefreshProviders,
    RefreshSkills,
    RefreshMcp,
    RefreshThreadArtifacts { thread_id: String },
    RefreshTurnTimeline { thread_id: String, turn_id: String },
}
