//! Workspace state and actions.

pub mod actions;
pub mod bootstrap;
pub mod selectors;

pub mod catalog;
pub mod directory;
pub mod projection;

pub mod commands;

pub(crate) mod controller;

pub mod intents;
pub use pioneer_protocol::{
    Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, Workspace,
};
