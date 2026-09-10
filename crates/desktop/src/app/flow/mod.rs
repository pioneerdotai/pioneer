mod bootstrap;
mod client_effects;
mod helpers;
mod lifecycle_setup;
mod session_refresh;
mod thread_list;
mod thread_start_execute;
mod thread_start_queue;
mod thread_start_scope;
mod turn_resume_execute;
mod turn_resume_queue;
mod workspace_bootstrap;
mod workspace_switch;
mod ws_events_connection;
mod ws_events_notifications;
mod ws_events_pump;

use super::root::{
    GatewayConnectionState, GatewayStatusLevel, MainContentView, PioneerDesktop,
    TaskThreadNavigationEntry,
};
use anyhow::anyhow;
use gpui_kit::component::{
    WindowExt,
    button::*,
    h_flex,
    notification::{Notification, NotificationType},
    popover::{Popover, PopoverState},
    separator::Separator,
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::gateway::types::{GatewayEndpoint, GatewayEndpointKind};
use pioneer_client::transport::ws::GatewayWsConnectSpec;
use pioneer_protocol::GatewayNotification;
use std::time::Duration;
use tracing::warn;

use client_effects::*;
use helpers::*;
pub(crate) use workspace_bootstrap::*;

#[cfg(test)]
mod tests;

pub(in crate::app) use ws_events_connection::gateway_status_message_text;
