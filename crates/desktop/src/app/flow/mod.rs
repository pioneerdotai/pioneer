mod bootstrap;
mod client_effects;
mod desktop_update_apply;
mod desktop_update_check;
mod helpers;
mod lifecycle_gateway_ops;
mod lifecycle_operation;
mod lifecycle_setup;
mod notifications_view;
mod popover_view;
mod session_refresh;
mod task_user_notifications;
mod thread_list;
mod thread_start_execute;
mod thread_start_queue;
mod thread_start_scope;
mod turn_resume_execute;
mod turn_resume_queue;
mod turn_resume_schedule;
mod workspace_bootstrap;
mod workspace_switch;
mod ws_events_connection;
mod ws_events_notifications;
mod ws_events_pump;

use super::root::{
    DesktopUpdateUiState, GatewayConnectionState, GatewayOperationSource, GatewaySetupAction,
    GatewayStatusLevel, MainContentView, PioneerDesktop, TaskThreadNavigationEntry,
};
use crate::app::gateway_setup::GatewaySetupFormState;
use crate::gateway::{GatewayInstallWarning, GatewayRuntime};
use anyhow::anyhow;
use gpui_kit::component::{
    WindowExt,
    button::*,
    h_flex,
    notification::{Notification, NotificationType},
    popover::{Popover, PopoverState},
    separator::Separator,
    spinner::Spinner,
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

struct GatewayOperationSuccess {
    runtime: GatewayRuntime,
    ws_connection_id: Option<u64>,
    ws_connected_ready: bool,
    install_warnings: Vec<GatewayInstallWarning>,
}

struct GatewayInstallWarningNotification;

#[cfg(test)]
mod tests;
