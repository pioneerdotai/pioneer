mod bootstrap;
mod client_effects;
mod helpers;
mod lifecycle_gateway_ops;
mod lifecycle_operation;
mod lifecycle_setup;
mod notifications_view;
mod popover_view;
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
    GatewayConnectionState, GatewayOperationSource, GatewaySetupAction, GatewayStatusLevel,
    MainContentView, PioneerDesktop,
};
use crate::app::gateway_setup::GatewaySetupFormState;
use crate::gateway::{GatewayInstallWarning, GatewayRuntime, GatewayWsClient};
use anyhow::anyhow;
use gpui::{prelude::*, *};
use gpui_component::{
    WindowExt,
    button::*,
    divider::Divider,
    h_flex,
    notification::{Notification, NotificationType},
    popover::{Popover, PopoverState},
    spinner::Spinner,
    theme::ActiveTheme,
    *,
};
use pioneer_client::gateway::types::{GatewayEndpoint, GatewayEndpointKind};
use pioneer_client::transport::ws::{GatewayWsConnectSpec, GatewayWsEvent};
use pioneer_protocol::generate_id;
use pioneer_protocol::{
    GatewayNotification, ThreadHistoryParams, ThreadHistoryResponse, ThreadTreeParams,
    TurnTimelineParams, TurnTimelineResponse, Workspace, WorkspaceChangedNotification,
    WorkspaceSelectParams,
};
use std::time::Duration;
use tracing::warn;

use client_effects::*;
use helpers::*;
pub(crate) use workspace_bootstrap::*;

#[cfg(test)]
use ws_events_notifications::{
    apply_workspace_changed_to_catalog, should_accept_thread_started_as_local_pending,
    should_refresh_workspace_bound_data,
};

const REMOTE_WS_CONNECT_TIMEOUT_MIN_MS: u64 = 5_000;
const ID_LEN: usize = pioneer_client::threads::start::THREAD_START_ID_LEN;

struct GatewayOperationSuccess {
    runtime: GatewayRuntime,
    ws_connection_id: Option<u64>,
    ws_connected_ready: bool,
    install_warnings: Vec<GatewayInstallWarning>,
}

struct ThreadStartBootstrapOutcome {
    workspace_id: String,
    response: pioneer_protocol::ThreadStartResponse,
}

struct ThreadStartBootstrapFailure {
    error: anyhow::Error,
}

struct GatewayInstallWarningNotification;

#[cfg(test)]
mod tests;
