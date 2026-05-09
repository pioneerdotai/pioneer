mod bootstrap;
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
mod ws_events_connection;
mod ws_events_notifications;
mod ws_events_pump;

use super::root::{
    GatewayConnectionState, GatewayOperationSource, GatewaySetupAction, GatewayStatusLevel,
    MainContentView, PioneerDesktop,
};
use crate::app::gateway_setup::GatewaySetupFormState;
use crate::gateway::{
    ActiveGatewayState, GatewayEndpoint, GatewayEndpointKind, GatewayInstallWarning,
    GatewayRuntime, GatewayWsClient, GatewayWsConnectSpec, GatewayWsEvent,
};
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
use pioneer_protocol::generate_id;
use pioneer_protocol::{
    GatewayNotification, ThreadHistoryParams, ThreadHistoryResponse, ThreadStartParams,
    ThreadTreeParams, TurnGetParams, TurnItemEventPayload, TurnItemsParams, TurnTimelineParams,
    TurnTimelineResponse,
};
use std::time::Duration;
use tracing::warn;

use helpers::*;
use workspace_bootstrap::*;

#[cfg(test)]
use ws_events_notifications::should_refresh_workspace_bound_data;

const THREAD_START_RETRY_INITIAL_DELAY_MS: u64 = 500;
const THREAD_START_RETRY_MAX_DELAY_MS: u64 = 5_000;
const TURN_RESUME_RETRY_INITIAL_DELAY_MS: u64 = 800;
const TURN_RESUME_RETRY_MAX_DELAY_MS: u64 = 5_000;
const REMOTE_WS_CONNECT_TIMEOUT_MIN_MS: u64 = 5_000;
const ID_LEN: usize = 21;
const WORKSPACE_START_SCOPE_BOOTSTRAP: &str = "__bootstrap__";

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
