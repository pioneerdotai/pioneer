use super::{
    build_remote_candidate_ws_connect_spec, build_ws_connect_spec,
    default_user_command_bin_dir_label, warning_notification_messages,
};
use crate::gateway::{GatewayInstallWarning, GatewayRuntime};
use pioneer_client::gateway::types::{GatewayEndpoint, GatewayEndpointKind};
use std::time::Duration;

#[test]
fn ws_connect_spec_uses_resolved_in_memory_auth_token() {
    let runtime = GatewayRuntime::for_ws_spec_tests();
    runtime
        .store_gateway_auth_token_for_tests("remote-123", "resolved-token")
        .expect("store test token");
    let endpoint = GatewayEndpoint {
        id: "remote-123".to_owned(),
        name: "Remote".to_owned(),
        address: "127.0.0.1:22000".to_owned(),
        kind: GatewayEndpointKind::Remote,
        auth_token_ref: Some("remote-123".to_owned()),
        workspace_id: None,
        service_name: None,
    };

    let spec = build_ws_connect_spec(&runtime, &endpoint).expect("build ws spec");

    assert_eq!(spec.auth_token.as_deref(), Some("resolved-token"));
}

#[test]
fn remote_ws_connect_spec_uses_remote_timeout_floor() {
    let runtime = GatewayRuntime::for_ws_spec_tests();
    let endpoint = GatewayEndpoint {
        id: "remote-123".to_owned(),
        name: "Remote".to_owned(),
        address: "127.0.0.1:22000".to_owned(),
        kind: GatewayEndpointKind::Remote,
        auth_token_ref: None,
        workspace_id: None,
        service_name: None,
    };

    let spec = build_ws_connect_spec(&runtime, &endpoint).expect("build remote ws spec");
    let candidate =
        build_remote_candidate_ws_connect_spec(&runtime, "Remote", "127.0.0.1:22000", "");

    assert_eq!(spec.timings.connect_timeout, Duration::from_millis(5_000));
    assert_eq!(
        candidate.timings.connect_timeout,
        Duration::from_millis(5_000)
    );
}

#[test]
fn local_ws_connect_spec_keeps_configured_timeout() {
    let runtime = GatewayRuntime::for_ws_spec_tests();
    let endpoint = GatewayEndpoint {
        id: "local".to_owned(),
        name: "Local".to_owned(),
        address: "0.0.0.0:17878".to_owned(),
        kind: GatewayEndpointKind::Local,
        auth_token_ref: None,
        workspace_id: None,
        service_name: Some("com.pioneer.gateway".to_owned()),
    };

    let spec = build_ws_connect_spec(&runtime, &endpoint).expect("build local ws spec");

    assert_eq!(spec.timings.connect_timeout, Duration::from_millis(300));
}

#[test]
fn warning_notification_uses_friendly_path_message_for_path_update_warning() {
    let warnings = vec![GatewayInstallWarning {
        code: "path_update_skipped".to_owned(),
        message: "failed to update profile".to_owned(),
    }];

    let messages = warning_notification_messages(&warnings);
    assert_eq!(messages.len(), 1);
    assert!(messages[0].contains(default_user_command_bin_dir_label()));
    assert!(!messages[0].contains("failed to update profile"));
}

#[test]
fn warning_notification_keeps_one_message_per_warning() {
    let warnings = vec![
        GatewayInstallWarning {
            code: "path_update_skipped".to_owned(),
            message: "first".to_owned(),
        },
        GatewayInstallWarning {
            code: "path_update_skipped".to_owned(),
            message: "second".to_owned(),
        },
        GatewayInstallWarning {
            code: "other_warning".to_owned(),
            message: "third".to_owned(),
        },
    ];

    let messages = warning_notification_messages(&warnings);
    assert_eq!(messages.len(), 3);
    assert!(messages[0].contains(default_user_command_bin_dir_label()));
    assert!(messages[1].contains(default_user_command_bin_dir_label()));
    assert_eq!(messages[2], "third");
}
