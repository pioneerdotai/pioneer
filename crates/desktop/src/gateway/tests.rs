use super::ActiveGatewayState;
use super::connectivity::normalize_address;
use super::registry::{default_registry, load_registry, normalize_registry, setup_required};
use super::runtime::{classify_local_gateway_state, is_same_gateway_version};
use super::types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry};
use pioneer_config::{
    AppConfig, DesktopConfig, GatewayAuthConfig, GatewayComputerUseToolsConfig, GatewayConfig,
    GatewayDatabaseConfig, GatewayProviderConfig, GatewayRuntimeConfig, GatewaySkillsConfig,
    GatewayThreadConfig, GatewayToolLoopBudgetConfig, GatewayToolRetryBudgetConfig,
    GatewayToolsConfig, GatewayWebToolsConfig, InstallConfig,
};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn normalize_address_rejects_empty_and_invalid_values() {
    assert!(normalize_address(" ").is_err());
    assert!(normalize_address("localhost").is_err());
}

#[test]
fn normalize_address_trims_value() {
    let value = normalize_address(" 0.0.0.0:17878 ").expect("address should be valid");
    assert_eq!(value, "0.0.0.0:17878");
}

#[test]
fn normalize_registry_enforces_local_and_deduplicates_remotes() {
    let config = test_config();
    let mut registry = GatewayRegistry {
        version: config.desktop.gateway.registry_version,
        active_gateway_id: Some("unknown-id".to_owned()),
        local: GatewayEndpoint {
            id: "bad-local".to_owned(),
            name: "Old Local".to_owned(),
            address: "127.0.0.1:9999".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token: None,
            workspace_id: None,
            service_name: None,
        },
        remotes: vec![
            GatewayEndpoint {
                id: "".to_owned(),
                name: " ".to_owned(),
                address: "127.0.0.1:22000".to_owned(),
                kind: GatewayEndpointKind::Local,
                auth_token: Some("  secret  ".to_owned()),
                workspace_id: Some("  ws_remote_a  ".to_owned()),
                service_name: Some("should-be-cleared".to_owned()),
            },
            GatewayEndpoint {
                id: "duplicate-id".to_owned(),
                name: "Remote Duplicate".to_owned(),
                address: "127.0.0.1:22000".to_owned(),
                kind: GatewayEndpointKind::Remote,
                auth_token: None,
                workspace_id: None,
                service_name: None,
            },
            GatewayEndpoint {
                id: "duplicate-id".to_owned(),
                name: "Unique".to_owned(),
                address: "127.0.0.1:22001".to_owned(),
                kind: GatewayEndpointKind::Remote,
                auth_token: None,
                workspace_id: Some("   ".to_owned()),
                service_name: None,
            },
        ],
    };

    normalize_registry(&mut registry, &config);

    assert_eq!(registry.local.id, config.desktop.gateway.local_gateway_id);
    assert_eq!(registry.local.kind, GatewayEndpointKind::Local);
    assert_eq!(registry.local.address, "0.0.0.0:17878");
    assert_eq!(registry.remotes.len(), 2);
    assert!(
        registry
            .remotes
            .iter()
            .all(|endpoint| endpoint.kind == GatewayEndpointKind::Remote)
    );
    assert!(
        registry
            .remotes
            .iter()
            .all(|endpoint| endpoint.service_name.is_none())
    );
    assert_eq!(
        registry.remotes[0].workspace_id.as_deref(),
        Some("ws_remote_a")
    );
    assert!(registry.remotes[1].workspace_id.is_none());
    assert!(registry.active_gateway_id.is_none());
}

#[test]
fn normalize_registry_preserves_local_auth_token() {
    let config = test_config();
    let mut registry = default_registry(&config);
    registry.local.auth_token = Some("  local-superuser-jwt  ".to_owned());
    registry.local.workspace_id = Some("  ws_local_001  ".to_owned());

    normalize_registry(&mut registry, &config);

    assert_eq!(
        registry.local.auth_token.as_deref(),
        Some("local-superuser-jwt")
    );
    assert_eq!(registry.local.workspace_id.as_deref(), Some("ws_local_001"));
}

#[test]
fn startup_flow_is_required_only_without_active_gateway() {
    let config = test_config();
    let mut registry = default_registry(&config);

    assert!(setup_required(&registry));

    registry.active_gateway_id = Some(config.desktop.gateway.local_gateway_id.clone());
    assert!(!setup_required(&registry));
}

#[test]
fn classify_local_gateway_state_handles_running_stopped_and_conflict() {
    assert_eq!(
        classify_local_gateway_state(true, true),
        ActiveGatewayState::Connected
    );
    assert_eq!(
        classify_local_gateway_state(false, true),
        ActiveGatewayState::Unreachable
    );
    assert_eq!(
        classify_local_gateway_state(false, false),
        ActiveGatewayState::Unreachable
    );
    assert_eq!(
        classify_local_gateway_state(true, false),
        ActiveGatewayState::LocalAddressConflict
    );
}

#[test]
fn gateway_version_comparison_requires_full_version_match() {
    assert!(is_same_gateway_version("0.1.0", "0.1.0"));
    assert!(is_same_gateway_version("v1.2.3", "1.2.3"));
    assert!(!is_same_gateway_version("1.2.3", "1.2.4"));
    assert!(!is_same_gateway_version("1.2.3-beta.1", "1.2.3"));
    assert!(!is_same_gateway_version("nightly", "1.0.0"));
}

#[test]
fn load_registry_creates_file_and_persists_default_state() {
    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");

    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    let registry = load_registry(&registry_path, &config).expect("failed to load registry");

    assert!(registry_path.exists());
    assert_eq!(registry.version, config.desktop.gateway.registry_version);
    assert_eq!(registry.local.id, config.desktop.gateway.local_gateway_id);
    assert!(registry.active_gateway_id.is_none());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn load_registry_rejects_unsupported_registry_version() {
    rust_i18n::set_locale("en");

    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");

    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    fs::write(
        &registry_path,
        r#"
version = 99
active_gateway_id = "local"

[local]
id = "local"
name = "Local Gateway"
address = "0.0.0.0:17878"
kind = "local"
service_name = "com.pioneer.gateway"
"#,
    )
    .expect("failed to write unsupported-version registry");

    let error = load_registry(&registry_path, &config)
        .expect_err("unsupported registry version should be rejected");
    assert!(
        format!("{error:#}").contains("unsupported gateway registry version"),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn load_registry_rejects_non_current_registry_version() {
    rust_i18n::set_locale("en");

    let config = test_config();
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).expect("failed to create test temp dir");

    let registry_path = temp_dir.join(config.desktop.gateway.registry_file_name.as_str());
    fs::write(
        &registry_path,
        r#"
version = 2
active_gateway_id = "local"

[local]
id = "local"
name = "Local Gateway"
address = "0.0.0.0:17878"
kind = "local"
service_name = "com.pioneer.gateway"
"#,
    )
    .expect("failed to write migratable registry");

    let error = load_registry(&registry_path, &config)
        .expect_err("non-current registry version should be rejected");
    assert!(
        format!("{error:#}").contains("unsupported gateway registry version"),
        "unexpected error: {error:#}"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

fn test_config() -> AppConfig {
    AppConfig {
        home_directory: ".pioneer.test".to_owned(),
        install_state_file_name: "install-state.toml".to_owned(),
        install: InstallConfig {
            unix_root_directory_name: "pioneer-test".to_owned(),
            macos_root_directory_name: "PioneerTest".to_owned(),
            windows_root_directory_name: "PioneerTest".to_owned(),
            managed_directory_name: "managed-test".to_owned(),
            binary_name: "pioneer-test".to_owned(),
            command_name: "pioneer-test".to_owned(),
            macos_background_item_name: "Pioneer Test".to_owned(),
            macos_associated_bundle_identifier: "ai.pioneer.test".to_owned(),
        },
        gateway: GatewayConfig {
            settings_version: 1,
            settings_file_name: "gateway-settings.toml".to_owned(),
            service_name: "com.pioneer.gateway".to_owned(),
            legacy_service_names: Vec::new(),
            listen_addr: "0.0.0.0:17878".to_owned(),
            outbound_queue_capacity: 128,
            thread: GatewayThreadConfig {
                default_model: "gpt-5.4".to_owned(),
                default_model_provider: "openai".to_owned(),
                summary_model: None,
                summary_model_provider: None,
                title_model: None,
                title_model_provider: None,
                max_context_tokens: 128_000,
                response_reserve_tokens: 16_000,
            },
            tools: GatewayToolsConfig {
                web: GatewayWebToolsConfig::default(),
                computer_use: GatewayComputerUseToolsConfig::default(),
                budget: GatewayToolLoopBudgetConfig::default(),
                retry: GatewayToolRetryBudgetConfig::default(),
            },
            skills: GatewaySkillsConfig::default(),
            provider: GatewayProviderConfig {
                default_timeout_secs: 120,
                attachments: Default::default(),
            },
            database: GatewayDatabaseConfig {
                file_name: "gateway.db".to_owned(),
                max_connections: 10,
                connect_timeout_ms: 5_000,
                acquire_timeout_ms: 5_000,
                idle_timeout_ms: 30_000,
                sqlx_logging: false,
                run_migrations_on_startup: true,
            },
            auth: GatewayAuthConfig {
                jwt_issuer: "pioneer".to_owned(),
                jwt_audience: "pioneer-clients".to_owned(),
                superuser_subject: "superuser".to_owned(),
                superuser_role: "superuser".to_owned(),
                secret_size_bytes: 64,
                token_ttl_seconds: 31536000,
                token_refresh_leeway_seconds: 86400,
            },
        },
        desktop: DesktopConfig {
            gateway: GatewayRuntimeConfig {
                connect_timeout_ms: 300,
                startup_timeout_ms: 10_000,
                poll_interval_ms: 200,
                ws_ping_interval_ms: 10_000,
                ws_pong_timeout_ms: 30_000,
                ws_reconnect_initial_ms: 500,
                ws_reconnect_max_ms: 10_000,
                ws_reconnect_jitter_percent: 20,
                registry_file_name: "gateway_registry.toml".to_owned(),
                local_gateway_id: "local".to_owned(),
                registry_version: 1,
            },
        },
    }
}

fn unique_temp_dir() -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!("pioneer-gateway-tests-{nanos}-{id}"))
}
