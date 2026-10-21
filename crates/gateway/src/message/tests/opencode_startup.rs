use super::*;
use crate::cli_runtime::opencode_session::startup_tests::StartupFixture;

#[test]
fn opencode_readiness_publishes_first_probe_catalog_without_second_process() {
    run_gateway_message_test("opencode-readiness-single-probe", || async {
        for mode in ["ready", "needs-auth", "catalog-error"] {
            let harness =
                setup_cli_runtime_security_harness(Some(CLIAgentRuntimeKind::OpenCode)).await;
            let processor = &harness.processor;
            let fixture = StartupFixture::new("", mode).await;
            let config = crate::isolated_test_app_config().unwrap();
            let settings_name = config.gateway.settings_file_name.as_str();
            let settings_path = processor.artifact_runtime_home.join(settings_name);
            let mut settings = crate::settings::load_or_create_gateway_settings(
                &settings_path,
                config.gateway.settings_version,
                settings_name,
            )
            .unwrap();
            settings
                .set_cli_runtime_settings_for_tests(pioneer_protocol::GatewayCliRuntimeSettings {
                    instances: vec![pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                        id: "opencode".into(),
                        kind: CLIAgentRuntimeKind::OpenCode,
                        display_name: "Fixture".into(),
                        nickname: "fixture-opencode".into(),
                        enabled: true,
                        binary_path: fixture.instance.binary_path.clone(),
                        home_path: fixture.instance.home_path.clone(),
                        shadow_home_path: None,
                        compaction_model: None,
                    }],
                })
                .unwrap();
            crate::settings::save_gateway_settings(&settings_path, &settings).unwrap();
            processor
                .refresh_cli_readiness_for_tests(&harness.workspace_id)
                .await
                .unwrap();
            let probe = processor
                .cli_runtime_probe_snapshot(&harness.workspace_id, "opencode")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(fixture.starts().len(), 1, "{mode}");
            let requests = std::fs::read_to_string(fixture.root.path().join("requests")).unwrap();
            assert_eq!(
                requests
                    .lines()
                    .filter(|line| *line == "/api/model")
                    .count(),
                1,
                "{mode}"
            );
            match mode {
                "ready" => {
                    assert_eq!(probe.summary.status, pioneer_protocol::RuntimeStatus::Ready);
                    assert_eq!(probe.summary.version.as_deref(), Some("fixture-v2"));
                    assert!(probe.summary.account.unwrap().authenticated);
                    assert!(probe.summary.capabilities.supports_mcp_tools);
                    assert!(
                        probe
                            .summary
                            .recent_stderr
                            .iter()
                            .any(|line| line.contains("synthetic OpenCode"))
                    );
                    let models = probe.models.unwrap().result;
                    assert!(models.error_message.is_none());
                    assert_eq!(models.models[0].id, "local/model");
                    assert!(models.models[0].is_custom);
                    assert_eq!(models.models[0].supports_vision, Some(true));
                    assert!(
                        models
                            .diagnostics
                            .iter()
                            .any(|diagnostic| diagnostic.code == "opencode_probe.server_ready")
                    );
                }
                "needs-auth" => {
                    assert_eq!(
                        probe.summary.status,
                        pioneer_protocol::RuntimeStatus::NeedsAuth
                    );
                    assert!(!probe.summary.account.unwrap().authenticated);
                    assert!(probe.models.is_none());
                    assert!(
                        probe
                            .summary
                            .diagnostics
                            .iter()
                            .any(|diagnostic| diagnostic.code == "opencode_probe.needs_auth")
                    );
                }
                _ => {
                    assert!(matches!(
                        probe.summary.status,
                        pioneer_protocol::RuntimeStatus::Error { .. }
                    ));
                    assert!(probe.models.is_none());
                    assert!(probe.summary.diagnostics.iter().any(|diagnostic| diagnostic.code == "opencode_probe.server_api_failed"));
                }
            }
        }
    });
}

#[test]
fn opencode_live_readiness_keeps_custom_models_diagnostics_and_catalog_errors() {
    run_gateway_message_test("opencode-live-readiness-catalog", || async {
        let harness = setup_cli_runtime_security_harness(Some(CLIAgentRuntimeKind::OpenCode)).await;
        for mode in ["ready", "needs-auth", "catalog-error"] {
            let fixture = StartupFixture::new("", mode).await;
            let live = harness
                .processor
                .cli_runtime_live_summary_from_instance_with_trace(
                    &harness.workspace_id,
                    fixture.instance.clone(),
                    None,
                )
                .await
                .unwrap();
            let models = live
                .models
                .expect("first probe supplies model result even on failure");
            assert_eq!(fixture.starts().len(), 1);
            assert!(
                models
                    .models
                    .iter()
                    .any(|model| model.id == "local/custom" && model.is_custom)
            );
            assert!(!models.diagnostics.is_empty());
            assert_eq!(models.error_message.is_none(), mode == "ready");
            if mode == "ready" {
                assert_eq!(live.summary.version.as_deref(), Some("fixture-v2"));
                assert!(live.summary.account.unwrap().authenticated);
            }
        }
    });
}
