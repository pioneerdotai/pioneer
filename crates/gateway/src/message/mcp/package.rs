//! Package paths/ownership over the ordinary native MCP parser and installer.
use anyhow::Result;
use pioneer_crud::PluginOwnershipWrite;
use pioneer_mcp::{InstallParseContext, McpInstallPlan, McpScopeKind, parse_install_config};
use pioneer_plugins::PackageMcpServer;
use std::path::Path;

pub(crate) fn package_install_plan(
    server: &PackageMcpServer,
    root: &Path,
    data: &Path,
    workspace: &str,
    owner: &PluginOwnershipWrite,
) -> Result<McpInstallPlan> {
    let config = pioneer_plugins::package_mcp::resolve_for(
        server,
        root,
        data,
        match owner.kind {
            pioneer_protocol::PackageKind::Plugin => {
                pioneer_plugins::package_mcp::PackageRootKind::Plugin
            }
            pioneer_protocol::PackageKind::App => {
                pioneer_plugins::package_mcp::PackageRootKind::App
            }
        },
    )?;
    let mut plan = parse_install_config(
        &serde_json::json!({"mcpServers":{owner.member_key.clone():config}}).to_string(),
        InstallParseContext {
            scope_kind: McpScopeKind::Workspace,
            scope_key: workspace.to_owned(),
            default_enabled: true,
            default_allow_implicit_invocation: false,
        },
    )?;
    // Native parsing, secret refs and policies remain untouched. Add only host
    // provenance; the same native installer below owns materialization/writes.
    for item in &mut plan.items {
        if let Some(installation) = &mut item.installation {
            let domain = match owner.kind {
                pioneer_protocol::PackageKind::Plugin => "plugin_id",
                pioneer_protocol::PackageKind::App => "app_id",
            };
            installation.source_ref[domain] = serde_json::json!(owner.plugin_id);
            installation.source_ref["member_key"] = serde_json::json!(owner.member_key);
            installation.fingerprint = pioneer_mcp::fingerprint_installation(installation);
        }
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    // Source-only regressions: NOT_RUN / NOT_COMPILED.
    use super::*;
    use pioneer_mcp::{McpConfigValue, McpTransportConfig};
    use pioneer_protocol::PackageKind;
    use serde_json::{Value, json};

    fn owner(kind: PackageKind) -> PluginOwnershipWrite {
        PluginOwnershipWrite {
            kind,
            plugin_id: "parent".into(),
            expected_revision: 1,
            member_key: "zenmoney".into(),
            member_path: None,
            package_fingerprint: "tree".into(),
            child_id: "reserved".into(),
        }
    }
    fn server(config: &Value) -> PackageMcpServer {
        pioneer_plugins::load_mcp(
            json!({"mcpServers":{"zenmoney":config}})
                .to_string()
                .as_bytes(),
            1,
        )
        .unwrap()
        .pop()
        .unwrap()
        .1
        .unwrap()
    }
    fn context() -> InstallParseContext {
        InstallParseContext {
            scope_kind: McpScopeKind::Workspace,
            scope_key: "ws".into(),
            default_enabled: true,
            default_allow_implicit_invocation: false,
        }
    }
    #[test]
    fn packages_preserve_native_http_auth_secrets_policies_and_timeouts() {
        for kind in [PackageKind::Plugin, PackageKind::App] {
            for tag in [None, Some("http"), Some("streamable-http")] {
                let mut config = json!({
                    "url":"http://example.test/mcp",
                    "headers":{"X-Key":"header-canary","Accept":"native-header-canary"},
                    "oauth":{"client_id":"registered","client_secret":"oauth-canary","scopes":["read"]},
                    "enabled":false,"allow_implicit_invocation":true,"required":true,
                    "startup_timeout_sec":7,"tool_timeout_sec":19
                });
                if let Some(tag) = tag {
                    config["type"] = tag.into();
                }
                let document = json!({"mcpServers":{"zenmoney":config}}).to_string();
                let reference = parse_install_config(&document, context()).unwrap();
                assert!(reference.items[0].is_valid());
                let input = server(&config);
                assert!(!format!("{input:?}").contains("canary"));
                let plan = package_install_plan(
                    &input,
                    Path::new("unused-root"),
                    Path::new("unused-data"),
                    "ws",
                    &owner(kind),
                )
                .unwrap();
                assert_eq!(plan.items.len(), 1);
                assert!(plan.items[0].is_valid());
                let installed = plan.items[0].installation.as_ref().unwrap();
                let native = reference.items[0].installation.as_ref().unwrap();
                assert_eq!(installed.transport, native.transport);
                assert_eq!(installed.auth, native.auth);
                assert_eq!(installed.secret_refs, native.secret_refs);
                assert_eq!(plan.items[0].secrets, reference.items[0].secrets);
                assert_eq!(plan.items[0].secrets.len(), 3);
                assert!(!installed.enabled);
                assert!(installed.allow_implicit_invocation && installed.required);
                assert_eq!(installed.scope_key, "ws");
                assert_eq!(installed.name, "zenmoney");
                let McpTransportConfig::StreamableHttp {
                    startup_timeout_ms,
                    tool_timeout_ms,
                    ..
                } = &installed.transport
                else {
                    panic!("expected native HTTP transport");
                };
                assert_eq!((*startup_timeout_ms, *tool_timeout_ms), (7000, 19000));
                let domain = if kind == PackageKind::App {
                    "app_id"
                } else {
                    "plugin_id"
                };
                assert_eq!(installed.source_ref[domain], "parent");
                assert_eq!(installed.source_ref["transport"], "streamable_http");
                assert!(!installed.source_ref.to_string().contains("canary"));
            }
        }
    }
    #[test]
    fn stdio_package_paths_use_native_secret_and_timeout_handling() {
        for kind in [PackageKind::Plugin, PackageKind::App] {
            let root = tempfile::tempdir().unwrap();
            let data = tempfile::tempdir().unwrap();
            std::fs::create_dir(root.path().join("bin")).unwrap();
            std::fs::write(root.path().join("bin/server"), "payload").unwrap();
            let (root_token, data_token, root_env, data_env) = if kind == PackageKind::App {
                ("${APP_ROOT}", "${APP_DATA}", "APP_ROOT", "APP_DATA")
            } else {
                (
                    "${PLUGIN_ROOT}",
                    "${PLUGIN_DATA}",
                    "PLUGIN_ROOT",
                    "PLUGIN_DATA",
                )
            };
            let separator = std::path::MAIN_SEPARATOR;
            let config = json!({"command":format!(".{separator}bin{separator}server"),"args":["two words",format!("{root_token}/helper.js"),"${HOME}"],
                "cwd":format!("{root_token}{separator}."),"env":{"TOKEN":data_token},"disabled":true,"timeout":3,"startup_timeout_sec":2});
            let plan = package_install_plan(
                &server(&config),
                root.path(),
                data.path(),
                "ws",
                &owner(kind),
            )
            .unwrap();
            assert!(plan.items[0].is_valid());
            let installed = plan.items[0].installation.as_ref().unwrap();
            assert!(!installed.enabled);
            let McpTransportConfig::Stdio {
                command,
                args,
                env,
                cwd,
                startup_timeout_ms,
                tool_timeout_ms,
            } = &installed.transport
            else {
                panic!("expected native stdio");
            };
            let canonical = root.path().canonicalize().unwrap();
            let data_path = data.path().canonicalize().unwrap();
            assert_eq!(command, canonical.join("bin/server").to_str().unwrap());
            assert_eq!(cwd.as_deref(), canonical.to_str());
            assert_eq!(
                args,
                &vec![
                    "two words".to_owned(),
                    format!("{}/helper.js", canonical.to_str().unwrap()),
                    "${HOME}".to_owned()
                ]
            );
            assert_eq!((*startup_timeout_ms, *tool_timeout_ms), (2000, 3000));
            for (key, expected) in [
                ("TOKEN", data_path.to_str().unwrap()),
                (root_env, canonical.to_str().unwrap()),
                (data_env, data_path.to_str().unwrap()),
            ] {
                let McpConfigValue::SecretRef { ref_id } = &env[key] else {
                    panic!("native env must use secret refs");
                };
                assert_eq!(
                    plan.items[0]
                        .secrets
                        .iter()
                        .find(|secret| &secret.ref_id == ref_id)
                        .unwrap()
                        .value,
                    expected
                );
            }
        }
    }
}
