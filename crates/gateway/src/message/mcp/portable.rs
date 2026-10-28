//! Pure portable-to-native mapping. The legacy parser is intentionally unused.
use anyhow::Result;
use pioneer_crud::PluginOwnershipWrite;
use pioneer_mcp::{
    McpAuthConfig, McpConfigValue, McpInstallPlan, McpInstallPlanItem, McpScopeKind,
    McpServerInstallation, McpSourceKind, McpTransportConfig,
};
use pioneer_plugins::{PortableServer, ResolvedServer};
use sha2::{Digest, Sha256};
use std::path::Path;

pub(crate) const PLUGIN_MCP_PREFIX: &str = "pplugin_";
pub(crate) fn internal_mcp_name(plugin: &str, member: &str) -> String {
    let mut hash = Sha256::new();
    hash.update((plugin.len() as u64).to_le_bytes());
    hash.update(plugin.as_bytes());
    hash.update(member.as_bytes());
    format!("{PLUGIN_MCP_PREFIX}{}", &hex::encode(hash.finalize())[..48])
}
/// Root/data are resolved host directories; lifecycle creates data before start.
/// Literal portable args/env/headers never acquire legacy secret/path syntax.
pub(crate) fn portable_install_plan(
    server: &PortableServer,
    root: &Path,
    data: &Path,
    workspace: &str,
    owner: &PluginOwnershipWrite,
) -> Result<McpInstallPlan> {
    let literal = |values: std::collections::BTreeMap<String, String>| {
        values
            .into_iter()
            .map(|(name, value)| (name, McpConfigValue::Literal { value }))
            .collect()
    };
    let transport = match pioneer_plugins::portable_mcp::resolve(server, root, data)? {
        ResolvedServer::Stdio {
            command,
            args,
            env,
            cwd,
        } => McpTransportConfig::Stdio {
            command,
            args,
            cwd: Some(cwd),
            env: literal(env),
            startup_timeout_ms: 180_000,
            tool_timeout_ms: 120_000,
        },
        ResolvedServer::StreamableHttp { url, headers } => McpTransportConfig::StreamableHttp {
            url,
            headers: literal(headers),
            startup_timeout_ms: 180_000,
            tool_timeout_ms: 120_000,
        },
    };
    let mut installation = McpServerInstallation {
        scope_kind: McpScopeKind::Workspace,
        scope_key: workspace.to_owned(),
        name: internal_mcp_name(&owner.plugin_id, &owner.member_key),
        display_name: Some(owner.member_key.clone()),
        source_kind: McpSourceKind::Config,
        source_ref: serde_json::json!({"plugin_id": owner.plugin_id, "member_key": owner.member_key}),
        transport,
        auth: McpAuthConfig::default(),
        secret_refs: vec![],
        enabled: true,
        allow_implicit_invocation: false,
        required: false,
        fingerprint: String::new(),
    };
    installation.fingerprint = pioneer_mcp::fingerprint_installation(&installation);
    Ok(McpInstallPlan {
        items: vec![McpInstallPlanItem {
            name: installation.name.clone(),
            installation: Some(installation),
            secrets: vec![],
            diagnostics: vec![],
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    fn owner(plugin: &str) -> PluginOwnershipWrite {
        PluginOwnershipWrite {
            plugin_id: plugin.into(),
            expected_revision: 1,
            member_key: "server / arbitrary portable key".into(),
            member_path: None,
            package_fingerprint: "tree".into(),
            child_id: "reserved".into(),
        }
    }
    #[test]
    fn portable_stdio_maps_literals_and_reserved_names_without_legacy_parser() {
        let root = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let server = PortableServer::Stdio {
            command: "node".into(),
            args: vec!["${HOME}".into()],
            cwd: None,
            env: BTreeMap::from([("CONFIG".into(), "${OTHER_ENV}".into())]),
        };
        let a =
            portable_install_plan(&server, root.path(), data.path(), "ws", &owner("a")).unwrap();
        let b =
            portable_install_plan(&server, root.path(), data.path(), "ws", &owner("b")).unwrap();
        let row = a.items[0].installation.as_ref().unwrap();
        assert_ne!(row.name, b.items[0].name);
        assert!(row.name.len() <= 64);
        assert!(!row.allow_implicit_invocation);
        assert!(a.items[0].secrets.is_empty());
        let McpTransportConfig::Stdio { args, env, cwd, .. } = &row.transport else {
            panic!()
        };
        assert_eq!(args[0], "${HOME}");
        assert_eq!(
            env["CONFIG"],
            McpConfigValue::Literal {
                value: "${OTHER_ENV}".into()
            }
        );
        assert!(env.contains_key("PLUGIN_ROOT") && env.contains_key("PLUGIN_DATA"));
        assert!(cwd.is_some());
    }
    #[test]
    fn portable_http_keeps_query_and_literal_headers() {
        let root = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let server = PortableServer::StreamableHttp {
            url: "https://example.org/mcp?tenant=one".into(),
            headers: BTreeMap::from([("X-Config".into(), "${PLUGIN_ROOT}".into())]),
        };
        let plan =
            portable_install_plan(&server, root.path(), data.path(), "ws", &owner("a")).unwrap();
        let row = plan.items[0].installation.as_ref().unwrap();
        let McpTransportConfig::StreamableHttp { url, headers, .. } = &row.transport else {
            panic!()
        };
        assert_eq!(url, "https://example.org/mcp?tenant=one");
        assert_eq!(
            headers["X-Config"],
            McpConfigValue::Literal {
                value: "${PLUGIN_ROOT}".into()
            }
        );
        assert!(row.auth.oauth.is_none());
        assert_eq!(row.source_ref["plugin_id"], "a");
    }
}
