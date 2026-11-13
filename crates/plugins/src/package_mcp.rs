//! Native MCP configuration plus package-only path preparation.
//! The native parser owns transport, OAuth, secret and settings validation.
use crate::{Boundary, Diagnostic, Snapshot};
use anyhow::{Context, bail};
use pioneer_mcp::{InstallParseContext, McpScopeKind, parse_install_config};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageRootKind {
    Plugin,
    App,
}
impl PackageRootKind {
    fn root(self) -> &'static str {
        match self {
            Self::Plugin => "${PLUGIN_ROOT}",
            Self::App => "${APP_ROOT}",
        }
    }
    fn data(self) -> &'static str {
        match self {
            Self::Plugin => "${PLUGIN_DATA}",
            Self::App => "${APP_DATA}",
        }
    }
    fn env(self) -> (&'static str, &'static str) {
        match self {
            Self::Plugin => ("PLUGIN_ROOT", "PLUGIN_DATA"),
            Self::App => ("APP_ROOT", "APP_DATA"),
        }
    }
}

/// Preserve the complete authored native config, including future native fields.
/// This wrapper adds no independent MCP wire format or transport enum.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PackageMcpServer(Value);
impl std::fmt::Debug for PackageMcpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PackageMcpServer([redacted])")
    }
}
impl PackageMcpServer {
    pub fn config(&self) -> &Value {
        &self.0
    }
}

/// Bounded package adapter over the very same parser used by native mcp/install.
/// Validation creates no installations or secrets in storage. Preview refs are
/// discarded; Gateway reparses with the real scope at the install boundary.
pub fn load_mcp(
    bytes: &[u8],
    max_servers: usize,
) -> Result<Vec<(String, Result<PackageMcpServer, Diagnostic>)>, Diagnostic> {
    let invalid = || {
        Diagnostic::new(
            "invalid_mcp_document",
            Boundary::Mcp,
            "mcp.json",
            "Invalid native MCP configuration document",
        )
    };
    let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
    let document: Value = serde_json::from_str(text).map_err(|_| invalid())?;
    if document
        .get("mcpServers")
        .and_then(Value::as_object)
        .is_some_and(|servers| servers.len() > max_servers)
    {
        return Err(Diagnostic::new(
            "component_limit",
            Boundary::HostEffect,
            "mcp.json",
            "Package exceeds the host component limit",
        ));
    }
    let plan = parse_install_config(
        text,
        InstallParseContext {
            scope_kind: McpScopeKind::Workspace,
            scope_key: "package-preview".into(),
            default_enabled: true,
            default_allow_implicit_invocation: false,
        },
    )
    .map_err(|_| invalid())?;
    Ok(plan
        .items
        .into_iter()
        .map(|item| {
            let result = if item.is_valid() {
                Ok(PackageMcpServer(document["mcpServers"][&item.name].clone()))
            } else {
                let code = item
                    .diagnostics
                    .iter()
                    .find(|d| d.is_error())
                    .map_or("invalid_mcp_server", |d| d.code.as_str());
                Err(Diagnostic::new(
                    code,
                    Boundary::Server,
                    &format!(
                        "mcp.json/mcpServers/{}",
                        crate::diagnostic::pointer_key(&item.name)
                    ),
                    "MCP configuration was rejected by native validation",
                ))
            };
            (item.name, result)
        })
        .collect())
}

/// Substitute only package-owned tokens, once; leave native config syntax alone.
pub fn expand(value: &str, root: &str, data: &str) -> String {
    expand_for(value, root, data, PackageRootKind::Plugin)
}
fn expand_for(value: &str, root: &str, data: &str, roots: PackageRootKind) -> String {
    let mut result = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(index) = rest.find("${") {
        result.push_str(&rest[..index]);
        rest = &rest[index..];
        if let Some(next) = rest.strip_prefix(roots.root()) {
            result.push_str(root);
            rest = next;
        } else if let Some(next) = rest.strip_prefix(roots.data()) {
            result.push_str(data);
            rest = next;
        } else {
            result.push_str("${");
            rest = &rest[2..];
        }
    }
    result.push_str(rest);
    result
}
pub fn expanded_environment(
    env: &BTreeMap<String, String>,
    root: &str,
    data: &str,
) -> BTreeMap<String, String> {
    expanded_environment_for(env, root, data, PackageRootKind::Plugin)
}
fn expanded_environment_for(
    env: &BTreeMap<String, String>,
    root: &str,
    data: &str,
    roots: PackageRootKind,
) -> BTreeMap<String, String> {
    let mut values: BTreeMap<_, _> = env
        .iter()
        .map(|(k, v)| (k.clone(), expand_for(v, root, data, roots)))
        .collect();
    values.insert(roots.env().0.into(), root.into());
    values.insert(roots.env().1.into(), data.into());
    values
}

fn contained_key(value: &str) -> Option<String> {
    let mut parts = Vec::new();
    for part in Path::new(value).components() {
        match part {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                parts.pop()?;
            }
            std::path::Component::Normal(part) => parts.push(part.to_str()?),
            _ => return None,
        }
    }
    Some(parts.join("/"))
}
fn root_relative<'a>(value: &'a str, token: &str) -> Option<&'a str> {
    if value == token {
        Some("")
    } else {
        let suffix = value.strip_prefix(token)?;
        suffix.strip_prefix('/').or_else(|| {
            if cfg!(windows) {
                suffix.strip_prefix('\\')
            } else {
                None
            }
        })
    }
}
fn is_host_path(value: &str) -> bool {
    matches!(
        Path::new(value).components().next(),
        Some(std::path::Component::RootDir | std::path::Component::Prefix(_))
    )
}
fn command_relative<'a>(command: &'a str, roots: PackageRootKind) -> Option<&'a str> {
    root_relative(command, roots.root()).or_else(|| {
        (!is_host_path(command)
            && (command.contains(std::path::MAIN_SEPARATOR)
                || (cfg!(windows) && command.contains('/'))
                || matches!(command, "." | "..")))
        .then_some(command)
    })
}

/// Host absolute paths keep native semantics. References into the package must
/// exist in the selected snapshot; data-directory refs must remain in data.
pub(crate) fn validate_snapshot_paths(server: &PackageMcpServer, snapshot: &Snapshot) -> bool {
    validate_snapshot_paths_for(server, snapshot, PackageRootKind::Plugin)
}
pub(crate) fn validate_snapshot_paths_for(
    server: &PackageMcpServer,
    snapshot: &Snapshot,
    roots: PackageRootKind,
) -> bool {
    let config = server.config();
    let Some(command) = config.get("command").and_then(Value::as_str) else {
        return true;
    };
    if config
        .get("env")
        .and_then(Value::as_object)
        .is_some_and(|env| {
            env.keys().any(|name| {
                let (root, data) = roots.env();
                if cfg!(windows) {
                    name.eq_ignore_ascii_case(root) || name.eq_ignore_ascii_case(data)
                } else {
                    name == root || name == data
                }
            })
        })
    {
        return false;
    }
    if root_relative(command.trim(), roots.data()).is_some() {
        return false;
    }
    if let Some(relative) = command_relative(command.trim(), roots) {
        if contained_key(relative).is_none_or(|key| {
            !matches!(
                snapshot.entries().get(&key),
                Some(crate::Entry::File { .. })
            )
        }) {
            return false;
        }
    }
    match config.get("cwd").and_then(Value::as_str).map(str::trim) {
        None | Some("") => true,
        Some(value) if value == roots.root() || value == roots.data() => true,
        Some(value) if root_relative(value, roots.data()).is_some() => {
            root_relative(value, roots.data())
                .and_then(contained_key)
                .is_some()
        }
        Some(value) => {
            if is_host_path(value) {
                return true;
            }
            let relative = root_relative(value, roots.root()).unwrap_or(value);
            contained_key(relative).is_some_and(|key| {
                key.is_empty()
                    || matches!(
                        snapshot.entries().get(&key),
                        Some(crate::Entry::Directory { .. })
                    )
            })
        }
    }
}

pub fn resolve(server: &PackageMcpServer, root: &Path, data: &Path) -> anyhow::Result<Value> {
    resolve_for(server, root, data, PackageRootKind::Plugin)
}
/// Only command/cwd, package tokens and the host-owned root env are adapted.
/// All other native fields flow unchanged into the native install parser.
pub fn resolve_for(
    server: &PackageMcpServer,
    root: &Path,
    data: &Path,
    roots: PackageRootKind,
) -> anyhow::Result<Value> {
    let mut config = server.config().clone();
    let Some(command) = config
        .get("command")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_owned())
    else {
        return Ok(config);
    };
    let root = root.canonicalize()?;
    let data = data.canonicalize()?;
    if !root.is_dir() || !data.is_dir() {
        bail!("invalid_package_roots");
    }
    let root_text = root.to_str().context("invalid_package_root")?;
    let data_text = data.to_str().context("invalid_package_data")?;
    if root_relative(&command, roots.data()).is_some() {
        bail!("invalid_package_command");
    }
    let command = if let Some(relative) = command_relative(&command, roots) {
        let path = crate::containment::resolve_contained(&root, &root.join(relative))?;
        if !path.is_file() {
            bail!("invalid_command_kind");
        }
        path.to_str().context("invalid_command_path")?.to_owned()
    } else {
        command
    };
    let cwd = match config.get("cwd").and_then(Value::as_str).map(str::trim) {
        None | Some("") => root.clone(),
        Some(value) if root_relative(value, roots.data()).is_some() => {
            let suffix = root_relative(value, roots.data()).context("invalid_data_cwd")?;
            crate::containment::resolve_data_directory(&data, suffix)?
        }
        Some(value) if is_host_path(value) => Path::new(value).to_path_buf(),
        Some(value) => {
            let expanded = expand_for(value, root_text, data_text, roots);
            let path = crate::containment::resolve_contained(&root, &root.join(expanded))?;
            if !path.is_dir() {
                bail!("invalid_cwd_kind");
            }
            path
        }
    };
    if let Some(args) = config.get_mut("args").and_then(Value::as_array_mut) {
        for arg in args {
            if let Some(value) = arg.as_str() {
                *arg = Value::String(expand_for(value, root_text, data_text, roots));
            }
        }
    }
    let authored_env: BTreeMap<String, String> = config
        .get("env")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let (root_env, data_env) = roots.env();
    if authored_env.keys().any(|key| {
        if cfg!(windows) {
            key.eq_ignore_ascii_case(root_env) || key.eq_ignore_ascii_case(data_env)
        } else {
            key == root_env || key == data_env
        }
    }) {
        bail!("package_root_override");
    }
    config["command"] = Value::String(command);
    config["cwd"] = Value::String(cwd.to_str().context("invalid_cwd_path")?.to_owned());
    config["env"] = json!(expanded_environment_for(
        &authored_env,
        root_text,
        data_text,
        roots
    ));
    Ok(config)
}
