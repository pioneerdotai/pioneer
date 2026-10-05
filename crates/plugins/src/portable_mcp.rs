use crate::{Boundary, Diagnostic, MCP_SCHEMA};
use http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
};
use url::{Host, Url};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum PortableServer {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },
    StreamableHttp {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    Sse {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
}

pub fn parse_mcp(
    bytes: &[u8],
    max_servers: usize,
) -> Result<Vec<(String, Result<PortableServer, Diagnostic>)>, Diagnostic> {
    let fail = || {
        Diagnostic::new(
            "invalid_mcp_document",
            Boundary::Mcp,
            "mcp.json",
            "Invalid MCP envelope or unsupported schema version",
        )
    };
    let root: Value = serde_json::from_slice(bytes).map_err(|_| fail())?;
    let object = root.as_object().ok_or_else(fail)?;
    if object.keys().any(|k| k != "$schema" && k != "mcpServers")
        || object.get("$schema").and_then(Value::as_str) != Some(MCP_SCHEMA)
    {
        return Err(fail());
    }
    let servers = object
        .get("mcpServers")
        .and_then(Value::as_object)
        .ok_or_else(fail)?;
    if servers.len() > max_servers {
        return Err(Diagnostic::new(
            "component_limit",
            Boundary::HostEffect,
            "mcp.json/mcpServers",
            "Package exceeds the host component limit",
        ));
    }
    Ok(servers
        .iter()
        .map(|(key, value)| (key.clone(), parse_server(key, value)))
        .collect())
}
fn parse_server(key: &str, value: &Value) -> Result<PortableServer, Diagnostic> {
    let pointer = format!(
        "mcp.json/mcpServers/{}",
        crate::diagnostic::pointer_key(key)
    );
    let fail = || {
        Diagnostic::new(
            "invalid_mcp_server",
            Boundary::Server,
            &pointer,
            "Invalid declared transport configuration",
        )
    };
    if value.get("cwd").is_some_and(|cwd| !cwd.is_string()) {
        return Err(fail());
    }
    let server: PortableServer = serde_json::from_value(value.clone()).map_err(|_| fail())?;
    match &server {
        PortableServer::Stdio {
            command,
            args,
            env,
            cwd,
        } => {
            if !valid_command(command)
                || args.iter().any(|v| v.contains('\0'))
                || env.iter().any(|(k, v)| {
                    k.is_empty() || k.contains(['=', '\0']) || v.contains('\0') || reserved_env(k)
                })
                || cwd
                    .as_ref()
                    .is_some_and(|s| !valid_cwd(s) || s.contains('\0'))
            {
                return Err(fail());
            }
        }
        PortableServer::StreamableHttp { url, headers } | PortableServer::Sse { url, headers } => {
            if !valid_url(url) || !valid_headers(headers) {
                return Err(fail());
            }
        }
    }
    if matches!(server, PortableServer::Sse { .. }) {
        return Err(Diagnostic::new(
            "unsupported_transport",
            Boundary::Server,
            &pointer,
            "Legacy HTTP+SSE transport is unsupported",
        ));
    }
    Ok(server)
}
fn reserved_env(key: &str) -> bool {
    if cfg!(windows) {
        key.eq_ignore_ascii_case("PLUGIN_ROOT") || key.eq_ignore_ascii_case("PLUGIN_DATA")
    } else {
        key == "PLUGIN_ROOT" || key == "PLUGIN_DATA"
    }
}
fn valid_command(s: &str) -> bool {
    if s.is_empty() || s.chars().any(|c| c.is_whitespace() || c == '\0') {
        return false;
    }
    if s.starts_with("./") {
        return s.len() > 2;
    }
    // Bare executable names are resolved by Command::new on the Gateway.
    !s.contains(['/', '\\', ':']) && s != "." && s != ".."
}
fn valid_cwd(s: &str) -> bool {
    s.starts_with("./")
        || s == "${PLUGIN_ROOT}"
        || s.starts_with("${PLUGIN_ROOT}/")
        || s == "${PLUGIN_DATA}"
        || s.starts_with("${PLUGIN_DATA}/")
}
fn valid_url(s: &str) -> bool {
    let Ok(url) = Url::parse(s) else {
        return false;
    };
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.host().is_none()
    {
        return false;
    }
    // URL parser normalizes empty userinfo away; it is still prohibited.
    if s.split_once("://").is_none_or(|(_, rest)| {
        rest.split(['/', '?', '#'])
            .next()
            .is_some_and(|a| a.contains('@'))
    }) {
        return false;
    }
    match url.scheme() {
        "https" => true,
        "http" => match url.host() {
            Some(Host::Domain("localhost")) => true,
            Some(Host::Ipv4(ip)) => IpAddr::V4(ip).is_loopback(),
            Some(Host::Ipv6(ip)) => IpAddr::V6(ip).is_loopback(),
            _ => false,
        },
        _ => false,
    }
}
fn valid_headers(headers: &BTreeMap<String, String>) -> bool {
    let mut seen = BTreeSet::new();
    headers.iter().all(|(k, v)| {
        HeaderName::from_bytes(k.as_bytes()).is_ok()
            && HeaderValue::from_str(v).is_ok()
            && seen.insert(k.to_ascii_lowercase())
    })
}

/// Replace only exact tokens in the original input. Replacement text is never
/// rescanned, including roots which happen to contain another token.
pub fn expand(value: &str, root: &str, data: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(index) = rest.find("${PLUGIN_") {
        result.push_str(&rest[..index]);
        rest = &rest[index..];
        if let Some(next) = rest.strip_prefix("${PLUGIN_ROOT}") {
            result.push_str(root);
            rest = next;
        } else if let Some(next) = rest.strip_prefix("${PLUGIN_DATA}") {
            result.push_str(data);
            rest = next;
        } else {
            result.push_str("${PLUGIN_");
            rest = &rest[9..];
        }
    }
    result.push_str(rest);
    result
}

/// Convert portable stdio values without imposing path semantics on opaque
/// arguments or environment values. Cwd/command containment is a separate step.
pub fn expanded_environment(
    env: &BTreeMap<String, String>,
    root: &str,
    data: &str,
) -> BTreeMap<String, String> {
    let mut values: BTreeMap<_, _> = env
        .iter()
        .map(|(k, v)| (k.clone(), expand(v, root, data)))
        .collect();
    values.insert("PLUGIN_ROOT".into(), root.into());
    values.insert("PLUGIN_DATA".into(), data.into());
    values
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedServer {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        cwd: String,
    },
    StreamableHttp {
        url: String,
        headers: BTreeMap<String, String>,
    },
}

/// Called against host-controlled managed package files and the dedicated
/// existing data root. It does not create directories or start any executable.
/// Lifecycle owns data-root creation and must recheck cwd before subprocess use.
pub fn resolve(
    server: &PortableServer,
    root: &std::path::Path,
    data: &std::path::Path,
) -> anyhow::Result<ResolvedServer> {
    use anyhow::{Context, bail};
    parse_server("adapter", &serde_json::to_value(server)?)
        .map_err(|_| anyhow::anyhow!("invalid_portable_server"))?;
    if let PortableServer::StreamableHttp { url, headers } = server {
        return Ok(ResolvedServer::StreamableHttp {
            url: url.clone(),
            headers: headers.clone(),
        });
    }
    let root = root.canonicalize()?;
    let data = data.canonicalize()?;
    if !root.is_dir() || !data.is_dir() {
        bail!("invalid_plugin_roots");
    }
    let root_text = root.to_str().context("invalid_plugin_root")?;
    let data_text = data.to_str().context("invalid_plugin_data")?;
    match server {
        PortableServer::Stdio {
            command,
            args,
            env,
            cwd,
        } => {
            let command = if let Some(relative) = command.strip_prefix("./") {
                let path = crate::containment::resolve_contained(&root, &root.join(relative))?;
                if !path.is_file() {
                    bail!("invalid_command_kind");
                }
                path.to_str().context("invalid_command_path")?.to_owned()
            } else {
                command.clone()
            };
            let cwd = match cwd.as_deref() {
                None => root.clone(),
                Some(value)
                    if value == "${PLUGIN_DATA}" || value.starts_with("${PLUGIN_DATA}/") =>
                {
                    let expanded = expand(value, root_text, data_text);
                    let suffix = expanded
                        .strip_prefix(data_text)
                        .context("invalid_data_cwd")?
                        .strip_prefix('/')
                        .unwrap_or("");
                    crate::containment::resolve_data_directory(&data, suffix)?
                }
                Some(value) => {
                    let expanded = expand(value, root_text, data_text);
                    let path = if let Some(relative) = expanded.strip_prefix("./") {
                        root.join(relative)
                    } else {
                        std::path::PathBuf::from(expanded)
                    };
                    let resolved = crate::containment::resolve_contained(&root, &path)?;
                    if !resolved.is_dir() {
                        bail!("invalid_cwd_kind");
                    }
                    resolved
                }
            };
            Ok(ResolvedServer::Stdio {
                command,
                args: args
                    .iter()
                    .map(|a| expand(a, root_text, data_text))
                    .collect(),
                env: expanded_environment(env, root_text, data_text),
                cwd: cwd.to_str().context("invalid_cwd_path")?.to_owned(),
            })
        }
        PortableServer::StreamableHttp { url, headers } => Ok(ResolvedServer::StreamableHttp {
            url: url.clone(),
            headers: headers.clone(),
        }),
        PortableServer::Sse { .. } => bail!("unsupported_transport"),
    }
}

/// Preview checks the package path subset against the immutable byte snapshot.
/// Opaque args/env/headers are deliberately excluded from containment checks.
pub(crate) fn validate_snapshot_paths(server: &PortableServer, snapshot: &crate::Snapshot) -> bool {
    use crate::Entry;
    let contained_key = |value: &str| -> Option<String> {
        let mut parts = Vec::new();
        for component in value.split('/') {
            match component {
                "" | "." => {}
                ".." => {
                    parts.pop()?;
                }
                part if !part.contains(['\\', ':']) => parts.push(part),
                _ => return None,
            }
        }
        Some(parts.join("/"))
    };
    match server {
        PortableServer::Stdio { command, cwd, .. } => {
            if let Some(path) = command.strip_prefix("./") {
                if contained_key(path).is_none_or(|key| {
                    !matches!(snapshot.entries().get(&key), Some(Entry::File { .. }))
                }) {
                    return false;
                }
            }
            match cwd.as_deref() {
                None | Some("${PLUGIN_ROOT}") | Some("${PLUGIN_DATA}") => true,
                Some(value) if value.starts_with("${PLUGIN_DATA}/") => value
                    .strip_prefix("${PLUGIN_DATA}/")
                    .and_then(contained_key)
                    .is_some(),
                Some(value) => {
                    let path = value
                        .strip_prefix("./")
                        .or_else(|| value.strip_prefix("${PLUGIN_ROOT}/"));
                    path.and_then(contained_key).is_some_and(|key| {
                        key.is_empty()
                            || matches!(snapshot.entries().get(&key), Some(Entry::Directory { .. }))
                    })
                }
            }
        }
        _ => true,
    }
}
