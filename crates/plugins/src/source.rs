use crate::{
    Boundary, Diagnostic, Entry, MCP_SCHEMA, Manifest, PLUGIN_SCHEMA, Snapshot, parse_manifest,
};
use serde_json::{Map, Value};
use std::borrow::Cow;

fn invalid(code: &str, path: &str) -> Diagnostic {
    Diagnostic::new(
        code,
        Boundary::Package,
        path,
        "Plugin source is missing, invalid or uses an unsupported layout",
    )
}

/// Import only package metadata and contained component locations. Host-specific
/// commands, hooks, UI configuration and extensions are never interpreted here.
pub(crate) fn manifest(
    snapshot: &Snapshot,
) -> Result<(Manifest, Vec<Diagnostic>, bool), Diagnostic> {
    if snapshot.entries().contains_key("plugin.json") {
        let bytes = snapshot
            .file("plugin.json")
            .ok_or_else(|| invalid("fatal_manifest", "plugin.json"))?;
        let (manifest, diagnostics) = parse_manifest(bytes)?;
        return Ok((manifest, diagnostics, false));
    }
    let vendor_paths = [
        ".codex-plugin/plugin.json",
        ".claude-plugin/plugin.json",
        ".cursor-plugin/plugin.json",
    ];
    // Canonical wins above, then Codex, Claude, Cursor. A malformed selected
    // manifest is an error, never a reason to silently try a lower priority one.
    let path = vendor_paths
        .into_iter()
        .find(|path| snapshot.entries().contains_key(*path))
        .ok_or_else(|| invalid("fatal_manifest", "plugin.json"))?;
    let bytes = snapshot
        .file(path)
        .filter(|bytes| bytes.len() <= 1024 * 1024)
        .ok_or_else(|| invalid("fatal_manifest", path))?;
    let raw: Value = serde_json::from_slice(bytes).map_err(|_| invalid("fatal_manifest", path))?;
    let object = raw
        .as_object()
        .ok_or_else(|| invalid("fatal_manifest", path))?;
    if object.len() > 256 {
        return Err(invalid("manifest_limit", path));
    }
    if object.get("skills").is_some_and(|value| {
        !matches!(
            value.as_str(),
            Some("./skills/" | "./skills" | "skills/" | "skills")
        )
    }) {
        return Err(invalid("unsupported_plugin_layout", path));
    }
    if object.get("mcpServers").is_some_and(|value| {
        !matches!(
            value.as_str(),
            Some("./.mcp.json" | ".mcp.json" | "./mcp.json" | "mcp.json")
        )
    }) {
        return Err(invalid("unsupported_plugin_layout", path));
    }
    let mut canonical = Map::new();
    canonical.insert("$schema".into(), Value::String(PLUGIN_SCHEMA.into()));
    for key in [
        "name",
        "version",
        "description",
        "author",
        "homepage",
        "repository",
        "license",
        "keywords",
    ] {
        if let Some(value) = object.get(key) {
            canonical.insert(key.into(), value.clone());
        }
    }
    let bytes = serde_json::to_vec(&canonical).map_err(|_| invalid("fatal_manifest", path))?;
    let (mut manifest, mut diagnostics) = parse_manifest(&bytes).map_err(|mut error| {
        error.pointer = error.pointer.replacen("plugin.json", path, 1);
        error
    })?;
    diagnostics.push(Diagnostic::new(
        "vendor_subset_import",
        Boundary::Package,
        path,
        "Vendor import supports only metadata, skills/ and portable MCP; hooks, commands, UI and other host features are not imported",
    ));
    for other in vendor_paths {
        if other != path && snapshot.entries().contains_key(other) {
            diagnostics.push(Diagnostic::new(
                "ignored_vendor_manifest",
                Boundary::Package,
                other,
                "Vendor manifest ignored; priority is canonical, Codex, Claude, Cursor",
            ));
        }
    }
    for key in object.keys() {
        if ![
            "name",
            "version",
            "description",
            "author",
            "homepage",
            "repository",
            "license",
            "keywords",
            "skills",
            "mcpServers",
        ]
        .contains(&key.as_str())
        {
            diagnostics.push(Diagnostic::new(
                "ignored_vendor_feature",
                Boundary::Extension,
                &format!("{path}/{}", crate::diagnostic::pointer_key(key)),
                "Vendor field is not imported or executed",
            ));
        }
    }
    for feature in ["hooks", "commands", "agents"] {
        if !object.contains_key(feature) && snapshot.entries().contains_key(feature) {
            diagnostics.push(Diagnostic::new(
                "ignored_vendor_feature",
                Boundary::Extension,
                feature,
                "Vendor component location is not imported or executed",
            ));
        }
    }
    manifest.raw = raw;
    Ok((manifest, diagnostics, true))
}

pub(crate) fn mcp<'a>(
    snapshot: &'a Snapshot,
    manifest: &Manifest,
    imported: bool,
) -> Result<Option<Cow<'a, [u8]>>, Diagnostic> {
    let declared = imported
        .then(|| manifest.raw.get("mcpServers").and_then(Value::as_str))
        .flatten();
    let path = declared
        .map(|path| path.trim_start_matches("./"))
        .unwrap_or_else(|| {
            if imported && !snapshot.entries().contains_key("mcp.json") {
                ".mcp.json"
            } else {
                "mcp.json"
            }
        });
    let bytes = match snapshot.entries().get(path) {
        None if declared.is_none() => return Ok(None),
        Some(Entry::File { bytes, .. }) => bytes,
        _ => {
            return Err(Diagnostic::new(
                "invalid_mcp_location",
                Boundary::Mcp,
                path,
                "MCP configuration must be a contained regular file",
            ));
        }
    };
    if !imported {
        return Ok(Some(Cow::Borrowed(bytes)));
    }
    let mut document: Value =
        serde_json::from_slice(bytes).map_err(|_| invalid("invalid_mcp_document", path))?;
    let object = document
        .as_object_mut()
        .ok_or_else(|| invalid("invalid_mcp_document", path))?;
    if object
        .get("$schema")
        .is_some_and(|schema| schema.as_str() != Some(MCP_SCHEMA))
    {
        return Err(invalid("invalid_mcp_document", path));
    }
    object.insert("$schema".into(), Value::String(MCP_SCHEMA.into()));
    if let Some(servers) = object.get_mut("mcpServers").and_then(Value::as_object_mut) {
        for server in servers.values_mut().filter_map(Value::as_object_mut) {
            if server.get("type").and_then(Value::as_str) == Some("http") {
                server.insert("type".into(), Value::String("streamable-http".into()));
            } else if !server.contains_key("type") {
                let kind = if server.contains_key("command") {
                    "stdio"
                } else {
                    "streamable-http"
                };
                server.insert("type".into(), Value::String(kind.into()));
            }
        }
    }
    let bytes = serde_json::to_vec(&document).map_err(|_| invalid("invalid_mcp_document", path))?;
    Ok(Some(Cow::Owned(bytes)))
}
