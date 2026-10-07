use crate::{Boundary, Diagnostic};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const SPEC_VERSION: &str = "1.0.0";
pub const PLUGIN_SCHEMA: &str = "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json";
pub const MCP_SCHEMA: &str = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json";
pub const PLUGIN_SCHEMA_BYTES: &[u8] = include_bytes!("../schemas/1.0.0/plugin.schema.json");
pub const MCP_SCHEMA_BYTES: &[u8] = include_bytes!("../schemas/1.0.0/mcp.schema.json");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub name: String,
    pub version: Option<String>,
    pub description: Option<String>,
    /// Immutable provenance, never interpreted as execution configuration.
    pub raw: Value,
}

pub fn parse_manifest(bytes: &[u8]) -> Result<(Manifest, Vec<Diagnostic>), Diagnostic> {
    let fail = |code: &str, pointer: &str, message: &str| {
        Diagnostic::new(code, Boundary::Package, pointer, message)
    };
    if bytes.len() > 1024 * 1024 {
        return Err(fail(
            "manifest_limit",
            "plugin.json",
            "Manifest exceeds host byte limit",
        ));
    }
    let raw: Value = serde_json::from_slice(bytes).map_err(|_| {
        fail(
            "fatal_manifest",
            "plugin.json",
            "Manifest must be valid JSON",
        )
    })?;
    let object = raw.as_object().ok_or_else(|| {
        fail(
            "fatal_manifest",
            "plugin.json",
            "Manifest must be an object",
        )
    })?;
    if object.get("$schema").and_then(Value::as_str) != Some(PLUGIN_SCHEMA) {
        return Err(fail(
            "unsupported_schema_version",
            "plugin.json/$schema",
            "Only the canonical Agent Plugins 1.0.0 manifest schema is supported",
        ));
    }
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| valid_name(s))
        .ok_or_else(|| {
            fail(
                "fatal_manifest",
                "plugin.json/name",
                "Invalid portable plugin name",
            )
        })?
        .to_owned();
    for key in [
        "version",
        "description",
        "homepage",
        "repository",
        "license",
    ] {
        if object.get(key).is_some_and(|v| !v.is_string()) {
            return Err(fail(
                "fatal_manifest",
                &format!("plugin.json/{key}"),
                "Metadata must be a string",
            ));
        }
    }
    if let Some(author) = object.get("author") {
        let author = author.as_object().ok_or_else(|| {
            fail(
                "fatal_manifest",
                "plugin.json/author",
                "Author must be an object",
            )
        })?;
        if author
            .iter()
            .any(|(k, v)| !["name", "email", "url"].contains(&k.as_str()) || !v.is_string())
        {
            return Err(fail(
                "fatal_manifest",
                "plugin.json/author",
                "Author permits only name, email and url strings",
            ));
        }
    }
    if object.get("keywords").is_some_and(|v| {
        v.as_array()
            .is_none_or(|a| a.iter().any(|v| !v.is_string()))
    }) {
        return Err(fail(
            "fatal_manifest",
            "plugin.json/keywords",
            "Keywords must be an array of strings",
        ));
    }
    if object.len() > 256 {
        return Err(fail(
            "manifest_limit",
            "plugin.json",
            "Manifest exceeds host field limit",
        ));
    }
    let mut diagnostics = Vec::new();
    for key in object.keys() {
        if ![
            "$schema",
            "name",
            "version",
            "description",
            "author",
            "homepage",
            "repository",
            "license",
            "keywords",
            "extensions",
        ]
        .contains(&key.as_str())
        {
            diagnostics.push(fail(
                "unknown_manifest_field",
                &format!("plugin.json/{}", crate::diagnostic::pointer_key(key)),
                "Unknown manifest field ignored",
            ));
        }
    }
    if object.get("extensions").is_some_and(|v| !v.is_object()) {
        diagnostics.push(Diagnostic::new(
            "invalid_extensions_container",
            Boundary::Extension,
            "plugin.json/extensions",
            "Non-object extensions ignored",
        ));
    }
    // No namespace is implemented by the headless loader. In particular, do
    // not inspect foreign values, even if they violate the generic schema.
    let version = optional_string(object, "version");
    let description = optional_string(object, "description");
    Ok((
        Manifest {
            name,
            version,
            description,
            raw,
        },
        diagnostics,
    ))
}
fn optional_string(object: &Map<String, Value>, key: &str) -> Option<String> {
    object.get(key).and_then(Value::as_str).map(str::to_owned)
}
fn valid_name(s: &str) -> bool {
    let alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    (1..=64).contains(&s.len())
        && s.as_bytes().first().is_some_and(|b| alnum(*b))
        && s.as_bytes().last().is_some_and(|b| alnum(*b))
        && s.bytes().all(|b| alnum(b) || b == b'-' || b == b'.')
        && !s.contains("--")
        && !s.contains("..")
}
