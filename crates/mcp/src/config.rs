use crate::domain::{
    McpAuthConfig, McpAvailabilitySnapshot, McpConfigValue, McpScopeKind, McpSecretRef,
    McpServerInstallation, McpSourceKind, McpTransportConfig,
};
use crate::error::{McpConfigDocumentError, McpValidationDiagnostic};
use crate::fingerprint::fingerprint_installation;
use crate::redaction::redact_string_map;
use crate::secrets::{McpSecretMaterialization, secret_ref_for};
use crate::validation::is_valid_server_name;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use url::Url;

const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 180_000;
const DEFAULT_TOOL_TIMEOUT_MS: u64 = 120_000;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct InstallParseContext {
    pub scope_kind: McpScopeKind,
    pub scope_key: String,
    pub default_enabled: bool,
    pub default_allow_implicit_invocation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpInstallPlan {
    #[serde(default)]
    pub items: Vec<McpInstallPlanItem>,
}

impl McpInstallPlan {
    pub fn availability_snapshot(&self) -> McpAvailabilitySnapshot {
        McpAvailabilitySnapshot::phase1_from_installations(
            self.items
                .iter()
                .filter_map(|item| item.installation.as_ref()),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpInstallPlanItem {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installation: Option<McpServerInstallation>,
    #[serde(default)]
    pub secrets: Vec<McpSecretMaterialization>,
    #[serde(default)]
    pub diagnostics: Vec<McpValidationDiagnostic>,
}

impl McpInstallPlanItem {
    pub fn is_valid(&self) -> bool {
        self.installation.is_some()
            && !self
                .diagnostics
                .iter()
                .any(McpValidationDiagnostic::is_error)
    }
}

pub fn parse_install_config(
    config_json: &str,
    context: InstallParseContext,
) -> Result<McpInstallPlan, McpConfigDocumentError> {
    let root = serde_json::from_str::<Value>(config_json).map_err(|error| {
        McpConfigDocumentError::new(
            "invalid_json",
            format!("failed to parse MCP config JSON: {error}"),
            Some("$".to_owned()),
        )
    })?;

    let servers = root
        .get("mcpServers")
        .ok_or_else(|| {
            McpConfigDocumentError::new(
                "missing_mcp_servers",
                "`mcpServers` object is required",
                Some("$.mcpServers".to_owned()),
            )
        })?
        .as_object()
        .ok_or_else(|| {
            McpConfigDocumentError::new(
                "missing_mcp_servers",
                "`mcpServers` must be an object",
                Some("$.mcpServers".to_owned()),
            )
        })?;

    let mut items = Vec::new();
    for (name, value) in servers {
        items.push(parse_server(name, value, &context));
    }
    items.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(McpInstallPlan { items })
}

fn parse_server(name: &str, value: &Value, context: &InstallParseContext) -> McpInstallPlanItem {
    let mut diagnostics = Vec::new();
    let mut secrets = Vec::new();

    if !is_valid_server_name(name) {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_server_name",
            "MCP server name must match ^[A-Za-z0-9_-]{1,64}$",
            Some(format!("$.mcpServers.{name}")),
        ));
    }

    let Some(object) = value.as_object() else {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_server_config",
            "MCP server config must be an object",
            Some(format!("$.mcpServers.{name}")),
        ));
        return McpInstallPlanItem {
            name: name.to_owned(),
            installation: None,
            secrets,
            diagnostics,
        };
    };

    let has_command = object.contains_key("command");
    let has_url = object.contains_key("url");
    if has_command && has_url {
        diagnostics.push(McpValidationDiagnostic::error(
            "ambiguous_transport",
            "MCP server config must not contain both `command` and `url`",
            Some(format!("$.mcpServers.{name}")),
        ));
    } else if !has_command && !has_url {
        diagnostics.push(McpValidationDiagnostic::error(
            "missing_transport",
            "MCP server config must contain either `command` or `url`",
            Some(format!("$.mcpServers.{name}")),
        ));
    }

    let enabled = parse_enabled(name, object, context.default_enabled, &mut diagnostics);
    let allow_implicit_invocation = parse_allow_implicit_invocation(
        name,
        object,
        context.default_allow_implicit_invocation,
        &mut diagnostics,
    );
    let required = bool_field(object, "required").unwrap_or(false);
    let startup_timeout_ms =
        timeout_ms_field(object, "startup_timeout_sec", name, &mut diagnostics)
            .unwrap_or(DEFAULT_STARTUP_TIMEOUT_MS);
    let tool_timeout_ms = timeout_ms_field(object, "tool_timeout_sec", name, &mut diagnostics)
        .or_else(|| timeout_ms_field(object, "timeout", name, &mut diagnostics))
        .unwrap_or(DEFAULT_TOOL_TIMEOUT_MS);

    let transport = if has_command && !has_url {
        parse_stdio_transport(
            name,
            object,
            context,
            startup_timeout_ms,
            tool_timeout_ms,
            &mut diagnostics,
            &mut secrets,
        )
    } else if has_url && !has_command {
        parse_http_transport(
            name,
            object,
            context,
            startup_timeout_ms,
            tool_timeout_ms,
            &mut diagnostics,
            &mut secrets,
        )
    } else {
        None
    };

    if diagnostics.iter().any(McpValidationDiagnostic::is_error) {
        return McpInstallPlanItem {
            name: name.to_owned(),
            installation: None,
            secrets,
            diagnostics,
        };
    }

    let transport = transport.expect("transport must be present when diagnostics have no errors");
    let source_ref = source_ref(name, &transport);
    let secret_refs = secrets
        .iter()
        .map(|secret| McpSecretRef {
            ref_id: secret.ref_id.clone(),
            name: secret
                .ref_id
                .rsplit(':')
                .next()
                .unwrap_or_default()
                .to_owned(),
            source: secret_source(secret.ref_id.as_str()),
        })
        .collect::<Vec<_>>();

    let mut installation = McpServerInstallation {
        scope_kind: context.scope_kind.clone(),
        scope_key: context.scope_key.clone(),
        name: name.to_owned(),
        display_name: None,
        source_kind: McpSourceKind::Config,
        source_ref,
        transport,
        auth: McpAuthConfig::default(),
        secret_refs,
        enabled,
        allow_implicit_invocation,
        required,
        fingerprint: String::new(),
    };
    installation.fingerprint = fingerprint_installation(&installation);

    McpInstallPlanItem {
        name: name.to_owned(),
        installation: Some(installation),
        secrets,
        diagnostics,
    }
}

fn parse_stdio_transport(
    name: &str,
    object: &Map<String, Value>,
    context: &InstallParseContext,
    startup_timeout_ms: u64,
    tool_timeout_ms: u64,
    diagnostics: &mut Vec<McpValidationDiagnostic>,
    secrets: &mut Vec<McpSecretMaterialization>,
) -> Option<McpTransportConfig> {
    let command = string_field(object, "command");
    let Some(command) = command.filter(|value| !value.trim().is_empty()) else {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_command",
            "`command` must be a non-empty string",
            Some(format!("$.mcpServers.{name}.command")),
        ));
        return None;
    };

    let args = string_array_field(object, "args", name, diagnostics).unwrap_or_default();
    let cwd = match object.get("cwd") {
        Some(value) => match value.as_str() {
            Some(raw) if !raw.trim().is_empty() => Some(raw.trim().to_owned()),
            Some(_) => None,
            None => {
                diagnostics.push(McpValidationDiagnostic::error(
                    "invalid_cwd",
                    "`cwd` must be a string",
                    Some(format!("$.mcpServers.{name}.cwd")),
                ));
                None
            }
        },
        None => None,
    };
    let env = secret_string_map_field(object, "env", name, "env", context, diagnostics, secrets)
        .unwrap_or_default();

    Some(McpTransportConfig::Stdio {
        command: command.trim().to_owned(),
        args,
        cwd,
        env,
        startup_timeout_ms,
        tool_timeout_ms,
    })
}

fn parse_http_transport(
    name: &str,
    object: &Map<String, Value>,
    context: &InstallParseContext,
    startup_timeout_ms: u64,
    tool_timeout_ms: u64,
    diagnostics: &mut Vec<McpValidationDiagnostic>,
    secrets: &mut Vec<McpSecretMaterialization>,
) -> Option<McpTransportConfig> {
    let url = string_field(object, "url");
    let Some(url) = url.filter(|value| !value.trim().is_empty()) else {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_url",
            "`url` must be a non-empty string",
            Some(format!("$.mcpServers.{name}.url")),
        ));
        return None;
    };

    match Url::parse(url.trim()) {
        Ok(parsed) if matches!(parsed.scheme(), "http" | "https") => {}
        _ => {
            diagnostics.push(McpValidationDiagnostic::error(
                "invalid_url",
                "`url` must be an http or https URL",
                Some(format!("$.mcpServers.{name}.url")),
            ));
            return None;
        }
    }

    let headers = secret_string_map_field(
        object,
        "headers",
        name,
        "header",
        context,
        diagnostics,
        secrets,
    )
    .unwrap_or_default();

    Some(McpTransportConfig::StreamableHttp {
        url: url.trim().to_owned(),
        headers,
        startup_timeout_ms,
        tool_timeout_ms,
    })
}

fn parse_enabled(
    name: &str,
    object: &Map<String, Value>,
    default_enabled: bool,
    diagnostics: &mut Vec<McpValidationDiagnostic>,
) -> bool {
    let enabled = bool_field(object, "enabled");
    let disabled = bool_field(object, "disabled");

    if object.contains_key("enabled") && enabled.is_none() {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_enabled_state",
            "`enabled` must be a boolean",
            Some(format!("$.mcpServers.{name}.enabled")),
        ));
    }
    if object.contains_key("disabled") && disabled.is_none() {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_enabled_state",
            "`disabled` must be a boolean",
            Some(format!("$.mcpServers.{name}.disabled")),
        ));
    }
    if enabled.is_some() && disabled.is_some() {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_enabled_state",
            "`enabled` and `disabled` must not both be set",
            Some(format!("$.mcpServers.{name}")),
        ));
    }

    enabled.unwrap_or_else(|| disabled.map(|value| !value).unwrap_or(default_enabled))
}

fn parse_allow_implicit_invocation(
    name: &str,
    object: &Map<String, Value>,
    default_allow_implicit_invocation: bool,
    diagnostics: &mut Vec<McpValidationDiagnostic>,
) -> bool {
    let allow_implicit_invocation = bool_field(object, "allow_implicit_invocation");
    if object.contains_key("allow_implicit_invocation") && allow_implicit_invocation.is_none() {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_implicit_state",
            "`allow_implicit_invocation` must be a boolean",
            Some(format!("$.mcpServers.{name}.allow_implicit_invocation")),
        ));
    }
    allow_implicit_invocation.unwrap_or(default_allow_implicit_invocation)
}

fn bool_field(object: &Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key).and_then(Value::as_bool)
}

fn string_field<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key).and_then(Value::as_str)
}

fn string_array_field(
    object: &Map<String, Value>,
    key: &str,
    server_name: &str,
    diagnostics: &mut Vec<McpValidationDiagnostic>,
) -> Option<Vec<String>> {
    let Some(value) = object.get(key) else {
        return None;
    };
    let Some(array) = value.as_array() else {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_args",
            format!("`{key}` must be an array of strings"),
            Some(format!("$.mcpServers.{server_name}.{key}")),
        ));
        return None;
    };

    let mut values = Vec::new();
    for (index, item) in array.iter().enumerate() {
        match item.as_str() {
            Some(value) => values.push(value.to_owned()),
            None => diagnostics.push(McpValidationDiagnostic::error(
                "invalid_args",
                format!("`{key}` must contain only strings"),
                Some(format!("$.mcpServers.{server_name}.{key}[{index}]")),
            )),
        }
    }
    Some(values)
}

fn secret_string_map_field(
    object: &Map<String, Value>,
    key: &str,
    server_name: &str,
    source: &str,
    context: &InstallParseContext,
    diagnostics: &mut Vec<McpValidationDiagnostic>,
    secrets: &mut Vec<McpSecretMaterialization>,
) -> Option<BTreeMap<String, McpConfigValue>> {
    let Some(value) = object.get(key) else {
        return None;
    };
    let Some(map) = value.as_object() else {
        diagnostics.push(McpValidationDiagnostic::error(
            if key == "env" {
                "invalid_env"
            } else {
                "invalid_headers"
            },
            format!("`{key}` must be an object of string values"),
            Some(format!("$.mcpServers.{server_name}.{key}")),
        ));
        return None;
    };

    let mut values = BTreeMap::new();
    for (entry_key, entry_value) in map {
        match entry_value.as_str() {
            Some(raw) => {
                let (config_value, _, materialization) = secret_ref_for(
                    &context.scope_kind,
                    context.scope_key.as_str(),
                    server_name,
                    source,
                    entry_key,
                    raw,
                );
                values.insert(entry_key.clone(), config_value);
                secrets.push(materialization);
            }
            None => diagnostics.push(McpValidationDiagnostic::error(
                if key == "env" {
                    "invalid_env"
                } else {
                    "invalid_headers"
                },
                format!("`{key}` values must be strings"),
                Some(format!("$.mcpServers.{server_name}.{key}.{entry_key}")),
            )),
        }
    }
    Some(values)
}

fn timeout_ms_field(
    object: &Map<String, Value>,
    key: &str,
    server_name: &str,
    diagnostics: &mut Vec<McpValidationDiagnostic>,
) -> Option<u64> {
    let value = object.get(key)?;
    let Some(seconds) = value.as_u64() else {
        diagnostics.push(McpValidationDiagnostic::error(
            "invalid_timeout",
            format!("`{key}` must be a positive integer number of seconds"),
            Some(format!("$.mcpServers.{server_name}.{key}")),
        ));
        return None;
    };
    Some(seconds.saturating_mul(1000))
}

fn source_ref(name: &str, transport: &McpTransportConfig) -> Value {
    match transport {
        McpTransportConfig::Stdio {
            command,
            args,
            cwd,
            env,
            ..
        } => {
            let redacted_env = env
                .keys()
                .map(|key| (key.clone(), crate::redaction::REDACTED_VALUE.to_owned()))
                .collect::<BTreeMap<_, _>>();
            json!({
                "source_kind": "config",
                "server": name,
                "transport": "stdio",
                "command": command,
                "args": args,
                "cwd": cwd,
                "env": redact_string_map(&redacted_env),
            })
        }
        McpTransportConfig::StreamableHttp { url, headers, .. } => {
            let redacted_headers = headers
                .keys()
                .map(|key| (key.clone(), crate::redaction::REDACTED_VALUE.to_owned()))
                .collect::<BTreeMap<_, _>>();
            json!({
                "source_kind": "config",
                "server": name,
                "transport": "streamable_http",
                "url": url,
                "headers": redact_string_map(&redacted_headers),
            })
        }
    }
}

fn secret_source(ref_id: &str) -> String {
    let mut parts = ref_id.rsplit(':');
    let _key = parts.next();
    parts.next().unwrap_or_default().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{McpDependencyKey, McpUnavailableReason};

    fn context() -> InstallParseContext {
        InstallParseContext {
            scope_kind: McpScopeKind::Workspace,
            scope_key: "w1".to_owned(),
            default_enabled: true,
            default_allow_implicit_invocation: true,
        }
    }

    #[test]
    fn parses_stdio_sample_without_leaking_secret() {
        let input = r#"{
          "mcpServers": {
            "resend": {
              "command": "npx",
              "args": ["-y", "resend-mcp"],
              "env": {"RESEND_API_KEY": "re_xxxxxxxxx"}
            }
          }
        }"#;
        let plan = parse_install_config(input, context()).unwrap();
        assert_eq!(plan.items.len(), 1);
        let item = &plan.items[0];
        assert!(item.is_valid());
        let installation = item.installation.as_ref().unwrap();
        assert_eq!(installation.transport_kind(), "stdio");
        assert_eq!(item.secrets.len(), 1);
        let transport_json = serde_json::to_string(&installation.transport).unwrap();
        let source_json = serde_json::to_string(&installation.source_ref).unwrap();
        let diagnostics_json = serde_json::to_string(&item.diagnostics).unwrap();
        assert!(!transport_json.contains("re_xxxxxxxxx"));
        assert!(!source_json.contains("re_xxxxxxxxx"));
        assert!(!diagnostics_json.contains("re_xxxxxxxxx"));
    }

    #[test]
    fn parses_http_sample_without_leaking_secret() {
        let input = r#"{
          "mcpServers": {
            "resend": {
              "url": "http://127.0.0.1:3000/mcp",
              "headers": {"Authorization": "Bearer re_xxxxxxxxx"}
            }
          }
        }"#;
        let plan = parse_install_config(input, context()).unwrap();
        let installation = plan.items[0].installation.as_ref().unwrap();
        assert_eq!(installation.transport_kind(), "streamable_http");
        let transport_json = serde_json::to_string(&installation.transport).unwrap();
        let source_json = serde_json::to_string(&installation.source_ref).unwrap();
        assert!(!transport_json.contains("re_xxxxxxxxx"));
        assert!(!source_json.contains("re_xxxxxxxxx"));
    }

    #[test]
    fn parses_multiple_servers_and_itemizes_invalid_server() {
        let input = r#"{
          "mcpServers": {
            "bad name": {"command": "npx"},
            "resend": {"command": "npx"}
          }
        }"#;
        let plan = parse_install_config(input, context()).unwrap();
        assert_eq!(plan.items.len(), 2);
        assert!(
            plan.items
                .iter()
                .any(|item| item.name == "resend" && item.is_valid())
        );
        assert!(plan.items.iter().any(|item| {
            item.name == "bad name"
                && !item.is_valid()
                && item
                    .diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.code == "invalid_server_name")
        }));
    }

    #[test]
    fn rejects_missing_mcp_servers() {
        let error = parse_install_config("{}", context()).unwrap_err();
        assert_eq!(error.diagnostic.code, "missing_mcp_servers");
    }

    #[test]
    fn rejects_invalid_json_document() {
        let error = parse_install_config("{", context()).unwrap_err();
        assert_eq!(error.diagnostic.code, "invalid_json");
    }

    #[test]
    fn rejects_ambiguous_transport() {
        let input = r#"{"mcpServers":{"resend":{"command":"npx","url":"http://x.test"}}}"#;
        let plan = parse_install_config(input, context()).unwrap();
        assert_eq!(plan.items[0].diagnostics[0].code, "ambiguous_transport");
    }

    #[test]
    fn rejects_missing_transport() {
        let input = r#"{"mcpServers":{"resend":{}}}"#;
        let plan = parse_install_config(input, context()).unwrap();
        assert_eq!(plan.items[0].diagnostics[0].code, "missing_transport");
    }

    #[test]
    fn rejects_invalid_command() {
        let input = r#"{"mcpServers":{"resend":{"command":""}}}"#;
        let plan = parse_install_config(input, context()).unwrap();
        assert_eq!(plan.items[0].diagnostics[0].code, "invalid_command");
    }

    #[test]
    fn rejects_invalid_shapes() {
        let input = r#"{
          "mcpServers": {
            "resend": {
              "command": "npx",
              "args": "bad",
              "env": []
            }
          }
        }"#;
        let plan = parse_install_config(input, context()).unwrap();
        let codes = plan.items[0]
            .diagnostics
            .iter()
            .map(|item| item.code.as_str())
            .collect::<Vec<_>>();
        assert!(codes.contains(&"invalid_args"));
        assert!(codes.contains(&"invalid_env"));
    }

    #[test]
    fn rejects_invalid_url() {
        let input = r#"{"mcpServers":{"resend":{"url":"ftp://example.com/mcp"}}}"#;
        let plan = parse_install_config(input, context()).unwrap();
        assert_eq!(plan.items[0].diagnostics[0].code, "invalid_url");
    }

    #[test]
    fn rejects_invalid_headers_shape() {
        let input = r#"{"mcpServers":{"resend":{"url":"https://example.com/mcp","headers":[]}}}"#;
        let plan = parse_install_config(input, context()).unwrap();
        assert_eq!(plan.items[0].diagnostics[0].code, "invalid_headers");
    }

    #[test]
    fn rejects_enabled_disabled_conflict_and_invalid_timeout() {
        let input = r#"{
          "mcpServers": {
            "resend": {
              "command": "npx",
              "enabled": true,
              "disabled": false,
              "timeout": "slow"
            }
          }
        }"#;
        let plan = parse_install_config(input, context()).unwrap();
        let codes = plan.items[0]
            .diagnostics
            .iter()
            .map(|item| item.code.as_str())
            .collect::<Vec<_>>();
        assert!(codes.contains(&"invalid_enabled_state"));
        assert!(codes.contains(&"invalid_timeout"));
    }

    #[test]
    fn applies_normalization_defaults_for_stdio_and_http() {
        let stdio =
            parse_install_config(r#"{"mcpServers":{"a":{"command":"npx"}}}"#, context()).unwrap();
        let stdio_installation = stdio.items[0].installation.as_ref().unwrap();
        match &stdio_installation.transport {
            McpTransportConfig::Stdio {
                args,
                env,
                startup_timeout_ms,
                tool_timeout_ms,
                ..
            } => {
                assert!(args.is_empty());
                assert!(env.is_empty());
                assert_eq!(*startup_timeout_ms, DEFAULT_STARTUP_TIMEOUT_MS);
                assert_eq!(*tool_timeout_ms, DEFAULT_TOOL_TIMEOUT_MS);
            }
            other => panic!("expected stdio transport, got {other:?}"),
        }

        let http = parse_install_config(
            r#"{"mcpServers":{"b":{"url":"https://example.com/mcp"}}}"#,
            context(),
        )
        .unwrap();
        let http_installation = http.items[0].installation.as_ref().unwrap();
        match &http_installation.transport {
            McpTransportConfig::StreamableHttp {
                headers,
                startup_timeout_ms,
                tool_timeout_ms,
                ..
            } => {
                assert!(headers.is_empty());
                assert_eq!(*startup_timeout_ms, DEFAULT_STARTUP_TIMEOUT_MS);
                assert_eq!(*tool_timeout_ms, DEFAULT_TOOL_TIMEOUT_MS);
            }
            other => panic!("expected streamable_http transport, got {other:?}"),
        }
    }

    #[test]
    fn applies_custom_timeouts_and_disabled_flag() {
        let input = r#"{
          "mcpServers": {
            "resend": {
              "command": "npx",
              "startup_timeout_sec": 2,
              "tool_timeout_sec": 7,
              "disabled": true,
              "allow_implicit_invocation": false
            }
          }
        }"#;
        let plan = parse_install_config(input, context()).unwrap();
        let installation = plan.items[0].installation.as_ref().unwrap();
        assert!(!installation.enabled);
        assert!(!installation.allow_implicit_invocation);
        match &installation.transport {
            McpTransportConfig::Stdio {
                startup_timeout_ms,
                tool_timeout_ms,
                ..
            } => {
                assert_eq!(*startup_timeout_ms, 2_000);
                assert_eq!(*tool_timeout_ms, 7_000);
            }
            other => panic!("expected stdio transport, got {other:?}"),
        }
    }

    #[test]
    fn fingerprint_is_stable_and_ignores_raw_secret_values() {
        let first = parse_install_config(
            r#"{"mcpServers":{"resend":{"env":{"RESEND_API_KEY":"re_first"},"args":["-y","resend-mcp"],"command":"npx"}}}"#,
            context(),
        )
        .unwrap();
        let second = parse_install_config(
            r#"{"mcpServers":{"resend":{"command":"npx","args":["-y","resend-mcp"],"env":{"RESEND_API_KEY":"re_second"}}}}"#,
            context(),
        )
        .unwrap();

        let first_installation = first.items[0].installation.as_ref().unwrap();
        let second_installation = second.items[0].installation.as_ref().unwrap();
        assert_eq!(
            first_installation.fingerprint,
            second_installation.fingerprint
        );
        assert!(!first_installation.fingerprint.contains("re_first"));
        assert!(!second_installation.fingerprint.contains("re_second"));
    }

    #[test]
    fn availability_maps_enabled_and_disabled_servers() {
        let input = r#"{"mcpServers":{"resend":{"command":"npx","disabled":true}}}"#;
        let plan = parse_install_config(input, context()).unwrap();
        let snapshot = plan.availability_snapshot();
        assert_eq!(snapshot.blocked.len(), 1);
        assert_eq!(
            snapshot
                .blocked
                .get(&McpDependencyKey::Server {
                    server: "resend".to_owned()
                })
                .unwrap(),
            &McpUnavailableReason::Disabled
        );

        let input = r#"{"mcpServers":{"github":{"command":"npx"}}}"#;
        let plan = parse_install_config(input, context()).unwrap();
        let snapshot = plan.availability_snapshot();
        assert_eq!(
            snapshot
                .blocked
                .get(&McpDependencyKey::Server {
                    server: "github".to_owned()
                })
                .unwrap(),
            &McpUnavailableReason::NotStarted
        );
    }
}
