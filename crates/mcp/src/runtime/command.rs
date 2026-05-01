use crate::domain::{McpConfigValue, McpTransportConfig};
use crate::runtime::{McpRuntimeError, McpSecretResolver};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedStdioTransport {
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub env: BTreeMap<String, String>,
    pub startup_timeout_ms: u64,
    pub tool_timeout_ms: u64,
    pub secrets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedHttpTransport {
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub startup_timeout_ms: u64,
    pub tool_timeout_ms: u64,
    pub secrets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializedTransport {
    Stdio(MaterializedStdioTransport),
    StreamableHttp(MaterializedHttpTransport),
}

pub fn materialize_transport(
    transport: &McpTransportConfig,
    resolver: &dyn McpSecretResolver,
) -> Result<MaterializedTransport, McpRuntimeError> {
    match transport {
        McpTransportConfig::Stdio {
            command,
            args,
            cwd,
            env,
            startup_timeout_ms,
            tool_timeout_ms,
        } => {
            let (env, secrets) = materialize_values(env, resolver, "stdio env")?;
            Ok(MaterializedTransport::Stdio(MaterializedStdioTransport {
                command: command.clone(),
                args: args.clone(),
                cwd: cwd.clone(),
                env,
                startup_timeout_ms: *startup_timeout_ms,
                tool_timeout_ms: *tool_timeout_ms,
                secrets,
            }))
        }
        McpTransportConfig::StreamableHttp {
            url,
            headers,
            startup_timeout_ms,
            tool_timeout_ms,
        } => {
            let (headers, secrets) = materialize_values(headers, resolver, "HTTP header")
                .map_err(|error| McpRuntimeError::auth_required(error.message))?;
            Ok(MaterializedTransport::StreamableHttp(
                MaterializedHttpTransport {
                    url: url.clone(),
                    headers,
                    startup_timeout_ms: *startup_timeout_ms,
                    tool_timeout_ms: *tool_timeout_ms,
                    secrets,
                },
            ))
        }
    }
}

fn materialize_values(
    values: &BTreeMap<String, McpConfigValue>,
    resolver: &dyn McpSecretResolver,
    context: &str,
) -> Result<(BTreeMap<String, String>, Vec<String>), McpRuntimeError> {
    let mut materialized = BTreeMap::new();
    let mut secrets = Vec::new();

    for (key, value) in values {
        let resolved = match value {
            McpConfigValue::Literal { value } => value.clone(),
            McpConfigValue::SecretRef { ref_id } => {
                resolver.resolve_mcp_secret(ref_id).ok_or_else(|| {
                    McpRuntimeError::failed(format!("missing secret for {context} `{key}`"))
                })?
            }
        };
        if matches!(value, McpConfigValue::SecretRef { .. }) {
            secrets.push(resolved.clone());
        }
        materialized.insert(key.clone(), resolved);
    }

    Ok((materialized, secrets))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::McpTransportConfig;
    use async_trait::async_trait;
    use std::collections::HashMap;

    struct StaticResolver(HashMap<String, String>);

    #[async_trait]
    impl McpSecretResolver for StaticResolver {
        fn resolve_mcp_secret(&self, ref_id: &str) -> Option<String> {
            self.0.get(ref_id).cloned()
        }
    }

    #[test]
    fn stdio_env_secret_refs_resolve() {
        let mut env = BTreeMap::new();
        env.insert(
            "TOKEN".to_owned(),
            McpConfigValue::SecretRef {
                ref_id: "secret".to_owned(),
            },
        );
        let transport = McpTransportConfig::Stdio {
            command: "node".to_owned(),
            args: Vec::new(),
            cwd: None,
            env,
            startup_timeout_ms: 1_000,
            tool_timeout_ms: 1_000,
        };
        let resolver = StaticResolver(HashMap::from([("secret".to_owned(), "value".to_owned())]));

        let materialized = materialize_transport(&transport, &resolver).unwrap();

        match materialized {
            MaterializedTransport::Stdio(stdio) => {
                assert_eq!(stdio.env.get("TOKEN").map(String::as_str), Some("value"));
                assert_eq!(stdio.secrets, vec!["value".to_owned()]);
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    #[test]
    fn missing_http_header_ref_becomes_auth_required() {
        let mut headers = BTreeMap::new();
        headers.insert(
            "Authorization".to_owned(),
            McpConfigValue::SecretRef {
                ref_id: "missing".to_owned(),
            },
        );
        let transport = McpTransportConfig::StreamableHttp {
            url: "http://127.0.0.1/mcp".to_owned(),
            headers,
            startup_timeout_ms: 1_000,
            tool_timeout_ms: 1_000,
        };
        let resolver = StaticResolver(HashMap::new());

        let error = materialize_transport(&transport, &resolver).unwrap_err();

        assert_eq!(error.state, crate::domain::McpRuntimeState::AuthRequired);
        assert!(error.message.contains("missing secret"));
    }
}
