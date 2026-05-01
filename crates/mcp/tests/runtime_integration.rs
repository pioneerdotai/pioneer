use axum::extract::Json;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use pioneer_mcp::{
    McpAuthConfig, McpRuntimeConnector, McpRuntimeState, McpScopeKind, McpSecretResolver,
    McpServerInstallation, McpSourceKind, McpTransportConfig, RmcpRuntimeConnector,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

struct EmptyResolver;

impl McpSecretResolver for EmptyResolver {
    fn resolve_mcp_secret(&self, _ref_id: &str) -> Option<String> {
        None
    }
}

#[tokio::test]
async fn stdio_server_reaches_ready_and_cleans_up() -> anyhow::Result<()> {
    let Some(python) = python_command() else {
        eprintln!("skipping stdio MCP integration test: python3 not found");
        return Ok(());
    };
    let script = write_stdio_fixture()?;
    let installation = installation(McpTransportConfig::Stdio {
        command: python,
        args: vec![script.display().to_string()],
        cwd: None,
        env: BTreeMap::new(),
        startup_timeout_ms: 5_000,
        tool_timeout_ms: 5_000,
    });

    let connector = RmcpRuntimeConnector::new();
    let mut session = connector
        .connect(
            installation,
            "stdio-fixture".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await?;

    assert_eq!(session.initial_catalog().tools_count(), 2);
    let result = session
        .call_tool("send", json!({"message":"hello"}))
        .await?;
    assert!(!result.is_error);
    assert_eq!(
        result
            .structured_content
            .as_ref()
            .and_then(|value| value.get("ok")),
        Some(&json!(true))
    );
    session.shutdown().await;
    let _ = fs::remove_file(script);
    Ok(())
}

#[tokio::test]
async fn streamable_http_server_reaches_ready() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let app = axum::Router::new().route("/mcp", post(mcp_http_handler));
        let _ = axum::serve(listener, app).await;
    });
    let installation = installation(McpTransportConfig::StreamableHttp {
        url: format!("http://{addr}/mcp"),
        headers: BTreeMap::new(),
        startup_timeout_ms: 5_000,
        tool_timeout_ms: 5_000,
    });

    let connector = RmcpRuntimeConnector::new();
    let mut session = connector
        .connect(
            installation,
            "http-fixture".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await?;

    assert_eq!(session.initial_catalog().tools_count(), 2);
    let result = session
        .call_tool("send", json!({"message":"hello"}))
        .await?;
    assert!(!result.is_error);
    assert_eq!(
        result
            .structured_content
            .as_ref()
            .and_then(|value| value.get("ok")),
        Some(&json!(true))
    );
    session.shutdown().await;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_command_not_found_is_failed() {
    let installation = installation(McpTransportConfig::Stdio {
        command: "definitely-missing-pioneer-mcp-fixture-binary".to_owned(),
        args: Vec::new(),
        cwd: None,
        env: BTreeMap::new(),
        startup_timeout_ms: 300,
        tool_timeout_ms: 300,
    });

    let connector = RmcpRuntimeConnector::new();
    let result = connector
        .connect(
            installation,
            "stdio-failed".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await;
    let Err(error) = result else {
        panic!("missing stdio command should fail during spawn");
    };

    assert_eq!(error.state, McpRuntimeState::Failed);
}

#[tokio::test]
async fn streamable_http_auth_failure_is_auth_required() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let app = axum::Router::new().route("/mcp", post(auth_required_handler));
        let _ = axum::serve(listener, app).await;
    });
    let installation = installation(McpTransportConfig::StreamableHttp {
        url: format!("http://{addr}/mcp"),
        headers: BTreeMap::new(),
        startup_timeout_ms: 1_000,
        tool_timeout_ms: 1_000,
    });

    let connector = RmcpRuntimeConnector::new();
    let result = connector
        .connect(
            installation,
            "http-auth".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await;
    let Err(error) = result else {
        panic!("auth server should fail during initialize");
    };

    assert_eq!(
        error.state,
        McpRuntimeState::AuthRequired,
        "unexpected error: {}",
        error.message
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn optional_catalog_failure_marks_session_degraded() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let app = axum::Router::new().route("/mcp", post(degraded_http_handler));
        let _ = axum::serve(listener, app).await;
    });
    let installation = installation(McpTransportConfig::StreamableHttp {
        url: format!("http://{addr}/mcp"),
        headers: BTreeMap::new(),
        startup_timeout_ms: 5_000,
        tool_timeout_ms: 5_000,
    });

    let connector = RmcpRuntimeConnector::new();
    let mut session = connector
        .connect(
            installation,
            "http-degraded".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await?;

    assert_eq!(session.initial_catalog().tools_count(), 2);
    assert!(session.degraded_reason().is_some());
    session.shutdown().await;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn optional_catalog_method_not_found_is_not_degraded() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let app = axum::Router::new().route("/mcp", post(method_not_found_optional_http_handler));
        let _ = axum::serve(listener, app).await;
    });
    let installation = installation(McpTransportConfig::StreamableHttp {
        url: format!("http://{addr}/mcp"),
        headers: BTreeMap::new(),
        startup_timeout_ms: 5_000,
        tool_timeout_ms: 5_000,
    });

    let connector = RmcpRuntimeConnector::new();
    let mut session = connector
        .connect(
            installation,
            "http-optional-unsupported".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await?;

    assert_eq!(session.initial_catalog().tools_count(), 2);
    assert_eq!(session.initial_catalog().resources_count(), 0);
    assert_eq!(session.initial_catalog().resource_templates_count(), 0);
    assert_eq!(session.initial_catalog().prompts_count(), 0);
    assert!(session.degraded_reason().is_none());
    session.shutdown().await;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn streamable_http_unavailable_url_is_failed() {
    let installation = installation(McpTransportConfig::StreamableHttp {
        url: "http://127.0.0.1:9/mcp".to_owned(),
        headers: BTreeMap::new(),
        startup_timeout_ms: 300,
        tool_timeout_ms: 300,
    });

    let connector = RmcpRuntimeConnector::new();
    let result = connector
        .connect(
            installation,
            "http-failed".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await;
    let Err(error) = result else {
        panic!("unavailable server should fail during initialize");
    };

    assert_eq!(error.state, McpRuntimeState::Failed);
}

async fn degraded_http_handler(Json(body): Json<Value>) -> Response {
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let method = body
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if method == "prompts/list" {
        return Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32000, "message": "prompts unavailable"}
        }))
        .into_response();
    }
    mcp_http_handler(Json(body)).await
}

async fn method_not_found_optional_http_handler(Json(body): Json<Value>) -> Response {
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let method = body
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if matches!(
        method,
        "resources/list" | "resources/templates/list" | "prompts/list"
    ) {
        return Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": "Method not found"}
        }))
        .into_response();
    }
    mcp_http_handler(Json(body)).await
}

async fn mcp_http_handler(Json(body): Json<Value>) -> Response {
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let method = body
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match method {
        "initialize" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": initialize_result()
        }))
        .into_response(),
        "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
        "tools/list" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": tools_list_result()
        }))
        .into_response(),
        "resources/list" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {"resources": []}
        }))
        .into_response(),
        "resources/templates/list" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {"resourceTemplates": []}
        }))
        .into_response(),
        "prompts/list" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {"prompts": []}
        }))
        .into_response(),
        "tools/call" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": tool_call_result(body.get("params").cloned().unwrap_or(Value::Null))
        }))
        .into_response(),
        _ => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": "method not found"}
        }))
        .into_response(),
    }
}

async fn auth_required_handler() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::WWW_AUTHENTICATE,
        "Bearer".parse().unwrap(),
    );
    (StatusCode::UNAUTHORIZED, headers).into_response()
}

fn installation(transport: McpTransportConfig) -> McpServerInstallation {
    McpServerInstallation {
        scope_kind: McpScopeKind::Workspace,
        scope_key: "workspace".to_owned(),
        name: "fixture".to_owned(),
        display_name: None,
        source_kind: McpSourceKind::Config,
        source_ref: json!({"kind":"test"}),
        transport,
        auth: McpAuthConfig::default(),
        secret_refs: Vec::new(),
        enabled: true,
        allow_implicit_invocation: true,
        required: false,
        fingerprint: "fixture-fingerprint".to_owned(),
    }
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {"listChanged": true},
            "resources": {"listChanged": true},
            "prompts": {"listChanged": true}
        },
        "serverInfo": {
            "name": "fixture",
            "version": "1.0.0"
        }
    })
}

fn tools_list_result() -> Value {
    json!({
        "tools": [
            {
                "name": "send",
                "description": "Send a message",
                "inputSchema": {"type": "object", "properties": {}}
            },
            {
                "name": "domains",
                "description": "List domains",
                "inputSchema": {"type": "object", "properties": {}}
            }
        ]
    })
}

fn tool_call_result(params: Value) -> Value {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    json!({
        "content": [{"type": "text", "text": format!("called {name}")}],
        "structuredContent": {"ok": true, "tool": name},
        "isError": false
    })
}

fn python_command() -> Option<String> {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|_| "python3".to_owned())
}

fn write_stdio_fixture() -> anyhow::Result<PathBuf> {
    let path = std::env::temp_dir().join(format!(
        "pioneer-mcp-stdio-fixture-{}.py",
        unix_timestamp_secs()
    ));
    fs::write(
        &path,
        r#"
import json
import sys

def send(value):
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    request = json.loads(line)
    method = request.get("method")
    request_id = request.get("id")
    if method == "initialize":
        send({"jsonrpc":"2.0","id":request_id,"result":{
            "protocolVersion":"2024-11-05",
            "capabilities":{"tools":{"listChanged":True},"resources":{"listChanged":True},"prompts":{"listChanged":True}},
            "serverInfo":{"name":"fixture","version":"1.0.0"}
        }})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send({"jsonrpc":"2.0","id":request_id,"result":{
            "tools":[
                {"name":"send","description":"Send a message","inputSchema":{"type":"object","properties":{}}},
                {"name":"domains","description":"List domains","inputSchema":{"type":"object","properties":{}}}
            ]
        }})
    elif method == "resources/list":
        send({"jsonrpc":"2.0","id":request_id,"result":{"resources":[]}})
    elif method == "resources/templates/list":
        send({"jsonrpc":"2.0","id":request_id,"result":{"resourceTemplates":[]}})
    elif method == "prompts/list":
        send({"jsonrpc":"2.0","id":request_id,"result":{"prompts":[]}})
    elif method == "tools/call":
        params = request.get("params") or {}
        name = params.get("name")
        send({"jsonrpc":"2.0","id":request_id,"result":{
            "content":[{"type":"text","text":"called " + str(name)}],
            "structuredContent":{"ok":True,"tool":name},
            "isError":False
        }})
    else:
        send({"jsonrpc":"2.0","id":request_id,"error":{"code":-32601,"message":"method not found"}})
"#,
    )?;
    Ok(path)
}

fn unix_timestamp_secs() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}
