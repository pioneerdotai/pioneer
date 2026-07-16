use axum::extract::{Json, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use pioneer_mcp::{
    McpAuthConfig, McpRuntimeConnector, McpRuntimeErrorKind, McpRuntimeState, McpScopeKind,
    McpSecretResolver, McpServerInstallation, McpSourceKind, McpTransportConfig,
    RmcpRuntimeConnector,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

struct EmptyResolver;

#[derive(Default)]
struct CancellationFixture {
    call_started: tokio::sync::Notify,
    cancellation_received: tokio::sync::Notify,
    release_call: tokio::sync::Notify,
}

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
        .call_tool(
            "send",
            json!({"message":"hello"}),
            Duration::from_secs(5),
            CancellationToken::new(),
        )
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
        .call_tool(
            "send",
            json!({"message":"hello"}),
            Duration::from_secs(5),
            CancellationToken::new(),
        )
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
async fn streamable_http_tool_call_cancellation_reaches_upstream() -> anyhow::Result<()> {
    let fixture = Arc::new(CancellationFixture::default());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_fixture = fixture.clone();
    let server = tokio::spawn(async move {
        let app = axum::Router::new()
            .route("/mcp", post(cancellation_http_handler))
            .with_state(server_fixture);
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
            "http-cancellation-fixture".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await?;
    let cancellation = CancellationToken::new();
    let cancellation_trigger = cancellation.clone();
    let fixture_for_trigger = fixture.clone();
    let trigger = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture_for_trigger.call_started.notified(),
        )
        .await
        .expect("MCP tools/call should reach the HTTP fixture");
        cancellation_trigger.cancel();
    });

    let error = session
        .call_tool(
            "send",
            json!({"message":"cancel me"}),
            Duration::from_secs(5),
            cancellation,
        )
        .await
        .expect_err("cancelled MCP tools/call must not complete successfully");
    trigger.await?;
    assert_eq!(error.kind, McpRuntimeErrorKind::Cancelled);
    fixture.release_call.notify_one();
    let follow_up = tokio::time::timeout(
        Duration::from_secs(1),
        session.call_tool(
            "domains",
            json!({}),
            Duration::from_secs(1),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("cancelled HTTP transport should reject follow-up work promptly");
    assert!(follow_up.is_err());
    session.shutdown().await;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_tool_call_cancellation_notification_reaches_upstream() -> anyhow::Result<()> {
    let Some(python) = python_command() else {
        eprintln!("skipping stdio MCP cancellation test: python3 not found");
        return Ok(());
    };
    let (script, marker) = write_stdio_cancellation_fixture()?;
    let installation = installation(McpTransportConfig::Stdio {
        command: python,
        args: vec![script.display().to_string(), marker.display().to_string()],
        cwd: None,
        env: BTreeMap::new(),
        startup_timeout_ms: 5_000,
        tool_timeout_ms: 5_000,
    });
    let connector = RmcpRuntimeConnector::new();
    let mut session = connector
        .connect(
            installation,
            "stdio-cancellation-fixture".to_owned(),
            Arc::new(EmptyResolver),
            unix_timestamp_secs(),
        )
        .await?;
    let cancellation = CancellationToken::new();
    let cancellation_trigger = cancellation.clone();
    let marker_for_trigger = marker.clone();
    let trigger = tokio::spawn(async move {
        wait_for_marker(&marker_for_trigger, "started").await;
        cancellation_trigger.cancel();
    });

    let error = session
        .call_tool(
            "send",
            json!({"message":"cancel me"}),
            Duration::from_secs(5),
            cancellation,
        )
        .await
        .expect_err("cancelled stdio MCP tools/call must not succeed");
    trigger.await?;
    wait_for_marker(&marker, "cancelled").await;
    assert_eq!(error.kind, McpRuntimeErrorKind::Cancelled);

    let follow_up = session
        .call_tool(
            "domains",
            json!({}),
            Duration::from_secs(2),
            CancellationToken::new(),
        )
        .await?;
    assert!(!follow_up.is_error);

    session.shutdown().await;
    let _ = fs::remove_file(script);
    let _ = fs::remove_file(marker);
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

async fn cancellation_http_handler(
    State(fixture): State<Arc<CancellationFixture>>,
    Json(body): Json<Value>,
) -> Response {
    let method = body
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match method {
        "tools/call" => {
            fixture.call_started.notify_one();
            fixture.release_call.notified().await;
            Json(json!({
                "jsonrpc": "2.0",
                "id": body.get("id").cloned().unwrap_or(Value::Null),
                "result": tool_call_result(body.get("params").cloned().unwrap_or(Value::Null))
            }))
            .into_response()
        }
        "notifications/cancelled" => {
            fixture.cancellation_received.notify_one();
            fixture.release_call.notify_one();
            StatusCode::ACCEPTED.into_response()
        }
        _ => mcp_http_handler(Json(body)).await,
    }
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

fn write_stdio_cancellation_fixture() -> anyhow::Result<(PathBuf, PathBuf)> {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    );
    let script = std::env::temp_dir().join(format!("pioneer-mcp-cancel-{unique}.py"));
    let marker = std::env::temp_dir().join(format!("pioneer-mcp-cancel-{unique}.marker"));
    fs::write(
        &script,
        r#"
import json
import pathlib
import sys

marker = pathlib.Path(sys.argv[1])

def mark(value):
    with marker.open("a", encoding="utf-8") as output:
        output.write(value + "\n")
        output.flush()

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
            "serverInfo":{"name":"cancellation-fixture","version":"1.0.0"}
        }})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send({"jsonrpc":"2.0","id":request_id,"result":{"tools":[
            {"name":"send","description":"Pause until cancelled","inputSchema":{"type":"object","properties":{}}},
            {"name":"domains","description":"Follow-up health check","inputSchema":{"type":"object","properties":{}}}
        ]}})
    elif method == "resources/list":
        send({"jsonrpc":"2.0","id":request_id,"result":{"resources":[]}})
    elif method == "resources/templates/list":
        send({"jsonrpc":"2.0","id":request_id,"result":{"resourceTemplates":[]}})
    elif method == "prompts/list":
        send({"jsonrpc":"2.0","id":request_id,"result":{"prompts":[]}})
    elif method == "tools/call":
        params = request.get("params") or {}
        name = params.get("name")
        if name == "send":
            mark("started")
            continue
        send({"jsonrpc":"2.0","id":request_id,"result":{
            "content":[{"type":"text","text":"called " + str(name)}],
            "structuredContent":{"ok":True,"tool":name},
            "isError":False
        }})
    elif method == "notifications/cancelled":
        mark("cancelled")
    else:
        send({"jsonrpc":"2.0","id":request_id,"error":{"code":-32601,"message":"method not found"}})
"#,
    )?;
    Ok((script, marker))
}

async fn wait_for_marker(path: &std::path::Path, expected: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let contents = fs::read_to_string(path).unwrap_or_default();
        if contents.lines().any(|line| line == expected) {
            return;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "timed out waiting for marker `{expected}`"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn unix_timestamp_secs() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}
