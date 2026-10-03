use async_trait::async_trait;
use axum::{
    Router,
    extract::{Form, Json, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine;
use pioneer_keystore::{
    MemorySecretStore, SecretEntryMeta, SecretFilter, SecretId, SecretMeta, SecretStore,
};
use pioneer_mcp::{
    McpAuthConfig, McpInvocationBudget, McpOAuthConfig, McpOAuthProvider, McpRuntimeConnector,
    McpRuntimeState, McpScopeKind, McpSecretResolver, McpServerInstallation, McpSourceKind,
    McpTransportConfig, RmcpRuntimeConnector,
};
use pioneer_mcp_oauth::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
const REDIRECT: &str = "http://127.0.0.1:37643/oauth/mcp/callback";
struct Sink {
    events: Mutex<Vec<OAuthEvent>>,
    available: AtomicBool,
}
#[async_trait]
impl OAuthEventSink for Sink {
    async fn client_available(&self, _: u64, _: &str) -> bool {
        self.available.load(Ordering::Relaxed)
    }
    async fn emit(&self, event: OAuthEvent) {
        self.events.lock().unwrap().push(event);
    }
}
impl Sink {
    async fn browser(&self) -> OAuthEvent {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(e) = self
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|e| e.authorization_url.is_some())
                    .cloned()
                {
                    return e;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("browser effect")
    }
    async fn wait_state(&self, flow: Option<&str>, state: OAuthState) -> OAuthEvent {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(event) = self
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|event| {
                        (flow.is_none() || event.flow_id.as_deref() == flow) && event.state == state
                    })
                    .cloned()
                {
                    return event;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("final OAuth event")
    }
    fn browsers(&self) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.authorization_url.is_some())
            .count()
    }
}
struct Fake {
    base: String,
    protected: AtomicBool,
    registrations: AtomicUsize,
    registration_unavailable: AtomicBool,
    registration_delay: AtomicBool,
    exchanges: AtomicUsize,
    refreshes: AtomicUsize,
    token_mode: AtomicUsize,
    token_delay: AtomicBool,
    preregistered_post: AtomicBool,
    tools_executed: AtomicUsize,
    discovery_mode: AtomicUsize,
    authorization_server: Mutex<Option<String>>,
    discovery_headers: Mutex<Vec<HeaderMap>>,
    deny_mcp: AtomicUsize,
    resources: AtomicBool,
    stateful: AtomicBool,
    expected_refresh: Mutex<String>,
    requests: Mutex<Vec<(HeaderMap, HashMap<String, String>)>>,
    dcr: Mutex<Vec<Value>>,
}
struct Server {
    data: Arc<Fake>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(protected: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let data = Arc::new(Fake {
            base,
            protected: AtomicBool::new(protected),
            registrations: AtomicUsize::new(0),
            registration_unavailable: AtomicBool::new(false),
            registration_delay: AtomicBool::new(false),
            exchanges: AtomicUsize::new(0),
            refreshes: AtomicUsize::new(0),
            token_mode: AtomicUsize::new(0),
            token_delay: AtomicBool::new(false),
            preregistered_post: AtomicBool::new(false),
            tools_executed: AtomicUsize::new(0),
            discovery_mode: AtomicUsize::new(0),
            authorization_server: Mutex::new(None),
            discovery_headers: Mutex::new(vec![]),
            deny_mcp: AtomicUsize::new(0),
            resources: AtomicBool::new(false),
            stateful: AtomicBool::new(false),
            expected_refresh: Mutex::new("refresh-0".into()),
            requests: Mutex::new(vec![]),
            dcr: Mutex::new(vec![]),
        });
        let router = Router::new()
            .route(
                "/.well-known/oauth-protected-resource/mcp",
                get(resource_metadata),
            )
            .route(
                "/.well-known/oauth-protected-resource",
                get(resource_metadata),
            )
            .route("/.well-known/oauth-authorization-server", get(as_metadata))
            .route("/register", post(register))
            .route("/token", post(token))
            .route("/mcp", post(mcp).get(mcp_stream))
            .route(
                "/alias",
                post(|| async { axum::response::Redirect::temporary("/mcp") }),
            )
            .with_state(data.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { data, task }
    }
    fn installation(&self) -> McpServerInstallation {
        McpServerInstallation {
            scope_kind: McpScopeKind::Workspace,
            scope_key: "workspace".into(),
            name: "test".into(),
            display_name: None,
            source_kind: McpSourceKind::Config,
            source_ref: json!({}),
            transport: McpTransportConfig::StreamableHttp {
                url: format!("{}/mcp", self.data.base),
                headers: BTreeMap::new(),
                startup_timeout_ms: 3000,
                tool_timeout_ms: 3000,
            },
            auth: McpAuthConfig::default(),
            secret_refs: vec![],
            enabled: true,
            allow_implicit_invocation: false,
            required: false,
            fingerprint: "stable-config".into(),
        }
    }
}
async fn resource_metadata(State(s): State<Arc<Fake>>) -> Json<Value> {
    Json(
        json!({"resource":format!("{}/mcp",s.base),"authorization_servers":[s.authorization_server.lock().unwrap().clone().unwrap_or_else(||s.base.clone())],"scopes_supported":["read","write"]}),
    )
}
async fn as_metadata(State(s): State<Arc<Fake>>, headers: HeaderMap) -> Response {
    s.discovery_headers.lock().unwrap().push(headers);
    match s.discovery_mode.load(Ordering::SeqCst) {
        1 => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        2 => return StatusCode::NOT_FOUND.into_response(),
        _ => {}
    }
    Json(json!({"issuer":s.base,"authorization_endpoint":format!("{}/authorize",s.base),"token_endpoint":format!("{}/token",s.base),"registration_endpoint":format!("{}/register",s.base),"token_endpoint_auth_methods_supported":["client_secret_basic","client_secret_post","none"],"response_types_supported":["code"],"grant_types_supported":["authorization_code","refresh_token"],"code_challenge_methods_supported":["S256"],"authorization_response_iss_parameter_supported":true,"scopes_supported":["read","write","offline_access"]})).into_response()
}
async fn register(State(s): State<Arc<Fake>>, Json(body): Json<Value>) -> Response {
    s.registrations.fetch_add(1, Ordering::SeqCst);
    if s.registration_delay.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    s.dcr.lock().unwrap().push(body.clone());
    if s.registration_unavailable.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    Json(
        json!({"client_id":"registered-client","client_secret":"client-secret-canary","redirect_uris":body["redirect_uris"],"client_id_issued_at":123,"token_endpoint_auth_method":"client_secret_post"}),
    ).into_response()
}
async fn token(
    State(s): State<Arc<Fake>>,
    headers: HeaderMap,
    Form(fields): Form<HashMap<String, String>>,
) -> Response {
    // Both methods are supported globally, but DCR assigns POST to this client.
    // Enforce the per-client contract on both exchange and refresh.
    if fields.get("client_id").map(String::as_str) == Some("registered-client")
        && s.registrations.load(Ordering::SeqCst) > 0
    {
        assert!(headers.get("authorization").is_none());
        assert_eq!(
            fields.get("client_secret").map(String::as_str),
            Some("client-secret-canary")
        );
    } else if s.preregistered_post.load(Ordering::SeqCst) {
        assert!(headers.get("authorization").is_none());
        assert_eq!(
            fields.get("client_id").map(String::as_str),
            Some("pre-client")
        );
        assert_eq!(
            fields.get("client_secret").map(String::as_str),
            Some("pre-secret")
        );
    } else {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !authorization.starts_with("Basic ") {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&authorization[6..])
            .unwrap();
        assert!(matches!(
            String::from_utf8(decoded).unwrap().as_str(),
            "pre-client:pre-secret"
                | "registered-client:first-client-secret"
                | "registered-client:rotated-client-secret"
        ));
    }
    s.requests.lock().unwrap().push((headers, fields.clone()));
    assert_eq!(fields.get("resource").unwrap(), &format!("{}/mcp", s.base));
    if fields["grant_type"] == "authorization_code" {
        s.exchanges.fetch_add(1, Ordering::SeqCst);
        if s.token_delay.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        assert!(fields.contains_key("code_verifier"));
        assert_eq!(fields.get("redirect_uri").unwrap(), REDIRECT);
    } else {
        s.refreshes.fetch_add(1, Ordering::SeqCst);
        if fields.get("refresh_token") != Some(&*s.expected_refresh.lock().unwrap()) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"invalid_grant"})),
            )
                .into_response();
        }
    }
    match s.token_mode.load(Ordering::SeqCst) {
        2 => return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"temporarily_unavailable","error_description":"sensitive-canary"})),
        )
            .into_response(),
        3 => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"invalid_grant","error_description":"sensitive-canary"})),
            )
                .into_response();
        }
        _ => {}
    }
    let n = s.refreshes.load(Ordering::SeqCst);
    let mut result = json!({"access_token":format!("access-{n}"),"token_type":"Bearer","expires_in":3600,"scope":"read offline_access","vendor":{"retained":true}});
    if s.token_mode.load(Ordering::SeqCst) != 1 || fields["grant_type"] == "authorization_code" {
        let next = format!("refresh-{n}");
        *s.expected_refresh.lock().unwrap() = next.clone();
        result["refresh_token"] = json!(next);
    }
    Json(result).into_response()
}
async fn mcp(State(s): State<Arc<Fake>>, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    let deny = s.deny_mcp.load(Ordering::SeqCst);
    if deny == 6 && body["method"] == "resources/list" {
        return (
            StatusCode::UNAUTHORIZED,
            [("www-authenticate", "Bearer")],
            "",
        )
            .into_response();
    }
    if deny == 2 {
        return (StatusCode::FORBIDDEN, "ordinary forbidden sensitive-canary").into_response();
    }
    if deny == 5 {
        return (
            StatusCode::FORBIDDEN,
            [("www-authenticate", "Bearer realm=\"other\"")],
            "",
        )
            .into_response();
    }
    if deny == 3 {
        return (
            StatusCode::FORBIDDEN,
            [(
                "www-authenticate",
                "Bearer error=\"insufficient_scope\", scope=\"write\"",
            )],
            "",
        )
            .into_response();
    }
    if s.protected.load(Ordering::SeqCst)
        && (!headers.contains_key("authorization")
            || deny == 1
            || (deny == 4 && headers.get("authorization").unwrap() == "Bearer access-0"))
    {
        return (StatusCode::UNAUTHORIZED,[("www-authenticate",format!("Bearer resource_metadata=\"{}/.well-known/oauth-protected-resource/mcp\", scope=\"read\"",s.base))],"").into_response();
    }
    if body["method"] == "tools/call" {
        s.tools_executed.fetch_add(1, Ordering::SeqCst);
    }
    let Some(id) = body.get("id") else {
        return StatusCode::ACCEPTED.into_response();
    };
    let result = match body["method"].as_str().unwrap_or_default() {
        "initialize" => {
            let mut response = json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"fake","version":"1"}});
            if s.resources.load(Ordering::SeqCst) {
                response["capabilities"]["resources"] = json!({});
            }
            response
        }
        "tools/list" => json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
        "tools/call" => json!({"content":[{"type":"text","text":"ok"}]}),
        _ => json!({}),
    };
    let mut response = Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response();
    if s.stateful.load(Ordering::SeqCst) {
        response
            .headers_mut()
            .insert("mcp-session-id", "test-session".parse().unwrap());
    }
    response
}
struct Empty;
impl McpSecretResolver for Empty {
    fn resolve_mcp_secret(&self, _: &str) -> Option<String> {
        None
    }
}
struct Harness {
    server: Server,
    sink: Arc<Sink>,
    persistence: OAuthPersistence,
    service: McpOAuthService,
    store: Arc<FailingStore>,
}
impl Harness {
    async fn new(protected: bool) -> Self {
        let server = Server::new(protected).await;
        let sink = Arc::new(Sink {
            events: Mutex::new(vec![]),
            available: AtomicBool::new(true),
        });
        let store = Arc::new(FailingStore::default());
        let persistence = OAuthPersistence::new(store.clone(), None);
        let service = McpOAuthService::new(persistence.clone(), sink.clone()).unwrap();
        Self {
            server,
            sink,
            persistence,
            service,
            store,
        }
    }
    async fn start(&self) -> OAuthEvent {
        self.service
            .begin_install("installation", &self.server.installation(), 10, REDIRECT)
            .await
            .unwrap();
        self.service
            .challenge(
                "installation",
                &format!(
                    "Bearer resource_metadata=\"{}/.well-known/oauth-protected-resource/mcp\"",
                    self.server.data.base
                ),
                false,
            )
            .await;
        self.sink.browser().await
    }
    fn callback(&self, event: &OAuthEvent) -> OAuthCallback {
        let url = url::Url::parse(event.authorization_url.as_ref().unwrap()).unwrap();
        OAuthCallback {
            flow_id: event.flow_id.clone().unwrap(),
            state: url
                .query_pairs()
                .find(|(k, _)| k == "state")
                .unwrap()
                .1
                .into_owned(),
            code: Some("code-canary".into()),
            issuer: Some(self.server.data.base.clone()),
            error: None,
        }
    }
    async fn login(&self) -> OAuthEvent {
        let e = self.start().await;
        self.service
            .callback("installation", 10, self.callback(&e))
            .await
            .unwrap();
        self.sink
            .wait_state(e.flow_id.as_deref(), OAuthState::Authorized)
            .await;
        e
    }
    async fn connect(
        &self,
    ) -> Result<Box<dyn pioneer_mcp::McpRuntimeSession>, pioneer_mcp::McpRuntimeError> {
        RmcpRuntimeConnector::with_oauth(Arc::new(self.service.clone()))
            .connect(
                self.server.installation(),
                "installation".into(),
                Arc::new(Empty),
                0,
            )
            .await
    }
}

#[tokio::test]
async fn public_mcp_connects_without_oauth_or_browser() {
    let h = Harness::new(false).await;
    h.service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    let mut session = h.connect().await.unwrap();
    assert_eq!(session.initial_catalog().tools_count(), 1);
    assert_eq!(h.sink.browsers(), 0);
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 0);
    session.shutdown().await;
    h.service.shutdown().await;
}
#[tokio::test]
async fn successful_public_connection_retires_install_intent_without_terminal_oauth_ui() {
    let h = Harness::new(false).await;
    h.service.shutdown().await;
    let clock = Arc::new(JumpClock(Mutex::new(std::time::SystemTime::now())));
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            clock: clock.clone(),
            poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    let mut session = RmcpRuntimeConnector::with_oauth(Arc::new(service.clone()))
        .connect(
            h.server.installation(),
            "installation".into(),
            Arc::new(Empty),
            0,
        )
        .await
        .unwrap();
    // Expire the unused install only after the real HTTP connection completes.
    // Its setup must not race an artificial 100 ms OAuth deadline.
    clock.jump_after_sleep();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(h.sink.browsers(), 0);
    let states: Vec<_> = h
        .sink
        .events
        .lock()
        .unwrap()
        .iter()
        .map(|event| event.state)
        .collect();
    assert!(
        !states.iter().any(|state| matches!(
            state,
            OAuthState::Preparing
                | OAuthState::Cancelled
                | OAuthState::TimedOut
                | OAuthState::Failed
        )),
        "unexpected OAuth states: {states:?}"
    );
    session.shutdown().await;
    service.shutdown().await;
}
#[tokio::test]
async fn public_connection_retires_install_intent_after_a_contended_worker_read() {
    let h = Harness::new(false).await;
    h.service.shutdown().await;
    let clock = Arc::new(JumpClock(Mutex::new(std::time::SystemTime::now())));
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            clock: clock.clone(),
            poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();

    let read = Arc::new(PreStageRead::default());
    *h.store.pre_stage_read.lock().unwrap() = Some(read.clone());
    read.armed.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), read.entered.notified())
        .await
        .expect("worker must hold the entry while reading credentials");

    let installation = h.server.installation();
    let mut retirement =
        Box::pin(service.connection_established("installation", &installation, false));
    let pending = tokio::time::timeout(Duration::from_millis(20), &mut retirement)
        .await
        .is_err();
    read.resume();
    if pending {
        tokio::time::timeout(Duration::from_secs(3), retirement)
            .await
            .expect("public install retirement must finish after the read releases");
    }
    clock.jump_after_sleep();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let states: Vec<_> = h
        .sink
        .events
        .lock()
        .unwrap()
        .iter()
        .map(|event| event.state)
        .collect();
    assert!(
        states.is_empty(),
        "retired public install emitted {states:?}"
    );
    service.shutdown().await;
}

#[tokio::test]
async fn public_connection_preserves_inflight_oauth_preparation() {
    let h = Harness::new(true).await;
    h.service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    let read = Arc::new(PreStageRead::default());
    *h.store.pre_stage_read.lock().unwrap() = Some(read.clone());
    read.armed.store(true, Ordering::SeqCst);
    h.service.challenge("installation", "Bearer", false).await;
    tokio::time::timeout(Duration::from_secs(3), read.entered.notified())
        .await
        .expect("OAuth preparation must hold the entry while reading credentials");

    let installation = h.server.installation();
    let completed = tokio::time::timeout(
        Duration::from_secs(1),
        h.service
            .connection_established("installation", &installation, false),
    )
    .await
    .is_ok();
    read.resume();
    assert!(completed, "public connection waited behind active consent");
    let event = h.sink.browser().await;
    assert_eq!(event.state, OAuthState::AwaitingCallback);
    h.service
        .cancel("installation", 10, event.flow_id.as_deref().unwrap())
        .await
        .unwrap();
    h.service.shutdown().await;
}

#[tokio::test]
async fn protected_install_starts_one_browser_flow_and_callback_connects() {
    let h = Harness::new(true).await;
    h.service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    assert_eq!(
        h.connect().await.err().unwrap().state,
        McpRuntimeState::AuthRequired
    );
    let e = h.sink.browser().await;
    for client in [10, 11, 12] {
        h.service
            .begin_install("installation", &h.server.installation(), client, REDIRECT)
            .await
            .unwrap();
        h.service.challenge("installation", "Bearer", false).await;
    }
    h.service
        .callback("installation", 10, h.callback(&e))
        .await
        .unwrap();
    h.sink
        .wait_state(e.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    assert_eq!(h.sink.browsers(), 1);
    let mut session = h.connect().await.unwrap();
    assert_eq!(session.initial_catalog().tools_count(), 1);
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 1);
    session.shutdown().await;
    h.service.shutdown().await;
}
#[tokio::test]
async fn wrong_state_or_issuer_does_not_consume_valid_callback() {
    let h = Harness::new(true).await;
    let e = h.start().await;
    let mut bad = h.callback(&e);
    bad.state = "forged".into();
    assert!(h.service.callback("installation", 10, bad).await.is_err());
    let mut bad = h.callback(&e);
    bad.issuer = Some("https://wrong.test".into());
    assert!(h.service.callback("installation", 10, bad).await.is_err());
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 0);
    h.service
        .callback("installation", 10, h.callback(&e))
        .await
        .unwrap();
    h.sink
        .wait_state(e.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    h.service.shutdown().await;
}
#[tokio::test]
async fn duplicate_callback_never_exchanges_code_twice() {
    let h = Harness::new(true).await;
    let e = h.login().await;
    assert!(
        h.service
            .callback("installation", 10, h.callback(&e))
            .await
            .is_err()
    );
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn consent_denial_terminates_flow() {
    let h = Harness::new(true).await;
    let e = h.start().await;
    let mut cb = h.callback(&e);
    cb.code = None;
    cb.error = Some("access_denied".into());
    h.service.callback("installation", 10, cb).await.unwrap();
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 0);
    assert!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::Denied)
    );
    assert!(
        h.service
            .callback("installation", 10, h.callback(&e))
            .await
            .is_err()
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn cancellation_and_uninstall_reject_late_callbacks() {
    let h = Harness::new(true).await;
    let e = h.start().await;
    h.service
        .cancel("installation", 10, e.flow_id.as_ref().unwrap())
        .await
        .unwrap();
    assert!(
        h.service
            .callback("installation", 10, h.callback(&e))
            .await
            .is_err()
    );
    h.service.disconnect("installation").await.unwrap();
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    assert!(
        h.service
            .callback("installation", 10, h.callback(&e))
            .await
            .is_err()
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn resource_replacement_fences_previous_flow_and_tokens() {
    let h = Harness::new(true).await;
    let e = h.start().await;
    let mut replacement = h.server.installation();
    if let McpTransportConfig::StreamableHttp { url, .. } = &mut replacement.transport {
        *url = format!("{}/other", h.server.data.base);
    }
    h.service
        .synchronize("installation", &replacement)
        .await
        .unwrap();
    assert!(
        h.service
            .callback("installation", 10, h.callback(&e))
            .await
            .is_err()
    );
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    assert!(
        h.service
            .client("installation", &replacement)
            .await
            .unwrap()
            .is_none()
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn restart_restores_complete_registration_and_absolute_expiry() {
    let h = Harness::new(true).await;
    h.login().await;
    let before = h.persistence.read("installation").await.unwrap().unwrap();
    assert_eq!(before.registration.redirect_uri, REDIRECT);
    assert_eq!(
        before
            .registration
            .registration_response
            .as_ref()
            .unwrap()
            .additional_fields["client_id_issued_at"],
        123
    );
    assert_eq!(
        before.registration.registration_request.as_ref().unwrap()["redirect_uris"][0],
        REDIRECT
    );
    assert_eq!(
        before.registration.client_secret.as_deref(),
        Some("client-secret-canary")
    );
    let received = before.credentials.as_ref().unwrap().token_received_at;
    h.service.shutdown().await;
    let restored = McpOAuthService::new(h.persistence.clone(), h.sink.clone()).unwrap();
    let client = restored
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(client.get_access_token().await.unwrap(), "access-0");
    client
        .auth_manager
        .lock()
        .await
        .refresh_token()
        .await
        .unwrap();
    let after = h.persistence.read("installation").await.unwrap().unwrap();
    assert_eq!(before.credentials.unwrap().token_received_at, received);
    assert_eq!(
        after.registration.client_secret,
        before.registration.client_secret
    );
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 1);
    let requests = h.server.data.requests.lock().unwrap();
    let (headers, fields) = &requests[1];
    assert!(headers.get("authorization").is_none());
    assert_eq!(
        fields.get("client_id").map(String::as_str),
        Some("registered-client")
    );
    assert_eq!(
        fields.get("client_secret").map(String::as_str),
        Some("client-secret-canary")
    );
    drop(requests);
    restored.shutdown().await;
}
#[tokio::test]
async fn refresh_rotation_is_saved_and_no_rotation_preserves_refresh() {
    let h = Harness::new(true).await;
    h.login().await;
    let client = h
        .service
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    client
        .auth_manager
        .lock()
        .await
        .refresh_token()
        .await
        .unwrap();
    let first = h.persistence.read("installation").await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(first.credentials.unwrap()).unwrap()["token_response"]["refresh_token"],
        "refresh-1"
    );
    h.server.data.token_mode.store(1, Ordering::SeqCst);
    client
        .auth_manager
        .lock()
        .await
        .refresh_token()
        .await
        .unwrap();
    let second = h.persistence.read("installation").await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(second.credentials.unwrap()).unwrap()["token_response"]["refresh_token"],
        "refresh-1"
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn concurrent_expiry_refresh_uses_one_manager_and_one_rotation() {
    let h = Harness::new(true).await;
    h.login().await;
    let mut record = h.persistence.read("installation").await.unwrap().unwrap();
    record.credentials.as_mut().unwrap().token_received_at = Some(1);
    h.persistence.write("installation", record).await.unwrap();
    let client = h
        .service
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    let (a, b, c) = tokio::join!(
        client.get_access_token(),
        client.get_access_token(),
        client.get_access_token()
    );
    assert_eq!(a.unwrap(), "access-1");
    assert_eq!(b.unwrap(), "access-1");
    assert_eq!(c.unwrap(), "access-1");
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn transient_token_failure_preserves_credentials() {
    let h = Harness::new(true).await;
    h.login().await;
    let before = h.persistence.read("installation").await.unwrap().unwrap();
    h.server.data.token_mode.store(2, Ordering::SeqCst);
    let client = h
        .service
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        client.auth_manager.lock().await.refresh_token().await,
        Err(rmcp::transport::auth::AuthError::TokenRefreshFailed(_))
    ));
    assert_eq!(
        serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn invalid_grant_requires_signin_without_automatic_browser_loop() {
    let h = Harness::new(true).await;
    h.login().await;
    h.server.data.token_mode.store(3, Ordering::SeqCst);
    h.server.data.deny_mcp.store(1, Ordering::SeqCst);
    assert_eq!(
        h.connect().await.err().unwrap().state,
        McpRuntimeState::AuthRequired
    );
    h.service.challenge("installation", "Bearer", false).await;
    assert_eq!(h.sink.browsers(), 1);
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .is_some()
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn silent_401_recovery_refreshes_once_then_connects() {
    let h = Harness::new(true).await;
    h.login().await;
    h.server.data.deny_mcp.store(4, Ordering::SeqCst);
    let mut session = h.connect().await.unwrap();
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(h.sink.browsers(), 1);
    session.shutdown().await;
    h.service.shutdown().await;
}
#[tokio::test]
async fn refusal_after_fresh_token_is_bounded() {
    let h = Harness::new(true).await;
    h.login().await;
    h.server.data.deny_mcp.store(1, Ordering::SeqCst);
    assert_eq!(
        h.connect().await.err().unwrap().state,
        McpRuntimeState::AuthRequired
    );
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn ordinary_forbidden_and_scope_challenge_have_different_states() {
    let h = Harness::new(true).await;
    h.login().await;
    h.server.data.deny_mcp.store(2, Ordering::SeqCst);
    assert_eq!(
        h.connect().await.err().unwrap().state,
        McpRuntimeState::Failed
    );
    h.server.data.deny_mcp.store(3, Ordering::SeqCst);
    assert_eq!(
        h.connect().await.err().unwrap().state,
        McpRuntimeState::AuthRequired
    );
    assert!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::InsufficientScope)
    );
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn loss_during_tool_call_is_auth_required_and_does_not_replay_call() {
    let h = Harness::new(true).await;
    h.login().await;
    let mut session = h.connect().await.unwrap();
    h.server.data.deny_mcp.store(1, Ordering::SeqCst);
    let error = session
        .call_tool(
            "echo",
            json!({}),
            McpInvocationBudget {
                max_arguments_bytes: 1024,
            },
            Duration::from_secs(3),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.state, McpRuntimeState::AuthRequired);
    assert_eq!(h.sink.browsers(), 1);
    session.shutdown().await;
    h.service.shutdown().await;
}
#[tokio::test]
async fn garbage_collection_and_disconnect_keep_other_secret_namespaces() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service
        .garbage_collect(&Default::default())
        .await
        .unwrap();
    assert!(h.persistence.ids().await.unwrap().is_empty());
    assert!(
        h.service
            .callback(
                "installation",
                10,
                OAuthCallback {
                    flow_id: "stale".into(),
                    state: "stale".into(),
                    code: None,
                    issuer: None,
                    error: None
                }
            )
            .await
            .is_err()
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn refresh_does_not_change_configuration_or_effective_secret_fingerprint() {
    let h = Harness::new(true).await;
    h.login().await;
    let installation = h.server.installation();
    let config = pioneer_mcp::fingerprint_installation(&installation);
    let secrets =
        pioneer_mcp::effective_secret_material_fingerprint(&installation, &Empty, b"process")
            .unwrap();
    let client = h
        .service
        .client("installation", &installation)
        .await
        .unwrap()
        .unwrap();
    client
        .auth_manager
        .lock()
        .await
        .refresh_token()
        .await
        .unwrap();
    assert_eq!(
        config,
        pioneer_mcp::fingerprint_installation(&h.server.installation())
    );
    assert_eq!(
        secrets,
        pioneer_mcp::effective_secret_material_fingerprint(&installation, &Empty, b"process")
            .unwrap()
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn remote_callback_relay_only_accepts_initiating_client() {
    let h = Harness::new(true).await;
    let e = h.start().await;
    assert!(
        h.service
            .callback("installation", 11, h.callback(&e))
            .await
            .is_err()
    );
    h.service
        .callback("installation", 10, h.callback(&e))
        .await
        .unwrap();
    h.sink
        .wait_state(e.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    assert_eq!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| e.client_id)
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([10])
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn flow_timeout_cleans_callback_state() {
    let h = Harness::new(true).await;
    h.service.shutdown().await;
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            flow_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    service.challenge("installation", "Bearer", false).await;
    let e = h.sink.browser().await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        service
            .callback("installation", 10, h.callback(&e))
            .await
            .is_err()
    );
    assert!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::TimedOut)
    );
    assert!(!service.event_is_current(&e, &h.server.installation()).await);
    service.shutdown().await;
}

#[derive(Default)]
struct FailingStore {
    inner: MemorySecretStore,
    fail: AtomicBool,
    commit_clock: Mutex<Option<Arc<CommitClock>>>,
    pre_stage_read: Mutex<Option<Arc<PreStageRead>>>,
    mutation_phase: std::sync::atomic::AtomicU8,
    mutation_after_write: AtomicBool,
    mutation_readback_failure: AtomicBool,
    reads_failed: AtomicBool,
    uncertain_delete: AtomicBool,
    marker_unreadable: AtomicBool,
    marker_deleted: tokio::sync::Notify,
    mutations: AtomicUsize,
}
impl SecretStore for FailingStore {
    fn get_string(&self, id: &SecretId) -> pioneer_keystore::Result<Option<String>> {
        if id.user().ends_with("::promotion") && self.marker_unreadable.load(Ordering::SeqCst) {
            return Err(pioneer_keystore::KeystoreError::ReadFailed(
                "injected marker outage".into(),
            ));
        }
        if id.user() == "installation" && self.reads_failed.load(Ordering::SeqCst) {
            return Err(pioneer_keystore::KeystoreError::ReadFailed(
                "injected readback failure".into(),
            ));
        }
        if let Some(barrier) = self.pre_stage_read.lock().unwrap().clone() {
            if barrier.armed.swap(false, Ordering::SeqCst) {
                barrier.entered.notify_one();
                let mut released = barrier.released.lock().unwrap();
                while !*released {
                    let next = barrier
                        .resumed
                        .wait_timeout(released, Duration::from_secs(3))
                        .unwrap();
                    released = next.0;
                    assert!(!next.1.timed_out(), "pre-stage read not released");
                }
            }
        }
        self.inner.get_string(id)
    }
    fn put_string(
        &self,
        id: &SecretId,
        value: &str,
        meta: SecretMeta,
    ) -> pioneer_keystore::Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(pioneer_keystore::KeystoreError::WriteFailed(
                "secret-canary".into(),
            ));
        }
        let phase = serde_json::from_str::<Value>(value)
            .ok()
            .and_then(|value| value.get("pending_consent").cloned())
            .filter(|pending| pending.is_object())
            .map(|pending| if pending["committed"] == true { 2 } else { 1 })
            .unwrap_or(0);
        if phase != 0 && self.mutation_phase.load(Ordering::SeqCst) == phase {
            if self.mutation_after_write.load(Ordering::SeqCst) {
                self.inner.put_string(id, value, meta)?;
            }
            if self.mutation_readback_failure.load(Ordering::SeqCst) {
                self.reads_failed.store(true, Ordering::SeqCst);
            }
            return Err(pioneer_keystore::KeystoreError::WriteFailed(
                "injected mutation outcome".into(),
            ));
        }
        self.inner.put_string(id, value, meta)?;
        self.mutations.fetch_add(1, Ordering::SeqCst);
        // Barrier is armed only AFTER the real atomic put has succeeded.
        if let Some(clock) = self.commit_clock.lock().unwrap().as_ref() {
            if serde_json::from_str::<Value>(value)
                .ok()
                .is_some_and(|value| value["pending_consent"]["candidate"].is_object())
                && clock.arm.swap(false, Ordering::SeqCst)
            {
                clock.block_next.store(true, Ordering::SeqCst);
            }
        }
        Ok(())
    }
    fn delete(&self, id: &SecretId) -> pioneer_keystore::Result<bool> {
        let deleted = self.inner.delete(id)?;
        self.mutations.fetch_add(1, Ordering::SeqCst);
        if id.user().ends_with("::promotion") && self.uncertain_delete.load(Ordering::SeqCst) {
            self.marker_unreadable.store(true, Ordering::SeqCst);
            self.marker_deleted.notify_one();
            return Err(pioneer_keystore::KeystoreError::WriteFailed(
                "post-delete error".into(),
            ));
        }
        Ok(deleted)
    }
    fn exists(&self, id: &SecretId) -> pioneer_keystore::Result<bool> {
        self.inner.exists(id)
    }
    fn list(&self, filter: SecretFilter) -> pioneer_keystore::Result<Vec<SecretEntryMeta>> {
        self.inner.list(filter)
    }
}
#[tokio::test]
async fn storage_failure_does_not_acknowledge_authorization_or_leak_provider_data() {
    let h = Harness::new(true).await;
    let event = h.start().await;
    h.store.fail.store(true, Ordering::SeqCst);
    h.service
        .callback("installation", 10, h.callback(&event))
        .await
        .unwrap();
    let failure = h
        .sink
        .wait_state(event.flow_id.as_deref(), OAuthState::Failed)
        .await;
    assert_eq!(
        failure.diagnostic.as_deref(),
        Some("OAuth credential storage unavailable")
    );
    assert!(!failure.diagnostic.as_deref().unwrap().contains("canary"));
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .is_none()
    );
    assert!(
        !h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::Authorized)
    );
    assert!(
        h.service
            .callback("installation", 10, h.callback(&event))
            .await
            .is_err()
    );
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 1);
    // A failed save must not leave the SDK's freshly cached, unsaved grant
    // usable by a later MCP connection, even though the callback was accepted.
    let restored = h
        .service
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    assert!(restored.get_access_token().await.is_err());
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn expired_credentials_survive_resave_and_refresh_before_first_connection() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service.shutdown().await;
    let mut record = h.persistence.read("installation").await.unwrap().unwrap();
    record.credentials.as_mut().unwrap().token_received_at = Some(1);
    h.persistence
        .write("installation", record.clone())
        .await
        .unwrap();
    h.persistence
        .write(
            "installation",
            h.persistence.read("installation").await.unwrap().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .unwrap()
            .token_received_at,
        Some(1)
    );
    let service = McpOAuthService::new(h.persistence.clone(), h.sink.clone()).unwrap();
    let connector = RmcpRuntimeConnector::with_oauth(Arc::new(service.clone()));
    let mut session = connector
        .connect(
            h.server.installation(),
            "installation".into(),
            Arc::new(Empty),
            0,
        )
        .await
        .unwrap();
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(h.sink.browsers(), 1);
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .unwrap()
            .token_received_at
            .unwrap()
            > 1
    );
    session.shutdown().await;
    service.shutdown().await;
}
#[tokio::test]
async fn pre_registered_secret_scopes_and_s256_are_used_without_dcr() {
    let h = Harness::new(true).await;
    let mut installation = h.server.installation();
    h.store
        .put_string(
            &SecretId::mcp_secret("registered-secret").unwrap(),
            "pre-secret",
            pioneer_keystore::SecretMeta::new(pioneer_keystore::SecretKind::McpSecret, None, 0),
        )
        .unwrap();
    installation.auth.oauth = Some(McpOAuthConfig {
        token_endpoint_auth_method: Some("client_secret_basic".into()),
        client_id: Some("pre-client".into()),
        client_secret_ref: Some("registered-secret".into()),
        scopes: vec!["write".into(), "offline_access".into()],
        issuer: Some(h.server.data.base.clone()),
    });
    h.service
        .begin_install("installation", &installation, 10, REDIRECT)
        .await
        .unwrap();
    h.service.challenge("installation", "Bearer", false).await;
    let event = h.sink.browser().await;
    let url = url::Url::parse(event.authorization_url.as_ref().unwrap()).unwrap();
    let query = url.query_pairs().collect::<HashMap<_, _>>();
    assert_eq!(query["code_challenge_method"], "S256");
    assert!(query["scope"].split_whitespace().any(|s| s == "write"));
    assert!(
        query["scope"]
            .split_whitespace()
            .any(|s| s == "offline_access")
    );
    h.service
        .callback("installation", 10, h.callback(&event))
        .await
        .unwrap();
    h.sink
        .wait_state(event.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    let requests = h.server.data.requests.lock().unwrap();
    let verifier = &requests[0].1["code_verifier"];
    assert_eq!(
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(verifier.as_bytes())),
        query["code_challenge"]
    );
    assert_eq!(
        requests[0].0.get("authorization").unwrap(),
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("pre-client:pre-secret")
        )
        .as_str()
    );
    drop(requests);
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 0);
    h.service.shutdown().await;
}
#[tokio::test]
async fn incompatible_redirect_registration_is_rejected_without_new_tab() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service
        .sign_in(
            "installation",
            &h.server.installation(),
            10,
            "http://127.0.0.1:37644/oauth/mcp/callback",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(h.sink.browsers(), 1);
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 1);
    assert!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::Failed)
    );
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .is_some()
    );
    h.service.shutdown().await;
}
#[tokio::test]
async fn installation_intent_retries_discovery_after_transient_failure() {
    let h = Harness::new(true).await;
    h.service.shutdown().await;
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    h.server.data.discovery_mode.store(1, Ordering::SeqCst);
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    service.challenge("installation", "Bearer", false).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(h.sink.browsers(), 0);
    h.server.data.discovery_mode.store(0, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            if h.sink.browsers() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    service.shutdown().await;
}
#[tokio::test]
async fn initiator_disconnect_cancels_listener_effect_and_late_callback() {
    let h = Harness::new(true).await;
    h.service.shutdown().await;
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    service.challenge("installation", "Bearer", false).await;
    let event = h.sink.browser().await;
    h.sink.available.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        service
            .callback("installation", 10, h.callback(&event))
            .await
            .is_err()
    );
    assert!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::Cancelled && e.flow_id == event.flow_id)
    );
    service.shutdown().await;
}
#[tokio::test]
async fn synthesized_legacy_endpoints_do_not_open_browser() {
    let h = Harness::new(true).await;
    h.server.data.discovery_mode.store(2, Ordering::SeqCst);
    h.service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    h.service.challenge("installation", "Bearer", false).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(h.sink.browsers(), 0);
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 0);
    h.service.shutdown().await;
}
#[tokio::test]
async fn independent_managers_share_file_refresh_guard_and_reload_rotated_token() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service.shutdown().await;
    let path = std::env::temp_dir().join(format!("pioneer-oauth-test-{}", uuid::Uuid::new_v4()));
    let persistence = OAuthPersistence::new(h.store.clone(), Some(path.clone()));
    let mut record = persistence.read("installation").await.unwrap().unwrap();
    record.credentials.as_mut().unwrap().token_received_at = Some(1);
    persistence.write("installation", record).await.unwrap();
    let a = McpOAuthService::new(persistence.clone(), h.sink.clone()).unwrap();
    let b = McpOAuthService::new(persistence.clone(), h.sink.clone()).unwrap();
    let ca = a
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    let cb = b
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    let (ra, rb) = tokio::join!(ca.get_access_token(), cb.get_access_token());
    let tokens = std::collections::HashSet::from([ra.unwrap(), rb.unwrap()]);
    // rmcp 3.5 serializes refresh but deliberately does not skip an explicit
    // refresh after waiting. Both managers must use the latest rotating token.
    assert_eq!(
        tokens,
        std::collections::HashSet::from(["access-1".into(), "access-2".into()])
    );
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 2);
    let requests = h.server.data.requests.lock().unwrap();
    assert_eq!(requests[1].1["refresh_token"], "refresh-0");
    assert_eq!(requests[2].1["refresh_token"], "refresh-1");
    drop(requests);
    a.shutdown().await;
    b.shutdown().await;
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn forbidden_with_unrelated_challenge_is_not_scope_upgrade() {
    let h = Harness::new(true).await;
    h.login().await;
    h.server.data.deny_mcp.store(5, Ordering::SeqCst);
    let error = h.connect().await.err().unwrap();
    assert_eq!(error.kind, pioneer_mcp::McpRuntimeErrorKind::Forbidden);
    assert_eq!(h.sink.browsers(), 1);
    assert!(
        !h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::InsufficientScope)
    );
    h.service.shutdown().await;
}

#[tokio::test]
async fn background_expiry_refresh_recovers_from_transient_failure_without_browser_or_restart_event()
 {
    let h = Harness::new(true).await;
    h.login().await;
    h.service.shutdown().await;
    let mut record = h.persistence.read("installation").await.unwrap().unwrap();
    record.credentials.as_mut().unwrap().token_received_at = Some(1);
    h.persistence
        .write("installation", record.clone())
        .await
        .unwrap();
    h.server.data.token_mode.store(2, Ordering::SeqCst);
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .synchronize("installation", &h.server.installation())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.server.data.refreshes.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(record).unwrap()
    );
    h.server.data.token_mode.store(0, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            if h.persistence
                .read("installation")
                .await
                .unwrap()
                .unwrap()
                .credentials
                .unwrap()
                .token_received_at
                .unwrap()
                > 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.sink.browsers(), 1);
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 2);
    assert_eq!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.state == OAuthState::Authorized)
            .count(),
        1
    );
    assert_eq!(
        service.state("installation").await,
        Some(OAuthState::Authorized)
    );
    service.shutdown().await;
}
#[tokio::test]
async fn issuer_config_replacement_never_uses_existing_tokens() {
    let h = Harness::new(true).await;
    h.login().await;
    let mut installation = h.server.installation();
    installation.auth.oauth = Some(McpOAuthConfig {
        issuer: Some("https://different-issuer.test".into()),
        ..Default::default()
    });
    h.service
        .synchronize("installation", &installation)
        .await
        .unwrap();
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    h.service
        .sign_in("installation", &installation, 10, REDIRECT)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 1);
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn explicit_authorization_header_never_starts_native_oauth() {
    let h = Harness::new(true).await;
    let mut installation = h.server.installation();
    if let McpTransportConfig::StreamableHttp { headers, .. } = &mut installation.transport {
        headers.insert(
            "Authorization".into(),
            pioneer_mcp::McpConfigValue::Literal {
                value: "Bearer explicit".into(),
            },
        );
    }
    h.service
        .synchronize("installation", &installation)
        .await
        .unwrap();
    h.service
        .begin_install("installation", &installation, 10, REDIRECT)
        .await
        .unwrap();
    let connector = RmcpRuntimeConnector::with_oauth(Arc::new(h.service.clone()));
    let mut session = connector
        .connect(installation, "installation".into(), Arc::new(Empty), 0)
        .await
        .unwrap();
    assert_eq!(h.sink.browsers(), 0);
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 0);
    session.shutdown().await;
    h.service.shutdown().await;
}

#[tokio::test]
async fn repeated_token_exchanges_are_rate_limited_without_erasing_credentials() {
    let h = Harness::new(true).await;
    h.login().await;
    let client = h
        .service
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    for _ in 0..6 {
        client
            .auth_manager
            .lock()
            .await
            .refresh_token()
            .await
            .unwrap();
    }
    assert!(matches!(
        client.auth_manager.lock().await.refresh_token().await,
        Err(rmcp::transport::auth::AuthError::TokenRefreshFailed(_))
    ));
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 6);
    assert_eq!(h.sink.browsers(), 1);
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .is_some()
    );
    h.service.shutdown().await;
}

#[tokio::test]
async fn replacing_native_oauth_with_explicit_header_clears_old_credentials() {
    let h = Harness::new(true).await;
    h.login().await;
    let mut installation = h.server.installation();
    if let McpTransportConfig::StreamableHttp { headers, .. } = &mut installation.transport {
        headers.insert(
            "Authorization".into(),
            pioneer_mcp::McpConfigValue::Literal {
                value: "Bearer explicit".into(),
            },
        );
    }
    h.service
        .synchronize("installation", &installation)
        .await
        .unwrap();
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    assert!(
        h.service
            .client("installation", &installation)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}

#[tokio::test]
async fn replacing_http_with_stdio_fences_callback_and_removes_registration() {
    let h = Harness::new(true).await;
    let event = h.start().await;
    let mut installation = h.server.installation();
    installation.transport = McpTransportConfig::Stdio {
        command: "mcp-remote".into(),
        args: vec![],
        env: BTreeMap::new(),
        cwd: None,
        startup_timeout_ms: 1_000,
        tool_timeout_ms: 1_000,
    };
    h.service
        .synchronize("installation", &installation)
        .await
        .unwrap();
    assert!(
        h.service
            .callback("installation", 10, h.callback(&event))
            .await
            .is_err()
    );
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 0);
    h.service.shutdown().await;
}

#[tokio::test]
async fn initial_registration_transient_failure_retries_without_extra_tab() {
    let h = Harness::new(true).await;
    h.service.shutdown().await;
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    h.server
        .data
        .registration_unavailable
        .store(true, Ordering::SeqCst);
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    service.challenge("installation", "Bearer", false).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.server.data.registrations.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.sink.browsers(), 0);
    h.server
        .data
        .registration_unavailable
        .store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(6), async {
        while h.sink.browsers() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.sink.browsers(), 1);
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 2);
    service.shutdown().await;
}
#[tokio::test]
async fn scope_upgrade_requires_explicit_consent_and_preserves_registration() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service
        .challenge(
            "installation",
            "Bearer error=\"insufficient_scope\", scope=\"admin\"",
            true,
        )
        .await;
    assert_eq!(h.sink.browsers(), 1);
    h.service
        .sign_in("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.sink.browsers() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let event = h
        .sink
        .events
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|e| e.authorization_url.is_some())
        .cloned()
        .unwrap();
    let url = url::Url::parse(event.authorization_url.as_ref().unwrap()).unwrap();
    let scope = url
        .query_pairs()
        .find(|(k, _)| k == "scope")
        .unwrap()
        .1
        .into_owned();
    assert!(scope.split_whitespace().any(|s| s == "admin"));
    assert!(scope.split_whitespace().any(|s| s == "read"));
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 1);
    h.service
        .callback("installation", 10, h.callback(&event))
        .await
        .unwrap();
    h.sink
        .wait_state(event.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), 2);
    h.service.shutdown().await;
}

#[tokio::test]
async fn changing_referenced_client_secret_invalidates_saved_authorization() {
    let h = Harness::new(true).await;
    let secret_id = SecretId::mcp_secret("client-identity").unwrap();
    let put = |value: &str| {
        h.store
            .put_string(
                &secret_id,
                value,
                SecretMeta::new(pioneer_keystore::SecretKind::McpSecret, None, 0),
            )
            .unwrap()
    };
    put("first-client-secret");
    let mut installation = h.server.installation();
    installation.auth.oauth = Some(McpOAuthConfig {
        token_endpoint_auth_method: Some("client_secret_basic".into()),
        client_id: Some("registered-client".into()),
        client_secret_ref: Some("client-identity".into()),
        ..Default::default()
    });
    h.service
        .begin_install("installation", &installation, 10, REDIRECT)
        .await
        .unwrap();
    h.service.challenge("installation", "Bearer", false).await;
    let event = h.sink.browser().await;
    h.service
        .callback("installation", 10, h.callback(&event))
        .await
        .unwrap();
    h.sink
        .wait_state(event.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .is_some()
    );
    put("replacement-client-secret");
    h.service
        .synchronize("installation", &installation)
        .await
        .unwrap();
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    assert_eq!(h.sink.browsers(), 1);
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 0);
    h.service.shutdown().await;
}

#[tokio::test]
async fn resource_replacement_after_restart_cleans_stale_credentials_without_browser() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service.shutdown().await;
    let service = McpOAuthService::new(h.persistence.clone(), h.sink.clone()).unwrap();
    let mut installation = h.server.installation();
    if let McpTransportConfig::StreamableHttp { url, .. } = &mut installation.transport {
        url.push_str("/different-resource");
    }
    service
        .synchronize("installation", &installation)
        .await
        .unwrap();
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    assert_eq!(h.sink.browsers(), 1);
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 0);
    service.shutdown().await;
}

async fn mcp_stream(State(s): State<Arc<Fake>>) -> Response {
    if s.deny_mcp.load(Ordering::SeqCst) == 7 {
        (
            StatusCode::UNAUTHORIZED,
            [("www-authenticate", "Bearer")],
            "",
        )
            .into_response()
    } else {
        StatusCode::METHOD_NOT_ALLOWED.into_response()
    }
}
#[tokio::test]
async fn authorization_loss_during_optional_catalog_discovery_is_not_degraded() {
    let h = Harness::new(true).await;
    h.login().await;
    h.server.data.resources.store(true, Ordering::SeqCst);
    h.server.data.deny_mcp.store(6, Ordering::SeqCst);
    assert_eq!(
        h.connect().await.err().unwrap().state,
        McpRuntimeState::AuthRequired
    );
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn stream_authorization_loss_marks_auth_required_without_reopening_browser() {
    let h = Harness::new(true).await;
    h.login().await;
    h.server.data.stateful.store(true, Ordering::SeqCst);
    h.server.data.deny_mcp.store(7, Ordering::SeqCst);
    let connected = h.connect().await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.service.state("installation").await != Some(OAuthState::AuthRequired) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if let Ok(mut session) = connected {
        // rmcp can keep POST usable when its standalone GET stream fails.
        // Gateway consumes AuthRequired and stops the runtime (covered by its
        // focused wiring test); the lower layer must publish the typed loss.
        assert!(
            h.sink
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.state == OAuthState::AuthRequired)
        );
        session.shutdown().await;
    }
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}

#[tokio::test]
async fn cancel_interrupts_inflight_code_exchange_and_fences_late_save() {
    let h = Harness::new(true).await;
    let event = h.start().await;
    h.server.data.token_delay.store(true, Ordering::SeqCst);
    let service = h.service.clone();
    let callback = h.callback(&event);
    let exchange =
        tokio::spawn(async move { service.callback("installation", 10, callback).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.server.data.exchanges.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(
        Duration::from_millis(500),
        h.service
            .cancel("installation", 10, event.flow_id.as_ref().unwrap()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(exchange.await.unwrap().is_ok());
    h.sink
        .wait_state(event.flow_id.as_deref(), OAuthState::Cancelled)
        .await;
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .is_none()
    );
    assert!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::Cancelled
                && e.client_id == Some(10)
                && e.flow_id == event.flow_id)
    );
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}

#[tokio::test]
async fn deadline_interrupts_inflight_exchange_without_acknowledging_authorization() {
    let h = Harness::new(true).await;
    h.service.shutdown().await;
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            flow_timeout: Duration::from_millis(200),
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    service.challenge("installation", "Bearer", false).await;
    let event = h.sink.browser().await;
    h.server.data.token_delay.store(true, Ordering::SeqCst);
    tokio::time::timeout(
        Duration::from_millis(100),
        service.callback("installation", 10, h.callback(&event)),
    )
    .await
    .unwrap()
    .unwrap();
    h.sink
        .wait_state(event.flow_id.as_deref(), OAuthState::TimedOut)
        .await;
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .is_none()
    );
    assert!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::TimedOut && e.flow_id == event.flow_id)
    );
    assert!(
        !h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::Authorized)
    );
    service.shutdown().await;
}

#[tokio::test]
async fn public_http_redirect_keeps_existing_transport_behavior_without_browser() {
    let h = Harness::new(false).await;
    let mut installation = h.server.installation();
    if let McpTransportConfig::StreamableHttp { url, .. } = &mut installation.transport {
        *url = format!("{}/alias", h.server.data.base);
    }
    h.service
        .begin_install("installation", &installation, 10, REDIRECT)
        .await
        .unwrap();
    let mut session = RmcpRuntimeConnector::with_oauth(Arc::new(h.service.clone()))
        .connect(installation, "installation".into(), Arc::new(Empty), 0)
        .await
        .unwrap();
    assert_eq!(h.sink.browsers(), 0);
    session.shutdown().await;
    h.service.shutdown().await;
}

struct JumpClock(Mutex<std::time::SystemTime>);
impl OAuthClock for JumpClock {
    fn now(&self) -> std::time::SystemTime {
        *self.0.lock().unwrap()
    }
}
impl JumpClock {
    fn jump_after_sleep(&self) {
        *self.0.lock().unwrap() += Duration::from_secs(3600);
    }
}
#[tokio::test]
async fn sleep_expired_install_intent_never_opens_a_delayed_browser() {
    let h = Harness::new(true).await;
    h.service.shutdown().await;
    let clock = Arc::new(JumpClock(Mutex::new(std::time::SystemTime::now())));
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            clock: clock.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    clock.jump_after_sleep();
    service.challenge("installation", "Bearer", false).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.sink.browsers(), 0);
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 0);
    service.shutdown().await;
}
#[tokio::test]
async fn wall_deadline_after_sleep_fences_inflight_token_save() {
    let h = Harness::new(true).await;
    h.service.shutdown().await;
    let clock = Arc::new(JumpClock(Mutex::new(std::time::SystemTime::now())));
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            clock: clock.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    service.challenge("installation", "Bearer", false).await;
    let event = h.sink.browser().await;
    h.server.data.token_delay.store(true, Ordering::SeqCst);
    let owner = service.clone();
    let callback = h.callback(&event);
    let exchange = tokio::spawn(async move { owner.callback("installation", 10, callback).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.server.data.exchanges.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    clock.jump_after_sleep();
    assert!(exchange.await.unwrap().is_ok());
    h.sink
        .wait_state(event.flow_id.as_deref(), OAuthState::TimedOut)
        .await;
    assert!(
        h.persistence
            .read("installation")
            .await
            .unwrap()
            .unwrap()
            .credentials
            .is_none()
    );
    assert!(
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::TimedOut)
    );
    assert!(
        !h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.state == OAuthState::Authorized)
    );
    service.shutdown().await;
}

#[tokio::test]
async fn reconnect_detects_discovered_issuer_replacement_before_using_old_grant() {
    let h = Harness::new(true).await;
    h.login().await;
    let other = Server::new(true).await;
    *h.server.data.authorization_server.lock().unwrap() = Some(other.data.base.clone());
    let error = h.connect().await.err().unwrap();
    assert_eq!(error.state, McpRuntimeState::AuthRequired);
    assert_eq!(
        h.service.state("installation").await,
        Some(OAuthState::AuthRequired)
    );
    assert_eq!(h.server.data.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(other.data.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(other.data.exchanges.load(Ordering::SeqCst), 0);
    assert!(
        other
            .data
            .discovery_headers
            .lock()
            .unwrap()
            .iter()
            .all(|headers| !headers.contains_key("authorization"))
    );
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}
#[tokio::test]
async fn disconnect_can_resume_public_mcp_without_credentials_or_another_browser() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service.disconnect("installation").await.unwrap();
    h.server.data.protected.store(false, Ordering::SeqCst);
    let mut session = h.connect().await.unwrap();
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    assert_eq!(h.sink.browsers(), 1);
    session.shutdown().await;
    h.service.shutdown().await;
}

#[tokio::test]
async fn delayed_anonymous_challenge_cannot_invalidate_completed_authorization() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service
        .challenge_from_session(
            "installation",
            &h.server.installation(),
            None,
            "Bearer",
            false,
        )
        .await;
    assert_eq!(
        h.service.state("installation").await,
        Some(OAuthState::Authorized)
    );
    let mut session = h.connect().await.unwrap();
    assert_eq!(h.sink.browsers(), 1);
    session.shutdown().await;
    h.service.shutdown().await;
}

#[tokio::test]
async fn replaced_session_manager_cannot_invalidate_new_authorization() {
    let h = Harness::new(true).await;
    h.login().await;
    let old = h
        .service
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    let old_authorized = h
        .sink
        .events
        .lock()
        .unwrap()
        .iter()
        .find(|event| event.state == OAuthState::Authorized)
        .unwrap()
        .clone();
    h.service.disconnect("installation").await.unwrap();
    h.sink.events.lock().unwrap().clear();
    h.login().await;
    h.service
        .challenge_from_session(
            "installation",
            &h.server.installation(),
            Some(&old),
            "Bearer",
            false,
        )
        .await;
    assert_eq!(
        h.service.state("installation").await,
        Some(OAuthState::Authorized)
    );
    assert!(
        !h.service
            .event_is_current(&old_authorized, &h.server.installation())
            .await
    );
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}

#[tokio::test]
async fn queued_browser_effect_is_rejected_after_callback_and_cancellation() {
    let h = Harness::new(true).await;
    let event = h.start().await;
    assert!(
        h.service
            .event_is_current(&event, &h.server.installation())
            .await
    );
    h.service
        .callback("installation", 10, h.callback(&event))
        .await
        .unwrap();
    h.sink
        .wait_state(event.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    assert!(
        !h.service
            .event_is_current(&event, &h.server.installation())
            .await
    );
    h.service.disconnect("installation").await.unwrap();
    assert!(
        !h.service
            .event_is_current(&event, &h.server.installation())
            .await
    );
    h.service.shutdown().await;
}

#[tokio::test]
async fn sleep_during_registration_expires_intent_before_browser_effect() {
    let h = Harness::new(true).await;
    h.service.shutdown().await;
    h.server
        .data
        .registration_delay
        .store(true, Ordering::SeqCst);
    let clock = Arc::new(JumpClock(Mutex::new(std::time::SystemTime::now())));
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            clock: clock.clone(),
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .begin_install("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    service.challenge("installation", "Bearer", false).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.server.data.registrations.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    clock.jump_after_sleep();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(h.sink.browsers(), 0);
    assert_eq!(
        service.state("installation").await,
        Some(OAuthState::TimedOut)
    );
    service.shutdown().await;
}

#[tokio::test]
async fn repeated_install_of_saved_authorization_cannot_restart_browser_on_invalid_grant() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service
        .begin_install("installation", &h.server.installation(), 11, REDIRECT)
        .await
        .unwrap();
    h.server.data.token_mode.store(3, Ordering::SeqCst);
    h.server.data.deny_mcp.store(1, Ordering::SeqCst);
    assert_eq!(
        h.connect().await.err().unwrap().state,
        McpRuntimeState::AuthRequired
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.sink.browsers(), 1);
    assert_eq!(
        h.service.state("installation").await,
        Some(OAuthState::AuthRequired)
    );
    h.service.shutdown().await;
}

#[tokio::test]
async fn callback_preparation_failure_is_reported_only_after_protected_challenge() {
    let h = Harness::new(true).await;
    h.service
        .begin_install_without_callback("installation", &h.server.installation(), 10, "workspace")
        .await
        .unwrap();
    assert_eq!(h.sink.browsers(), 0);
    assert_eq!(
        h.connect().await.err().unwrap().state,
        McpRuntimeState::AuthRequired
    );
    let failure = h.sink.wait_state(None, OAuthState::Failed).await;
    assert_eq!(
        failure.diagnostic.as_deref(),
        Some("oauth_callback_preparation_failed")
    );
    assert!(failure.authorization_url.is_none());
    assert_eq!(h.sink.browsers(), 0);
    h.service
        .sign_in("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    let browser = h.sink.browser().await;
    h.service
        .callback("installation", 10, h.callback(&browser))
        .await
        .unwrap();
    h.sink
        .wait_state(browser.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    h.service.shutdown().await;
}

#[tokio::test]
async fn preparing_operation_is_cancellable_before_registration_returns() {
    let h = Harness::new(true).await;
    h.server
        .data
        .registration_delay
        .store(true, Ordering::SeqCst);
    h.service
        .sign_in("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    let preparing = h.sink.wait_state(None, OAuthState::Preparing).await;
    let operation = preparing
        .flow_id
        .as_deref()
        .expect("Preparing already owns an operation UUID");
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.server.data.registrations.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        h.service.cancel("installation", 10, operation),
    )
    .await
    .unwrap()
    .unwrap();
    h.sink
        .wait_state(Some(operation), OAuthState::Cancelled)
        .await;
    h.service.shutdown().await;
    assert_eq!(h.sink.browsers(), 0);
}

#[tokio::test]
async fn preparing_deadline_and_disappearing_initiator_publish_terminal_events() {
    for disappear in [false, true] {
        let h = Harness::new(true).await;
        h.service.shutdown().await;
        let service = McpOAuthService::with_options(
            h.persistence.clone(),
            h.sink.clone(),
            OAuthServiceOptions {
                install_timeout: Duration::from_millis(50),
                poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .unwrap();
        h.server
            .data
            .registration_delay
            .store(true, Ordering::SeqCst);
        service
            .sign_in("installation", &h.server.installation(), 10, REDIRECT)
            .await
            .unwrap();
        let preparing = h.sink.wait_state(None, OAuthState::Preparing).await;
        if disappear {
            h.sink.available.store(false, Ordering::SeqCst);
        }
        h.sink
            .wait_state(
                preparing.flow_id.as_deref(),
                if disappear {
                    OAuthState::Cancelled
                } else {
                    OAuthState::TimedOut
                },
            )
            .await;
        service.shutdown().await;
        assert_eq!(h.sink.browsers(), 0);
    }
}

#[tokio::test]
async fn unchanged_identity_preserves_pending_callback_across_timeout_update() {
    let h = Harness::new(true).await;
    let browser = h.start().await;
    let mut updated = h.server.installation();
    if let McpTransportConfig::StreamableHttp {
        tool_timeout_ms, ..
    } = &mut updated.transport
    {
        *tool_timeout_ms += 500;
    }
    h.service
        .synchronize("installation", &updated)
        .await
        .unwrap();
    assert!(h.service.event_is_current(&browser, &updated).await);
    h.service
        .callback("installation", 10, h.callback(&browser))
        .await
        .unwrap();
    h.sink
        .wait_state(browser.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    assert_eq!(h.sink.browsers(), 1);
    h.service.shutdown().await;
}

#[tokio::test]
async fn managed_clear_keeps_signed_out_admission_until_explicit_signin() {
    let h = Harness::new(true).await;
    h.login().await;
    assert!(h.service.cleanup_available("installation").await);
    let exchanges = h.server.data.exchanges.load(Ordering::SeqCst);
    let registrations = h.server.data.registrations.load(Ordering::SeqCst);
    h.service
        .disconnect_managed("installation", &h.server.installation(), 10, "workspace")
        .await
        .unwrap();
    assert_eq!(
        h.service.state("installation").await,
        Some(OAuthState::AuthRequired)
    );
    assert!(!h.service.cleanup_available("installation").await);
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    h.sink.events.lock().unwrap().clear();
    // Reconciliation/transport admission must fail immediately, not initiate
    // an anonymous connection, consent, exchange or registration after Clear.
    h.service
        .synchronize("installation", &h.server.installation())
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), h.connect())
        .await
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(error.state, McpRuntimeState::AuthRequired);
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), exchanges);
    assert_eq!(
        h.server.data.registrations.load(Ordering::SeqCst),
        registrations
    );
    assert_eq!(h.sink.browsers(), 0);
    assert!(!h.service.cleanup_available("installation").await);
    h.service
        .sign_in("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    let event = h.sink.browser().await;
    h.service
        .callback("installation", 10, h.callback(&event))
        .await
        .unwrap();
    h.sink
        .wait_state(event.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    assert!(h.service.cleanup_available("installation").await);
    h.service.shutdown().await;
}

#[tokio::test]
async fn redirect_mismatch_can_be_cleared_before_fresh_signin() {
    let h = Harness::new(true).await;
    h.login().await;
    let mut record = h.persistence.read("installation").await.unwrap().unwrap();
    record.registration.redirect_uri = "http://127.0.0.1:37644/oauth/mcp/callback".into();
    h.persistence.write("installation", record).await.unwrap();
    h.sink.events.lock().unwrap().clear();
    h.service
        .sign_in("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    let failure = h.sink.wait_state(None, OAuthState::Failed).await;
    assert_eq!(
        failure.diagnostic.as_deref(),
        Some("OAuth callback address does not match the saved registration")
    );
    assert_eq!(h.sink.browsers(), 0);
    // Changing Desktop's callback port must not silently rewrite or clear the
    // provider's saved registration. Explicit Clear is the recovery boundary.
    let unchanged = h.persistence.read("installation").await.unwrap().unwrap();
    assert_eq!(
        unchanged.registration.redirect_uri,
        "http://127.0.0.1:37644/oauth/mcp/callback"
    );
    assert!(unchanged.credentials.is_some());
    h.service.disconnect("installation").await.unwrap();
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    h.service
        .sign_in("installation", &h.server.installation(), 10, REDIRECT)
        .await
        .unwrap();
    let browser = h.sink.browser().await;
    h.service
        .callback("installation", 10, h.callback(&browser))
        .await
        .unwrap();
    h.sink
        .wait_state(browser.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 2);
    h.service.shutdown().await;
}

#[tokio::test]
async fn transient_refresh_has_one_recovery_event_and_normal_rotation_has_none() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service.shutdown().await;
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    let client = service
        .client("installation", &h.server.installation())
        .await
        .unwrap()
        .unwrap();
    service
        .transient_failure_from_session("installation", &h.server.installation(), Some(&client))
        .await;
    h.sink.wait_state(None, OAuthState::Failed).await;
    h.sink.wait_state(None, OAuthState::Recovered).await;
    let recovery_count = || {
        h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.state == OAuthState::Recovered)
            .count()
    };
    assert_eq!(recovery_count(), 1);
    client
        .auth_manager
        .lock()
        .await
        .refresh_token()
        .await
        .unwrap();
    assert_eq!(recovery_count(), 1);
    assert_eq!(h.sink.browsers(), 1);
    service.shutdown().await;
}

#[tokio::test]
async fn preregistered_post_method_survives_exchange_and_restart_refresh() {
    let h = Harness::new(true).await;
    h.server
        .data
        .preregistered_post
        .store(true, Ordering::SeqCst);
    h.store
        .put_string(
            &SecretId::mcp_secret("pre-post").unwrap(),
            "pre-secret",
            SecretMeta::new(pioneer_keystore::SecretKind::McpSecret, None, 0),
        )
        .unwrap();
    let mut installation = h.server.installation();
    installation.auth.oauth = Some(McpOAuthConfig {
        client_id: Some("pre-client".into()),
        client_secret_ref: Some("pre-post".into()),
        token_endpoint_auth_method: Some("client_secret_post".into()),
        ..Default::default()
    });
    h.service
        .sign_in("installation", &installation, 10, REDIRECT)
        .await
        .unwrap();
    let browser = h.sink.browser().await;
    h.service
        .callback("installation", 10, h.callback(&browser))
        .await
        .unwrap();
    h.sink
        .wait_state(browser.flow_id.as_deref(), OAuthState::Authorized)
        .await;
    h.service.shutdown().await;
    let restored = McpOAuthService::new(h.persistence.clone(), h.sink.clone()).unwrap();
    let client = restored
        .client("installation", &installation)
        .await
        .unwrap()
        .unwrap();
    client
        .auth_manager
        .lock()
        .await
        .refresh_token()
        .await
        .unwrap();
    assert_eq!(h.server.data.registrations.load(Ordering::SeqCst), 0);
    let requests = h.server.data.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|(headers, fields)| headers.get("authorization").is_none()
                && fields.get("client_id").map(String::as_str) == Some("pre-client")
                && fields.get("client_secret").map(String::as_str) == Some("pre-secret"))
    );
    drop(requests);
    restored.shutdown().await;
}

#[tokio::test]
async fn token_endpoint_failure_during_live_tool_call_recovers_without_replaying_tool() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service.shutdown().await;
    let service = McpOAuthService::with_options(
        h.persistence.clone(),
        h.sink.clone(),
        OAuthServiceOptions {
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    let mut session = RmcpRuntimeConnector::with_oauth(Arc::new(service.clone()))
        .connect(
            h.server.installation(),
            "installation".into(),
            Arc::new(Empty),
            0,
        )
        .await
        .unwrap();
    h.server.data.token_mode.store(2, Ordering::SeqCst);
    h.server.data.deny_mcp.store(4, Ordering::SeqCst);
    let error = session
        .call_tool(
            "echo",
            json!({}),
            McpInvocationBudget {
                max_arguments_bytes: 1024,
            },
            Duration::from_secs(3),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.kind,
        pioneer_mcp::McpRuntimeErrorKind::TransientRefresh
    );
    h.sink.wait_state(None, OAuthState::Failed).await;
    let record_before = h.persistence.read("installation").await.unwrap().unwrap();
    assert!(record_before.credentials.is_some());
    h.server.data.token_mode.store(0, Ordering::SeqCst);
    h.sink.wait_state(None, OAuthState::Recovered).await;
    assert_eq!(h.sink.browsers(), 1);
    let credentials = h
        .persistence
        .read("installation")
        .await
        .unwrap()
        .unwrap()
        .credentials
        .unwrap();
    assert_eq!(
        serde_json::to_value(credentials).unwrap()["token_response"]["access_token"],
        json!(format!(
            "access-{}",
            h.server.data.refreshes.load(Ordering::SeqCst)
        ))
    );
    assert_eq!(h.server.data.tools_executed.load(Ordering::SeqCst), 0);
    session.shutdown().await;
    service.shutdown().await;
}

// The injected clock pauses the exchange AFTER SDK save completes, before it
// acquires terminal admission. It does not pause inside a started blocking put.
struct CommitClock {
    now: Mutex<std::time::SystemTime>,
    arm: AtomicBool,
    block_next: AtomicBool,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    release: std::sync::Condvar,
}
impl CommitClock {
    fn new() -> Self {
        Self {
            now: Mutex::new(std::time::SystemTime::now()),
            arm: AtomicBool::new(false),
            block_next: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            release: std::sync::Condvar::new(),
        }
    }
    fn resume(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}
impl OAuthClock for CommitClock {
    fn now(&self) -> std::time::SystemTime {
        if self.block_next.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.release.wait(released).unwrap();
            }
        }
        *self.now.lock().unwrap()
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_decision_after_successful_put_linearizes_cancel_timeout_and_success() {
    for saved_grant in [false, true] {
        for decision in [
            OAuthState::Cancelled,
            OAuthState::TimedOut,
            OAuthState::Authorized,
        ] {
            let h = Harness::new(true).await;
            if saved_grant {
                h.login().await;
            }
            h.service.shutdown().await;
            h.sink.events.lock().unwrap().clear();
            let clock = Arc::new(CommitClock::new());
            *h.store.commit_clock.lock().unwrap() = Some(clock.clone());
            let service = McpOAuthService::with_options(
                h.persistence.clone(),
                h.sink.clone(),
                OAuthServiceOptions {
                    clock: clock.clone(),
                    poll_interval: Duration::from_secs(3600),
                    ..Default::default()
                },
            )
            .unwrap();
            let prior_client = if saved_grant {
                Some(
                    service
                        .client("installation", &h.server.installation())
                        .await
                        .unwrap()
                        .unwrap(),
                )
            } else {
                None
            };
            if saved_grant {
                service
                    .challenge(
                        "installation",
                        "Bearer error=\"insufficient_scope\", scope=\"admin\"",
                        true,
                    )
                    .await;
            }
            service
                .sign_in("installation", &h.server.installation(), 10, REDIRECT)
                .await
                .unwrap();
            let event = h.sink.browser().await;
            if saved_grant {
                assert!(
                    url::Url::parse(event.authorization_url.as_ref().unwrap())
                        .unwrap()
                        .query_pairs()
                        .any(|(key, value)| key == "scope"
                            && value.split_whitespace().any(|scope| scope == "admin"))
                );
            }
            let previous =
                serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap())
                    .unwrap();
            h.server.data.refreshes.store(7, Ordering::SeqCst);
            let pre_stage = Arc::new(PreStageRead::default());
            let _barrier_cleanup = ConsentBarrierCleanup {
                clock: clock.clone(),
                read: pre_stage.clone(),
            };
            *h.store.pre_stage_read.lock().unwrap() = Some(pre_stage.clone());
            pre_stage.armed.store(true, Ordering::SeqCst);
            clock.arm.store(true, Ordering::SeqCst);
            service
                .callback("installation", 10, h.callback(&event))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(3), pre_stage.entered.notified())
                .await
                .unwrap();
            // Exchange owns its lease, but has not staged consent yet. The old
            // real SDK manager must acquire its own ownership, not borrow Weak.
            let mut earlier_reader = prior_client.as_ref().map(|client| {
                let client = client.clone();
                tokio::spawn(async move { client.get_access_token().await })
            });
            if let Some(reader) = &mut earlier_reader {
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), reader)
                        .await
                        .is_err()
                );
            }
            pre_stage.resume();
            tokio::time::timeout(Duration::from_secs(3), clock.entered.notified())
                .await
                .unwrap();
            // The new grant is actually durable while its owner still holds the
            // refresh lease. Cancellation must restore the exact prior grant.
            let written =
                serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap())
                    .unwrap();
            assert_ne!(written, previous);
            assert!(written["pending_consent"]["candidate"].is_object());
            assert_eq!(written["credentials"], previous["credentials"]);
            if let Some(reader) = &mut earlier_reader {
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), reader)
                        .await
                        .is_err(),
                    "pre-stage reader must still wait through pending successful put"
                );
            }
            if decision == OAuthState::Cancelled {
                tokio::time::timeout(
                    Duration::from_millis(100),
                    service.cancel("installation", 10, event.flow_id.as_ref().unwrap()),
                )
                .await
                .unwrap()
                .unwrap();
            } else if decision == OAuthState::TimedOut {
                *clock.now.lock().unwrap() += Duration::from_secs(4000);
            }
            clock.resume();
            h.sink.wait_state(event.flow_id.as_deref(), decision).await;
            let persisted =
                serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap())
                    .unwrap();
            if let Some(reader) = earlier_reader {
                let token = reader.await.unwrap().unwrap();
                let expected = if decision == OAuthState::Authorized {
                    &written["pending_consent"]["candidate"]["token_response"]["access_token"]
                } else {
                    &previous["credentials"]["token_response"]["access_token"]
                };
                assert_eq!(token, expected.as_str().unwrap());
            }
            if decision == OAuthState::Authorized {
                let mut committed = written.clone();
                committed["credentials"] = committed["pending_consent"]["candidate"].clone();
                committed.as_object_mut().unwrap().remove("pending_consent");
                assert_eq!(persisted, committed);
                if let Some(client) = &prior_client {
                    assert_eq!(
                        client.get_access_token().await.unwrap(),
                        persisted["credentials"]["token_response"]["access_token"]
                            .as_str()
                            .unwrap()
                    );
                }
                assert!(
                    service
                        .cancel("installation", 10, event.flow_id.as_ref().unwrap())
                        .await
                        .is_err()
                );
            } else {
                assert_eq!(
                    persisted, previous,
                    "cancel/expiry must preserve the pre-consent atomic record"
                );
                if let Some(client) = &prior_client {
                    assert_eq!(
                        client.get_access_token().await.unwrap(),
                        previous["credentials"]["token_response"]["access_token"]
                            .as_str()
                            .unwrap()
                    );
                }
                assert!(
                    !h.sink
                        .events
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|event| event.state == OAuthState::Authorized)
                );
            }
            service.shutdown().await;
            let restored = McpOAuthService::new(h.persistence.clone(), h.sink.clone()).unwrap();
            restored
                .synchronize("installation", &h.server.installation())
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap())
                    .unwrap(),
                persisted
            );
            let connection = RmcpRuntimeConnector::with_oauth(Arc::new(restored.clone()))
                .connect(
                    h.server.installation(),
                    "installation".into(),
                    Arc::new(Empty),
                    0,
                )
                .await;
            if saved_grant || decision == OAuthState::Authorized {
                let mut session = connection.unwrap();
                assert_eq!(session.initial_catalog().tools_count(), 1);
                session.shutdown().await;
            } else {
                assert_eq!(
                    connection.err().unwrap().state,
                    McpRuntimeState::AuthRequired
                );
            }
            restored.shutdown().await;
        }
    }
}

#[tokio::test]
async fn explicit_sign_in_retries_without_challenge_on_fresh_and_cancelled_entries() {
    for cancelled_before in [false, true] {
        let h = Harness::new(true).await;
        if cancelled_before {
            let old = h.start().await;
            h.service
                .cancel("installation", 10, old.flow_id.as_ref().unwrap())
                .await
                .unwrap();
            h.sink
                .wait_state(old.flow_id.as_deref(), OAuthState::Cancelled)
                .await;
        }
        h.sink.events.lock().unwrap().clear();
        h.service.shutdown().await;
        let service = McpOAuthService::with_options(
            h.persistence.clone(),
            h.sink.clone(),
            OAuthServiceOptions {
                poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .unwrap();
        h.server
            .data
            .registration_unavailable
            .store(true, Ordering::SeqCst);
        // The registration-only case needs discovery to fail even when DCR is
        // reused: mode 1 is a transient discovery endpoint failure.
        if cancelled_before {
            h.server.data.discovery_mode.store(1, Ordering::SeqCst);
        }
        service
            .sign_in("installation", &h.server.installation(), 10, REDIRECT)
            .await
            .unwrap();
        let operation = h.sink.wait_state(None, OAuthState::Preparing).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if h.sink
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event.state == OAuthState::Preparing && event.diagnostic.is_some())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        h.server
            .data
            .registration_unavailable
            .store(false, Ordering::SeqCst);
        h.server.data.discovery_mode.store(0, Ordering::SeqCst);
        let browser = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(event) = h
                    .sink
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|event| event.authorization_url.is_some())
                    .cloned()
                {
                    break event;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(browser.flow_id, operation.flow_id);
        assert_eq!(h.sink.browsers(), 1);
        service
            .cancel("installation", 10, browser.flow_id.as_ref().unwrap())
            .await
            .unwrap();
        service.shutdown().await;
    }
}

#[tokio::test]
async fn explicit_sign_in_backoff_is_cancellable_and_keeps_original_deadline_without_challenge() {
    for timeout in [false, true] {
        let h = Harness::new(true).await;
        h.service.shutdown().await;
        h.server
            .data
            .registration_unavailable
            .store(true, Ordering::SeqCst);
        let service = McpOAuthService::with_options(
            h.persistence.clone(),
            h.sink.clone(),
            OAuthServiceOptions {
                install_timeout: if timeout {
                    Duration::from_millis(300)
                } else {
                    Duration::from_secs(30)
                },
                poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .sign_in("installation", &h.server.installation(), 10, REDIRECT)
            .await
            .unwrap();
        let preparing = h.sink.wait_state(None, OAuthState::Preparing).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if h.sink
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event.state == OAuthState::Preparing && event.diagnostic.is_some())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        if !timeout {
            service
                .cancel("installation", 10, preparing.flow_id.as_ref().unwrap())
                .await
                .unwrap();
        }
        h.sink
            .wait_state(
                preparing.flow_id.as_deref(),
                if timeout {
                    OAuthState::TimedOut
                } else {
                    OAuthState::Cancelled
                },
            )
            .await;
        h.server
            .data
            .registration_unavailable
            .store(false, Ordering::SeqCst);
        service.shutdown().await;
        assert_eq!(h.sink.browsers(), 0);
        assert!(h.persistence.read("installation").await.unwrap().is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_or_timed_out_consent_with_failed_rollback_cannot_authorize_after_restart() {
    for saved_grant in [false, true] {
        for timeout in [false, true] {
            let h = Harness::new(true).await;
            if saved_grant {
                h.login().await;
            }
            h.service.shutdown().await;
            h.sink.events.lock().unwrap().clear();
            let clock = Arc::new(CommitClock::new());
            let _barrier_cleanup = ConsentBarrierCleanup {
                clock: clock.clone(),
                read: Arc::new(PreStageRead::default()),
            };
            *h.store.commit_clock.lock().unwrap() = Some(clock.clone());
            let service = McpOAuthService::with_options(
                h.persistence.clone(),
                h.sink.clone(),
                OAuthServiceOptions {
                    clock: clock.clone(),
                    poll_interval: Duration::from_secs(3600),
                    ..Default::default()
                },
            )
            .unwrap();
            service
                .sign_in("installation", &h.server.installation(), 10, REDIRECT)
                .await
                .unwrap();
            let event = h.sink.browser().await;
            let previous =
                serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap())
                    .unwrap();
            clock.arm.store(true, Ordering::SeqCst);
            service
                .callback("installation", 10, h.callback(&event))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(3), clock.entered.notified())
                .await
                .unwrap();
            if timeout {
                *clock.now.lock().unwrap() += Duration::from_secs(4000);
            } else {
                service
                    .cancel("installation", 10, event.flow_id.as_ref().unwrap())
                    .await
                    .unwrap();
            }
            h.store.fail.store(true, Ordering::SeqCst);
            clock.resume();
            let failure = h
                .sink
                .wait_state(event.flow_id.as_deref(), OAuthState::Failed)
                .await;
            assert_eq!(
                failure.diagnostic.as_deref(),
                Some("OAuth storage rollback failed")
            );
            let pending =
                serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap())
                    .unwrap();
            assert!(pending["pending_consent"]["candidate"].is_object());
            assert_eq!(pending["credentials"], previous["credentials"]);
            assert!(
                service
                    .synchronize("installation", &h.server.installation())
                    .await
                    .is_err()
            );
            service.shutdown().await;
            let restarted = McpOAuthService::new(h.persistence.clone(), h.sink.clone()).unwrap();
            assert!(
                restarted
                    .client("installation", &h.server.installation())
                    .await
                    .is_err()
            );
            assert!(
                !h.sink
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event.state == OAuthState::Authorized)
            );
            h.store.fail.store(false, Ordering::SeqCst);
            restarted
                .synchronize("installation", &h.server.installation())
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(h.persistence.read("installation").await.unwrap().unwrap())
                    .unwrap(),
                previous
            );
            if saved_grant {
                assert!(
                    restarted
                        .client("installation", &h.server.installation())
                        .await
                        .unwrap()
                        .unwrap()
                        .get_access_token()
                        .await
                        .is_ok()
                );
            }
            restarted.shutdown().await;
        }
    }
}

#[derive(Default)]
struct PreStageRead {
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    resumed: std::sync::Condvar,
}
impl PreStageRead {
    fn resume(&self) {
        *self.released.lock().unwrap() = true;
        self.resumed.notify_all();
    }
}
struct ConsentBarrierCleanup {
    clock: Arc<CommitClock>,
    read: Arc<PreStageRead>,
}
impl Drop for ConsentBarrierCleanup {
    fn drop(&mut self) {
        self.read.resume();
        self.clock.resume();
    }
}

#[tokio::test]
async fn recovery_revision_becomes_stale_when_a_new_session_refresh_failure_arrives() {
    let h = Harness::new(true).await;
    h.login().await;
    let installation = h.server.installation();
    let client = h
        .service
        .client("installation", &installation)
        .await
        .unwrap()
        .unwrap();
    h.service
        .transient_failure_from_session("installation", &installation, Some(&client))
        .await;
    let recovered = h.sink.wait_state(None, OAuthState::Recovered).await;
    assert!(h.service.event_is_current(&recovered, &installation).await);
    h.server.data.token_mode.store(2, Ordering::SeqCst);
    h.service
        .transient_failure_from_session("installation", &installation, Some(&client))
        .await;
    assert!(!h.service.event_is_current(&recovered, &installation).await);
    assert_eq!(
        h.service.state("installation").await,
        Some(OAuthState::Failed)
    );
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consent_pending_and_promotion_post_mutation_errors_match_durable_outcome() {
    for saved_grant in [false, true] {
        for phase in [1, 2] {
            for after in [false, true] {
                for unknown in [false, true] {
                    let h = Harness::new(true).await;
                    if saved_grant {
                        h.login().await;
                    }
                    h.sink.events.lock().unwrap().clear();
                    h.service
                        .sign_in("installation", &h.server.installation(), 10, REDIRECT)
                        .await
                        .unwrap();
                    let event = h.sink.browser().await;
                    let previous = serde_json::to_value(
                        h.persistence.read("installation").await.unwrap().unwrap(),
                    )
                    .unwrap();
                    h.server.data.refreshes.store(19, Ordering::SeqCst);
                    h.store.mutation_phase.store(phase, Ordering::SeqCst);
                    h.store.mutation_after_write.store(after, Ordering::SeqCst);
                    h.store
                        .mutation_readback_failure
                        .store(unknown, Ordering::SeqCst);
                    h.service
                        .callback("installation", 10, h.callback(&event))
                        .await
                        .unwrap();
                    let success = after && !unknown;
                    let terminal = h
                        .sink
                        .wait_state(
                            event.flow_id.as_deref(),
                            if success {
                                OAuthState::Authorized
                            } else {
                                OAuthState::Failed
                            },
                        )
                        .await;
                    assert_eq!(terminal.state == OAuthState::Authorized, success);
                    // Restoring reads does not itself approve an unconfirmed candidate.
                    h.store.reads_failed.store(false, Ordering::SeqCst);
                    h.store.mutation_phase.store(0, Ordering::SeqCst);
                    let independent = OAuthPersistence::new(h.store.clone(), None);
                    let visible = serde_json::to_value(
                        independent.read("installation").await.unwrap().unwrap(),
                    )
                    .unwrap();
                    if !success {
                        assert_eq!(visible["credentials"], previous["credentials"]);
                    } else {
                        assert!(visible["pending_consent"].is_null());
                    }
                    h.service
                        .synchronize("installation", &h.server.installation())
                        .await
                        .unwrap();
                    let same = serde_json::to_value(
                        independent.read("installation").await.unwrap().unwrap(),
                    )
                    .unwrap();
                    assert_eq!(same["credentials"], visible["credentials"]);
                    h.service.shutdown().await;
                    let restarted =
                        McpOAuthService::new(independent.clone(), h.sink.clone()).unwrap();
                    restarted
                        .synchronize("installation", &h.server.installation())
                        .await
                        .unwrap();
                    let recovered = serde_json::to_value(
                        independent.read("installation").await.unwrap().unwrap(),
                    )
                    .unwrap();
                    if !success {
                        assert_eq!(recovered, previous);
                    }
                    if success || saved_grant {
                        let token = restarted
                            .client("installation", &h.server.installation())
                            .await
                            .unwrap()
                            .unwrap()
                            .get_access_token()
                            .await
                            .unwrap();
                        if success {
                            assert_eq!(token, "access-19");
                        } else {
                            assert_eq!(
                                token,
                                previous["credentials"]["token_response"]["access_token"]
                                    .as_str()
                                    .unwrap()
                            );
                        }
                    } else {
                        match restarted
                            .client("installation", &h.server.installation())
                            .await
                        {
                            Ok(Some(client)) => assert!(matches!(
                                client.get_access_token().await,
                                Err(rmcp::transport::auth::AuthError::AuthorizationRequired)
                            )),
                            Err(error) => {
                                assert_eq!(error.state, pioneer_mcp::McpRuntimeState::AuthRequired)
                            }
                            Ok(None) => panic!("stored registration must restore its manager"),
                        }
                    }
                    restarted.shutdown().await;
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_for_admitted_bind_cleanup_before_final_mutation_drain() {
    let h = Harness::new(true).await;
    h.login().await;
    h.service.shutdown().await;
    let previous = h.persistence.read("installation").await.unwrap().unwrap();
    let mut pending = serde_json::to_value(&previous).unwrap();
    pending["pending_consent"] =
        json!({"previous":previous,"candidate":pending["credentials"].clone(),"committed":false});
    h.persistence
        .write("installation", serde_json::from_value(pending).unwrap())
        .await
        .unwrap();
    let read = Arc::new(PreStageRead {
        armed: AtomicBool::new(true),
        entered: tokio::sync::Notify::new(),
        released: Mutex::new(false),
        resumed: std::sync::Condvar::new(),
    });
    struct Release(Arc<PreStageRead>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.resume();
        }
    }
    let _release = Release(read.clone());
    *h.store.pre_stage_read.lock().unwrap() = Some(read.clone());
    let service = McpOAuthService::new(h.persistence.clone(), h.sink.clone()).unwrap();
    let owner = service.clone();
    let installation = h.server.installation();
    let caller =
        tokio::spawn(async move { owner.synchronize("installation", &installation).await });
    tokio::time::timeout(Duration::from_secs(3), read.entered.notified())
        .await
        .unwrap();
    let before = h.store.mutations.load(Ordering::SeqCst);
    let owner = service.clone();
    let mut shutdown = tokio::spawn(async move { owner.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(h.store.mutations.load(Ordering::SeqCst), before);
    assert!(
        service
            .synchronize("late", &h.server.installation())
            .await
            .is_err()
    );
    read.resume();
    assert!(
        caller.await.unwrap().is_err(),
        "no replacement publication after close"
    );
    shutdown.await.unwrap();
    assert!(
        h.store.mutations.load(Ordering::SeqCst) > before,
        "admitted cleanup is drained before return"
    );
    let finished = h.store.mutations.load(Ordering::SeqCst);
    assert!(service.disconnect("installation").await.is_err());
    assert_eq!(h.store.mutations.load(Ordering::SeqCst), finished);
    let recovered = h.persistence.read("installation").await.unwrap().unwrap();
    assert!(recovered.pending_consent.is_none());
    assert_eq!(
        serde_json::to_value(recovered).unwrap(),
        serde_json::to_value(previous).unwrap()
    );
}

#[cfg(feature = "test-support")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncertain_promotion_is_manageable_and_retired_without_false_terminal_outcome() {
    // Successful reconciliation, deadline, initiator loss, clear, replacement, shutdown.
    for retirement in 0..6 {
        let h = Harness::new(true).await;
        h.login().await;
        h.service.shutdown().await;
        h.sink.events.lock().unwrap().clear();
        let clock = Arc::new(CommitClock::new());
        let service = McpOAuthService::with_options(
            h.persistence.clone(),
            h.sink.clone(),
            OAuthServiceOptions {
                clock: clock.clone(),
                poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .sign_in("installation", &h.server.installation(), 10, REDIRECT)
            .await
            .unwrap();
        let browser = h.sink.browser().await;
        h.store.uncertain_delete.store(true, Ordering::SeqCst);
        service
            .callback("installation", 10, h.callback(&browser))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), h.store.marker_deleted.notified())
            .await
            .unwrap();
        let resolving = h
            .sink
            .wait_state(browser.flow_id.as_deref(), OAuthState::Resolving)
            .await;
        assert_eq!(
            service.state("installation").await,
            Some(OAuthState::Resolving)
        );
        assert!(
            service
                .event_is_current(&resolving, &h.server.installation())
                .await
        );
        assert!(
            service
                .cancel("installation", 10, browser.flow_id.as_deref().unwrap())
                .await
                .is_err(),
            "consent winner cannot be retroactively cancelled"
        );
        let independent = OAuthPersistence::new(h.store.clone(), None);
        assert!(independent.read("installation").await.is_err());
        if retirement == 1 {
            *clock.now.lock().unwrap() += Duration::from_secs(1200);
        }
        if retirement == 2 {
            h.sink.available.store(false, Ordering::SeqCst);
        }
        if retirement == 1 || retirement == 2 {
            tokio::time::timeout(Duration::from_secs(3), async {
                while !service
                    .resolution_finished_for_test(
                        "installation",
                        browser.flow_id.as_deref().unwrap(),
                    )
                    .await
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                service.state("installation").await,
                Some(OAuthState::Resolving)
            );
        }
        if retirement == 3 || retirement == 4 {
            h.store.uncertain_delete.store(false, Ordering::SeqCst);
        }
        if retirement == 4 {
            let mut replacement = h.server.installation();
            if let McpTransportConfig::StreamableHttp { url, .. } = &mut replacement.transport {
                url.push_str("/replacement");
            }
            service
                .synchronize("installation", &replacement)
                .await
                .unwrap();
        }
        if retirement == 3 {
            tokio::time::timeout(Duration::from_secs(3), service.disconnect("installation"))
                .await
                .unwrap()
                .unwrap();
        }
        if retirement == 5 {
            tokio::time::timeout(Duration::from_secs(3), service.shutdown())
                .await
                .unwrap();
        }
        assert!(!h.sink.events.lock().unwrap().iter().any(|e| matches!(
            e.state,
            OAuthState::Authorized
                | OAuthState::Failed
                | OAuthState::TimedOut
                | OAuthState::Cancelled
        )));
        h.store.uncertain_delete.store(false, Ordering::SeqCst);
        h.store.marker_unreadable.store(false, Ordering::SeqCst);
        if retirement == 0 {
            h.sink
                .wait_state(browser.flow_id.as_deref(), OAuthState::Authorized)
                .await;
            assert!(
                independent
                    .read("installation")
                    .await
                    .unwrap()
                    .unwrap()
                    .credentials
                    .is_some()
            );
        } else if retirement == 3 {
            assert!(independent.read("installation").await.unwrap().is_none());
        } else if retirement == 4 {
            assert!(independent.read("installation").await.unwrap().is_none());
        } else {
            let restarted = McpOAuthService::new(independent.clone(), h.sink.clone()).unwrap();
            restarted
                .synchronize("installation", &h.server.installation())
                .await
                .unwrap();
            assert_eq!(
                restarted.state("installation").await,
                Some(OAuthState::Authorized),
                "readable confirmed durable outcome is restored"
            );
            restarted.shutdown().await;
        }
        service.shutdown().await;
        if retirement != 0 {
            assert!(
                !h.sink
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|e| e.state == OAuthState::Authorized && e.flow_id == browser.flow_id)
            );
        }
        if retirement != 0 {
            assert!(
                !service
                    .event_is_current(&resolving, &h.server.installation())
                    .await
            );
        }
    }
}

#[cfg(feature = "test-support")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn winner_retirement_before_notification_preserves_resolving_projection() {
    for deadline in [false, true] {
        let h = Harness::new(true).await;
        h.service.shutdown().await;
        let hooks = Arc::new(OAuthTestHooks::default());
        hooks.pause_after_winner.store(true, Ordering::SeqCst);
        struct Release(Arc<OAuthTestHooks>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.publish_resolution.notify_one();
            }
        }
        let _release = Release(hooks.clone());
        let clock = Arc::new(CommitClock::new());
        let service = McpOAuthService::with_options(
            h.persistence.clone(),
            h.sink.clone(),
            OAuthServiceOptions {
                test_hooks: Some(hooks.clone()),
                clock: clock.clone(),
                poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .sign_in("installation", &h.server.installation(), 10, REDIRECT)
            .await
            .unwrap();
        let browser = h.sink.browser().await;
        service
            .callback("installation", 10, h.callback(&browser))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), hooks.winner_reserved.notified())
            .await
            .unwrap();
        assert_eq!(
            service.state("installation").await,
            Some(OAuthState::Resolving)
        );
        if deadline {
            *clock.now.lock().unwrap() += Duration::from_secs(1200);
        } else {
            h.sink.available.store(false, Ordering::SeqCst);
        }
        tokio::time::timeout(Duration::from_secs(3), hooks.retirement_observed.notified())
            .await
            .unwrap();
        assert_eq!(
            service.state("installation").await,
            Some(OAuthState::Resolving)
        );
        hooks.publish_resolution.notify_one();
        let event = h
            .sink
            .wait_state(browser.flow_id.as_deref(), OAuthState::Resolving)
            .await;
        assert!(
            service
                .event_is_current(&event, &h.server.installation())
                .await
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while !service
                .resolution_finished_for_test("installation", browser.flow_id.as_deref().unwrap())
                .await
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!h.sink.events.lock().unwrap().iter().any(|e| matches!(
            e.state,
            OAuthState::Authorized
                | OAuthState::Failed
                | OAuthState::TimedOut
                | OAuthState::Cancelled
        )));
        // Management clear remains usable even when the original client is gone.
        service.disconnect("installation").await.unwrap();
        assert!(h.persistence.read("installation").await.unwrap().is_none());
        let retired = h
            .sink
            .wait_state(browser.flow_id.as_deref(), OAuthState::Retired)
            .await;
        assert_eq!(retired.client_id, Some(10));
        assert!(retired.authorization_url.is_none());
        service.shutdown().await;
    }
}
#[cfg(feature = "test-support")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolving_retirement_targets_old_client_and_preserves_equal_identity_flow() {
    for replacement in 0..6 {
        let h = Harness::new(true).await;
        h.service.shutdown().await;
        let service = McpOAuthService::with_options(
            h.persistence.clone(),
            h.sink.clone(),
            OAuthServiceOptions {
                poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .unwrap();
        service
            .sign_in("installation", &h.server.installation(), 10, REDIRECT)
            .await
            .unwrap();
        let browser = h.sink.browser().await;
        h.store.uncertain_delete.store(true, Ordering::SeqCst);
        service
            .callback("installation", 10, h.callback(&browser))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), h.store.marker_deleted.notified())
            .await
            .unwrap();
        let resolving = h
            .sink
            .wait_state(browser.flow_id.as_deref(), OAuthState::Resolving)
            .await;
        let mut equal = h.server.installation();
        if let McpTransportConfig::StreamableHttp {
            tool_timeout_ms, ..
        } = &mut equal.transport
        {
            *tool_timeout_ms += 100;
        }
        service.synchronize("installation", &equal).await.unwrap();
        assert!(service.event_is_current(&resolving, &equal).await);
        assert!(
            !h.sink
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.state == OAuthState::Retired)
        );
        h.store.uncertain_delete.store(false, Ordering::SeqCst);
        let mut new = equal.clone();
        if replacement == 0 {
            new.auth.oauth = Some(McpOAuthConfig {
                scopes: vec!["additional".into()],
                ..Default::default()
            });
            service
                .sign_in("installation", &new, 11, REDIRECT)
                .await
                .unwrap();
        }
        if replacement == 1 {
            if let McpTransportConfig::StreamableHttp { headers, .. } = &mut new.transport {
                headers.insert(
                    "Authorization".into(),
                    pioneer_mcp::McpConfigValue::Literal {
                        value: "test-header".into(),
                    },
                );
            }
            service.synchronize("installation", &new).await.unwrap();
        }
        if replacement == 2 {
            new.transport = McpTransportConfig::Stdio {
                command: "mcp-remote".into(),
                args: vec!["https://example.invalid/mcp".into()],
                cwd: None,
                env: Default::default(),
                startup_timeout_ms: 5000,
                tool_timeout_ms: 120000,
            };
            service.synchronize("installation", &new).await.unwrap();
        }
        if replacement == 5 {
            if let McpTransportConfig::StreamableHttp { url, .. } = &mut new.transport {
                url.push_str("/public");
            }
            service.synchronize("installation", &new).await.unwrap();
        }
        if replacement == 3 {
            service.suspend("installation").await.unwrap();
        }
        if replacement == 4 {
            service.disconnect("installation").await.unwrap();
        }
        let retired = h
            .sink
            .wait_state(browser.flow_id.as_deref(), OAuthState::Retired)
            .await;
        assert_eq!(retired.client_id, Some(10));
        assert_eq!(retired.flow_id, browser.flow_id);
        assert!(retired.authorization_url.is_none());
        assert!(service.event_is_current(&retired, &new).await);
        assert!(!service.event_is_current(&resolving, &new).await);
        if replacement == 0 {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(event) = h
                        .sink
                        .events
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|e| {
                            e.state == OAuthState::AwaitingCallback && e.client_id == Some(11)
                        })
                        .cloned()
                    {
                        break event;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
        h.store.marker_unreadable.store(false, Ordering::SeqCst);
        service.shutdown().await;
    }
}

#[tokio::test]
async fn cleanup_synchronization_and_suspend_preserve_management_without_restoring_consent() {
    let h = Harness::new(true).await;
    h.login().await;
    h.sink.events.lock().unwrap().clear();
    let installation = h.server.installation();
    let exchanges = h.server.data.exchanges.load(Ordering::SeqCst);
    h.store.uncertain_delete.store(true, Ordering::SeqCst);
    assert!(
        h.service
            .disconnect_managed("installation", &installation, 10, "workspace")
            .await
            .is_err()
    );
    h.service
        .synchronize("installation", &installation)
        .await
        .unwrap();
    h.service.suspend("installation").await.unwrap();
    assert!(
        h.service
            .bound_to_installation("installation", &installation)
            .await
    );
    assert_eq!(
        h.service.state("installation").await,
        Some(OAuthState::CleanupRequired)
    );
    assert!(
        McpOAuthProvider::client(&h.service, "installation", &installation)
            .await
            .is_err()
    );
    assert!(
        h.service
            .sign_in("installation", &installation, 10, REDIRECT)
            .await
            .is_err()
    );
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), exchanges);
    assert_eq!(h.sink.browsers(), 0);
    assert!(
        !h.sink
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.state == OAuthState::Authorized)
    );
    h.store.uncertain_delete.store(false, Ordering::SeqCst);
    h.store.marker_unreadable.store(false, Ordering::SeqCst);
    h.service
        .disconnect_managed("installation", &installation, 10, "workspace")
        .await
        .unwrap();
    assert!(h.persistence.read("installation").await.unwrap().is_none());
    assert_eq!(
        h.service.state("installation").await,
        Some(OAuthState::AuthRequired)
    );
    assert!(!h.service.cleanup_available("installation").await);
    assert_eq!(
        McpOAuthProvider::client(&h.service, "installation", &installation)
            .await
            .err()
            .expect("confirmed Clear must require explicit sign-in")
            .state,
        McpRuntimeState::AuthRequired
    );
    assert_eq!(h.server.data.exchanges.load(Ordering::SeqCst), exchanges);
    assert_eq!(h.sink.browsers(), 0);
    h.service.shutdown().await;
}
