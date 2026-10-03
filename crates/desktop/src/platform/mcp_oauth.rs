//! Native loopback callback on the browser device, including remote Gateways.
use pioneer_client::mcp::oauth::{
    McpOAuthShell, OAuthBrowserAdmission, OAuthBrowserEffectResult, OAuthCallbackFields,
    OAuthCallbackRelay, OAuthPreparationError, OAuthPresentation,
};
use pioneer_protocol::AuthSecretString;
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
const CALLBACK_PATH: &str = "/oauth/mcp/callback";
struct Pending {
    flow: String,
    relay: OAuthCallbackRelay,
    admission: OAuthBrowserAdmission,
    deadline: SystemTime,
}
struct Listener {
    callback_port: u16,
    pending: Mutex<HashMap<String, Pending>>,
    stopped: AtomicBool,
    prepared: Mutex<SystemTime>,
}
pub(crate) struct DesktopMcpOAuthShell {
    callback_port: Result<std::num::NonZeroU16, OAuthPreparationError>,
    listener: Mutex<Option<Arc<Listener>>>,
    browser: Arc<dyn Fn(&str) -> bool + Send + Sync>,
}
impl DesktopMcpOAuthShell {
    pub(crate) fn new(
        config: Result<pioneer_config::DesktopMcpOAuthConfig, OAuthPreparationError>,
    ) -> Self {
        Self {
            callback_port: config.map(|config| config.callback_port),
            listener: Mutex::new(None),
            browser: Arc::new(open_browser),
        }
    }
}
impl McpOAuthShell for DesktopMcpOAuthShell {
    fn configuration_error(&self) -> Option<OAuthPreparationError> {
        self.callback_port.as_ref().err().copied()
    }

    fn prepare(&self) -> anyhow::Result<String> {
        let port = self
            .callback_port
            .as_ref()
            .map_err(|reason| anyhow::Error::new(*reason))?
            .get();
        let redirect = format!("http://127.0.0.1:{port}{CALLBACK_PATH}");
        let mut owner = self.listener.lock().expect("OAuth listener owner poisoned");
        if let Some(listener) = owner
            .as_ref()
            .filter(|l| !l.stopped.load(Ordering::Acquire))
        {
            *listener.prepared.lock().unwrap() = SystemTime::now();
            return Ok(redirect);
        }
        // A stopped worker may still be finishing its last callback response.
        // Retry only our own closing listener, never change the registered URI.
        let closing_ours = owner
            .as_ref()
            .is_some_and(|l| l.stopped.load(Ordering::Acquire));
        let mut socket = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port));
        if closing_ours {
            for _ in 0..10 {
                if socket.is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
                socket = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port));
            }
        }
        let socket =
            socket.map_err(|_| anyhow::Error::new(OAuthPreparationError::PortUnavailable))?;
        socket.set_nonblocking(true)?;
        let listener = Arc::new(Listener {
            callback_port: port,
            pending: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
            prepared: Mutex::new(SystemTime::now()),
        });
        let worker = listener.clone();
        std::thread::Builder::new()
            .name("desktop-mcp-oauth-callback".into())
            .spawn(move || {
                while !worker.stopped.load(Ordering::Acquire) {
                    worker.pending.lock().unwrap().retain(|_, pending| {
                        if pending.deadline > SystemTime::now() {
                            true
                        } else {
                            pending.admission.retire();
                            false
                        }
                    });
                    if worker.pending.lock().unwrap().is_empty()
                        && worker
                            .prepared
                            .lock()
                            .unwrap()
                            .elapsed()
                            .unwrap_or_default()
                            > Duration::from_secs(600)
                    {
                        worker.stopped.store(true, Ordering::Release);
                        break;
                    }
                    match socket.accept() {
                        Ok((stream, _)) => handle_callback(stream, &worker),
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(25))
                        }
                        Err(_) => break,
                    }
                }
                drop(socket);
                worker.stopped.store(true, Ordering::Release);
            })?;
        *owner = Some(listener);
        Ok(redirect)
    }
    fn authorize(
        &self,
        event: &OAuthPresentation,
        relay: OAuthCallbackRelay,
        admission: OAuthBrowserAdmission,
    ) -> OAuthBrowserEffectResult {
        if !admission.is_current() {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        }
        // Another flow may have released the shared listener while this install
        // was recovering from a network failure. Rebind the same registered URI.
        if self.prepare().is_err() {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        }
        let Some(url) = event.authorization_url.as_ref() else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let Ok(parsed) = url::Url::parse(url.expose_secret()) else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        if !matches!(parsed.scheme(), "http" | "https")
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        }
        let Some(state) = parsed
            .query_pairs()
            .find(|(k, _)| k == "state")
            .map(|(_, v)| v.into_owned())
        else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let Some(flow) = event.flow_id.as_ref() else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let owner = self.listener.lock().unwrap();
        let Some(listener) = owner
            .as_ref()
            .filter(|l| !l.stopped.load(Ordering::Acquire))
        else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let mut pending = listener.pending.lock().unwrap();
        if pending.contains_key(&state) {
            return OAuthBrowserEffectResult::Opened;
        }
        pending.insert(
            state,
            Pending {
                flow: flow.clone(),
                relay,
                admission: admission.clone(),
                deadline: SystemTime::now() + Duration::from_secs(300),
            },
        );
        drop(pending);
        drop(owner);
        if !admission.claim() {
            self.release(flow);
            return OAuthBrowserEffectResult::CallbackUnavailable;
        }
        let opened = (self.browser)(url.expose_secret());
        admission.complete();
        if opened {
            OAuthBrowserEffectResult::Opened
        } else {
            OAuthBrowserEffectResult::BrowserUnavailable
        }
    }
    fn retry_authorize(&self, event: &OAuthPresentation) -> OAuthBrowserEffectResult {
        let Some(url) = &event.authorization_url else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let owner = self.listener.lock().unwrap();
        let Some(listener) = owner
            .as_ref()
            .filter(|listener| !listener.stopped.load(Ordering::Acquire))
        else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let pending = listener.pending.lock().unwrap();
        let Some(admission) = pending
            .values()
            .find(|entry| {
                Some(&entry.flow) == event.flow_id.as_ref() && entry.deadline > SystemTime::now()
            })
            .map(|entry| entry.admission.clone())
        else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        drop(pending);
        drop(owner);
        if !admission.claim() {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        }
        let opened = (self.browser)(url.expose_secret());
        admission.complete();
        if opened {
            OAuthBrowserEffectResult::Opened
        } else {
            OAuthBrowserEffectResult::BrowserUnavailable
        }
    }
    fn release(&self, flow: &str) {
        if let Some(listener) = self.listener.lock().unwrap().as_ref() {
            let mut pending = listener.pending.lock().unwrap();
            pending.retain(|_, p| {
                if p.flow == flow {
                    p.admission.retire();
                    false
                } else {
                    true
                }
            });
            if pending.is_empty() {
                *listener.prepared.lock().unwrap() = SystemTime::now() - Duration::from_secs(601);
            }
        }
    }
    fn shutdown(&self) {
        if let Some(listener) = self.listener.lock().unwrap().take() {
            let mut pending = listener.pending.lock().unwrap();
            for entry in pending.values() {
                entry.admission.retire();
            }
            pending.clear();
            listener.stopped.store(true, Ordering::Release);
        }
    }
}
impl Drop for DesktopMcpOAuthShell {
    fn drop(&mut self) {
        self.shutdown();
    }
}
fn handle_callback(mut stream: TcpStream, listener: &Listener) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
    let mut bytes = [0u8; 8192];
    let result = (|| {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut count = 0;
        loop {
            stream
                .set_read_timeout(Some(deadline.checked_duration_since(Instant::now())?))
                .ok()?;
            let received = stream.read(bytes.get_mut(count..)?).ok()?;
            if received == 0 {
                return None;
            }
            count += received;
            if bytes[..count].windows(4).any(|part| part == b"\r\n\r\n") {
                break;
            }
            if count == bytes.len() {
                return None;
            }
        }
        let request = std::str::from_utf8(&bytes[..count]).ok()?;
        let mut line = request.lines().next()?.split_whitespace();
        if line.next() != Some("GET") {
            return None;
        }
        let target = line.next()?;
        if !target.starts_with("/oauth/mcp/callback?") {
            return None;
        }
        let url = url::Url::parse(&format!(
            "http://127.0.0.1:{}{target}",
            listener.callback_port
        ))
        .ok()?;
        let fields = parse_fields(&url)?;
        let state = fields.state.expose_secret().to_owned();
        let relay = listener
            .pending
            .lock()
            .unwrap()
            .get(&state)
            .filter(|p| p.deadline > SystemTime::now())
            .map(|p| p.relay.clone())?;
        let accepted = relay(fields);
        if accepted {
            if let Some(entry) = listener.pending.lock().unwrap().remove(&state) {
                entry.admission.retire();
            }
        }
        Some(accepted)
    })()
    .unwrap_or(false);
    let body = if result {
        rust_i18n::t!("mcp.oauth.callback_received").to_string()
    } else {
        rust_i18n::t!("mcp.oauth.callback_rejected").to_string()
    };
    let status = if result {
        "202 Accepted"
    } else {
        "400 Bad Request"
    };
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nContent-Security-Policy: default-src 'none'\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
}
fn parse_fields(url: &url::Url) -> Option<OAuthCallbackFields> {
    let mut fields = HashMap::new();
    for (key, value) in url.query_pairs() {
        if fields
            .insert(key.into_owned(), value.into_owned())
            .is_some()
        {
            return None;
        }
    }
    Some(OAuthCallbackFields {
        state: AuthSecretString::new(fields.remove("state")?),
        code: fields.remove("code").map(AuthSecretString::new),
        issuer: fields.remove("iss"),
        error: fields.remove("error"),
    })
}
fn open_browser(url: &str) -> bool {
    // Invoked by Client's owned gateway-events/actions background threads,
    // never the GPUI thread. Keep hardened/default-browser policy unchanged.
    // webbrowser trace/debug output can include the target URL. Pioneer uses
    // the tracing log bridge; silence this call instead of recording OAuth state.
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        webbrowser::open(url).is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(listener: Arc<Listener>, query: &str) -> String {
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = socket.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            handle_callback(socket.accept().unwrap().0, &listener);
        });
        let mut client = TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        write!(
            client,
            "GET /oauth/mcp/callback?{query} HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        worker.join().unwrap();
        response
    }
    #[test]
    fn loopback_callback_relays_once_and_never_echoes_code_or_state() {
        use std::sync::atomic::AtomicUsize;
        let calls = Arc::new(AtomicUsize::new(0));
        let received = calls.clone();
        let listener = Arc::new(Listener {
            callback_port: 49152,
            pending: Mutex::new(HashMap::from([(
                "state-canary".into(),
                Pending {
                    flow: "flow".into(),
                    deadline: SystemTime::now() + Duration::from_secs(30),
                    admission: OAuthBrowserAdmission::default(),
                    relay: Arc::new(move |fields| {
                        assert_eq!(fields.code.as_ref().unwrap().expose_secret(), "code-canary");
                        assert_eq!(fields.issuer.as_deref(), Some("https://issuer.test"));
                        received.fetch_add(1, Ordering::SeqCst);
                        true
                    }),
                },
            )])),
            prepared: Mutex::new(SystemTime::now()),
            stopped: AtomicBool::new(false),
        });
        assert!(
            request(listener.clone(), "state=wrong&code=code-canary").starts_with("HTTP/1.1 400")
        );
        let response = request(
            listener.clone(),
            "state=state-canary&code=code-canary&iss=https%3A%2F%2Fissuer.test",
        );
        assert!(response.starts_with("HTTP/1.1 202 Accepted"));
        assert!(!response.contains("canary"));
        assert!(response.contains("Cache-Control: no-store"));
        assert!(
            request(listener, "state=state-canary&code=code-canary").starts_with("HTTP/1.1 400")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn failed_launcher_keeps_relay_retry_and_expired_or_retired_operation_cannot_launch() {
        use std::sync::atomic::AtomicUsize;
        let listener = Arc::new(Listener {
            callback_port: 49152,
            pending: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
            prepared: Mutex::new(SystemTime::now()),
        });
        let launches = Arc::new(AtomicUsize::new(0));
        let count = launches.clone();
        let ready = listener.clone();
        let shell = DesktopMcpOAuthShell {
            callback_port: Ok(std::num::NonZeroU16::new(49152).unwrap()),
            listener: Mutex::new(Some(listener.clone())),
            browser: Arc::new(move |_| {
                assert!(!ready.stopped.load(Ordering::Acquire));
                assert!(
                    ready
                        .pending
                        .lock()
                        .unwrap()
                        .values()
                        .any(|entry| entry.flow == "flow")
                );
                count.fetch_add(1, Ordering::SeqCst) != 0
            }),
        };
        let event = OAuthPresentation {
            workspace_id: "workspace".into(),
            server_id: "server".into(),
            name: "server".into(),
            scope_kind: pioneer_protocol::McpScopeKind::Workspace,
            flow_id: Some("flow".into()),
            state: pioneer_protocol::McpOAuthState::AwaitingCallback,
            authorization_url: Some(AuthSecretString::new(
                "https://provider.test/authorize?state=state",
            )),
            diagnostic: None,
        };
        assert_eq!(
            shell.authorize(&event, Arc::new(|_| true), OAuthBrowserAdmission::default()),
            OAuthBrowserEffectResult::BrowserUnavailable
        );
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        shell.authorize(
            &event,
            Arc::new(|_| panic!("duplicate must preserve relay")),
            OAuthBrowserAdmission::default(),
        );
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert_eq!(
            shell.retry_authorize(&event),
            OAuthBrowserEffectResult::Opened
        );
        assert_eq!(launches.load(Ordering::SeqCst), 2);
        let retired = OAuthBrowserAdmission::default();
        retired.retire();
        assert_eq!(
            shell.authorize(&event, Arc::new(|_| panic!("retired relay")), retired),
            OAuthBrowserEffectResult::CallbackUnavailable
        );
        assert_eq!(launches.load(Ordering::SeqCst), 2);
        listener
            .pending
            .lock()
            .unwrap()
            .get_mut("state")
            .unwrap()
            .deadline = SystemTime::UNIX_EPOCH;
        assert_eq!(
            shell.retry_authorize(&event),
            OAuthBrowserEffectResult::CallbackUnavailable
        );
        shell.release("flow");
        assert_eq!(
            shell.retry_authorize(&event),
            OAuthBrowserEffectResult::CallbackUnavailable
        );
        assert_eq!(launches.load(Ordering::SeqCst), 2);
    }

    fn test_shell(port: u16) -> DesktopMcpOAuthShell {
        let mut shell = DesktopMcpOAuthShell::new(Ok(pioneer_config::DesktopMcpOAuthConfig {
            callback_port: std::num::NonZeroU16::new(port).unwrap(),
        }));
        shell.browser = Arc::new(|_| false);
        shell
    }

    fn test_event(flow: &str, state: &str) -> OAuthPresentation {
        OAuthPresentation {
            workspace_id: "workspace".into(),
            server_id: "server".into(),
            name: "server".into(),
            scope_kind: pioneer_protocol::McpScopeKind::Workspace,
            flow_id: Some(flow.into()),
            state: pioneer_protocol::McpOAuthState::AwaitingCallback,
            authorization_url: Some(AuthSecretString::new(format!(
                "https://provider.test/authorize?state={state}"
            ))),
            diagnostic: None,
        }
    }

    fn wire_callback(port: u16, state: &str) -> String {
        let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        write!(
            client,
            "GET /oauth/mcp/callback?state={state}&code=code HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        response
    }

    #[test]
    fn configured_ports_bind_matching_redirect_and_isolate_relays() {
        use std::sync::atomic::AtomicUsize;
        // Reserve distinct ephemeral fixture ports; never occupy the user's defaults.
        let sockets: Vec<_> = (0..2)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<_> = sockets
            .iter()
            .map(|s| s.local_addr().unwrap().port())
            .collect();
        drop(sockets);
        let shells = [test_shell(ports[0]), test_shell(ports[1])];
        let calls = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        for (i, shell) in shells.iter().enumerate() {
            assert!(shell.listener.lock().unwrap().is_none());
            assert_eq!(
                shell.prepare().unwrap(),
                format!("http://127.0.0.1:{}{CALLBACK_PATH}", ports[i])
            );
            let count = calls.clone();
            assert_eq!(
                shell.authorize(
                    &test_event(&format!("flow{i}"), &format!("state{i}")),
                    Arc::new(move |_| {
                        count[i].fetch_add(1, Ordering::SeqCst);
                        true
                    }),
                    OAuthBrowserAdmission::default()
                ),
                OAuthBrowserEffectResult::BrowserUnavailable
            );
        }
        assert!(wire_callback(ports[0], "state1").starts_with("HTTP/1.1 400"));
        assert!(wire_callback(ports[1], "state0").starts_with("HTTP/1.1 400"));
        for i in 0..2 {
            assert!(wire_callback(ports[i], &format!("state{i}")).starts_with("HTTP/1.1 202"));
            assert_eq!(calls[i].load(Ordering::SeqCst), 1);
            assert!(wire_callback(ports[i], &format!("state{i}")).starts_with("HTTP/1.1 400"));
        }
        // Release rejects a late callback while another relay keeps the listener alive.
        let shell = &shells[0];
        for flow in ["released", "remaining"] {
            shell.authorize(
                &test_event(flow, flow),
                Arc::new(|_| panic!("late callback")),
                OAuthBrowserAdmission::default(),
            );
        }
        shell.release("released");
        assert!(wire_callback(ports[0], "released").starts_with("HTTP/1.1 400"));
        let retired_listener = shell.listener.lock().unwrap().as_ref().unwrap().clone();
        shell.shutdown();
        assert!(request(retired_listener, "state=remaining&code=late").starts_with("HTTP/1.1 400"));
        assert_eq!(
            shell.retry_authorize(&test_event("remaining", "remaining")),
            OAuthBrowserEffectResult::CallbackUnavailable
        );
    }

    #[test]
    fn closing_own_listener_rebinds_the_same_configured_port() {
        let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reserved.local_addr().unwrap().port();
        drop(reserved);
        let shell = test_shell(port);
        let redirect = shell.prepare().unwrap();
        let closing = shell.listener.lock().unwrap().as_ref().unwrap().clone();
        // The worker still owns the socket until it observes this signal.
        closing.stopped.store(true, Ordering::Release);
        assert_eq!(shell.prepare().unwrap(), redirect);
        let current = shell.listener.lock().unwrap().as_ref().unwrap().clone();
        assert!(!Arc::ptr_eq(&closing, &current));
        assert_eq!(current.callback_port, port);
        assert_eq!(
            shell.authorize(
                &test_event("flow", "state"),
                Arc::new(|_| true),
                OAuthBrowserAdmission::default()
            ),
            OAuthBrowserEffectResult::BrowserUnavailable
        );
        assert!(wire_callback(port, "state").starts_with("HTTP/1.1 202"));
    }

    #[test]
    fn occupied_configured_port_does_not_launch_and_can_retry_after_release() {
        let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = occupied.local_addr().unwrap().port();
        let mut shell = test_shell(port);
        shell.browser = Arc::new(|_| panic!("browser must not open without listener"));
        assert_eq!(
            shell.prepare().unwrap_err().to_string(),
            "oauth_callback_port_unavailable"
        );
        assert_eq!(
            shell.authorize(
                &test_event("flow", "state"),
                Arc::new(|_| true),
                OAuthBrowserAdmission::default()
            ),
            OAuthBrowserEffectResult::CallbackUnavailable
        );
        assert!(shell.listener.lock().unwrap().is_none());
        drop(occupied);
        shell.browser = Arc::new(|_| false);
        assert_eq!(
            shell.prepare().unwrap(),
            format!("http://127.0.0.1:{port}{CALLBACK_PATH}")
        );
        assert_eq!(
            shell.authorize(
                &test_event("flow", "state"),
                Arc::new(|_| true),
                OAuthBrowserAdmission::default()
            ),
            OAuthBrowserEffectResult::BrowserUnavailable
        );
        assert!(wire_callback(port, "state").starts_with("HTTP/1.1 202"));
    }

    #[test]
    fn invalid_configuration_never_falls_back_to_default_listener() {
        let shell = DesktopMcpOAuthShell::new(Err(OAuthPreparationError::ConfigurationLoad));
        assert!(shell.prepare().is_err());
        assert!(shell.listener.lock().unwrap().is_none());
    }

    #[test]
    fn configuration_causes_do_not_bind_or_launch_and_new_shell_can_recover() {
        for reason in [
            OAuthPreparationError::InvalidCallbackPort,
            OAuthPreparationError::ConfigurationLoad,
        ] {
            let mut shell = DesktopMcpOAuthShell::new(Err(reason));
            shell.browser = Arc::new(|_| panic!("configuration failure must not launch a browser"));
            assert_eq!(shell.configuration_error(), Some(reason));
            let error = shell.prepare().unwrap_err();
            assert_eq!(error.downcast_ref::<OAuthPreparationError>(), Some(&reason));
            assert_eq!(error.to_string(), reason.code());
            assert_eq!(
                shell.authorize(
                    &test_event("flow", "state"),
                    Arc::new(|_| panic!("no listener")),
                    OAuthBrowserAdmission::default()
                ),
                OAuthBrowserEffectResult::CallbackUnavailable
            );
            assert!(shell.listener.lock().unwrap().is_none());
        }
        let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reserved.local_addr().unwrap().port();
        drop(reserved);
        let repaired = test_shell(port);
        assert_eq!(repaired.configuration_error(), None);
        assert_eq!(
            repaired.prepare().unwrap(),
            format!("http://127.0.0.1:{port}{CALLBACK_PATH}")
        );
        assert_eq!(
            repaired.authorize(
                &test_event("new", "state"),
                Arc::new(|_| true),
                OAuthBrowserAdmission::default()
            ),
            OAuthBrowserEffectResult::BrowserUnavailable
        );
        assert!(wire_callback(port, "state").starts_with("HTTP/1.1 202"));
    }

    #[test]
    fn callback_parser_rejects_duplicate_state_and_preserves_issuer() {
        assert!(
            parse_fields(
                &url::Url::parse("http://127.0.0.1/callback?state=a&state=b&code=c").unwrap()
            )
            .is_none()
        );
        let fields = parse_fields(
            &url::Url::parse(
                "http://127.0.0.1/callback?state=a&code=c&iss=https%3A%2F%2Fissuer.test",
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(fields.issuer.as_deref(), Some("https://issuer.test"));
        assert_eq!(fields.code.unwrap().expose_secret(), "c");
    }
}
