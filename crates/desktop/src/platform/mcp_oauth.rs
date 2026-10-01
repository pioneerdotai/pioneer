//! Native loopback callback on the browser device, including remote Gateways.
use pioneer_client::mcp::oauth::{
    McpOAuthShell, OAuthBrowserAdmission, OAuthBrowserEffectResult, OAuthCallbackFields,
    OAuthCallbackRelay, OAuthPresentation,
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
const REDIRECT: &str = "http://127.0.0.1:37643/oauth/mcp/callback";
struct Pending {
    flow: String,
    relay: OAuthCallbackRelay,
    admission: OAuthBrowserAdmission,
    deadline: SystemTime,
}
struct Listener {
    pending: Mutex<HashMap<String, Pending>>,
    stopped: AtomicBool,
    prepared: Mutex<SystemTime>,
}
pub(crate) struct DesktopMcpOAuthShell {
    listener: Mutex<Option<Arc<Listener>>>,
    browser: Arc<dyn Fn(&str) -> bool + Send + Sync>,
}
impl Default for DesktopMcpOAuthShell {
    fn default() -> Self {
        Self {
            listener: Mutex::new(None),
            browser: Arc::new(open_browser),
        }
    }
}
impl McpOAuthShell for DesktopMcpOAuthShell {
    fn prepare(&self) -> anyhow::Result<String> {
        let mut owner = self.listener.lock().expect("OAuth listener owner poisoned");
        if let Some(listener) = owner
            .as_ref()
            .filter(|l| !l.stopped.load(Ordering::Acquire))
        {
            *listener.prepared.lock().unwrap() = SystemTime::now();
            return Ok(REDIRECT.into());
        }
        // A stopped worker may still be finishing its last callback response.
        // Retry only our own closing listener, never change the registered URI.
        let closing_ours = owner
            .as_ref()
            .is_some_and(|l| l.stopped.load(Ordering::Acquire));
        let mut socket = TcpListener::bind("127.0.0.1:37643");
        if closing_ours {
            for _ in 0..10 {
                if socket.is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
                socket = TcpListener::bind("127.0.0.1:37643");
            }
        }
        let socket = socket.map_err(|_| anyhow::anyhow!("oauth_callback_port_unavailable"))?;
        socket.set_nonblocking(true)?;
        let listener = Arc::new(Listener {
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
        Ok(REDIRECT.into())
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
        let url = url::Url::parse(&format!("http://127.0.0.1:37643{target}")).ok()?;
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
            pending: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
            prepared: Mutex::new(SystemTime::now()),
        });
        let launches = Arc::new(AtomicUsize::new(0));
        let count = launches.clone();
        let ready = listener.clone();
        let shell = DesktopMcpOAuthShell {
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
