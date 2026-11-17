//! Preview owns its Chromium child and a scratch profile; it never attaches to
//! agent-browser's daemon, sessions or cookies. Browser installation is shared.
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use process_wrap::tokio::{CommandWrap, KillOnDrop};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::OnceLock,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    sync::{Mutex, Semaphore},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

fn owned_process(command: Command) -> CommandWrap {
    // The same process ownership primitives as Pioneer CLI runtimes: a new
    // Unix group or Windows Job Object, never an existing skill browser/process.
    let mut wrapped = CommandWrap::from(command);
    wrapped.wrap(KillOnDrop);
    #[cfg(unix)]
    wrapped.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    wrapped.wrap(process_wrap::tokio::JobObject);
    wrapped
}

static INSTALL: Mutex<()> = Mutex::const_new(());
static CAPACITY: OnceLock<Semaphore> = OnceLock::new();
pub(super) const SCREENSHOT_BUDGET: usize = 3 * 1024 * 1024;
const MAX_CAPTURE_HEIGHT: u32 = 8192;
const MAX_DIAGNOSTICS: usize = 16_384;
const INITIAL_LOAD_BUDGET: Duration = Duration::from_secs(8);
// Installed before authored scripts: the load event includes initial images and
// external scripts. Future animations/requests are deliberately not awaited.
const INITIAL_LOAD: &str = "<script>Object.defineProperty(window,'__pioneerInitialLoad',{value:new Promise(resolve=>{const ready=event=>{if(!event.isTrusted)return;window.removeEventListener('load',ready);resolve()};window.addEventListener('load',ready)})});</script>";
// Capture starts at document y=0. Include the body's document offset while
// allowing the measured content to shrink after the loading viewport changes.
const DOCUMENT_EXTENT: &str = "(()=>{const b=document.body;if(!b)return 1;const r=b.getBoundingClientRect();let bottom=Math.max(1,r.bottom+scrollY,r.top+scrollY+b.scrollHeight);for(const e of b.children)bottom=Math.max(bottom,e.getBoundingClientRect().bottom+scrollY);return Math.ceil(bottom)})()";

pub(super) struct Capture {
    pub appearance: &'static str,
    pub width: u32,
    pub content_height: u64,
    pub captured_height: u32,
    pub diagnostics: Vec<String>,
    pub png: Vec<u8>,
}

async fn discover() -> Result<Option<PathBuf>> {
    // This is the current agent-browser installation convention. Do not invent
    // another cache or independently download Chromium/headless-shell.
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| anyhow!("browser home is unavailable"))?;
    let home = PathBuf::from(home);
    let mut configured = None;
    let explicit = std::env::var_os("AGENT_BROWSER_CONFIG").map(PathBuf::from);
    let configs = explicit.as_ref().map_or_else(
        || {
            vec![
                home.join(".agent-browser/config.json"),
                PathBuf::from("agent-browser.json"),
            ]
        },
        |path| vec![path.clone()],
    );
    for path in configs {
        match tokio::fs::metadata(&path).await {
            Ok(metadata) => {
                if metadata.len() > 65_536 {
                    bail!("skill browser configuration exceeds the read budget");
                }
                let value: Value = serde_json::from_slice(&tokio::fs::read(path).await?)
                    .context("invalid skill browser configuration")?;
                if let Some(path) = value["executablePath"].as_str() {
                    configured = Some(PathBuf::from(path));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && explicit.is_none() => {}
            Err(error) => return Err(error.into()),
        }
    }
    if let Some(path) = std::env::var_os("AGENT_BROWSER_EXECUTABLE_PATH") {
        configured = Some(PathBuf::from(path));
    }
    if let Some(path) = configured {
        if !tokio::fs::metadata(&path).await.is_ok_and(|m| m.is_file()) {
            bail!("configured skill browser executable is unavailable on this Gateway");
        }
        return Ok(Some(path));
    }
    // Match the installed agent-browser 0.27 discovery order. These are reads
    // of its existing locations; only `agent-browser install` provisions files.
    #[cfg(target_os = "macos")]
    let chrome_paths = [
        "Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        "chrome-mac-x64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    ];
    #[cfg(target_os = "windows")]
    let chrome_paths = ["chrome.exe", "chrome-win64/chrome.exe"];
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let chrome_paths = ["chrome", "chrome-linux64/chrome"];
    if let Some(path) = cached_browser(
        &home.join(".agent-browser/browsers"),
        "chrome-",
        &chrome_paths,
    )
    .await?
    {
        return Ok(Some(path));
    }
    #[cfg(target_os = "macos")]
    let system_paths = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    ]
    .map(PathBuf::from)
    .to_vec();
    #[cfg(target_os = "windows")]
    let system_paths = {
        let mut paths = Vec::new();
        if let Some(local) = std::env::var_os("LOCALAPPDATA") {
            paths.push(PathBuf::from(&local).join("Google/Chrome/Application/chrome.exe"));
            paths.push(
                PathBuf::from(local).join("BraveSoftware/Brave-Browser/Application/brave.exe"),
            );
        }
        paths.extend(
            [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            ]
            .map(PathBuf::from),
        );
        paths
    };
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let system_paths = {
        let paths = std::env::var_os("PATH").unwrap_or_default();
        [
            "google-chrome",
            "google-chrome-stable",
            "chromium-browser",
            "chromium",
            "brave-browser",
            "brave-browser-stable",
        ]
        .into_iter()
        .flat_map(|name| std::env::split_paths(&paths).map(move |root| root.join(name)))
        .collect::<Vec<_>>()
    };
    for path in system_paths {
        if tokio::fs::metadata(&path).await.is_ok_and(|m| m.is_file()) {
            return Ok(Some(path));
        }
    }
    #[cfg(target_os = "macos")]
    let puppeteer_paths = [
        "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        "chrome-mac-x64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    ];
    #[cfg(target_os = "windows")]
    let puppeteer_paths = ["chrome-win64/chrome.exe"];
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let puppeteer_paths = ["chrome-linux64/chrome"];
    let mut puppeteer = std::env::var_os("PUPPETEER_CACHE_DIR")
        .map(|root| vec![PathBuf::from(root).join("chrome")])
        .unwrap_or_default();
    puppeteer.push(home.join(".cache/puppeteer/chrome"));
    for root in puppeteer {
        if let Some(path) = cached_browser(&root, "", &puppeteer_paths).await? {
            return Ok(Some(path));
        }
    }
    #[cfg(target_os = "macos")]
    let playwright_paths = ["chrome-mac/Chromium.app/Contents/MacOS/Chromium"];
    #[cfg(target_os = "windows")]
    let playwright_paths = ["chrome-win/chrome.exe"];
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let playwright_paths = ["chrome-linux64/chrome"];
    let mut playwright = std::env::var_os("PLAYWRIGHT_BROWSERS_PATH")
        .map(|root| vec![PathBuf::from(root)])
        .unwrap_or_default();
    playwright.push(home.join(".cache/ms-playwright"));
    for root in playwright {
        if let Some(path) = cached_browser(&root, "chromium-", &playwright_paths).await? {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

async fn cached_browser(root: &Path, prefix: &str, relatives: &[&str]) -> Result<Option<PathBuf>> {
    let mut entries = match tokio::fs::read_dir(root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut versions = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_name().to_string_lossy().starts_with(prefix)
            && entry.file_type().await?.is_dir()
        {
            versions.push(entry.path());
        }
    }
    versions.sort();
    for version in versions.into_iter().rev() {
        for relative in relatives {
            let path = version.join(relative);
            if tokio::fs::metadata(&path).await.is_ok_and(|m| m.is_file()) {
                return Ok(Some(path));
            }
        }
    }
    Ok(None)
}

async fn ensure_browser(cancel: &CancellationToken) -> Result<PathBuf> {
    if let Some(path) = discover().await? {
        return Ok(path);
    }
    let _install = tokio::select! {
        _ = cancel.cancelled() => bail!("preview cancelled while waiting for browser installation"),
        lock = INSTALL.lock() => lock,
    };
    if let Some(path) = discover().await? {
        return Ok(path);
    }
    let mut installer = Command::new("agent-browser");
    installer
        .arg("install")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = owned_process(installer)
        .spawn()
        .context("browser missing; install the pioneer/browser skill's agent-browser CLI")?;
    let mut observation = pioneer_observability::resources::ChildRegistration::register(
        child.id(),
        pioneer_observability::resources::ChildRole::Browser,
        None,
    );
    let status = tokio::select! {
        _ = cancel.cancelled() => None,
        _ = tokio::time::sleep(Duration::from_secs(100)) => None,
        status = child.wait() => Some(status?),
    };
    let Some(status) = status else {
        let _ = Box::into_pin(child.kill()).await;
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .context("browser installer cleanup deadline exceeded")?;
        observe_browser_wait(&mut observation, status)?;
        bail!("preview cancelled or browser installation deadline exceeded");
    };
    observation.exited(status.success());
    if !status.success() {
        bail!(
            "agent-browser install failed; repair pioneer/browser browser dependencies on this Gateway"
        );
    }
    discover()
        .await?
        .ok_or_else(|| anyhow!("agent-browser install did not produce a supported browser"))
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
struct Cdp {
    socket: Socket,
    next_id: u64,
    diagnostics: Vec<String>,
    diagnostic_bytes: usize,
}
impl Cdp {
    async fn call(&mut self, method: &str, params: Value, session: Option<&str>) -> Result<Value> {
        self.next_id += 1;
        let id = self.next_id;
        let mut request = json!({"id":id,"method":method,"params":params});
        if let Some(session) = session {
            request["sessionId"] = session.into();
        }
        self.socket
            .send(Message::Text(request.to_string().into()))
            .await?;
        loop {
            let message = self
                .socket
                .next()
                .await
                .ok_or_else(|| anyhow!("preview browser disconnected"))??;
            let Message::Text(text) = message else {
                continue;
            };
            let response: Value = serde_json::from_str(&text)?;
            if response["id"] == id {
                if response.get("error").is_some() {
                    bail!("browser rejected {method}");
                }
                return Ok(response["result"].clone());
            }
            let diagnostic = matches!(
                response["method"].as_str(),
                Some(
                    "Runtime.consoleAPICalled"
                        | "Runtime.exceptionThrown"
                        | "Network.loadingFailed"
                )
            ) || (response["method"] == "Network.responseReceived"
                && response["params"]["response"]["status"]
                    .as_f64()
                    .is_some_and(|status| status >= 400.));
            if response["sessionId"].as_str() == session
                && diagnostic
                && self.diagnostic_bytes < MAX_DIAGNOSTICS
            {
                let text = response["params"].to_string();
                let remaining = MAX_DIAGNOSTICS - self.diagnostic_bytes;
                let mut end = remaining.min(text.len());
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                let bounded = text[..end].to_owned();
                self.diagnostic_bytes += bounded.len();
                self.diagnostics.push(bounded);
            }
        }
    }
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) async fn capture(
    html: &str,
    scratch_root: &Path,
    cancel: &CancellationToken,
) -> Result<Vec<Capture>> {
    // An owned task completes child cleanup even if the caller's tool future is
    // dropped by the execution governor. Dropping this guard requests cancellation.
    let local = cancel.child_token();
    let _guard = CancelOnDrop(local.clone());
    let html = html.to_owned();
    let scratch = scratch_root.to_owned();
    tokio::spawn(async move { capture_owned(&html, &scratch, &local).await }).await?
}

async fn capture_owned(
    html: &str,
    scratch_root: &Path,
    cancel: &CancellationToken,
) -> Result<Vec<Capture>> {
    // try_acquire bounds admission without growing another queue behind tool scheduling.
    let _permit = CAPACITY
        .get_or_init(|| Semaphore::new(1))
        .try_acquire()
        .map_err(|_| anyhow!("html preview is busy; retry after the current capture"))?;
    if cancel.is_cancelled() {
        bail!("preview cancelled before browser startup");
    }
    // The installer owns cancellation and reaping. Do not race it with an
    // outer select that could drop its cleanup branch before child.wait().
    let browser = ensure_browser(cancel).await?;
    if cancel.is_cancelled() {
        bail!("preview cancelled before browser startup");
    }
    tokio::fs::create_dir_all(scratch_root).await?;
    let profile = tempfile::Builder::new()
        .prefix("html-preview-")
        .tempdir_in(scratch_root)?;
    let mut command = Command::new(browser);
    pioneer_cli_agent_runtime::process::scrub_inherited_cli_environment(&mut command);
    command
        .args([
            "--headless=new",
            "--remote-debugging-port=0",
            "--remote-debugging-address=127.0.0.1",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-background-networking",
        ])
        .arg(format!("--user-data-dir={}", profile.path().display()))
        .arg("about:blank")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = owned_process(command)
        .spawn()
        .context("could not start skill-installed browser on execution Gateway")?;
    let mut observation = pioneer_observability::resources::ChildRegistration::register(
        child.id(),
        pioneer_observability::resources::ChildRole::Browser,
        None,
    );
    let stderr = child
        .stderr()
        .take()
        .context("browser stderr unavailable")?;
    let operation = async {
        let mut reader = BufReader::new(stderr);
        let mut endpoint = None;
        // Bound startup log bytes as well as lines, then retain the original pipe.
        {
            let mut lines = (&mut reader).take(64 * 1024).lines();
            for _ in 0..128 {
                let line = lines.next_line().await?.ok_or_else(|| anyhow!("browser stopped before CDP became available or exceeded the startup log budget"))?;
                if let Some(url) = line.strip_prefix("DevTools listening on ") {
                    endpoint = Some(url.to_owned());
                    break;
                }
            }
        }
        let endpoint = endpoint.context("browser did not expose a CDP endpoint")?;
        if !endpoint.starts_with("ws://127.0.0.1:") {
            bail!("browser exposed a non-local CDP endpoint");
        }
        // Drain stderr after endpoint discovery so Chromium cannot block on its pipe.
        let drain = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut reader, &mut tokio::io::sink()).await;
        });
        struct AbortOnDrop(tokio::task::JoinHandle<()>);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _drain = AbortOnDrop(drain);
        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(SCREENSHOT_BUDGET * 4 / 3 + 65_536);
        config.max_frame_size = config.max_message_size;
        let (socket, _) = connect_async_with_config(endpoint, Some(config), false).await?;
        let mut cdp = Cdp {
            socket,
            next_id: 0,
            diagnostics: Vec::new(),
            diagnostic_bytes: 0,
        };
        let mut captures = Vec::with_capacity(6);
        let mut bytes_left = SCREENSHOT_BUDGET;
        for (appearance, width) in pioneer_tools::html::PREVIEW_FRAMES {
            let dark = appearance == "dark";
            let document = super::prepared_document(&format!("{INITIAL_LOAD}{html}"), dark);
            cdp.diagnostics.clear();
            cdp.diagnostic_bytes = 0;
            let context = cdp
                .call("Target.createBrowserContext", json!({}), None)
                .await?;
            let context_id = context["browserContextId"]
                .as_str()
                .context("context id missing")?;
            let target = cdp
                .call(
                    "Target.createTarget",
                    json!({"url":"about:blank","browserContextId":context_id}),
                    None,
                )
                .await?;
            let target_id = target["targetId"].as_str().context("target id missing")?;
            let session = cdp
                .call(
                    "Target.attachToTarget",
                    json!({"targetId":target_id,"flatten":true}),
                    None,
                )
                .await?;
            let session = session["sessionId"]
                .as_str()
                .context("session id missing")?;
            cdp.call("Page.enable", json!({}), Some(session)).await?;
            cdp.call("Runtime.enable", json!({}), Some(session)).await?;
            cdp.call("Network.enable", json!({}), Some(session)).await?;
            cdp.call(
                "Emulation.setDeviceMetricsOverride",
                json!({"width":width,"height":1,"deviceScaleFactor":1,"mobile":false}),
                Some(session),
            )
            .await?;
            cdp.call("Emulation.setEmulatedMedia", json!({"features":[{"name":"prefers-color-scheme","value":if dark {"dark"} else {"light"}}]}), Some(session)).await?;
            let tree = cdp
                .call("Page.getFrameTree", json!({}), Some(session))
                .await?;
            let frame = tree["frameTree"]["frame"]["id"]
                .as_str()
                .context("frame id missing")?;

            let result = tokio::time::timeout(INITIAL_LOAD_BUDGET, async {
                cdp.call(
                    "Page.setDocumentContent",
                    json!({"frameId":frame,"html":document}),
                    Some(session),
                ).await?;
                let expression = format!("(async()=>{{await window.__pioneerInitialLoad;await document.fonts.ready;await new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)));return {DOCUMENT_EXTENT}}})()");
                cdp.call("Runtime.evaluate", json!({"expression":expression,"awaitPromise":true,"returnByValue":true}), Some(session)).await
            }).await.with_context(|| format!("initial resource/font load timed out at {width} CSS px"))??;
            let measured_height = result["result"]["value"]
                .as_u64()
                .context("document height measurement failed")?;
            // Let a naturally sized page settle at its full height before capture;
            // a one-pixel loading viewport must not dictate its final browser layout.
            let viewport_height = measured_height.clamp(1, u64::from(MAX_CAPTURE_HEIGHT));
            cdp.call("Emulation.setDeviceMetricsOverride",json!({"width":width,"height":viewport_height,"deviceScaleFactor":1,"mobile":false}),Some(session)).await?;
            let expression = format!(
                "(async()=>{{await new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)));return {DOCUMENT_EXTENT}}})()"
            );
            let result = cdp
                .call(
                    "Runtime.evaluate",
                    json!({"expression":expression,"awaitPromise":true,"returnByValue":true}),
                    Some(session),
                )
                .await?;
            let content_height = result["result"]["value"]
                .as_u64()
                .context("settled document height measurement failed")?;
            let height = content_height.clamp(1, u64::from(MAX_CAPTURE_HEIGHT)) as u32;
            let screenshot = cdp.call("Page.captureScreenshot", json!({"format":"png","captureBeyondViewport":true,"clip":{"x":0,"y":0,"width":width,"height":height,"scale":1}}), Some(session)).await?;
            let encoded = screenshot["data"].as_str().context("screenshot missing")?;
            if encoded.len() > bytes_left.saturating_mul(4) / 3 + 4 {
                bail!("six preview images exceed the shared transport budget");
            }
            let png = base64::engine::general_purpose::STANDARD.decode(encoded)?;
            bytes_left = bytes_left
                .checked_sub(png.len())
                .context("six preview images exceed the shared transport budget")?;
            if cdp.diagnostic_bytes >= MAX_DIAGNOSTICS {
                cdp.diagnostics
                    .push("Console/page diagnostics truncated at the per-frame budget".into());
            }
            if content_height > u64::from(height) {
                cdp.diagnostics.push(format!("Screenshot truncated at {height}px; document is {content_height}px. Publication height is unaffected."));
            }
            captures.push(Capture {
                appearance,
                width,
                content_height,
                captured_height: height,
                diagnostics: std::mem::take(&mut cdp.diagnostics),
                png,
            });
            cdp.call(
                "Target.disposeBrowserContext",
                json!({"browserContextId":context_id}),
                None,
            )
            .await?;
        }
        Ok(captures)
    };
    let result = tokio::select! {
        _ = cancel.cancelled() => Err(anyhow!("preview cancelled")),
        result = tokio::time::timeout(Duration::from_secs(90), operation) => result.unwrap_or_else(|_| Err(anyhow!("preview capture deadline exceeded"))),
    };
    // Kill only the process we created, then remove its profile after exit.
    Box::into_pin(child.kill())
        .await
        .context("preview process cleanup failed")?;
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .context("preview process cleanup deadline exceeded")?;
    observe_browser_wait(&mut observation, status)?;
    profile.close().context("preview profile cleanup failed")?;
    result
}

// The process outcome is already known here, before preview/profile errors.
fn observe_browser_wait(
    observation: &mut pioneer_observability::resources::ChildRegistration,
    waited: std::io::Result<std::process::ExitStatus>,
) -> std::io::Result<std::process::ExitStatus> {
    waited.inspect(|status| observation.exited(status.success()))
}

#[cfg(test)]
mod tests {
    use super::*;
    static BROWSER_FIXTURE: Mutex<()> = Mutex::const_new(());

    /// Initial resource readiness, per-preset load errors and cancellation use
    /// an isolated local fixture server. Written only; browser never launched.
    #[tokio::test]
    async fn html_initial_resource_load_and_errors_are_bounded_and_width_scoped() {
        let _fixture = BROWSER_FIXTURE.lock().await;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let blocked = std::sync::Arc::new(tokio::sync::Notify::new());
        let observed = blocked.clone();
        let server = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let observed = observed.clone();
                tasks.spawn(async move {
                    let mut request = [0u8; 8192];
                    let size = socket.read(&mut request).await.unwrap();
                    let request = String::from_utf8_lossy(&request[..size]);
                    if request.contains("/blocked") {
                        observed.notify_one();
                        std::future::pending::<()>().await;
                        return;
                    }
                    // A resource delay in the fixture, not a production sleep.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let (status, mime, body) = if request.contains("/slow.js") {
                        ("200 OK","application/javascript","console.log('initial-script',innerWidth)")
                    } else if request.contains("/slow.svg") {
                        ("200 OK","image/svg+xml","<svg xmlns='http://www.w3.org/2000/svg' width='1' height='1'/>")
                    } else { ("404 Not Found","text/plain","missing fixture") };
                    let response = format!("HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                    socket.write_all(response.as_bytes()).await.unwrap();
                });
            }
        });
        struct AbortServer(tokio::task::JoinHandle<()>);
        impl Drop for AbortServer {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _server = AbortServer(server);
        let scratch = tempfile::tempdir().unwrap();
        let html = format!(
            r#"<style>body{{position:relative;top:100px}}#content{{height:10px}}img{{display:none}}</style><div id='content'>Page</div><img src='http://{address}/slow.svg' onload="document.getElementById('content').style.height='260px';console.log('initial-image',innerWidth)"><img src='http://{address}/missing.svg' onerror="console.error('initial-error',innerWidth)"><script src='http://{address}/slow.js'></script>"#
        );
        let captures = capture(&html, scratch.path(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(captures.len(), 6);
        for (capture, (appearance, width)) in captures.iter().zip([
            ("light", 360),
            ("light", 768),
            ("light", 1024),
            ("dark", 360),
            ("dark", 768),
            ("dark", 1024),
        ]) {
            assert_eq!(capture.appearance, appearance);
            assert_eq!(capture.content_height, 360);
            assert_eq!(
                u32::from_be_bytes(capture.png[20..24].try_into().unwrap()),
                360
            );
            for marker in ["initial-image", "initial-script", "initial-error"] {
                assert!(
                    capture
                        .diagnostics
                        .iter()
                        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                        .any(|event| event["args"][0]["value"] == marker
                            && event["args"][1]["value"] == width)
                );
            }
            assert!(capture.diagnostics.iter().any(|line| line.contains("404")));
        }
        let blocked_html = format!("<img src='http://{address}/blocked'>");
        let cancel = CancellationToken::new();
        let child_cancel = cancel.clone();
        let root = scratch.path().to_owned();
        let pending =
            tokio::spawn(async move { capture(&blocked_html, &root, &child_cancel).await });
        tokio::time::timeout(Duration::from_secs(10), blocked.notified())
            .await
            .unwrap();
        cancel.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), pending)
                .await
                .unwrap()
                .unwrap()
                .err()
                .expect("cancelled load")
                .to_string()
                .contains("cancel")
        );
        let timeout_html = format!("<img src='http://{address}/blocked'>");
        let error = capture(&timeout_html, scratch.path(), &CancellationToken::new())
            .await
            .err()
            .expect("initial load must time out");
        assert!(
            error
                .to_string()
                .contains("initial resource/font load timed out")
        );
    }

    /// Requires the skill browser on the isolated test Gateway. This test is
    /// written for the independent review gate; it has never been executed.
    #[tokio::test]
    async fn html_capture_presets_use_fresh_contexts_and_cancelled_process_profiles_are_reaped() {
        let _fixture = BROWSER_FIXTURE.lock().await;
        let scratch = tempfile::tempdir().unwrap();
        let html = r#"<!doctype html><style>body{margin:0;position:relative;top:100px}div{height:260px}</style><script>window.fixtureCount=(window.fixtureCount||0)+1;console.log('html-capture-fixture',window.fixtureCount,window.innerWidth,getComputedStyle(document.documentElement).colorScheme)</script><div>Page</div>"#;
        let captures = capture(html, scratch.path(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(captures.len(), 6);
        for (capture, (appearance, width)) in captures.iter().zip([
            ("light", 360),
            ("light", 768),
            ("light", 1024),
            ("dark", 360),
            ("dark", 768),
            ("dark", 1024),
        ]) {
            assert_eq!(capture.appearance, appearance);
            assert_eq!(capture.width, width);
            assert_eq!(&capture.png[..8], b"\x89PNG\r\n\x1a\n");
            assert_eq!(
                u32::from_be_bytes(capture.png[16..20].try_into().unwrap()),
                width
            );
            assert_eq!(
                u32::from_be_bytes(capture.png[20..24].try_into().unwrap()),
                360
            );
            assert!(
                capture
                    .diagnostics
                    .iter()
                    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                    .any(|event| event["args"][0]["value"] == "html-capture-fixture"
                        && event["args"][1]["value"] == 1
                        && event["args"][2]["value"] == width
                        && event["args"][3]["value"] == appearance)
            );
        }
        let cancel = CancellationToken::new();
        let cancellation = cancel.clone();
        let root = scratch.path().to_owned();
        let pending = tokio::spawn(async move {
            capture("<script>while(true){}</script>", &root, &cancellation).await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        let error = tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .unwrap()
            .unwrap()
            .err()
            .expect("capture must be cancelled");
        assert!(error.to_string().contains("cancel"));
        assert!(
            tokio::fs::read_dir(scratch.path())
                .await
                .unwrap()
                .next_entry()
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[cfg(all(test, any(unix, windows)))]
mod exit_observation_tests {
    use super::*;
    #[test]
    fn cleanup_wait_records_exit_before_preview_or_profile_error() {
        let old = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(true);
        for (index, success) in [true, false].into_iter().enumerate() {
            #[cfg(unix)]
            let status = {
                use std::os::unix::process::ExitStatusExt;
                std::process::ExitStatus::from_raw(if success { 0 } else { 7 << 8 })
            };
            #[cfg(windows)]
            let status = {
                use std::os::windows::process::ExitStatusExt;
                std::process::ExitStatus::from_raw(if success { 0 } else { 7 })
            };
            let id = 742000 + index as u64;
            let mut observation = pioneer_observability::resources::ChildRegistration::register(
                Some(std::process::id()),
                pioneer_observability::resources::ChildRole::Browser,
                Some(id),
            );
            let observed = observe_browser_wait(&mut observation, Ok(status)).unwrap();
            assert_eq!(observed, status);
            let _later_preview_or_profile_result: Result<()> =
                Err(anyhow!("fixture preview/profile cleanup failed"));
            drop(observation);
            let snapshot = pioneer_observability::resources::resource_snapshot().unwrap();
            let child = snapshot
                .children
                .iter()
                .find(|c| c.owner_generation == Some(id))
                .unwrap();
            assert_eq!(
                child.outcome,
                if success {
                    "exited_success"
                } else {
                    "exited_failure"
                }
            );
            assert!(
                child.exited_monotonic_ms.is_some() && child.ownership_ended_monotonic_ms.is_some()
            );
            assert_eq!(child.final_usage, "incomplete_no_wait_rusage");
            let token = child.work.unwrap().token;
            let live = pioneer_observability::live::snapshot().unwrap();
            assert!(!live.active.iter().any(|w| w.context.token == token));
            assert_eq!(live.journal.iter().filter(|r| matches!(&r.payload, pioneer_observability::live::JournalPayload::Work(w) if w.context.token == token && matches!(w.state, pioneer_observability::live::WorkState::Terminal(_)))).count(), 1);
        }
        let mut unknown = pioneer_observability::resources::ChildRegistration::register(
            Some(std::process::id()),
            pioneer_observability::resources::ChildRole::Browser,
            Some(742002),
        );
        assert!(
            observe_browser_wait(&mut unknown, Err(std::io::Error::other("wait failed"))).is_err()
        );
        drop(unknown);
        let snapshot = pioneer_observability::resources::resource_snapshot().unwrap();
        let child = snapshot
            .children
            .iter()
            .find(|c| c.owner_generation == Some(742002))
            .unwrap();
        assert_eq!(child.outcome, "owner_dropped_exit_unknown");
        assert!(child.exited_monotonic_ms.is_none());
        pioneer_observability::set_telemetry_enabled(old);
    }
}
