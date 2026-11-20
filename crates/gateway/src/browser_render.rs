//! Shared browser process and CDP session ownership.
use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use process_wrap::tokio::{CommandWrap, KillOnDrop};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    sync::Mutex,
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
const MAX_DIAGNOSTICS: usize = 16_384;
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
pub(crate) struct Cdp {
    socket: Socket,
    next_id: u64,
    pub(crate) diagnostics: Vec<String>,
    pub(crate) diagnostic_bytes: usize,
    pub(crate) assets: Option<AssetSource>,
}
impl Cdp {
    pub(crate) fn diagnostics_limit_reached(&self) -> bool {
        self.diagnostic_bytes >= MAX_DIAGNOSTICS
    }

    pub(crate) async fn capture_png(&mut self, session: &str, clip: Value) -> Result<Value> {
        self.call(
            "Page.captureScreenshot",
            json!({"format":"png","captureBeyondViewport":true,"clip":clip}),
            Some(session),
        )
        .await
    }

    pub(crate) async fn print_pdf(&mut self, session: &str, options: Value) -> Result<Value> {
        self.call("Page.printToPDF", options, Some(session)).await
    }

    pub(crate) async fn call(
        &mut self,
        method: &str,
        params: Value,
        session: Option<&str>,
    ) -> Result<Value> {
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
            if response["method"] == "Fetch.requestPaused" {
                let params = &response["params"];
                let Some(session) = response["sessionId"].as_str() else {
                    bail!("intercepted request missing session");
                };
                let Some(request_id) = params["requestId"].as_str() else {
                    bail!("intercepted request missing ID");
                };
                let (method, params) = match self.assets.as_mut() {
                    Some(source) => source.reply(request_id, params).await,
                    None => (
                        "Fetch.failRequest",
                        json!({"requestId":request_id,"errorReason":"BlockedByClient"}),
                    ),
                };
                self.next_id += 1;
                self.socket.send(Message::Text(json!({"id":self.next_id,"method":method,"params":params,"sessionId":session}).to_string().into())).await?;
                continue;
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

pub(crate) async fn run<T, F, G>(
    scratch_root: &Path,
    cancel: &CancellationToken,
    profile_prefix: &'static str,
    message_budget: usize,
    deadline: Duration,
    browser_args: &'static [&'static str],
    guard: G,
    operation: F,
) -> Result<T>
where
    T: Send + 'static,
    G: Send + 'static,
    F: for<'a> FnOnce(&'a mut Cdp) -> futures_util::future::BoxFuture<'a, Result<T>>
        + Send
        + 'static,
{
    let local = cancel.child_token();
    let _guard = CancelOnDrop(local.clone());
    let scratch_root = scratch_root.to_owned();
    tokio::spawn(async move {
        let _guard = guard;
        run_owned(
            &scratch_root,
            &local,
            profile_prefix,
            message_budget,
            deadline,
            browser_args,
            operation,
        )
        .await
    })
    .await?
}

async fn run_owned<T, F>(
    scratch_root: &Path,
    cancel: &CancellationToken,
    profile_prefix: &'static str,
    message_budget: usize,
    deadline: Duration,
    browser_args: &'static [&'static str],
    operation: F,
) -> Result<T>
where
    F: for<'a> FnOnce(&'a mut Cdp) -> futures_util::future::BoxFuture<'a, Result<T>>,
{
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
        .prefix(profile_prefix)
        .tempdir_in(scratch_root)?;
    let mut command = Command::new(browser);
    pioneer_cli_agent_runtime::process::scrub_inherited_cli_environment(&mut command);
    command.args(browser_args);
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
        config.max_message_size = Some(message_budget);
        config.max_frame_size = config.max_message_size;
        let (socket, _) = connect_async_with_config(endpoint, Some(config), false).await?;
        let mut cdp = Cdp {
            socket,
            next_id: 0,
            diagnostics: Vec::new(),
            diagnostic_bytes: 0,
            assets: None,
        };
        operation(&mut cdp).await
    };
    let result = tokio::select! {
        _ = cancel.cancelled() => Err(anyhow!("preview cancelled")),
        result = tokio::time::timeout(deadline, operation) => result.unwrap_or_else(|_| Err(anyhow!("preview capture deadline exceeded"))),
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
pub(crate) fn observe_browser_wait(
    observation: &mut pioneer_observability::resources::ChildRegistration,
    waited: std::io::Result<std::process::ExitStatus>,
) -> std::io::Result<std::process::ExitStatus> {
    waited.inspect(|status| observation.exited(status.success()))
}

mod assets;
pub(crate) use assets::{AssetSource, RENDER_ORIGIN};
