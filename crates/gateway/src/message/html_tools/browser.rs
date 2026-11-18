//! HTML preview presets and capture policy for the agent tool.
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use serde_json::json;
use std::{path::Path, sync::OnceLock, time::Duration};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

static CAPACITY: OnceLock<Semaphore> = OnceLock::new();
pub(super) const SCREENSHOT_BUDGET: usize = 3 * 1024 * 1024;
const MAX_CAPTURE_HEIGHT: u32 = 8192;
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

pub(super) async fn capture(
    html: &str,
    scratch_root: &Path,
    cancel: &CancellationToken,
) -> Result<Vec<Capture>> {
    let permit = CAPACITY
        .get_or_init(|| Semaphore::new(1))
        .try_acquire()
        .map_err(|_| anyhow!("html preview is busy; retry after the current capture"))?;
    let html = html.to_owned();
    crate::browser_render::run(
        scratch_root,
        cancel,
        "html-preview-",
        SCREENSHOT_BUDGET * 4 / 3 + 65_536,
        Duration::from_secs(90),
        &[],
        permit,
        move |cdp| Box::pin(async move {
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
            let screenshot = cdp.capture_png(session, json!({"x":0,"y":0,"width":width,"height":height,"scale":1})).await?;
            let encoded = screenshot["data"].as_str().context("screenshot missing")?;
            if encoded.len() > bytes_left.saturating_mul(4) / 3 + 4 {
                bail!("six preview images exceed the shared transport budget");
            }
            let png = base64::engine::general_purpose::STANDARD.decode(encoded)?;
            bytes_left = bytes_left
                .checked_sub(png.len())
                .context("six preview images exceed the shared transport budget")?;
            if cdp.diagnostics_limit_reached() {
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
        }),
    ).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser_render::observe_browser_wait;
    use tokio::sync::Mutex;
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
