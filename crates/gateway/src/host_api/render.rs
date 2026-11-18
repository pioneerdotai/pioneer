//! App render jobs use backend execution authority and a bounded binary HTTP reply.
use super::*;
use crate::browser_render::{self, AssetSource, Cdp, RENDER_ORIGIN};
use anyhow::{Context, Result, ensure};
use axum::body::{Body, to_bytes};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use serde::Deserialize;

const INPUT_LIMIT: usize = 64 * 1024 * 1024;
const OUTPUT_LIMIT: usize = 32 * 1024 * 1024;
const BROWSER_ARGS: &[&str] = &[
    "--proxy-server=http://127.0.0.1:9",
    "--proxy-bypass-list=<-loopback>",
    "--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1",
    "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
];
static APP_RENDER_CAPACITY: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Job {
    Screenshot { path: String, input: Value },
    Pdf { html: String },
}

struct Output {
    mime: &'static str,
    bytes: Vec<u8>,
    metadata: Option<String>,
}

pub(crate) async fn request(
    State(state): State<GatewayHttpState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if !state.is_ready() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if headers.contains_key("origin") {
        return StatusCode::FORBIDDEN.into_response();
    }
    let credential = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let processor = &state.message_processor;
    let Some(scope) = credential.and_then(|c| processor.host_api().admit(c)) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if processor.validate_host_execution(&scope).await.is_err() {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Ok(permit) = APP_RENDER_CAPACITY.try_acquire() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let bytes = tokio::select! {
        _ = scope.cancel.cancelled() => return StatusCode::FORBIDDEN.into_response(),
        result = tokio::time::timeout(Duration::from_secs(30), to_bytes(body, INPUT_LIMIT)) => match result {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
            Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
        },
    };
    let Ok(job) = serde_json::from_slice::<Job>(&bytes) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    drop(bytes);
    let source = if let Job::Screenshot { path, .. } = &job {
        if !valid_path(path) {
            return StatusCode::BAD_REQUEST.into_response();
        }
        match processor.app_render_source(&scope, path.clone()).await {
            Ok(source) => Some(source),
            Err(_) => return StatusCode::FORBIDDEN.into_response(),
        }
    } else {
        None
    };
    let rendered = browser_render::run(
        &processor.app_render_scratch(),
        &scope.cancel,
        "app-render-",
        OUTPUT_LIMIT * 4 / 3 + 65_536,
        Duration::from_secs(90),
        BROWSER_ARGS,
        permit,
        move |cdp| Box::pin(render(cdp, job, source)),
    )
    .await;
    if scope.cancel.is_cancelled() {
        return StatusCode::FORBIDDEN.into_response();
    }
    match rendered {
        Ok(result) => {
            let mut response = axum::http::Response::builder()
                .header("content-type", result.mime)
                .header("cache-control", "no-store");
            if let Some(metadata) = result.metadata {
                response = response.header("pioneer-render-metadata", metadata);
            }
            response
                .body(Body::from(result.bytes))
                .expect("validated render response")
        }
        Err(error) => {
            tracing::debug!(%error, "App browser render failed");
            (StatusCode::BAD_GATEWAY, "host.render_failed").into_response()
        }
    }
}

fn valid_path(path: &str) -> bool {
    path.len() <= 256
        && path.strip_prefix('/').is_some_and(|path| {
            path.split('/').all(|p| {
                !p.is_empty()
                    && p != "."
                    && p != ".."
                    && p.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            })
        })
        && !path.starts_with("/api/")
        && path != "/api"
        && !path.starts_with("/assets/")
}

async fn render(cdp: &mut Cdp, job: Job, source: Option<AssetSource>) -> Result<Output> {
    cdp.assets = source;
    let context = cdp
        .call("Target.createBrowserContext", json!({}), None)
        .await?;
    let target = cdp
        .call(
            "Target.createTarget",
            json!({"url":"about:blank",
        "browserContextId":context["browserContextId"]}),
            None,
        )
        .await?;
    let attached = cdp
        .call(
            "Target.attachToTarget",
            json!({"targetId":target["targetId"],"flatten":true}),
            None,
        )
        .await?;
    let session = attached["sessionId"]
        .as_str()
        .context("render session missing")?;
    cdp.call("Page.enable", json!({}), Some(session)).await?;
    cdp.call("Runtime.enable", json!({}), Some(session)).await?;
    cdp.call(
        "Fetch.enable",
        json!({"patterns":[{"urlPattern":"*"}]}),
        Some(session),
    )
    .await?;
    match job {
        Job::Screenshot { path, input } => {
            viewport(cdp, session, 1400, 1600).await?;
            let nav = cdp
                .call(
                    "Page.navigate",
                    json!({"url":format!("{RENDER_ORIGIN}{path}")}),
                    Some(session),
                )
                .await?;
            ensure!(nav["errorText"].is_null(), "app render page failed to load");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                if evaluate(cdp, session, "Boolean(window.__pioneerRender)")
                    .await
                    .is_ok_and(|ready| ready.as_bool() == Some(true))
                {
                    break;
                }
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    "app render page not ready"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            let size = evaluate(
                cdp,
                session,
                &format!(
                    "window.__pioneerRender.prepare(JSON.parse({}))",
                    serde_json::to_string(&serde_json::to_string(&input)?)?
                ),
            )
            .await?;
            viewport(
                cdp,
                session,
                dimension(&size["width"], 8352)?,
                dimension(&size["height"], 16240)?,
            )
            .await?;
            let output = evaluate(cdp, session, "window.__pioneerRender.capture()").await?;
            let clip = &output["clip"];
            let width = positive(&clip["width"], 8192.)?;
            let height = positive(&clip["height"], 16000.)?;
            let x = offset(&clip["x"])?;
            let y = offset(&clip["y"])?;
            ensure!(
                width * height <= 20_000_000. && x + width <= 8352. && y + height <= 16240.,
                "render bounds exceeded"
            );
            let metadata = serde_json::to_vec(&output["metadata"])?;
            ensure!(metadata.len() <= 4096, "render metadata too large");
            let result = cdp
                .capture_png(
                    session,
                    json!({"x":x,"y":y,"width":width,"height":height,"scale":1}),
                )
                .await?;
            Ok(Output {
                mime: "image/png",
                bytes: decode(result)?,
                metadata: Some(URL_SAFE_NO_PAD.encode(metadata)),
            })
        }
        Job::Pdf { html } => {
            ensure!(html.len() <= 40_000_000, "PDF HTML limit exceeded");
            viewport(cdp, session, 1400, 1600).await?;
            let tree = cdp
                .call("Page.getFrameTree", json!({}), Some(session))
                .await?;
            let frame = tree["frameTree"]["frame"]["id"]
                .as_str()
                .context("PDF frame missing")?;
            // Keep printing content offline. The intercept rejects all network
            // requests for this job, and the policy prevents application scripts.
            let policy = r#"<meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src data: blob:; font-src data:; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'">"#;
            let html = if let Some(end) = html.find("<head>").map(|i| i + "<head>".len()) {
                format!("{}{policy}{}", &html[..end], &html[end..])
            } else {
                format!("{policy}{html}")
            };
            cdp.call(
                "Page.setDocumentContent",
                json!({"frameId":frame,"html":html}),
                Some(session),
            )
            .await?;
            evaluate(cdp, session, r#"(async () => {
                if (document.readyState !== "complete") await new Promise(resolve => window.addEventListener("load", resolve, {once:true}));
                await document.fonts.ready;
            })()"#).await?;
            let result = cdp
                .print_pdf(
                    session,
                    json!({
                        "printBackground":true,"preferCSSPageSize":true,"generateTaggedPDF":true
                    }),
                )
                .await?;
            Ok(Output {
                mime: "application/pdf",
                bytes: decode(result)?,
                metadata: None,
            })
        }
    }
}

async fn viewport(cdp: &mut Cdp, session: &str, width: u64, height: u64) -> Result<()> {
    cdp.call(
        "Emulation.setDeviceMetricsOverride",
        json!({"width":width,"height":height,"deviceScaleFactor":1,"mobile":false}),
        Some(session),
    )
    .await?;
    Ok(())
}

async fn evaluate(cdp: &mut Cdp, session: &str, expression: &str) -> Result<Value> {
    let result = cdp
        .call(
            "Runtime.evaluate",
            json!({"expression":expression,"awaitPromise":true,"returnByValue":true}),
            Some(session),
        )
        .await?;
    ensure!(
        result["exceptionDetails"].is_null(),
        "app render page evaluation failed"
    );
    Ok(result["result"]["value"].clone())
}

fn dimension(value: &Value, maximum: u64) -> Result<u64> {
    Ok(value
        .as_u64()
        .filter(|n| *n > 0 && *n <= maximum)
        .context("invalid viewport")?)
}

fn positive(value: &Value, maximum: f64) -> Result<f64> {
    Ok(value
        .as_f64()
        .filter(|n| *n > 0. && *n <= maximum)
        .context("invalid capture size")?)
}

fn offset(value: &Value) -> Result<f64> {
    Ok(value
        .as_f64()
        .filter(|n| *n >= 0.)
        .context("invalid capture offset")?)
}

fn decode(result: Value) -> Result<Vec<u8>> {
    let data = result["data"].as_str().context("render data missing")?;
    ensure!(
        data.len() <= OUTPUT_LIMIT * 4 / 3 + 4,
        "render output too large"
    );
    let bytes = STANDARD.decode(data)?;
    ensure!(
        !bytes.is_empty() && bytes.len() <= OUTPUT_LIMIT,
        "invalid render output size"
    );
    Ok(bytes)
}
