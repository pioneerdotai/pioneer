//! Desktop App presentation. The retained WebView moves; execution belongs to Gateway.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use gpui_webview::WebView;
use pioneer_client::core::ClientCore;
use std::{sync::Arc, time::Duration};
mod view_bridge;
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AppPlacement {
    Bottom,
    Right,
    Expanded,
}
pub enum AppViewEvent {
    Placement,
    Close,
}
pub struct AppView {
    client: Arc<ClientCore>,
    ticket: u64,
    placement: AppPlacement,
    publication: Option<pioneer_client::apps::AppViewPublication>,
    webview: Option<Entity<WebView>>,
    context: Option<wry::WebContext>,
    _storage: tempfile::TempDir,
    visible: bool,
    focus: FocusHandle,
    theme_snapshot: serde_json::Value,
    _theme: Subscription,
    document_loaded: Arc<std::sync::atomic::AtomicBool>,
    host_requests: Option<Task<()>>,
    _poll: Option<Task<()>>,
    failed: bool,
}
impl EventEmitter<AppViewEvent> for AppView {}
impl Drop for AppView {
    fn drop(&mut self) {
        self.client.close_app_view(self.ticket);
    }
}
impl AppView {
    pub fn new(
        client: Arc<ClientCore>,
        workspace: String,
        app: String,
        window: &mut Window,
        cx: &mut App,
    ) -> anyhow::Result<Entity<Self>> {
        let storage = tempfile::tempdir()?;
        let ticket = client.begin_app_view(workspace, app)?;
        Ok(cx.new(|cx: &mut Context<Self>| {
            let poll = cx.spawn_in(window, async move |owner, cx| {
                loop {
                    let timer = cx.background_executor().timer(Duration::from_millis(500));
                    timer.await;
                    if owner
                        .update_in(cx, |view, window, cx| view.refresh(window, cx))
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let theme_subscription =
                cx.observe_global_in::<gpui_kit::component::Theme>(window, |view, _, cx| {
                    view.theme_snapshot = theme_snapshot(cx);
                    view.apply_theme(cx);
                    cx.notify();
                });
            Self {
                client,
                ticket,
                placement: AppPlacement::Expanded,
                publication: None,
                webview: None,
                context: None,
                _storage: storage,
                visible: true,
                focus: cx.focus_handle(),
                theme_snapshot: theme_snapshot(cx),
                _theme: theme_subscription,
                document_loaded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                host_requests: None,
                _poll: Some(poll),
                failed: false,
            }
        }))
    }
    pub fn placement(&self) -> AppPlacement {
        self.placement
    }
    pub fn set_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        if self.visible == visible {
            return;
        }
        self.visible = visible;
        self.sync_webview_visibility(cx);
        cx.notify();
    }
    fn sync_webview_visibility(&self, cx: &mut App) {
        if let Some(view) = &self.webview
            && view.read(cx).visible() != self.visible
        {
            view.update(cx, |view, cx| {
                let _ = view.set_visible(self.visible);
                // Showing a retained native view must run geometry prepaint.
                cx.notify();
            });
        }
    }
    pub fn hide(&self, cx: &App) {
        if let Some(view) = &self.webview {
            let _ = view.read(cx).set_visible(false);
        }
    }
    fn close(&mut self, cx: &mut Context<Self>) {
        self.hide(cx);
        self.host_requests.take();
        cx.emit(AppViewEvent::Close);
    }
    fn place(&mut self, placement: AppPlacement, cx: &mut Context<Self>) {
        if self.placement == placement {
            return;
        }
        self.placement = placement;
        if let Some(view) = &self.webview {
            // Reuse the loaded page, but remeasure it in the new panel.
            view.update(cx, |_, cx| cx.notify());
        }
        cx.emit(AppViewEvent::Placement);
        cx.notify();
    }
    // Native IPC wakes this task immediately; view commands never wait for
    // the publication/theme refresh timer. Dropping the view cancels reception.
    fn handle_view_request(&mut self, request: view_bridge::Request, cx: &mut Context<Self>) {
        match request.command() {
            view_bridge::Command::SetPlacement(placement) => self.place(placement, cx),
            view_bridge::Command::Close => {
                self.close(cx);
                return;
            }
        }
        if let Some(view) = &self.webview {
            let development = self
                .publication
                .as_ref()
                .expect("ready publication")
                .development;
            if let Some(response) = request.response(development) {
                let _ = view.read(cx).evaluate_script(&response);
            }
        }
    }
    fn apply_theme(&self, cx: &App) {
        if let Some(view) = &self.webview {
            let view = view.read(cx);
            apply_native_theme(view, cx);
            let publication = self.publication.as_ref().expect("ready publication");
            let script = if publication.development {
                dev_theme_script(
                    &self.theme_snapshot,
                    publication.url.as_ref().expect("ready URL").expose_secret(),
                )
            } else {
                theme_script(&self.theme_snapshot)
            };
            let _ = view.evaluate_script(&script);
        }
    }
    fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let next = self.client.app_view_publication(self.ticket);
        let changed = self
            .publication
            .as_ref()
            .map(|p| (&p.state, &p.title, &p.url, &p.base_url))
            != next
                .as_ref()
                .map(|p| (&p.state, &p.title, &p.url, &p.base_url));
        self.publication = next;
        if self.publication.as_ref().is_none_or(|p| p.state != "ready") {
            self.hide(cx);
            self.webview.take();
            self.host_requests.take();
            if changed {
                cx.notify();
            }
            return;
        }
        if self.webview.is_none() && !self.failed {
            let url = self
                .publication
                .as_ref()
                .and_then(|p| p.url.as_ref())
                .map(|u| u.expose_secret().to_owned());
            if let Some(url) = url {
                self.theme_snapshot = theme_snapshot(cx);
                let publication = self.publication.as_ref().expect("ready publication");
                let attributes = view_attributes(
                    &url,
                    publication.base_url.as_ref().map(|u| u.expose_secret()),
                    publication.development,
                    &t!("apps.view_title").to_string(),
                    &self.theme_snapshot,
                );
                if let Ok((mut attributes, mut host_requests)) = attributes {
                    self.context =
                        Some(wry::WebContext::new(Some(self._storage.path().to_owned())));
                    let document_loaded = self.document_loaded.clone();
                    attributes.context = self.context.as_mut();
                    let builder = wry::WebViewBuilder::new_with_attributes(attributes)
                        .with_background_color(native_background(cx))
                        .with_on_page_load_handler(move |event, _| {
                            if matches!(event, wry::PageLoadEvent::Finished) {
                                document_loaded.store(true, std::sync::atomic::Ordering::Release);
                            }
                        });
                    match WebView::build(builder, window, cx) {
                        Ok(view) => {
                            apply_native_theme(&view, cx);
                            self.webview = Some(cx.new(|_| view));
                            self.host_requests =
                                Some(cx.spawn_in(window, async move |owner, cx| {
                                    while let Some(request) = host_requests.recv().await {
                                        let closing = matches!(
                                            request.command(),
                                            view_bridge::Command::Close
                                        );
                                        if owner
                                            .update_in(cx, |view, _, cx| {
                                                view.handle_view_request(request, cx);
                                            })
                                            .is_err()
                                            || closing
                                        {
                                            break;
                                        }
                                    }
                                }));
                        }
                        Err(_) => self.failed = true,
                    }
                } else {
                    self.failed = true;
                }
            }
        }
        if self
            .document_loaded
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.apply_theme(cx);
        }
        self.sync_webview_visibility(cx);
        if changed || self.failed {
            cx.notify();
        }
    }
}
impl Render for AppView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let expanded = self.placement == AppPlacement::Expanded;
        let content = if let Some(view) = &self.webview {
            view.clone().into_any_element()
        } else {
            let state = self
                .publication
                .as_ref()
                .map(|p| p.state.as_str())
                .unwrap_or("loading");
            div()
                .p_4()
                .child(
                    t!(match state {
                        "loading" => "apps.loading",
                        "offline" => "apps.offline",
                        "denied" => "apps.denied",
                        "unavailable" => "apps.unavailable",
                        _ => "apps.failed",
                    })
                    .to_string(),
                )
                .into_any_element()
        };
        let content_region = div().flex_1().min_h_0().child(content);
        v_flex()
            .track_focus(&self.focus)
            .size_full()
            .min_h_0()
            .min_w_0()
            .overflow_hidden()
            .bg(cx.theme().background)
            // The resizable owns the one-pixel seam. Reserve its width without
            // painting a second translucent line over it. Native WebViews must
            // stay inside this inset, including beside a bottom-docked App.
            .when(!expanded, |panel| panel.pl(px(1.)))
            .when(self.placement == AppPlacement::Bottom, |panel| {
                panel.pt(px(1.))
            })
            .child(content_region)
    }
}

impl Focusable for AppView {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus.clone()
    }
}

fn apply_native_theme(view: &WebView, cx: &App) {
    let _ = view.set_theme(if cx.theme().mode.is_dark() {
        wry::Theme::Dark
    } else {
        wry::Theme::Light
    });
    // Also applies the macOS backdrop through gpui-webview's native adapter;
    // Wry's builder background alone is not implemented on that platform.
    let _ = view.set_background_color(native_background(cx));
}

fn native_background(cx: &App) -> wry::RGBA {
    let color = cx.theme().background.to_rgb();
    let byte = |component: f32| (component.clamp(0., 1.) * 255.).round() as u8;
    (byte(color.r), byte(color.g), byte(color.b), byte(color.a))
}

// Serialize the complete native theme instead of maintaining a second list of
// colors. New Theme fields, configs and nested tokens cross the bridge intact.
fn theme_snapshot(cx: &App) -> serde_json::Value {
    let mut snapshot = serde_json::to_value(cx.theme()).expect("theme serialization");
    snapshot["semantic_tokens"] =
        serde_json::to_value(cx.theme().semantic_tokens()).expect("semantic theme serialization");
    snapshot
}

/// These are the actual Wry loader attributes, consumed by the existing panel.
/// Only a view-scoped presentation bridge is exposed to either App presentation.
fn view_attributes<'a>(
    url: &str,
    prefix: Option<&str>,
    development: bool,
    title: &str,
    palette: &serde_json::Value,
) -> anyhow::Result<(
    wry::WebViewAttributes<'a>,
    tokio::sync::mpsc::Receiver<view_bridge::Request>,
)> {
    let mut attributes = wry::WebViewAttributes {
        incognito: true,
        devtools: development,
        clipboard: false,
        visible: false,
        download_started_handler: Some(Box::new(|_, _| false)),
        new_window_req_handler: Some(Box::new(|_, _| wry::NewWindowResponse::Deny)),
        ..Default::default()
    };
    let script = if development {
        let parsed = url::Url::parse(url)?;
        anyhow::ensure!(
            parsed.scheme() == "http" && parsed.host().is_some(),
            "invalid dev URL"
        );
        let origin = parsed.origin();
        // Top-level navigation stays on this origin. Subresources, fetch and
        // WebSockets use the browser's ordinary policies, without interception.
        attributes.navigation_handler = Some(Box::new(move |candidate| {
            url::Url::parse(&candidate).is_ok_and(|next| {
                next.origin() == origin && next.username().is_empty() && next.password().is_none()
            })
        }));
        attributes.url = Some(url.to_owned());
        let base = prefix.ok_or_else(|| anyhow::anyhow!("missing App view prefix"))?;
        let origin = serde_json::to_string(&parsed.origin().ascii_serialization())?;
        let base = serde_json::to_string(base)?;
        format!(
            r#"if(window.top===window&&location.origin==={origin})Object.defineProperty(window,"__pioneerAppBaseUrl",{{value:{base}}});{}"#,
            dev_theme_script(palette, url)
        )
    } else {
        let prefix = prefix
            .ok_or_else(|| anyhow::anyhow!("missing App view prefix"))?
            .to_owned();
        attributes.navigation_handler = Some(Box::new(move |url| {
            url == "about:blank" || url.starts_with(&prefix)
        }));
        attributes.html = Some(isolated_document(url, title, palette));
        theme_script(palette)
    };
    // Seed exact host surface colors before either the dev page or the
    // sandboxed installed App can paint its SSR/loading surface.
    let origin = url::Url::parse(url)?.origin().ascii_serialization();
    let surface = include_str!("theme_surface.js")
        .replace("__PIONEER_ORIGIN__", &serde_json::to_string(&origin)?)
        .replace("__PIONEER_THEME__", &palette.to_string());
    attributes
        .initialization_scripts
        .push(wry::InitializationScript {
            script: surface,
            for_main_frame_only: false,
        });
    attributes
        .initialization_scripts
        .push(wry::InitializationScript {
            script,
            for_main_frame_only: true,
        });
    let requests = view_bridge::install(&mut attributes, development, url)?;
    Ok((attributes, requests))
}

fn dev_theme_script(palette: &serde_json::Value, url: &str) -> String {
    let origin = serde_json::to_string(
        &url::Url::parse(url)
            .expect("validated dev URL")
            .origin()
            .ascii_serialization(),
    )
    .expect("origin JSON");
    // Wry on Windows injects initialization scripts into subframes too. Guard
    // both origin and top frame. This carries theme data only, no RPC bridge.
    // Leave the application's DOM intact so SSR hydration sees its own markup.
    format!(
        r#"(()=>{{if(window.top!==window||location.origin!=={origin})return;const p={palette};const send=()=>window.postMessage({{type:'pioneer.theme',theme:p}},location.origin);send();document.addEventListener('DOMContentLoaded',send,{{once:true}})}})();"#
    )
}
fn isolated_document(url: &str, title: &str, palette: &serde_json::Value) -> String {
    let escape = |v: &str| {
        v.replace('&', "&amp;")
            .replace('"', "&quot;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    };
    // The first parsed frame already has the host colors, before initialization
    // scripts or the iframe document load. Theme updates reuse the same variables.
    let appearance = palette["mode"].as_str().expect("theme appearance");
    let background = palette["colors"]["background"]
        .as_str()
        .expect("theme background");
    let foreground = palette["colors"]["foreground"]
        .as_str()
        .expect("theme foreground");
    // Hide the child's initial unstyled canvas until its load event. Visibility
    // preserves its viewport size, unlike removing it from layout. The inline
    // handler also covers a cached load preceding theme-script initialization.
    format!(
        r#"<!doctype html><html style="color-scheme:{};--pioneer-background:{};--pioneer-foreground:{}"><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>html,body,iframe{{margin:0;width:100%;height:100%;border:0;overflow:hidden;background:var(--pioneer-background);color:var(--pioneer-foreground)}}iframe{{display:block}}</style></head><body><iframe style="visibility:hidden" onload="this.style.visibility='visible'" title="{}" src="{}" sandbox="allow-scripts allow-forms" referrerpolicy="no-referrer"></iframe></body></html>"#,
        escape(appearance),
        escape(background),
        escape(foreground),
        escape(title),
        escape(url)
    )
}
fn theme_script(palette: &serde_json::Value) -> String {
    // Only the Pioneer wrapper owns these styles. Wry on Windows can also run
    // initialization scripts inside the application's iframe.
    format!(
        r#"(()=>{{if(window.top!==window)return;const p={palette};const apply=()=>{{const r=document.documentElement;if(!r)return;r.style.colorScheme=p.mode;for(const k of ['background','foreground'])r.style.setProperty('--pioneer-'+k,p.colors[k]);const f=document.querySelector('iframe');if(f){{const send=()=>f.contentWindow.postMessage({{type:'pioneer.theme',theme:p}},'*');f.onload=()=>{{send();f.style.visibility='visible'}};send()}}}};apply();document.addEventListener('DOMContentLoaded',apply,{{once:true}})}})();"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn palette() -> serde_json::Value {
        let theme = gpui_kit::component::Theme::default();
        let mut snapshot = serde_json::to_value(&theme).unwrap();
        snapshot["semantic_tokens"] = serde_json::to_value(theme.semantic_tokens()).unwrap();
        snapshot
    }

    #[::core::prelude::v1::test]
    fn dev_panel_passes_the_url_to_wry_with_only_the_presentation_bridge() {
        // Inspect the actual loader attributes passed to WebView::build. A
        // registration-only or location-planner test cannot cover this boundary.
        let prefix = "http://localhost:17878/storage/app-views/dev-grant/";
        let (attributes, _requests) = view_attributes(
            "http://localhost:3000",
            Some(prefix),
            true,
            "App",
            &palette(),
        )
        .unwrap();
        assert!(attributes.initialization_scripts.iter().any(|script| {
            script.script.contains("__pioneerAppBaseUrl") && script.script.contains(prefix)
        }));
        assert_eq!(attributes.url.as_deref(), Some("http://localhost:3000"));
        assert!(attributes.html.is_none());
        assert!(attributes.headers.is_none());
        assert!(attributes.ipc_handler.is_some());
        assert!(attributes.custom_protocols.is_empty());
        assert!(attributes.proxy_config.is_none());
        assert!(attributes.incognito);
        assert!(attributes.devtools);
        let navigate = attributes.navigation_handler.unwrap();
        for url in [
            "http://localhost:3000/",
            "http://localhost:3000/home?tab=1#details",
            "http://localhost:3000/deep/path",
        ] {
            assert!(navigate(url.into()), "{url}");
        }
        for url in [
            "http://localhost:30001/",
            "http://localhost:3000.evil.test/",
            "http://localhost:17878/storage/app-views/x/",
            "http://127.0.0.1:3000/",
            "http://localhost:3000@evil.test/",
            "https://localhost:3000/",
            "file:///tmp/index.html",
            "pioneer://internal",
            "about:blank",
        ] {
            assert!(!navigate(url.into()), "{url}");
        }
        assert!(attributes.new_window_req_handler.is_some());
    }

    #[::core::prelude::v1::test]
    fn installed_panel_keeps_its_sandboxed_document_and_devtools_disabled() {
        let prefix = "http://localhost:17878/storage/app-views/release-grant/";
        let (attributes, _requests) = view_attributes(
            &format!("{prefix}index.html"),
            Some(prefix),
            false,
            "App",
            &palette(),
        )
        .unwrap();
        assert!(attributes.url.is_none());
        let html = attributes.html.as_ref().unwrap();
        assert!(html.contains("sandbox=\"allow-scripts allow-forms\""));
        assert!(html.contains(&format!("src=\"{prefix}index.html\"")));
        assert!(!attributes.devtools);
        let navigate = attributes.navigation_handler.unwrap();
        assert!(navigate("about:blank".into()));
        assert!(navigate(format!("{prefix}next")));
        assert!(!navigate("http://localhost:3000/".into()));
    }
}
