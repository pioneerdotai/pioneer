//! Desktop App presentation. The retained WebView moves; execution belongs to Gateway.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
use gpui_kit::component::{button::*, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use gpui_webview::WebView;
use pioneer_client::core::ClientCore;
use std::{sync::Arc, time::Duration};
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
    previous: AppPlacement,
    publication: Option<pioneer_client::apps::AppViewPublication>,
    webview: Option<Entity<WebView>>,
    context: Option<wry::WebContext>,
    _storage: tempfile::TempDir,
    visible: bool,
    focus: FocusHandle,
    palette: serde_json::Value,
    document_loaded: Arc<std::sync::atomic::AtomicBool>,
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
            Self {
                client,
                ticket,
                placement: AppPlacement::Bottom,
                previous: AppPlacement::Bottom,
                publication: None,
                webview: None,
                context: None,
                _storage: storage,
                visible: true,
                focus: cx.focus_handle(),
                palette: palette(cx),
                document_loaded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
    fn place(&mut self, placement: AppPlacement, cx: &mut Context<Self>) {
        if self.placement == placement {
            return;
        }
        if placement == AppPlacement::Expanded {
            self.previous = self.placement;
        }
        self.placement = placement;
        if let Some(view) = &self.webview {
            // Reuse the loaded page, but remeasure it in the new panel.
            view.update(cx, |_, cx| cx.notify());
        }
        cx.emit(AppViewEvent::Placement);
        cx.notify();
    }
    fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let next = self.client.app_view_publication(self.ticket);
        let changed = self
            .publication
            .as_ref()
            .map(|p| (&p.state, &p.title, &p.url))
            != next.as_ref().map(|p| (&p.state, &p.title, &p.url));
        self.publication = next;
        if self.publication.as_ref().is_none_or(|p| p.state != "ready") {
            self.hide(cx);
            self.webview.take();
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
                let prefix = self
                    .publication
                    .as_ref()
                    .and_then(|p| p.base_url.as_ref())
                    .map(|u| u.expose_secret().to_owned());
                if let Some(prefix) = prefix {
                    self.palette = palette(cx);
                    self.context =
                        Some(wry::WebContext::new(Some(self._storage.path().to_owned())));
                    let document_loaded = self.document_loaded.clone();
                    let builder = wry::WebViewBuilder::new_with_web_context(
                        self.context.as_mut().expect("owned context"),
                    )
                    .with_background_color(native_background(cx))
                    .with_html(isolated_document(
                        &url,
                        &t!("apps.view_title").to_string(),
                        &self.palette,
                    ))
                    .with_initialization_script(theme_script(&self.palette))
                    .with_on_page_load_handler(move |event, _| {
                        if matches!(event, wry::PageLoadEvent::Finished) {
                            document_loaded.store(true, std::sync::atomic::Ordering::Release);
                        }
                    })
                    .with_incognito(true)
                    .with_devtools(false)
                    .with_clipboard(false)
                    .with_download_started_handler(|_, _| false)
                    .with_visible(false)
                    .with_navigation_handler(move |url| {
                        url == "about:blank" || url.starts_with(&prefix)
                    })
                    .with_new_window_req_handler(|_, _| wry::NewWindowResponse::Deny);
                    match WebView::build(builder, window, cx) {
                        Ok(view) => {
                            apply_native_theme(&view, cx);
                            self.webview = Some(cx.new(|_| view));
                        }
                        Err(_) => self.failed = true,
                    }
                } else {
                    self.failed = true;
                }
            }
        }
        let palette = palette(cx);
        if self.palette != palette
            || self
                .document_loaded
                .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.palette = palette;
            if let Some(view) = &self.webview {
                let view = view.read(cx);
                apply_native_theme(view, cx);
                let _ = view.evaluate_script(&theme_script(&self.palette));
            }
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
        let title = self
            .publication
            .as_ref()
            .map(|p| p.title.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| t!("apps.view_title").to_string());
        let header = h_flex()
            .items_center()
            .gap_2()
            .px_3()
            .py_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(div().flex_1().min_w_0().truncate().child(title))
            .child(
                Button::new("app-bottom")
                    .small()
                    .ghost()
                    .icon(gpui_kit::assets::IconName::PanelBottom)
                    .tooltip(t!("apps.bottom").to_string())
                    .accessibility_label(t!("apps.bottom").to_string())
                    .on_click(cx.listener(|view, _, _, cx| view.place(AppPlacement::Bottom, cx))),
            )
            .child(
                Button::new("app-right")
                    .small()
                    .ghost()
                    .icon(gpui_kit::assets::IconName::PanelRight)
                    .tooltip(t!("apps.right").to_string())
                    .accessibility_label(t!("apps.right").to_string())
                    .on_click(cx.listener(|view, _, _, cx| view.place(AppPlacement::Right, cx))),
            )
            .child(
                Button::new("app-expand")
                    .small()
                    .ghost()
                    .icon(if expanded {
                        gpui_kit::assets::IconName::Minimize2
                    } else {
                        gpui_kit::assets::IconName::Maximize2
                    })
                    .tooltip(
                        t!(if expanded {
                            "apps.exit_expanded"
                        } else {
                            "apps.expand"
                        })
                        .to_string(),
                    )
                    .accessibility_label(
                        t!(if expanded {
                            "apps.exit_expanded"
                        } else {
                            "apps.expand"
                        })
                        .to_string(),
                    )
                    .on_click(cx.listener(|view, _, _, cx| {
                        let placement = if view.placement == AppPlacement::Expanded {
                            view.previous
                        } else {
                            AppPlacement::Expanded
                        };
                        view.place(placement, cx);
                    })),
            )
            .child(
                Button::new("app-close")
                    .small()
                    .ghost()
                    .icon(gpui_kit::assets::IconName::X)
                    .tooltip(t!("apps.close_view").to_string())
                    .accessibility_label(t!("apps.close_view").to_string())
                    .on_click(cx.listener(|view, _, _, cx| {
                        view.hide(cx);
                        cx.emit(AppViewEvent::Close);
                    })),
            );
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
            .child(header)
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

fn palette(cx: &App) -> serde_json::Value {
    let theme = cx.theme();
    let css = |color: Hsla| {
        let c = color.to_rgb();
        format!("rgba({},{},{},{})", c.r * 255., c.g * 255., c.b * 255., c.a)
    };
    serde_json::json!({"appearance":if theme.mode.is_dark(){"dark"}else{"light"},"background":css(theme.background),"foreground":css(theme.foreground),"muted":css(theme.muted_foreground),"border":css(theme.border),"accent":css(theme.primary),"accent-foreground":css(theme.primary_foreground)})
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
    let appearance = palette["appearance"].as_str().expect("theme appearance");
    let background = palette["background"].as_str().expect("theme background");
    let foreground = palette["foreground"].as_str().expect("theme foreground");
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
    format!(
        r#"(()=>{{const p={palette};const apply=()=>{{const r=document.documentElement;if(!r)return;r.style.colorScheme=p.appearance;for(const k of ['background','foreground','muted','border','accent','accent-foreground'])r.style.setProperty('--pioneer-'+k,p[k]);const f=document.querySelector('iframe');if(f){{const send=()=>f.contentWindow.postMessage({{type:'pioneer.theme',palette:p}},'*');f.onload=()=>{{send();f.style.visibility='visible'}};send()}}}};apply();document.addEventListener('DOMContentLoaded',apply,{{once:true}})}})();"#
    )
}
