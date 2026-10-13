//! Inline HTML row ownership. Kit owns the native Wry geometry/composition;
//! this view owns exact source, visibility, ephemeral instance and DOM height.
use super::{controller::DesktopTimelineController, layout_store::RowMeasurementKey};
use crate::screen::TimelineView;
use gpui_kit::{
    component::{theme::ActiveTheme, v_flex},
    prelude::*,
    *,
};
use gpui_webview::WebView;
use pioneer_client::{
    artifacts::{
        ArtifactStatus, HtmlPublication,
        workflow::{ArtifactActionIdentity, ArtifactActionKind},
    },
    core::{ClientCore, ClientTransitionOutcome},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

static INSTANCE: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Layout {
    instance: u64,
    width: f64,
    height: f64,
}
fn layout(raw: &str, instance: u64, width: f64) -> Option<f32> {
    if raw.len() > 1024 || width <= 0. {
        return None;
    }
    let value: Layout = serde_json::from_str(raw).ok()?;
    (value.instance == instance
        && value.width.is_finite()
        && (value.width - width).abs() <= 1.
        && value.height.is_finite()
        && value.height >= 1.
        && value.height.ceil() <= 9_007_199_254_740_991.)
        .then_some(value.height.ceil() as f32)
}
fn palette(cx: &App) -> serde_json::Value {
    let theme = cx.theme();
    let css = |color: Hsla| {
        let color = color.to_rgb();
        format!(
            "rgba({},{},{},{})",
            color.r * 255.,
            color.g * 255.,
            color.b * 255.,
            color.a
        )
    };
    serde_json::json!({"appearance":if theme.mode.is_dark(){"dark"}else{"light"},
        "background":css(theme.background),"foreground":css(theme.foreground),
        "muted":css(theme.muted_foreground),"border":css(theme.border),"accent":css(theme.primary)})
}
fn script(instance: u64, palette: &serde_json::Value) -> String {
    format!(
        r#"(()=>{{if(window.top!==window)return;const instance={instance};let scheduled=false,last='';const measure=()=>{{scheduled=false;const b=document.body;if(!b)return;const r=b.getBoundingClientRect();let bottom=Math.max(1,r.bottom+scrollY,r.top+scrollY+b.scrollHeight);for(const e of b.children)bottom=Math.max(bottom,e.getBoundingClientRect().bottom+scrollY);const payload=JSON.stringify({{instance,width:innerWidth,height:Math.ceil(bottom)}});if(payload!==last){{last=payload;window.ipc.postMessage(payload)}}}};const schedule=()=>{{if(!scheduled){{scheduled=true;requestAnimationFrame(measure)}}}};const apply=p=>{{const r=document.documentElement;if(!r)return;r.style.colorScheme=p.appearance;for(const k of ['background','foreground','muted','border','accent'])r.style.setProperty('--pioneer-'+k,p[k]);schedule()}};window.addEventListener('pioneer-theme',e=>apply(e.detail));window.addEventListener('resize',schedule);window.addEventListener('pioneer-measure',()=>{{last='';schedule()}});window.addEventListener('load',schedule);document.addEventListener('DOMContentLoaded',()=>{{const resize=new ResizeObserver(schedule);resize.observe(document.body);resize.observe(document.documentElement);new MutationObserver(schedule).observe(document.documentElement,{{subtree:true,childList:true,attributes:true,characterData:true}});document.fonts.ready.then(schedule);document.fonts.addEventListener('loadingdone',schedule);apply({palette});schedule()}});apply({palette});}})();"#
    )
}

pub(super) struct HtmlView {
    publication: HtmlPublication,
    client: Arc<ClientCore>,
    runtime: PathBuf,
    owner: WeakEntity<TimelineView>,
    key: RowMeasurementKey,
    permitted: bool,
    can_load: bool,
    visible: bool,
    auth_generation: u64,
    source: Option<Arc<str>>,
    action: Option<ArtifactActionIdentity>,
    download: Option<Task<()>>,
    webview: Option<Entity<WebView>>,
    messages: Option<Task<()>>,
    instance: u64,
    height: Pixels,
    bounds: Bounds<Pixels>,
    palette: serde_json::Value,
    failed: bool,
}
impl Drop for HtmlView {
    fn drop(&mut self) {
        if let Some(identity) = &self.action {
            self.client.cancel_artifact_action(identity);
        }
        // Dropping Kit's owning entity hides/destroys the child. No raw handles
        // escape this owner; messages/download tasks are cancelled on drop.
    }
}
impl HtmlView {
    pub(super) fn new(
        publication: HtmlPublication,
        client: Arc<ClientCore>,
        runtime: PathBuf,
        owner: WeakEntity<TimelineView>,
        key: RowMeasurementKey,
        permitted: bool,
        can_load: bool,
    ) -> Self {
        let auth_generation = client.authorization_connection_generation();
        Self {
            publication,
            client,
            runtime,
            owner,
            key,
            permitted,
            can_load,
            visible: false,
            auth_generation,
            source: None,
            action: None,
            download: None,
            webview: None,
            messages: None,
            instance: 0,
            height: px(1.),
            bounds: Bounds::default(),
            palette: serde_json::Value::Null,
            failed: false,
        }
    }
    pub(super) fn matches(&self, publication: &HtmlPublication) -> bool {
        self.publication.artifact.artifact_id == publication.artifact.artifact_id
            && self.publication.artifact.version_id == publication.artifact.version_id
    }
    pub(super) fn synchronize(
        &mut self,
        publication: HtmlPublication,
        key: RowMeasurementKey,
        permitted: bool,
        can_load: bool,
        cx: &mut Context<Self>,
    ) {
        let generation = self.client.authorization_connection_generation();
        if self.publication == publication
            && self.key == key
            && self.permitted == permitted
            && self.can_load == can_load
            && self.auth_generation == generation
        {
            return;
        }
        if can_load && !self.can_load && self.source.is_none() {
            self.failed = false;
        }
        self.can_load = can_load;
        self.publication = publication;
        self.key = key;
        if self.permitted != permitted || self.auth_generation != generation {
            self.close(cx);
            self.permitted = permitted;
            self.auth_generation = generation;
            self.failed = false;
            // A new authorization owner must download/revalidate exact source.
            self.source = None;
        }
        cx.notify();
    }
    pub(super) fn set_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        if self.visible == visible {
            return;
        }
        self.visible = visible;
        if !visible || !self.permitted {
            self.close(cx);
        }
        cx.notify();
    }
    fn close(&mut self, cx: &mut Context<Self>) {
        if let Some(view) = self.webview.take() {
            let _ = view.read(cx).set_visible(false);
        }
        self.messages = None;
        self.instance = 0;
        if let Some(identity) = self.action.take() {
            self.client.cancel_artifact_action(&identity);
        }
        self.download = None;
    }
    fn load(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_load || self.source.is_some() || self.download.is_some() || self.failed {
            return;
        }
        let publication = self.publication.clone();
        if self
            .client
            .begin_artifact_action(
                publication.thread_id.clone(),
                publication.artifact.artifact_id.clone(),
                publication.artifact.version_id.clone(),
                ArtifactActionKind::Share,
            )
            .outcome()
            != ClientTransitionOutcome::Changed
        {
            self.failed = true;
            return;
        }
        let identity = self
            .client
            .artifact_snapshot(&publication.thread_id)
            .and_then(|p| {
                p.actions
                    .iter()
                    .find(|a| {
                        a.identity.artifact_id == publication.artifact.artifact_id
                            && a.identity.version_id == publication.artifact.version_id
                    })
                    .map(|a| a.identity.clone())
            });
        let Some(identity) = identity else {
            self.failed = true;
            return;
        };
        self.action = Some(identity.clone());
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        self.download = Some(cx.spawn_in(window, async move |view, cx| {
            let operation = identity.clone();
            let worker = client.clone();
            let result = cx
                .background_spawn(async move {
                    let (_, result) = worker.download_artifact_action(
                        &operation,
                        format!("html:{}", operation.generation),
                        runtime,
                    )?;
                    anyhow::ensure!(
                        result.artifact_id == publication.artifact.artifact_id
                            && Some(&result.version_id) == publication.artifact.version_id.as_ref()
                            && Some(&result.sha256) == publication.artifact.sha256.as_ref()
                            && Some(result.size_bytes) == publication.artifact.size_bytes
                            && result.size_bytes <= 512_000 * 4,
                        "HTML exact source mismatch"
                    );
                    let bytes = std::fs::read(result.local_path.as_path())?;
                    anyhow::ensure!(
                        bytes.len() as u64 == result.size_bytes
                            && Sha256::digest(&bytes)
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                                == result.sha256,
                        "HTML cache integrity mismatch"
                    );
                    Ok::<_, anyhow::Error>(String::from_utf8(bytes)?)
                })
                .await;
            let _ = view.update_in(cx, |view, window, cx| {
                if view.action.as_ref() != Some(&identity) || !view.visible || !view.permitted {
                    return;
                }
                view.download = None;
                if client.claim_artifact_presentation(&identity) {
                    let error = if result.is_err() {
                        Some("download_failed".into())
                    } else {
                        None
                    };
                    client.complete_artifact_presentation(&identity, error);
                    match result {
                        Ok(html) => view.source = Some(html.into()),
                        Err(_) => view.failed = true,
                    }
                } else {
                    view.failed = true;
                }
                view.action = None;
                view.changed(window, cx);
                cx.notify();
            });
        }));
    }
    fn changed(&self, window: &mut Window, cx: &mut Context<Self>) {
        let owner = self.owner.clone();
        let id = self.key.id.clone();
        // Leave the child update before measuring/reconciling its retained row.
        window.defer(cx, move |window, cx| {
            let _ = owner.update(cx, |view, cx| {
                if view.row_registry.get(&id).is_none() {
                    return;
                }
                view.layout_store.invalidate_row(&id);
                view.reconcile_timeline(window, cx);
                DesktopTimelineController::schedule(view, window, cx);
                cx.notify();
            });
        });
    }
    fn build(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.webview.is_some() || !self.visible || !self.permitted || self.failed {
            return;
        }
        let Some(source) = self.source.as_ref() else {
            self.load(window, cx);
            return;
        };
        self.instance = INSTANCE.fetch_add(1, Ordering::Relaxed);
        let instance = self.instance;
        let (sender, mut receiver) = watch::channel(None::<String>);
        let palette = palette(cx);
        let initialization = script(instance, &palette);
        let html = format!(
            "<!doctype html><meta name=viewport content=\"width=device-width,initial-scale=1\"><style>html,body{{margin:0;background:var(--pioneer-background);color:var(--pioneer-foreground)}}</style><script>{initialization}</script>{source}"
        );
        let builder = wry::WebViewBuilder::new()
            .with_html(html)
            .with_visible(false)
            .with_incognito(true)
            .with_devtools(false)
            .with_clipboard(false)
            .with_navigation_handler(|url| url == "about:blank")
            .with_new_window_req_handler(|_, _| wry::NewWindowResponse::Deny)
            .with_ipc_handler(move |request| {
                if matches!(request.uri().to_string().as_str(), "about:blank" | "")
                    && request.body().len() <= 1024
                {
                    sender.send_replace(Some(request.into_body()));
                }
            });
        match WebView::build(builder, window, cx) {
            Ok(view) => {
                let bounds = self.bounds;
                let rect = wry::Rect {
                    position: wry::dpi::LogicalPosition::new(
                        f64::from(bounds.origin.x),
                        f64::from(bounds.origin.y),
                    )
                    .into(),
                    size: wry::dpi::LogicalSize::new(
                        f64::from(bounds.size.width.max(px(1.))),
                        f64::from(self.height),
                    )
                    .into(),
                };
                if view.set_bounds(rect).is_err() || view.set_visible(true).is_err() {
                    self.failed = true;
                    self.changed(window, cx);
                    return;
                }
                self.webview = Some(cx.new(|_| view));
                self.palette = palette;
                self.messages = Some(cx.spawn_in(window, async move |view, cx| {
                    while receiver.changed().await.is_ok() {
                        let raw = receiver.borrow_and_update().clone();
                        let Some(raw) = raw else {
                            continue;
                        };
                        if view
                            .update_in(cx, |view, window, cx| {
                                if view.instance != instance || !view.visible || !view.permitted {
                                    return;
                                }
                                if let Some(height) =
                                    layout(&raw, instance, f64::from(view.bounds.size.width))
                                {
                                    if view.height != px(height) {
                                        view.height = px(height);
                                        view.changed(window, cx);
                                        cx.notify();
                                    }
                                }
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                }));
            }
            Err(_) => {
                self.failed = true;
                self.changed(window, cx);
            }
        }
    }
    // Resolved document geometry is available after prepaint. This lifecycle
    // update owns mount/theme effects; Render only composes retained content.
    fn update_bounds(
        &mut self,
        bounds: Bounds<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let width_changed = self.bounds.size.width != bounds.size.width;
        self.bounds = bounds;
        if self.visible
            && self.permitted
            && !self.failed
            && self.webview.is_none()
            && (self.source.is_some() || (self.can_load && self.download.is_none()))
        {
            self.build(window, cx);
            if self.failed {
                self.changed(window, cx);
            }
            cx.notify();
        }
        if let Some(webview) = self.webview.clone() {
            let next_palette = palette(cx);
            if self.palette != next_palette {
                let script = format!(
                    "window.dispatchEvent(new CustomEvent('pioneer-theme',{{detail:{next_palette}}}));"
                );
                if webview.read(cx).evaluate_script(&script).is_err() {
                    self.failed = true;
                    self.close(cx);
                    self.changed(window, cx);
                    cx.notify();
                    return;
                }
                self.palette = next_palette;
            }
            if width_changed {
                let _ = webview
                    .read(cx)
                    .evaluate_script("window.dispatchEvent(new Event('pioneer-measure'));");
            }
        }
    }
}
impl Render for HtmlView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let content =
            if !self.permitted || self.publication.artifact.status != ArtifactStatus::Ready {
                div()
                    .child(t!("artifacts.action.error.revoked"))
                    .into_any_element()
            } else if self.failed {
                div().child(t!("timeline.html.failed")).into_any_element()
            } else if self.source.is_none() {
                div().child(t!("timeline.html.loading")).into_any_element()
            } else {
                div()
                    .w_full()
                    .h(self.height)
                    .flex_shrink_0()
                    .when_some(self.webview.clone(), |this, view| this.child(view))
                    .into_any_element()
            };
        let weak = cx.weak_entity();
        let instance = self.instance;
        let auth_generation = self.auth_generation;
        let content = div().relative().w_full().child(content).child(
            canvas(
                move |bounds, window, cx| {
                    let weak = weak.clone();
                    window.defer(cx, move |window, cx| {
                        let _ = weak.update(cx, |view, cx| {
                            if view.instance != instance || view.auth_generation != auth_generation
                            {
                                return;
                            }
                            view.update_bounds(bounds, window, cx);
                        });
                    });
                },
                |_, _, _, _| {},
            )
            .absolute()
            .size_full(),
        );
        v_flex()
            .w_full()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(self.publication.title.clone()),
            )
            .child(content)
    }
}

#[cfg(test)]
mod tests {
    use super::layout;
    #[::core::prelude::v1::test]
    fn natural_height_and_instance_width_fencing() {
        assert_eq!(
            layout(r#"{"instance":1,"width":360,"height":100000}"#, 1, 360.),
            Some(100000.)
        );
        assert_eq!(
            layout(r#"{"instance":1,"width":360,"height":260}"#, 2, 360.),
            None
        );
        assert_eq!(
            layout(r#"{"instance":1,"width":360,"height":260}"#, 1, 768.),
            None
        );
        assert_eq!(
            layout(
                r#"{"instance":1,"width":360,"height":260,"rpc":"file"}"#,
                1,
                360.
            ),
            None
        );
        assert_eq!(
            layout(r#"{"instance":1,"width":360,"height":1e36}"#, 1, 360.),
            None
        );
        assert_eq!(layout(&" ".repeat(1025), 1, 360.), None);
    }
}
