//! Inline HTML row ownership. Kit owns the native Wry geometry/composition;
//! this view owns exact source, visibility, ephemeral instance and DOM height.
use super::{controller::DesktopTimelineController, layout_store::RowMeasurementKey};
use crate::screen::TimelineView;
use gpui_kit::{component::theme::ActiveTheme, prelude::*, *};
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

fn html_download_operation_id(identity: &ArtifactActionIdentity) -> String {
    format!("html-{}", identity.generation)
}

// Visibility never changes the full document bounds or DOM layout dimensions.
fn document_intersects_viewport(document: Bounds<Pixels>, viewport: Bounds<Pixels>) -> bool {
    document.size.width > px(0.)
        && document.size.height > px(0.)
        && viewport.size.width > px(0.)
        && viewport.size.height > px(0.)
        && document.intersects(&viewport)
}

static INSTANCE: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Layout {
    instance: u64,
    width: f64,
    height: f64,
    #[serde(default, rename = "wheelY")]
    wheel_y: f64,
}
fn layout(raw: &str, instance: u64, width: f64) -> Option<f32> {
    if raw.len() > 1024 || width <= 0. {
        return None;
    }
    let value: Layout = serde_json::from_str(raw).ok()?;
    (value.instance == instance
        && value.width.is_finite()
        && (value.width - width).abs() <= 1.
        && value.wheel_y.is_finite()
        && value.wheel_y.abs() <= 9_007_199_254_740_991.
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
        "muted":css(theme.muted_foreground),"border":css(theme.border),"accent":css(theme.primary),"accent-foreground":css(theme.primary_foreground)})
}
fn script(instance: u64, palette: &serde_json::Value) -> String {
    format!(
        r#"(()=>{{if(window.top!==window)return;const instance={instance};let scheduled=false,last='',wheelY=0;const measure=()=>{{scheduled=false;const b=document.body;if(!b)return;const r=b.getBoundingClientRect();let bottom=Math.max(1,r.bottom+scrollY,r.top+scrollY+b.scrollHeight);for(const e of b.children)bottom=Math.max(bottom,e.getBoundingClientRect().bottom+scrollY);const payload=JSON.stringify({{instance,width:innerWidth,height:Math.ceil(bottom),wheelY}});if(payload!==last){{last=payload;window.ipc.postMessage(payload)}}}};const schedule=()=>{{if(!scheduled){{scheduled=true;requestAnimationFrame(measure)}}}};const apply=p=>{{const r=document.documentElement;if(!r)return;r.style.colorScheme=p.appearance;for(const k of ['background','foreground','muted','border','accent','accent-foreground'])r.style.setProperty('--pioneer-'+k,p[k]);schedule()}};window.addEventListener('pioneer-theme',e=>apply(e.detail));window.addEventListener('resize',schedule);window.addEventListener('pioneer-measure',()=>{{last='';schedule()}});window.addEventListener('load',schedule);window.addEventListener('wheel',e=>{{if(e.ctrlKey||e.defaultPrevented||!Number.isFinite(e.deltaY)||e.deltaY===0)return;for(const node of e.composedPath()){{if(!(node instanceof Element)||node===document.body||node===document.documentElement)continue;const style=getComputedStyle(node);if(/^(auto|scroll|overlay)$/.test(style.overflowY)&&node.scrollHeight>node.clientHeight+1&&((e.deltaY>0&&node.scrollTop+node.clientHeight<node.scrollHeight-1)||(e.deltaY<0&&node.scrollTop>0)))return;}}const delta=e.deltaY*(e.deltaMode===1?16:e.deltaMode===2?innerHeight:1);if(!Number.isFinite(delta))return;e.preventDefault();wheelY+=Math.max(-10000,Math.min(10000,delta));schedule()}},{{passive:false}});document.addEventListener('DOMContentLoaded',()=>{{const resize=new ResizeObserver(schedule);resize.observe(document.body);resize.observe(document.documentElement);new MutationObserver(schedule).observe(document.documentElement,{{subtree:true,childList:true,attributes:true,characterData:true}});document.fonts.ready.then(schedule);document.fonts.addEventListener('loadingdone',schedule);apply({palette});measure()}});apply({palette});}})();"#
    )
}

pub(super) struct HtmlView {
    publication: HtmlPublication,
    response_thread: String,
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
    row_offset: Option<Point<Pixels>>,
    nearby: bool,
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
        response_thread: String,
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
            response_thread,
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
            row_offset: None,
            nearby: true,
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
        if self.publication.artifact.status != ArtifactStatus::Ready {
            self.close(cx);
        }
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
    pub(super) fn prepare_nearby(
        &mut self,
        width: Pixels,
        row_origin: Point<Pixels>,
        viewport: Bounds<Pixels>,
        first: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.visible
            || !self.permitted
            || self.failed
            || self.publication.artifact.status != ArtifactStatus::Ready
        {
            return;
        }
        // Measured siblings follow their own location as the row scrolls. Before
        // first layout only its first document can be prepared from the row origin.
        let offset = match self.row_offset {
            Some(offset) => offset,
            None if first => Point::default(),
            None => return,
        };
        self.bounds = Bounds::new(row_origin + offset, size(width, self.height));
        if !self.retain_nearby(self.bounds, viewport, cx) {
            return;
        }
        // Offscreen preparation keeps full DOM dimensions and an empty native clip.
        if let Some(webview) = &self.webview {
            if webview
                .read(cx)
                .set_geometry(self.bounds, Bounds::default())
                .is_err()
            {
                self.failed = true;
                self.close(cx);
            }
        } else {
            self.build(Bounds::default(), window, cx);
        }
        if self.failed {
            self.changed(window, cx);
            cx.notify();
        }
    }
    fn retain_nearby(
        &mut self,
        bounds: Bounds<Pixels>,
        viewport: Bounds<Pixels>,
        cx: &mut Context<Self>,
    ) -> bool {
        let preparation = Bounds::new(
            viewport.origin - point(px(0.), viewport.size.height),
            size(viewport.size.width, viewport.size.height * 3.),
        );
        // Admission follows the whole document, even while a parent is still
        // laid out at its previous (smaller) intrinsic height.
        let document = Bounds::new(bounds.origin, size(bounds.size.width, self.height));
        self.nearby = document_intersects_viewport(document, preparation);
        if !self.nearby {
            let live = self.webview.is_some()
                || self.messages.is_some()
                || self.download.is_some()
                || self.action.is_some()
                || self.instance != 0;
            self.close(cx);
            if live {
                cx.notify();
            }
        }
        self.nearby
    }

    fn close(&mut self, cx: &mut Context<Self>) {
        if let Some(view) = self.webview.take() {
            if let Err(error) = view.read(cx).set_visible(false) {
                tracing::warn!(%error, "HTML WebView visibility cleanup reported an error");
            }
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
                self.response_thread.clone(),
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
            .artifact_snapshot(&self.response_thread)
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
                    // Shared action admission waits for a free slot and is cancelled
                    // by close() retiring this exact identity; capacity is not failure.
                    let (_, result) = worker.download_artifact_action(
                        &operation,
                        html_download_operation_id(&operation),
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
                // Finish preparing a nearby page without waiting until it enters the list.
                if !view.failed && view.bounds.size.width > px(0.) {
                    view.build(Bounds::default(), window, cx);
                }
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
    fn build(&mut self, clip: Bounds<Pixels>, window: &mut Window, cx: &mut Context<Self>) {
        if self.webview.is_some() || !self.visible || !self.nearby || !self.permitted || self.failed
        {
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
                let document = Bounds::new(
                    bounds.origin,
                    size(bounds.size.width.max(px(1.)), self.height),
                );
                if view.set_visible(true).is_err() || view.set_geometry(document, clip).is_err() {
                    self.failed = true;
                    self.changed(window, cx);
                    return;
                }
                self.webview = Some(cx.new(|_| view));
                self.palette = palette;
                self.messages = Some(cx.spawn_in(window, async move |view, cx| {
                    let mut last_wheel_y = 0.;
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
                                    if let Ok(message) = serde_json::from_str::<Layout>(&raw) {
                                        let delta = message.wheel_y - last_wheel_y;
                                        last_wheel_y = message.wheel_y;
                                        if delta != 0. {
                                            let owner = view.owner.clone();
                                            let _ = owner.update(cx, |owner, cx| {
                                                if !owner.thread_timeline_view_state.visible {
                                                    return;
                                                }
                                                let scroll = owner
                                                    .thread_timeline_view_state
                                                    .scroll_handle
                                                    .clone();
                                                let mut offset = scroll.offset();
                                                let delta = px(delta.clamp(-10000., 10000.) as f32);
                                                owner.on_timeline_scroll_delta(-delta, cx);
                                                offset.y = (offset.y - delta)
                                                    .clamp(-scroll.max_offset().y, px(0.));
                                                scroll.set_offset(offset);
                                                DesktopTimelineController::schedule(
                                                    owner, window, cx,
                                                );
                                                cx.notify();
                                            });
                                        }
                                    }
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
        // The old screen and its painted rows can survive through frame clones.
        // A stale geometry callback must never mount a page in another route.
        let position = self.owner.upgrade().and_then(|owner| {
            let owner = owner.read(cx);
            if !owner.thread_timeline_view_state.visible
                || !owner
                    .row_registry
                    .get(&self.key.id)
                    .and_then(|slot| slot.view.as_ref())
                    .is_some_and(|row| row.read(cx).owns_html(cx.entity_id()))
            {
                return None;
            }
            let scroll = &owner.thread_timeline_view_state.scroll_handle;
            let viewport = scroll.bounds();
            let index = owner.layout_store.index.borrow();
            let top = index.origin(index.rank(&self.key.id)?)?;
            Some((
                viewport,
                viewport.origin + point(px(0.), top + scroll.offset().y),
            ))
        });
        let Some((viewport, row_origin)) = position else {
            let had_instance = self.webview.is_some() || self.download.is_some();
            self.close(cx);
            if had_instance {
                cx.notify();
            }
            return;
        };
        self.row_offset = Some(bounds.origin - row_origin);
        if !self.retain_nearby(bounds, viewport, cx) {
            return;
        }
        if self.visible
            && self.publication.artifact.status == ArtifactStatus::Ready
            && self.permitted
            && !self.failed
            && self.webview.is_none()
            && (self.source.is_some() || (self.can_load && self.download.is_none()))
        {
            self.build(viewport, window, cx);
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
        let content = div().relative().w_full().flex_shrink_0().child(content);
        let content = content.child(
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
            .top_0()
            .left_0()
            .size_full(),
        );
        content
    }
}

#[cfg(test)]
mod tests {
    use super::layout;
    #[::core::prelude::v1::test]
    fn real_html_operation_id_reaches_shared_download_and_keeps_exact_target() {
        use pioneer_client::artifacts::operations::ArtifactDownloadTarget;
        use pioneer_client::core::ClientCore;
        use std::sync::Arc;
        let client = Arc::new(ClientCore::new());
        let target = ArtifactDownloadTarget {
            thread_id: Some("response-thread".into()),
            workspace_id: "workspace".into(),
            artifact_id: "document".into(),
            version_id: Some("immutable-version".into()),
        };
        let identity = super::ArtifactActionIdentity {
            thread_id: "response-thread".into(),
            artifact_id: "document".into(),
            version_id: Some("immutable-version".into()),
            generation: 42,
        };
        let operation_id = super::html_download_operation_id(&identity);
        let operation = client
            .begin_artifact_download(operation_id.clone(), target.clone())
            .expect("HTML caller's ID must pass the actual shared boundary");
        assert_eq!(
            client
                .artifact_download_snapshot(&operation_id)
                .unwrap()
                .target,
            target
        );
        assert_eq!(
            client
                .artifact_download_snapshot(&operation_id)
                .unwrap()
                .target
                .version_id,
            identity.version_id
        );
        drop(operation);
    }

    #[gpui_kit::test]
    fn hiding_thread_retires_html_retained_only_by_the_previous_painted_frame(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        use crate::{
            test_support::{ThreadPorts, binding_router, install_thread_timeline},
            thread::{ThreadView, ThreadViewConfig},
        };
        use gpui_kit::{component::Root, prelude::*, *};
        use pioneer_client::{core::ClientCore, timeline::semantic::TopLevelPageMergeMode};
        use std::sync::Arc;
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "html-owner", "initial");
        client.apply_thread_timeline_page(serde_json::from_value(serde_json::json!({
            "workspaceId":"workspace", "threadId":"html-owner", "projectionVersion":1,
            "blocks":[{"workspaceId":"workspace","threadId":"html-owner","blockId":"answer","turnId":"turn","sortKey":"1","kind":{
                "kind":"assistant_message","itemId":"answer","text":"Final response","htmlPublications":[{
                    "workspaceId":"workspace","threadId":"html-owner","turnId":"turn","callId":"html-call","title":"Document",
                    "artifact":{"artifact_id":"document","version_id":"version","display_name":"Document","kind":"file","mime_type":"text/html","size_bytes":10,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","status":"ready"}
                },{
                    "workspaceId":"workspace","threadId":"html-owner","turnId":"turn","callId":"html-call-b","title":"Document B",
                    "artifact":{"artifact_id":"document-b","version_id":"version-b","display_name":"Document B","kind":"file","mime_type":"text/html","size_bytes":10,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","status":"ready"}
                }]
            }}], "page":{"hasMoreBefore":false,"hasMoreAfter":false}
        })).unwrap(), TopLevelPageMergeMode::Reset);
        let (registrar, deliver) = binding_router(client.clone());
        let (root, cx) = cx.add_window_view(|window, cx| {
            let thread = ThreadView::new(
                ThreadViewConfig::new(
                    client,
                    "html-owner".into(),
                    registrar,
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                ),
                window,
                cx,
            );
            Root::new(thread, window, cx)
        });
        deliver();
        cx.run_until_parked();
        for _ in 0..3 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        let thread = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadView>().unwrap()
        });
        let timeline = thread.read_with(cx, |thread, _| thread.timeline_for_test());
        let (row, html) = cx.update(|window, cx| {
            timeline.update(cx, |timeline, cx| {
                let prepared = timeline
                    .thread_timeline_view_state
                    .prepared
                    .as_ref()
                    .unwrap();
                let slot = prepared.slots[0].clone();
                let key = prepared.row_inputs[0].0.clone();
                let row = slot.view.as_ref().unwrap().clone();
                let presentation =
                    super::super::row_view::RowPresentation::new(slot, key, None, timeline, cx);
                let html = presentation.html[0].clone();
                let sibling = presentation.html[1].clone();
                row.update(cx, |row, cx| row.set_html_visible(true, cx));
                // No native browser is constructed: this fixture has no artifact capability.
                assert!(html.read(cx).visible);
                assert!(html.read(cx).webview.is_none());
                let viewport = Bounds::new(point(px(100.), px(0.)), size(px(768.), px(700.)));
                let row_origin = point(px(100.), px(-7000.));
                // Two documents belong to the same still-visible response. At B,
                // A is several viewports away; lifecycle Tasks model owned work
                // without constructing a native browser in this isolated fixture.
                for (view, offset, height, instance) in [
                    (&html, px(0.), px(4000.), 77),
                    (&sibling, px(4000.), px(5000.), 88),
                ] {
                    view.update(cx, |view, cx| {
                        view.source = Some("<p>verified source</p>".into());
                        view.height = height;
                        view.row_offset = Some(point(px(0.), offset));
                        view.bounds =
                            Bounds::new(row_origin + point(px(0.), offset), size(px(768.), height));
                        view.instance = instance;
                        view.messages = Some(
                            cx.spawn_in(window, async |_, _| std::future::pending::<()>().await),
                        );
                        view.download = Some(
                            cx.spawn_in(window, async |_, _| std::future::pending::<()>().await),
                        );
                    });
                }
                let identity = html.read(cx).publication.clone();
                let key = html.read(cx).key.clone();
                html.update(cx, |html, cx| {
                    assert!(!html.retain_nearby(html.bounds, viewport, cx));
                    html.permitted = true;
                    // Offscreen row preparation must not resurrect this far sibling.
                    html.prepare_nearby(px(768.), row_origin, viewport, true, window, cx);
                    assert_eq!(html.instance, 0);
                    assert!(
                        html.messages.is_none()
                            && html.download.is_none()
                            && html.webview.is_none()
                    );
                    assert_eq!(html.height, px(4000.));
                    assert_eq!(html.source.as_deref(), Some("<p>verified source</p>"));
                    assert_eq!(html.publication, identity);
                    assert_eq!(html.key, key);
                    assert!(
                        layout(
                            r#"{"instance":77,"width":768,"height":1}"#,
                            html.instance,
                            768.
                        )
                        .is_none()
                    );
                });
                sibling.update(cx, |sibling, cx| {
                    assert!(sibling.retain_nearby(sibling.bounds, viewport, cx));
                    assert_eq!(sibling.instance, 88);
                    assert!(sibling.messages.is_some() && sibling.download.is_some());
                    assert_eq!(sibling.height, px(5000.));
                });
                assert!(html.read(cx).visible && sibling.read(cx).visible);
                assert!(row.read(cx).owns_html(sibling.entity_id()));
                // Exercise geometry on the retained owner, not a copied Bounds.
                let sibling_identity = sibling.read(cx).publication.clone();
                let sibling_key = sibling.read(cx).key.clone();
                sibling.update(cx, |sibling, cx| {
                    for (top, height) in [
                        (540., 260.),         // 160px visible, 100px below the viewport.
                        (700., 260.),         // Below the viewport, still in the preparation zone.
                        (-260., 260.),        // Above the viewport, also still prepared.
                        (-50_000., 100_000.), // Only 700px of the full document visible.
                        (-99_999., 100_000.), // Only its last pixel visible.
                    ] {
                        sibling.height = px(height);
                        sibling.bounds =
                            Bounds::new(point(px(100.), px(top)), size(px(768.), sibling.height));
                        assert!(sibling.retain_nearby(sibling.bounds, viewport, cx));
                        assert_eq!(sibling.instance, 88);
                        assert!(sibling.messages.is_some() && sibling.download.is_some());
                        assert_eq!(sibling.height, px(height));
                        assert_eq!(sibling.bounds.size.height, px(height));
                        assert_eq!(sibling.source.as_deref(), Some("<p>verified source</p>"));
                    }
                    // A stale, shorter wrapper must not retire the still-visible
                    // tail of a long document during intrinsic-height reconciliation.
                    sibling.bounds =
                        Bounds::new(point(px(100.), px(-50_000.)), size(px(768.), px(700.)));
                    assert!(sibling.retain_nearby(sibling.bounds, viewport, cx));
                    assert_eq!(sibling.instance, 88);
                    assert!(sibling.messages.is_some() && sibling.download.is_some());
                    assert_eq!(sibling.height, px(100_000.));
                    // The entire document is now above the preparation zone (-700px).
                    sibling.bounds.origin.y = px(-100_701.);
                    assert!(!sibling.retain_nearby(sibling.bounds, viewport, cx));
                    assert_eq!(sibling.instance, 0);
                    assert!(sibling.messages.is_none() && sibling.download.is_none());
                    assert!(sibling.webview.is_none());
                    assert_eq!(sibling.height, px(100_000.));
                    assert_eq!(sibling.source.as_deref(), Some("<p>verified source</p>"));
                    assert_eq!(sibling.publication, sibling_identity);
                    assert_eq!(sibling.key, sibling_key);
                    assert!(
                        layout(
                            r#"{"instance":88,"width":768,"height":1}"#,
                            sibling.instance,
                            768.
                        )
                        .is_none()
                    );
                });
                // Returning A's full bounds to the zone re-admits it, without
                // losing verified source or changing the canonical response.
                html.update(cx, |html, cx| {
                    assert!(html.retain_nearby(
                        Bounds::new(viewport.origin, size(px(768.), html.height)),
                        viewport,
                        cx
                    ));
                    assert_eq!(html.height, px(4000.));
                    assert_eq!(html.publication, identity);
                });
                // A refreshed registry can omit an owner still held by the painted snapshot.
                timeline.row_registry = Default::default();
                (row, html)
            })
        });
        cx.update(|window, cx| {
            thread.update(cx, |thread, cx| thread.set_visible(false, window, cx));
            assert!(!html.read(cx).visible);
            assert!(
                timeline
                    .read(cx)
                    .thread_timeline_view_state
                    .prepared
                    .is_none()
            );
            // Late prepaint work from the retained row cannot resurrect a retired page.
            html.update(cx, |html, cx| {
                html.update_bounds(
                    Bounds::new(point(px(100.), px(80.)), size(px(768.), px(260.))),
                    window,
                    cx,
                )
            });
            assert_eq!(html.read(cx).instance, 0);
            assert!(html.read(cx).webview.is_none());
            assert!(html.read(cx).download.is_none());
            assert!(row.read(cx).owns_html(html.entity_id()));
        });
    }

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
