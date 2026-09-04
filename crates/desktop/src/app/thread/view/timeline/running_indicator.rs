use super::TimelineRowTopSpacing;
use super::items::format_elapsed_ms;
use super::model::{TimelineRow, TimelineRowKind};
use crate::app::root::PioneerDesktop;
use crate::assets::PioneerAssetsSource;
use gpui_kit::component::{StyledExt, h_flex, theme::ActiveTheme, v_flex};
use gpui_kit::{AssetSource as _, ImageSource, RenderImage, prelude::*, *};
use image::{AnimationDecoder as _, Rgba, codecs::webp::WebPDecoder};
use pioneer_client::security::ClientTurnSecuritySummary;
use pioneer_client::timeline::labels::{RunningTurnDisplay, now_unix_ms};
use std::{
    collections::HashMap,
    io::Cursor,
    rc::Rc,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

const MIN_DINO_FRAME_DELAY: Duration = Duration::from_millis(16);
const INDICATOR_CACHE_TTL: Duration = Duration::from_secs(60);
const DINO_OFFSCREEN_GRACE: Duration = Duration::from_secs(2);
const ELAPSED_OFFSCREEN_GRACE: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct RunningDinoFrame {
    image: Arc<RenderImage>,
    delay: Duration,
}

struct RunningDinoAssets {
    light: Vec<RunningDinoFrame>,
    dark: Vec<RunningDinoFrame>,
}

impl RunningDinoAssets {
    fn frames(&self, is_dark: bool) -> &[RunningDinoFrame] {
        if is_dark { &self.dark } else { &self.light }
    }

    fn delay(&self, frame_index: usize) -> Duration {
        self.light[frame_index % self.light.len()].delay
    }

    fn frame_count(&self) -> usize {
        self.light.len()
    }
}

enum RunningDinoAssetState {
    Unloaded,
    Loading(Vec<WeakEntity<RunningDinoView>>),
    Ready(Arc<RunningDinoAssets>),
    Failed,
}

struct RunningDinoAssetLoader {
    state: Mutex<RunningDinoAssetState>,
}

impl Default for RunningDinoAssetLoader {
    fn default() -> Self {
        Self {
            state: Mutex::new(RunningDinoAssetState::Unloaded),
        }
    }
}

enum RunningDinoAssetRegistration {
    StartLoading,
    Waiting,
    Ready(Arc<RunningDinoAssets>),
    Failed,
}

impl RunningDinoAssetLoader {
    fn lock(&self) -> MutexGuard<'_, RunningDinoAssetState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn register(&self, waiter: WeakEntity<RunningDinoView>) -> RunningDinoAssetRegistration {
        let mut state = self.lock();
        match &mut *state {
            RunningDinoAssetState::Unloaded => {
                *state = RunningDinoAssetState::Loading(vec![waiter]);
                RunningDinoAssetRegistration::StartLoading
            }
            RunningDinoAssetState::Loading(waiters) => {
                waiters.push(waiter);
                RunningDinoAssetRegistration::Waiting
            }
            RunningDinoAssetState::Ready(assets) => {
                RunningDinoAssetRegistration::Ready(assets.clone())
            }
            RunningDinoAssetState::Failed => RunningDinoAssetRegistration::Failed,
        }
    }

    fn finish(
        &self,
        result: std::result::Result<RunningDinoAssets, String>,
    ) -> (
        Option<Arc<RunningDinoAssets>>,
        Vec<WeakEntity<RunningDinoView>>,
    ) {
        let mut state = self.lock();
        let waiters = match std::mem::replace(&mut *state, RunningDinoAssetState::Failed) {
            RunningDinoAssetState::Loading(waiters) => waiters,
            other => {
                *state = other;
                return (None, Vec::new());
            }
        };
        match result {
            Ok(assets) => {
                let assets = Arc::new(assets);
                *state = RunningDinoAssetState::Ready(assets.clone());
                (Some(assets), waiters)
            }
            Err(error) => {
                tracing::error!(error, "failed to preload running indicator animation");
                (None, waiters)
            }
        }
    }
}

fn decode_running_dino_frames(
    asset_path: &str,
) -> std::result::Result<Vec<RunningDinoFrame>, String> {
    let bytes = PioneerAssetsSource
        .load(asset_path)
        .map_err(|error| format!("failed to load embedded {asset_path}: {error:#}"))?
        .ok_or_else(|| format!("embedded {asset_path} is missing"))?;
    let mut decoder = WebPDecoder::new(Cursor::new(bytes.as_ref()))
        .map_err(|error| format!("failed to decode embedded {asset_path}: {error:#}"))?;
    let _ = decoder.set_background_color(Rgba([0, 0, 0, 0]));
    let frames = decoder
        .into_frames()
        .collect_frames()
        .map_err(|error| format!("failed to decode frames from {asset_path}: {error:#}"))?;
    if frames.is_empty() {
        return Err(format!(
            "embedded running indicator {asset_path} has no frames"
        ));
    }

    Ok(frames
        .into_iter()
        .map(|mut frame| {
            let delay = Duration::from(frame.delay()).max(MIN_DINO_FRAME_DELAY);
            for pixel in frame.buffer_mut().chunks_exact_mut(4) {
                pixel.swap(0, 2);
            }
            RunningDinoFrame {
                image: Arc::new(RenderImage::new(vec![frame])),
                delay,
            }
        })
        .collect())
}

fn decode_running_dino_assets() -> std::result::Result<RunningDinoAssets, String> {
    let light = decode_running_dino_frames("dino-light.webp")?;
    let dark = decode_running_dino_frames("dino-dark.webp")?;
    if light.len() != dark.len() {
        return Err("light and dark running indicators have different frame counts".to_owned());
    }
    if light
        .iter()
        .zip(&dark)
        .any(|(light, dark)| light.delay != dark.delay)
    {
        return Err("light and dark running indicators have different frame timing".to_owned());
    }
    Ok(RunningDinoAssets { light, dark })
}

pub(crate) struct RunningDinoView {
    assets_loader: Arc<RunningDinoAssetLoader>,
    assets: Option<Arc<RunningDinoAssets>>,
    asset_request_registered: bool,
    frame_index: usize,
    last_rendered_at: Instant,
    clock_active: bool,
    reduce_motion: bool,
}

impl RunningDinoView {
    fn new(assets_loader: Arc<RunningDinoAssetLoader>) -> Self {
        Self {
            assets_loader,
            assets: None,
            asset_request_registered: false,
            frame_index: 0,
            last_rendered_at: Instant::now(),
            clock_active: false,
            reduce_motion: false,
        }
    }

    fn ensure_assets(&mut self, cx: &mut Context<Self>) {
        if self.assets.is_some() || self.asset_request_registered {
            return;
        }
        match self.assets_loader.register(cx.weak_entity()) {
            RunningDinoAssetRegistration::Ready(assets) => {
                self.assets = Some(assets);
                self.asset_request_registered = true;
            }
            RunningDinoAssetRegistration::StartLoading => {
                self.asset_request_registered = true;
                let loader = self.assets_loader.clone();
                cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
                    let mut cx = cx.clone();
                    async move {
                        let decoded = cx
                            .background_executor()
                            .spawn(async move { decode_running_dino_assets() })
                            .await;
                        let (assets, waiters) = loader.finish(decoded);
                        for waiter in waiters {
                            let assets = assets.clone();
                            let _ = waiter.update(&mut cx, |view, cx| {
                                view.assets = assets;
                                cx.notify();
                            });
                        }
                    }
                })
                .detach();
            }
            RunningDinoAssetRegistration::Waiting | RunningDinoAssetRegistration::Failed => {
                self.asset_request_registered = true;
            }
        }
    }

    fn ensure_clock(&mut self, cx: &mut Context<Self>) {
        if self.clock_active || self.reduce_motion || self.assets.is_none() {
            return;
        }
        self.clock_active = true;
        let first_delay = self
            .assets
            .as_ref()
            .map(|assets| assets.delay(self.frame_index))
            .unwrap_or(MIN_DINO_FRAME_DELAY);
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let mut delay = first_delay;
                loop {
                    cx.background_executor().timer(delay).await;
                    let next_delay = this
                        .update(&mut cx, |view, cx| {
                            if view.reduce_motion {
                                view.clock_active = false;
                                return None;
                            }
                            if view.last_rendered_at.elapsed() > DINO_OFFSCREEN_GRACE {
                                view.clock_active = false;
                                // If the UI thread was merely busy, this final
                                // notification renders the still-mounted view
                                // and restarts its clock. An actually unmounted
                                // entity remains quiet after this one wake-up.
                                cx.notify();
                                return None;
                            }
                            let assets = view.assets.as_ref()?;
                            view.frame_index = (view.frame_index + 1) % assets.frame_count();
                            let next_delay = assets.delay(view.frame_index);
                            cx.notify();
                            Some(next_delay)
                        })
                        .ok()
                        .flatten();
                    let Some(next_delay) = next_delay else {
                        break;
                    };
                    delay = next_delay;
                }
            }
        })
        .detach();
    }
}

impl Render for RunningDinoView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.last_rendered_at = Instant::now();
        self.reduce_motion = cx.reduce_motion();
        if self.reduce_motion {
            self.frame_index = 0;
        }
        self.ensure_assets(cx);
        self.ensure_clock(cx);

        let image = self.assets.as_ref().and_then(|assets| {
            let frames = assets.frames(cx.theme().mode.is_dark());
            frames
                .get(self.frame_index % frames.len())
                .map(|frame| ImageSource::Render(frame.image.clone()))
        });
        div().w_full().h_full().when_some(image, |this, image| {
            this.child(
                img(image)
                    .id("running-turn-dino-static-frame")
                    .w_full()
                    .h_full()
                    .object_fit(ObjectFit::Contain),
            )
        })
    }
}

pub(crate) struct RunningElapsedView {
    started_at_unix_ms: i64,
    show_dino: bool,
    last_rendered_at: Instant,
    clock_active: bool,
}

impl RunningElapsedView {
    fn new(started_at_unix_ms: i64, show_dino: bool) -> Self {
        Self {
            started_at_unix_ms,
            show_dino,
            last_rendered_at: Instant::now(),
            clock_active: false,
        }
    }

    fn ensure_clock(&mut self, cx: &mut Context<Self>) {
        if self.clock_active {
            return;
        }
        self.clock_active = true;
        let started_at_unix_ms = self.started_at_unix_ms;
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                loop {
                    let delay = next_elapsed_tick_delay(started_at_unix_ms, now_unix_ms());
                    cx.background_executor().timer(delay).await;
                    let keep_running = this
                        .update(&mut cx, |view, cx| {
                            if view.last_rendered_at.elapsed() > ELAPSED_OFFSCREEN_GRACE {
                                view.clock_active = false;
                                // Distinguish a temporarily stalled UI from an
                                // offscreen view without polling forever. A
                                // mounted view renders once and restarts on the
                                // next absolute-second boundary.
                                cx.notify();
                                return false;
                            }
                            cx.notify();
                            true
                        })
                        .unwrap_or(false);
                    if !keep_running {
                        break;
                    }
                }
            }
        })
        .detach();
    }
}

impl Render for RunningElapsedView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.last_rendered_at = Instant::now();
        self.ensure_clock(cx);
        let elapsed_ms = now_unix_ms().saturating_sub(self.started_at_unix_ms).max(0) as u64;
        let elapsed = if elapsed_ms >= 1_000 {
            format_elapsed_ms(elapsed_ms)
        } else {
            String::new()
        };

        div()
            .id("running-activity-elapsed")
            .pt_1()
            .when(!self.show_dino, |this| this.pt_0().mb(px(2.)))
            .font_semibold()
            .child(elapsed)
    }
}

fn next_elapsed_tick_delay(started_at_unix_ms: i64, now_unix_ms: i64) -> Duration {
    let elapsed_ms = now_unix_ms.saturating_sub(started_at_unix_ms).max(0);
    let until_next_second = 1_000_i64.saturating_sub(elapsed_ms.rem_euclid(1_000));
    Duration::from_millis(u64::try_from(until_next_second.max(1)).unwrap_or(1_000))
}

struct CachedIndicatorView<T> {
    view: Entity<T>,
    last_used: Instant,
}

struct RunningElapsedViewEntry {
    started_at_unix_ms: i64,
    show_dino: bool,
    cached: CachedIndicatorView<RunningElapsedView>,
}

pub(crate) struct RunningIndicatorViewCache {
    assets_loader: Arc<RunningDinoAssetLoader>,
    dino: HashMap<String, CachedIndicatorView<RunningDinoView>>,
    elapsed: HashMap<String, RunningElapsedViewEntry>,
}

impl Default for RunningIndicatorViewCache {
    fn default() -> Self {
        Self {
            assets_loader: Arc::default(),
            dino: HashMap::new(),
            elapsed: HashMap::new(),
        }
    }
}

impl RunningIndicatorViewCache {
    fn prune(&mut self, now: Instant) {
        self.dino
            .retain(|_, entry| now.duration_since(entry.last_used) <= INDICATOR_CACHE_TTL);
        self.elapsed
            .retain(|_, entry| now.duration_since(entry.cached.last_used) <= INDICATOR_CACHE_TTL);
    }
}

impl PioneerDesktop {
    pub(super) fn semantic_timeline_has_running_turn_row(&self) -> bool {
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);
        let model = self.semantic_timeline_render_model(active_thread_id.as_deref());
        model.rows.iter().any(|row| {
            matches!(
                row,
                super::TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::RunningTurn(_),
                    ..
                })
            )
        })
    }

    pub(super) fn hydrate_running_turn_rows(
        &self,
        rows: Rc<Vec<TimelineRow>>,
    ) -> Rc<Vec<TimelineRow>> {
        let Some((running_row_index, running_turn)) =
            rows.iter().enumerate().find_map(|(index, row)| {
                if let TimelineRowKind::RunningTurn(running_turn) = &row.kind {
                    Some((index, running_turn))
                } else {
                    None
                }
            })
        else {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            state.running_turn_indicator_fallback_turn_id = None;
            state.running_turn_indicator_fallback_started_at_unix_ms = None;
            return rows;
        };

        let now = now_unix_ms();
        let started_at = {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            let started_at = if let Some(started_at) = running_turn.started_at_unix_ms {
                state.running_turn_indicator_fallback_turn_id = Some(running_turn.turn_id.clone());
                state.running_turn_indicator_fallback_started_at_unix_ms = Some(started_at);
                started_at
            } else {
                if state.running_turn_indicator_fallback_turn_id.as_deref()
                    != Some(running_turn.turn_id.as_str())
                {
                    state.running_turn_indicator_fallback_turn_id =
                        Some(running_turn.turn_id.clone());
                    state.running_turn_indicator_fallback_started_at_unix_ms = Some(now);
                }

                state
                    .running_turn_indicator_fallback_started_at_unix_ms
                    .unwrap_or(now)
            };

            started_at
        };

        if running_turn.started_at_unix_ms == Some(started_at) {
            return rows;
        }

        let mut hydrated_rows = rows.as_ref().clone();
        if let Some(row) = hydrated_rows.get_mut(running_row_index)
            && let TimelineRowKind::RunningTurn(running_turn) = &mut row.kind
        {
            running_turn.started_at_unix_ms = Some(started_at);
        }

        Rc::new(hydrated_rows)
    }

    pub(super) fn running_turn_dino_view(
        &self,
        activity_id: String,
        cx: &mut Context<Self>,
    ) -> Entity<RunningDinoView> {
        let now = Instant::now();
        let mut cache = self.running_indicator_views.borrow_mut();
        cache.prune(now);
        if let Some(entry) = cache.dino.get_mut(&activity_id) {
            entry.last_used = now;
            return entry.view.clone();
        }

        let assets_loader = cache.assets_loader.clone();
        let view = cx.new(|_| RunningDinoView::new(assets_loader));
        cache.dino.insert(
            activity_id,
            CachedIndicatorView {
                view: view.clone(),
                last_used: now,
            },
        );
        view
    }

    fn running_elapsed_view(
        &self,
        activity_id: String,
        started_at_unix_ms: i64,
        show_dino: bool,
        cx: &mut Context<Self>,
    ) -> Entity<RunningElapsedView> {
        let now = Instant::now();
        let mut cache = self.running_indicator_views.borrow_mut();
        cache.prune(now);
        if let Some(entry) = cache.elapsed.get_mut(&activity_id)
            && entry.started_at_unix_ms == started_at_unix_ms
            && entry.show_dino == show_dino
        {
            entry.cached.last_used = now;
            return entry.cached.view.clone();
        }

        let view = cx.new(|_| RunningElapsedView::new(started_at_unix_ms, show_dino));
        cache.elapsed.insert(
            activity_id,
            RunningElapsedViewEntry {
                started_at_unix_ms,
                show_dino,
                cached: CachedIndicatorView {
                    view: view.clone(),
                    last_used: now,
                },
            },
        );
        view
    }

    pub(super) fn render_running_turn_row(
        &self,
        running_turn: &RunningTurnDisplay,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let content = self.render_running_activity_content(
            format!("turn:{}", running_turn.turn_id),
            running_turn.started_at_unix_ms,
            running_turn.state.clone(),
            running_turn.security_summary.as_ref(),
            self.active_task_thread_navigation().is_none(),
            cx,
        );

        self.render_item_row(
            top_spacing,
            is_last_row,
            content_width,
            div().w_full().pt_5().child(content).into_any_element(),
        )
    }

    pub(super) fn render_running_activity_content(
        &self,
        activity_id: String,
        started_at_unix_ms: Option<i64>,
        state: Option<pioneer_protocol::TurnWorkState>,
        security_summary: Option<&ClientTurnSecuritySummary>,
        show_dino: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let started_at = started_at_unix_ms.unwrap_or_else(now_unix_ms);
        let dino =
            show_dino.then(|| self.running_turn_dino_view(format!("content:{activity_id}"), cx));
        let elapsed = self.running_elapsed_view(activity_id, started_at, show_dino, cx);
        let status_label = match state {
            Some(pioneer_protocol::TurnWorkState::Starting) => {
                t!("timeline.task.status.queued").to_string()
            }
            Some(pioneer_protocol::TurnWorkState::WaitingForApproval) => {
                t!("timeline.task.status.waiting").to_string()
            }
            _ => t!("timeline.running.turn").to_string(),
        };

        h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .gap_4()
            .text_sm()
            .child(
                h_flex()
                    .items_center()
                    .gap_2()
                    .when_some(dino, |this, dino| this.child(div().size_8().child(dino)))
                    .child(
                        v_flex()
                            .pt_1()
                            .gap_1()
                            .when(!show_dino, |this| this.pt_0().mb(px(2.)))
                            .child(div().font_semibold().child(status_label))
                            .when_some(security_summary, |this, summary| {
                                this.child(self.render_turn_security_summary(summary, cx))
                            }),
                    ),
            )
            .child(elapsed)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_running_dino_assets, next_elapsed_tick_delay};
    use std::time::Duration;

    #[test]
    fn running_dino_is_split_into_static_frames_at_embedded_cadence() {
        let assets = decode_running_dino_assets().expect("embedded animation should decode");
        assert!(assets.frame_count() > 1);
        assert_eq!(assets.dark.len(), assets.light.len());
        for (light, dark) in assets.light.iter().zip(&assets.dark) {
            assert_eq!(light.image.frame_count(), 1);
            assert_eq!(dark.image.frame_count(), 1);
            assert_eq!(light.delay, dark.delay);
        }
    }

    #[test]
    fn elapsed_clock_aligns_to_absolute_seconds_without_drift() {
        assert_eq!(
            next_elapsed_tick_delay(1_000, 1_000),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_elapsed_tick_delay(1_000, 1_250),
            Duration::from_millis(750)
        );
        assert_eq!(
            next_elapsed_tick_delay(1_000, 6_999),
            Duration::from_millis(1)
        );
        assert_eq!(
            next_elapsed_tick_delay(10_000, 9_000),
            Duration::from_secs(1)
        );
    }
}
