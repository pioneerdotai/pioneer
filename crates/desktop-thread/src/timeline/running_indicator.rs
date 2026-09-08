use super::TimelineRowTopSpacing;
use super::items::format_elapsed_ms;
use super::model::TimelineRow;
use super::model::TimelineRowKind;
use crate::screen::TimelineView;
use gpui_kit::ImageSource;
use gpui_kit::RenderImage;
use gpui_kit::component::StyledExt;
use gpui_kit::component::h_flex;
use gpui_kit::component::theme::ActiveTheme;
use gpui_kit::component::v_flex;
use gpui_kit::prelude::*;
use gpui_kit::*;
use image::AnimationDecoder as _;
use image::Rgba;
use image::codecs::webp::WebPDecoder;
use pioneer_client::security::ClientTurnSecuritySummary;
use pioneer_client::timeline::labels::RunningTurnDisplay;
use pioneer_client::timeline::labels::now_unix_ms;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::Duration;
use std::time::Instant;

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
    let bytes: &[u8] = match asset_path {
        "dino-light.webp" => include_bytes!("../../assets/dino-light.webp"),
        "dino-dark.webp" => include_bytes!("../../assets/dino-dark.webp"),
        _ => return Err(format!("embedded {asset_path} is missing")),
    };
    let mut decoder = WebPDecoder::new(Cursor::new(bytes))
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
    last_visible_at: Instant,
    viewport_visible: bool,
    clock_active: bool,
    clock_task: Option<gpui_kit::Task<()>>,
    suspended: bool,
    reduce_motion: bool,
}

impl RunningDinoView {
    fn new(assets_loader: Arc<RunningDinoAssetLoader>, active: bool) -> Self {
        Self {
            assets_loader,
            assets: None,
            asset_request_registered: false,
            frame_index: 0,
            last_visible_at: Instant::now(),
            viewport_visible: false,
            clock_active: false,
            clock_task: None,
            suspended: !active,
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
                                view.ensure_clock(cx);
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

    fn synchronize_motion(&mut self, cx: &mut Context<Self>) {
        let reduced = cx.reduce_motion();
        if self.reduce_motion == reduced {
            return;
        }
        self.reduce_motion = reduced;
        if reduced {
            self.clock_task.take();
            self.clock_active = false;
            self.frame_index = 0;
        } else {
            self.ensure_clock(cx);
        }
        cx.notify();
    }

    // The pinned App exposes motion changes as a window refresh, without an
    // observer. Deliver that changed platform input after paint; the handler
    // owns clock transitions. Equal refreshes do not schedule any work.
    fn motion_refresh_input(&self, cx: &Context<Self>) -> impl IntoElement {
        let previous = self.reduce_motion;
        let entity = cx.weak_entity();
        canvas(
            |_, _, _| (),
            move |_, _, window, cx| {
                if previous != cx.reduce_motion() {
                    window.defer(cx, move |_, cx| {
                        let _ = entity.update(cx, |view, cx| view.synchronize_motion(cx));
                    });
                }
            },
        )
        .absolute()
        .size_full()
    }

    fn ensure_clock(&mut self, cx: &mut Context<Self>) {
        if self.suspended
            || !self.viewport_visible
            || self.clock_active
            || self.reduce_motion
            || self.assets.is_none()
        {
            return;
        }
        self.clock_active = true;
        let first_delay = self
            .assets
            .as_ref()
            .map(|assets| assets.delay(self.frame_index))
            .unwrap_or(MIN_DINO_FRAME_DELAY);
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(
            record_animation_activity(
                pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningDinoClock,
                pioneer_client::timeline::diagnostics::DiagnosticAction::Scheduled,
                pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
            )
        );
        self.clock_task = Some(cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let mut delay = first_delay;
                loop {
                    cx.background_executor().timer(delay).await;
                    pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_animation_activity(
                        pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningDinoClock,
                        pioneer_client::timeline::diagnostics::DiagnosticAction::Woke,
                        pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                    ));
                    let next_delay = this
                        .update(&mut cx, |view, cx| {
                            if view.reduce_motion {
                                view.clock_active = false;
                                return None;
                            }
                            if view.suspended || (!view.viewport_visible && view.last_visible_at.elapsed() > DINO_OFFSCREEN_GRACE) {
                                view.clock_active = false;
                                // Visibility entry resumes the retained clock.
                                // This final notification preserves the existing
                                // grace-period completion cadence.
                                pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(
                                    record_animation_activity(
                                        pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningDinoClock,
                                        pioneer_client::timeline::diagnostics::DiagnosticAction::Requested,
                                        pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                                    )
                                );
                                cx.notify();
                                return None;
                            }
                            let assets = view.assets.as_ref()?;
                            view.frame_index = (view.frame_index + 1) % assets.frame_count();
                            let next_delay = assets.delay(view.frame_index);
                            pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(
                                record_animation_activity(
                                    pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningDinoClock,
                                    pioneer_client::timeline::diagnostics::DiagnosticAction::Requested,
                                    pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                                )
                            );
                            cx.notify();
                            Some(next_delay)
                        })
                        .ok()
                        .flatten();
                    let Some(next_delay) = next_delay else {
                        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_animation_activity(
                            pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningDinoClock,
                            pioneer_client::timeline::diagnostics::DiagnosticAction::Cancelled,
                            pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                        ));
                        break;
                    };
                    delay = next_delay;
                    pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_animation_activity(
                        pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningDinoClock,
                        pioneer_client::timeline::diagnostics::DiagnosticAction::Scheduled,
                        pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                    ));
                }
            }
        }));
    }
}

impl Render for RunningDinoView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_render(
            pioneer_client::timeline::diagnostics::RenderRegion::RunningDino
        ));

        let image = self.assets.as_ref().and_then(|assets| {
            let frames = assets.frames(cx.theme().mode.is_dark());
            frames
                .get(self.frame_index % frames.len())
                .map(|frame| ImageSource::Render(frame.image.clone()))
        });
        div()
            .w_full()
            .h_full()
            .relative()
            .child(self.motion_refresh_input(cx))
            .when_some(image, |this, image| {
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
    elapsed_ms: u64,
    started_at_unix_ms: i64,
    show_dino: bool,
    last_visible_at: Instant,
    viewport_visible: bool,
    clock_active: bool,
    clock_task: Option<gpui_kit::Task<()>>,
    suspended: bool,
}

impl RunningElapsedView {
    fn new(started_at_unix_ms: i64, show_dino: bool, active: bool) -> Self {
        Self {
            started_at_unix_ms,
            elapsed_ms: now_unix_ms().saturating_sub(started_at_unix_ms).max(0) as u64,
            show_dino,
            last_visible_at: Instant::now(),
            viewport_visible: false,
            clock_active: false,
            clock_task: None,
            suspended: !active,
        }
    }

    fn ensure_clock(&mut self, cx: &mut Context<Self>) {
        if self.suspended || !self.viewport_visible || self.clock_active {
            return;
        }
        let elapsed_ms = now_unix_ms().saturating_sub(self.started_at_unix_ms).max(0) as u64;
        if self.elapsed_ms != elapsed_ms {
            self.elapsed_ms = elapsed_ms;
            cx.notify();
        }
        self.clock_active = true;
        let started_at_unix_ms = self.started_at_unix_ms;
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_animation_activity(
            pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningElapsedClock,
            pioneer_client::timeline::diagnostics::DiagnosticAction::Scheduled,
            pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
        ));
        self.clock_task = Some(cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                loop {
                    let delay = next_elapsed_tick_delay(started_at_unix_ms, now_unix_ms());
                    cx.background_executor().timer(delay).await;
                    pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_animation_activity(
                        pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningElapsedClock,
                        pioneer_client::timeline::diagnostics::DiagnosticAction::Woke,
                        pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                    ));
                    let keep_running = this
                        .update(&mut cx, |view, cx| {
                            view.elapsed_ms = now_unix_ms().saturating_sub(view.started_at_unix_ms).max(0) as u64;
                            if view.suspended || (!view.viewport_visible && view.last_visible_at.elapsed() > ELAPSED_OFFSCREEN_GRACE) {
                                view.clock_active = false;
                                // Distinguish a temporarily stalled UI from an
                                // offscreen view without polling forever. A
                                // mounted view renders once and restarts on the
                                // next absolute-second boundary.
                                pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(
                                    record_animation_activity(
                                        pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningElapsedClock,
                                        pioneer_client::timeline::diagnostics::DiagnosticAction::Requested,
                                        pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                                    )
                                );
                                cx.notify();
                                return false;
                            }
                            pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(
                                record_animation_activity(
                                    pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningElapsedClock,
                                    pioneer_client::timeline::diagnostics::DiagnosticAction::Requested,
                                    pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                                )
                            );
                            cx.notify();
                            true
                        })
                        .unwrap_or(false);
                    if !keep_running {
                        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_animation_activity(
                            pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningElapsedClock,
                            pioneer_client::timeline::diagnostics::DiagnosticAction::Cancelled,
                            pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                        ));
                        break;
                    }
                    pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_animation_activity(
                        pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningElapsedClock,
                        pioneer_client::timeline::diagnostics::DiagnosticAction::Scheduled,
                        pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
                    ));
                }
            }
        }));
    }
}

impl Render for RunningElapsedView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_render(
            pioneer_client::timeline::diagnostics::RenderRegion::RunningElapsed
        ));
        let elapsed_ms = self.elapsed_ms;
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
    active: bool,
    assets_loader: Arc<RunningDinoAssetLoader>,
    dino: HashMap<String, CachedIndicatorView<RunningDinoView>>,
    elapsed: HashMap<String, RunningElapsedViewEntry>,
}

impl Default for RunningIndicatorViewCache {
    fn default() -> Self {
        Self {
            active: true,
            assets_loader: Arc::default(),
            dino: HashMap::new(),
            elapsed: HashMap::new(),
        }
    }
}

impl RunningIndicatorViewCache {
    pub(crate) fn set_visible_activities(
        &mut self,
        activities: &std::collections::HashSet<String>,
        cx: &mut App,
    ) {
        let now = Instant::now();
        for (id, entry) in &self.dino {
            let visible = activities.contains(id);
            entry.view.update(cx, |view, cx| {
                if view.viewport_visible != visible {
                    view.last_visible_at = now;
                }
                view.viewport_visible = visible;
                view.synchronize_motion(cx);
                if visible {
                    view.ensure_assets(cx);
                    view.ensure_clock(cx);
                }
            });
        }
        for (id, entry) in &self.elapsed {
            let visible = activities.contains(id);
            entry.cached.view.update(cx, |view, cx| {
                if view.viewport_visible != visible {
                    view.last_visible_at = now;
                }
                view.viewport_visible = visible;
                if visible {
                    view.ensure_clock(cx);
                }
            });
        }
    }
    pub(crate) fn set_active(&mut self, active: bool, cx: &mut App) {
        self.active = active;
        for entry in self.dino.values() {
            entry.view.update(cx, |view, cx| {
                if view.suspended == !active {
                    return;
                }
                view.suspended = !active;
                if !active {
                    view.clock_task.take();
                    view.clock_active = false;
                } else {
                    view.ensure_clock(cx);
                    cx.notify();
                }
            });
        }
        for entry in self.elapsed.values() {
            entry.cached.view.update(cx, |view, cx| {
                if view.suspended == !active {
                    return;
                }
                view.suspended = !active;
                if !active {
                    view.clock_task.take();
                    view.clock_active = false;
                } else {
                    view.ensure_clock(cx);
                    cx.notify();
                }
            });
        }
    }

    fn prune(&mut self, now: Instant) {
        self.dino
            .retain(|_, entry| now.duration_since(entry.last_used) <= INDICATOR_CACHE_TTL);
        self.elapsed
            .retain(|_, entry| now.duration_since(entry.cached.last_used) <= INDICATOR_CACHE_TTL);
    }
}

impl TimelineView {
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

    pub(super) fn prepare_running_dino(
        &self,
        activity_id: String,
        cx: &mut Context<Self>,
    ) -> Entity<RunningDinoView> {
        let now = Instant::now();
        let mut cache = self.running_indicator_views.borrow_mut();
        cache.prune(now);
        if let Some(entry) = cache.dino.get_mut(&activity_id) {
            entry.last_used = now;
            entry.view.update(cx, |view, cx| {
                view.ensure_assets(cx);
                view.ensure_clock(cx);
            });
            return entry.view.clone();
        }

        let assets_loader = cache.assets_loader.clone();
        let view = cx.new(|cx| {
            let mut view = RunningDinoView::new(assets_loader, cache.active);
            view.reduce_motion = cx.reduce_motion();
            view.ensure_assets(cx);
            view.ensure_clock(cx);
            view
        });
        cache.dino.insert(
            activity_id,
            CachedIndicatorView {
                view: view.clone(),
                last_used: now,
            },
        );
        view
    }

    pub(super) fn prepare_running_elapsed(
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

        let view = cx.new(|cx| {
            let mut view = RunningElapsedView::new(started_at_unix_ms, show_dino, cache.active);
            view.ensure_clock(cx);
            view
        });
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

    pub(super) fn running_turn_dino_view(
        &self,
        activity_id: String,
        _cx: &mut Context<Self>,
    ) -> Option<Entity<RunningDinoView>> {
        self.running_indicator_views
            .borrow()
            .dino
            .get(&activity_id)
            .map(|entry| entry.view.clone())
    }
    fn running_elapsed_view(
        &self,
        activity_id: String,
        _started_at_unix_ms: i64,
        _show_dino: bool,
        _cx: &mut Context<Self>,
    ) -> Option<Entity<RunningElapsedView>> {
        self.running_indicator_views
            .borrow()
            .elapsed
            .get(&activity_id)
            .map(|entry| entry.cached.view.clone())
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
        state: Option<pioneer_client::timeline::types::TurnWorkState>,
        security_summary: Option<&ClientTurnSecuritySummary>,
        show_dino: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_render(
            pioneer_client::timeline::diagnostics::RenderRegion::RunningActivity
        ));
        let started_at = started_at_unix_ms.unwrap_or(0);
        let dino = show_dino
            .then(|| self.running_turn_dino_view(format!("content:{activity_id}"), cx))
            .flatten();
        let elapsed = self.running_elapsed_view(activity_id, started_at, show_dino, cx);
        let status_label = match state {
            Some(pioneer_client::timeline::types::TurnWorkState::Starting) => {
                t!("timeline.task.status.queued").to_string()
            }
            Some(pioneer_client::timeline::types::TurnWorkState::WaitingForApproval) => {
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
            .children(elapsed)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::decode_running_dino_assets;
    use super::next_elapsed_tick_delay;
    use std::time::Duration;

    #[gpui_kit::test]
    fn warm_route_retains_clock_owner_but_cancels_work_until_remount(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        use super::{
            CachedIndicatorView, RunningElapsedView, RunningElapsedViewEntry,
            RunningIndicatorViewCache,
        };
        use gpui_kit::AppContext;
        let weak = cx.update(|cx| {
            let mut cache = RunningIndicatorViewCache::default();
            cache.set_active(false, cx);
            let inactive = cx.new(|cx| {
                let mut view = RunningElapsedView::new(1_000, false, cache.active);
                view.ensure_clock(cx);
                view
            });
            assert!(inactive.read(cx).clock_task.is_none());
            assert!(inactive.read(cx).suspended);

            let view = cx.new(|cx| {
                let mut view = RunningElapsedView::new(1_000, false, true);
                view.ensure_clock(cx);
                view
            });
            let identity = view.entity_id();
            let weak = view.downgrade();
            cache.elapsed.insert(
                "thread/turn".into(),
                RunningElapsedViewEntry {
                    started_at_unix_ms: 1_000,
                    show_dino: false,
                    cached: CachedIndicatorView {
                        view,
                        last_used: std::time::Instant::now(),
                    },
                },
            );
            cache.set_active(false, cx);
            let retained = &cache.elapsed["thread/turn"].cached.view;
            assert_eq!(retained.entity_id(), identity);
            assert!(retained.read(cx).suspended);
            assert!(retained.read(cx).clock_task.is_none());
            retained.update(cx, |view, cx| view.ensure_clock(cx));
            assert!(retained.read(cx).clock_task.is_none());
            retained.update(cx, |view, _| view.elapsed_ms = 0);
            cache.set_active(true, cx);
            cache.set_visible_activities(
                &std::collections::HashSet::from(["thread/turn".into()]),
                cx,
            );
            let retained = &cache.elapsed["thread/turn"].cached.view;
            assert_eq!(retained.entity_id(), identity);
            retained.update(cx, |view, cx| view.ensure_clock(cx));
            assert!(retained.read(cx).clock_task.is_some());
            assert!(
                retained.read(cx).elapsed_ms > 0,
                "remount refreshes elapsed before the next tick"
            );
            drop(cache);
            weak
        });
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
    }

    #[gpui_kit::test]
    fn window_motion_refresh_stops_and_resumes_clock_without_viewport_input(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        use super::*;
        cx.update(gpui_kit::init);
        let assets = Arc::new(decode_running_dino_assets().unwrap());
        let (view, cx) = cx.add_window_view(|_, cx| {
            let mut view = RunningDinoView::new(Arc::default(), true);
            view.assets = Some(assets);
            view.viewport_visible = true;
            view.ensure_clock(cx);
            view
        });
        assert!(view.read_with(cx, |view, _| view.clock_active));
        cx.update(|window, cx| {
            cx.set_reduce_motion(true);
            // Render remains a pure read, even with a changed platform flag.
            view.update(cx, |view, cx| {
                drop(view.render(window, cx));
                assert!(!view.reduce_motion);
                assert!(view.clock_active);
            });
            window.draw(cx).clear(cx);
        });
        cx.run_until_parked();
        view.read_with(cx, |view, _| {
            assert!(view.reduce_motion);
            assert!(!view.clock_active);
            assert!(view.clock_task.is_none());
            assert_eq!(view.frame_index, 0);
        });
        cx.update(|window, cx| {
            cx.set_reduce_motion(false);
            window.draw(cx).clear(cx);
        });
        cx.run_until_parked();
        view.read_with(cx, |view, _| {
            assert!(!view.reduce_motion);
            assert!(view.clock_active);
            assert!(view.clock_task.is_some());
        });
        let notifications = std::rc::Rc::new(std::cell::Cell::new(0));
        let _subscription = cx.update(|_, cx| {
            let notifications = notifications.clone();
            cx.observe(&view, move |_, _| {
                notifications.set(notifications.get() + 1)
            })
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        assert_eq!(notifications.get(), 0, "equal motion refresh is quiet");
    }

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
