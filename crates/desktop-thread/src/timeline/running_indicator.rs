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

// The indicator is 32 logical pixels; retain enough pixels for a 4x display.
const DINO_FRAME_MAX_PIXELS: u32 = 128;
const MIN_DINO_FRAME_DELAY: Duration = Duration::from_millis(16);

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

pub(super) struct RunningDinoAssetLoader {
    state: Mutex<RunningDinoAssetState>,
}

// RenderImage IDs also identify textures in each window's sprite atlas. Keeping
// one bounded set across thread mounts prevents abandoned IDs accumulating there.
#[derive(Default)]
struct SharedRunningDinoAssets(Arc<RunningDinoAssetLoader>);
impl Global for SharedRunningDinoAssets {}

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
        .map(|frame| {
            let delay = Duration::from(frame.delay()).max(MIN_DINO_FRAME_DELAY);
            let resized = image::DynamicImage::ImageRgba8(frame.into_buffer())
                .resize(
                    DINO_FRAME_MAX_PIXELS,
                    DINO_FRAME_MAX_PIXELS,
                    image::imageops::FilterType::Lanczos3,
                )
                .into_rgba8();
            let mut frame = image::Frame::from_parts(
                resized,
                0,
                0,
                image::Delay::from_saturating_duration(delay),
            );
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
    generation: u64,
    viewport_visible: bool,
    clock_active: bool,
    clock_task: Option<gpui_kit::Task<()>>,
    suspended: bool,
    reduce_motion: bool,
}

impl RunningDinoView {
    pub(super) fn new(assets_loader: Arc<RunningDinoAssetLoader>, active: bool) -> Self {
        Self {
            assets_loader,
            assets: None,
            asset_request_registered: false,
            frame_index: 0,
            generation: 0,
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
            self.generation += 1;
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
        let generation = self.generation;
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
                            if view.generation != generation {
                                return None;
                            }
                            if view.reduce_motion || view.suspended || !view.viewport_visible {
                                view.clock_active = false;
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

impl RunningDinoView {
    pub(super) fn set_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        let changed = self.viewport_visible != visible;
        self.viewport_visible = visible;
        self.suspended = !visible;
        if !visible {
            if changed {
                self.generation += 1;
            }
            self.clock_task.take();
            self.clock_active = false;
            self.frame_index = 0;
        } else {
            self.synchronize_motion(cx);
            self.ensure_assets(cx);
            self.ensure_clock(cx);
        }
        if changed {
            cx.notify();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ElapsedPlacement {
    Inline,
    Running { show_dino: bool },
}

/// A mounted wall-clock sample advanced by monotonic time, with a floor across suspension.
pub(super) struct ElapsedAnchor {
    sampled_elapsed_ms: i128,
    mounted_at: Instant,
    floor_ms: u64,
}
impl ElapsedAnchor {
    fn new(start: i64, wall: i64, now: Instant, floor_ms: u64) -> Self {
        Self {
            sampled_elapsed_ms: wall as i128 - start as i128,
            mounted_at: now,
            floor_ms,
        }
    }
    fn elapsed(&self, now: Instant) -> u64 {
        let elapsed = (self.sampled_elapsed_ms
            + now
                .saturating_duration_since(self.mounted_at)
                .as_millis()
                .min(i128::MAX as u128) as i128)
            .clamp(0, u64::MAX as i128) as u64;
        elapsed.max(self.floor_ms)
    }
}
fn elapsed_delay(elapsed: u64) -> Duration {
    Duration::from_millis(1_000 - elapsed % 1_000)
}

pub(crate) struct RunningElapsedView {
    started_at: i64,
    placement: ElapsedPlacement,
    anchor: Option<ElapsedAnchor>,
    floor_ms: u64,
    displayed_seconds: u64,
    task: Option<Task<()>>,
    generation: u64,
}
macro_rules! record_elapsed_clock {
    ($action:ident) => {
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_animation_activity(
            pioneer_client::timeline::diagnostics::AnimationSourceId::TimelineRunningElapsedClock,
            pioneer_client::timeline::diagnostics::DiagnosticAction::$action,
            pioneer_client::timeline::diagnostics::Visibility::NotApplicable,
        ));
    };
}

impl RunningElapsedView {
    pub(super) fn new(started_at: i64, placement: ElapsedPlacement) -> Self {
        Self {
            started_at,
            placement,
            anchor: None,
            floor_ms: 0,
            displayed_seconds: 0,
            task: None,
            generation: 0,
        }
    }
    pub(super) fn set_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        if visible == self.anchor.is_some() {
            return;
        }
        self.generation += 1;
        if self.task.take().is_some() {
            record_elapsed_clock!(Cancelled);
        }
        if !visible {
            if let Some(anchor) = self.anchor.take() {
                self.floor_ms = anchor.elapsed(Instant::now());
            }
            return;
        }
        let anchor = ElapsedAnchor::new(
            self.started_at,
            now_unix_ms(),
            Instant::now(),
            self.floor_ms,
        );
        let elapsed = anchor.elapsed(Instant::now());
        self.anchor = Some(anchor);
        self.displayed_seconds = elapsed / 1_000;
        cx.notify();
        let generation = self.generation;
        self.task = Some(cx.spawn(async move |weak, cx| {
            let mut delay = elapsed_delay(elapsed);
            loop {
                record_elapsed_clock!(Scheduled);
                cx.background_executor().timer(delay).await;
                record_elapsed_clock!(Woke);
                let next = weak
                    .update(cx, |view, cx| {
                        if view.generation != generation {
                            return None;
                        }
                        let elapsed = view.anchor.as_ref()?.elapsed(Instant::now());
                        view.floor_ms = view.floor_ms.max(elapsed);
                        let seconds = elapsed / 1_000;
                        if seconds != view.displayed_seconds {
                            view.displayed_seconds = seconds;
                            record_elapsed_clock!(Requested);
                            cx.notify();
                        }
                        Some(elapsed_delay(elapsed))
                    })
                    .ok()
                    .flatten();
                let Some(next) = next else {
                    break;
                };
                delay = next;
            }
        }));
    }
}
impl Render for RunningElapsedView {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        let label = if self.displayed_seconds == 0 && self.placement != ElapsedPlacement::Inline {
            String::new()
        } else {
            format_elapsed_ms(self.displayed_seconds.saturating_mul(1_000))
        };
        div()
            .id("running-activity-elapsed")
            .whitespace_nowrap()
            .flex_shrink_0()
            .text_right()
            .when(
                matches!(self.placement, ElapsedPlacement::Running { .. }),
                |this| this.font_semibold().pt_1(),
            )
            .when(
                self.placement == ElapsedPlacement::Running { show_dino: false },
                |this| this.pt_0().mb(px(2.)),
            )
            .child(label)
    }
}

pub(super) struct ActivityIndicatorView {
    #[cfg_attr(not(feature = "qualification-diagnostics"), allow(dead_code))]
    source: ActivitySpinnerKind,
    visible: bool,
}
impl ActivityIndicatorView {
    pub(super) fn new(source: ActivitySpinnerKind) -> Self {
        Self {
            source,
            visible: false,
        }
    }
    pub(super) fn set_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        if self.visible != visible {
            self.visible = visible;
            cx.notify();
        }
    }
}
impl Render for ActivityIndicatorView {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div().size_4().when(self.visible, |this| {
            this.child(
                crate::qualification_diagnostics::spinner!(self.source.diagnostic_source())
                    .icon(gpui_kit::component::IconName::Loader),
            )
        })
    }
}

/// Published avatar activities only; clocks stop immediately when the avatar is not visible.
pub(crate) struct TimelineAvatarActivities {
    pub(super) assets_loader: Arc<RunningDinoAssetLoader>,
    dino: HashMap<String, Entity<RunningDinoView>>,
    active: bool,
}
impl TimelineAvatarActivities {
    pub(crate) fn new(cx: &mut App) -> Self {
        Self {
            assets_loader: cx.default_global::<SharedRunningDinoAssets>().0.clone(),
            dino: HashMap::new(),
            active: false,
        }
    }
    pub(crate) fn set_active(&mut self, active: bool, cx: &mut App) {
        self.active = active;
        if !active {
            for view in self.dino.values() {
                view.update(cx, |view, cx| view.set_visible(false, cx));
            }
        }
    }
    pub(crate) fn set_visible_activities(
        &mut self,
        ids: &std::collections::HashSet<String>,
        cx: &mut App,
    ) {
        for (id, view) in &self.dino {
            view.update(cx, |view, cx| {
                view.set_visible(self.active && ids.contains(id), cx)
            });
        }
    }
    pub(super) fn retain(&mut self, live: &std::collections::HashSet<String>) {
        self.dino.retain(|id, _| live.contains(id));
    }
}
impl TimelineView {
    pub(super) fn prepare_running_dino(
        &self,
        id: String,
        cx: &mut Context<Self>,
    ) -> Entity<RunningDinoView> {
        let mut views = self.avatar_activities.borrow_mut();
        if let Some(view) = views.dino.get(&id) {
            return view.clone();
        }
        let view = cx.new(|_| RunningDinoView::new(views.assets_loader.clone(), false));
        views.dino.insert(id, view.clone());
        view
    }
    pub(super) fn running_turn_dino_view(
        &self,
        id: String,
        _: &mut Context<Self>,
    ) -> Option<Entity<RunningDinoView>> {
        self.avatar_activities.borrow().dino.get(&id).cloned()
    }
    pub(super) fn semantic_timeline_has_running_turn_row(&self) -> bool {
        self.thread_timeline_view_state
            .model
            .rows
            .iter()
            .any(|row| {
                matches!(
                    row,
                    super::TimelineRenderRow::Timeline(TimelineRow {
                        kind: TimelineRowKind::RunningTurn(_),
                        ..
                    })
                )
            })
    }
}
impl super::row_view::RowPresentation {
    pub(super) fn render_running_turn_row(
        &self,
        running_turn: &RunningTurnDisplay,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut App,
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
        _started_at_unix_ms: Option<i64>,
        state: Option<pioneer_client::timeline::types::TurnWorkState>,
        security_summary: Option<&ClientTurnSecuritySummary>,
        show_dino: bool,
        cx: &mut App,
    ) -> AnyElement {
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_render(
            pioneer_client::timeline::diagnostics::RenderRegion::RunningActivity
        ));
        let dino = show_dino
            .then(|| self.running_turn_dino_view(format!("content:{activity_id}"), cx))
            .flatten();
        let elapsed = self.inline_elapsed();
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
                    .when(show_dino, |this| this.child(div().size_8().children(dino)))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ActivitySpinnerKind {
    Reasoning,
    Command,
    FileChange,
    DynamicTool,
    WebSearch,
    WebFetch,
    Download,
}
impl ActivitySpinnerKind {
    #[cfg(feature = "qualification-diagnostics")]
    fn diagnostic_source(self) -> pioneer_client::timeline::diagnostics::AnimationSourceId {
        use pioneer_client::timeline::diagnostics::AnimationSourceId as Source;
        match self {
            Self::Reasoning => Source::TimelineRunningReasoning,
            Self::Command => Source::TimelineRunningCommand,
            Self::FileChange => Source::TimelineRunningFileChange,
            Self::DynamicTool => Source::TimelineRunningDynamicTool,
            Self::WebSearch => Source::TimelineRunningWebSearch,
            Self::WebFetch => Source::TimelineRunningWebFetch,
            Self::Download => Source::TimelineRunningDownload,
        }
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;
    use core::prelude::v1::test;
    #[core::prelude::v1::test]
    fn elapsed_boundaries_and_late_wakes_use_absolute_samples() {
        assert_eq!(elapsed_delay(1_000), Duration::from_millis(1_000));
        assert_eq!(elapsed_delay(1_250), Duration::from_millis(750));
        assert_eq!(elapsed_delay(6_999), Duration::from_millis(1));
        let now = Instant::now();
        let anchor = ElapsedAnchor::new(1_000, 2_250, now, 0);
        assert_eq!(anchor.elapsed(now + Duration::from_millis(8_749)), 9_999);
        assert_eq!(
            elapsed_delay(anchor.elapsed(now + Duration::from_millis(8_749))),
            Duration::from_millis(1)
        );
    }
    #[core::prelude::v1::test]
    fn elapsed_anchor_ignores_wall_jumps_and_remount_preserves_floor() {
        let now = Instant::now();
        let anchor = ElapsedAnchor::new(1_000, 2_250, now, 0);
        let floor = anchor.elapsed(now + Duration::from_secs(10));
        let backward = ElapsedAnchor::new(1_000, 0, now, floor);
        assert_eq!(backward.elapsed(now), 11_250);
        let forward = ElapsedAnchor::new(1_000, 100_000, now, floor);
        assert_eq!(forward.elapsed(now), 99_000);
        let revised_start = ElapsedAnchor::new(100_000, 100_250, now, 0);
        assert_eq!(revised_start.elapsed(now), 250);
        let future = ElapsedAnchor::new(3_000, 1_000, now, 0);
        assert_eq!(future.elapsed(now + Duration::from_secs(1)), 0);
        assert_eq!(future.elapsed(now + Duration::from_secs(3)), 1_000);
    }
    #[gpui_kit::test]
    fn elapsed_visibility_owns_one_timer_and_reduced_motion_does_not_stop_it(
        cx: &mut TestAppContext,
    ) {
        cx.update(|cx| cx.set_reduce_motion(true));
        let view =
            cx.new(|_| RunningElapsedView::new(now_unix_ms() - 2_250, ElapsedPlacement::Inline));
        view.update(cx, |view, cx| {
            assert!(view.task.is_none());
            view.set_visible(true, cx);
            let generation = view.generation;
            assert!(view.task.is_some());
            assert!(view.displayed_seconds >= 2);
            view.set_visible(true, cx);
            assert_eq!(view.generation, generation);
            view.set_visible(false, cx);
            assert!(view.task.is_none());
            assert!(view.anchor.is_none());
            let floor = view.floor_ms;
            view.set_visible(true, cx);
            assert!(view.task.is_some());
            assert!(view.anchor.as_ref().unwrap().elapsed(Instant::now()) >= floor);
            view.set_visible(false, cx);
        });
    }
    #[core::prelude::v1::test]
    fn embedded_dino_frames_preserve_order_and_delay_table() {
        let assets = decode_running_dino_assets().unwrap();
        assert!(assets.frame_count() > 1);
        assert_eq!(assets.dark.len(), assets.light.len());
        for (light, dark) in assets.light.iter().zip(&assets.dark) {
            assert_eq!(light.delay, dark.delay);
            assert_eq!(light.image.frame_count(), 1);
            assert_eq!(dark.image.frame_count(), 1);
        }
    }
    #[test]
    fn running_indicator_decode_has_a_bounded_pixel_budget() {
        let assets = decode_running_dino_assets().unwrap();
        let frames = assets.light.iter().chain(&assets.dark);
        let mut bytes = 0;
        for frame in frames {
            let size = frame.image.size(0);
            assert!(size.width.0 <= 128 && size.height.0 <= 128);
            bytes += frame.image.as_bytes(0).unwrap().len();
        }
        assert!(
            bytes < 1024 * 1024,
            "32px indicator retains {bytes} decoded bytes"
        );
    }

    #[gpui_kit::test]
    fn running_indicator_remounts_reuse_texture_identity(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let mut identities = std::collections::HashSet::new();
        for _ in 0..20 {
            let activities = cx.update(TimelineAvatarActivities::new);
            let view = cx.new(|_| RunningDinoView::new(activities.assets_loader.clone(), false));
            view.update(cx, |view, cx| view.set_visible(true, cx));
            cx.run_until_parked();
            view.read_with(cx, |view, _| {
                let assets = view
                    .assets
                    .as_ref()
                    .expect("visible indicator must load its frames");
                for frame in assets.light.iter().chain(&assets.dark) {
                    identities.insert(frame.image.id);
                }
            });
            view.update(cx, |view, cx| view.set_visible(false, cx));
            let weak = view.downgrade();
            drop(view);
            drop(activities);
            cx.run_until_parked();
            assert!(
                weak.upgrade().is_none(),
                "shared assets must not retain retired views"
            );
        }
        assert_eq!(
            identities.len(),
            12,
            "remounting must not leave new image IDs in the window atlas"
        );
    }

    #[gpui_kit::test]
    fn dino_suspension_cancels_immediately_and_remount_starts_at_initial_frame(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let assets = decode_running_dino_assets().unwrap();
        let view = cx.new(|_| {
            let mut view = RunningDinoView::new(Arc::default(), false);
            view.assets = Some(Arc::new(assets));
            view
        });
        view.update(cx, |view, cx| {
            view.set_visible(true, cx);
            assert!(view.clock_task.is_some());
            view.frame_index = 2;
            view.set_visible(false, cx);
            assert!(view.clock_task.is_none());
            assert_eq!(view.frame_index, 0);
            cx.set_reduce_motion(true);
            view.set_visible(true, cx);
            assert!(view.clock_task.is_none());
            assert_eq!(view.frame_index, 0);
            view.set_visible(false, cx);
        });
    }
}
