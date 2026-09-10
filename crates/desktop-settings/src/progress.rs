use gpui_kit::component::{
    ActiveTheme,
    plot::shape::{Arc, ArcData},
};
use gpui_kit::{prelude::*, *};
use std::{
    f32::consts::TAU,
    time::{Duration, Instant},
};
const DURATION: Duration = Duration::from_millis(150);
pub(crate) struct ProgressIndicatorView {
    from: f32,
    target: f32,
    started: Instant,
}
impl ProgressIndicatorView {
    pub fn new(cx: &mut App) -> Entity<Self> {
        cx.new(|cx| Self {
            from: 0.,
            target: 0.,
            started: cx.background_executor().now(),
        })
    }
    fn sample(&self, now: Instant) -> f32 {
        let delta = (now.saturating_duration_since(self.started).as_secs_f32()
            / DURATION.as_secs_f32())
        .min(1.);
        self.from + (self.target - self.from) * delta
    }
    pub fn set_value(&mut self, value: f32, cx: &mut Context<Self>) {
        let value = if value.is_finite() {
            value.clamp(0., 100.)
        } else {
            0.
        };
        if self.target == value {
            return;
        }
        let now = cx.background_executor().now();
        self.from = if cx.reduce_motion() {
            value
        } else {
            self.sample(now)
        };
        self.target = value;
        self.started = now;
        cx.notify();
    }
    fn render_circle(value: f32, color: Hsla) -> impl IntoElement {
        struct PrepaintState {
            value: f32,
            inner_radius: f32,
            outer_radius: f32,
            bounds: Bounds<Pixels>,
        }

        canvas(
            move |bounds: Bounds<Pixels>, _window: &mut Window, _cx: &mut App| {
                let stroke_width = (bounds.size.width * 0.15).min(px(5.0));
                let actual_size = bounds.size.width.min(bounds.size.height);
                let radius = (actual_size.as_f32() - stroke_width.as_f32()) / 2.0;

                PrepaintState {
                    value,
                    inner_radius: radius - stroke_width.as_f32() / 2.0,
                    outer_radius: radius + stroke_width.as_f32() / 2.0,
                    bounds,
                }
            },
            move |_bounds, prepaint, window: &mut Window, _cx: &mut App| {
                let arc = Arc::new()
                    .inner_radius(prepaint.inner_radius)
                    .outer_radius(prepaint.outer_radius);

                arc.paint(
                    &ArcData {
                        data: &(),
                        index: 0,
                        value: 100.0,
                        start_angle: 0.0,
                        end_angle: TAU,
                        pad_angle: 0.0,
                    },
                    color.opacity(0.2),
                    None,
                    None,
                    &prepaint.bounds,
                    window,
                );

                if prepaint.value > 0.0 {
                    arc.paint(
                        &ArcData {
                            data: &(),
                            index: 1,
                            value: prepaint.value,
                            start_angle: 0.0,
                            end_angle: prepaint.value / 100.0 * TAU,
                            pad_angle: 0.0,
                        },
                        color,
                        None,
                        None,
                        &prepaint.bounds,
                        window,
                    );
                }
            },
        )
        .absolute()
        .size_full()
    }
}
impl Render for ProgressIndicatorView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let now = cx.background_executor().now();
        let value = if cx.reduce_motion() {
            self.target
        } else {
            self.sample(now)
        };
        if !cx.reduce_motion()
            && self.from != self.target
            && now.saturating_duration_since(self.started) < DURATION
        {
            window.request_animation_frame();
        }
        div()
            .relative()
            .size_4()
            .child(Self::render_circle(value, cx.theme().progress_bar))
    }
}

#[cfg(test)]
mod tests {
    use super::{DURATION, ProgressIndicatorView};
    use gpui_kit::TestAppContext;
    #[gpui_kit::test]
    fn retained_transition_retargets_from_current_value_and_reduced_motion_finishes(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let progress = cx.update(ProgressIndicatorView::new);
        cx.update(|cx| progress.update(cx, |view, cx| view.set_value(100., cx)));
        let (start, from) = progress.read_with(cx, |view, _| (view.started, view.from));
        assert_eq!(from, 0.);
        assert_eq!(
            progress.read_with(cx, |view, _| view.sample(start + DURATION / 2)),
            50.
        );
        cx.update(|cx| progress.update(cx, |view, cx| view.set_value(100., cx)));
        assert_eq!(progress.read_with(cx, |view, _| view.started), start);
        cx.update(|cx| {
            cx.set_reduce_motion(true);
            progress.update(cx, |view, cx| view.set_value(75., cx));
        });
        assert_eq!(
            progress.read_with(cx, |view, _| (view.from, view.target)),
            (75., 75.)
        );
        let weak = progress.downgrade();
        drop(progress);
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
    }
}
