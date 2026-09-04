use gpui_kit::component::{ActiveTheme, Sizable, Size, StyledExt};
use gpui_kit::prelude::FluentBuilder as _;
use gpui_kit::{
    Animation, AnimationExt as _, AnyElement, App, Bounds, ElementId, Hsla,
    InteractiveElement as _, IntoElement, ParentElement, Pixels, RenderOnce, StyleRefinement,
    Styled, Window, canvas, div, px, relative,
};
use std::{f32::consts::TAU, time::Duration};

use gpui_kit::component::plot::shape::{Arc, ArcData};

// Compatibility backport of GPUI Component's ProgressCircle for the 0.5.1 API.
#[derive(IntoElement)]
pub struct ProgressCircle {
    id: ElementId,
    style: StyleRefinement,
    color: Option<Hsla>,
    value: f32,
    size: Size,
    children: Vec<AnyElement>,
}

struct ProgressState {
    value: f32,
}

impl ProgressCircle {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            style: StyleRefinement::default(),
            color: None,
            value: 0.0,
            size: Size::default(),
            children: Vec::new(),
        }
    }

    pub fn value(mut self, value: f32) -> Self {
        self.value = value.clamp(0.0, 100.0);
        self
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

impl Styled for ProgressCircle {
    fn style(&mut self) -> &mut StyleRefinement {
        &mut self.style
    }
}

impl Sizable for ProgressCircle {
    fn with_size(mut self, size: impl Into<Size>) -> Self {
        self.size = size.into();
        self
    }
}

impl ParentElement for ProgressCircle {
    fn extend(&mut self, elements: impl IntoIterator<Item = AnyElement>) {
        self.children.extend(elements);
    }
}

impl RenderOnce for ProgressCircle {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let value = self.value;
        let state = window.use_keyed_state(self.id.clone(), cx, |_, _| ProgressState { value });
        let previous_value = state.read(cx).value;
        let color = self.color.unwrap_or(cx.theme().progress_bar);

        div()
            .id(self.id)
            .flex()
            .items_center()
            .justify_center()
            .line_height(relative(1.0))
            .map(|this| match self.size {
                Size::XSmall => this.size_2(),
                Size::Small => this.size_3(),
                Size::Medium => this.size_4(),
                Size::Large => this.size_5(),
                Size::Size(size) => this.size(size * 0.75),
            })
            .refine_style(&self.style)
            .children(self.children)
            .map(|this| {
                if previous_value == value {
                    return this
                        .child(Self::render_circle(value, color))
                        .into_any_element();
                }

                let duration = Duration::from_secs_f64(0.15);
                cx.spawn({
                    let state = state.clone();
                    async move |cx| {
                        cx.background_executor().timer(duration).await;
                        _ = state.update(cx, |state, _| state.value = value);
                    }
                })
                .detach();

                this.with_animation(
                    ("progress-circle-animation", previous_value.to_bits() as u64),
                    Animation::new(duration),
                    move |this, delta| {
                        let animated_value = previous_value + (value - previous_value) * delta;
                        this.child(Self::render_circle(animated_value, color))
                    },
                )
                .into_any_element()
            })
    }
}
