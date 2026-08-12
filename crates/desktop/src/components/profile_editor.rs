use std::path::PathBuf;

use gpui::{prelude::*, *};
use gpui_component::{
    Disableable, Icon, IconName, Sizable, StyledExt,
    avatar::Avatar,
    h_flex,
    input::{Input, InputState},
    spinner::Spinner,
    theme::ActiveTheme,
    v_flex,
};

use crate::components::buttonts::default_primary_button;

pub const PROFILE_EDITOR_MAX_WIDTH_PX: f32 = 720.;
const PROFILE_HEADER_SIDE_WIDTH_PX: f32 = 96.;

pub fn profile_avatar(display_name: String, avatar_path: Option<PathBuf>) -> AnyElement {
    Avatar::new()
        .name(display_name)
        .large()
        .when_some(avatar_path, |avatar, path| avatar.src(path))
        .into_any_element()
}

pub fn profile_identity_group(
    avatar: AnyElement,
    first_name: Entity<InputState>,
    last_name: Entity<InputState>,
    error: Option<String>,
    cx: &mut App,
) -> AnyElement {
    v_flex()
        .w_full()
        .gap_1()
        .child(
            h_flex()
                .w_full()
                .items_center()
                .gap_3()
                .p_4()
                .rounded_2xl()
                .bg(cx.theme().muted)
                .child(avatar)
                .child(
                    v_flex()
                        .min_w_0()
                        .flex_1()
                        .child(Input::new(&first_name).appearance(false).p_0().min_w_0())
                        .child(div().w_full().border_t_1().border_color(cx.theme().border))
                        .child(Input::new(&last_name).appearance(false).p_0().min_w_0()),
                ),
        )
        .child(
            div()
                .text_xs()
                .ml_4()
                .opacity(0.6)
                .child(t!("settings.profile.name_hint").to_string()),
        )
        .when_some(error, |this, error| {
            this.child(
                div()
                    .text_xs()
                    .ml_4()
                    .text_color(cx.theme().danger)
                    .child(error),
            )
        })
        .into_any_element()
}

pub fn profile_username_field(
    id: impl Into<ElementId>,
    nickname: String,
    error: Option<String>,
    on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    cx: &mut App,
) -> AnyElement {
    v_flex()
        .w_full()
        .gap_1()
        .child(
            h_flex()
                .id(id)
                .w_full()
                .items_center()
                .justify_between()
                .gap_3()
                .px_4()
                .py_4()
                .rounded_2xl()
                .bg(cx.theme().muted)
                .cursor_pointer()
                .on_click(on_click)
                .child(
                    div()
                        .text_sm()
                        .font_medium()
                        .opacity(0.8)
                        .child(t!("settings.profile.username").to_string()),
                )
                .child(
                    h_flex()
                        .min_w_0()
                        .gap_2()
                        .when(!nickname.is_empty(), |this| {
                            this.child(
                                div()
                                    .min_w_0()
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .text_sm()
                                    .opacity(0.6)
                                    .child(format!("@{nickname}")),
                            )
                        })
                        .child(Icon::new(IconName::ChevronRight).size_4().opacity(0.6)),
                ),
        )
        .when_some(error, |this, error| {
            this.child(
                div()
                    .text_xs()
                    .ml_4()
                    .text_color(cx.theme().danger)
                    .child(error),
            )
        })
        .into_any_element()
}

pub fn profile_username_editor(
    nickname: Entity<InputState>,
    error: Option<String>,
    cx: &mut App,
) -> AnyElement {
    v_flex()
        .w_full()
        .gap_6()
        .child(
            v_flex()
                .gap_1()
                .child(
                    Input::new(&nickname)
                        .large()
                        .cleanable(true)
                        .h(px(48.))
                        .bg(cx.theme().muted)
                        .rounded_2xl()
                        .border_0()
                        .min_w_0(),
                )
                .when_some(error, |this, error| {
                    this.child(
                        div()
                            .ml_3()
                            .text_xs()
                            .text_color(cx.theme().danger)
                            .child(error),
                    )
                })
                .child(
                    div()
                        .ml_3()
                        .text_xs()
                        .opacity(0.6)
                        .line_height(relative(1.35))
                        .child(t!("settings.profile.username_hint").to_string()),
                ),
        )
        .child(
            div()
                .ml_3()
                .text_xs()
                .opacity(0.6)
                .line_height(relative(1.35))
                .child(t!("settings.profile.username_rules").to_string()),
        )
        .into_any_element()
}

#[allow(clippy::too_many_arguments)]
pub fn profile_editor_header(
    title: String,
    back_button_id: impl Into<ElementId>,
    done_button_id: impl Into<ElementId>,
    done_label: String,
    saving: bool,
    can_complete: bool,
    on_back: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    on_done: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    cx: &mut App,
) -> AnyElement {
    h_flex()
        .w_full()
        .h(px(56.))
        .items_center()
        .px_6()
        .border_b_1()
        .border_color(cx.theme().border)
        .child(
            div().w(px(PROFILE_HEADER_SIDE_WIDTH_PX)).flex_none().child(
                default_primary_button(back_button_id)
                    .w_8()
                    .p_0()
                    .rounded_full()
                    .disabled(saving)
                    .child(Icon::new(IconName::ChevronLeft).size_5())
                    .on_click(on_back),
            ),
        )
        .child(
            div()
                .min_w_0()
                .flex_1()
                .text_center()
                .text_sm()
                .font_medium()
                .child(title),
        )
        .child(
            h_flex()
                .w(px(PROFILE_HEADER_SIDE_WIDTH_PX))
                .flex_none()
                .justify_end()
                .child(
                    default_primary_button(done_button_id)
                        .w(px(76.))
                        .px_0()
                        .rounded_full()
                        .disabled(saving || !can_complete)
                        .child(if saving {
                            Spinner::new().small().into_any_element()
                        } else {
                            div()
                                .text_sm()
                                .font_medium()
                                .child(done_label)
                                .into_any_element()
                        })
                        .on_click(on_done),
                ),
        )
        .into_any_element()
}

pub fn profile_editor_page(
    scroll_id: impl Into<ElementId>,
    header: AnyElement,
    content: AnyElement,
    error: Option<String>,
    cx: &mut App,
) -> AnyElement {
    v_flex()
        .size_full()
        .bg(cx.theme().background)
        .child(header)
        .child(
            v_flex()
                .id(scroll_id)
                .flex_1()
                .min_h_0()
                .overflow_y_scroll()
                .p_6()
                .child(
                    h_flex().w_full().justify_center().child(
                        v_flex()
                            .w_full()
                            .max_w(px(PROFILE_EDITOR_MAX_WIDTH_PX))
                            .gap_6()
                            .child(content)
                            .when_some(error, |body, error| {
                                body.child(
                                    div()
                                        .text_sm()
                                        .text_center()
                                        .text_color(cx.theme().danger)
                                        .child(error),
                                )
                            }),
                    ),
                ),
        )
        .into_any_element()
}
