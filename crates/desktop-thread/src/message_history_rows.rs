use chrono::{Local, TimeZone};
use gpui_kit::component::{theme::ActiveTheme, v_flex};
use gpui_kit::{prelude::*, *};
use pioneer_client::timeline::rows::MessageRevisionPresentation;
pub(super) fn render_revision(revision: &MessageRevisionPresentation, cx: &mut App) -> AnyElement {
    let body = if revision.content_redacted {
        t!("timeline.message.deleted").to_string()
    } else {
        revision.text.clone().unwrap_or_default()
    };
    v_flex()
        .id(("message-revision", revision.revision))
        .gap_1()
        .p_3()
        .rounded_2xl()
        .border_1()
        .border_color(cx.theme().border)
        .child(
            div()
                .text_xs()
                .opacity(0.6)
                .child(format_revision_date(revision.created_at)),
        )
        .child(
            div()
                .w_full()
                .min_w_0()
                .whitespace_normal()
                .text_sm()
                .child(body),
        )
        .into_any_element()
}

fn format_revision_date(created_at: i64) -> String {
    Local
        .timestamp_opt(created_at, 0)
        .single()
        .map(|date| date.format("%d.%m.%Y %H:%M").to_string())
        .unwrap_or_else(|| "-".to_owned())
}
