use crate::app::PioneerDesktop;
use chrono::{Local, TimeZone};
use gpui_kit::component::{
    Disableable, StyledExt, WindowExt, button::Button, theme::ActiveTheme, v_flex,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::timeline::rows::{
    MessageRevisionPagePresentation, MessageRevisionPresentation, project_message_revision_page,
};
use pioneer_protocol::TurnMessageRevisionsPageParams;

#[derive(Clone, Debug)]
pub(in crate::app) struct DesktopMessageRevisionDialogState {
    pub(super) page: MessageRevisionPagePresentation,
    pub(super) loading_more: bool,
    pub(super) error: Option<String>,
}

impl PioneerDesktop {
    pub(in crate::app) fn open_message_revision_history(
        &mut self,
        thread_id: String,
        turn_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if thread_id.is_empty() || turn_id.is_empty() || self.message_revision_loading {
            return;
        }
        self.message_revision_loading = true;
        let sender = self.gateway.client_runtime.ws_command_sender().clone();
        let window_handle = window.window_handle();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.turn_message_revisions_page(TurnMessageRevisionsPageParams {
                            thread_id,
                            turn_id,
                            cursor: None,
                            limit: Some(50),
                        })
                    })
                    .await;
                let mut page = None;
                let _ = this.update(&mut cx, |view, cx| {
                    view.message_revision_loading = false;
                    if let Ok(response) = result {
                        page = Some(project_message_revision_page(response));
                    }
                    cx.notify();
                });
                if let Some(page) = page {
                    let this = this.clone();
                    let _ = window_handle.update(&mut cx, |_root, window, cx| {
                        let _ = this.update(cx, |view, cx| {
                            view.render_message_revision_dialog(page, window, cx);
                        });
                    });
                }
            }
        })
        .detach();
    }

    fn render_message_revision_dialog(
        &mut self,
        page: MessageRevisionPagePresentation,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let state = cx.new(|_| DesktopMessageRevisionDialogState {
            page,
            loading_more: false,
            error: None,
        });
        self.message_revision_dialog = Some(state.clone());
        let desktop = cx.entity().clone();
        window.open_dialog(cx, move |dialog, window, cx| {
            let snapshot = state.read(cx).clone();
            let mut content = v_flex().w_full().min_w_0().pt_4().gap_2();
            for revision in &snapshot.page.revisions {
                content = content.child(render_revision(revision, cx));
            }
            if snapshot.page.revisions.is_empty() {
                content = content.child(t!("timeline.message.revisions_empty").to_string());
            }
            if let Some(error) = &snapshot.error {
                content = content.child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().danger)
                        .child(error.clone()),
                );
            }
            if snapshot.page.next_cursor.is_some() {
                let state = state.clone();
                let desktop = desktop.clone();
                content = content.child(
                    Button::new("message-revisions-more")
                        .label(t!("timeline.message.revisions_more").to_string())
                        .disabled(snapshot.loading_more)
                        .on_click(move |_, _, cx| {
                            let _ = desktop.update(cx, |view, cx| {
                                view.load_more_message_revisions(state.clone(), cx);
                            });
                        }),
                );
            }
            dialog
                .w(px(480.))
                .max_h(window.viewport_size().height * 0.8)
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("timeline.message.revisions_title").to_string()),
                )
                .child(content)
        });
    }

    fn load_more_message_revisions(
        &mut self,
        state: Entity<DesktopMessageRevisionDialogState>,
        cx: &mut Context<Self>,
    ) {
        let snapshot = state.read(cx);
        if snapshot.loading_more {
            return;
        }
        let Some(cursor) = snapshot.page.next_cursor.clone() else {
            return;
        };
        let thread_id = snapshot.page.thread_id.clone();
        let turn_id = snapshot.page.turn_id.clone();
        state.update(cx, |state, cx| {
            state.loading_more = true;
            state.error = None;
            cx.notify();
        });
        let sender = self.gateway.client_runtime.ws_command_sender().clone();
        cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.turn_message_revisions_page(TurnMessageRevisionsPageParams {
                            thread_id,
                            turn_id,
                            cursor: Some(cursor),
                            limit: Some(50),
                        })
                    })
                    .await;
                let _ = state.update(&mut cx, |state, cx| {
                    state.loading_more = false;
                    match result {
                        Ok(response) => {
                            let next = project_message_revision_page(response);
                            state.page.revisions.extend(next.revisions);
                            state.page.next_cursor = next.next_cursor;
                        }
                        Err(_) => {
                            state.error = Some(t!("timeline.message.revisions_failed").to_string());
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }
}

fn render_revision(revision: &MessageRevisionPresentation, cx: &mut App) -> AnyElement {
    let body = if revision.content_redacted {
        t!("timeline.message.deleted").to_string()
    } else {
        revision.text.clone().unwrap_or_default()
    };
    v_flex()
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

#[cfg(test)]
mod tests {
    #[test]
    fn revision_ui_uses_existing_rpc_and_shared_projection() {
        let source = include_str!("message_revisions.rs");
        assert!(source.contains("turn_message_revisions_page"));
        assert!(source.contains("project_message_revision_page"));
        assert!(source.contains("next_cursor"));
        assert!(!source.contains(&["conversation", "_message"].concat()));
    }
}
