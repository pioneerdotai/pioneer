use super::{
    autosave::{AgentsDocEditorLoadState, AgentsDocEditorSaveState},
    state::AgentsDocEditor,
};
use gpui::{prelude::*, *};
use gpui_component::{button::*, input::Input, scroll::ScrollableElement, theme::ActiveTheme, *};

const AGENTS_DOC_EDITOR_LINE_HEIGHT_PX: f32 = 25.0;

impl AgentsDocEditor {
    fn render_loaded(&self, cx: &mut Context<Self>) -> AnyElement {
        v_flex()
            .size_full()
            .min_h_0()
            .gap_3()
            .when(
                matches!(
                    self.autosave.save_state,
                    AgentsDocEditorSaveState::Conflict { .. }
                ),
                |this| this.child(self.render_conflict_panel(cx)),
            )
            .child(
                div().flex_1().min_h(px(280.)).w_full().child(
                    Input::new(&self.input)
                        .bordered(false)
                        .focus_bordered(false)
                        .p_0()
                        .h_full()
                        .text_size(px(14.))
                        .opacity(0.85)
                        .line_height(px(AGENTS_DOC_EDITOR_LINE_HEIGHT_PX))
                        .rounded_none(),
                ),
            )
            .into_any_element()
    }

    fn render_conflict_panel(&self, cx: &mut Context<Self>) -> AnyElement {
        let (local_content, remote_doc) = match &self.autosave.save_state {
            AgentsDocEditorSaveState::Conflict {
                local_content,
                remote_doc,
            } => (local_content.clone(), remote_doc.clone()),
            _ => return div().into_any_element(),
        };
        let editor_for_reload = cx.entity().clone();
        let editor_for_overwrite = cx.entity().clone();

        v_flex()
            .gap_3()
            .rounded_md()
            .border_1()
            .border_color(cx.theme().danger.opacity(0.3))
            .bg(cx.theme().danger.opacity(0.08))
            .p_3()
            .child(
                h_flex()
                    .items_center()
                    .justify_between()
                    .gap_3()
                    .child(
                        div()
                            .text_sm()
                            .font_medium()
                            .text_color(cx.theme().danger)
                            .child(t!("editor.agents_doc.conflict.title").to_string()),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                Button::new("agents-doc-conflict-reload")
                                    .small()
                                    .outline()
                                    .label(
                                        t!("editor.agents_doc.conflict.reload_remote").to_string(),
                                    )
                                    .on_click(move |_, window, cx| {
                                        let _ = editor_for_reload.update(cx, |editor, cx| {
                                            editor.reload_remote_conflict(window, cx);
                                        });
                                    }),
                            )
                            .child(
                                Button::new("agents-doc-conflict-overwrite")
                                    .small()
                                    .outline()
                                    .label(
                                        t!("editor.agents_doc.conflict.overwrite_remote")
                                            .to_string(),
                                    )
                                    .on_click(move |_, _window, cx| {
                                        let _ = editor_for_overwrite.update(cx, |editor, cx| {
                                            editor.overwrite_remote_conflict(cx);
                                        });
                                    }),
                            ),
                    ),
            )
            .child(
                div()
                    .text_xs()
                    .opacity(0.75)
                    .child(t!("editor.agents_doc.conflict.description").to_string()),
            )
            .child(
                h_flex()
                    .gap_3()
                    .child(agents_doc_conflict_preview_pane(
                        t!("editor.agents_doc.conflict.local").to_string(),
                        local_content,
                        cx,
                    ))
                    .child(agents_doc_conflict_preview_pane(
                        t!("editor.agents_doc.conflict.remote").to_string(),
                        remote_doc.content,
                        cx,
                    )),
            )
            .into_any_element()
    }

    fn render_save_state(&self, cx: &mut Context<Self>) -> AnyElement {
        match &self.autosave.save_state {
            AgentsDocEditorSaveState::Clean => div()
                .text_xs()
                .opacity(0.6)
                .child(t!("editor.agents_doc.save_state.clean").to_string())
                .into_any_element(),
            AgentsDocEditorSaveState::Dirty => div()
                .text_xs()
                .opacity(0.6)
                .child(t!("editor.agents_doc.save_state.dirty").to_string())
                .into_any_element(),
            AgentsDocEditorSaveState::Saving => div()
                .text_xs()
                .opacity(0.6)
                .child(t!("editor.agents_doc.save_state.saving").to_string())
                .into_any_element(),
            AgentsDocEditorSaveState::Saved { saved_at: _ } => div()
                .text_xs()
                .opacity(0.6)
                .child(t!("editor.agents_doc.save_state.saved").to_string())
                .into_any_element(),
            AgentsDocEditorSaveState::Error { message } => {
                let editor = cx.entity().clone();
                h_flex()
                    .items_center()
                    .justify_between()
                    .gap_3()
                    .child(
                        div().text_xs().text_color(cx.theme().danger).child(
                            t!(
                                "editor.agents_doc.save_state.error",
                                message = message.as_str()
                            )
                            .to_string(),
                        ),
                    )
                    .child(
                        Button::new("agents-doc-retry-save")
                            .small()
                            .outline()
                            .label(t!("editor.agents_doc.retry_save").to_string())
                            .on_click(move |_, window, cx| {
                                let _ = editor.update(cx, |editor, cx| {
                                    editor.retry_save_now(window, cx);
                                    cx.notify();
                                });
                            }),
                    )
                    .into_any_element()
            }
            AgentsDocEditorSaveState::Conflict { .. } => div()
                .text_xs()
                .text_color(cx.theme().danger)
                .child(t!("editor.agents_doc.save_state.conflict").to_string())
                .into_any_element(),
        }
    }

    fn render_error(&self, message: &str, cx: &mut Context<Self>) -> AnyElement {
        let editor = cx.entity().clone();

        v_flex()
            .gap_3()
            .rounded_md()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .p_3()
            .child(
                div()
                    .text_sm()
                    .font_medium()
                    .child(t!("editor.agents_doc.load_error").to_string()),
            )
            .child(div().text_xs().opacity(0.65).child(message.to_owned()))
            .child(
                Button::new("agents-doc-retry-load")
                    .small()
                    .outline()
                    .label(t!("editor.agents_doc.retry").to_string())
                    .on_click(move |_, _window, cx| {
                        let _ = editor.update(cx, |editor, cx| {
                            editor.start_load(cx);
                            cx.notify();
                        });
                    }),
            )
            .into_any_element()
    }
}

impl Render for AgentsDocEditor {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            .min_w_0()
            .min_h_0()
            .bg(cx.theme().background)
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .px_6()
                    .py_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        h_flex()
                            .items_center()
                            .gap_2()
                            .child(
                                Icon::new(IconName::File)
                                    .size_4()
                                    .text_color(cx.theme().foreground),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .font_semibold()
                                    .child(t!("editor.agents_doc.title").to_string()),
                            ),
                    )
                    .child(self.render_save_state(cx)),
            )
            .child(match &self.load_state {
                AgentsDocEditorLoadState::Loading => div()
                    .flex_1()
                    .w_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .text_sm()
                    .opacity(0.65)
                    .child(t!("editor.agents_doc.loading").to_string())
                    .into_any_element(),
                AgentsDocEditorLoadState::Loaded => self.render_loaded(cx),
                AgentsDocEditorLoadState::Failed(message) => self.render_error(message, cx),
            })
    }
}

fn agents_doc_conflict_preview_pane(
    label: String,
    content: String,
    cx: &mut Context<AgentsDocEditor>,
) -> AnyElement {
    v_flex()
        .flex_1()
        .min_w(px(0.))
        .gap_1()
        .child(div().text_xs().font_medium().opacity(0.7).child(label))
        .child(
            div()
                .h(px(110.))
                .w_full()
                .overflow_y_scrollbar()
                .rounded_md()
                .border_1()
                .border_color(cx.theme().border)
                .bg(cx.theme().background)
                .p_2()
                .text_xs()
                .child(content),
        )
        .into_any_element()
}
