use crate::{
    app::{PioneerDesktop, thread::thread_display_title},
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui_kit::component::{
    StyledExt, WindowExt,
    dialog::DialogFooter,
    form::{field, v_form},
    input::{Input, InputState},
    v_flex,
};
use gpui_kit::{prelude::*, *};
use std::rc::Rc;

impl PioneerDesktop {
    pub(in crate::app) fn open_rename_thread_dialog(
        &mut self,
        thread_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.can_manage_thread_presentation(thread_id.as_str()) {
            return;
        }
        let Some(coordinator) = self.thread_coordinator(thread_id.as_str()) else {
            return;
        };
        let Some(thread) = coordinator.thread() else {
            return;
        };
        let initial_name = thread_display_title(thread)
            .unwrap_or_else(|| t!("sidebar.thread.untitled").to_string());
        let rename_input_state = cx.new(|cx| InputState::new(window, cx));
        rename_input_state.update(cx, |state, cx| {
            state.set_value(initial_name, window, cx);
        });
        let desktop_entity = cx.entity().clone();
        let save_rename: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let thread_id = thread_id.clone();
            let rename_input_state = rename_input_state.clone();
            move |cx| {
                let new_name = rename_input_state.read(cx).value().trim().to_owned();
                if new_name.is_empty() {
                    return false;
                }
                let _ = desktop_entity.update(cx, |view, cx| {
                    view.rename_thread_from_sidebar(thread_id.clone(), new_name.clone(), cx);
                    cx.notify();
                });
                true
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            rename_input_state.update(cx, |state, cx| state.focus(window, cx));

            dialog
                .gap_1()
                .rounded_2xl()
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("dialog.thread.rename.title").to_string()),
                )
                .on_ok({
                    let save_rename = save_rename.clone();
                    move |_, _, cx| save_rename(cx)
                })
                .footer(DialogFooter::new().children({
                    let save_rename = save_rename.clone();
                    vec![
                        default_outline_button("rename-thread-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .on_click(|_, window, cx| {
                                window.close_dialog(cx);
                            })
                            .into_any_element(),
                        default_primary_button("rename-thread-save")
                            .label(t!("buttons.save").to_string())
                            .on_click({
                                let save_rename = save_rename.clone();
                                move |_, window, cx| {
                                    if save_rename(cx) {
                                        window.close_dialog(cx);
                                    }
                                }
                            })
                            .into_any_element(),
                    ]
                }))
                .child(
                    v_flex()
                        .w_full()
                        .pb_5()
                        .gap_4()
                        .child(
                            div()
                                .text_sm()
                                .opacity(0.6)
                                .child(t!("dialog.thread.rename.description").to_string()),
                        )
                        .child(
                            v_form().child(
                                field()
                                    .label(t!("common.name").to_string())
                                    .child(Input::new(&rename_input_state).min_w_0()),
                            ),
                        ),
                )
        });
    }

    pub(super) fn open_rename_folder_dialog(
        &mut self,
        folder_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
        let Some(folder) = self.thread_folder(folder_id.as_str()).cloned() else {
            return;
        };
        let initial_name = folder.name;
        let rename_input_state = cx.new(|cx| InputState::new(window, cx));
        rename_input_state.update(cx, |state, cx| {
            state.set_value(initial_name.clone(), window, cx);
        });
        let desktop_entity = cx.entity().clone();
        let save_rename: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let folder_id = folder_id.clone();
            let rename_input_state = rename_input_state.clone();
            move |cx| {
                let new_name = rename_input_state.read(cx).value().trim().to_owned();
                if new_name.is_empty() {
                    return false;
                }
                let _ = desktop_entity.update(cx, |view, cx| {
                    view.rename_folder_from_sidebar(folder_id.clone(), new_name.clone(), cx);
                    cx.notify();
                });
                true
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            rename_input_state.update(cx, |state, cx| state.focus(window, cx));

            dialog
                .gap_1()
                .rounded_2xl()
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("dialog.folder.rename.title").to_string()),
                )
                .on_ok({
                    let save_rename = save_rename.clone();
                    move |_, _, cx| save_rename(cx)
                })
                .footer(DialogFooter::new().children({
                    let save_rename = save_rename.clone();
                    vec![
                        default_outline_button("rename-folder-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .on_click(|_, window, cx| {
                                window.close_dialog(cx);
                            })
                            .into_any_element(),
                        default_primary_button("rename-folder-save")
                            .label(t!("buttons.save").to_string())
                            .on_click({
                                let save_rename = save_rename.clone();
                                move |_, window, cx| {
                                    if save_rename(cx) {
                                        window.close_dialog(cx);
                                    }
                                }
                            })
                            .into_any_element(),
                    ]
                }))
                .child(
                    v_flex()
                        .w_full()
                        .pb_5()
                        .gap_4()
                        .child(
                            div()
                                .text_sm()
                                .opacity(0.6)
                                .child(t!("dialog.folder.rename.description").to_string()),
                        )
                        .child(
                            v_form().child(
                                field()
                                    .label(t!("common.name").to_string())
                                    .child(Input::new(&rename_input_state).min_w_0()),
                            ),
                        ),
                )
        });
    }
}
