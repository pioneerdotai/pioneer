use super::ThreadHeaderView;
use crate::buttons::*;
use gpui_kit::component::{
    StyledExt, WindowExt,
    dialog::DialogFooter,
    form::{field, v_form},
    input::{Input, InputState},
    v_flex,
};
use gpui_kit::{prelude::*, *};
use std::rc::Rc;
impl ThreadHeaderView {
    pub(crate) fn open_rename_thread_dialog(
        &mut self,
        thread_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.can_manage() {
            return;
        }
        let Some(coordinator) = self.client.thread_coordinator_snapshot(thread_id.as_str()) else {
            return;
        };
        let Some(thread) = coordinator.thread() else {
            return;
        };
        let initial_name = pioneer_client::threads::title::thread_display_title(thread)
            .unwrap_or_else(|| t!("sidebar.thread.untitled").to_string());
        let rename_input_state = cx.new(|cx| InputState::new(window, cx));
        rename_input_state.update(cx, |state, cx| {
            state.set_value(initial_name, window, cx);
        });
        let desktop_entity = cx.weak_entity();
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
                    if let Some(thread) = view.client.thread_coordinator_snapshot(&thread_id) {
                        view.client
                            .dispatch(pioneer_client::core::ClientIntent::Workspace {
                            intent:
                                pioneer_client::workspaces::intents::WorkspaceIntent::RenameThread {
                                    workspace_id: thread.workspace_id.clone(),
                                    thread_id: thread_id.clone(),
                                    name: new_name.clone(),
                                },
                        });
                    }
                    cx.notify();
                });
                true
            }
        });

        self.dialog_open.set(true);
        let open = self.dialog_open.clone();
        window.open_dialog(cx, move |dialog, window, cx| {
            rename_input_state.update(cx, |state, cx| state.focus(window, cx));

            dialog
                .on_close({
                    let open = open.clone();
                    move |_, _, _| open.set(false)
                })
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
                            .on_click({
                                let open = open.clone();
                                move |_, window, cx| {
                                    open.set(false);
                                    window.close_dialog(cx);
                                }
                            })
                            .into_any_element(),
                        default_primary_button("rename-thread-save")
                            .label(t!("buttons.save").to_string())
                            .on_click({
                                let save_rename = save_rename.clone();
                                let open = open.clone();
                                move |_, window, cx| {
                                    if save_rename(cx) {
                                        open.set(false);
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
}
