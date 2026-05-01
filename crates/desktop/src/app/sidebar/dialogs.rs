use crate::{
    app::PioneerDesktop,
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui::{prelude::*, *};
use gpui_component::{
    StyledExt, WindowExt,
    input::{Input, InputState},
    v_flex,
};
use std::rc::Rc;

impl PioneerDesktop {
    pub(super) fn open_rename_folder_dialog(
        &mut self,
        folder_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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
                .footer({
                    let save_rename = save_rename.clone();
                    move |_, _, _, _| {
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
                    }
                })
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
                        .child(Input::new(&rename_input_state)),
                )
        });
    }
}
