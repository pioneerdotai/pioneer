use crate::{
    app::root::PioneerDesktop,
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui::{prelude::*, *};
use gpui_component::{
    Disableable, StyledExt, WindowExt,
    form::{field, v_form},
    input::{Input, InputState},
    theme::ActiveTheme,
    v_flex,
};
use std::{cell::RefCell, rc::Rc};

impl PioneerDesktop {
    pub(in crate::app) fn open_rename_workspace_dialog(
        &mut self,
        workspace_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.workspace_action_in_progress() {
            return;
        }

        let Some(workspace) = self.workspace_by_id(workspace_id.as_str()).cloned() else {
            return;
        };

        let name_input_state = cx.new(|cx| {
            InputState::new(window, cx).placeholder(t!("workspace.dialog.name_label").to_string())
        });
        name_input_state.update(cx, |state, cx| {
            state.set_value(workspace.name.clone(), window, cx);
        });
        let desktop_entity = cx.entity().clone();
        let field_error: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        let initial_name = workspace.name.trim().to_owned();

        let rename_workspace: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let name_input_state = name_input_state.clone();
            let field_error = field_error.clone();
            let workspace_id = workspace_id.clone();

            move |cx| {
                let name = name_input_state.read(cx).value().trim().to_owned();
                if name.is_empty() {
                    *field_error.borrow_mut() = Some(t!("workspace.dialog.empty_name").to_string());
                    return false;
                }

                *field_error.borrow_mut() = None;
                let error = desktop_entity.update(cx, |view, cx| {
                    if view.rename_workspace_from_dialog(workspace_id.clone(), name.clone(), cx) {
                        None
                    } else {
                        view.workspaces_error()
                            .map(str::to_owned)
                            .or_else(|| Some(t!("workspace.error.rename_failed").to_string()))
                    }
                });
                if let Some(error) = error {
                    *field_error.borrow_mut() = Some(error);
                    return false;
                }
                true
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            name_input_state.update(cx, |state, cx| state.focus(window, cx));
            let current_name = name_input_state.read(cx).value().trim().to_owned();
            let can_submit = !current_name.is_empty() && current_name != initial_name;
            let field_error_message = field_error.borrow().clone();

            dialog
                .w(px(384.))
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("workspace.dialog.rename_title").to_string()),
                )
                .on_ok({
                    let rename_workspace = rename_workspace.clone();
                    move |_, _, cx| rename_workspace(cx)
                })
                .footer({
                    let rename_workspace = rename_workspace.clone();
                    move |_, _, _, _| {
                        vec![
                            default_outline_button("rename-workspace-cancel")
                                .label(t!("buttons.cancel").to_string())
                                .outline()
                                .on_click(|_, window, cx| {
                                    window.close_dialog(cx);
                                })
                                .into_any_element(),
                            default_primary_button("rename-workspace-save")
                                .label(t!("buttons.save").to_string())
                                .disabled(!can_submit)
                                .on_click({
                                    let rename_workspace = rename_workspace.clone();
                                    move |_, window, cx| {
                                        if rename_workspace(cx) {
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
                                .line_height(relative(1.35))
                                .child(t!("workspace.dialog.rename_description").to_string()),
                        )
                        .child(
                            v_form()
                                .child(
                                    field()
                                        .label(t!("workspace.dialog.name_label").to_string())
                                        .child(Input::new(&name_input_state).min_w_0()),
                                )
                                .when_some(field_error_message, |this, error| {
                                    this.child(
                                        field().label_indent(false).child(
                                            div()
                                                .text_sm()
                                                .text_color(cx.theme().danger)
                                                .line_height(relative(1.3))
                                                .whitespace_normal()
                                                .child(error),
                                        ),
                                    )
                                }),
                        ),
                )
        });
    }

    pub(in crate::app) fn open_create_workspace_dialog(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.workspace_action_in_progress() {
            return;
        }

        let name_input_state = cx.new(|cx| {
            InputState::new(window, cx).placeholder(t!("workspace.dialog.name_label").to_string())
        });
        let desktop_entity = cx.entity().clone();
        let field_error: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));

        let create_workspace: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let name_input_state = name_input_state.clone();
            let field_error = field_error.clone();

            move |cx| {
                let name = name_input_state.read(cx).value().trim().to_owned();
                if name.is_empty() {
                    *field_error.borrow_mut() = Some(t!("workspace.dialog.empty_name").to_string());
                    return false;
                }

                *field_error.borrow_mut() = None;
                let error = desktop_entity.update(cx, |view, cx| {
                    if view.create_workspace_from_dialog(name.clone(), cx) {
                        None
                    } else {
                        view.workspaces_error()
                            .map(str::to_owned)
                            .or_else(|| Some(t!("workspace.error.create_failed").to_string()))
                    }
                });
                if let Some(error) = error {
                    *field_error.borrow_mut() = Some(error);
                    return false;
                }
                true
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            name_input_state.update(cx, |state, cx| state.focus(window, cx));
            let current_name = name_input_state.read(cx).value().trim().to_owned();
            let can_submit = !current_name.is_empty();
            let field_error_message = field_error.borrow().clone();

            dialog
                .w(px(384.))
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("workspace.dialog.create_title").to_string()),
                )
                .on_ok({
                    let create_workspace = create_workspace.clone();
                    move |_, _, cx| create_workspace(cx)
                })
                .footer({
                    let create_workspace = create_workspace.clone();
                    move |_, _, _, _| {
                        vec![
                            default_outline_button("create-workspace-cancel")
                                .label(t!("buttons.cancel").to_string())
                                .outline()
                                .on_click(|_, window, cx| {
                                    window.close_dialog(cx);
                                })
                                .into_any_element(),
                            default_primary_button("create-workspace-save")
                                .label(t!("workspace.action.create").to_string())
                                .disabled(!can_submit)
                                .on_click({
                                    let create_workspace = create_workspace.clone();
                                    move |_, window, cx| {
                                        if create_workspace(cx) {
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
                                .line_height(relative(1.35))
                                .child(t!("workspace.dialog.create_description").to_string()),
                        )
                        .child(
                            v_form()
                                .child(
                                    field()
                                        .label(t!("workspace.dialog.name_label").to_string())
                                        .child(Input::new(&name_input_state).min_w_0()),
                                )
                                .when_some(field_error_message, |this, error| {
                                    this.child(
                                        field().label_indent(false).child(
                                            div()
                                                .text_sm()
                                                .text_color(cx.theme().danger)
                                                .line_height(relative(1.3))
                                                .whitespace_normal()
                                                .child(error),
                                        ),
                                    )
                                }),
                        ),
                )
        });
    }
}
