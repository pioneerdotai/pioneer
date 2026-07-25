use crate::{
    app::root::{ComposerCapability, GatewayConnectionState, PioneerDesktop},
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui::{prelude::*, *};
use gpui_component::{
    WindowExt,
    button::*,
    form::{field, v_form},
    input::{Input, InputState},
    scroll::ScrollableElement,
    theme::ActiveTheme,
    *,
};
use pioneer_client::composer::capabilities as composer_capabilities;
use pioneer_client::composer::capabilities::{
    McpCapabilityUnavailableReason, SelectableMcpCapability, SelectableSkillCapability,
    SkillCapabilityUnavailableReason,
};
use pioneer_client::composer::skill_selection::{
    ComposerSkillPickerProjection, ComposerSkillSelection, SelectablePackedSkillCapability,
    SelectableSkillPackCapability, normalize_composer_skill_selections,
    project_composer_skill_picker, reduce_composer_skill_selection_toggle,
};
use pioneer_client::composer::state_machine::ComposerDomainAction;
use pioneer_client::mcp::{details as mcp_details, list as mcp_list};
use pioneer_client::skills::catalog::{self as skill_catalog, SkillManagementProjection};
use pioneer_protocol::SkillPackId;
use std::collections::HashSet;

struct CapabilityPickerState {
    search: Entity<InputState>,
    selected: HashSet<String>,
    skill_management: SkillManagementProjection,
    skill_selections: Vec<ComposerSkillSelection>,
    expanded_skill_pack_ids: HashSet<SkillPackId>,
    skill_loading: bool,
    skill_error: Option<String>,
    mcp_server_rows: Vec<SelectableMcpCapability>,
    mcp_server_loading: bool,
    mcp_server_error: Option<String>,
    mcp_tool_rows: Vec<SelectableMcpCapability>,
    active_mcp_server_id: Option<String>,
    mcp_tool_loading_server_id: Option<String>,
    mcp_tool_error: Option<String>,
    did_focus_search: bool,
}

impl CapabilityPickerState {
    fn new(window: &mut Window, cx: &mut Context<Self>, placeholder: impl Into<String>) -> Self {
        Self {
            search: cx.new(|cx| InputState::new(window, cx).placeholder(placeholder.into())),
            selected: HashSet::new(),
            skill_management: SkillManagementProjection::default(),
            skill_selections: Vec::new(),
            expanded_skill_pack_ids: HashSet::new(),
            skill_loading: false,
            skill_error: None,
            mcp_server_rows: Vec::new(),
            mcp_server_loading: false,
            mcp_server_error: None,
            mcp_tool_rows: Vec::new(),
            active_mcp_server_id: None,
            mcp_tool_loading_server_id: None,
            mcp_tool_error: None,
            did_focus_search: false,
        }
    }

    fn focus_search_once(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.did_focus_search {
            return;
        }
        self.search.update(cx, |state, cx| state.focus(window, cx));
        self.did_focus_search = true;
    }

    fn query(&self, cx: &App) -> String {
        self.search.read(cx).value().to_string()
    }

    fn toggle_skill_selection(
        &mut self,
        picker: &ComposerSkillPickerProjection,
        selection: ComposerSkillSelection,
        cx: &mut Context<Self>,
    ) {
        self.skill_selections = reduce_desktop_skill_picker_selection(
            self.skill_selections.as_slice(),
            picker,
            selection,
        );
        cx.notify();
    }

    fn toggle_skill_pack_expanded(&mut self, pack_id: SkillPackId, cx: &mut Context<Self>) {
        if !self.expanded_skill_pack_ids.remove(&pack_id) {
            self.expanded_skill_pack_ids.insert(pack_id);
        }
        cx.notify();
    }

    fn toggle_mcp_selected(&mut self, row: &SelectableMcpCapability, cx: &mut Context<Self>) {
        let update = composer_capabilities::toggle_mcp_capability_selection(
            &mut self.selected,
            self.mcp_server_rows.as_slice(),
            self.mcp_tool_rows.as_slice(),
            row,
        );
        if update.collapse_active_server {
            self.active_mcp_server_id = None;
        }
        cx.notify();
    }

    fn start_loading_skills(&mut self, cx: &mut Context<Self>) {
        self.skill_loading = true;
        self.skill_error = None;
        cx.notify();
    }

    fn finish_loading_skills(
        &mut self,
        management: SkillManagementProjection,
        cx: &mut Context<Self>,
    ) {
        self.expanded_skill_pack_ids
            .retain(|pack_id| management.packs.iter().any(|row| &row.pack.id == pack_id));
        self.skill_management = management;
        self.skill_selections =
            normalize_composer_skill_selections(self.skill_selections.iter().cloned());
        self.skill_loading = false;
        self.skill_error = None;
        cx.notify();
    }

    fn fail_loading_skills(&mut self, error: String, cx: &mut Context<Self>) {
        self.skill_loading = false;
        self.skill_error = Some(error);
        cx.notify();
    }

    fn start_loading_mcp_servers(&mut self, cx: &mut Context<Self>) {
        self.mcp_server_loading = true;
        self.mcp_server_error = None;
        cx.notify();
    }

    fn finish_loading_mcp_servers(
        &mut self,
        rows: Vec<SelectableMcpCapability>,
        cx: &mut Context<Self>,
    ) {
        composer_capabilities::replace_mcp_server_capability_rows(
            &mut self.mcp_server_rows,
            self.mcp_tool_rows.as_slice(),
            &mut self.selected,
            rows,
        );
        self.mcp_server_loading = false;
        self.mcp_server_error = None;
        cx.notify();
    }

    fn fail_loading_mcp_servers(&mut self, error: String, cx: &mut Context<Self>) {
        self.mcp_server_loading = false;
        self.mcp_server_error = Some(error);
        cx.notify();
    }

    fn toggle_mcp_tools(&mut self, server_id: &str, cx: &mut Context<Self>) -> bool {
        let should_load = composer_capabilities::toggle_mcp_tool_capability_panel(
            &self.selected,
            self.mcp_server_rows.as_slice(),
            self.mcp_tool_rows.as_slice(),
            &mut self.active_mcp_server_id,
            &mut self.mcp_tool_error,
            self.mcp_tool_loading_server_id.as_deref(),
            server_id,
        );
        if !should_load {
            cx.notify();
        }

        should_load
    }

    fn start_loading_mcp_tools(&mut self, server_id: String, cx: &mut Context<Self>) {
        self.active_mcp_server_id = Some(server_id.clone());
        self.mcp_tool_loading_server_id = Some(server_id);
        self.mcp_tool_error = None;
        cx.notify();
    }

    fn finish_loading_mcp_tools(
        &mut self,
        server_id: &str,
        rows: Vec<SelectableMcpCapability>,
        cx: &mut Context<Self>,
    ) {
        composer_capabilities::replace_mcp_tool_capability_rows_for_server(
            &mut self.mcp_tool_rows,
            self.mcp_server_rows.as_slice(),
            &mut self.selected,
            server_id,
            rows,
        );
        if self.mcp_tool_loading_server_id.as_deref() == Some(server_id) {
            self.mcp_tool_loading_server_id = None;
        }
        self.mcp_tool_error = None;
        cx.notify();
    }

    fn merge_mcp_tool_rows(&mut self, rows: Vec<SelectableMcpCapability>, cx: &mut Context<Self>) {
        if composer_capabilities::merge_mcp_tool_capability_rows(
            &mut self.mcp_tool_rows,
            self.mcp_server_rows.as_slice(),
            &mut self.selected,
            rows,
        ) {
            cx.notify();
        }
    }

    fn fail_loading_mcp_tools(&mut self, server_id: &str, error: String, cx: &mut Context<Self>) {
        if self.mcp_tool_loading_server_id.as_deref() == Some(server_id) {
            self.mcp_tool_loading_server_id = None;
        }
        self.mcp_tool_error = Some(error);
        cx.notify();
    }
}

fn reduce_desktop_skill_picker_selection(
    current: &[ComposerSkillSelection],
    picker: &ComposerSkillPickerProjection,
    selection: ComposerSkillSelection,
) -> Vec<ComposerSkillSelection> {
    reduce_composer_skill_selection_toggle(current, picker, selection).selections
}

impl PioneerDesktop {
    pub(super) fn open_composer_skills_picker(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(target_thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        let desktop_entity = cx.entity().clone();
        let management = self.skills_management.clone();
        let initial_selections = self.composer_skill_selections.clone();
        let picker_state = cx.new(|cx| {
            let mut state = CapabilityPickerState::new(
                window,
                cx,
                t!("chat.composer.capability_picker.search_skills").to_string(),
            );
            state.skill_management = management;
            state.skill_selections = initial_selections;
            state
        });
        self.load_composer_skill_picker_rows(picker_state.clone(), cx);

        window.open_dialog(cx, move |dialog, window, cx| {
            picker_state.update(cx, |state, cx| state.focus_search_once(window, cx));
            let (picker, selections, expanded_pack_ids, loading, error) = {
                let state = picker_state.read(cx);
                let query = state.query(cx);
                (
                    project_composer_skill_picker(&state.skill_management, query.as_str()),
                    state.skill_selections.clone(),
                    state.expanded_skill_pack_ids.clone(),
                    state.skill_loading,
                    state.skill_error.clone(),
                )
            };

            dialog
                .w(px(520.))
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("chat.composer.add_menu.skills").to_string()),
                )
                .footer({
                    let desktop_entity = desktop_entity.clone();
                    let picker_state = picker_state.clone();
                    let target_thread_id = target_thread_id.clone();
                    move |_, _, _, cx| {
                        let selections = normalize_composer_skill_selections(
                            picker_state.read(cx).skill_selections.iter().cloned(),
                        );

                        vec![
                            default_outline_button("composer-skills-cancel")
                                .label(t!("buttons.cancel").to_string())
                                .outline()
                                .on_click(|_, window, cx| window.close_dialog(cx))
                                .into_any_element(),
                            default_primary_button("composer-skills-save")
                                .label(t!("buttons.add").to_string())
                                .disabled(selections.is_empty())
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    let target_thread_id = target_thread_id.clone();
                                    move |_, window, cx| {
                                        if selections.is_empty() {
                                            return;
                                        }
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            if view.current_active_thread_id()
                                                != Some(target_thread_id.as_str())
                                            {
                                                return;
                                            }
                                            view.reduce_composer_domain(
                                                ComposerDomainAction::SetSkillSelections {
                                                    selections: selections.clone(),
                                                },
                                            );
                                            cx.notify();
                                        });
                                        window.close_dialog(cx);
                                    }
                                })
                                .into_any_element(),
                        ]
                    }
                })
                .child(
                    v_flex()
                        .w_full()
                        .gap_1p5()
                        .py_4()
                        .child(render_picker_filter_form(&picker_state.read(cx).search))
                        .child(render_skill_rows(
                            picker,
                            selections,
                            expanded_pack_ids,
                            loading,
                            error,
                            picker_state.clone(),
                            cx,
                        )),
                )
        });
    }

    pub(super) fn open_composer_mcp_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(target_thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        let desktop_entity = cx.entity().clone();
        let server_rows = self.selectable_mcp_server_capabilities("");
        let initial_details = self.mcp_server_details.clone();
        let picker_state = cx.new(|cx| {
            let mut state = CapabilityPickerState::new(
                window,
                cx,
                t!("chat.composer.capability_picker.search_mcp").to_string(),
            );
            state.mcp_server_rows = server_rows;
            if let Some(details) = initial_details {
                state.mcp_tool_rows =
                    composer_capabilities::filter_mcp_tool_capability_rows(&details, "");
            }
            state
        });
        self.load_composer_mcp_picker_servers(picker_state.clone(), cx);

        window.open_dialog(cx, move |dialog, window, cx| {
            picker_state.update(cx, |state, cx| state.focus_search_once(window, cx));
            let (
                server_rows,
                tool_rows,
                has_query,
                active_server_id,
                selected,
                loading_server_id,
                tool_error,
                server_loading,
                server_error,
                loaded_tool_server_ids,
            ) = {
                let state = picker_state.read(cx);
                let query = state.query(cx);
                let has_query = composer_capabilities::has_capability_query(query.as_str());
                let active_server_id = state.active_mcp_server_id.clone();
                let selected_server_ids = composer_capabilities::selected_mcp_server_ids(
                    state.mcp_server_rows.as_slice(),
                    &state.selected,
                );
                let active_server_selected = active_server_id
                    .as_deref()
                    .is_some_and(|server_id| selected_server_ids.contains(server_id));
                let loaded_tool_server_ids = composer_capabilities::loaded_mcp_tool_server_ids(
                    state.mcp_tool_rows.as_slice(),
                );
                (
                    composer_capabilities::filter_selectable_mcp_capability_rows(
                        state.mcp_server_rows.as_slice(),
                        query.as_str(),
                    ),
                    if has_query {
                        composer_capabilities::filter_search_mcp_tool_capability_rows(
                            state.mcp_tool_rows.as_slice(),
                            &selected_server_ids,
                            query.as_str(),
                        )
                    } else if active_server_selected {
                        Vec::new()
                    } else {
                        composer_capabilities::filter_active_mcp_tool_capability_rows(
                            state.mcp_tool_rows.as_slice(),
                            active_server_id.as_deref(),
                            query.as_str(),
                        )
                    },
                    has_query,
                    active_server_id,
                    state.selected.clone(),
                    state.mcp_tool_loading_server_id.clone(),
                    state.mcp_tool_error.clone(),
                    state.mcp_server_loading,
                    state.mcp_server_error.clone(),
                    loaded_tool_server_ids,
                )
            };

            dialog
                .w(px(600.))
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("chat.composer.add_menu.mcp").to_string()),
                )
                .footer({
                    let desktop_entity = desktop_entity.clone();
                    let picker_state = picker_state.clone();
                    let target_thread_id = target_thread_id.clone();
                    move |_, _, _, cx| {
                        let capabilities =
                            selected_mcp_composer_capabilities(&picker_state.read(cx));

                        vec![
                            default_outline_button("composer-mcp-cancel")
                                .label(t!("buttons.cancel").to_string())
                                .outline()
                                .on_click(|_, window, cx| window.close_dialog(cx))
                                .into_any_element(),
                            default_primary_button("composer-mcp-save")
                                .label(t!("buttons.add").to_string())
                                .disabled(capabilities.is_empty())
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    let target_thread_id = target_thread_id.clone();
                                    move |_, window, cx| {
                                        if capabilities.is_empty() {
                                            return;
                                        }
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            if view.current_active_thread_id()
                                                != Some(target_thread_id.as_str())
                                            {
                                                return;
                                            }
                                            view.add_composer_capabilities(capabilities.clone());
                                            cx.notify();
                                        });
                                        window.close_dialog(cx);
                                    }
                                })
                                .into_any_element(),
                        ]
                    }
                })
                .child(
                    v_flex()
                        .w_full()
                        .gap_1p5()
                        .py_4()
                        .child(render_picker_filter_form(&picker_state.read(cx).search))
                        .child(render_mcp_rows(
                            server_rows,
                            tool_rows,
                            has_query,
                            active_server_id,
                            selected,
                            loading_server_id,
                            tool_error,
                            server_loading,
                            server_error,
                            loaded_tool_server_ids,
                            desktop_entity.clone(),
                            picker_state.clone(),
                            cx,
                        )),
                )
        });
    }

    pub(crate) fn composer_skill_picker_projection(
        &self,
        query: &str,
    ) -> ComposerSkillPickerProjection {
        project_composer_skill_picker(&self.skills_management, query)
    }

    pub(super) fn selectable_mcp_server_capabilities(
        &self,
        query: &str,
    ) -> Vec<SelectableMcpCapability> {
        composer_capabilities::filter_mcp_server_capability_rows(self.mcp_servers.as_slice(), query)
    }

    fn load_composer_skill_picker_rows(
        &mut self,
        picker_state: Entity<CapabilityPickerState>,
        cx: &mut Context<Self>,
    ) {
        picker_state.update(cx, |state, cx| state.start_loading_skills(cx));

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            picker_state.update(cx, |state, cx| {
                state.fail_loading_skills(t!("skills.error.gateway_not_connected").to_string(), cx);
            });
            return;
        }

        let Some(workspace_id) = self.skills_workspace_scope() else {
            picker_state.update(cx, |state, cx| {
                state
                    .fail_loading_skills(t!("skills.error.workspace_not_selected").to_string(), cx);
            });
            return;
        };

        let connection_id = self.gateway.ws_connection_id;
        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let workspace_id_for_request = workspace_id.clone();
                let result = cx
                    .background_spawn(async move {
                        ws_sender
                            .skills_list(skill_catalog::skill_list_params(workspace_id_for_request))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| match result {
                    Ok(response) => {
                        let split =
                            skill_catalog::derive_skills_catalog_and_installed(response.skills);
                        let management = skill_catalog::project_skill_management(
                            split.installed.as_slice(),
                            response.packs,
                        );
                        if view.gateway.ws_connection_id == connection_id
                            && view.skills_workspace_scope().as_deref()
                                == Some(workspace_id.as_str())
                        {
                            // The picker, chips, and submit preparation must
                            // resolve selections against the same catalog.
                            view.skills_management = management.clone();
                        }
                        picker_state.update(cx, |state, cx| {
                            state.finish_loading_skills(management, cx);
                        });
                        cx.notify();
                    }
                    Err(error) => {
                        picker_state.update(cx, |state, cx| {
                            state.fail_loading_skills(
                                format!("{}: {error:#}", t!("skills.error.load_failed")),
                                cx,
                            );
                        });
                    }
                });
            }
        })
        .detach();
    }

    fn load_composer_mcp_picker_servers(
        &mut self,
        picker_state: Entity<CapabilityPickerState>,
        cx: &mut Context<Self>,
    ) {
        picker_state.update(cx, |state, cx| state.start_loading_mcp_servers(cx));

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            picker_state.update(cx, |state, cx| {
                state.fail_loading_mcp_servers(
                    t!("mcp.error.gateway_not_connected").to_string(),
                    cx,
                );
            });
            return;
        }

        let Some(workspace_id) = self.mcp_workspace_scope() else {
            picker_state.update(cx, |state, cx| {
                state.fail_loading_mcp_servers(
                    t!("mcp.error.workspace_not_selected").to_string(),
                    cx,
                );
            });
            return;
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let cx = cx.clone();
            async move {
                let workspace_id_for_prefetch = workspace_id.clone();
                let prefetch_ws_sender = ws_sender.clone();
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_list(mcp_list::mcp_list_params(workspace_id))
                    })
                    .await;

                let prefetch_server_ids = match result {
                    Ok(response) => {
                        let reduction =
                            composer_capabilities::reduce_composer_mcp_server_picker_rows_response(
                                response, "",
                            );
                        let prefetch_server_ids = reduction.prefetch_server_ids.clone();
                        let _ = cx.update(|cx| {
                            picker_state.update(cx, |state, cx| {
                                state.finish_loading_mcp_servers(reduction.rows, cx);
                            });
                        });
                        prefetch_server_ids
                    }
                    Err(error) => {
                        let error = format!("{error:#}");
                        let _ = cx.update(|cx| {
                            picker_state.update(cx, |state, cx| {
                                state.fail_loading_mcp_servers(
                                    t!("mcp.error.load_servers_failed", error = error.as_str())
                                        .to_string(),
                                    cx,
                                );
                            });
                        });
                        return;
                    }
                };

                if prefetch_server_ids.is_empty() {
                    return;
                }

                let prefetch_rows = cx
                    .background_spawn(async move {
                        let mut rows = Vec::new();
                        for server_id in prefetch_server_ids {
                            let details = prefetch_ws_sender.mcp_server_details(
                                mcp_details::mcp_server_details_params(
                                    workspace_id_for_prefetch.clone(),
                                    server_id,
                                ),
                            );
                            if let Ok(details) = details {
                                rows.extend(
                                    composer_capabilities::reduce_composer_mcp_tool_picker_rows_response(
                                        details, "",
                                    )
                                    .rows,
                                );
                            }
                        }
                        rows
                    })
                    .await;

                let _ = cx.update(|cx| {
                    picker_state.update(cx, |state, cx| {
                        state.merge_mcp_tool_rows(prefetch_rows, cx);
                    });
                });
            }
        })
        .detach();
    }

    fn load_composer_mcp_picker_tools(
        &mut self,
        server_id: String,
        picker_state: Entity<CapabilityPickerState>,
        cx: &mut Context<Self>,
    ) {
        picker_state.update(cx, |state, cx| {
            state.start_loading_mcp_tools(server_id.clone(), cx);
        });

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            picker_state.update(cx, |state, cx| {
                state.fail_loading_mcp_tools(
                    server_id.as_str(),
                    t!("mcp.error.gateway_not_connected").to_string(),
                    cx,
                );
            });
            return;
        }

        let Some(workspace_id) = self.mcp_workspace_scope() else {
            picker_state.update(cx, |state, cx| {
                state.fail_loading_mcp_tools(
                    server_id.as_str(),
                    t!("mcp.error.workspace_not_selected").to_string(),
                    cx,
                );
            });
            return;
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let cx = cx.clone();
            async move {
                let server_id_for_request = server_id.clone();
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_server_details(mcp_details::mcp_server_details_params(
                            workspace_id,
                            server_id_for_request,
                        ))
                    })
                    .await;

                let _ = cx.update(|cx| {
                    picker_state.update(cx, |state, cx| match result {
                        Ok(response) => {
                            let reduction =
                                composer_capabilities::reduce_composer_mcp_tool_picker_rows_response(
                                    response, "",
                                );
                            state.finish_loading_mcp_tools(
                                server_id.as_str(),
                                reduction.rows,
                                cx,
                            );
                        }
                        Err(error) => {
                            let error = format!("{error:#}");
                            state.fail_loading_mcp_tools(
                                server_id.as_str(),
                                t!("mcp.error.load_details_failed", error = error.as_str())
                                    .to_string(),
                                cx,
                            );
                        }
                    });
                });
            }
        })
        .detach();
    }
}

fn render_picker_filter_form(search: &Entity<InputState>) -> AnyElement {
    v_form()
        .child(
            field()
                .label_indent(false)
                .child(Input::new(search).min_w_0()),
        )
        .into_any_element()
}

fn render_skill_rows(
    picker: ComposerSkillPickerProjection,
    selections: Vec<ComposerSkillSelection>,
    expanded_pack_ids: HashSet<SkillPackId>,
    loading: bool,
    error: Option<String>,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    if picker.standalone.is_empty() && picker.packs.is_empty() {
        if loading {
            return empty_picker_state(
                t!("chat.composer.capability_picker.loading_skills").to_string(),
                cx,
            );
        }
        if let Some(error) = error {
            return picker_error_state(error, cx);
        }
        return empty_picker_state(
            t!("chat.composer.capability_picker.no_skills").to_string(),
            cx,
        );
    }

    let mut list = v_flex().max_h(px(360.)).overflow_y_scrollbar();

    if loading {
        list = list.child(picker_status_banner(
            t!("chat.composer.capability_picker.refreshing_skills").to_string(),
            cx,
        ));
    }
    if let Some(error) = error {
        list = list.child(picker_error_banner(error, cx));
    }

    let picker_for_rows = picker.clone();
    let mut rows = picker
        .standalone
        .into_iter()
        .map(|row| {
            render_standalone_skill_picker_row(
                row,
                &selections,
                picker_for_rows.clone(),
                picker_state.clone(),
                cx,
            )
        })
        .collect::<Vec<_>>();
    rows.extend(picker.packs.into_iter().flat_map(|pack| {
        let is_expanded = expanded_pack_ids.contains(&pack.pack_id);
        let mut pack_rows = vec![render_skill_pack_picker_row(
            pack.clone(),
            &selections,
            is_expanded,
            picker_for_rows.clone(),
            picker_state.clone(),
            cx,
        )];
        if is_expanded {
            pack_rows.extend(pack.children.into_iter().map(|child| {
                render_packed_skill_picker_row(
                    child,
                    &selections,
                    picker_for_rows.clone(),
                    picker_state.clone(),
                    cx,
                )
            }));
        }
        pack_rows
    }));

    let row_count = rows.len();
    list.children(rows.into_iter().enumerate().map(|(row_index, row)| {
        div()
            .w_full()
            .when(row_index + 1 < row_count, |this| this.pb_2())
            .child(row)
    }))
    .into_any_element()
}

fn render_standalone_skill_picker_row(
    row: SelectableSkillCapability,
    selections: &[ComposerSkillSelection],
    picker: ComposerSkillPickerProjection,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    let selection = ComposerSkillSelection::Skill {
        skill_id: row.skill_id.clone(),
        pack_id: None,
    };
    render_skill_picker_leaf_row(row, selection, selections, picker, picker_state, cx)
}

fn render_packed_skill_picker_row(
    child: SelectablePackedSkillCapability,
    selections: &[ComposerSkillSelection],
    picker: ComposerSkillPickerProjection,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    let selection = ComposerSkillSelection::Skill {
        skill_id: child.skill.skill_id.clone(),
        pack_id: Some(child.pack_id),
    };
    render_skill_picker_leaf_row(child.skill, selection, selections, picker, picker_state, cx)
}

fn render_skill_picker_leaf_row(
    row: SelectableSkillCapability,
    selection: ComposerSkillSelection,
    selections: &[ComposerSkillSelection],
    picker: ComposerSkillPickerProjection,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    let is_selected = selections.contains(&selection);
    let key_id = stable_picker_row_id(selection.key().as_str());
    let disabled_reason =
        skill_capability_unavailable_reason_label(row.unavailable_reason.as_ref());
    let selection_for_row = selection.clone();
    let selection_for_toggle = selection;
    let picker_for_row = picker.clone();
    let picker_state_for_row = picker_state.clone();
    let picker_state_for_toggle = picker_state.clone();

    h_flex()
        .id(("composer-skill-picker-row", key_id))
        .w_full()
        .items_center()
        .gap_3()
        .rounded_md()
        .border_1()
        .border_color(if is_selected {
            cx.theme().blue
        } else {
            cx.theme().border
        })
        .bg(cx.theme().background)
        .p_2()
        .when(row.selectable, |this| {
            this.cursor_pointer().on_click(move |_, _, cx| {
                cx.stop_propagation();
                picker_state_for_row.update(cx, |state, cx| {
                    state.toggle_skill_selection(&picker_for_row, selection_for_row.clone(), cx);
                });
            })
        })
        .child(
            v_flex()
                .flex_1()
                .min_w_0()
                .gap_1()
                .child(div().text_sm().font_medium().child(row.label))
                .child(
                    div()
                        .text_xs()
                        .opacity(0.6)
                        .line_height(relative(1.45))
                        .child(if row.description.trim().is_empty() {
                            row.slug
                        } else {
                            row.description
                        }),
                )
                .when_some(disabled_reason, |this, reason| {
                    this.child(div().text_xs().text_color(cx.theme().danger).child(reason))
                }),
        )
        .child(render_picker_select_control(
            ("composer-skill-picker-toggle", key_id),
            is_selected,
            !row.selectable,
            move |_, _, cx| {
                picker_state_for_toggle.update(cx, |state, cx| {
                    state.toggle_skill_selection(&picker, selection_for_toggle.clone(), cx);
                });
            },
            cx,
        ))
        .into_any_element()
}

fn render_skill_pack_picker_row(
    pack: SelectableSkillPackCapability,
    selections: &[ComposerSkillSelection],
    expanded: bool,
    picker: ComposerSkillPickerProjection,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    let selection = ComposerSkillSelection::SkillPack {
        pack_id: pack.pack_id.clone(),
    };
    let is_selected = selections.contains(&selection);
    let key_id = stable_picker_row_id(pack.key.as_str());
    let pack_id = pack.pack_id.clone();
    let selection_for_row = selection.clone();
    let selection_for_toggle = selection;
    let picker_for_row = picker.clone();
    let picker_state_for_row = picker_state.clone();
    let picker_state_for_toggle = picker_state.clone();
    let picker_state_for_expand = picker_state.clone();

    h_flex()
        .id(("composer-skill-pack-picker-row", key_id))
        .w_full()
        .items_center()
        .gap_3()
        .rounded_md()
        .border_1()
        .border_color(if is_selected {
            cx.theme().blue
        } else {
            cx.theme().border
        })
        .bg(cx.theme().background)
        .p_2()
        .when(pack.selectable, |this| {
            this.cursor_pointer().on_click(move |_, _, cx| {
                cx.stop_propagation();
                picker_state_for_row.update(cx, |state, cx| {
                    state.toggle_skill_selection(&picker_for_row, selection_for_row.clone(), cx);
                });
            })
        })
        .child(
            v_flex()
                .flex_1()
                .min_w_0()
                .gap_1()
                .child(div().text_sm().font_medium().child(pack.label))
                .child(
                    div()
                        .text_xs()
                        .opacity(0.6)
                        .child(pack.children.len().to_string()),
                ),
        )
        .child(render_picker_select_control(
            ("composer-skill-pack-picker-toggle", key_id),
            is_selected,
            !pack.selectable,
            move |_, _, cx| {
                picker_state_for_toggle.update(cx, |state, cx| {
                    state.toggle_skill_selection(&picker, selection_for_toggle.clone(), cx);
                });
            },
            cx,
        ))
        .child(
            Button::new(("composer-skill-pack-expand", key_id))
                .small()
                .compact()
                .ghost()
                .icon(if expanded {
                    IconName::ChevronUp
                } else {
                    IconName::ChevronDown
                })
                .on_click(move |_, _, cx| {
                    cx.stop_propagation();
                    picker_state_for_expand.update(cx, |state, cx| {
                        state.toggle_skill_pack_expanded(pack_id.clone(), cx);
                    });
                }),
        )
        .into_any_element()
}

fn render_picker_select_control(
    id: impl Into<ElementId>,
    is_selected: bool,
    disabled: bool,
    on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    cx: &mut App,
) -> AnyElement {
    div()
        .id(id)
        .flex_none()
        .size_6()
        .flex()
        .items_center()
        .justify_center()
        .rounded_full()
        .border_1()
        .border_color(if is_selected {
            cx.theme().blue
        } else {
            cx.theme().border
        })
        .bg(if is_selected {
            cx.theme().blue
        } else {
            cx.theme().background
        })
        .when(!disabled, |this| {
            this.cursor_pointer().on_click(move |event, window, cx| {
                cx.stop_propagation();
                on_click(event, window, cx);
            })
        })
        .when(is_selected, |this| {
            this.child(
                Icon::new(IconName::Check)
                    .size_4()
                    .text_color(cx.theme().background),
            )
        })
        .into_any_element()
}

fn render_mcp_rows(
    server_rows: Vec<SelectableMcpCapability>,
    tool_rows: Vec<SelectableMcpCapability>,
    has_query: bool,
    active_server_id: Option<String>,
    selected: HashSet<String>,
    loading_server_id: Option<String>,
    tool_error: Option<String>,
    server_loading: bool,
    server_error: Option<String>,
    loaded_tool_server_ids: HashSet<String>,
    desktop_entity: Entity<PioneerDesktop>,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    if server_rows.is_empty()
        && tool_rows.is_empty()
        && !server_loading
        && loading_server_id.is_none()
        && server_error.is_none()
        && tool_error.is_none()
    {
        return empty_picker_state(
            t!("chat.composer.capability_picker.no_mcp_servers").to_string(),
            cx,
        );
    }

    let has_server_rows = !server_rows.is_empty();
    let has_tool_rows = !tool_rows.is_empty();

    let mut rows = v_flex().max_h(px(420.)).overflow_y_scrollbar().gap_2();

    if has_server_rows {
        rows = rows.child(
            v_flex()
                .gap_1()
                .child(
                    div()
                        .text_xs()
                        .font_medium()
                        .opacity(0.6)
                        .child(t!("chat.composer.capability_picker.servers").to_string()),
                )
                .child(render_mcp_server_rows(
                    server_rows,
                    if has_query {
                        Vec::new()
                    } else {
                        tool_rows.clone()
                    },
                    active_server_id.as_deref(),
                    loading_server_id.as_deref(),
                    if has_query { None } else { tool_error.clone() },
                    &loaded_tool_server_ids,
                    selected.clone(),
                    desktop_entity.clone(),
                    picker_state.clone(),
                    cx,
                )),
        );
    }

    if server_loading {
        rows = rows.child(picker_status_banner(
            t!("chat.composer.capability_picker.refreshing_mcp_servers").to_string(),
            cx,
        ));
    }

    if let Some(error) = server_error {
        rows = rows.child(picker_error_banner(error, cx));
    }

    if has_query
        && loading_server_id.is_some()
        && loading_server_id.as_deref() == active_server_id.as_deref()
    {
        rows = rows.child(
            div()
                .rounded_md()
                .border_1()
                .border_color(cx.theme().border)
                .p_2()
                .text_sm()
                .opacity(0.6)
                .child(t!("chat.composer.capability_picker.loading_tools").to_string()),
        );
    }

    if has_query && let Some(error) = tool_error {
        rows = rows.child(
            div()
                .p_2()
                .text_sm()
                .text_color(cx.theme().danger)
                .child(error),
        );
    }

    if has_query && has_tool_rows {
        rows = rows.child(
            v_flex()
                .gap_1()
                .pt_2()
                .child(
                    div()
                        .text_xs()
                        .font_medium()
                        .opacity(0.6)
                        .child(t!("chat.composer.capability_picker.tools").to_string()),
                )
                .child(v_flex().gap_2().children(tool_rows.into_iter().map(|row| {
                    render_mcp_row(
                        row,
                        active_server_id.as_deref(),
                        loading_server_id.as_deref(),
                        &loaded_tool_server_ids,
                        selected.clone(),
                        desktop_entity.clone(),
                        picker_state.clone(),
                        cx,
                    )
                }))),
        );
    }

    rows.into_any_element()
}

fn render_mcp_server_rows(
    server_rows: Vec<SelectableMcpCapability>,
    tool_rows: Vec<SelectableMcpCapability>,
    active_server_id: Option<&str>,
    loading_server_id: Option<&str>,
    tool_error: Option<String>,
    loaded_tool_server_ids: &HashSet<String>,
    selected: HashSet<String>,
    desktop_entity: Entity<PioneerDesktop>,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    v_flex()
        .gap_2()
        .children(server_rows.into_iter().flat_map(|row| {
            let server_id = row.server_id.clone();
            let is_active_server = active_server_id == Some(server_id.as_str());
            let mut elements = vec![render_mcp_row(
                row,
                active_server_id,
                loading_server_id,
                loaded_tool_server_ids,
                selected.clone(),
                desktop_entity.clone(),
                picker_state.clone(),
                cx,
            )];

            if is_active_server {
                if loading_server_id == Some(server_id.as_str()) {
                    elements.push(render_mcp_tools_loading_row(cx));
                }

                if let Some(error) = tool_error.clone() {
                    elements.push(render_mcp_tools_error_row(error, cx));
                }

                let active_tool_rows = tool_rows
                    .iter()
                    .filter(|tool_row| tool_row.server_id == server_id)
                    .cloned()
                    .collect::<Vec<_>>();
                if !active_tool_rows.is_empty() {
                    elements.push(render_mcp_tool_rows(
                        active_tool_rows,
                        active_server_id,
                        loading_server_id,
                        loaded_tool_server_ids,
                        selected.clone(),
                        desktop_entity.clone(),
                        picker_state.clone(),
                        cx,
                    ));
                }
            }

            elements
        }))
        .into_any_element()
}

fn render_mcp_tool_rows(
    tool_rows: Vec<SelectableMcpCapability>,
    active_server_id: Option<&str>,
    loading_server_id: Option<&str>,
    loaded_tool_server_ids: &HashSet<String>,
    selected: HashSet<String>,
    desktop_entity: Entity<PioneerDesktop>,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    v_flex()
        .gap_2()
        .children(tool_rows.into_iter().map(|row| {
            render_mcp_row(
                row,
                active_server_id,
                loading_server_id,
                loaded_tool_server_ids,
                selected.clone(),
                desktop_entity.clone(),
                picker_state.clone(),
                cx,
            )
        }))
        .into_any_element()
}

fn render_mcp_tools_loading_row(cx: &mut App) -> AnyElement {
    div()
        .rounded_md()
        .border_1()
        .border_color(cx.theme().border)
        .p_2()
        .text_sm()
        .opacity(0.6)
        .child(t!("chat.composer.capability_picker.loading_tools").to_string())
        .into_any_element()
}

fn render_mcp_tools_error_row(error: String, cx: &mut App) -> AnyElement {
    div()
        .p_2()
        .text_sm()
        .text_color(cx.theme().danger)
        .child(error)
        .into_any_element()
}

fn render_mcp_row(
    row: SelectableMcpCapability,
    active_server_id: Option<&str>,
    loading_server_id: Option<&str>,
    _loaded_tool_server_ids: &HashSet<String>,
    selected: HashSet<String>,
    desktop_entity: Entity<PioneerDesktop>,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    let is_selected = selected.contains(row.key.as_str());
    let is_active_server = active_server_id == Some(row.server_id.as_str());
    let key = row.key.clone();
    let key_id = stable_picker_row_id(key.as_str());
    let server_id = row.server_id.clone();
    let server_id_hash = stable_picker_row_id(server_id.as_str());
    let can_load_tools = row.raw_tool_name.is_none() && row.selectable && !is_selected;
    let is_tool = row.raw_tool_name.is_some();
    let is_loading_tools = can_load_tools && loading_server_id == Some(row.server_id.as_str());
    let is_selectable = row.selectable;
    let disabled_reason = mcp_capability_unavailable_reason_label(row.unavailable_reason.as_ref());
    let description = mcp_capability_row_description(&row);
    let row_for_row_click = row.clone();
    let row_for_toggle = row.clone();
    let picker_state_for_row = picker_state.clone();
    let picker_state_for_toggle = picker_state.clone();

    h_flex()
        .id(("composer-mcp-picker-row", key_id))
        .w_full()
        .items_center()
        .gap_3()
        .rounded_md()
        .border_1()
        .border_color(if is_selected {
            cx.theme().blue
        } else {
            cx.theme().border
        })
        .bg(cx.theme().background)
        .p_2()
        .when(is_selectable, |this| {
            this.cursor_pointer().on_click(move |_, _, cx| {
                cx.stop_propagation();
                picker_state_for_row.update(cx, |state, cx| {
                    state.toggle_mcp_selected(&row_for_row_click, cx);
                });
            })
        })
        .child(
            v_flex()
                .flex_1()
                .min_w_0()
                .gap_1()
                .child(div().text_sm().font_medium().child(row.label.clone()))
                .when(!is_tool, |this| {
                    this.child(
                        div()
                            .text_xs()
                            .opacity(0.6)
                            .line_height(relative(1.25))
                            .child(description),
                    )
                })
                .when_some(disabled_reason, |this, reason| {
                    this.child(div().text_xs().text_color(cx.theme().danger).child(reason))
                }),
        )
        .child(render_picker_select_control(
            ("composer-mcp-picker-toggle", key_id),
            is_selected,
            !row.selectable,
            move |_, _, cx| {
                picker_state_for_toggle.update(cx, |state, cx| {
                    state.toggle_mcp_selected(&row_for_toggle, cx);
                });
            },
            cx,
        ))
        .when(can_load_tools, |this| {
            this.child(
                Button::new(("composer-mcp-load-tools", server_id_hash))
                    .small()
                    .compact()
                    .ghost()
                    .icon(if is_active_server {
                        IconName::ChevronUp
                    } else {
                        IconName::ChevronDown
                    })
                    .disabled(is_loading_tools)
                    .on_click({
                        let picker_state = picker_state.clone();
                        let desktop_entity = desktop_entity.clone();
                        move |_, _, cx| {
                            cx.stop_propagation();
                            let should_load = picker_state.update(cx, |state, cx| {
                                state.toggle_mcp_tools(server_id.as_str(), cx)
                            });
                            if should_load {
                                let _ = desktop_entity.update(cx, |view, cx| {
                                    view.load_composer_mcp_picker_tools(
                                        server_id.clone(),
                                        picker_state.clone(),
                                        cx,
                                    );
                                });
                            }
                        }
                    }),
            )
        })
        .into_any_element()
}

fn stable_picker_row_id(value: &str) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn empty_picker_state(message: String, _cx: &mut App) -> AnyElement {
    div()
        .w_full()
        .h(px(120.))
        .flex()
        .items_center()
        .justify_center()
        .text_sm()
        .opacity(0.6)
        .child(message)
        .into_any_element()
}

fn picker_status_banner(message: String, _cx: &mut App) -> AnyElement {
    div()
        .p_2()
        .text_sm()
        .opacity(0.65)
        .child(message)
        .into_any_element()
}

fn picker_error_banner(message: String, cx: &mut App) -> AnyElement {
    div()
        .p_2()
        .text_sm()
        .text_color(cx.theme().danger)
        .child(message)
        .into_any_element()
}

fn picker_error_state(message: String, cx: &mut App) -> AnyElement {
    div()
        .w_full()
        .min_h(px(120.))
        .flex()
        .items_center()
        .justify_center()
        .p_3()
        .text_sm()
        .text_color(cx.theme().danger)
        .child(message)
        .into_any_element()
}

fn skill_capability_unavailable_reason_label(
    reason: Option<&SkillCapabilityUnavailableReason>,
) -> Option<String> {
    match reason? {
        SkillCapabilityUnavailableReason::DisabledByPolicy => {
            Some(t!("chat.composer.capability_picker.disabled_by_policy").to_string())
        }
        SkillCapabilityUnavailableReason::Inactive { status_reason } => {
            status_reason.clone().or_else(|| {
                Some(t!("chat.composer.capability_picker.blocked_by_runtime_checks").to_string())
            })
        }
    }
}

fn mcp_capability_unavailable_reason_label(
    reason: Option<&McpCapabilityUnavailableReason>,
) -> Option<String> {
    match reason? {
        McpCapabilityUnavailableReason::DisabledByPolicy => {
            Some(t!("chat.composer.capability_picker.disabled_by_policy").to_string())
        }
        McpCapabilityUnavailableReason::RuntimeUnavailable => {
            Some(t!("chat.composer.capability_picker.runtime_unavailable").to_string())
        }
        McpCapabilityUnavailableReason::RuntimeNotReady => {
            Some(t!("chat.composer.capability_picker.runtime_not_ready").to_string())
        }
        McpCapabilityUnavailableReason::NoToolCatalog => {
            Some(t!("chat.composer.capability_picker.no_tool_catalog").to_string())
        }
    }
}

fn mcp_capability_row_description(row: &SelectableMcpCapability) -> String {
    if let Some(count) = row.tools_count {
        return t!("chat.composer.capability_picker.tools_count", count = count).to_string();
    }

    row.description.clone()
}

fn selected_mcp_composer_capabilities(state: &CapabilityPickerState) -> Vec<ComposerCapability> {
    composer_capabilities::selected_mcp_composer_capabilities_from_rows(
        state.mcp_server_rows.as_slice(),
        state.mcp_tool_rows.as_slice(),
        &state.selected,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::SkillId;

    fn skill_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("skill id")
    }

    fn pack_id(character: char) -> SkillPackId {
        SkillPackId::new(character.to_string().repeat(21)).expect("pack id")
    }

    fn selectable_skill(character: char) -> SelectableSkillCapability {
        let skill_id = skill_id(character);
        SelectableSkillCapability {
            key: pioneer_protocol::skill_capability_key(&skill_id),
            skill_id,
            label: character.to_string(),
            display_name: character.to_string(),
            description: String::new(),
            owner: None,
            slug: character.to_string(),
            source_kind: "user".to_owned(),
            selectable: true,
            unavailable_reason: None,
        }
    }

    fn picker() -> ComposerSkillPickerProjection {
        let pack_id = pack_id('P');
        ComposerSkillPickerProjection {
            standalone: vec![selectable_skill('S')],
            packs: vec![SelectableSkillPackCapability {
                key: format!("skill_pack:{pack_id}"),
                pack_id: pack_id.clone(),
                label: "Pack".to_owned(),
                children: vec![SelectablePackedSkillCapability {
                    pack_id,
                    member_key: "child".to_owned(),
                    skill: selectable_skill('C'),
                }],
                selectable: true,
            }],
        }
    }

    #[::core::prelude::v1::test]
    fn desktop_picker_uses_shared_full_to_partial_transition_with_stable_ids() {
        let picker = picker();
        let full = ComposerSkillSelection::SkillPack {
            pack_id: pack_id('P'),
        };
        let child = ComposerSkillSelection::Skill {
            skill_id: skill_id('C'),
            pack_id: Some(pack_id('P')),
        };

        let selected = reduce_desktop_skill_picker_selection(&[], &picker, full);
        let partial = reduce_desktop_skill_picker_selection(&selected, &picker, child.clone());

        assert_eq!(partial, vec![child]);
    }

    #[::core::prelude::v1::test]
    fn desktop_picker_keeps_manual_all_children_partial() {
        let mut picker = picker();
        let pack_id = pack_id('P');
        picker.packs[0]
            .children
            .push(SelectablePackedSkillCapability {
                pack_id: pack_id.clone(),
                member_key: "second".to_owned(),
                skill: selectable_skill('D'),
            });
        let first = ComposerSkillSelection::Skill {
            skill_id: skill_id('C'),
            pack_id: Some(pack_id.clone()),
        };
        let second = ComposerSkillSelection::Skill {
            skill_id: skill_id('D'),
            pack_id: Some(pack_id),
        };

        let selected = reduce_desktop_skill_picker_selection(&[], &picker, first);
        let selected = reduce_desktop_skill_picker_selection(&selected, &picker, second);

        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .all(|selection| matches!(selection, ComposerSkillSelection::Skill { .. }))
        );
    }
}
