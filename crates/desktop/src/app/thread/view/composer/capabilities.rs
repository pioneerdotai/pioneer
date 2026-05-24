use crate::{
    app::root::{
        ComposerCapability, ComposerCapabilityKind, GatewayConnectionState, PioneerDesktop,
    },
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
use pioneer_protocol::{
    McpListItem, McpListParams, McpRuntimeState, McpScopeKind, McpServerDetailsParams,
    McpServerDetailsResponse, SkillListItem, SkillListParams,
};
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SelectableSkillCapability {
    pub(super) key: String,
    pub(super) label: String,
    pub(super) description: String,
    pub(super) slug: String,
    pub(super) source_kind: String,
    pub(super) selectable: bool,
    pub(super) unavailable_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SelectableMcpCapability {
    pub(super) key: String,
    pub(super) label: String,
    pub(super) description: String,
    pub(super) server_id: String,
    pub(super) server_name: String,
    pub(super) raw_tool_name: Option<String>,
    pub(super) scope_kind: McpScopeKind,
    pub(super) selectable: bool,
    pub(super) unavailable_reason: Option<String>,
}

struct CapabilityPickerState {
    search: Entity<InputState>,
    selected: HashSet<String>,
    skill_rows: Vec<SelectableSkillCapability>,
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
            skill_rows: Vec::new(),
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
        self.search.read(cx).value().trim().to_ascii_lowercase()
    }

    fn toggle_selected(&mut self, key: &str, cx: &mut Context<Self>) {
        if !self.selected.remove(key) {
            self.selected.insert(key.to_owned());
        }
        cx.notify();
    }

    fn toggle_mcp_selected(&mut self, row: &SelectableMcpCapability, cx: &mut Context<Self>) {
        if row.raw_tool_name.is_none() {
            if self.selected.remove(row.key.as_str()) {
                cx.notify();
                return;
            }

            self.selected.insert(row.key.clone());
            self.active_mcp_server_id = None;
            for tool_row in self
                .mcp_tool_rows
                .iter()
                .filter(|tool_row| tool_row.server_id == row.server_id)
            {
                self.selected.remove(tool_row.key.as_str());
            }
            cx.notify();
            return;
        }

        if !self.selected.remove(row.key.as_str()) {
            self.selected.insert(row.key.clone());
            if let Some(server_row) = self
                .mcp_server_rows
                .iter()
                .find(|server_row| server_row.server_id == row.server_id)
            {
                self.selected.remove(server_row.key.as_str());
            }
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
        rows: Vec<SelectableSkillCapability>,
        cx: &mut Context<Self>,
    ) {
        self.skill_rows = rows;
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
        self.mcp_server_rows = rows;
        self.mcp_server_loading = false;
        self.mcp_server_error = None;
        cx.notify();
    }

    fn fail_loading_mcp_servers(&mut self, error: String, cx: &mut Context<Self>) {
        self.mcp_server_loading = false;
        self.mcp_server_error = Some(error);
        cx.notify();
    }

    fn mcp_tools_loaded(&self, server_id: &str) -> bool {
        self.mcp_tool_rows
            .iter()
            .any(|row| row.server_id.as_str() == server_id)
    }

    fn toggle_mcp_tools(&mut self, server_id: &str, cx: &mut Context<Self>) -> bool {
        if self
            .mcp_server_rows
            .iter()
            .any(|row| row.server_id.as_str() == server_id && self.selected.contains(&row.key))
        {
            self.active_mcp_server_id = None;
            cx.notify();
            return false;
        }

        if self.active_mcp_server_id.as_deref() == Some(server_id) {
            self.active_mcp_server_id = None;
            cx.notify();
            return false;
        }

        if self.mcp_tools_loaded(server_id)
            || self.mcp_tool_loading_server_id.as_deref() == Some(server_id)
        {
            self.active_mcp_server_id = Some(server_id.to_owned());
            self.mcp_tool_error = None;
            cx.notify();
            return false;
        }

        true
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
        self.mcp_tool_rows
            .retain(|row| row.server_id.as_str() != server_id);
        self.mcp_tool_rows.extend(rows);
        if self.mcp_tool_loading_server_id.as_deref() == Some(server_id) {
            self.mcp_tool_loading_server_id = None;
        }
        self.mcp_tool_error = None;
        cx.notify();
    }

    fn merge_mcp_tool_rows(&mut self, rows: Vec<SelectableMcpCapability>, cx: &mut Context<Self>) {
        let server_ids = rows
            .iter()
            .map(|row| row.server_id.clone())
            .collect::<HashSet<_>>();
        if server_ids.is_empty() {
            return;
        }

        self.mcp_tool_rows
            .retain(|row| !server_ids.contains(row.server_id.as_str()));
        self.mcp_tool_rows.extend(rows);
        cx.notify();
    }

    fn fail_loading_mcp_tools(&mut self, server_id: &str, error: String, cx: &mut Context<Self>) {
        if self.mcp_tool_loading_server_id.as_deref() == Some(server_id) {
            self.mcp_tool_loading_server_id = None;
        }
        self.mcp_tool_error = Some(error);
        cx.notify();
    }
}

impl PioneerDesktop {
    pub(super) fn open_composer_skills_picker(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let desktop_entity = cx.entity().clone();
        let rows = self.selectable_skill_capabilities("");
        let picker_state = cx.new(|cx| {
            let mut state = CapabilityPickerState::new(
                window,
                cx,
                t!("chat.composer.capability_picker.search_skills").to_string(),
            );
            state.skill_rows = rows;
            state
        });
        self.load_composer_skill_picker_rows(picker_state.clone(), cx);

        window.open_dialog(cx, move |dialog, window, cx| {
            picker_state.update(cx, |state, cx| state.focus_search_once(window, cx));
            let (rows, selected, loading, error) = {
                let state = picker_state.read(cx);
                let query = state.query(cx);
                (
                    filter_selectable_skill_capability_rows(
                        state.skill_rows.as_slice(),
                        query.as_str(),
                    ),
                    state.selected.clone(),
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
                    move |_, _, _, cx| {
                        let capabilities =
                            selected_skill_composer_capabilities(&picker_state.read(cx));

                        vec![
                            default_outline_button("composer-skills-cancel")
                                .label(t!("buttons.cancel").to_string())
                                .outline()
                                .on_click(|_, window, cx| window.close_dialog(cx))
                                .into_any_element(),
                            default_primary_button("composer-skills-save")
                                .label(t!("buttons.add").to_string())
                                .disabled(capabilities.is_empty())
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    move |_, window, cx| {
                                        if capabilities.is_empty() {
                                            return;
                                        }
                                        let _ = desktop_entity.update(cx, |view, cx| {
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
                        .child(render_skill_rows(
                            rows,
                            selected,
                            loading,
                            error,
                            picker_state.clone(),
                            cx,
                        )),
                )
        });
    }

    pub(super) fn open_composer_mcp_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) {
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
                state.active_mcp_server_id = Some(details.server.id.clone());
                state.mcp_tool_rows = filter_mcp_tool_capability_rows(&details, "");
            }
            state
        });
        self.load_composer_mcp_picker_servers(picker_state.clone(), cx);

        window.open_dialog(cx, move |dialog, window, cx| {
            picker_state.update(cx, |state, cx| state.focus_search_once(window, cx));
            let (
                server_rows,
                tool_rows,
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
                let has_query = !query.is_empty();
                let active_server_id = state.active_mcp_server_id.clone();
                let selected_server_ids = state
                    .mcp_server_rows
                    .iter()
                    .filter(|row| state.selected.contains(row.key.as_str()) && row.selectable)
                    .map(|row| row.server_id.clone())
                    .collect::<HashSet<_>>();
                let active_server_selected = active_server_id
                    .as_deref()
                    .is_some_and(|server_id| selected_server_ids.contains(server_id));
                let loaded_tool_server_ids = state
                    .mcp_tool_rows
                    .iter()
                    .map(|row| row.server_id.clone())
                    .collect::<HashSet<_>>();
                (
                    filter_selectable_mcp_capability_rows(
                        state.mcp_server_rows.as_slice(),
                        query.as_str(),
                    ),
                    if has_query {
                        filter_search_mcp_tool_capability_rows(
                            state.mcp_tool_rows.as_slice(),
                            &selected_server_ids,
                            query.as_str(),
                        )
                    } else if active_server_selected {
                        Vec::new()
                    } else {
                        filter_active_mcp_tool_capability_rows(
                            state.mcp_tool_rows.as_slice(),
                            active_server_id.as_deref(),
                            query.as_str(),
                        )
                    },
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
                                    move |_, window, cx| {
                                        if capabilities.is_empty() {
                                            return;
                                        }
                                        let _ = desktop_entity.update(cx, |view, cx| {
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

    pub(super) fn selectable_skill_capabilities(
        &self,
        query: &str,
    ) -> Vec<SelectableSkillCapability> {
        filter_skill_capability_rows(self.installed_skills.as_slice(), query)
    }

    pub(super) fn selectable_mcp_server_capabilities(
        &self,
        query: &str,
    ) -> Vec<SelectableMcpCapability> {
        let query = query.trim().to_ascii_lowercase();
        let mut rows = self
            .mcp_servers
            .iter()
            .map(selectable_mcp_server_from_item)
            .filter(|row| {
                query.is_empty()
                    || row.label.to_ascii_lowercase().contains(query.as_str())
                    || row
                        .server_name
                        .to_ascii_lowercase()
                        .contains(query.as_str())
                    || row
                        .description
                        .to_ascii_lowercase()
                        .contains(query.as_str())
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.label
                .to_ascii_lowercase()
                .cmp(&right.label.to_ascii_lowercase())
                .then_with(|| left.key.cmp(&right.key))
        });
        rows
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

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.skills_list(SkillListParams {
                            workspace_id,
                            include_health: true,
                            include_policy: true,
                        })
                    })
                    .await;

                let _ = cx.update(|cx| {
                    picker_state.update(cx, |state, cx| match result {
                        Ok(response) => {
                            let installed = response
                                .skills
                                .into_iter()
                                .filter(|skill| skill.install.installed)
                                .collect::<Vec<_>>();
                            state.finish_loading_skills(
                                filter_skill_capability_rows(installed.as_slice(), ""),
                                cx,
                            );
                        }
                        Err(error) => {
                            state.fail_loading_skills(
                                format!("{}: {error:#}", t!("skills.error.load_failed")),
                                cx,
                            );
                        }
                    });
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
                    .background_spawn(
                        async move { ws_sender.mcp_list(McpListParams { workspace_id }) },
                    )
                    .await;

                let prefetch_server_ids = match result {
                    Ok(response) => {
                        let rows = response
                            .servers
                            .iter()
                            .map(selectable_mcp_server_from_item)
                            .collect::<Vec<_>>();
                        let prefetch_server_ids = rows
                            .iter()
                            .filter(|row| row.selectable)
                            .map(|row| row.server_id.clone())
                            .collect::<Vec<_>>();
                        let _ = cx.update(|cx| {
                            picker_state.update(cx, |state, cx| {
                                state.finish_loading_mcp_servers(
                                    filter_selectable_mcp_capability_rows(rows.as_slice(), ""),
                                    cx,
                                );
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
                            let details =
                                prefetch_ws_sender.mcp_server_details(McpServerDetailsParams {
                                    workspace_id: workspace_id_for_prefetch.clone(),
                                    server_id,
                                });
                            if let Ok(details) = details {
                                rows.extend(filter_mcp_tool_capability_rows(&details, ""));
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
                        ws_sender.mcp_server_details(McpServerDetailsParams {
                            workspace_id,
                            server_id: server_id_for_request,
                        })
                    })
                    .await;

                let _ = cx.update(|cx| {
                    picker_state.update(cx, |state, cx| match result {
                        Ok(response) => {
                            let rows = filter_mcp_tool_capability_rows(&response, "");
                            state.finish_loading_mcp_tools(server_id.as_str(), rows, cx);
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
    rows: Vec<SelectableSkillCapability>,
    selected: HashSet<String>,
    loading: bool,
    error: Option<String>,
    picker_state: Entity<CapabilityPickerState>,
    cx: &mut App,
) -> AnyElement {
    if rows.is_empty() {
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

    let row_count = rows.len();
    list.children(rows.into_iter().enumerate().map(|(row_index, row)| {
        let is_selected = selected.contains(row.key.as_str());
        let key = row.key.clone();
        let key_id = stable_picker_row_id(key.as_str());
        let is_selectable = row.selectable;
        let key_for_row = key.clone();
        let key_for_toggle = key.clone();
        let picker_state_for_row = picker_state.clone();
        let picker_state_for_toggle = picker_state.clone();
        let disabled_reason = row.unavailable_reason.clone();
        let row_element = h_flex()
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
            .when(is_selectable, |this| {
                this.cursor_pointer().on_click(move |_, _, cx| {
                    cx.stop_propagation();
                    picker_state_for_row.update(cx, |state, cx| {
                        state.toggle_selected(key_for_row.as_str(), cx);
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
                        state.toggle_selected(key_for_toggle.as_str(), cx);
                    });
                },
                cx,
            ));

        div()
            .w_full()
            .when(row_index + 1 < row_count, |this| this.pb_2())
            .child(row_element)
    }))
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

    let mut rows = v_flex()
        .max_h(px(420.))
        .overflow_y_scrollbar()
        .gap_2()
        .child(
            v_flex()
                .gap_1()
                .child(
                    div()
                        .text_xs()
                        .font_medium()
                        .opacity(0.6)
                        .child(t!("chat.composer.capability_picker.servers").to_string()),
                )
                .child(
                    v_flex()
                        .gap_2()
                        .children(server_rows.into_iter().map(|row| {
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
                        })),
                ),
        );

    if server_loading {
        rows = rows.child(picker_status_banner(
            t!("chat.composer.capability_picker.refreshing_mcp_servers").to_string(),
            cx,
        ));
    }

    if let Some(error) = server_error {
        rows = rows.child(picker_error_banner(error, cx));
    }

    if loading_server_id.is_some() && loading_server_id.as_deref() == active_server_id.as_deref() {
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

    if let Some(error) = tool_error {
        rows = rows.child(
            div()
                .p_2()
                .text_sm()
                .text_color(cx.theme().danger)
                .child(error),
        );
    }

    if !tool_rows.is_empty() {
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
    let disabled_reason = row.unavailable_reason.clone();
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
                            .child(row.description.clone()),
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

fn selected_skill_composer_capabilities(state: &CapabilityPickerState) -> Vec<ComposerCapability> {
    state
        .skill_rows
        .iter()
        .filter(|row| state.selected.contains(row.key.as_str()) && row.selectable)
        .map(|row| ComposerCapability {
            id: row.key.clone(),
            label: row.label.clone(),
            kind: ComposerCapabilityKind::Skill {
                slug: row.slug.clone(),
                source_kind: row.source_kind.clone(),
            },
        })
        .collect()
}

fn selected_mcp_composer_capabilities(state: &CapabilityPickerState) -> Vec<ComposerCapability> {
    selected_mcp_composer_capabilities_from_rows(
        state.mcp_server_rows.as_slice(),
        state.mcp_tool_rows.as_slice(),
        &state.selected,
    )
}

fn selected_mcp_composer_capabilities_from_rows(
    server_rows: &[SelectableMcpCapability],
    tool_rows: &[SelectableMcpCapability],
    selected: &HashSet<String>,
) -> Vec<ComposerCapability> {
    let selected_server_ids = server_rows
        .iter()
        .filter(|row| selected.contains(row.key.as_str()) && row.selectable)
        .map(|row| row.server_id.as_str())
        .collect::<HashSet<_>>();

    server_rows
        .iter()
        .chain(tool_rows.iter())
        .filter(|row| selected.contains(row.key.as_str()) && row.selectable)
        .filter(|row| {
            row.raw_tool_name.is_none() || !selected_server_ids.contains(row.server_id.as_str())
        })
        .cloned()
        .map(mcp_row_to_composer_capability)
        .collect()
}

fn filter_selectable_skill_capability_rows(
    rows: &[SelectableSkillCapability],
    query: &str,
) -> Vec<SelectableSkillCapability> {
    let query = query.trim().to_ascii_lowercase();
    rows.iter()
        .filter(|row| {
            query.is_empty()
                || row.label.to_ascii_lowercase().contains(query.as_str())
                || row.slug.to_ascii_lowercase().contains(query.as_str())
                || row
                    .description
                    .to_ascii_lowercase()
                    .contains(query.as_str())
        })
        .cloned()
        .collect()
}

fn filter_selectable_mcp_capability_rows(
    rows: &[SelectableMcpCapability],
    query: &str,
) -> Vec<SelectableMcpCapability> {
    let query = query.trim().to_ascii_lowercase();
    rows.iter()
        .filter(|row| {
            query.is_empty()
                || row.label.to_ascii_lowercase().contains(query.as_str())
                || row
                    .server_name
                    .to_ascii_lowercase()
                    .contains(query.as_str())
                || row
                    .raw_tool_name
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains(query.as_str())
                || row
                    .description
                    .to_ascii_lowercase()
                    .contains(query.as_str())
        })
        .cloned()
        .collect()
}

fn filter_active_mcp_tool_capability_rows(
    rows: &[SelectableMcpCapability],
    active_server_id: Option<&str>,
    query: &str,
) -> Vec<SelectableMcpCapability> {
    let Some(active_server_id) = active_server_id else {
        return Vec::new();
    };
    let active_rows = rows
        .iter()
        .filter(|row| row.server_id.as_str() == active_server_id)
        .cloned()
        .collect::<Vec<_>>();
    filter_selectable_mcp_capability_rows(active_rows.as_slice(), query)
}

fn filter_search_mcp_tool_capability_rows(
    rows: &[SelectableMcpCapability],
    selected_server_ids: &HashSet<String>,
    query: &str,
) -> Vec<SelectableMcpCapability> {
    filter_selectable_mcp_capability_rows(rows, query)
        .into_iter()
        .filter(|row| {
            row.raw_tool_name.is_some() && !selected_server_ids.contains(row.server_id.as_str())
        })
        .collect()
}

fn selectable_skill_from_item(skill: &SkillListItem) -> SelectableSkillCapability {
    let unavailable_reason = if !skill.policy.enabled {
        Some(t!("chat.composer.capability_picker.disabled_by_policy").to_string())
    } else if skill.status != "active" {
        skill.status_reason.clone().or_else(|| {
            Some(t!("chat.composer.capability_picker.blocked_by_runtime_checks").to_string())
        })
    } else {
        None
    };
    let key = ComposerCapabilityKind::Skill {
        slug: skill.slug.clone(),
        source_kind: skill.source_kind.clone(),
    }
    .key();

    SelectableSkillCapability {
        key,
        label: skill.display_name.clone(),
        description: skill.description.clone(),
        slug: skill.slug.clone(),
        source_kind: skill.source_kind.clone(),
        selectable: unavailable_reason.is_none(),
        unavailable_reason,
    }
}

fn filter_skill_capability_rows(
    skills: &[SkillListItem],
    query: &str,
) -> Vec<SelectableSkillCapability> {
    let query = query.trim().to_ascii_lowercase();
    let mut rows = skills
        .iter()
        .map(selectable_skill_from_item)
        .filter(|row| {
            query.is_empty()
                || row.label.to_ascii_lowercase().contains(query.as_str())
                || row.slug.to_ascii_lowercase().contains(query.as_str())
                || row
                    .description
                    .to_ascii_lowercase()
                    .contains(query.as_str())
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.label
            .to_ascii_lowercase()
            .cmp(&right.label.to_ascii_lowercase())
            .then_with(|| left.key.cmp(&right.key))
    });
    rows
}

fn selectable_mcp_server_from_item(server: &McpListItem) -> SelectableMcpCapability {
    let unavailable_reason = mcp_server_unavailable_reason(server);
    let label = server
        .display_name
        .clone()
        .unwrap_or_else(|| server.name.clone());
    let key = ComposerCapabilityKind::McpServer {
        name: server.name.clone(),
        scope_kind: server.scope,
    }
    .key();

    SelectableMcpCapability {
        key,
        label,
        description: t!(
            "chat.composer.capability_picker.tools_count",
            count = server.tools_count
        )
        .to_string(),
        server_id: server.id.clone(),
        server_name: server.name.clone(),
        raw_tool_name: None,
        scope_kind: server.scope,
        selectable: unavailable_reason.is_none(),
        unavailable_reason,
    }
}

fn mcp_server_unavailable_reason(server: &McpListItem) -> Option<String> {
    if !server.policy.enabled {
        return Some(t!("chat.composer.capability_picker.disabled_by_policy").to_string());
    }
    if !server.runtime.live {
        return Some(t!("chat.composer.capability_picker.runtime_unavailable").to_string());
    }
    if !matches!(
        server.runtime.state,
        McpRuntimeState::Ready | McpRuntimeState::Degraded
    ) {
        return Some(t!("chat.composer.capability_picker.runtime_not_ready").to_string());
    }
    if server.tools_count == 0 {
        return Some(t!("chat.composer.capability_picker.no_tool_catalog").to_string());
    }
    None
}

fn filter_mcp_tool_capability_rows(
    details: &McpServerDetailsResponse,
    query: &str,
) -> Vec<SelectableMcpCapability> {
    let query = query.trim().to_ascii_lowercase();
    let server = &details.server;
    let server_label = server
        .display_name
        .clone()
        .unwrap_or_else(|| server.name.clone());
    let server_selectable = mcp_server_unavailable_reason(server).is_none();
    let mut rows = details
        .catalog
        .tools
        .iter()
        .map(|tool| {
            let label = tool
                .title
                .clone()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| tool.name.clone());
            let description = tool.description.clone().unwrap_or_default();
            let unavailable_reason = if server_selectable {
                None
            } else {
                mcp_server_unavailable_reason(server)
            };
            let key = ComposerCapabilityKind::McpTool {
                server_name: server.name.clone(),
                raw_tool_name: tool.name.clone(),
                scope_kind: server.scope,
            }
            .key();
            SelectableMcpCapability {
                key,
                label: format!("{server_label}/{label}"),
                description,
                server_id: server.id.clone(),
                server_name: server.name.clone(),
                raw_tool_name: Some(tool.name.clone()),
                scope_kind: server.scope,
                selectable: unavailable_reason.is_none(),
                unavailable_reason,
            }
        })
        .filter(|row| {
            query.is_empty()
                || row.label.to_ascii_lowercase().contains(query.as_str())
                || row
                    .raw_tool_name
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains(query.as_str())
                || row
                    .description
                    .to_ascii_lowercase()
                    .contains(query.as_str())
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.label
            .to_ascii_lowercase()
            .cmp(&right.label.to_ascii_lowercase())
            .then_with(|| left.key.cmp(&right.key))
    });
    rows
}

fn mcp_row_to_composer_capability(row: SelectableMcpCapability) -> ComposerCapability {
    let kind = match row.raw_tool_name {
        Some(raw_tool_name) => ComposerCapabilityKind::McpTool {
            server_name: row.server_name,
            raw_tool_name,
            scope_kind: row.scope_kind,
        },
        None => ComposerCapabilityKind::McpServer {
            name: row.server_name,
            scope_kind: row.scope_kind,
        },
    };
    ComposerCapability {
        id: row.key,
        label: row.label,
        kind,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        filter_mcp_tool_capability_rows, filter_search_mcp_tool_capability_rows,
        filter_skill_capability_rows, mcp_row_to_composer_capability,
        mcp_server_unavailable_reason, selectable_mcp_server_from_item, selectable_skill_from_item,
        selected_mcp_composer_capabilities_from_rows,
    };
    use pioneer_protocol::{
        McpListItem, McpPolicyState, McpRuntimeState, McpRuntimeStatus, McpScopeKind,
        McpServerCatalogDetails, McpServerDetailsResponse, McpServerHealthDetails, McpServerStatus,
        McpSourceKind, McpToolCatalogItem, McpTransportSummary, SkillHealthSummary,
        SkillInstallState, SkillListItem, SkillPolicyState,
    };
    use std::collections::HashSet;

    #[test]
    fn selectable_skill_preserves_disabled_reason() {
        let mut item = skill_item("tests/example");
        item.policy.enabled = false;

        let row = selectable_skill_from_item(&item);

        assert!(!row.selectable);
        assert_eq!(
            row.unavailable_reason.as_deref(),
            Some("Disabled by policy")
        );
    }

    #[test]
    fn selectable_skill_rows_filter_by_label_slug_and_description() {
        let mut docs = skill_item("tests/docs-writer");
        docs.display_name = "Docs Writer".to_owned();
        docs.description = "Creates release notes".to_owned();
        let mut image = skill_item("tests/imagegen");
        image.display_name = "Imagegen".to_owned();
        image.description = "Generate bitmap assets".to_owned();

        let rows = filter_skill_capability_rows(&[image.clone(), docs.clone()], "release");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].slug, "tests/docs-writer");

        let rows = filter_skill_capability_rows(&[image, docs], "imagegen");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, "skill:user:tests/imagegen");
        assert!(rows[0].selectable);
    }

    #[test]
    fn mcp_server_requires_live_runtime_and_catalog() {
        let mut server = mcp_server("server-a");
        server.runtime.live = false;
        assert_eq!(
            mcp_server_unavailable_reason(&server).as_deref(),
            Some("Runtime unavailable")
        );

        server.runtime.live = true;
        server.tools_count = 0;
        assert_eq!(
            mcp_server_unavailable_reason(&server).as_deref(),
            Some("No tool catalog available")
        );
    }

    #[test]
    fn mcp_tool_rows_filter_and_convert_to_tool_capability() {
        let details = mcp_details("browser", vec![mcp_tool("open", "Open page")]);

        let rows = filter_mcp_tool_capability_rows(&details, "page");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, "mcp-tool:workspace:browser:open");
        assert_eq!(rows[0].raw_tool_name.as_deref(), Some("open"));
        assert!(rows[0].selectable);

        let capability = mcp_row_to_composer_capability(rows[0].clone());
        assert_eq!(capability.id, "mcp-tool:workspace:browser:open");
    }

    #[test]
    fn mcp_search_can_match_tools_from_unopened_servers() {
        let browser = mcp_details("browser", vec![mcp_tool("open", "Open page")]);
        let resend = mcp_details("resend", vec![mcp_tool("add_contact", "Add contact")]);
        let mut rows = filter_mcp_tool_capability_rows(&browser, "");
        rows.extend(filter_mcp_tool_capability_rows(&resend, ""));

        let filtered = filter_search_mcp_tool_capability_rows(&rows, &HashSet::new(), "contact");

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].server_name, "resend");
        assert_eq!(filtered[0].raw_tool_name.as_deref(), Some("add_contact"));
    }

    #[test]
    fn selected_mcp_capabilities_skip_tools_when_server_is_selected() {
        let server = selectable_mcp_server_from_item(&mcp_server("resend"));
        let details = mcp_details(
            "resend",
            vec![mcp_tool("add_contact", "Add contact to audience")],
        );
        let tool = filter_mcp_tool_capability_rows(&details, "contact")
            .into_iter()
            .next()
            .expect("test tool should match query");
        let mut selected = HashSet::new();
        selected.insert(server.key.clone());
        selected.insert(tool.key.clone());

        let capabilities =
            selected_mcp_composer_capabilities_from_rows(&[server], &[tool], &selected);

        assert_eq!(capabilities.len(), 1);
        assert_eq!(capabilities[0].id, "mcp-server:workspace:resend");
    }

    fn skill_item(slug: &str) -> SkillListItem {
        SkillListItem {
            slug: slug.to_owned(),
            source_kind: "user".to_owned(),
            display_name: "Example".to_owned(),
            description: "Example skill".to_owned(),
            version: None,
            fingerprint: "fingerprint".to_owned(),
            trust_level: "community".to_owned(),
            install: SkillInstallState {
                managed: false,
                installed: true,
                install_path: None,
                updated_at: None,
            },
            policy: SkillPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            health: SkillHealthSummary {
                status: "ok".to_owned(),
                dependency_failures: Vec::new(),
                security_blocks: Vec::new(),
                validation_issues: Vec::new(),
            },
            status: "active".to_owned(),
            status_reason: None,
        }
    }

    fn mcp_server(name: &str) -> McpListItem {
        McpListItem {
            id: format!("mcp:{name}"),
            name: name.to_owned(),
            display_name: None,
            scope: McpScopeKind::Workspace,
            source_kind: McpSourceKind::Config,
            transport: McpTransportSummary::Stdio {
                command: "server".to_owned(),
            },
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            fingerprint: "fingerprint".to_owned(),
            runtime: McpRuntimeStatus {
                state: McpRuntimeState::Ready,
                live: true,
                last_seen_at: None,
                last_error: None,
            },
            tools_count: 1,
            resources_count: 0,
            resource_templates_count: 0,
            prompts_count: 0,
            status: McpServerStatus::Ready,
            status_reason: None,
        }
    }

    fn mcp_tool(name: &str, description: &str) -> McpToolCatalogItem {
        McpToolCatalogItem {
            name: name.to_owned(),
            title: None,
            description: Some(description.to_owned()),
            input_schema_summary: None,
            annotations: None,
        }
    }

    fn mcp_details(server_name: &str, tools: Vec<McpToolCatalogItem>) -> McpServerDetailsResponse {
        let mut server = mcp_server(server_name);
        server.tools_count = tools.len();
        McpServerDetailsResponse {
            snapshot_version: 1,
            generated_at: 1_700_000_000,
            server: server.clone(),
            catalog: McpServerCatalogDetails {
                catalog_version: Some("catalog-v1".to_owned()),
                generated_at: Some(1_700_000_000),
                server_info: serde_json::json!({ "name": server.name }),
                server_instructions_hash: None,
                tools,
                resources: Vec::new(),
                resource_templates: Vec::new(),
                prompts: Vec::new(),
            },
            health: McpServerHealthDetails {
                runtime: server.runtime.clone(),
                status: server.status,
                status_reason: server.status_reason.clone(),
                last_error: None,
                retry_attempt: None,
                next_retry_at: None,
                catalog_version: Some("catalog-v1".to_owned()),
                stderr_tail: None,
            },
            audit: Vec::new(),
            recent_bindings: Vec::new(),
        }
    }
}
