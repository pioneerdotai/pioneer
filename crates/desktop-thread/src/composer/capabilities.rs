use crate::buttons::default_outline_button;
use crate::buttons::default_primary_button;
use crate::composer::ComposerCapability;
use crate::composer::ComposerView;
use gpui_kit::component::WindowExt;
use gpui_kit::component::button::*;
use gpui_kit::component::dialog::DialogFooter;
use gpui_kit::component::form::field;
use gpui_kit::component::form::v_form;
use gpui_kit::component::input::Input;
use gpui_kit::component::input::InputState;
use gpui_kit::component::scroll::ScrollableElement;
use gpui_kit::component::theme::ActiveTheme;
use gpui_kit::component::*;
use gpui_kit::prelude::*;
use gpui_kit::*;
use pioneer_client::composer::capabilities as composer_capabilities;
use pioneer_client::composer::capabilities::McpCapabilityUnavailableReason;
use pioneer_client::composer::capabilities::SelectableMcpCapability;
use pioneer_client::composer::capabilities::SelectableSkillCapability;
use pioneer_client::composer::capabilities::SkillCapabilityUnavailableReason;
use pioneer_client::composer::skill_selection::ComposerSkillPickerProjection;
use pioneer_client::composer::skill_selection::ComposerSkillSelection;
use pioneer_client::composer::skill_selection::SelectablePackedSkillCapability;
use pioneer_client::composer::skill_selection::SelectableSkillPackCapability;
use pioneer_client::composer::skill_selection::project_composer_skill_picker;
#[cfg(test)]
use pioneer_client::composer::skill_selection::reduce_composer_skill_selection_toggle;
use pioneer_client::skills::catalog::SkillManagementProjection;
use pioneer_client::timeline::types::SkillPackId;
use std::collections::HashSet;

use pioneer_client::composer::catalog::ComposerCatalogIntent;
use pioneer_client::composer::catalog::ComposerCatalogKind;
use pioneer_client::composer::catalog::ComposerCatalogPublication;
use pioneer_client::composer::catalog::ComposerCatalogRequestState;
use pioneer_client::composer::catalog::ComposerPickerKind;
use pioneer_client::composer::catalog::ComposerPickerSelection;
use pioneer_client::composer::store::ComposerOperationIdentity;
use pioneer_client::core::ClientCore;
use pioneer_client::core::ClientPublicationReference;
use pioneer_client::core::ClientScope;
use pioneer_desktop_foundation::ClientBindingRegistrar;
use pioneer_desktop_foundation::ClientBindingRegistration;
use pioneer_desktop_foundation::ClientPublicationSink;
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

struct PickerBinding {
    scope: ClientScope,
    sequence: Cell<u64>,
    input: RefCell<Option<Arc<ComposerCatalogPublication>>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl ClientPublicationSink for PickerBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        if publication.scope() != &self.scope
            || publication.snapshot().sequence().get() <= self.sequence.get()
        {
            return;
        }
        self.sequence.set(publication.snapshot().sequence().get());
        let next = publication
            .typed::<ComposerCatalogPublication>()
            .map(|p| p.payload());
        if self
            .input
            .borrow()
            .as_ref()
            .zip(next.as_ref())
            .is_some_and(|(old, next)| old.revision > next.revision)
        {
            return;
        }
        *self.input.borrow_mut() = next;
        self.changed.send_modify(|v| *v = v.saturating_add(1));
    }
}
pub(crate) struct CapabilityPickerState {
    client: Arc<ClientCore>,
    identity: ComposerOperationIdentity,
    binding: Arc<PickerBinding>,
    registration: Option<ClientBindingRegistration>,
    task: Option<Task<()>>,
    _release: Subscription,
    dialog_open: Rc<Cell<bool>>,
    search: Entity<InputState>,
    expanded_skill_pack_ids: HashSet<SkillPackId>,
    active_mcp_server_id: Option<String>,
}
impl CapabilityPickerState {
    fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        identity: ComposerOperationIdentity,
        window: &mut Window,
        cx: &mut Context<Self>,
        placeholder: String,
    ) -> Self {
        let scope = ClientScope::ComposerCatalog {
            thread_id: identity.thread_id.clone(),
        };
        let binding = Arc::new(PickerBinding {
            scope: scope.clone(),
            sequence: Cell::new(0),
            input: RefCell::new(client.composer_catalog_snapshot(&identity.thread_id)),
            changed: tokio::sync::watch::channel(0).0,
        });
        let mut changes = binding.changed.subscribe();
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        let registration = registrar.register(scope, Arc::downgrade(&sink));
        let task = cx.spawn_in(window, async move |view, cx| {
            while changes.changed().await.is_ok() {
                let _ = *changes.borrow_and_update();
                if view
                    .update_in(cx, |view, window, cx| {
                        if view
                            .input()
                            .as_ref()
                            .and_then(|p| p.session.as_ref())
                            .is_none_or(|session| session.identity != view.identity)
                        {
                            view.close(window, cx);
                        }
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let release = cx.on_release_in(window, |view, window, cx| view.close(window, cx));
        Self {
            client,
            identity,
            binding,
            registration: Some(registration),
            task: Some(task),
            _release: release,
            dialog_open: Rc::new(Cell::new(false)),
            search: cx.new(|cx| InputState::new(window, cx).placeholder(placeholder)),
            expanded_skill_pack_ids: HashSet::new(),
            active_mcp_server_id: None,
        }
    }
    fn input(&self) -> Option<Arc<ComposerCatalogPublication>> {
        self.binding.input.borrow().clone()
    }
    fn send(&mut self, intent: ComposerCatalogIntent, cx: &mut Context<Self>) {
        self.client.composer_catalog_intent(intent);
        *self.binding.input.borrow_mut() = self
            .client
            .composer_catalog_snapshot(&self.identity.thread_id);
        cx.notify();
    }
    fn retire(&mut self) {
        self.client
            .composer_catalog_intent(ComposerCatalogIntent::ClosePicker {
                identity: self.identity.clone(),
            });
        self.task.take();
        self.registration.take();
    }
    pub(super) fn close(&mut self, window: &mut Window, cx: &mut App) {
        self.retire();
        if self.dialog_open.replace(false) {
            window.close_dialog(cx);
        }
    }
    fn commit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.send(
            ComposerCatalogIntent::CommitPicker {
                identity: self.identity.clone(),
            },
            cx,
        );
        self.close(window, cx);
    }
    fn query(&self, cx: &App) -> String {
        self.search.read(cx).value().to_string()
    }
    fn skill_management(&self) -> SkillManagementProjection {
        self.input().map(|p| p.skills.clone()).unwrap_or_default()
    }
    fn skill_selections(&self) -> Vec<ComposerSkillSelection> {
        self.input()
            .and_then(|p| {
                p.session.as_ref().and_then(|s| match &s.selection {
                    ComposerPickerSelection::Skills { selections } => Some(selections.clone()),
                    _ => None,
                })
            })
            .unwrap_or_default()
    }
    fn selected(&self) -> HashSet<String> {
        self.input()
            .and_then(|p| {
                p.session.as_ref().and_then(|s| match &s.selection {
                    ComposerPickerSelection::Mcp { selected } => {
                        Some(selected.iter().cloned().collect())
                    }
                    _ => None,
                })
            })
            .unwrap_or_default()
    }
    fn skill_loading(&self) -> bool {
        self.input()
            .is_some_and(|p| p.skill_request.state == ComposerCatalogRequestState::Loading)
    }
    fn skill_error(&self) -> Option<String> {
        self.input().and_then(|p| match &p.skill_request.state {
            ComposerCatalogRequestState::Failed { message } => {
                Some(if p.skill_request.transport_unavailable() {
                    t!("skills.error.gateway_not_connected").to_string()
                } else {
                    format!("{}: {message}", t!("skills.error.load_failed"))
                })
            }
            _ => None,
        })
    }
    fn mcp_server_rows(&self) -> Vec<SelectableMcpCapability> {
        self.input()
            .map(|p| p.mcp_servers.clone())
            .unwrap_or_default()
    }
    fn mcp_tool_rows(&self) -> Vec<SelectableMcpCapability> {
        self.input()
            .map(|p| p.mcp_tools.clone())
            .unwrap_or_default()
    }
    fn mcp_server_loading(&self) -> bool {
        self.input()
            .is_some_and(|p| p.mcp_request.state == ComposerCatalogRequestState::Loading)
    }
    fn mcp_server_error(&self) -> Option<String> {
        self.input().and_then(|p| match &p.mcp_request.state {
            ComposerCatalogRequestState::Failed { message } => {
                Some(if p.mcp_request.transport_unavailable() {
                    t!("mcp.error.gateway_not_connected").to_string()
                } else {
                    t!("mcp.error.load_servers_failed", error = message.as_str()).to_string()
                })
            }
            _ => None,
        })
    }
    fn mcp_tool_loading_server_id(&self) -> Option<String> {
        self.active_mcp_server_id
            .as_ref()
            .filter(|id| {
                self.input().is_some_and(|p| {
                    p.tool_requests
                        .get(*id)
                        .is_some_and(|r| r.state == ComposerCatalogRequestState::Loading)
                })
            })
            .cloned()
    }
    fn mcp_tool_error(&self) -> Option<String> {
        let id = self.active_mcp_server_id.as_ref()?;
        self.input().and_then(|p| {
            p.tool_requests.get(id).and_then(|r| match &r.state {
                ComposerCatalogRequestState::Failed { message } => {
                    Some(if r.transport_unavailable() {
                        t!("mcp.error.gateway_not_connected").to_string()
                    } else {
                        t!("mcp.error.load_details_failed", error = message.as_str()).to_string()
                    })
                }
                _ => None,
            })
        })
    }
    fn toggle_skill_selection(
        &mut self,
        _picker: &ComposerSkillPickerProjection,
        selection: ComposerSkillSelection,
        cx: &mut Context<Self>,
    ) {
        self.send(
            ComposerCatalogIntent::ToggleSkill {
                identity: self.identity.clone(),
                selection,
            },
            cx,
        );
    }
    fn toggle_skill_pack_expanded(&mut self, pack_id: SkillPackId, cx: &mut Context<Self>) {
        if !self.expanded_skill_pack_ids.remove(&pack_id) {
            self.expanded_skill_pack_ids.insert(pack_id);
        }
        cx.notify();
    }
    fn toggle_mcp_selected(&mut self, row: &SelectableMcpCapability, cx: &mut Context<Self>) {
        self.send(
            ComposerCatalogIntent::ToggleMcp {
                identity: self.identity.clone(),
                key: row.key.clone(),
            },
            cx,
        );
        if row.raw_tool_name.is_none() && self.selected().contains(&row.key) {
            self.active_mcp_server_id = None;
        }
    }
    fn toggle_mcp_tools(&mut self, server_id: &str, cx: &mut Context<Self>) {
        let selected = self.selected();
        let servers = self.mcp_server_rows();
        let tools = self.mcp_tool_rows();
        let loading = self.mcp_tool_loading_server_id();
        let mut error = self.mcp_tool_error();
        if composer_capabilities::toggle_mcp_tool_capability_panel(
            &selected,
            &servers,
            &tools,
            &mut self.active_mcp_server_id,
            &mut error,
            loading.as_deref(),
            server_id,
        ) {
            self.send(
                ComposerCatalogIntent::Retry {
                    thread_id: self.identity.thread_id.clone(),
                    draft_id: self.identity.draft_id,
                    catalog: ComposerCatalogKind::McpTools {
                        server_id: server_id.into(),
                    },
                },
                cx,
            );
        }
        cx.notify();
    }
}

impl Drop for CapabilityPickerState {
    fn drop(&mut self) {
        self.retire();
    }
}

#[cfg(test)]
fn reduce_desktop_skill_picker_selection(
    current: &[ComposerSkillSelection],
    picker: &ComposerSkillPickerProjection,
    selection: ComposerSkillSelection,
) -> Vec<ComposerSkillSelection> {
    reduce_composer_skill_selection_toggle(current, picker, selection).selections
}

impl ComposerView {
    fn new_capability_picker(
        &mut self,
        kind: ComposerPickerKind,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<Entity<CapabilityPickerState>> {
        let capabilities = self.principal_presentation_capabilities();
        if !self.can_start_active_thread_agent_presentation()
            || !match kind {
                ComposerPickerKind::Skills => capabilities.can_use_skills,
                ComposerPickerKind::Mcp => capabilities.can_use_mcp,
            }
        {
            return None;
        }
        if let Some(old) = self.capability_picker.take() {
            old.update(cx, |state, cx| state.close(window, cx));
        }
        let input = self.composer_input.as_ref()?;
        if self.current_active_thread_id() != Some(input.thread_id()) {
            return None;
        }
        let client = self.client.clone();
        let result = client.composer_catalog_intent(ComposerCatalogIntent::OpenPicker {
            thread_id: input.thread_id().into(),
            draft_id: input.draft_id(),
            picker: kind,
            deferred: true,
        });
        if result.outcome() != pioneer_client::core::ClientTransitionOutcome::Changed {
            return None;
        }
        let identity = client
            .composer_catalog_snapshot(input.thread_id())?
            .session
            .as_ref()?
            .identity
            .clone();
        let placeholder = match kind {
            ComposerPickerKind::Skills => {
                t!("chat.composer.capability_picker.search_skills").to_string()
            }
            ComposerPickerKind::Mcp => t!("chat.composer.capability_picker.search_mcp").to_string(),
        };
        let registrar = self.thread_bindings.registrar();
        let picker = cx.new(|cx| {
            CapabilityPickerState::new(client, registrar, identity, window, cx, placeholder)
        });
        self.capability_picker = Some(picker.clone());
        Some(picker)
    }
    pub(super) fn open_composer_skills_picker(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(picker_state) = self.new_capability_picker(ComposerPickerKind::Skills, window, cx)
        else {
            return;
        };
        let weak = picker_state.downgrade();
        window.open_dialog(cx, move |dialog, _window, cx| {
            let Some(picker_state) = weak.upgrade() else {
                return dialog;
            };
            let (picker, selections, expanded_pack_ids, loading, error) = {
                let state = picker_state.read(cx);
                let query = state.query(cx);
                (
                    project_composer_skill_picker(&state.skill_management(), query.as_str()),
                    state.skill_selections(),
                    state.expanded_skill_pack_ids.clone(),
                    state.skill_loading(),
                    state.skill_error(),
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
                .on_close({
                    let weak = picker_state.downgrade();
                    move |_, _, cx| {
                        let _ = weak.update(cx, |state, _| {
                            state.dialog_open.set(false);
                            state.retire();
                        });
                    }
                })
                .footer(DialogFooter::new().children(vec![
                        default_outline_button("composer-skills-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .on_click({
                                let weak = picker_state.downgrade();
                                move |_, window, cx| {
                                    let _ = weak.update(cx, |state, cx| state.close(window, cx));
                                }
                            })
                            .into_any_element(),
                        default_primary_button("composer-skills-save")
                            .label(t!("buttons.add").to_string())
                            .disabled(picker_state.read(cx).skill_selections().is_empty())
                            .on_click({
                                let weak = picker_state.downgrade();
                                move |_, window, cx| {
                                    let _ = weak.update(cx, |state, cx| state.commit(window, cx));
                                }
                            })
                            .into_any_element(),
                    ]))
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
                            picker_state.downgrade(),
                            cx,
                        )),
                )
        });
        picker_state.update(cx, |state, cx| {
            state.dialog_open.set(true);
            state.search.update(cx, |input, cx| input.focus(window, cx));
        });
    }

    pub(super) fn open_composer_mcp_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(picker_state) = self.new_capability_picker(ComposerPickerKind::Mcp, window, cx)
        else {
            return;
        };
        let weak = picker_state.downgrade();
        window.open_dialog(cx, move |dialog, _window, cx| {
            let Some(picker_state) = weak.upgrade() else {
                return dialog;
            };
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
                    state.mcp_server_rows().as_slice(),
                    &state.selected(),
                );
                let active_server_selected = active_server_id
                    .as_deref()
                    .is_some_and(|server_id| selected_server_ids.contains(server_id));
                let loaded_tool_server_ids = composer_capabilities::loaded_mcp_tool_server_ids(
                    state.mcp_tool_rows().as_slice(),
                );
                (
                    composer_capabilities::filter_selectable_mcp_capability_rows(
                        state.mcp_server_rows().as_slice(),
                        query.as_str(),
                    ),
                    if has_query {
                        composer_capabilities::filter_search_mcp_tool_capability_rows(
                            state.mcp_tool_rows().as_slice(),
                            &selected_server_ids,
                            query.as_str(),
                        )
                    } else if active_server_selected {
                        Vec::new()
                    } else {
                        composer_capabilities::filter_active_mcp_tool_capability_rows(
                            state.mcp_tool_rows().as_slice(),
                            active_server_id.as_deref(),
                            query.as_str(),
                        )
                    },
                    has_query,
                    active_server_id,
                    state.selected(),
                    state.mcp_tool_loading_server_id(),
                    state.mcp_tool_error(),
                    state.mcp_server_loading(),
                    state.mcp_server_error(),
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
                .on_close({
                    let weak = picker_state.downgrade();
                    move |_, _, cx| {
                        let _ = weak.update(cx, |state, _| {
                            state.dialog_open.set(false);
                            state.retire();
                        });
                    }
                })
                .footer(DialogFooter::new().children(vec![
                        default_outline_button("composer-mcp-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .on_click({
                                let weak = picker_state.downgrade();
                                move |_, window, cx| {
                                    let _ = weak.update(cx, |state, cx| state.close(window, cx));
                                }
                            })
                            .into_any_element(),
                        default_primary_button("composer-mcp-save")
                            .label(t!("buttons.add").to_string())
                            .disabled(
                                selected_mcp_composer_capabilities(&picker_state.read(cx))
                                    .is_empty(),
                            )
                            .on_click({
                                let weak = picker_state.downgrade();
                                move |_, window, cx| {
                                    let _ = weak.update(cx, |state, cx| state.commit(window, cx));
                                }
                            })
                            .into_any_element(),
                    ]))
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
                            picker_state.downgrade(),
                            cx,
                        )),
                )
        });
        picker_state.update(cx, |state, cx| {
            state.dialog_open.set(true);
            state.search.update(cx, |input, cx| input.focus(window, cx));
        });
    }

    pub(crate) fn composer_skill_picker_projection(
        &self,
        query: &str,
    ) -> ComposerSkillPickerProjection {
        self.composer_input
            .as_ref()
            .map(|input| {
                self.client.composer_catalog_skill_picker(
                    input.thread_id(),
                    input.draft_id(),
                    query,
                )
            })
            .unwrap_or_default()
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
                let _ = picker_state_for_row.update(cx, |state, cx| {
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
                let _ = picker_state_for_toggle.update(cx, |state, cx| {
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
                let _ = picker_state_for_row.update(cx, |state, cx| {
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
                let _ = picker_state_for_toggle.update(cx, |state, cx| {
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
                    let _ = picker_state_for_expand.update(cx, |state, cx| {
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
    picker_state: WeakEntity<CapabilityPickerState>,
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
                let _ = picker_state_for_row.update(cx, |state, cx| {
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
                let _ = picker_state_for_toggle.update(cx, |state, cx| {
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
                        move |_, _, cx| {
                            cx.stop_propagation();
                            let _ = picker_state.update(cx, |state, cx| {
                                state.toggle_mcp_tools(server_id.as_str(), cx)
                            });
                        }
                    }),
            )
        })
        .into_any_element()
}

fn stable_picker_row_id(value: &str) -> u64 {
    use std::hash::Hash;
    use std::hash::Hasher;

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
        state.mcp_server_rows().as_slice(),
        state.mcp_tool_rows().as_slice(),
        &state.selected(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::timeline::types::SkillId;

    fn skill_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("skill id")
    }

    fn pack_id(character: char) -> SkillPackId {
        SkillPackId::new(character.to_string().repeat(21)).expect("pack id")
    }

    fn selectable_skill(character: char) -> SelectableSkillCapability {
        let skill_id = skill_id(character);
        SelectableSkillCapability {
            key: ComposerSkillSelection::Skill {
                skill_id: skill_id.clone(),
                pack_id: None,
            }
            .key(),
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

#[cfg(test)]
mod lifecycle_tests {
    use super::CapabilityPickerState;
    use gpui_kit::AppContext;
    use gpui_kit::Context;
    use gpui_kit::FocusHandle;
    use gpui_kit::InteractiveElement;
    use gpui_kit::IntoElement;
    use gpui_kit::ParentElement;
    use gpui_kit::Render;
    use gpui_kit::TestAppContext;
    use gpui_kit::Window;
    use gpui_kit::component::Root;
    use gpui_kit::component::WindowExt;
    use gpui_kit::div;
    use pioneer_client::composer::store::ComposerIntent;
    use pioneer_client::composer::store::ComposerOperationIdentity;
    use pioneer_client::core::ClientCore;
    use std::sync::Arc;
    struct Host {
        focus: FocusHandle,
    }
    impl Render for Host {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().track_focus(&self.focus)
        }
    }
    #[gpui_kit::test]
    fn picker_drop_releases_dialog_binding_and_input_without_retaining_the_thread(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let (root, cx) = cx.add_window_view(|window, cx| {
            let host = cx.new(|cx| Host {
                focus: cx.focus_handle(),
            });
            Root::new(host, window, cx)
        });
        let core = Arc::new(ClientCore::new());
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: Default::default(),
        });
        let draft = core.composer_snapshot("a").unwrap();
        let (registrar, deliver) = crate::test_support::binding_router(core.clone());
        let (weak, search, trigger) = cx.update(|window, cx| {
            let host = root.read(cx).view().clone().downcast::<Host>().unwrap();
            let trigger = host.read(cx).focus.clone();
            trigger.focus(window, cx);
            let picker = cx.new(|cx| {
                CapabilityPickerState::new(
                    core.clone(),
                    registrar,
                    ComposerOperationIdentity {
                        thread_id: "a".into(),
                        draft_id: draft.draft_id(),
                        generation: 1,
                    },
                    window,
                    cx,
                    "Search".into(),
                )
            });
            let weak = picker.downgrade();
            let dialog = weak.clone();
            window.open_dialog(cx, move |dialog_view, _, cx| {
                let Some(picker) = dialog.upgrade() else {
                    return dialog_view;
                };
                dialog_view.child(super::render_picker_filter_form(&picker.read(cx).search))
            });
            let search = picker.update(cx, |picker, cx| {
                picker.dialog_open.set(true);
                picker.search.update(cx, |input, cx| {
                    input.set_value("query", window, cx);
                    input.focus(window, cx);
                });
                picker.search.downgrade()
            });
            deliver();
            assert!(Arc::ptr_eq(&draft, &core.composer_snapshot("a").unwrap()));
            drop(picker);
            (weak, search, trigger)
        });
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
        assert!(search.upgrade().is_none());
        cx.update(|window, cx| assert_eq!(window.focused(cx), Some(trigger)));
    }
}
