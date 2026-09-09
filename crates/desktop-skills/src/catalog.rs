use crate::{binding::CatalogBinding, sidebar::CatalogSidebar, table::*};
use gpui_kit::component::VirtualListScrollHandle;
use gpui_kit::component::table::TableState;
use gpui_kit::{prelude::*, *};
pub(crate) use pioneer_client::state::client_state::GatewayConnectionState;
use pioneer_client::{
    authorization::{PrincipalPresentationCapabilities, principal_presentation_capabilities},
    core::{ClientCore, ClientScope},
    navigation::{ClientNavigationState, SemanticDestination},
    skills::{operations::*, store::*, types::*},
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
static NEXT_MOUNT: AtomicU64 = AtomicU64::new(1);
pub struct SkillsCatalogConfig {
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
}
impl SkillsCatalogConfig {
    pub fn new(client: Arc<ClientCore>, registrar: Arc<dyn ClientBindingRegistrar>) -> Self {
        Self { client, registrar }
    }
}
#[derive(Clone, PartialEq)]
pub(crate) struct GatewayInput {
    pub connection_state: GatewayConnectionState,
    pub capabilities: PrincipalPresentationCapabilities,
    pub endpoint_id: Option<String>,
    pub connection_id: Option<u64>,
}
#[derive(Clone, PartialEq)]
pub(crate) struct CatalogInput {
    pub navigation_input: Arc<ClientNavigationState>,
    pub gateway: GatewayInput,
    pub installed_skills: Vec<SkillListItem>,
    pub skills_catalog: Vec<SkillListItem>,
    pub skills_management: pioneer_client::skills::catalog::SkillManagementProjection,
    pub skills_health_details: std::collections::HashMap<SkillId, SkillHealthItem>,
    pub skills_loading: bool,
    pub skills_error: Option<String>,
    pub pending: std::collections::HashSet<SkillId>,
    pub pending_packs: std::collections::HashSet<SkillPackId>,
}
impl CatalogInput {
    fn selected(&self) -> Option<&SkillListItem> {
        let id = self.navigation_input.skill_id()?;
        self.installed_skills.iter().find(|s| &s.skill_id == id)
    }
    fn same_parent(&self, other: &Self) -> bool {
        self.navigation_input.workspace_id() == other.navigation_input.workspace_id()
            && self.navigation_input.destination() == other.navigation_input.destination()
            && self.gateway == other.gateway
    }
    fn same_screen(&self, other: &Self) -> bool {
        self.same_parent(other)
            && if matches!(
                self.navigation_input.destination(),
                SemanticDestination::Skills { skill_id: Some(_) }
            ) {
                self.selected() == other.selected()
                    && self.skills_health_details == other.skills_health_details
                    && self.selected().map(|s| self.is_skill_pending(&s.skill_id))
                        == other
                            .selected()
                            .map(|s| other.is_skill_pending(&s.skill_id))
            } else {
                self.skills_loading == other.skills_loading
                    && self.skills_error == other.skills_error
            }
    }
    fn same_sidebar(&self, other: &Self) -> bool {
        self.same_parent(other)
            && self.selected() == other.selected()
            && self.skills_loading == other.skills_loading
            && self.selected().map(|s| self.is_skill_pending(&s.skill_id))
                == other
                    .selected()
                    .map(|s| other.is_skill_pending(&s.skill_id))
    }

    fn empty(client: &ClientCore) -> Self {
        Self {
            navigation_input: client.navigation_snapshot(),
            gateway: GatewayInput {
                connection_state: GatewayConnectionState::Disconnected,
                capabilities: Default::default(),
                endpoint_id: None,
                connection_id: None,
            },
            installed_skills: vec![],
            skills_catalog: vec![],
            skills_management: Default::default(),
            skills_health_details: Default::default(),
            skills_loading: false,
            skills_error: None,
            pending: Default::default(),
            pending_packs: Default::default(),
        }
    }
    pub fn principal_presentation_capabilities(&self) -> PrincipalPresentationCapabilities {
        self.gateway.capabilities
    }
    pub fn is_skill_pending(&self, id: &SkillId) -> bool {
        self.pending.contains(id)
    }
    pub fn is_skill_pack_pending(&self, id: &SkillPackId) -> bool {
        self.pending_packs.contains(id)
    }
}
pub struct SkillsCatalogView {
    pub(crate) rows:
        std::collections::BTreeMap<String, Entity<crate::list::SkillManagementRowView>>,
    pub(crate) management_rows: std::rc::Rc<Vec<crate::list::DesktopSkillManagementRow>>,
    pub(crate) client: Arc<ClientCore>,
    pub(crate) input: Arc<CatalogInput>,
    pub(crate) binding: Arc<CatalogBinding>,
    sidebar: Entity<CatalogSidebar>,
    demand: Option<SkillsDemand>,
    demand_key: Option<(String, Option<SkillId>)>,
    pub(crate) sidebar_width: Pixels,
    pub(crate) mount: u64,
    pub(crate) window_active: bool,
    pub(crate) presentation_error: Option<String>,
    pub(crate) native_task: Option<Task<()>>,
    pub(crate) parent_generation: u64,
    pub(crate) skills_list_scroll_handle: VirtualListScrollHandle,
    pub(crate) skills_details_expanded_sections:
        std::collections::BTreeMap<String, std::collections::HashSet<String>>,
    pub(crate) skills_expanded_pack_ids: std::collections::HashSet<SkillPackId>,
    pub(crate) skills_audit_table_state: Entity<TableState<SkillDiagnosticsTableDelegate>>,
    pub(crate) upload: Entity<crate::upload::UploadView>,
    pub(crate) _upload_visibility: Subscription,
    _activation: Subscription,
    _publication_task: Task<()>,
}
impl SkillsCatalogView {
    pub(crate) fn principal_presentation_capabilities(&self) -> PrincipalPresentationCapabilities {
        self.input.principal_presentation_capabilities()
    }
    pub(crate) fn is_skill_pending(&self, id: &SkillId) -> bool {
        self.input.is_skill_pending(id)
    }

    pub fn new(config: SkillsCatalogConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        let mount = NEXT_MOUNT.fetch_add(1, Ordering::Relaxed);
        cx.new(|cx| {
            let binding = CatalogBinding::new(config.registrar.clone());
            let mut changed = binding.changed.subscribe();
            let handle = window.window_handle();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if cx
                        .update_window(handle, |_, window, cx| {
                            view.update(cx, |view, cx| view.sync_publications(window, cx))
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let input = Arc::new(CatalogInput::empty(&config.client));
            let owner = cx.weak_entity();
            let sidebar = cx.new(|_| CatalogSidebar {
                owner,
                input: input.clone(),
            });
            let table = cx.new(|cx| {
                TableState::new(
                    SkillDiagnosticsTableDelegate::new("skills-audit-table"),
                    window,
                    cx,
                )
                .row_selectable(false)
                .col_selectable(false)
                .sortable(false)
                .col_movable(false)
                .col_resizable(false)
                .loop_selection(false)
            });
            let upload =
                crate::upload::UploadView::new(config.client.clone(), config.registrar, cx);
            let upload_visibility = cx.subscribe(
                &upload,
                |view: &mut Self, upload, _: &crate::upload::UploadVisibility, cx| {
                    view.presentation_error = upload.read(cx).error();
                    cx.notify();
                },
            );
            let activation = cx.observe_window_activation(window, |view: &mut Self, window, cx| {
                view.set_window_active(window.is_window_active(), cx);
                view.sync_publications(window, cx);
            });
            let mut view = Self {
                _activation: activation,
                rows: Default::default(),
                management_rows: Default::default(),
                skills_list_scroll_handle: VirtualListScrollHandle::new(),
                skills_details_expanded_sections: Default::default(),
                skills_expanded_pack_ids: Default::default(),
                skills_audit_table_state: table,
                upload,
                _upload_visibility: upload_visibility,
                client: config.client,
                input,
                binding,
                sidebar,
                demand: None,
                demand_key: None,
                sidebar_width: px(320.),
                mount,
                window_active: window.is_window_active(),
                presentation_error: None,
                native_task: None,
                parent_generation: 0,
                _publication_task: task,
            };
            view.sync_publications(window, cx);
            view
        })
    }
    pub fn sidebar(&self) -> AnyView {
        self.sidebar.clone().into()
    }
    pub fn set_sidebar_width(&mut self, width: Pixels, cx: &mut Context<Self>) {
        if self.sidebar_width != width {
            self.sidebar_width = width;
            cx.notify();
        }
    }
    pub fn set_window_active(&mut self, active: bool, cx: &mut Context<Self>) {
        if self.window_active != active {
            self.window_active = active;
            if !active {
                self.parent_generation = self
                    .parent_generation
                    .checked_add(1)
                    .expect("catalog lifetime exhausted");
                self.native_task = None;
            }
            self.sync_demand();
            if !active {
                self.upload.update(cx, |upload, cx| upload.cancel(cx));
            }
            cx.notify();
        }
    }
    pub(crate) fn ui_id(&self, domain: &str, role: &str) -> SharedString {
        format!(
            "skills:{}:{}:{domain}:{role}",
            self.mount,
            self.input
                .navigation_input
                .workspace_id()
                .unwrap_or_default()
        )
        .into()
    }
    fn visible(&self) -> bool {
        matches!(
            self.input.navigation_input.destination(),
            SemanticDestination::Skills { .. }
        )
    }
    fn sync_demand(&mut self) {
        let wanted = (self.window_active
            && self.visible()
            && self.input.gateway.connection_state == GatewayConnectionState::Connected)
            .then(|| {
                self.input.navigation_input.workspace_id().map(|w| {
                    (
                        w.to_owned(),
                        self.input.navigation_input.skill_id().cloned(),
                    )
                })
            })
            .flatten();
        if self.demand_key != wanted {
            self.demand = None;
            self.demand_key = wanted.clone();
            if let Some((workspace, id)) = wanted {
                self.demand = Some(self.client.acquire_skills_demand(&workspace, id.as_ref()));
            }
        }
    }
    fn sync_publications(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let mut next = CatalogInput::empty(&self.client);
        let session = self.client.gateway_session();
        next.gateway.connection_state = session
            .status
            .as_ref()
            .map_or(GatewayConnectionState::Disconnected, |s| s.connection_state);
        next.gateway.endpoint_id = session.startup.endpoint_id.clone();
        next.gateway.connection_id = session.startup.connection_id;
        next.gateway.capabilities = self
            .client
            .authorization_snapshot(next.navigation_input.workspace_id(), None)
            .or_else(|| self.client.authorization_snapshot(None, None))
            .as_ref()
            .map(principal_presentation_capabilities)
            .unwrap_or_default();
        let mut scopes = vec![
            ClientScope::Navigation,
            ClientScope::Session,
            ClientScope::Administration { workspace_id: None },
        ];
        if let Some(workspace) = next.navigation_input.workspace_id().map(str::to_owned) {
            scopes.push(ClientScope::Administration {
                workspace_id: Some(workspace.clone()),
            });
            scopes.push(ClientScope::Skills {
                workspace_id: Some(workspace.clone()),
            });
            if let Some(catalog) = self.client.skills_catalog_snapshot(&workspace) {
                next.skills_catalog = catalog.catalog.iter().map(|s| (**s).clone()).collect();
                next.installed_skills = catalog.installed.iter().map(|s| (**s).clone()).collect();
                next.skills_management = (*catalog.management).clone();
                next.skills_loading = catalog.request == SkillsLoadState::Loading;
                next.skills_error = (catalog.request == SkillsLoadState::Failed)
                    .then(|| t!("skills.error.load_failed", error = "").to_string());
            }
            for skill in &next.skills_catalog {
                scopes.push(ClientScope::SkillsAction {
                    workspace_id: workspace.clone(),
                    target: skill.skill_id.to_string(),
                });
                if let Some(action) = self
                    .client
                    .skills_action_snapshot(&workspace, &skill.skill_id.to_string())
                {
                    if action.state == SkillsActionState::Pending {
                        next.pending.insert(skill.skill_id.clone());
                    } else if action.state == SkillsActionState::Failed {
                        next.skills_error = Some(match action.kind {
                            SkillsActionKind::Policy => {
                                t!("skills.error.policy_failed").to_string()
                            }
                            SkillsActionKind::Remove => {
                                t!("skills.error.uninstall_failed").to_string()
                            }
                            SkillsActionKind::RemovePack => {
                                t!("skills.error.uninstall_pack_failed").to_string()
                            }
                        });
                    }
                }
            }
            for pack in &next.skills_management.packs {
                scopes.push(ClientScope::SkillsAction {
                    workspace_id: workspace.clone(),
                    target: pack.pack.id.to_string(),
                });
                if let Some(action) = self
                    .client
                    .skills_action_snapshot(&workspace, &pack.pack.id.to_string())
                {
                    if action.state == SkillsActionState::Pending {
                        next.pending_packs.insert(pack.pack.id.clone());
                    } else if action.state == SkillsActionState::Failed {
                        next.skills_error =
                            Some(t!("skills.error.uninstall_pack_failed").to_string());
                    }
                }
            }
            if let Some(skill) = next.navigation_input.skill_id() {
                scopes.push(ClientScope::SkillsDetails {
                    workspace_id: workspace.clone(),
                    skill_id: skill.clone(),
                });
                if let Some(detail) = self.client.skills_details_snapshot(&workspace, skill) {
                    if let Some(health) = &detail.health {
                        next.skills_health_details
                            .insert(skill.clone(), (**health).clone());
                    }
                }
            }
        }
        if self.input.navigation_input.workspace_id() != next.navigation_input.workspace_id()
            || self.input.gateway.endpoint_id != next.gateway.endpoint_id
            || self.input.gateway.connection_id != next.gateway.connection_id
            || self.input.gateway.capabilities != next.gateway.capabilities
        {
            self.rows.clear();
            self.skills_details_expanded_sections.clear();
            self.skills_list_scroll_handle = VirtualListScrollHandle::new();
            self.skills_expanded_pack_ids.clear();
        }
        if self.input.navigation_input.workspace_id() != next.navigation_input.workspace_id()
            || self.input.gateway.capabilities != next.gateway.capabilities
            || self.input.gateway.connection_id != next.gateway.connection_id
            || self.input.gateway.endpoint_id != next.gateway.endpoint_id
            || self.input.navigation_input.destination() != next.navigation_input.destination()
        {
            self.parent_generation = self
                .parent_generation
                .checked_add(1)
                .expect("catalog lifetime exhausted");
            self.native_task = None;
            self.presentation_error = None;
            self.upload.update(cx, |upload, cx| upload.cancel(cx));
        }
        let changed = *self.input != next;
        let screen_changed = !self.input.same_screen(&next);
        let sidebar_changed = !self.input.same_sidebar(&next);
        if changed {
            self.input = Arc::new(next);
            let structure_changed = self.sync_rows(cx);
            if sidebar_changed {
                self.sidebar.update(cx, |sidebar, cx| {
                    sidebar.input = self.input.clone();
                    cx.notify();
                });
            }
            if let Some(id) = self.input.navigation_input.skill_id()
                && let Some(skill) = self
                    .input
                    .installed_skills
                    .iter()
                    .find(|s| &s.skill_id == id)
            {
                let health = self.input.skills_health_details.get(id);
                let diagnostics =
                    pioneer_client::skills::presentation::skill_detail_diagnostics(skill, health);
                self.sync_skill_diagnostics_tables(&diagnostics.recent_audit, cx);
            } else {
                self.sync_skill_diagnostics_tables(&[], cx);
            }
            if screen_changed || structure_changed {
                cx.notify();
            }
        }
        self.binding.set_scopes(&scopes);
        self.sync_demand();
        let _ = window;
    }
}
impl Render for SkillsCatalogView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if matches!(
            self.input.navigation_input.destination(),
            SemanticDestination::Skills { skill_id: Some(_) }
        ) {
            self.render_skill_details(window, cx)
        } else {
            self.render_skills(window, cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Arc, ClientBindingRegistrar, ClientScope, SkillsCatalogConfig, SkillsCatalogView};
    use gpui_kit::TestAppContext;
    use pioneer_client::navigation::NavigationIntent;
    use pioneer_desktop_foundation::{ClientBindingRegistration, ClientPublicationSink};
    use std::{cell::Cell, rc::Rc, sync::Weak};
    struct Registrar;
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            _: ClientScope,
            _: Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            ClientBindingRegistration::new(|| {})
        }
    }
    #[gpui_kit::test]
    fn changed_skill_retains_other_row_and_does_not_notify_list(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::client();
        client.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            None,
        );
        client.navigate_skills(pioneer_client::skills::route::SkillsRoute::List);
        let response = || {
            pioneer_client::skills::catalog::project_skills_snapshot(
                vec![
                    pioneer_client::catalog_test_support::skill('A'),
                    pioneer_client::catalog_test_support::skill('B'),
                ],
                vec![],
            )
        };
        client.accept_skills_catalog_for_test("workspace", response());
        let (root, cx) = cx.add_window_view(|window, cx| {
            let view = SkillsCatalogView::new(
                SkillsCatalogConfig::new(client.clone(), Arc::new(Registrar)),
                window,
                cx,
            );
            gpui_kit::component::Root::new(view, window, cx)
        });
        let view = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<SkillsCatalogView>().unwrap()
        });
        let key = format!(
            "skill:{}",
            pioneer_client::catalog_test_support::skill('B').skill_id
        );
        let b = view.read_with(cx, |v, _| v.rows[&key].clone());
        let counts = Rc::new([Cell::new(0), Cell::new(0)]);
        let subs = cx.update(|_, cx| {
            let cb = counts.clone();
            let cr = counts.clone();
            (
                cx.observe(&b, move |_, _| cb[0].set(cb[0].get() + 1)),
                cx.observe(&view, move |_, _| cr[1].set(cr[1].get() + 1)),
            )
        });
        let mut changed = response();
        changed.catalog[0].description = "changed A".into();
        changed.installed[0] = changed.catalog[0].clone();
        changed.management =
            pioneer_client::skills::catalog::project_skill_management(&changed.installed, vec![]);
        client.accept_skills_catalog_for_test("workspace", changed);
        cx.update(|window, cx| view.update(cx, |v, cx| v.sync_publications(window, cx)));
        cx.run_until_parked();
        assert_eq!(counts.iter().map(Cell::get).collect::<Vec<_>>(), vec![0, 0]);
        assert_eq!(
            b.entity_id(),
            view.read_with(cx, |v, _| v.rows[&key].entity_id())
        );
        drop(subs);
    }
    #[gpui_kit::test]
    fn two_windows_aggregate_demand_and_warm_roots_release_it(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::client();
        client.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            None,
        );
        client.navigate(
            NavigationIntent::Navigate {
                destination: pioneer_client::navigation::SemanticDestination::Skills {
                    skill_id: None,
                },
            },
            None,
        );
        let (root, cx) = cx.add_window_view(|window, cx| {
            let view = SkillsCatalogView::new(
                SkillsCatalogConfig::new(client.clone(), Arc::new(Registrar)),
                window,
                cx,
            );
            gpui_kit::component::Root::new(view, window, cx)
        });
        let first = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<SkillsCatalogView>().unwrap()
        });
        let second = cx.update(|window, cx| {
            SkillsCatalogView::new(
                SkillsCatalogConfig::new(client.clone(), Arc::new(Registrar)),
                window,
                cx,
            )
        });
        cx.update(|_, cx| {
            for view in [&first, &second] {
                view.update(cx, |view, cx| {
                    Arc::make_mut(&mut view.input).gateway.connection_state =
                        super::GatewayConnectionState::Connected;
                    view.set_window_active(false, cx);
                    view.set_window_active(true, cx);
                });
            }
        });
        assert_eq!(client.skills_demand_count_for_test("workspace"), 2);
        cx.update(|_, cx| first.update(cx, |view, cx| view.set_window_active(false, cx)));
        assert_eq!(client.skills_demand_count_for_test("workspace"), 1);
        cx.update(|_, cx| second.update(cx, |view, cx| view.set_window_active(false, cx)));
        assert_eq!(client.skills_demand_count_for_test("workspace"), 0);
        cx.update(|_, cx| first.update(cx, |view, cx| view.set_window_active(true, cx)));
        assert_eq!(client.skills_demand_count_for_test("workspace"), 1);
        client.navigate(
            NavigationIntent::Navigate {
                destination: pioneer_client::navigation::SemanticDestination::Threads,
            },
            None,
        );
        cx.update(|window, cx| first.update(cx, |view, cx| view.sync_publications(window, cx)));
        assert_eq!(client.skills_demand_count_for_test("workspace"), 0);
        assert_eq!(first.read_with(cx, |view, _| view.demand.is_none()), true);
    }

    #[gpui_kit::test]
    fn upload_progress_notifies_only_operation_child_and_terminal_visibility_once(
        cx: &mut TestAppContext,
    ) {
        use pioneer_client::skills::{
            operations::SkillUploadTarget,
            upload_flow::{SkillUploadPublication, SkillUploadState},
        };
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::client();
        client.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            None,
        );
        client.navigate_skills(pioneer_client::skills::route::SkillsRoute::List);
        client.accept_skills_catalog_for_test(
            "workspace",
            pioneer_client::skills::catalog::project_skills_snapshot(
                vec![pioneer_client::catalog_test_support::skill('A')],
                vec![],
            ),
        );
        let (root, cx) = cx.add_window_view(|window, cx| {
            let view = SkillsCatalogView::new(
                SkillsCatalogConfig::new(client.clone(), Arc::new(Registrar)),
                window,
                cx,
            );
            gpui_kit::component::Root::new(view, window, cx)
        });
        let view = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<SkillsCatalogView>().unwrap()
        });
        let upload = view.read_with(cx, |view, _| view.upload.clone());
        let publication = |revision, sent_bytes, state| {
            Some(Arc::new(SkillUploadPublication {
                operation_id: 7,
                generation: 7,
                revision,
                state,
                sent_bytes,
                total_bytes: 8,
                target: SkillUploadTarget::Install,
                pack: false,
            }))
        };
        cx.update(|_, cx| {
            upload.update(cx, |upload, cx| {
                upload.apply_publication(publication(1, 0, SkillUploadState::Uploading), cx)
            })
        });
        cx.run_until_parked();
        let counts = Rc::new([Cell::new(0), Cell::new(0)]);
        let subs = cx.update(|_, cx| {
            let child = counts.clone();
            let parent = counts.clone();
            (
                cx.observe(&upload, move |_, _| child[0].set(child[0].get() + 1)),
                cx.observe(&view, move |_, _| parent[1].set(parent[1].get() + 1)),
            )
        });
        let catalog = client.skills_catalog_snapshot("workspace").unwrap();
        cx.update(|_, cx| {
            upload.update(cx, |upload, cx| {
                upload.apply_publication(publication(2, 2, SkillUploadState::Uploading), cx)
            })
        });
        cx.run_until_parked();
        assert_eq!([counts[0].get(), counts[1].get()], [1, 0]);
        assert!(Arc::ptr_eq(
            &catalog,
            &client.skills_catalog_snapshot("workspace").unwrap()
        ));
        cx.update(|_, cx| {
            upload.update(cx, |upload, cx| {
                upload.apply_publication(publication(2, 2, SkillUploadState::Uploading), cx)
            })
        });
        cx.run_until_parked();
        assert_eq!([counts[0].get(), counts[1].get()], [1, 0]);
        cx.update(|_, cx| {
            upload.update(cx, |upload, cx| {
                upload.apply_publication(publication(3, 2, SkillUploadState::Cancelled), cx)
            })
        });
        cx.run_until_parked();
        assert_eq!([counts[0].get(), counts[1].get()], [2, 1]);
        drop(subs);
    }
    #[gpui_kit::test]
    fn audit_table_refresh_preserves_entity_and_emits_no_user_selection(cx: &mut TestAppContext) {
        use pioneer_client::skills::types::SkillAuditTimelineItem;
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::client();
        let (root, cx) = cx.add_window_view(|window, cx| {
            let view = SkillsCatalogView::new(
                SkillsCatalogConfig::new(client, Arc::new(Registrar)),
                window,
                cx,
            );
            gpui_kit::component::Root::new(view, window, cx)
        });
        let view = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<SkillsCatalogView>().unwrap()
        });
        let table = view.read_with(cx, |view, _| view.skills_audit_table_state.clone());
        let events = Rc::new(Cell::new(0));
        let count = events.clone();
        let sub = cx.update(|_, cx| {
            cx.subscribe(
                &table,
                move |_, _: &gpui_kit::component::table::TableEvent, _| count.set(count.get() + 1),
            )
        });
        let row = |action: &str, time| SkillAuditTimelineItem {
            action: action.into(),
            decision: "allow".into(),
            reason_code: None,
            created_at: time,
            details_json: "{}".into(),
        };
        let a = row("install", 1);
        let b = row("update", 2);
        cx.update(|_, cx| {
            view.update(cx, |view, cx| {
                view.sync_skill_diagnostics_tables(&[a.clone(), b.clone()], cx)
            })
        });
        cx.run_until_parked();
        let keys = table.read_with(cx, |table, _| table.delegate().model().keys.clone());
        cx.update(|_, cx| {
            view.update(cx, |view, cx| {
                view.sync_skill_diagnostics_tables(&[b, a], cx)
            })
        });
        cx.run_until_parked();
        assert_eq!(events.get(), 0);
        assert_eq!(
            table.entity_id(),
            view.read_with(cx, |view, _| view.skills_audit_table_state.entity_id())
        );
        assert_eq!(
            table.read_with(cx, |table, _| table.delegate().model().keys.clone()),
            vec![keys[1].clone(), keys[0].clone()]
        );
        drop(sub);
    }
}
