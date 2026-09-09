use crate::{binding::CatalogBinding, sidebar::CatalogSidebar, table::*};
use gpui_kit::component::VirtualListScrollHandle;
use gpui_kit::component::table::TableState;
use gpui_kit::{prelude::*, *};
pub(crate) use pioneer_client::state::client_state::GatewayConnectionState;
use pioneer_client::{
    authorization::{PrincipalPresentationCapabilities, principal_presentation_capabilities},
    core::{ClientCore, ClientScope},
    mcp::{operations::*, store::*, types::*},
    navigation::{ClientNavigationState, SemanticDestination},
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
static NEXT_MOUNT: AtomicU64 = AtomicU64::new(1);
pub struct McpCatalogConfig {
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
}
impl McpCatalogConfig {
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
    pub mcp_servers: Vec<McpListItem>,
    pub mcp_server_details: Option<McpServerDetailsResponse>,
    pub mcp_loading: bool,
    pub mcp_details_loading: bool,
    pub mcp_error: Option<String>,
    pub pending: std::collections::HashSet<String>,
}
impl CatalogInput {
    fn selected(&self) -> Option<&McpListItem> {
        let id = self.navigation_input.mcp_server_id()?;
        self.mcp_servers.iter().find(|s| s.id == id)
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
                SemanticDestination::Mcp { server_id: Some(_) }
            ) {
                self.selected() == other.selected()
                    && self.mcp_server_details == other.mcp_server_details
                    && self.mcp_details_loading == other.mcp_details_loading
                    && self.mcp_error == other.mcp_error
                    && self.selected().map(|s| self.is_mcp_pending(&s.id))
                        == other.selected().map(|s| other.is_mcp_pending(&s.id))
            } else {
                self.mcp_servers
                    .iter()
                    .map(|s| &s.id)
                    .eq(other.mcp_servers.iter().map(|s| &s.id))
                    && self.mcp_loading == other.mcp_loading
                    && self.mcp_error == other.mcp_error
            }
    }
    fn same_sidebar(&self, other: &Self) -> bool {
        self.same_parent(other)
            && self.selected() == other.selected()
            && self.mcp_loading == other.mcp_loading
            && self.is_mcp_pending(pioneer_client::mcp::list::MCP_INSTALL_PENDING_KEY)
                == other.is_mcp_pending(pioneer_client::mcp::list::MCP_INSTALL_PENDING_KEY)
            && self.selected().map(|s| self.is_mcp_pending(&s.id))
                == other.selected().map(|s| other.is_mcp_pending(&s.id))
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
            mcp_servers: vec![],
            mcp_server_details: None,
            mcp_loading: false,
            mcp_details_loading: false,
            mcp_error: None,
            pending: Default::default(),
        }
    }
    pub fn principal_presentation_capabilities(&self) -> PrincipalPresentationCapabilities {
        self.gateway.capabilities
    }
    pub fn is_mcp_pending(&self, name: &str) -> bool {
        self.pending.contains(name)
    }
}
pub struct McpCatalogView {
    pub(crate) rows: std::collections::BTreeMap<String, Entity<crate::list::McpServerRow>>,
    pub(crate) client: Arc<ClientCore>,
    pub(crate) input: Arc<CatalogInput>,
    binding: Arc<CatalogBinding>,
    sidebar: Entity<CatalogSidebar>,
    demand: Option<McpDemand>,
    demand_key: Option<(String, Option<String>)>,
    pub(crate) sidebar_width: Pixels,
    pub(crate) mount: u64,
    pub(crate) window_active: bool,
    pub(crate) presentation_error: Option<String>,
    pub(crate) native_task: Option<Task<()>>,
    pub(crate) parent_generation: u64,
    pub(crate) mcp_list_scroll_handle: VirtualListScrollHandle,
    pub(crate) mcp_details_expanded_sections:
        std::collections::BTreeMap<String, std::collections::HashSet<String>>,
    pub(crate) mcp_audit_table_state: Entity<TableState<McpDiagnosticsTableDelegate>>,
    pub(crate) config_form: Option<Entity<crate::dialogs::McpConfigForm>>,
    _activation: Subscription,
    _publication_task: Task<()>,
    pub(crate) dialogs: Vec<Entity<crate::dialog_lifetime::DialogLifetime>>,
    _release: Subscription,
}
impl McpCatalogView {
    pub(crate) fn principal_presentation_capabilities(&self) -> PrincipalPresentationCapabilities {
        self.input.principal_presentation_capabilities()
    }
    pub(crate) fn is_mcp_pending(&self, name: &str) -> bool {
        self.input.is_mcp_pending(name)
    }

    pub fn new(config: McpCatalogConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        let mount = NEXT_MOUNT.fetch_add(1, Ordering::Relaxed);
        cx.new(|cx| {
            let binding = CatalogBinding::new(config.registrar);
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
                    McpDiagnosticsTableDelegate::new("mcp-audit-table"),
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
            let activation = cx.observe_window_activation(window, |view: &mut Self, window, cx| {
                view.set_window_active(window.is_window_active(), cx);
                view.sync_publications(window, cx);
            });
            let release = cx.on_release_in(window, |view: &mut Self, window, cx| {
                for dialog in &view.dialogs {
                    dialog.update(cx, |dialog, cx| dialog.invalidate(window, cx));
                }
            });
            let mut view = Self {
                _activation: activation,
                dialogs: vec![],
                _release: release,
                rows: Default::default(),
                mcp_list_scroll_handle: VirtualListScrollHandle::new(),
                mcp_details_expanded_sections: Default::default(),
                mcp_audit_table_state: table,
                config_form: None,
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
            cx.notify();
        }
    }
    pub(crate) fn ui_id(&self, domain: &str, role: &str) -> SharedString {
        format!(
            "mcp:{}:{}:{domain}:{role}",
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
            SemanticDestination::Mcp { .. }
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
                        self.input
                            .navigation_input
                            .mcp_server_id()
                            .map(str::to_owned),
                    )
                })
            })
            .flatten();
        if self.demand_key != wanted {
            self.demand = None;
            self.demand_key = wanted.clone();
            if let Some((workspace, id)) = wanted {
                self.demand = Some(
                    self.client
                        .acquire_mcp_demand(&workspace, id.as_ref().map(String::as_str)),
                );
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
            scopes.push(ClientScope::Mcp {
                workspace_id: Some(workspace.clone()),
            });
            let catalog = self.client.mcp_catalog_snapshot(&workspace);
            if let Some(catalog) = catalog {
                next.mcp_servers = catalog.servers().iter().map(|s| (**s).clone()).collect();
                next.mcp_loading = catalog.request() == McpLoadState::Loading;
                next.mcp_error = (catalog.request() == McpLoadState::Failed)
                    .then(|| t!("mcp.error.load_servers_failed", error = "").to_string());
            }
            for server in &next.mcp_servers {
                let scope = ClientScope::McpAction {
                    workspace_id: workspace.clone(),
                    target: server.id.clone(),
                };
                scopes.push(scope);
                if let Some(action) = self.client.mcp_action_snapshot(&workspace, &server.id) {
                    if action.state == McpActionState::Pending {
                        next.pending.insert(server.id.clone());
                    } else if action.state == McpActionState::Failed {
                        next.mcp_error = Some(match action.kind {
                            McpActionKind::Policy => {
                                t!("mcp.error.policy_update_failed", error = "").to_string()
                            }
                            McpActionKind::Restart => {
                                t!("mcp.error.restart_failed", error = "").to_string()
                            }
                            McpActionKind::Remove => {
                                t!("mcp.error.uninstall_failed", error = "").to_string()
                            }
                            McpActionKind::Configure => {
                                t!("mcp.dialog.error.install_failed", error = "").to_string()
                            }
                        });
                    }
                }
            }
            scopes.push(ClientScope::McpAction {
                workspace_id: workspace.clone(),
                target: "configuration".into(),
            });
            if self
                .client
                .mcp_action_snapshot(&workspace, "configuration")
                .is_some_and(|p| p.state == McpActionState::Pending)
            {
                next.pending
                    .insert(pioneer_client::mcp::list::MCP_INSTALL_PENDING_KEY.into());
            }
            if let Some(server) = next.navigation_input.mcp_server_id() {
                scopes.push(ClientScope::McpDetails {
                    workspace_id: workspace.clone(),
                    server_id: server.into(),
                });
                if let Some(details) = self.client.mcp_details_snapshot(&workspace, server) {
                    next.mcp_server_details = details.details().map(|d| (**d).clone());
                    next.mcp_details_loading = details.request() == McpLoadState::Loading;
                    if details.request() == McpLoadState::Failed {
                        next.mcp_error =
                            Some(t!("mcp.error.load_details_failed", error = "").to_string());
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
            self.mcp_details_expanded_sections.clear();
            self.mcp_list_scroll_handle = VirtualListScrollHandle::new();
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
        }
        let changed = *self.input != next;
        let screen_changed = !self.input.same_screen(&next);
        let sidebar_changed = !self.input.same_sidebar(&next);
        if changed {
            self.input = Arc::new(next);
            self.sync_rows(cx);
            if sidebar_changed {
                self.sidebar.update(cx, |sidebar, cx| {
                    sidebar.input = self.input.clone();
                    cx.notify();
                });
            }
            let audit = self
                .input
                .mcp_server_details
                .as_ref()
                .and_then(|d| d.management.as_ref())
                .map(|m| m.audit.clone())
                .unwrap_or_default();
            self.sync_mcp_details_tables(&audit, cx);
            if screen_changed {
                cx.notify();
            }
        }
        self.binding.set_scopes(&scopes);
        self.sync_demand();
        self.sync_config_form(window, cx);
        let _ = window;
    }
}
impl Render for McpCatalogView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if matches!(
            self.input.navigation_input.destination(),
            SemanticDestination::Mcp { server_id: Some(_) }
        ) {
            self.render_mcp_details(window, cx)
        } else {
            self.render_mcp(window, cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Arc, ClientBindingRegistrar, ClientScope, McpCatalogConfig, McpCatalogView};
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
    fn changed_server_updates_only_its_retained_row_and_reorder_keeps_other_identity(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::client();
        client.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            None,
        );
        client.navigate_mcp(pioneer_client::mcp::route::McpRoute::List);
        client.accept_mcp_catalog_for_test(
            "workspace",
            pioneer_client::catalog_test_support::mcp(&["a", "b"]),
        );
        let (root, cx) = cx.add_window_view(|window, cx| {
            let view = McpCatalogView::new(
                McpCatalogConfig::new(client.clone(), Arc::new(Registrar)),
                window,
                cx,
            );
            gpui_kit::component::Root::new(view, window, cx)
        });
        let view = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<McpCatalogView>().unwrap()
        });
        let (a, b) = view.read_with(cx, |v, _| (v.rows["a"].clone(), v.rows["b"].clone()));
        let counts = Rc::new([Cell::new(0), Cell::new(0), Cell::new(0)]);
        let subscriptions = cx.update(|_, cx| {
            let ca = counts.clone();
            let cb = counts.clone();
            let cr = counts.clone();
            (
                cx.observe(&a, move |_, _| ca[0].set(ca[0].get() + 1)),
                cx.observe(&b, move |_, _| cb[1].set(cb[1].get() + 1)),
                cx.observe(&view, move |_, _| cr[2].set(cr[2].get() + 1)),
            )
        });
        let mut changed = pioneer_client::catalog_test_support::mcp(&["a", "b"]);
        changed.servers[0].status = pioneer_client::mcp::types::McpServerStatus::Restarting;
        client.accept_mcp_catalog_for_test("workspace", changed.clone());
        cx.update(|window, cx| view.update(cx, |v, cx| v.sync_publications(window, cx)));
        cx.run_until_parked();
        assert_eq!(
            counts.iter().map(Cell::get).collect::<Vec<_>>(),
            vec![1, 0, 0]
        );
        client.accept_mcp_catalog_for_test("workspace", changed.clone());
        cx.update(|window, cx| view.update(cx, |v, cx| v.sync_publications(window, cx)));
        cx.run_until_parked();
        assert_eq!(counts[0].get(), 1);
        changed.servers.reverse();
        client.accept_mcp_catalog_for_test("workspace", changed);
        cx.update(|window, cx| view.update(cx, |v, cx| v.sync_publications(window, cx)));
        assert_eq!(
            view.read_with(cx, |v, _| v.rows["b"].entity_id()),
            b.entity_id()
        );
        drop(subscriptions);
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
                destination: pioneer_client::navigation::SemanticDestination::Mcp {
                    server_id: None,
                },
            },
            None,
        );
        let (root, cx) = cx.add_window_view(|window, cx| {
            let view = McpCatalogView::new(
                McpCatalogConfig::new(client.clone(), Arc::new(Registrar)),
                window,
                cx,
            );
            gpui_kit::component::Root::new(view, window, cx)
        });
        let first = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<McpCatalogView>().unwrap()
        });
        let second = cx.update(|window, cx| {
            McpCatalogView::new(
                McpCatalogConfig::new(client.clone(), Arc::new(Registrar)),
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
        assert_eq!(client.mcp_demand_count_for_test("workspace"), 2);
        cx.update(|_, cx| first.update(cx, |view, cx| view.set_window_active(false, cx)));
        assert_eq!(client.mcp_demand_count_for_test("workspace"), 1);
        cx.update(|_, cx| second.update(cx, |view, cx| view.set_window_active(false, cx)));
        assert_eq!(client.mcp_demand_count_for_test("workspace"), 0);
        cx.update(|_, cx| first.update(cx, |view, cx| view.set_window_active(true, cx)));
        assert_eq!(client.mcp_demand_count_for_test("workspace"), 1);
        client.navigate(
            NavigationIntent::Navigate {
                destination: pioneer_client::navigation::SemanticDestination::Threads,
            },
            None,
        );
        cx.update(|window, cx| first.update(cx, |view, cx| view.sync_publications(window, cx)));
        assert_eq!(client.mcp_demand_count_for_test("workspace"), 0);
        assert_eq!(first.read_with(cx, |view, _| view.demand.is_none()), true);
    }
    #[gpui_kit::test]
    fn cached_detail_failure_notifies_the_visible_detail(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::client();
        client.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            None,
        );
        client.accept_mcp_catalog_for_test(
            "workspace",
            pioneer_client::catalog_test_support::mcp(&["a"]),
        );
        client.navigate_mcp(pioneer_client::mcp::route::McpRoute::Details("a".into()));
        client.accept_mcp_details_for_test(
            "workspace",
            "a",
            Ok(pioneer_client::catalog_test_support::mcp_detail("a")),
        );
        let (root, cx) = cx.add_window_view(|window, cx| {
            let view = McpCatalogView::new(
                McpCatalogConfig::new(client.clone(), Arc::new(Registrar)),
                window,
                cx,
            );
            gpui_kit::component::Root::new(view, window, cx)
        });
        let view = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<McpCatalogView>().unwrap()
        });
        let count = Rc::new(Cell::new(0));
        let observed = count.clone();
        let sub =
            cx.update(|_, cx| cx.observe(&view, move |_, _| observed.set(observed.get() + 1)));
        client.accept_mcp_details_for_test(
            "workspace",
            "a",
            Err(std::io::Error::other("synthetic failure").into()),
        );
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        cx.run_until_parked();
        assert_eq!(count.get(), 1);
        assert!(view.read_with(cx, |view, _| view.input.mcp_error.is_some()));
        drop(sub);
    }
}
