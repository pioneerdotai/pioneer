use super::*;
use pioneer_client::{
    navigation::SemanticDestination, state::client_state::GatewayConnectionState,
};

impl PluginsView {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        component_details: PluginComponentDetailsFactory,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let binding = CatalogBinding::new(registrar.clone());
            let mut changed = binding.watch();
            let publications = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if view
                        .update(cx, |view, cx| view.sync_publications(cx))
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let upload = PluginUploadView::new(client.clone(), registrar, cx);
            let upload_changes = cx.subscribe(
                &upload,
                |view: &mut Self, upload, _: &UploadVisibility, cx| {
                    let publication = upload.read(cx).publication().cloned();
                    view.sync_upload(publication, cx);
                },
            );
            let mut view = Self {
                client,
                binding,
                input: Arc::new(PluginPublication::default()),
                workspace: None,
                connection: 0,
                authorization: None,
                window_active: false,
                demand: None,
                demand_key: None,
                selected: None,
                source_picker: None,
                upload,
                applying_update: None,
                preview_dismissed: false,
                presentation_error: None,
                restore_confirmation: None,
                sidebar_width: px(0.),
                route_child: None,
                component_details,
                child_details: None,
                child_locator: None,
                window: window.window_handle(),
                child_sync_pending: false,
                _publications: publications,
                _upload_changes: upload_changes,
            };
            view.sync_publications(cx);
            view
        })
    }

    pub fn set_sidebar_width(&mut self, width: Pixels, cx: &mut Context<Self>) {
        self.sidebar_width = width;
        if let Some(child) = &self.child_details {
            (child.set_sidebar_width)(width, self.window_active, cx);
        }
    }

    pub fn set_window_active(&mut self, active: bool, cx: &mut Context<Self>) {
        if self.window_active != active {
            self.window_active = active;
            if let Some(child) = &self.child_details {
                (child.set_sidebar_width)(self.sidebar_width, active, cx);
            }
            self.sync_publications(cx);
        }
    }

    fn sync_demand(&mut self) {
        let navigation = self.client.navigation_snapshot();
        let session = self.client.gateway_session();
        let connected = session
            .status
            .as_ref()
            .is_some_and(|status| status.connection_state == GatewayConnectionState::Connected);
        let wanted = (self.window_active
            && connected
            && matches!(
                navigation.destination(),
                SemanticDestination::Plugins | SemanticDestination::Plugin { .. }
            ))
        .then(|| {
            self.workspace.as_ref().map(|workspace| {
                (
                    workspace.clone(),
                    self.selected.clone(),
                    self.connection,
                    self.client.plugins_management_allowed(workspace),
                )
            })
        })
        .flatten();
        if self.demand_key != wanted {
            self.demand = None;
            self.demand_key = wanted.clone();
            if let Some((workspace, selected, _, _)) = wanted {
                self.demand = Some(
                    self.client
                        .acquire_plugin_demand(&workspace, selected.as_deref()),
                );
            }
        }
    }

    pub(super) fn sync_publications(&mut self, cx: &mut Context<Self>) {
        let workspace = self
            .client
            .navigation_snapshot()
            .workspace_id()
            .map(str::to_owned);
        let connection = self.client.authorization_connection_generation();
        let authorization = self.client.authorization_permissions_epoch();
        if self.workspace != workspace
            || self.connection != connection
            || self.authorization != authorization
        {
            self.demand = None;
            self.demand_key = None;
            self.workspace = workspace;
            self.connection = connection;
            self.authorization = authorization;
            self.selected = None;
            if let Some(child) = self.child_details.take() {
                (child.set_sidebar_width)(self.sidebar_width, false, cx);
            }
            self.child_locator = None;
            self.restore_confirmation = None;
            self.source_picker = None;
            self.applying_update = None;
            self.presentation_error = None;
            self.upload.update(cx, |upload, cx| upload.clear(cx));
            self.input = Arc::new(PluginPublication::default());
            cx.notify();
        }
        let mut scopes = vec![
            ClientScope::Navigation,
            ClientScope::Session,
            ClientScope::Administration { workspace_id: None },
        ];
        if let Some(workspace) = &self.workspace {
            scopes.push(ClientScope::Administration {
                workspace_id: Some(workspace.clone()),
            });
            scopes.push(ClientScope::Plugins {
                workspace_id: workspace.clone(),
            });
        }
        self.binding.set_scopes(&scopes);
        let navigation = self.client.navigation_snapshot();
        let (selected, child) = match navigation.destination() {
            SemanticDestination::Plugin {
                plugin_id,
                component,
            } => (Some(plugin_id.clone()), component.clone()),
            _ => (None, None),
        };
        if self.selected != selected || self.route_child != child {
            self.selected = selected;
            self.route_child = child;
            if let Some(child) = self.child_details.take() {
                (child.set_sidebar_width)(self.sidebar_width, false, cx);
            }
            self.child_locator = None;
            self.restore_confirmation = None;
            cx.notify();
        }
        self.sync_demand();
        let next = self
            .workspace
            .as_ref()
            .and_then(|workspace| {
                if let Some(id) = &self.selected {
                    return self
                        .client
                        .plugin_details_publication(workspace, id)
                        .map(|mut p| {
                            // Catalog read and command failures have distinct owners.
                            p.error = self
                                .client
                                .snapshot(&ClientScope::Plugins {
                                    workspace_id: workspace.clone(),
                                })
                                .and_then(|p| p.snapshot().payload::<PluginPublication>())
                                .and_then(|p| p.error.clone());
                            Arc::new(p)
                        });
                }
                self.client
                    .snapshot(&ClientScope::Plugins {
                        workspace_id: workspace.clone(),
                    })
                    .and_then(|publication| publication.snapshot().payload::<PluginPublication>())
            })
            .filter(|publication| {
                publication.connection_id == self.client.gateway_session().startup.connection_id
            })
            .unwrap_or_else(|| Arc::new(PluginPublication::default()));
        let changed = self.input.connection_id != next.connection_id
            || self.input.plugin_id != next.plugin_id
            || self.input.plugins != next.plugins
            || self.input.details != next.details
            || self.input.loaded != next.loaded
            || self.input.busy != next.busy
            || self.input.managing != next.managing
            || self.input.error != next.error
            || self.input.operations != next.operations;
        self.input = next;
        self.sync_update_upload(cx);
        self.schedule_child_sync(cx);
        if changed {
            cx.notify();
        }
    }

    fn sync_update_upload(&mut self, cx: &mut Context<Self>) {
        let (Some((parent, operation)), Some(workspace)) = (&self.applying_update, &self.workspace)
        else {
            return;
        };
        let Some(upload) = self.client.skill_upload_snapshot(workspace, *operation) else {
            return;
        };
        let exact = matches!(&upload.target, SkillUploadTarget::PluginUpdatePreview { plugin_id, .. } if plugin_id == parent);
        // Only consumption of this exact preview acknowledges its Apply. A
        // selected parent's older result cannot establish this upload's outcome.
        if exact
            && upload.state == SkillUploadState::Succeeded
            && !upload.mutation_uncertain
            && upload.plugin_update_preview.is_none()
            && self.input.operations.get(parent).is_none_or(|o| !o.busy)
        {
            self.applying_update = None;
            self.upload.update(cx, |view, cx| view.clear(cx));
            cx.notify();
        }
    }

    fn schedule_child_sync(&mut self, cx: &mut Context<Self>) {
        if self.child_sync_pending {
            return;
        }
        self.child_sync_pending = true;
        let window = self.window;
        let owner = cx.weak_entity();
        cx.defer(move |cx| {
            let _ = window.update(cx, |_, window, cx| {
                let _ = owner.update(cx, |view, cx| {
                    view.child_sync_pending = false;
                    view.sync_child_surface(window, cx);
                });
            });
        });
    }

    fn sync_child_surface(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // A deferred callback always resolves the latest publication/route, not
        // a captured member that may have been restored with a new native ID.
        let navigation = self.client.navigation_snapshot();
        let wanted = self.workspace.as_ref().and_then(|workspace| {
            if navigation.workspace_id() != Some(workspace.as_str()) {
                return None;
            }
            let SemanticDestination::Plugin {
                plugin_id,
                component: Some(key),
            } = navigation.destination()
            else {
                return None;
            };
            let parent = self
                .client
                .plugin_details_publication(workspace, plugin_id)?
                .details?;
            let member = parent
                .components
                .into_iter()
                .find(|c| c.kind == key.kind && c.member_key == key.member_key)?;
            let native = match member.kind.as_str() {
                "mcp" => member.mcp_installation_id.clone()?,
                "skill" => member.skill_id.as_ref()?.to_string(),
                _ => return None,
            };
            Some((
                (
                    workspace.clone(),
                    plugin_id.clone(),
                    key.kind.clone(),
                    key.member_key.clone(),
                    native,
                ),
                member,
            ))
        });
        let locator = wanted.as_ref().map(|(locator, _)| locator.clone());
        if self.child_locator == locator {
            return;
        }
        if let Some(child) = self.child_details.take() {
            (child.set_sidebar_width)(self.sidebar_width, false, cx);
        }
        self.child_locator = locator;
        if let Some(((workspace, parent, _, _, _), member)) = wanted {
            self.child_details = (self.component_details)(&workspace, &parent, &member, window, cx);
            if let Some(child) = &self.child_details {
                (child.set_sidebar_width)(self.sidebar_width, self.window_active, cx);
            }
        }
        cx.notify();
    }

    pub(super) fn selected_plugin(&self) -> Option<&PluginItem> {
        let selected = self.selected.as_ref()?;
        self.input
            .details
            .as_ref()
            .filter(|plugin| &plugin.id == selected)
            .or_else(|| {
                self.input
                    .plugins
                    .iter()
                    .find(|plugin| &plugin.id == selected)
            })
    }

    pub(super) fn select_plugin(&mut self, selected: Option<String>, cx: &mut Context<Self>) {
        self.client.navigate(
            pioneer_client::navigation::NavigationIntent::Navigate {
                destination: selected.map_or(SemanticDestination::Plugins, |plugin_id| {
                    SemanticDestination::Plugin {
                        plugin_id,
                        component: None,
                    }
                }),
            },
            None,
        );
        self.sync_publications(cx);
        cx.notify();
    }

    pub(super) fn send_intent(&mut self, intent: PluginIntent, cx: &mut Context<Self>) {
        if let Some(workspace) = &self.workspace {
            let applying_update = match &intent {
                PluginIntent::ApplyUpdate {
                    upload_operation, ..
                } => self
                    .upload
                    .read(cx)
                    .publication()
                    .and_then(|p| match &p.target {
                        SkillUploadTarget::PluginUpdatePreview { plugin_id, .. }
                            if p.operation_id == *upload_operation =>
                        {
                            Some((plugin_id.clone(), *upload_operation))
                        }
                        _ => None,
                    }),
                _ => None,
            };
            if self.connection != self.client.authorization_connection_generation()
                || self.authorization != self.client.authorization_permissions_epoch()
                || !self.connected()
            {
                self.presentation_error = Some(t!("plugins.change_failed").to_string());
                cx.notify();
                return;
            }
            let parent = match &intent {
                PluginIntent::Mutate { plugin_id, .. }
                | PluginIntent::SetEnabled { plugin_id, .. } => Some(plugin_id.clone()),
                PluginIntent::Observe { plugin_id } => plugin_id.clone(),
                PluginIntent::Install { .. } => None,
                PluginIntent::ApplyUpdate { .. } => {
                    self.upload
                        .read(cx)
                        .publication()
                        .and_then(|p| match &p.target {
                            SkillUploadTarget::PluginUpdatePreview { plugin_id, .. } => {
                                Some(plugin_id.clone())
                            }
                            _ => None,
                        })
                }
                _ => self.selected.clone(),
            };
            let transition = if let Some(id) = &parent {
                self.client.plugin_intent_for(workspace, id, intent)
            } else {
                self.client.plugin_intent(workspace, intent)
            };
            self.presentation_error = matches!(
                transition.outcome(),
                pioneer_client::core::ClientTransitionOutcome::Rejected
                    | pioneer_client::core::ClientTransitionOutcome::Stale
            )
            .then(|| t!("plugins.change_failed").to_string());
            cx.notify();
            if applying_update.is_some()
                && transition.outcome() == pioneer_client::core::ClientTransitionOutcome::Changed
            {
                self.applying_update = applying_update;
            }
            self.sync_publications(cx);
        }
    }

    pub(super) fn refresh(&mut self, cx: &mut Context<Self>) {
        self.send_intent(
            PluginIntent::Observe {
                plugin_id: self.selected.clone(),
            },
            cx,
        );
    }

    pub(super) fn connected(&self) -> bool {
        self.client
            .gateway_session()
            .status
            .as_ref()
            .is_some_and(|s| s.connection_state == GatewayConnectionState::Connected)
    }
    fn operation(&self) -> Option<&pioneer_client::plugins::runtime::PluginOperationPublication> {
        self.selected
            .as_ref()
            .and_then(|id| self.input.operations.get(id))
    }
    pub(super) fn readable(&self) -> bool {
        self.workspace
            .as_deref()
            .and_then(|workspace| self.client.authorization_snapshot(Some(workspace), None))
            .is_some_and(|a| {
                let p = pioneer_client::authorization::principal_presentation_capabilities(&a);
                p.can_manage_capabilities || (p.can_use_skills && p.can_use_mcp)
            })
    }
    pub(super) fn access_label(&self) -> String {
        let known = self
            .workspace
            .as_deref()
            .and_then(|w| self.client.authorization_snapshot(Some(w), None))
            .is_some();
        t!(if known {
            "plugins.forbidden"
        } else {
            "plugins.unavailable"
        })
        .to_string()
    }
    pub(super) fn missing_parent_label(&self) -> String {
        if !self.connected() {
            return t!("plugins.offline").to_string();
        }
        if !self.readable() {
            return self.access_label();
        }
        if self.input.error.is_some() {
            return t!("plugins.error").to_string();
        }
        if self.initial_loading() {
            return t!("plugins.loading").to_string();
        }
        t!("plugins.unavailable").to_string()
    }
    pub(super) fn initial_loading(&self) -> bool {
        self.connected()
            && self.readable()
            && self.demand.is_some()
            && self.input.busy
            && !self.input.loaded
            && self.input.error.is_none()
    }
    pub(super) fn busy(&self, cx: &App) -> bool {
        !self.connected()
            || self.operation().is_some_and(|o| o.busy || (o.uncertain && !self.workspace.as_deref().zip(self.selected.as_deref()).is_some_and(|(workspace, parent)| self.client.plugin_parent_recovery_available(workspace, parent))))
            || self.upload.read(cx).active()
            || self
                .upload
                .read(cx)
                .publication()
                .is_some_and(|p| p.mutation_uncertain)
            || self.source_picker.is_some()
            || self.restore_confirmation.is_some()
            || self
                .selected
                .as_ref()
                .is_some_and(|id| self.selected_plugin().is_none_or(|p| &p.id != id))
    }
    pub(super) fn installation_error(&self, cx: &App) -> Option<String> {
        if self
            .upload
            .read(cx)
            .publication()
            .is_some_and(|p| p.mutation_uncertain)
        {
            return Some(t!("plugins.uncertain").to_string());
        }
        self.presentation_error
            .clone()
            .or_else(|| {
                self.operation().and_then(|o| {
                    if o.uncertain {
                        Some(t!("plugins.uncertain").to_string())
                    } else {
                        o.error
                            .as_ref()
                            .map(|_| t!("plugins.change_failed").to_string())
                    }
                })
            })
            .or_else(|| self.upload.read(cx).error())
    }

    pub(super) fn pick_source(&mut self, target: SkillUploadTarget, cx: &mut Context<Self>) {
        if self.busy(cx)
            || self.applying_update.is_some()
            || self
                .workspace
                .as_ref()
                .is_none_or(|w| !self.client.plugins_management_allowed(w))
        {
            return;
        }
        let paths = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: true,
            multiple: false,
            prompt: None,
        });
        let workspace = self.workspace.clone();
        let connection = self.connection;
        let authorization = self.authorization;
        self.source_picker = Some(cx.spawn(async move |view, cx| {
            let answer = paths.await;
            let _ = view.update(cx, |view, cx| {
                if view.workspace != workspace
                    || view.connection != connection
                    || view.client.authorization_permissions_epoch() != authorization
                {
                    return;
                }
                view.source_picker = None;
                cx.notify();
                let path = match answer {
                    Ok(Ok(Some(mut paths))) => paths.pop(),
                    Ok(Ok(None)) => None,
                    _ => {
                        view.presentation_error =
                            Some(t!("plugins.upload_source_unavailable").to_string());
                        None
                    }
                };
                let (Some(path), Some(workspace)) = (path, workspace) else {
                    return;
                };
                if !view.connected() || !view.client.plugins_management_allowed(&workspace) {
                    return;
                }
                view.preview_dismissed = false;
                let result = view
                    .upload
                    .update(cx, |upload, cx| upload.start(workspace, target, path, cx));
                view.presentation_error = result.err().map(PluginUploadView::failure_message);
            });
        }));
        cx.notify();
    }

    fn sync_upload(
        &mut self,
        publication: Option<Arc<SkillUploadPublication>>,
        cx: &mut Context<Self>,
    ) {
        if let Some(publication) =
            publication.filter(|publication| publication.state == SkillUploadState::Succeeded)
            && let Some(plugin) = &publication.plugin_result
        {
            self.select_plugin(Some(plugin.id.clone()), cx);
        }
        cx.notify();
    }
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
