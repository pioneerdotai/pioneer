use super::{Arc, PluginItem, PluginPublication, PluginsView, SemanticDestination};
use gpui_kit::{AppContext, TestAppContext};
use pioneer_client::{core::*, navigation::NavigationIntent};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    sync::Weak,
};

struct Registrar {
    client: Arc<ClientCore>,
    sinks: Rc<RefCell<HashMap<ClientScope, Weak<dyn ClientPublicationSink>>>>,
}
impl ClientBindingRegistrar for Registrar {
    fn register(
        &self,
        scope: ClientScope,
        sink: Weak<dyn ClientPublicationSink>,
    ) -> ClientBindingRegistration {
        self.sinks.borrow_mut().insert(scope.clone(), sink.clone());
        if let Some(publication) = self.client.snapshot(&scope)
            && let Some(sink) = sink.upgrade()
        {
            sink.publish(publication);
        }
        let sinks = self.sinks.clone();
        ClientBindingRegistration::new(move || {
            sinks.borrow_mut().remove(&scope);
        })
    }
}
impl Registrar {
    fn deliver(&self, scope: &ClientScope) {
        let sink = self.sinks.borrow().get(scope).unwrap().upgrade().unwrap();
        sink.publish(self.client.snapshot(scope).unwrap());
    }
}
fn publish(client: &ClientCore, scope: &ClientScope, input: PluginPublication) {
    publish_value(client, scope.clone(), |authority, scope, revisions| {
        authority.publication(scope, revisions, Arc::new(input))
    });
}

#[gpui_kit::test]
fn scoped_publications_update_plugin_details_without_redrawing_unchanged_content(
    cx: &mut TestAppContext,
) {
    cx.update(gpui_kit::init);
    let client = pioneer_client::catalog_test_support::client();
    set_origin(&client);
    select_workspace(&client);
    let transition = client.navigate(
        NavigationIntent::Navigate {
            destination: SemanticDestination::Plugins,
        },
        None,
    );
    assert_eq!(transition.outcome(), ClientTransitionOutcome::Changed);
    let scope = ClientScope::Plugins {
        workspace_id: "workspace".into(),
    };
    let plugin = PluginItem {
        id: "P".repeat(21),
        name: "Before".into(),
        version: None,
        description: None,
        metadata: Default::default(),
        enabled: true,
        allow_implicit_invocation: false,
        state: "installed".into(),
        revision: 1,
        status: "installed".into(),
        components: vec![],
        diagnostics: vec![],
    };
    let mut input = PluginPublication {
        revision: 1,
        generation: 1,
        loaded: true,
        connection_id: client.gateway_session().startup.connection_id,
        plugins: vec![plugin.clone()],
        details: Some(plugin.clone()),
        plugin_id: Some(plugin.id.clone()),
        ..Default::default()
    };
    publish(&client, &scope, input.clone());
    let registrar = Arc::new(Registrar {
        client: client.clone(),
        sinks: Rc::default(),
    });
    let (root, cx) = cx.add_window_view(|window, cx| {
        let view = PluginsView::new(
            client.clone(),
            registrar.clone(),
            Box::new(|_, _, _, _, _| None),
            window,
            cx,
        );
        gpui_kit::component::Root::new(view, window, cx)
    });
    let view = root.read_with(cx, |root, _| {
        root.view().clone().downcast::<PluginsView>().unwrap()
    });
    cx.update(|_, cx| {
        view.update(cx, |view, cx| {
            view.select_plugin(Some(plugin.id.clone()), cx)
        })
    });
    cx.run_until_parked();
    assert_eq!(
        view.read_with(cx, |view, _| view.selected_plugin().unwrap().name.clone()),
        "Before"
    );
    let notifications = Rc::new(Cell::new(0));
    let count = notifications.clone();
    let _observation = cx.update(|_, cx| cx.observe(&view, move |_, _| count.set(count.get() + 1)));
    input.revision += 1;
    input.generation += 1;
    publish(&client, &scope, input.clone());
    registrar.deliver(&scope);
    cx.run_until_parked();
    assert_eq!(notifications.get(), 0);

    input.revision += 1;
    input.plugins[0].name = "After".into();
    input.details.as_mut().unwrap().name = "After".into();
    publish(&client, &scope, input);
    registrar.deliver(&scope);
    cx.run_until_parked();
    assert_eq!(notifications.get(), 1);
    cx.update(|_, cx| {
        view.update(cx, |view, cx| {
            view.set_window_active(true, cx);
            view.set_window_active(false, cx);
            view.set_window_active(true, cx);
        })
    });
    cx.run_until_parked();
    assert_eq!(notifications.get(), 1);
    view.read_with(cx, |view, _| {
        assert_eq!(view.selected.as_deref(), Some(plugin.id.as_str()));
        assert_eq!(view.selected_plugin().unwrap().name, "After");
        assert!(!view.initial_loading());
    });
}

fn publish_value(
    client: &ClientCore,
    scope: ClientScope,
    draft: impl FnOnce(&ClientMutationAuthority, ClientScope, ClientRevisions) -> ClientPublicationDraft,
) {
    let current = client
        .snapshot(&scope)
        .map(|publication| publication.revisions());
    let advance = |revision: u64| revision.checked_add(1).expect("fixture revision overflow");
    let revisions = ClientRevisions::new(
        DomainRevision::new(advance(current.map_or(0, |r| r.domain().get()))),
        PresentationRevision::new(advance(current.map_or(0, |r| r.presentation().get()))),
        ContentRevision::new(advance(current.map_or(0, |r| r.content().get()))),
        ScopedRevision::new(advance(current.map_or(0, |r| r.scoped().get()))),
    );
    let authority = ClientMutationAuthority::for_test();
    let transition = client.transition(
        &authority,
        vec![draft(&authority, scope.clone(), revisions)],
        vec![],
    );
    assert_eq!(
        transition.outcome(),
        ClientTransitionOutcome::Changed,
        "fixture publication was not admitted for {scope:?}",
    );
    assert_eq!(client.snapshot(&scope).unwrap().revisions(), revisions);
}
fn parent(id: &str) -> PluginItem {
    PluginItem {
        id: id.into(),
        name: id.into(),
        version: None,
        description: None,
        metadata: Default::default(),
        enabled: true,
        allow_implicit_invocation: false,
        state: "installed".into(),
        revision: 1,
        status: "installed".into(),
        components: vec![],
        diagnostics: vec![],
    }
}
fn select_workspace(client: &ClientCore) {
    let transition = client.navigate(
        NavigationIntent::SelectWorkspace {
            workspace_id: Some("workspace".into()),
        },
        None,
    );
    assert_eq!(transition.outcome(), ClientTransitionOutcome::Changed);
    assert_eq!(
        client.navigation_snapshot().workspace_id(),
        Some("workspace")
    );
}
fn set_origin(client: &ClientCore) {
    let mut session = (*client.gateway_session()).clone();
    session.startup.connection_id = Some(7); // Existing catalog_test_support origin.
    publish_value(
        client,
        ClientScope::Session,
        |authority, scope, revisions| authority.publication(scope, revisions, Arc::new(session)),
    );
    assert_eq!(client.gateway_session().startup.connection_id, Some(7));
}
fn set_connected(client: &ClientCore) {
    let mut session = (*client.gateway_session()).clone();
    session.status = Some(pioneer_client::state::reducers::GatewayStatusProjection {
        status: pioneer_client::state::reducers::GatewayStatusTextUpdate::KeepExisting,
        status_level: pioneer_client::state::client_state::GatewayStatusLevel::Connected,
        connection_state: pioneer_client::state::client_state::GatewayConnectionState::Connected,
        clear_gateway_error: false,
    });
    publish_value(
        client,
        ClientScope::Session,
        |authority, scope, revisions| authority.publication(scope, revisions, Arc::new(session)),
    );
    assert_eq!(
        client
            .gateway_session()
            .status
            .as_ref()
            .unwrap()
            .connection_state,
        pioneer_client::state::client_state::GatewayConnectionState::Connected,
    );
}
fn workspace_access(client: &ClientCore, allowed: bool) {
    use pioneer_protocol::*;
    let mut authorization = client.authorization_snapshot(None, None).unwrap();
    authorization.authorization_revision += 1;
    authorization.global.can_manage_capabilities = false;
    let resources = AuthorizationOperationalResourceProjection {
        fingerprint: "fixture".into(),
        ..Default::default()
    };
    authorization.workspace = Some(AuthorizationWorkspaceCapabilitySnapshot {
        workspace_id: "workspace".into(),
        capabilities: AuthorizationWorkspaceCapabilities {
            can_read: true,
            can_use_skills: allowed,
            can_use_mcp: allowed,
            ..Default::default()
        },
        operational_resources: resources.clone(),
        execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
            fingerprint: "fixture".into(),
            resources,
            permission_options: vec![],
            can_attach_artifacts: false,
            mcp_invocation_limits: Default::default(),
        },
    });
    let (generation, connection) = client.current_auth_ticket();
    assert_eq!(
        client.accept_authorization_projection(generation, connection, authorization),
        pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted,
    );
    let accepted = client
        .authorization_snapshot(Some("workspace"), None)
        .unwrap();
    let capabilities = &accepted.workspace.as_ref().unwrap().capabilities;
    assert_eq!(capabilities.can_use_skills, allowed);
    assert_eq!(capabilities.can_use_mcp, allowed);
}

#[gpui_kit::test]
fn accepted_update_keeps_exact_upload_through_other_parent_and_uncertain_settlement(
    cx: &mut TestAppContext,
) {
    use pioneer_client::plugins::runtime::PluginOperationPublication;
    use pioneer_client::skills::{operations::SkillUploadTarget, upload_flow::*};
    use pioneer_protocol::*;
    cx.update(gpui_kit::init);
    let client = pioneer_client::catalog_test_support::client();
    set_origin(&client);
    select_workspace(&client);
    let registrar = Arc::new(Registrar {
        client: client.clone(),
        sinks: Rc::default(),
    });
    let (root, cx) = cx.add_window_view(|window, cx| {
        let view = PluginsView::new(
            client.clone(),
            registrar.clone(),
            Box::new(|_, _, _, _, _| None),
            window,
            cx,
        );
        gpui_kit::component::Root::new(view, window, cx)
    });
    let view = root.read_with(cx, |root, _| {
        root.view().clone().downcast::<PluginsView>().unwrap()
    });
    let mut publication = PluginPublication {
        revision: 1,
        connection_id: Some(7),
        loaded: true,
        plugins: vec![parent("A"), parent("B")],
        ..Default::default()
    };
    publication.operations.insert(
        "A".into(),
        PluginOperationPublication {
            busy: true,
            ..Default::default()
        },
    );
    publication.operations.insert(
        "B".into(),
        PluginOperationPublication {
            result: Some(PluginsMutationResponse {
                plugin: Some(parent("B")),
                removed: false,
            }),
            ..Default::default()
        },
    );
    let mut upload = SkillUploadPublication {
        failure: None,
        mutation_uncertain: false,
        plugin_preview: None,
        plugin_result: None,
        plugin_update_preview: Some(PluginUpdatePreviewPublication {
            upload_id: "exact-upload".into(),
            preview: PluginsUpdatePreviewResponse {
                supporting_files_changed: false,

                package: PluginsPreviewResponse {
                    name: "A".into(),
                    version: None,
                    description: None,
                    fingerprint: "exact-fingerprint".into(),
                    components: vec![],
                    diagnostics: vec![],
                },
                added: vec![],
                updated: vec![],
                removed: vec![],
                identity_resets: vec![],
                authorization_changes: vec![],
            },
        }),
        operation_id: 71,
        generation: 1,
        revision: 1,
        state: SkillUploadState::Succeeded,
        sent_bytes: 8,
        total_bytes: 8,
        target: SkillUploadTarget::PluginUpdatePreview {
            plugin_id: "A".into(),
            expected_revision: 1,
        },
        pack: false,
    };
    let scope = ClientScope::Plugins {
        workspace_id: "workspace".into(),
    };
    let upload_scope = ClientScope::SkillsUpload {
        workspace_id: "workspace".into(),
        operation_id: 71,
    };
    publish_value(
        &client,
        upload_scope.clone(),
        |authority, scope, revisions| {
            authority.publication(scope, revisions, Arc::new(upload.clone()))
        },
    );
    publish(&client, &scope, publication.clone());
    cx.update(|_, cx| {
        view.update(cx, |v, cx| {
            v.applying_update = Some(("A".into(), 71));
            v.select_plugin(Some("B".into()), cx);
        })
    });
    assert_eq!(
        view.read_with(cx, |v, _| v.applying_update.clone()),
        Some(("A".into(), 71))
    );
    publication.operations.get_mut("A").unwrap().busy = false;
    publication.operations.get_mut("A").unwrap().uncertain = true;
    publication.error = Some("read failed".into());
    publication.revision += 1;
    upload.mutation_uncertain = true;
    upload.revision += 1;
    publish_value(
        &client,
        upload_scope.clone(),
        |authority, scope, revisions| {
            authority.publication(scope, revisions, Arc::new(upload.clone()))
        },
    );
    publish(&client, &scope, publication.clone());
    cx.update(|_, cx| view.update(cx, |v, cx| v.sync_publications(cx)));
    assert!(view.read_with(cx, |v, _| v.applying_update.is_some()));
    assert_eq!(
        client
            .skill_upload_snapshot("workspace", 71)
            .unwrap()
            .plugin_update_preview
            .as_ref()
            .unwrap()
            .preview
            .package
            .fingerprint,
        "exact-fingerprint"
    );
    publication.operations.get_mut("A").unwrap().uncertain = false;
    publication.revision += 1;
    upload.mutation_uncertain = false;
    upload.plugin_update_preview = None;
    upload.revision += 1;
    publish_value(&client, upload_scope, |authority, scope, revisions| {
        authority.publication(scope, revisions, Arc::new(upload))
    });
    publish(&client, &scope, publication);
    cx.update(|_, cx| view.update(cx, |v, cx| v.sync_publications(cx)));
    assert!(view.read_with(cx, |v, _| v.applying_update.is_none()));
    assert_eq!(
        view.read_with(cx, |v, _| v.selected.clone()),
        Some("B".into())
    );
}

struct NativeSurface {
    _demand: Option<pioneer_client::mcp::store::McpDemand>,
}
impl gpui_kit::Render for NativeSurface {
    fn render(
        &mut self,
        _: &mut gpui_kit::Window,
        _: &mut gpui_kit::Context<Self>,
    ) -> impl gpui_kit::IntoElement {
        gpui_kit::div()
    }
}
#[gpui_kit::test]
fn publication_replaces_child_only_when_native_locator_changes_and_releases_old_demand(
    cx: &mut TestAppContext,
) {
    cx.update(gpui_kit::init);
    let client = pioneer_client::catalog_test_support::client();
    set_origin(&client);
    select_workspace(&client);
    let registrar = Arc::new(Registrar {
        client: client.clone(),
        sinks: Rc::default(),
    });
    let mounts = Rc::new(RefCell::new(Vec::new()));
    let created = mounts.clone();
    let native_client = client.clone();
    let (root, cx) = cx.add_window_view(|window, cx| {
        let view = PluginsView::new(
            client.clone(),
            registrar.clone(),
            Box::new(move |workspace, _, member, _, cx| {
                let id = member.mcp_installation_id.as_ref().unwrap();
                created.borrow_mut().push(id.clone());
                let child = cx.new(|_| NativeSurface {
                    _demand: Some(native_client.acquire_mcp_demand(workspace, Some(id))),
                });
                let activity = child.downgrade();
                Some(super::PluginComponentDetails::new(
                    child.clone().into(),
                    child.into(),
                    move |_, active, cx| {
                        if !active {
                            let _ = activity.update(cx, |child, _| {
                                child._demand.take();
                            });
                        }
                    },
                ))
            }),
            window,
            cx,
        );
        gpui_kit::component::Root::new(view, window, cx)
    });
    let view = root.read_with(cx, |root, _| {
        root.view().clone().downcast::<PluginsView>().unwrap()
    });
    let mut plugin = parent("A");
    plugin
        .components
        .push(pioneer_client::plugins::PluginComponentItem {
            kind: "mcp".into(),
            member_key: "member".into(),
            status: "installed".into(),
            diagnostic: None,
            skill_id: None,
            mcp_installation_id: Some("old".into()),
            runtime_status: None,
        });
    let mut publication = PluginPublication {
        revision: 1,
        connection_id: Some(7),
        loaded: true,
        plugins: vec![plugin],
        ..Default::default()
    };
    let scope = ClientScope::Plugins {
        workspace_id: "workspace".into(),
    };
    publish(&client, &scope, publication.clone());
    let transition = client.navigate(
        NavigationIntent::Navigate {
            destination: SemanticDestination::Plugin {
                plugin_id: "A".into(),
                component: Some(pioneer_client::plugins::PluginComponentKey {
                    kind: "mcp".into(),
                    member_key: "member".into(),
                }),
            },
        },
        None,
    );
    assert_eq!(transition.outcome(), ClientTransitionOutcome::Changed);
    assert!(matches!(
        client.navigation_snapshot().destination(),
        SemanticDestination::Plugin { plugin_id, component: Some(_) } if plugin_id == "A",
    ));
    cx.update(|_, cx| view.update(cx, |v, cx| v.set_window_active(true, cx)));
    cx.run_until_parked();
    assert_eq!(
        view.read_with(cx, |v, _| v.selected.clone()),
        Some("A".into())
    );
    assert_eq!(&*mounts.borrow(), &["old".to_string()]);
    let original = view.read_with(cx, |v, _| v.child_details.as_ref().unwrap().content.clone());
    assert_eq!(client.mcp_demand_count_for_test("workspace"), 1);
    publication.plugins[0].components[0].runtime_status = Some("restarting".into());
    publication.revision += 1;
    publish(&client, &scope, publication.clone());
    cx.update(|_, cx| view.update(cx, |v, cx| v.sync_publications(cx)));
    cx.run_until_parked();
    assert_eq!(
        view.read_with(cx, |v, _| v
            .child_details
            .as_ref()
            .unwrap()
            .content
            .entity_id()),
        original.entity_id()
    );
    drop(original);
    publication.plugins[0].components[0].mcp_installation_id = Some("new".into());
    publication.revision += 1;
    publish(&client, &scope, publication.clone());
    cx.update(|_, cx| view.update(cx, |v, cx| v.sync_publications(cx)));
    cx.run_until_parked();
    assert_eq!(&*mounts.borrow(), &["old".to_string(), "new".to_string()]);
    assert_eq!(client.mcp_demand_count_for_test("workspace"), 1);
    publication.plugins[0].components[0].mcp_installation_id = None;
    publication.revision += 1;
    publish(&client, &scope, publication);
    cx.update(|_, cx| view.update(cx, |v, cx| v.sync_publications(cx)));
    cx.run_until_parked();
    assert!(view.read_with(cx, |v, _| v.child_details.is_none()));
    assert_eq!(client.mcp_demand_count_for_test("workspace"), 0);
}

#[gpui_kit::test]
fn missing_parent_uses_admission_authorization_and_connection_states(cx: &mut TestAppContext) {
    cx.update(gpui_kit::init);
    let client = pioneer_client::catalog_test_support::client();
    set_origin(&client);
    select_workspace(&client);
    let registrar = Arc::new(Registrar {
        client: client.clone(),
        sinks: Rc::default(),
    });
    let (root, cx) = cx.add_window_view(|window, cx| {
        gpui_kit::component::Root::new(
            PluginsView::new(
                client.clone(),
                registrar,
                Box::new(|_, _, _, _, _| None),
                window,
                cx,
            ),
            window,
            cx,
        )
    });
    let view = root.read_with(cx, |root, _| {
        root.view().clone().downcast::<PluginsView>().unwrap()
    });
    assert!(!view.read_with(cx, |v, _| v.connected()));
    assert_eq!(
        view.read_with(cx, |v, _| v.missing_parent_label()),
        t!("plugins.offline").to_string()
    );
    cx.update(|_, cx| view.update(cx, |v, cx| v.select_plugin(Some("missing".into()), cx)));
    assert!(matches!(
        client.navigation_snapshot().destination(),
        SemanticDestination::Plugin { plugin_id, .. } if plugin_id == "missing",
    ));
    set_connected(&client);
    cx.update(|_, cx| view.update(cx, |v, cx| v.sync_publications(cx)));
    assert!(view.read_with(cx, |v, _| v.connected() && v.selected_plugin().is_none()));
    assert_eq!(
        view.read_with(cx, |v, _| v.missing_parent_label()),
        t!("plugins.unavailable").to_string()
    );
    workspace_access(&client, false);
    cx.update(|_, cx| {
        view.update(cx, |v, cx| {
            v.sync_publications(cx);
            Arc::make_mut(&mut v.input).error = Some("read failed".into());
        })
    });
    assert!(view.read_with(cx, |v, _| v.connected()
        && !v.readable()
        && v.input.error.is_some()));
    assert_eq!(
        view.read_with(cx, |v, _| v.missing_parent_label()),
        t!("plugins.forbidden").to_string()
    );
    workspace_access(&client, true);
    cx.update(|_, cx| {
        view.update(cx, |v, cx| {
            v.set_window_active(true, cx);
            let p = Arc::make_mut(&mut v.input);
            p.busy = true;
            p.error = None;
        })
    });
    assert!(view.read_with(cx, |v, _| v.connected()
        && v.readable()
        && v.initial_loading()));
    assert_eq!(
        view.read_with(cx, |v, _| v.missing_parent_label()),
        t!("plugins.loading").to_string()
    );
    cx.update(|_, cx| {
        view.update(cx, |v, _| {
            let p = Arc::make_mut(&mut v.input);
            p.loaded = true;
            p.busy = false;
        })
    });
    assert!(view.read_with(cx, |v, _| {
        v.connected() && v.input.loaded && !v.initial_loading() && v.selected_plugin().is_none()
    }));
    assert_eq!(
        view.read_with(cx, |v, _| v.missing_parent_label()),
        t!("plugins.unavailable").to_string()
    );
}
