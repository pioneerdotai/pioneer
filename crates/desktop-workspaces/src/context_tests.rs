use super::{WorkspaceNavigationConfig, WorkspaceNavigationView};
use gpui_kit::component::{Root, WindowExt};
use gpui_kit::{AppContext, TestAppContext};
use pioneer_client::core::ClientCore;
use pioneer_client::core::ClientScope;
use pioneer_desktop_foundation::ClientBindingRegistrar;
use pioneer_desktop_foundation::{ClientBindingRegistration, ClientPublicationSink};
use std::{cell::RefCell, collections::HashSet};
use std::{collections::HashMap, rc::Rc, sync::Arc};

struct Registrar(Rc<RefCell<HashSet<ClientScope>>>);
impl ClientBindingRegistrar for Registrar {
    fn register(
        &self,
        scope: ClientScope,
        _: std::sync::Weak<dyn ClientPublicationSink>,
    ) -> ClientBindingRegistration {
        self.0.borrow_mut().insert(scope.clone());
        let registrations = self.0.clone();
        ClientBindingRegistration::new(move || {
            registrations.borrow_mut().remove(&scope);
        })
    }
}
pub(super) fn config(
    core: Arc<ClientCore>,
    registrations: Rc<RefCell<HashSet<ClientScope>>>,
) -> WorkspaceNavigationConfig {
    WorkspaceNavigationConfig::new(
        core,
        Arc::new(Registrar(registrations)),
        |_, _| HashMap::new(),
        |_, _, _| {},
        |builder, window, cx| {
            window.open_dialog(cx, move |dialog, window, cx| builder(dialog, window, cx))
        },
        |builder, window, cx| {
            window.open_dialog(cx, move |dialog, window, cx| builder(dialog, window, cx))
        },
        |builder, window, cx| {
            window.open_dialog(cx, move |dialog, window, cx| builder(dialog, window, cx))
        },
        |builder, window, cx| {
            window.open_dialog(cx, move |dialog, window, cx| builder(dialog, window, cx))
        },
    )
}

#[gpui_kit::test]
fn folder_pointer_and_keyboard_expansion_survives_reconciliation(cx: &mut TestAppContext) {
    use gpui_kit::{Modifiers, point, px};
    use pioneer_client::workspaces::projection::thread_tree_snapshot_from_parts;

    cx.update(gpui_kit::init);
    cx.update(|cx| cx.set_reduce_motion(true));
    let core = Arc::new(ClientCore::new());
    core.open_workspace_thread("w".into(), None, None);
    let saves = Rc::new(RefCell::new(Vec::new()));
    let saved = saves.clone();
    let mut config = config(core.clone(), Rc::new(RefCell::new(HashSet::new())));
    config.save_expansion = Rc::new(move |_, state, _| saved.borrow_mut().push(state));
    let folders = vec![
        serde_json::from_value(serde_json::json!({"id":"parent","workspace_id":"w","name":"Parent","created_at":1,"updated_at":1})).unwrap(),
        serde_json::from_value(serde_json::json!({"id":"child","workspace_id":"w","parent_folder_id":"parent","name":"Child","created_at":1,"updated_at":1})).unwrap(),
    ];
    let thread: pioneer_client::workspaces::Thread = serde_json::from_value(serde_json::json!({
        "id":"t","workspace_id":"w","name":"Thread","preview":"","mode":"Chat",
        "model":"m","model_provider":"p","created_at":1,"updated_at":1,"status":"Idle","turns":[]
    }))
    .unwrap();
    core.upsert_thread(thread.clone());
    let snapshot = thread_tree_snapshot_from_parts(
        "w".into(),
        vec![thread],
        vec![],
        vec![
            serde_json::from_value(serde_json::json!({"thread_id":"t","unread_count":9})).unwrap(),
        ],
        folders,
        vec![
            serde_json::from_value(serde_json::json!({
                "thread_id":"t","workspace_id":"w","folder_id":"child"
            }))
            .unwrap(),
        ],
        vec![],
    );
    let publication = Arc::new(serde_json::from_value(serde_json::json!({
        "revision":1,"snapshot":snapshot,"changes":{"changed":[],"removed":[],"reordered_folders":[]},"loading":false,"error":null
    })).unwrap());
    let (root, cx) = cx.add_window_view(|window, cx| {
        let sidebar = cx.new(|cx| {
            let mut sidebar = super::sidebar::ThreadSidebarView::new(config, cx);
            sidebar.input = Some(publication);
            sidebar.refresh_sidebar_tree_state(false, false, cx);
            sidebar
        });
        Root::new(sidebar, window, cx)
    });
    let sidebar = root.read_with(cx, |root, _| {
        root.view()
            .clone()
            .downcast::<super::sidebar::ThreadSidebarView>()
            .unwrap()
    });
    let tree = sidebar.read_with(cx, |sidebar, _| sidebar.thread_tree_state.clone());
    let folder_id = pioneer_client::threads::tree::sidebar_folder_node_id("parent");
    let folder_id: gpui_kit::SharedString = folder_id.into();
    let child_id: gpui_kit::SharedString =
        pioneer_client::threads::tree::sidebar_folder_node_id("child").into();
    let position = tree.read_with(cx, |tree, _| {
        let index = tree.index_of(&folder_id).unwrap();
        let bounds = tree.scroll_handle().0.borrow().base_handle.bounds();
        point(
            bounds.left() + px(70.),
            bounds.top() + px(index as f32 * 32. + 16.),
        )
    });
    for expanded in [true, false, true, false] {
        let before = saves.borrow().len();
        cx.simulate_click(position, Modifiers::default());
        cx.run_until_parked();
        assert_eq!(
            saves.borrow().len(),
            before + 1,
            "one state change per click"
        );
        assert_eq!(
            saves.borrow().last().unwrap().get("parent"),
            Some(&expanded)
        );
        assert_eq!(
            sidebar.read_with(cx, |sidebar, _| sidebar.visible_node_count),
            if expanded { 3 } else { 2 },
            "root context area must track pointer disclosure immediately"
        );
        assert_eq!(
            sidebar.read_with(cx, |sidebar, _| sidebar
                .input
                .as_ref()
                .unwrap()
                .snapshot()
                .unread_by_folder_id["parent"]),
            9
        );
        sidebar.update(cx, |sidebar, cx| {
            sidebar.refresh_sidebar_tree_state(false, false, cx);
        });
        cx.run_until_parked();
        assert_eq!(
            tree.read_with(cx, |tree, _| (
                tree.entry(tree.index_of(&folder_id).unwrap())
                    .unwrap()
                    .is_expanded(),
                tree.index_of(&child_id).is_some(),
            )),
            (expanded, expanded)
        );
    }
    // Counter-only publications must keep the selected folder and its expansion.
    let selected = tree.read_with(cx, |tree, _| tree.selected_index());
    assert!(selected.is_some());
    for (revision, count, running) in [(2, 5, false), (3, 0, true), (4, 0, false)] {
        let mut next = snapshot.clone();
        if running {
            next.running_task_thread_ids.insert("t".into());
            next.running_task_folder_ids
                .extend(["parent".into(), "child".into()]);
        }
        next.unread[0].unread_count = count;
        for total in next.unread_by_folder_id.values_mut() {
            *total = count;
        }
        sidebar.update(cx, |sidebar, cx| {
            sidebar.input = Some(Arc::new(serde_json::from_value(serde_json::json!({
                "revision": revision, "snapshot": next,
                "changes":{"changed":[{"kind":"folder","id":"parent"},{"kind":"folder","id":"child"}],
                           "removed":[],"reordered_folders":[]},"loading":false,"error":null
            })).unwrap()));
            if sidebar.refresh_sidebar_tree_state(false, false, cx) {
                cx.notify();
            }
        });
        cx.run_until_parked();
        assert_eq!(
            sidebar.read_with(cx, |sidebar, _| sidebar.task_activity_indicators.len()),
            if running { 3 } else { 0 }
        );
        assert_eq!(
            tree.read_with(cx, |tree, _| tree.selected_index()),
            selected
        );
        assert!(!tree.read_with(cx, |tree, _| {
            tree.entry(tree.index_of(&folder_id).unwrap())
                .unwrap()
                .is_expanded()
        }));
        assert_eq!(
            sidebar.read_with(cx, |sidebar, _| sidebar
                .input
                .as_ref()
                .unwrap()
                .snapshot()
                .unread_by_folder_id["parent"]),
            count
        );
    }
    cx.update(|window, cx| tree.update(cx, |tree, cx| tree.focus(window, cx)));
    for (key, expanded) in [("right", true), ("left", false)] {
        let before = saves.borrow().len();
        cx.simulate_keystrokes(key);
        cx.run_until_parked();
        assert_eq!(saves.borrow().len(), before + 1);
        assert_eq!(
            saves.borrow().last().unwrap().get("parent"),
            Some(&expanded)
        );
        assert_eq!(
            sidebar.read_with(cx, |sidebar, _| sidebar.visible_node_count),
            if expanded { 3 } else { 2 },
            "root context area must track keyboard disclosure immediately"
        );
    }
}
#[gpui_kit::test]
fn retained_workspace_root_releases_its_scoped_bindings(cx: &mut TestAppContext) {
    cx.update(gpui_kit::init);
    let core = Arc::new(ClientCore::new());
    let registrations = Rc::new(RefCell::new(HashSet::new()));
    let config = config(core, registrations.clone());
    let (root, cx) = cx.add_window_view(|window, cx| {
        let view = cx.new(|cx| WorkspaceNavigationView::new(config, cx));
        Root::new(view, window, cx)
    });
    let view = root.read_with(cx, |root, _| {
        root.view()
            .clone()
            .downcast::<WorkspaceNavigationView>()
            .unwrap()
    });
    assert_eq!(registrations.borrow().len(), 5);
    view.update(cx, |view, cx| view.close(cx));
    assert!(registrations.borrow().is_empty());
}
#[test]
fn workspace_feature_has_no_shell_or_sibling_import_or_render_time_resource_creation() {
    for source in [
        include_str!("lib.rs"),
        include_str!("sidebar.rs"),
        include_str!("tree_view.rs"),
        include_str!("catalog_view.rs"),
        include_str!("dialogs.rs"),
        include_str!("catalog_dialogs.rs"),
    ] {
        for forbidden in [
            "pioneer_desktop_update",
            "pioneer_desktop_task_notifications",
            concat!("LegacyScreen", "Adapter"),
            "DesktopShellView",
            "GatewayWsEvent",
        ] {
            assert!(!source.contains(forbidden), "{forbidden}");
        }
    }
    for source in [
        include_str!("tree_view.rs"),
        include_str!("catalog_view.rs"),
    ] {
        assert!(!source.contains("cx.subscribe("));
        assert!(!source.contains("cx.focus_handle("));
        assert!(!source.contains("cx.spawn("));
        assert!(!source.contains("thread-tree-row-{ix}"));
    }
}

#[gpui_kit::test]
fn controlled_tree_keeps_domain_selection_and_retained_state_across_metadata_and_reorder(
    cx: &mut TestAppContext,
) {
    cx.update(gpui_kit::init);
    let core = Arc::new(ClientCore::new());
    let thread = |id: &str, updated: i64| {
        serde_json::from_value(serde_json::json!({
        "id":id,"workspace_id":"w","name":id,"preview":"","mode":"Chat","model":"m","model_provider":"p","created_at":1,"updated_at":updated,"status":"Idle","turns":[]
    })).unwrap()
    };
    core.upsert_thread(thread("a", 1));
    core.upsert_thread(thread("b", 2));
    core.navigate(
        pioneer_client::navigation::NavigationIntent::SelectThread {
            workspace_id: Some("w".into()),
            thread_id: Some("a".into()),
        },
        None,
    );
    let config = config(core.clone(), Rc::new(RefCell::new(HashSet::new())));
    let view = cx.new(|cx| WorkspaceNavigationView::new(config, cx));
    let sidebar = view.read_with(cx, |view, _| view.sidebar.clone());
    let events = Rc::new(RefCell::new(0usize));
    let event_count = events.clone();
    let _subscription = view.update(cx, |_, cx| {
        cx.subscribe(
            &sidebar,
            move |_, _, _: &super::WorkspaceNavigationEvent, _| *event_count.borrow_mut() += 1,
        )
    });
    let tree = sidebar.read_with(cx, |sidebar, _| sidebar.thread_tree_state.clone());
    let selected = tree.read_with(cx, |tree, _| {
        tree.selected_item().map(|item| item.id.clone())
    });
    let navigation = core.snapshot(&ClientScope::Navigation).unwrap();
    core.apply_directory_read(
        "w",
        "a",
        &serde_json::from_value(serde_json::json!({"sort_key":"1","through_turn_id":"turn"}))
            .unwrap(),
        3,
    );
    sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
    assert_eq!(
        tree.read_with(cx, |tree, _| tree
            .selected_item()
            .map(|item| item.id.clone())),
        selected
    );
    core.upsert_thread(thread("a", 4));
    sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
    assert_eq!(
        tree.read_with(cx, |tree, _| tree
            .selected_item()
            .map(|item| item.id.clone())),
        selected
    );
    assert_eq!(
        navigation.revisions(),
        core.snapshot(&ClientScope::Navigation).unwrap().revisions()
    );
    core.open_workspace_thread("w".into(), Some("b".into()), None);
    sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
    assert_eq!(
        *events.borrow(),
        0,
        "another capability's navigation must not emit a workspace command event"
    );
    core.open_workspace_thread("w".into(), Some("a".into()), None);
    sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
    core.remove_thread_store("a");
    sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
    assert!(tree.read_with(cx, |tree, _| tree.selected_item().is_none()));
    assert_eq!(
        tree,
        sidebar.read_with(cx, |sidebar, _| sidebar.thread_tree_state.clone())
    );
    view.update(cx, |view, cx| view.close(cx));
}

#[gpui_kit::test]
fn workspace_lock_uses_canonical_composer_operations_without_a_shell_callback(
    cx: &mut TestAppContext,
) {
    use pioneer_client::composer::store::{
        ComposerIntent, ComposerOperationCompletion, ComposerOperationKind,
    };
    use pioneer_client::core::ClientTransitionOutcome;
    cx.update(gpui_kit::init);
    let core = pioneer_client::catalog_test_support::settings_client();
    assert!(core.current_auth().is_some());
    core.upsert_thread(serde_json::from_value(serde_json::json!({
        "id":"a","workspace_id":"w","name":"Thread","preview":"","mode":"Chat",
        "model":"m","model_provider":"p","created_at":1,"updated_at":1,"status":"Idle","turns":[]
    })).unwrap());
    core.activate_thread(Some("a".into()), Some("w".into()));
    core.composer_intent(ComposerIntent::Open {
        thread_id: "a".into(),
        defaults: Default::default(),
    });
    let config = config(core.clone(), Rc::new(RefCell::new(HashSet::new())));
    let (root, cx) = cx.add_window_view(|window, cx| {
        let view = cx.new(|cx| WorkspaceNavigationView::new(config, cx));
        Root::new(view, window, cx)
    });
    let sidebar = root.read_with(cx, |root, cx| {
        root.view()
            .clone()
            .downcast::<WorkspaceNavigationView>()
            .unwrap()
            .read(cx)
            .sidebar
            .clone()
    });
    cx.run_until_parked();
    let tree = sidebar.read_with(cx, |sidebar, _| sidebar.thread_tree_state.clone());
    let notifications = Rc::new(RefCell::new(0usize));
    let tree_notifications = Rc::new(RefCell::new(0usize));
    let _observations = cx.update(|_, cx| {
        let notifications = notifications.clone();
        let tree_notifications = tree_notifications.clone();
        (
            cx.observe(&sidebar, move |_, _| *notifications.borrow_mut() += 1),
            cx.observe(&tree, move |_, _| *tree_notifications.borrow_mut() += 1),
        )
    });
    // Text publications share the same sidebar inputs and effective lock.
    for text in ["one", "two", ""] {
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: core.composer_snapshot("a").unwrap().draft_id(),
            text: text.into(),
        });
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        cx.run_until_parked();
        assert_eq!(*notifications.borrow(), 0);
        assert_eq!(*tree_notifications.borrow(), 0);
    }
    for (operation, locked) in [
        (ComposerOperationKind::Send, true),
        (ComposerOperationKind::Voice, true),
        (ComposerOperationKind::PickFiles, false),
    ] {
        let before = *notifications.borrow();
        let draft = core.composer_snapshot("a").unwrap().draft_id();
        assert_eq!(
            core.composer_intent(ComposerIntent::BeginOperation {
                thread_id: "a".into(),
                draft_id: draft,
                operation
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        let input = core.composer_snapshot("a").unwrap();
        let identity = input.operation().unwrap().identity.clone();
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        cx.run_until_parked();
        assert_eq!(
            sidebar.read_with(cx, |sidebar, _| sidebar.context_locked),
            locked
        );
        assert_eq!(*notifications.borrow() > before, locked);
        assert_eq!(
            *tree_notifications.borrow(),
            0,
            "lock changes preserve the tree"
        );
        assert!(
            Arc::ptr_eq(&input, &core.composer_snapshot("a").unwrap()),
            "presentation must not echo a Client intent"
        );
        core.complete_composer_operation(identity.clone(), ComposerOperationCompletion::Cancelled);
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        cx.run_until_parked();
        assert!(!sidebar.read_with(cx, |sidebar, _| sidebar.context_locked));
        assert!(
            !core.complete_composer_operation(identity, ComposerOperationCompletion::Cancelled)
        );
    }
    let selected = tree.read_with(cx, |tree, _| tree.selected_item().unwrap().id.clone());
    let input = sidebar.read_with(cx, |sidebar, _| sidebar.input.clone().unwrap());
    let navigation = core.navigation_snapshot();
    let catalog = core.workspace_catalog();
    let owner = core.authorization_connection_generation();
    let capabilities = core.authorization_snapshot(Some("w"), Some("a"));
    assert!(!core.gateway_refresh_in_flight());
    assert!(!sidebar.read_with(cx, |sidebar, _| sidebar.workspace_unavailable()));
    let refresh = core.schedule_gateway_refresh().unwrap();
    for unavailable in [true, false] {
        if unavailable {
            assert!(core.start_gateway_refresh(refresh));
        } else {
            assert!(core.finish_gateway_refresh(refresh));
        }
        let before = *notifications.borrow();
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        cx.run_until_parked();
        assert_eq!(
            sidebar.read_with(cx, |sidebar, _| sidebar.workspace_unavailable()),
            unavailable
        );
        assert_eq!(*notifications.borrow(), before + 1);
        assert_eq!(*tree_notifications.borrow(), 0);
        assert_eq!(
            tree.read_with(cx, |tree, _| tree.selected_item().unwrap().id.clone()),
            selected
        );
        assert!(sidebar.read_with(cx, |sidebar, _| Arc::ptr_eq(
            &input,
            sidebar.input.as_ref().unwrap()
        )));
        assert_eq!(core.authorization_connection_generation(), owner);
        assert_eq!(core.navigation_snapshot(), navigation);
        assert_eq!(core.workspace_catalog(), catalog);
        assert_eq!(
            core.authorization_snapshot(Some("w"), Some("a")),
            capabilities
        );
        sidebar.update(cx, |sidebar, cx| sidebar.sync(cx));
        cx.run_until_parked();
        assert_eq!(
            *notifications.borrow(),
            before + 1,
            "unchanged availability must not notify again"
        );
        assert_eq!(*tree_notifications.borrow(), 0);
    }
}
