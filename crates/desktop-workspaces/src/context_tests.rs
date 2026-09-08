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
fn config(
    core: Arc<ClientCore>,
    registrations: Rc<RefCell<HashSet<ClientScope>>>,
) -> WorkspaceNavigationConfig {
    WorkspaceNavigationConfig::new(
        core,
        Arc::new(Registrar(registrations)),
        |_, _| HashMap::new(),
        |_, _, _| {},
        |_| false,
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
    let core = Arc::new(ClientCore::new());
    core.open_workspace_thread("w".into(), None, None);
    let saves = Rc::new(RefCell::new(Vec::new()));
    let saved = saves.clone();
    let mut config = config(core, Rc::new(RefCell::new(HashSet::new())));
    config.save_expansion = Rc::new(move |_, state, _| saved.borrow_mut().push(state));
    let folders = vec![
        serde_json::from_value(serde_json::json!({"id":"parent","workspace_id":"w","name":"Parent","created_at":1,"updated_at":1})).unwrap(),
        serde_json::from_value(serde_json::json!({"id":"child","workspace_id":"w","parent_folder_id":"parent","name":"Child","created_at":1,"updated_at":1})).unwrap(),
    ];
    let snapshot =
        thread_tree_snapshot_from_parts("w".into(), vec![], vec![], folders, vec![], vec![]);
    let publication = Arc::new(serde_json::from_value(serde_json::json!({
        "revision":1,"snapshot":snapshot,"changes":{"changed":[],"removed":[],"reordered_folders":[]},"loading":false,"error":null
    })).unwrap());
    let (root, cx) = cx.add_window_view(|window, cx| {
        let sidebar = cx.new(|cx| {
            let mut sidebar = super::sidebar::ThreadSidebarView::new(config, cx);
            sidebar.input = Some(publication);
            sidebar.rebuild_sidebar_tree_state(cx);
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
        sidebar.update(cx, |sidebar, cx| sidebar.rebuild_sidebar_tree_state(cx));
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
    assert_eq!(registrations.borrow().len(), 4);
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
            "LegacyScreenAdapter",
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
