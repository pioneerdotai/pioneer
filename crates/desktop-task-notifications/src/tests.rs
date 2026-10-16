use super::{
    DesktopNotificationPort, TaskNotificationConfig, TaskNotificationIdentity,
    TaskNotificationLabels, TaskNotificationView,
};
use gpui_kit::{AppContext, TestAppContext};
use pioneer_client::{
    core::ClientCore,
    tasks::notifications::{TaskInboxPublication, TaskNotificationCompletion},
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, sync::Arc};
struct Registrar;
impl ClientBindingRegistrar for Registrar {
    fn register(
        &self,
        _: pioneer_client::core::ClientScope,
        _: std::sync::Weak<dyn ClientPublicationSink>,
    ) -> ClientBindingRegistration {
        ClientBindingRegistration::new(|| {})
    }
}
#[derive(Default)]
struct FakePort {
    presented: RefCell<Vec<TaskNotificationIdentity>>,
    removed: RefCell<Vec<TaskNotificationIdentity>>,
}
impl DesktopNotificationPort for FakePort {
    fn present(
        &self,
        identity: &TaskNotificationIdentity,
        _: &str,
        _: Arc<dyn Fn(TaskNotificationCompletion) + Send + Sync>,
    ) {
        self.presented.borrow_mut().push(identity.clone());
    }
    fn remove(&self, identity: &TaskNotificationIdentity) {
        self.removed.borrow_mut().push(identity.clone());
    }
}
fn input(revision: u64, actionable: bool) -> Arc<TaskInboxPublication> {
    Arc::new(serde_json::from_value(serde_json::json!({
        "workspace_id":"a", "revision":revision, "next_cursor":null,"unread_count":if actionable {1}else{0}, "loading":false, "error":null, "opened":null, "native_notifications":[],
        "items":[{"revision":revision,"dismissing":false,"notification":{"workspaceId":"a","taskId":"t","runId":"r","title":"Task","threadId":"root","threadTitle":"Root","status":"succeeded","completedAt":1,"readAt":if actionable { None } else { Some(2) }}}]
    })).unwrap())
}
#[gpui_kit::test]
fn fake_native_resources_dedupe_replace_clear_and_teardown(cx: &mut TestAppContext) {
    cx.update(gpui_kit::init);
    let port = Arc::new(FakePort::default());
    let config = TaskNotificationConfig::new(
        Arc::new(ClientCore::new()),
        Arc::new(Registrar),
        TaskNotificationLabels::new(
            "title".into(),
            "task".into(),
            "loading".into(),
            "empty".into(),
            "completed".into(),
            "read".into(),
        ),
    )
    .notification_port(port.clone());
    let view = cx.new(|cx| TaskNotificationView::new(config, cx));
    view.update(cx, |view, _| {
        view.input = Some(input(1, true));
        view.sync_native();
        view.sync_native();
        assert_eq!(port.presented.borrow().len(), 1);
        view.input = Some(input(2, true));
        view.sync_native();
        assert_eq!(port.presented.borrow().len(), 2);
        assert_eq!(port.removed.borrow().len(), 1);
        view.input = Some(input(3, false));
        view.sync_native();
        assert!(view.delivered.is_empty());
        assert_eq!(port.removed.borrow().len(), 2);
        view.input = Some(input(4, true));
        view.sync_native();
        let active = view.delivered["t"].active.clone();
        view.close();
        assert!(!active.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(port.removed.borrow().len(), 3);
        assert!(view.delivered.is_empty());
    });
}

#[gpui_kit::test]
fn workspace_change_discards_old_result_cards_and_read_count(cx: &mut TestAppContext) {
    cx.update(gpui_kit::init);
    let client = Arc::new(ClientCore::new());
    let view = cx.new(|cx| {
        TaskNotificationView::new(
            TaskNotificationConfig::new(
                client.clone(),
                Arc::new(Registrar),
                TaskNotificationLabels::new(
                    "title".into(),
                    "task".into(),
                    "loading".into(),
                    "empty".into(),
                    "completed".into(),
                    "read".into(),
                ),
            ),
            cx,
        )
    });
    view.update(cx, |view, _| {
        view.workspace = Some("a".into());
        view.input = Some(input(1, true));
    });
    client.open_workspace_thread("b".into(), None, None);
    view.update(cx, |view, cx| {
        view.sync(cx);
        assert_eq!(view.workspace.as_deref(), Some("b"));
        assert!(view.input.is_none());
    });
}

#[gpui_kit::test]
fn keyboard_navigation_reaches_virtualized_history_without_reading_it(cx: &mut TestAppContext) {
    use gpui_kit::component::Root;
    cx.update(gpui_kit::init);
    cx.update(super::init);
    let (root, cx) = cx.add_window_view(|window, cx| {
        let view = cx.new(|cx| {
            TaskNotificationView::new(
                TaskNotificationConfig::new(
                    Arc::new(ClientCore::new()),
                    Arc::new(Registrar),
                    TaskNotificationLabels::new(
                        "title".into(),
                        "task".into(),
                        "loading".into(),
                        "empty".into(),
                        "completed".into(),
                        "read".into(),
                    ),
                ),
                cx,
            )
        });
        Root::new(view, window, cx)
    });
    let view = root.read_with(cx, |root, _| {
        root.view()
            .clone()
            .downcast::<TaskNotificationView>()
            .unwrap()
    });
    view.update(cx, |view, cx| {
        let mut publication = serde_json::to_value(input(1, true).as_ref()).unwrap();
        let template = publication["items"][0].clone();
        publication["items"] = (0..32)
            .map(|ix| {
                let mut row = template.clone();
                row["notification"]["runId"] = format!("run-{ix:03}").into();
                row
            })
            .collect::<Vec<_>>()
            .into();
        publication["unread_count"] = 120.into();
        view.workspace = Some("a".into());
        view.input = Some(Arc::new(serde_json::from_value(publication).unwrap()));
        view.inbox_open = true;
        cx.notify();
    });
    cx.run_until_parked();
    cx.update(|window, cx| {
        let focus = view.read(cx).inbox_focus.clone();
        focus.focus(window, cx);
    });
    for _ in 0..32 {
        cx.simulate_keystrokes("down");
        cx.run_until_parked();
    }
    view.read_with(cx, |view, _| {
        assert_eq!(view.selected_run.as_deref(), Some("run-031"));
        assert!(view.scroll.base_handle().offset().y < gpui_kit::Pixels::ZERO);
        assert_eq!(view.input.as_ref().unwrap().unread_count(), 120);
        assert!(
            view.input
                .as_ref()
                .unwrap()
                .items()
                .iter()
                .all(|item| item.is_actionable())
        );
    });
    cx.simulate_keystrokes("escape");
    cx.run_until_parked();
    view.read_with(cx, |view, _| assert!(!view.inbox_open));
}
