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
        "workspace_id":"a", "revision":revision, "next_cursor":null, "loading":false, "error":null, "opened":null, "native_notifications":[],
        "items":[{"revision":revision,"dismissing":false,"notification":{"notificationId":"n","workspaceId":"a","taskId":"t","runId":"r","deliveryId":"d","createdAt":1,"acknowledgedAt":if actionable { None } else { Some(2) }}}]
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
