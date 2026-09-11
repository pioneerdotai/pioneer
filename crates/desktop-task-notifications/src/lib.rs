//! Retained task inbox presentation over Client publications.
use gpui_kit::component::{
    Icon, IconName, Sizable, StyledExt, button::*, h_flex, popover::Popover, theme::ActiveTheme,
    v_flex,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::{ClientCore, ClientIntent, ClientPublicationReference, ClientScope},
    tasks::notifications::{
        TaskInboxPublication, TaskNotificationCompletion, TaskNotificationIntent,
    },
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, collections::HashMap, sync::Arc};

pub struct TaskNotificationLabels {
    title: String,
    task: String,
    loading: String,
    empty: String,
    completed: String,
    mark_read: String,
}
impl TaskNotificationLabels {
    pub fn new(
        title: String,
        task: String,
        loading: String,
        empty: String,
        completed: String,
        mark_read: String,
    ) -> Self {
        Self {
            title,
            task,
            loading,
            empty,
            completed,
            mark_read,
        }
    }
}
pub use pioneer_client::tasks::notifications::TaskNotificationEffect as TaskNotificationIdentity;
/// An optional OS surface. The current in-app inbox does not imply OS delivery.
pub trait DesktopNotificationPort {
    fn present(
        &self,
        identity: &TaskNotificationIdentity,
        summary: &str,
        completion: Arc<dyn Fn(TaskNotificationCompletion) + Send + Sync>,
    );
    fn remove(&self, identity: &TaskNotificationIdentity);
}
pub struct TaskNotificationConfig {
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    labels: TaskNotificationLabels,
    native: Option<Arc<dyn DesktopNotificationPort>>,
}
impl TaskNotificationConfig {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        labels: TaskNotificationLabels,
    ) -> Self {
        Self {
            client,
            registrar,
            labels,
            native: None,
        }
    }
    pub fn notification_port(mut self, port: Arc<dyn DesktopNotificationPort>) -> Self {
        self.native = Some(port);
        self
    }
}
struct Binding {
    publications: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl Default for Binding {
    fn default() -> Self {
        Self {
            publications: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        }
    }
}
impl ClientPublicationSink for Binding {
    fn publish(&self, publication: ClientPublicationReference) {
        let mut current = self.publications.borrow_mut();
        if current
            .get(publication.scope())
            .is_some_and(|p| p.revisions().scoped() >= publication.revisions().scoped())
        {
            return;
        }
        current.insert(publication.scope().clone(), publication);
        self.changed.send_modify(|serial| *serial += 1);
    }
}
actions!(task_notifications, [RetryTask]);
#[derive(Clone, Debug, PartialEq, gpui_kit::Action, serde::Deserialize)]
#[action(no_json)]
pub struct DismissTask {
    pub notification_id: String,
    pub revision: u64,
}
#[derive(Clone, Debug, PartialEq, gpui_kit::Action, serde::Deserialize)]
#[action(no_json)]
pub struct OpenTask {
    pub notification_id: String,
    pub revision: u64,
}
pub enum TaskNotificationEvent {
    OpenThread { thread_id: String },
}
struct NativeDelivery {
    identity: TaskNotificationIdentity,
    active: Arc<std::sync::atomic::AtomicBool>,
}
impl Drop for NativeDelivery {
    fn drop(&mut self) {
        self.active
            .store(false, std::sync::atomic::Ordering::Release);
    }
}
pub struct TaskNotificationView {
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    binding: Arc<Binding>,
    registrations: Vec<ClientBindingRegistration>,
    inbox_registration: Option<ClientBindingRegistration>,
    workspace: Option<String>,
    input: Option<Arc<TaskInboxPublication>>,
    labels: Arc<TaskNotificationLabels>,
    native: Option<Arc<dyn DesktopNotificationPort>>,
    delivered: HashMap<String, NativeDelivery>,
    changes: Option<Task<()>>,
    focus: FocusHandle,
    permissions_epoch: (Option<(u64, u64)>, Option<u64>),
    opened_operation: u64,
}
impl EventEmitter<TaskNotificationEvent> for TaskNotificationView {}
impl TaskNotificationView {
    pub fn new(config: TaskNotificationConfig, cx: &mut Context<Self>) -> Self {
        let binding = Arc::new(Binding::default());
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        let registrations = [
            ClientScope::Navigation,
            ClientScope::Administration { workspace_id: None },
        ]
        .into_iter()
        .map(|scope| config.registrar.register(scope, Arc::downgrade(&sink)))
        .collect();
        let mut changes = binding.changed.subscribe();
        let task = cx.spawn(async move |view, cx| {
            while changes.changed().await.is_ok() {
                if view.update(cx, |view, cx| view.sync(cx)).is_err() {
                    break;
                }
            }
        });
        let mut view = Self {
            client: config.client,
            registrar: config.registrar,
            binding,
            registrations,
            inbox_registration: None,
            workspace: None,
            input: None,
            labels: Arc::new(config.labels),
            native: config.native,
            delivered: HashMap::new(),
            changes: Some(task),
            focus: cx.focus_handle(),
            permissions_epoch: (None, None),
            opened_operation: 0,
        };
        view.sync(cx);
        view
    }
    fn sync(&mut self, cx: &mut Context<Self>) {
        let workspace = self
            .client
            .navigation_snapshot()
            .workspace_id()
            .map(str::to_owned);
        let workspace_changed = workspace != self.workspace;
        if workspace_changed {
            self.clear_native();
            self.inbox_registration.take();
            self.input = None;
            self.binding
                .publications
                .borrow_mut()
                .retain(|scope, _| !matches!(scope, ClientScope::TaskInbox { .. }));
            self.workspace = workspace;
            if let Some(workspace) = &self.workspace {
                let sink: Arc<dyn ClientPublicationSink> = self.binding.clone();
                self.inbox_registration = Some(self.registrar.register(
                    ClientScope::TaskInbox {
                        workspace_id: workspace.clone(),
                    },
                    Arc::downgrade(&sink),
                ));
            }
            cx.notify();
        }
        let permissions_epoch = (
            self.client.authorization_permissions_epoch(),
            self.client.gateway_session().startup.connection_id,
        );
        if workspace_changed || permissions_epoch != self.permissions_epoch {
            self.permissions_epoch = permissions_epoch;
            self.refresh();
        }
        let next = self
            .workspace
            .as_deref()
            .and_then(|w| self.client.task_inbox(w));
        if self.input.as_ref().map(|p| p.revision()) == next.as_ref().map(|p| p.revision()) {
            return;
        }
        self.input = next;
        if let Some(opened) = self.input.as_ref().and_then(|inbox| inbox.opened()) {
            if opened.operation_id() > self.opened_operation {
                self.opened_operation = opened.operation_id();
                cx.emit(TaskNotificationEvent::OpenThread {
                    thread_id: opened.thread_id().to_owned(),
                });
            }
        }
        self.sync_native();
        cx.notify();
    }
    pub fn refresh(&self) {
        if let Some(workspace) = &self.workspace {
            self.client.dispatch(ClientIntent::TaskNotification {
                intent: TaskNotificationIntent::Refresh {
                    workspace_id: workspace.clone(),
                },
            });
        }
    }
    fn dismiss(&self, id: String, revision: u64) {
        if let Some(workspace) = &self.workspace {
            self.client.dispatch(ClientIntent::TaskNotification {
                intent: TaskNotificationIntent::Dismiss {
                    workspace_id: workspace.clone(),
                    notification_id: id,
                    revision,
                },
            });
        }
    }
    fn sync_native(&mut self) {
        let Some(port) = &self.native else {
            return;
        };
        let mut live = HashMap::new();
        if let Some(input) = &self.input {
            for item in input.items().iter().filter(|i| i.is_actionable()) {
                let notification = item.notification();
                if live.contains_key(&notification.task_id) {
                    continue;
                }
                let identity = item.native_effect().expect("actionable notification");
                if self
                    .delivered
                    .get(identity.task_id())
                    .is_some_and(|delivery| delivery.identity == identity)
                {
                    live.insert(
                        identity.task_id().to_owned(),
                        self.delivered
                            .remove(identity.task_id())
                            .expect("live delivery"),
                    );
                    continue;
                }
                {
                    if let Some(previous) = self.delivered.remove(identity.task_id()) {
                        previous
                            .active
                            .store(false, std::sync::atomic::Ordering::Release);
                        port.remove(&previous.identity);
                    }
                    let summary = notification
                        .result
                        .as_ref()
                        .and_then(|r| r.summary.clone())
                        .or_else(|| notification.error.as_ref().map(|e| e.error.message.clone()))
                        .unwrap_or_else(|| self.labels.completed.clone());
                    let client = Arc::downgrade(&self.client);
                    let effect = identity.clone();
                    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
                    let completion_active = active.clone();
                    port.present(
                        &identity,
                        &summary,
                        Arc::new(move |completion| {
                            if !completion_active.swap(false, std::sync::atomic::Ordering::AcqRel) {
                                return;
                            }
                            if let Some(client) = client.upgrade() {
                                client.dispatch(ClientIntent::TaskNotification {
                                    intent: TaskNotificationIntent::NativeCompletion {
                                        workspace_id: effect.workspace_id().to_owned(),
                                        effect: effect.clone(),
                                        completion,
                                    },
                                });
                            }
                        }),
                    );
                    live.insert(
                        identity.task_id().to_owned(),
                        NativeDelivery { identity, active },
                    );
                }
            }
        }
        for (id, identity) in &self.delivered {
            if !live.contains_key(id) {
                identity
                    .active
                    .store(false, std::sync::atomic::Ordering::Release);
                port.remove(&identity.identity);
            }
        }
        self.delivered = live;
    }
    fn clear_native(&mut self) {
        if let Some(port) = &self.native {
            for identity in self.delivered.values() {
                identity
                    .active
                    .store(false, std::sync::atomic::Ordering::Release);
                port.remove(&identity.identity);
            }
        }
        self.delivered.clear();
    }
    pub fn close(&mut self) {
        self.changes.take();
        self.inbox_registration.take();
        self.registrations.clear();
        self.clear_native();
        self.input = None;
    }
}
impl Drop for TaskNotificationView {
    fn drop(&mut self) {
        self.close();
    }
}
impl Render for TaskNotificationView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let unread = self.input.as_ref().map_or(0, |p| {
            p.items().iter().filter(|i| i.is_actionable()).count()
        });
        let notifications = self
            .input
            .as_ref()
            .map(|p| p.items().iter().take(20).cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let loading = self.input.as_ref().is_some_and(|p| p.is_loading());
        let error = self
            .input
            .as_ref()
            .and_then(|p| p.error().map(str::to_owned));
        let labels = self.labels.clone();
        let view = cx.weak_entity();
        div()
            .track_focus(&self.focus)
            .key_context("TaskNotifications")
            .on_action(cx.listener(|view, _: &RetryTask, _, _| view.refresh()))
            .on_action(cx.listener(|view, action: &DismissTask, _, _| {
                view.dismiss(action.notification_id.clone(), action.revision)
            }))
            .on_action(cx.listener(|view, action: &OpenTask, _, _| {
                if let Some(workspace_id) = &view.workspace {
                    view.client.dispatch(ClientIntent::TaskNotification {
                        intent: TaskNotificationIntent::Open {
                            workspace_id: workspace_id.clone(),
                            notification_id: action.notification_id.clone(),
                            revision: action.revision,
                        },
                    });
                }
            }))
            .child(
                Popover::new("task-user-notifications-popover")
                    .anchor(Anchor::TopRight)
                    .trigger(
                        Button::new("task-user-notifications-trigger")
                            .ghost()
                            .small()
                            .compact()
                            .tooltip(labels.title.clone())
                            .child(
                                h_flex()
                                    .gap_1()
                                    .child(
                                        Icon::new(IconName::Bell)
                                            .size_3p5()
                                            .opacity(if unread > 0 { 1.0 } else { 0.6 }),
                                    )
                                    .when(unread > 0, |this| {
                                        this.child(
                                            div()
                                                .text_xs()
                                                .text_color(cx.theme().blue)
                                                .child(unread.to_string()),
                                        )
                                    }),
                            ),
                    )
                    .content(move |_, _, cx| {
                        let labels = labels.clone();
                        let view = view.clone();
                        v_flex()
                            .w(px(380.))
                            .max_h(px(460.))
                            .gap_2()
                            .p_2()
                            .when(loading, |this| {
                                this.child(
                                    div().text_xs().opacity(0.6).child(labels.loading.clone()),
                                )
                            })
                            .when_some(error.clone(), |this, error| {
                                this.child(
                                    div().text_xs().text_color(cx.theme().danger).child(error),
                                )
                            })
                            .when(notifications.is_empty() && !loading, |this| {
                                this.child(div().text_xs().opacity(0.6).child(labels.empty.clone()))
                            })
                            .children(notifications.iter().map(|item| {
                                let notification = item.notification();
                                let id = notification.notification_id.clone();
                                let revision = item.revision();
                                let view = view.clone();
                                let summary = notification
                                    .result
                                    .as_ref()
                                    .map(|r| {
                                        r.summary
                                            .clone()
                                            .unwrap_or_else(|| labels.completed.clone())
                                    })
                                    .unwrap_or_else(|| {
                                        notification
                                            .error
                                            .as_ref()
                                            .map(|e| e.error.message.clone())
                                            .unwrap_or_else(|| labels.completed.clone())
                                    });
                                v_flex()
                                    .w_full()
                                    .gap_1()
                                    .p_2()
                                    .rounded_md()
                                    .border_1()
                                    .border_color(cx.theme().border)
                                    .child(div().text_xs().font_semibold().child(format!(
                                        "{} · {}",
                                        labels.task, notification.task_id
                                    )))
                                    .child(
                                        div()
                                            .text_xs()
                                            .line_height(relative(1.3))
                                            .whitespace_normal()
                                            .child(summary),
                                    )
                                    .when(item.is_actionable(), |this| {
                                        this.child(
                                            Button::new(format!("ack-task-notification-{id}"))
                                                .ghost()
                                                .xsmall()
                                                .label(labels.mark_read.clone())
                                                .on_click(move |_, _, cx| {
                                                    let _ = view.update(cx, |view, _| {
                                                        view.dismiss(id.clone(), revision)
                                                    });
                                                }),
                                        )
                                    })
                            }))
                    }),
            )
    }
}

#[cfg(test)]
mod tests;
