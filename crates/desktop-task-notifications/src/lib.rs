//! Retained task inbox presentation over Client publications.
use gpui_kit::component::{
    Disableable, Icon, IconName, Sizable, StyledExt, VirtualListScrollHandle, button::*, h_flex,
    popover::Popover, scroll::ScrollableElement, separator::Separator, spinner::Spinner,
    theme::ActiveTheme, v_flex, v_virtual_list,
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
    open: String,
    mark_all_read: String,
    read: String,
    unread: String,
    retry: String,
    failed: String,
    blocked: String,
    cancelled: String,
    timed_out: String,
    loading_failed: String,
    format_time: fn(i64) -> String,
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
            open: task.clone(),
            mark_all_read: mark_read.clone(),
            read: mark_read.clone(),
            unread: title.clone(),
            retry: loading.clone(),
            failed: completed.clone(),
            blocked: completed.clone(),
            cancelled: completed.clone(),
            timed_out: completed.clone(),
            loading_failed: empty.clone(),
            format_time: |at| at.to_string(),
            mark_read,
            title,
            task,
            loading,
            empty,
            completed,
        }
    }
    pub fn history(
        mut self,
        open: String,
        mark_all_read: String,
        read: String,
        unread: String,
        retry: String,
        loading_failed: String,
    ) -> Self {
        self.open = open;
        self.mark_all_read = mark_all_read;
        self.read = read;
        self.unread = unread;
        self.retry = retry;
        self.loading_failed = loading_failed;
        self
    }
    pub fn terminal_statuses(
        mut self,
        failed: String,
        blocked: String,
        cancelled: String,
        timed_out: String,
    ) -> Self {
        self.failed = failed;
        self.blocked = blocked;
        self.cancelled = cancelled;
        self.timed_out = timed_out;
        self
    }
    pub fn time_formatter(mut self, formatter: fn(i64) -> String) -> Self {
        self.format_time = formatter;
        self
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
actions!(
    task_notifications,
    [RetryTask, NextResult, PreviousResult, OpenSelectedResult]
);

pub fn init(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("down", NextResult, Some("TaskCompletionInbox")),
        KeyBinding::new("up", PreviousResult, Some("TaskCompletionInbox")),
        KeyBinding::new("enter", OpenSelectedResult, Some("TaskCompletionInbox")),
    ]);
}
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
    scroll: VirtualListScrollHandle,
    inbox_open: bool,
    inbox_focus: FocusHandle,
    selected_run: Option<String>,
    lazy: LazyInboxDemand,
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
            scroll: VirtualListScrollHandle::new(),
            inbox_open: false,
            inbox_focus: cx.focus_handle(),
            selected_run: None,
            lazy: LazyInboxDemand::default(),
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
            self.scroll = VirtualListScrollHandle::new();
            self.lazy = LazyInboxDemand::default();
            self.inbox_open = false;
            self.selected_run = None;
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
            self.clear_native();
            self.input = None;
            self.binding
                .publications
                .borrow_mut()
                .retain(|scope, _| !matches!(scope, ClientScope::TaskInbox { .. }));
            self.permissions_epoch = permissions_epoch;
            self.lazy = LazyInboxDemand::default();
            self.refresh();
            cx.notify();
        }
        let next = self
            .workspace
            .as_deref()
            .and_then(|w| self.client.task_inbox(w));
        if self.input.as_ref().map(|p| p.revision()) == next.as_ref().map(|p| p.revision()) {
            return;
        }
        self.lazy.observe(
            next.as_ref().and_then(|input| input.next_cursor()),
            next.as_ref().is_some_and(|input| input.error().is_some()),
        );
        self.input = next;
        if let Some(opened) = self.input.as_ref().and_then(|inbox| inbox.opened()) {
            if opened.operation_id() > self.opened_operation {
                self.opened_operation = opened.operation_id();
                self.inbox_open = false;
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
                        .summary
                        .clone()
                        .or_else(|| notification.error.clone())
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
/// A changed cursor/range creates one deferred demand. Failed cursors require
/// an explicit Retry; repeated measurement never issues another request.
#[derive(Default)]
struct LazyInboxDemand {
    requested_cursor: Option<String>,
    last_range: Option<(usize, usize)>,
    fill_pages: usize,
    failed: bool,
}
impl LazyInboxDemand {
    fn observe(&mut self, cursor: Option<&str>, failed: bool) {
        if self.requested_cursor.as_deref() != cursor {
            self.requested_cursor = None;
            self.failed = false;
        }
        if self.requested_cursor.is_some() && failed {
            // Unrelated successful refreshes must not hide a page's Retry or
            // silently retry its failed cursor on every new publication.
            self.failed = true;
        }
    }
    fn retry(&mut self) {
        self.requested_cursor = None;
        self.failed = false;
        self.fill_pages = 0;
    }
    fn demand(
        &mut self,
        end: usize,
        count: usize,
        cursor: Option<&str>,
        loading: bool,
        failed: bool,
    ) -> Option<String> {
        if self
            .last_range
            .is_some_and(|(old_end, old_count)| old_count == count && old_end != end)
        {
            self.fill_pages = 0;
        }
        self.last_range = Some((end, count));
        let cursor = cursor?;
        if loading
            || failed
            || self.failed
            || end.saturating_add(3) < count
            || self.requested_cursor.as_deref() == Some(cursor)
        {
            return None;
        }
        if end >= count {
            if self.fill_pages >= 3 {
                return None;
            }
            self.fill_pages += 1;
        }
        self.requested_cursor = Some(cursor.to_owned());
        Some(cursor.to_owned())
    }
}
impl TaskNotificationView {
    fn open(&self, id: String, revision: u64) {
        if let Some(workspace) = &self.workspace {
            self.client.dispatch(ClientIntent::TaskNotification {
                intent: TaskNotificationIntent::Open {
                    workspace_id: workspace.clone(),
                    notification_id: id,
                    revision,
                },
            });
        }
    }
    fn read_all(&self) {
        if let Some(workspace_id) = &self.workspace {
            self.client.dispatch(ClientIntent::TaskNotification {
                intent: TaskNotificationIntent::ReadAll {
                    workspace_id: workspace_id.clone(),
                },
            });
        }
    }
    fn retry(&mut self) {
        self.lazy.retry();
        if let Some(workspace_id) = &self.workspace {
            self.client.dispatch(ClientIntent::TaskNotification {
                intent: TaskNotificationIntent::Retry {
                    workspace_id: workspace_id.clone(),
                },
            });
        }
    }
    fn viewport_demand(&mut self, end: usize, cx: &mut Context<Self>) {
        let Some(input) = &self.input else {
            return;
        };
        let Some(cursor) = self.lazy.demand(
            end,
            input.items().len(),
            input.next_cursor(),
            input.is_loading(),
            input.error().is_some(),
        ) else {
            return;
        };
        let workspace = self.workspace.clone();
        let epoch = self.permissions_epoch;
        // VirtualList reports the resolved range for wheels, thumb dragging,
        // keyboard scrolling and resize. Domain work runs after layout.
        let underfilled = end >= input.items().len();
        let owner = cx.weak_entity();
        cx.defer(move |cx| {
            let _ = owner.update(cx, |view, _| {
                if view.workspace != workspace || view.permissions_epoch != epoch {
                    return;
                }
                if view.inbox_open
                    && view.input.as_ref().is_some_and(|input| {
                        !input.is_loading()
                            && input.error().is_none()
                            && input.next_cursor() == Some(cursor.as_str())
                    })
                {
                    view.load_more();
                } else if view.lazy.requested_cursor.as_deref() == Some(cursor.as_str()) {
                    view.lazy.requested_cursor = None;
                    view.lazy.fill_pages = view
                        .lazy
                        .fill_pages
                        .saturating_sub(usize::from(underfilled));
                }
            });
        });
    }

    fn load_more(&self) {
        if let Some(workspace) = &self.workspace {
            self.client.dispatch(ClientIntent::TaskNotification {
                intent: TaskNotificationIntent::LoadMore {
                    workspace_id: workspace.clone(),
                },
            });
        }
    }
    fn move_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
        let Some(input) = &self.input else { return };
        let items = input.items();
        if items.is_empty() {
            self.selected_run = None;
            return;
        }
        let ix = self.selected_run.as_deref().and_then(|id| {
            items
                .iter()
                .position(|item| item.notification().run_id == id)
        });
        let ix = ix.map_or(if delta < 0 { items.len() - 1 } else { 0 }, |ix| {
            ix.saturating_add_signed(delta).min(items.len() - 1)
        });
        self.selected_run = Some(items[ix].notification().run_id.clone());
        self.scroll.scroll_to_item(ix, ScrollStrategy::Top);
        cx.notify();
    }
    fn render_row(
        &self,
        item: &pioneer_client::tasks::notifications::TaskInboxItem,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let entry = item.notification();
        let status = match entry.status {
            pioneer_client::tasks::notifications::TaskRunStatus::Succeeded => {
                &self.labels.completed
            }
            pioneer_client::tasks::notifications::TaskRunStatus::Failed => &self.labels.failed,
            pioneer_client::tasks::notifications::TaskRunStatus::Blocked => &self.labels.blocked,
            pioneer_client::tasks::notifications::TaskRunStatus::Cancelled => {
                &self.labels.cancelled
            }
            pioneer_client::tasks::notifications::TaskRunStatus::TimedOut => &self.labels.timed_out,
            _ => &self.labels.completed,
        };
        let summary = entry
            .error
            .clone()
            .or_else(|| entry.summary.clone())
            .unwrap_or_default();
        let open_id = entry.run_id.clone();
        let read_id = open_id.clone();
        let revision = item.revision();
        let busy = item.is_dismissing();
        v_flex()
            .id(SharedString::from(format!(
                "task-inbox-row-{}",
                entry.run_id
            )))
            .w_full()
            .h(rems(8.))
            .p_2()
            .pr_4()
            .gap_1()
            .border_b_1()
            .border_color(cx.theme().border)
            .when(
                self.selected_run.as_deref() == Some(entry.run_id.as_str()),
                |this| this.bg(cx.theme().muted),
            )
            .child(
                div()
                    .text_sm()
                    .font_semibold()
                    .truncate()
                    .child(if entry.title.is_empty() {
                        self.labels.task.clone()
                    } else {
                        entry.title.clone()
                    }),
            )
            .child(
                h_flex()
                    .w_full()
                    .gap_2()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(div().flex_1().min_w_0().truncate().child(
                        if entry.thread_title.trim().is_empty() {
                            entry.thread_id.clone()
                        } else {
                            entry.thread_title.clone()
                        },
                    ))
                    .child((self.labels.format_time)(entry.completed_at)),
            )
            .child(
                h_flex()
                    .w_full()
                    .gap_2()
                    .text_xs()
                    .child(status.clone())
                    .child(div().text_color(cx.theme().muted_foreground).child(
                        if item.is_actionable() {
                            self.labels.unread.clone()
                        } else {
                            self.labels.read.clone()
                        },
                    )),
            )
            .child(div().text_xs().truncate().child(summary))
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new(format!("open-task-inbox-{open_id}"))
                            .ghost()
                            .small()
                            .label(self.labels.open.clone())
                            .disabled(busy)
                            .on_click(cx.listener(move |view, _, _, _| {
                                view.open(open_id.clone(), revision)
                            })),
                    )
                    .when(item.is_actionable(), |this| {
                        this.child(
                            Button::new(format!("read-task-inbox-{read_id}"))
                                .ghost()
                                .small()
                                .label(self.labels.mark_read.clone())
                                .disabled(busy)
                                .on_click(cx.listener(move |view, _, _, _| {
                                    view.dismiss(read_id.clone(), revision)
                                })),
                        )
                    }),
            )
            .into_any_element()
    }
}
impl Render for TaskNotificationView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let unread = self.input.as_ref().map_or(0, |p| p.unread_count());
        let labels = self.labels.clone();
        let owner = cx.weak_entity();
        let open_owner = owner.clone();
        div()
            .track_focus(&self.focus)
            .key_context("TaskNotifications")
            .on_action(cx.listener(|view, _: &RetryTask, _, _| view.retry()))
            .on_action(cx.listener(|view, action: &DismissTask, _, _| {
                view.dismiss(action.notification_id.clone(), action.revision)
            }))
            .on_action(cx.listener(|view, action: &OpenTask, _, _| {
                view.open(action.notification_id.clone(), action.revision)
            }))
            .child(
                Popover::new("task-user-notifications-popover")
                    .p_0()
                    .anchor(Anchor::TopRight)
                    .open(self.inbox_open)
                    .track_focus(&self.inbox_focus)
                    .on_open_change(move |open, _, cx| {
                        let _ = open_owner.update(cx, |view, cx| {
                            view.inbox_open = *open;
                            cx.notify();
                        });
                    })
                    .trigger(
                        Button::new("task-user-notifications-trigger")
                            .ghost()
                            .small()
                            .compact()
                            .tooltip(labels.title.clone())
                            .accessibility_label(labels.title.clone())
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
                                                .text_color(cx.theme().primary)
                                                .child(unread.to_string()),
                                        )
                                    }),
                            ),
                    )
                    .content(move |_, window, cx| {
                        let Some(owner) = owner.upgrade() else {
                            return div().into_any_element();
                        };
                        let view = owner.read(cx);
                        let input = view.input.clone();
                        let scroll = view.scroll.clone();
                        let focus = view.inbox_focus.clone();
                        let labels = labels.clone();
                        let loading = input.as_ref().is_some_and(|p| p.is_loading());
                        let failed =
                            view.lazy.failed || input.as_ref().is_some_and(|p| p.error().is_some());
                        let count = input.as_ref().map_or(0, |p| p.items().len());
                        // Fixed row geometry follows interface zoom. Only visible
                        // rows create elements; run IDs retain each control's identity.
                        let sizes =
                            std::rc::Rc::new(vec![
                                size(Pixels::ZERO, window.rem_size() * 8.);
                                count
                            ]);
                        let retry_owner = owner.downgrade();
                        let all_owner = owner.downgrade();
                        v_flex()
                            .track_focus(&focus)
                            .key_context("TaskCompletionInbox")
                            .on_action(window.listener_for(
                                &owner,
                                |view, _: &NextResult, window, cx| {
                                    view.inbox_focus.focus(window, cx);
                                    view.move_selection(1, cx);
                                },
                            ))
                            .on_action(window.listener_for(
                                &owner,
                                |view, _: &PreviousResult, window, cx| {
                                    view.inbox_focus.focus(window, cx);
                                    view.move_selection(-1, cx);
                                },
                            ))
                            .on_action(window.listener_for(
                                &owner,
                                |view, _: &OpenSelectedResult, window, cx| {
                                    // Preserve Enter activation on focused row buttons.
                                    if !view.inbox_focus.is_focused(window) {
                                        cx.propagate();
                                        return;
                                    }
                                    let selected = view
                                        .input
                                        .as_ref()
                                        .and_then(|input| {
                                            input.items().iter().find(|item| {
                                                Some(item.notification().run_id.as_str())
                                                    == view.selected_run.as_deref()
                                            })
                                        })
                                        .cloned();
                                    if let Some(item) = selected {
                                        view.open(
                                            item.notification().run_id.clone(),
                                            item.revision(),
                                        );
                                    }
                                },
                            ))
                            .w_96()
                            .debug_selector(|| "task-inbox-popup".into())
                            .gap_2()
                            .child(
                                h_flex()
                                    .w_full()
                                    .h_8()
                                    .p_2()
                                    .pb_0()
                                    .gap_2()
                                    .debug_selector(|| "task-inbox-header".into())
                                    .text_sm()
                                    .font_semibold()
                                    .child(
                                        div()
                                            .flex_1()
                                            .min_w_0()
                                            .truncate()
                                            .child(labels.title.clone()),
                                    )
                                    .when(loading, |this| {
                                        this.child(
                                            h_flex()
                                                .gap_1()
                                                .text_xs()
                                                .font_normal()
                                                .text_color(cx.theme().muted_foreground)
                                                .child(Spinner::new().small())
                                                .child(labels.loading.clone()),
                                        )
                                    }),
                            )
                            .when(failed, |this| {
                                this.child(
                                    v_flex()
                                        .px_2()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(cx.theme().danger)
                                                .child(labels.loading_failed.clone()),
                                        )
                                        .child(
                                            Button::new("retry-task-inbox")
                                                .ghost()
                                                .small()
                                                .label(labels.retry.clone())
                                                .on_click(move |_, _, cx| {
                                                    let _ = retry_owner
                                                        .update(cx, |view, _| view.retry());
                                                }),
                                        ),
                                )
                            })
                            .when(count == 0 && !loading && !failed, |this| {
                                this.child(
                                    div()
                                        .p_2()
                                        .text_sm()
                                        .text_color(cx.theme().muted_foreground)
                                        .child(labels.empty.clone()),
                                )
                            })
                            .when(count > 0, |this| {
                                this.child(
                                    div()
                                        .h_80()
                                        .debug_selector(|| "task-inbox-viewport".into())
                                        .w_full()
                                        .relative()
                                        .child(
                                            v_virtual_list(
                                                owner.clone(),
                                                "task-inbox-list",
                                                sizes,
                                                |view, range, _, cx| {
                                                    view.viewport_demand(range.end, cx);
                                                    let Some(input) = view.input.clone() else {
                                                        return Vec::new();
                                                    };
                                                    range
                                                        .filter_map(|ix| {
                                                            input.items().get(ix).map(|item| {
                                                                view.render_row(item, cx)
                                                            })
                                                        })
                                                        .collect::<Vec<_>>()
                                                },
                                            )
                                            .track_scroll(&scroll),
                                        )
                                        .vertical_scrollbar(&scroll),
                                )
                            })
                            .child(Separator::horizontal())
                            .child(
                                h_flex()
                                    .debug_selector(|| "task-inbox-footer".into())
                                    .p_2()
                                    .pt_0()
                                    .justify_start()
                                    .child(
                                        Button::new("read-all-task-inbox")
                                            .debug_selector(|| "task-inbox-read-all".into())
                                            .ghost()
                                            .xsmall()
                                            .compact()
                                            .disabled(
                                                loading
                                                    || input.as_ref().is_none_or(|input| {
                                                        input.unread_count() == 0
                                                    }),
                                            )
                                            .child(
                                                div()
                                                    .opacity(0.6)
                                                    .child(labels.mark_all_read.clone()),
                                            )
                                            .on_click(move |_, _, cx| {
                                                let _ =
                                                    all_owner.update(cx, |view, _| view.read_all());
                                            }),
                                    ),
                            )
                            .into_any_element()
                    }),
            )
    }
}

#[cfg(test)]
mod tests;
