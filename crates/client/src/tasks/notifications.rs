//! Personal completed-run inbox ownership, pagination and request fencing.
use crate::core::{ClientCore, ClientMutationAuthority, ClientScope, ClientTransition};
pub use pioneer_protocol::TaskRunStatus;
use pioneer_protocol::{TaskCompletionInboxEntry, TaskCompletionInboxListResponse};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    thread::JoinHandle,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
// `notification_id` in the retained intent/effect API is the canonical run ID.
// Legacy outbox IDs are used only by task/user_notification/* RPCs.
pub enum TaskNotificationIntent {
    Refresh {
        workspace_id: String,
    },
    LoadMore {
        workspace_id: String,
    },
    Dismiss {
        workspace_id: String,
        notification_id: String,
        revision: u64,
    },
    Retry {
        workspace_id: String,
    },
    Open {
        workspace_id: String,
        notification_id: String,
        revision: u64,
    },
    NativeCompletion {
        workspace_id: String,
        effect: TaskNotificationEffect,
        completion: TaskNotificationCompletion,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskNotificationCompletion {
    Activated,
    Dismissed,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskNotificationEffect {
    workspace_id: String,
    notification_id: String,
    task_id: String,
    revision: u64,
}
impl TaskNotificationEffect {
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn notification_id(&self) -> &str {
        &self.notification_id
    }
    pub fn task_id(&self) -> &str {
        &self.task_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskInboxItem {
    notification: TaskCompletionInboxEntry,
    revision: u64,
    dismissing: bool,
}
impl TaskInboxItem {
    pub fn notification(&self) -> &TaskCompletionInboxEntry {
        &self.notification
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn is_dismissing(&self) -> bool {
        self.dismissing
    }
    pub fn native_effect(&self) -> Option<TaskNotificationEffect> {
        self.is_actionable().then(|| TaskNotificationEffect {
            workspace_id: self.notification.workspace_id.clone(),
            notification_id: self.notification.run_id.clone(),
            task_id: self.notification.task_id.clone(),
            revision: self.revision,
        })
    }
    pub fn is_actionable(&self) -> bool {
        self.notification.read_at.is_none()
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskNotificationPublication {
    operation_id: u64,
    notification_id: String,
    task_id: String,
    task_revision: u64,
    thread_id: String,
}
impl TaskNotificationPublication {
    pub fn operation_id(&self) -> u64 {
        self.operation_id
    }
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskInboxPublication {
    workspace_id: String,
    revision: u64,
    items: Vec<TaskInboxItem>,
    next_cursor: Option<String>,
    unread_count: u64,
    opened: Option<TaskNotificationPublication>,
    native_notifications: Vec<TaskNotificationEffect>,
    loading: bool,
    error: Option<String>,
}
impl TaskInboxPublication {
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn items(&self) -> &[TaskInboxItem] {
        &self.items
    }
    pub fn is_loading(&self) -> bool {
        self.loading
    }
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
    pub fn native_notifications(&self) -> &[TaskNotificationEffect] {
        &self.native_notifications
    }
    pub fn opened(&self) -> Option<&TaskNotificationPublication> {
        self.opened.as_ref()
    }
    pub fn unread_count(&self) -> u64 {
        self.unread_count
    }
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct InboxIdentity {
    connection: u64,
    principal: String,
    authorization: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum RefreshRange {
    Through(String),
    End,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Request {
    workspace: String,
    generation: u64,
    identity: InboxIdentity,
    notification: Option<(String, u64)>,
    open: bool,
    navigation_revision: Option<u64>,
    cursor: Option<String>,
    refresh_range: Option<RefreshRange>,
}
#[derive(Default)]
struct Inbox {
    publication: TaskInboxPublication,
    request: Option<Request>,
    refresh_again: bool,
    pending_actions: std::collections::VecDeque<TaskNotificationIntent>,
}
#[derive(Default)]
pub struct TaskNotificationStore {
    inboxes: HashMap<String, Inbox>,
    generation: u64,
    identity: Option<InboxIdentity>,
    native_completed: std::collections::HashSet<TaskNotificationEffect>,
    subscriptions: HashMap<String, usize>,
}
impl TaskNotificationStore {
    pub(crate) fn invalidate(&mut self) {
        self.inboxes.clear();
        self.native_completed.clear();
        self.identity = None;
        self.generation += 1;
    }
    fn accept_native_completion(&mut self, effect: &TaskNotificationEffect) -> bool {
        self.inboxes.get(&effect.workspace_id).is_some_and(|inbox| {
            inbox
                .publication
                .items
                .iter()
                .any(|item| item.native_effect().as_ref() == Some(effect))
        }) && self.native_completed.insert(effect.clone())
    }
    fn begin(
        &mut self,
        identity: InboxIdentity,
        workspace: String,
        notification: Option<(String, u64)>,
    ) -> Option<Request> {
        if self.identity.as_ref() != Some(&identity) {
            self.invalidate();
            self.identity = Some(identity.clone());
        }
        let inbox = self
            .inboxes
            .entry(workspace.clone())
            .or_insert_with(|| Inbox {
                publication: TaskInboxPublication {
                    workspace_id: workspace.clone(),
                    ..Default::default()
                },
                ..Default::default()
            });
        if inbox.request.is_some() {
            if notification.is_none() {
                inbox.refresh_again = true;
            }
            return None;
        }
        if let Some((id, revision)) = &notification {
            let item = inbox
                .publication
                .items
                .iter_mut()
                .find(|item| item.notification.run_id == *id && item.revision == *revision)?;
            item.dismissing = true;
        } else {
            inbox.publication.loading = true;
        }
        inbox.publication.error = None;
        self.generation += 1;
        let refresh_range =
            (notification.is_none() && !inbox.publication.items.is_empty()).then(|| {
                match &inbox.publication.next_cursor {
                    Some(cursor) => RefreshRange::Through(cursor.clone()),
                    None => RefreshRange::End,
                }
            });
        let request = Request {
            workspace,
            generation: self.generation,
            identity,
            notification,
            open: false,
            navigation_revision: None,
            cursor: None,
            refresh_range,
        };
        inbox.request = Some(request.clone());
        Some(request)
    }
    fn complete(
        &mut self,
        request: &Request,
        result: Result<TaskCompletionInboxListResponse, String>,
    ) -> Option<bool> {
        if self.identity.as_ref() != Some(&request.identity) {
            return None;
        }
        let inbox = self.inboxes.get_mut(&request.workspace)?;
        if inbox.request.as_ref() != Some(request) {
            return None;
        }
        inbox.request = None;
        inbox.publication.loading = false;
        for item in &mut inbox.publication.items {
            item.dismissing = false;
        }
        match result {
            Err(error) => {
                // A failed revalidation must never preserve a closed result.
                inbox.publication.items.clear();
                inbox.publication.unread_count = 0;
                inbox.publication.next_cursor = None;
                inbox.publication.error = Some(error);
            }
            Ok(response) => {
                inbox.publication.unread_count = response.unread_count;
                if response
                    .entries
                    .iter()
                    .any(|n| n.workspace_id != request.workspace)
                {
                    inbox.publication.items.clear();
                    inbox.publication.next_cursor = None;
                    inbox.publication.unread_count = 0;
                    inbox.publication.error = Some("Task inbox scope mismatch".into());
                } else if let Some((id, revision)) = &request.notification {
                    if let Some(next) = response.entries.into_iter().find(|n| n.run_id == *id) {
                        if let Some(item) = inbox.publication.items.iter_mut().find(|item| {
                            item.notification.run_id == *id && item.revision == *revision
                        }) {
                            if item.notification != next {
                                item.notification = next;
                                item.revision = request.generation;
                            }
                        }
                    }
                } else {
                    let old = std::mem::take(&mut inbox.publication.items)
                        .into_iter()
                        .map(|i| (i.notification.run_id.clone(), i))
                        .collect::<HashMap<_, _>>();
                    let mut merged = if request.cursor.is_some() {
                        old.clone()
                    } else {
                        HashMap::new()
                    };
                    for notification in response.entries {
                        let revision = old
                            .get(&notification.run_id)
                            .filter(|previous| previous.notification == notification)
                            .map_or(request.generation, |previous| previous.revision);
                        merged.insert(
                            notification.run_id.clone(),
                            TaskInboxItem {
                                notification,
                                revision,
                                dismissing: false,
                            },
                        );
                    }
                    let mut items = merged.into_values().collect::<Vec<_>>();
                    items.sort_by(|a, b| {
                        (b.notification.completed_at, &b.notification.run_id)
                            .cmp(&(a.notification.completed_at, &a.notification.run_id))
                    });
                    inbox.publication.items = items;
                    inbox.publication.next_cursor = response.next_cursor;
                }
            }
        }
        let actionable = inbox
            .publication
            .items
            .iter()
            .filter_map(TaskInboxItem::native_effect)
            .collect::<std::collections::HashSet<_>>();
        self.native_completed.retain(|effect| {
            effect.workspace_id != request.workspace || actionable.contains(effect)
        });
        Some(std::mem::take(&mut inbox.refresh_again))
    }
}

/// The inbox owns its bounded request queue; workers never retain the shell.
#[derive(Default)]
pub(crate) struct TaskNotificationController {
    pub(crate) store: TaskNotificationStore,
    sender: Option<mpsc::SyncSender<Request>>,
    task: Option<JoinHandle<()>>,
}
impl TaskNotificationController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.store.invalidate();
    }
}
impl Drop for TaskNotificationController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl ClientCore {
    pub(crate) fn task_inbox_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::TaskInbox { workspace_id } = scope else {
            return;
        };
        let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
        let count = owner
            .store
            .subscriptions
            .entry(workspace_id.clone())
            .or_default();
        *count = if added {
            count.saturating_add(1)
        } else {
            count.saturating_sub(1)
        };
        let suspended = *count == 0;
        if suspended {
            owner.store.subscriptions.remove(workspace_id);
        }
        drop(owner);
        if suspended {
            self.task_inbox_demand_changed(scope, crate::core::ClientDemand::Suspended);
        }
    }
    pub(crate) fn task_inbox_demand_changed(
        &self,
        scope: &ClientScope,
        demand: crate::core::ClientDemand,
    ) {
        let ClientScope::TaskInbox { workspace_id } = scope else {
            return;
        };
        if demand != crate::core::ClientDemand::Suspended {
            return;
        }
        let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
        owner
            .store
            .native_completed
            .retain(|effect| &effect.workspace_id != workspace_id);
        if owner.store.inboxes.remove(workspace_id).is_some() {
            owner.store.generation += 1;
            let revision = self
                .snapshot(scope)
                .map_or(1, |publication| publication.revisions().scoped().get() + 1);
            self.publish(
                &ClientMutationAuthority { _private: () },
                scope.clone(),
                crate::threads::registry::revisions(revision),
                Arc::new(()),
                vec![],
            );
        }
    }
    pub(crate) fn start_task_notification_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<Request>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-task-inbox".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if core.is_stopped() {
                        return;
                    }
                    // A queued request may have lost access before its transport turn.
                    if !core.task_inbox_request_is_current(&request) {
                        continue;
                    }
                    if request.open {
                        core.open_task_notification(request);
                        continue;
                    }
                    let sender = core.transport_runtime().ws_command_sender();
                    let result = if let Some((id, _)) = &request.notification {
                        inbox_entry_rpc(&sender, &request.workspace, id, true).map(|r| {
                            TaskCompletionInboxListResponse {
                                entries: vec![r.entry],
                                unread_count: r.unread_count,
                                next_cursor: None,
                            }
                        })
                    } else {
                        load_task_inbox_pages(
                            &request,
                            || core.task_inbox_request_is_current(&request),
                            |cursor| {
                                crate::rpc::send_json_rpc_request_typed(
                                    &sender,
                                    pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_LIST,
                                    &pioneer_protocol::TaskCompletionInboxListParams {
                                        workspace_id: request.workspace.clone(),
                                        cursor,
                                        limit: Some(30),
                                    },
                                    crate::rpc::RPC_REQUEST_TIMEOUT,
                                )
                            },
                        )
                    };
                    core.complete_task_inbox(request, result.map_err(|e| format!("{e:#}")));
                }
            })
            .expect("task inbox worker could not start");
        let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
    fn task_inbox_identity(&self, workspace: &str, acknowledge: bool) -> Option<InboxIdentity> {
        let auth = self.current_auth()?;
        let capabilities = self
            .authorization_snapshot(Some(workspace), None)?
            .workspace?
            .capabilities;
        if !capabilities.can_read_own_notifications
            || (acknowledge && !capabilities.can_acknowledge_own_notifications)
        {
            return None;
        }
        Some(InboxIdentity {
            connection: self.gateway_http_generation()?,
            principal: auth.principal.id.to_string(),
            authorization: self.authorization_connection_generation(),
        })
    }
    fn task_inbox_request_is_current(&self, request: &Request) -> bool {
        !self.is_stopped()
            && self
                .task_inbox_identity(
                    &request.workspace,
                    request.notification.is_some() && !request.open,
                )
                .as_ref()
                == Some(&request.identity)
            && self
                .task_notifications
                .lock()
                .expect("task inbox poisoned")
                .store
                .inboxes
                .get(&request.workspace)
                .is_some_and(|inbox| inbox.request.as_ref() == Some(request))
    }
    pub fn task_inbox(&self, workspace: &str) -> Option<Arc<TaskInboxPublication>> {
        self.snapshot(&ClientScope::TaskInbox {
            workspace_id: workspace.into(),
        })?
        .snapshot()
        .payload::<TaskInboxPublication>()
    }
    pub fn task_notification_intent(&self, intent: TaskNotificationIntent) -> ClientTransition {
        if let TaskNotificationIntent::NativeCompletion {
            workspace_id,
            effect,
            completion,
        } = intent
        {
            let authority = ClientMutationAuthority { _private: () };
            if workspace_id != effect.workspace_id
                || self
                    .task_inbox_identity(
                        &workspace_id,
                        completion == TaskNotificationCompletion::Dismissed,
                    )
                    .is_none()
            {
                return self.transition(&authority, vec![], vec![]);
            }
            let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
            if !owner.store.accept_native_completion(&effect) {
                return self.transition(&authority, vec![], vec![]);
            }
            drop(owner);
            return self.task_notification_intent(match completion {
                TaskNotificationCompletion::Activated => TaskNotificationIntent::Open {
                    workspace_id,
                    notification_id: effect.notification_id,
                    revision: effect.revision,
                },
                TaskNotificationCompletion::Dismissed => TaskNotificationIntent::Dismiss {
                    workspace_id,
                    notification_id: effect.notification_id,
                    revision: effect.revision,
                },
            });
        }
        let deferred_intent = intent.clone();
        let open = matches!(&intent, TaskNotificationIntent::Open { .. });
        let load_more = matches!(&intent, TaskNotificationIntent::LoadMore { .. });
        let (workspace, notification) = match intent {
            TaskNotificationIntent::NativeCompletion { .. } => {
                unreachable!("handled native completion")
            }
            TaskNotificationIntent::Refresh { workspace_id }
            | TaskNotificationIntent::Retry { workspace_id }
            | TaskNotificationIntent::LoadMore { workspace_id } => (workspace_id, None),
            TaskNotificationIntent::Open {
                workspace_id,
                notification_id,
                revision,
            }
            | TaskNotificationIntent::Dismiss {
                workspace_id,
                notification_id,
                revision,
            } => (workspace_id, Some((notification_id, revision))),
        };
        let authority = ClientMutationAuthority { _private: () };
        let Some(identity) = self.task_inbox_identity(&workspace, notification.is_some() && !open)
        else {
            return self.transition(&authority, vec![], vec![]);
        };
        let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
        if self.is_stopped() {
            return self.transition(&authority, vec![], vec![]);
        }
        if notification.is_some() {
            if let Some(inbox) = owner.store.inboxes.get_mut(&workspace) {
                if inbox.request.is_some() {
                    if inbox.pending_actions.len() < 100
                        && !inbox.pending_actions.contains(&deferred_intent)
                    {
                        inbox.pending_actions.push_back(deferred_intent);
                    }
                    return self.transition(&authority, vec![], vec![]);
                }
            }
        }
        let cursor = if load_more {
            let Some(cursor) = owner
                .store
                .inboxes
                .get(&workspace)
                .and_then(|inbox| inbox.publication.next_cursor.clone())
            else {
                return self.transition(&authority, vec![], vec![]);
            };
            Some(cursor)
        } else {
            None
        };
        if !open
            && notification.as_ref().is_some_and(|(id, revision)| {
                owner.store.inboxes.get(&workspace).is_none_or(|inbox| {
                    !inbox.publication.items.iter().any(|item| {
                        item.notification.run_id == *id
                            && item.revision == *revision
                            && item.is_actionable()
                    })
                })
            })
        {
            return self.transition(&authority, vec![], vec![]);
        }
        let Some(mut request) = owner.store.begin(identity, workspace.clone(), notification) else {
            return self.transition(&authority, vec![], vec![]);
        };
        request.cursor = cursor;
        if load_more {
            request.refresh_range = None;
        }
        owner
            .store
            .inboxes
            .get_mut(&workspace)
            .expect("inbox exists")
            .request = Some(request.clone());
        if open {
            request.open = true;
            request.navigation_revision = self
                .snapshot(&ClientScope::Navigation)
                .map(|p| p.revisions().scoped().get());
            let inbox = owner
                .store
                .inboxes
                .get_mut(&workspace)
                .expect("inbox exists");
            inbox.request = Some(request.clone());
        }
        if owner
            .sender
            .as_ref()
            .is_none_or(|sender| sender.try_send(request.clone()).is_err())
        {
            owner
                .store
                .complete(&request, Err("Task inbox request unavailable".into()));
        }
        self.publish_task_inbox(&mut owner.store, &workspace)
    }
    fn open_task_notification(&self, request: Request) {
        let Some((notification_id, revision)) = request.notification.as_ref() else {
            return;
        };
        let notification = self.task_inbox(&request.workspace).and_then(|inbox| {
            inbox
                .items
                .iter()
                .find(|item| {
                    item.notification.run_id == *notification_id && item.revision == *revision
                })
                .map(|item| item.notification.clone())
        });
        let Some(notification) = notification else {
            return;
        };
        let result = (|| -> anyhow::Result<pioneer_protocol::Thread> {
            let sender = self.transport_runtime().ws_command_sender();
            let current =
                inbox_entry_rpc(&sender, &request.workspace, &notification.run_id, false)?.entry;
            let target = notification_thread_target(&notification, &current)?;
            let thread = sender
                .thread_get(pioneer_protocol::ThreadGetParams { thread_id: target })?
                .thread;
            anyhow::ensure!(
                thread.workspace_id == request.workspace,
                "Task inbox thread scope mismatch"
            );
            Ok(thread)
        })();
        if self.task_inbox_identity(&request.workspace, false).as_ref() != Some(&request.identity) {
            return;
        }
        let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
        let Some(inbox) = owner.store.inboxes.get_mut(&request.workspace) else {
            return;
        };
        if inbox.request.as_ref() != Some(&request) || self.is_stopped() {
            return;
        }
        inbox.request = None;
        for item in &mut inbox.publication.items {
            item.dismissing = false;
        }
        match result {
            Ok(thread)
                if self.navigation_snapshot().workspace_id() == Some(&request.workspace)
                    && self
                        .snapshot(&ClientScope::Navigation)
                        .map(|p| p.revisions().scoped().get())
                        == request.navigation_revision =>
            {
                let thread_id = thread.id.clone();
                self.upsert_thread(thread);
                let transition = self.open_workspace_thread(
                    request.workspace.clone(),
                    Some(thread_id.clone()),
                    request.navigation_revision,
                );
                if !matches!(
                    transition.outcome(),
                    crate::core::ClientTransitionOutcome::Stale
                        | crate::core::ClientTransitionOutcome::Rejected
                ) {
                    inbox.publication.opened = Some(TaskNotificationPublication {
                        operation_id: request.generation,
                        notification_id: notification_id.clone(),
                        task_id: notification.task_id,
                        task_revision: *revision,
                        thread_id,
                    });
                }
            }
            Ok(_) => {}
            Err(error) => {
                inbox.publication.items.clear();
                inbox.publication.unread_count = 0;
                inbox.publication.next_cursor = None;
                inbox.publication.error = Some(format!("{error:#}"));
            }
        }
        let opened = inbox
            .publication
            .opened
            .as_ref()
            .is_some_and(|opened| opened.operation_id == request.generation);
        self.publish_task_inbox(&mut owner.store, &request.workspace);
        drop(owner);
        if opened {
            // Navigation committed successfully. Read is a separate, ACL-checked
            // server operation and never changes a thread read cursor.
            self.task_notification_intent(TaskNotificationIntent::Dismiss {
                workspace_id: request.workspace.clone(),
                notification_id: notification_id.clone(),
                revision: *revision,
            });
        }
        self.continue_task_inbox_actions(&request.workspace);
    }
    fn continue_task_inbox_actions(&self, workspace: &str) {
        // Skip stale deferred actions without stranding a later valid action.
        // A started request owns the remainder until its matching completion.
        loop {
            let next = {
                let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
                let Some(inbox) = owner.store.inboxes.get_mut(workspace) else {
                    return;
                };
                if inbox.request.is_some() {
                    return;
                }
                inbox.pending_actions.pop_front().or_else(|| {
                    std::mem::take(&mut inbox.refresh_again).then(|| {
                        TaskNotificationIntent::Refresh {
                            workspace_id: workspace.to_owned(),
                        }
                    })
                })
            };
            let Some(intent) = next else {
                return;
            };
            self.task_notification_intent(intent);
        }
    }
    fn publish_task_inbox(
        &self,
        store: &mut TaskNotificationStore,
        workspace: &str,
    ) -> ClientTransition {
        let inbox = store.inboxes.get_mut(workspace).expect("inbox exists");
        let scope = ClientScope::TaskInbox {
            workspace_id: workspace.into(),
        };
        let previous = self.task_inbox(workspace);
        let next = &mut inbox.publication;
        next.native_notifications = next
            .items
            .iter()
            .filter_map(TaskInboxItem::native_effect)
            .collect();
        next.revision = previous.as_ref().map_or(0, |p| p.revision);
        let authority = ClientMutationAuthority { _private: () };
        if previous.as_ref().is_some_and(|p| p.as_ref() == next) {
            return self.transition(&authority, vec![], vec![]);
        }
        // Scope revisions survive protected publication eviction.
        next.revision = self
            .snapshot(&scope)
            .map_or(1, |p| p.revisions().scoped().get() + 1);
        self.publish(
            &authority,
            scope,
            crate::threads::registry::revisions(next.revision),
            Arc::new(next.clone()),
            vec![],
        )
    }
    fn complete_task_inbox(
        &self,
        request: Request,
        result: Result<TaskCompletionInboxListResponse, String>,
    ) {
        if self
            .task_inbox_identity(&request.workspace, request.notification.is_some())
            .as_ref()
            != Some(&request.identity)
        {
            return;
        }
        {
            let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
            if self.is_stopped() {
                return;
            }
            let Some(refresh) = owner.store.complete(&request, result) else {
                return;
            };
            if let Some(inbox) = owner.store.inboxes.get_mut(&request.workspace) {
                inbox.refresh_again |= refresh;
            }
            self.publish_task_inbox(&mut owner.store, &request.workspace);
        }
        self.continue_task_inbox_actions(&request.workspace);
    }

    pub(crate) fn observe_task_notification(
        &self,
        notification: &pioneer_protocol::GatewayNotification,
    ) {
        use pioneer_protocol::GatewayNotification::*;
        let workspace = match notification {
            TaskRunCompleted(h) => Some(h.context.workspace_id.as_str()),
            TaskRunFailed(h) | TaskRunBlocked(h) | TaskRunCancelled(h) => {
                Some(h.context.workspace_id.as_str())
            }
            TaskCompleted(h) => Some(h.context.workspace_id.as_str()),
            TaskFailed(h) | TaskBlocked(h) => Some(h.context.workspace_id.as_str()),
            TaskCancelled(h) => Some(h.context.workspace_id.as_str()),
            TaskUpdated(h) => Some(h.context.workspace_id.as_str()),
            TaskTreeChanged(h) => Some(h.context.workspace_id.as_str()),
            TaskCompletionInboxChanged(h) => Some(h.workspace_id.as_str()),
            TaskUserNotificationDelivered(h)
                if self
                    .current_auth()
                    .is_some_and(|a| a.principal.id.as_str() == h.recipient_principal_id) =>
            {
                Some(h.workspace_id.as_str())
            }
            _ => None,
        };
        if let Some(workspace) = workspace
            && self.task_inbox(workspace).is_some()
        {
            self.task_notification_intent(TaskNotificationIntent::Refresh {
                workspace_id: workspace.to_owned(),
            });
        }
    }
}

/// Revalidate the loaded prefix through its previous exact keyset frontier.
/// Stage pages within the existing request, then replace the publication once;
/// absence in this authoritative prefix removes revoked/nonterminal records.
fn load_task_inbox_pages(
    request: &Request,
    mut is_current: impl FnMut() -> bool,
    mut fetch: impl FnMut(Option<String>) -> anyhow::Result<TaskCompletionInboxListResponse>,
) -> anyhow::Result<TaskCompletionInboxListResponse> {
    // Gateway's existing cursor is JSON (exact saved timestamp, run ID).
    // Compare that key, not the public entry's lossy epoch-second timestamp.
    let decode =
        |cursor: &str| -> anyhow::Result<(String, String)> { Ok(serde_json::from_str(cursor)?) };
    let through = match &request.refresh_range {
        Some(RefreshRange::Through(cursor)) => Some(decode(cursor)?),
        _ => None,
    };
    let mut cursor = request.cursor.clone();
    let mut entries = Vec::new();
    loop {
        anyhow::ensure!(is_current(), "Task inbox request is stale");
        let page = fetch(cursor.clone())?;
        anyhow::ensure!(
            page.entries
                .iter()
                .all(|entry| entry.workspace_id == request.workspace),
            "Task inbox scope mismatch"
        );
        let next_key = page.next_cursor.as_deref().map(decode).transpose()?;
        if let Some(next) = &next_key {
            anyhow::ensure!(!page.entries.is_empty(), "Empty task inbox continuation");
            if let Some(cursor) = &cursor {
                anyhow::ensure!(next < &decode(cursor)?, "Task inbox cursor did not advance");
            }
        }
        let finished = request.refresh_range.is_none()
            || next_key.is_none()
            || through
                .as_ref()
                .is_some_and(|end| next_key.as_ref().is_some_and(|next| next <= end));
        entries.extend(page.entries);
        if finished {
            return Ok(TaskCompletionInboxListResponse {
                entries,
                unread_count: page.unread_count,
                next_cursor: page.next_cursor,
            });
        }
        cursor = page.next_cursor;
    }
}

fn inbox_entry_rpc(
    sender: &crate::transport::ws::runtime::GatewayWsCommandSender,
    workspace: &str,
    run_id: &str,
    read: bool,
) -> anyhow::Result<pioneer_protocol::TaskCompletionInboxEntryResponse> {
    crate::rpc::send_json_rpc_request_typed(
        sender,
        if read {
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_READ
        } else {
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_GET
        },
        &pioneer_protocol::TaskCompletionInboxEntryParams {
            workspace_id: workspace.to_owned(),
            run_id: run_id.to_owned(),
        },
        crate::rpc::RPC_REQUEST_TIMEOUT,
    )
}

fn notification_thread_target(
    cached: &TaskCompletionInboxEntry,
    current: &TaskCompletionInboxEntry,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        current.workspace_id == cached.workspace_id
            && current.task_id == cached.task_id
            && current.run_id == cached.run_id
            && !current.thread_id.is_empty(),
        "Task inbox context mismatch"
    );
    Ok(current.thread_id.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity(principal: &str) -> InboxIdentity {
        InboxIdentity {
            connection: 1,
            principal: principal.into(),
            authorization: 1,
        }
    }
    fn notification(id: &str, workspace: &str) -> TaskCompletionInboxEntry {
        serde_json::from_value(serde_json::json!({"workspaceId":workspace,"taskId":"task-a","runId":id,"title":"Task","threadId":"root","threadTitle":"Thread","status":"succeeded","completedAt":10})).unwrap()
    }
    fn response(
        items: Vec<TaskCompletionInboxEntry>,
    ) -> Result<TaskCompletionInboxListResponse, String> {
        Ok(TaskCompletionInboxListResponse {
            entries: items,
            unread_count: 1,
            next_cursor: None,
        })
    }
    #[test]
    fn deferred_actions_are_drained_after_access_loss_and_last_binding_closes_inbox() {
        let core = Arc::new(ClientCore::new());
        {
            let mut owner = core.task_notifications.lock().unwrap();
            let request = owner.store.begin(identity("p"), "a".into(), None).unwrap();
            owner
                .store
                .complete(&request, response(vec![notification("n", "a")]));
            let inbox = owner.store.inboxes.get_mut("a").unwrap();
            for revision in 1..4 {
                inbox
                    .pending_actions
                    .push_back(TaskNotificationIntent::Dismiss {
                        workspace_id: "a".into(),
                        notification_id: "n".into(),
                        revision,
                    });
            }
            inbox.refresh_again = true;
            core.publish_task_inbox(&mut owner.store, "a");
        }
        core.continue_task_inbox_actions("a");
        {
            let owner = core.task_notifications.lock().unwrap();
            assert!(owner.store.inboxes["a"].pending_actions.is_empty());
            assert!(!owner.store.inboxes["a"].refresh_again);
        }
        let subscription = core.subscribe(
            ClientScope::TaskInbox {
                workspace_id: "a".into(),
            },
            std::num::NonZeroUsize::new(4).unwrap(),
        );
        drop(subscription);
        assert!(core.task_inbox("a").is_none());
        assert!(
            core.task_notifications
                .lock()
                .unwrap()
                .store
                .inboxes
                .is_empty()
        );
    }
    #[test]
    fn native_completion_matches_task_revision_once_and_terminal_state_releases_dedupe() {
        let mut store = TaskNotificationStore::default();
        let request = store.begin(identity("p"), "a".into(), None).unwrap();
        store.complete(&request, response(vec![notification("n", "a")]));
        let effect = store.inboxes["a"].publication.items[0]
            .native_effect()
            .unwrap();
        let mut wrong = effect.clone();
        wrong.workspace_id = "b".into();
        assert!(!store.accept_native_completion(&wrong));
        wrong = effect.clone();
        wrong.task_id = "wrong".into();
        assert!(!store.accept_native_completion(&wrong));
        wrong = effect.clone();
        wrong.revision += 1;
        assert!(!store.accept_native_completion(&wrong));
        assert!(store.accept_native_completion(&effect));
        assert!(!store.accept_native_completion(&effect));
        let dismiss = store
            .begin(
                identity("p"),
                "a".into(),
                Some(("n".into(), effect.revision)),
            )
            .unwrap();
        let mut terminal = notification("n", "a");
        terminal.read_at = Some(20);
        store.complete(&dismiss, response(vec![terminal]));
        assert!(store.native_completed.is_empty());
        assert!(!store.accept_native_completion(&effect));
        store.invalidate();
        assert!(!store.accept_native_completion(&effect));
    }
    #[test]
    fn inbox_publications_do_not_notify_other_workspace_navigation_or_avatars() {
        let core = Arc::new(ClientCore::new());
        let other = core.subscribe(
            ClientScope::TaskInbox {
                workspace_id: "b".into(),
            },
            std::num::NonZeroUsize::new(4).unwrap(),
        );
        let navigation = core.navigation_snapshot();
        let mut store = TaskNotificationStore::default();
        let request = store.begin(identity("p"), "a".into(), None).unwrap();
        store.complete(&request, response(vec![notification("n", "a")]));
        core.publish_task_inbox(&mut store, "a");
        let first = core.task_inbox("a").unwrap();
        core.publish_task_inbox(&mut store, "a");
        assert!(Arc::ptr_eq(&first, &core.task_inbox("a").unwrap()));
        assert_eq!(first.native_notifications.len(), 1);
        assert_eq!(
            serde_json::from_value::<TaskInboxPublication>(
                serde_json::to_value(first.as_ref()).unwrap()
            )
            .unwrap(),
            *first
        );
        assert!(other.try_next().is_none());
        assert_eq!(navigation.as_ref(), core.navigation_snapshot().as_ref());
    }
    #[test]
    fn requests_are_fenced_by_principal_workspace_and_generation() {
        let mut store = TaskNotificationStore::default();
        let old = store.begin(identity("old"), "a".into(), None).unwrap();
        let current = store.begin(identity("new"), "a".into(), None).unwrap();
        assert_eq!(
            store.complete(&old, response(vec![notification("old", "a")])),
            None
        );
        assert_eq!(
            store.complete(&current, response(vec![notification("foreign", "b")])),
            Some(false)
        );
        assert!(store.inboxes["a"].publication.items.is_empty());
        assert!(store.inboxes["a"].publication.error.is_some());
        assert_eq!(
            store.complete(&current, response(vec![notification("late", "a")])),
            None
        );
    }
    #[test]
    fn duplicate_hint_coalesces_and_equal_refresh_preserves_item_identity() {
        let mut store = TaskNotificationStore::default();
        let first = store.begin(identity("p"), "a".into(), None).unwrap();
        assert!(store.begin(identity("p"), "a".into(), None).is_none());
        assert_eq!(
            store.complete(&first, response(vec![notification("n", "a")])),
            Some(true)
        );
        let before = store.inboxes["a"].publication.items.clone();
        let second = store.begin(identity("p"), "a".into(), None).unwrap();
        store.complete(&second, response(vec![notification("n", "a")]));
        assert_eq!(store.inboxes["a"].publication.items, before);
        assert!(
            store
                .begin(
                    identity("p"),
                    "a".into(),
                    Some(("n".into(), before[0].revision + 1))
                )
                .is_none()
        );
        let dismiss = store
            .begin(
                identity("p"),
                "a".into(),
                Some(("n".into(), before[0].revision)),
            )
            .unwrap();
        let mut acknowledged = notification("n", "a");
        acknowledged.read_at = Some(20);
        store.complete(&dismiss, response(vec![acknowledged]));
        assert!(!store.inboxes["a"].publication.items[0].is_actionable());
        assert!(
            store
                .begin(
                    identity("p"),
                    "a".into(),
                    Some(("n".into(), before[0].revision))
                )
                .is_none()
        );
        store.invalidate();
        assert!(store.inboxes.is_empty());
        assert_eq!(store.complete(&dismiss, response(vec![])), None);
    }
    #[test]
    fn paginated_history_uses_server_total_merges_by_run_and_updates_existing_rows() {
        let mut store = TaskNotificationStore::default();
        let first = store.begin(identity("p"), "a".into(), None).unwrap();
        store.complete(
            &first,
            Ok(TaskCompletionInboxListResponse {
                entries: vec![notification("r3", "a"), notification("r2", "a")],
                unread_count: 87,
                next_cursor: Some("cursor".into()),
            }),
        );
        assert_eq!(store.inboxes["a"].publication.unread_count(), 87);
        assert_eq!(store.inboxes["a"].publication.items().len(), 2);
        let mut next = store.begin(identity("p"), "a".into(), None).unwrap();
        next.cursor = Some("cursor".into());
        store.inboxes.get_mut("a").unwrap().request = Some(next.clone());
        let mut changed = notification("r2", "a");
        changed.status = TaskRunStatus::Failed;
        changed.read_at = Some(15);
        store.complete(
            &next,
            Ok(TaskCompletionInboxListResponse {
                entries: vec![
                    changed.clone(),
                    notification("r1", "a"),
                    notification("r1", "a"),
                ],
                unread_count: 86,
                next_cursor: None,
            }),
        );
        let publication = &store.inboxes["a"].publication;
        assert_eq!(publication.items().len(), 3);
        assert_eq!(publication.unread_count(), 86);
        assert_eq!(publication.items()[1].notification(), &changed);
        // Read entries can still be explicitly opened from history.
        let revision = publication.items()[1].revision();
        assert!(
            store
                .begin(identity("p"), "a".into(), Some(("r2".into(), revision)))
                .is_some()
        );
    }
    fn history_cursor(run: &str) -> String {
        serde_json::to_string(&("2026-10-16 10:00:00 +00:00", run)).unwrap()
    }
    fn history_entries(last: usize) -> Vec<TaskCompletionInboxEntry> {
        (0..=last)
            .rev()
            .map(|ix| notification(&format!("r{ix:03}"), "a"))
            .collect()
    }
    fn history_request(store: &mut TaskNotificationStore, more: bool) -> Request {
        let cursor = more.then(|| store.inboxes["a"].publication.next_cursor.clone().unwrap());
        let mut request = store.begin(identity("p"), "a".into(), None).unwrap();
        if more {
            request.cursor = cursor;
            request.refresh_range = None;
            store.inboxes.get_mut("a").unwrap().request = Some(request.clone());
        }
        request
    }
    fn history_response(
        request: &Request,
        source: &[TaskCompletionInboxEntry],
    ) -> (TaskCompletionInboxListResponse, Vec<Option<String>>) {
        let mut cursors = Vec::new();
        let response = load_task_inbox_pages(
            request,
            || true,
            |cursor| {
                cursors.push(cursor.clone());
                let before = cursor
                    .map(|cursor| serde_json::from_str::<(String, String)>(&cursor).unwrap().1);
                let mut entries = source
                    .iter()
                    .filter(|entry| before.as_ref().is_none_or(|id| &entry.run_id < id))
                    .take(31)
                    .cloned()
                    .collect::<Vec<_>>();
                let next_cursor = (entries.len() > 30).then(|| history_cursor(&entries[29].run_id));
                entries.truncate(30);
                Ok(TaskCompletionInboxListResponse {
                    entries,
                    unread_count: source
                        .iter()
                        .filter(|entry| entry.read_at.is_none())
                        .count() as u64,
                    next_cursor,
                })
            },
        )
        .unwrap();
        (response, cursors)
    }
    fn two_history_pages(source: &[TaskCompletionInboxEntry]) -> TaskNotificationStore {
        let mut store = TaskNotificationStore::default();
        for more in [false, true] {
            let request = history_request(&mut store, more);
            let (response, cursors) = history_response(&request, source);
            assert_eq!(cursors.len(), 1);
            store.complete(&request, Ok(response));
        }
        assert_eq!(store.inboxes["a"].publication.items.len(), 60);
        store
    }
    #[test]
    fn own_second_page_read_and_invalidation_keep_history_revisions_and_continuation() {
        let mut source = history_entries(99);
        let mut store = two_history_pages(&source);
        let before = store.inboxes["a"].publication.items.clone();
        let item = before
            .iter()
            .find(|item| item.notification.run_id == "r050")
            .unwrap();
        let read = store
            .begin(
                identity("p"),
                "a".into(),
                Some(("r050".into(), item.revision)),
            )
            .unwrap();
        let changed = source
            .iter_mut()
            .find(|entry| entry.run_id == "r050")
            .unwrap();
        changed.read_at = Some(20);
        assert!(store.begin(identity("p"), "a".into(), None).is_none());
        assert_eq!(
            store.complete(
                &read,
                Ok(TaskCompletionInboxListResponse {
                    entries: vec![changed.clone()],
                    unread_count: 99,
                    next_cursor: None,
                }),
            ),
            Some(true)
        );
        // The own read RPC's invalidation is coalesced while read is in flight.
        // Starting it after completion captures the entire loaded frontier.
        let request = history_request(&mut store, false);
        assert_eq!(
            request.refresh_range,
            Some(RefreshRange::Through(history_cursor("r040")))
        );
        let after_read = store.inboxes["a"].publication.items.clone();
        let (response, cursors) = history_response(&request, &source);
        assert_eq!(cursors, vec![None, Some(history_cursor("r070"))]);
        store.complete(&request, Ok(response));
        let inbox = &store.inboxes["a"].publication;
        assert_eq!(
            inbox.items, after_read,
            "a read invalidation does not reset or revise equal cards"
        );
        assert_eq!(inbox.unread_count, 99);
        assert_eq!(inbox.next_cursor, Some(history_cursor("r040")));
        assert_eq!(inbox.items[49].notification.run_id, "r050");
        assert!(inbox.items[49].notification.read_at.is_some());
        let more = history_request(&mut store, true);
        let (response, _) = history_response(&more, &source);
        assert_eq!(response.entries[0].run_id, "r039");
        store.complete(&more, Ok(response));
        assert_eq!(store.inboxes["a"].publication.items.len(), 90);
    }
    #[test]
    fn remote_read_and_removed_boundary_are_authoritatively_revalidated_across_pages() {
        let mut source = history_entries(99);
        let mut store = two_history_pages(&source);
        // A remote device reads an old entry; another entry loses ACL and the
        // frontier entry resumes execution. Neither absent row may be retained.
        source
            .iter_mut()
            .find(|entry| entry.run_id == "r050")
            .unwrap()
            .read_at = Some(25);
        source
            .iter_mut()
            .find(|entry| entry.run_id == "r080")
            .unwrap()
            .title = "Updated task".into();
        source.retain(|entry| entry.run_id != "r055" && entry.run_id != "r040");
        source.insert(0, notification("r100", "a"));
        let request = history_request(&mut store, false);
        let (response, cursors) = history_response(&request, &source);
        assert_eq!(
            cursors.len(),
            2,
            "a removed cursor row must not require scanning to the end"
        );
        store.complete(&request, Ok(response));
        let inbox = &store.inboxes["a"].publication;
        assert_eq!(inbox.items.len(), 60);
        assert_eq!(inbox.items[0].notification.run_id, "r100");
        assert_eq!(inbox.next_cursor, Some(history_cursor("r039")));
        assert_eq!(inbox.unread_count, 98);
        assert!(
            inbox.items.iter().all(
                |item| item.notification.run_id != "r055" && item.notification.run_id != "r040"
            )
        );
        assert!(
            inbox
                .items
                .iter()
                .find(|item| item.notification.run_id == "r050")
                .unwrap()
                .notification
                .read_at
                .is_some()
        );
        assert_eq!(
            inbox
                .items
                .iter()
                .find(|item| item.notification.run_id == "r080")
                .unwrap()
                .notification
                .title,
            "Updated task"
        );
        let more = history_request(&mut store, true);
        let (response, _) = history_response(&more, &source);
        assert_eq!(response.entries[0].run_id, "r038");
        store.complete(&more, Ok(response));
        let ids = store.inboxes["a"]
            .publication
            .items
            .iter()
            .map(|item| item.notification.run_id.clone())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            ids.len(),
            90,
            "continuation has neither gaps nor duplicates"
        );
        assert!(
            (9..=100)
                .filter(|ix| *ix != 40 && *ix != 55)
                .all(|ix| ids.contains(&format!("r{ix:03}")))
        );
    }
    #[test]
    fn more_than_one_page_of_new_completions_preserves_the_previous_loaded_range() {
        let source = history_entries(99);
        let mut store = two_history_pages(&source);
        let before = store.inboxes["a"].publication.items.clone();
        let source = history_entries(134);
        let request = history_request(&mut store, false);
        let (response, cursors) = history_response(&request, &source);
        assert_eq!(
            cursors.len(),
            4,
            "refresh follows the old frontier, not the old page count"
        );
        store.complete(&request, Ok(response));
        let inbox = &store.inboxes["a"].publication;
        assert_eq!(inbox.unread_count, 135);
        assert_eq!(inbox.items.len(), 120);
        for old in before {
            assert_eq!(
                inbox
                    .items
                    .iter()
                    .find(|item| item.notification.run_id == old.notification.run_id),
                Some(&old)
            );
        }
        let more = history_request(&mut store, true);
        let (response, _) = history_response(&more, &source);
        assert_eq!(response.entries[0].run_id, "r014");
        store.complete(&more, Ok(response));
        assert_eq!(store.inboxes["a"].publication.items.len(), 135);
        assert!(store.inboxes["a"].publication.next_cursor.is_none());
        let request = history_request(&mut store, false);
        assert_eq!(request.refresh_range, Some(RefreshRange::End));
        let (response, cursors) = history_response(&request, &source);
        assert_eq!(cursors.len(), 5);
        store.complete(&request, Ok(response));
        assert_eq!(store.inboxes["a"].publication.items.len(), 135);
    }
    #[test]
    fn refresh_frontier_compares_exact_cursor_timestamps_even_with_equal_public_seconds() {
        let mut store = two_history_pages(&history_entries(99));
        let mut request = history_request(&mut store, false);
        request.refresh_range = Some(RefreshRange::Through(
            serde_json::to_string(&("2026-10-16 10:00:00.100 +00:00", "r050")).unwrap(),
        ));
        let mut calls = 0;
        let response = load_task_inbox_pages(
            &request,
            || true,
            |_| {
                calls += 1;
                let (at, id) = if calls == 1 {
                    ("2026-10-16 10:00:00.200 +00:00", "r001")
                } else {
                    ("2026-10-16 10:00:00.100 +00:00", "r049")
                };
                Ok(TaskCompletionInboxListResponse {
                    entries: vec![notification(id, "a")],
                    unread_count: 100,
                    next_cursor: Some(serde_json::to_string(&(at, id)).unwrap()),
                })
            },
        )
        .unwrap();
        assert_eq!(
            calls, 2,
            "a lower run ID alone cannot cross a newer fractional timestamp"
        );
        assert_eq!(response.entries.len(), 2);
        assert_eq!(
            response.next_cursor,
            Some(serde_json::to_string(&("2026-10-16 10:00:00.100 +00:00", "r049")).unwrap())
        );
    }
    #[test]
    fn multipage_refresh_checks_fences_between_pages_and_rejects_nonadvancing_cursors() {
        let mut store = two_history_pages(&history_entries(99));
        let request = history_request(&mut store, false);
        let mut checks = 0;
        let mut fetched = 0;
        let result = load_task_inbox_pages(
            &request,
            || {
                checks += 1;
                checks == 1
            },
            |_| {
                fetched += 1;
                Ok(TaskCompletionInboxListResponse {
                    entries: vec![notification("r099", "a")],
                    unread_count: 100,
                    next_cursor: Some(history_cursor("r099")),
                })
            },
        );
        assert!(result.is_err());
        assert_eq!(fetched, 1);
        store.invalidate();
        assert_eq!(
            store.complete(&request, result.map_err(|error| error.to_string())),
            None
        );
        let result = load_task_inbox_pages(
            &request,
            || true,
            |_| {
                Ok(TaskCompletionInboxListResponse {
                    entries: vec![notification("r099", "a")],
                    unread_count: 100,
                    next_cursor: Some(history_cursor("r099")),
                })
            },
        );
        assert!(result.unwrap_err().to_string().contains("did not advance"));
    }
    #[test]
    fn failed_revalidation_does_not_publish_partial_pages_or_retain_unverified_cards() {
        let mut store = two_history_pages(&history_entries(99));
        let request = history_request(&mut store, false);
        let mut calls = 0;
        let result = load_task_inbox_pages(
            &request,
            || true,
            |_| {
                calls += 1;
                if calls == 2 {
                    anyhow::bail!("page unavailable");
                }
                Ok(TaskCompletionInboxListResponse {
                    entries: vec![notification("r099", "a")],
                    unread_count: 100,
                    next_cursor: Some(history_cursor("r099")),
                })
            },
        );
        assert_eq!(calls, 2);
        assert_eq!(
            store.complete(&request, result.map_err(|error| error.to_string())),
            Some(false)
        );
        let inbox = &store.inboxes["a"].publication;
        assert!(inbox.items.is_empty());
        assert_eq!(inbox.unread_count, 0);
        assert!(inbox.next_cursor.is_none());
        assert_eq!(inbox.error.as_deref(), Some("page unavailable"));
    }
    #[test]
    fn open_targets_the_current_authorized_user_context_of_the_exact_run() {
        let cached = notification("run", "a");
        let mut current = cached.clone();
        current.thread_id = "collaboration-root".into();
        assert_eq!(
            notification_thread_target(&cached, &current).unwrap(),
            "collaboration-root"
        );
        current.run_id = "another-occurrence".into();
        assert!(notification_thread_target(&cached, &current).is_err());
        current = cached.clone();
        current.workspace_id = "b".into();
        assert!(notification_thread_target(&cached, &current).is_err());
        current = cached.clone();
        current.task_id = "other-task".into();
        assert!(notification_thread_target(&cached, &current).is_err());
    }
    #[test]
    fn reconnect_and_authorization_loss_fence_all_loaded_pages_and_pending_actions() {
        let mut store = TaskNotificationStore::default();
        let before = store.begin(identity("p"), "a".into(), None).unwrap();
        let mut reconnected = identity("p");
        reconnected.connection += 1;
        let after = store.begin(reconnected, "a".into(), None).unwrap();
        assert_eq!(
            store.complete(&before, response(vec![notification("old", "a")])),
            None
        );
        store.complete(&after, response(vec![notification("fresh", "a")]));
        let pending = store
            .begin(after.identity.clone(), "a".into(), None)
            .unwrap();
        store.invalidate();
        assert!(store.inboxes.is_empty());
        assert_eq!(
            store.complete(&pending, response(vec![notification("closed", "a")])),
            None
        );
        let new_workspace = store.begin(identity("p"), "b".into(), None).unwrap();
        store.complete(&new_workspace, response(vec![notification("other", "b")]));
        assert!(store.inboxes.get("a").is_none());
    }
}
