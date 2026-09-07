//! Exact-recipient inbox ownership and request fencing.
use crate::core::{ClientCore, ClientMutationAuthority, ClientScope, ClientTransition};
use pioneer_protocol::{TaskUserNotification, TaskUserNotificationListResponse};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    thread::JoinHandle,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskNotificationIntent {
    Refresh {
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
    notification: TaskUserNotification,
    revision: u64,
    dismissing: bool,
}
impl TaskInboxItem {
    pub fn notification(&self) -> &TaskUserNotification {
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
            notification_id: self.notification.notification_id.clone(),
            task_id: self.notification.task_id.clone(),
            revision: self.revision,
        })
    }
    pub fn is_actionable(&self) -> bool {
        self.notification.acknowledged_at.is_none()
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
struct Request {
    workspace: String,
    generation: u64,
    identity: InboxIdentity,
    notification: Option<(String, u64)>,
    open: bool,
    navigation_revision: Option<u64>,
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
            let item = inbox.publication.items.iter_mut().find(|item| {
                item.notification.notification_id == *id
                    && item.revision == *revision
                    && item.is_actionable()
            })?;
            item.dismissing = true;
        } else {
            inbox.publication.loading = true;
        }
        inbox.publication.error = None;
        self.generation += 1;
        let request = Request {
            workspace,
            generation: self.generation,
            identity,
            notification,
            open: false,
            navigation_revision: None,
        };
        inbox.request = Some(request.clone());
        Some(request)
    }
    fn complete(
        &mut self,
        request: &Request,
        result: Result<TaskUserNotificationListResponse, String>,
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
            Err(error) => inbox.publication.error = Some(error),
            Ok(response) => {
                if response
                    .notifications
                    .iter()
                    .any(|n| n.workspace_id != request.workspace)
                {
                    inbox.publication.error = Some("Task inbox scope mismatch".into());
                } else if let Some((id, revision)) = &request.notification {
                    if let Some(next) = response
                        .notifications
                        .into_iter()
                        .find(|n| n.notification_id == *id)
                    {
                        if let Some(item) = inbox.publication.items.iter_mut().find(|item| {
                            item.notification.notification_id == *id && item.revision == *revision
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
                        .map(|i| (i.notification.notification_id.clone(), i))
                        .collect::<HashMap<_, _>>();
                    let mut seen = std::collections::HashSet::new();
                    inbox.publication.items = response
                        .notifications
                        .into_iter()
                        .filter(|n| seen.insert(n.notification_id.clone()))
                        .take(100)
                        .map(|notification| {
                            let revision = old
                                .get(&notification.notification_id)
                                .filter(|previous| previous.notification == notification)
                                .map_or(request.generation, |previous| previous.revision);
                            TaskInboxItem {
                                notification,
                                revision,
                                dismissing: false,
                            }
                        })
                        .collect();
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
                    if core
                        .task_inbox_identity(
                            &request.workspace,
                            request.notification.is_some() && !request.open,
                        )
                        .as_ref()
                        != Some(&request.identity)
                        || !core
                            .task_notifications
                            .lock()
                            .expect("task inbox poisoned")
                            .store
                            .inboxes
                            .get(&request.workspace)
                            .is_some_and(|inbox| inbox.request.as_ref() == Some(&request))
                    {
                        continue;
                    }
                    if request.open {
                        core.open_task_notification(request);
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    let result = if let Some((id, _)) = &request.notification {
                        sender
                            .task_user_notification_acknowledge(
                                pioneer_protocol::TaskUserNotificationAcknowledgeParams {
                                    workspace_id: request.workspace.clone(),
                                    notification_id: id.clone(),
                                },
                            )
                            .map(|r| TaskUserNotificationListResponse {
                                notifications: vec![r.notification],
                                next_cursor: None,
                            })
                    } else {
                        sender.task_user_notification_list(
                            pioneer_protocol::TaskUserNotificationListParams {
                                workspace_id: request.workspace.clone(),
                                cursor: None,
                                limit: Some(100),
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
        let (workspace, notification) = match intent {
            TaskNotificationIntent::NativeCompletion { .. } => {
                unreachable!("handled native completion")
            }
            TaskNotificationIntent::Refresh { workspace_id }
            | TaskNotificationIntent::Retry { workspace_id } => (workspace_id, None),
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
        let Some(mut request) = owner.store.begin(identity, workspace.clone(), notification) else {
            return self.transition(&authority, vec![], vec![]);
        };
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
            inbox
                .publication
                .items
                .iter_mut()
                .for_each(|item| item.dismissing = false);
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
                    item.notification.notification_id == *notification_id
                        && item.revision == *revision
                })
                .map(|item| item.notification.clone())
        });
        let Some(notification) = notification else {
            return;
        };
        let result = (|| -> anyhow::Result<pioneer_protocol::Thread> {
            let sender = self.compatibility_runtime().ws_command_sender();
            let task: pioneer_protocol::TaskGetResponse = crate::rpc::send_json_rpc_request_typed(
                &sender,
                pioneer_protocol::constants::methods::TASK_GET,
                &pioneer_protocol::TaskGetParams {
                    task_id: notification.task_id.clone(),
                },
                crate::rpc::RPC_REQUEST_TIMEOUT,
            )?;
            let thread_id = notification_thread_target(
                &notification,
                &task.task.id,
                &task.task.workspace_id,
                &task.task_run_thread_bindings,
            )?;
            anyhow::ensure!(
                self.task_inbox_identity(&request.workspace, false).as_ref()
                    == Some(&request.identity),
                "Task notification access changed"
            );
            let thread = sender
                .thread_get(pioneer_protocol::ThreadGetParams { thread_id })?
                .thread;
            anyhow::ensure!(
                thread.workspace_id == request.workspace,
                "Task notification thread scope mismatch"
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
            Err(error) => inbox.publication.error = Some(format!("{error:#}")),
        }
        self.publish_task_inbox(&mut owner.store, &request.workspace);
        drop(owner);
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
        result: Result<TaskUserNotificationListResponse, String>,
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
        if let pioneer_protocol::GatewayNotification::TaskUserNotificationDelivered(hint) =
            notification
        {
            if self
                .current_auth()
                .is_some_and(|a| a.principal.id.as_str() == hint.recipient_principal_id)
                && self.task_inbox(&hint.workspace_id).is_some()
            {
                self.task_notification_intent(TaskNotificationIntent::Refresh {
                    workspace_id: hint.workspace_id.clone(),
                });
            }
        }
    }
}

fn notification_thread_target(
    notification: &TaskUserNotification,
    task_id: &str,
    workspace_id: &str,
    bindings: &[pioneer_protocol::TaskRunThreadBinding],
) -> anyhow::Result<String> {
    anyhow::ensure!(
        task_id == notification.task_id && workspace_id == notification.workspace_id,
        "Task notification scope mismatch"
    );
    let targets = bindings
        .iter()
        .filter(|binding| {
            binding.task_id == notification.task_id
                && binding.run_id == notification.run_id
                && binding.binding_kind
                    == pioneer_protocol::TaskRunThreadBindingKind::PrimaryExecutor
        })
        .map(|binding| binding.thread_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    anyhow::ensure!(
        targets.len() == 1,
        "Task notification thread target unavailable"
    );
    Ok(targets.into_iter().next().expect("one target").to_owned())
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
    fn notification(id: &str, workspace: &str) -> TaskUserNotification {
        serde_json::from_value(serde_json::json!({"notificationId":id,"workspaceId":workspace,"taskId":"task-a","runId":"run-a","deliveryId":"delivery-a","createdAt":10})).unwrap()
    }
    fn response(
        items: Vec<TaskUserNotification>,
    ) -> Result<TaskUserNotificationListResponse, String> {
        Ok(TaskUserNotificationListResponse {
            notifications: items,
            next_cursor: None,
        })
    }
    #[test]
    fn activation_requires_one_primary_thread_of_the_matching_task_run() {
        use pioneer_protocol::{TaskRunThreadBinding, TaskRunThreadBindingKind};
        let notification = notification("n", "a");
        let binding = TaskRunThreadBinding {
            id: "binding".into(),
            task_id: "task-a".into(),
            run_id: "run-a".into(),
            execution_id: None,
            thread_id: "thread-a".into(),
            binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
            created_at: 1,
        };
        let target = |bindings: &[TaskRunThreadBinding]| {
            notification_thread_target(&notification, "task-a", "a", bindings)
        };
        assert_eq!(target(std::slice::from_ref(&binding)).unwrap(), "thread-a");
        assert!(
            notification_thread_target(&notification, "other", "a", std::slice::from_ref(&binding))
                .is_err()
        );
        assert!(
            notification_thread_target(
                &notification,
                "task-a",
                "other",
                std::slice::from_ref(&binding)
            )
            .is_err()
        );
        let mut other = binding.clone();
        other.run_id = "old-run".into();
        assert!(target(std::slice::from_ref(&other)).is_err());
        other = binding.clone();
        other.thread_id = "thread-b".into();
        assert!(target(&[binding, other]).is_err());
        assert!(target(&[]).is_err());
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
        terminal.acknowledged_at = Some(20);
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
        acknowledged.acknowledged_at = Some(20);
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
}
