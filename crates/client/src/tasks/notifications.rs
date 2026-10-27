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
    ReadAll {
        workspace_id: String,
    },
    /// Native-only shared viewport evidence; shells submit TimelineDemand.
    #[serde(skip)]
    Viewed {
        params: pioneer_protocol::TaskCompletionInboxViewedParams,
    },
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
    read_all: bool,
    viewed: Option<pioneer_protocol::TaskCompletionInboxViewedParams>,
    viewed_epoch: u64,
    navigation_revision: Option<u64>,
    cursor: Option<String>,
    push_revision: Option<(String, u64)>,
    refresh_range: Option<RefreshRange>,
}
#[derive(Default)]
struct Inbox {
    push_paused: bool,
    push_revision: Option<(String, u64)>,
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
    viewed_confirmed: std::collections::HashSet<(String, String, String)>,
    viewed_epochs: HashMap<String, u64>,
    viewed_attempted: std::collections::HashSet<(String, String, String)>,
    // Transient transport failures, renewable by real visibility/domain facts.
    // These are never treated as server-negative or confirmed evidence.
    viewed_failed: std::collections::HashSet<(String, String, String)>,
}
// New servers provide the exact SQLite key; the fallback is exclusively for
// legacy servers whose entries predate the negotiated protocol.
fn compare_inbox_entries(
    a: &TaskCompletionInboxEntry,
    b: &TaskCompletionInboxEntry,
) -> std::cmp::Ordering {
    if a.completed_key.is_empty() || b.completed_key.is_empty() {
        (a.completed_at, &a.run_id).cmp(&(b.completed_at, &b.run_id))
    } else {
        (&a.completed_key, &a.run_id).cmp(&(&b.completed_key, &b.run_id))
    }
}

impl TaskNotificationStore {
    pub(crate) fn invalidate(&mut self) {
        self.inboxes.clear();
        self.native_completed.clear();
        self.viewed_confirmed.clear();
        self.viewed_epochs.clear();
        self.viewed_attempted.clear();
        self.viewed_failed.clear();
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
    fn record_viewed(&mut self, request: &Request, confirmed: Option<&[String]>) {
        if self.identity.as_ref() != Some(&request.identity)
            || self
                .inboxes
                .get(&request.workspace)
                .is_none_or(|inbox| inbox.request.as_ref() != Some(request))
        {
            return;
        }
        let Some(viewed) = &request.viewed else {
            return;
        };
        let current_epoch = self
            .viewed_epochs
            .get(&request.workspace)
            .copied()
            .unwrap_or(0)
            == request.viewed_epoch;
        let Some(confirmed) = confirmed else {
            // One failure per current real view. Unrelated source revisions
            // cannot create an RPC loop; a real revisit renews this gate.
            if current_epoch {
                for proof in viewed_proof_keys(viewed) {
                    self.viewed_failed.insert((
                        viewed.workspace_id.clone(),
                        viewed.thread_id.clone(),
                        proof,
                    ));
                }
            }
            return;
        };
        // New terminal/delivery facts and real revisits fence older responses.
        // Successful durable receipts remain valid across those epochs.
        if current_epoch {
            for proof in viewed_proof_keys(viewed) {
                self.viewed_attempted.insert((
                    viewed.workspace_id.clone(),
                    viewed.thread_id.clone(),
                    proof,
                ));
            }
        }
        for proof in confirmed {
            self.viewed_confirmed.insert((
                viewed.workspace_id.clone(),
                viewed.thread_id.clone(),
                proof.clone(),
            ));
        }
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
            read_all: false,
            viewed: None,
            viewed_epoch: 0,
            navigation_revision: None,
            cursor: None,
            push_revision: inbox.push_revision.clone(),
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
        self.complete_request(request, result.map(Some))
    }
    // None is a successful viewed operation which changed no receipts. Keep
    // the publication while still releasing ownership and deferred actions.
    fn complete_request(
        &mut self,
        request: &Request,
        result: Result<Option<TaskCompletionInboxListResponse>, String>,
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
        // The versioned stream owns mutation values. A later packet also fences
        // legacy page responses started before it; releasing the request still
        // drains actions and clears dismissing state.
        let result = if inbox.push_revision != request.push_revision {
            // Neither success nor failure of an older RPC owns newer stream data.
            Ok(None)
        } else if inbox.push_revision.is_some()
            && (request.notification.is_some() || request.read_all || request.viewed.is_some())
        {
            result.map(|_| None)
        } else {
            result
        };
        match result {
            Err(error) => {
                // A failed revalidation must never preserve a closed result.
                if request.cursor.is_none()
                    && request.notification.is_none()
                    && !request.read_all
                    && request.viewed.is_none()
                {
                    inbox.publication.items.clear();
                    inbox.publication.next_cursor = None;
                    inbox.publication.unread_count = 0;
                }
                inbox.publication.error = Some(error);
            }
            Ok(None) => {}
            Ok(Some(response)) => {
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
                    inbox
                        .publication
                        .items
                        .retain(|item| item.notification.read_at.is_none());
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
                    let mut items = merged
                        .into_values()
                        .filter(|item| item.notification.read_at.is_none())
                        .collect::<Vec<_>>();
                    items.sort_by(|a, b| compare_inbox_entries(&b.notification, &a.notification));
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
fn viewed_proof_keys(
    params: &pioneer_protocol::TaskCompletionInboxViewedParams,
) -> impl Iterator<Item = String> + '_ {
    params
        .run_ids
        .iter()
        .map(|id| format!("run:{id}"))
        .chain(params.result_turn_ids.iter().map(|id| format!("turn:{id}")))
        .chain(params.result_items.iter().map(|item| item.proof_key()))
}

/// Keep each writer quantum bounded without dropping evidence on large/zoomed
/// viewports. This is transport batching of actual viewed rows, not prefetch.
fn viewed_batches(
    params: &pioneer_protocol::TaskCompletionInboxViewedParams,
) -> impl Iterator<Item = pioneer_protocol::TaskCompletionInboxViewedParams> + '_ {
    let run_count = params.run_ids.len();
    let turn_count = params.result_turn_ids.len();
    let item_start = run_count + turn_count;
    let count = item_start + params.result_items.len();
    (0..count).step_by(100).map(move |start| {
        let end = start + 100;
        pioneer_protocol::TaskCompletionInboxViewedParams {
            workspace_id: params.workspace_id.clone(),
            thread_id: params.thread_id.clone(),
            run_ids: params.run_ids[start.min(run_count)..end.min(run_count)].to_vec(),
            result_turn_ids: params.result_turn_ids[start.saturating_sub(run_count).min(turn_count)
                ..end.saturating_sub(run_count).min(turn_count)]
                .to_vec(),
            result_items: params.result_items[start
                .saturating_sub(item_start)
                .min(params.result_items.len())
                ..end
                    .saturating_sub(item_start)
                    .min(params.result_items.len())]
                .to_vec(),
        }
    })
}
impl ClientCore {
    /// Called only after accepting changed actual visibility, outside the
    /// thread-registry lock. It renews failures, never successful evidence.
    pub(crate) fn renew_task_inbox_view(
        &self,
        params: &pioneer_protocol::TaskCompletionInboxViewedParams,
    ) {
        let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
        *owner
            .store
            .viewed_epochs
            .entry(params.workspace_id.clone())
            .or_default() += 1;
        owner.store.viewed_failed.retain(|(workspace, thread, _)| {
            workspace != &params.workspace_id || thread != &params.thread_id
        });
    }

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
        self.queue_workspace_push(workspace_id);
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
            let needs_bootstrap = {
                let owner = self.task_notifications.lock().expect("task inbox poisoned");
                owner.store.subscriptions.contains_key(workspace_id)
                    && !owner.store.inboxes.contains_key(workspace_id)
            };
            if needs_bootstrap {
                self.task_notification_intent(TaskNotificationIntent::Refresh {
                    workspace_id: workspace_id.clone(),
                });
            }
            self.queue_workspace_push(workspace_id);
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
        drop(owner);
        self.queue_workspace_push(workspace_id);
    }
    pub(crate) fn start_task_notification_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<Request>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-task-inbox".into())
            .spawn(move || {
                while let Ok(mut request) = receiver.recv() {
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
                    let result = core.execute_task_inbox_request(&mut request);
                    core.complete_task_inbox(request, result.map_err(|e| format!("{e:#}")));
                }
            })
            .expect("task inbox worker could not start");
        let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
    fn execute_task_inbox_request(
        &self,
        request: &mut Request,
    ) -> anyhow::Result<Option<TaskCompletionInboxListResponse>> {
        let sender = self.transport_runtime().ws_command_sender();
        if request.read_all {
            return crate::rpc::send_json_rpc_request_typed(
                &sender,
                pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_READ_ALL,
                &pioneer_protocol::TaskCompletionInboxListParams {
                    workspace_id: request.workspace.clone(),
                    cursor: None,
                    limit: Some(30),
                },
                crate::rpc::RPC_REQUEST_TIMEOUT,
            )
            .map(Some);
        }
        if let Some(viewed) = &request.viewed {
            let result: anyhow::Result<Vec<String>> = crate::rpc::send_json_rpc_request_typed(
                &sender,
                pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
                viewed,
                crate::rpc::RPC_REQUEST_TIMEOUT,
            );
            if self.task_inbox_request_is_current(request) {
                let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
                owner
                    .store
                    .record_viewed(request, result.as_ref().ok().map(Vec::as_slice));
            }
            if result?.is_empty() || self.workspace_push_inbox_active(&request.workspace) {
                return Ok(None);
            }
        }
        if let Some((id, _)) = &request.notification {
            return inbox_entry_rpc(&sender, &request.workspace, id, true).map(|response| {
                Some(TaskCompletionInboxListResponse {
                    entries: vec![response.entry],
                    unread_count: response.unread_count,
                    next_cursor: None,
                })
            });
        }
        self.pause_workspace_push_for_page(&request.workspace);
        // Pause fences ingress first. Capture the actual RPC baseline afterwards,
        // including packets accepted while this page waited in the worker queue.
        // Revalidate exact ownership before changing either copy of the request.
        {
            let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
            anyhow::ensure!(
                owner.store.identity.as_ref() == Some(&request.identity),
                "Stale task inbox identity"
            );
            let inbox = owner
                .store
                .inboxes
                .get_mut(&request.workspace)
                .ok_or_else(|| anyhow::anyhow!("Stale task inbox request"))?;
            anyhow::ensure!(
                inbox.request.as_ref() == Some(&*request),
                "Stale task inbox request"
            );
            request.push_revision = inbox.push_revision.clone();
            inbox.request = Some(request.clone());
        }
        load_task_inbox_pages(
            request,
            || self.task_inbox_request_is_current(request),
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
        .map(Some)
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
                    (request.notification.is_some() && !request.open)
                        || request.read_all
                        || request.viewed.is_some(),
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
        if let TaskNotificationIntent::Viewed { params } = &intent {
            if params.run_ids.len() + params.result_turn_ids.len() + params.result_items.len() > 100
            {
                let mut transition =
                    self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
                for batch in viewed_batches(params) {
                    transition = self
                        .task_notification_intent(TaskNotificationIntent::Viewed { params: batch });
                }
                return transition;
            }
        }
        if let TaskNotificationIntent::Retry { workspace_id } = &intent {
            {
                let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
                owner
                    .store
                    .viewed_attempted
                    .retain(|(workspace, _, _)| workspace != workspace_id);
                owner
                    .store
                    .viewed_failed
                    .retain(|(workspace, _, _)| workspace != workspace_id);
                *owner
                    .store
                    .viewed_epochs
                    .entry(workspace_id.clone())
                    .or_default() += 1;
            }
            self.refresh_viewed_task_inbox(workspace_id);
            return self.task_notification_intent(TaskNotificationIntent::Refresh {
                workspace_id: workspace_id.clone(),
            });
        }
        let read_all = matches!(&intent, TaskNotificationIntent::ReadAll { .. });
        let mut viewed = if let TaskNotificationIntent::Viewed { params } = &intent {
            Some(params.clone())
        } else {
            None
        };
        let mut deferred_intent = intent.clone();
        let open = matches!(&intent, TaskNotificationIntent::Open { .. });
        let load_more = matches!(&intent, TaskNotificationIntent::LoadMore { .. });
        let (workspace, notification) = match intent {
            TaskNotificationIntent::NativeCompletion { .. } => {
                unreachable!("handled native completion")
            }
            TaskNotificationIntent::ReadAll { workspace_id }
            | TaskNotificationIntent::Refresh { workspace_id }
            | TaskNotificationIntent::Retry { workspace_id }
            | TaskNotificationIntent::LoadMore { workspace_id } => (workspace_id, None),
            TaskNotificationIntent::Viewed { params, .. } => (params.workspace_id, None),
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
        let Some(identity) = self.task_inbox_identity(
            &workspace,
            (notification.is_some() && !open) || read_all || viewed.is_some(),
        ) else {
            return self.transition(&authority, vec![], vec![]);
        };
        let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
        if self.is_stopped() {
            return self.transition(&authority, vec![], vec![]);
        }
        if owner.store.identity.as_ref() != Some(&identity) {
            owner.store.invalidate();
            owner.store.identity = Some(identity.clone());
        }
        if let Some(params) = &mut viewed {
            let already = |proof: String| {
                owner.store.viewed_confirmed.contains(&(
                    workspace.clone(),
                    params.thread_id.clone(),
                    proof.clone(),
                )) || owner.store.viewed_attempted.contains(&(
                    workspace.clone(),
                    params.thread_id.clone(),
                    proof.clone(),
                )) || owner.store.viewed_failed.contains(&(
                    workspace.clone(),
                    params.thread_id.clone(),
                    proof,
                ))
            };
            params.run_ids.retain(|id| !already(format!("run:{id}")));
            params
                .result_turn_ids
                .retain(|id| !already(format!("turn:{id}")));
            params
                .result_items
                .retain(|item| !already(item.proof_key()));
            if params.run_ids.is_empty()
                && params.result_turn_ids.is_empty()
                && params.result_items.is_empty()
            {
                return self.transition(&authority, vec![], vec![]);
            }
            deferred_intent = TaskNotificationIntent::Viewed {
                params: params.clone(),
            };
        }
        if notification.is_some() || read_all || viewed.is_some() || load_more {
            if let Some(inbox) = owner.store.inboxes.get_mut(&workspace) {
                if inbox.request.is_some() {
                    if read_all
                        && inbox
                            .request
                            .as_ref()
                            .is_some_and(|request| request.read_all)
                    {
                        return self.transition(&authority, vec![], vec![]);
                    }
                    // Repeated demand for an in-flight page is already satisfied.
                    if load_more
                        && inbox
                            .request
                            .as_ref()
                            .is_some_and(|request| request.cursor.is_some())
                    {
                        return self.transition(&authority, vec![], vec![]);
                    }
                    if inbox.pending_actions.len() < 100
                        && !inbox.pending_actions.contains(&deferred_intent)
                    {
                        inbox.pending_actions.push_back(deferred_intent);
                    }
                    if read_all || load_more {
                        inbox.publication.loading = true;
                        return self.publish_task_inbox(&mut owner.store, &workspace);
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
        request.read_all = read_all;
        request.viewed = viewed;
        request.viewed_epoch = owner
            .store
            .viewed_epochs
            .get(&workspace)
            .copied()
            .unwrap_or(0);
        if read_all {
            request.refresh_range = None;
        }
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
            // A push may have removed or revised the queued card. Finalize only
            // this request and drain deferred actions without stale navigation.
            self.complete_task_inbox(request, Ok(None));
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
        inbox.publication.loading = false;
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
            Err(_) if inbox.push_revision != request.push_revision => {
                // This Open RPC no longer owns the current stream's cards or
                // total. Release its request, retaining newer authority.
            }
            Err(error) => {
                inbox.publication.items.clear();
                inbox.publication.unread_count = 0;
                inbox.publication.next_cursor = None;
                inbox.publication.error = Some(format!("{error:#}"));
            }
        }
        self.publish_task_inbox(&mut owner.store, &request.workspace);
        drop(owner);
        self.continue_task_inbox_actions(&request.workspace);
    }
    fn continue_task_inbox_actions(&self, workspace: &str) {
        // Skip stale deferred actions without stranding a later valid action.
        // A started request owns the remainder until its matching completion.
        loop {
            let (next, resume_push) = {
                let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
                let Some(inbox) = owner.store.inboxes.get_mut(workspace) else {
                    return;
                };
                if inbox.request.is_some() {
                    return;
                }
                let next = inbox.pending_actions.pop_front().or_else(|| {
                    std::mem::take(&mut inbox.refresh_again).then(|| {
                        TaskNotificationIntent::Refresh {
                            workspace_id: workspace.to_owned(),
                        }
                    })
                });
                (next, inbox.push_paused)
            };
            let Some(intent) = next else {
                // A workspace wake may have been consumed while this request
                // blocked a paused scope. Both normal and Open completion drain
                // here; retain the obligation until the last deferred action
                // releases it, without requeuing while an RPC is still active.
                if resume_push {
                    self.queue_workspace_push(workspace);
                }
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
        let drafts = self
            .task_inbox_publication_draft(store, workspace)
            .into_iter()
            .collect();
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![])
    }
    pub(crate) fn task_inbox_publication_draft(
        &self,
        store: &mut TaskNotificationStore,
        workspace: &str,
    ) -> Option<crate::core::ClientPublicationDraft> {
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
            return None;
        }
        // Scope revisions survive protected publication eviction.
        next.revision = self
            .snapshot(&scope)
            .map_or(1, |p| p.revisions().scoped().get() + 1);
        Some(authority.publication(
            scope,
            crate::threads::registry::revisions(next.revision),
            Arc::new(next.clone()),
        ))
    }
    fn complete_task_inbox(
        &self,
        request: Request,
        result: Result<Option<TaskCompletionInboxListResponse>, String>,
    ) {
        if self
            .task_inbox_identity(
                &request.workspace,
                (request.notification.is_some() && !request.open)
                    || request.read_all
                    || request.viewed.is_some(),
            )
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
            let Some(refresh) = owner.store.complete_request(&request, result) else {
                return;
            };
            if let Some(inbox) = owner.store.inboxes.get_mut(&request.workspace) {
                inbox.refresh_again |= refresh;
            }
            self.publish_task_inbox(&mut owner.store, &request.workspace);
        }
        self.continue_task_inbox_actions(&request.workspace);
        if request.notification.is_none() && !request.read_all && request.viewed.is_none() {
            self.queue_workspace_push(&request.workspace);
        }
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
            TaskDeliveryDelivered(h) => Some(h.context.workspace_id.as_str()),
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
        if let Some(workspace) = workspace {
            if self.task_inbox(workspace).is_some() && !self.workspace_push_inbox_active(workspace)
            {
                self.task_notification_intent(TaskNotificationIntent::Refresh {
                    workspace_id: workspace.to_owned(),
                });
            }
            if matches!(
                notification,
                TaskRunCompleted(_)
                    | TaskRunFailed(_)
                    | TaskRunBlocked(_)
                    | TaskRunCancelled(_)
                    | TaskCompleted(_)
                    | TaskFailed(_)
                    | TaskBlocked(_)
                    | TaskCancelled(_)
                    | TaskDeliveryDelivered(_)
            ) {
                let mut owner = self.task_notifications.lock().expect("task inbox poisoned");
                *owner
                    .store
                    .viewed_epochs
                    .entry(workspace.to_owned())
                    .or_default() += 1;
                owner
                    .store
                    .viewed_attempted
                    .retain(|(w, _, _)| w != workspace);
                owner.store.viewed_failed.retain(|(w, _, _)| w != workspace);
                drop(owner);
                // Completion and destination-link commits can follow the visible
                // final row. Retry only geometry still owned by an active viewport.
                self.refresh_viewed_task_inbox(workspace);
            }
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

impl ClientCore {
    pub(crate) fn inbox_push_demand(
        &self,
        workspace: &str,
    ) -> Option<(Vec<String>, Option<String>)> {
        self.task_inbox_identity(workspace, false)?;
        let owner = self.task_notifications.lock().expect("task inbox poisoned");
        if !owner.store.subscriptions.contains_key(workspace) {
            return None;
        }
        let inbox = owner.store.inboxes.get(workspace)?;
        if (inbox.request.is_some() && inbox.push_revision.is_none())
            || inbox.publication.error.is_some()
        {
            return None;
        }
        Some((
            inbox
                .publication
                .items
                .iter()
                .map(|i| i.notification.run_id.clone())
                .collect(),
            inbox.publication.next_cursor.clone(),
        ))
    }
    pub(crate) fn pause_inbox_push_epoch(store: &mut TaskNotificationStore, workspace: &str) {
        if let Some(inbox) = store.inboxes.get_mut(workspace) {
            inbox.push_paused = true;
        }
    }
    pub(crate) fn inbox_push_page_pending(&self, workspace: &str) -> bool {
        let owner = self.task_notifications.lock().expect("task inbox poisoned");
        owner
            .store
            .inboxes
            .get(workspace)
            .is_some_and(|inbox| inbox.push_paused && inbox.request.is_some())
    }
    pub(crate) fn set_inbox_push_epoch(
        store: &mut TaskNotificationStore,
        workspace: &str,
        epoch: Option<&str>,
    ) {
        if let Some(inbox) = store.inboxes.get_mut(workspace) {
            inbox.push_revision = epoch.map(|epoch| (epoch.to_owned(), 0));
            inbox.push_paused = false;
        }
    }
    pub(crate) fn validate_inbox_push(
        store: &TaskNotificationStore,
        packet: &pioneer_protocol::WorkspacePushNotification,
    ) -> bool {
        let Some(patch) = &packet.inbox else {
            return true;
        };
        let Some(inbox) = store.inboxes.get(&packet.workspace_id) else {
            return false;
        };
        store.subscriptions.contains_key(&packet.workspace_id)
            && inbox
                .push_revision
                .as_ref()
                .is_some_and(|(epoch, _)| epoch == &packet.epoch)
            && patch.upserts.len() <= 4096
            && patch.removes.len() <= 4096
            && patch.upserts.iter().all(|entry| {
                entry.workspace_id == packet.workspace_id
                    && entry.read_at.is_none()
                    && !entry.completed_key.is_empty()
            })
            && inbox
                .publication
                .next_cursor
                .as_deref()
                .is_none_or(|cursor| serde_json::from_str::<(String, String)>(cursor).is_ok())
            && patch
                .next_cursor
                .as_deref()
                .is_none_or(|cursor| serde_json::from_str::<(String, String)>(cursor).is_ok())
    }
    pub(crate) fn apply_inbox_push(
        &self,
        store: &mut TaskNotificationStore,
        packet: &pioneer_protocol::WorkspacePushNotification,
    ) -> bool {
        let Some(patch) = &packet.inbox else {
            return true;
        };
        let Some(inbox) = store.inboxes.get_mut(&packet.workspace_id) else {
            return false;
        };
        if inbox
            .push_revision
            .as_ref()
            .is_none_or(|(epoch, _)| epoch != &packet.epoch)
            || patch.upserts.len() > 4096
            || patch.upserts.iter().any(|entry| {
                entry.workspace_id != packet.workspace_id
                    || entry.read_at.is_some()
                    || entry.completed_key.is_empty()
            })
        {
            return false;
        }
        let frontier: Option<(String, String)> = match inbox
            .publication
            .next_cursor
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
        {
            Ok(value) => value,
            Err(_) => return false,
        };
        // Keep the previously loaded keyset range when a new completion arrives.
        // Only the explicit retained-prefix cap may evict its oldest rows.
        let budget = 4096;
        store.generation += 1;
        let generation = store.generation;
        let inbox = store
            .inboxes
            .get_mut(&packet.workspace_id)
            .expect("inbox exists");
        let old = std::mem::take(&mut inbox.publication.items)
            .into_iter()
            .map(|item| (item.notification.run_id.clone(), item))
            .collect::<HashMap<_, _>>();
        let replace = packet.snapshot || patch.replace;
        let mut next = if replace { HashMap::new() } else { old.clone() };
        for id in &patch.removes {
            next.remove(id);
        }
        for entry in &patch.upserts {
            let key = (entry.completed_key.clone(), entry.run_id.clone());
            if !replace
                && !next.contains_key(&entry.run_id)
                && frontier.as_ref().is_some_and(|frontier| &key < frontier)
            {
                continue;
            }
            let previous = old.get(&entry.run_id);
            let revision = previous
                .filter(|item| item.notification == *entry)
                .map_or(generation, |item| item.revision);
            next.insert(
                entry.run_id.clone(),
                TaskInboxItem {
                    notification: entry.clone(),
                    revision,
                    dismissing: previous.is_some_and(|item| item.dismissing),
                },
            );
        }
        let mut items = next.into_values().collect::<Vec<_>>();
        items.sort_by(|a, b| compare_inbox_entries(&b.notification, &a.notification));
        if replace {
            inbox.publication.next_cursor = patch.next_cursor.clone();
        } else if items.len() > budget {
            items.truncate(budget);
            inbox.publication.next_cursor = items.last().and_then(|item| {
                serde_json::to_string(&(
                    item.notification.completed_key.as_str(),
                    item.notification.run_id.as_str(),
                ))
                .ok()
            });
        }
        inbox.publication.items = items;
        inbox.publication.unread_count = patch.unread_count;
        inbox.publication.error = None;
        inbox.push_revision = Some((packet.epoch.clone(), packet.revision));
        let actionable = inbox
            .publication
            .items
            .iter()
            .filter_map(TaskInboxItem::native_effect)
            .collect::<std::collections::HashSet<_>>();
        store.native_completed.retain(|effect| {
            effect.workspace_id != packet.workspace_id || actionable.contains(effect)
        });
        true
    }
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
    fn new_completion_keeps_the_loaded_page_frontier() {
        let core = ClientCore::new();
        let mut store = TaskNotificationStore::default();
        let entries = (0..30)
            .map(|index| {
                let mut entry = notification(&format!("run-{index:02}"), "a");
                entry.completed_key = format!("2026-10-22 09:00:{index:02}.000000");
                entry
            })
            .collect::<Vec<_>>();
        let frontier =
            serde_json::to_string(&(&entries[0].completed_key, &entries[0].run_id)).unwrap();
        let request = store.begin(identity("p"), "a".into(), None).unwrap();
        store.complete(
            &request,
            Ok(TaskCompletionInboxListResponse {
                entries: entries.clone(),
                unread_count: 50,
                next_cursor: Some(frontier.clone()),
            }),
        );
        store.subscriptions.insert("a".into(), 1);
        ClientCore::set_inbox_push_epoch(&mut store, "a", Some("epoch"));
        let mut newest = notification("newest", "a");
        newest.completed_key = "2026-10-22 09:01:00.000000".into();
        let mut packet = pioneer_protocol::WorkspacePushNotification {
            workspace_id: "a".into(),
            epoch: "epoch".into(),
            revision: 1,
            snapshot: true,
            resync_required: false,
            thread_summaries: vec![],
            indicators: vec![],
            inbox: Some(pioneer_protocol::TaskInboxPatch {
                replace: false,
                upserts: entries,
                removes: vec![],
                unread_count: 50,
                next_cursor: Some(frontier.clone()),
            }),
        };
        assert!(core.apply_inbox_push(&mut store, &packet));
        packet.revision = 2;
        packet.snapshot = false;
        packet.inbox = Some(pioneer_protocol::TaskInboxPatch {
            replace: false,
            upserts: vec![newest],
            removes: vec![],
            unread_count: 51,
            next_cursor: None,
        });
        assert!(core.apply_inbox_push(&mut store, &packet));
        core.publish_task_inbox(&mut store, "a");
        let inbox = core.task_inbox("a").unwrap();
        assert_eq!(inbox.items().len(), 31);
        assert_eq!(
            inbox.items().last().unwrap().notification().run_id,
            "run-00"
        );
        assert_eq!(inbox.next_cursor(), Some(frontier.as_str()));
    }

    #[test]
    fn inbox_batch_preserves_exact_frontier_and_new_values_against_late_read_ack() {
        let core = ClientCore::new();
        let mut store = TaskNotificationStore::default();
        let mut r2 = notification("r2", "a");
        r2.completed_key = "2026-10-22 09:00:00.900000".into();
        let mut r1 = notification("r1", "a");
        r1.completed_key = "2026-10-22 09:00:00.100000".into();
        let frontier = serde_json::to_string(&(r1.completed_key.as_str(), "r1")).unwrap();
        let initial = store.begin(identity("p"), "a".into(), None).unwrap();
        store.complete(
            &initial,
            Ok(TaskCompletionInboxListResponse {
                entries: vec![r2.clone(), r1.clone()],
                unread_count: 2,
                next_cursor: Some(frontier.clone()),
            }),
        );
        store.subscriptions.insert("a".into(), 1);
        ClientCore::set_inbox_push_epoch(&mut store, "a", Some("epoch"));
        let mut packet = pioneer_protocol::WorkspacePushNotification {
            workspace_id: "a".into(),
            epoch: "epoch".into(),
            revision: 1,
            snapshot: true,
            resync_required: false,
            thread_summaries: vec![],
            indicators: vec![],
            inbox: Some(pioneer_protocol::TaskInboxPatch {
                replace: false,
                upserts: vec![r2.clone(), r1],
                removes: vec![],
                unread_count: 2,
                next_cursor: Some(frontier.clone()),
            }),
        };
        assert!(ClientCore::validate_inbox_push(&store, &packet));
        assert!(core.apply_inbox_push(&mut store, &packet));
        core.publish_task_inbox(&mut store, "a");
        let before = core.task_inbox("a").unwrap();
        let r2_revision = before
            .items()
            .iter()
            .find(|i| i.notification().run_id == "r2")
            .unwrap()
            .revision();
        let r1_revision = before
            .items()
            .iter()
            .find(|i| i.notification().run_id == "r1")
            .unwrap()
            .revision();
        let read = store
            .begin(identity("p"), "a".into(), Some(("r2".into(), r2_revision)))
            .unwrap();
        let mut newest = notification("r3", "a");
        newest.completed_key = "2026-10-22 09:00:00.950000".into();
        let mut outside = notification("r0", "a");
        outside.completed_key = "2026-10-22 09:00:00.050000".into();
        packet.revision = 2;
        packet.snapshot = false;
        packet.inbox = Some(pioneer_protocol::TaskInboxPatch {
            replace: false,
            upserts: vec![outside, newest],
            removes: vec!["r2".into()],
            unread_count: 8,
            next_cursor: None,
        });
        assert!(ClientCore::validate_inbox_push(&store, &packet));
        assert!(core.apply_inbox_push(&mut store, &packet));
        core.publish_task_inbox(&mut store, "a");
        let after = core.task_inbox("a").unwrap();
        assert_eq!(
            after
                .items()
                .iter()
                .map(|i| i.notification().run_id.as_str())
                .collect::<Vec<_>>(),
            ["r3", "r1"]
        );
        assert_eq!(after.items()[1].revision(), r1_revision);
        assert_eq!(after.next_cursor(), Some(frontier.as_str()));
        assert_eq!(after.unread_count(), 8);
        // Delayed mutation reply carries a stale absolute total. The stream's
        // newer result remains authoritative, including the removed identity.
        r2.read_at = Some(11);
        store.complete(
            &read,
            Ok(TaskCompletionInboxListResponse {
                entries: vec![r2],
                unread_count: 999,
                next_cursor: None,
            }),
        );
        core.publish_task_inbox(&mut store, "a");
        let final_state = core.task_inbox("a").unwrap();
        assert_eq!(final_state.unread_count(), 8);
        assert_eq!(
            final_state
                .items()
                .iter()
                .map(|i| i.notification().run_id.as_str())
                .collect::<Vec<_>>(),
            ["r3", "r1"]
        );
        packet.revision = 3;
        packet.inbox = Some(pioneer_protocol::TaskInboxPatch {
            replace: true,
            upserts: vec![],
            removes: vec![],
            unread_count: 0,
            next_cursor: None,
        });
        assert!(!packet.snapshot);
        assert!(ClientCore::validate_inbox_push(&store, &packet));
        assert!(core.apply_inbox_push(&mut store, &packet));
        core.publish_task_inbox(&mut store, "a");
        let read_all = core.task_inbox("a").unwrap();
        assert!(read_all.items().is_empty());
        assert_eq!(read_all.unread_count(), 0);
        assert_eq!(read_all.next_cursor(), None);
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
        assert!(store.inboxes["a"].publication.items.is_empty());
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
        assert_eq!(publication.items().len(), 2);
        assert_eq!(publication.unread_count(), 86);
        assert!(
            publication
                .items()
                .iter()
                .all(|item| item.notification.run_id != "r2")
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
                    .filter(|entry| entry.read_at.is_none())
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
        assert_eq!(inbox.items.len(), 60);
        assert_eq!(inbox.unread_count, 99);
        assert_eq!(inbox.next_cursor, Some(history_cursor("r039")));
        assert!(
            inbox
                .items
                .iter()
                .all(|item| item.notification.run_id != "r050")
        );
        for previous in after_read {
            assert!(
                inbox.items.iter().any(|item| item == &previous),
                "unchanged cards retain revisions"
            );
        }
        let more = history_request(&mut store, true);
        let (response, _) = history_response(&more, &source);
        assert_eq!(response.entries[0].run_id, "r038");
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
        assert_eq!(inbox.next_cursor, Some(history_cursor("r038")));
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
                .all(|item| item.notification.run_id != "r050")
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
        assert_eq!(response.entries[0].run_id, "r037");
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
            (8..=100)
                .filter(|ix| *ix != 40 && *ix != 55 && *ix != 50)
                .all(|ix| ids.contains(&format!("r{ix:03}")))
        );
    }
    #[test]
    fn large_viewport_evidence_is_batched_without_losing_runs_or_result_items() {
        let params = pioneer_protocol::TaskCompletionInboxViewedParams {
            workspace_id: "a".into(),
            thread_id: "thread".into(),
            run_ids: (0..110).map(|ix| format!("run-{ix}")).collect(),
            result_turn_ids: (0..12).map(|ix| format!("turn-{ix}")).collect(),
            result_items: (0..9)
                .map(|ix| pioneer_protocol::TaskCompletionInboxViewedResult {
                    turn_id: format!("result-{ix}"),
                    item_id: format!("item-{ix}"),
                })
                .collect(),
        };
        let batches = viewed_batches(&params).collect::<Vec<_>>();
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| batch.run_ids.len()
            + batch.result_turn_ids.len()
            + batch.result_items.len()
            <= 100));
        assert_eq!(
            batches
                .iter()
                .flat_map(|batch| batch.run_ids.clone())
                .collect::<Vec<_>>(),
            params.run_ids
        );
        assert_eq!(
            batches
                .iter()
                .flat_map(|batch| batch.result_turn_ids.clone())
                .collect::<Vec<_>>(),
            params.result_turn_ids
        );
        assert_eq!(
            batches
                .iter()
                .flat_map(|batch| batch.result_items.clone())
                .collect::<Vec<_>>(),
            params.result_items
        );
    }

    struct WorkerTransport {
        requests: mpsc::Sender<(String, crate::rpc::JsonRpcResponseSender)>,
    }
    impl crate::rpc::JsonRpcRequestTransport for WorkerTransport {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            self.requests
                .send((request["method"].as_str().unwrap().into(), response))
                .map_err(|e| e.to_string())
        }
    }
    fn worker_connection(core: &ClientCore, generation: u64) {
        use pioneer_protocol::*;
        core.transport_runtime()
            .ws_command_sender()
            .set_http_session_access_for_test(Some(crate::transport::http::GatewayHttpAccess {
                gateway_base_url: GatewayBaseUrl::parse_presentation("https://example.test")
                    .unwrap(),
                gateway_id: GatewayId::new("GAAAAAAAAAAAAAAAAAAAA").unwrap(),
                session_id: AuthSessionId::new("SAAAAAAAAAAAAAAAAAAAA").unwrap(),
                generation,
                access_expires_at_unix: 1000,
                access_token: AuthSecretString::new("synthetic-token"),
            }));
    }
    fn worker_fixture() -> (
        Arc<ClientCore>,
        mpsc::Receiver<(String, crate::rpc::JsonRpcResponseSender)>,
    ) {
        let (core, receiver) = worker_fixture_before_start();
        core.start_task_notification_controller();
        (core, receiver)
    }
    fn worker_fixture_before_start() -> (
        Arc<ClientCore>,
        mpsc::Receiver<(String, crate::rpc::JsonRpcResponseSender)>,
    ) {
        use pioneer_protocol::*;
        let core = crate::catalog_test_support::settings_client();
        let mut snapshot = core.authorization_snapshot(None, None).unwrap();
        let resources = AuthorizationOperationalResourceProjection {
            fingerprint: "inbox-resources".into(),
            ..Default::default()
        };
        snapshot.workspace = Some(AuthorizationWorkspaceCapabilitySnapshot {
            workspace_id: "a".into(),
            capabilities: AuthorizationWorkspaceCapabilities {
                can_read_own_notifications: true,
                can_acknowledge_own_notifications: true,
                ..Default::default()
            },
            operational_resources: resources.clone(),
            execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                fingerprint: "inbox-test".into(),
                resources,
                permission_options: vec![],
                can_attach_artifacts: false,
                mcp_invocation_limits: Default::default(),
            },
        });
        let (generation, connection) = core.current_auth_ticket();
        assert_eq!(
            core.accept_authorization_projection(generation, connection, snapshot),
            crate::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        worker_connection(&core, 7);
        crate::catalog_test_support::replace_connection(&core, 7);
        assert!(core.task_inbox_identity("a", true).is_some());
        let (requests, receiver) = mpsc::channel();
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(Arc::new(WorkerTransport { requests }));
        (core, receiver)
    }
    struct InboxRegistrationTransport {
        core: std::sync::Weak<ClientCore>,
        delegate: Arc<dyn crate::rpc::JsonRpcRequestTransport + Send + Sync>,
    }
    impl crate::rpc::JsonRpcRequestTransport for InboxRegistrationTransport {
        fn send_json_rpc_request(
            &self,
            id: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let wire: serde_json::Value = serde_json::from_str(&payload).unwrap();
            if wire["method"] != pioneer_protocol::constants::methods::WORKSPACE_PUSH {
                return self.delegate.send_json_rpc_request(id, payload, response);
            }
            let params: pioneer_protocol::WorkspacePushParams =
                serde_json::from_value(wire["params"].clone()).unwrap();
            if params.inbox {
                let mut entry = notification("run", "a");
                entry.completed_key = "2026-10-22 09:00:00.000000".into();
                let cursor = serde_json::to_string(&(&entry.completed_key, &entry.run_id)).unwrap();
                worker_ingress(
                    &self.core.upgrade().unwrap(),
                    serde_json::json!({
                        "workspaceId":"a", "epoch":params.epoch, "revision":1,
                        "snapshot":true, "resyncRequired":false, "indicators":[],
                        "inbox":{"replace":true,"upserts":[entry],"removes":[],"unreadCount":8,"nextCursor":cursor}
                    }),
                );
            }
            response
                .send(Ok(serde_json::json!({"supported":true})))
                .unwrap();
            Ok(())
        }
    }
    fn worker_ingress(core: &ClientCore, params: serde_json::Value) {
        let wire = pioneer_protocol::JsonRpcNotification::from_params(
            pioneer_protocol::constants::events::WORKSPACE_PUSH_CHANGED,
            &params,
        )
        .unwrap();
        let event = pioneer_protocol::GatewayNotification::from_jsonrpc(wire).unwrap();
        assert!(core.observe_workspace_notification(&event));
    }
    fn worker_push(
        core: &ClientCore,
        epoch: &str,
        revision: u64,
        entries: Vec<TaskCompletionInboxEntry>,
        removes: Vec<String>,
    ) {
        worker_ingress(
            core,
            serde_json::json!({
                "workspaceId":"a", "epoch":epoch, "revision":revision,
                "snapshot":false, "resyncRequired":false, "indicators":[],
                "inbox":{"upserts":entries,"removes":removes,"unreadCount":8}
            }),
        );
    }
    fn worker_seed_push(core: &Arc<ClientCore>) -> (crate::core::ClientSubscription, String) {
        let identity = core.task_inbox_identity("a", true).unwrap();
        let mut owner = core.task_notifications.lock().unwrap();
        let request = owner.store.begin(identity, "a".into(), None).unwrap();
        let mut entry = notification("run", "a");
        entry.completed_key = "2026-10-22 09:00:00.000000".into();
        let cursor = serde_json::to_string(&(&entry.completed_key, &entry.run_id)).unwrap();
        owner.store.complete(
            &request,
            Ok(TaskCompletionInboxListResponse {
                entries: vec![entry],
                unread_count: 8,
                next_cursor: Some(cursor),
            }),
        );
        drop(owner);
        let binding = core.subscribe(
            ClientScope::TaskInbox {
                workspace_id: "a".into(),
            },
            std::num::NonZeroUsize::new(8).unwrap(),
        );
        let sender = core.transport_runtime().ws_command_sender();
        let requests = &sender.test_requests;
        let delegate = requests.lock().unwrap().take().unwrap();
        *requests.lock().unwrap() = Some(Arc::new(InboxRegistrationTransport {
            core: Arc::downgrade(core),
            delegate,
        }));
        core.reconcile_workspace_push("a");
        assert!(core.workspace_push_inbox_active("a"));
        let epoch = core.task_notifications.lock().unwrap().store.inboxes["a"]
            .push_revision
            .as_ref()
            .unwrap()
            .0
            .clone();
        (binding, epoch)
    }
    #[test]
    fn worker_stale_open_releases_request_and_runs_next_action() {
        for (remove, late_error) in [(true, false), (false, false), (true, true), (false, true)] {
            let (core, requests) = worker_fixture_before_start();
            let (_binding, epoch) = worker_seed_push(&core);
            let navigation = core.navigation_snapshot();
            let identity = core.task_inbox_identity("a", false).unwrap();
            let request = {
                // Plan the queued operation before starting the real worker:
                // push can deterministically win before its transport turn.
                let mut owner = core.task_notifications.lock().unwrap();
                let revision = owner.store.inboxes["a"].publication.items[0].revision;
                let mut request = owner
                    .store
                    .begin(identity, "a".into(), Some(("run".into(), revision)))
                    .unwrap();
                request.open = true;
                let inbox = owner.store.inboxes.get_mut("a").unwrap();
                inbox.request = Some(request.clone());
                inbox
                    .pending_actions
                    .push_back(TaskNotificationIntent::ReadAll {
                        workspace_id: "a".into(),
                    });
                request
            };
            let change = || {
                let mut changed = notification("run", "a");
                changed.completed_key = "2026-10-22 09:00:00.000000".into();
                changed.summary = Some("changed while Open queued".into());
                worker_push(
                    &core,
                    &epoch,
                    2,
                    if remove { vec![] } else { vec![changed] },
                    if remove { vec!["run".into()] } else { vec![] },
                );
            };
            if !late_error {
                change();
            }
            core.start_task_notification_controller();
            core.task_notifications
                .lock()
                .unwrap()
                .sender
                .as_ref()
                .unwrap()
                .try_send(request)
                .unwrap();
            if late_error {
                let response = worker_request(
                    &requests,
                    pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_GET,
                );
                change();
                response
                    .send(Err(crate::rpc::JsonRpcResponseError::transport(
                        "stale Open failure",
                    )))
                    .unwrap();
            }
            worker_request(
                &requests,
                pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_READ_ALL,
            )
            .send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
                entries: vec![],
                unread_count: 0,
                next_cursor: None,
            })
            .unwrap()))
            .unwrap();
            wait_worker_idle(&core);
            let inbox = core.task_inbox("a").unwrap();
            assert!(!inbox.is_loading());
            assert!(inbox.items().iter().all(|item| !item.dismissing));
            assert!(inbox.opened.is_none());
            assert!(inbox.error().is_none());
            assert_eq!(inbox.unread_count(), 8);
            assert_eq!(inbox.items().len(), if remove { 0 } else { 1 });
            assert_eq!(core.navigation_snapshot(), navigation);
            core.shutdown();
        }
    }
    #[test]
    fn worker_page_handoff_keeps_queued_push_and_accepts_requested_frontier() {
        let (core, requests) = worker_fixture_before_start();
        let (_binding, epoch) = worker_seed_push(&core);
        let identity = core.task_inbox_identity("a", false).unwrap();
        let request = {
            let mut owner = core.task_notifications.lock().unwrap();
            let cursor = owner.store.inboxes["a"].publication.next_cursor.clone();
            let mut request = owner.store.begin(identity, "a".into(), None).unwrap();
            request.cursor = cursor;
            request.refresh_range = None;
            owner.store.inboxes.get_mut("a").unwrap().request = Some(request.clone());
            request
        };
        let mut newest = notification("new", "a");
        newest.completed_key = "2026-10-22 09:01:00.000000".into();
        worker_push(&core, &epoch, 2, vec![newest], vec![]);
        core.start_task_notification_controller();
        core.task_notifications
            .lock()
            .unwrap()
            .sender
            .as_ref()
            .unwrap()
            .try_send(request)
            .unwrap();
        let response = worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_LIST,
        );
        assert!(!core.workspace_push_inbox_active("a"));
        // Actual ingress of the retired epoch cannot race the page RPC.
        worker_push(&core, &epoch, 3, vec![], vec!["new".into()]);
        assert!(
            core.task_inbox("a")
                .unwrap()
                .items()
                .iter()
                .any(|item| item.notification.run_id == "new")
        );
        let mut older = notification("older", "a");
        older.completed_key = "2026-10-22 08:59:00.000000".into();
        let frontier = serde_json::to_string(&(&older.completed_key, &older.run_id)).unwrap();
        response
            .send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
                entries: vec![older],
                unread_count: 8,
                next_cursor: Some(frontier.clone()),
            })
            .unwrap()))
            .unwrap();
        wait_worker_idle(&core);
        let inbox = core.task_inbox("a").unwrap();
        assert_eq!(
            inbox
                .items()
                .iter()
                .map(|item| item.notification.run_id.as_str())
                .collect::<Vec<_>>(),
            vec!["new", "run", "older"]
        );
        assert_eq!(inbox.next_cursor(), Some(frontier.as_str()));
        core.shutdown();
    }
    #[test]
    fn worker_late_mutation_error_does_not_erase_new_authority() {
        let (core, requests) = worker_fixture();
        let (_binding, epoch) = worker_seed_push(&core);
        let revision = core.task_inbox("a").unwrap().items()[0].revision;
        core.task_notification_intent(TaskNotificationIntent::Dismiss {
            workspace_id: "a".into(),
            notification_id: "run".into(),
            revision,
        });
        let response = worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_READ,
        );
        {
            // A real accepted packet supersedes the mutation's RPC baseline.
            let mut newest = notification("new", "a");
            newest.completed_key = "2026-10-22 09:01:00.000000".into();
            worker_push(&core, &epoch, 2, vec![newest], vec![]);
        }
        response
            .send(Err(crate::rpc::JsonRpcResponseError::transport(
                "stale transport failure",
            )))
            .unwrap();
        wait_worker_idle(&core);
        let inbox = core.task_inbox("a").unwrap();
        assert_eq!(inbox.items().len(), 2);
        assert_eq!(inbox.unread_count(), 8);
        assert!(inbox.error().is_none());
        core.shutdown();
    }
    #[test]
    fn visible_inbox_reloads_after_suspend_without_shell_refresh() {
        let (core, requests) = worker_fixture();
        let scope = ClientScope::TaskInbox {
            workspace_id: "a".into(),
        };
        let _binding = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(8).unwrap());
        core.task_notification_intent(TaskNotificationIntent::Refresh {
            workspace_id: "a".into(),
        });
        worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_LIST,
        )
        .send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
            entries: vec![notification("old", "a")],
            unread_count: 1,
            next_cursor: None,
        })
        .unwrap()))
        .unwrap();
        wait_worker_idle(&core);
        assert_eq!(
            core.task_inbox("a").unwrap().items()[0]
                .notification()
                .run_id,
            "old"
        );
        core.task_inbox_demand_changed(&scope, crate::core::ClientDemand::Suspended);
        assert!(core.task_inbox("a").is_none());
        core.task_inbox_demand_changed(&scope, crate::core::ClientDemand::Visible);
        worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_LIST,
        )
        .send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
            entries: vec![notification("new", "a")],
            unread_count: 1,
            next_cursor: None,
        })
        .unwrap()))
        .unwrap();
        wait_worker_idle(&core);
        assert_eq!(
            core.task_inbox("a").unwrap().items()[0]
                .notification()
                .run_id,
            "new"
        );
        core.shutdown();
    }
    fn worker_view_params() -> pioneer_protocol::TaskCompletionInboxViewedParams {
        pioneer_protocol::TaskCompletionInboxViewedParams {
            workspace_id: "a".into(),
            thread_id: "thread".into(),
            run_ids: vec!["run".into()],
            result_turn_ids: vec![],
            result_items: vec![],
        }
    }
    fn real_view(core: &ClientCore) {
        // The same renewal used by the production validated viewport planner.
        let params = worker_view_params();
        core.renew_task_inbox_view(&params);
        core.task_notification_intent(TaskNotificationIntent::Viewed { params });
    }
    fn worker_request(
        receiver: &mpsc::Receiver<(String, crate::rpc::JsonRpcResponseSender)>,
        expected: &str,
    ) -> crate::rpc::JsonRpcResponseSender {
        let (method, response) = receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("inbox worker request");
        assert_eq!(method, expected);
        response
    }
    fn finish_worker_page(receiver: &mpsc::Receiver<(String, crate::rpc::JsonRpcResponseSender)>) {
        worker_request(
            receiver,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_LIST,
        )
        .send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
            entries: vec![],
            unread_count: 1,
            next_cursor: None,
        })
        .unwrap()))
        .unwrap();
    }
    fn wait_worker_idle(core: &ClientCore) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if core
                .task_notifications
                .lock()
                .unwrap()
                .store
                .inboxes
                .get("a")
                .is_some_and(|i| i.request.is_none())
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "inbox worker did not finish"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn worker_viewed_rpc_error_allows_real_revisit_after_successful_refresh() {
        for failure in [
            crate::rpc::JsonRpcResponseError::transport("temporary failure"),
            crate::rpc::JsonRpcResponseError::server(
                Some(-32603),
                "temporary server failure",
                None,
            ),
        ] {
            let (core, requests) = worker_fixture();
            real_view(&core);
            worker_request(
                &requests,
                pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
            )
            .send(Err(failure))
            .unwrap();
            wait_worker_idle(&core);
            assert!(core.task_inbox("a").unwrap().error().is_some());
            assert!(
                core.task_notifications
                    .lock()
                    .unwrap()
                    .store
                    .viewed_attempted
                    .is_empty()
            );
            // Source revisions/renders with the same actually visible rows
            // do not renew the error gate, even after a successful refresh.
            for _ in 0..3 {
                core.task_notification_intent(TaskNotificationIntent::Viewed {
                    params: worker_view_params(),
                });
            }
            assert!(
                requests
                    .recv_timeout(std::time::Duration::from_millis(30))
                    .is_err(),
                "no automatic retry loop"
            );
            core.task_notification_intent(TaskNotificationIntent::Refresh {
                workspace_id: "a".into(),
            });
            finish_worker_page(&requests);
            wait_worker_idle(&core);
            core.task_notification_intent(TaskNotificationIntent::Viewed {
                params: worker_view_params(),
            });
            assert!(
                requests
                    .recv_timeout(std::time::Duration::from_millis(30))
                    .is_err(),
                "ordinary refresh cannot spin failed view RPCs"
            );
            real_view(&core);
            worker_request(
                &requests,
                pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
            )
            .send(Ok(serde_json::json!(["run:run"])))
            .unwrap();
            finish_worker_page(&requests);
            wait_worker_idle(&core);
            real_view(&core);
            assert!(
                requests
                    .recv_timeout(std::time::Duration::from_millis(30))
                    .is_err(),
                "successful proof is deduplicated"
            );
            core.shutdown();
        }
    }

    #[test]
    fn worker_negative_view_keeps_prefix_and_continues_independent_refresh() {
        let (core, requests) = worker_fixture();
        core.task_notification_intent(TaskNotificationIntent::Refresh {
            workspace_id: "a".into(),
        });
        worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_LIST,
        )
        .send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
            entries: vec![notification("existing", "a")],
            unread_count: 1,
            next_cursor: None,
        })
        .unwrap()))
        .unwrap();
        wait_worker_idle(&core);
        let before = core.task_inbox("a").unwrap();
        real_view(&core);
        let response = worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
        );
        // A genuinely independent mutation arrives while viewed owns the worker.
        core.observe_task_notification(
            &pioneer_protocol::GatewayNotification::TaskCompletionInboxChanged(
                pioneer_protocol::TaskCompletionInboxChangedNotification {
                    workspace_id: "a".into(),
                },
            ),
        );
        response.send(Ok(serde_json::json!([]))).unwrap();
        let refresh = worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_LIST,
        );
        let retained = core.task_inbox("a").unwrap();
        assert_eq!(retained.items(), before.items());
        assert_eq!(retained.unread_count(), 1);
        refresh
            .send(Ok(serde_json::to_value(TaskCompletionInboxListResponse {
                entries: vec![],
                unread_count: 0,
                next_cursor: None,
            })
            .unwrap()))
            .unwrap();
        wait_worker_idle(&core);
        assert_eq!(core.task_inbox("a").unwrap().unread_count(), 0);
        assert!(
            requests
                .recv_timeout(std::time::Duration::from_millis(30))
                .is_err(),
            "only the independent refresh, no negative-view list/count"
        );
        // Neither the cached count=0 nor a previous negative proof may suppress
        // a new concrete result proof.
        let mut next = worker_view_params();
        next.run_ids = vec!["late-run".into()];
        core.renew_task_inbox_view(&next);
        core.task_notification_intent(TaskNotificationIntent::Viewed { params: next });
        worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
        )
        .send(Ok(serde_json::json!(["run:late-run"])))
        .unwrap();
        finish_worker_page(&requests);
        wait_worker_idle(&core);
        assert!(
            requests
                .recv_timeout(std::time::Duration::from_millis(30))
                .is_err()
        );
        core.shutdown();
    }

    #[test]
    fn worker_positive_view_reloads_once_and_retains_independent_pending_hint() {
        for independent_hint in [false, true] {
            let (core, requests) = worker_fixture();
            real_view(&core);
            let response = worker_request(
                &requests,
                pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
            );
            if independent_hint {
                core.observe_task_notification(
                    &pioneer_protocol::GatewayNotification::TaskCompletionInboxChanged(
                        pioneer_protocol::TaskCompletionInboxChangedNotification {
                            workspace_id: "a".into(),
                        },
                    ),
                );
            }
            // Gateway skips only this successful viewed's own source hint.
            response.send(Ok(serde_json::json!(["run:run"]))).unwrap();
            finish_worker_page(&requests);
            if independent_hint {
                finish_worker_page(&requests);
            }
            wait_worker_idle(&core);
            assert!(
                requests
                    .recv_timeout(std::time::Duration::from_millis(30))
                    .is_err()
            );
            real_view(&core);
            assert!(
                requests
                    .recv_timeout(std::time::Duration::from_millis(30))
                    .is_err(),
                "successful confirmation remains deduplicated"
            );
            core.shutdown();
        }
    }

    #[test]
    fn worker_successful_negative_evidence_is_distinct_from_rpc_error() {
        let (core, requests) = worker_fixture();
        real_view(&core);
        worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
        )
        .send(Ok(serde_json::json!([])))
        .unwrap();
        wait_worker_idle(&core);
        assert!(
            !core
                .task_notifications
                .lock()
                .unwrap()
                .store
                .viewed_attempted
                .is_empty()
        );
        core.task_notification_intent(TaskNotificationIntent::Refresh {
            workspace_id: "a".into(),
        });
        finish_worker_page(&requests);
        wait_worker_idle(&core);
        real_view(&core);
        assert!(
            requests
                .recv_timeout(std::time::Duration::from_millis(30))
                .is_err(),
            "negative evidence waits for a new domain epoch"
        );
        core.shutdown();
    }

    #[test]
    fn worker_viewed_confirmation_cannot_cross_connection_identity() {
        let (core, requests) = worker_fixture();
        real_view(&core);
        let old_response = worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
        );
        worker_connection(&core, 8);
        crate::catalog_test_support::replace_connection(&core, 8);
        // A new real view resets the operation owner and queues its own request.
        real_view(&core);
        old_response
            .send(Ok(serde_json::json!(["run:run"])))
            .unwrap();
        worker_request(
            &requests,
            pioneer_protocol::constants::methods::TASK_COMPLETION_INBOX_VIEWED,
        )
        .send(Ok(serde_json::json!(["run:run"])))
        .unwrap();
        finish_worker_page(&requests);
        wait_worker_idle(&core);
        assert_eq!(
            core.task_notifications
                .lock()
                .unwrap()
                .store
                .viewed_confirmed
                .len(),
            1
        );
        core.shutdown();
    }

    #[test]
    fn late_terminal_fact_and_identity_change_cannot_poison_viewport_dedupe() {
        let mut store = TaskNotificationStore::default();
        let mut request = store.begin(identity("p"), "a".into(), None).unwrap();
        request.viewed = Some(pioneer_protocol::TaskCompletionInboxViewedParams {
            workspace_id: "a".into(),
            thread_id: "thread".into(),
            run_ids: vec!["run".into()],
            result_turn_ids: vec![],
            result_items: vec![],
        });
        store.inboxes.get_mut("a").unwrap().request = Some(request.clone());
        store.viewed_epochs.insert("a".into(), 1);
        store.record_viewed(&request, None);
        assert!(
            store.viewed_failed.is_empty(),
            "an old failure cannot fence a renewed view"
        );
        store.record_viewed(&request, Some(&[]));
        assert!(
            store.viewed_attempted.is_empty(),
            "old negative response cannot suppress retry after a terminal commit"
        );
        store.record_viewed(&request, Some(&["run:run".into()]));
        assert!(
            store
                .viewed_confirmed
                .contains(&("a".into(), "thread".into(), "run:run".into()))
        );
        store.invalidate();
        store.begin(identity("other"), "a".into(), None).unwrap();
        store.record_viewed(&request, Some(&["run:run".into()]));
        assert!(
            store.viewed_confirmed.is_empty(),
            "a retired session cannot populate another principal's dedupe"
        );
    }

    #[test]
    fn page_and_bulk_read_errors_keep_the_visible_prefix_count_and_retry_cursor() {
        let source = history_entries(99);
        let mut store = two_history_pages(&source);
        let before = store.inboxes["a"].publication.clone();
        let request = history_request(&mut store, true);
        assert_eq!(
            store.complete(&request, Err("page unavailable".into())),
            Some(false)
        );
        let failed = &store.inboxes["a"].publication;
        assert_eq!(failed.items, before.items);
        assert_eq!(failed.next_cursor, before.next_cursor);
        assert_eq!(failed.unread_count, before.unread_count);
        assert!(failed.error.is_some());
        let retry = history_request(&mut store, true);
        assert_eq!(retry.cursor, request.cursor);
        let (response, _) = history_response(&retry, &source);
        store.complete(&retry, Ok(response));
        assert_eq!(store.inboxes["a"].publication.items.len(), 90);
        let before = store.inboxes["a"].publication.clone();
        let mut bulk = history_request(&mut store, false);
        bulk.read_all = true;
        store.inboxes.get_mut("a").unwrap().request = Some(bulk.clone());
        store.complete(&bulk, Err("bulk unavailable".into()));
        assert_eq!(store.inboxes["a"].publication.items, before.items);
        assert_eq!(
            store.inboxes["a"].publication.unread_count,
            before.unread_count
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
