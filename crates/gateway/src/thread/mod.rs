use anyhow::{Result, anyhow, bail};
use pioneer_config::AppConfig;
use pioneer_protocol::{
    SandboxMode, SandboxPolicy, Thread, ThreadClosedNotification, ThreadMode, ThreadOriginKind,
    ThreadSidebarVisibility, ThreadStartParams, ThreadStartResponse, ThreadStartedNotification,
    ThreadStatus, ThreadUnsubscribeResponse, ThreadUnsubscribeStatus, Turn, TurnStartParams,
    TurnStartResponse, TurnStartedNotification, TurnStatus,
};
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;

use crate::helpers::unix_timestamp_secs;
use crate::session::ConnectionId;

const DEFAULT_SANDBOX_MODE: SandboxMode = SandboxMode::FullAccess;
const DEFAULT_THREAD_MODE: ThreadMode = ThreadMode::Chat;

#[derive(Debug)]
pub struct ThreadStartOutcome {
    pub response: ThreadStartResponse,
    pub started_notification: ThreadStartedNotification,
    pub started_notification_connection_ids: Vec<ConnectionId>,
}

#[derive(Debug)]
pub struct TurnStartOutcome {
    pub response: TurnStartResponse,
    pub started_notification: TurnStartedNotification,
    pub started_notification_connection_ids: Vec<ConnectionId>,
    pub materialization: TurnStartMaterialization,
    pub rollback_context: TurnStartRollbackContext,
}

#[derive(Debug, Clone)]
pub struct TurnStartMaterialization {
    pub thread: Thread,
    pub turn: pioneer_protocol::Turn,
    pub input: Vec<pioneer_protocol::UserInput>,
    pub sandbox_mode: SandboxMode,
}

#[derive(Debug, Clone)]
pub struct TurnStartRollbackContext {
    pub thread_id: String,
    pub turn_id: String,
    pub previous_preview: String,
    pub previous_status: ThreadStatus,
    pub previous_updated_at: i64,
    pub previous_mode: ThreadMode,
    pub previous_model: String,
    pub previous_model_provider: String,
    pub previous_sandbox_mode: SandboxMode,
}

#[derive(Debug, Clone)]
pub struct TurnFinishRollbackContext {
    pub thread_id: String,
    pub turn_id: String,
    pub previous_thread_status: ThreadStatus,
    pub previous_thread_updated_at: i64,
    pub previous_turn_status: TurnStatus,
    pub previous_turn_error: Option<String>,
}

#[derive(Debug)]
pub struct TurnFinishOutcome {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn: pioneer_protocol::Turn,
    pub connection_ids: Vec<ConnectionId>,
    pub rollback_context: TurnFinishRollbackContext,
}

pub struct ThreadUnsubscribeOutcome {
    pub response: ThreadUnsubscribeResponse,
    pub closed_notification: Option<ThreadClosedNotification>,
    pub closed_notification_connection_ids: Vec<ConnectionId>,
}

struct ThreadEntry {
    thread: Thread,
    sandbox_mode: SandboxMode,
    connection_ids: HashSet<ConnectionId>,
}

#[derive(Default)]
struct ThreadStateManager {
    threads: HashMap<String, ThreadEntry>,
    thread_ids_by_connection: HashMap<ConnectionId, HashSet<String>>,
}

pub struct ThreadManager {
    default_model: String,
    default_model_provider: String,
    state: RwLock<ThreadStateManager>,
}

impl ThreadManager {
    pub fn new(
        default_model: impl Into<String>,
        default_model_provider: impl Into<String>,
    ) -> Self {
        Self {
            default_model: default_model.into(),
            default_model_provider: default_model_provider.into(),
            state: RwLock::new(ThreadStateManager::default()),
        }
    }

    pub fn from_app_config(config: &AppConfig) -> Self {
        Self::new(
            config.gateway.thread.default_model.clone(),
            config.gateway.thread.default_model_provider.clone(),
        )
    }

    pub async fn thread_start(
        &self,
        connection_id: ConnectionId,
        workspace_id: String,
        params: ThreadStartParams,
    ) -> Result<ThreadStartOutcome> {
        self.thread_start_with_seed(Some(connection_id), workspace_id, params, None, None)
            .await
    }

    pub(crate) async fn thread_start_seeded(
        &self,
        connection_id: ConnectionId,
        workspace_id: String,
        params: ThreadStartParams,
        seed_thread: Option<Thread>,
        seed_sandbox_mode: Option<SandboxMode>,
    ) -> Result<ThreadStartOutcome> {
        self.thread_start_with_seed(
            Some(connection_id),
            workspace_id,
            params,
            seed_thread,
            seed_sandbox_mode,
        )
        .await
    }

    pub async fn system_thread_start_seeded(
        &self,
        workspace_id: String,
        params: ThreadStartParams,
        seed_thread: Option<Thread>,
        seed_sandbox_mode: Option<SandboxMode>,
    ) -> Result<ThreadStartOutcome> {
        self.thread_start_with_seed(None, workspace_id, params, seed_thread, seed_sandbox_mode)
            .await
    }

    async fn thread_start_with_seed(
        &self,
        connection_id: Option<ConnectionId>,
        workspace_id: String,
        params: ThreadStartParams,
        seed_thread: Option<Thread>,
        seed_sandbox_mode: Option<SandboxMode>,
    ) -> Result<ThreadStartOutcome> {
        let thread_id = params.thread_id.trim();
        if thread_id.is_empty() {
            return Err(anyhow!("`thread_id` is required for `thread/start`"));
        }
        let thread_id = thread_id.to_owned();

        let requested_model = match params.model.as_deref() {
            Some(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    bail!("`model` cannot be empty for `thread/start`");
                }
                Some(trimmed.to_owned())
            }
            None => None,
        };

        let requested_name = match params.name.as_deref() {
            Some(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    bail!("`name` cannot be empty for `thread/start`");
                }
                Some(trimmed.to_owned())
            }
            None => None,
        };

        let requested_model_provider = match params.model_provider.as_deref() {
            Some(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    bail!("`model_provider` cannot be empty for `thread/start`");
                }
                Some(trimmed.to_owned())
            }
            None => None,
        };

        let requested_mode = params.mode;
        let requested_origin_kind = params.origin_kind;
        let requested_sidebar_visibility = params.sidebar_visibility;
        let requested_agent_nickname = params.agent_nickname.clone();
        let requested_agent_role = params.agent_role.clone();

        let model = requested_model
            .clone()
            .unwrap_or_else(|| self.default_model.clone());
        let model_provider = requested_model_provider
            .clone()
            .unwrap_or_else(|| self.default_model_provider.clone());

        let sandbox_mode = params.sandbox.unwrap_or(DEFAULT_SANDBOX_MODE);
        let mode = requested_mode.unwrap_or(DEFAULT_THREAD_MODE);

        let now = unix_timestamp_secs()? as i64;
        let (thread, effective_sandbox_mode) = {
            let mut state = self.state.write().await;
            if state.threads.contains_key(thread_id.as_str()) {
                let existing_workspace_id = state
                    .threads
                    .get(thread_id.as_str())
                    .map(|entry| entry.thread.workspace_id.clone())
                    .unwrap_or_default();
                if existing_workspace_id != workspace_id {
                    bail!(
                        "thread `{thread_id}` belongs to workspace `{}`",
                        existing_workspace_id
                    );
                }

                if let Some(connection_id) = connection_id {
                    state
                        .thread_ids_by_connection
                        .entry(connection_id)
                        .or_default()
                        .insert(thread_id.clone());
                }

                let existing_entry = state
                    .threads
                    .get_mut(thread_id.as_str())
                    .expect("thread should still exist");
                if let Some(connection_id) = connection_id {
                    existing_entry.connection_ids.insert(connection_id);
                }

                if let Some(seed_thread) = seed_thread.as_ref() {
                    merge_thread_metadata(&mut existing_entry.thread, seed_thread);
                }
                if requested_name.is_some() {
                    existing_entry.thread.name = requested_name.clone();
                }

                (existing_entry.thread.clone(), existing_entry.sandbox_mode)
            } else {
                let thread = if let Some(mut seed_thread) = seed_thread {
                    if seed_thread.id != thread_id {
                        bail!(
                            "seed thread id mismatch: expected `{thread_id}`, got `{}`",
                            seed_thread.id
                        );
                    }
                    if seed_thread.workspace_id != workspace_id {
                        bail!(
                            "seed thread workspace mismatch: expected `{workspace_id}`, got `{}`",
                            seed_thread.workspace_id
                        );
                    }

                    if let Some(requested_mode) = requested_mode {
                        seed_thread.mode = requested_mode;
                    }
                    if let Some(requested_model) = requested_model.clone() {
                        seed_thread.model = requested_model;
                    }
                    if let Some(requested_model_provider) = requested_model_provider.clone() {
                        seed_thread.model_provider = requested_model_provider;
                    }
                    if let Some(requested_origin_kind) = requested_origin_kind {
                        seed_thread.origin_kind = requested_origin_kind;
                    }
                    if let Some(requested_sidebar_visibility) = requested_sidebar_visibility {
                        seed_thread.sidebar_visibility = requested_sidebar_visibility;
                    }
                    if requested_name.is_some() {
                        seed_thread.name = requested_name.clone();
                    }
                    if requested_agent_nickname.is_some() {
                        seed_thread.agent_nickname = requested_agent_nickname.clone();
                    }
                    if requested_agent_role.is_some() {
                        seed_thread.agent_role = requested_agent_role.clone();
                    }

                    seed_thread.status = ThreadStatus::Idle;
                    seed_thread.turns = Vec::new();
                    seed_thread
                } else {
                    Thread {
                        id: thread_id.clone(),
                        workspace_id,
                        name: requested_name,
                        preview: String::new(),
                        mode,
                        model: model.clone(),
                        model_provider: model_provider.clone(),
                        created_at: now,
                        updated_at: now,
                        status: ThreadStatus::Idle,
                        origin_kind: requested_origin_kind.unwrap_or(ThreadOriginKind::User),
                        sidebar_visibility: requested_sidebar_visibility
                            .unwrap_or(ThreadSidebarVisibility::Visible),
                        agent_nickname: requested_agent_nickname,
                        agent_role: requested_agent_role,
                        turns: Vec::new(),
                    }
                };

                let sandbox_mode = seed_sandbox_mode.unwrap_or(sandbox_mode);

                if let Some(connection_id) = connection_id {
                    state
                        .thread_ids_by_connection
                        .entry(connection_id)
                        .or_default()
                        .insert(thread_id.clone());
                }
                let connection_ids = connection_id
                    .map(|connection_id| HashSet::from([connection_id]))
                    .unwrap_or_default();
                state.threads.insert(
                    thread_id,
                    ThreadEntry {
                        thread: thread.clone(),
                        sandbox_mode,
                        connection_ids,
                    },
                );

                (thread, sandbox_mode)
            }
        };

        let response = ThreadStartResponse {
            thread: thread.clone(),
            sandbox: SandboxPolicy::from_mode(effective_sandbox_mode),
        };

        Ok(ThreadStartOutcome {
            response,
            started_notification: ThreadStartedNotification { thread },
            started_notification_connection_ids: connection_id.into_iter().collect(),
        })
    }

    pub async fn thread_get(&self, thread_id: &str) -> Option<Thread> {
        let state = self.state.read().await;
        state
            .threads
            .get(thread_id)
            .map(|entry| entry.thread.clone())
    }

    pub async fn sync_thread_metadata_from_persisted(&self, thread: &Thread) {
        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(thread.id.as_str()) else {
            return;
        };
        merge_thread_metadata(&mut entry.thread, thread);
    }

    pub async fn list_threads_for_workspace_visible_to(
        &self,
        workspace_id: &str,
        connection_id: Option<ConnectionId>,
    ) -> Vec<Thread> {
        let state = self.state.read().await;
        state
            .threads
            .values()
            .filter(|entry| entry.thread.workspace_id == workspace_id)
            .filter(|entry| thread_visible_to_connection(entry, connection_id))
            .map(|entry| entry.thread.clone())
            .collect()
    }

    pub async fn turn_start(
        &self,
        connection_id: ConnectionId,
        params: TurnStartParams,
    ) -> Result<TurnStartOutcome> {
        self.turn_start_for_actor(Some(connection_id), params).await
    }

    pub async fn system_turn_start(&self, params: TurnStartParams) -> Result<TurnStartOutcome> {
        self.turn_start_for_actor(None, params).await
    }

    async fn turn_start_for_actor(
        &self,
        connection_id: Option<ConnectionId>,
        params: TurnStartParams,
    ) -> Result<TurnStartOutcome> {
        let thread_id = params.thread_id.trim();
        if thread_id.is_empty() {
            bail!("`thread_id` is required for `turn/start`");
        }

        let turn_id = params.turn_id.trim();
        if turn_id.is_empty() {
            return Err(anyhow!("`turn_id` is required for `turn/start`"));
        }
        let turn_id = turn_id.to_owned();

        let requested_model = match params.model.as_deref() {
            Some(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    bail!("`model` cannot be empty for `turn/start`");
                }
                Some(trimmed.to_owned())
            }
            None => None,
        };

        let requested_model_provider = match params.model_provider.as_deref() {
            Some(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    bail!("`model_provider` cannot be empty for `turn/start`");
                }
                Some(trimmed.to_owned())
            }
            None => None,
        };

        let requested_mode = params.mode;
        let requested_sandbox_mode = params.sandbox_policy.map(|policy| policy.mode);

        let now = unix_timestamp_secs()? as i64;

        let mut state = self.state.write().await;

        let Some(entry) = state.threads.get_mut(thread_id) else {
            bail!("thread `{thread_id}` is not loaded");
        };

        if let Some(connection_id) = connection_id
            && !entry.connection_ids.contains(&connection_id)
        {
            bail!("connection `{connection_id}` is not subscribed to thread `{thread_id}`");
        }

        let has_running_turn = entry
            .thread
            .turns
            .iter()
            .any(|turn| turn.status == TurnStatus::InProgress);

        if has_running_turn {
            bail!("thread `{thread_id}` already has a running turn");
        }

        let previous_preview = entry.thread.preview.clone();
        let previous_status = entry.thread.status;
        let previous_updated_at = entry.thread.updated_at;
        let previous_mode = entry.thread.mode;
        let previous_model = entry.thread.model.clone();
        let previous_model_provider = entry.thread.model_provider.clone();
        let previous_sandbox_mode = entry.sandbox_mode;

        if let Some(mode) = requested_mode {
            entry.thread.mode = mode;
        }
        if let Some(model) = requested_model {
            entry.thread.model = model;
        }
        if let Some(model_provider) = requested_model_provider {
            entry.thread.model_provider = model_provider;
        }
        if let Some(sandbox_mode) = requested_sandbox_mode {
            entry.sandbox_mode = sandbox_mode;
        }

        let turn = pioneer_protocol::Turn {
            id: turn_id,
            status: TurnStatus::InProgress,
            error: None,
            prompt_manifest: None,
        };

        if entry.thread.preview.is_empty() {
            if let Some(text_input) = params.input.iter().find_map(|input| match input {
                pioneer_protocol::UserInput::Text { text, .. } => Some(text.trim()),
                _ => None,
            }) {
                if !text_input.is_empty() {
                    entry.thread.preview = text_input.to_owned();
                }
            }
        }

        entry.thread.turns.push(turn.clone());
        entry.thread.status = ThreadStatus::Active;
        entry.thread.updated_at = now;

        let started_notification = TurnStartedNotification {
            workspace_id: entry.thread.workspace_id.clone(),
            thread_id: thread_id.to_owned(),
            turn: turn.clone(),
        };

        let response = TurnStartResponse { turn };

        let started_notification_connection_ids = entry.connection_ids.iter().copied().collect();

        let materialization = TurnStartMaterialization {
            thread: entry.thread.clone(),
            turn: started_notification.turn.clone(),
            input: params.input.clone(),
            sandbox_mode: entry.sandbox_mode,
        };

        let rollback_context = TurnStartRollbackContext {
            thread_id: thread_id.to_owned(),
            turn_id: started_notification.turn.id.clone(),
            previous_preview,
            previous_status,
            previous_updated_at,
            previous_mode,
            previous_model,
            previous_model_provider,
            previous_sandbox_mode,
        };

        Ok(TurnStartOutcome {
            response,
            started_notification,
            started_notification_connection_ids,
            materialization,
            rollback_context,
        })
    }

    pub async fn rollback_turn_start(&self, context: TurnStartRollbackContext) {
        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(&context.thread_id) else {
            return;
        };

        entry.thread.turns.retain(|turn| turn.id != context.turn_id);
        entry.thread.preview = context.previous_preview;
        entry.thread.status = context.previous_status;
        entry.thread.updated_at = context.previous_updated_at;
        entry.thread.mode = context.previous_mode;
        entry.thread.model = context.previous_model;
        entry.thread.model_provider = context.previous_model_provider;
        entry.sandbox_mode = context.previous_sandbox_mode;
    }

    pub async fn turn_finish(
        &self,
        thread_id: &str,
        turn_id: &str,
        status: TurnStatus,
        error: Option<String>,
    ) -> Result<TurnFinishOutcome> {
        let now = unix_timestamp_secs()? as i64;
        let mut state = self.state.write().await;

        let Some(entry) = state.threads.get_mut(thread_id) else {
            bail!("thread `{thread_id}` is not loaded");
        };

        let Some(turn) = entry
            .thread
            .turns
            .iter_mut()
            .find(|turn| turn.id == turn_id)
        else {
            bail!("turn `{turn_id}` not found in thread `{thread_id}`");
        };

        if turn.status != TurnStatus::InProgress {
            bail!("turn `{turn_id}` is not in progress");
        }

        let previous_thread_status = entry.thread.status;
        let previous_thread_updated_at = entry.thread.updated_at;
        let previous_turn_status = turn.status;
        let previous_turn_error = turn.error.clone();

        turn.status = status;
        turn.error = error;
        entry.thread.status = ThreadStatus::Idle;
        entry.thread.updated_at = now;

        Ok(TurnFinishOutcome {
            thread_id: thread_id.to_owned(),
            workspace_id: entry.thread.workspace_id.clone(),
            turn: turn.clone(),
            connection_ids: entry.connection_ids.iter().copied().collect(),
            rollback_context: TurnFinishRollbackContext {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                previous_thread_status,
                previous_thread_updated_at,
                previous_turn_status,
                previous_turn_error,
            },
        })
    }

    pub async fn rollback_turn_finish(&self, context: TurnFinishRollbackContext) {
        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(&context.thread_id) else {
            return;
        };

        let Some(turn) = entry
            .thread
            .turns
            .iter_mut()
            .find(|turn| turn.id == context.turn_id)
        else {
            return;
        };

        turn.status = context.previous_turn_status;
        turn.error = context.previous_turn_error;
        entry.thread.status = context.previous_thread_status;
        entry.thread.updated_at = context.previous_thread_updated_at;
    }

    pub async fn subscribed_connection_ids(&self, thread_id: &str) -> Vec<ConnectionId> {
        let state = self.state.read().await;
        state
            .threads
            .get(thread_id)
            .map(|entry| entry.connection_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub async fn turn_get(&self, thread_id: &str, turn_id: &str) -> Option<(String, Turn)> {
        let state = self.state.read().await;
        let entry = state.threads.get(thread_id)?;
        let turn = entry.thread.turns.iter().find(|turn| turn.id == turn_id)?;
        Some((entry.thread.workspace_id.clone(), turn.clone()))
    }

    pub async fn set_turn_prompt_manifest(
        &self,
        thread_id: &str,
        turn_id: &str,
        manifest: pioneer_protocol::PromptManifest,
    ) {
        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(thread_id) else {
            return;
        };
        let Some(turn) = entry
            .thread
            .turns
            .iter_mut()
            .find(|turn| turn.id == turn_id)
        else {
            return;
        };
        turn.prompt_manifest = Some(manifest);
    }

    pub async fn unload_orphaned_thread_if_idle(&self, thread_id: &str) -> bool {
        let mut state = self.state.write().await;

        let Some(entry) = state.threads.get(thread_id) else {
            return false;
        };

        if !entry.connection_ids.is_empty() || has_in_progress_turn(&entry.thread) {
            return false;
        }

        state.threads.remove(thread_id);
        true
    }

    pub async fn connection_closed(&self, connection_id: ConnectionId) -> Vec<String> {
        let mut removed_thread_ids = Vec::new();

        let mut state = self.state.write().await;

        let thread_ids = state
            .thread_ids_by_connection
            .remove(&connection_id)
            .unwrap_or_default();

        for thread_id in thread_ids {
            let should_remove = match state.threads.get_mut(&thread_id) {
                Some(entry) => {
                    entry.connection_ids.remove(&connection_id);
                    entry.connection_ids.is_empty() && !has_in_progress_turn(&entry.thread)
                }
                None => false,
            };

            if should_remove {
                state.threads.remove(&thread_id);
                removed_thread_ids.push(thread_id);
            }
        }

        removed_thread_ids
    }

    pub async fn thread_unsubscribe(
        &self,
        connection_id: ConnectionId,
        thread_id: &str,
    ) -> ThreadUnsubscribeOutcome {
        let mut state = self.state.write().await;

        let Some(entry) = state.threads.get_mut(thread_id) else {
            return ThreadUnsubscribeOutcome {
                response: ThreadUnsubscribeResponse {
                    status: ThreadUnsubscribeStatus::NotLoaded,
                },
                closed_notification: None,
                closed_notification_connection_ids: Vec::new(),
            };
        };

        if !entry.connection_ids.remove(&connection_id) {
            return ThreadUnsubscribeOutcome {
                response: ThreadUnsubscribeResponse {
                    status: ThreadUnsubscribeStatus::NotSubscribed,
                },
                closed_notification: None,
                closed_notification_connection_ids: Vec::new(),
            };
        }

        if let Some(thread_ids) = state.thread_ids_by_connection.get_mut(&connection_id) {
            thread_ids.remove(thread_id);
            if thread_ids.is_empty() {
                state.thread_ids_by_connection.remove(&connection_id);
            }
        }

        let should_remove_thread = state.threads.get(thread_id).is_some_and(|entry| {
            entry.connection_ids.is_empty() && !has_in_progress_turn(&entry.thread)
        });

        let closed_notification = if should_remove_thread {
            let workspace_id = state
                .threads
                .get(thread_id)
                .map(|entry| entry.thread.workspace_id.clone())
                .unwrap_or_default();
            state.threads.remove(thread_id);
            Some(ThreadClosedNotification {
                workspace_id,
                thread_id: thread_id.to_owned(),
            })
        } else {
            None
        };

        ThreadUnsubscribeOutcome {
            response: ThreadUnsubscribeResponse {
                status: ThreadUnsubscribeStatus::Unsubscribed,
            },
            closed_notification,
            closed_notification_connection_ids: if should_remove_thread {
                vec![connection_id]
            } else {
                Vec::new()
            },
        }
    }
}

fn has_in_progress_turn(thread: &Thread) -> bool {
    thread
        .turns
        .iter()
        .any(|turn| turn.status == TurnStatus::InProgress)
}

fn thread_visible_to_connection(entry: &ThreadEntry, connection_id: Option<ConnectionId>) -> bool {
    if !is_empty_unnamed_draft_thread(&entry.thread) {
        return true;
    }

    if entry.connection_ids.is_empty() {
        return true;
    }

    connection_id.is_some_and(|connection_id| entry.connection_ids.contains(&connection_id))
}

fn is_empty_unnamed_draft_thread(thread: &Thread) -> bool {
    thread.status == ThreadStatus::Idle
        && thread.turns.is_empty()
        && thread.preview.trim().is_empty()
        && thread
            .name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
}

fn merge_thread_metadata(existing_thread: &mut Thread, incoming_thread: &Thread) {
    if existing_thread.id != incoming_thread.id
        || existing_thread.workspace_id != incoming_thread.workspace_id
    {
        return;
    }

    if let Some(name) = incoming_thread
        .name
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        existing_thread.name = Some(name.to_owned());
    }

    if existing_thread.preview.trim().is_empty() && !incoming_thread.preview.trim().is_empty() {
        existing_thread.preview = incoming_thread.preview.clone();
    }

    if incoming_thread.updated_at > existing_thread.updated_at {
        existing_thread.updated_at = incoming_thread.updated_at;
    }

    existing_thread.origin_kind = incoming_thread.origin_kind;
    existing_thread.sidebar_visibility = incoming_thread.sidebar_visibility;
}

#[cfg(test)]
impl ThreadManager {
    pub(crate) async fn subscribe_connection(
        &self,
        thread_id: &str,
        connection_id: ConnectionId,
    ) -> bool {
        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(thread_id) else {
            return false;
        };

        entry.connection_ids.insert(connection_id);
        state
            .thread_ids_by_connection
            .entry(connection_id)
            .or_default()
            .insert(thread_id.to_owned());
        true
    }

    pub(crate) async fn has_thread(&self, thread_id: &str) -> bool {
        self.state.read().await.threads.contains_key(thread_id)
    }
}

#[cfg(test)]
mod tests {
    use super::ThreadManager;
    use pioneer_protocol::{
        Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStartParams,
        ThreadStatus, ThreadUnsubscribeStatus, TurnStartParams, TurnStatus, UserInput,
    };

    fn start_params(thread_id: &str) -> ThreadStartParams {
        ThreadStartParams {
            thread_id: thread_id.to_owned(),
            workspace_id: "ws_000000000000000001".to_owned(),
            name: None,
            model: None,
            model_provider: None,
            sandbox: None,
            mode: None,
            origin_kind: None,
            sidebar_visibility: None,
            agent_nickname: None,
            agent_role: None,
        }
    }

    #[tokio::test]
    async fn thread_start_creates_idle_thread_and_response() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let params = ThreadStartParams {
            thread_id: "thr_000000000000000001".to_owned(),
            workspace_id: "ws_000000000000000001".to_owned(),
            name: None,
            model: Some("o3".to_owned()),
            model_provider: None,
            sandbox: None,
            mode: Some(ThreadMode::Chat),
            origin_kind: None,
            sidebar_visibility: None,
            agent_nickname: None,
            agent_role: None,
        };

        let outcome = manager
            .thread_start(1, "ws_000000000000000001".to_owned(), params)
            .await
            .expect("thread start should succeed");

        assert_eq!(outcome.response.thread.status, ThreadStatus::Idle);
        assert_eq!(outcome.response.thread.mode, ThreadMode::Chat);
        assert_eq!(
            outcome.response.thread.workspace_id,
            "ws_000000000000000001"
        );
    }

    #[tokio::test]
    async fn thread_start_uses_chat_mode_by_default() {
        let manager = ThreadManager::new("o4-mini", "openai");

        let outcome = manager
            .thread_start(
                1,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000002"),
            )
            .await
            .expect("thread start should succeed");

        assert_eq!(outcome.response.thread.mode, ThreadMode::Chat);
    }

    #[tokio::test]
    async fn thread_start_accepts_agent_mode() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let mut params = start_params("thr_000000000000000099");
        params.mode = Some(ThreadMode::Agent);

        let outcome = manager
            .thread_start(1, "ws_000000000000000001".to_owned(), params)
            .await
            .expect("thread start should succeed");

        assert_eq!(outcome.response.thread.mode, ThreadMode::Agent);
    }

    #[tokio::test]
    async fn thread_start_uses_configured_model_defaults() {
        let manager = ThreadManager::new("gpt-4.1", "custom-provider");

        let outcome = manager
            .thread_start(
                1,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000003"),
            )
            .await
            .expect("thread start should succeed");

        assert_eq!(outcome.response.thread.model, "gpt-4.1");
        assert_eq!(outcome.response.thread.model_provider, "custom-provider");
    }

    #[tokio::test]
    async fn thread_start_with_seed_preserves_updated_at_on_subscribe() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let seed_updated_at = 1_700_000_000_i64;
        let seed_thread = Thread {
            workspace_id: "ws_000000000000000001".to_owned(),
            id: "thr_000000000000000099".to_owned(),
            name: Some("seed".to_owned()),
            preview: "seed preview".to_owned(),
            mode: ThreadMode::Chat,
            model: "o4-mini".to_owned(),
            model_provider: "openai".to_owned(),
            created_at: seed_updated_at - 10,
            updated_at: seed_updated_at,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: vec![pioneer_protocol::Turn {
                id: "turn_000000000000000099".to_owned(),
                status: TurnStatus::Completed,
                error: None,
                prompt_manifest: None,
            }],
        };

        let outcome = manager
            .thread_start_seeded(
                1,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000099"),
                Some(seed_thread),
                None,
            )
            .await
            .expect("thread start with seed should succeed");

        assert_eq!(outcome.response.thread.updated_at, seed_updated_at);
    }

    #[tokio::test]
    async fn thread_start_with_seed_refreshes_existing_thread_name() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_id = "thr_000000000000000120";

        let initial = manager
            .thread_start(
                1,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id),
            )
            .await
            .expect("initial thread start should succeed");

        let mut seed_thread = initial.response.thread.clone();
        seed_thread.name = Some("Обновленный заголовок".to_owned());

        let outcome = manager
            .thread_start_seeded(
                2,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id),
                Some(seed_thread),
                None,
            )
            .await
            .expect("seeded thread start should succeed");

        assert_eq!(
            outcome.response.thread.name.as_deref(),
            Some("Обновленный заголовок")
        );
    }

    #[tokio::test]
    async fn connection_closed_removes_empty_thread_with_last_subscriber() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000004"),
            )
            .await
            .expect("thread start should succeed");

        let thread_id = outcome.response.thread.id;
        let removed = manager.connection_closed(10).await;

        assert_eq!(removed, vec![thread_id.clone()]);
        assert!(!manager.has_thread(&thread_id).await);
    }

    #[tokio::test]
    async fn connection_closed_keeps_thread_when_other_subscriber_exists() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000005"),
            )
            .await
            .expect("thread start should succeed");

        let thread_id = outcome.response.thread.id;
        assert!(manager.subscribe_connection(&thread_id, 11).await);

        let removed = manager.connection_closed(10).await;
        assert!(removed.is_empty());
        assert!(manager.has_thread(&thread_id).await);

        let removed = manager.connection_closed(11).await;
        assert_eq!(removed, vec![thread_id.clone()]);
        assert!(!manager.has_thread(&thread_id).await);
    }

    #[tokio::test]
    async fn connection_closed_keeps_thread_when_turn_is_in_progress() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000012"),
            )
            .await
            .expect("thread start should succeed");

        let thread_id = thread_outcome.response.thread.id.clone();
        let turn_id = "turn_000000000000000200".to_owned();
        manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    input: vec![UserInput::Text {
                        text: "long running".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                },
            )
            .await
            .expect("turn start should succeed");

        let removed = manager.connection_closed(10).await;
        assert!(removed.is_empty());
        assert!(manager.has_thread(&thread_id).await);

        manager
            .turn_finish(&thread_id, &turn_id, TurnStatus::Completed, None)
            .await
            .expect("turn finish should succeed");

        let unloaded = manager.unload_orphaned_thread_if_idle(&thread_id).await;
        assert!(unloaded);
        assert!(!manager.has_thread(&thread_id).await);
    }

    #[tokio::test]
    async fn thread_unsubscribe_returns_not_loaded_for_unknown_thread() {
        let manager = ThreadManager::new("o4-mini", "openai");

        let outcome = manager.thread_unsubscribe(1, "missing").await;

        assert_eq!(outcome.response.status, ThreadUnsubscribeStatus::NotLoaded);
        assert!(outcome.closed_notification.is_none());
    }

    #[tokio::test]
    async fn thread_unsubscribe_returns_not_subscribed_for_other_connection() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000006"),
            )
            .await
            .expect("thread start should succeed");
        let thread_id = outcome.response.thread.id;

        let outcome = manager.thread_unsubscribe(11, &thread_id).await;

        assert_eq!(
            outcome.response.status,
            ThreadUnsubscribeStatus::NotSubscribed
        );
        assert!(outcome.closed_notification.is_none());
        assert!(manager.has_thread(&thread_id).await);
    }

    #[tokio::test]
    async fn thread_unsubscribe_removes_empty_thread_when_last_subscriber_leaves() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000007"),
            )
            .await
            .expect("thread start should succeed");
        let thread_id = outcome.response.thread.id;

        let outcome = manager.thread_unsubscribe(10, &thread_id).await;

        assert_eq!(
            outcome.response.status,
            ThreadUnsubscribeStatus::Unsubscribed
        );
        assert_eq!(
            outcome
                .closed_notification
                .map(|value| (value.workspace_id, value.thread_id)),
            Some(("ws_000000000000000001".to_owned(), thread_id.clone()))
        );
        assert!(!manager.has_thread(&thread_id).await);
    }

    #[tokio::test]
    async fn thread_unsubscribe_keeps_thread_when_subscribers_remain() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000008"),
            )
            .await
            .expect("thread start should succeed");
        let thread_id = outcome.response.thread.id;
        assert!(manager.subscribe_connection(&thread_id, 11).await);

        let outcome = manager.thread_unsubscribe(10, &thread_id).await;

        assert_eq!(
            outcome.response.status,
            ThreadUnsubscribeStatus::Unsubscribed
        );
        assert!(outcome.closed_notification.is_none());
        assert!(manager.has_thread(&thread_id).await);
    }

    #[tokio::test]
    async fn turn_start_creates_running_turn_and_marks_thread_active() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000009"),
            )
            .await
            .expect("thread start should succeed");

        let turn_outcome = manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: thread_outcome.response.thread.id.clone(),
                    turn_id: "turn_000000000000000001".to_owned(),
                    input: vec![UserInput::Text {
                        text: "hello world".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                },
            )
            .await
            .expect("turn start should succeed");

        assert_eq!(turn_outcome.response.turn.status, TurnStatus::InProgress);
        assert_eq!(
            turn_outcome.started_notification.turn.status,
            TurnStatus::InProgress
        );
    }

    #[tokio::test]
    async fn turn_start_applies_model_and_provider_overrides_to_thread() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000010"),
            )
            .await
            .expect("thread start should succeed");

        let turn_outcome = manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: thread_outcome.response.thread.id.clone(),
                    turn_id: "turn_000000000000000010".to_owned(),
                    model: Some("o3".to_owned()),
                    model_provider: Some("custom-provider".to_owned()),
                    input: vec![UserInput::Text {
                        text: "override model".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    sandbox_policy: None,
                    mode: None,
                },
            )
            .await
            .expect("turn start should succeed");

        assert_eq!(turn_outcome.materialization.thread.model, "o3");
        assert_eq!(
            turn_outcome.materialization.thread.model_provider,
            "custom-provider"
        );
    }

    #[tokio::test]
    async fn turn_start_rejects_when_thread_missing() {
        let manager = ThreadManager::new("o4-mini", "openai");

        let error = manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: "thr_missing".to_owned(),
                    turn_id: "turn_000000000000000002".to_owned(),
                    input: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                },
            )
            .await
            .expect_err("missing thread should fail");

        assert!(
            error.to_string().contains("not loaded"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn rollback_turn_start_restores_previous_thread_state() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000011"),
            )
            .await
            .expect("thread start should succeed");

        let turn_outcome = manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: thread_outcome.response.thread.id.clone(),
                    turn_id: "turn_000000000000000099".to_owned(),
                    model: Some("o3".to_owned()),
                    model_provider: Some("custom-provider".to_owned()),
                    input: vec![UserInput::Text {
                        text: "hello world".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    sandbox_policy: None,
                    mode: None,
                },
            )
            .await
            .expect("turn start should succeed");

        assert_eq!(turn_outcome.materialization.thread.model, "o3");
        assert_eq!(
            turn_outcome.materialization.thread.model_provider,
            "custom-provider"
        );

        manager
            .rollback_turn_start(turn_outcome.rollback_context.clone())
            .await;

        let second_turn_outcome = manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: thread_outcome.response.thread.id.clone(),
                    turn_id: "turn_000000000000000100".to_owned(),
                    input: vec![UserInput::Text {
                        text: "second".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                },
            )
            .await
            .expect("second turn start should succeed");
        assert_eq!(second_turn_outcome.materialization.thread.model, "o4-mini");
        assert_eq!(
            second_turn_outcome.materialization.thread.model_provider,
            "openai"
        );

        manager
            .rollback_turn_start(second_turn_outcome.rollback_context)
            .await;

        let removed = manager.connection_closed(10).await;
        assert_eq!(removed, vec!["thr_000000000000000011".to_owned()]);
    }
}
