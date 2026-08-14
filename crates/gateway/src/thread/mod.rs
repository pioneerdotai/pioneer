use anyhow::{Context, Result, anyhow, bail};
use pioneer_config::AppConfig;
use pioneer_protocol::{
    AuthSessionId, PrincipalId, SandboxMode, SandboxPolicy, Thread, ThreadClosedNotification,
    ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStartParams, ThreadStartResponse,
    ThreadStartedNotification, ThreadStatus, ThreadUnsubscribeResponse, ThreadUnsubscribeStatus,
    Turn, TurnKind, TurnStartParams, TurnStartResponse, TurnStartedNotification, TurnStatus,
    UserInput,
};
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;

use crate::helpers::unix_timestamp_secs;
use crate::session::ConnectionId;

const DEFAULT_SANDBOX_MODE: SandboxMode = SandboxMode::FullAccess;
const DEFAULT_THREAD_MODE: ThreadMode = ThreadMode::Message;

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
pub struct CompletedMessageTurnStartOutcome {
    pub response: TurnStartResponse,
    pub started_notification: TurnStartedNotification,
    pub completed_notification: pioneer_protocol::TurnCompletedNotification,
    pub notification_connection_ids: Vec<ConnectionId>,
    pub materialization: TurnStartMaterialization,
    final_thread: Thread,
}

#[derive(Debug, Clone)]
pub struct TurnStartMaterialization {
    pub thread: Thread,
    pub turn: pioneer_protocol::Turn,
    pub input: Vec<pioneer_protocol::UserInput>,
    pub capabilities: Vec<pioneer_protocol::TurnCapability>,
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
    pub previous_reasoning_effort: Option<String>,
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
    pub rollback_context: TurnFinishRollbackContext,
}

pub struct ThreadUnsubscribeOutcome {
    pub response: ThreadUnsubscribeResponse,
    pub closed_notification: Option<ThreadClosedNotification>,
    pub(crate) closed_notification_subscribers: Vec<ThreadSubscriber>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ThreadSubscriptionIdentity {
    pub(crate) principal_id: PrincipalId,
    pub(crate) session_id: AuthSessionId,
}

impl ThreadSubscriptionIdentity {
    pub(crate) fn new(principal_id: PrincipalId, session_id: AuthSessionId) -> Self {
        Self {
            principal_id,
            session_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ThreadSubscriber {
    pub(crate) connection_id: ConnectionId,
    pub(crate) identity: ThreadSubscriptionIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeDraftAccess {
    workspace_id: String,
    thread_id: String,
    visibility: Option<pioneer_protocol::ThreadVisibility>,
    owner: ThreadSubscriber,
}

impl RuntimeDraftAccess {
    pub(crate) fn workspace_id(&self) -> &str {
        self.workspace_id.as_str()
    }

    pub(crate) fn thread_id(&self) -> &str {
        self.thread_id.as_str()
    }

    pub(crate) const fn visibility(&self) -> Option<pioneer_protocol::ThreadVisibility> {
        self.visibility
    }

    pub(crate) fn owner(&self) -> &ThreadSubscriber {
        &self.owner
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ThreadEntryLifecycle {
    RuntimeDraft { owner: ThreadSubscriber },
    Durable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ThreadStartKind {
    RuntimeDraft,
    Durable,
}

struct ThreadEntry {
    thread: Thread,
    sandbox_mode: SandboxMode,
    subscribers: HashMap<ConnectionId, ThreadSubscriptionIdentity>,
    lifecycle: ThreadEntryLifecycle,
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

    /// Validates and prepares a new user-addressable thread without publishing
    /// it into either the in-memory subscription graph or durable storage.
    /// The caller selects the runtime-draft or immediately durable lifecycle.
    pub(crate) fn prepare_new_user_thread(
        &self,
        workspace_id: String,
        params: &ThreadStartParams,
    ) -> Result<(Thread, SandboxMode)> {
        let thread_id = params.thread_id.trim();
        if thread_id.is_empty() {
            bail!("`thread_id` is required for `thread/start`");
        }
        let workspace_id = workspace_id.trim();
        if workspace_id.is_empty() {
            bail!("`workspace_id` is required for `thread/start`");
        }
        if matches!(
            params.origin_kind,
            Some(ThreadOriginKind::TaskRun | ThreadOriginKind::System)
        ) || matches!(
            params.sidebar_visibility,
            Some(ThreadSidebarVisibility::Hidden)
        ) {
            bail!("internal thread attributes are not accepted by `thread/start`");
        }

        let trimmed_optional =
            |field: &'static str, value: Option<&str>| -> Result<Option<String>> {
                value
                    .map(|value| {
                        let value = value.trim();
                        if value.is_empty() {
                            bail!("`{field}` cannot be empty for `thread/start`");
                        }
                        Ok(value.to_owned())
                    })
                    .transpose()
            };
        let name = trimmed_optional("name", params.name.as_deref())?;
        let model = trimmed_optional("model", params.model.as_deref())?
            .unwrap_or_else(|| self.default_model.clone());
        let model_provider = trimmed_optional("model_provider", params.model_provider.as_deref())?
            .unwrap_or_else(|| self.default_model_provider.clone());
        let now = unix_timestamp_secs()? as i64;
        let sandbox_mode = params.sandbox.unwrap_or(DEFAULT_SANDBOX_MODE);

        Ok((
            Thread {
                id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                name,
                preview: String::new(),
                mode: params.mode.unwrap_or(DEFAULT_THREAD_MODE),
                model,
                model_provider,
                reasoning_effort: None,
                created_at: now,
                updated_at: now,
                status: ThreadStatus::Idle,
                origin_kind: params.origin_kind.unwrap_or(ThreadOriginKind::User),
                sidebar_visibility: ThreadSidebarVisibility::Visible,
                agent_nickname: params.agent_nickname.clone(),
                agent_role: params.agent_role.clone(),
                visibility: None,
                turns: Vec::new(),
            },
            sandbox_mode,
        ))
    }

    #[cfg(test)]
    pub async fn thread_start(
        &self,
        connection_id: ConnectionId,
        workspace_id: String,
        params: ThreadStartParams,
    ) -> Result<ThreadStartOutcome> {
        self.thread_start_authenticated(
            connection_id,
            test_subscription_identity(connection_id),
            workspace_id,
            params,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn thread_start_authenticated(
        &self,
        connection_id: ConnectionId,
        identity: ThreadSubscriptionIdentity,
        workspace_id: String,
        params: ThreadStartParams,
    ) -> Result<ThreadStartOutcome> {
        self.thread_start_draft_authenticated(
            connection_id,
            identity,
            workspace_id,
            params,
            None,
            None,
        )
        .await
    }

    pub(crate) async fn thread_start_draft_authenticated(
        &self,
        connection_id: ConnectionId,
        identity: ThreadSubscriptionIdentity,
        workspace_id: String,
        params: ThreadStartParams,
        seed_thread: Option<Thread>,
        seed_sandbox_mode: Option<SandboxMode>,
    ) -> Result<ThreadStartOutcome> {
        self.thread_start_with_seed(
            Some(ThreadSubscriber {
                connection_id,
                identity,
            }),
            workspace_id,
            params,
            seed_thread,
            seed_sandbox_mode,
            ThreadStartKind::RuntimeDraft,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn thread_start_seeded(
        &self,
        connection_id: ConnectionId,
        workspace_id: String,
        params: ThreadStartParams,
        seed_thread: Option<Thread>,
        seed_sandbox_mode: Option<SandboxMode>,
    ) -> Result<ThreadStartOutcome> {
        self.thread_start_seeded_authenticated(
            connection_id,
            test_subscription_identity(connection_id),
            workspace_id,
            params,
            seed_thread,
            seed_sandbox_mode,
        )
        .await
    }

    pub(crate) async fn thread_start_seeded_authenticated(
        &self,
        connection_id: ConnectionId,
        identity: ThreadSubscriptionIdentity,
        workspace_id: String,
        params: ThreadStartParams,
        seed_thread: Option<Thread>,
        seed_sandbox_mode: Option<SandboxMode>,
    ) -> Result<ThreadStartOutcome> {
        self.thread_start_with_seed(
            Some(ThreadSubscriber {
                connection_id,
                identity,
            }),
            workspace_id,
            params,
            seed_thread,
            seed_sandbox_mode,
            ThreadStartKind::Durable,
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
        self.thread_start_with_seed(
            None,
            workspace_id,
            params,
            seed_thread,
            seed_sandbox_mode,
            ThreadStartKind::Durable,
        )
        .await
    }

    /// Restores persisted thread state for background lifecycle processing.
    /// Unlike `system_thread_start_seeded`, this preserves existing turns and
    /// status because the caller is resuming projection of an already-running
    /// turn rather than starting a new thread session.
    pub async fn system_thread_restore_persisted(
        &self,
        mut thread: Thread,
        sandbox_mode: Option<SandboxMode>,
    ) -> Result<()> {
        let thread_id = thread.id.clone();
        let workspace_id = thread.workspace_id.clone();
        let mut state = self.state.write().await;
        if let Some(entry) = state.threads.get_mut(thread_id.as_str()) {
            if entry.thread.workspace_id != workspace_id {
                bail!(
                    "thread `{thread_id}` belongs to workspace `{}`",
                    entry.thread.workspace_id
                );
            }
            merge_thread_metadata(&mut entry.thread, &thread);
            for turn in thread.turns {
                if !entry
                    .thread
                    .turns
                    .iter()
                    .any(|existing| existing.id == turn.id)
                {
                    entry.thread.turns.push(turn);
                }
            }
            entry.thread.status = foreground_thread_status(&entry.thread);
            entry.lifecycle = ThreadEntryLifecycle::Durable;
            return Ok(());
        }

        thread.status = foreground_thread_status(&thread);
        state.threads.insert(
            thread_id,
            ThreadEntry {
                thread,
                sandbox_mode: sandbox_mode.unwrap_or(DEFAULT_SANDBOX_MODE),
                subscribers: HashMap::new(),
                lifecycle: ThreadEntryLifecycle::Durable,
            },
        );
        Ok(())
    }

    async fn thread_start_with_seed(
        &self,
        subscriber: Option<ThreadSubscriber>,
        workspace_id: String,
        params: ThreadStartParams,
        seed_thread: Option<Thread>,
        seed_sandbox_mode: Option<SandboxMode>,
        start_kind: ThreadStartKind,
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

                let promote_to_durable = match (
                    &state
                        .threads
                        .get(thread_id.as_str())
                        .expect("thread should still exist")
                        .lifecycle,
                    start_kind,
                    subscriber.as_ref(),
                ) {
                    (
                        ThreadEntryLifecycle::RuntimeDraft { owner },
                        ThreadStartKind::RuntimeDraft,
                        Some(candidate),
                    ) if owner == candidate => false,
                    (
                        ThreadEntryLifecycle::RuntimeDraft { .. },
                        ThreadStartKind::RuntimeDraft,
                        _,
                    ) => bail!("thread `{thread_id}` is owned by another connection"),
                    (ThreadEntryLifecycle::RuntimeDraft { .. }, ThreadStartKind::Durable, _) => {
                        true
                    }
                    (ThreadEntryLifecycle::Durable, ThreadStartKind::RuntimeDraft, _) => {
                        bail!("thread `{thread_id}` is already durable")
                    }
                    (ThreadEntryLifecycle::Durable, ThreadStartKind::Durable, _) => false,
                };
                if let Some(subscriber) = subscriber.as_ref() {
                    state
                        .thread_ids_by_connection
                        .entry(subscriber.connection_id)
                        .or_default()
                        .insert(thread_id.clone());
                }

                let existing_entry = state
                    .threads
                    .get_mut(thread_id.as_str())
                    .expect("thread should still exist");
                if promote_to_durable {
                    existing_entry.lifecycle = ThreadEntryLifecycle::Durable;
                }
                if let Some(subscriber) = subscriber.as_ref() {
                    existing_entry
                        .subscribers
                        .insert(subscriber.connection_id, subscriber.identity.clone());
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
                        reasoning_effort: None,
                        created_at: now,
                        updated_at: now,
                        status: ThreadStatus::Idle,
                        origin_kind: requested_origin_kind.unwrap_or(ThreadOriginKind::User),
                        sidebar_visibility: requested_sidebar_visibility
                            .unwrap_or(ThreadSidebarVisibility::Visible),
                        agent_nickname: requested_agent_nickname,
                        agent_role: requested_agent_role,
                        visibility: None,
                        turns: Vec::new(),
                    }
                };

                let sandbox_mode = seed_sandbox_mode.unwrap_or(sandbox_mode);

                if let Some(subscriber) = subscriber.as_ref() {
                    state
                        .thread_ids_by_connection
                        .entry(subscriber.connection_id)
                        .or_default()
                        .insert(thread_id.clone());
                }
                let subscribers = subscriber
                    .as_ref()
                    .map(|subscriber| {
                        HashMap::from([(subscriber.connection_id, subscriber.identity.clone())])
                    })
                    .unwrap_or_default();
                let lifecycle = match start_kind {
                    ThreadStartKind::RuntimeDraft => ThreadEntryLifecycle::RuntimeDraft {
                        owner: subscriber.as_ref().cloned().ok_or_else(|| {
                            anyhow!("runtime draft requires an authenticated owner")
                        })?,
                    },
                    ThreadStartKind::Durable => ThreadEntryLifecycle::Durable,
                };
                state.threads.insert(
                    thread_id,
                    ThreadEntry {
                        thread: thread.clone(),
                        sandbox_mode,
                        subscribers,
                        lifecycle,
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
            started_notification_connection_ids: subscriber
                .into_iter()
                .map(|subscriber| subscriber.connection_id)
                .collect(),
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

    /// Applies the committed mutable fields of one Message Turn without
    /// replacing unrelated foreground execution state held by the loaded
    /// thread. The persisted snapshot remains authoritative for preview and
    /// the mutated Turn itself.
    pub async fn sync_message_mutation_from_persisted(
        &self,
        persisted_thread: &Thread,
        turn_id: &str,
    ) -> Result<()> {
        let persisted_turn = persisted_thread
            .turns
            .iter()
            .find(|turn| turn.id == turn_id)
            .filter(|turn| turn.mode == ThreadMode::Message && turn.status == TurnStatus::Completed)
            .context("persisted Message mutation snapshot is unavailable")?
            .clone();

        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(persisted_thread.id.as_str()) else {
            return Ok(());
        };
        if entry.thread.workspace_id != persisted_thread.workspace_id {
            bail!("persisted Message mutation workspace differs from loaded thread");
        }
        let loaded_turn = entry
            .thread
            .turns
            .iter_mut()
            .find(|turn| turn.id == turn_id)
            .context("persisted Message mutation target is missing from loaded thread")?;
        *loaded_turn = persisted_turn;
        entry.thread.preview = persisted_thread.preview.clone();
        entry.thread.updated_at = entry.thread.updated_at.max(persisted_thread.updated_at);
        Ok(())
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
            .filter(|entry| match &entry.lifecycle {
                ThreadEntryLifecycle::Durable => true,
                ThreadEntryLifecycle::RuntimeDraft { owner } => {
                    connection_id == Some(owner.connection_id)
                }
            })
            .map(|entry| entry.thread.clone())
            .collect()
    }

    /// Resolves a runtime-only draft only for its exact authenticated owner.
    /// Persisted threads deliberately never pass through this path.
    pub(crate) async fn authorize_runtime_draft(
        &self,
        connection_id: ConnectionId,
        identity: &ThreadSubscriptionIdentity,
        thread_id: &str,
        expected_workspace_id: Option<&str>,
    ) -> Option<RuntimeDraftAccess> {
        let state = self.state.read().await;
        let entry = state.threads.get(thread_id)?;
        let ThreadEntryLifecycle::RuntimeDraft { owner } = &entry.lifecycle else {
            return None;
        };
        if owner.connection_id != connection_id
            || &owner.identity != identity
            || entry.subscribers.get(&connection_id) != Some(identity)
            || expected_workspace_id
                .is_some_and(|workspace_id| workspace_id != entry.thread.workspace_id)
        {
            return None;
        }
        Some(RuntimeDraftAccess {
            workspace_id: entry.thread.workspace_id.clone(),
            thread_id: entry.thread.id.clone(),
            visibility: entry.thread.visibility,
            owner: owner.clone(),
        })
    }

    /// Completes the in-memory half of the first-turn transaction after the
    /// durable thread, creator membership, and turn have committed.
    pub(crate) async fn mark_runtime_draft_durable(&self, access: &RuntimeDraftAccess) -> bool {
        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(access.thread_id()) else {
            return false;
        };
        if entry.thread.workspace_id != access.workspace_id {
            return false;
        }
        match &entry.lifecycle {
            ThreadEntryLifecycle::RuntimeDraft { owner } if owner == access.owner() => {
                entry.lifecycle = ThreadEntryLifecycle::Durable;
                true
            }
            ThreadEntryLifecycle::Durable => true,
            ThreadEntryLifecycle::RuntimeDraft { .. } => false,
        }
    }

    #[cfg(test)]
    pub async fn turn_start(
        &self,
        connection_id: ConnectionId,
        params: TurnStartParams,
    ) -> Result<TurnStartOutcome> {
        self.turn_start_for_actor(Some(connection_id), params, None, Vec::new())
            .await
    }

    pub async fn turn_start_with_user_metadata(
        &self,
        connection_id: ConnectionId,
        params: TurnStartParams,
        author: Option<pioneer_protocol::TurnAuthorSnapshot>,
        mentions: Vec<pioneer_protocol::TurnMention>,
    ) -> Result<TurnStartOutcome> {
        self.turn_start_for_actor(Some(connection_id), params, author, mentions)
            .await
    }

    pub async fn turn_start_with_user_metadata_and_permission_profile(
        &self,
        connection_id: ConnectionId,
        params: TurnStartParams,
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
        author: Option<pioneer_protocol::TurnAuthorSnapshot>,
        mentions: Vec<pioneer_protocol::TurnMention>,
    ) -> Result<TurnStartOutcome> {
        self.turn_start_for_actor_with_permission_profile(
            Some(connection_id),
            params,
            Some(permission_profile),
            author,
            mentions,
        )
        .await
    }

    pub async fn system_turn_start_with_permission_profile(
        &self,
        params: TurnStartParams,
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
    ) -> Result<TurnStartOutcome> {
        self.turn_start_for_actor_with_permission_profile(
            None,
            params,
            Some(permission_profile),
            None,
            Vec::new(),
        )
        .await
    }

    async fn turn_start_for_actor(
        &self,
        connection_id: Option<ConnectionId>,
        params: TurnStartParams,
        author: Option<pioneer_protocol::TurnAuthorSnapshot>,
        mentions: Vec<pioneer_protocol::TurnMention>,
    ) -> Result<TurnStartOutcome> {
        self.turn_start_for_actor_with_permission_profile(
            connection_id,
            params,
            None,
            author,
            mentions,
        )
        .await
    }

    async fn turn_start_for_actor_with_permission_profile(
        &self,
        connection_id: Option<ConnectionId>,
        params: TurnStartParams,
        resolved_permission_profile: Option<pioneer_protocol::TurnPermissionProfileSnapshot>,
        author: Option<pioneer_protocol::TurnAuthorSnapshot>,
        mentions: Vec<pioneer_protocol::TurnMention>,
    ) -> Result<TurnStartOutcome> {
        pioneer_protocol::validate_turn_execution_envelope(&params)
            .map_err(|message| anyhow!(message))?;
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
        let permission_profile = resolved_permission_profile.unwrap_or_else(|| {
            pioneer_protocol::resolve_turn_permission_profile(params.permission_profile.as_ref())
        });
        let now = unix_timestamp_secs()? as i64;

        let mut state = self.state.write().await;

        let Some(entry) = state.threads.get_mut(thread_id) else {
            bail!("thread `{thread_id}` is not loaded");
        };

        if let Some(connection_id) = connection_id
            && !entry.subscribers.contains_key(&connection_id)
        {
            bail!("connection `{connection_id}` is not subscribed to thread `{thread_id}`");
        }

        if let Some(existing) = entry.thread.turns.iter().find(|turn| turn.id == turn_id) {
            bail!(
                "turn `{turn_id}` already exists in thread `{thread_id}` with status `{:?}`",
                existing.status
            );
        }

        let has_running_turn = entry.thread.turns.iter().any(turn_owns_foreground);

        if has_running_turn {
            bail!("thread `{thread_id}` already has a running turn");
        }

        let previous_preview = entry.thread.preview.clone();
        let previous_status = entry.thread.status;
        let previous_updated_at = entry.thread.updated_at;
        let previous_mode = entry.thread.mode;
        let previous_model = entry.thread.model.clone();
        let previous_model_provider = entry.thread.model_provider.clone();
        let previous_reasoning_effort = entry.thread.reasoning_effort.clone();
        let previous_sandbox_mode = entry.sandbox_mode;
        let requested_reasoning_effort = params
            .reasoning
            .as_ref()
            .map(|reasoning| reasoning.effort.trim())
            .filter(|effort| !effort.is_empty())
            .map(str::to_owned);

        let effective_mode = requested_mode.unwrap_or(entry.thread.mode);
        if let Some(model) = requested_model {
            entry.thread.model = model;
        }
        if let Some(model_provider) = requested_model_provider {
            entry.thread.model_provider = model_provider;
        }
        entry.thread.reasoning_effort = requested_reasoning_effort;
        let turn = pioneer_protocol::Turn {
            id: turn_id,
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: effective_mode,
            author,
            reply_to_turn_id: params.reply_to_turn_id.clone(),
            mentions,
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile,
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

        let started_notification_connection_ids = entry.subscribers.keys().copied().collect();

        let materialization = TurnStartMaterialization {
            thread: entry.thread.clone(),
            turn: started_notification.turn.clone(),
            input: params.input.clone(),
            capabilities: params.capabilities.clone(),
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
            previous_reasoning_effort,
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

    /// Prepares one ordinary Message Turn as Started + immediately Completed
    /// without mutating in-memory state or acquiring foreground execution
    /// ownership. The caller persists both canonical events atomically before
    /// applying the final state with `commit_completed_message_turn`.
    pub async fn prepare_completed_message_turn(
        &self,
        connection_id: ConnectionId,
        params: &TurnStartParams,
        author: pioneer_protocol::TurnAuthorSnapshot,
        mentions: Vec<pioneer_protocol::TurnMention>,
    ) -> Result<CompletedMessageTurnStartOutcome> {
        let thread_id = params.thread_id.trim();
        if thread_id.is_empty() {
            bail!("`thread_id` is required for `turn/start`");
        }
        let turn_id = params.turn_id.trim();
        if turn_id.is_empty() {
            bail!("`turn_id` is required for `turn/start`");
        }

        let now = unix_timestamp_secs()? as i64;
        let state = self.state.read().await;
        let Some(entry) = state.threads.get(thread_id) else {
            bail!("thread `{thread_id}` is not loaded");
        };
        if !entry.subscribers.contains_key(&connection_id) {
            bail!("connection `{connection_id}` is not subscribed to thread `{thread_id}`");
        }
        if entry.thread.turns.iter().any(|turn| turn.id == turn_id) {
            bail!("turn `{turn_id}` already exists in thread `{thread_id}`");
        }
        let effective_mode = params.mode.unwrap_or(entry.thread.mode);
        if effective_mode != ThreadMode::Message {
            bail!("completed-on-start transition requires Message mode");
        }

        let permission_profile = pioneer_protocol::resolve_turn_permission_profile(None);
        let started_turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: ThreadMode::Message,
            author: Some(author),
            reply_to_turn_id: params.reply_to_turn_id.clone(),
            mentions,
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile,
        };
        let mut completed_turn = started_turn.clone();
        completed_turn.status = TurnStatus::Completed;

        let mut started_thread = entry.thread.clone();
        if started_thread.preview.is_empty()
            && let Some(text) = params.input.iter().find_map(|input| match input {
                UserInput::Text { text, .. } => Some(text.trim()),
                _ => None,
            })
            && !text.is_empty()
        {
            started_thread.preview = text.to_owned();
        }
        started_thread.updated_at = now;
        started_thread.turns.push(started_turn.clone());

        let mut final_thread = started_thread.clone();
        if let Some(turn) = final_thread.turns.last_mut() {
            *turn = completed_turn.clone();
        }

        Ok(CompletedMessageTurnStartOutcome {
            response: TurnStartResponse {
                turn: completed_turn.clone(),
            },
            started_notification: TurnStartedNotification {
                workspace_id: started_thread.workspace_id.clone(),
                thread_id: thread_id.to_owned(),
                turn: started_turn.clone(),
            },
            completed_notification: pioneer_protocol::TurnCompletedNotification {
                workspace_id: started_thread.workspace_id.clone(),
                thread_id: thread_id.to_owned(),
                turn: completed_turn,
            },
            notification_connection_ids: entry.subscribers.keys().copied().collect(),
            materialization: TurnStartMaterialization {
                thread: started_thread,
                turn: started_turn,
                input: params.input.clone(),
                capabilities: Vec::new(),
                sandbox_mode: entry.sandbox_mode,
            },
            final_thread,
        })
    }

    /// Applies only the already-committed terminal Message projection to the
    /// loaded Thread. Durable state remains authoritative if the Thread was
    /// unloaded between prepare and commit.
    pub async fn commit_completed_message_turn(
        &self,
        outcome: &CompletedMessageTurnStartOutcome,
    ) -> Result<()> {
        let thread_id = outcome.completed_notification.thread_id.as_str();
        let turn_id = outcome.completed_notification.turn.id.as_str();
        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(thread_id) else {
            bail!("thread `{thread_id}` is not loaded");
        };
        if let Some(existing) = entry.thread.turns.iter().find(|turn| turn.id == turn_id) {
            if existing == &outcome.completed_notification.turn {
                return Ok(());
            }
            bail!("turn `{turn_id}` already exists with different state");
        }
        if entry.thread.preview.is_empty()
            && let Some(preview) =
                outcome
                    .materialization
                    .input
                    .iter()
                    .find_map(|input| match input {
                        UserInput::Text { text, .. } => Some(text.trim()),
                        _ => None,
                    })
            && !preview.is_empty()
        {
            entry.thread.preview = preview.to_owned();
        }
        entry.thread.updated_at = entry.thread.updated_at.max(outcome.final_thread.updated_at);
        entry
            .thread
            .turns
            .push(outcome.completed_notification.turn.clone());
        Ok(())
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
        entry.thread.reasoning_effort = context.previous_reasoning_effort;
        entry.sandbox_mode = context.previous_sandbox_mode;

        let discard_disconnected_runtime_draft = entry.subscribers.is_empty()
            && matches!(&entry.lifecycle, ThreadEntryLifecycle::RuntimeDraft { .. })
            && !has_in_progress_conversation_turn(&entry.thread);
        if discard_disconnected_runtime_draft {
            state.threads.remove(&context.thread_id);
        }
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

    /// Applies an authoritative terminal Turn only after its database
    /// transaction committed. Missing unloaded state is harmless; a different
    /// terminal outcome or duplicate in-memory identity is rejected.
    pub async fn commit_terminal_turn(&self, thread_id: &str, terminal: &Turn) -> Result<bool> {
        let now = unix_timestamp_secs()? as i64;
        let mut state = self.state.write().await;
        let Some(entry) = state.threads.get_mut(thread_id) else {
            return Ok(false);
        };
        let matching = entry
            .thread
            .turns
            .iter_mut()
            .filter(|turn| turn.id == terminal.id)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            bail!(
                "thread `{thread_id}` has {} in-memory Turns with identity `{}`",
                matching.len(),
                terminal.id
            );
        }
        let current = matching.into_iter().next().expect("one matching Turn");
        if current.status != TurnStatus::InProgress {
            if current == terminal {
                return Ok(true);
            }
            bail!(
                "turn `{}` already has conflicting terminal status `{:?}`",
                terminal.id,
                current.status
            );
        }
        *current = terminal.clone();
        entry.thread.status = if entry.thread.turns.iter().any(turn_owns_foreground) {
            ThreadStatus::Active
        } else {
            ThreadStatus::Idle
        };
        entry.thread.updated_at = entry.thread.updated_at.max(now);
        Ok(true)
    }

    pub async fn subscribed_connection_ids(&self, thread_id: &str) -> Vec<ConnectionId> {
        let state = self.state.read().await;
        state
            .threads
            .get(thread_id)
            .map(|entry| entry.subscribers.keys().copied().collect())
            .unwrap_or_default()
    }

    pub(crate) async fn subscribed_connections(&self, thread_id: &str) -> Vec<ThreadSubscriber> {
        let state = self.state.read().await;
        state
            .threads
            .get(thread_id)
            .map(|entry| {
                entry
                    .subscribers
                    .iter()
                    .map(|(connection_id, identity)| ThreadSubscriber {
                        connection_id: *connection_id,
                        identity: identity.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) async fn subscribed_connections_for_candidates(
        &self,
        thread_id: &str,
        candidate_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ThreadSubscriber> {
        let state = self.state.read().await;
        let Some(entry) = state.threads.get(thread_id) else {
            return Vec::new();
        };

        candidate_connection_ids
            .into_iter()
            .filter_map(|connection_id| {
                entry
                    .subscribers
                    .get(&connection_id)
                    .cloned()
                    .map(|identity| ThreadSubscriber {
                        connection_id,
                        identity,
                    })
            })
            .collect()
    }

    /// Removes only subscriptions in the committed authorization scope.
    ///
    /// A workspace-wide access loss removes every thread subscription in that
    /// workspace for the connection. A thread-scoped loss removes only the
    /// exact thread. Unrelated subscriptions and active background turns are
    /// preserved.
    pub(crate) async fn evict_connection_scope(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        thread_id: Option<&str>,
    ) -> Vec<ThreadSubscriber> {
        let workspace_id = workspace_id.trim();
        if workspace_id.is_empty() {
            return Vec::new();
        }

        let mut state = self.state.write().await;
        let candidates = state
            .thread_ids_by_connection
            .get(&connection_id)
            .into_iter()
            .flat_map(|thread_ids| thread_ids.iter())
            .filter_map(|candidate_thread_id| {
                let entry = state.threads.get(candidate_thread_id)?;
                (entry.thread.workspace_id == workspace_id
                    && thread_id.is_none_or(|thread_id| thread_id == candidate_thread_id))
                .then(|| candidate_thread_id.clone())
            })
            .collect::<Vec<_>>();
        let mut removed = Vec::with_capacity(candidates.len());

        for candidate_thread_id in candidates {
            let identity = state
                .threads
                .get_mut(candidate_thread_id.as_str())
                .and_then(|entry| entry.subscribers.remove(&connection_id));
            if let Some(identity) = identity {
                removed.push(ThreadSubscriber {
                    connection_id,
                    identity,
                });
            }
            if let Some(thread_ids) = state.thread_ids_by_connection.get_mut(&connection_id) {
                thread_ids.remove(candidate_thread_id.as_str());
            }
            let remove_thread =
                state
                    .threads
                    .get(candidate_thread_id.as_str())
                    .is_some_and(|entry| {
                        entry.subscribers.is_empty()
                            && !has_in_progress_conversation_turn(&entry.thread)
                    });
            if remove_thread {
                state.threads.remove(candidate_thread_id.as_str());
            }
        }

        if state
            .thread_ids_by_connection
            .get(&connection_id)
            .is_some_and(HashSet::is_empty)
        {
            state.thread_ids_by_connection.remove(&connection_id);
        }

        removed
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

        if !entry.subscribers.is_empty() || has_in_progress_conversation_turn(&entry.thread) {
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
                    entry.subscribers.remove(&connection_id);
                    entry.subscribers.is_empty()
                        && !has_in_progress_conversation_turn(&entry.thread)
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
                closed_notification_subscribers: Vec::new(),
            };
        };

        let Some(removed_identity) = entry.subscribers.remove(&connection_id) else {
            return ThreadUnsubscribeOutcome {
                response: ThreadUnsubscribeResponse {
                    status: ThreadUnsubscribeStatus::NotSubscribed,
                },
                closed_notification: None,
                closed_notification_subscribers: Vec::new(),
            };
        };

        if let Some(thread_ids) = state.thread_ids_by_connection.get_mut(&connection_id) {
            thread_ids.remove(thread_id);
            if thread_ids.is_empty() {
                state.thread_ids_by_connection.remove(&connection_id);
            }
        }

        let should_remove_thread = state.threads.get(thread_id).is_some_and(|entry| {
            entry.subscribers.is_empty() && !has_in_progress_conversation_turn(&entry.thread)
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
            closed_notification_subscribers: if should_remove_thread {
                vec![ThreadSubscriber {
                    connection_id,
                    identity: removed_identity,
                }]
            } else {
                Vec::new()
            },
        }
    }
}

fn turn_owns_foreground(turn: &Turn) -> bool {
    turn.turn_kind == TurnKind::Conversation && turn.status == TurnStatus::InProgress
}

fn has_in_progress_conversation_turn(thread: &Thread) -> bool {
    thread.turns.iter().any(turn_owns_foreground)
}

fn foreground_thread_status(thread: &Thread) -> ThreadStatus {
    if has_in_progress_conversation_turn(thread) {
        ThreadStatus::Active
    } else {
        ThreadStatus::Idle
    }
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

        entry
            .subscribers
            .insert(connection_id, test_subscription_identity(connection_id));
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
fn test_subscription_identity(connection_id: ConnectionId) -> ThreadSubscriptionIdentity {
    let _ = connection_id;
    ThreadSubscriptionIdentity::new(
        PrincipalId::new("P00000000000000000001").expect("test principal id"),
        AuthSessionId::new("S00000000000000000001").expect("test auth session id"),
    )
}

#[cfg(test)]
mod tests {
    use super::{ThreadManager, ThreadSubscriptionIdentity};
    use pioneer_protocol::{
        PermissionBehavior, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStartParams, ThreadStatus, ThreadUnsubscribeStatus, ToolPermissionPolicySnapshot,
        TurnKind, TurnOrigin, TurnPermissionMode, TurnPermissionProfileSelection,
        TurnPermissionProfileSource, TurnReasoningSelection, TurnStartParams, TurnStatus,
        UserInput,
    };

    fn start_params(thread_id: &str) -> ThreadStartParams {
        ThreadStartParams {
            thread_id: thread_id.to_owned(),
            workspace_id: "ws_000000000000000001".to_owned(),
            name: None,
            model: None,
            model_provider: None,
            sandbox: None,
            mode: Some(ThreadMode::Chat),
            origin_kind: None,
            sidebar_visibility: None,
            visibility: None,
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
            visibility: None,
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
    async fn new_user_thread_without_mode_defaults_to_message() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let mut params = start_params("thr_message_default");
        params.mode = None;
        let outcome = manager
            .thread_start(10, "ws_000000000000000001".to_owned(), params)
            .await
            .expect("thread start should succeed");
        assert_eq!(outcome.response.thread.mode, ThreadMode::Message);
    }

    #[tokio::test]
    async fn completed_message_does_not_take_foreground_ownership() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_id = "thr_message_during_agent";
        let thread_start = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id),
            )
            .await
            .expect("thread start should succeed");
        let running = manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: thread_id.to_owned(),
                    turn_id: "turn_running_agent".to_owned(),
                    input: vec![UserInput::Text {
                        text: "run".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    capabilities: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: Some(ThreadMode::Agent),
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
            )
            .await
            .expect("Agent turn should start");
        assert_eq!(running.response.turn.status, TurnStatus::InProgress);

        let message = manager
            .prepare_completed_message_turn(
                10,
                &TurnStartParams {
                    thread_id: thread_id.to_owned(),
                    turn_id: "turn_message".to_owned(),
                    input: vec![UserInput::Text {
                        text: "hello".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    capabilities: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: Some(ThreadMode::Message),
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
                pioneer_protocol::TurnAuthorSnapshot {
                    actor: pioneer_protocol::PersistedActorRef::System,
                    display_name: "System".to_owned(),
                    nickname: "system".to_owned(),
                    avatar_revision: None,
                },
                Vec::new(),
            )
            .await
            .expect("Message should prepare while Agent runs");
        assert_eq!(message.response.turn.status, TurnStatus::Completed);
        assert_eq!(
            message.started_notification.turn.status,
            TurnStatus::InProgress
        );
        assert_eq!(
            message.completed_notification.turn.status,
            TurnStatus::Completed
        );
        assert_eq!(
            manager
                .thread_get(thread_id)
                .await
                .expect("thread")
                .turns
                .len(),
            1,
            "prepare must not expose a running Message"
        );

        manager
            .commit_completed_message_turn(&message)
            .await
            .expect("committed Message should apply");
        let loaded_thread = manager.thread_get(thread_id).await.expect("thread");
        assert_eq!(loaded_thread.status, ThreadStatus::Active);
        assert_eq!(loaded_thread.mode, ThreadMode::Chat);
        assert_eq!(loaded_thread.turns.len(), 2);
        assert_eq!(loaded_thread.turns[0].status, TurnStatus::InProgress);
        assert_eq!(loaded_thread.turns[1].status, TurnStatus::Completed);
        assert_eq!(loaded_thread.turns[1].mode, ThreadMode::Message);
        assert_eq!(
            loaded_thread.workspace_id,
            thread_start.response.thread.workspace_id
        );

        let mut persisted_thread = loaded_thread.clone();
        persisted_thread.preview.clear();
        persisted_thread.updated_at = persisted_thread.updated_at.saturating_add(1);
        let persisted_message = persisted_thread
            .turns
            .iter_mut()
            .find(|turn| turn.id == "turn_message")
            .expect("persisted Message");
        persisted_message.message_revision = 1;
        persisted_message.message_deleted = true;
        manager
            .sync_message_mutation_from_persisted(&persisted_thread, "turn_message")
            .await
            .expect("Message mutation should synchronize");
        let synchronized = manager.thread_get(thread_id).await.expect("thread");
        assert_eq!(synchronized.status, ThreadStatus::Active);
        assert_eq!(synchronized.turns[0].status, TurnStatus::InProgress);
        assert!(synchronized.turns[1].message_deleted);
        assert_eq!(synchronized.turns[1].message_revision, 1);
        assert!(synchronized.preview.is_empty());
    }

    #[tokio::test]
    async fn terminal_native_turn_identity_cannot_be_admitted_again() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_id = "thr_native_duplicate_terminal";
        manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id),
            )
            .await
            .expect("thread should start");
        let params = TurnStartParams {
            thread_id: thread_id.to_owned(),
            turn_id: "turn_native_duplicate_terminal".to_owned(),
            input: vec![UserInput::Text {
                text: "execute once".to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: Some(ThreadMode::Agent),
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };
        manager
            .turn_start(10, params.clone())
            .await
            .expect("first admission should succeed");
        manager
            .turn_finish(
                thread_id,
                params.turn_id.as_str(),
                TurnStatus::Completed,
                None,
            )
            .await
            .expect("first turn should finish");

        let error = manager
            .turn_start(10, params)
            .await
            .expect_err("terminal turn identity must not be admitted twice");
        assert!(
            format!("{error:#}").contains("already exists"),
            "duplicate admission must return a deterministic identity conflict: {error:#}"
        );
        let loaded = manager.thread_get(thread_id).await.expect("thread");
        let matches = loaded
            .turns
            .iter()
            .filter(|turn| turn.id == "turn_native_duplicate_terminal")
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].status, TurnStatus::Completed);
    }

    #[tokio::test]
    async fn concurrent_native_turn_identity_admission_has_exactly_one_winner() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_id = "thr_native_duplicate_concurrent";
        manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id),
            )
            .await
            .expect("thread should start");
        let params = TurnStartParams {
            thread_id: thread_id.to_owned(),
            turn_id: "turn_native_duplicate_concurrent".to_owned(),
            input: vec![UserInput::Text {
                text: "execute one admission".to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: Some(ThreadMode::Agent),
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };
        let (first, second) = tokio::join!(
            manager.turn_start(10, params.clone()),
            manager.turn_start(10, params)
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let rejected = first.err().or_else(|| second.err()).expect("one rejection");
        assert!(format!("{rejected:#}").contains("already exists"));
        let loaded = manager.thread_get(thread_id).await.expect("thread");
        assert_eq!(
            loaded
                .turns
                .iter()
                .filter(|turn| turn.id == "turn_native_duplicate_concurrent")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn every_native_terminal_status_fences_sequential_identity_reuse() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_id = "thr_native_terminal_identity_matrix";
        manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id),
            )
            .await
            .expect("thread should start");
        for (suffix, status) in [
            ("completed", TurnStatus::Completed),
            ("failed", TurnStatus::Failed),
            ("blocked", TurnStatus::Blocked),
            ("interrupted", TurnStatus::Interrupted),
        ] {
            let params = TurnStartParams {
                thread_id: thread_id.to_owned(),
                turn_id: format!("turn_terminal_{suffix}"),
                input: vec![UserInput::Text {
                    text: suffix.to_owned(),
                    text_elements: Vec::new(),
                }],
                capabilities: Vec::new(),
                model: None,
                model_provider: None,
                sandbox_policy: None,
                mode: Some(ThreadMode::Agent),
                reply_to_turn_id: None,
                mentioned_principal_ids: Vec::new(),
                execution_backend: None,
                reasoning: None,
                permission_profile: None,
                cli_runtime_options: None,
            };
            manager
                .turn_start(10, params.clone())
                .await
                .expect("first admission should succeed");
            manager
                .turn_finish(thread_id, params.turn_id.as_str(), status, None)
                .await
                .expect("turn should become terminal");
            assert!(
                manager.turn_start(10, params).await.is_err(),
                "{status:?} identity must not admit another execution"
            );
        }
    }

    #[tokio::test]
    async fn concurrent_completed_messages_merge_without_regressing_loaded_thread_state() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_id = "thr_concurrent_messages";
        let started = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id),
            )
            .await
            .expect("thread should start");
        let base_updated_at = started.response.thread.updated_at;
        let author = pioneer_protocol::TurnAuthorSnapshot {
            actor: pioneer_protocol::PersistedActorRef::System,
            display_name: "System".to_owned(),
            nickname: "system".to_owned(),
            avatar_revision: None,
        };
        let params = |turn_id: &str, text: &str| TurnStartParams {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            input: vec![UserInput::Text {
                text: text.to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: Some(ThreadMode::Message),
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };
        let mut older = manager
            .prepare_completed_message_turn(
                10,
                &params("turn_message_older", "older"),
                author.clone(),
                Vec::new(),
            )
            .await
            .expect("older Message should prepare");
        let mut newer = manager
            .prepare_completed_message_turn(
                10,
                &params("turn_message_newer", "newer"),
                author,
                Vec::new(),
            )
            .await
            .expect("newer Message should prepare");
        older.final_thread.updated_at = base_updated_at + 1;
        newer.final_thread.updated_at = base_updated_at + 2;

        manager
            .commit_completed_message_turn(&newer)
            .await
            .expect("newer committed Message should apply");
        manager
            .commit_completed_message_turn(&older)
            .await
            .expect("older committed Message should merge");

        let thread = manager.thread_get(thread_id).await.expect("thread");
        assert_eq!(thread.preview, "newer");
        assert_eq!(thread.updated_at, base_updated_at + 2);
        assert_eq!(thread.turns.len(), 2);
        assert!(thread.turns.iter().all(|turn| {
            turn.mode == ThreadMode::Message && turn.status == TurnStatus::Completed
        }));
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
            reasoning_effort: None,
            created_at: seed_updated_at - 10,
            updated_at: seed_updated_at,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: vec![pioneer_protocol::Turn {
                id: "turn_000000000000000099".to_owned(),
                status: TurnStatus::Completed,
                turn_kind: Default::default(),
                origin: Default::default(),
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
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
    async fn persisted_thread_restore_preserves_running_turn_for_terminal_lifecycle() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_id = "thr_000000000000000121";
        let turn_id = "turn_000000000000000121";
        let thread = Thread {
            workspace_id: "ws_000000000000000001".to_owned(),
            id: thread_id.to_owned(),
            name: Some("restored".to_owned()),
            preview: "running work".to_owned(),
            mode: ThreadMode::Agent,
            model: "o4-mini".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_010,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: vec![pioneer_protocol::Turn {
                id: turn_id.to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: Default::default(),
                origin: Default::default(),
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            }],
        };

        manager
            .system_thread_restore_persisted(thread, None)
            .await
            .expect("persisted thread restore should succeed");
        manager
            .turn_finish(thread_id, turn_id, TurnStatus::Completed, None)
            .await
            .expect("restored turn should use the normal terminal lifecycle");

        let (_, turn) = manager
            .turn_get(thread_id, turn_id)
            .await
            .expect("restored turn should remain addressable");
        assert_eq!(turn.status, TurnStatus::Completed);
    }

    #[tokio::test]
    async fn persisted_task_run_occurrence_does_not_block_new_conversation_turn() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let workspace_id = "ws_000000000000000001";
        let thread_id = "thr_000000000000000122";
        let task_turn_id = "run_0000000000000000122";
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: Some("restored detached task".to_owned()),
            preview: "background work".to_owned(),
            mode: ThreadMode::Agent,
            model: "o4-mini".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_010,
            // Legacy projections marked the parent active for this task turn.
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: vec![pioneer_protocol::Turn {
                id: task_turn_id.to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: TurnKind::TaskRun,
                origin: TurnOrigin::DetachedTask,
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            }],
        };

        manager
            .system_thread_restore_persisted(thread, None)
            .await
            .expect("persisted task occurrence should restore");
        assert_eq!(
            manager
                .thread_get(thread_id)
                .await
                .expect("restored parent should exist")
                .status,
            ThreadStatus::Idle
        );

        manager
            .thread_start_seeded(
                10,
                workspace_id.to_owned(),
                start_params(thread_id),
                None,
                None,
            )
            .await
            .expect("user should subscribe to restored parent");
        let foreground = manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: thread_id.to_owned(),
                    turn_id: "turn_000000000000000122".to_owned(),
                    input: vec![UserInput::Text {
                        text: "continue chatting".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    capabilities: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
            )
            .await
            .expect("detached task occurrence must not block a conversation turn");

        assert_eq!(foreground.response.turn.turn_kind, TurnKind::Conversation);
        assert_eq!(foreground.response.turn.status, TurnStatus::InProgress);
        assert_eq!(
            manager
                .turn_get(thread_id, task_turn_id)
                .await
                .expect("task occurrence should remain independently in progress")
                .1
                .status,
            TurnStatus::InProgress
        );
    }

    #[tokio::test]
    async fn connection_closed_discards_runtime_draft_with_its_owner() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000004"),
            )
            .await
            .expect("thread start should succeed");

        let thread_id = outcome.response.thread.id.clone();
        let removed = manager.connection_closed(10).await;

        assert_eq!(removed, vec![thread_id.clone()]);
        assert!(!manager.has_thread(&thread_id).await);
    }

    #[tokio::test]
    async fn runtime_draft_authorization_requires_exact_connection_and_identity() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let identity = ThreadSubscriptionIdentity::new(
            pioneer_protocol::PrincipalId::new("P00000000000000000001").expect("principal id"),
            pioneer_protocol::AuthSessionId::new("S00000000000000000001").expect("session id"),
        );
        let thread_id = "thr_runtime_owner_exact";
        manager
            .thread_start_authenticated(
                10,
                identity.clone(),
                "ws_000000000000000001".to_owned(),
                start_params(thread_id),
            )
            .await
            .expect("runtime draft should start");

        assert!(
            manager
                .authorize_runtime_draft(10, &identity, thread_id, Some("ws_000000000000000001"),)
                .await
                .is_some()
        );
        assert!(
            manager
                .authorize_runtime_draft(11, &identity, thread_id, Some("ws_000000000000000001"),)
                .await
                .is_none()
        );
        let other_session = ThreadSubscriptionIdentity::new(
            identity.principal_id.clone(),
            pioneer_protocol::AuthSessionId::new("S00000000000000000002")
                .expect("other session id"),
        );
        assert!(
            manager
                .authorize_runtime_draft(
                    10,
                    &other_session,
                    thread_id,
                    Some("ws_000000000000000001"),
                )
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn connection_closed_keeps_durable_thread_when_other_subscriber_exists() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000005"),
            )
            .await
            .expect("thread start should succeed");

        let thread_id = outcome.response.thread.id.clone();
        manager
            .thread_start_seeded(
                11,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id.as_str()),
                Some(outcome.response.thread),
                None,
            )
            .await
            .expect("persisted thread should accept another subscriber");

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
                    capabilities: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
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
    async fn thread_unsubscribe_discards_runtime_draft_when_owner_leaves() {
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
    async fn thread_unsubscribe_keeps_durable_thread_when_subscribers_remain() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000008"),
            )
            .await
            .expect("thread start should succeed");
        let thread_id = outcome.response.thread.id.clone();
        manager
            .thread_start_seeded(
                11,
                "ws_000000000000000001".to_owned(),
                start_params(thread_id.as_str()),
                Some(outcome.response.thread),
                None,
            )
            .await
            .expect("persisted thread should accept another subscriber");

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
                    capabilities: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
            )
            .await
            .expect("turn start should succeed");

        assert_eq!(turn_outcome.response.turn.status, TurnStatus::InProgress);
        assert_eq!(
            turn_outcome.started_notification.turn.status,
            TurnStatus::InProgress
        );
        let permission_profile = &turn_outcome.response.turn.permission_profile;
        assert_eq!(permission_profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            permission_profile.source,
            TurnPermissionProfileSource::Defaulted
        );
        assert_eq!(
            permission_profile.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
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
                    capabilities: Vec::new(),
                    sandbox_policy: None,
                    mode: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: Some(TurnReasoningSelection {
                        effort: "high".to_owned(),
                    }),
                    permission_profile: None,
                    cli_runtime_options: None,
                },
            )
            .await
            .expect("turn start should succeed");

        assert_eq!(turn_outcome.materialization.thread.model, "o3");
        assert_eq!(
            turn_outcome.materialization.thread.model_provider,
            "custom-provider"
        );
        assert_eq!(
            turn_outcome
                .materialization
                .thread
                .reasoning_effort
                .as_deref(),
            Some("high")
        );
    }

    #[tokio::test]
    async fn turn_start_uses_composer_permission_profile_selection() {
        let manager = ThreadManager::new("o4-mini", "openai");
        let thread_outcome = manager
            .thread_start(
                10,
                "ws_000000000000000001".to_owned(),
                start_params("thr_000000000000000013"),
            )
            .await
            .expect("thread start should succeed");

        let turn_outcome = manager
            .turn_start(
                10,
                TurnStartParams {
                    thread_id: thread_outcome.response.thread.id.clone(),
                    turn_id: "turn_000000000000000013".to_owned(),
                    input: Vec::new(),
                    capabilities: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: Some(TurnPermissionProfileSelection {
                        mode: TurnPermissionMode::Supervised,
                    }),
                    cli_runtime_options: None,
                },
            )
            .await
            .expect("turn start should succeed");

        let permission_profile = &turn_outcome.response.turn.permission_profile;
        assert_eq!(permission_profile.mode, TurnPermissionMode::Supervised);
        assert_eq!(
            permission_profile.source,
            TurnPermissionProfileSource::Composer
        );
        assert_eq!(
            permission_profile.effective_policy.default_behavior,
            PermissionBehavior::Ask
        );
        assert_eq!(
            permission_profile.effective_policy.file_read,
            PermissionBehavior::Allow
        );
        assert_eq!(
            turn_outcome.materialization.turn.permission_profile.mode,
            TurnPermissionMode::Supervised
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
                    capabilities: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
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
                    capabilities: Vec::new(),
                    sandbox_policy: None,
                    mode: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: Some(TurnReasoningSelection {
                        effort: "high".to_owned(),
                    }),
                    permission_profile: None,
                    cli_runtime_options: None,
                },
            )
            .await
            .expect("turn start should succeed");

        assert_eq!(turn_outcome.materialization.thread.model, "o3");
        assert_eq!(
            turn_outcome.materialization.thread.model_provider,
            "custom-provider"
        );
        assert_eq!(
            turn_outcome
                .materialization
                .thread
                .reasoning_effort
                .as_deref(),
            Some("high")
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
                    capabilities: Vec::new(),
                    model: None,
                    model_provider: None,
                    sandbox_policy: None,
                    mode: None,
                    reply_to_turn_id: None,
                    mentioned_principal_ids: Vec::new(),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
            )
            .await
            .expect("second turn start should succeed");
        assert_eq!(second_turn_outcome.materialization.thread.model, "o4-mini");
        assert_eq!(
            second_turn_outcome.materialization.thread.model_provider,
            "openai"
        );
        assert!(
            second_turn_outcome
                .materialization
                .thread
                .reasoning_effort
                .is_none()
        );

        manager
            .rollback_turn_start(second_turn_outcome.rollback_context)
            .await;

        let removed = manager.connection_closed(10).await;
        assert_eq!(removed, vec!["thr_000000000000000011".to_owned()]);
    }
}
