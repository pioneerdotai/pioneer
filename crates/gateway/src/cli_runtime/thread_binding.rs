#![allow(dead_code)]
// Persists Pioneer thread to native CLI runtime thread bindings.

use crate::cli_runtime::continuation::CliProviderContinuation;
use crate::cli_runtime::manager::{
    CLIAgentRuntimeSession, CLIAgentRuntimeThreadOpenParams, CLIAgentRuntimeThreadOpenSnapshot,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use pioneer_crud::{
    CliRuntimeThreadBindingRecord, CrudStore, NewCliRuntimeThreadBinding,
    PrepareClaudeProviderSessionBinding, PreparedClaudeProviderSessionMode,
    deserialize_cli_runtime_json, serialize_cli_runtime_json,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct ClaudeProviderSessionPrepareRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub runtime_id: String,
    pub cwd: String,
    pub model: Option<String>,
    pub force_new: bool,
    pub fork: Option<ClaudeProviderForkSource>,
    pub prepared_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaudeProviderForkSource {
    pub source_session_id: Uuid,
    pub source_turn_id: String,
    pub boundary_message_uuid: Uuid,
}

/// Create or load the real Claude UUID before process allocation. This is the
/// only place where a fresh provider session identity is minted for a Pioneer
/// thread; a reload always reuses the durable value.
pub(crate) async fn prepare_claude_provider_session(
    store: &CrudStore,
    request: ClaudeProviderSessionPrepareRequest,
) -> Result<CliProviderContinuation> {
    for (label, value) in [
        ("workspace_id", request.workspace_id.as_str()),
        ("thread_id", request.thread_id.as_str()),
        ("runtime_id", request.runtime_id.as_str()),
        ("cwd", request.cwd.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("Claude provider session request `{label}` cannot be empty");
        }
    }
    let requested_fork = request.fork.clone();
    let proposed_provider_session_id = Uuid::new_v4();
    let (root, cursor) = if let Some(fork) = request.fork.as_ref() {
        if request.force_new
            || fork.source_session_id.is_nil()
            || fork.boundary_message_uuid.is_nil()
        {
            bail!("Claude fork preparation has an invalid source or replacement mode");
        }
        (
            Some(fork.source_session_id.to_string()),
            serialize_cli_runtime_json(&serde_json::json!({
                "forkSourceTurnId": fork.source_turn_id,
                "forkBoundaryMessageUuid": fork.boundary_message_uuid,
            }))?,
        )
    } else {
        (
            None,
            serialize_cli_runtime_json(&serde_json::json!({
                "provider": "claude",
                "providerSessionId": "<redacted>"
            }))?,
        )
    };
    let prepared = store
        .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
            thread_binding: NewCliRuntimeThreadBinding {
                thread_id: request.thread_id,
                workspace_id: request.workspace_id,
                runtime_id: request.runtime_id,
                runtime_kind: "claude".to_owned(),
                native_thread_id: proposed_provider_session_id.to_string(),
                native_session_id: Some(proposed_provider_session_id.to_string()),
                native_root_thread_id: root,
                native_cwd: Some(request.cwd),
                native_model: request.model,
                resume_cursor_json: cursor,
                status: "active".to_owned(),
                created_at: request.prepared_at,
                updated_at: request.prepared_at,
            },
            proposed_provider_session_id: proposed_provider_session_id.to_string(),
            force_new: request.force_new,
        })
        .await
        .context("failed to prepare durable Claude provider session binding")?;
    let provider = prepared
        .binding
        .provider_session
        .context("prepared Claude binding is missing provider session metadata")?;
    let provider_session_id = Uuid::parse_str(provider.provider_session_id.as_str())
        .context("durable Claude provider session identity is not a UUID")?;
    if provider_session_id.is_nil() {
        bail!("durable Claude provider session identity cannot be nil");
    }
    if let Some(expected) = requested_fork.as_ref() {
        let cursor = deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(
            prepared.binding.resume_cursor_json.as_str(),
        )?;
        anyhow::ensure!(
            prepared.binding.native_root_thread_id.as_deref()
                == Some(expected.source_session_id.to_string().as_str())
                && cursor
                    .provider_fields
                    .get("forkSourceTurnId")
                    .and_then(serde_json::Value::as_str)
                    == Some(expected.source_turn_id.as_str())
                && cursor
                    .provider_fields
                    .get("forkBoundaryMessageUuid")
                    .and_then(serde_json::Value::as_str)
                    == Some(expected.boundary_message_uuid.to_string().as_str()),
            "concurrent Claude session preparation changed the accepted fork source or boundary"
        );
    }
    Ok(match prepared.mode {
        PreparedClaudeProviderSessionMode::New => {
            let cursor = deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(
                prepared.binding.resume_cursor_json.as_str(),
            )?;
            if let Some(boundary) = cursor
                .provider_fields
                .get("forkBoundaryMessageUuid")
                .and_then(serde_json::Value::as_str)
            {
                CliProviderContinuation::ClaudeFork {
                    source_session_id: Uuid::parse_str(
                        prepared
                            .binding
                            .native_root_thread_id
                            .as_deref()
                            .context("Claude fork has no source session ID")?,
                    )?,
                    boundary_message_uuid: Uuid::parse_str(boundary)
                        .context("Claude fork boundary is not a message UUID")?,
                    provider_session_id,
                }
            } else {
                CliProviderContinuation::ClaudeNew {
                    provider_session_id,
                }
            }
        }
        PreparedClaudeProviderSessionMode::Resume => CliProviderContinuation::ClaudeResume {
            provider_session_id,
        },
    })
}

const CONTEXT_RECEIPT_VERSION: u32 = 4;

#[cfg(test)]
type TurnGuardLookupRegistry = std::sync::Mutex<
    std::collections::HashMap<(usize, String), std::sync::Weak<TurnGuardLookupState>>,
>;

#[cfg(test)]
#[derive(Default)]
struct TurnGuardLookupState {
    scans: std::sync::atomic::AtomicUsize,
    pages: std::sync::Mutex<Vec<(usize, usize)>>,
}

#[cfg(test)]
static TURN_GUARD_LOOKUPS: std::sync::LazyLock<TurnGuardLookupRegistry> =
    std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) struct TurnGuardLookupObserver {
    key: (usize, String),
    state: std::sync::Arc<TurnGuardLookupState>,
}

#[cfg(test)]
impl TurnGuardLookupObserver {
    pub(crate) fn pages(&self) -> Vec<(usize, usize)> {
        self.state.pages.lock().unwrap().clone()
    }

    pub(crate) fn scans(&self) -> usize {
        self.state.scans.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl Drop for TurnGuardLookupObserver {
    fn drop(&mut self) {
        TURN_GUARD_LOOKUPS.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
pub(crate) fn observe_turn_guard_lookups(
    store: &CrudStore,
    workspace: &str,
) -> TurnGuardLookupObserver {
    let key = (
        store.database_connection().runtime_identity(),
        workspace.to_owned(),
    );
    let state = std::sync::Arc::new(TurnGuardLookupState::default());
    assert!(
        TURN_GUARD_LOOKUPS
            .lock()
            .unwrap()
            .insert(key.clone(), std::sync::Arc::downgrade(&state))
            .is_none(),
        "turn guard lookup observer already installed"
    );
    TurnGuardLookupObserver { key, state }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CliRuntimeDeliveredTurn {
    pub(crate) turn_id: String,
    /// Receipts written before this field existed belonged to the basis owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) thread_id: Option<String>,
    pub(crate) message_revision: u64,
    pub(crate) message_deleted: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CliRuntimeDeliveredSource {
    pub(crate) source_thread_id: String,
    pub(crate) scope: String,
    pub(crate) id: String,
    pub(crate) version: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CliRuntimeContextBasis {
    pub(crate) execution_thread_id: String,
    pub(crate) manifest_owner_thread_id: String,
    pub(crate) history_json: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) delivered_turns: Vec<CliRuntimeDeliveredTurn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) delivered_sources: Vec<CliRuntimeDeliveredSource>,
}

pub(crate) fn cli_runtime_context_basis(
    execution_thread_id: &str,
    manifest_owner_thread_id: &str,
    history_json: String,
    direct_sources: &[pioneer_agent::compaction::composition::ScopedHistorySource],
) -> CliRuntimeContextBasis {
    CliRuntimeContextBasis {
        execution_thread_id: execution_thread_id.to_owned(),
        // This manifest is the immutable accepted authority boundary. The
        // exact provider-visible projection is represented independently by
        // `delivered_sources`, so a later summary never masquerades as the raw
        // snapshot it replaced.
        manifest_owner_thread_id: manifest_owner_thread_id.to_owned(),
        history_json,
        delivered_turns: Vec::new(),
        delivered_sources: direct_sources
            .iter()
            .map(|source| CliRuntimeDeliveredSource {
                source_thread_id: source.thread.clone(),
                scope: source.source.scope.clone(),
                id: source.source.id.clone(),
                version: source.source.version.clone(),
            })
            .collect(),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CliRuntimeSentContextBasis {
    #[serde(flatten)]
    pub(crate) completed: CliRuntimeContextBasis,
    pub(crate) pending_turn: CliRuntimeDeliveredTurn,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct CliRuntimeContextReceipt {
    version: u32,
    native_thread_id: String,
    accepted_turn_id: String,
    accepted_turn_revision: u64,
    accepted_turn_deleted: bool,
    /// A start acknowledgement may carry the previous basis. It does not
    /// advance this completed frontier until canonical completion is proved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    continuation_head: Option<CliRuntimeDeliveredTurn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_owner_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_history_json: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_manifest_owner_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    delivered_turns: Vec<CliRuntimeDeliveredTurn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    delivered_sources: Vec<CliRuntimeDeliveredSource>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CliRuntimeResumeCursor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pioneer_context: Option<CliRuntimeContextReceipt>,
    /// Preserve provider-specific cursor fields added by either adapter. The
    /// Pioneer receipt is an extension of that cursor, not a replacement.
    #[serde(flatten)]
    provider_fields: std::collections::BTreeMap<String, serde_json::Value>,
}

const TURN_INPUT_CONTEXT_BASIS_FIELD: &str = "pioneerContextBasis";

/// Resolve the provider frontier past attempts that stopped before dispatch.
/// The caller holds the continuation lease. A durable CLI binding/attempt is
/// written before provider start; its presence forbids skipping an uncertain RPC.
pub(crate) async fn previous_delivered_parent_turn(
    store: &CrudStore,
    thread: &str,
    mut previous: Option<String>,
) -> Result<Option<String>> {
    use pioneer_protocol::{TurnKind, TurnStatus};
    // Terminal status alone is not proof of non-delivery. Failed/Interrupted
    // are safe only with the same no-dispatch checks used for Blocked below.
    let unsuccessful = |status| {
        matches!(
            status,
            TurnStatus::Blocked | TurnStatus::Failed | TurnStatus::Interrupted
        )
    };
    for _ in 0..16 {
        let Some(id) = previous.as_deref() else {
            return Ok(None);
        };
        let (_, turn) = store
            .get_turn(thread, id)
            .await?
            .context("CLI predecessor missing")?;
        if !unsuccessful(turn.status) || turn.message_revision != 0 || turn.message_deleted {
            return Ok(previous);
        }
        let (execution_thread, execution_turn, launch) = if turn.turn_kind == TurnKind::TaskRun {
            let run = store
                .get_task_run(id)
                .await?
                .context("CLI predecessor TaskRun missing")?;
            let task = store
                .get_task_record(&run.task_id)
                .await?
                .context("CLI predecessor Task missing")?;
            let Some(work) = task
                .metadata
                .as_ref()
                .and_then(|m| m.composer_work.as_ref())
            else {
                return Ok(previous);
            };
            let Some(child) = store.get_latest_task_run_turn(id).await? else {
                return Ok(previous);
            };
            if work.launch.thread_id != thread
                || !run.status.is_terminal()
                || run.attempt_number != 1
                || child.kind != pioneer_protocol::TaskRunTurnKind::Initial
                || child.sequence != 1
            {
                return Ok(previous);
            }
            let (_, launch) = store
                .get_turn(thread, &work.launch.turn_id)
                .await?
                .context("CLI predecessor Composer launch missing")?;
            let adjacent = store
                .turn_before_launch_and_intervening_by_creation_order(thread, id, id)
                .await?
                .context("CLI predecessor order missing")?
                .0;
            if adjacent.as_deref() != Some(launch.id.as_str())
                || launch.message_revision != 0
                || launch.message_deleted
            {
                return Ok(previous);
            }
            (child.thread_id, child.turn_id, launch.id)
        } else {
            (thread.to_owned(), id.to_owned(), id.to_owned())
        };
        let (_, execution) = store
            .get_turn(&execution_thread, &execution_turn)
            .await?
            .context("CLI predecessor execution missing")?;
        if !unsuccessful(execution.status)
            || execution.message_revision != 0
            || execution.message_deleted
            || !store
                .get_turn_execution(&execution_turn)
                .await?
                .is_some_and(|e| {
                    e.executor_kind == pioneer_crud::TurnExecutorKind::CliRuntime
                        && !e.status.is_active()
                })
            || store
                .get_cli_runtime_turn_binding(&execution_turn)
                .await?
                .is_some()
            || store
                .latest_cli_runtime_turn_attempt(&execution_turn)
                .await?
                .is_some()
        {
            return Ok(previous);
        }
        // Reuse the bounded timestamp/bucket seek, not a scan of thread history.
        // Later turns are expected here; only the immediate predecessor is needed.
        previous = store
            .turn_before_launch_and_intervening_by_creation_order(thread, &launch, id)
            .await?
            .context("CLI predecessor launch order missing")?
            .0;
    }
    bail!("too many undispatched CLI attempts; continuation requires inspection")
}

pub(crate) fn provider_receipt_head_state(
    binding: &CliRuntimeThreadBindingRecord,
    previous: Option<(&str, u64, bool)>,
) -> Result<(bool, bool)> {
    let cursor =
        deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(binding.resume_cursor_json.as_str())
            .context("CLI runtime resume cursor is malformed")?;
    let conflicts = match (cursor.pioneer_context.as_ref(), previous) {
        (Some(receipt), Some((turn_id, revision, deleted)))
            if receipt.accepted_turn_id == turn_id =>
        {
            receipt.native_thread_id != binding.native_thread_id
                || receipt.accepted_turn_revision != revision
                || receipt.accepted_turn_deleted != deleted
        }
        _ => false,
    };
    let covers = matches!(
        (cursor.pioneer_context.as_ref(), previous),
        (Some(receipt), Some((turn_id, _, _)))
            if receipt.version == CONTEXT_RECEIPT_VERSION
                && receipt.accepted_turn_id == turn_id
                && (receipt.completed_turn_id.as_deref() == Some(turn_id)
                    || receipt.delivered_turns.iter().any(|delivered| delivered.turn_id == turn_id))
                && receipt.context_owner_thread_id.as_deref() == Some(binding.thread_id.as_str())
                && receipt.context_history_json.is_some()
    );
    Ok((conflicts, covers))
}

#[cfg(test)]
pub(crate) fn provider_receipt_conflicts_with_head(
    binding: &CliRuntimeThreadBindingRecord,
    previous: Option<(&str, u64, bool)>,
) -> Result<bool> {
    Ok(provider_receipt_head_state(binding, previous)?.0)
}

/// A provider fork is already a complete conversation up to its source turn.
/// Its first new Pioneer turn may fail preflight or be cancelled before any
/// receipt exists. Keep the durable fork boundary as the proof for retry.
pub(crate) fn binding_has_prepared_child_fork(
    binding: &CliRuntimeThreadBindingRecord,
    source: &pioneer_crud::CliRuntimeTurnBindingRecord,
) -> Result<bool> {
    if binding.runtime_kind != "codex"
        || binding.status != "active"
        || binding.thread_id != source.thread_id
        || binding.workspace_id != source.workspace_id
        || binding.runtime_id != source.runtime_id
        || binding.runtime_kind != source.runtime_kind
        || source.status != "completed"
        || source.continuation_thread_id == source.thread_id
    {
        return Ok(false);
    }
    Ok(prepared_child_fork_source(binding)?
        .as_ref()
        .is_some_and(|(turn, boundary)| {
            turn == &source.turn_id && Some(boundary.as_str()) == source.native_turn_id.as_deref()
        })
        && binding.native_root_thread_id.as_deref() == Some(source.native_thread_id.as_str())
        && binding.native_thread_id != source.native_thread_id)
}

pub(crate) fn prepared_child_fork_source(
    binding: &CliRuntimeThreadBindingRecord,
) -> Result<Option<(String, String)>> {
    let cursor = deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(
        binding.resume_cursor_json.as_str(),
    )?;
    if binding.status != "active" || cursor.pioneer_context.is_some() {
        return Ok(None);
    }
    Ok(
        match (
            cursor
                .provider_fields
                .get("forkSourceTurnId")
                .and_then(serde_json::Value::as_str),
            cursor
                .provider_fields
                .get("forkBoundaryTurnId")
                .or_else(|| cursor.provider_fields.get("forkBoundaryMessageUuid"))
                .and_then(serde_json::Value::as_str),
        ) {
            (Some(source), Some(boundary)) if !source.is_empty() && !boundary.is_empty() => {
                Some((source.to_owned(), boundary.to_owned()))
            }
            _ => None,
        },
    )
}

pub(crate) struct PendingCodexForkIntent {
    pub(crate) source_turn_id: String,
    pub(crate) boundary_turn_id: String,
    pub(crate) marker: String,
}

pub(crate) fn pending_codex_fork_intent(
    binding: &CliRuntimeThreadBindingRecord,
) -> Result<Option<PendingCodexForkIntent>> {
    if binding.status != "fork_pending" {
        return Ok(None);
    }
    anyhow::ensure!(
        binding.runtime_kind == "codex",
        "non-Codex fork intent is unsupported"
    );
    let cursor = deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(
        binding.resume_cursor_json.as_str(),
    )?;
    let get = |name| {
        cursor
            .provider_fields
            .get(name)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .with_context(|| format!("pending Codex fork has no {name}"))
    };
    Ok(Some(PendingCodexForkIntent {
        source_turn_id: get("forkSourceTurnId")?,
        boundary_turn_id: get("forkBoundaryTurnId")?,
        marker: get("forkMarker")?,
    }))
}

pub(crate) fn confirmed_codex_fork_cursor(
    pending: &CliRuntimeThreadBindingRecord,
    fork_native_thread_id: &str,
) -> Result<String> {
    let mut cursor = deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(
        pending.resume_cursor_json.as_str(),
    )?;
    anyhow::ensure!(
        pending_codex_fork_intent(pending)?.is_some(),
        "Codex fork intent is no longer pending"
    );
    cursor.thread_id = Some(fork_native_thread_id.to_owned());
    serialize_cli_runtime_json(&cursor)
}

pub(crate) async fn completed_context_basis_from_turn_binding(
    store: &CrudStore,
    source: &pioneer_crud::CliRuntimeTurnBindingRecord,
) -> Result<Option<CliRuntimeContextBasis>> {
    if source.status != "completed" || source.native_turn_id.is_none() {
        return Ok(None);
    }
    let sent = sent_context_basis_from_input_mapping(source.input_mapping_json.as_str())?;
    let (mut basis, pending_turn) = if let Some(sent) = sent {
        if sent.pending_turn.turn_id != source.turn_id {
            bail!("CLI fork source context belongs to a different turn");
        }
        (sent.completed, sent.pending_turn)
    } else {
        // The context receipt was introduced after existing CLI sessions and
        // Task snapshots. Recover the immutable accepted history that the old
        // execution actually used; never substitute today's mutable thread
        // projection for that historical boundary.
        let frozen = if let Some(snapshot) = store
            .get_turn_runtime_snapshot(source.turn_id.as_str())
            .await?
        {
            Some((snapshot.thread_id, snapshot.history_json))
        } else if let Some(run_turn) = store
            .get_task_run_turn_by_turn(source.thread_id.as_str(), source.turn_id.as_str())
            .await?
        {
            store
                .get_task_run_conversation_snapshot(run_turn.run_id.as_str())
                .await?
                .map(|snapshot| (snapshot.conversation_thread_id, snapshot.history_json))
        } else {
            None
        };
        let Some((manifest_owner, history_json)) = frozen else {
            return Ok(None);
        };
        if history_json.trim_start().starts_with('[') {
            return Ok(None);
        }
        let descriptor: pioneer_compaction::frozen::FrozenHistoryRef =
            serde_json::from_str(history_json.as_str())?;
        let direct_sources = crate::compaction::frozen::frozen_history_direct_sources(
            store,
            source.workspace_id.as_str(),
            &descriptor,
        )
        .await?;
        let Some((_, turn)) = store
            .get_turn(source.thread_id.as_str(), source.turn_id.as_str())
            .await?
        else {
            return Ok(None);
        };
        // Prior history snapshots do not contain the separately submitted
        // current input. A legacy mapping proves that it was sent, while an
        // unedited revision-zero Turn proves which immutable input it was.
        // Any later edit/delete (or an earlier edit with no recorded sent
        // revision) requires a fresh history bridge.
        let mapping = match serde_json::from_str::<
            pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputMapping,
        >(source.input_mapping_json.as_str())
        {
            Ok(mapping) => mapping,
            Err(_) => return Ok(None),
        };
        if mapping.input.is_empty() || turn.message_revision != 0 || turn.message_deleted {
            return Ok(None);
        }
        (
            cli_runtime_context_basis(
                source.thread_id.as_str(),
                manifest_owner.as_str(),
                history_json,
                &direct_sources,
            ),
            CliRuntimeDeliveredTurn {
                turn_id: source.turn_id.clone(),
                thread_id: Some(source.thread_id.clone()),
                message_revision: turn.message_revision,
                message_deleted: turn.message_deleted,
            },
        )
    };
    if basis
        .delivered_turns
        .iter()
        .all(|turn| turn.turn_id != source.turn_id)
    {
        basis.delivered_turns.push(pending_turn);
    }
    basis.delivered_sources.extend(
        completed_turn_output_sources(
            store,
            source.workspace_id.as_str(),
            source.thread_id.as_str(),
            source.turn_id.as_str(),
        )
        .await?,
    );
    if source.continuation_thread_id != source.thread_id
        && let Some(run_turn) = store
            .get_task_run_turn_by_turn(source.thread_id.as_str(), source.turn_id.as_str())
            .await?
        && let Some(run) = store.get_task_run(run_turn.run_id.as_str()).await?
        && let Some(task) = store.get_task_record(run.task_id.as_str()).await?
        && let Some(work) = task
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.composer_work.as_ref())
    {
        // An older service turn sent its Composer launch separately from the
        // frozen prior history. Recover guards for that delivered input and
        // its occurrence too; otherwise an edit to either would be hidden by
        // the newer completed child turn when reconstructing a legacy basis.
        for parent_turn_id in [work.launch.turn_id.as_str(), run.id.as_str()] {
            let (_, parent_turn) = store
                .get_turn(source.continuation_thread_id.as_str(), parent_turn_id)
                .await?
                .with_context(|| format!("legacy Composer turn {parent_turn_id} is missing"))?;
            if parent_turn.message_revision != 0 || parent_turn.message_deleted {
                return Ok(None);
            }
            if basis.delivered_turns.iter().all(|turn| {
                turn.turn_id != parent_turn_id
                    || turn.thread_id.as_deref() != Some(source.continuation_thread_id.as_str())
            }) {
                basis.delivered_turns.push(CliRuntimeDeliveredTurn {
                    turn_id: parent_turn_id.to_owned(),
                    thread_id: Some(source.continuation_thread_id.clone()),
                    message_revision: parent_turn.message_revision,
                    message_deleted: parent_turn.message_deleted,
                });
            }
            if parent_turn_id == work.launch.turn_id {
                basis.delivered_sources.extend(
                    completed_turn_output_sources(
                        store,
                        source.workspace_id.as_str(),
                        source.continuation_thread_id.as_str(),
                        parent_turn_id,
                    )
                    .await?,
                );
            }
        }
    }
    basis.delivered_sources.sort_by(|left, right| {
        (&left.source_thread_id, &left.scope, &left.id, &left.version).cmp(&(
            &right.source_thread_id,
            &right.scope,
            &right.id,
            &right.version,
        ))
    });
    basis.delivered_sources.dedup();
    Ok(Some(basis))
}

pub(crate) async fn completed_context_basis_is_current(
    store: &CrudStore,
    workspace_id: &str,
    basis: &CliRuntimeContextBasis,
) -> Result<bool> {
    if !crate::compaction::frozen::validate_frozen_history_authority(
        store,
        workspace_id,
        basis.manifest_owner_thread_id.as_str(),
        basis.history_json.as_str(),
    )
    .await?
    {
        return Ok(false);
    }
    let authority: pioneer_compaction::frozen::FrozenHistoryRef =
        serde_json::from_str(basis.history_json.as_str())?;
    if authority.messages > 0 && basis.delivered_sources.is_empty() {
        return Ok(false);
    }
    if !validate_delivered_turn_guards(
        store,
        workspace_id,
        basis.execution_thread_id.as_str(),
        basis.delivered_turns.as_slice(),
    )
    .await?
    {
        return Ok(false);
    }
    let direct = basis
        .delivered_sources
        .iter()
        .map(|source| {
            (
                source.source_thread_id.clone(),
                pioneer_compaction::SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                },
            )
        })
        .collect::<Vec<_>>();
    crate::compaction::frozen::validate_direct_history_sources_current(store, workspace_id, &direct)
        .await
}

async fn validate_delivered_turn_guards(
    store: &CrudStore,
    workspace_id: &str,
    execution_thread_id: &str,
    turns: &[CliRuntimeDeliveredTurn],
) -> Result<bool> {
    #[cfg(not(test))]
    let _ = workspace_id;
    #[cfg(test)]
    let observer = TURN_GUARD_LOOKUPS
        .lock()
        .unwrap()
        .get(&(
            store.database_connection().runtime_identity(),
            workspace_id.to_owned(),
        ))
        .and_then(std::sync::Weak::upgrade);
    #[cfg(test)]
    if let Some(observer) = observer.as_ref() {
        observer
            .scans
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    let mut by_thread = std::collections::BTreeMap::<&str, Vec<&CliRuntimeDeliveredTurn>>::new();
    let mut unique = std::collections::BTreeSet::new();
    for turn in turns {
        let owner = turn.thread_id.as_deref().unwrap_or(execution_thread_id);
        if !unique.insert((owner, turn.turn_id.as_str())) {
            return Ok(false);
        }
        by_thread.entry(owner).or_default().push(turn);
    }
    for (owner, expected) in by_thread {
        let ids = expected
            .iter()
            .map(|turn| turn.turn_id.clone())
            .collect::<Vec<_>>();
        let mut actual = std::collections::BTreeMap::new();
        let mut start = 0usize;
        while start < ids.len() {
            let mut end = start;
            let mut bytes = 2usize;
            while end < ids.len() && end - start < pioneer_crud::TURN_MESSAGE_GUARD_PAGE_ROWS {
                let encoded = serde_json::to_vec(&ids[end])?;
                let separator = usize::from(end > start);
                if end > start
                    && bytes
                        .saturating_add(separator)
                        .saturating_add(encoded.len())
                        > pioneer_crud::TURN_MESSAGE_GUARD_PAGE_BYTES
                {
                    break;
                }
                ensure_turn_guard_size(encoded.len())?;
                bytes = bytes
                    .saturating_add(separator)
                    .saturating_add(encoded.len());
                end += 1;
            }
            let page = store
                .get_turn_message_guards_by_thread_and_ids(owner, &ids[start..end])
                .await?;
            #[cfg(test)]
            if let Some(observer) = observer.as_ref() {
                observer.pages.lock().unwrap().push((end - start, bytes));
            }
            for turn in page {
                actual.insert(
                    turn.id,
                    (
                        u64::try_from(turn.message_revision)
                            .context("persisted turn message revision is negative")?,
                        turn.message_deleted_at.is_some(),
                    ),
                );
            }
            start = end;
        }
        if actual.len() != expected.len()
            || !expected.iter().all(|turn| {
                actual.get(turn.turn_id.as_str())
                    == Some(&(turn.message_revision, turn.message_deleted))
            })
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) async fn claude_assistant_record_boundary(
    store: &CrudStore,
    source: &pioneer_crud::CliRuntimeTurnBindingRecord,
) -> Result<Option<Uuid>> {
    if source.runtime_kind != "claude" || source.status != "completed" {
        return Ok(None);
    }
    let event =
        latest_claude_source_event(store, source, "claude/assistant_record_boundary").await?;
    if let Some(event) = event {
        return Ok(Some({
            let value: serde_json::Value = serde_json::from_str(&event.payload_redacted_json)?;
            Uuid::parse_str(
                value
                    .get("messageUuid")
                    .and_then(serde_json::Value::as_str)
                    .context("Claude fork boundary event has no message UUID")?,
            )
            .context("Claude fork boundary is not a transcript record UUID")?
        }));
    }
    // Before the dedicated boundary row existed, debug-enabled canonical
    // events retained the adapter's result payload for this exact Pioneer and
    // provider turn. Only its assistantRecordUuid is a transcript boundary;
    // API message.id and synthetic native turn IDs are not substitutes.
    if let Some(boundary) = claude_terminal_record_uuid(store, source).await? {
        return Ok(Some(boundary));
    }
    // Earlier adapters retained the provider record UUID in a redacted final
    // assistant item when native capture was enabled. Keep the exact turn and
    // session filter, so a later parent answer cannot become this boundary.
    let item = latest_claude_source_event(store, source, "item_completed").await?;
    let Some(item) = item else { return Ok(None) };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&item.payload_redacted_json) else {
        return Ok(None);
    };
    if value
        .pointer("/native/method")
        .and_then(serde_json::Value::as_str)
        != Some("assistant/text")
    {
        return Ok(None);
    }
    value
        .pointer("/native/payload_redacted/uuid")
        .and_then(serde_json::Value::as_str)
        .map(|raw| Uuid::parse_str(raw).context("legacy Claude assistant record UUID is malformed"))
        .transpose()
}

/// Exact-turn terminal evidence is needed both before canonical completion and
/// when restoring a legacy completed child. The Completed status fence belongs
/// only to the caller that is about to fork from that child.
pub(crate) async fn claude_terminal_record_uuid(
    store: &CrudStore,
    source: &pioneer_crud::CliRuntimeTurnBindingRecord,
) -> Result<Option<Uuid>> {
    fn find_boundary(value: &serde_json::Value) -> Option<&str> {
        match value {
            serde_json::Value::Object(fields) => fields
                .get("assistantRecordUuid")
                .and_then(serde_json::Value::as_str)
                .or_else(|| fields.values().find_map(find_boundary)),
            serde_json::Value::Array(values) => values.iter().find_map(find_boundary),
            _ => None,
        }
    }
    let Some(event) = latest_claude_source_event(store, source, "turn_completed").await? else {
        return Ok(None);
    };
    // Older optional debug journals may contain a redacted or malformed
    // terminal payload. They cannot prove a boundary, but must not prevent
    // the exact-turn item evidence fallback below from being considered.
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&event.payload_redacted_json) else {
        return Ok(None);
    };
    find_boundary(&value)
        .map(|boundary| {
            Uuid::parse_str(boundary).context("Claude terminal boundary is not a transcript UUID")
        })
        .transpose()
}

async fn latest_claude_source_event(
    store: &CrudStore,
    source: &pioneer_crud::CliRuntimeTurnBindingRecord,
    method: &str,
) -> Result<Option<pioneer_crud::CliRuntimeNativeEventRecord>> {
    // Older canonical journals used the provider continuation owner (parent)
    // for a service child. The exact Pioneer turn and provider session filters
    // keep that compatibility lookup bounded to this answer.
    for thread in [
        source.thread_id.as_str(),
        source.continuation_thread_id.as_str(),
    ] {
        if let Some(event) = store
            .latest_cli_runtime_native_event(pioneer_crud::CliRuntimeNativeEventListFilter {
                runtime_id: Some(source.runtime_id.clone()),
                thread_id: Some(thread.to_owned()),
                turn_id: Some(source.turn_id.clone()),
                native_thread_id: Some(source.native_thread_id.clone()),
                native_turn_id: source.native_turn_id.clone(),
                native_method: Some(method.to_owned()),
                limit: Some(1),
            })
            .await?
        {
            return Ok(Some(event));
        }
        if source.thread_id == source.continuation_thread_id {
            break;
        }
    }
    Ok(None)
}

pub(crate) async fn binding_has_prepared_claude_fork(
    store: &CrudStore,
    binding: &CliRuntimeThreadBindingRecord,
    source: &pioneer_crud::CliRuntimeTurnBindingRecord,
) -> Result<bool> {
    if binding.runtime_kind != "claude"
        || source.runtime_kind != "claude"
        || binding.workspace_id != source.workspace_id
        || binding.runtime_id != source.runtime_id
        || binding.thread_id != source.thread_id
        || source.continuation_thread_id == source.thread_id
        || binding.native_root_thread_id.as_deref() != Some(source.native_thread_id.as_str())
        || binding.native_thread_id == source.native_thread_id
    {
        return Ok(false);
    }
    let Some((turn_id, boundary)) = prepared_child_fork_source(binding)? else {
        return Ok(false);
    };
    Ok(turn_id == source.turn_id
        && claude_assistant_record_boundary(store, source)
            .await?
            .is_some_and(|uuid| uuid.to_string() == boundary))
}

/// A service Task can append to the parent's CLI conversation without adding
/// a new parent Turn. Its receipt records the exact parent head observed
/// before that append, so a later Native parent Turn cannot be skipped.
pub(crate) async fn service_child_context_basis_from_binding(
    store: &CrudStore,
    binding: &CliRuntimeThreadBindingRecord,
    previous: Option<(&str, u64, bool)>,
) -> Result<Option<CliRuntimeContextBasis>> {
    let cursor =
        deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(binding.resume_cursor_json.as_str())
            .context("CLI runtime resume cursor is malformed")?;
    let Some(receipt) = cursor.pioneer_context else {
        return Ok(None);
    };
    let Some((turn_id, revision, deleted)) = previous else {
        return Ok(None);
    };
    if receipt.version != CONTEXT_RECEIPT_VERSION
        || receipt.native_thread_id != binding.native_thread_id
        || receipt.continuation_head.as_ref()
            != Some(&CliRuntimeDeliveredTurn {
                turn_id: turn_id.to_owned(),
                thread_id: None,
                message_revision: revision,
                message_deleted: deleted,
            })
    {
        return Ok(None);
    }
    let Some(child_turn) = store
        .get_cli_runtime_turn_binding(receipt.accepted_turn_id.as_str())
        .await?
    else {
        return Ok(None);
    };
    if child_turn.thread_id == binding.thread_id
        || child_turn.continuation_thread_id != binding.thread_id
        || child_turn.workspace_id != binding.workspace_id
        || child_turn.runtime_id != binding.runtime_id
        || child_turn.runtime_kind != binding.runtime_kind
        || child_turn.native_thread_id != binding.native_thread_id
        || child_turn.native_turn_id.is_none()
        || child_turn.status != "completed"
    {
        return Ok(None);
    }
    let Some((_, persisted_child_turn)) = store
        .get_turn(child_turn.thread_id.as_str(), child_turn.turn_id.as_str())
        .await?
    else {
        return Ok(None);
    };
    if persisted_child_turn.message_revision != receipt.accepted_turn_revision
        || persisted_child_turn.message_deleted != receipt.accepted_turn_deleted
    {
        return Ok(None);
    }
    let Some(owner) = receipt.context_owner_thread_id.as_deref() else {
        return Ok(None);
    };
    current_context_basis_for_receipt(
        store,
        binding,
        binding.workspace_id.as_str(),
        owner,
        Some((
            child_turn.turn_id.as_str(),
            receipt.accepted_turn_revision,
            receipt.accepted_turn_deleted,
        )),
        &receipt,
    )
    .await
}

pub(crate) fn completed_context_basis_from_binding(
    binding: &CliRuntimeThreadBindingRecord,
) -> Result<Option<CliRuntimeContextBasis>> {
    let cursor =
        deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(&binding.resume_cursor_json)
            .context("CLI runtime resume cursor is malformed")?;
    let Some(receipt) = cursor.pioneer_context else {
        return Ok(None);
    };
    Ok(
        match (
            receipt.context_owner_thread_id,
            receipt.context_history_json,
        ) {
            (Some(execution_thread_id), Some(history_json)) => Some(CliRuntimeContextBasis {
                manifest_owner_thread_id: receipt
                    .context_manifest_owner_thread_id
                    .unwrap_or_else(|| execution_thread_id.clone()),
                execution_thread_id,
                history_json,
                delivered_turns: receipt.delivered_turns,
                delivered_sources: receipt.delivered_sources,
            }),
            (None, None) => None,
            _ => bail!("CLI runtime context receipt is incomplete"),
        },
    )
}

pub(crate) fn persist_sent_context_basis_in_input_mapping(
    input_mapping_json: &str,
    basis: &CliRuntimeSentContextBasis,
) -> Result<String> {
    let mut value: serde_json::Value = serde_json::from_str(input_mapping_json)
        .context("CLI runtime input mapping is malformed")?;
    let object = value
        .as_object_mut()
        .context("CLI runtime input mapping must be an object")?;
    object.insert(
        TURN_INPUT_CONTEXT_BASIS_FIELD.to_owned(),
        serde_json::to_value(basis)?,
    );
    serialize_cli_runtime_json(&value)
}

pub(crate) fn sent_context_basis_from_input_mapping(
    input_mapping_json: &str,
) -> Result<Option<CliRuntimeSentContextBasis>> {
    let value: serde_json::Value = serde_json::from_str(input_mapping_json)
        .context("CLI runtime input mapping is malformed")?;
    let Some(object) = value.as_object() else {
        bail!("CLI runtime input mapping must be an object");
    };
    object
        .get(TURN_INPUT_CONTEXT_BASIS_FIELD)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .context("CLI runtime sent-context basis is malformed")
}

#[cfg(test)]
pub(crate) async fn binding_has_current_context(
    store: &CrudStore,
    binding: &CliRuntimeThreadBindingRecord,
    workspace_id: &str,
    execution_thread_id: &str,
    previous_turn: Option<(&str, u64, bool)>,
) -> Result<bool> {
    Ok(current_context_basis_from_binding(
        store,
        binding,
        workspace_id,
        execution_thread_id,
        previous_turn,
    )
    .await?
    .is_some())
}

pub(crate) async fn current_context_basis_from_binding(
    store: &CrudStore,
    binding: &CliRuntimeThreadBindingRecord,
    workspace_id: &str,
    execution_thread_id: &str,
    previous_turn: Option<(&str, u64, bool)>,
) -> Result<Option<CliRuntimeContextBasis>> {
    // Decode before inspecting the Pioneer head. A missing head means the
    // conversation needs bootstrap, but it must never make a corrupt provider
    // cursor silently replaceable.
    let cursor =
        deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(&binding.resume_cursor_json)
            .context("CLI runtime resume cursor is malformed")?;
    let Some(receipt) = cursor.pioneer_context else {
        return Ok(None);
    };
    current_context_basis_for_receipt(
        store,
        binding,
        workspace_id,
        execution_thread_id,
        previous_turn,
        &receipt,
    )
    .await
}

async fn current_context_basis_for_receipt(
    store: &CrudStore,
    binding: &CliRuntimeThreadBindingRecord,
    workspace_id: &str,
    execution_thread_id: &str,
    previous_turn: Option<(&str, u64, bool)>,
    receipt: &CliRuntimeContextReceipt,
) -> Result<Option<CliRuntimeContextBasis>> {
    let Some((previous_turn_id, previous_turn_revision, previous_turn_deleted)) = previous_turn
    else {
        return Ok(None);
    };
    if !(receipt.version == CONTEXT_RECEIPT_VERSION
        && receipt.native_thread_id == binding.native_thread_id
        && receipt.accepted_turn_id == previous_turn_id
        && receipt.accepted_turn_revision == previous_turn_revision
        && receipt.accepted_turn_deleted == previous_turn_deleted)
    {
        return Ok(None);
    }
    if receipt.completed_turn_id.as_deref() != Some(previous_turn_id)
        && !receipt.delivered_turns.iter().any(|turn| {
            turn.turn_id == previous_turn_id
                && turn.message_revision == previous_turn_revision
                && turn.message_deleted == previous_turn_deleted
        })
    {
        return Ok(None);
    }
    let Some(accepted) = store.get_cli_runtime_turn_binding(previous_turn_id).await? else {
        return Ok(None);
    };
    if accepted.status != "completed"
        || accepted.native_thread_id != binding.native_thread_id
        || accepted.continuation_thread_id != binding.thread_id
    {
        return Ok(None);
    }
    let (Some(execution_owner), Some(history_json)) = (
        receipt.context_owner_thread_id.as_deref(),
        receipt.context_history_json.as_deref(),
    ) else {
        // The start acknowledgement is recovery metadata for the accepted
        // provider turn, not evidence that a later turn may resume the whole
        // canonical conversation without bootstrap.
        return Ok(None);
    };
    if execution_owner != execution_thread_id {
        return Ok(None);
    }
    let manifest_owner = receipt
        .context_manifest_owner_thread_id
        .as_deref()
        .unwrap_or(execution_owner);
    let basis = CliRuntimeContextBasis {
        execution_thread_id: execution_owner.to_owned(),
        manifest_owner_thread_id: manifest_owner.to_owned(),
        history_json: history_json.to_owned(),
        delivered_turns: receipt.delivered_turns.clone(),
        delivered_sources: receipt.delivered_sources.clone(),
    };
    Ok(
        completed_context_basis_is_current(store, workspace_id, &basis)
            .await?
            .then_some(basis),
    )
}

fn ensure_turn_guard_size(encoded_id_bytes: usize) -> Result<()> {
    if encoded_id_bytes.saturating_add(2) > pioneer_crud::TURN_MESSAGE_GUARD_PAGE_BYTES {
        bail!("one provider continuity turn guard exceeds validation bound");
    }
    Ok(())
}

pub(crate) async fn record_cli_runtime_context_receipt(
    store: &CrudStore,
    thread_id: &str,
    native_thread_id: &str,
    accepted_turn_id: &str,
    accepted_turn_revision: u64,
    accepted_turn_deleted: bool,
    continuation_head: Option<CliRuntimeDeliveredTurn>,
    updated_at: DateTimeWithTimeZone,
) -> Result<CliRuntimeThreadBindingRecord> {
    let binding = store
        .get_cli_runtime_thread_binding(thread_id)
        .await?
        .context("CLI runtime thread binding is missing for context receipt")?;
    if binding.native_thread_id != native_thread_id {
        bail!("CLI runtime thread changed before context delivery was confirmed");
    }
    let mut cursor =
        deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(binding.resume_cursor_json.as_str())
            .context(
                "CLI runtime resume cursor is malformed; refusing to replace provider metadata",
            )?;
    cursor.thread_id = Some(native_thread_id.to_owned());
    let completed = cursor.pioneer_context.take();
    cursor.pioneer_context = Some(CliRuntimeContextReceipt {
        version: CONTEXT_RECEIPT_VERSION,
        native_thread_id: native_thread_id.to_owned(),
        accepted_turn_id: accepted_turn_id.to_owned(),
        accepted_turn_revision,
        accepted_turn_deleted,
        completed_turn_id: completed
            .as_ref()
            .and_then(|receipt| receipt.completed_turn_id.clone()),
        continuation_head,
        context_owner_thread_id: completed
            .as_ref()
            .and_then(|receipt| receipt.context_owner_thread_id.clone()),
        context_history_json: completed
            .as_ref()
            .and_then(|receipt| receipt.context_history_json.clone()),
        context_manifest_owner_thread_id: completed
            .as_ref()
            .and_then(|receipt| receipt.context_manifest_owner_thread_id.clone()),
        delivered_turns: completed
            .as_ref()
            .map(|receipt| receipt.delivered_turns.clone())
            .unwrap_or_default(),
        delivered_sources: completed
            .map(|receipt| receipt.delivered_sources)
            .unwrap_or_default(),
    });
    store
        .update_cli_runtime_thread_resume_cursor(
            thread_id,
            native_thread_id,
            binding.resume_cursor_json.as_str(),
            serialize_cli_runtime_json(&cursor)?,
            updated_at,
        )
        .await
}

pub(crate) async fn record_cli_runtime_completed_context(
    store: &CrudStore,
    thread_id: &str,
    native_thread_id: &str,
    accepted_turn: CliRuntimeDeliveredTurn,
    sent_basis: CliRuntimeSentContextBasis,
    updated_at: DateTimeWithTimeZone,
) -> Result<CliRuntimeThreadBindingRecord> {
    let binding = store
        .get_cli_runtime_thread_binding(thread_id)
        .await?
        .context("CLI runtime thread binding is missing for completed context")?;
    if binding.native_thread_id != native_thread_id {
        bail!("CLI runtime thread changed before completed context was recorded");
    }
    let mut cursor =
        deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(binding.resume_cursor_json.as_str())
            .context("CLI runtime resume cursor is malformed")?;
    // A Task's service child may execute a turn in its parent's provider
    // conversation. The captured history belongs to that execution child,
    // while the provider binding belongs to the parent. Check the durable
    // turn binding instead of assuming both Pioneer IDs are identical.
    let turn_binding = store
        .get_cli_runtime_turn_binding(accepted_turn.turn_id.as_str())
        .await?
        .context("CLI runtime accepted turn binding is missing")?;
    if turn_binding.thread_id
        != accepted_turn
            .thread_id
            .as_deref()
            .unwrap_or(sent_basis.completed.execution_thread_id.as_str())
        || turn_binding.continuation_thread_id != thread_id
        || turn_binding.native_thread_id != native_thread_id
    {
        bail!("CLI runtime sent context does not match its durable turn binding");
    }
    let execution_thread_id = turn_binding.thread_id.as_str();
    let authority: pioneer_compaction::frozen::FrozenHistoryRef =
        serde_json::from_str(&sent_basis.completed.history_json)
            .context("CLI runtime accepted context boundary is malformed")?;
    if authority.messages > 0 && sent_basis.completed.delivered_sources.is_empty() {
        bail!("CLI runtime sent context is missing its direct projection sources");
    }
    if sent_basis.pending_turn != accepted_turn {
        bail!("CLI runtime turn changed after its context was sent");
    }
    let mut delivered_turns = sent_basis.completed.delivered_turns;
    if delivered_turns
        .last()
        .is_none_or(|turn| turn.turn_id != sent_basis.pending_turn.turn_id)
    {
        delivered_turns.push(sent_basis.pending_turn);
    }
    let mut delivered_sources = sent_basis.completed.delivered_sources;
    delivered_sources.extend(
        completed_turn_output_sources(
            store,
            binding.workspace_id.as_str(),
            execution_thread_id,
            accepted_turn.turn_id.as_str(),
        )
        .await?,
    );
    delivered_sources.sort_by(|left, right| {
        (&left.source_thread_id, &left.scope, &left.id, &left.version).cmp(&(
            &right.source_thread_id,
            &right.scope,
            &right.id,
            &right.version,
        ))
    });
    delivered_sources.dedup();
    cursor.thread_id = Some(native_thread_id.to_owned());
    let continuation_head = cursor
        .pioneer_context
        .as_ref()
        .and_then(|receipt| receipt.continuation_head.clone());
    cursor.pioneer_context = Some(CliRuntimeContextReceipt {
        version: CONTEXT_RECEIPT_VERSION,
        native_thread_id: native_thread_id.to_owned(),
        accepted_turn_id: accepted_turn.turn_id.clone(),
        accepted_turn_revision: accepted_turn.message_revision,
        accepted_turn_deleted: accepted_turn.message_deleted,
        completed_turn_id: Some(accepted_turn.turn_id.clone()),
        continuation_head,
        context_owner_thread_id: Some(sent_basis.completed.execution_thread_id),
        context_history_json: Some(sent_basis.completed.history_json),
        context_manifest_owner_thread_id: Some(sent_basis.completed.manifest_owner_thread_id),
        delivered_turns,
        delivered_sources,
    });
    store
        .update_cli_runtime_thread_resume_cursor(
            thread_id,
            native_thread_id,
            binding.resume_cursor_json.as_str(),
            serialize_cli_runtime_json(&cursor)?,
            updated_at,
        )
        .await
}

async fn completed_turn_output_sources(
    store: &CrudStore,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<Vec<CliRuntimeDeliveredSource>> {
    // Completion extends the pre-send input guard only with identities emitted
    // by this accepted provider turn. Metadata pages are bounded and contain
    // no payloads; provider-context rows suppress their UI event aliases.
    let mut sources = Vec::new();
    let mut provider_items = std::collections::BTreeSet::new();
    for kind in [
        pioneer_crud::compaction::PagedSource::Input,
        pioneer_crud::compaction::PagedSource::ProviderContext,
        pioneer_crud::compaction::PagedSource::Event,
    ] {
        let mut after = 0_i64;
        loop {
            let page = store
                .compaction_source_metadata_page(workspace_id, thread_id, turn_id, kind, after)
                .await?;
            if page.entries.is_empty() {
                break;
            }
            for row in page.entries {
                let delivered = match kind {
                    pioneer_crud::compaction::PagedSource::ProviderContext => {
                        if let Some(item_id) = row.item_id.as_ref() {
                            provider_items.insert(item_id.clone());
                        }
                        true
                    }
                    pioneer_crud::compaction::PagedSource::Event => {
                        let projection_kind = row.projection_kind.as_deref().context(
                            "completed CLI event is missing canonical projection metadata",
                        )?;
                        !row.item_id
                            .as_ref()
                            .is_some_and(|item_id| provider_items.contains(item_id))
                            && matches!(
                                projection_kind,
                                "assistant"
                                    | "reasoning"
                                    | "observation"
                                    | "tool_observation"
                                    | "update"
                            )
                    }
                    pioneer_crud::compaction::PagedSource::Input => true,
                };
                if delivered {
                    sources.push(CliRuntimeDeliveredSource {
                        source_thread_id: thread_id.to_owned(),
                        scope: row.reference.scope,
                        id: row.reference.id,
                        version: row.reference.version,
                    });
                }
            }
            if page.next_sequence <= after {
                bail!("completed CLI source pagination did not advance");
            }
            after = page.next_sequence;
        }
    }
    Ok(sources)
}

#[derive(Debug, Clone)]
pub(crate) struct CLIAgentRuntimeThreadBindingOpenRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub cwd: String,
    pub model: Option<String>,
    pub approval_policy: Option<String>,
    pub sandbox: Option<serde_json::Value>,
    pub permissions: Option<String>,
    pub service_tier: Option<String>,
    pub resume_existing: bool,
    pub request_timeout: Duration,
    pub opened_at: DateTimeWithTimeZone,
}

impl CLIAgentRuntimeThreadBindingOpenRequest {
    fn start_params(&self) -> CLIAgentRuntimeThreadOpenParams {
        CLIAgentRuntimeThreadOpenParams {
            cwd: self.cwd.clone(),
            model: self.model.clone(),
            approval_policy: self.approval_policy.clone(),
            sandbox: self.sandbox.clone(),
            permissions: self.permissions.clone(),
            service_tier: self.service_tier.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CLIAgentRuntimeThreadBindingOpenMode {
    Started,
    Resumed,
}

#[derive(Debug, Clone)]
pub(crate) struct CLIAgentRuntimeThreadBindingOpenResult {
    pub binding: CliRuntimeThreadBindingRecord,
    pub mode: CLIAgentRuntimeThreadBindingOpenMode,
}

#[async_trait]
pub(crate) trait CLIAgentRuntimeThreadOpenClient: Send + Sync {
    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot>;

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot>;
}

#[async_trait]
impl CLIAgentRuntimeThreadOpenClient for std::sync::Arc<dyn CLIAgentRuntimeSession> {
    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        CLIAgentRuntimeSession::start_thread(self.as_ref(), params, timeout).await
    }

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        CLIAgentRuntimeSession::resume_thread(self.as_ref(), native_thread_id, params, timeout)
            .await
    }
}

pub(crate) async fn open_cli_runtime_thread_binding<C>(
    store: &CrudStore,
    client: &C,
    request: CLIAgentRuntimeThreadBindingOpenRequest,
) -> Result<CLIAgentRuntimeThreadBindingOpenResult>
where
    C: CLIAgentRuntimeThreadOpenClient + ?Sized,
{
    validate_generic_open_request(&request)?;

    let existing = store
        .get_cli_runtime_thread_binding(request.thread_id.as_str())
        .await
        .with_context(|| {
            format!(
                "failed to read CLI runtime binding for thread `{}`",
                request.thread_id
            )
        })?;
    if let Some(existing) = existing.as_ref() {
        validate_existing_generic_binding(existing, &request)?;
    }

    let start_params = request.start_params();
    let (mode, opened, preserve_native_metadata) = match existing.as_ref() {
        Some(binding) if request.resume_existing => {
            let opened = client
                .resume_thread(
                    binding.native_thread_id.as_str(),
                    start_params,
                    request.request_timeout,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to resume native CLI runtime thread `{}` for Pioneer thread `{}`",
                        binding.native_thread_id, request.thread_id
                    )
                })?;
            if opened.native_thread_id != binding.native_thread_id {
                bail!(
                    "CLI runtime thread resume returned native thread `{}` for stored native thread `{}`",
                    opened.native_thread_id,
                    binding.native_thread_id
                );
            }
            (CLIAgentRuntimeThreadBindingOpenMode::Resumed, opened, true)
        }
        Some(_) | None => {
            let opened = client
                .start_thread(start_params, request.request_timeout)
                .await
                .with_context(|| {
                    format!(
                        "failed to start native CLI runtime thread for Pioneer thread `{}`",
                        request.thread_id
                    )
                })?;
            let preserve_native_metadata = existing
                .as_ref()
                .is_some_and(|binding| binding.native_thread_id == opened.native_thread_id);
            (
                CLIAgentRuntimeThreadBindingOpenMode::Started,
                opened,
                preserve_native_metadata,
            )
        }
    };

    let binding = store
        .upsert_cli_runtime_thread_binding(generic_thread_binding_from_opened(
            &request,
            existing.as_ref(),
            &opened,
            preserve_native_metadata,
        )?)
        .await
        .with_context(|| {
            format!(
                "failed to persist CLI runtime binding for thread `{}`",
                request.thread_id
            )
        })?;

    Ok(CLIAgentRuntimeThreadBindingOpenResult { binding, mode })
}

fn generic_thread_binding_from_opened(
    request: &CLIAgentRuntimeThreadBindingOpenRequest,
    existing: Option<&CliRuntimeThreadBindingRecord>,
    opened: &CLIAgentRuntimeThreadOpenSnapshot,
    preserve_native_metadata: bool,
) -> Result<NewCliRuntimeThreadBinding> {
    let resume_cursor_json = if preserve_native_metadata {
        let existing = existing.context(
            "resumed CLI runtime thread is missing the binding whose metadata must be preserved",
        )?;
        if existing.native_thread_id != opened.native_thread_id {
            bail!(
                "resumed CLI runtime returned provider thread `{}` instead of `{}`",
                opened.native_thread_id,
                existing.native_thread_id
            );
        }
        let mut cursor = deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(
            existing.resume_cursor_json.as_str(),
        )
        .context("existing CLI runtime resume cursor is malformed")?;
        cursor.thread_id = Some(opened.native_thread_id.clone());
        serialize_cli_runtime_json(&cursor)?
    } else {
        // A new provider conversation must not inherit opaque cursor fields or
        // a Pioneer receipt belonging to the superseded provider thread.
        serialize_cli_runtime_json(&CliRuntimeResumeCursor {
            thread_id: Some(opened.native_thread_id.clone()),
            ..Default::default()
        })?
    };
    Ok(NewCliRuntimeThreadBinding {
        thread_id: request.thread_id.clone(),
        workspace_id: request.workspace_id.clone(),
        runtime_id: request.runtime_id.clone(),
        runtime_kind: request.runtime_kind.clone(),
        native_thread_id: opened.native_thread_id.clone(),
        native_session_id: if request.runtime_kind == "claude" {
            Some(opened.native_thread_id.clone())
        } else {
            preserve_native_metadata
                .then(|| existing.and_then(|binding| binding.native_session_id.clone()))
                .flatten()
        },
        native_root_thread_id: preserve_native_metadata
            .then(|| existing.and_then(|binding| binding.native_root_thread_id.clone()))
            .flatten(),
        native_cwd: opened.cwd.clone().or_else(|| Some(request.cwd.clone())),
        native_model: opened.model.clone().or_else(|| request.model.clone()),
        resume_cursor_json,
        status: "active".to_owned(),
        created_at: existing
            .map(|binding| binding.created_at)
            .unwrap_or(request.opened_at),
        updated_at: request.opened_at,
    })
}

fn validate_generic_open_request(request: &CLIAgentRuntimeThreadBindingOpenRequest) -> Result<()> {
    for (label, value) in [
        ("workspace_id", request.workspace_id.as_str()),
        ("thread_id", request.thread_id.as_str()),
        ("runtime_id", request.runtime_id.as_str()),
        ("runtime_kind", request.runtime_kind.as_str()),
        ("cwd", request.cwd.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("CLI runtime thread binding request `{label}` cannot be empty");
        }
    }
    Ok(())
}

fn validate_existing_generic_binding(
    existing: &CliRuntimeThreadBindingRecord,
    request: &CLIAgentRuntimeThreadBindingOpenRequest,
) -> Result<()> {
    if existing.workspace_id != request.workspace_id {
        bail!(
            "CLI runtime binding for thread `{}` belongs to workspace `{}` not `{}`",
            request.thread_id,
            existing.workspace_id,
            request.workspace_id
        );
    }
    if request.resume_existing
        && (existing.runtime_id != request.runtime_id
            || existing.runtime_kind != request.runtime_kind)
    {
        bail!(
            "CLI runtime binding for thread `{}` belongs to runtime `{}`/`{}` not `{}`/`{}`",
            request.thread_id,
            existing.runtime_id,
            existing.runtime_kind,
            request.runtime_id,
            request.runtime_kind
        );
    }
    if existing.status != "active" {
        bail!(
            "CLI runtime binding for thread `{}` is `{}`",
            request.thread_id,
            existing.status
        );
    }
    if request.resume_existing {
        deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(
            existing.resume_cursor_json.as_str(),
        )
        .context("existing CLI runtime resume cursor is malformed")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CLIAgentRuntimeThreadBindingOpenMode, CLIAgentRuntimeThreadBindingOpenRequest,
        CLIAgentRuntimeThreadOpenClient, binding_has_current_context,
        binding_has_prepared_child_fork, open_cli_runtime_thread_binding,
        prepared_child_fork_source, provider_receipt_conflicts_with_head,
        record_cli_runtime_context_receipt,
    };
    use crate::cli_runtime::manager::{
        CLIAgentRuntimeThreadOpenParams, CLIAgentRuntimeThreadOpenSnapshot,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        CliRuntimeThreadBindingRecord, CliRuntimeTurnBindingRecord, CrudStore,
        NewCliRuntimeThreadBinding,
    };
    use sea_orm::entity::prelude::DateTimeWithTimeZone;
    use sea_orm::{Database, DatabaseConnection};
    use serde_json::json;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Debug)]
    struct FakeCliRuntimeThreadClient {
        starts: Mutex<Vec<CLIAgentRuntimeThreadOpenParams>>,
        resumes: Mutex<Vec<(String, CLIAgentRuntimeThreadOpenParams)>>,
        start_result: Mutex<std::result::Result<CLIAgentRuntimeThreadOpenSnapshot, String>>,
        resume_result: Mutex<std::result::Result<CLIAgentRuntimeThreadOpenSnapshot, String>>,
    }

    #[async_trait]
    impl CLIAgentRuntimeThreadOpenClient for FakeCliRuntimeThreadClient {
        async fn start_thread(
            &self,
            params: CLIAgentRuntimeThreadOpenParams,
            _timeout: Duration,
        ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
            self.starts.lock().expect("starts lock").push(params);
            self.start_result
                .lock()
                .expect("start result lock")
                .clone()
                .map_err(anyhow::Error::msg)
        }

        async fn resume_thread(
            &self,
            native_thread_id: &str,
            params: CLIAgentRuntimeThreadOpenParams,
            _timeout: Duration,
        ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
            self.resumes
                .lock()
                .expect("resumes lock")
                .push((native_thread_id.to_owned(), params));
            self.resume_result
                .lock()
                .expect("resume result lock")
                .clone()
                .map_err(anyhow::Error::msg)
        }
    }

    impl FakeCliRuntimeThreadClient {
        fn new() -> Self {
            Self {
                starts: Mutex::new(Vec::new()),
                resumes: Mutex::new(Vec::new()),
                start_result: Mutex::new(Ok(open_snapshot("cli-thread-started"))),
                resume_result: Mutex::new(Ok(open_snapshot("cli-thread-existing"))),
            }
        }

        fn set_resume_error(&self, message: &str) {
            *self.resume_result.lock().expect("resume result lock") = Err(message.to_owned());
        }
    }

    async fn setup_store() -> (DatabaseConnection, CrudStore) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory connection");
        Migrator::up(&connection, None)
            .await
            .expect("migrations should apply");
        let store = CrudStore::new(connection.clone());
        (connection, store)
    }

    fn open_request(thread_id: &str, opened_at: i64) -> CLIAgentRuntimeThreadBindingOpenRequest {
        CLIAgentRuntimeThreadBindingOpenRequest {
            workspace_id: "ws_cli_binding".to_owned(),
            thread_id: thread_id.to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            cwd: "/tmp/project".to_owned(),
            model: Some("gpt-5".to_owned()),
            approval_policy: Some("on-request".to_owned()),
            sandbox: Some(json!("workspace-write")),
            permissions: None,
            service_tier: None,
            resume_existing: true,
            request_timeout: Duration::from_secs(5),
            opened_at: unix_to_datetime(opened_at),
        }
    }

    fn open_snapshot(native_thread_id: &str) -> CLIAgentRuntimeThreadOpenSnapshot {
        CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: native_thread_id.to_owned(),
            cwd: Some("/tmp/project".to_owned()),
            model: Some("gpt-5".to_owned()),
            raw: json!({ "thread": { "id": native_thread_id } }),
        }
    }

    fn unix_to_datetime(timestamp: i64) -> DateTimeWithTimeZone {
        chrono::DateTime::from_timestamp(timestamp, 0)
            .expect("valid timestamp")
            .fixed_offset()
    }

    #[test]
    fn provider_receipt_detects_a_changed_head() {
        let at = unix_to_datetime(100);
        let binding = CliRuntimeThreadBindingRecord {
            thread_id: "parent".into(),
            workspace_id: "workspace".into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            native_thread_id: "provider-parent".into(),
            native_session_id: None,
            native_root_thread_id: None,
            native_cwd: None,
            native_model: None,
            resume_cursor_json: r#"{"threadId":"provider-parent"}"#.into(),
            status: "active".into(),
            mcp: None,
            provider_session: None,
            created_at: at,
            updated_at: at,
        };
        assert!(
            !provider_receipt_conflicts_with_head(&binding, Some(("parent-turn", 1, false)),)
                .unwrap()
        );
        let mut with_receipt = binding.clone();
        with_receipt.resume_cursor_json = r#"{"threadId":"provider-parent","pioneerContext":{"version":4,"nativeThreadId":"provider-parent","acceptedTurnId":"parent-turn","acceptedTurnRevision":0,"acceptedTurnDeleted":false}}"#.into();
        assert!(
            provider_receipt_conflicts_with_head(&with_receipt, Some(("parent-turn", 1, false)),)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cli_runtime_binding_first_start_persists_thread_binding() {
        let (_connection, store) = setup_store().await;
        let client = FakeCliRuntimeThreadClient::new();

        let result =
            open_cli_runtime_thread_binding(&store, &client, open_request("thread_cli_a", 100))
                .await
                .expect("first open should succeed");

        assert_eq!(result.mode, CLIAgentRuntimeThreadBindingOpenMode::Started);
        assert_eq!(result.binding.native_thread_id, "cli-thread-started");
        assert_eq!(result.binding.native_cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(result.binding.native_model.as_deref(), Some("gpt-5"));
        assert_eq!(
            result.binding.resume_cursor_json,
            r#"{"threadId":"cli-thread-started"}"#
        );
        assert_eq!(client.starts.lock().expect("starts lock").len(), 1);
        assert!(client.resumes.lock().expect("resumes lock").is_empty());
    }

    #[tokio::test]
    async fn pending_child_fork_never_opens_an_empty_provider_thread() {
        let (_connection, store) = setup_store().await;
        let at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "child".into(),
                workspace_id: "ws_cli_binding".into(),
                runtime_id: "codex".into(),
                runtime_kind: "codex".into(),
                native_thread_id: "parent-provider-thread".into(),
                native_session_id: None,
                native_root_thread_id: Some("parent-provider-thread".into()),
                native_cwd: Some("/tmp/project".into()),
                native_model: Some("gpt-5".into()),
                resume_cursor_json: r#"{"forkBoundaryTurnId":"completed-provider-turn"}"#.into(),
                status: "fork_pending".into(),
                created_at: at,
                updated_at: at,
            })
            .await
            .expect("persist fork fence");
        let client = FakeCliRuntimeThreadClient::new();
        let error = open_cli_runtime_thread_binding(&store, &client, open_request("child", 101))
            .await
            .expect_err("uncertain fork must remain fenced");
        assert!(error.to_string().contains("fork_pending"));
        assert!(client.starts.lock().unwrap().is_empty());
        assert!(client.resumes.lock().unwrap().is_empty());
    }

    #[test]
    fn confirmed_child_fork_remains_usable_before_its_first_new_turn() {
        let at = unix_to_datetime(100);
        let source = CliRuntimeTurnBindingRecord {
            turn_id: "child-first-answer".into(),
            thread_id: "child".into(),
            continuation_thread_id: "parent".into(),
            workspace_id: "workspace".into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            native_thread_id: "provider-parent".into(),
            native_turn_id: Some("provider-boundary".into()),
            request_id: None,
            status: "completed".into(),
            model: None,
            cwd: None,
            sandbox_json: None,
            approval_policy: None,
            input_mapping_json: "{}".into(),
            mcp: None,
            native_goal_status: None,
            native_goal_turn_id: None,
            native_goal_observed_at: None,
            created_at: at,
            updated_at: at,
        };
        let binding = CliRuntimeThreadBindingRecord {
            thread_id: "child".into(),
            workspace_id: "workspace".into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            native_thread_id: "provider-child".into(),
            native_session_id: None,
            native_root_thread_id: Some("provider-parent".into()),
            native_cwd: None,
            native_model: None,
            resume_cursor_json: r#"{"threadId":"provider-child","forkSourceTurnId":"child-first-answer","forkBoundaryTurnId":"provider-boundary","forkMarker":"pioneer-cli-fork:nonce"}"#.into(),
            status: "active".into(),
            mcp: None,
            provider_session: None,
            created_at: at,
            updated_at: at,
        };
        assert!(binding_has_prepared_child_fork(&binding, &source).unwrap());
        assert_eq!(
            prepared_child_fork_source(&binding).unwrap(),
            Some(("child-first-answer".into(), "provider-boundary".into()))
        );
        let mut changed = source.clone();
        changed.native_turn_id = Some("later-parent-turn".into());
        assert!(!binding_has_prepared_child_fork(&binding, &changed).unwrap());
    }

    #[tokio::test]
    async fn cli_runtime_binding_resume_uses_stored_native_thread() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_resume".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "cli-thread-existing".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("gpt-5".to_owned()),
                resume_cursor_json: r#"{"threadId":"cli-thread-existing","providerOpaque":{"cursor":7},"pioneerContext":{"version":1,"nativeThreadId":"cli-thread-existing","acceptedTurnId":"turn-old","acceptedTurnRevision":0,"acceptedTurnDeleted":false}}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("seed binding should persist");
        let client = FakeCliRuntimeThreadClient::new();

        let result = open_cli_runtime_thread_binding(
            &store,
            &client,
            open_request("thread_cli_resume", 200),
        )
        .await
        .expect("resume should succeed");

        assert_eq!(result.mode, CLIAgentRuntimeThreadBindingOpenMode::Resumed);
        assert_eq!(result.binding.native_thread_id, "cli-thread-existing");
        assert_eq!(result.binding.created_at, opened_at);
        assert_eq!(result.binding.updated_at, unix_to_datetime(200));
        let resumed_cursor: serde_json::Value =
            serde_json::from_str(&result.binding.resume_cursor_json).unwrap();
        assert_eq!(resumed_cursor["providerOpaque"]["cursor"], 7);
        assert_eq!(
            resumed_cursor["pioneerContext"]["acceptedTurnId"],
            "turn-old"
        );
        let acknowledged = record_cli_runtime_context_receipt(
            &store,
            "thread_cli_resume",
            "cli-thread-existing",
            "turn-new",
            0,
            false,
            None,
            unix_to_datetime(201),
        )
        .await
        .expect("receipt after a real resume should persist");
        let acknowledged_cursor: serde_json::Value =
            serde_json::from_str(&acknowledged.resume_cursor_json).unwrap();
        assert_eq!(acknowledged_cursor["providerOpaque"]["cursor"], 7);
        assert_eq!(
            acknowledged_cursor["pioneerContext"]["acceptedTurnId"],
            "turn-new"
        );
        assert!(client.starts.lock().expect("starts lock").is_empty());
        let resumes = client.resumes.lock().expect("resumes lock");
        assert_eq!(resumes.len(), 1);
        assert_eq!(resumes[0].0, "cli-thread-existing");
    }

    #[tokio::test]
    async fn cli_runtime_binding_resume_error_keeps_existing_binding() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_error".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "cli-thread-existing".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("gpt-5".to_owned()),
                resume_cursor_json: r#"{"threadId":"cli-thread-existing"}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("seed binding should persist");
        let client = FakeCliRuntimeThreadClient::new();
        client.set_resume_error("native thread missing");

        let error =
            open_cli_runtime_thread_binding(&store, &client, open_request("thread_cli_error", 200))
                .await
                .expect_err("resume failure should surface");
        assert!(
            format!("{error:#}").contains("failed to resume native CLI runtime thread"),
            "resume error should be actionable"
        );

        let binding = store
            .get_cli_runtime_thread_binding("thread_cli_error")
            .await
            .expect("binding read should succeed")
            .expect("binding should remain");
        assert_eq!(binding.native_thread_id, "cli-thread-existing");
        assert_eq!(binding.updated_at, opened_at);
        assert!(client.starts.lock().expect("starts lock").is_empty());
        assert_eq!(client.resumes.lock().expect("resumes lock").len(), 1);
    }

    #[tokio::test]
    async fn cli_runtime_binding_rejects_malformed_cursor_before_provider_resume() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_bad_cursor".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "cli-thread-existing".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("gpt-5".to_owned()),
                resume_cursor_json: "{malformed".to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("malformed cursor fixture should persist");
        let client = FakeCliRuntimeThreadClient::new();
        let malformed = store
            .get_cli_runtime_thread_binding("thread_cli_bad_cursor")
            .await
            .unwrap()
            .unwrap();
        let continuity_error = binding_has_current_context(
            &store,
            &malformed,
            "ws_cli_binding",
            "thread_cli_bad_cursor",
            None,
        )
        .await
        .expect_err("a missing Pioneer head must not bypass malformed cursor validation");
        assert!(format!("{continuity_error:#}").contains("resume cursor is malformed"));
        let error = open_cli_runtime_thread_binding(
            &store,
            &client,
            open_request("thread_cli_bad_cursor", 200),
        )
        .await
        .expect_err("malformed cursor must not be replaced by an empty cursor");
        assert!(format!("{error:#}").contains("resume cursor is malformed"));
        assert!(client.starts.lock().expect("starts lock").is_empty());
        assert!(client.resumes.lock().expect("resumes lock").is_empty());
        let preserved = store
            .get_cli_runtime_thread_binding("thread_cli_bad_cursor")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(preserved.resume_cursor_json, "{malformed");
        assert_eq!(preserved.updated_at, opened_at);
    }

    #[tokio::test]
    async fn cli_runtime_binding_non_resumable_runtime_starts_new_native_thread() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_non_resumable".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "claude".to_owned(),
                runtime_kind: "claude".to_owned(),
                native_thread_id: "cli-thread-existing".to_owned(),
                native_session_id: Some("native-session-existing".to_owned()),
                native_root_thread_id: Some("native-root-existing".to_owned()),
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("sonnet".to_owned()),
                resume_cursor_json: r#"{"threadId":"cli-thread-existing"}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("seed binding should persist");
        let client = FakeCliRuntimeThreadClient::new();
        let mut request = open_request("thread_cli_non_resumable", 200);
        request.runtime_id = "claude".to_owned();
        request.runtime_kind = "claude".to_owned();
        request.model = Some("sonnet".to_owned());
        request.resume_existing = false;

        let result = open_cli_runtime_thread_binding(&store, &client, request)
            .await
            .expect("non-resumable runtime should start a fresh native thread");

        assert_eq!(result.mode, CLIAgentRuntimeThreadBindingOpenMode::Started);
        assert_eq!(result.binding.native_thread_id, "cli-thread-started");
        assert_eq!(
            result.binding.native_session_id.as_deref(),
            Some("cli-thread-started")
        );
        assert_eq!(result.binding.native_root_thread_id, None);
        assert_eq!(result.binding.created_at, opened_at);
        assert_eq!(result.binding.updated_at, unix_to_datetime(200));
        assert_eq!(client.starts.lock().expect("starts lock").len(), 1);
        assert!(client.resumes.lock().expect("resumes lock").is_empty());
    }

    #[tokio::test]
    async fn claude_start_preserves_cursor_when_durable_uuid_is_unchanged() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_claude_same_uuid".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "claude".to_owned(),
                runtime_kind: "claude".to_owned(),
                native_thread_id: "claude-durable-uuid".to_owned(),
                native_session_id: Some("claude-durable-uuid".to_owned()),
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("sonnet".to_owned()),
                resume_cursor_json: r#"{"threadId":"claude-durable-uuid","providerOpaque":{"cursor":7}}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("Claude binding fixture should persist");
        let client = FakeCliRuntimeThreadClient::new();
        *client.start_result.lock().expect("start result lock") =
            Ok(open_snapshot("claude-durable-uuid"));
        let mut request = open_request("thread_claude_same_uuid", 200);
        request.runtime_id = "claude".to_owned();
        request.runtime_kind = "claude".to_owned();
        request.model = Some("sonnet".to_owned());
        request.resume_existing = false;

        let result = open_cli_runtime_thread_binding(&store, &client, request)
            .await
            .expect("Claude start should reopen its durable UUID");

        assert_eq!(result.mode, CLIAgentRuntimeThreadBindingOpenMode::Started);
        assert_eq!(result.binding.native_thread_id, "claude-durable-uuid");
        let cursor: serde_json::Value =
            serde_json::from_str(&result.binding.resume_cursor_json).unwrap();
        assert_eq!(cursor["providerOpaque"]["cursor"], 7);
        assert_eq!(client.starts.lock().expect("starts lock").len(), 1);
        assert!(client.resumes.lock().expect("resumes lock").is_empty());
    }

    #[tokio::test]
    async fn cli_runtime_context_receipt_requires_exact_provider_and_pioneer_heads() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_receipt".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "provider-thread-a".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("gpt-5".to_owned()),
                resume_cursor_json:
                    r#"{"threadId":"provider-thread-a","providerOpaque":{"cursor":7}}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("legacy binding fixture should persist");
        let legacy = store
            .get_cli_runtime_thread_binding("thread_cli_receipt")
            .await
            .unwrap()
            .unwrap();
        assert!(
            !binding_has_current_context(
                &store,
                &legacy,
                "ws_cli_binding",
                "thread_cli_receipt",
                Some(("turn-a", 0, false)),
            )
            .await
            .unwrap(),
            "the receipt validator alone cannot infer context from an old binding; the caller also checks the completed prior CLI turn"
        );

        let confirmed = record_cli_runtime_context_receipt(
            &store,
            "thread_cli_receipt",
            "provider-thread-a",
            "turn-a",
            0,
            false,
            None,
            unix_to_datetime(200),
        )
        .await
        .expect("confirmed provider start should persist its context receipt");
        assert!(
            !binding_has_current_context(
                &store,
                &confirmed,
                "ws_cli_binding",
                "thread_cli_receipt",
                Some(("turn-a", 0, false)),
            )
            .await
            .unwrap(),
            "provider acknowledgement alone must not claim completed context continuity"
        );
        let confirmed_json: serde_json::Value =
            serde_json::from_str(&confirmed.resume_cursor_json).unwrap();
        assert_eq!(confirmed_json["providerOpaque"]["cursor"], 7);
        for head in [
            Some(("turn-b", 0, false)),
            Some(("turn-a", 1, false)),
            Some(("turn-a", 0, true)),
            None,
        ] {
            assert!(
                !binding_has_current_context(
                    &store,
                    &confirmed,
                    "ws_cli_binding",
                    "thread_cli_receipt",
                    head,
                )
                .await
                .unwrap()
            );
        }

        let mut mismatched_provider = confirmed;
        mismatched_provider.native_thread_id = "provider-thread-b".to_owned();
        assert!(
            !binding_has_current_context(
                &store,
                &mismatched_provider,
                "ws_cli_binding",
                "thread_cli_receipt",
                Some(("turn-a", 0, false)),
            )
            .await
            .unwrap(),
            "a receipt for another provider branch must never authorize resume"
        );
    }
}
