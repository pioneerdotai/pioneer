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
    pub prepared_at: DateTimeWithTimeZone,
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
    let proposed_provider_session_id = Uuid::new_v4();
    let prepared = store
        .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
            thread_binding: NewCliRuntimeThreadBinding {
                thread_id: request.thread_id,
                workspace_id: request.workspace_id,
                runtime_id: request.runtime_id,
                runtime_kind: "claude".to_owned(),
                native_thread_id: proposed_provider_session_id.to_string(),
                native_session_id: Some(proposed_provider_session_id.to_string()),
                native_root_thread_id: None,
                native_cwd: Some(request.cwd),
                native_model: request.model,
                resume_cursor_json: serialize_cli_runtime_json(&serde_json::json!({
                    "provider": "claude",
                    "providerSessionId": "<redacted>"
                }))?,
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
    Ok(match prepared.mode {
        PreparedClaudeProviderSessionMode::New => CliProviderContinuation::ClaudeNew {
            provider_session_id,
        },
        PreparedClaudeProviderSessionMode::Resume => CliProviderContinuation::ClaudeResume {
            provider_session_id,
        },
    })
}

const CONTEXT_RECEIPT_VERSION: u32 = 4;

#[cfg(test)]
type TurnGuardLookupRegistry = std::sync::Mutex<
    std::collections::HashMap<
        (usize, String),
        std::sync::Weak<std::sync::Mutex<Vec<(usize, usize)>>>,
    >,
>;

#[cfg(test)]
static TURN_GUARD_LOOKUPS: std::sync::LazyLock<TurnGuardLookupRegistry> =
    std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) struct TurnGuardLookupObserver {
    key: (usize, String),
    pages: std::sync::Arc<std::sync::Mutex<Vec<(usize, usize)>>>,
}

#[cfg(test)]
impl TurnGuardLookupObserver {
    pub(crate) fn pages(&self) -> Vec<(usize, usize)> {
        self.pages.lock().unwrap().clone()
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
    let pages = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    assert!(
        TURN_GUARD_LOOKUPS
            .lock()
            .unwrap()
            .insert(key.clone(), std::sync::Arc::downgrade(&pages))
            .is_none(),
        "turn guard lookup observer already installed"
    );
    TurnGuardLookupObserver { key, pages }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CliRuntimeDeliveredTurn {
    pub(crate) turn_id: String,
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

pub(crate) async fn binding_has_current_context(
    store: &CrudStore,
    binding: &CliRuntimeThreadBindingRecord,
    workspace_id: &str,
    execution_thread_id: &str,
    previous_turn: Option<(&str, u64, bool)>,
) -> Result<bool> {
    // Decode before inspecting the Pioneer head. A missing head means the
    // conversation needs bootstrap, but it must never make a corrupt provider
    // cursor silently replaceable.
    let cursor =
        deserialize_cli_runtime_json::<CliRuntimeResumeCursor>(&binding.resume_cursor_json)
            .context("CLI runtime resume cursor is malformed")?;
    let Some((previous_turn_id, previous_turn_revision, previous_turn_deleted)) = previous_turn
    else {
        return Ok(false);
    };
    let Some(receipt) = cursor.pioneer_context else {
        return Ok(false);
    };
    if !(receipt.version == CONTEXT_RECEIPT_VERSION
        && receipt.native_thread_id == binding.native_thread_id
        && receipt.accepted_turn_id == previous_turn_id
        && receipt.accepted_turn_revision == previous_turn_revision
        && receipt.accepted_turn_deleted == previous_turn_deleted)
    {
        return Ok(false);
    }
    let (Some(execution_owner), Some(history_json)) = (
        receipt.context_owner_thread_id.as_deref(),
        receipt.context_history_json.as_deref(),
    ) else {
        // The start acknowledgement is recovery metadata for the accepted
        // provider turn, not evidence that a later turn may resume the whole
        // canonical conversation without bootstrap.
        return Ok(false);
    };
    if execution_owner != execution_thread_id {
        return Ok(false);
    }
    let manifest_owner = receipt
        .context_manifest_owner_thread_id
        .as_deref()
        .unwrap_or(execution_owner);
    if !crate::compaction::frozen::validate_frozen_history_authority(
        store,
        workspace_id,
        manifest_owner,
        history_json,
    )
    .await?
    {
        return Ok(false);
    }
    let authority: pioneer_compaction::frozen::FrozenHistoryRef =
        serde_json::from_str(history_json)?;
    if authority.messages > 0 && receipt.delivered_sources.is_empty() {
        // Version 4 receipts must identify the actual restored projection.
        // An authority manifest alone cannot prove which raw/checkpoint sources
        // the provider saw.
        return Ok(false);
    }
    if !receipt.delivered_turns.is_empty() {
        let ids = receipt
            .delivered_turns
            .iter()
            .map(|turn| turn.turn_id.clone())
            .collect::<Vec<_>>();
        let unique = ids.iter().collect::<std::collections::BTreeSet<_>>();
        if unique.len() != ids.len() {
            return Ok(false);
        }
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
                .get_turn_message_guards_by_thread_and_ids(execution_thread_id, &ids[start..end])
                .await?;
            #[cfg(test)]
            if let Some(pages) = TURN_GUARD_LOOKUPS
                .lock()
                .unwrap()
                .get(&(
                    store.database_connection().runtime_identity(),
                    workspace_id.to_owned(),
                ))
                .and_then(std::sync::Weak::upgrade)
            {
                pages.lock().unwrap().push((end - start, bytes));
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
        if actual.len() != receipt.delivered_turns.len() {
            return Ok(false);
        }
        if !receipt.delivered_turns.iter().all(|expected| {
            actual.get(expected.turn_id.as_str())
                == Some(&(expected.message_revision, expected.message_deleted))
        }) {
            return Ok(false);
        }
    }
    let delivered_sources = receipt
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
    crate::compaction::frozen::validate_direct_history_sources_current(
        store,
        workspace_id,
        delivered_sources.as_slice(),
    )
    .await
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
    cursor.pioneer_context = Some(CliRuntimeContextReceipt {
        version: CONTEXT_RECEIPT_VERSION,
        native_thread_id: native_thread_id.to_owned(),
        accepted_turn_id: accepted_turn_id.to_owned(),
        accepted_turn_revision,
        accepted_turn_deleted,
        context_owner_thread_id: None,
        context_history_json: None,
        context_manifest_owner_thread_id: None,
        delivered_turns: Vec::new(),
        delivered_sources: Vec::new(),
    });
    store
        .update_cli_runtime_thread_resume_cursor(
            thread_id,
            native_thread_id,
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
    if sent_basis.completed.execution_thread_id != thread_id {
        bail!("CLI runtime sent context belongs to another execution thread");
    }
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
            thread_id,
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
    cursor.pioneer_context = Some(CliRuntimeContextReceipt {
        version: CONTEXT_RECEIPT_VERSION,
        native_thread_id: native_thread_id.to_owned(),
        accepted_turn_id: accepted_turn.turn_id,
        accepted_turn_revision: accepted_turn.message_revision,
        accepted_turn_deleted: accepted_turn.message_deleted,
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
                    pioneer_crud::compaction::PagedSource::Input => false,
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
    if existing.runtime_id != request.runtime_id || existing.runtime_kind != request.runtime_kind {
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
        open_cli_runtime_thread_binding, record_cli_runtime_context_receipt,
    };
    use crate::cli_runtime::manager::{
        CLIAgentRuntimeThreadOpenParams, CLIAgentRuntimeThreadOpenSnapshot,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{CrudStore, NewCliRuntimeThreadBinding};
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
            "a pre-fix binding must bootstrap because native_thread_id alone is not evidence"
        );

        let confirmed = record_cli_runtime_context_receipt(
            &store,
            "thread_cli_receipt",
            "provider-thread-a",
            "turn-a",
            0,
            false,
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
