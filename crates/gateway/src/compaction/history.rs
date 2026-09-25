//! Source-aware canonical line loading. All database reads release Maintenance
//! capacity before decoding. A shared fence can freeze several related lines.
use super::*;

use pioneer_agent::compaction::composition::ScopedHistorySource;
use pioneer_crud::{
    CanonicalTurnEventPayload as Event,
    compaction::{HistoryReadFence, PagedSource, SourceRecord},
};
use pioneer_provider::{
    AttachmentArtifactContext, AttachmentDataSource, CanonicalProviderRoundEnvelope, ChatMessage,
    MessageAttachment, MessageContentPart, MessageProvenance, MessageSourceRef, Role,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[cfg(test)]
#[derive(Default)]
struct PayloadBatchStats {
    calls: std::sync::atomic::AtomicUsize,
    max_rows: std::sync::atomic::AtomicUsize,
    max_returned_raw_bytes: std::sync::atomic::AtomicUsize,
    current_raw_bytes: std::sync::atomic::AtomicUsize,
    peak_concurrent_raw_bytes: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct PayloadBatchStatsSnapshot {
    pub(crate) calls: usize,
    pub(crate) max_rows: usize,
    pub(crate) max_returned_raw_bytes: usize,
    pub(crate) current_raw_bytes: usize,
    pub(crate) peak_concurrent_raw_bytes: usize,
}

#[cfg(test)]
pub(super) struct RawPayloadLease {
    stats: Option<std::sync::Arc<PayloadBatchStats>>,
    bytes: usize,
}

#[cfg(test)]
impl Drop for RawPayloadLease {
    fn drop(&mut self) {
        if let Some(stats) = &self.stats {
            use std::sync::atomic::Ordering;
            stats
                .current_raw_bytes
                .fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
tokio::task_local! {
    static PAYLOAD_BATCH_STATS: std::sync::Arc<PayloadBatchStats>;
}

#[cfg(test)]
pub(super) async fn with_payload_batch_stats<F: std::future::Future>(
    future: F,
) -> (F::Output, PayloadBatchStatsSnapshot) {
    use std::sync::atomic::Ordering;
    let stats = std::sync::Arc::new(PayloadBatchStats::default());
    let output = PAYLOAD_BATCH_STATS.scope(stats.clone(), future).await;
    let snapshot = PayloadBatchStatsSnapshot {
        calls: stats.calls.load(Ordering::SeqCst),
        max_rows: stats.max_rows.load(Ordering::SeqCst),
        max_returned_raw_bytes: stats.max_returned_raw_bytes.load(Ordering::SeqCst),
        current_raw_bytes: stats.current_raw_bytes.load(Ordering::SeqCst),
        peak_concurrent_raw_bytes: stats.peak_concurrent_raw_bytes.load(Ordering::SeqCst),
    };
    (output, snapshot)
}

#[cfg(test)]
#[derive(Default)]
struct SelectedTurnLoadPauseState {
    selected: std::sync::Mutex<BTreeSet<String>>,
    payload_stats: std::sync::Mutex<Option<PayloadBatchStatsSnapshot>>,
    reached: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    reached_notify: tokio::sync::Notify,
    release_notify: tokio::sync::Notify,
}

#[cfg(test)]
type SelectedTurnLoadKey = (usize, String, String);

#[cfg(test)]
static SELECTED_TURN_LOAD_PAUSES: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<SelectedTurnLoadKey, std::sync::Weak<SelectedTurnLoadPauseState>>,
    >,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) struct SelectedTurnLoadPause {
    key: SelectedTurnLoadKey,
    state: std::sync::Arc<SelectedTurnLoadPauseState>,
}

#[cfg(test)]
impl SelectedTurnLoadPause {
    pub(crate) async fn reached(&self) {
        loop {
            let notified = self.state.reached_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.reached_now() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn reached_now(&self) -> bool {
        self.state.reached.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn selected(&self) -> BTreeSet<String> {
        self.state.selected.lock().unwrap().clone()
    }

    pub(crate) fn payload_stats(&self) -> Option<PayloadBatchStatsSnapshot> {
        *self.state.payload_stats.lock().unwrap()
    }

    pub(crate) fn release(&self) {
        self.state
            .released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.state.release_notify.notify_waiters();
    }
}

#[cfg(test)]
impl Drop for SelectedTurnLoadPause {
    fn drop(&mut self) {
        self.release();
        SELECTED_TURN_LOAD_PAUSES.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
pub(crate) fn pause_selected_turn_load(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
) -> SelectedTurnLoadPause {
    let key = (
        store.database_connection().runtime_identity(),
        workspace.to_owned(),
        thread.to_owned(),
    );
    let state = std::sync::Arc::new(SelectedTurnLoadPauseState::default());
    assert!(
        SELECTED_TURN_LOAD_PAUSES
            .lock()
            .unwrap()
            .insert(key.clone(), std::sync::Arc::downgrade(&state))
            .is_none(),
        "selected-turn loader pause already installed"
    );
    SelectedTurnLoadPause { key, state }
}

#[cfg(test)]
async fn wait_if_selected_turn_load_paused(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turns: &BTreeSet<String>,
) -> Option<std::sync::Arc<SelectedTurnLoadPauseState>> {
    let key = (
        store.database_connection().runtime_identity(),
        workspace.to_owned(),
        thread.to_owned(),
    );
    let state = SELECTED_TURN_LOAD_PAUSES
        .lock()
        .unwrap()
        .get(&key)
        .and_then(std::sync::Weak::upgrade);
    if let Some(state) = state {
        *state.selected.lock().unwrap() = turns.clone();
        state
            .reached
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state.reached_notify.notify_waiters();
        loop {
            let notified = state.release_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if state.released.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
        return Some(state);
    }
    None
}

#[cfg(test)]
pub(super) fn observe_payload_batch(payloads: &[String]) -> RawPayloadLease {
    let bytes = payloads.iter().map(String::len).sum();
    let stats = PAYLOAD_BATCH_STATS
        .try_with(|stats| {
            use std::sync::atomic::Ordering;
            stats.calls.fetch_add(1, Ordering::SeqCst);
            stats.max_rows.fetch_max(payloads.len(), Ordering::SeqCst);
            stats
                .max_returned_raw_bytes
                .fetch_max(bytes, Ordering::SeqCst);
            let current = stats.current_raw_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes;
            stats
                .peak_concurrent_raw_bytes
                .fetch_max(current, Ordering::SeqCst);
            stats.clone()
        })
        .ok();
    RawPayloadLease { stats, bytes }
}

struct SourcePayloadBatch {
    entries: std::vec::IntoIter<(SourceRecord, String)>,
    #[cfg(test)]
    _raw_payloads: RawPayloadLease,
}

impl Iterator for SourcePayloadBatch {
    type Item = (SourceRecord, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next()
    }
}

/// Preparation belongs to the requesting operation, never a detached startup
/// scan. Each quantum releases SQLite capacity; cancellation leaves a durable
/// cursor for the next request. Capture the history fence only after this returns.
pub(crate) async fn prepare_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
) -> Result<()> {
    while !store
        .compaction_prepare_history_quantum(workspace, thread)
        .await?
    {
        tokio::task::yield_now().await;
    }
    Ok(())
}

pub(crate) async fn source_payload(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    source: &mut SourceRecord,
) -> Result<String> {
    if let Some(payload) = source.payload.take() {
        #[cfg(test)]
        let _raw_payloads = observe_payload_batch(std::slice::from_ref(&payload));
        return Ok(payload);
    }
    let payload = reference_payload(store, workspace, thread, &source.reference).await?;
    #[cfg(test)]
    let _raw_payloads = observe_payload_batch(std::slice::from_ref(&payload));
    Ok(payload)
}

async fn take_source_payload_batch(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    records: &mut VecDeque<SourceRecord>,
) -> Result<SourcePayloadBatch> {
    let Some(first) = records.front_mut() else {
        return Ok(SourcePayloadBatch {
            entries: Vec::new().into_iter(),
            #[cfg(test)]
            _raw_payloads: RawPayloadLease {
                stats: None,
                bytes: 0,
            },
        });
    };
    if let Some(payload) = first.payload.take() {
        let record = records.pop_front().expect("front record disappeared");
        #[cfg(test)]
        let _raw_payloads = observe_payload_batch(std::slice::from_ref(&payload));
        let entries = vec![(record, payload)];
        return Ok(SourcePayloadBatch {
            entries: entries.into_iter(),
            #[cfg(test)]
            _raw_payloads,
        });
    }
    let references = records
        .iter()
        .take(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize)
        .take_while(|record| record.payload.is_none())
        .map(|record| record.reference.clone())
        .collect::<Vec<_>>();
    let (consumed, payloads) = store
        .compaction_reference_payload_batch(workspace, thread, &references)
        .await?;
    ensure!(
        consumed > 0 && consumed == payloads.len(),
        "history payload batch made no progress"
    );
    #[cfg(test)]
    let _raw_payloads = observe_payload_batch(&payloads);
    let mut ready = Vec::with_capacity(consumed);
    for payload in payloads {
        let mut record = records.pop_front().expect("payload record disappeared");
        record.incomplete = false;
        ready.push((record, payload));
    }
    Ok(SourcePayloadBatch {
        entries: ready.into_iter(),
        #[cfg(test)]
        _raw_payloads,
    })
}

pub(crate) async fn reference_payload(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Result<String> {
    store
        .compaction_reference_payload(workspace, thread, reference)
        .await?
        .ok_or_else(|| anyhow::anyhow!("selected history source disappeared"))
}

pub(crate) async fn prepare_references(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    references: &[SourceRef],
) -> Result<()> {
    store
        .compaction_prepare_references(workspace, thread, references)
        .await
}

/// Resolve retained exact event relationship metadata in bounded batches without
/// touching raw payloads or requiring the covered event to remain live.
pub(crate) async fn historical_event_projections(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    references: impl IntoIterator<Item = SourceRef>,
) -> Result<Vec<pioneer_crud::compaction::HistoricalEventProjection>> {
    let references = references
        .into_iter()
        .filter(|source| source.scope.starts_with("event:"))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut projections = Vec::new();
    let mut start = 0;
    while start < references.len() {
        let mut end = start;
        let mut bytes = 2_usize;
        while end < references.len()
            && end - start < pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize
        {
            let encoded = serde_json::to_vec(&references[end])?;
            let separator = usize::from(end > start);
            ensure!(
                encoded.len().saturating_add(2) <= pioneer_crud::compaction::SOURCE_PAGE_BYTES,
                "historical event projection reference exceeds metadata bound"
            );
            if end > start
                && bytes
                    .saturating_add(separator)
                    .saturating_add(encoded.len())
                    > pioneer_crud::compaction::SOURCE_PAGE_BYTES
            {
                break;
            }
            bytes = bytes
                .saturating_add(separator)
                .saturating_add(encoded.len());
            end += 1;
        }
        projections.extend(
            store
                .compaction_historical_event_projections(workspace, thread, &references[start..end])
                .await?,
        );
        start = end;
    }
    Ok(projections)
}

fn source_turn(source: &SourceRef) -> Option<&str> {
    source.scope.split_once(':').map(|(_, turn)| turn)
}

async fn metadata(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    high_water: i64,
    fence: &HistoryReadFence,
) -> Result<Vec<SourceRecord>> {
    let capture_order = match kind {
        PagedSource::Input => fence.input_order,
        PagedSource::Event => fence.event_order,
        PagedSource::ProviderContext => fence.context_order,
    };
    let mut records = Vec::new();
    let mut after = 0;
    while after < high_water {
        let page = store
            .compaction_source_metadata_page_at_fence(
                workspace,
                thread,
                turn,
                kind,
                after,
                capture_order,
            )
            .await?;
        ensure!(
            page.next_sequence > after,
            "history discovery lost its captured boundary"
        );
        after = page.next_sequence;
        records.extend(
            page.entries
                .into_iter()
                .filter(|row| row.sequence <= high_water),
        );
    }
    Ok(records)
}
fn origin(
    workspace: &str,
    thread: &str,
    turn: &str,
    unit: &str,
    sources: Vec<SourceRef>,
) -> MessageProvenance {
    MessageProvenance {
        logical_turn_id: None,
        workspace_id: workspace.into(),
        thread_id: thread.into(),
        context_thread: None,
        unit_id: format!("{turn}:{unit}"),
        sources: sources
            .into_iter()
            .map(|source| MessageSourceRef {
                scope: source.scope,
                id: source.id,
                version: source.version,
            })
            .collect(),
        complete: true,
        protected_input: false,
        inherited: false,
    }
}
pub(crate) fn input_message(inputs: &[pioneer_protocol::UserInput]) -> Result<ChatMessage> {
    let mut text = Vec::new();
    let mut parts = Vec::new();
    for input in inputs {
        match input {
            pioneer_protocol::UserInput::Text { text: value, .. } => text.push(value.clone()),
            pioneer_protocol::UserInput::Image { url } => parts.push(MessageContentPart::image(
                historical_url_attachment(url, "image/*"),
            )),
            pioneer_protocol::UserInput::LocalImage { path } => parts.push(
                MessageContentPart::image(historical_path_attachment(path, "image/*")),
            ),
            pioneer_protocol::UserInput::File { url } => parts.push(MessageContentPart::file(
                historical_url_attachment(url, "application/octet-stream"),
            )),
            pioneer_protocol::UserInput::LocalFile { path } => {
                parts.push(MessageContentPart::file(historical_path_attachment(
                    path,
                    "application/octet-stream",
                )))
            }
            pioneer_protocol::UserInput::Audio { url } => parts.push(MessageContentPart::audio(
                historical_url_attachment(url, "audio/*"),
            )),
            pioneer_protocol::UserInput::LocalAudio { path } => parts.push(
                MessageContentPart::audio(historical_path_attachment(path, "audio/*")),
            ),
            pioneer_protocol::UserInput::Video { url } => parts.push(MessageContentPart::video(
                historical_url_attachment(url, "video/*"),
            )),
            pioneer_protocol::UserInput::LocalVideo { path } => parts.push(
                MessageContentPart::video(historical_path_attachment(path, "video/*")),
            ),
            // Exact artifact kind and version are captured by the durable
            // UserMessage attachment event, not inferred from this launch ID.
            reference @ (pioneer_protocol::UserInput::Artifact { .. }
            | pioneer_protocol::UserInput::Mention { .. }) => text.push(format!(
                "Historical input reference: {}",
                serde_json::to_string(reference)?
            )),
        }
    }
    let mut message = ChatMessage::user(text.join("\n"));
    message.content_parts = parts;
    Ok(message)
}

/// Wire-compatible renderer for reference-based manifests captured before
/// historical media became typed content parts. Frozen restore uses this only
/// as a digest-checked compatibility candidate; execution projection continues
/// to use [`input_message`] and therefore never derives attachment authority
/// from this display text.
pub(crate) fn legacy_input_message(inputs: &[pioneer_protocol::UserInput]) -> Result<ChatMessage> {
    let mut text = Vec::new();
    for input in inputs {
        match input {
            pioneer_protocol::UserInput::Text { text: value, .. } => text.push(value.clone()),
            reference => text.push(format!(
                "Historical input reference: {}",
                serde_json::to_string(reference)?
            )),
        }
    }
    Ok(ChatMessage::user(text.join("\n")))
}

fn historical_url_attachment(url: &str, mime_type: &str) -> MessageAttachment {
    MessageAttachment::from_url(url.to_owned(), historical_media_mime(url, mime_type))
}

fn historical_path_attachment(path: &str, mime_type: &str) -> MessageAttachment {
    MessageAttachment::from_path(path.to_owned(), historical_media_mime(path, mime_type))
}

fn historical_media_mime(location: &str, fallback: &str) -> String {
    if let Some(data_mime) = location
        .strip_prefix("data:")
        .and_then(|value| value.split_once(';').map(|(mime, _)| mime))
        .filter(|mime| !mime.trim().is_empty())
    {
        return data_mime.to_owned();
    }
    let kind = match fallback {
        "image/*" => pioneer_provider::InputContentType::Image,
        "audio/*" => pioneer_provider::InputContentType::Audio,
        "video/*" => pioneer_provider::InputContentType::Video,
        _ => pioneer_provider::InputContentType::File,
    };
    pioneer_provider::infer_mime_from_reference(location, kind)
}

fn historical_user_attachment_part(
    attachment: pioneer_protocol::UserMessageAttachment,
) -> Option<MessageContentPart> {
    use pioneer_protocol::UserMessageAttachment as Attachment;
    Some(match attachment {
        Attachment::Image { url } => {
            MessageContentPart::image(historical_url_attachment(url.as_str(), "image/*"))
        }
        Attachment::LocalImage { path } => {
            MessageContentPart::image(historical_path_attachment(path.as_str(), "image/*"))
        }
        Attachment::File { url } => MessageContentPart::file(historical_url_attachment(
            url.as_str(),
            "application/octet-stream",
        )),
        Attachment::LocalFile { path } => MessageContentPart::file(historical_path_attachment(
            path.as_str(),
            "application/octet-stream",
        )),
        Attachment::Audio { url } => {
            MessageContentPart::audio(historical_url_attachment(url.as_str(), "audio/*"))
        }
        Attachment::LocalAudio { path } => {
            MessageContentPart::audio(historical_path_attachment(path.as_str(), "audio/*"))
        }
        Attachment::Video { url } => {
            MessageContentPart::video(historical_url_attachment(url.as_str(), "video/*"))
        }
        Attachment::LocalVideo { path } => {
            MessageContentPart::video(historical_path_attachment(path.as_str(), "video/*"))
        }
        Attachment::Artifact { artifact } => historical_artifact_part(artifact),
        Attachment::Skill { .. }
        | Attachment::SkillPack { .. }
        | Attachment::McpServer { .. }
        | Attachment::McpTool { .. } => return None,
    })
}

fn immutable_artifact_reference(
    artifact_id: &str,
    version_id: Option<&str>,
    name: Option<String>,
    mime_type: Option<String>,
    workspace_id: Option<&str>,
) -> MessageAttachment {
    MessageAttachment {
        mime_type: mime_type.unwrap_or_else(|| "application/octet-stream".to_owned()),
        name,
        size_bytes: None,
        sha256: None,
        source: AttachmentDataSource::Reference {
            reference: format!("pioneer-artifact:{artifact_id}"),
        },
        artifact: Some(AttachmentArtifactContext {
            workspace_id: workspace_id.unwrap_or_default().to_owned(),
            artifact_id: artifact_id.to_owned(),
            artifact_version_id: version_id.map(str::to_owned),
        }),
    }
}

fn historical_artifact_part(artifact: pioneer_protocol::ArtifactRef) -> MessageContentPart {
    let is_image = matches!(
        artifact.kind,
        pioneer_protocol::ArtifactKind::Image
            | pioneer_protocol::ArtifactKind::GeneratedImage
            | pioneer_protocol::ArtifactKind::Screenshot
    );
    let mut attachment = immutable_artifact_reference(
        artifact.artifact_id.as_str(),
        artifact.version_id.as_deref(),
        Some(artifact.display_name),
        artifact.mime_type,
        None,
    );
    attachment.size_bytes = artifact.size_bytes;
    attachment.sha256 = artifact.sha256;
    if is_image {
        MessageContentPart::Image { image: attachment }
    } else {
        MessageContentPart::File { file: attachment }
    }
}
struct Round {
    sequence: i64,
    envelope: CanonicalProviderRoundEnvelope,
    assistant_source: SourceRef,
    results: BTreeMap<String, (SourceRef, ChatMessage)>,
}
fn finish_round(
    workspace: &str,
    thread: &str,
    turn: &str,
    round: Round,
    active: bool,
    output: &mut Vec<(i64, Vec<ChatMessage>)>,
) -> Result<()> {
    let calls = round
        .envelope
        .message
        .tool_calls
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("canonical round has no calls"))?;
    ensure!(
        round.envelope.version == 1
            && !round.envelope.round_id.trim().is_empty()
            && round.envelope.message.role == Role::Assistant
            && round.envelope.termination == pioneer_provider::ProviderTermination::ToolCalls
            && !calls.is_empty()
            && calls.len() == round.envelope.calls.len(),
        "invalid canonical provider round"
    );
    let mut ids = BTreeSet::new();
    let mut items = BTreeSet::new();
    for (ordinal, (call, identity)) in calls.iter().zip(&round.envelope.calls).enumerate() {
        ensure!(
            identity.ordinal as usize == ordinal
                && !call.id.trim().is_empty()
                && !call.name.trim().is_empty()
                && !identity.turn_item_id.trim().is_empty()
                && serde_json::from_str::<serde_json::Value>(&call.arguments).is_ok()
                && identity.provider_call_id == call.id
                && ids.insert(call.id.clone())
                && items.insert(identity.turn_item_id.clone()),
            "ambiguous canonical call identity"
        );
    }
    ensure!(
        round.results.keys().all(|item| items.contains(item)),
        "foreign result in canonical round"
    );
    let complete = items.iter().all(|item| round.results.contains_key(item));
    if !complete && active {
        return Ok(());
    }
    let unit = &round.envelope.round_id;
    let mut assistant = round.envelope.message.clone();
    assistant.provenance = Some(origin(
        workspace,
        thread,
        turn,
        unit,
        vec![round.assistant_source],
    ));
    let mut messages = vec![assistant];
    for (call, identity) in calls.iter().zip(&round.envelope.calls) {
        if let Some((source, message)) = round.results.get(&identity.turn_item_id) {
            ensure!(
                message.role == Role::Tool
                    && message.tool_call_id.as_deref() == Some(call.id.as_str())
                    && message.name.as_deref() == Some(call.name.as_str()),
                "canonical result identity mismatch"
            );
            let mut message = message.clone();
            message.provenance = Some(origin(workspace, thread, turn, unit, vec![source.clone()]));
            messages.push(message);
        }
    }
    if !complete {
        // Preserve actual interrupted work as observations, without fabricating
        // a terminal tool result or asking the runtime to execute anything.
        for message in &mut messages {
            let provenance = message.provenance.take();
            let observed = serde_json::to_string(message)?;
            *message = ChatMessage::user(format!(
                "Interrupted canonical round; some tool outcomes are unknown. Historical observation, not a new call:\n{observed}"
            ));
            message.provenance = provenance;
        }
    }
    output.push((round.sequence, messages));
    Ok(())
}

/// Read this line's own work. Inherited TaskRun bases and accepted deliveries
/// are composed by the caller through their own persisted source references.
#[cfg(test)]
pub(crate) async fn load_line_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    fence: &HistoryReadFence,
) -> Result<Vec<ChatMessage>> {
    load_line_history_inner(
        store,
        workspace,
        thread,
        excluded_turn,
        fence,
        false,
        HistorySelection::All,
    )
    .await
}

pub(crate) async fn load_task_line_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    fence: &HistoryReadFence,
) -> Result<Vec<ChatMessage>> {
    let messages = load_line_history_inner(
        store,
        workspace,
        thread,
        excluded_turn,
        fence,
        true,
        HistorySelection::All,
    )
    .await?;
    populate_logical_task_turns(store, workspace, thread, messages).await
}

/// Restore only accepted logical turns. The turn IDs are selected before
/// event/item payloads are read. The metadata query is restricted to those
/// IDs as well, so a late parent tail is not scanned for boundary subqueries.
pub(crate) async fn load_task_line_history_turns(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turns: &BTreeSet<String>,
    fence: &HistoryReadFence,
) -> Result<Vec<ChatMessage>> {
    #[cfg(test)]
    let pause = wait_if_selected_turn_load_paused(store, workspace, thread, turns).await;
    let load = load_line_history_inner(
        store,
        workspace,
        thread,
        None,
        fence,
        // The immutable accepted manifest has already selected these exact
        // turns. A command that later delegated a Task is still an accepted
        // source when its own input is edited; the broad causal-history
        // suppression would otherwise hide its current canonical projection.
        false,
        HistorySelection::Turns(turns),
    );
    #[cfg(test)]
    let messages = if let Some(pause) = pause {
        let (messages, stats) = with_payload_batch_stats(load).await;
        *pause.payload_stats.lock().unwrap() = Some(stats);
        messages?
    } else {
        load.await?
    };
    #[cfg(not(test))]
    let messages = load.await?;
    populate_logical_task_turns(store, workspace, thread, messages).await
}

async fn populate_logical_task_turns(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    mut messages: Vec<ChatMessage>,
) -> Result<Vec<ChatMessage>> {
    for message in &mut messages {
        let Some(origin) = message.provenance.as_mut() else {
            continue;
        };
        let mut logical = None;
        for reference in &origin.sources {
            if let Some(command) = store
                .compaction_task_delivery_command(
                    workspace,
                    thread,
                    &SourceRef {
                        scope: reference.scope.clone(),
                        id: reference.id.clone(),
                        version: reference.version.clone(),
                    },
                )
                .await?
            {
                ensure!(
                    logical.as_ref().is_none_or(|previous| previous == &command),
                    "canonical message spans different Task command outcomes"
                );
                logical = Some(command);
            }
        }
        origin.logical_turn_id = logical;
    }
    Ok(messages)
}

/// Load current canonical history while omitting stable source identities that
/// are already represented by a published checkpoint. Filtering happens on
/// metadata before payload reads, so editing a covered source neither reloads
/// its new body nor turns it into a new tail entry.
pub(crate) struct HistoryCoverageSelection<'a> {
    pub(crate) sources: &'a BTreeSet<ScopedHistorySource>,
    pub(crate) item_aliases: &'a BTreeSet<(String, String, String)>,
    pub(crate) event_input_evidence: &'a BTreeMap<ScopedHistorySource, String>,
}

pub(crate) async fn load_task_line_history_excluding(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    fence: &HistoryReadFence,
    coverage: HistoryCoverageSelection<'_>,
) -> Result<Vec<ChatMessage>> {
    let messages = load_line_history_inner(
        store,
        workspace,
        thread,
        excluded_turn,
        fence,
        true,
        HistorySelection::AllExcept {
            covered: coverage.sources,
            covered_item_aliases: coverage.item_aliases,
            covered_event_input_evidence: coverage.event_input_evidence,
        },
    )
    .await?;
    populate_logical_task_turns(store, workspace, thread, messages).await
}

/// Reconstruct only the supplied exact canonical leaves. Later unrelated rows
/// may contribute metadata to discovery, but their payloads are never decoded.
#[cfg(test)]
pub(crate) async fn load_exact_line_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    fence: &HistoryReadFence,
    selected: &BTreeSet<SourceRef>,
) -> Result<Vec<ChatMessage>> {
    load_line_history_inner(
        store,
        workspace,
        thread,
        None,
        fence,
        false,
        HistorySelection::Sources(selected),
    )
    .await
}

/// Freeze only work physically produced by this child through the completed
/// result turn. Later turns are filtered before any payload is loaded. Inherited
/// TaskRun context is deliberately not a delivered contribution.
pub(crate) async fn load_task_output_history(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    through_turn: &str,
    fence: &HistoryReadFence,
) -> Result<Vec<ChatMessage>> {
    load_line_history_inner(
        store,
        workspace,
        thread,
        None,
        fence,
        false,
        HistorySelection::ThroughTurn(through_turn),
    )
    .await
}

enum HistorySelection<'a> {
    All,
    Turns(&'a BTreeSet<String>),
    AllExcept {
        covered: &'a BTreeSet<ScopedHistorySource>,
        covered_item_aliases: &'a BTreeSet<(String, String, String)>,
        covered_event_input_evidence: &'a BTreeMap<ScopedHistorySource, String>,
    },
    #[cfg(test)]
    Sources(&'a BTreeSet<SourceRef>),
    ThroughTurn(&'a str),
}

async fn load_line_history_inner(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    fence: &HistoryReadFence,
    causal_task_context: bool,
    selection: HistorySelection<'_>,
) -> Result<Vec<ChatMessage>> {
    let (
        selected,
        selected_turns,
        covered,
        covered_item_aliases,
        covered_event_input_evidence,
        through_turn,
    ) = match selection {
        HistorySelection::All => (None, None, None, None, None, None),
        HistorySelection::Turns(turns) => (None, Some(turns), None, None, None, None),
        HistorySelection::AllExcept {
            covered,
            covered_item_aliases,
            covered_event_input_evidence,
        } => (
            None,
            None,
            Some(covered),
            Some(covered_item_aliases),
            Some(covered_event_input_evidence),
            None,
        ),
        #[cfg(test)]
        HistorySelection::Sources(sources) => (Some(sources), None, None, None, None, None),
        HistorySelection::ThroughTurn(turn) => (None, None, None, None, None, Some(turn)),
    };
    // `Sources` is test-only, so production builds otherwise have no `Some`
    // branch from which to infer the collection behind `selected`.
    let selected: Option<&BTreeSet<SourceRef>> = selected;
    let covered_identities = covered.map(|covered| {
        covered
            .iter()
            .filter(|source| source.thread == thread)
            .map(|source| (source.source.scope.clone(), source.source.id.clone()))
            .collect::<BTreeSet<_>>()
    });
    let covered_item_alias_index = covered_item_aliases.map(|aliases| {
        let mut index = BTreeMap::<&str, BTreeMap<&str, BTreeSet<&str>>>::new();
        for (source_thread, source_turn, item) in aliases {
            index
                .entry(source_thread)
                .or_default()
                .entry(source_turn)
                .or_default()
                .insert(item);
        }
        index
    });
    let is_covered = |source: &SourceRef| {
        covered_identities
            .as_ref()
            .is_some_and(|covered| covered.contains(&(source.scope.clone(), source.id.clone())))
    };
    // A checkpoint may replace an event-input while leaving its UI copy in the
    // uncovered tail. Preserve the exact, captured relationship before raw
    // filtering. The revision table retains the last decoded exact revision
    // after the covered event body is physically removed.
    let covered_event_projections = if let Some(covered) = covered {
        historical_event_projections(
            store,
            workspace,
            thread,
            covered
                .iter()
                .filter(|source| source.thread == thread)
                .map(|source| source.source.clone()),
        )
        .await?
    } else {
        Vec::new()
    };
    let mut covered_event_input_turns = covered_event_input_evidence
        .into_iter()
        .flat_map(|evidence| evidence.iter())
        .filter(|(source, role)| {
            source.thread == thread && matches!(role.as_str(), "authoritative" | "deleted")
        })
        .filter_map(|(source, _)| source_turn(&source.source).map(str::to_owned))
        .collect::<BTreeSet<_>>();
    covered_event_input_turns.extend(
        covered_event_projections
            .iter()
            .filter(|projection| {
                matches!(
                    projection.projection_kind.as_str(),
                    "input" | "input_revision" | "input_deleted"
                )
            })
            .filter_map(|projection| source_turn(&projection.reference).map(str::to_owned))
            .collect::<BTreeSet<_>>(),
    );
    let mut covered_authoritative_event_input_turns = covered_event_input_evidence
        .into_iter()
        .flat_map(|evidence| evidence.iter())
        .filter(|(source, role)| source.thread == thread && role.as_str() == "authoritative")
        .filter_map(|(source, _)| source_turn(&source.source).map(str::to_owned))
        .collect::<BTreeSet<_>>();
    covered_authoritative_event_input_turns.extend(
        covered_event_projections
            .iter()
            .filter(|projection| {
                matches!(
                    projection.projection_kind.as_str(),
                    "input" | "input_revision"
                )
            })
            .filter_map(|projection| source_turn(&projection.reference).map(str::to_owned))
            .collect::<BTreeSet<_>>(),
    );
    let mut turns = Vec::new();
    if let Some(selected) = selected_turns {
        let mut batch = Vec::new();
        let mut bytes = 0_usize;
        for id in selected {
            ensure!(
                id.len() <= pioneer_crud::compaction::SOURCE_PAGE_BYTES,
                "selected history turn ID exceeds metadata page bound"
            );
            if !batch.is_empty()
                && (batch.len() == 64
                    || bytes.saturating_add(id.len()) > pioneer_crud::compaction::SOURCE_PAGE_BYTES)
            {
                turns.extend(
                    store
                        .compaction_history_selected_turn_page(workspace, thread, &batch, fence)
                        .await?,
                );
                batch.clear();
                bytes = 0;
            }
            bytes += id.len();
            batch.push(id.clone());
        }
        if !batch.is_empty() {
            turns.extend(
                store
                    .compaction_history_selected_turn_page(workspace, thread, &batch, fence)
                    .await?,
            );
        }
        turns.retain(|turn| Some(turn.id.as_str()) != excluded_turn);
    } else {
        let mut after = String::new();
        loop {
            let page = store
                .compaction_history_turn_page(workspace, thread, &after, fence)
                .await?;
            let Some(last) = page.last() else { break };
            ensure!(last.id > after, "history turn page made no progress");
            after = last.id.clone();
            turns.extend(
                page.into_iter()
                    .filter(|turn| Some(turn.id.as_str()) != excluded_turn),
            );
        }
    }
    // IDs do not encode chronology. In particular, a detached Task answer may
    // share its parent's creation second and sort before its source request.
    turns.sort_by(|a, b| {
        (
            &a.created_at,
            a.creation_order,
            a.legacy_creation_order,
            &a.id,
        )
            .cmp(&(
                &b.created_at,
                b.creation_order,
                b.legacy_creation_order,
                &b.id,
            ))
    });
    if let Some(through_turn) = through_turn {
        let end = turns
            .iter()
            .position(|turn| turn.id == through_turn)
            .ok_or_else(|| anyhow::anyhow!("completed output turn is outside its history fence"))?;
        ensure!(
            turns[end].status == "completed",
            "output turn is not completed"
        );
        turns.truncate(end + 1);
    }
    // Refresh relationship metadata before deciding whether earlier Task
    // commands are closed by later occurrence/delivery turns. A cold cache
    // must not drop a command merely because its outcome sorts after it.
    if let Some(selected) = selected {
        let source_turns = selected
            .iter()
            .filter_map(|source| source.scope.split_once(':').map(|(_, turn)| turn))
            .collect::<BTreeSet<_>>();
        turns.retain(|turn| source_turns.contains(turn.id.as_str()));
    }
    if let Some(selected_turns) = selected_turns {
        turns.retain(|turn| selected_turns.contains(&turn.id));
    }
    let mut events_by_turn = Vec::with_capacity(turns.len());
    for turn in &turns {
        let covered_turn_items = covered_item_alias_index
            .as_ref()
            .and_then(|threads| threads.get(thread))
            .and_then(|turns| turns.get(turn.id.as_str()));
        let mut events = metadata(
            &store,
            workspace,
            thread,
            &turn.id,
            PagedSource::Event,
            turn.event_high_water,
            fence,
        )
        .await?;
        let mut has_event_input = covered_event_input_turns.contains(&turn.id)
            || events.iter().any(|row| {
                matches!(
                    row.projection_kind.as_deref(),
                    Some("input" | "input_revision" | "input_deleted")
                )
            });
        let mut has_authoritative_event_input = covered_authoritative_event_input_turns
            .contains(&turn.id)
            || events.iter().any(|row| {
                matches!(
                    row.projection_kind.as_deref(),
                    Some("input" | "input_revision")
                )
            });
        events.retain(|event| {
            !is_covered(&event.reference)
                && !event.item_id.as_ref().is_some_and(|item| {
                    covered_turn_items.is_some_and(|items| items.contains(item.as_str()))
                })
                && selected.is_none_or(|sources| sources.contains(&event.reference))
        });
        for event in &mut events {
            if event.projection_kind.is_none() {
                let payload = source_payload(&store, workspace, thread, event).await?;
                let parsed: Event = serde_json::from_str(&payload)?;
                ensure!(
                    store
                        .compaction_record_event_projection(
                            workspace,
                            thread,
                            &event.reference,
                            &parsed
                        )
                        .await?,
                    "canonical event changed while refreshing history metadata"
                );
                let (item, kind) = pioneer_crud::compaction::event_projection_metadata(&parsed);
                event.item_id = item;
                event.projection_kind = Some(kind.into());
                // Relationship discovery retains metadata only. Bodies that
                // survive the later projection filters are loaded on demand.
                event.payload = None;
                event.incomplete = true;
            }
        }
        has_event_input |= events.iter().any(|row| {
            matches!(
                row.projection_kind.as_deref(),
                Some("input" | "input_revision" | "input_deleted")
            )
        });
        has_authoritative_event_input |= events.iter().any(|row| {
            matches!(
                row.projection_kind.as_deref(),
                Some("input" | "input_revision")
            )
        });
        events_by_turn.push((events, has_event_input, has_authoritative_event_input));
    }
    let mut history = Vec::new();
    for (turn, (events, has_event_input, has_authoritative_event_input)) in
        turns.into_iter().zip(events_by_turn)
    {
        // A later terminal transition cannot expose the pending portion of an
        // active parent's already captured snapshot. Use only events below its
        // fence; mutable turn.status is not a historical boundary.
        let captured_active = events
            .iter()
            .filter_map(|event| {
                use pioneer_protocol::constants::events as kinds;
                match event.source_type.as_str() {
                    kinds::TURN_COMPLETED | kinds::TURN_FAILED | kinds::TURN_BLOCKED => Some(false),
                    kinds::TURN_STARTED
                    | kinds::TURN_EXECUTION_WINDOW_STARTED
                    | kinds::TURN_EXECUTION_WINDOW_CONTINUED => Some(true),
                    kinds::ITEM_STARTED if event.projection_kind.as_deref() == Some("start") => {
                        Some(true)
                    }
                    _ => None,
                }
            })
            .next_back()
            .unwrap_or(true);
        let failure_sequences = events
            .iter()
            .filter(|row| {
                matches!(
                    row.source_type.as_str(),
                    pioneer_protocol::constants::events::TURN_FAILED
                        | pioneer_protocol::constants::events::TURN_BLOCKED
                )
            })
            .map(|row| row.sequence)
            .collect::<BTreeSet<_>>();
        let delivery_outcome_sequences = events
            .iter()
            .filter(|row| {
                row.source_type == pioneer_protocol::constants::events::ITEM_COMPLETED
                    && row
                        .item_id
                        .as_deref()
                        .and_then(pioneer_protocol::task_delivery_id_from_result_item_id)
                        .is_some()
            })
            .map(|row| row.sequence)
            .collect::<BTreeSet<_>>();
        let starts = events
            .iter()
            .filter(|row| row.projection_kind.as_deref() == Some("start"))
            .filter_map(|row| row.item_id.as_ref().map(|id| (id.clone(), row.sequence)))
            .collect::<BTreeMap<_, _>>();
        let mut contexts = metadata(
            &store,
            workspace,
            thread,
            &turn.id,
            PagedSource::ProviderContext,
            turn.context_high_water,
            fence,
        )
        .await?;
        // Covered provider rows are removed before payload reads, but their
        // durable item relationship must still suppress the corresponding UI
        // event copy. This is metadata-only and does not make an unrelated
        // event or item part of checkpoint coverage.
        let context_aliases = contexts
            .iter()
            .filter(|row| is_covered(&row.reference))
            .filter_map(|row| row.item_id.clone())
            .collect::<BTreeSet<_>>();
        if covered.is_some() {
            let mut uncovered = Vec::with_capacity(contexts.len());
            for row in contexts {
                if is_covered(&row.reference) {
                    continue;
                }
                if row.source_type == "tool_result_v2" {
                    let item_covered = if let Some(item) = row.item_id.as_deref() {
                        match store
                            .compaction_tool_item_reference(workspace, thread, &turn.id, item)
                            .await?
                        {
                            Some(reference) => is_covered(&reference),
                            // Published checkpoint metadata records the exact
                            // replay row as a covered alias. Without that saved
                            // link, a missing item is not guessed to be covered.
                            None => false,
                        }
                    } else {
                        false
                    };
                    if item_covered {
                        continue;
                    }
                }
                uncovered.push(row);
            }
            contexts = uncovered;
        }
        if let Some(selected) = selected {
            let mut exact = Vec::new();
            for row in contexts {
                let keep = if selected.contains(&row.reference) {
                    true
                } else if row.source_type == "tool_result_v2" {
                    if let Some(item) = row.item_id.as_deref() {
                        store
                            .compaction_tool_item_reference(workspace, thread, &turn.id, item)
                            .await?
                            .is_some_and(|reference| selected.contains(&reference))
                    } else {
                        false
                    }
                } else {
                    false
                };
                if keep {
                    exact.push(row);
                }
            }
            contexts = exact;
        }
        let mut aliases = covered_item_alias_index
            .as_ref()
            .and_then(|threads| threads.get(thread))
            .and_then(|turns| turns.get(turn.id.as_str()))
            .into_iter()
            .flat_map(|items| items.iter().copied())
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        aliases.extend(context_aliases);
        aliases.extend(contexts.iter().filter_map(|row| row.item_id.clone()));
        let mut ordered = Vec::<(i64, Vec<ChatMessage>)>::new();
        let use_input_rows = if let Some(selected) = selected {
            selected
                .iter()
                .any(|source| source.scope == format!("input:{}", turn.id))
        } else {
            turn.input_high_water > 0
                && (matches!(turn.send_mode.as_deref(), Some("agent" | "chat")) || !has_event_input)
        };
        if use_input_rows {
            let mut rows = metadata(
                &store,
                workspace,
                thread,
                &turn.id,
                PagedSource::Input,
                turn.input_high_water,
                fence,
            )
            .await?;
            rows.retain(|row| {
                !is_covered(&row.reference)
                    && selected.is_none_or(|sources| sources.contains(&row.reference))
            });
            let mut inputs = Vec::new();
            let mut sources = Vec::new();
            let mut rows = VecDeque::from(rows);
            while !rows.is_empty() {
                for (row, payload) in
                    take_source_payload_batch(store, workspace, thread, &mut rows).await?
                {
                    inputs.push(serde_json::from_str::<pioneer_protocol::UserInput>(
                        &payload,
                    )?);
                    sources.push(row.reference);
                }
            }
            if !inputs.is_empty() {
                let mut message = input_message(&inputs)?;
                message.provenance =
                    Some(origin(workspace, thread, &turn.id, "user-input", sources));
                ordered.push((0, vec![message]));
            }
        }
        let mut pending: Option<Round> = None;
        let mut contexts = VecDeque::from(contexts);
        while !contexts.is_empty() {
            for (row, payload) in
                take_source_payload_batch(store, workspace, thread, &mut contexts).await?
            {
                match row.source_type.as_str() {
                    "assistant_round" => {
                        if let Some(round) = pending.take() {
                            finish_round(
                                workspace,
                                thread,
                                &turn.id,
                                round,
                                captured_active,
                                &mut ordered,
                            )?;
                        }
                        if let Ok(envelope) =
                            serde_json::from_str::<CanonicalProviderRoundEnvelope>(&payload)
                        {
                            aliases.extend(
                                envelope.calls.iter().map(|call| call.turn_item_id.clone()),
                            );
                            pending = Some(Round {
                                sequence: starts
                                    .get(&envelope.round_id)
                                    .copied()
                                    .unwrap_or(row.sequence),
                                envelope,
                                assistant_source: row.reference,
                                results: BTreeMap::new(),
                            });
                        } else {
                            let mut message = ChatMessage::user(format!(
                                "Legacy provider observation (available original):\n{payload}"
                            ));
                            message.provenance = Some(origin(
                                workspace,
                                thread,
                                &turn.id,
                                &row.reference.id,
                                vec![row.reference.clone()],
                            ));
                            ordered.push((row.sequence, vec![message]));
                        }
                    }
                    "provider_observation" => {
                        let mut message = provider_observation(&payload)?;
                        message.provenance = Some(origin(
                            workspace,
                            thread,
                            &turn.id,
                            &row.reference.id,
                            vec![row.reference.clone()],
                        ));
                        let order = row
                            .item_id
                            .as_ref()
                            .and_then(|id| starts.get(id))
                            .copied()
                            .unwrap_or(row.sequence);
                        ordered.push((order, vec![message]));
                    }
                    "tool_result_v2" => {
                        let round = pending
                            .as_mut()
                            .ok_or_else(|| anyhow::anyhow!("canonical tool result has no round"))?;
                        let item = row.item_id.ok_or_else(|| {
                            anyhow::anyhow!("tool result has no durable item identity")
                        })?;
                        let view: pioneer_tools::ToolResultView = serde_json::from_str(&payload)?;
                        let pioneer_tools::ToolResultView::Json {
                            value,
                            truncated: false,
                        } = view
                        else {
                            anyhow::bail!("canonical result is incomplete")
                        };
                        let full: ChatMessage = serde_json::from_value(value)?;
                        let reference = serde_json::json!({"workspace_id":workspace,"thread_id":thread,"turn_id":turn.id,"item_id":item}).to_string();
                        let message = pioneer_agent::compaction::restored_tool_result_message(
                            &full, &reference,
                        )?;
                        let source = store
                            .compaction_tool_item_reference(workspace, thread, &turn.id, &item)
                            .await?
                            .unwrap_or(row.reference);
                        ensure!(
                            round.results.insert(item, (source, message)).is_none(),
                            "duplicate canonical tool result"
                        );
                    }
                    _ => {
                        let mut message = ChatMessage::user(format!(
                            "Legacy provider observation; outcome is not inferred:\n{payload}"
                        ));
                        message.provenance = Some(origin(
                            workspace,
                            thread,
                            &turn.id,
                            &row.reference.id,
                            vec![row.reference.clone()],
                        ));
                        ordered.push((row.sequence, vec![message]));
                    }
                }
            }
        }
        if let Some(round) = pending {
            finish_round(
                workspace,
                thread,
                &turn.id,
                round,
                captured_active,
                &mut ordered,
            )?;
        }
        let latest_input = events
            .iter()
            .filter(|row| {
                matches!(
                    row.projection_kind.as_deref(),
                    Some("input" | "input_revision" | "input_deleted")
                )
            })
            .map(|row| row.sequence)
            .max();
        let last_input_revision = events
            .iter()
            .filter(|row| {
                matches!(
                    row.projection_kind.as_deref(),
                    Some("input_revision" | "input_deleted")
                )
            })
            .map(|row| row.sequence)
            .max();
        let mut last_attachment_copies = BTreeMap::<String, i64>::new();
        for row in &events {
            if row.projection_kind.as_deref() == Some("input_copy")
                && let Some(item) = &row.item_id
            {
                last_attachment_copies
                    .entry(item.clone())
                    .and_modify(|sequence| *sequence = (*sequence).max(row.sequence))
                    .or_insert(row.sequence);
            }
        }
        let mut selected_events = events.into_iter().filter(|row| {
            let kind = row.projection_kind.as_deref().unwrap_or("observation");
            let superseded_copy = kind == "input_copy"
                && (last_input_revision.is_some_and(|revision| row.sequence <= revision)
                    || row.item_id.as_ref().is_some_and(|item| {
                        last_attachment_copies.get(item) != Some(&row.sequence)
                    }));
            let metadata_only = matches!(kind, "start" | "technical")
                || row.item_id.as_ref().is_some_and(|id| aliases.contains(id));
            let superseded_input = matches!(kind, "input" | "input_revision" | "input_deleted")
                && (use_input_rows || Some(row.sequence) != latest_input);
            !superseded_copy && !metadata_only && !superseded_input
        });
        loop {
            let mut page = selected_events
                .by_ref()
                .take(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize)
                .collect::<Vec<_>>();
            if page.is_empty() {
                break;
            }
            while !page.is_empty() {
                let references = page
                    .iter()
                    .map(|row| row.reference.clone())
                    .collect::<Vec<_>>();
                let (consumed, payloads) = store
                    .compaction_reference_payload_batch(workspace, thread, &references)
                    .await?;
                ensure!(
                    consumed > 0 && consumed == payloads.len(),
                    "event payload batch made no progress"
                );
                let ready = page.drain(..consumed).collect::<Vec<_>>();
                for (row, payload) in ready.into_iter().zip(payloads) {
                    let event: Event = serde_json::from_str(&payload)?;
                    ensure!(
                        event.workspace_id() == workspace
                            && event.thread_id() == thread
                            && event.turn_id() == turn.id,
                        "canonical event scope mismatch"
                    );
                    let Some(mut message) = event_message_with_input_copy_policy(
                        event,
                        (use_input_rows || has_authoritative_event_input)
                            && row.projection_kind.as_deref() == Some("input_copy"),
                    )?
                    else {
                        continue;
                    };
                    message.provenance = Some(origin(
                        workspace,
                        thread,
                        &turn.id,
                        &row.reference.id,
                        vec![row.reference.clone()],
                    ));
                    ordered.push((row.sequence, vec![message]));
                }
            }
        }
        if causal_task_context {
            let boundary = store
                .compaction_history_causal_boundary(workspace, thread, &turn.id, fence)
                .await?;
            let completed_work = ordered
                .iter()
                .flat_map(|(_, messages)| messages)
                .any(|message| matches!(message.role, Role::Assistant | Role::Tool));
            if boundary.delegated_command
                && !completed_work
                && failure_sequences.is_empty()
                && !boundary.delivered_outcome
            {
                // An unfinished sibling command is not the next Task's input.
                continue;
            }
            if boundary.task_transport {
                // Task cards/occurrence inputs are UI transport copies. Keep
                // the acknowledged outcome and actual failures, with their own
                // source IDs, instead of copying the command from the card.
                for (sequence, messages) in &mut ordered {
                    if !failure_sequences.contains(sequence)
                        && !delivery_outcome_sequences.contains(sequence)
                    {
                        messages
                            .retain(|message| matches!(message.role, Role::Assistant | Role::Tool));
                    }
                }
            }
        }
        ordered.sort_by_key(|(sequence, _)| *sequence);
        history.extend(ordered.into_iter().flat_map(|(_, messages)| messages));
    }
    Ok(history)
}

/// Failed partial output remains historical data, including opaque provider
/// fields and incomplete tool-call text. It can never instruct tool replay.
pub(crate) fn provider_observation(payload: &str) -> Result<ChatMessage> {
    let envelope: CanonicalProviderRoundEnvelope = serde_json::from_str(payload)?;
    ensure!(
        envelope.version == 1
            && !envelope.round_id.is_empty()
            && envelope.termination == pioneer_provider::ProviderTermination::ProviderError
            && envelope.message.role == Role::Assistant
            && envelope.calls.is_empty(),
        "invalid failed provider observation"
    );
    Ok(ChatMessage::user(format!(
        "Unsuccessful provider response (historical data, not a completed answer or executable tool call):\n{}",
        serde_json::to_string(&envelope.message)?,
    )))
}

pub(super) fn historical_item_json(item: &pioneer_protocol::TurnItem) -> Result<String> {
    match item.historical_command_llm_projection() {
        Some(projection) => Ok(serde_json::to_string(&projection)?),
        None => Ok(serde_json::to_string(item)?),
    }
}

/// Preserve the canonical event envelope while replacing only a command item
/// in the model-facing copy. Both cold history and compaction use this renderer.
pub(super) fn historical_command_event_json(event: &Event) -> Result<Option<String>> {
    let item = match event {
        Event::ItemCompleted(value) => &value.item,
        Event::ItemUpdated(value) => &value.item,
        _ => return Ok(None),
    };
    let Some(projection) = item.historical_command_llm_projection() else {
        return Ok(None);
    };
    let mut value = serde_json::to_value(event)?;
    let item = value
        .get_mut("payload")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|payload| payload.get_mut("item"))
        .ok_or_else(|| anyhow::anyhow!("historical command event lost its item payload"))?;
    *item = serde_json::to_value(projection)?;
    Ok(Some(serde_json::to_string(&value)?))
}

/// Current canonical event renderer. Frozen restore also retains explicit
/// digest-checked compatibility candidates for older manifests.
pub(crate) fn event_message(event: Event) -> Result<Option<ChatMessage>> {
    event_message_with_input_copy_policy(event, false)
}

pub(crate) fn event_message_suppressing_input_copy_media(
    event: Event,
) -> Result<Option<ChatMessage>> {
    event_message_with_input_copy_policy(event, true)
}

/// Wire-compatible renderer for frozen manifests written before typed
/// historical media. It is deliberately kept separate from the execution
/// renderer so metadata text can prove an old digest without becoming the
/// source of attachment authority.
pub(crate) fn legacy_event_message(event: Event) -> Result<Option<ChatMessage>> {
    Ok(Some(match event {
        Event::TurnStarted(value) if !value.input.is_empty() => legacy_input_message(&value.input)?,
        Event::TurnMessageEdited(value) if !value.input.is_empty() => {
            legacy_input_message(&value.input)?
        }
        Event::TurnStarted(_) | Event::TurnMessageEdited(_) => return Ok(None),
        Event::TurnMessageDeleted(_) => return Ok(None),
        Event::ItemCompleted(value) => match value.item {
            pioneer_protocol::TurnItem::UserMessage { attachments, .. } => {
                if attachments.is_empty() {
                    return Ok(None);
                }
                ChatMessage::user(format!(
                    "Historical attachment references (metadata only; content is not reattached):\n{}",
                    serde_json::to_string(&attachments)?,
                ))
            }
            pioneer_protocol::TurnItem::AgentMessage { text, .. } => ChatMessage::assistant(text),
            pioneer_protocol::TurnItem::Reasoning {
                summary, content, ..
            } => ChatMessage::assistant(format!(
                "Reasoning recorded for a previous response:\n{}",
                content
                    .into_iter()
                    .chain(summary)
                    .collect::<Vec<_>>()
                    .join("\n")
            )),
            item => ChatMessage::user(format!(
                "Recorded historical event:\n{}",
                serde_json::to_string(&item)?
            )),
        },
        Event::TurnCompleted(value) => {
            ChatMessage::user(format!("Historical turn status: {:?}", value.turn.status))
        }
        Event::TurnFailed(value) => ChatMessage::user(format!(
            "Historical turn {:?}: {:?}",
            value.turn.status, value.turn.error
        )),
        Event::TurnBlocked(value) => {
            ChatMessage::user(format!("Historical turn blocked: {:?}", value.turn.error))
        }
        other => ChatMessage::user(format!(
            "Recorded historical status; not a successful model response:\n{}",
            serde_json::to_string(&other)?
        )),
    }))
}

fn event_message_with_input_copy_policy(
    event: Event,
    suppress_non_artifact_input_copy_media: bool,
) -> Result<Option<ChatMessage>> {
    Ok(Some(match event {
        Event::TurnStarted(value) if !value.input.is_empty() => input_message(&value.input)?,
        Event::TurnMessageEdited(value) if !value.input.is_empty() => input_message(&value.input)?,
        Event::TurnStarted(_) | Event::TurnMessageEdited(_) => return Ok(None),
        Event::TurnMessageDeleted(_) => return Ok(None),
        Event::ItemCompleted(value) => match value.item {
            pioneer_protocol::TurnItem::UserMessage { attachments, .. } => {
                if attachments.is_empty() {
                    return Ok(None);
                }
                // Artifact entries retain their accepted version as typed
                // canonical content. Materialization is deferred to the
                // request-owned resolver; authority is never reconstructed by
                // parsing a display string.
                let mut message = ChatMessage::user(String::new());
                let mut capability_metadata = Vec::new();
                for attachment in attachments {
                    if suppress_non_artifact_input_copy_media
                        && matches!(
                            &attachment,
                            pioneer_protocol::UserMessageAttachment::Image { .. }
                                | pioneer_protocol::UserMessageAttachment::LocalImage { .. }
                                | pioneer_protocol::UserMessageAttachment::File { .. }
                                | pioneer_protocol::UserMessageAttachment::LocalFile { .. }
                                | pioneer_protocol::UserMessageAttachment::Audio { .. }
                                | pioneer_protocol::UserMessageAttachment::LocalAudio { .. }
                                | pioneer_protocol::UserMessageAttachment::Video { .. }
                                | pioneer_protocol::UserMessageAttachment::LocalVideo { .. }
                        )
                    {
                        // The canonical input row is the authoritative copy of
                        // ordinary media. The linked input_copy event remains
                        // authoritative for ArtifactRef versions and capability
                        // metadata, but must not duplicate the same persisted
                        // UserInput in provider history.
                        continue;
                    }
                    if let Some(part) = historical_user_attachment_part(attachment.clone()) {
                        message.content_parts.push(part);
                    } else {
                        capability_metadata.push(attachment);
                    }
                }
                if !capability_metadata.is_empty() {
                    message.content = format!(
                        "Historical capability references (metadata only; not active capabilities):\n{}",
                        serde_json::to_string(&capability_metadata)?,
                    );
                }
                if message.content_parts.is_empty() && message.content.is_empty() {
                    return Ok(None);
                }
                message
            }
            pioneer_protocol::TurnItem::AgentMessage { text, .. } => ChatMessage::assistant(text),
            pioneer_protocol::TurnItem::Reasoning {
                summary, content, ..
            } => ChatMessage::assistant(format!(
                "Reasoning recorded for a previous response:\n{}",
                content
                    .into_iter()
                    .chain(summary)
                    .collect::<Vec<_>>()
                    .join("\n")
            )),
            item => ChatMessage::user(format!(
                "Recorded historical event:\n{}",
                historical_item_json(&item)?
            )),
        },
        Event::TurnCompleted(value) => {
            ChatMessage::user(format!("Historical turn status: {:?}", value.turn.status))
        }
        Event::TurnFailed(value) => ChatMessage::user(format!(
            "Historical turn {:?}: {:?}",
            value.turn.status, value.turn.error
        )),
        Event::TurnBlocked(value) => {
            ChatMessage::user(format!("Historical turn blocked: {:?}", value.turn.error))
        }
        event @ Event::ItemUpdated(_) => ChatMessage::user(format!(
            "Recorded historical status; not a successful model response:\n{}",
            match historical_command_event_json(&event)? {
                Some(projected) => projected,
                None => serde_json::to_string(&event)?,
            }
        )),
        other => ChatMessage::user(format!(
            "Recorded historical status; not a successful model response:\n{}",
            serde_json::to_string(&other)?
        )),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completed_command_event(output: &str) -> Event {
        Event::ItemCompleted(pioneer_protocol::ItemCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item: pioneer_protocol::TurnItem::CommandExecution {
                id: "command".into(),
                tool_name: "exec_command".into(),
                arguments: serde_json::json!({"command":["printf","fixture"],"cwd":"/workspace"}),
                status: pioneer_protocol::ToolCallStatus::Completed,
                recovery_policy: None,
                output_policy: pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(
                    "exec_command",
                ),
                display: pioneer_protocol::ToolDisplayPayload::Shell {
                    stdout: Some(output.into()),
                    stderr: None,
                    aggregated_output: Some(output.into()),
                    exit_code: Some(0),
                    duration_ms: Some(1),
                    timed_out: Some(false),
                    truncated: false,
                },
                storage: pioneer_protocol::ToolStoragePayload::Shell {
                    stdout: Some(output.into()),
                    stderr: None,
                    aggregated_output: Some(output.into()),
                    exit_code: Some(0),
                    duration_ms: Some(1),
                    timed_out: Some(false),
                    truncated: false,
                },
                recovery: None,
                command: vec!["printf".into(), "fixture".into()],
                cwd: Some("/workspace".into()),
                success: Some(true),
                outcome: None,
                observation: None,
            },
        })
    }

    #[test]
    fn cold_history_projects_completed_command_output_once() {
        let output = "cold-history-unique-output ".repeat(64);
        let message = event_message(completed_command_event(&output))
            .unwrap()
            .unwrap();
        assert_eq!(message.content.matches(&output).count(), 1);
        let payload = message
            .content
            .strip_prefix("Recorded historical event:\n")
            .expect("historical event prefix");
        let value: serde_json::Value = serde_json::from_str(payload).unwrap();
        let object = value.as_object().expect("historical projection object");
        assert!(!object.contains_key("outputPolicy"));
        assert!(!object.contains_key("storage"));
        assert!(!object.contains_key("display"));
        assert_eq!(
            value.pointer("/result/representations/0/exitCode"),
            Some(&serde_json::json!(0))
        );
        assert_eq!(
            value.pointer("/result/outputs/0/sources/0"),
            Some(&serde_json::json!("display"))
        );
    }

    #[test]
    fn command_event_has_distinct_legacy_and_model_renderers() {
        let legacy = legacy_event_message(completed_command_event("legacy-output"))
            .unwrap()
            .unwrap();
        assert_eq!(legacy.content.matches("legacy-output").count(), 4);
        let projected = event_message(completed_command_event("legacy-output"))
            .unwrap()
            .unwrap();
        assert_eq!(projected.content.matches("legacy-output").count(), 1);
    }

    #[test]
    fn updated_command_event_keeps_its_envelope_and_projects_output_once() {
        let output = "updated-command-unique-output ".repeat(64);
        let Event::ItemCompleted(completed) = completed_command_event(&output) else {
            unreachable!()
        };
        let mut item = completed.item;
        let pioneer_protocol::TurnItem::CommandExecution { storage, .. } = &mut item else {
            unreachable!()
        };
        let pioneer_protocol::ToolStoragePayload::Shell { stderr, .. } = storage else {
            unreachable!()
        };
        *stderr = Some("updated-storage-diagnostic".into());
        let event = Event::ItemUpdated(pioneer_protocol::ItemUpdatedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item,
        });
        let legacy = legacy_event_message(event.clone()).unwrap().unwrap();
        let current = event_message(event).unwrap().unwrap();
        assert!(legacy.content.matches(&output).count() > 1);
        assert_eq!(current.content.matches(&output).count(), 1);
        assert_eq!(
            current
                .content
                .matches("updated-storage-diagnostic")
                .count(),
            1
        );
        let json = current
            .content
            .strip_prefix("Recorded historical status; not a successful model response:\n")
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(value["kind"], "item_updated");
        assert_eq!(value["payload"]["workspace_id"], "ws");
        assert_eq!(value["payload"]["item"]["toolName"], "exec_command");
        assert!(value["payload"]["item"].get("display").is_none());
        assert!(value["payload"]["item"].get("storage").is_none());

        let noncommand = Event::ItemUpdated(pioneer_protocol::ItemUpdatedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item: pioneer_protocol::TurnItem::AgentMessage {
                id: "agent".into(),
                text: "unchanged update".into(),
                phase: Default::default(),
                markdown: None,
                markdown_version: None,
            },
        });
        assert_eq!(
            event_message(noncommand.clone()).unwrap(),
            legacy_event_message(noncommand).unwrap()
        );
    }

    #[test]
    fn malformed_or_failed_round_cannot_become_completed_inherited_work() {
        let envelope = CanonicalProviderRoundEnvelope {
            version: 1,
            round_id: "round".into(),
            termination: pioneer_provider::ProviderTermination::ToolCalls,
            message: ChatMessage::assistant_tool_calls(
                None::<String>,
                vec![pioneer_provider::ProviderToolCall {
                    id: "call".into(),
                    name: "read_file".into(),
                    arguments: "{}".into(),
                }],
            ),
            calls: vec![pioneer_provider::ProviderCallIdentity {
                provider_call_id: "call".into(),
                turn_item_id: "item".into(),
                ordinal: 0,
            }],
        };
        for bad in 0..5 {
            let mut envelope = envelope.clone();
            match bad {
                0 => envelope.termination = pioneer_provider::ProviderTermination::ProviderError,
                1 => envelope.round_id.clear(),
                2 => envelope.message.tool_calls.as_mut().unwrap()[0]
                    .name
                    .clear(),
                3 => envelope.message.tool_calls.as_mut().unwrap()[0].arguments = "{".into(),
                _ => envelope.calls[0].turn_item_id.clear(),
            }
            let round = Round {
                sequence: 1,
                envelope,
                assistant_source: SourceRef {
                    scope: "context:turn".into(),
                    id: "source".into(),
                    version: "revision:1".into(),
                },
                results: BTreeMap::new(),
            };
            assert!(finish_round("ws", "thread", "turn", round, true, &mut Vec::new()).is_err());
        }
    }
}
