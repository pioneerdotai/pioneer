//! Immutable, revision-scoped timeline output. Only the thread registry accepts projections.

use super::{
    rows::{TimelineRow, TimelineRowKind},
    semantic::{SemanticTimelineRowId, SemanticTimelineRowKind, SemanticTimelineRows},
    semantic_render::render_semantic_timeline_rows,
};
use crate::threads::registry::ThreadDomainSnapshot;
use crate::{
    cli_runtime::approvals::{CLIRuntimePendingRequestEntry, PendingRequest},
    conversation::{ConversationViewState, ItemView},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RowId(String);
impl RowId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelinePendingRequestRow {
    pub key: String,
    pub request: PendingRequest,
    pub author: Option<pioneer_protocol::TurnAuthorSnapshot>,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimelineRenderRow {
    Timeline(TimelineRow),
    PendingRequest(TimelinePendingRequestRow),
}
impl TimelineRenderRow {
    pub fn key(&self) -> &str {
        match self {
            Self::Timeline(row) => &row.key,
            Self::PendingRequest(row) => &row.key,
        }
    }
}

/// A self-contained row. Positional indexes in the value are local to this row.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelineRowSnapshot {
    id: RowId,
    revision: u64,
    content_revision: u64,
    metadata_revision: u64,
    value: TimelineRenderRow,
    item: Option<ItemView>,
    content: Option<super::item_presentation::TimelineItemPresentation>,
    turn_id: Option<String>,
    anchor_item_id: Option<String>,
    semantic_id: Option<SemanticTimelineRowId>,
}
impl TimelineRowSnapshot {
    pub fn id(&self) -> &RowId {
        &self.id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn metadata_revision(&self) -> u64 {
        self.metadata_revision
    }
    pub fn content_revision(&self) -> u64 {
        self.content_revision
    }
    pub fn value(&self) -> &TimelineRenderRow {
        &self.value
    }
    pub fn content(&self) -> Option<&super::item_presentation::TimelineItemPresentation> {
        self.content.as_ref()
    }
    pub fn item(&self) -> Option<&ItemView> {
        self.item.as_ref()
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelineGroup {
    pub id: String,
    pub first_row: usize,
    pub last_row: usize,
    pub user_message: bool,
    pub current_principal: bool,
    pub author: Option<pioneer_protocol::TurnAuthorSnapshot>,
    pub has_running: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Serialize)]
pub struct TimelineSnapshot {
    thread_id: String,
    generation: u64,
    source_revision: u64,
    revision: u64,
    rows: Arc<Vec<Arc<TimelineRowSnapshot>>>,
    groups: Arc<Vec<TimelineGroup>>,
    page: super::semantic::TimelineLoadedRange,
    has_loaded_page: bool,
    status: super::semantic::TimelineRequestStatus,
    // Existing native rendering uses these immutable typed lookup tables.
    #[serde(skip)]
    projection: Arc<ConversationViewState>,
    #[serde(skip)]
    render_rows: Arc<Vec<TimelineRenderRow>>,
    #[serde(skip)]
    semantic_rows: Arc<SemanticTimelineRows>,
    #[serde(skip)]
    semantic_row_ids: Arc<HashMap<String, SemanticTimelineRowId>>,
    #[serde(skip)]
    row_revisions: Arc<HashMap<String, u64>>,
}
impl TimelineSnapshot {
    /// Incremental publications serialize only revision metadata; row values travel in the change set.
    pub fn serialized_header(&self) -> serde_json::Value {
        serde_json::json!({ "thread_id": self.thread_id, "generation": self.generation, "source_revision": self.source_revision, "revision": self.revision, "groups": self.groups, "page": self.page, "status": self.status, "has_loaded_page": self.has_loaded_page })
    }

    pub(crate) fn presented_boundary_allows(
        &self,
        key: &super::semantic::SemanticTimelineRequestKey,
        visible: &[usize],
        threshold: usize,
    ) -> bool {
        use super::semantic::SemanticTimelineRequestKey as K;
        let Some(first) = visible.iter().min() else {
            return false;
        };
        let last = *visible.iter().max().unwrap();
        match key {
            K::ThreadBefore { .. } => *first <= threshold,
            K::ThreadAfter { .. } => {
                last >= self.rows.len().saturating_sub(1).saturating_sub(threshold)
            }
            K::TurnWorkBefore { turn_id, .. } | K::TurnWorkAfter { turn_id, .. } => {
                let span = self
                    .rows
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| {
                        row.item.is_some()
                            && row.turn_id.as_ref() == Some(turn_id)
                            && matches!(
                                row.semantic_id,
                                Some(SemanticTimelineRowId::TurnWorkItem { .. })
                            )
                    })
                    .map(|(ix, _)| ix)
                    .collect::<Vec<_>>();
                let shown = span
                    .iter()
                    .filter(|ix| visible.contains(ix))
                    .copied()
                    .collect::<Vec<_>>();
                let (Some(start), Some(end), Some(shown_start), Some(shown_end)) =
                    (span.first(), span.last(), shown.first(), shown.last())
                else {
                    return false;
                };
                if matches!(key, K::TurnWorkBefore { .. }) {
                    *shown_start <= start.saturating_add(threshold)
                } else {
                    *shown_end >= end.saturating_sub(threshold)
                }
            }
            _ => true,
        }
    }
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn source_revision(&self) -> u64 {
        self.source_revision
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn groups(&self) -> Arc<Vec<TimelineGroup>> {
        self.groups.clone()
    }
    pub fn rows(&self) -> &[Arc<TimelineRowSnapshot>] {
        &self.rows
    }
    pub fn projection(&self) -> Arc<ConversationViewState> {
        self.projection.clone()
    }
    pub fn render_rows(&self) -> Arc<Vec<TimelineRenderRow>> {
        self.render_rows.clone()
    }
    pub fn semantic_rows(&self) -> Arc<SemanticTimelineRows> {
        self.semantic_rows.clone()
    }
    pub fn semantic_row_ids(&self) -> Arc<HashMap<String, SemanticTimelineRowId>> {
        self.semantic_row_ids.clone()
    }
    pub fn row_revisions(&self) -> Arc<HashMap<String, u64>> {
        self.row_revisions.clone()
    }
}

#[derive(Clone, Serialize)]
pub struct ThreadPresentationSnapshot {
    thread_id: String,
    timeline: Arc<TimelineSnapshot>,
}
impl ThreadPresentationSnapshot {
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }
    pub fn timeline(&self) -> Arc<TimelineSnapshot> {
        self.timeline.clone()
    }
    pub(crate) fn new(timeline: Arc<TimelineSnapshot>) -> Self {
        Self {
            thread_id: timeline.thread_id.clone(),
            timeline,
        }
    }
}

/// Apply removals first, then replacements/insertions, then the complete new order.
/// Replacements keep their identity. An absent order means membership/order is unchanged.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimelineChangeSet {
    pub thread_id: String,
    pub generation: u64,
    pub from_revision: u64,
    pub to_revision: u64,
    pub inserted: Vec<Arc<TimelineRowSnapshot>>,
    pub removed: Vec<RowId>,
    pub replaced: Vec<Arc<TimelineRowSnapshot>>,
    pub order: Option<Vec<RowId>>,
}

pub(crate) fn project(
    id: &str,
    generation: u64,
    source: &ThreadDomainSnapshot,
    previous: Option<&TimelineSnapshot>,
    revision: u64,
    now_ms: i64,
) -> (Arc<TimelineSnapshot>, TimelineChangeSet) {
    let coordinator = source.coordinator();
    let mut projection = ConversationViewState {
        turns: coordinator.conversation.projection().turns.clone(),
        ..Default::default()
    };
    if let Some(thread) = coordinator.thread() {
        for turn in &thread.turns {
            projection.upsert_turn_snapshot_metadata(turn);
        }
    }
    let semantic_rows = super::semantic::flatten_semantic_timeline(&source.semantic(), id)
        .unwrap_or_else(|| SemanticTimelineRows {
            thread_id: id.to_owned(),
            ..Default::default()
        });
    let model = render_semantic_timeline_rows(&semantic_rows.rows, projection);
    let mut projection = model.projection;
    projection.revision = revision;
    let mut render_rows: Vec<_> = model
        .rows
        .into_iter()
        .map(TimelineRenderRow::Timeline)
        .collect();
    let previous_by_id: HashMap<_, _> = previous
        .into_iter()
        .flat_map(|p| p.rows.iter())
        .map(|row| (row.id.as_str(), row))
        .collect();
    for row in &mut render_rows {
        if let TimelineRenderRow::Timeline(TimelineRow {
            kind: TimelineRowKind::RunningTurn(running),
            key,
            ..
        }) = row
        {
            if running.started_at_unix_ms.is_none() {
                running.started_at_unix_ms = previous_by_id
                    .get(key.as_str())
                    .and_then(|row| match &row.value {
                        TimelineRenderRow::Timeline(TimelineRow {
                            kind: TimelineRowKind::RunningTurn(old),
                            ..
                        }) => old.started_at_unix_ms,
                        _ => None,
                    })
                    .or(Some(now_ms));
            }
        }
    }
    let semantic_pending = semantic_rows.rows.iter().filter_map(|row| {
        let SemanticTimelineRowKind::PendingRequest { block } = &row.kind else {
            return None;
        };
        let pioneer_protocol::TimelineBlockKind::PendingRequest {
            runtime_id,
            request_id,
            status,
            item_id,
            request,
            ..
        } = &block.kind
        else {
            return None;
        };
        status.is_open().then(|| {
            (
                CLIRuntimePendingRequestEntry {
                    workspace_id: block.workspace_id.clone(),
                    runtime_id: runtime_id.clone(),
                    request_id: request_id.clone(),
                    thread_id: Some(block.thread_id.clone()),
                    turn_id: block.turn_id.clone(),
                    item_id: item_id.clone(),
                    visible_thread_ids: Vec::new(),
                    request: request.clone(),
                }
                .into_pending_request(),
                row.author.clone(),
            )
        })
    });
    let pending = semantic_pending.chain(
        source
            .pending()
            .iter()
            .cloned()
            .map(|request| (request, None)),
    );
    let running_ix = render_rows
        .iter()
        .position(|row| {
            matches!(
                row,
                TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::RunningTurn(_),
                    ..
                })
            )
        })
        .unwrap_or(render_rows.len());
    let mut seen = HashSet::new();
    let pending = pending
        .filter_map(|(request, author)| {
            let key = format!("timeline-pending-request::{}", request.request_id);
            seen.insert(key.clone())
                .then_some(TimelineRenderRow::PendingRequest(
                    TimelinePendingRequestRow {
                        key,
                        request,
                        author,
                    },
                ))
        })
        .collect::<Vec<_>>();
    render_rows.splice(running_ix..running_ix, pending);

    let mut inserted = Vec::new();
    let mut replaced = Vec::new();
    let mut row_revisions = HashMap::new();
    let rows = render_rows
        .iter()
        .map(|render_row| {
            let mut value = render_row.clone();
            let item = match &mut value {
                TimelineRenderRow::Timeline(TimelineRow {
                    kind:
                        TimelineRowKind::Item { timeline_index }
                        | TimelineRowKind::UserMessage { timeline_index, .. },
                    ..
                }) => {
                    let item = projection
                        .timeline
                        .get(*timeline_index)
                        .and_then(|entry| projection.item_for_timeline_entry(entry))
                        .cloned();
                    *timeline_index = 0;
                    item
                }
                _ => None,
            };
            let id = RowId(value.key().to_owned());
            let old = previous_by_id.get(id.as_str());
            let turn_id = item
                .as_ref()
                .map(|item| item.turn_id.clone())
                .or_else(|| match &value {
                    TimelineRenderRow::Timeline(TimelineRow {
                        kind: TimelineRowKind::TurnWorkToggle(group),
                        ..
                    }) => toggle_turn_id(&group.toggle_key),
                    TimelineRenderRow::Timeline(TimelineRow {
                        kind: TimelineRowKind::CoalescedTools(group),
                        ..
                    }) => toggle_turn_id(&group.toggle_key),
                    TimelineRenderRow::Timeline(TimelineRow {
                        kind: TimelineRowKind::RunningTurn(running),
                        ..
                    }) => Some(running.turn_id.clone()),
                    TimelineRenderRow::PendingRequest(pending) => pending.request.turn_id.clone(),
                    _ => None,
                });
            let anchor_item_id = match &value {
                TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::TurnWorkToggle(group),
                    ..
                }) => projection
                    .timeline
                    .iter()
                    .find(|entry| {
                        entry.id == group.anchor_entry_id || entry.item_id == group.anchor_entry_id
                    })
                    .map(|entry| entry.item_id.clone()),
                _ => None,
            };
            let mut content = item.as_ref().map(super::item_presentation::project_item);
            if let (
                Some(content),
                TimelineRenderRow::Timeline(TimelineRow {
                    kind: TimelineRowKind::UserMessage { presentation, .. },
                    ..
                }),
            ) = (&mut content, &value)
            {
                content.attachments = presentation
                    .attachments
                    .iter()
                    .map(super::item_presentation::project_attachment)
                    .collect();
            }
            let mut row = TimelineRowSnapshot {
                content,
                turn_id,
                anchor_item_id,
                semantic_id: model.semantic_row_ids.get(id.as_str()).cloned(),
                id,
                // A reinserted identity must not alias an older subscriber or measurement cache.
                revision: old.map_or(revision, |r| r.revision),
                content_revision: old.map_or(revision, |r| r.content_revision),
                metadata_revision: old.map_or(revision, |r| r.metadata_revision),
                value,
                item,
            };
            let row = if let Some(old) = old.filter(|old| old.as_ref() == &row) {
                (*old).clone()
            } else {
                if let Some(old) = old {
                    row.revision = old.revision + 1;
                    row.content_revision = old.content_revision
                        + u64::from(
                            old.item != row.item
                                || old.content != row.content
                                || !same_row_body(&old.value, &row.value),
                        );
                    row.metadata_revision = old.metadata_revision
                        + u64::from(
                            row_author(&old.value) != row_author(&row.value)
                                || old.turn_id != row.turn_id
                                || old.anchor_item_id != row.anchor_item_id
                                || old.semantic_id != row.semantic_id,
                        );
                }
                let row = Arc::new(row);
                if old.is_some() {
                    replaced.push(row.clone());
                } else {
                    inserted.push(row.clone());
                }
                row
            };
            row_revisions.insert(row.id.0.clone(), row.revision);
            row
        })
        .collect::<Vec<_>>();
    let order: Vec<_> = rows.iter().map(|row| row.id.clone()).collect();
    let old_order: Vec<_> = previous
        .into_iter()
        .flat_map(|p| p.rows.iter())
        .map(|row| row.id.clone())
        .collect();
    let ids: HashSet<_> = order.iter().collect();
    let changes = TimelineChangeSet {
        thread_id: id.to_owned(),
        generation,
        from_revision: revision - 1,
        to_revision: revision,
        removed: old_order
            .iter()
            .filter(|id| !ids.contains(id))
            .cloned()
            .collect(),
        inserted,
        replaced,
        order: (order != old_order).then_some(order),
    };
    let groups = project_timeline_groups(&render_rows, &projection, source.current_principal_id());
    let semantic = source.semantic();
    let top_level = semantic.thread(id).map(|thread| &thread.top_level);
    let page = top_level
        .map(|cache| cache.loaded_range.clone())
        .unwrap_or_default();
    let status = top_level
        .map(|cache| cache.request_status.clone())
        .unwrap_or_default();
    (
        Arc::new(TimelineSnapshot {
            has_loaded_page: top_level.is_some_and(|cache| cache.has_loaded_page),
            groups: Arc::new(groups),
            page,
            status,
            thread_id: id.to_owned(),
            generation,
            source_revision: source.timeline_revision(),
            revision,
            rows: Arc::new(rows),
            projection: Arc::new(projection),
            render_rows: Arc::new(render_rows),
            semantic_rows: Arc::new(semantic_rows),
            semantic_row_ids: Arc::new(model.semantic_row_ids),
            row_revisions: Arc::new(row_revisions),
        }),
        changes,
    )
}

/// Semantic groups use domain authorship and Turn identity; shells resolve only avatar/layout presentation.
pub fn project_timeline_groups(
    rows: &[TimelineRenderRow],
    projection: &ConversationViewState,
    current_principal_id: Option<&str>,
) -> Vec<TimelineGroup> {
    let mut groups: Vec<TimelineGroup> = Vec::new();
    let mut previous_cluster = None;
    for (ix, row) in rows.iter().enumerate() {
        let (author, user_message, current_principal, has_running, turn_id) = match row {
            TimelineRenderRow::Timeline(value) => {
                let item = match value.kind {
                    TimelineRowKind::Item { timeline_index }
                    | TimelineRowKind::UserMessage { timeline_index, .. } => projection
                        .timeline
                        .get(timeline_index)
                        .and_then(|entry| projection.item_for_timeline_entry(entry)),
                    _ => None,
                };
                let implicit = value.author.is_none()
                    && match &value.kind {
                        TimelineRowKind::Item { .. } => item.is_some_and(|item| {
                            matches!(item.item, pioneer_protocol::TurnItem::UserMessage { .. })
                        }),
                        TimelineRowKind::UserMessage { presentation, .. } => {
                            presentation.item_id == format!("user_{}", presentation.turn_id)
                                || presentation.item_id
                                    == format!("turn:{}:user", presentation.turn_id)
                                || presentation.block_id
                                    == format!("turn:{}:user", presentation.turn_id)
                        }
                        _ => false,
                    };
                let turn_id = match &value.kind {
                    TimelineRowKind::Item { timeline_index }
                    | TimelineRowKind::UserMessage { timeline_index, .. } => projection
                        .timeline
                        .get(*timeline_index)
                        .map(|entry| entry.turn_id.clone()),
                    _ => None,
                }
                .or_else(|| match &value.kind {
                    TimelineRowKind::RunningTurn(running) => Some(running.turn_id.clone()),
                    TimelineRowKind::TurnWorkToggle(group) => projection
                        .timeline
                        .iter()
                        .find(|entry| {
                            entry.id == group.anchor_entry_id
                                || entry.item_id == group.anchor_entry_id
                        })
                        .map(|entry| entry.turn_id.clone())
                        .or_else(|| toggle_turn_id(&group.toggle_key)),
                    TimelineRowKind::CoalescedTools(group) => toggle_turn_id(&group.toggle_key),
                    _ => None,
                });
                (value.author.clone(), implicit || matches!(value.kind, TimelineRowKind::UserMessage { .. }), implicit || matches!(value.kind, TimelineRowKind::UserMessage { .. }) && value.author.as_ref().is_some_and(|author| matches!(&author.actor, pioneer_protocol::PersistedActorRef::Principal(principal) if Some(principal.as_str()) == current_principal_id)), matches!(value.kind, TimelineRowKind::RunningTurn(_)), turn_id)
            }
            TimelineRenderRow::PendingRequest(value) => (
                value.author.clone(),
                false,
                false,
                false,
                value.request.turn_id.clone(),
            ),
        };
        let cluster = if current_principal {
            "current-user".to_owned()
        } else if user_message {
            format!(
                "user:{}",
                author
                    .as_ref()
                    .map(|author| serde_json::to_string(&author.actor).expect("actor serializes"))
                    .unwrap_or_else(|| row.key().to_owned())
            )
        } else {
            format!(
                "agent:{}",
                turn_id.unwrap_or_else(|| format!("standalone::{}", row.key()))
            )
        };
        if previous_cluster.as_ref() == Some(&cluster) {
            let group = groups.last_mut().expect("existing cluster");
            group.last_row = ix;
            group.has_running |= has_running;
        } else {
            let author = if user_message {
                author
            } else {
                author.filter(|author| {
                    matches!(
                        author.actor,
                        pioneer_protocol::PersistedActorRef::AgentExecution(_)
                    )
                })
            };
            groups.push(TimelineGroup {
                id: row.key().to_owned(),
                first_row: ix,
                last_row: ix,
                user_message,
                current_principal,
                author,
                has_running,
            });
            previous_cluster = Some(cluster);
        }
    }
    groups
}

fn toggle_turn_id(key: &str) -> Option<String> {
    key.strip_prefix(super::semantic_render::SEMANTIC_TURN_WORK_GROUP_PREFIX)
        .or_else(|| key.strip_prefix("turn-work-group::"))
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn row_author(row: &TimelineRenderRow) -> &Option<pioneer_protocol::TurnAuthorSnapshot> {
    match row {
        TimelineRenderRow::Timeline(row) => &row.author,
        TimelineRenderRow::PendingRequest(row) => &row.author,
    }
}
fn same_row_body(a: &TimelineRenderRow, b: &TimelineRenderRow) -> bool {
    match (a, b) {
        (TimelineRenderRow::Timeline(a), TimelineRenderRow::Timeline(b)) => {
            let mut a = a.kind.clone();
            let mut b = b.kind.clone();
            if let TimelineRowKind::UserMessage { presentation, .. } = &mut a {
                presentation.author = None;
            }
            if let TimelineRowKind::UserMessage { presentation, .. } = &mut b {
                presentation.author = None;
            }
            a == b
        }
        (TimelineRenderRow::PendingRequest(a), TimelineRenderRow::PendingRequest(b)) => {
            a.request == b.request
        }
        _ => false,
    }
}
