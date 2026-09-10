//! Retained row rendering. Screen handles are used only to dispatch user actions.
use super::{layout_store::RowMeasurementKey, row_registry::TimelineRowSlotView, *};
use crate::screen::TimelineView;
use gpui_kit::{prelude::*, *};
use pioneer_client::timeline::types::TurnAuthorSnapshot;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[derive(Clone)]
pub(super) struct RowActions {
    pub owner: WeakEntity<TimelineView>,
}
impl RowActions {
    pub fn listener<E: 'static>(
        &self,
        action: impl Fn(&mut TimelineView, &E, &mut Window, &mut Context<TimelineView>) + 'static,
    ) -> impl Fn(&E, &mut Window, &mut App) + 'static {
        let owner = self.owner.clone();
        move |event, window, cx| {
            let _ = owner.update(cx, |view, cx| action(view, event, window, cx));
        }
    }
}

#[derive(Clone)]
pub(crate) struct RowPresentation {
    pub(super) actions: RowActions,
    pub(super) thread_id: String,
    pub(super) principal_id: Option<PrincipalId>,
    pub(super) terminal_height: Pixels,
    pub(super) reasoning_body: SharedString,
    pub(super) reasoning_has_body: bool,
    workspace_id: Option<String>,
    author: TimelineAuthorPresentation,
    pub(super) catalog_input:
        Option<Arc<pioneer_client::composer::catalog::ComposerCatalogPublication>>,
    principal_capabilities: pioneer_client::authorization::PrincipalPresentationCapabilities,
    thread_capabilities: Option<pioneer_client::authorization::ThreadPresentationCapabilities>,
    artifact_policy: pioneer_client::artifacts::presentation::ArtifactPresentationPolicy,
    artifact_paths: Vec<(
        pioneer_client::artifacts::preview::ArtifactRef,
        Option<PathBuf>,
    )>,
    file_opener: String,
    task_child: bool,
    pub(super) pending_request_views:
        HashMap<(String, String), Entity<crate::approvals::PendingRequestView>>,
    pub(super) task_review_views:
        HashMap<(String, String), Entity<crate::task_review::TaskReviewActionView>>,
    highlights: HashMap<u64, Entity<super::code_highlighting::MarkdownHighlightController>>,
    dino: Option<Entity<super::running_indicator::RunningDinoView>>,
    spinner: Option<Entity<super::running_indicator::ActivityIndicatorView>>,
    assets: Arc<super::running_indicator::RunningDinoAssetLoader>,
    elapsed: Option<Entity<super::running_indicator::RunningElapsedView>>,
    snapshot: Arc<pioneer_client::timeline::presentation::TimelineRowSnapshot>,
    projection: pioneer_client::conversation::ConversationViewState,
    content: super::TimelineItemPresentations,
    terminal: Option<super::terminal_registry::TerminalPresentation>,
    pub(super) body_only: bool,
    body: Option<(Entity<WorkItemBodyView>, Pixels)>,
    key: RowMeasurementKey,
    group_author: Option<TurnAuthorSnapshot>,
}
impl RowPresentation {
    pub(super) fn new(
        slot: Arc<TimelineRowSlotView>,
        key: RowMeasurementKey,
        group_author: Option<TurnAuthorSnapshot>,
        screen: &TimelineView,
        cx: &mut Context<TimelineView>,
    ) -> Self {
        let exact_author = match slot.snapshot().value() {
            TimelineRenderRow::Timeline(row) => row.author.as_ref(),
            _ => None,
        };
        let artifact_policy = screen.active_artifact_presentation_policy();
        let artifact_paths = slot
            .snapshot()
            .item()
            .and_then(|item| match &item.item {
                pioneer_client::timeline::types::TurnItem::UserMessage { attachments, .. } => Some(
                    pioneer_client::timeline::labels::parse_user_attachments(attachments)
                        .into_iter()
                        .filter_map(|attachment| attachment.artifact)
                        .map(|artifact| {
                            let path = if artifact_policy.can_open {
                                screen.thread_artifact_preview_path(&artifact, false)
                            } else {
                                None
                            };
                            (artifact, path)
                        })
                        .collect(),
                ),
                _ => None,
            })
            .unwrap_or_default();
        let mut highlights = HashMap::new();
        if let Some(document) = slot
            .snapshot()
            .content()
            .and_then(|c| c.markdown_presentation.as_ref())
        {
            for node in document.code_blocks() {
                let id =
                    super::markdown::markdown_node_interaction_id(&document.document_id, node.id);
                if let Some(entry) = screen.markdown_highlights.borrow().get(&id) {
                    highlights.insert(id, entry.view.clone());
                }
            }
        }
        let reasoning_body = slot
            .snapshot()
            .item()
            .map(|item| match &item.item {
                pioneer_client::timeline::types::TurnItem::Reasoning {
                    summary, content, ..
                } => pioneer_client::timeline::labels::reasoning_text(
                    summary,
                    content,
                    Self::timeline_entry_text(item),
                ),
                _ => String::new(),
            })
            .unwrap_or_default();
        let (dino, elapsed) = (None, None);
        Self {
            actions: RowActions {
                owner: cx.weak_entity(),
            },
            thread_id: screen.thread_id.clone(),
            terminal_height: slot.snapshot().item().filter(|item| matches!(item.item,
                pioneer_client::timeline::types::TurnItem::CommandExecution { .. })).map(|item| {
                    let width = key.content_width - if key.grouping.avatar_group_kind == Some(TimelineAvatarGroupKind::Agent) {
                        layout::TIMELINE_AVATAR_RAIL_WIDTH
                    } else { px(0.) };
                    let text = pioneer_client::timeline::labels::command_execution_terminal_text(
                        &item.item, Self::timeline_entry_text(item), |output| Self::truncate_for_card(output, 24_000));
                    Self::command_body_height(&text, width)
                }).unwrap_or(px(140.)),
            reasoning_has_body: !reasoning_body.trim().is_empty(),
            reasoning_body: reasoning_body.into(),
            principal_id: screen
                .identity_input
                .as_ref()
                .and_then(|p| p.current_auth.as_ref())
                .map(|p| p.principal.id.clone()),
            workspace_id: screen.thread_workspace_id(&screen.thread_id),
            author: screen.current_timeline_author_presentation(exact_author),
            catalog_input: screen.catalog_input.clone(),
            principal_capabilities: screen.principal_presentation_capabilities(),
            thread_capabilities: screen.thread_presentation_capabilities(&screen.thread_id),
            artifact_policy,
            artifact_paths,
            file_opener: screen.active_thread_file_opener(cx),
            task_child: screen.active_task_thread_navigation().is_some(),
            pending_request_views: screen.pending_request_views.iter().filter(|((thread,id),_)| thread == &screen.thread_id && matches!(slot.snapshot().value(), TimelineRenderRow::PendingRequest(row) if &row.request.request_id == id)).map(|(id,view)|(id.clone(),view.clone())).collect(),
            task_review_views: screen.task_review_views.iter().filter(|((thread,id),_)| thread == &screen.thread_id && slot.snapshot().content().and_then(|c|c.tool.as_ref()).and_then(|t|t.task_review.as_ref()).is_some_and(|review| review.items.iter().any(|item| &item.candidate_id == id))).map(|(id,view)|(id.clone(),view.clone())).collect(),
            highlights,
            dino,
            spinner: None,
            assets: screen
                .avatar_activities
                .borrow()
                .assets_loader
                .clone(),
            elapsed,
            snapshot: slot.snapshot().clone(),
            projection: slot.projection.clone(),
            content: slot.content.clone(),
            terminal: None,
            body_only: false,
            body: None,
            key,
            group_author,
        }
    }
    pub(super) fn render(&self, cx: &mut App) -> AnyElement {
        self.render_timeline_row(
            &self.projection,
            &self.content,
            self.snapshot.value(),
            self.key.last,
            self.key.grouping,
            self.group_author.as_ref(),
            self.key.content_width,
            self.key.expanded,
            self.terminal.as_ref().map(|t| t.view.clone()),
            cx,
        )
    }
    fn same_input(&self, other: &Self) -> bool {
        self.key == other.key
            && self.workspace_id == other.workspace_id
            && self.file_opener == other.file_opener
            && self.task_child == other.task_child
            && self.pending_request_views == other.pending_request_views
            && self.task_review_views == other.task_review_views
            && self.highlights == other.highlights
    }
    pub(super) fn tool_content(
        &self,
    ) -> Option<&pioneer_client::timeline::item_presentation::TimelineToolContent> {
        self.snapshot
            .content()
            .and_then(|content| content.tool.as_ref())
    }
    pub(super) fn current_active_thread_id(&self) -> Option<&str> {
        Some(&self.thread_id)
    }
    pub(super) fn active_task_thread_navigation(&self) -> Option<()> {
        self.task_child.then_some(())
    }
    pub(super) fn current_timeline_author_presentation(
        &self,
        _: Option<&TurnAuthorSnapshot>,
    ) -> TimelineAuthorPresentation {
        self.author.clone()
    }
    pub(super) fn principal_presentation_capabilities(
        &self,
    ) -> pioneer_client::authorization::PrincipalPresentationCapabilities {
        self.principal_capabilities
    }
    pub(super) fn thread_presentation_capabilities(
        &self,
        _: &str,
    ) -> Option<pioneer_client::authorization::ThreadPresentationCapabilities> {
        self.thread_capabilities
    }
    pub(super) fn active_artifact_presentation_policy(
        &self,
    ) -> pioneer_client::artifacts::presentation::ArtifactPresentationPolicy {
        self.artifact_policy
    }
    pub(super) fn thread_artifact_preview_path(
        &self,
        artifact: &pioneer_client::artifacts::preview::ArtifactRef,
        _: bool,
    ) -> Option<PathBuf> {
        self.artifact_paths
            .iter()
            .find(|(id, _)| id == artifact)
            .and_then(|(_, path)| path.clone())
    }
    pub(super) fn active_thread_file_opener(&self, _: &App) -> String {
        self.file_opener.clone()
    }
    pub(super) fn timeline_entry_text(
        item: &pioneer_client::conversation::reducer::ItemView,
    ) -> &str {
        pioneer_client::timeline::labels::timeline_entry_text(item)
    }
    pub(super) fn render_code_highlighted_text(&self, id: u64, source: &str) -> AnyElement {
        self.highlights
            .get(&id)
            .map(|v| v.clone().into_any_element())
            .unwrap_or_else(|| {
                StyledText::new(SharedString::new(Arc::<str>::from(source))).into_any_element()
            })
    }
    pub(super) fn running_turn_dino_view(
        &self,
        _: String,
        _: &mut App,
    ) -> Option<Entity<super::running_indicator::RunningDinoView>> {
        self.dino.clone()
    }
}

pub(crate) struct TimelineRowView {
    #[cfg(test)]
    renders: usize,
    presentation: RowPresentation,
    visible: bool,
}
impl TimelineRowView {
    pub(super) fn new(
        presentation: RowPresentation,
        body_height: Option<Pixels>,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut owner = Self {
            #[cfg(test)]
            renders: 0,
            presentation,
            visible: false,
        };
        owner.prepare_activity(cx);
        owner.prepare_body(body_height, cx);
        owner
    }
    fn prepare_body(&mut self, height: Option<Pixels>, cx: &mut Context<Self>) {
        if !self.presentation.supports_body() {
            self.presentation.body = None;
            return;
        }
        let Some(height) = height else {
            return;
        };
        let mut body_input = self.presentation.clone();
        body_input.body_only = true;
        body_input.body = None;
        let view = if let Some((view, _)) = &self.presentation.body {
            view.update(cx, |view, cx| {
                // Position in the list and expansion belong to the row shell.
                // They do not change the intrinsic body or invalidate its render.
                let mut comparable = body_input.clone();
                comparable.key.last = view.presentation.key.last;
                comparable.key.expanded = view.presentation.key.expanded;
                comparable.key.grouping.top_spacing = view.presentation.key.grouping.top_spacing;
                if !view.presentation.same_input(&comparable) {
                    view.presentation = body_input;
                    cx.notify();
                }
            });
            view.clone()
        } else {
            cx.new(|_| WorkItemBodyView {
                #[cfg(test)]
                renders: 0,
                presentation: body_input,
            })
        };
        self.presentation.body = Some((view, height));
    }
    pub(super) fn synchronize(
        &mut self,
        mut presentation: RowPresentation,
        body_height: Option<Pixels>,
        cx: &mut Context<Self>,
    ) {
        if self.presentation.same_input(&presentation) {
            return;
        }
        presentation.body = self.presentation.body.take();
        presentation.terminal = self.presentation.terminal.take();
        if self.presentation.activity_input() == presentation.activity_input() {
            presentation.dino = self.presentation.dino.take();
            presentation.elapsed = self.presentation.elapsed.take();
            presentation.spinner = self.presentation.spinner.take();
        }
        self.presentation = presentation;
        self.prepare_activity(cx);
        self.prepare_body(body_height, cx);
        cx.notify();
    }
}
impl Render for TimelineRowView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        #[cfg(test)]
        {
            self.renders += 1;
        }
        self.presentation.render(cx)
    }
}

impl RowPresentation {
    pub(super) fn is_expanded(&self) -> bool {
        self.key.expanded
    }
    pub(super) fn supports_body(&self) -> bool {
        use pioneer_client::timeline::types::TurnItem;
        self.snapshot.item().is_some_and(|item| {
            matches!(
                item.item,
                TurnItem::Reasoning { .. }
                    | TurnItem::CommandExecution { .. }
                    | TurnItem::FileChange { .. }
                    | TurnItem::DynamicToolCall { .. }
                    | TurnItem::WebSearch { .. }
                    | TurnItem::WebFetch { .. }
                    | TurnItem::Download { .. }
            )
        })
    }
    pub(super) fn body_width(&self) -> Pixels {
        (self.key.content_width
            - if self.key.grouping.avatar_group_kind == Some(TimelineAvatarGroupKind::Agent) {
                layout::TIMELINE_AVATAR_RAIL_WIDTH
            } else {
                px(0.)
            }
            - 2. * layout::TIMELINE_CONTENT_HORIZONTAL_PADDING)
            .max(px(1.))
    }
    pub(super) fn body_element(&self) -> Option<AnyElement> {
        self.body
            .as_ref()
            .filter(|_| !self.body_only)
            .map(|(view, _)| view.clone().into_any_element())
    }

    pub(super) fn render_body(&self, cx: &mut App) -> AnyElement {
        let width = self.key.content_width
            - if self.key.grouping.avatar_group_kind == Some(TimelineAvatarGroupKind::Agent) {
                layout::TIMELINE_AVATAR_RAIL_WIDTH
            } else {
                px(0.)
            };
        self.render_timeline_row_body(
            &self.projection,
            &self.content,
            self.snapshot.value(),
            self.key.last,
            self.key.grouping.top_spacing,
            width,
            self.key.expanded,
            self.terminal.as_ref().map(|t| t.view.clone()),
            cx,
        )
    }
    pub(super) fn body_measurement(
        &self,
        cx: &mut App,
    ) -> Option<(
        pioneer_client::timeline::presentation::RowId,
        Pixels,
        AnyElement,
    )> {
        if !self.supports_body() {
            return None;
        }
        let mut body = self.clone();
        body.body_only = true;
        body.body = None;
        Some((
            self.snapshot.id().clone(),
            self.body_width(),
            body.render_body(cx),
        ))
    }
}
struct WorkItemBodyView {
    #[cfg(test)]
    renders: usize,
    presentation: RowPresentation,
}
impl Render for WorkItemBodyView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        #[cfg(test)]
        {
            self.renders += 1;
        }
        self.presentation.render_body(cx)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowActivityInput {
    started_at: Option<i64>,
    dino: bool,
    placement: super::running_indicator::ElapsedPlacement,
    spinner: Option<super::running_indicator::ActivitySpinnerKind>,
}
impl RowActivityInput {
    fn for_row(
        snapshot: &pioneer_client::timeline::presentation::TimelineRowSnapshot,
        task_child: bool,
    ) -> Self {
        use super::running_indicator::ActivitySpinnerKind as Source;
        use super::running_indicator::ElapsedPlacement;
        use pioneer_client::timeline::types::TurnItem;
        let mut activity = RowActivityInput {
            started_at: None,
            dino: false,
            placement: ElapsedPlacement::Inline,
            spinner: None,
        };
        match snapshot.value() {
            TimelineRenderRow::Timeline(TimelineRow {
                kind: TimelineRowKind::RunningTurn(turn),
                ..
            }) => {
                activity.started_at = turn.started_at_unix_ms;
                activity.dino = !task_child;
                activity.placement = ElapsedPlacement::Running {
                    show_dino: activity.dino,
                };
            }
            _ => {
                if let Some(item) = snapshot.item().filter(|i| {
                    i.status == pioneer_client::conversation::TimelineEntryStatus::Running
                }) {
                    activity.started_at = item.started_at_unix_ms;
                    activity.spinner = match &item.item {
                        TurnItem::Reasoning { .. } => Some(Source::Reasoning),
                        TurnItem::CommandExecution { .. } => Some(Source::Command),
                        TurnItem::FileChange { .. } => Some(Source::FileChange),
                        TurnItem::DynamicToolCall { .. } => Some(Source::DynamicTool),
                        TurnItem::WebSearch { .. } => Some(Source::WebSearch),
                        TurnItem::WebFetch { .. } => Some(Source::WebFetch),
                        TurnItem::Download { .. } => Some(Source::Download),
                        TurnItem::Task { item: task } => {
                            activity.dino = true;
                            activity.started_at = task
                                .started_at
                                .map(|time| time.saturating_mul(1_000))
                                .or(item.started_at_unix_ms)
                                .or(Some(task.created_at.saturating_mul(1_000)));
                            activity.placement = ElapsedPlacement::Running { show_dino: true };
                            None
                        }
                        _ => {
                            activity.started_at = None;
                            None
                        }
                    };
                }
            }
        }
        activity
    }
}
impl RowPresentation {
    fn activity_input(&self) -> RowActivityInput {
        RowActivityInput::for_row(&self.snapshot, self.task_child)
    }
    pub(super) fn spinner_element(
        &self,
        _: super::running_indicator::ActivitySpinnerKind,
    ) -> AnyElement {
        if !self.body_only {
            if let Some(spinner) = &self.spinner {
                return spinner.clone().into_any_element();
            }
        }
        div().size_4().into_any_element()
    }
    pub(super) fn inline_elapsed(&self) -> Option<AnyElement> {
        if self.body_only || self.activity_input().started_at.is_none() {
            return None;
        }
        Some(
            self.elapsed
                .as_ref()
                .map(|v| v.clone().into_any_element())
                .unwrap_or_else(|| div().flex_shrink_0().into_any_element()),
        )
    }
}
impl TimelineRowView {
    fn prepare_activity(&mut self, cx: &mut Context<Self>) {
        use super::running_indicator::{
            ActivityIndicatorView, RunningDinoView, RunningElapsedView,
        };
        let input = self.presentation.activity_input();
        if input.dino && self.presentation.dino.is_none() {
            self.presentation.dino =
                Some(cx.new(|_| RunningDinoView::new(self.presentation.assets.clone(), false)));
        }
        if let Some(start) = input
            .started_at
            .filter(|_| self.presentation.elapsed.is_none())
        {
            self.presentation.elapsed =
                Some(cx.new(|_| RunningElapsedView::new(start, input.placement)));
        }
        if let Some(source) = input
            .spinner
            .filter(|_| self.presentation.spinner.is_none())
        {
            self.presentation.spinner = Some(cx.new(|_| ActivityIndicatorView::new(source)));
        }
        self.set_visible(self.visible, cx);
    }
    fn set_terminal(
        &mut self,
        terminal: Option<super::terminal_registry::TerminalPresentation>,
        cx: &mut Context<Self>,
    ) {
        let changed = self
            .presentation
            .terminal
            .as_ref()
            .map(|t| t.view.entity_id())
            != terminal.as_ref().map(|t| t.view.entity_id());
        self.presentation.terminal = terminal.clone();
        // The measured body has its own presentation. Release its reference too.
        if let Some((body, _)) = &self.presentation.body {
            body.update(cx, |body, cx| {
                body.presentation.terminal = terminal;
                if changed {
                    cx.notify();
                }
            });
        }
        if changed {
            cx.notify();
        }
    }
    pub(super) fn set_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        self.visible = visible;
        if let Some(view) = &self.presentation.dino {
            view.update(cx, |view, cx| view.set_visible(visible, cx));
        }
        if let Some(view) = &self.presentation.elapsed {
            view.update(cx, |view, cx| view.set_visible(visible, cx));
        }
        if let Some(view) = &self.presentation.spinner {
            view.update(cx, |view, cx| view.set_visible(visible, cx));
        }
    }
}

impl TimelineView {
    pub(crate) fn set_row_activities_visible(
        &self,
        visible: &std::collections::HashSet<String>,
        cx: &mut Context<Self>,
    ) {
        for slot in self.row_registry.slots() {
            if let Some(view) = &slot.view {
                let shown = visible.contains(slot.snapshot().id().as_str());
                if view.read(cx).visible != shown {
                    view.update(cx, |view, cx| view.set_visible(shown, cx));
                }
            }
        }
    }

    pub(crate) fn set_row_terminals_visible(
        &self,
        visible: &std::collections::HashSet<String>,
        cx: &mut Context<Self>,
    ) {
        // Data and geometry remain retained; terminal emulators belong only to
        // the visible rows, not every work item loaded while scrolling history.
        let slots = self.row_registry.slots();
        let terminal_rows = slots
            .iter()
            .filter(|slot| {
                visible.contains(slot.snapshot().id().as_str())
                    && slot
                        .view
                        .as_ref()
                        .is_some_and(|view| view.read(cx).presentation.key.expanded)
            })
            .map(|slot| slot.snapshot().id().as_str())
            .collect::<std::collections::HashSet<_>>();
        self.thread_timeline_terminal_item
            .borrow_mut()
            .retain(|id| terminal_rows.contains(id));
        for slot in &slots {
            if let Some(view) = &slot.view {
                if !terminal_rows.contains(slot.snapshot().id().as_str())
                    && view.read(cx).presentation.terminal.is_none()
                {
                    continue;
                }
                let terminal =
                    if terminal_rows.contains(slot.snapshot().id().as_str()) {
                        slot.snapshot().item().filter(|item| matches!(item.item,
                        pioneer_client::timeline::types::TurnItem::CommandExecution { .. }))
                        .map(|item| {
                            let entry = &slot.projection.timeline[0];
                            let previous = view.read(cx).presentation.terminal.clone();
                            let width = view.read(cx).presentation.key.content_width;
                            self.prepare_command_terminal(entry, item, width, previous.as_ref(), cx)
                        })
                    } else {
                        None
                    };
                view.update(cx, |view, cx| {
                    view.set_terminal(terminal, cx);
                });
            }
        }
    }
}

impl TimelineRowView {
    pub(super) fn matches_activity(
        &self,
        snapshot: &pioneer_client::timeline::presentation::TimelineRowSnapshot,
    ) -> bool {
        self.presentation.activity_input()
            == RowActivityInput::for_row(snapshot, self.presentation.task_child)
    }
    pub(super) fn matches_input(&self, key: &RowMeasurementKey) -> bool {
        &self.presentation.key == key
    }
}
pub(super) fn row_dependency_revision(
    screen: &TimelineView,
    row: &pioneer_client::timeline::presentation::TimelineRowSnapshot,
    cx: &App,
) -> u64 {
    use pioneer_client::timeline::types::TurnItem;
    use std::hash::{Hash, Hasher};
    fn ptr<T>(value: &Option<Arc<T>>) -> usize {
        value.as_ref().map_or(0, |p| Arc::as_ptr(p) as usize)
    }
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    match row.item().map(|item| &item.item) {
        Some(TurnItem::UserMessage { .. }) => {
            ptr(&screen.identity_input).hash(&mut hash);
            ptr(&screen.thread_member_input).hash(&mut hash);
            ptr(&screen.artifact_input).hash(&mut hash);
            ptr(&screen.thread_capability_input).hash(&mut hash);
            screen
                .active_artifact_presentation_policy()
                .can_open
                .hash(&mut hash);
        }
        Some(TurnItem::DynamicToolCall { .. }) => {
            ptr(&screen.identity_input).hash(&mut hash);
            ptr(&screen.thread_capability_input).hash(&mut hash);
            ptr(&screen.catalog_input).hash(&mut hash);
        }
        _ => {}
    }
    if row
        .content()
        .is_some_and(|c| c.markdown_presentation.is_some())
    {
        screen.active_thread_file_opener(cx).hash(&mut hash);
    }
    if let TimelineRenderRow::PendingRequest(row) = row.value() {
        screen
            .pending_request_views
            .get(&(screen.thread_id.clone(), row.request.request_id.clone()))
            .map(|v| v.entity_id())
            .hash(&mut hash);
    }
    if let Some(review) = row
        .content()
        .and_then(|c| c.tool.as_ref())
        .and_then(|t| t.task_review.as_ref())
    {
        for item in &review.items {
            screen
                .task_review_views
                .get(&(screen.thread_id.clone(), item.candidate_id.clone()))
                .map(|v| v.entity_id())
                .hash(&mut hash);
        }
    }
    hash.finish()
}

#[cfg(test)]
impl TimelineRowView {
    pub(crate) fn terminal_for_test(&self) -> Option<Entity<terminal::TerminalView>> {
        self.presentation
            .terminal
            .as_ref()
            .map(|terminal| terminal.view.clone())
    }
    pub(crate) fn render_counts(&self, cx: &App) -> (usize, usize) {
        (
            self.renders,
            self.presentation
                .body
                .as_ref()
                .map_or(0, |(view, _)| view.read(cx).renders),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_support::*,
        thread::{ThreadView, ThreadViewConfig},
    };
    use core::prelude::v1::test;
    use gpui_kit::component::Root;
    use pioneer_client::{
        core::{ClientCore, ClientScope},
        timeline::semantic::{TopLevelPageMergeMode, WorkPageMergeMode},
    };

    fn publish_work(client: &Arc<ClientCore>, revision: i64, extra: bool) {
        let work = serde_json::json!({"turnId":"turn","presentation":"expanded_terminal_no_final","state":"completed",
            "workCount":7,"visibleWorkCount":7,"hiddenWorkCount":0,"hasMoreBefore":false,"hasMoreAfter":false});
        let mut blocks = vec![
            serde_json::json!({"workspaceId":"workspace","threadId":"a","blockId":"work-block",
            "turnId":"turn","sortKey":"1","kind":{"kind":"turn_work","work":work}}),
        ];
        if extra {
            blocks.push(serde_json::json!({"workspaceId":"workspace","threadId":"a","blockId":"extra",
            "turnId":"next","sortKey":"2","kind":{"kind":"user_message","text":"An unrelated message","mode":"Message"}}));
        }
        client.apply_thread_timeline_page(
            serde_json::from_value(serde_json::json!({
            "workspaceId":"workspace","threadId":"a","projectionVersion":1,"blocks":blocks,
            "page":{"hasMoreBefore":false,"hasMoreAfter":false}}))
            .unwrap(),
            TopLevelPageMergeMode::Reset,
        );
        let kinds = [
            ("reasoning", "reasoning"),
            ("commandExecution", "command_execution"),
            ("fileChange", "file_change"),
            ("dynamicToolCall", "dynamic_tool_call"),
            ("webSearch", "web_search"),
            ("webFetch", "web_fetch"),
            ("download", "download"),
        ];
        let items = kinds.into_iter().enumerate().map(|(index,(kind,item_type))| {
            let mut item = serde_json::json!({"type":kind,"id":kind,"toolName":"synthetic","arguments":{"query":"query","url":"https://example.test"},
                "status":"in_progress","command":["synthetic"],"summary":["Thinking about a synthetic example"],"content":["Reasoning body"],
                "outputPolicy":{"llm":{"mode":"summary_only"},"llmRetention":{"mode":"do_not_retain"},
                    "timeline":{"mode":"full","max_bytes":24000},"storage":{"mode":"none"},"recovery":{"mode":"none"},"deltas":{"mode":"disabled"}},
                "display":{"kind":"shell","stdout":"Synthetic body text","truncated":false},"storage":{"kind":"none"}});
            if kind == "webSearch" { item["results"] = serde_json::json!([{ "rank":1,"source":"synthetic","title":"Example result","url":"https://example.test","snippet":"Result description"}]); }
            let changed = kind == "dynamicToolCall" && revision >= 3;
            if changed { item["display"]["stdout"] = serde_json::json!("Changed body"); }
            if changed && revision >= 4 { item["status"] = serde_json::json!("completed"); }
            serde_json::json!({"workItemId":kind,"itemId":kind,"turnId":"turn","orderKey":format!("{index:03}"),
                "sourceSequence":if changed {revision} else {1},"sourceUpdatedAtUnixMicros":if changed {revision} else {1},
                "itemType":item_type,"status":if changed && revision >= 4 {"completed"} else {"running"},"item":item})
        }).collect::<Vec<_>>();
        client.apply_turn_work_page(serde_json::from_value(serde_json::json!({
            "workspaceId":"workspace","threadId":"a","turnId":"turn","projectionVersion":1,
            "sourceHighWatermark":revision,"projectionUpdatedAtUnixMicros":revision,"work":work,"items":items,
            "page":{"hasMoreBefore":false,"hasMoreAfter":false}})).unwrap(),WorkPageMergeMode::Reset);
        client.set_thread_turn_work_expanded("a", "turn", true);
        let _flush = client.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            std::num::NonZeroUsize::new(8).unwrap(),
        );
    }
    #[test]
    fn activity_source_survives_content_updates_but_ends_with_semantic_completion() {
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "message");
        let snapshot = || {
            client
                .snapshot(&ClientScope::Timeline {
                    thread_id: "a".into(),
                })
                .unwrap()
                .snapshot()
                .payload::<pioneer_client::timeline::presentation::TimelineSnapshot>()
                .unwrap()
        };
        publish_work(&client, 1, false);
        let first = snapshot();
        let first = first
            .rows()
            .iter()
            .find(|row| row.id().as_str() == "dynamicToolCall")
            .unwrap();
        let input = RowActivityInput::for_row(first, false);
        assert!(input.spinner.is_some());
        publish_work(&client, 3, false);
        let changed = snapshot();
        let changed = changed
            .rows()
            .iter()
            .find(|row| row.id().as_str() == "dynamicToolCall")
            .unwrap();
        assert_ne!(first.content_revision(), changed.content_revision());
        assert_eq!(input, RowActivityInput::for_row(changed, false));
        publish_work(&client, 4, false);
        let completed = snapshot();
        let completed = completed
            .rows()
            .iter()
            .find(|row| row.id().as_str() == "dynamicToolCall")
            .unwrap();
        assert!(
            RowActivityInput::for_row(completed, false)
                .spinner
                .is_none()
        );
    }
    struct RowHost {
        row: Entity<TimelineRowView>,
        width: Pixels,
    }
    impl Render for RowHost {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().w(self.width).child(self.row.clone())
        }
    }
    struct MeasurementProbe {
        element: std::rc::Rc<std::cell::RefCell<Option<AnyElement>>>,
        width: AvailableSpace,
        result: std::rc::Rc<std::cell::Cell<Size<Pixels>>>,
    }
    impl Render for MeasurementProbe {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            let element = self.element.clone();
            let width = self.width;
            let result = self.result.clone();
            canvas(
                move |_, window, cx| {
                    if let Some(mut element) = element.borrow_mut().take() {
                        result.set(element.layout_as_root(
                            size(width, AvailableSpace::MaxContent),
                            window,
                            cx,
                        ));
                    }
                },
                |_, _, _, _| (),
            )
            .size_full()
        }
    }
    fn measure(element: AnyElement, width: Pixels, cx: &mut VisualTestContext) -> Size<Pixels> {
        measure_space(element, AvailableSpace::Definite(width), cx)
    }
    fn measure_space(
        element: AnyElement,
        width: AvailableSpace,
        cx: &mut VisualTestContext,
    ) -> Size<Pixels> {
        let result = std::rc::Rc::new(std::cell::Cell::new(Size::default()));
        cx.update(|window, cx| {
            window.replace_root(cx, |window, cx| {
                Root::new(
                    cx.new(|_| MeasurementProbe {
                        element: std::rc::Rc::new(std::cell::RefCell::new(Some(element))),
                        width,
                        result: result.clone(),
                    }),
                    window,
                    cx,
                )
            });
            window.draw(cx).clear(cx);
        });
        cx.run_until_parked();
        result.get()
    }
    #[gpui_kit::test]
    fn elapsed_uses_intrinsic_width_and_keeps_long_labels_on_one_line(cx: &mut TestAppContext) {
        use super::super::running_indicator::{ElapsedPlacement, RunningElapsedView};
        cx.update(gpui_kit::init);
        let (_, cx) = cx.add_window_view(|window, cx| Root::new(cx.new(|_| Empty), window, cx));
        for placement in [
            ElapsedPlacement::Inline,
            ElapsedPlacement::Running { show_dino: true },
            ElapsedPlacement::Running { show_dino: false },
        ] {
            let elapsed = cx.new(|_| {
                RunningElapsedView::new(
                    pioneer_client::timeline::labels::now_unix_ms() - 1_234_567 * 3_600_000,
                    placement,
                )
            });
            elapsed.update(cx, |view, cx| view.set_visible(true, cx));
            // Use the same header container for both measurements, including
            // the running label's existing bottom margin.
            let intrinsic = measure_space(
                gpui_kit::component::h_flex()
                    .child(elapsed.clone())
                    .into_any_element(),
                AvailableSpace::MaxContent,
                cx,
            );
            assert!(
                intrinsic.width > px(70.),
                "long elapsed must grow to fit its text: {intrinsic:?}"
            );
            let narrow = measure(
                gpui_kit::component::h_flex()
                    .child(elapsed.clone())
                    .into_any_element(),
                px(70.),
                cx,
            );
            assert_eq!(
                narrow.height, intrinsic.height,
                "elapsed must not wrap in a narrow header"
            );
            elapsed.update(cx, |view, cx| view.set_visible(false, cx));
        }
    }
    struct Empty;
    impl Render for Empty {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div()
        }
    }
    struct NarrowThread(Entity<ThreadView>);
    impl Render for NarrowThread {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().w(px(480.)).h_full().child(self.0.clone())
        }
    }
    #[gpui_kit::test]
    fn first_row_paint_uses_pane_width_instead_of_window_width(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "A message in a narrow pane");
        let (registrar, deliver) = binding_router(client.clone());
        let mut timeline = None;
        let (_, cx) = cx.add_window_view(|window, cx| {
            let thread = ThreadView::new(
                ThreadViewConfig::new(
                    client.clone(),
                    "a".into(),
                    registrar,
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                ),
                window,
                cx,
            );
            timeline = Some(thread.read(cx).timeline_for_test());
            Root::new(cx.new(|_| NarrowThread(thread)), window, cx)
        });
        deliver();
        cx.run_until_parked();
        let timeline = timeline.unwrap();
        let mut painted = false;
        for _ in 0..6 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
            timeline.read_with(cx, |view, cx| {
                if let Some(prepared) = &view.thread_timeline_view_state.prepared {
                    for slot in &prepared.slots {
                        if let Some(owner) = &slot.view {
                            if owner.read(cx).render_counts(cx).0 > 0 {
                                painted = true;
                                assert_eq!(prepared.list_width, px(480.));
                                assert_eq!(owner.read(cx).presentation.key.content_width, px(480.));
                            }
                        }
                    }
                }
            });
        }
        assert!(painted, "the actual retained message must paint");
    }
    #[gpui_kit::test]
    fn large_work_history_retains_only_visible_expanded_terminals(cx: &mut TestAppContext) {
        const COMMANDS: usize = 128;
        cx.background_executor.allow_parking();
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "message");
        let output = "history line\n".repeat(1500);
        let work = serde_json::json!({"turnId":"turn","presentation":"expanded_terminal_no_final",
            "state":"completed","workCount":COMMANDS,"visibleWorkCount":COMMANDS,"hiddenWorkCount":0,
            "hasMoreBefore":false,"hasMoreAfter":false});
        client.apply_thread_timeline_page(
            serde_json::from_value(serde_json::json!({
                "workspaceId":"workspace","threadId":"a","projectionVersion":1,
                "blocks":[{"workspaceId":"workspace","threadId":"a","blockId":"work-block",
                    "turnId":"turn","sortKey":"1","kind":{"kind":"turn_work","work":work}}],
                "page":{"hasMoreBefore":false,"hasMoreAfter":false}
            }))
            .unwrap(),
            TopLevelPageMergeMode::Reset,
        );
        let items = (0..COMMANDS).map(|index| {
            let id = format!("command-{index:03}");
            serde_json::json!({"workItemId":id,"itemId":id,"turnId":"turn","orderKey":format!("{index:03}"),
                "sourceSequence":1,"sourceUpdatedAtUnixMicros":1,"itemType":"command_execution","status":"completed",
                "item":{"type":"commandExecution","id":id,"toolName":"synthetic","arguments":{},
                    "status":"completed","command":["synthetic"],
                    "outputPolicy":{"llm":{"mode":"summary_only"},"llmRetention":{"mode":"do_not_retain"},
                        "timeline":{"mode":"full","max_bytes":24000},"storage":{"mode":"none"},
                        "recovery":{"mode":"none"},"deltas":{"mode":"disabled"}},
                    "display":{"kind":"shell","stdout":output,"truncated":false},"storage":{"kind":"none"}}})
        }).collect::<Vec<_>>();
        client.apply_turn_work_page(
            serde_json::from_value(serde_json::json!({
                "workspaceId":"workspace","threadId":"a","turnId":"turn","projectionVersion":1,
                "sourceHighWatermark":1,"projectionUpdatedAtUnixMicros":1,"work":work,"items":items,
                "page":{"hasMoreBefore":false,"hasMoreAfter":false}
            }))
            .unwrap(),
            WorkPageMergeMode::Reset,
        );
        client.set_thread_turn_work_expanded("a", "turn", true);
        let (registrar, deliver) = binding_router(client.clone());
        let (root, cx) = cx.add_window_view(|window, cx| {
            Root::new(
                ThreadView::new(
                    ThreadViewConfig::new(
                        client.clone(),
                        "a".into(),
                        registrar,
                        Arc::new(ThreadPorts),
                        Arc::new(ThreadPorts),
                        Arc::new(ThreadPorts),
                    ),
                    window,
                    cx,
                ),
                window,
                cx,
            )
        });
        deliver();
        cx.run_until_parked();
        for _ in 0..4 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        let thread = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadView>().unwrap()
        });
        let timeline = thread.read_with(cx, |view, _| view.timeline_for_test());
        let terminals = |cx: &gpui_kit::VisualTestContext| {
            timeline.read_with(cx, |view, cx| {
                view.row_registry
                    .slots()
                    .iter()
                    .filter_map(|slot| slot.terminal_for_test(cx))
                    .map(|terminal| terminal.downgrade())
                    .collect::<Vec<_>>()
            })
        };
        timeline.read_with(cx, |view, _| {
            assert_eq!(
                view.row_registry
                    .slots()
                    .iter()
                    .filter(|s| s.snapshot().item().is_some())
                    .count(),
                COMMANDS
            );
        });
        assert!(
            terminals(cx).is_empty(),
            "collapsed history must not allocate terminal grids"
        );
        // Simulate the viewport advancing through the entire loaded history. All
        // commands remain expanded, as when returning to previously opened rows.
        let mut previous: Vec<gpui_kit::WeakEntity<terminal::TerminalView>> = Vec::new();
        for index in 0..COMMANDS {
            let id = format!("command-{index:03}");
            cx.update(|window, cx| {
                timeline.update(cx, |view, cx| {
                    super::super::controller::DesktopTimelineController::expand(
                        view, &id, window, cx,
                    );
                })
            });
            for _ in 0..2 {
                cx.update(|window, cx| window.draw(cx).clear(cx));
                cx.run_until_parked();
            }
            timeline.update(cx, |view, cx| {
                view.set_row_terminals_visible(&[id].into_iter().collect(), cx)
            });
            cx.run_until_parked();
            assert!(
                previous.iter().all(|terminal| terminal.upgrade().is_none()),
                "offscreen terminal survived"
            );
            previous = terminals(cx);
            assert_eq!(
                previous.len(),
                1,
                "terminal count must follow viewport, not loaded history"
            );
        }
        // Re-enter the first command: its full source still exists and can be replayed.
        timeline.update(cx, |view, cx| {
            view.set_row_terminals_visible(&["command-000".to_owned()].into_iter().collect(), cx)
        });
        cx.run_until_parked();
        assert!(previous.iter().all(|terminal| terminal.upgrade().is_none()));
        previous = terminals(cx);
        assert_eq!(previous.len(), 1);
        timeline.read_with(cx, |view, cx| {
            let slot = view
                .row_registry
                .slots()
                .into_iter()
                .find(|s| s.snapshot().id().as_str() == "command-000")
                .unwrap();
            let item = slot.snapshot().item().unwrap();
            assert_eq!(
                serde_json::to_value(&item.item).unwrap()["display"]["stdout"],
                output
            );
            let terminal = slot.terminal_for_test(cx).unwrap();
            assert!(
                terminal.read(cx).dimensions().1 >= 1500,
                "full output grid is preserved when reopened"
            );
        });
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                super::super::controller::DesktopTimelineController::expand(
                    view,
                    "command-000",
                    window,
                    cx,
                );
            })
        });
        for _ in 0..2 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        timeline.update(cx, |view, cx| {
            view.set_row_terminals_visible(&["command-000".to_owned()].into_iter().collect(), cx)
        });
        cx.run_until_parked();
        assert!(
            previous.iter().all(|terminal| terminal.upgrade().is_none()),
            "collapsing a visible command must release its grid"
        );
        assert!(terminals(cx).is_empty());
        timeline.update(cx, |view, cx| {
            view.set_row_terminals_visible(&["command-127".to_owned()].into_iter().collect(), cx)
        });
        previous = terminals(cx);
        assert_eq!(previous.len(), 1);
        cx.update(|window, cx| thread.update(cx, |view, cx| view.set_visible(false, window, cx)));
        cx.run_until_parked();
        assert!(previous.iter().all(|terminal| terminal.upgrade().is_none()));
        assert!(terminals(cx).is_empty());
    }

    #[gpui_kit::test]
    fn command_terminals_are_released_when_thread_is_hidden(cx: &mut TestAppContext) {
        cx.background_executor.allow_parking();
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "message");
        publish_work(&client, 1, false);
        let (registrar, deliver) = binding_router(client.clone());
        let (root, cx) = cx.add_window_view(|window, cx| {
            Root::new(
                ThreadView::new(
                    ThreadViewConfig::new(
                        client.clone(),
                        "a".into(),
                        registrar,
                        Arc::new(ThreadPorts),
                        Arc::new(ThreadPorts),
                        Arc::new(ThreadPorts),
                    ),
                    window,
                    cx,
                ),
                window,
                cx,
            )
        });
        deliver();
        cx.run_until_parked();
        for _ in 0..4 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        let thread = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadView>().unwrap()
        });
        let timeline = thread.read_with(cx, |view, _| view.timeline_for_test());
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                super::super::controller::DesktopTimelineController::dispatch(
                    view,
                    &super::super::controller::TimelineAction::Expand {
                        entry_id: "commandExecution".into(),
                    },
                    window,
                    cx,
                );
            })
        });
        for _ in 0..3 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        timeline.update(cx, |view, cx| {
            view.set_row_terminals_visible(
                &["commandExecution".to_owned()].into_iter().collect(),
                cx,
            )
        });
        let terminals = timeline.read_with(cx, |view, cx| {
            view.row_registry
                .slots()
                .iter()
                .filter_map(|slot| {
                    slot.view.as_ref().and_then(|row| {
                        row.read(cx)
                            .presentation
                            .terminal
                            .as_ref()
                            .map(|t| t.view.downgrade())
                    })
                })
                .collect::<Vec<_>>()
        });
        assert!(
            !terminals.is_empty(),
            "fixture must create a command terminal"
        );
        cx.update(|window, cx| thread.update(cx, |view, cx| view.set_visible(false, window, cx)));
        cx.run_until_parked();
        for terminal in terminals {
            assert!(
                terminal.upgrade().is_none(),
                "hidden thread retains the terminal through its row or body"
            );
        }
    }

    #[gpui_kit::test]
    fn row_owners_survive_insert_and_resize_and_drop_with_thread(cx: &mut TestAppContext) {
        cx.background_executor.allow_parking();
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "message");
        publish_work(&client, 1, false);
        let (registrar, deliver) = binding_router(client.clone());
        let (root, cx) = cx.add_window_view(|window, cx| {
            Root::new(
                ThreadView::new(
                    ThreadViewConfig::new(
                        client.clone(),
                        "a".into(),
                        registrar,
                        Arc::new(ThreadPorts),
                        Arc::new(ThreadPorts),
                        Arc::new(ThreadPorts),
                    ),
                    window,
                    cx,
                ),
                window,
                cx,
            )
        });
        deliver();
        cx.run_until_parked();
        for _ in 0..4 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        let thread = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadView>().unwrap()
        });
        let timeline = thread.read_with(cx, |view, _| view.timeline_for_test());
        let before = timeline.read_with(cx, |view, cx| {
            view.row_registry
                .slots()
                .iter()
                .map(|slot| {
                    let owner = slot.view.as_ref().unwrap();
                    (
                        slot.snapshot().id().clone(),
                        owner.entity_id(),
                        owner.downgrade(),
                        owner.read(cx).render_counts(cx),
                    )
                })
                .collect::<Vec<_>>()
        });
        publish_work(&client, 2, true);
        deliver();
        cx.run_until_parked();
        for _ in 0..4 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        timeline.read_with(cx, |view, cx| {
            for (id, identity, _, counts) in &before {
                let owner = view.row_registry.get(id).unwrap().view.as_ref().unwrap();
                assert_eq!(owner.entity_id(), *identity, "insertion replaced {id:?}");
                assert_eq!(
                    owner.read(cx).render_counts(cx).1,
                    counts.1,
                    "insertion rebuilt unchanged body {id:?}"
                );
            }
        });
        for width in [420., 960., 650., 420.] {
            cx.simulate_resize(size(px(width), px(700.)));
            for _ in 0..5 {
                cx.update(|window, cx| window.draw(cx).clear(cx));
                cx.run_until_parked();
            }
            timeline.read_with(cx, |view, cx| {
                let prepared = view.thread_timeline_view_state.prepared.as_ref().unwrap();
                assert_eq!(
                    prepared.list_width,
                    view.thread_timeline_view_state
                        .scroll_handle
                        .bounds()
                        .size
                        .width
                        .max(px(280.))
                );
                for slot in &prepared.slots {
                    assert_eq!(
                        slot.view
                            .as_ref()
                            .unwrap()
                            .read(cx)
                            .presentation
                            .key
                            .content_width,
                        prepared.content_width
                    );
                }
                for (id, identity, _, _) in &before {
                    assert_eq!(
                        view.row_registry
                            .get(id)
                            .unwrap()
                            .view
                            .as_ref()
                            .unwrap()
                            .entity_id(),
                        *identity
                    );
                }
            });
        }
        timeline.update(cx, |view, cx| view.retire_timeline_rows(cx));
        cx.run_until_parked();
        for (_, _, weak, _) in before {
            assert!(
                weak.upgrade().is_none(),
                "row must not retain its parent through a cycle"
            );
        }
    }
    #[gpui_kit::test]
    fn all_work_families_keep_inline_geometry_and_retained_identity_without_scene_cache(
        cx: &mut TestAppContext,
    ) {
        cx.background_executor.allow_parking();
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "message");
        publish_work(&client, 1, false);
        let (registrar, deliver) = binding_router(client.clone());
        let (root, cx) = cx.add_window_view(|window, cx| {
            Root::new(
                ThreadView::new(
                    ThreadViewConfig::new(
                        client.clone(),
                        "a".into(),
                        registrar,
                        Arc::new(ThreadPorts),
                        Arc::new(ThreadPorts),
                        Arc::new(ThreadPorts),
                    ),
                    window,
                    cx,
                ),
                window,
                cx,
            )
        });
        deliver();
        cx.run_until_parked();
        for _ in 0..4 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        let thread = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadView>().unwrap()
        });
        let timeline = thread.read_with(cx, |view, _| view.timeline_for_test());
        let slots = timeline.read_with(cx, |view, _| view.row_registry.slots());
        let rows = slots
            .iter()
            .filter_map(|s| s.view.clone())
            .filter(|v| v.read_with(cx, |v, _| v.presentation.supports_body()))
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 7, "every work item must have a retained owner");
        for row in rows {
            for width in [px(320.), px(600.), px(760.)] {
                // Measure the unchanged inline presentation and the retained version at
                // the same actual width. Retained bodies must not invent geometry.
                let mut input = row.read_with(cx, |view, _| view.presentation.clone());
                input.key.content_width = width;
                input.key.expanded = true;
                input.body = None;
                let (_, body_width, body) = cx.update(|_, cx| input.body_measurement(cx).unwrap());
                let height = measure(body, body_width, cx).height;
                assert!(
                    height > px(0.),
                    "nonempty body must reserve height: {:?}",
                    input.snapshot.id()
                );
                let natural_element = cx.update(|_, cx| input.render(cx));
                let natural = measure(natural_element, width, cx);
                row.update(cx, |view, cx| view.synchronize(input, Some(height), cx));
                let retained_input = row.read_with(cx, |view, _| view.presentation.clone());
                let retained_element = cx.update(|_, cx| retained_input.render(cx));
                let retained = measure(retained_element, width, cx);
                assert!(
                    (natural.height - retained.height).abs() < px(1.),
                    "at {width:?}: natural {natural:?}, retained {retained:?}"
                );
                assert_eq!(natural.width, retained.width);
                row.update(cx, |view, cx| view.set_visible(true, cx));
                cx.update(|window, cx| {
                    window.replace_root(cx, |window, cx| {
                        Root::new(
                            cx.new(|_| RowHost {
                                row: row.clone(),
                                width,
                            }),
                            window,
                            cx,
                        )
                    });
                });
                cx.run_until_parked();
                cx.update(|window, cx| window.draw(cx).clear(cx));
                cx.run_until_parked();
                let retained_body = row.read_with(cx, |v, _| {
                    v.presentation.body.as_ref().unwrap().0.entity_id()
                });
                let prepared_snapshot = row.read_with(cx, |v, _| v.presentation.snapshot.clone());
                let before = row.read_with(cx, |v, cx| v.render_counts(cx));
                let callbacks = cx.update(|window, cx| window.simulate_next_frame(cx));
                assert!(callbacks > 0, "stock Spinner must schedule a frame");
                cx.update(|window, cx| window.draw(cx).clear(cx));
                cx.run_until_parked();
                let after = row.read_with(cx, |v, cx| v.render_counts(cx));
                assert!(after.0 > before.0);
                assert!(after.1 > before.1, "uncached body must render at {width:?}");
                row.read_with(cx, |v, _| {
                    assert_eq!(
                        v.presentation.body.as_ref().unwrap().0.entity_id(),
                        retained_body
                    );
                    assert!(
                        Arc::ptr_eq(&v.presentation.snapshot, &prepared_snapshot),
                        "animation cannot reprepare semantic input"
                    );
                });
                row.update(cx, |v, cx| v.set_visible(false, cx));
                cx.update(|window, cx| window.draw(cx).clear(cx));
                cx.run_until_parked();
                cx.update(|window, cx| {
                    window.simulate_next_frame(cx);
                    window.draw(cx).clear(cx);
                });
                cx.run_until_parked();
                assert_eq!(
                    cx.update(|window, cx| window.simulate_next_frame(cx)),
                    0,
                    "no successor after suspension"
                );
            }
        }
    }
}
