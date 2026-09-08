use super::TimelineLayoutIndex;
use gpui_kit::*;
use std::{collections::HashMap, rc::Rc};
use terminal::TerminalView;
type TimelineMeasurementPass =
    Rc<std::cell::RefCell<Option<Box<dyn FnOnce(&mut Window, &mut App)>>>>;

pub(crate) struct TimelineViewState {
    pub(crate) measurement: Option<TimelineMeasurementPass>,
    pub(crate) layout_generation: u64,
    pub(crate) scroll_handle: gpui_kit::component::VirtualListScrollHandle,
    pub(crate) expanded: std::cell::RefCell<std::collections::HashSet<String>>,
    pub(crate) model: super::TimelineRenderModel,
    pub(crate) prepared: Option<super::view::PreparedTimeline>,
    pub(crate) viewport: Bounds<Pixels>,
    pub(crate) viewport_offset: Point<Pixels>,
    pub(crate) visible_range: std::ops::Range<usize>,
    pub(crate) viewport_revision: u64,
    pub(crate) layout_rem: Pixels,
    pub(crate) visible: bool,
    pub(crate) demand_generation: u64,
    pub(crate) demand_active: bool,
    pub(crate) reconciliation_pending: bool,
    presentation: std::cell::RefCell<TimelinePresentationState>,
}
impl Default for TimelineViewState {
    fn default() -> Self {
        Self {
            scroll_handle: gpui_kit::component::VirtualListScrollHandle::new(),
            expanded: Default::default(),
            model: super::TimelineRenderModel::empty(),
            prepared: None,
            measurement: None,
            layout_generation: 0,
            viewport: Default::default(),
            viewport_offset: Default::default(),
            visible_range: 0..0,
            viewport_revision: 0,
            layout_rem: px(0.),
            visible: true,
            demand_generation: 1,
            demand_active: false,
            reconciliation_pending: false,
            presentation: Default::default(),
        }
    }
}
impl std::ops::Deref for TimelineViewState {
    type Target = std::cell::RefCell<TimelinePresentationState>;
    fn deref(&self) -> &Self::Target {
        &self.presentation
    }
}
#[derive(Default)]
pub(crate) struct TimelinePresentationState {
    pub(crate) active_thread_id: Option<String>,
    pub(crate) item_count: usize,
    pub(crate) tail_entry_id: Option<String>,
    pub(crate) tail_text_len: usize,
    pub(crate) autoscroll_paused_by_user: bool,
    pub(crate) pending_follow_bottom: bool,
    pub(crate) measured_list_width: Pixels,
    pub(crate) entry_layout_cache: HashMap<String, CachedTimelineEntryLayout>,
    pub(crate) cached_render_active_thread_id: Option<String>,
    pub(crate) cached_render_width_px: i32,
    pub(crate) cached_render_item_count: usize,
    pub(crate) cached_render_tail_entry_id: Option<String>,
    pub(crate) cached_render_tail_fingerprint: u64,
    pub(crate) cached_render_model_fingerprint: u64,
    pub cached_render_expanded_revision: u64,
    pub(crate) cached_render_principal_id: Option<String>,
    pub(crate) cached_render_task_child_thread: bool,
    pub(crate) cached_item_sizes: Option<Rc<Vec<Size<Pixels>>>>,
    pub(crate) cached_timeline_layout_index: Option<Rc<TimelineLayoutIndex>>,
    pub(crate) expanded_revision: u64,
    pub(crate) scroll: super::TimelineScrollState,
    pub(crate) semantic_prefetch_scroll_generation: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CachedTimelineEntryLayout {
    pub(crate) render_fingerprint: u64,
    pub(crate) height: Pixels,
}

pub(crate) struct CachedTimelineTerminal {
    pub(crate) content_hash: u64,
    pub(crate) view: Entity<TerminalView>,
}
