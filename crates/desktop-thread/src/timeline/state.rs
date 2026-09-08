use gpui_kit::*;
use std::rc::Rc;
pub(crate) struct TimelineViewState {
    pub(crate) scroll_handle: gpui_kit::component::VirtualListScrollHandle,
    pub(crate) expanded: std::cell::RefCell<std::collections::HashSet<String>>,
    pub(crate) model: super::TimelineRenderModel,
    pub(crate) prepared: Option<super::view::PreparedTimeline>,
    pub(crate) viewport: Bounds<Pixels>,
    pub(crate) viewport_offset: Point<Pixels>,
    pub(crate) visible_range: std::ops::Range<usize>,
    pub(crate) viewport_revision: u64,
    pub(crate) layout_rem: Pixels,
    pub(crate) layout_text_style: TextStyle,
    pub(crate) layout_locale: String,
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
            viewport: Default::default(),
            viewport_offset: Default::default(),
            visible_range: 0..0,
            viewport_revision: 0,
            layout_rem: px(0.),
            layout_text_style: TextStyle::default(),
            layout_locale: rust_i18n::locale().to_string(),
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
    pub(crate) expanded_revision: u64,
    pub(crate) scroll: super::TimelineScrollState,
    pub(crate) semantic_prefetch_scroll_generation: u64,
}
