use super::TimelineLayoutIndex;
use gpui_kit::*;
use std::{collections::HashMap, rc::Rc};
use terminal::TerminalView;
#[derive(Default)]
pub(crate) struct ThreadTimelineViewState {
    pub(crate) active_thread_id: Option<String>,
    pub(crate) item_count: usize,
    pub(crate) tail_entry_id: Option<String>,
    pub(crate) tail_text_len: usize,
    pub(crate) last_read_requested_through_turn_id: Option<String>,
    pub(crate) autoscroll_paused_by_user: bool,
    pub(crate) measured_list_width: Pixels,
    pub(crate) pending_width_probe: bool,
    pub(crate) width_probe_attempts: u8,
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
    pub(crate) pending_scroll_anchor: Option<TimelineScrollAnchor>,
    pub(crate) semantic_prefetch_scroll_generation: u64,
    pub(crate) semantic_prefetch_consumed_scroll_generation: u64,
}

pub(crate) struct TimelineScrollAnchor {
    pub(crate) thread_id: String,
    pub(crate) row_key: String,
    pub(crate) row_top_offset_px: Pixels,
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
