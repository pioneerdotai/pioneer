//! Timeline row models used by platform renderers.

use crate::timeline::labels::RunningTurnDisplay;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TurnWorkGroupRow {
    pub toggle_key: String,
    pub anchor_entry_id: String,
    pub elapsed_ms: Option<u64>,
    pub is_open: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum TimelineCoalescedToolsKind {
    CompletedTaskTools,
    RepeatedTaskWait,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineCoalescedToolsRow {
    pub toggle_key: String,
    pub count: usize,
    pub is_open: bool,
    pub kind: TimelineCoalescedToolsKind,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TimelineRowKind {
    Item { timeline_index: usize },
    TurnWorkToggle(TurnWorkGroupRow),
    CoalescedTools(TimelineCoalescedToolsRow),
    RunningTurn(RunningTurnDisplay),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineRow {
    pub key: String,
    pub kind: TimelineRowKind,
}
