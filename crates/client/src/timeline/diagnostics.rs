//! Existing timeline and activity measurements used by the retained Desktop consumer.
//! Recording remains in the observability owner; this module exposes its typed seam.
#[cfg(feature = "qualification-diagnostics")]
pub use pioneer_observability::{
    AnimationSourceId, DiagnosticAction, RenderRegion, TimelineStage, Visibility,
};
pub use pioneer_observability::{
    DesktopCodeHighlightCacheStatus, DesktopCodeHighlightFallbackReason,
    DesktopCodeHighlightMetric, DesktopCodeHighlightOutcome, DesktopCodeHighlightTheme,
    DesktopTimelineCacheStatus, DesktopTimelineContentKind, DesktopTimelineOutcome,
    DesktopTimelineStage, DesktopTimelineStageMetric, record_desktop_code_highlight,
    record_desktop_timeline_stage, record_qualification_diagnostic,
};
