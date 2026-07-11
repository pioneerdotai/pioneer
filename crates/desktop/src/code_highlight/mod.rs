//! GPUI-independent syntax tokenization shared by timeline code blocks and Proposal 40 diff views.
//!
//! The module owns language aliases, themes, bounded parsing and cached byte-range spans. Callers
//! remain responsible for scheduling work off the render path and mapping spans into their UI.

mod cache;
mod highlighter;
mod language;
mod model;
mod theme;

pub(crate) use cache::{
    CodeHighlightJob, CodeHighlightLookup, DesktopCodeHighlightCache, make_highlight_key,
};
pub(crate) use highlighter::highlight_code;
pub(crate) use language::{CanonicalLanguage, normalize_language_hint};
pub(crate) use model::{
    CodeThemeId, HIGHLIGHT_ENGINE_REVISION, HighlightError, HighlightFallbackReason, HighlightKey,
    HighlightLimits, HighlightOutcome, HighlightSpan, HighlightedCode, Rgba8,
};

#[cfg(test)]
mod tests;
