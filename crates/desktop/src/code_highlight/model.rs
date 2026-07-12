use std::{fmt, ops::Range};

pub(crate) const HIGHLIGHT_ENGINE_REVISION: u16 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CodeThemeId {
    Light,
    Dark,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct HighlightKey {
    pub(crate) source_sha256: [u8; 32],
    pub(crate) canonical_language: String,
    pub(crate) theme: CodeThemeId,
    pub(crate) engine_revision: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HighlightLimits {
    pub(crate) max_source_bytes: usize,
    pub(crate) max_spans: usize,
}

impl HighlightLimits {
    pub(crate) const DESKTOP: Self = Self {
        max_source_bytes: 256 * 1024,
        max_spans: 40_000,
    };

    #[cfg(test)]
    pub(crate) const fn new(max_source_bytes: usize, max_spans: usize) -> Self {
        Self {
            max_source_bytes,
            max_spans,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Rgba8 {
    pub(crate) red: u8,
    pub(crate) green: u8,
    pub(crate) blue: u8,
    pub(crate) alpha: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HighlightSpan {
    pub(crate) byte_range: Range<usize>,
    pub(crate) foreground: Rgba8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HighlightedCode {
    pub(crate) key: HighlightKey,
    pub(crate) resolved_language: Option<String>,
    pub(crate) spans: Vec<HighlightSpan>,
    pub(crate) source_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum HighlightFallbackReason {
    Empty,
    Plaintext,
    UnknownLanguage,
    SourceTooLarge,
    SpanLimit,
    ParserError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HighlightOutcome {
    Highlighted(HighlightedCode),
    Fallback(HighlightFallbackReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HighlightError {
    InvalidSourceRange {
        start: usize,
        end: usize,
        source_bytes: usize,
    },
    SourceReconstruction,
}

impl fmt::Display for HighlightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSourceRange {
                start,
                end,
                source_bytes,
            } => write!(
                formatter,
                "highlighter returned invalid source range {start}..{end} for {source_bytes} bytes"
            ),
            Self::SourceReconstruction => {
                formatter.write_str("highlight spans did not reconstruct the source")
            }
        }
    }
}

impl std::error::Error for HighlightError {}
