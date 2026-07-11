use std::sync::OnceLock;

use syntect::{
    easy::HighlightLines,
    highlighting::Color,
    parsing::{SyntaxReference, SyntaxSet},
    util::LinesWithEndings,
};

use super::{
    CanonicalLanguage, CodeThemeId, HighlightError, HighlightFallbackReason, HighlightLimits,
    HighlightOutcome, HighlightSpan, HighlightedCode, Rgba8, make_highlight_key,
    normalize_language_hint,
};

static SYNTAX_SET: OnceLock<SyntaxSet> = OnceLock::new();

pub(crate) fn highlight_code(
    source: &str,
    language_hint: Option<&str>,
    theme_id: CodeThemeId,
    limits: HighlightLimits,
) -> Result<HighlightOutcome, HighlightError> {
    if source.is_empty() {
        return Ok(HighlightOutcome::Fallback(HighlightFallbackReason::Empty));
    }
    if source.len() > limits.max_source_bytes {
        return Ok(HighlightOutcome::Fallback(
            HighlightFallbackReason::SourceTooLarge,
        ));
    }

    let language = normalize_language_hint(language_hint);
    match language {
        CanonicalLanguage::Plaintext => {
            return Ok(HighlightOutcome::Fallback(
                HighlightFallbackReason::Plaintext,
            ));
        }
        CanonicalLanguage::Unknown => {
            return Ok(HighlightOutcome::Fallback(
                HighlightFallbackReason::UnknownLanguage,
            ));
        }
        CanonicalLanguage::Known(_) => {}
    }

    let syntaxes = SYNTAX_SET.get_or_init(two_face::syntax::extra_newlines);
    let Some(syntax) = resolve_syntax(syntaxes, language) else {
        return Ok(HighlightOutcome::Fallback(
            HighlightFallbackReason::ParserError,
        ));
    };
    let theme = super::theme::theme(theme_id);
    let mut highlighter = HighlightLines::new(syntax, theme);
    let mut spans = Vec::new();
    let mut source_offset = 0usize;

    for line in LinesWithEndings::from(source) {
        let highlighted = match highlighter.highlight_line(line, syntaxes) {
            Ok(highlighted) => highlighted,
            Err(_) => {
                return Ok(HighlightOutcome::Fallback(
                    HighlightFallbackReason::ParserError,
                ));
            }
        };
        let mut line_offset = 0usize;
        for (style, token) in highlighted {
            let start = source_offset.saturating_add(line_offset);
            let end = start.saturating_add(token.len());
            validate_range(source, start, end)?;
            if source.get(start..end) != Some(token) {
                return Err(HighlightError::SourceReconstruction);
            }
            push_merged_span(&mut spans, start..end, rgba(style.foreground));
            if spans.len() > limits.max_spans {
                return Ok(HighlightOutcome::Fallback(
                    HighlightFallbackReason::SpanLimit,
                ));
            }
            line_offset = line_offset.saturating_add(token.len());
        }
        if line_offset != line.len() {
            return Err(HighlightError::SourceReconstruction);
        }
        source_offset = source_offset.saturating_add(line.len());
    }
    if source_offset != source.len() {
        return Err(HighlightError::SourceReconstruction);
    }

    Ok(HighlightOutcome::Highlighted(HighlightedCode {
        key: make_highlight_key(source, language, theme_id),
        resolved_language: Some(language.cache_name().to_owned()),
        spans,
        source_bytes: source.len(),
    }))
}

fn resolve_syntax(syntaxes: &SyntaxSet, language: CanonicalLanguage) -> Option<&SyntaxReference> {
    language
        .syntax_token()
        .and_then(|token| syntaxes.find_syntax_by_token(token))
}

fn rgba(color: Color) -> Rgba8 {
    Rgba8 {
        red: color.r,
        green: color.g,
        blue: color.b,
        alpha: color.a,
    }
}

fn push_merged_span(spans: &mut Vec<HighlightSpan>, range: std::ops::Range<usize>, color: Rgba8) {
    if range.is_empty() {
        return;
    }
    if let Some(previous) = spans.last_mut()
        && previous.byte_range.end == range.start
        && previous.foreground == color
    {
        previous.byte_range.end = range.end;
        return;
    }
    spans.push(HighlightSpan {
        byte_range: range,
        foreground: color,
    });
}

fn validate_range(source: &str, start: usize, end: usize) -> Result<(), HighlightError> {
    if start > end
        || end > source.len()
        || !source.is_char_boundary(start)
        || !source.is_char_boundary(end)
    {
        return Err(HighlightError::InvalidSourceRange {
            start,
            end,
            source_bytes: source.len(),
        });
    }
    Ok(())
}
