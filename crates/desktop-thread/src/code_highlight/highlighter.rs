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
const MIN_TOKEN_CONTRAST: f64 = 3.0;

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
            push_merged_span(
                &mut spans,
                start..end,
                readable_foreground(style.foreground, theme_id),
            );
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

fn readable_foreground(color: Color, theme_id: CodeThemeId) -> Rgba8 {
    let background = super::theme::render_background(theme_id);
    let original = Rgba8 {
        red: color.r,
        green: color.g,
        blue: color.b,
        alpha: color.a,
    };
    if contrast_ratio(original, background) >= MIN_TOKEN_CONTRAST {
        return original;
    }

    let target = if relative_luminance(background.r, background.g, background.b) > 0.5 {
        0
    } else {
        255
    };
    let mut low = 1u16;
    let mut high = 255u16;
    while low < high {
        let amount = (low + high) / 2;
        let candidate = mix_toward(original, target, amount);
        if contrast_ratio(candidate, background) >= MIN_TOKEN_CONTRAST {
            high = amount;
        } else {
            low = amount + 1;
        }
    }
    mix_toward(original, target, low)
}

fn mix_toward(color: Rgba8, target: u8, amount: u16) -> Rgba8 {
    fn channel(value: u8, target: u8, amount: u16) -> u8 {
        let value = u16::from(value);
        let target = u16::from(target);
        let mixed = if target >= value {
            value + ((target - value) * amount + 127) / 255
        } else {
            value - ((value - target) * amount + 127) / 255
        };
        mixed as u8
    }

    Rgba8 {
        red: channel(color.red, target, amount),
        green: channel(color.green, target, amount),
        blue: channel(color.blue, target, amount),
        alpha: color.alpha,
    }
}

fn contrast_ratio(foreground: Rgba8, background: Color) -> f64 {
    let foreground = relative_luminance(foreground.red, foreground.green, foreground.blue);
    let background = relative_luminance(background.r, background.g, background.b);
    let (lighter, darker) = if foreground >= background {
        (foreground, background)
    } else {
        (background, foreground)
    };
    (lighter + 0.05) / (darker + 0.05)
}

fn relative_luminance(red: u8, green: u8, blue: u8) -> f64 {
    fn channel(value: u8) -> f64 {
        let value = f64::from(value) / 255.0;
        if value <= 0.04045 {
            value / 12.92
        } else {
            ((value + 0.055) / 1.055).powf(2.4)
        }
    }
    0.2126 * channel(red) + 0.7152 * channel(green) + 0.0722 * channel(blue)
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
