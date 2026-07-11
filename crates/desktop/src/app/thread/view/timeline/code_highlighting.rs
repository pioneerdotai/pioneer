use std::{sync::Arc, time::Instant};

use gpui::{prelude::*, *};
use gpui_component::theme::ActiveTheme as _;

use crate::{
    app::PioneerDesktop,
    code_highlight::{
        CodeHighlightJob, CodeHighlightLookup, CodeThemeId, HighlightFallbackReason,
        HighlightLimits, HighlightOutcome, Rgba8, highlight_code, normalize_language_hint,
    },
};

impl PioneerDesktop {
    pub(super) fn render_code_highlighted_text(
        &self,
        source: &str,
        language_hint: Option<&str>,
        cx: &mut Context<Self>,
    ) -> StyledText {
        let theme = if cx.theme().mode.is_dark() {
            CodeThemeId::Dark
        } else {
            CodeThemeId::Light
        };
        let request = self.code_highlight_cache.borrow_mut().request(
            source,
            language_hint,
            theme,
            HighlightLimits::DESKTOP,
        );
        if request.observe_immediate_fallback {
            let (outcome, fallback_reason) = match &request.lookup {
                CodeHighlightLookup::Fallback(reason) => ("fallback", fallback_reason(*reason)),
                CodeHighlightLookup::Unavailable => ("fallback", "cache_capacity"),
                _ => ("error", "unexpected_immediate_state"),
            };
            log_code_highlight_observation(
                normalize_language_hint(language_hint).cache_name(),
                source.len(),
                theme,
                "miss",
                outcome,
                fallback_reason,
                0,
                0,
            );
        }
        if request.observe_cache_hit {
            let (outcome, fallback_reason, span_count) = match &request.lookup {
                CodeHighlightLookup::Ready(code) => ("highlighted", "none", code.spans.len()),
                CodeHighlightLookup::Fallback(reason) => ("fallback", fallback_reason(*reason), 0),
                _ => ("error", "unexpected_cache_hit_state", 0),
            };
            log_code_highlight_observation(
                normalize_language_hint(language_hint).cache_name(),
                source.len(),
                theme,
                "hit",
                outcome,
                fallback_reason,
                span_count,
                0,
            );
        }
        for job in request.jobs {
            Self::spawn_code_highlight_job(job, cx);
        }

        let text = SharedString::new(Arc::<str>::from(source));
        let CodeHighlightLookup::Ready(code) = request.lookup else {
            return StyledText::new(text);
        };
        let highlights = code.spans.iter().map(|span| {
            (
                span.byte_range.clone(),
                HighlightStyle {
                    color: Some(gpui_color(span.foreground)),
                    ..Default::default()
                },
            )
        });
        StyledText::new(text).with_highlights(highlights)
    }

    fn spawn_code_highlight_job(job: CodeHighlightJob, cx: &mut Context<Self>) {
        let completion_key = job.key.clone();
        let completion_generation = job.generation;
        let source_bytes = job.source.len();
        let theme = job.theme;
        let started = Instant::now();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        highlight_code(
                            job.source.as_ref(),
                            job.language_hint.as_deref(),
                            job.theme,
                            job.limits,
                        )
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    let duration_ms = started.elapsed().as_millis();
                    let (mut outcome, mut fallback_reason, span_count) =
                        result_observation(&result);
                    let completion = view.code_highlight_cache.borrow_mut().complete(
                        &completion_key,
                        completion_generation,
                        result,
                    );
                    if !completion.accepted {
                        outcome = "stale";
                        fallback_reason = "none";
                    }
                    log_code_highlight_observation(
                        completion_key.canonical_language.as_str(),
                        source_bytes,
                        theme,
                        "miss",
                        outcome,
                        fallback_reason,
                        span_count,
                        duration_ms,
                    );
                    for queued_job in completion.jobs {
                        Self::spawn_code_highlight_job(queued_job, cx);
                    }
                    if completion.visible_output_changed {
                        cx.notify();
                    }
                });
            }
        })
        .detach();
    }
}

fn result_observation(
    result: &Result<HighlightOutcome, crate::code_highlight::HighlightError>,
) -> (&'static str, &'static str, usize) {
    match result {
        Ok(HighlightOutcome::Highlighted(code)) => ("highlighted", "none", code.spans.len()),
        Ok(HighlightOutcome::Fallback(reason)) => ("fallback", fallback_reason(*reason), 0),
        Err(_) => ("error", "parser_error", 0),
    }
}

fn fallback_reason(reason: HighlightFallbackReason) -> &'static str {
    match reason {
        HighlightFallbackReason::Empty => "empty",
        HighlightFallbackReason::Plaintext => "plaintext",
        HighlightFallbackReason::UnknownLanguage => "unknown_language",
        HighlightFallbackReason::SourceTooLarge => "source_too_large",
        HighlightFallbackReason::SpanLimit => "span_limit",
        HighlightFallbackReason::ParserError => "parser_error",
    }
}

#[allow(clippy::too_many_arguments)]
fn log_code_highlight_observation(
    language: &str,
    source_bytes: usize,
    theme: CodeThemeId,
    cache: &'static str,
    outcome: &'static str,
    fallback_reason: &'static str,
    span_count: usize,
    duration_ms: u128,
) {
    tracing::debug!(
        target: "pioneer.desktop.code_highlight",
        surface = "desktop_timeline",
        language,
        source_size_bucket = source_size_bucket(source_bytes),
        duration_ms_bucket = duration_ms_bucket(duration_ms),
        cache,
        outcome,
        fallback_reason,
        span_count_bucket = span_count_bucket(span_count),
        theme = theme_name(theme),
        "timeline code highlighting completed"
    );
}

fn source_size_bucket(source_bytes: usize) -> &'static str {
    match source_bytes {
        0 => "empty",
        1..=4_096 => "1b_4kib",
        4_097..=32_768 => "4kib_32kib",
        32_769..=262_144 => "32kib_256kib",
        _ => "over_256kib",
    }
}

fn duration_ms_bucket(duration_ms: u128) -> &'static str {
    match duration_ms {
        0 => "under_1ms",
        1..=8 => "1ms_8ms",
        9..=32 => "9ms_32ms",
        33..=100 => "33ms_100ms",
        _ => "over_100ms",
    }
}

fn span_count_bucket(span_count: usize) -> &'static str {
    match span_count {
        0 => "none",
        1..=100 => "1_100",
        101..=1_000 => "101_1000",
        1_001..=10_000 => "1001_10000",
        _ => "over_10000",
    }
}

fn theme_name(theme: CodeThemeId) -> &'static str {
    match theme {
        CodeThemeId::Light => "light",
        CodeThemeId::Dark => "dark",
    }
}

fn gpui_color(color: Rgba8) -> Hsla {
    rgba(u32::from_be_bytes([
        color.red,
        color.green,
        color.blue,
        color.alpha,
    ]))
    .into()
}

#[cfg(test)]
mod tests {
    use super::gpui_color;
    use crate::code_highlight::Rgba8;

    #[test]
    fn rgba_conversion_keeps_all_channels() {
        let color = gpui_color(Rgba8 {
            red: 0x12,
            green: 0x34,
            blue: 0x56,
            alpha: 0x78,
        });
        let expected: gpui::Hsla = gpui::rgba(0x12345678).into();
        assert_eq!(color, expected);
    }
}
