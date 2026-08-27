use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use gpui::{prelude::*, *};
use gpui_component::theme::ActiveTheme as _;
use pioneer_observability::{
    DesktopCodeHighlightCacheStatus, DesktopCodeHighlightFallbackReason,
    DesktopCodeHighlightMetric, DesktopCodeHighlightOutcome, DesktopCodeHighlightTheme,
    record_desktop_code_highlight,
};

use crate::{
    app::PioneerDesktop,
    code_highlight::{
        CodeHighlightJob, CodeHighlightLookup, CodeThemeId, HighlightFallbackReason,
        HighlightLimits, HighlightOutcome, Rgba8, highlight_code,
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
                CodeHighlightLookup::Fallback(reason) => (
                    DesktopCodeHighlightOutcome::Fallback,
                    fallback_reason(*reason),
                ),
                CodeHighlightLookup::Unavailable => (
                    DesktopCodeHighlightOutcome::Fallback,
                    DesktopCodeHighlightFallbackReason::CacheCapacity,
                ),
                _ => (
                    DesktopCodeHighlightOutcome::Error,
                    DesktopCodeHighlightFallbackReason::UnexpectedState,
                ),
            };
            observe_code_highlight(
                source.len(),
                theme,
                DesktopCodeHighlightCacheStatus::Miss,
                outcome,
                fallback_reason,
                0,
                None,
            );
        }
        if request.observe_cache_hit {
            let (outcome, fallback_reason, span_count) = match &request.lookup {
                CodeHighlightLookup::Ready(code) => (
                    DesktopCodeHighlightOutcome::Highlighted,
                    DesktopCodeHighlightFallbackReason::None,
                    code.spans.len(),
                ),
                CodeHighlightLookup::Fallback(reason) => (
                    DesktopCodeHighlightOutcome::Fallback,
                    fallback_reason(*reason),
                    0,
                ),
                _ => (
                    DesktopCodeHighlightOutcome::Error,
                    DesktopCodeHighlightFallbackReason::UnexpectedState,
                    0,
                ),
            };
            observe_code_highlight(
                source.len(),
                theme,
                DesktopCodeHighlightCacheStatus::Hit,
                outcome,
                fallback_reason,
                span_count,
                None,
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
                    let elapsed = started.elapsed();
                    let (mut outcome, mut fallback_reason, span_count) =
                        result_observation(&result);
                    let completion = view.code_highlight_cache.borrow_mut().complete(
                        &completion_key,
                        completion_generation,
                        result,
                    );
                    if !completion.accepted {
                        outcome = DesktopCodeHighlightOutcome::Stale;
                        fallback_reason = DesktopCodeHighlightFallbackReason::None;
                    }
                    observe_code_highlight(
                        source_bytes,
                        theme,
                        DesktopCodeHighlightCacheStatus::Miss,
                        outcome,
                        fallback_reason,
                        span_count,
                        Some(elapsed),
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
) -> (
    DesktopCodeHighlightOutcome,
    DesktopCodeHighlightFallbackReason,
    usize,
) {
    match result {
        Ok(HighlightOutcome::Highlighted(code)) => (
            DesktopCodeHighlightOutcome::Highlighted,
            DesktopCodeHighlightFallbackReason::None,
            code.spans.len(),
        ),
        Ok(HighlightOutcome::Fallback(reason)) => (
            DesktopCodeHighlightOutcome::Fallback,
            fallback_reason(*reason),
            0,
        ),
        Err(_) => (
            DesktopCodeHighlightOutcome::Error,
            DesktopCodeHighlightFallbackReason::ParserError,
            0,
        ),
    }
}

fn fallback_reason(reason: HighlightFallbackReason) -> DesktopCodeHighlightFallbackReason {
    match reason {
        HighlightFallbackReason::Empty => DesktopCodeHighlightFallbackReason::Empty,
        HighlightFallbackReason::Plaintext => DesktopCodeHighlightFallbackReason::Plaintext,
        HighlightFallbackReason::UnknownLanguage => {
            DesktopCodeHighlightFallbackReason::UnknownLanguage
        }
        HighlightFallbackReason::SourceTooLarge => {
            DesktopCodeHighlightFallbackReason::SourceTooLarge
        }
        HighlightFallbackReason::SpanLimit => DesktopCodeHighlightFallbackReason::SpanLimit,
        HighlightFallbackReason::ParserError => DesktopCodeHighlightFallbackReason::ParserError,
    }
}

#[allow(clippy::too_many_arguments)]
fn observe_code_highlight(
    source_bytes: usize,
    theme: CodeThemeId,
    cache: DesktopCodeHighlightCacheStatus,
    outcome: DesktopCodeHighlightOutcome,
    fallback_reason: DesktopCodeHighlightFallbackReason,
    span_count: usize,
    elapsed: Option<Duration>,
) {
    record_desktop_code_highlight(DesktopCodeHighlightMetric {
        cache,
        outcome,
        fallback_reason,
        theme: match theme {
            CodeThemeId::Light => DesktopCodeHighlightTheme::Light,
            CodeThemeId::Dark => DesktopCodeHighlightTheme::Dark,
        },
        source_bytes,
        span_count,
        elapsed,
    });
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
