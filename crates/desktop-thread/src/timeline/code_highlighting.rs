//! One syntax result and cancellable foreground generation per live code block.
use crate::code_highlight::{
    CodeThemeId, HighlightLimits, HighlightOutcome, HighlightedCode, highlight_code,
};
use crate::screen::TimelineView;
use gpui_kit::component::ActiveTheme;
use gpui_kit::{prelude::*, *};
use std::sync::Arc;

#[derive(Clone, PartialEq)]
struct HighlightInput {
    revision: u64,
    theme_revision: u64,
    typography: TextStyle,
    engine: u16,
    theme: CodeThemeId,
    source: String,
    language: Option<String>,
}
pub(crate) struct LiveHighlight {
    input: HighlightInput,
    pub view: Entity<MarkdownHighlightController>,
}
pub(crate) struct MarkdownHighlightController {
    input: HighlightInput,
    generation: u64,
    completed_generation: Option<u64>,
    result: Option<HighlightedCode>,
    task: Option<Task<()>>,
}
impl MarkdownHighlightController {
    fn new(input: HighlightInput, cx: &mut Context<Self>) -> Self {
        let mut owner = Self {
            input,
            generation: 0,
            completed_generation: None,
            result: None,
            task: None,
        };
        owner.start(cx);
        owner
    }
    fn synchronize(&mut self, input: HighlightInput, cx: &mut Context<Self>) {
        if self.input == input {
            return;
        }
        self.task.take();
        self.input = input;
        self.result = None;
        self.start(cx);
        cx.notify();
    }
    fn finish(&mut self, generation: u64, code: Option<HighlightedCode>) -> bool {
        if self.generation != generation || self.completed_generation == Some(generation) {
            return false;
        }
        self.completed_generation = Some(generation);
        self.task = None;
        let changed = code.is_some();
        self.result = code;
        changed
    }
    fn start(&mut self, cx: &mut Context<Self>) {
        self.generation += 1;
        self.completed_generation = None;
        let generation = self.generation;
        let input = self.input.clone();
        self.task = Some(cx.spawn(async move |weak, cx| {
            // Failure is a terminal plain-text result for this input revision.
            let result = cx
                .background_spawn(async move {
                    highlight_code(
                        &input.source,
                        input.language.as_deref(),
                        input.theme,
                        HighlightLimits::DESKTOP,
                    )
                })
                .await;
            let _ = weak.update(cx, |owner, cx| {
                let code = match result {
                    Ok(HighlightOutcome::Highlighted(code)) => Some(code),
                    _ => None,
                };
                if owner.finish(generation, code) {
                    cx.notify();
                }
            });
        }));
    }
}
impl Render for MarkdownHighlightController {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        let text = StyledText::new(SharedString::new(Arc::<str>::from(
            self.input.source.as_str(),
        )));
        if let Some(code) = &self.result {
            text.with_highlights(code.spans.iter().map(|span| {
                (
                    span.byte_range.clone(),
                    HighlightStyle {
                        color: Some(
                            rgba(u32::from_be_bytes([
                                span.foreground.red,
                                span.foreground.green,
                                span.foreground.blue,
                                span.foreground.alpha,
                            ]))
                            .into(),
                        ),
                        ..Default::default()
                    },
                )
            }))
        } else {
            text
        }
    }
}
impl TimelineView {
    pub(super) fn prepare_code_highlight(
        &self,
        id: u64,
        revision: u64,
        source: &str,
        language: Option<&str>,
        cx: &mut Context<Self>,
    ) {
        let theme = if cx.theme().mode.is_dark() {
            CodeThemeId::Dark
        } else {
            CodeThemeId::Light
        };
        let input = HighlightInput {
            revision,
            theme_revision: self.layout_store.theme_revision,
            typography: self.thread_timeline_view_state.layout_text_style.clone(),
            engine: crate::code_highlight::HIGHLIGHT_ENGINE_REVISION,
            theme,
            source: source.into(),
            language: language.map(str::to_owned),
        };
        let mut live = self.markdown_highlights.borrow_mut();
        if let Some(existing) = live.get_mut(&id) {
            if existing.input == input {
                return;
            }
            existing
                .view
                .update(cx, |owner, cx| owner.synchronize(input.clone(), cx));
            existing.input = input;
        } else {
            let view = cx.new(|cx| MarkdownHighlightController::new(input.clone(), cx));
            live.insert(id, LiveHighlight { input, view });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CodeThemeId, HighlightInput, MarkdownHighlightController};
    use gpui_kit::{AppContext, TestAppContext, TextStyle};
    fn input(source: &str) -> HighlightInput {
        HighlightInput {
            revision: 1,
            theme_revision: 1,
            typography: TextStyle::default(),
            engine: crate::code_highlight::HIGHLIGHT_ENGINE_REVISION,
            theme: CodeThemeId::Dark,
            source: source.into(),
            language: Some("rust".into()),
        }
    }
    #[gpui_kit::test]
    fn generations_are_scoped_equal_inputs_quiet_and_drop_releases_owner(cx: &mut TestAppContext) {
        let a = cx.new(|cx| MarkdownHighlightController::new(input("let a = 1;"), cx));
        let b = cx.new(|cx| MarkdownHighlightController::new(input("let a = 1;"), cx));
        assert_ne!(a.entity_id(), b.entity_id());
        a.update(cx, |owner, cx| {
            owner.synchronize(input("let a = 1;"), cx);
            assert_eq!(owner.generation, 1);
            let mut next = owner.input.clone();
            next.theme_revision += 1;
            owner.synchronize(next, cx);
            assert_eq!(owner.generation, 2);
            assert!(!owner.finish(1, None));
            owner.task.take();
            assert!(!owner.finish(2, None));
            assert_eq!(owner.completed_generation, Some(2));
            assert!(!owner.finish(2, None)); // bounded failure: no repeated task for equal revision
            owner.synchronize(owner.input.clone(), cx);
            assert!(owner.task.is_none());
        });
        b.read_with(cx, |owner, _| assert_eq!(owner.generation, 1));
        let weak = a.downgrade();
        drop(a);
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
        cx.run_until_parked();
        b.update(cx, |owner, _| {
            assert_eq!(owner.completed_generation, Some(1));
            assert!(owner.result.is_some());
            assert!(!owner.finish(1, None));
        });
    }
}
