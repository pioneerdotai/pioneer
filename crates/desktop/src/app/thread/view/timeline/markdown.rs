use crate::app::PioneerDesktop;
use gpui::{prelude::*, *};
use gpui_component::{StyledExt, h_flex, theme::ActiveTheme, v_flex};
use pioneer_protocol::{
    MarkdownBlock, MarkdownDocument, MarkdownInline, MarkdownList, MarkdownMark, MarkdownMarkKind,
};
use std::{ops::Range, sync::Arc};

#[derive(Clone, Copy, Debug)]
enum MarkdownTextVariant {
    Paragraph,
    Heading(u8),
}

impl PioneerDesktop {
    pub(super) fn render_markdown_auto(
        &self,
        text: &str,
        document: Option<&MarkdownDocument>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if let Some(document) = document {
            self.render_markdown_document(document, cx)
        } else {
            self.render_markdown_plain(text, cx)
        }
    }

    pub(super) fn render_markdown_plain(&self, text: &str, _cx: &mut Context<Self>) -> AnyElement {
        div()
            .w_full()
            .overflow_hidden()
            .whitespace_normal()
            .text_sm()
            .line_height(relative(1.65))
            .child(text.to_owned())
            .into_any_element()
    }

    pub(super) fn render_markdown_document(
        &self,
        document: &MarkdownDocument,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if document.blocks.is_empty() {
            return div()
                .w_full()
                .text_sm()
                .line_height(relative(1.6))
                .into_any_element();
        }

        let mut content = v_flex().w_full().overflow_hidden().gap_0();
        let mut previous_block: Option<&MarkdownBlock> = None;
        for (index, block) in document.blocks.iter().enumerate() {
            let top_spacing = Self::markdown_block_spacing(previous_block, block, index);
            if top_spacing > px(0.) {
                content = content.child(div().w_full().h(top_spacing));
            }
            content = content.child(self.render_markdown_block(block, cx));
            previous_block = Some(block);
        }
        content.into_any_element()
    }

    fn markdown_block_spacing(
        previous: Option<&MarkdownBlock>,
        current: &MarkdownBlock,
        index: usize,
    ) -> Pixels {
        if index == 0 {
            return px(0.);
        }

        if matches!(previous, Some(MarkdownBlock::Rule)) || matches!(current, MarkdownBlock::Rule) {
            return px(20.);
        }

        if matches!(current, MarkdownBlock::Heading { .. }) {
            return px(20.);
        }

        px(8.)
    }

    fn render_markdown_block(&self, block: &MarkdownBlock, cx: &mut Context<Self>) -> AnyElement {
        match block {
            MarkdownBlock::Paragraph(inline) => {
                self.render_markdown_inline(inline, MarkdownTextVariant::Paragraph, cx)
            }
            MarkdownBlock::Heading { level, content } => {
                self.render_markdown_inline(content, MarkdownTextVariant::Heading(*level), cx)
            }
            MarkdownBlock::List(list) => self.render_markdown_list(list, cx),
            MarkdownBlock::Quote { blocks } => self.render_markdown_quote(blocks, cx),
            MarkdownBlock::Code { language, text } => {
                self.render_markdown_code_block(language.as_deref(), text.as_str(), cx)
            }
            MarkdownBlock::Rule => div()
                .w_full()
                .h(px(1.))
                .bg(cx.theme().border)
                .opacity(0.8)
                .into_any_element(),
        }
    }

    fn render_markdown_inline(
        &self,
        inline: &MarkdownInline,
        variant: MarkdownTextVariant,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let text = if inline.text.is_empty() {
            SharedString::new_static(" ")
        } else {
            SharedString::new(Arc::<str>::from(inline.text.as_str()))
        };

        let highlights =
            normalized_markdown_highlights(inline.text.as_str(), &inline.marks, |mark| {
                self.markdown_highlight(mark, cx)
            });

        let styled_text = if highlights.is_empty() {
            StyledText::new(text)
        } else {
            StyledText::new(text).with_highlights(highlights)
        };

        let base = div()
            .w_full()
            .overflow_hidden()
            .whitespace_normal()
            .child(styled_text);
        let styled = match variant {
            MarkdownTextVariant::Paragraph => base.text_sm().line_height(relative(1.65)),
            MarkdownTextVariant::Heading(level) => match level {
                1 => base.text_2xl().font_bold().line_height(relative(1.2)),
                2 => base.text_xl().font_bold().line_height(relative(1.25)),
                3 => base.text_lg().font_semibold().line_height(relative(1.3)),
                4 => base.text_base().font_semibold().line_height(relative(1.35)),
                _ => base.text_sm().font_semibold().line_height(relative(1.45)),
            },
        };

        styled.into_any_element()
    }

    fn markdown_highlight(&self, mark: &MarkdownMark, cx: &mut Context<Self>) -> HighlightStyle {
        match &mark.kind {
            MarkdownMarkKind::Bold => HighlightStyle {
                font_weight: Some(FontWeight::SEMIBOLD),
                ..Default::default()
            },
            MarkdownMarkKind::Italic => HighlightStyle {
                font_style: Some(FontStyle::Italic),
                ..Default::default()
            },
            MarkdownMarkKind::Strike => HighlightStyle {
                strikethrough: Some(StrikethroughStyle {
                    thickness: px(1.),
                    color: Some(cx.theme().muted_foreground),
                }),
                ..Default::default()
            },
            MarkdownMarkKind::Code => HighlightStyle {
                background_color: Some(cx.theme().muted.opacity(0.85)),
                ..Default::default()
            },
            MarkdownMarkKind::Link { url: _ } => HighlightStyle {
                color: Some(cx.theme().link),
                underline: Some(UnderlineStyle {
                    thickness: px(1.),
                    color: Some(cx.theme().link),
                    wavy: false,
                }),
                ..Default::default()
            },
        }
    }

    fn render_markdown_list(&self, list: &MarkdownList, cx: &mut Context<Self>) -> AnyElement {
        let mut rows = v_flex().w_full().overflow_hidden().gap_1();
        for (index, item) in list.items.iter().enumerate() {
            let prefix = if let Some(checked) = item.checked {
                if checked {
                    "[x]".to_owned()
                } else {
                    "[ ]".to_owned()
                }
            } else if list.ordered {
                format!("{}.", list.start.saturating_add(index))
            } else {
                "•".to_owned()
            };

            let mut content = v_flex().w_full().gap_2();
            for block in &item.blocks {
                content = content.child(self.render_markdown_block(block, cx));
            }

            rows = rows.child(
                h_flex()
                    .w_full()
                    .items_start()
                    .gap_2()
                    .overflow_hidden()
                    .child(
                        div()
                            .ml_4()
                            .flex_none()
                            .text_sm()
                            .opacity(0.75)
                            .line_height(relative(1.65))
                            .child(prefix),
                    )
                    .child(div().w_full().flex_1().overflow_hidden().child(content)),
            );
        }
        rows.into_any_element()
    }

    fn render_markdown_quote(
        &self,
        blocks: &[MarkdownBlock],
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut content = v_flex().w_full().gap_2();
        for block in blocks {
            content = content.child(self.render_markdown_block(block, cx));
        }

        div()
            .w_full()
            .overflow_hidden()
            .bg(cx.theme().muted.opacity(0.5))
            .border_1()
            .border_color(cx.theme().border)
            .rounded_2xl()
            .p_3()
            .child(content)
            .into_any_element()
    }

    fn render_markdown_code_block(
        &self,
        language: Option<&str>,
        text: &str,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut body = v_flex().w_full().gap_2();
        if let Some(language) = language.filter(|language| !language.trim().is_empty()) {
            body = body.child(
                div()
                    .text_xs()
                    .opacity(0.65)
                    .line_height(relative(1.1))
                    .child(language.trim().to_owned()),
            );
        }
        let code_text = if text.is_empty() { " " } else { text };
        body = body.child(
            div()
                .w_full()
                .overflow_hidden()
                .whitespace_normal()
                .text_sm()
                .line_height(relative(1.45))
                .font_family("monospace")
                .child(code_text.to_owned()),
        );

        div()
            .w_full()
            .overflow_hidden()
            .bg(cx.theme().muted.opacity(0.65))
            .border_1()
            .border_color(cx.theme().border)
            .rounded_2xl()
            .p_3()
            .child(body)
            .into_any_element()
    }
}

fn normalize_mark_range(text: &str, mark: &MarkdownMark) -> (usize, usize) {
    let text_len = text.len();
    let start = snap_to_char_boundary_backward(text, mark.start.min(text_len));
    let end = snap_to_char_boundary_forward(text, mark.end.min(text_len));
    (start, end)
}

fn normalized_markdown_highlights(
    text: &str,
    marks: &[MarkdownMark],
    mut highlight_for_mark: impl FnMut(&MarkdownMark) -> HighlightStyle,
) -> Vec<(Range<usize>, HighlightStyle)> {
    // GPUI StyledText expects sorted, non-overlapping highlight ranges.
    // Markdown marks can overlap when styles are nested, such as bold italic text.
    let highlights = marks.iter().filter_map(|mark| {
        let (start, end) = normalize_mark_range(text, mark);
        (start < end).then(|| (start..end, highlight_for_mark(mark)))
    });

    combine_highlights(
        std::iter::empty::<(Range<usize>, HighlightStyle)>(),
        highlights,
    )
    .collect()
}

fn snap_to_char_boundary_backward(text: &str, mut index: usize) -> usize {
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn snap_to_char_boundary_forward(text: &str, mut index: usize) -> usize {
    let text_len = text.len();
    while index < text_len && !text.is_char_boundary(index) {
        index += 1;
    }
    index
}
