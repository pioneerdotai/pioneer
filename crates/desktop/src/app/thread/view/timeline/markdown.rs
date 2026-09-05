use crate::{
    app::PioneerDesktop,
    assets::PioneerIconName,
    file_opener::{LocalFileTarget, local_file_target, open_local_file},
};
use gpui_kit::component::{
    IconName, IconNamed, StyledExt, clipboard::Clipboard, h_flex, theme::ActiveTheme, v_flex,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::conversation::TimelineEntryStatus;
use pioneer_protocol::{
    MarkdownBlock, MarkdownDocument, MarkdownInline, MarkdownList, MarkdownMark, MarkdownMarkKind,
};
use std::{ops::Range, sync::Arc, time::Instant};

use super::layout::TIMELINE_ROW_MEASUREMENT_GUARD;

const TIMELINE_MESSAGE_FONT_SIZE_REM: f32 = 0.875;
const TIMELINE_MESSAGE_LINE_HEIGHT_RATIO: f32 = 1.65;
const MARKDOWN_LINK_ICON_PLACEHOLDER: &str = "\u{2007}\u{2007}";
const MARKDOWN_LINK_ICON_FONT_SCALE: f32 = 0.85;

pub(super) fn timeline_message_text_bottom_inset(window: &Window) -> Pixels {
    let font_size = px(window.rem_size().as_f32() * TIMELINE_MESSAGE_FONT_SIZE_REM);
    let line_height = px((font_size.as_f32() * TIMELINE_MESSAGE_LINE_HEIGHT_RATIO).round());
    let text_style = window.text_style();
    let font_id = window.text_system().resolve_font(&text_style.font());
    let ascent = window.text_system().ascent(font_id, font_size).as_f32();
    // GPUI stores the OpenType descender with its native negative sign.
    let descent = window
        .text_system()
        .descent(font_id, font_size)
        .as_f32()
        .abs();
    let text_height = px(ascent + descent);
    let lower_leading = px(((line_height - text_height).as_f32() / 2.).max(0.));

    lower_leading + TIMELINE_ROW_MEASUREMENT_GUARD
}

#[derive(Clone, Copy, Debug)]
enum MarkdownTextVariant {
    Paragraph,
    Heading(u8),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CodeHighlightPolicy {
    Disabled,
    FinalMessage,
}

impl CodeHighlightPolicy {
    pub(super) fn for_timeline_status(status: TimelineEntryStatus) -> Self {
        if status == TimelineEntryStatus::Completed {
            Self::FinalMessage
        } else {
            Self::Disabled
        }
    }
}

struct MarkdownLinkText {
    id: ElementId,
    text: InteractiveText,
    text_layout: TextLayout,
    icons: Vec<MarkdownLinkIcon>,
    icon_color: Hsla,
    accessible_text: SharedString,
}

struct MarkdownLinkIcon {
    offset: usize,
    path: SharedString,
}

impl IntoElement for MarkdownLinkText {
    type Element = Self;

    fn into_element(self) -> Self::Element {
        self
    }
}

impl Element for MarkdownLinkText {
    type RequestLayoutState = ();
    type PrepaintState = Hitbox;

    fn id(&self) -> Option<ElementId> {
        Some(self.id.clone())
    }

    fn source_location(&self) -> Option<&'static core::panic::Location<'static>> {
        None
    }

    fn a11y_role(&self) -> Option<Role> {
        Some(Role::Label)
    }

    fn write_a11y_info(&self, node: &mut accesskit::Node) {
        node.set_value(self.accessible_text.to_string());
    }

    fn request_layout(
        &mut self,
        global_id: Option<&GlobalElementId>,
        inspector_id: Option<&InspectorElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> (LayoutId, Self::RequestLayoutState) {
        self.text
            .request_layout(global_id, inspector_id, window, cx)
    }

    fn prepaint(
        &mut self,
        global_id: Option<&GlobalElementId>,
        inspector_id: Option<&InspectorElementId>,
        bounds: Bounds<Pixels>,
        request_layout: &mut Self::RequestLayoutState,
        window: &mut Window,
        cx: &mut App,
    ) -> Self::PrepaintState {
        self.text
            .prepaint(global_id, inspector_id, bounds, request_layout, window, cx)
    }

    fn paint(
        &mut self,
        global_id: Option<&GlobalElementId>,
        inspector_id: Option<&InspectorElementId>,
        bounds: Bounds<Pixels>,
        request_layout: &mut Self::RequestLayoutState,
        hitbox: &mut Self::PrepaintState,
        window: &mut Window,
        cx: &mut App,
    ) {
        self.text.paint(
            global_id,
            inspector_id,
            bounds,
            request_layout,
            hitbox,
            window,
            cx,
        );

        let line_height = self.text_layout.line_height();
        let font_size = window.text_style().font_size.to_pixels(window.rem_size());
        for icon in &self.icons {
            let Some(start) = self.text_layout.position_for_index(icon.offset) else {
                continue;
            };
            let Some(end) = self.text_layout.position_for_index(
                icon.offset
                    .saturating_add(MARKDOWN_LINK_ICON_PLACEHOLDER.len()),
            ) else {
                continue;
            };
            if start.y != end.y {
                continue;
            }

            let reserved_width = end.x - start.x;
            let icon_size =
                px((font_size.as_f32() * MARKDOWN_LINK_ICON_FONT_SCALE)
                    .min(reserved_width.as_f32()));
            if icon_size <= px(0.) {
                continue;
            }
            let icon_bounds = Bounds::new(
                point(
                    start.x + (reserved_width - icon_size) / 2.,
                    start.y + (line_height - icon_size) / 1.9,
                ),
                size(icon_size, icon_size),
            );
            let _ = window.paint_svg(
                icon_bounds,
                icon.path.clone(),
                None,
                TransformationMatrix::default(),
                self.icon_color,
                cx,
            );
        }
    }
}

impl PioneerDesktop {
    pub(super) fn render_markdown_auto(
        &self,
        interaction_scope: &str,
        text: &str,
        document: Option<&MarkdownDocument>,
        code_highlight_policy: CodeHighlightPolicy,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if let Some(document) = document {
            self.render_markdown_document(interaction_scope, document, code_highlight_policy, cx)
        } else {
            self.render_markdown_plain(text, cx)
        }
    }

    pub(super) fn render_markdown_plain(&self, text: &str, _cx: &mut Context<Self>) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::Markdown
        ));
        pioneer_observability::record_qualification_diagnostic!(record_timeline(
            pioneer_observability::TimelineStage::MarkdownElementBuild,
            pioneer_observability::DiagnosticAction::Executed,
            1,
        ));
        let started = Instant::now();
        let element = div()
            .w_full()
            .overflow_hidden()
            .whitespace_normal()
            .text_sm()
            .line_height(relative(1.65))
            .child(text.to_owned())
            .into_any_element();
        pioneer_observability::record_desktop_timeline_stage(
            pioneer_observability::DesktopTimelineStageMetric {
                stage: pioneer_observability::DesktopTimelineStage::MarkdownElementBuild,
                cache: pioneer_observability::DesktopTimelineCacheStatus::NotApplicable,
                content: pioneer_observability::DesktopTimelineContentKind::PlainText,
                outcome: pioneer_observability::DesktopTimelineOutcome::Ok,
                elapsed: started.elapsed(),
                input_bytes: Some(text.len()),
                block_count: Some(0),
                row_count: None,
            },
        );
        element
    }

    pub(super) fn render_markdown_document(
        &self,
        interaction_scope: &str,
        document: &MarkdownDocument,
        code_highlight_policy: CodeHighlightPolicy,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::Markdown
        ));
        pioneer_observability::record_qualification_diagnostic!(record_timeline(
            pioneer_observability::TimelineStage::MarkdownElementBuild,
            pioneer_observability::DiagnosticAction::Executed,
            1,
        ));
        pioneer_observability::record_qualification_diagnostic!(record_timeline(
            pioneer_observability::TimelineStage::MarkdownDocumentProjection,
            pioneer_observability::DiagnosticAction::Executed,
            u64::try_from(document.blocks.len()).unwrap_or(u64::MAX),
        ));
        let started = Instant::now();
        if document.blocks.is_empty() {
            let element = div()
                .w_full()
                .text_sm()
                .line_height(relative(1.6))
                .into_any_element();
            pioneer_observability::record_desktop_timeline_stage(
                pioneer_observability::DesktopTimelineStageMetric {
                    stage: pioneer_observability::DesktopTimelineStage::MarkdownElementBuild,
                    cache: pioneer_observability::DesktopTimelineCacheStatus::NotApplicable,
                    content: pioneer_observability::DesktopTimelineContentKind::Markdown,
                    outcome: pioneer_observability::DesktopTimelineOutcome::Ok,
                    elapsed: started.elapsed(),
                    input_bytes: None,
                    block_count: Some(0),
                    row_count: None,
                },
            );
            return element;
        }

        let mut content = v_flex().w_full().overflow_hidden().gap_0();
        let mut previous_block: Option<&MarkdownBlock> = None;
        let interaction_root = markdown_interaction_root_id(interaction_scope);
        for (index, block) in document.blocks.iter().enumerate() {
            let top_spacing = Self::markdown_block_spacing(previous_block, block, index);
            if top_spacing > px(0.) {
                content = content.child(div().w_full().h(top_spacing));
            }
            content = content.child(self.render_markdown_block(
                block,
                code_highlight_policy,
                markdown_child_interaction_id(interaction_root, index),
                cx,
            ));
            previous_block = Some(block);
        }
        let element = content.into_any_element();
        pioneer_observability::record_desktop_timeline_stage(
            pioneer_observability::DesktopTimelineStageMetric {
                stage: pioneer_observability::DesktopTimelineStage::MarkdownElementBuild,
                cache: pioneer_observability::DesktopTimelineCacheStatus::NotApplicable,
                content: pioneer_observability::DesktopTimelineContentKind::Markdown,
                outcome: pioneer_observability::DesktopTimelineOutcome::Ok,
                elapsed: started.elapsed(),
                input_bytes: None,
                block_count: Some(document.blocks.len()),
                row_count: None,
            },
        );
        element
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

    fn render_markdown_block(
        &self,
        block: &MarkdownBlock,
        code_highlight_policy: CodeHighlightPolicy,
        interaction_id: u64,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match block {
            MarkdownBlock::Paragraph(inline) => self.render_markdown_inline(
                inline,
                MarkdownTextVariant::Paragraph,
                interaction_id,
                cx,
            ),
            MarkdownBlock::Heading { level, content } => self.render_markdown_inline(
                content,
                MarkdownTextVariant::Heading(*level),
                interaction_id,
                cx,
            ),
            MarkdownBlock::List(list) => {
                self.render_markdown_list(list, code_highlight_policy, interaction_id, cx)
            }
            MarkdownBlock::Quote { blocks } => {
                self.render_markdown_quote(blocks, code_highlight_policy, interaction_id, cx)
            }
            MarkdownBlock::Code { language, text } => self.render_markdown_code_block(
                language.as_deref(),
                text.as_str(),
                code_highlight_policy,
                cx,
            ),
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
        interaction_id: u64,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let base = div().w_full().overflow_hidden().whitespace_normal();
        let links = normalized_markdown_links(inline.text.as_str(), &inline.marks);
        let base = if links.is_empty() {
            base.child(self.styled_markdown_inline_text(inline.text.as_str(), &inline.marks, cx))
        } else {
            let presentation =
                markdown_inline_with_link_icons(inline.text.as_str(), &inline.marks, links);
            let styled_text = self.styled_markdown_inline_text(
                presentation.text.as_str(),
                &presentation.marks,
                cx,
            );
            let text_layout = styled_text.layout().clone();
            let ranges = presentation
                .links
                .iter()
                .map(|link| link.range.clone())
                .collect();
            let targets: Vec<_> = presentation
                .links
                .iter()
                .map(|link| MarkdownClickTarget::from_url(link.url.clone()))
                .collect();
            let icons = presentation
                .icon_offsets
                .iter()
                .copied()
                .zip(targets.iter())
                .map(|(offset, target)| MarkdownLinkIcon {
                    offset,
                    path: target.icon_path(),
                })
                .collect();
            let selected_file_opener = self.active_thread_file_opener(cx);
            let element_id: ElementId = ("timeline-markdown-inline", interaction_id).into();
            let text = InteractiveText::new(element_id.clone(), styled_text).on_click(
                ranges,
                move |range_index, _, _| {
                    let Some(target) = targets.get(range_index) else {
                        return;
                    };
                    let result = match target {
                        MarkdownClickTarget::Web(url) => {
                            webbrowser::open(url.as_ref()).map_err(anyhow::Error::from)
                        }
                        MarkdownClickTarget::LocalFile(target) => {
                            open_local_file(selected_file_opener, target)
                        }
                    };
                    if let Err(error) = result {
                        tracing::warn!(
                            error = %format!("{error:#}"),
                            "failed to open timeline markdown link"
                        );
                    }
                },
            );
            base.child(MarkdownLinkText {
                id: element_id,
                text,
                text_layout,
                icons,
                icon_color: cx.theme().link,
                accessible_text: SharedString::new(Arc::<str>::from(inline.text.as_str())),
            })
        };
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

    fn styled_markdown_inline_text(
        &self,
        text: &str,
        marks: &[MarkdownMark],
        cx: &mut Context<Self>,
    ) -> StyledText {
        let shared_text = if text.is_empty() {
            SharedString::new_static(" ")
        } else {
            SharedString::new(Arc::<str>::from(text))
        };
        let highlights =
            normalized_markdown_highlights(text, marks, |mark| self.markdown_highlight(mark, cx));

        if highlights.is_empty() {
            StyledText::new(shared_text)
        } else {
            StyledText::new(shared_text).with_highlights(highlights)
        }
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
                font_weight: Some(FontWeight::MEDIUM),
                ..Default::default()
            },
        }
    }

    fn render_markdown_list(
        &self,
        list: &MarkdownList,
        code_highlight_policy: CodeHighlightPolicy,
        interaction_id: u64,
        cx: &mut Context<Self>,
    ) -> AnyElement {
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
            let item_interaction_id = markdown_child_interaction_id(interaction_id, index);
            for (block_index, block) in item.blocks.iter().enumerate() {
                content = content.child(self.render_markdown_block(
                    block,
                    code_highlight_policy,
                    markdown_child_interaction_id(item_interaction_id, block_index),
                    cx,
                ));
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
        code_highlight_policy: CodeHighlightPolicy,
        interaction_id: u64,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut content = v_flex().w_full().gap_2();
        for (index, block) in blocks.iter().enumerate() {
            content = content.child(self.render_markdown_block(
                block,
                code_highlight_policy,
                markdown_child_interaction_id(interaction_id, index),
                cx,
            ));
        }

        div()
            .w_full()
            .overflow_hidden()
            .bg(cx.theme().muted.opacity(0.5))
            .rounded_2xl()
            .p_3()
            .child(content)
            .into_any_element()
    }

    fn render_markdown_code_block(
        &self,
        language: Option<&str>,
        text: &str,
        code_highlight_policy: CodeHighlightPolicy,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_timeline(
            pioneer_observability::TimelineStage::MarkdownCodeBlockProjection,
            pioneer_observability::DiagnosticAction::Executed,
            1,
        ));
        let language_label = sanitized_language_label(language);
        let clipboard_id = code_block_clipboard_id(language_label.as_deref(), text);
        let header = h_flex()
            .w_full()
            .h(px(20.))
            .items_center()
            .justify_between()
            .child(
                div()
                    .text_xs()
                    .opacity(0.6)
                    .line_height(relative(1.1))
                    .when_some(language_label, |this, language| this.child(language)),
            )
            .child(
                div()
                    .opacity(0.6)
                    .child(Clipboard::new(clipboard_id).value(text.to_owned())),
            );
        let mut body = v_flex().w_full().gap_2().child(header);
        let styled_code = match code_highlight_policy {
            CodeHighlightPolicy::Disabled => {
                StyledText::new(SharedString::new(Arc::<str>::from(text)))
            }
            CodeHighlightPolicy::FinalMessage => {
                self.render_code_highlighted_text(text, language, cx)
            }
        };
        body = body.child(
            div()
                .w_full()
                .min_h(px(20.))
                .overflow_hidden()
                // In GPUI `WhiteSpace::Normal` controls soft wrapping; it does not rewrite the
                // `StyledText` source. This keeps newlines/tabs copyable while avoiding nested
                // horizontal-scroll arbitration inside the virtualized timeline.
                .whitespace_normal()
                .text_sm()
                .line_height(relative(1.45))
                .font_family("monospace")
                .child(styled_code),
        );

        div()
            .w_full()
            .overflow_hidden()
            .bg(cx.theme().muted.opacity(0.75))
            .rounded_2xl()
            .p_3()
            .child(body)
            .into_any_element()
    }
}

fn code_block_clipboard_id(language: Option<&str>, text: &str) -> SharedString {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    let mut hasher = DefaultHasher::new();
    language.hash(&mut hasher);
    text.hash(&mut hasher);
    SharedString::from(format!("copy-code-block-{:016x}", hasher.finish()))
}

fn sanitized_language_label(language: Option<&str>) -> Option<String> {
    let token = language?
        .trim_matches(|character: char| character.is_ascii_whitespace())
        .split_ascii_whitespace()
        .next()?;
    if token.is_empty() {
        return None;
    }
    let mut end = token.len().min(64);
    while !token.is_char_boundary(end) {
        end -= 1;
    }
    let label: String = token[..end]
        .chars()
        .filter(|character| !character.is_control())
        .collect();
    (!label.is_empty()).then_some(label)
}

fn normalize_mark_range(text: &str, mark: &MarkdownMark) -> (usize, usize) {
    let text_len = text.len();
    let start = snap_to_char_boundary_backward(text, mark.start.min(text_len));
    let end = snap_to_char_boundary_forward(text, mark.end.min(text_len));
    (start, end)
}

fn markdown_interaction_root_id(scope: &str) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    scope.hash(&mut hasher);
    hasher.finish()
}

fn markdown_child_interaction_id(parent: u64, index: usize) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    parent.hash(&mut hasher);
    index.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, PartialEq, Eq)]
struct MarkdownLink {
    range: Range<usize>,
    url: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MarkdownClickTarget {
    Web(Arc<str>),
    LocalFile(LocalFileTarget),
}

impl MarkdownClickTarget {
    fn from_url(url: Arc<str>) -> Self {
        if let Some(target) = local_file_target(url.as_ref()) {
            Self::LocalFile(target)
        } else {
            Self::Web(url)
        }
    }

    fn icon_path(&self) -> SharedString {
        match self {
            Self::Web(_) => PioneerIconName::Globe.path(),
            Self::LocalFile(_) => IconName::File.path(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct MarkdownInlineWithLinkIcons {
    text: String,
    marks: Vec<MarkdownMark>,
    links: Vec<MarkdownLink>,
    icon_offsets: Vec<usize>,
}

fn normalized_markdown_links(text: &str, marks: &[MarkdownMark]) -> Vec<MarkdownLink> {
    let mut links: Vec<_> = marks
        .iter()
        .filter_map(|mark| {
            let MarkdownMarkKind::Link { url } = &mark.kind else {
                return None;
            };
            let (start, end) = normalize_mark_range(text, mark);
            (start < end).then(|| MarkdownLink {
                range: start..end,
                url: Arc::from(url.as_str()),
            })
        })
        .collect();
    links.sort_by_key(|link| (link.range.start, link.range.end));

    let mut previous_end = 0;
    links.retain(|link| {
        if link.range.start < previous_end {
            return false;
        }
        previous_end = link.range.end;
        true
    });
    links
}

fn markdown_inline_with_link_icons(
    text: &str,
    marks: &[MarkdownMark],
    links: Vec<MarkdownLink>,
) -> MarkdownInlineWithLinkIcons {
    let link_starts: Vec<_> = links.iter().map(|link| link.range.start).collect();
    let mut rendered_text = String::with_capacity(
        text.len()
            + links
                .len()
                .saturating_mul(MARKDOWN_LINK_ICON_PLACEHOLDER.len()),
    );
    let mut cursor = 0;
    let mut icon_offsets = Vec::with_capacity(links.len());
    for link in &links {
        rendered_text.push_str(&text[cursor..link.range.start]);
        icon_offsets.push(rendered_text.len());
        rendered_text.push_str(MARKDOWN_LINK_ICON_PLACEHOLDER);
        cursor = link.range.start;
    }
    rendered_text.push_str(&text[cursor..]);

    let rendered_marks = marks
        .iter()
        .filter_map(|mark| {
            let (start, end) = normalize_mark_range(text, mark);
            (start < end).then(|| MarkdownMark {
                start: markdown_index_after_link_icons(start, &link_starts),
                end: markdown_range_end_after_link_icons(end, &link_starts),
                kind: mark.kind.clone(),
            })
        })
        .collect();
    let rendered_links = links
        .into_iter()
        .map(|link| {
            let text_start = markdown_index_after_link_icons(link.range.start, &link_starts);
            MarkdownLink {
                range: text_start.saturating_sub(MARKDOWN_LINK_ICON_PLACEHOLDER.len())
                    ..markdown_range_end_after_link_icons(link.range.end, &link_starts),
                url: link.url,
            }
        })
        .collect();

    MarkdownInlineWithLinkIcons {
        text: rendered_text,
        marks: rendered_marks,
        links: rendered_links,
        icon_offsets,
    }
}

fn markdown_index_after_link_icons(index: usize, link_starts: &[usize]) -> usize {
    index
        + link_starts
            .partition_point(|link_start| *link_start <= index)
            .saturating_mul(MARKDOWN_LINK_ICON_PLACEHOLDER.len())
}

fn markdown_range_end_after_link_icons(index: usize, link_starts: &[usize]) -> usize {
    index
        + link_starts
            .partition_point(|link_start| *link_start < index)
            .saturating_mul(MARKDOWN_LINK_ICON_PLACEHOLDER.len())
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

#[cfg(test)]
mod tests {
    use super::{
        CodeHighlightPolicy, MARKDOWN_LINK_ICON_PLACEHOLDER, MarkdownClickTarget, MarkdownLink,
        markdown_inline_with_link_icons, normalized_markdown_links, sanitized_language_label,
    };
    use pioneer_client::conversation::TimelineEntryStatus;
    use pioneer_protocol::{MarkdownMark, MarkdownMarkKind};
    use std::sync::Arc;

    #[test]
    fn code_highlighting_is_limited_to_completed_timeline_items() {
        assert_eq!(
            CodeHighlightPolicy::for_timeline_status(TimelineEntryStatus::Completed),
            CodeHighlightPolicy::FinalMessage
        );
        for status in [
            TimelineEntryStatus::Running,
            TimelineEntryStatus::Failed,
            TimelineEntryStatus::Cancelled,
            TimelineEntryStatus::Blocked,
        ] {
            assert_eq!(
                CodeHighlightPolicy::for_timeline_status(status),
                CodeHighlightPolicy::Disabled
            );
        }
    }

    #[test]
    fn code_highlight_language_label_is_single_token_control_free_and_byte_bounded() {
        assert_eq!(
            sanitized_language_label(Some("  rust metadata\nignored")),
            Some("rust".to_owned())
        );
        assert_eq!(sanitized_language_label(Some("\n\t")), None);
        assert!(
            sanitized_language_label(Some(&"x".repeat(80)))
                .unwrap()
                .len()
                <= 64
        );
        assert!(
            sanitized_language_label(Some(&"界".repeat(30)))
                .unwrap()
                .len()
                <= 64
        );
    }

    #[test]
    fn markdown_links_keep_urls_and_normalize_clickable_ranges() {
        let text = "go to café now";
        let marks = vec![
            MarkdownMark {
                start: 6,
                end: 10,
                kind: MarkdownMarkKind::Link {
                    url: "https://example.com/cafe".to_owned(),
                },
            },
            MarkdownMark {
                start: 0,
                end: 2,
                kind: MarkdownMarkKind::Bold,
            },
            MarkdownMark {
                start: text.len(),
                end: text.len() + 10,
                kind: MarkdownMarkKind::Link {
                    url: "https://example.com/empty".to_owned(),
                },
            },
        ];

        assert_eq!(
            normalized_markdown_links(text, &marks),
            vec![MarkdownLink {
                range: 6..11,
                url: Arc::from("https://example.com/cafe"),
            }]
        );
    }

    #[test]
    fn markdown_links_do_not_create_overlapping_click_targets() {
        let marks = vec![
            MarkdownMark {
                start: 0,
                end: 5,
                kind: MarkdownMarkKind::Link {
                    url: "https://example.com/first".to_owned(),
                },
            },
            MarkdownMark {
                start: 3,
                end: 8,
                kind: MarkdownMarkKind::Link {
                    url: "https://example.com/second".to_owned(),
                },
            },
        ];

        let links = normalized_markdown_links("overlaps", &marks);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].url.as_ref(), "https://example.com/first");
    }

    #[test]
    fn markdown_link_icons_precede_links_and_shift_marks_and_click_targets() {
        let text = "one and two";
        let marks = vec![
            MarkdownMark {
                start: 0,
                end: 3,
                kind: MarkdownMarkKind::Link {
                    url: "https://example.com/one".to_owned(),
                },
            },
            MarkdownMark {
                start: 8,
                end: 11,
                kind: MarkdownMarkKind::Link {
                    url: "https://example.com/two".to_owned(),
                },
            },
        ];
        let links = normalized_markdown_links(text, &marks);

        let presentation = markdown_inline_with_link_icons(text, &marks, links);

        assert_eq!(
            presentation.text,
            format!("{MARKDOWN_LINK_ICON_PLACEHOLDER}one and {MARKDOWN_LINK_ICON_PLACEHOLDER}two")
        );
        assert_eq!(presentation.icon_offsets, vec![0, 14]);
        assert_eq!(presentation.marks[0].start..presentation.marks[0].end, 6..9);
        assert_eq!(
            presentation.marks[1].start..presentation.marks[1].end,
            20..23
        );
        assert_eq!(presentation.links[0].range, 0..9);
        assert_eq!(presentation.links[1].range, 14..23);
    }

    #[cfg(unix)]
    #[test]
    fn markdown_click_targets_distinguish_local_files_from_web_urls() {
        assert!(matches!(
            MarkdownClickTarget::from_url(Arc::from("/tmp/example.rs:4:2")),
            MarkdownClickTarget::LocalFile(_)
        ));
        assert_eq!(
            MarkdownClickTarget::from_url(Arc::from("https://example.com")),
            MarkdownClickTarget::Web(Arc::from("https://example.com"))
        );
    }
}
