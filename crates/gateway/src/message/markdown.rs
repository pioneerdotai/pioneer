use markdown::{
    ParseOptions,
    mdast::{self, Node},
};
use pioneer_protocol::{
    MarkdownBlock, MarkdownDocument, MarkdownInline, MarkdownList, MarkdownListItem, MarkdownMark,
    MarkdownMarkKind,
};
use std::panic::{AssertUnwindSafe, catch_unwind};

#[derive(Debug, Clone, Default)]
struct InlineStyle {
    bold: bool,
    italic: bool,
    strike: bool,
    code: bool,
    link: Option<String>,
}

fn push_styled_text(out: &mut MarkdownInline, text: &str, style: &InlineStyle) {
    if text.is_empty() {
        return;
    }

    let start = out.text.len();
    out.text.push_str(text);
    let end = out.text.len();

    if style.bold {
        out.marks.push(MarkdownMark {
            start,
            end,
            kind: MarkdownMarkKind::Bold,
        });
    }
    if style.italic {
        out.marks.push(MarkdownMark {
            start,
            end,
            kind: MarkdownMarkKind::Italic,
        });
    }
    if style.strike {
        out.marks.push(MarkdownMark {
            start,
            end,
            kind: MarkdownMarkKind::Strike,
        });
    }
    if style.code {
        out.marks.push(MarkdownMark {
            start,
            end,
            kind: MarkdownMarkKind::Code,
        });
    }
    if let Some(url) = &style.link {
        out.marks.push(MarkdownMark {
            start,
            end,
            kind: MarkdownMarkKind::Link { url: url.clone() },
        });
    }
}

pub(super) fn parse_markdown_document(source: &str) -> MarkdownDocument {
    let normalized = source.replace("\r\n", "\n").replace('\r', "\n");
    if normalized.trim().is_empty() {
        return MarkdownDocument::default();
    }

    match catch_markdown_unwind(|| parse_markdown_document_inner(normalized.as_str())) {
        Some(document) => document,
        None => {
            tracing::warn!(
                source_bytes = normalized.len(),
                "markdown conversion panicked; preserving event content as plain text"
            );
            MarkdownDocument::from_plain_text(normalized)
        }
    }
}

fn catch_markdown_unwind<T>(convert: impl FnOnce() -> T) -> Option<T> {
    catch_unwind(AssertUnwindSafe(convert)).ok()
}

fn parse_markdown_document_inner(normalized: &str) -> MarkdownDocument {
    let root = match markdown::to_mdast(normalized, &ParseOptions::gfm()) {
        Ok(Node::Root(root)) => root,
        Ok(node) => return MarkdownDocument::from_plain_text(node.to_string()),
        Err(_) => return MarkdownDocument::from_plain_text(normalized.to_owned()),
    };

    let blocks = parse_blocks(&root.children);
    if blocks.is_empty() {
        MarkdownDocument::from_plain_text(normalized.to_owned())
    } else {
        MarkdownDocument { blocks }
    }
}

fn parse_blocks(nodes: &[Node]) -> Vec<MarkdownBlock> {
    let mut blocks = Vec::new();
    for node in nodes {
        if let Some(block) = parse_block(node) {
            blocks.push(block);
        }
    }
    blocks
}

fn parse_block(node: &Node) -> Option<MarkdownBlock> {
    match node {
        Node::Paragraph(paragraph) => Some(MarkdownBlock::Paragraph(parse_inline_nodes(
            &paragraph.children,
        ))),
        Node::Heading(heading) => Some(MarkdownBlock::Heading {
            level: heading.depth.clamp(1, 6),
            content: parse_inline_nodes(&heading.children),
        }),
        Node::List(list) => Some(MarkdownBlock::List(parse_list(list))),
        Node::Blockquote(quote) => Some(MarkdownBlock::Quote {
            blocks: parse_blocks(&quote.children),
        }),
        Node::Code(code) => Some(MarkdownBlock::Code {
            language: code.lang.clone(),
            text: code.value.clone(),
        }),
        Node::Math(math) => Some(MarkdownBlock::Code {
            language: Some("math".to_owned()),
            text: math.value.clone(),
        }),
        Node::ThematicBreak(_) => Some(MarkdownBlock::Rule),
        Node::Table(table) => Some(parse_table(table)),
        Node::Toml(toml) => Some(MarkdownBlock::Code {
            language: Some("toml".to_owned()),
            text: toml.value.clone(),
        }),
        Node::Yaml(yaml) => Some(MarkdownBlock::Code {
            language: Some("yaml".to_owned()),
            text: yaml.value.clone(),
        }),
        Node::MdxjsEsm(esm) => Some(MarkdownBlock::Code {
            language: Some("javascript".to_owned()),
            text: esm.value.clone(),
        }),
        Node::MdxFlowExpression(expr) => Some(MarkdownBlock::Code {
            language: Some("mdx".to_owned()),
            text: expr.value.clone(),
        }),
        Node::FootnoteDefinition(definition) => Some(MarkdownBlock::Quote {
            blocks: parse_blocks(&definition.children),
        }),
        Node::Definition(_) => None,
        _ => {
            let inline = parse_inline_node(node, &InlineStyle::default());
            if inline.text.trim().is_empty() {
                None
            } else {
                Some(MarkdownBlock::Paragraph(inline))
            }
        }
    }
}

fn parse_table(table: &mdast::Table) -> MarkdownBlock {
    let mut lines = Vec::new();
    for row in &table.children {
        let Node::TableRow(row) = row else {
            continue;
        };

        let mut cells = Vec::new();
        for cell in &row.children {
            let Node::TableCell(cell) = cell else {
                continue;
            };

            let inline = parse_inline_nodes(&cell.children);
            cells.push(inline.text.trim().to_owned());
        }
        lines.push(cells.join(" | "));
    }

    MarkdownBlock::Code {
        language: Some("table".to_owned()),
        text: lines.join("\n"),
    }
}

fn parse_list(list: &mdast::List) -> MarkdownList {
    let mut items = Vec::new();
    for child in &list.children {
        let Node::ListItem(item) = child else {
            continue;
        };

        let mut blocks = parse_blocks(&item.children);
        if blocks.is_empty() {
            blocks.push(MarkdownBlock::Paragraph(MarkdownInline::default()));
        }
        items.push(MarkdownListItem {
            checked: item.checked,
            blocks,
        });
    }

    MarkdownList {
        ordered: list.ordered,
        start: list.start.unwrap_or(1).max(1) as usize,
        items,
    }
}

fn parse_inline_nodes(nodes: &[Node]) -> MarkdownInline {
    let mut out = MarkdownInline::default();
    let style = InlineStyle::default();
    for node in nodes {
        push_inline_node(&mut out, node, &style);
    }
    out
}

fn parse_inline_node(node: &Node, style: &InlineStyle) -> MarkdownInline {
    let mut out = MarkdownInline::default();
    push_inline_node(&mut out, node, style);
    out
}

fn push_inline_node(out: &mut MarkdownInline, node: &Node, style: &InlineStyle) {
    match node {
        Node::Text(text) => push_styled_text(out, &text.value, style),
        Node::InlineCode(code) => {
            let mut child_style = style.clone();
            child_style.code = true;
            push_styled_text(out, &code.value, &child_style);
        }
        Node::InlineMath(math) => {
            let mut child_style = style.clone();
            child_style.code = true;
            push_styled_text(out, &math.value, &child_style);
        }
        Node::Emphasis(emphasis) => {
            let mut child_style = style.clone();
            child_style.italic = true;
            for child in &emphasis.children {
                push_inline_node(out, child, &child_style);
            }
        }
        Node::Strong(strong) => {
            let mut child_style = style.clone();
            child_style.bold = true;
            for child in &strong.children {
                push_inline_node(out, child, &child_style);
            }
        }
        Node::Delete(delete) => {
            let mut child_style = style.clone();
            child_style.strike = true;
            for child in &delete.children {
                push_inline_node(out, child, &child_style);
            }
        }
        Node::Link(link) => {
            let mut child_style = style.clone();
            child_style.link = Some(link.url.clone());
            for child in &link.children {
                push_inline_node(out, child, &child_style);
            }
        }
        Node::LinkReference(link) => {
            let mut child_style = style.clone();
            child_style.link = Some(format!("#{}", link.identifier));
            for child in &link.children {
                push_inline_node(out, child, &child_style);
            }
        }
        Node::Image(image) => {
            let label = if image.alt.trim().is_empty() {
                "[image]".to_owned()
            } else {
                format!("[image: {}]", image.alt.trim())
            };
            push_styled_text(out, &label, style);
        }
        Node::ImageReference(image) => {
            let label = if image.alt.trim().is_empty() {
                "[image]".to_owned()
            } else {
                format!("[image: {}]", image.alt.trim())
            };
            push_styled_text(out, &label, style);
        }
        Node::FootnoteReference(footnote) => {
            push_styled_text(out, format!("[{}]", footnote.identifier).as_str(), style);
        }
        Node::Break(_) => push_styled_text(out, "\n", style),
        Node::Html(html) => {
            if is_html_break(&html.value) {
                push_styled_text(out, "\n", style);
            } else if !html.value.trim().is_empty() {
                push_styled_text(out, &html.value, style);
            }
        }
        Node::MdxTextExpression(expr) => push_styled_text(out, &expr.value, style),
        _ => {
            if let Some(children) = node.children() {
                for child in children {
                    push_inline_node(out, child, style);
                }
            } else {
                let text = node.to_string();
                if !text.is_empty() {
                    push_styled_text(out, &text, style);
                }
            }
        }
    }
}

fn is_html_break(raw_html: &str) -> bool {
    let html = raw_html.trim().to_ascii_lowercase();
    html == "<br>" || html == "<br/>" || html == "<br />"
}

#[cfg(test)]
mod tests {
    use super::catch_markdown_unwind;

    #[test]
    fn markdown_conversion_panic_is_contained() {
        let converted = catch_markdown_unwind(|| -> () {
            panic!("malformed markdown parser state");
        });

        assert!(converted.is_none());
    }
}
