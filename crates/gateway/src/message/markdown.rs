use markdown::{
    ParseOptions,
    mdast::{self, Node},
};
use pioneer_protocol::{
    MarkdownBlock, MarkdownDocument, MarkdownInline, MarkdownList, MarkdownListItem, MarkdownMark,
    MarkdownMarkKind, MarkdownTable, MarkdownTableAlignment,
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, Instant};

pub(super) struct ProfiledMarkdownDocument {
    pub(super) document: MarkdownDocument,
    pub(super) elapsed: Duration,
}

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
    parse_markdown_document_profiled(
        source,
        pioneer_observability::GatewayMarkdownStreamKind::Snapshot,
    )
    .document
}

pub(super) fn parse_streaming_markdown_document(
    source: &str,
    stream: pioneer_observability::GatewayMarkdownStreamKind,
) -> ProfiledMarkdownDocument {
    parse_markdown_document_profiled(source, stream)
}

fn parse_markdown_document_profiled(
    source: &str,
    stream: pioneer_observability::GatewayMarkdownStreamKind,
) -> ProfiledMarkdownDocument {
    let started = Instant::now();
    let normalized = source.replace("\r\n", "\n").replace('\r', "\n");
    let (document, outcome) = if normalized.trim().is_empty() {
        (
            MarkdownDocument::default(),
            pioneer_observability::GatewayMarkdownOutcome::Empty,
        )
    } else {
        match catch_markdown_unwind(|| parse_markdown_document_inner(normalized.as_str())) {
            Some(parsed) => parsed,
            None => {
                tracing::warn!(
                    source_bytes = normalized.len(),
                    "markdown conversion panicked; preserving event content as plain text"
                );
                (
                    MarkdownDocument::from_plain_text(normalized),
                    pioneer_observability::GatewayMarkdownOutcome::PanicFallback,
                )
            }
        }
    };
    let elapsed = started.elapsed();
    pioneer_observability::record_gateway_markdown_stage(
        pioneer_observability::GatewayMarkdownStageMetric {
            stage: pioneer_observability::GatewayMarkdownStage::Parse,
            stream,
            outcome,
            elapsed,
            input_bytes: Some(source.len()),
            output_bytes: None,
            block_count: Some(document.blocks.len()),
        },
    );
    ProfiledMarkdownDocument { document, elapsed }
}

fn catch_markdown_unwind<T>(convert: impl FnOnce() -> T) -> Option<T> {
    catch_unwind(AssertUnwindSafe(convert)).ok()
}

fn parse_markdown_document_inner(
    normalized: &str,
) -> (
    MarkdownDocument,
    pioneer_observability::GatewayMarkdownOutcome,
) {
    let root = match markdown::to_mdast(normalized, &ParseOptions::gfm()) {
        Ok(Node::Root(root)) => root,
        Ok(node) => {
            return (
                MarkdownDocument::from_plain_text(node.to_string()),
                pioneer_observability::GatewayMarkdownOutcome::Fallback,
            );
        }
        Err(_) => {
            return (
                MarkdownDocument::from_plain_text(normalized.to_owned()),
                pioneer_observability::GatewayMarkdownOutcome::Fallback,
            );
        }
    };

    let mut definitions = Vec::new();
    collect_table_definitions(&root.children, 0, false, &mut definitions);
    let blocks = parse_blocks(&root.children, normalized, &definitions);
    if blocks.is_empty() {
        (
            MarkdownDocument::from_plain_text(normalized.to_owned()),
            pioneer_observability::GatewayMarkdownOutcome::Fallback,
        )
    } else {
        (
            MarkdownDocument { blocks },
            pioneer_observability::GatewayMarkdownOutcome::Ok,
        )
    }
}

fn parse_blocks(
    nodes: &[Node],
    source: &str,
    definitions: &[TableReferenceDefinition<'_>],
) -> Vec<MarkdownBlock> {
    let mut blocks = Vec::new();
    for node in nodes {
        if let Some(block) = parse_block(node, source, definitions) {
            blocks.push(block);
        }
    }
    blocks
}

fn parse_block(
    node: &Node,
    source: &str,
    definitions: &[TableReferenceDefinition<'_>],
) -> Option<MarkdownBlock> {
    match node {
        Node::Paragraph(paragraph) => Some(MarkdownBlock::Paragraph(parse_inline_nodes(
            &paragraph.children,
        ))),
        Node::Heading(heading) => Some(MarkdownBlock::Heading {
            level: heading.depth.clamp(1, 6),
            content: parse_inline_nodes(&heading.children),
        }),
        Node::List(list) => Some(MarkdownBlock::List(parse_list(list, source, definitions))),
        Node::Blockquote(quote) => Some(MarkdownBlock::Quote {
            blocks: parse_blocks(&quote.children, source, definitions),
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
        Node::Table(table) => Some(parse_table(table, source, definitions)),
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
            blocks: parse_blocks(&definition.children, source, definitions),
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

// Borrow parser definitions in document order: the first definition wins, as in GFM.
struct TableReferenceDefinition<'a> {
    node: &'a mdast::Definition,
    quote_depth: usize,
    indented: bool,
}

fn collect_table_definitions<'a>(
    nodes: &'a [Node],
    quote_depth: usize,
    indented: bool,
    out: &mut Vec<TableReferenceDefinition<'a>>,
) {
    for node in nodes {
        if let Node::Definition(definition) = node {
            out.push(TableReferenceDefinition {
                node: definition,
                quote_depth,
                indented,
            });
        }
        if let Some(children) = node.children() {
            collect_table_definitions(
                children,
                quote_depth + usize::from(matches!(node, Node::Blockquote(_))),
                indented || matches!(node, Node::ListItem(_) | Node::FootnoteDefinition(_)),
                out,
            );
        }
    }
}

fn resolve_table_references(
    nodes: &mut [Node],
    definitions: &[TableReferenceDefinition<'_>],
    used: &mut Vec<usize>,
) {
    for node in nodes {
        let identifier = match node {
            Node::LinkReference(reference) => Some(reference.identifier.as_str()),
            Node::ImageReference(reference) => Some(reference.identifier.as_str()),
            _ => None,
        };
        if let Some(ix) =
            identifier.and_then(|id| definitions.iter().position(|d| d.node.identifier == id))
        {
            if !used.contains(&ix) {
                used.push(ix);
            }
            if let Node::LinkReference(reference) = node {
                let definition = definitions[ix].node;
                *node = Node::Link(mdast::Link {
                    children: std::mem::take(&mut reference.children),
                    position: reference.position.clone(),
                    url: definition.url.clone(),
                    title: definition.title.clone(),
                });
            }
        }
        if let Some(children) = node.children_mut() {
            resolve_table_references(children, definitions, used);
        }
    }
}

fn table_definition_source(definition: &TableReferenceDefinition<'_>, source: &str) -> String {
    let position = definition
        .node
        .position
        .as_ref()
        .expect("GFM definition position");
    source[position.start.offset..position.end.offset]
        .split('\n')
        .enumerate()
        .map(|(ix, mut line)| {
            if ix == 0 {
                return line.trim_start_matches([' ', '\t']);
            }
            // Positions remove the first line's containers only. Remove ancestor
            // quote markers and list indentation on continuation lines.
            for _ in 0..definition.quote_depth {
                let Some(rest) = line.trim_start_matches([' ', '\t']).strip_prefix('>') else {
                    break;
                };
                line = rest
                    .strip_prefix(' ')
                    .or_else(|| rest.strip_prefix('\t'))
                    .unwrap_or(rest);
            }
            if definition.indented {
                line = line.trim_start_matches([' ', '\t']);
            }
            line
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_table(
    table: &mdast::Table,
    source: &str,
    definitions: &[TableReferenceDefinition<'_>],
) -> MarkdownBlock {
    let rows: Vec<_> = table
        .children
        .iter()
        .filter_map(|node| {
            if let Node::TableRow(row) = node {
                Some(row)
            } else {
                None
            }
        })
        .collect();
    let mut used = Vec::new();
    let mut cells = |row: &mdast::TableRow| {
        row.children
            .iter()
            .filter_map(|node| {
                if let Node::TableCell(cell) = node {
                    let mut children = cell.children.clone();
                    resolve_table_references(&mut children, definitions, &mut used);
                    Some(parse_inline_nodes(&children))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    };

    // markdown-rs puts row positions after container prefixes and before EOL.
    // A whole-table slice would retain Quote/List prefixes on subsequent lines.
    let row_source = |row: &mdast::TableRow| {
        let position = row.position.as_ref().expect("GFM row position");
        // unist offsets omit markdown-rs virtual spaces inside a tab. Remove
        // remaining line indentation so a container tab cannot become code.
        source[position.start.offset..position.end.offset].trim_start_matches([' ', '\t'])
    };
    let header = rows[0];
    let header_end = header
        .position
        .as_ref()
        .expect("GFM header position")
        .end
        .offset;
    let table_end = table
        .position
        .as_ref()
        .expect("GFM table position")
        .end
        .offset;
    // The delimiter has no mdast row. Its first syntax byte is |, : or -;
    // whitespace and > before it belong to indentation / ancestor quotes.
    let delimiter = source[header_end + 1..table_end]
        .split('\n')
        .next()
        .expect("GFM delimiter line")
        .trim_start_matches([' ', '\t', '>']);
    let mut fragment = vec![row_source(header), delimiter];
    fragment.extend(rows.iter().skip(1).map(|row| row_source(row)));

    let headers = cells(header);
    let rows = rows.iter().skip(1).map(|row| cells(row)).collect();
    let source_context = source;
    let mut source = fragment.join("\n");
    for ix in used {
        source.push_str("\n\n");
        source.push_str(&table_definition_source(&definitions[ix], source_context));
    }
    MarkdownBlock::Table(MarkdownTable {
        headers,
        rows,
        alignments: table
            .align
            .iter()
            .map(|align| match align {
                mdast::AlignKind::None => MarkdownTableAlignment::None,
                mdast::AlignKind::Left => MarkdownTableAlignment::Left,
                mdast::AlignKind::Center => MarkdownTableAlignment::Center,
                mdast::AlignKind::Right => MarkdownTableAlignment::Right,
            })
            .collect(),
        source,
    })
}

fn parse_list(
    list: &mdast::List,
    source: &str,
    definitions: &[TableReferenceDefinition<'_>],
) -> MarkdownList {
    let mut items = Vec::new();
    for child in &list.children {
        let Node::ListItem(item) = child else {
            continue;
        };

        let mut blocks = parse_blocks(&item.children, source, definitions);
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
    use super::{
        catch_markdown_unwind, parse_markdown_document, parse_streaming_markdown_document,
    };
    use pioneer_protocol::{MarkdownBlock, MarkdownMarkKind, MarkdownTableAlignment};

    #[test]
    fn table_preserves_gfm_and_inline_cells_without_container_prefixes() {
        let fragment = "| **Имя** | [файл][docs] |\n| :--- | ---: |\n| a\\|b | `x` |";
        let source = format!(
            "> 1. {}\n\n> [DOCS]: pioneer://file/example\n> [unused]: https://unused.example",
            fragment.replace('\n', "\n>    ")
        );
        let document = parse_markdown_document(&source);
        let MarkdownBlock::Quote { blocks } = &document.blocks[0] else {
            panic!("quote")
        };
        let MarkdownBlock::List(list) = &blocks[0] else {
            panic!("list")
        };
        let MarkdownBlock::Table(table) = &list.items[0].blocks[0] else {
            panic!("table")
        };
        assert_eq!(
            table.source,
            format!("{fragment}\n\n[DOCS]: pioneer://file/example")
        );
        assert_eq!(
            table.alignments,
            vec![MarkdownTableAlignment::Left, MarkdownTableAlignment::Right]
        );
        assert_eq!(table.headers[0].text, "Имя");
        assert!(matches!(
            table.headers[0].marks[0].kind,
            MarkdownMarkKind::Bold
        ));
        assert!(matches!(
            table.headers[1].marks[0].kind,
            MarkdownMarkKind::Link { ref url } if url == "pioneer://file/example"
        ));
        assert_eq!(table.rows[0][0].text, "a|b");
        assert!(matches!(
            table.rows[0][1].marks[0].kind,
            MarkdownMarkKind::Code
        ));
    }

    #[test]
    fn profiled_streaming_parser_preserves_the_markdown_document() {
        let source = "# Heading\n\nA **bold** paragraph.\n\n```rust\nlet answer = 42;\n```";

        let expected = parse_markdown_document(source);
        let profiled = parse_streaming_markdown_document(
            source,
            pioneer_observability::GatewayMarkdownStreamKind::AgentMessage,
        );

        assert_eq!(profiled.document, expected);
    }

    #[test]
    fn profiled_streaming_parser_preserves_empty_documents() {
        let profiled = parse_streaming_markdown_document(
            " \r\n\t",
            pioneer_observability::GatewayMarkdownStreamKind::AgentMessage,
        );

        assert!(profiled.document.blocks.is_empty());
    }

    #[test]
    fn markdown_conversion_panic_is_contained() {
        let converted = catch_markdown_unwind(|| -> () {
            panic!("malformed markdown parser state");
        });

        assert!(converted.is_none());
    }
}
