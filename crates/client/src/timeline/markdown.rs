//! Semantic Markdown identities and safe source serialization shared by shells.
use pioneer_protocol::{MarkdownBlock, MarkdownDocument, MarkdownInline, MarkdownMarkKind};
use serde::{Deserialize, Serialize};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkdownNode {
    pub id: u64,
    pub revision: u64,
    pub block: MarkdownBlock,
    pub children: Vec<MarkdownNode>,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkdownPresentation {
    pub document_id: String,
    pub revision: u64,
    pub nodes: Vec<MarkdownNode>,
    pub source: String,
    next_id: u64,
}
impl MarkdownPresentation {
    pub fn project(
        scope: &str,
        document: &MarkdownDocument,
        previous: Option<&Self>,
        revision: u64,
    ) -> Self {
        let prefix = format!("markdown:{}:{scope}:", scope.len());
        let previous = previous.filter(|old| old.document_id.starts_with(&prefix));
        let document_id = previous.map_or_else(
            || format!("{prefix}{revision}"),
            |old| old.document_id.clone(),
        );
        if let Some(old) = previous.filter(|old| {
            old.nodes
                .iter()
                .map(|node| &node.block)
                .eq(document.blocks.iter())
        }) {
            return old.clone();
        }
        fn flatten<'a>(nodes: &'a [MarkdownNode], result: &mut Vec<&'a MarkdownNode>) {
            for node in nodes {
                result.push(node);
                flatten(&node.children, result);
            }
        }
        let mut old = Vec::new();
        if let Some(previous) = previous {
            flatten(&previous.nodes, &mut old);
        }
        let mut next_id = previous.map_or(1, |old| old.next_id);
        let mut used = std::collections::HashSet::new();
        fn nodes(
            input: &[MarkdownBlock],
            old: &[&MarkdownNode],
            used: &mut std::collections::HashSet<u64>,
            next_id: &mut u64,
            revision: u64,
        ) -> Vec<MarkdownNode> {
            input
                .iter()
                .map(|block| {
                    let matching = old
                        .iter()
                        .find(|node| node.block == *block && !used.contains(&node.id));
                    let (id, node_revision) = if let Some(node) = matching {
                        used.insert(node.id);
                        (node.id, node.revision)
                    } else {
                        let id = *next_id;
                        *next_id += 1;
                        (id, revision)
                    };
                    let nested = match block {
                        MarkdownBlock::Quote { blocks } => blocks.clone(),
                        MarkdownBlock::List(list) => list
                            .items
                            .iter()
                            .flat_map(|item| item.blocks.clone())
                            .collect(),
                        _ => Vec::new(),
                    };
                    MarkdownNode {
                        id,
                        revision: node_revision,
                        block: block.clone(),
                        children: nodes(&nested, old, used, next_id, revision),
                    }
                })
                .collect()
        }
        let nodes = nodes(&document.blocks, &old, &mut used, &mut next_id, revision);
        Self {
            document_id,
            revision,
            nodes,
            source: serialize_blocks(&document.blocks),
            next_id,
        }
    }
    pub fn code_blocks(&self) -> Vec<&MarkdownNode> {
        fn visit<'a>(nodes: &'a [MarkdownNode], output: &mut Vec<&'a MarkdownNode>) {
            for node in nodes {
                if matches!(node.block, MarkdownBlock::Code { .. }) {
                    output.push(node);
                }
                visit(&node.children, output);
            }
        }
        let mut output = Vec::new();
        visit(&self.nodes, &mut output);
        output
    }
}
/// Clamp an offset to a UTF-8 boundary. Shells choose the endpoint affinity.
pub fn markdown_byte_boundary(text: &str, offset: usize, forward: bool) -> usize {
    let mut offset = offset.min(text.len());
    while !text.is_char_boundary(offset) {
        if forward {
            offset += 1;
        } else {
            offset -= 1;
        }
    }
    offset
}
pub fn normalize_code_source(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}
pub fn fence_language(language: Option<&str>) -> String {
    let mut output = String::new();
    let mut started = false;
    for ch in language.unwrap_or_default().chars() {
        if ch.is_whitespace() {
            if started {
                break;
            } else {
                continue;
            }
        }
        started = true;
        if ch == '`' || ch.is_control() {
            continue;
        }
        if output.len() + ch.len_utf8() > 64 {
            break;
        }
        output.push(ch);
    }
    output
}
fn serialize_blocks(blocks: &[MarkdownBlock]) -> String {
    let source = blocks
        .iter()
        .map(serialize_block)
        .collect::<Vec<_>>()
        .join("\n\n");
    let trimmed = source.trim();
    if trimmed.is_empty() {
        " ".to_owned()
    } else {
        trimmed.to_owned()
    }
}
fn serialize_block(block: &MarkdownBlock) -> String {
    match block {
        MarkdownBlock::Paragraph(inline) => serialize_inline(inline),
        MarkdownBlock::Heading { level, content } => format!(
            "{} {}",
            "#".repeat((*level).clamp(1, 6) as usize),
            serialize_inline(content)
        ),
        MarkdownBlock::Rule => "---".to_owned(),
        MarkdownBlock::Quote { blocks } => serialize_blocks(blocks)
            .split('\n')
            .map(|line| format!("> {line}"))
            .collect::<Vec<_>>()
            .join("\n"),
        MarkdownBlock::Code { language, text } => {
            let source = normalize_code_source(text);
            let mut longest = 0;
            let mut run = 0;
            for ch in source.chars() {
                if ch == '`' {
                    run += 1;
                    longest = longest.max(run);
                } else {
                    run = 0;
                }
            }
            let fence = "`".repeat(3.max(longest + 1));
            format!(
                "{fence}{}\n{source}\n{fence}",
                fence_language(language.as_deref())
            )
        }
        MarkdownBlock::List(list) => list
            .items
            .iter()
            .enumerate()
            .map(|(ix, item)| {
                let prefix = match item.checked {
                    Some(true) => "- [x]".to_owned(),
                    Some(false) => "- [ ]".to_owned(),
                    None if list.ordered => format!("{}.", list.start.saturating_add(ix)),
                    None => "-".to_owned(),
                };
                let indent = " ".repeat(prefix.len() + 1);
                let content = serialize_blocks(&item.blocks)
                    .split('\n')
                    .enumerate()
                    .map(|(ix, line)| {
                        if ix == 0 {
                            line.to_owned()
                        } else {
                            format!("{indent}{line}")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{prefix} {content}")
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}
fn serialize_inline(inline: &MarkdownInline) -> String {
    let text = if inline.text.is_empty() {
        " "
    } else {
        &inline.text
    };
    let marks = inline
        .marks
        .iter()
        .filter_map(|mark| {
            let start = markdown_byte_boundary(text, mark.start, false);
            let end = markdown_byte_boundary(text, mark.end, false);
            (start < end).then_some((mark, start, end))
        })
        .collect::<Vec<_>>();
    let mut boundaries = vec![0, text.len()];
    for (_, start, end) in &marks {
        boundaries.extend([*start, *end]);
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    boundaries
        .windows(2)
        .map(|range| {
            let mut value = text[range[0]..range[1]].to_owned();
            for (mark, _, _) in marks
                .iter()
                .filter(|(_, start, end)| *start <= range[0] && *end >= range[1])
            {
                value = match &mark.kind {
                    MarkdownMarkKind::Bold => format!("**{value}**"),
                    MarkdownMarkKind::Italic => format!("*{value}*"),
                    MarkdownMarkKind::Strike => format!("~~{value}~~"),
                    MarkdownMarkKind::Code => format!("`{}`", value.replace('`', "\\`")),
                    MarkdownMarkKind::Link { url } => format!("[{value}]({url})"),
                };
            }
            value
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    fn code(text: &str) -> MarkdownBlock {
        MarkdownBlock::Code {
            language: Some("ts".into()),
            text: text.into(),
        }
    }
    #[test]
    fn identities_follow_blocks_across_insert_reorder_and_change() {
        let first = MarkdownPresentation::project(
            "row-a",
            &MarkdownDocument {
                blocks: vec![code("one"), code("two")],
            },
            None,
            1,
        );
        let equal = MarkdownPresentation::project(
            "row-a",
            &MarkdownDocument {
                blocks: vec![code("one"), code("two")],
            },
            Some(&first),
            2,
        );
        assert_eq!(first, equal);
        let changed = MarkdownPresentation::project(
            "row-a",
            &MarkdownDocument {
                blocks: vec![code("new"), code("two"), code("one")],
            },
            Some(&first),
            3,
        );
        assert_eq!(changed.nodes[1].id, first.nodes[1].id);
        assert_eq!(changed.nodes[2].id, first.nodes[0].id);
        assert_ne!(changed.nodes[0].id, first.nodes[0].id);
        assert_ne!(
            first.document_id,
            MarkdownPresentation::project(
                "row-b",
                &MarkdownDocument {
                    blocks: vec![code("one")]
                },
                None,
                1
            )
            .document_id
        );
    }
    #[test]
    fn source_preserves_code_whitespace_and_safe_fences() {
        assert_eq!(serialize_blocks(&[code("value\n")]), "```ts\nvalue\n\n```");
        assert_eq!(
            serialize_blocks(&[code("before ``` after")]),
            "````ts\nbefore ``` after\n````"
        );
        assert_eq!(normalize_code_source("\tvalue  \r\n"), "\tvalue  \n");
        assert_eq!(fence_language(Some("ts`\nignored")), "ts");
        assert_eq!(markdown_byte_boundary("界", 1, false), 0);
        assert_eq!(markdown_byte_boundary("界", 1, true), 3);
    }
}
