// Safe unwrap/expect usage: regex patterns are compile-time constants.
#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Structure detection for markdown and text documents.
//!
//! This module detects structural elements in text, including:
//! - Markdown tables (pipe-delimited)
//! - Fenced code blocks (```)
//! - Lists (ordered and unordered)
//! - Headings (#)
//!
//! The detector produces a `StructuredDocument` that can be chunked
//! intelligently by the structural chunker.

use crate::types::structure::{
    DocumentElement, ElementData, ElementType, StructuredCell, StructuredCodeBlock,
    StructuredDocument, StructuredHeading, StructuredList, StructuredRow, StructuredTable,
};
use regex::Regex;
use std::sync::OnceLock;

/// Compiled regex patterns for structure detection.
#[allow(dead_code)]
struct Patterns {
    /// Markdown table row: | cell | cell |
    table_row: Regex,
    /// Table separator: |---|---|
    table_separator: Regex,
    /// Fenced code block start: ```language
    code_fence_start: Regex,
    /// Fenced code block end: ```
    code_fence_end: Regex,
    /// Unordered list item: - item or * item
    unordered_list: Regex,
    /// Ordered list item: 1. item
    ordered_list: Regex,
    /// Heading: # Title
    heading: Regex,
    /// Block quote: > text
    block_quote: Regex,
    /// Horizontal rule: --- or ***
    horizontal_rule: Regex,
}

impl Patterns {
    fn get() -> &'static Self {
        static PATTERNS: OnceLock<Patterns> = OnceLock::new();
        PATTERNS.get_or_init(|| Patterns {
            table_row: Regex::new(r"^\s*\|(.+)\|\s*$").unwrap(),
            table_separator: Regex::new(r"^\s*\|[\s:]*-+[\s:\-|]+\|\s*$").unwrap(),
            code_fence_start: Regex::new(r"^```(\w*)").unwrap(),
            code_fence_end: Regex::new(r"^```\s*$").unwrap(),
            unordered_list: Regex::new(r"^(\s*)[-*+]\s+(.+)$").unwrap(),
            ordered_list: Regex::new(r"^(\s*)(\d+)\.\s+(.+)$").unwrap(),
            heading: Regex::new(r"^(#{1,6})\s+(.+)$").unwrap(),
            block_quote: Regex::new(r"^>\s*(.*)$").unwrap(),
            horizontal_rule: Regex::new(r"^[-*_]{3,}\s*$").unwrap(),
        })
    }
}

/// Detect structure in text and produce a `StructuredDocument`.
#[must_use]
pub fn detect_structure(text: &str) -> StructuredDocument {
    let mut doc = StructuredDocument::from_text(text);
    let patterns = Patterns::get();

    let lines: Vec<&str> = text.lines().collect();
    let mut i = 0;
    let mut char_offset = 0;
    let mut table_counter = 0;

    while i < lines.len() {
        let line = lines[i];
        let line_start = char_offset;
        let line_len = line.len();

        // Try to detect each structure type
        if let Some((element, consumed, _end_offset)) =
            try_detect_table(&lines, i, line_start, patterns, &mut table_counter)
        {
            doc.add_element(element);
            // Skip consumed lines
            for j in i..i + consumed {
                char_offset += lines[j].len() + 1; // +1 for newline
            }
            i += consumed;
            continue;
        }

        if let Some((element, consumed, _end_offset)) =
            try_detect_code_block(&lines, i, line_start, patterns)
        {
            doc.add_element(element);
            for j in i..i + consumed {
                char_offset += lines[j].len() + 1;
            }
            i += consumed;
            continue;
        }

        if let Some((element, consumed)) = try_detect_list(&lines, i, line_start, patterns) {
            doc.add_element(element);
            for j in i..i + consumed {
                char_offset += lines[j].len() + 1;
            }
            i += consumed;
            continue;
        }

        if let Some(element) = try_detect_heading(line, line_start, patterns) {
            doc.add_element(element);
            char_offset += line_len + 1;
            i += 1;
            continue;
        }

        if patterns.horizontal_rule.is_match(line) {
            doc.add_element(DocumentElement {
                element_type: ElementType::Separator,
                char_start: line_start,
                char_end: line_start + line_len,
                data: ElementData::Separator,
            });
            char_offset += line_len + 1;
            i += 1;
            continue;
        }

        // Collect consecutive non-structural lines as paragraph
        let para_start = i;
        let para_char_start = line_start;
        while i < lines.len() {
            let l = lines[i];
            if l.trim().is_empty()
                || patterns.table_row.is_match(l)
                || patterns.code_fence_start.is_match(l)
                || patterns.unordered_list.is_match(l)
                || patterns.ordered_list.is_match(l)
                || patterns.heading.is_match(l)
                || patterns.horizontal_rule.is_match(l)
            {
                break;
            }
            char_offset += lines[i].len() + 1;
            i += 1;
        }

        if i > para_start {
            let para_text: String = lines[para_start..i].join("\n");
            if !para_text.trim().is_empty() {
                doc.add_element(DocumentElement::paragraph(
                    para_text,
                    para_char_start,
                    char_offset.saturating_sub(1),
                ));
            }
        } else {
            // Empty line or unmatched, skip
            char_offset += line_len + 1;
            i += 1;
        }
    }

    doc.update_counts();
    doc
}

/// Try to detect a markdown table starting at line index.
fn try_detect_table(
    lines: &[&str],
    start: usize,
    char_start: usize,
    patterns: &Patterns,
    table_counter: &mut usize,
) -> Option<(DocumentElement, usize, usize)> {
    // Need at least 2 lines for a table (header + separator)
    if start + 1 >= lines.len() {
        return None;
    }

    let first_line = lines[start];
    if !patterns.table_row.is_match(first_line) {
        return None;
    }

    // Check if second line is separator
    let second_line = lines[start + 1];
    if !patterns.table_separator.is_match(second_line) {
        return None;
    }

    // We have a table! Parse it
    *table_counter += 1;
    let table_id = format!("tbl_{:04}", *table_counter);

    let mut table = StructuredTable::new(&table_id);
    let mut raw_lines: Vec<&str> = vec![first_line, second_line];
    let mut consumed = 2;

    // Parse header row
    let header_cells = parse_table_row(first_line);
    table.headers = header_cells.clone();
    table.n_cols = header_cells.len();

    // Add header as first row
    let header_row = StructuredRow::new(
        0,
        header_cells
            .iter()
            .enumerate()
            .map(|(i, text)| StructuredCell::new(text.clone(), i))
            .collect(),
    )
    .as_header();
    table.rows.push(header_row);

    // Parse data rows
    let mut row_index = 1;
    for i in (start + 2)..lines.len() {
        let line = lines[i];
        if !patterns.table_row.is_match(line) {
            break;
        }
        raw_lines.push(line);
        consumed += 1;

        let cells = parse_table_row(line);
        let row = StructuredRow::new(
            row_index,
            cells
                .iter()
                .enumerate()
                .map(|(i, text)| StructuredCell::new(text.clone(), i))
                .collect(),
        );
        table.rows.push(row);
        row_index += 1;
    }

    table.raw_text = raw_lines.join("\n");
    let char_end = char_start + table.raw_text.len();

    Some((
        DocumentElement::table(table, char_start, char_end),
        consumed,
        char_end,
    ))
}

/// Parse a table row into cells.
fn parse_table_row(line: &str) -> Vec<String> {
    let trimmed = line.trim();
    let inner = if trimmed.starts_with('|') && trimmed.ends_with('|') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    inner
        .split('|')
        .map(|cell| cell.trim().to_string())
        .collect()
}

/// Try to detect a fenced code block.
fn try_detect_code_block(
    lines: &[&str],
    start: usize,
    char_start: usize,
    patterns: &Patterns,
) -> Option<(DocumentElement, usize, usize)> {
    let first_line = lines[start];
    let caps = patterns.code_fence_start.captures(first_line)?;

    let language = caps
        .get(1)
        .map(|m| m.as_str().to_string())
        .filter(|s| !s.is_empty());

    // Find closing fence
    let mut end_idx = start + 1;
    while end_idx < lines.len() {
        if patterns.code_fence_end.is_match(lines[end_idx]) {
            break;
        }
        end_idx += 1;
    }

    if end_idx >= lines.len() {
        // No closing fence found, treat as paragraph
        return None;
    }

    let consumed = end_idx - start + 1;
    let content_lines: Vec<&str> = lines[start + 1..end_idx].to_vec();
    let content = content_lines.join("\n");

    let raw_text = lines[start..=end_idx].join("\n");
    let char_end = char_start + raw_text.len();

    let mut block = StructuredCodeBlock::new(content);
    if let Some(lang) = language {
        block = block.with_language(lang);
    }

    Some((
        DocumentElement::code_block(block, char_start, char_end),
        consumed,
        char_end,
    ))
}

/// Try to detect a list (ordered or unordered).
fn try_detect_list(
    lines: &[&str],
    start: usize,
    char_start: usize,
    patterns: &Patterns,
) -> Option<(DocumentElement, usize)> {
    let first_line = lines[start];

    // Check for unordered list
    if let Some(caps) = patterns.unordered_list.captures(first_line) {
        let indent = caps.get(1).map_or(0, |m| m.as_str().len());
        let mut items: Vec<String> = vec![caps.get(2)?.as_str().to_string()];
        let mut consumed = 1;

        // Collect consecutive list items at same indentation
        for i in (start + 1)..lines.len() {
            if let Some(item_caps) = patterns.unordered_list.captures(lines[i]) {
                let item_indent = item_caps.get(1).map_or(0, |m| m.as_str().len());
                if item_indent == indent {
                    items.push(item_caps.get(2).unwrap().as_str().to_string());
                    consumed += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        let raw_text = lines[start..start + consumed].join("\n");
        let char_end = char_start + raw_text.len();

        return Some((
            DocumentElement::list(StructuredList::unordered(items), char_start, char_end),
            consumed,
        ));
    }

    // Check for ordered list
    if let Some(caps) = patterns.ordered_list.captures(first_line) {
        let indent = caps.get(1).map_or(0, |m| m.as_str().len());
        let start_num: usize = caps.get(2)?.as_str().parse().unwrap_or(1);
        let mut items: Vec<String> = vec![caps.get(3)?.as_str().to_string()];
        let mut consumed = 1;

        // Collect consecutive list items at same indentation
        for i in (start + 1)..lines.len() {
            if let Some(item_caps) = patterns.ordered_list.captures(lines[i]) {
                let item_indent = item_caps.get(1).map_or(0, |m| m.as_str().len());
                if item_indent == indent {
                    items.push(item_caps.get(3).unwrap().as_str().to_string());
                    consumed += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        let raw_text = lines[start..start + consumed].join("\n");
        let char_end = char_start + raw_text.len();

        let mut list = StructuredList::ordered(items);
        list.start = start_num;

        return Some((DocumentElement::list(list, char_start, char_end), consumed));
    }

    None
}

/// Try to detect a heading.
fn try_detect_heading(
    line: &str,
    char_start: usize,
    patterns: &Patterns,
) -> Option<DocumentElement> {
    let caps = patterns.heading.captures(line)?;
    // Safe: markdown headers are ### (max few chars).
    let level = u8::try_from(caps.get(1)?.as_str().len()).unwrap_or(0);
    let text = caps.get(2)?.as_str().to_string();
    let char_end = char_start + line.len();

    Some(DocumentElement::heading(
        StructuredHeading::new(level, text),
        char_start,
        char_end,
    ))
}

/// Detect ASCII/pipe tables in extracted PDF text.
///
/// PDF text extraction often produces tables that look like:
/// ```text
/// Name          Age    City
/// Alice         30     NYC
/// Bob           25     LA
/// ```
///
/// This function attempts to detect such patterns by analyzing
/// column alignment.
#[must_use]
pub fn detect_ascii_tables(text: &str) -> Vec<(usize, usize, StructuredTable)> {
    let mut tables = Vec::new();
    let lines: Vec<&str> = text.lines().collect();

    // Simple heuristic: look for consecutive lines with similar structure
    // (multiple space-separated columns)
    let mut i = 0;
    let mut table_counter = 0;

    while i < lines.len() {
        if let Some((table, consumed)) = try_detect_ascii_table(&lines, i, &mut table_counter) {
            let char_start = lines[..i].iter().map(|l| l.len() + 1).sum::<usize>();
            let char_end = char_start
                + lines[i..i + consumed]
                    .iter()
                    .map(|l| l.len() + 1)
                    .sum::<usize>();
            tables.push((char_start, char_end, table));
            i += consumed;
        } else {
            i += 1;
        }
    }

    tables
}

/// Try to detect an ASCII table (space-aligned columns).
fn try_detect_ascii_table(
    lines: &[&str],
    start: usize,
    table_counter: &mut usize,
) -> Option<(StructuredTable, usize)> {
    // Need at least 2 lines
    if start + 1 >= lines.len() {
        return None;
    }

    let first_line = lines[start];

    // Skip empty lines
    if first_line.trim().is_empty() {
        return None;
    }

    // Detect columns by finding consistent spacing patterns
    let columns = detect_column_positions(first_line);
    if columns.len() < 2 {
        return None;
    }

    // Check if following lines have similar column structure
    let mut consistent_lines = 1;
    for i in (start + 1)..lines.len() {
        let line = lines[i];
        if line.trim().is_empty() {
            break;
        }
        let line_cols = detect_column_positions(line);
        if !columns_roughly_match(&columns, &line_cols) {
            break;
        }
        consistent_lines += 1;
    }

    // Need at least 2 consistent lines
    if consistent_lines < 2 {
        return None;
    }

    // Build table
    *table_counter += 1;
    let table_id = format!("ascii_tbl_{:04}", *table_counter);
    let mut table = StructuredTable::new(&table_id);

    // Parse header (first line)
    let header_cells = split_by_columns(first_line, &columns);
    table.headers = header_cells.clone();
    table.n_cols = header_cells.len();

    let header_row = StructuredRow::new(
        0,
        header_cells
            .iter()
            .enumerate()
            .map(|(i, text)| StructuredCell::new(text.clone(), i))
            .collect(),
    )
    .as_header();
    table.rows.push(header_row);

    // Parse data rows
    for (row_idx, i) in ((start + 1)..(start + consistent_lines)).enumerate() {
        let cells = split_by_columns(lines[i], &columns);
        let row = StructuredRow::new(
            row_idx + 1,
            cells
                .iter()
                .enumerate()
                .map(|(i, text)| StructuredCell::new(text.clone(), i))
                .collect(),
        );
        table.rows.push(row);
    }

    table.raw_text = lines[start..start + consistent_lines].join("\n");

    Some((table, consistent_lines))
}

/// Detect column positions based on word boundaries and spacing.
fn detect_column_positions(line: &str) -> Vec<usize> {
    let mut positions = Vec::new();
    let mut in_word = false;
    let mut space_run = 0;

    for (i, ch) in line.char_indices() {
        if ch.is_whitespace() {
            if in_word {
                in_word = false;
                space_run = 1;
            } else {
                space_run += 1;
            }
        } else {
            if !in_word {
                // Start of new word
                if positions.is_empty() || space_run >= 2 {
                    positions.push(i);
                }
                in_word = true;
            }
            space_run = 0;
        }
    }

    positions
}

/// Check if two column position sets roughly match.
fn columns_roughly_match(cols1: &[usize], cols2: &[usize]) -> bool {
    if cols1.len() != cols2.len() {
        return false;
    }

    // Allow some tolerance in column positions
    const TOLERANCE: usize = 3;

    for (c1, c2) in cols1.iter().zip(cols2.iter()) {
        if (*c1 as isize - *c2 as isize).unsigned_abs() > TOLERANCE {
            return false;
        }
    }

    true
}

/// Split a line by column positions.
fn split_by_columns(line: &str, columns: &[usize]) -> Vec<String> {
    let chars: Vec<char> = line.chars().collect();
    let mut result = Vec::new();

    for (i, &start) in columns.iter().enumerate() {
        let end = columns.get(i + 1).copied().unwrap_or(chars.len());
        let cell: String = chars[start.min(chars.len())..end.min(chars.len())]
            .iter()
            .collect();
        result.push(cell.trim().to_string());
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_markdown_table() {
        let text = r"Some text before.

| Name | Age | City |
|------|-----|------|
| Alice | 30 | NYC |
| Bob | 25 | LA |

Some text after.";

        let doc = detect_structure(text);
        assert_eq!(doc.table_count, 1);

        let table = doc.tables().next().unwrap();
        assert_eq!(table.headers, vec!["Name", "Age", "City"]);
        assert_eq!(table.rows.len(), 3); // header + 2 data rows
        assert_eq!(table.data_row_count(), 2);
    }

    #[test]
    fn test_detect_code_block() {
        let text = r#"Here is some code:

```rust
fn main() {
    println!("Hello");
}
```

And more text."#;

        let doc = detect_structure(text);
        assert_eq!(doc.code_block_count, 1);

        let code_elem = doc
            .elements
            .iter()
            .find(|e| e.element_type == ElementType::CodeBlock)
            .unwrap();

        if let ElementData::CodeBlock(block) = &code_elem.data {
            assert_eq!(block.language, Some("rust".to_string()));
            assert!(block.content.contains("println!"));
        } else {
            panic!("Expected code block");
        }
    }

    #[test]
    fn test_detect_lists() {
        let text = r"Shopping list:

- Apples
- Bananas
- Oranges

Steps:

1. First step
2. Second step
3. Third step";

        let doc = detect_structure(text);

        let lists: Vec<_> = doc
            .elements
            .iter()
            .filter(|e| e.element_type == ElementType::List)
            .collect();

        assert_eq!(lists.len(), 2);
    }

    #[test]
    fn test_detect_headings() {
        let text = r"# Main Title

Some intro text.

## Section One

Content here.

### Subsection

More content.";

        let doc = detect_structure(text);

        let headings: Vec<_> = doc
            .elements
            .iter()
            .filter(|e| e.element_type == ElementType::Heading)
            .collect();

        assert_eq!(headings.len(), 3);

        if let ElementData::Heading(h) = &headings[0].data {
            assert_eq!(h.level, 1);
            assert_eq!(h.text, "Main Title");
        }
    }

    #[test]
    fn test_complex_document() {
        let text = r"# Report

## Summary

This report covers Q1 results.

| Quarter | Revenue | Growth |
|---------|---------|--------|
| Q1 2024 | $10M    | 15%    |
| Q1 2023 | $8.7M   | 12%    |

## Analysis

Key findings:

- Revenue increased by 15%
- Customer base grew
- Market share expanded

```python
def calculate_growth(current, previous):
    return (current - previous) / previous * 100
```

---

## Conclusion

Strong performance overall.";

        let doc = detect_structure(text);

        assert_eq!(doc.table_count, 1);
        assert_eq!(doc.code_block_count, 1);

        // Check headings
        let heading_count = doc
            .elements
            .iter()
            .filter(|e| e.element_type == ElementType::Heading)
            .count();
        assert_eq!(heading_count, 4);

        // Check lists
        let list_count = doc
            .elements
            .iter()
            .filter(|e| e.element_type == ElementType::List)
            .count();
        assert_eq!(list_count, 1);

        // Check separator
        let sep_count = doc
            .elements
            .iter()
            .filter(|e| e.element_type == ElementType::Separator)
            .count();
        assert_eq!(sep_count, 1);
    }

    #[test]
    fn test_table_parsing() {
        let row = "| Alice | 30 | NYC |";
        let cells = parse_table_row(row);
        assert_eq!(cells, vec!["Alice", "30", "NYC"]);
    }

    #[test]
    fn test_column_detection() {
        let line = "Name      Age    City";
        let positions = detect_column_positions(line);
        assert_eq!(positions.len(), 3);
    }

    #[test]
    fn test_ascii_table_detection() {
        let text = r"Name          Age    City
Alice         30     NYC
Bob           25     LA
Charlie       35     SF";

        let tables = detect_ascii_tables(text);
        assert_eq!(tables.len(), 1);

        let (_, _, table) = &tables[0];
        assert_eq!(table.headers.len(), 3);
        assert_eq!(table.data_row_count(), 3); // All rows treated as data in ASCII
    }
}
