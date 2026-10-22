//! Multi-page table merging.
//!
//! This module handles the detection and merging of tables that span
//! multiple pages in a document.

use super::types::{ExtractedTable, TableExtractionOptions, TableQuality, TableRow};

/// Merge tables that span multiple pages.
///
/// Tables are candidates for merging when:
/// 1. They are on consecutive pages
/// 2. They have the same column count
/// 3. They are from the same source file
/// 4. Their headers are similar (if both have headers)
///
/// # Arguments
/// * `tables` - Vector of tables to potentially merge
/// * `options` - Extraction options containing merge thresholds
///
/// # Returns
/// Vector of tables with multi-page tables merged
#[must_use]
pub fn merge_multi_page_tables(
    tables: Vec<ExtractedTable>,
    options: &TableExtractionOptions,
) -> Vec<ExtractedTable> {
    if !options.merge_multi_page || tables.len() < 2 {
        return tables;
    }

    // Sort tables by page order
    let mut sorted_tables = tables;
    sorted_tables.sort_by_key(|t| (t.page_start, t.page_end));

    let mut merged: Vec<ExtractedTable> = Vec::new();
    let mut skip_indices = std::collections::HashSet::new();

    for (i, table) in sorted_tables.iter().enumerate() {
        if skip_indices.contains(&i) {
            continue;
        }

        let mut current = table.clone();

        // Look for continuation on subsequent pages
        for (j, candidate) in sorted_tables.iter().enumerate().skip(i + 1) {
            if skip_indices.contains(&j) {
                continue;
            }

            // Check if candidate should be merged with current
            let merge_score = calculate_merge_score(&current, candidate, options);

            if merge_score >= options.header_similarity_threshold {
                current = merge_two_tables(current, candidate.clone());
                skip_indices.insert(j);
            } else {
                // Tables are sorted, so if this one doesn't merge, neither will later ones
                // (unless they're on a much later page)
                if candidate.page_start > current.page_end + 2 {
                    break;
                }
            }
        }

        merged.push(current);
    }

    merged
}

/// Calculate a score indicating how likely two tables should be merged.
///
/// Returns a score from 0.0 to 1.0, where higher means more likely to merge.
fn calculate_merge_score(
    first: &ExtractedTable,
    second: &ExtractedTable,
    options: &TableExtractionOptions,
) -> f32 {
    let mut score = 0.0f32;
    let mut factors = 0;

    // Factor 1: Must be consecutive pages (or nearly consecutive)
    let page_gap = second.page_start as i32 - first.page_end as i32;
    if page_gap == 1 {
        score += 1.0;
        factors += 1;
    } else if page_gap == 0 {
        // Same page - likely different tables
        return 0.0;
    } else if page_gap <= 2 {
        // Small gap - might be continuation with intervening content
        score += 0.5;
        factors += 1;
    } else {
        // Too far apart
        return 0.0;
    }

    // Factor 2: Same source file
    if first.source_file == second.source_file {
        score += 1.0;
        factors += 1;
    } else {
        return 0.0;
    }

    // Factor 3: Same column count
    if first.n_cols == second.n_cols {
        score += 1.0;
        factors += 1;
    } else {
        // Different column count - not a continuation
        return 0.0;
    }

    // Factor 4: Header similarity (if both have headers)
    if !first.headers.is_empty() && !second.headers.is_empty() {
        let similarity = calculate_header_similarity(&first.headers, &second.headers);
        if similarity >= options.header_similarity_threshold {
            score += similarity;
            factors += 1;
        } else {
            // Headers are different - probably different tables
            score -= 0.5;
        }
    } else if first.headers.is_empty() && second.headers.is_empty() {
        // Both without headers - could be continuation
        score += 0.5;
        factors += 1;
    }

    // Factor 5: Detection mode compatibility
    if first.detection_mode == second.detection_mode {
        score += 0.5;
        factors += 1;
    }

    // Factor 6: Position heuristics
    // First table should end near bottom of page, second should start near top
    // (We don't have precise position data, so this is a placeholder)
    score += 0.3;
    factors += 1;

    if factors == 0 {
        return 0.0;
    }

    score / factors as f32
}

/// Calculate similarity between two header sets.
///
/// Returns a score from 0.0 (completely different) to 1.0 (identical).
fn calculate_header_similarity(h1: &[String], h2: &[String]) -> f32 {
    if h1.is_empty() || h2.is_empty() {
        return 0.0;
    }

    if h1.len() != h2.len() {
        return 0.0;
    }

    // Normalize headers for comparison
    let norm1: Vec<String> = h1
        .iter()
        .map(|s| s.to_lowercase().trim().to_string())
        .collect();

    let norm2: Vec<String> = h2
        .iter()
        .map(|s| s.to_lowercase().trim().to_string())
        .collect();

    // Count exact matches
    let exact_matches = norm1.iter().zip(&norm2).filter(|(a, b)| a == b).count();

    // Count partial matches (one contains the other)
    let partial_matches = norm1
        .iter()
        .zip(&norm2)
        .filter(|(a, b)| a != b && (a.contains(b.as_str()) || b.contains(a.as_str())))
        .count();

    let total = h1.len();
    (exact_matches as f32 + partial_matches as f32 * 0.5) / total as f32
}

/// Merge two tables into one.
fn merge_two_tables(mut first: ExtractedTable, second: ExtractedTable) -> ExtractedTable {
    // Update page range
    first.page_end = second.page_end;

    // Determine if second table's first row is a repeated header
    let skip_header = should_skip_header(&first, &second);

    // Get rows to add from second table
    let rows_to_add: Vec<TableRow> = if skip_header {
        second
            .rows
            .into_iter()
            .filter(|r| !r.is_header_row)
            .collect()
    } else {
        second.rows
    };

    // Renumber rows and append
    let offset = first.rows.len();
    for mut row in rows_to_add {
        row.row_index += offset;
        first.rows.push(row);
    }

    // Update counts
    first.n_rows = first.rows.iter().filter(|r| !r.is_header_row).count();

    // Merge warnings
    first.warnings.extend(second.warnings);
    first.warnings.push(format!(
        "Merged with table from page {} (detected as continuation)",
        second.page_start
    ));

    // Update extraction time
    first.extraction_ms += second.extraction_ms;

    // Adjust quality for merged tables
    first.quality = combined_quality(first.quality, second.quality);
    first.confidence_score = f32::midpoint(first.confidence_score, second.confidence_score) - 0.05;
    first.confidence_score = first.confidence_score.max(0.0);

    first
}

/// Determine if the second table's header row should be skipped.
fn should_skip_header(first: &ExtractedTable, second: &ExtractedTable) -> bool {
    // If second table doesn't have headers, nothing to skip
    if second.headers.is_empty() {
        return false;
    }

    // If first table doesn't have headers, keep second's headers
    if first.headers.is_empty() {
        return false;
    }

    // Check header similarity - if very similar, it's a repeated header
    let similarity = calculate_header_similarity(&first.headers, &second.headers);
    similarity >= 0.8
}

/// Combine quality ratings from two tables.
fn combined_quality(q1: TableQuality, q2: TableQuality) -> TableQuality {
    match (q1, q2) {
        (TableQuality::High, TableQuality::High) => TableQuality::High,
        (TableQuality::Low, _) | (_, TableQuality::Low) => TableQuality::Medium,
        _ => TableQuality::Medium,
    }
}

/// Find potential table continuation candidates.
///
/// This is a utility function that identifies tables that might
/// be continuations of other tables without actually merging them.
#[must_use]
pub fn find_continuation_candidates(
    tables: &[ExtractedTable],
    options: &TableExtractionOptions,
) -> Vec<(usize, usize, f32)> {
    let mut candidates = Vec::new();

    for (i, first) in tables.iter().enumerate() {
        for (j, second) in tables.iter().enumerate().skip(i + 1) {
            let score = calculate_merge_score(first, second, options);
            if score >= options.header_similarity_threshold * 0.8 {
                candidates.push((i, j, score));
            }
        }
    }

    // Sort by score descending
    candidates.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::types::TableCell;

    fn make_test_table(
        id: &str,
        source: &str,
        page_start: u32,
        page_end: u32,
        headers: Vec<&str>,
        n_rows: usize,
    ) -> ExtractedTable {
        let mut table = ExtractedTable::new(id, source);
        table.page_start = page_start;
        table.page_end = page_end;
        table.headers = headers.iter().map(|s| (*s).to_string()).collect();
        table.n_cols = headers.len();
        table.n_rows = n_rows;

        // Add header row
        let header_cells: Vec<TableCell> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| TableCell::new(*h, i).as_header())
            .collect();
        table
            .rows
            .push(TableRow::new(0, page_start, header_cells).as_header());

        // Add data rows
        for row_idx in 0..n_rows {
            let cells: Vec<TableCell> = (0..headers.len())
                .map(|col| TableCell::new(format!("r{}c{}", row_idx + 1, col), col))
                .collect();
            table
                .rows
                .push(TableRow::new(row_idx + 1, page_start, cells));
        }

        table
    }

    #[test]
    fn test_header_similarity_identical() {
        let h1 = vec![
            "Date".to_string(),
            "Amount".to_string(),
            "Description".to_string(),
        ];
        let h2 = vec![
            "Date".to_string(),
            "Amount".to_string(),
            "Description".to_string(),
        ];

        let similarity = calculate_header_similarity(&h1, &h2);
        assert!((similarity - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_header_similarity_case_insensitive() {
        let h1 = vec!["DATE".to_string(), "AMOUNT".to_string()];
        let h2 = vec!["date".to_string(), "amount".to_string()];

        let similarity = calculate_header_similarity(&h1, &h2);
        assert!((similarity - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_header_similarity_different() {
        let h1 = vec!["Date".to_string(), "Amount".to_string()];
        let h2 = vec!["Name".to_string(), "Address".to_string()];

        let similarity = calculate_header_similarity(&h1, &h2);
        assert!(similarity < 0.5);
    }

    #[test]
    fn test_merge_consecutive_tables() {
        let t1 = make_test_table("t1", "doc.pdf", 1, 1, vec!["A", "B", "C"], 5);
        let t2 = make_test_table("t2", "doc.pdf", 2, 2, vec!["A", "B", "C"], 3);

        let options = TableExtractionOptions::default();
        let merged = merge_multi_page_tables(vec![t1, t2], &options);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].page_start, 1);
        assert_eq!(merged[0].page_end, 2);
        // 5 + 3 rows (header row not counted)
        assert_eq!(merged[0].n_rows, 8);
    }

    #[test]
    fn test_no_merge_different_columns() {
        let t1 = make_test_table("t1", "doc.pdf", 1, 1, vec!["A", "B", "C"], 5);
        let t2 = make_test_table("t2", "doc.pdf", 2, 2, vec!["X", "Y"], 3);

        let options = TableExtractionOptions::default();
        let merged = merge_multi_page_tables(vec![t1, t2], &options);

        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_no_merge_non_consecutive_pages() {
        let t1 = make_test_table("t1", "doc.pdf", 1, 1, vec!["A", "B"], 5);
        let t2 = make_test_table("t2", "doc.pdf", 5, 5, vec!["A", "B"], 3);

        let options = TableExtractionOptions::default();
        let merged = merge_multi_page_tables(vec![t1, t2], &options);

        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_merge_disabled() {
        let t1 = make_test_table("t1", "doc.pdf", 1, 1, vec!["A", "B"], 5);
        let t2 = make_test_table("t2", "doc.pdf", 2, 2, vec!["A", "B"], 3);

        let options = TableExtractionOptions {
            merge_multi_page: false,
            ..Default::default()
        };
        let merged = merge_multi_page_tables(vec![t1, t2], &options);

        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_combined_quality() {
        assert_eq!(
            combined_quality(TableQuality::High, TableQuality::High),
            TableQuality::High
        );
        assert_eq!(
            combined_quality(TableQuality::High, TableQuality::Medium),
            TableQuality::Medium
        );
        assert_eq!(
            combined_quality(TableQuality::High, TableQuality::Low),
            TableQuality::Medium
        );
        assert_eq!(
            combined_quality(TableQuality::Low, TableQuality::Low),
            TableQuality::Medium
        );
    }
}
