//! Audit report types for provenance tracking and compliance reporting.
//!
//! This module provides structured types for generating audit reports that trace
//! the sources used to answer questions, enabling compliance, verification, and
//! debugging of AI-generated responses.

use serde::{Deserialize, Serialize};

use super::ask::{AskMode, AskRetriever, AskStats};
use super::common::FrameId;

/// A source span representing a specific piece of evidence used in an answer.
///
/// This is a rich representation of a citation that includes all metadata
/// needed for audit trails, compliance reporting, and source verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpan {
    /// 1-based index in the citation list (matches [1], [2], etc. in answers).
    pub index: usize,

    /// Frame ID in the memory file.
    pub frame_id: FrameId,

    /// Document URI or path.
    pub uri: String,

    /// Document title (if available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// Byte range within the document [start, end).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_range: Option<(usize, usize)>,

    /// Relevance score (semantic similarity or BM25).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,

    /// Tags associated with the source document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,

    /// Labels associated with the source document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,

    /// Unix timestamp when the frame was added to memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_timestamp: Option<i64>,

    /// Content dates extracted from the document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_dates: Vec<String>,

    /// The actual text snippet used as context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

/// Options for generating an audit report.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuditOptions {
    /// Number of sources to retrieve (top-k).
    #[serde(default)]
    pub top_k: Option<usize>,

    /// Maximum characters per snippet.
    #[serde(default)]
    pub snippet_chars: Option<usize>,

    /// Retrieval mode (lex, sem, hybrid).
    #[serde(default)]
    pub mode: Option<AskMode>,

    /// Optional scope filter (URI prefix).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,

    /// Start timestamp filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<i64>,

    /// End timestamp filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<i64>,

    /// Include full text snippets in sources.
    #[serde(default)]
    pub include_snippets: bool,
}

/// A structured audit report for a question-answering session.
///
/// This report provides full provenance information for compliance,
/// debugging, and verification purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditReport {
    /// Version of the audit report format.
    pub version: String,

    /// Unix timestamp when the audit was generated.
    pub generated_at: i64,

    /// The question that was asked.
    pub question: String,

    /// The synthesized answer (if available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<String>,

    /// Retrieval mode used.
    pub mode: AskMode,

    /// Actual retriever used (may differ from mode if fallback occurred).
    pub retriever: AskRetriever,

    /// All source spans used to generate the answer.
    pub sources: Vec<SourceSpan>,

    /// Total number of documents searched.
    pub total_hits: usize,

    /// Performance statistics.
    pub stats: AskStats,

    /// Additional notes or warnings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

impl AuditReport {
    /// Format the report as human-readable text.
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut output = String::new();

        // Header
        output.push_str("══════════════════════════════════════════════════════════════════════\n");
        output.push_str("                        MEMVID AUDIT REPORT\n");
        output
            .push_str("══════════════════════════════════════════════════════════════════════\n\n");
        output.push_str(&format!(
            "Report Generated: {}\n",
            format_timestamp(self.generated_at)
        ));
        output.push_str(&format!("Report Version:   {}\n\n", self.version));

        // Question
        output.push_str("──────────────────────────────────────────────────────────────────────\n");
        output.push_str("QUESTION\n");
        output
            .push_str("──────────────────────────────────────────────────────────────────────\n\n");
        output.push_str(&self.question);
        output.push_str("\n\n");

        // Answer
        if let Some(answer) = &self.answer {
            output.push_str(
                "──────────────────────────────────────────────────────────────────────\n",
            );
            output.push_str("ANSWER\n");
            output.push_str(
                "──────────────────────────────────────────────────────────────────────\n\n",
            );
            output.push_str(answer);
            output.push_str("\n\n");
        }

        // Retrieval Info
        output.push_str("──────────────────────────────────────────────────────────────────────\n");
        output.push_str("RETRIEVAL DETAILS\n");
        output
            .push_str("──────────────────────────────────────────────────────────────────────\n\n");
        output.push_str(&format!("  Retrieval Mode:  {}\n", format_mode(self.mode)));
        output.push_str(&format!(
            "  Engine Used:     {}\n",
            format_retriever(self.retriever)
        ));
        output.push_str(&format!("  Sources Found:   {}\n", self.total_hits));
        output.push_str(&format!(
            "  Total Latency:   {} ms\n",
            self.stats.latency_ms
        ));
        output.push_str(&format!(
            "    - Retrieval:   {} ms\n",
            self.stats.retrieval_ms
        ));
        output.push_str(&format!(
            "    - Synthesis:   {} ms\n\n",
            self.stats.synthesis_ms
        ));

        // Sources
        output.push_str("──────────────────────────────────────────────────────────────────────\n");
        output.push_str(&format!("SOURCES ({})\n", self.sources.len()));
        output.push_str("──────────────────────────────────────────────────────────────────────\n");

        for source in &self.sources {
            output.push_str(&format!("\n[{}] {}\n", source.index, source.uri));
            output.push_str("    ─────────────────────────────────────────────────────────────\n");
            if let Some(title) = &source.title {
                output.push_str(&format!("    Title:       {title}\n"));
            }
            output.push_str(&format!("    Frame ID:    {}\n", source.frame_id));
            if let Some(score) = source.score {
                output.push_str(&format!("    Score:       {score:.4}\n"));
            }
            if let Some((start, end)) = source.chunk_range {
                output.push_str(&format!(
                    "    Byte Range:  {} - {} ({} bytes)\n",
                    start,
                    end,
                    end - start
                ));
            }
            if !source.tags.is_empty() {
                let tags_display: Vec<_> = source.tags.iter().take(5).cloned().collect();
                let tags_str = if source.tags.len() > 5 {
                    format!(
                        "{}, +{} more",
                        tags_display.join(", "),
                        source.tags.len() - 5
                    )
                } else {
                    tags_display.join(", ")
                };
                output.push_str(&format!("    Tags:        {tags_str}\n"));
            }
            if let Some(ts) = source.frame_timestamp {
                output.push_str(&format!("    Indexed:     {}\n", format_timestamp(ts)));
            }
            if !source.content_dates.is_empty() {
                let dates_display: Vec<_> = source.content_dates.iter().take(3).cloned().collect();
                output.push_str(&format!("    Content Era: {}\n", dates_display.join(", ")));
            }
            if let Some(snippet) = &source.snippet {
                output.push_str("\n    Excerpt:\n");
                // Format snippet with proper indentation and word wrap
                let formatted = format_snippet_block(snippet, 64, "    │ ");
                output.push_str(&formatted);
                output.push('\n');
            }
        }

        // Notes (only show important ones)
        let important_notes: Vec<_> = self
            .notes
            .iter()
            .filter(|n| !n.contains("fell back")) // Skip fallback notes
            .collect();

        if !important_notes.is_empty() {
            output.push_str(
                "\n──────────────────────────────────────────────────────────────────────\n",
            );
            output.push_str("NOTES\n");
            output.push_str(
                "──────────────────────────────────────────────────────────────────────\n\n",
            );
            for note in important_notes {
                output.push_str(&format!("  • {note}\n"));
            }
        }

        output
            .push_str("\n══════════════════════════════════════════════════════════════════════\n");
        output.push_str("                           END OF REPORT\n");
        output.push_str("══════════════════════════════════════════════════════════════════════\n");
        output
    }

    /// Format the report as Markdown.
    #[must_use]
    pub fn to_markdown(&self) -> String {
        let mut output = String::new();

        output.push_str("# Memvid Audit Report\n\n");
        output.push_str(&format!(
            "> **Generated:** {}  \n",
            format_timestamp(self.generated_at)
        ));
        output.push_str(&format!("> **Version:** {}\n\n", self.version));

        output.push_str("---\n\n");
        output.push_str("## Question\n\n");
        output.push_str(&format!("> {}\n\n", self.question));

        if let Some(answer) = &self.answer {
            output.push_str("## Answer\n\n");
            output.push_str(answer);
            output.push_str("\n\n");
        }

        output.push_str("---\n\n");
        output.push_str("## Retrieval Details\n\n");
        output.push_str("| Property | Value |\n");
        output.push_str("|:---------|:------|\n");
        output.push_str(&format!(
            "| **Retrieval Mode** | {} |\n",
            format_mode(self.mode)
        ));
        output.push_str(&format!(
            "| **Engine Used** | {} |\n",
            format_retriever(self.retriever)
        ));
        output.push_str(&format!("| **Sources Found** | {} |\n", self.total_hits));
        output.push_str(&format!(
            "| **Total Latency** | {} ms |\n",
            self.stats.latency_ms
        ));
        output.push_str(&format!(
            "| **Retrieval Time** | {} ms |\n",
            self.stats.retrieval_ms
        ));
        output.push_str(&format!(
            "| **Synthesis Time** | {} ms |\n",
            self.stats.synthesis_ms
        ));
        output.push_str("\n---\n\n");

        output.push_str(&format!("## Sources ({})\n\n", self.sources.len()));
        for source in &self.sources {
            output.push_str(&format!("### Source [{}]\n\n", source.index));
            output.push_str(&format!("**URI:** `{}`\n\n", source.uri));

            if let Some(title) = &source.title {
                output.push_str(&format!("**Title:** {title}\n\n"));
            }

            // Metadata table
            output.push_str("| Property | Value |\n");
            output.push_str("|:---------|:------|\n");
            output.push_str(&format!("| Frame ID | {} |\n", source.frame_id));
            if let Some(score) = source.score {
                output.push_str(&format!("| Relevance Score | {score:.4} |\n"));
            }
            if let Some((start, end)) = source.chunk_range {
                output.push_str(&format!(
                    "| Byte Range | `{} - {}` ({} bytes) |\n",
                    start,
                    end,
                    end - start
                ));
            }
            if let Some(ts) = source.frame_timestamp {
                output.push_str(&format!("| Indexed | {} |\n", format_timestamp(ts)));
            }
            output.push('\n');

            if !source.tags.is_empty() {
                let tags: Vec<_> = source
                    .tags
                    .iter()
                    .take(8)
                    .map(|t| format!("`{t}`"))
                    .collect();
                output.push_str(&format!("**Tags:** {}\n\n", tags.join(" ")));
            }

            if !source.content_dates.is_empty() {
                let dates: Vec<_> = source.content_dates.iter().take(5).cloned().collect();
                output.push_str(&format!("**Content Period:** {}\n\n", dates.join(", ")));
            }

            if let Some(snippet) = &source.snippet {
                output.push_str("**Excerpt:**\n\n");
                output.push_str("```text\n");
                output.push_str(&clean_snippet(snippet, 600));
                output.push_str("\n```\n\n");
            }

            output.push_str("---\n\n");
        }

        // Notes (only show important ones)
        let important_notes: Vec<_> = self
            .notes
            .iter()
            .filter(|n| !n.contains("fell back"))
            .collect();

        if !important_notes.is_empty() {
            output.push_str("## Notes\n\n");
            for note in important_notes {
                output.push_str(&format!("- {note}\n"));
            }
            output.push('\n');
        }

        output
    }
}

fn format_mode(mode: AskMode) -> &'static str {
    match mode {
        AskMode::Lex => "Lexical (keyword search)",
        AskMode::Sem => "Semantic (vector similarity)",
        AskMode::Hybrid => "Hybrid (lexical + semantic)",
    }
}

fn format_retriever(retriever: AskRetriever) -> &'static str {
    match retriever {
        AskRetriever::Lex => "Lexical Search",
        AskRetriever::Semantic => "Semantic Search",
        AskRetriever::Hybrid => "Hybrid Search",
        AskRetriever::LexFallback => "Lexical Search (semantic unavailable)",
        AskRetriever::TimelineFallback => "Timeline Fallback (no search results)",
    }
}

fn format_snippet_block(text: &str, width: usize, prefix: &str) -> String {
    let cleaned = text.trim().replace('\n', " ").replace("  ", " ");
    let mut result = String::new();
    let mut line = String::new();

    for word in cleaned.split_whitespace() {
        if line.len() + word.len() + 1 > width && !line.is_empty() {
            result.push_str(prefix);
            result.push_str(&line);
            result.push('\n');
            line.clear();
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }

    if !line.is_empty() {
        result.push_str(prefix);
        result.push_str(&line);
        result.push('\n');
    }

    // Limit to ~20 lines for comprehensive excerpts
    let lines: Vec<&str> = result.lines().take(20).collect();
    if result.lines().count() > 20 {
        format!("{}\n{}[...]\n", lines.join("\n"), prefix)
    } else {
        result
    }
}

fn clean_snippet(text: &str, max_len: usize) -> String {
    let cleaned = text.trim().replace('\n', " ").replace("  ", " ");
    if cleaned.len() <= max_len {
        cleaned
    } else {
        // Find word boundary
        let truncated = &cleaned[..max_len];
        if let Some(last_space) = truncated.rfind(' ') {
            format!("{}...", &truncated[..last_space])
        } else {
            format!("{truncated}...")
        }
    }
}

fn format_timestamp(ts: i64) -> String {
    // Basic ISO 8601 formatting without external dependencies
    use std::time::{Duration, UNIX_EPOCH};

    let datetime = UNIX_EPOCH + Duration::from_secs(ts.unsigned_abs());
    let secs = datetime
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Calculate date components (simplified)
    let days = secs / 86400;
    let remaining = secs % 86400;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;

    // Approximate year/month/day calculation
    let mut year = 1970i32;
    // Safe: days will fit in i32 for any reasonable usage (millions of years)
    #[allow(clippy::cast_possible_truncation)]
    let mut remaining_days = days as i32;

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let mut month = 1u32;
    let days_in_months = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    for days_in_month in days_in_months {
        if remaining_days < days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }

    let day = remaining_days + 1;

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_span_serialization() {
        let source = SourceSpan {
            index: 1,
            frame_id: 42,
            uri: "mv2://docs/readme.md".to_string(),
            title: Some("README".to_string()),
            chunk_range: Some((100, 500)),
            score: Some(0.95),
            tags: vec!["documentation".to_string()],
            labels: vec![],
            frame_timestamp: Some(1700000000),
            content_dates: vec![],
            snippet: Some("This is a test snippet.".to_string()),
        };

        let json = serde_json::to_string_pretty(&source).expect("serialize");
        assert!(json.contains("readme.md"));
        assert!(json.contains("0.95"));
    }

    #[test]
    fn test_audit_report_to_text() {
        let report = AuditReport {
            version: "1.0".to_string(),
            generated_at: 1700000000,
            question: "What is Memvid?".to_string(),
            answer: Some("Memvid is an AI memory system.".to_string()),
            mode: AskMode::Hybrid,
            retriever: AskRetriever::Hybrid,
            sources: vec![SourceSpan {
                index: 1,
                frame_id: 1,
                uri: "mv2://docs/intro.md".to_string(),
                title: Some("Introduction".to_string()),
                chunk_range: Some((0, 100)),
                score: Some(0.9),
                tags: vec![],
                labels: vec![],
                frame_timestamp: None,
                content_dates: vec![],
                snippet: Some("Memvid is...".to_string()),
            }],
            total_hits: 5,
            stats: AskStats {
                retrieval_ms: 10,
                synthesis_ms: 5,
                latency_ms: 15,
            },
            notes: vec![],
        };

        let text = report.to_text();
        assert!(text.contains("MEMVID AUDIT REPORT"));
        assert!(text.contains("What is Memvid?"));
        assert!(text.contains("Introduction"));
    }

    #[test]
    fn test_format_timestamp() {
        let ts = 1700000000; // 2023-11-14T22:13:20Z
        let formatted = format_timestamp(ts);
        assert!(formatted.starts_with("2023-11-14"));
    }
}
