//! Temporal Enrichment Module
//!
//! Implements the "Sliding Anchor" pipeline for resolving relative temporal
//! references during document ingestion. This module detects date anchors
//! in document text, tracks the current temporal context, and resolves
//! relative phrases like "last year" to absolute dates.
//!
//! # Architecture
//!
//! 1. **Anchor Detection**: Regex patterns detect explicit dates in text
//!    (e.g., "May 7, 2023", "2023-05-07", session headers)
//! 2. **Anchor Propagation**: State machine tracks current anchor through document
//! 3. **Phrase Detection**: Identifies relative temporal phrases ("last year", "next week")
//! 4. **Resolution**: Converts relative phrases to absolute dates using anchor
//! 5. **Context Injection**: Appends resolved context to chunk for embedding

#![cfg(feature = "temporal_enrich")]

use chrono::{Datelike, NaiveDate, NaiveDateTime};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Source of a temporal anchor
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnchorSource {
    /// From document/file metadata (creation date, etc.)
    DocumentMetadata,
    /// From explicit header (e.g., "Session 5 (May 7, 2023)")
    ExplicitHeader,
    /// From inline date in text (e.g., "On May 7, 2023, ...")
    InlineDate,
    /// Inherited from previous chunk/section
    Inherited,
}

/// A detected temporal anchor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalAnchorInfo {
    /// The resolved date
    pub date: NaiveDate,
    /// Source of this anchor
    pub source: AnchorSource,
    /// Confidence score (0.0-1.0)
    pub confidence: f32,
    /// Original text that produced this anchor
    pub original_text: String,
    /// Character offset in the original document
    pub char_offset: usize,
}

/// A detected relative temporal phrase
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelativePhrase {
    /// The phrase as found in text (e.g., "last year")
    pub phrase: String,
    /// Character offset in the chunk
    pub char_offset: usize,
    /// Length of the phrase
    pub length: usize,
    /// Resolved absolute value (if anchor available)
    pub resolved: Option<ResolvedTemporal>,
}

/// Resolved temporal value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolvedTemporal {
    /// Single date
    Date(NaiveDate),
    /// Date range (start, end)
    DateRange { start: NaiveDate, end: NaiveDate },
    /// Year only
    Year(i32),
    /// Month in a year
    Month { year: i32, month: u32 },
}

impl ResolvedTemporal {
    /// Format as a human-readable string
    #[must_use]
    pub fn to_display_string(&self) -> String {
        match self {
            Self::Date(d) => d.format("%B %d, %Y").to_string(),
            Self::DateRange { start, end } => {
                format!(
                    "{} to {}",
                    start.format("%B %d, %Y"),
                    end.format("%B %d, %Y")
                )
            }
            Self::Year(y) => y.to_string(),
            Self::Month { year, month } => {
                let month_name = match month {
                    1 => "January",
                    2 => "February",
                    3 => "March",
                    4 => "April",
                    5 => "May",
                    6 => "June",
                    7 => "July",
                    8 => "August",
                    9 => "September",
                    10 => "October",
                    11 => "November",
                    12 => "December",
                    _ => "Unknown",
                };
                format!("{month_name} {year}")
            }
        }
    }

    /// Format as ISO-8601 compatible string
    #[must_use]
    pub fn to_iso_string(&self) -> String {
        match self {
            Self::Date(d) => d.format("%Y-%m-%d").to_string(),
            Self::DateRange { start, end } => {
                format!("{}/{}", start.format("%Y-%m-%d"), end.format("%Y-%m-%d"))
            }
            Self::Year(y) => format!("{y}"),
            Self::Month { year, month } => format!("{year}-{month:02}"),
        }
    }
}

/// Result of temporal enrichment for a chunk
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TemporalEnrichment {
    /// Current anchor for this chunk
    pub anchor: Option<TemporalAnchorInfo>,
    /// Detected relative phrases with resolutions
    pub relative_phrases: Vec<RelativePhrase>,
    /// Generated context block to append to embedding text
    pub context_block: Option<String>,
}

/// Sliding anchor state machine for tracking temporal context
#[derive(Debug, Clone)]
pub struct TemporalAnchorTracker {
    /// Current anchor date
    current_anchor: Option<NaiveDate>,
    /// Source of current anchor
    anchor_source: Option<AnchorSource>,
    /// Confidence of current anchor
    anchor_confidence: f32,
    /// Original text of current anchor
    anchor_text: Option<String>,
}

impl Default for TemporalAnchorTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl TemporalAnchorTracker {
    /// Create a new tracker with no anchor
    #[must_use]
    pub fn new() -> Self {
        Self {
            current_anchor: None,
            anchor_source: None,
            anchor_confidence: 0.0,
            anchor_text: None,
        }
    }

    /// Create a tracker with an initial anchor from document metadata
    #[must_use]
    pub fn with_document_date(date: NaiveDate) -> Self {
        Self {
            current_anchor: Some(date),
            anchor_source: Some(AnchorSource::DocumentMetadata),
            anchor_confidence: 0.7,
            anchor_text: None,
        }
    }

    /// Get the current anchor date
    #[must_use]
    pub fn current_anchor(&self) -> Option<NaiveDate> {
        self.current_anchor
    }

    /// Process a line of text, updating anchor if a date is found
    pub fn process_line(&mut self, line: &str, char_offset: usize) -> Option<TemporalAnchorInfo> {
        // Try to detect a date anchor in this line
        if let Some((date, source, confidence, text)) = detect_anchor_in_line(line) {
            // Only update if new anchor has higher confidence or is more specific
            let should_update = self.current_anchor.is_none()
                || confidence > self.anchor_confidence
                || matches!(source, AnchorSource::ExplicitHeader);

            if should_update {
                self.current_anchor = Some(date);
                self.anchor_source = Some(source);
                self.anchor_confidence = confidence;
                self.anchor_text = Some(text.clone());

                return Some(TemporalAnchorInfo {
                    date,
                    source,
                    confidence,
                    original_text: text,
                    char_offset,
                });
            }
        }
        None
    }

    /// Get current anchor info
    #[must_use]
    pub fn anchor_info(&self) -> Option<TemporalAnchorInfo> {
        self.current_anchor.map(|date| TemporalAnchorInfo {
            date,
            source: self.anchor_source.unwrap_or(AnchorSource::Inherited),
            confidence: self.anchor_confidence,
            original_text: self.anchor_text.clone().unwrap_or_default(),
            char_offset: 0,
        })
    }
}

// Regex patterns for anchor detection
static SESSION_HEADER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)Session\s+\d+\s*\(([^)]+)\)").expect("valid regex"));

static DATE_HEADER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\[(?:SESSION_)?DATE:\s*([^\]]+)\]").expect("valid regex"));

static ISO_DATE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})").expect("valid regex"));

static LONG_DATE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})").expect("valid regex")
});

static SHORT_DATE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2}),?\s+(\d{4})",
    )
    .expect("valid regex")
});

static SLASH_DATE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(\d{1,2})/(\d{1,2})/(\d{2,4})").expect("valid regex"));

// Regex patterns for relative phrase detection
static RELATIVE_YEAR_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(last|this|next)\s+year\b").expect("valid regex"));

static RELATIVE_MONTH_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(last|this|next)\s+month\b").expect("valid regex"));

static RELATIVE_WEEK_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(last|this|next)\s+week\b").expect("valid regex"));

static AGO_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(\d+|a|one|two|three|four|five|six|seven|eight|nine|ten)\s+(days?|weeks?|months?|years?)\s+ago\b").expect("valid regex")
});

static IN_FUTURE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bin\s+(\d+|a|one|two|three|four|five|six|seven|eight|nine|ten)\s+(days?|weeks?|months?|years?)\b").expect("valid regex")
});

static RELATIVE_DAY_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(yesterday|today|tomorrow)\b").expect("valid regex"));

static RELATIVE_WEEKDAY_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)\b(last|this|next)\s+(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b",
    )
    .expect("valid regex")
});

/// Detect an anchor date in a line of text
fn detect_anchor_in_line(line: &str) -> Option<(NaiveDate, AnchorSource, f32, String)> {
    // Priority 1: Session headers (highest confidence)
    if let Some(caps) = SESSION_HEADER_PATTERN.captures(line) {
        if let Some(date_str) = caps.get(1) {
            if let Some(date) = parse_date_string(date_str.as_str()) {
                return Some((
                    date,
                    AnchorSource::ExplicitHeader,
                    0.95,
                    caps[0].to_string(),
                ));
            }
        }
    }

    // Priority 2: Date headers [DATE: ...]
    if let Some(caps) = DATE_HEADER_PATTERN.captures(line) {
        if let Some(date_str) = caps.get(1) {
            if let Some(date) = parse_date_string(date_str.as_str()) {
                return Some((
                    date,
                    AnchorSource::ExplicitHeader,
                    0.95,
                    caps[0].to_string(),
                ));
            }
        }
    }

    // Priority 3: ISO dates (2023-05-07)
    if let Some(caps) = ISO_DATE_PATTERN.captures(line) {
        let year: i32 = caps[1].parse().ok()?;
        let month: u32 = caps[2].parse().ok()?;
        let day: u32 = caps[3].parse().ok()?;
        if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some((date, AnchorSource::InlineDate, 0.9, caps[0].to_string()));
        }
    }

    // Priority 4: Long month names (May 7, 2023)
    if let Some(caps) = LONG_DATE_PATTERN.captures(line) {
        let month_str = &caps[1];
        let day: u32 = caps[2].parse().ok()?;
        let year: i32 = caps[3].parse().ok()?;
        let month = month_name_to_number(month_str)?;
        if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some((date, AnchorSource::InlineDate, 0.85, caps[0].to_string()));
        }
    }

    // Priority 5: Short month names (May 7, 2023)
    if let Some(caps) = SHORT_DATE_PATTERN.captures(line) {
        let month_str = &caps[1];
        let day: u32 = caps[2].parse().ok()?;
        let year: i32 = caps[3].parse().ok()?;
        let month = month_name_to_number(month_str)?;
        if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some((date, AnchorSource::InlineDate, 0.85, caps[0].to_string()));
        }
    }

    // Priority 6: Slash dates (MM/DD/YYYY) - lower confidence due to ambiguity
    if let Some(caps) = SLASH_DATE_PATTERN.captures(line) {
        let month: u32 = caps[1].parse().ok()?;
        let day: u32 = caps[2].parse().ok()?;
        let mut year: i32 = caps[3].parse().ok()?;
        // Handle 2-digit years
        if year < 100 {
            year += if year > 50 { 1900 } else { 2000 };
        }
        if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some((date, AnchorSource::InlineDate, 0.7, caps[0].to_string()));
        }
    }

    None
}

/// Parse a date string in various formats
fn parse_date_string(s: &str) -> Option<NaiveDate> {
    let s = s.trim();

    // Try ISO format first
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(date);
    }
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y/%m/%d") {
        return Some(date);
    }

    // Try datetime formats
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y/%m/%d (%a) %H:%M") {
        return Some(dt.date());
    }

    // Try long month format
    if let Some(caps) = LONG_DATE_PATTERN.captures(s) {
        let month_str = &caps[1];
        let day: u32 = caps[2].parse().ok()?;
        let year: i32 = caps[3].parse().ok()?;
        let month = month_name_to_number(month_str)?;
        return NaiveDate::from_ymd_opt(year, month, day);
    }

    // Try short month format
    if let Some(caps) = SHORT_DATE_PATTERN.captures(s) {
        let month_str = &caps[1];
        let day: u32 = caps[2].parse().ok()?;
        let year: i32 = caps[3].parse().ok()?;
        let month = month_name_to_number(month_str)?;
        return NaiveDate::from_ymd_opt(year, month, day);
    }

    None
}

/// Convert month name to number
fn month_name_to_number(name: &str) -> Option<u32> {
    match name.to_lowercase().as_str() {
        "january" | "jan" => Some(1),
        "february" | "feb" => Some(2),
        "march" | "mar" => Some(3),
        "april" | "apr" => Some(4),
        "may" => Some(5),
        "june" | "jun" => Some(6),
        "july" | "jul" => Some(7),
        "august" | "aug" => Some(8),
        "september" | "sep" | "sept" => Some(9),
        "october" | "oct" => Some(10),
        "november" | "nov" => Some(11),
        "december" | "dec" => Some(12),
        _ => None,
    }
}

/// Parse a number word to integer
fn parse_number_word(s: &str) -> Option<i32> {
    let s = s.to_lowercase();
    match s.as_str() {
        "a" | "one" => Some(1),
        "two" => Some(2),
        "three" => Some(3),
        "four" => Some(4),
        "five" => Some(5),
        "six" => Some(6),
        "seven" => Some(7),
        "eight" => Some(8),
        "nine" => Some(9),
        "ten" => Some(10),
        _ => s.parse().ok(),
    }
}

/// Detect relative temporal phrases in text
#[must_use]
pub fn detect_relative_phrases(text: &str) -> Vec<(String, usize, usize)> {
    let mut phrases = Vec::new();

    // Relative year patterns
    for caps in RELATIVE_YEAR_PATTERN.captures_iter(text) {
        let m = caps.get(0).expect("full match");
        phrases.push((m.as_str().to_string(), m.start(), m.len()));
    }

    // Relative month patterns
    for caps in RELATIVE_MONTH_PATTERN.captures_iter(text) {
        let m = caps.get(0).expect("full match");
        phrases.push((m.as_str().to_string(), m.start(), m.len()));
    }

    // Relative week patterns
    for caps in RELATIVE_WEEK_PATTERN.captures_iter(text) {
        let m = caps.get(0).expect("full match");
        phrases.push((m.as_str().to_string(), m.start(), m.len()));
    }

    // "N days/weeks/months/years ago" patterns
    for caps in AGO_PATTERN.captures_iter(text) {
        let m = caps.get(0).expect("full match");
        phrases.push((m.as_str().to_string(), m.start(), m.len()));
    }

    // "in N days/weeks/months/years" patterns
    for caps in IN_FUTURE_PATTERN.captures_iter(text) {
        let m = caps.get(0).expect("full match");
        phrases.push((m.as_str().to_string(), m.start(), m.len()));
    }

    // yesterday/today/tomorrow
    for caps in RELATIVE_DAY_PATTERN.captures_iter(text) {
        let m = caps.get(0).expect("full match");
        phrases.push((m.as_str().to_string(), m.start(), m.len()));
    }

    // last/next weekday
    for caps in RELATIVE_WEEKDAY_PATTERN.captures_iter(text) {
        let m = caps.get(0).expect("full match");
        phrases.push((m.as_str().to_string(), m.start(), m.len()));
    }

    // Sort by position
    phrases.sort_by_key(|(_, pos, _)| *pos);

    phrases
}

/// Resolve a relative phrase to an absolute date using an anchor
#[must_use]
pub fn resolve_relative_phrase(phrase: &str, anchor: NaiveDate) -> Option<ResolvedTemporal> {
    let lower = phrase.to_lowercase();

    // Relative year
    if lower.contains("last year") {
        return Some(ResolvedTemporal::Year(anchor.year() - 1));
    }
    if lower.contains("this year") {
        return Some(ResolvedTemporal::Year(anchor.year()));
    }
    if lower.contains("next year") {
        return Some(ResolvedTemporal::Year(anchor.year() + 1));
    }

    // Relative month
    if lower.contains("last month") {
        let (y, m) = if anchor.month() == 1 {
            (anchor.year() - 1, 12)
        } else {
            (anchor.year(), anchor.month() - 1)
        };
        return Some(ResolvedTemporal::Month { year: y, month: m });
    }
    if lower.contains("this month") {
        return Some(ResolvedTemporal::Month {
            year: anchor.year(),
            month: anchor.month(),
        });
    }
    if lower.contains("next month") {
        let (y, m) = if anchor.month() == 12 {
            (anchor.year() + 1, 1)
        } else {
            (anchor.year(), anchor.month() + 1)
        };
        return Some(ResolvedTemporal::Month { year: y, month: m });
    }

    // Relative week - return date range
    if lower.contains("last week") {
        let start =
            anchor - chrono::Duration::days(7 + anchor.weekday().num_days_from_monday() as i64);
        let end = start + chrono::Duration::days(6);
        return Some(ResolvedTemporal::DateRange { start, end });
    }
    if lower.contains("this week") {
        let start = anchor - chrono::Duration::days(anchor.weekday().num_days_from_monday() as i64);
        let end = start + chrono::Duration::days(6);
        return Some(ResolvedTemporal::DateRange { start, end });
    }
    if lower.contains("next week") {
        let start =
            anchor + chrono::Duration::days(7 - anchor.weekday().num_days_from_monday() as i64);
        let end = start + chrono::Duration::days(6);
        return Some(ResolvedTemporal::DateRange { start, end });
    }

    // yesterday/today/tomorrow
    if lower == "yesterday" {
        return Some(ResolvedTemporal::Date(anchor - chrono::Duration::days(1)));
    }
    if lower == "today" {
        return Some(ResolvedTemporal::Date(anchor));
    }
    if lower == "tomorrow" {
        return Some(ResolvedTemporal::Date(anchor + chrono::Duration::days(1)));
    }

    // N units ago
    if let Some(caps) = AGO_PATTERN.captures(&lower) {
        let count = parse_number_word(&caps[1])?;
        let unit = &caps[2];

        return match unit {
            u if u.starts_with("day") => Some(ResolvedTemporal::Date(
                anchor - chrono::Duration::days(count as i64),
            )),
            u if u.starts_with("week") => Some(ResolvedTemporal::Date(
                anchor - chrono::Duration::weeks(count as i64),
            )),
            u if u.starts_with("month") => {
                let total_months = anchor.year() * 12 + anchor.month() as i32 - count;
                let new_year = (total_months - 1) / 12;
                let new_month = ((total_months - 1) % 12 + 1) as u32;
                NaiveDate::from_ymd_opt(new_year, new_month, anchor.day().min(28))
                    .map(ResolvedTemporal::Date)
            }
            u if u.starts_with("year") => Some(ResolvedTemporal::Year(anchor.year() - count)),
            _ => None,
        };
    }

    // in N units
    if let Some(caps) = IN_FUTURE_PATTERN.captures(&lower) {
        let count = parse_number_word(&caps[1])?;
        let unit = &caps[2];

        return match unit {
            u if u.starts_with("day") => Some(ResolvedTemporal::Date(
                anchor + chrono::Duration::days(count as i64),
            )),
            u if u.starts_with("week") => Some(ResolvedTemporal::Date(
                anchor + chrono::Duration::weeks(count as i64),
            )),
            u if u.starts_with("month") => {
                let total_months = anchor.year() * 12 + anchor.month() as i32 + count;
                let new_year = (total_months - 1) / 12;
                let new_month = ((total_months - 1) % 12 + 1) as u32;
                NaiveDate::from_ymd_opt(new_year, new_month, anchor.day().min(28))
                    .map(ResolvedTemporal::Date)
            }
            u if u.starts_with("year") => Some(ResolvedTemporal::Year(anchor.year() + count)),
            _ => None,
        };
    }

    // Relative weekday (last Monday, next Friday, etc.)
    if let Some(caps) = RELATIVE_WEEKDAY_PATTERN.captures(&lower) {
        let direction = &caps[1];
        let weekday_name = &caps[2];

        let target_weekday = match weekday_name.to_lowercase().as_str() {
            "monday" => chrono::Weekday::Mon,
            "tuesday" => chrono::Weekday::Tue,
            "wednesday" => chrono::Weekday::Wed,
            "thursday" => chrono::Weekday::Thu,
            "friday" => chrono::Weekday::Fri,
            "saturday" => chrono::Weekday::Sat,
            "sunday" => chrono::Weekday::Sun,
            _ => return None,
        };

        let current_weekday = anchor.weekday();
        let days_diff = (target_weekday.num_days_from_monday() as i64)
            - (current_weekday.num_days_from_monday() as i64);

        let result_date = match direction.to_lowercase().as_str() {
            "last" => {
                let mut offset = days_diff;
                if offset >= 0 {
                    offset -= 7;
                }
                anchor + chrono::Duration::days(offset)
            }
            "this" => anchor + chrono::Duration::days(days_diff),
            "next" => {
                let mut offset = days_diff;
                if offset <= 0 {
                    offset += 7;
                }
                anchor + chrono::Duration::days(offset)
            }
            _ => return None,
        };

        return Some(ResolvedTemporal::Date(result_date));
    }

    None
}

/// Enrich a chunk of text with temporal context
///
/// This function:
/// 1. Detects any anchor dates in the text
/// 2. Finds all relative temporal phrases
/// 3. Resolves relative phrases using the anchor
/// 4. Generates a context block for embedding
#[must_use]
pub fn enrich_chunk(text: &str, tracker: &mut TemporalAnchorTracker) -> TemporalEnrichment {
    let mut result = TemporalEnrichment::default();

    // Process each line to detect anchors
    let mut char_offset = 0;
    for line in text.lines() {
        if let Some(anchor_info) = tracker.process_line(line, char_offset) {
            result.anchor = Some(anchor_info);
        }
        char_offset += line.len() + 1; // +1 for newline
    }

    // If no anchor in this chunk, inherit from tracker
    if result.anchor.is_none() {
        result.anchor = tracker.anchor_info();
    }

    // Detect relative phrases
    let phrases = detect_relative_phrases(text);

    // Resolve phrases if we have an anchor
    if let Some(ref anchor_info) = result.anchor {
        for (phrase, offset, len) in phrases {
            let resolved = resolve_relative_phrase(&phrase, anchor_info.date);
            result.relative_phrases.push(RelativePhrase {
                phrase,
                char_offset: offset,
                length: len,
                resolved,
            });
        }
    } else {
        // No anchor, can't resolve
        for (phrase, offset, len) in phrases {
            result.relative_phrases.push(RelativePhrase {
                phrase,
                char_offset: offset,
                length: len,
                resolved: None,
            });
        }
    }

    // Generate context block if we have resolutions
    let resolved_phrases: Vec<_> = result
        .relative_phrases
        .iter()
        .filter_map(|p| p.resolved.as_ref().map(|r| (p.phrase.clone(), r.clone())))
        .collect();

    if !resolved_phrases.is_empty() {
        let mut context_parts = Vec::new();

        if let Some(ref anchor) = result.anchor {
            context_parts.push(format!(
                "Document date context: {}",
                anchor.date.format("%B %d, %Y")
            ));
        }

        context_parts.push("Temporal references:".to_string());
        for (phrase, resolved) in &resolved_phrases {
            context_parts.push(format!(
                "- \"{}\" refers to {}",
                phrase,
                resolved.to_display_string()
            ));
        }

        result.context_block = Some(context_parts.join(" "));
    }

    result
}

/// Enrich a full document, returning enriched text with context blocks
#[must_use]
pub fn enrich_document(text: &str, document_date: Option<NaiveDate>) -> String {
    let mut tracker = match document_date {
        Some(date) => TemporalAnchorTracker::with_document_date(date),
        None => TemporalAnchorTracker::new(),
    };

    let enrichment = enrich_chunk(text, &mut tracker);

    // Append context block to text if we have temporal resolutions
    if let Some(context) = enrichment.context_block {
        format!("{text}\n\n[Temporal Context: {context}]")
    } else {
        text.to_string()
    }
}

/// Batch enrich multiple chunks, maintaining anchor state across chunks
pub fn enrich_chunks(
    chunks: &[String],
    document_date: Option<NaiveDate>,
) -> Vec<(String, TemporalEnrichment)> {
    let mut tracker = match document_date {
        Some(date) => TemporalAnchorTracker::with_document_date(date),
        None => TemporalAnchorTracker::new(),
    };

    chunks
        .iter()
        .map(|chunk| {
            let enrichment = enrich_chunk(chunk, &mut tracker);
            let enriched_text = if let Some(ref context) = enrichment.context_block {
                format!("{chunk}\n\n[Temporal Context: {context}]")
            } else {
                chunk.clone()
            };
            (enriched_text, enrichment)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anchor_detection_session_header() {
        let mut tracker = TemporalAnchorTracker::new();
        let line = "=== Session 5 (May 7, 2023) ===";
        let info = tracker.process_line(line, 0);
        assert!(info.is_some());
        let info = info.expect("anchor info");
        assert_eq!(
            info.date,
            NaiveDate::from_ymd_opt(2023, 5, 7).expect("valid date")
        );
        assert_eq!(info.source, AnchorSource::ExplicitHeader);
    }

    #[test]
    fn test_anchor_detection_iso_date() {
        let mut tracker = TemporalAnchorTracker::new();
        let line = "Event occurred on 2023-05-07 at noon.";
        let info = tracker.process_line(line, 0);
        assert!(info.is_some());
        let info = info.expect("anchor info");
        assert_eq!(
            info.date,
            NaiveDate::from_ymd_opt(2023, 5, 7).expect("valid date")
        );
    }

    #[test]
    fn test_relative_phrase_detection() {
        let text = "I did this last year. We'll meet next week. Two days ago was fun.";
        let phrases = detect_relative_phrases(text);
        assert_eq!(phrases.len(), 3);
        assert_eq!(phrases[0].0, "last year");
        assert_eq!(phrases[1].0, "next week");
        assert_eq!(phrases[2].0, "Two days ago");
    }

    #[test]
    fn test_resolve_last_year() {
        let anchor = NaiveDate::from_ymd_opt(2023, 5, 7).expect("valid date");
        let resolved = resolve_relative_phrase("last year", anchor);
        assert!(resolved.is_some());
        if let Some(ResolvedTemporal::Year(y)) = resolved {
            assert_eq!(y, 2022);
        } else {
            panic!("Expected Year resolution");
        }
    }

    #[test]
    fn test_resolve_last_week() {
        let anchor = NaiveDate::from_ymd_opt(2023, 5, 10).expect("valid date"); // Wednesday
        let resolved = resolve_relative_phrase("last week", anchor);
        assert!(resolved.is_some());
        if let Some(ResolvedTemporal::DateRange { start, end }) = resolved {
            assert_eq!(
                start,
                NaiveDate::from_ymd_opt(2023, 5, 1).expect("valid date")
            ); // Previous Monday
            assert_eq!(
                end,
                NaiveDate::from_ymd_opt(2023, 5, 7).expect("valid date")
            ); // Previous Sunday
        } else {
            panic!("Expected DateRange resolution");
        }
    }

    #[test]
    fn test_enrich_chunk() {
        let mut tracker = TemporalAnchorTracker::new();
        let text = "=== Session 1 (May 7, 2023) ===\n\nI painted a sunrise last year.";
        let enrichment = enrich_chunk(text, &mut tracker);

        assert!(enrichment.anchor.is_some());
        assert_eq!(enrichment.relative_phrases.len(), 1);
        assert!(enrichment.relative_phrases[0].resolved.is_some());
        assert!(enrichment.context_block.is_some());

        let context = enrichment.context_block.as_ref().expect("context");
        assert!(context.contains("2022"));
    }

    #[test]
    fn test_enrich_document() {
        let text = "Session 1 (May 7, 2023)\n\nMelanie: I painted a sunrise last year.";
        let enriched = enrich_document(text, None);
        assert!(enriched.contains("[Temporal Context:"));
        assert!(enriched.contains("2022"));
    }

    #[test]
    fn test_anchor_propagation() {
        let chunks = vec![
            "Session 1 (May 7, 2023)\n\nHello!".to_string(),
            "This happened last year.".to_string(), // Should inherit anchor
        ];
        let results = enrich_chunks(&chunks, None);

        // Second chunk should have resolution because anchor propagated
        assert!(results[1].1.anchor.is_some());
        assert!(!results[1].1.relative_phrases.is_empty());
        assert!(results[1].1.relative_phrases[0].resolved.is_some());
    }
}
