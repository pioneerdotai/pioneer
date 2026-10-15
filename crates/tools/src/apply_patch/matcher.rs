use crate::apply_patch::{Hunk, UpdateFile};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MatchResult {
    pub content: String,
    pub replacements: Vec<MatchedReplacement>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MatchedReplacement {
    pub hunk_line: usize,
    pub start_line: usize,
    pub removed_lines: usize,
    pub inserted_lines: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchErrorCode {
    ContextNotFound,
    AmbiguousContext,
    OverlappingHunks,
    InvalidUtf8,
    InvalidHunk,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MatchError {
    pub code: MatchErrorCode,
    pub hunk_line: usize,
    pub candidates: Vec<usize>,
    pub message: String,
}

impl MatchError {
    fn new(code: MatchErrorCode, hunk_line: usize, message: impl Into<String>) -> Self {
        Self {
            code,
            hunk_line,
            candidates: Vec::new(),
            message: message.into(),
        }
    }
}

impl fmt::Display for MatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "patch match error at hunk {}: {}",
            self.hunk_line, self.message
        )
    }
}

impl std::error::Error for MatchError {}

#[derive(Clone, Debug)]
struct LogicalLine {
    value: String,
    ending: String,
}

pub fn apply_update(source: &str, update: &UpdateFile) -> Result<MatchResult, MatchError> {
    apply_update_with_candidate_limit(source, update, 128)
}

/// Apply an Update against one exact text snapshot while bounding the number
/// of candidate locations retained for diagnostics. The matcher uses a
/// linear-time KMP scan per normalization level; it does not build an
/// unbounded vector of every matching offset in a large/repetitive file.
pub fn apply_update_with_candidate_limit(
    source: &str,
    update: &UpdateFile,
    max_candidate_matches: u32,
) -> Result<MatchResult, MatchError> {
    let (bom, mut lines) = split_lines(source)?;
    let preferred_ending = preferred_ending(&lines);
    let mut cursor = 0usize;
    let mut replacements: Vec<MatchedReplacement> = Vec::new();
    for hunk in &update.hunks {
        let old = hunk.old_lines();
        let new = hunk.new_lines();
        let start = find_hunk(&lines, hunk, &old, cursor, max_candidate_matches)?;
        let end = start + old.len();
        if end > lines.len() {
            return Err(MatchError::new(
                MatchErrorCode::ContextNotFound,
                hunk.header_line,
                "hunk extends beyond the current text",
            ));
        }
        if let Some(previous) = replacements.last()
            && start < previous.start_line + previous.removed_lines
        {
            return Err(MatchError::new(
                MatchErrorCode::OverlappingHunks,
                hunk.header_line,
                "hunks overlap",
            ));
        }
        // New logical lines use the file's deterministic dominant ending.
        // Existing unchanged lines retain their exact endings.  If a change
        // reaches an unterminated final line, keep the final-newline bit on
        // the replacement's last logical line instead of silently adding one.
        let preserve_unterminated_final_line = end == lines.len()
            && lines.last().is_some_and(|line| line.ending.is_empty())
            && !new.is_empty();
        if old.is_empty()
            && start == lines.len()
            && !lines.is_empty()
            && lines.last().is_some_and(|line| line.ending.is_empty())
        {
            // A logical last line without a terminator still needs a
            // separator before an insertion at EOF; otherwise the inserted
            // text would be concatenated directly onto that line.
            if let Some(last) = lines.last_mut() {
                last.ending = preferred_ending.clone();
            }
        }
        let inserted = new
            .into_iter()
            .map(|value| LogicalLine {
                value,
                ending: preferred_ending.clone(),
            })
            .collect::<Vec<_>>();
        let mut inserted = inserted;
        if preserve_unterminated_final_line {
            if let Some(last) = inserted.last_mut() {
                last.ending.clear();
            }
        }
        if old.is_empty() && lines.is_empty() {
            if let Some(last) = inserted.last_mut() {
                last.ending.clear();
            }
        }
        lines.splice(start..end, inserted);
        replacements.push(MatchedReplacement {
            hunk_line: hunk.header_line,
            start_line: start,
            removed_lines: old.len(),
            inserted_lines: hunk.new_lines().len(),
        });
        cursor = start + hunk.new_lines().len();
    }
    let mut content = String::new();
    if bom {
        content.push('\u{feff}');
    }
    for line in lines {
        content.push_str(&line.value);
        content.push_str(&line.ending);
    }
    Ok(MatchResult {
        content,
        replacements,
    })
}

fn find_hunk(
    lines: &[LogicalLine],
    hunk: &Hunk,
    old: &[String],
    cursor: usize,
    max_candidate_matches: u32,
) -> Result<usize, MatchError> {
    let mut start = cursor;
    if let Some(context) = &hunk.context {
        let contexts = find_candidates(
            lines,
            std::slice::from_ref(context),
            cursor,
            false,
            max_candidate_matches,
        );
        if contexts.len() != 1 {
            return Err(match_candidates(
                hunk.header_line,
                contexts,
                "context marker is missing or ambiguous",
            ));
        }
        start = contexts[0] + 1;
    }
    // An insertion-only hunk has no old lines to locate.  Its optional
    // context marker still identifies the insertion point; without a marker
    // the canonical default is the synthetic EOF position.  Do not route an
    // empty pattern through the candidate scanner, which would discard the
    // context-derived position and always return EOF.
    if old.is_empty() {
        return Ok(if hunk.context.is_some() {
            start.min(lines.len())
        } else {
            lines.len()
        });
    }
    let candidates = find_candidates(lines, old, start, hunk.end_of_file, max_candidate_matches);
    match candidates.as_slice() {
        [only] => Ok(*only),
        [] => Err(match_candidates(
            hunk.header_line,
            candidates,
            "expected lines not found",
        )),
        _ => Err(match_candidates(
            hunk.header_line,
            candidates,
            "expected lines are ambiguous",
        )),
    }
}

fn find_candidates(
    lines: &[LogicalLine],
    pattern: &[String],
    start: usize,
    eof: bool,
    max_candidate_matches: u32,
) -> Vec<usize> {
    if pattern.is_empty() {
        return vec![lines.len()];
    }
    let last_offset = lines.len().saturating_sub(pattern.len());
    let first_offset = if eof { last_offset } else { start };
    if pattern.len() > lines.len() || first_offset > last_offset {
        return Vec::new();
    }
    // Two locations are sufficient to prove ambiguity. Retain at most the
    // configured diagnostic bound, but never allow a value of one to turn a
    // genuinely ambiguous match into a false unique match.
    let candidate_limit = usize::try_from(max_candidate_matches)
        .unwrap_or(usize::MAX)
        .max(2);
    // Keep the matching ladder deliberately small and deterministic. The
    // first three levels are the only ones specified by the native contract:
    // exact (line endings are already removed from LogicalLine), trailing
    // whitespace normalization, and full edge-whitespace normalization.
    // Unicode punctuation normalization is intentionally not a fallback: it
    // has no vetted Pioneer reference corpus and could silently match text
    // the model did not identify.
    for mode in 0..3 {
        let mut candidates = Vec::new();
        let lps = longest_prefix_suffix(pattern, mode);
        let mut matched = 0usize;
        for offset in first_offset..lines.len() {
            while matched > 0 && !compare(&lines[offset].value, &pattern[matched], mode) {
                matched = lps[matched - 1];
            }
            if compare(&lines[offset].value, &pattern[matched], mode) {
                matched += 1;
            }
            if matched == pattern.len() {
                let candidate = offset + 1 - pattern.len();
                if candidate <= last_offset {
                    candidates.push(candidate);
                    if candidates.len() >= candidate_limit {
                        return candidates;
                    }
                }
                matched = lps[matched - 1];
            }
        }
        if !candidates.is_empty() {
            return candidates;
        }
    }
    Vec::new()
}

fn longest_prefix_suffix(pattern: &[String], mode: usize) -> Vec<usize> {
    let mut lps = vec![0; pattern.len()];
    let mut prefix_len = 0usize;
    let mut index = 1usize;
    while index < pattern.len() {
        if compare(&pattern[index], &pattern[prefix_len], mode) {
            prefix_len += 1;
            lps[index] = prefix_len;
            index += 1;
        } else if prefix_len > 0 {
            prefix_len = lps[prefix_len - 1];
        } else {
            index += 1;
        }
    }
    lps
}

fn compare(actual: &str, expected: &str, mode: usize) -> bool {
    match mode {
        0 => actual == expected,
        1 => {
            actual.trim_end_matches(char::is_whitespace)
                == expected.trim_end_matches(char::is_whitespace)
        }
        2 => actual.trim() == expected.trim(),
        _ => false,
    }
}

fn match_candidates(hunk_line: usize, candidates: Vec<usize>, message: &str) -> MatchError {
    let code = if candidates.is_empty() {
        MatchErrorCode::ContextNotFound
    } else {
        MatchErrorCode::AmbiguousContext
    };
    let mut error = MatchError::new(code, hunk_line, message);
    error.candidates = candidates.into_iter().take(128).collect();
    error
}

fn split_lines(source: &str) -> Result<(bool, Vec<LogicalLine>), MatchError> {
    let bom = source.starts_with('\u{feff}');
    let source = source.strip_prefix('\u{feff}').unwrap_or(source);
    if source.contains('\0') {
        return Err(MatchError::new(
            MatchErrorCode::InvalidUtf8,
            0,
            "text contains binary NUL",
        ));
    }
    let mut lines = Vec::new();
    let mut start = 0usize;
    let bytes = source.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let ending = match bytes[index] {
            b'\n' => "\n",
            b'\r' if bytes.get(index + 1) == Some(&b'\n') => "\r\n",
            b'\r' => "\r",
            _ => {
                index += 1;
                continue;
            }
        };
        let value_end = index;
        let ending_end = index + ending.len();
        lines.push(LogicalLine {
            value: source[start..value_end].to_owned(),
            ending: ending.to_owned(),
        });
        start = ending_end;
        index = ending_end;
    }
    if start < source.len() {
        lines.push(LogicalLine {
            value: source[start..].to_owned(),
            ending: String::new(),
        });
    }
    Ok((bom, lines))
}

fn preferred_ending(lines: &[LogicalLine]) -> String {
    // Match Codex's preserve-line-endings contract: the first existing line
    // ending is the preferred style for newly inserted lines.  Unchanged
    // lines still retain their own exact endings, so mixed files remain
    // deterministic without silently converting the file to the majority
    // style.
    lines
        .iter()
        .find_map(|line| (!line.ending.is_empty()).then_some(line.ending.clone()))
        .unwrap_or_else(|| "\n".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{PatchLimits, PatchRequest, PatchRequestSource};
    use crate::apply_patch::parse;

    fn update(text: &str) -> UpdateFile {
        let request = PatchRequest::from_provider_text(
            text,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        let document = parse(&request, PatchLimits::default()).unwrap();
        match &document.operations[0].body {
            crate::apply_patch::OperationBody::Update(update) => update.clone(),
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn unrelated_concurrent_lines_survive() {
        let patch =
            update("*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch");
        let result = apply_update("external\nold\nother\n", &patch).unwrap();
        assert_eq!(result.content, "external\nnew\nother\n");
    }

    #[test]
    fn duplicate_exact_context_is_ambiguous() {
        let patch =
            update("*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch");
        let error = apply_update("old\nold\n", &patch).unwrap_err();
        assert_eq!(error.code, MatchErrorCode::AmbiguousContext);
    }

    #[test]
    fn repeated_context_scan_keeps_bounded_diagnostics() {
        let patch =
            update("*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch");
        let source = "old\n".repeat(10_000);
        let error = apply_update_with_candidate_limit(&source, &patch, 2).unwrap_err();
        assert_eq!(error.code, MatchErrorCode::AmbiguousContext);
        assert_eq!(error.candidates.len(), 2);
    }

    #[test]
    fn whitespace_ladder_and_eof_are_deterministic() {
        let patch =
            update("*** Begin Patch\n*** Update File: file.txt\n@@\n-old  \n+new\n*** End Patch");
        let result = apply_update("old  \nlast", &patch).unwrap();
        assert_eq!(result.content, "new\nlast");
        let eof_patch = update(
            "*** Begin Patch\n*** Update File: file.txt\n@@\n-last\n+end\n*** End of File\n*** End Patch",
        );
        assert_eq!(
            apply_update("first\r\nlast\r\n", &eof_patch)
                .unwrap()
                .content,
            "first\r\nend\r\n"
        );
    }

    #[test]
    fn bom_and_crlf_are_preserved() {
        let patch =
            update("*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch");
        let result = apply_update("\u{feff}old\r\nkeep\r\n", &patch).unwrap();
        assert_eq!(result.content, "\u{feff}new\r\nkeep\r\n");
    }

    #[test]
    fn lone_cr_line_endings_are_preserved() {
        let patch =
            update("*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch");
        let result = apply_update("old\rkeep\r", &patch).unwrap();
        assert_eq!(result.content, "new\rkeep\r");
    }

    #[test]
    fn punctuation_near_match_is_rejected() {
        let patch = update(
            "*** Begin Patch\n*** Update File: file.txt\n@@\n-plain - text\n+changed\n*** End Patch",
        );
        let error = apply_update("plain — text\n", &patch).unwrap_err();
        assert_eq!(error.code, MatchErrorCode::ContextNotFound);
    }

    #[test]
    fn insertion_only_hunk_honors_context_marker() {
        let patch = update(
            "*** Begin Patch\n*** Update File: file.txt\n@@ middle\n+inserted\n*** End Patch",
        );
        let result = apply_update("first\nmiddle\nlast\n", &patch).unwrap();
        assert_eq!(result.content, "first\nmiddle\ninserted\nlast\n");
    }

    #[test]
    fn insertion_at_eof_separates_a_non_terminated_last_line() {
        let patch =
            update("*** Begin Patch\n*** Update File: file.txt\n@@\n+inserted\n*** End Patch");
        let result = apply_update("last", &patch).unwrap();
        assert_eq!(result.content, "last\ninserted");
    }

    #[test]
    fn inserted_lines_use_dominant_ending_in_mixed_source() {
        let patch = update(
            "*** Begin Patch\n*** Update File: file.txt\n@@\n-middle\n+changed\n*** End Patch",
        );
        let result = apply_update("first\nmiddle\r\nlast\r\n", &patch).unwrap();
        assert_eq!(result.content, "first\nchanged\nlast\r\n");
    }
}
