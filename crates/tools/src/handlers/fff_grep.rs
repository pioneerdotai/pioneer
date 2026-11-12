//! In-process FFF search for `grep_files`. The caller authorizes the requested
//! path before it reaches this module; each picker indexes only that path.

use crate::ToolError;
use fff_search::{
    Casing, Constraint, FFFMode, FFFQuery, FilePicker, FilePickerOptions, FuzzyQuery,
    GitRecencyConfig, GrepMode, GrepSearchOptions,
};
use globset::{Glob, GlobMatcher};
use regex::bytes::RegexBuilder;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

const MAX_SEARCH_FILES: usize = 20_000;
const MAX_SEARCH_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CACHED_PICKERS: usize = 32;
const LINE_PREVIEW_CHARS: usize = 2_000;
const EXCLUDED_DIRS: &[&str] = &[
    ".git",
    "target",
    "node_modules",
    ".next",
    "dist",
    "build",
    ".cache",
    ".turbo",
    ".parcel-cache",
    ".venv",
    "__pycache__",
];

pub(super) struct SearchRequest {
    pub path: PathBuf,
    pub workdir: PathBuf,
    pub turn_id: String,
    pub pattern: String,
    pub glob: Option<String>,
    pub case_sensitive: bool,
    pub max_results: usize,
    pub max_output_bytes: usize,
    pub timeout_ms: u64,
    pub broad_file_limit: usize,
    pub cancellation: CancellationToken,
}

pub(super) enum SearchOutcome {
    Complete {
        output: String,
        match_count: usize,
        truncated: bool,
        skipped_large_files: usize,
    },
    NeedsNarrowing {
        reason: &'static str,
        message: &'static str,
        scanned_file_count: Option<usize>,
    },
}

#[derive(Clone, PartialEq, Eq)]
struct PickerKey {
    turn_id: String,
    path: PathBuf,
    single_file: bool,
}

type PickerCell = Arc<OnceLock<Result<Arc<FilePicker>, String>>>;
static PICKERS: OnceLock<Mutex<VecDeque<(PickerKey, PickerCell)>>> = OnceLock::new();

pub(super) async fn search(
    request: SearchRequest,
    filesystem_slot: OwnedSemaphorePermit,
) -> Result<SearchOutcome, ToolError> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    let cancellation = request.cancellation.clone();
    let timeout_ms = request.timeout_ms;
    let mut worker = pioneer_observability::work::spawn_blocking(move || {
        let _filesystem_slot = filesystem_slot;
        search_blocking(request, &worker_cancelled)
    });
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            cancelled.store(true, Ordering::Release);
            Err(ToolError::cancelled("grep_files was cancelled"))
        }
        result = &mut worker => {
            result.map_err(|error| ToolError::execution_failed(format!("grep_files worker failed: {error}")))?
        }
        _ = sleep(Duration::from_millis(timeout_ms)) => {
            cancelled.store(true, Ordering::Release);
            Ok(SearchOutcome::NeedsNarrowing {
                reason: "timeout",
                message: "grep_files timed out. Narrow path or glob.",
                scanned_file_count: None,
            })
        }
    }
}

fn search_blocking(
    request: SearchRequest,
    cancelled: &Arc<AtomicBool>,
) -> Result<SearchOutcome, ToolError> {
    if request.pattern.is_empty() {
        return Err(ToolError::invalid_arguments(
            "grep pattern must not be empty",
        ));
    }
    // FFF falls back to literal text for an invalid expression. Keep the
    // tool's regex contract by rejecting it before searching.
    let regex_pattern = request.pattern.replace("\\n", "\n");
    RegexBuilder::new(regex_pattern.as_str())
        .case_insensitive(!request.case_sensitive)
        .multi_line(true)
        .unicode(false)
        .build()
        .map_err(|error| ToolError::invalid_arguments(format!("invalid grep pattern: {error}")))?;
    let glob = request
        .glob
        .as_deref()
        .map(|value| {
            Glob::new(value)
                .map(|pattern| pattern.compile_matcher())
                .map_err(|error| {
                    ToolError::invalid_arguments(format!("invalid grep glob: {error}"))
                })
        })
        .transpose()?;

    let metadata = std::fs::metadata(request.path.as_path()).map_err(|error| {
        ToolError::execution_failed(format!(
            "grep_files could not inspect {}: {error}",
            request.path.display()
        ))
    })?;
    if !metadata.is_dir() && !metadata.is_file() {
        return Err(ToolError::invalid_arguments(
            "grep_files path must be a directory or regular file",
        ));
    }
    let single_file = metadata.is_file();
    let picker = cached_picker(&request, single_file)?;
    if cancelled.load(Ordering::Acquire) {
        return Err(ToolError::cancelled("grep_files was cancelled"));
    }

    let limit = if request.glob.is_none() && request.path == request.workdir {
        request.broad_file_limit
    } else {
        MAX_SEARCH_FILES
    };
    let file_count = picker
        .get_files()
        .iter()
        .filter(|file| {
            let relative = file.relative_path(picker.as_ref());
            !excluded_path(Path::new(relative.as_str()))
                && glob
                    .as_ref()
                    .is_none_or(|matcher| matcher.is_match(&relative))
        })
        .count();
    if file_count > limit {
        return Ok(SearchOutcome::NeedsNarrowing {
            reason: "broad_workspace_search",
            message: "grep_files is too broad for this workspace. Narrow path or glob.",
            scanned_file_count: Some(file_count),
        });
    }

    search_fff(&request, &picker, glob.as_ref(), cancelled)
}

fn cached_picker(request: &SearchRequest, single_file: bool) -> Result<Arc<FilePicker>, ToolError> {
    let key = PickerKey {
        turn_id: request.turn_id.clone(),
        path: request.path.clone(),
        single_file,
    };
    let cache = PICKERS.get_or_init(|| Mutex::new(VecDeque::new()));
    let cell = {
        let mut entries = cache
            .lock()
            .map_err(|_| ToolError::internal("grep_files index cache lock is poisoned"))?;
        if let Some(index) = entries.iter().position(|(existing, _)| existing == &key) {
            let entry = entries.remove(index).expect("existing cache entry");
            let cell = Arc::clone(&entry.1);
            entries.push_back(entry);
            cell
        } else {
            let cell = Arc::new(OnceLock::new());
            entries.push_back((key, Arc::clone(&cell)));
            if entries.len() > MAX_CACHED_PICKERS {
                entries.pop_front();
            }
            cell
        }
    };
    cell.get_or_init(|| build_picker(request.path.as_path(), single_file))
        .as_ref()
        .map(Arc::clone)
        .map_err(|error| ToolError::execution_failed(error.clone()))
}

fn build_picker(path: &Path, single_file: bool) -> Result<Arc<FilePicker>, String> {
    let base_path = if single_file {
        path.parent()
            .ok_or_else(|| format!("grep_files could not find parent for {}", path.display()))?
    } else {
        path
    };
    let mut picker = FilePicker::new(FilePickerOptions {
        base_path: base_path.to_string_lossy().into_owned(),
        mode: FFFMode::Ai,
        watch: false,
        follow_symlinks: false,
        enable_fs_root_scanning: true,
        enable_home_dir_scanning: true,
        git_recency: GitRecencyConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    })
    .map_err(|error| format!("fff-search could not open {}: {error}", base_path.display()))?;
    if single_file {
        // add_new_file indexes only the authorized file. collect_files here
        // would scan its parent and include unauthorized siblings.
        picker
            .add_new_file(path)
            .ok_or_else(|| format!("fff-search could not index file {}", path.display()))?;
    } else {
        picker
            .collect_files()
            .map_err(|error| format!("fff-search could not scan {}: {error}", path.display()))?;
    }
    Ok(Arc::new(picker))
}

fn search_fff(
    request: &SearchRequest,
    picker: &FilePicker,
    glob: Option<&GlobMatcher>,
    cancelled: &Arc<AtomicBool>,
) -> Result<SearchOutcome, ToolError> {
    let constraints = request
        .glob
        .as_deref()
        .map(|pattern| vec![Constraint::Glob(pattern)])
        .unwrap_or_default();
    let query = FFFQuery {
        raw_query: request.pattern.as_str(),
        constraints,
        fuzzy_query: FuzzyQuery::Text(request.pattern.as_str()),
        location: None,
    };
    let result = picker.grep(
        &query,
        &GrepSearchOptions {
            max_file_size: MAX_SEARCH_FILE_BYTES,
            max_matches_per_file: request.max_results.saturating_add(1),
            smart_case: false,
            casing: Some(if request.case_sensitive {
                Casing::Sensitive
            } else {
                Casing::Insensitive
            }),
            page_limit: request.max_results.saturating_add(1),
            mode: GrepMode::Regex,
            abort_signal: Some(Arc::clone(cancelled)),
            ..Default::default()
        },
    );
    if cancelled.load(Ordering::Acquire) {
        return Err(ToolError::cancelled("grep_files was cancelled"));
    }
    if let Some(error) = result.regex_fallback_error {
        return Err(ToolError::invalid_arguments(format!(
            "fff-search could not use grep pattern: {error}"
        )));
    }
    // FFF can retry a constrained query as unconstrained literal text.
    // Such results do not satisfy grep_files' explicit regex/glob request.
    let matches = if result.literal_fallback {
        &[][..]
    } else {
        result.matches.as_slice()
    };
    let regex = RegexBuilder::new(&request.pattern.replace("\\n", "\n"))
        .case_insensitive(!request.case_sensitive)
        .multi_line(true)
        .unicode(false)
        .build()
        .map_err(|error| ToolError::invalid_arguments(error.to_string()))?;
    let (needs_region, needs_origin) = regex_source_needs(regex.as_str(), request.case_sensitive)?;
    // Match FFF's strategy switch, not whether the regex alphabet includes LF:
    // replace_newline_escapes selects MultiLine only for an odd slash run + n.
    let mut slashes = 0usize;
    let backend_multiline = request.pattern.bytes().any(|byte| {
        let newline_escape = byte == b'n' && slashes % 2 == 1;
        slashes = if byte == b'\\' { slashes + 1 } else { 0 };
        newline_escape
    });
    let mut output = BoundedOutput::new(request.max_results, request.max_output_bytes);
    // FFF only falls back after the constrained search found no matches.
    // Discarding the unconstrained retry does not truncate that original search.
    for matched in matches {
        if cancelled.load(Ordering::Acquire) {
            return Err(ToolError::cancelled("grep_files was cancelled"));
        }
        let Some(file) = result.files.get(matched.file_index) else {
            return Err(ToolError::execution_failed(
                "fff-search returned an invalid file index",
            ));
        };
        let relative = file.relative_path(picker);
        if excluded_path(Path::new(relative.as_str()))
            || glob.is_some_and(|matcher| !matcher.is_match(&relative))
        {
            continue;
        }
        let path = file.absolute_path(picker, picker.base_path());
        // Assertions need the original search slices, not a new input at the
        // returned row. Other expressions retain the one-line / LF-region paths.
        let source_start = if needs_origin { 0 } else { matched.byte_offset };
        let (backing, complete) = if needs_region || needs_origin {
            (read_source_region(path.as_path(), source_start)?, true)
        } else {
            read_source_line(path.as_path(), source_start, MAX_SEARCH_FILE_BYTES as usize)?
        };
        let row_offset = usize::try_from(matched.byte_offset - source_start)
            .map_err(|_| ToolError::execution_failed("grep source offset overflow"))?;
        let Some(row_source) = backing.get(row_offset..) else {
            output.truncated = true;
            continue;
        };
        // Context for assertions is private. A non-LF-consuming match still
        // displays only its own line, even when verification read the full file.
        let source = if needs_region {
            row_source
        } else {
            row_source.split_inclusive('\n').next().unwrap_or("")
        };
        let found = if needs_origin {
            backend_span_at_row(
                &regex,
                backing.as_bytes(),
                row_offset,
                backend_multiline,
                cancelled,
            )?
            .map(|span| span.start - row_offset..span.end - row_offset)
        } else {
            regex.find(source.as_bytes()).map(|found| found.range())
        };
        let first_line_end = source.find('\n').unwrap_or(source.len());
        let Some(found) = found else {
            output.truncated = true;
            continue;
        };
        if found.start > first_line_end {
            output.truncated = true;
            continue;
        }
        // Display may omit only terminators outside the verified span. If a
        // match includes them, retain the exact bytes and label the source
        // preview explicitly instead of silently erasing part of the match.
        let trimmed = source.trim_end_matches(['\r', '\n']);
        let matched_terminator = found.end > trimmed.len();
        let display = if matched_terminator { source } else { trimmed };
        let Some((preview, start)) = line_preview(display, found.start, found.end) else {
            output.truncated = true;
            continue;
        };
        let shortened = start != 0 || preview.len() != display.len() || !complete;
        let row = if shortened || matched_terminator {
            let match_notice = if found.end > start + preview.len() {
                "; match continues beyond preview"
            } else {
                ""
            };
            let notice = if shortened {
                "line preview shortened"
            } else {
                "source preview includes line terminator"
            };
            format!(
                "{preview} [{notice}{match_notice}; read_file start_byte={} (match byte={}) or start_line={}]",
                matched.byte_offset + start as u64,
                matched.byte_offset + found.start as u64,
                matched.line_number
            )
        } else {
            preview.to_owned()
        };
        if !output.push(path.as_path(), matched.line_number, &row) {
            break;
        }
    }
    // A literal fallback is attempted only after the constrained search reached
    // EOF without matches. Its pagination does not describe the requested glob.
    if !result.literal_fallback
        && (matches.len() > request.max_results || result.next_file_offset != 0)
    {
        output.truncated = true;
    }
    let skipped_large_files = picker
        .get_files()
        .iter()
        .filter(|file| file.size > MAX_SEARCH_FILE_BYTES)
        .filter(|file| {
            let relative = file.relative_path(picker);
            !excluded_path(Path::new(relative.as_str()))
                && glob.is_none_or(|matcher| matcher.is_match(&relative))
        })
        .count();
    Ok(output.finish(skipped_large_files))
}

/// Conservative context analysis using the engine's HIR parser. LF-consuming
/// atoms need a region; any assertion needs the original input/search origins.
fn regex_source_needs(pattern: &str, case_sensitive: bool) -> Result<(bool, bool), ToolError> {
    use regex_syntax::hir::{Class, HirKind};
    let hir = regex_syntax::ParserBuilder::new()
        .utf8(false)
        .unicode(false)
        .case_insensitive(!case_sensitive)
        .multi_line(true)
        .build()
        .parse(pattern)
        .map_err(|error| ToolError::invalid_arguments(error.to_string()))?;
    let mut needs_region = false;
    let mut needs_origin = false;
    let mut pending = vec![&hir];
    while let Some(node) = pending.pop() {
        let contains_lf = match node.kind() {
            HirKind::Literal(literal) => literal.0.contains(&b'\n'),
            HirKind::Class(Class::Bytes(class)) => class
                .iter()
                .any(|range| range.start() <= b'\n' && b'\n' <= range.end()),
            HirKind::Class(Class::Unicode(class)) => class
                .iter()
                .any(|range| range.start() <= '\n' && '\n' <= range.end()),
            _ => false,
        };
        needs_region |= contains_lf;
        needs_origin |= matches!(node.kind(), HirKind::Look(_));
        pending.extend(node.kind().subs());
    }
    Ok((needs_region, needs_origin))
}

/// Recover a row's first span with FFF's actual search-slice boundaries.
/// SliceByLine advances to the row end; MultiLine advances to the span end
/// (one byte for an empty span). The regex engine still evaluates every match.
fn backend_span_at_row(
    regex: &regex::bytes::Regex,
    bytes: &[u8],
    target_row: usize,
    multiline: bool,
    cancelled: &AtomicBool,
) -> Result<Option<std::ops::Range<usize>>, ToolError> {
    let mut pos = 0;
    while pos < bytes.len() {
        if cancelled.load(Ordering::Acquire) {
            return Err(ToolError::cancelled("grep_files was cancelled"));
        }
        let Some(found) = regex.find(&bytes[pos..]) else {
            break;
        };
        let start = pos + found.start();
        let end = pos + found.end();
        let row = bytes[..start]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |lf| lf + 1);
        if row == bytes.len() || row > target_row {
            break;
        }
        if row == target_row {
            return Ok(Some(start..end));
        }
        pos = if multiline {
            end + usize::from(start == end)
        } else {
            bytes[start..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |lf| start + lf + 1)
        };
    }
    Ok(None)
}

fn read_source_line(path: &Path, offset: u64, limit: usize) -> Result<(String, bool), ToolError> {
    let mut file = File::open(path).map_err(|error| {
        ToolError::execution_failed(format!(
            "grep_files could not reopen {}: {error}",
            path.display()
        ))
    })?;
    file.seek(SeekFrom::Start(offset)).map_err(|error| {
        ToolError::execution_failed(format!(
            "grep_files could not seek {}: {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    BufReader::new(file.take(limit as u64 + 1))
        .read_until(b'\n', &mut bytes)
        .map_err(|error| {
            ToolError::execution_failed(format!(
                "grep_files could not read {}: {error}",
                path.display()
            ))
        })?;
    let complete = bytes.len() <= limit;
    let end = bytes.len().min(limit);
    let line = std::str::from_utf8(&bytes[..end])
        .map_err(|_| ToolError::execution_failed("grep source line is not valid UTF-8"))?;
    Ok((line.to_owned(), complete))
}

fn read_source_region(path: &Path, offset: u64) -> Result<String, ToolError> {
    let mut file =
        File::open(path).map_err(|error| ToolError::execution_failed(error.to_string()))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| ToolError::execution_failed(error.to_string()))?;
    let mut bytes = Vec::new();
    file.take(MAX_SEARCH_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| ToolError::execution_failed(error.to_string()))?;
    if bytes.len() as u64 > MAX_SEARCH_FILE_BYTES {
        return Err(ToolError::execution_failed(
            "grep source grew beyond the hard source bound",
        ));
    }
    String::from_utf8(bytes)
        .map_err(|_| ToolError::execution_failed("grep source region is not valid UTF-8"))
}

/// Scalar-count ceiling (not bytes/graphemes), with real source boundaries.
/// A longer match is explicitly marked as continuing by the caller.
fn line_preview(line: &str, match_start: usize, match_end: usize) -> Option<(&str, usize)> {
    // The backend deliberately uses byte regex semantics. A byte match may
    // start/end within a UTF-8 scalar: expand the preview, not the match offset.
    let mut match_start = match_start;
    let mut match_end = match_end;
    while !line.is_char_boundary(match_start) {
        match_start = match_start.checked_sub(1)?;
    }
    while !line.is_char_boundary(match_end) {
        match_end = match_end.checked_add(1)?;
        if match_end > line.len() {
            return None;
        }
    }
    let match_chars = line[match_start..match_end].chars().count();
    if match_chars > LINE_PREVIEW_CHARS {
        let end = line[match_start..]
            .char_indices()
            .nth(LINE_PREVIEW_CHARS)
            .map_or(line.len(), |(offset, _)| match_start + offset);
        return Some((&line[match_start..end], match_start));
    }
    let before = (LINE_PREVIEW_CHARS - match_chars) / 2;
    let start = if before == 0 {
        match_start
    } else {
        line[..match_start]
            .char_indices()
            .rev()
            .nth(before - 1)
            .map_or(0, |(offset, _)| offset)
    };
    let end = line[start..]
        .char_indices()
        .nth(LINE_PREVIEW_CHARS)
        .map_or(line.len(), |(offset, _)| start + offset);
    Some((&line[start..end], start))
}

fn excluded_path(path: &Path) -> bool {
    path.components().any(|component| {
        matches!(component, Component::Normal(name) if EXCLUDED_DIRS.iter().any(|excluded| name == *excluded))
    })
}

struct BoundedOutput {
    text: String,
    matches: usize,
    max_results: usize,
    max_bytes: usize,
    truncated: bool,
}

impl BoundedOutput {
    fn new(max_results: usize, max_bytes: usize) -> Self {
        Self {
            text: String::new(),
            matches: 0,
            max_results,
            max_bytes,
            truncated: false,
        }
    }

    fn push(&mut self, path: &Path, line_number: u64, line: &str) -> bool {
        if self.matches >= self.max_results {
            self.truncated = true;
            return false;
        }
        let row = format!("{}:{line_number}:{line}\n", path.display());
        if self.text.len().saturating_add(row.len()) > self.max_bytes {
            self.truncated = true;
            return false;
        }
        self.text.push_str(row.as_str());
        self.matches += 1;
        true
    }

    fn finish(self, skipped_large_files: usize) -> SearchOutcome {
        SearchOutcome::Complete {
            output: self.text,
            match_count: self.matches,
            truncated: self.truncated,
            skipped_large_files,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_match_has_an_explicit_fragment_and_a_read_address() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("long-match.txt");
        std::fs::write(&path, "x".repeat(4000)).unwrap();
        let request = SearchRequest {
            path,
            workdir: root.path().to_path_buf(),
            turn_id: "long-match-preview".into(),
            pattern: "x+".into(),
            glob: None,
            case_sensitive: true,
            max_results: 10,
            max_output_bytes: 4096,
            timeout_ms: 1000,
            broad_file_limit: 5000,
            cancellation: CancellationToken::new(),
        };
        match search_blocking(request, &Arc::new(AtomicBool::new(false))).unwrap() {
            SearchOutcome::Complete {
                output,
                match_count,
                truncated,
                ..
            } => {
                assert!(output.contains(&"x".repeat(2000)));
                assert!(output.len() < 3000);
                assert!(output.contains("match continues beyond preview"));
                assert!(output.contains("read_file start_byte=0"));
                assert_eq!(match_count, 1);
                assert!(
                    !truncated,
                    "fragmented match text does not omit a match row"
                );
            }
            _ => panic!("expected long match"),
        }
    }

    #[test]
    fn byte_regex_preview_expands_to_the_enclosing_utf8_scalar() {
        let line = "éα";
        let regex = RegexBuilder::new(r"(?-u:\xB1)")
            .unicode(false)
            .build()
            .unwrap();
        let found = regex.find(line.as_bytes()).unwrap();
        assert_eq!(found.start(), 3);
        let (preview, start) = line_preview(line, found.start(), found.end()).unwrap();
        assert_eq!(preview, "éα");
        assert_eq!(start, 0);
    }

    #[test]
    fn multiline_alternative_preview_uses_the_original_match_span() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("multi.txt");
        std::fs::write(&path, "foo\nbar\n").unwrap();
        for pattern in [
            r"foo\nbar|foo",
            r"foo\sbar",
            r"foo\sbar|foo",
            r"foo[\s\S]bar|foo",
            r"foo\x0Abar|foo",
            r"(?s:foo.bar)|foo",
            r"\Afoo\nbar|foo",
            r"foo\nbar\b|foo",
            r"^foo\nbar$|foo",
        ] {
            let request = SearchRequest {
                path: path.clone(),
                workdir: root.path().to_path_buf(),
                turn_id: "multiline-preview".into(),
                pattern: pattern.into(),
                glob: None,
                case_sensitive: true,
                max_results: 10,
                max_output_bytes: 4096,
                timeout_ms: 1000,
                broad_file_limit: 5000,
                cancellation: CancellationToken::new(),
            };
            match search_blocking(request, &Arc::new(AtomicBool::new(false))).unwrap() {
                SearchOutcome::Complete {
                    output,
                    match_count,
                    truncated,
                    ..
                } => {
                    assert_eq!(
                        output,
                        format!("{}:1:foo\nbar\n", path.display()),
                        "{pattern}"
                    );
                    assert_eq!(match_count, 1);
                    assert!(!truncated);
                }
                _ => panic!("expected verified multi-line match"),
            }
        }
    }

    #[test]
    fn reuses_picker_in_one_turn_and_scopes_it_to_one_file() {
        let root = tempfile::tempdir().expect("search root");
        let selected = root.path().join("selected.rs");
        std::fs::write(&selected, "needle\n").expect("selected file");
        std::fs::write(root.path().join("sibling.rs"), "other\n").expect("sibling file");
        let request = SearchRequest {
            path: selected.clone(),
            workdir: root.path().to_path_buf(),
            turn_id: "picker_cache_test_turn".to_owned(),
            pattern: "needle".to_owned(),
            glob: None,
            case_sensitive: true,
            max_results: 10,
            max_output_bytes: 4096,
            timeout_ms: 1000,
            broad_file_limit: 5000,
            cancellation: CancellationToken::new(),
        };
        let first = cached_picker(&request, true).expect("initial picker");
        let second = cached_picker(&request, true).expect("reused picker");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.get_files().len(), 1);
        assert_eq!(
            first.get_files()[0].relative_path(first.as_ref()),
            "selected.rs"
        );

        let next_turn = SearchRequest {
            turn_id: "picker_cache_test_other_turn".to_owned(),
            ..request
        };
        let separate = cached_picker(&next_turn, true).expect("other turn picker");
        assert!(!Arc::ptr_eq(&first, &separate));
    }
}
