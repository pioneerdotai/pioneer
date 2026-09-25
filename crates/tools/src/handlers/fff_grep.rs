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
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

const MAX_SEARCH_FILES: usize = 20_000;
const MAX_SEARCH_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CACHED_PICKERS: usize = 32;
// FFF clips matched lines to 512 bytes, backing up at most three bytes
// to keep the prefix on a UTF-8 character boundary.
const FFF_LINE_CLIP_THRESHOLD: usize = 509;
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
    let mut worker = tokio::task::spawn_blocking(move || {
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
    let mut output = BoundedOutput::new(request.max_results, request.max_output_bytes);
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
        let expanded = if matched.line_content.len() >= FFF_LINE_CLIP_THRESHOLD {
            Some(read_full_line(
                path.as_path(),
                matched.byte_offset,
                request.max_output_bytes,
            )?)
        } else {
            None
        };
        let line = expanded
            .as_ref()
            .map(|(line, _)| line.as_str())
            .unwrap_or(matched.line_content.as_str());
        if expanded.as_ref().is_some_and(|(_, complete)| !complete) {
            output.truncated = true;
        }
        if !output.push(path.as_path(), matched.line_number, line) {
            break;
        }
    }
    if matches.len() > request.max_results || result.next_file_offset != 0 {
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

fn read_full_line(path: &Path, offset: u64, limit: usize) -> Result<(String, bool), ToolError> {
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
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            ToolError::execution_failed(format!(
                "grep_files could not read {}: {error}",
                path.display()
            ))
        })?;
    let newline = bytes.iter().position(|byte| *byte == b'\n');
    let complete = newline.is_some() || bytes.len() <= limit;
    let end = newline.unwrap_or(bytes.len()).min(limit);
    let line = String::from_utf8_lossy(&bytes[..end]);
    Ok((line.trim_end_matches('\r').to_owned(), complete))
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
