use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::process::Output;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::{Duration, timeout};

const DEFAULT_READ_MAX_BYTES: usize = 256 * 1024;
const HARD_MAX_READ_BYTES: usize = 1024 * 1024;
const DEFAULT_WRITE_MAX_BYTES: usize = 8 * 1024 * 1024;
const HARD_WRITE_MAX_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_EDIT_MAX_FILE_BYTES: usize = 8 * 1024 * 1024;
const HARD_EDIT_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_LIST_DEPTH: usize = 2;
const HARD_MAX_LIST_DEPTH: usize = 8;
const DEFAULT_LIST_LIMIT: usize = 512;
const HARD_MAX_LIST_LIMIT: usize = 4096;
const DEFAULT_GREP_RESULTS: usize = 200;
const HARD_MAX_GREP_RESULTS: usize = 500;
const DEFAULT_GREP_MAX_OUTPUT_BYTES: usize = 128 * 1024;
const HARD_MAX_GREP_OUTPUT_BYTES: usize = 512 * 1024;
const BROAD_GREP_FILE_LIMIT: usize = 5_000;
const DEFAULT_GREP_TIMEOUT_MS: u64 = 20_000;
const HARD_MAX_GREP_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_GREP_EXCLUDED_DIRS: &[&str] = &[
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

#[derive(Clone)]
pub struct ReadFileHandler {
    observation_store: Arc<FileObservationStore>,
}

pub struct ListDirHandler;
pub struct GrepHandler;

#[derive(Clone)]
pub struct WriteFileHandler {
    #[allow(dead_code)]
    observation_store: Arc<FileObservationStore>,
}

#[derive(Clone)]
pub struct EditFileHandler {
    #[allow(dead_code)]
    observation_store: Arc<FileObservationStore>,
}

impl ReadFileHandler {
    pub(crate) fn new(observation_store: Arc<FileObservationStore>) -> Self {
        Self { observation_store }
    }
}

impl Default for ReadFileHandler {
    fn default() -> Self {
        Self::new(Arc::new(FileObservationStore::default()))
    }
}

impl WriteFileHandler {
    pub(crate) fn new(observation_store: Arc<FileObservationStore>) -> Self {
        Self { observation_store }
    }
}

impl Default for WriteFileHandler {
    fn default() -> Self {
        Self::new(Arc::new(FileObservationStore::default()))
    }
}

impl EditFileHandler {
    pub(crate) fn new(observation_store: Arc<FileObservationStore>) -> Self {
        Self { observation_store }
    }
}

impl Default for EditFileHandler {
    fn default() -> Self {
        Self::new(Arc::new(FileObservationStore::default()))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct FileObservation {
    id: String,
    #[serde(rename = "path")]
    resolved_path: PathBuf,
    bytes: u64,
    sha256: String,
    mtime_ms: i64,
    complete: bool,
    source_tool_call_id: String,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct FileObservationStore {
    latest_complete_by_path: RwLock<BTreeMap<PathBuf, FileObservation>>,
    by_id: RwLock<BTreeMap<String, FileObservation>>,
}

#[allow(dead_code)]
impl FileObservationStore {
    fn record(&self, observation: FileObservation) {
        self.by_id
            .write()
            .expect("file observation id lock poisoned")
            .insert(observation.id.clone(), observation.clone());

        if observation.complete {
            self.latest_complete_by_path
                .write()
                .expect("file observation path lock poisoned")
                .insert(observation.resolved_path.clone(), observation);
        }
    }

    fn latest_complete_for_path(&self, resolved_path: &Path) -> Option<FileObservation> {
        self.latest_complete_by_path
            .read()
            .expect("file observation path lock poisoned")
            .get(resolved_path)
            .cloned()
    }

    fn complete_by_id_for_path(
        &self,
        observation_id: &str,
        resolved_path: &Path,
    ) -> Option<FileObservation> {
        let observation = self
            .by_id
            .read()
            .expect("file observation id lock poisoned")
            .get(observation_id)
            .cloned()?;
        (observation.complete && observation.resolved_path == resolved_path).then_some(observation)
    }
}

#[allow(dead_code)]
fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[allow(dead_code)]
fn system_time_mtime_ms(system_time: SystemTime) -> Option<i64> {
    let millis = system_time.duration_since(UNIX_EPOCH).ok()?.as_millis();
    i64::try_from(millis).ok()
}

#[allow(dead_code)]
fn metadata_mtime_ms(metadata: &std::fs::Metadata) -> Option<i64> {
    metadata.modified().ok().and_then(system_time_mtime_ms)
}

#[derive(Debug, Deserialize)]
struct ReadFileArgs {
    path: String,
    #[serde(default)]
    start_line: Option<usize>,
    #[serde(default)]
    end_line: Option<usize>,
    #[serde(default)]
    max_bytes: Option<usize>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WriteFileArgs {
    path: String,
    content: String,
    #[serde(default)]
    create_dirs: Option<bool>,
    #[serde(default)]
    overwrite: Option<bool>,
    #[serde(default)]
    read_observation_id: Option<String>,
    #[serde(default)]
    expected_sha256: Option<String>,
    #[serde(default)]
    expected_mtime_ms: Option<i64>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct EditFileArgs {
    path: String,
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: Option<bool>,
    #[serde(default)]
    read_observation_id: Option<String>,
    #[serde(default)]
    expected_sha256: Option<String>,
    #[serde(default)]
    expected_mtime_ms: Option<i64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteFileOperation {
    Created,
    Overwritten,
}

#[allow(dead_code)]
impl WriteFileOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Overwritten => "overwritten",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteFileTarget {
    original_path: String,
    resolved_path: PathBuf,
    operation: WriteFileOperation,
    created_dirs: Vec<PathBuf>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct EditFileTarget {
    original_path: String,
    resolved_path: PathBuf,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct EditFileLoadedTarget {
    target: EditFileTarget,
    text: String,
    current: CurrentFileState,
}

enum EditFileTargetValidation {
    Ready(EditFileTarget),
    Failed(Box<dyn ToolOutput>),
}

enum EditFileTextLoad {
    Loaded(EditFileLoadedTarget),
    Failed(Box<dyn ToolOutput>),
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct EditFileRawMatch {
    occurrences: usize,
    source: EditFileMatchSource,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum EditFileMatchSource {
    Raw {
        line_ending_mode: LineEndingMode,
    },
    CrlfFallback {
        normalized_text: String,
        normalized_old_string: String,
        normalized_new_string: String,
    },
}

impl EditFileMatchSource {
    fn line_ending_mode(&self) -> &'static str {
        match self {
            Self::Raw { line_ending_mode } => line_ending_mode.as_str(),
            Self::CrlfFallback { .. } => "crlf_fallback",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct EditFileComputedEdit {
    final_text: String,
    matches_replaced: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineEndingMode {
    None,
    Lf,
    Crlf,
    Mixed,
}

impl LineEndingMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lf => "lf",
            Self::Crlf => "crlf",
            Self::Mixed => "mixed",
        }
    }
}

enum EditFileRawMatchResult {
    Matched(EditFileRawMatch),
    Failed(Box<dyn ToolOutput>),
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileMutationTool {
    WriteFile,
    EditFile,
}

#[allow(dead_code)]
impl FileMutationTool {
    fn name(self) -> &'static str {
        match self {
            Self::WriteFile => "write_file",
            Self::EditFile => "edit_file",
        }
    }

    fn current_state_action_context(self) -> &'static str {
        match self {
            Self::WriteFile => "before overwrite",
            Self::EditFile => "before edit",
        }
    }

    fn read_required_message(self) -> &'static str {
        match self {
            Self::WriteFile => {
                "write_file cannot overwrite an existing file until read_file has observed the complete current file"
            }
            Self::EditFile => {
                "edit_file cannot modify an existing file until read_file has observed the complete current file"
            }
        }
    }

    fn stale_message(self) -> &'static str {
        match self {
            Self::WriteFile => "file changed before write_file could overwrite it",
            Self::EditFile => "file changed before edit_file could modify it",
        }
    }

    fn text_payload_label(self) -> &'static str {
        match self {
            Self::WriteFile => "write_file content",
            Self::EditFile => "edit_file result",
        }
    }

    fn verification_failed_message(self) -> &'static str {
        match self {
            Self::WriteFile => "write_file wrote bytes but post-write verification failed",
            Self::EditFile => "edit_file wrote bytes but post-write verification failed",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
struct FilePreconditionInput<'a> {
    tool: FileMutationTool,
    original_path: &'a str,
    resolved_path: &'a Path,
    read_observation_id: Option<&'a str>,
    expected_sha256: Option<&'a str>,
    expected_mtime_ms: Option<i64>,
}

#[allow(dead_code)]
impl FilePreconditionInput<'_> {
    fn has_explicit_precondition(&self) -> bool {
        self.expected_sha256.is_some() || self.expected_mtime_ms.is_some()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CurrentFileState {
    bytes: u64,
    sha256: String,
    mtime_ms: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct AtomicWriteResult {
    bytes_written: u64,
    sha256: String,
}

enum WriteVerification {
    Verified(CurrentFileState),
    Failed(Box<dyn ToolOutput>),
}

#[derive(Debug, Deserialize)]
struct ListDirArgs {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    depth: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_hidden: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct GrepArgs {
    pattern: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    glob: Option<String>,
    #[serde(default)]
    max_results: Option<usize>,
    #[serde(default)]
    max_output_bytes: Option<usize>,
    #[serde(default)]
    case_sensitive: Option<bool>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct DirEntryView {
    path: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
}

#[async_trait]
impl ToolHandler for ReadFileHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<ReadFileArgs>(invocation.payload)?;
        let file_path = normalize_path_lexically(resolve_path(
            invocation.workdir.as_path(),
            args.path.as_str(),
        ));

        let metadata = tokio::fs::metadata(file_path.as_path())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to stat file `{}`: {error}",
                    file_path.display()
                ))
            })?;
        if !metadata.is_file() {
            return Err(ToolError::invalid_arguments(format!(
                "`{}` is not a regular file",
                file_path.display()
            )));
        }

        let read_future = tokio::fs::read(file_path.as_path());
        let bytes = timeout(Duration::from_secs(15), read_future)
            .await
            .map_err(|_| {
                ToolError::execution_failed(format!(
                    "timed out while reading `{}`",
                    file_path.display()
                ))
            })?
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to read file `{}`: {error}",
                    file_path.display()
                ))
            })?;

        let max_bytes = args
            .max_bytes
            .unwrap_or(DEFAULT_READ_MAX_BYTES)
            .clamp(1, HARD_MAX_READ_BYTES);
        let full_byte_count = bytes.len();
        let full_sha256 = sha256_hex(bytes.as_slice());
        let mtime_ms = metadata_mtime_ms(&metadata).unwrap_or_default();
        let was_truncated = bytes.len() > max_bytes;
        let bytes = if was_truncated {
            bytes[..max_bytes].to_vec()
        } else {
            bytes
        };

        let text = String::from_utf8_lossy(bytes.as_slice()).to_string();
        let start = args.start_line.unwrap_or(1).max(1);
        let end = args.end_line.unwrap_or(usize::MAX).max(start);

        let mut rendered = String::new();
        rendered.push_str(format!("File: {}\n---\n", file_path.display()).as_str());
        for (index, line) in text.lines().enumerate() {
            let line_no = index.saturating_add(1);
            if line_no < start || line_no > end {
                continue;
            }
            rendered.push_str(format!("{:>6}\t{}\n", line_no, line).as_str());
        }

        if rendered.ends_with("---\n") {
            rendered.push_str("<empty selection>\n");
        }

        if was_truncated {
            rendered.push_str(format!("\n... [truncated at {} bytes]\n", max_bytes).as_str());
        }

        let complete = !was_truncated && args.start_line.is_none() && args.end_line.is_none();
        let observation = FileObservation {
            id: format!("read_file:{}", invocation.call_id),
            resolved_path: file_path.clone(),
            bytes: full_byte_count as u64,
            sha256: full_sha256,
            mtime_ms,
            complete,
            source_tool_call_id: invocation.call_id,
        };
        self.observation_store.record(observation.clone());

        let payload = serde_json::json!({
            "path": file_path.display().to_string(),
            "resolved_path": file_path.display().to_string(),
            "start_line": start,
            "end_line": if end == usize::MAX { JsonValue::Null } else { serde_json::json!(end) },
            "truncated": was_truncated,
            "max_bytes": max_bytes,
            "file_observation": observation,
            "output": rendered.clone(),
        });
        Ok(Box::new(FunctionToolOutput::with_payload(
            rendered, true, payload,
        )))
    }
}

#[async_trait]
impl ToolHandler for WriteFileHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_write_file_args(invocation.payload)?;
        let target = prepare_write_file_target(invocation.workdir.as_path(), &args).await?;
        if let Some(output) =
            verify_existing_file_preconditions(self.observation_store.as_ref(), &target, &args)
                .await?
        {
            return Ok(output);
        }

        let write_result = write_file_atomically(&target, args.content.as_str()).await?;
        let current = match verify_written_file(&target, &write_result).await? {
            WriteVerification::Verified(current) => current,
            WriteVerification::Failed(output) => return Ok(output),
        };
        let observation = record_file_mutation_observation(
            self.observation_store.as_ref(),
            FileMutationTool::WriteFile,
            invocation.call_id.as_str(),
            target.resolved_path.as_path(),
            &current,
        );

        Ok(write_file_success_output(&target, &current, observation))
    }
}

#[async_trait]
impl ToolHandler for EditFileHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_edit_file_args(invocation.payload)?;
        let target = match prepare_edit_file_target(invocation.workdir.as_path(), &args).await? {
            EditFileTargetValidation::Ready(target) => target,
            EditFileTargetValidation::Failed(output) => return Ok(output),
        };
        if args.old_string == args.new_string {
            return Ok(edit_file_identical_no_change_output(
                args.path.as_str(),
                target.resolved_path.as_path(),
            ));
        }
        let loaded = match load_edit_file_text(target).await? {
            EditFileTextLoad::Loaded(loaded) => loaded,
            EditFileTextLoad::Failed(output) => return Ok(output),
        };
        if let Some(output) = verify_edit_file_observation_preconditions(
            self.observation_store.as_ref(),
            &loaded,
            &args,
        )
        .await?
        {
            return Ok(output);
        }
        let raw_match = match compute_edit_file_raw_match(&loaded, &args) {
            EditFileRawMatchResult::Matched(raw_match) => raw_match,
            EditFileRawMatchResult::Failed(output) => return Ok(output),
        };
        let computed = compute_edit_file_replacement(&loaded, &args, &raw_match);
        if let Some(output) = edit_file_no_change_if_unchanged(&loaded, &computed) {
            return Ok(output);
        }
        let write_result = edit_file_atomically(&loaded, &computed).await?;
        let current = match verify_edited_file(&loaded, &write_result).await? {
            WriteVerification::Verified(current) => current,
            WriteVerification::Failed(output) => return Ok(output),
        };
        let observation = record_file_mutation_observation(
            self.observation_store.as_ref(),
            FileMutationTool::EditFile,
            invocation.call_id.as_str(),
            loaded.target.resolved_path.as_path(),
            &current,
        );
        Ok(edit_file_success_output(
            &loaded,
            &args,
            &raw_match,
            &computed,
            &current,
            observation,
        ))
    }
}

#[async_trait]
impl ToolHandler for ListDirHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<ListDirArgs>(invocation.payload)?;
        let base = args.path.unwrap_or_else(|| ".".to_owned());
        let root = resolve_path(invocation.workdir.as_path(), base.as_str());
        let depth_limit = args
            .depth
            .unwrap_or(DEFAULT_LIST_DEPTH)
            .min(HARD_MAX_LIST_DEPTH);
        let limit = args
            .limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, HARD_MAX_LIST_LIMIT);
        let include_hidden = args.include_hidden.unwrap_or(false);

        let mut queue = VecDeque::new();
        queue.push_back((root.clone(), 0usize));

        let mut items = Vec::new();
        while let Some((path, depth)) = queue.pop_front() {
            if items.len() >= limit {
                break;
            }

            let mut read_dir = tokio::fs::read_dir(path.as_path()).await.map_err(|error| {
                ToolError::execution_failed(format!("failed to list `{}`: {error}", path.display()))
            })?;
            let mut entries = Vec::new();
            while let Some(entry) = read_dir.next_entry().await.map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to read dir entry under `{}`: {error}",
                    path.display()
                ))
            })? {
                entries.push(entry);
            }

            entries.sort_by_key(|entry| entry.path());

            for entry in entries {
                if items.len() >= limit {
                    break;
                }

                let entry_path = entry.path();
                let file_name = entry.file_name().to_string_lossy().to_string();
                if !include_hidden && file_name.starts_with('.') {
                    continue;
                }

                let file_type = entry.file_type().await.map_err(|error| {
                    ToolError::execution_failed(format!(
                        "failed to inspect file type for `{}`: {error}",
                        entry_path.display()
                    ))
                })?;

                let kind = if file_type.is_dir() {
                    "dir"
                } else if file_type.is_file() {
                    "file"
                } else if file_type.is_symlink() {
                    "symlink"
                } else {
                    "other"
                }
                .to_owned();

                let size = if file_type.is_file() {
                    Some(
                        entry
                            .metadata()
                            .await
                            .map_err(|error| {
                                ToolError::execution_failed(format!(
                                    "failed to read metadata for `{}`: {error}",
                                    entry_path.display()
                                ))
                            })?
                            .len(),
                    )
                } else {
                    None
                };

                items.push(DirEntryView {
                    path: entry_path.display().to_string(),
                    kind: kind.clone(),
                    size,
                });

                if file_type.is_dir() && !file_type.is_symlink() && depth < depth_limit {
                    queue.push_back((entry_path, depth.saturating_add(1)));
                }
            }
        }

        let payload = serde_json::json!({
            "root": root.display().to_string(),
            "truncated": items.len() >= limit,
            "entries": items,
        });
        let body = serde_json::to_string_pretty(&payload).map_err(|error| {
            ToolError::internal(format!("failed to serialize list_dir result: {error}"))
        })?;

        Ok(Box::new(FunctionToolOutput::with_payload(
            body, true, payload,
        )))
    }
}

#[async_trait]
impl ToolHandler for GrepHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<GrepArgs>(invocation.payload)?;
        let base = args.path.as_deref().unwrap_or(".");
        let search_path = resolve_path_within_workdir(invocation.workdir.as_path(), base)?;

        let max_results = args
            .max_results
            .unwrap_or(DEFAULT_GREP_RESULTS)
            .clamp(1, HARD_MAX_GREP_RESULTS);
        let max_output_bytes = args
            .max_output_bytes
            .unwrap_or(DEFAULT_GREP_MAX_OUTPUT_BYTES)
            .clamp(1, HARD_MAX_GREP_OUTPUT_BYTES);
        let case_sensitive = args.case_sensitive.unwrap_or(true);
        let timeout_ms = args
            .timeout_ms
            .unwrap_or(DEFAULT_GREP_TIMEOUT_MS)
            .clamp(1, HARD_MAX_GREP_TIMEOUT_MS);
        let is_broad_workspace_search = args.path.is_none() && args.glob.is_none();
        if is_broad_workspace_search
            && let Some(file_count) = count_rg_search_files(
                search_path.as_path(),
                invocation.workdir.as_path(),
                &invocation.environment,
                timeout_ms.min(3_000),
            )
            .await?
            && file_count > BROAD_GREP_FILE_LIMIT
        {
            return Ok(needs_narrowing_output(
                "grep_files is too broad for this workspace. Narrow path or glob.",
                search_path.as_path(),
                Some(file_count),
                max_results,
                max_output_bytes,
                "broad_workspace_search",
            ));
        }

        let rg_search = run_rg_search(
            args.pattern.as_str(),
            args.glob.as_deref(),
            case_sensitive,
            max_results,
            search_path.as_path(),
            invocation.workdir.as_path(),
            &invocation.environment,
            timeout_ms,
        )
        .await;
        let (output, backend_note, used_fallback) = match rg_search {
            Ok(Some(output)) => (output, None, false),
            Err(ToolError::ExecutionFailed(message)) if message.contains("timed out") => {
                return Ok(needs_narrowing_output(
                    "grep_files timed out. Narrow path or glob.",
                    search_path.as_path(),
                    None,
                    max_results,
                    max_output_bytes,
                    "timeout",
                ));
            }
            Err(error) => return Err(error),
            Ok(None) => {
                let fallback = run_grep_fallback(
                    args.pattern.as_str(),
                    case_sensitive,
                    search_path.as_path(),
                    invocation.workdir.as_path(),
                    &invocation.environment,
                    timeout_ms,
                )
                .await;
                let output = match fallback {
                    Ok(output) => output,
                    Err(ToolError::ExecutionFailed(message)) if message.contains("timed out") => {
                        return Ok(needs_narrowing_output(
                            "grep_files timed out. Narrow path or glob.",
                            search_path.as_path(),
                            None,
                            max_results,
                            max_output_bytes,
                            "timeout",
                        ));
                    }
                    Err(error) => return Err(error),
                };
                let note = if args.glob.is_some() {
                    "note: rg is unavailable; used grep fallback (glob filter is ignored)"
                        .to_owned()
                } else {
                    "note: rg is unavailable; used grep fallback".to_owned()
                };
                (output, Some(note), true)
            }
        };

        let exit_code = output.status.code().unwrap_or(-1);
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let (truncated_stdout, output_truncated) =
            truncate_lines_and_bytes(stdout.as_str(), max_results, max_output_bytes);

        if exit_code == 0 {
            let body = if truncated_stdout.trim().is_empty() {
                "no matches".to_owned()
            } else {
                truncated_stdout
            };
            let body = prepend_note(backend_note.as_deref(), body);
            let payload = serde_json::json!({
                "status": "ok",
                "engine": if used_fallback { "grep" } else { "rg" },
                "path": search_path.display().to_string(),
                "exit_code": exit_code,
                "truncated": output_truncated,
                "max_results": max_results,
                "max_output_bytes": max_output_bytes,
                "stdout": stdout,
                "stderr": stderr,
                "output": body.clone(),
            });
            Ok(Box::new(FunctionToolOutput::with_payload(
                body, true, payload,
            )))
        } else if exit_code == 1 {
            let body = prepend_note(backend_note.as_deref(), "no matches".to_owned());
            let payload = serde_json::json!({
                "status": "no_matches",
                "engine": if used_fallback { "grep" } else { "rg" },
                "path": search_path.display().to_string(),
                "exit_code": exit_code,
                "truncated": false,
                "max_results": max_results,
                "max_output_bytes": max_output_bytes,
                "stdout": stdout,
                "stderr": stderr,
                "output": body.clone(),
            });
            Ok(Box::new(FunctionToolOutput::with_payload(
                body, true, payload,
            )))
        } else {
            let engine = if used_fallback { "grep" } else { "rg" };
            let body = prepend_note(
                backend_note.as_deref(),
                format!(
                    "{engine} failed (exit={exit_code})\npath={}\n{stderr}",
                    search_path.display()
                ),
            );
            let payload = serde_json::json!({
                "status": "failed",
                "engine": engine,
                "path": search_path.display().to_string(),
                "exit_code": exit_code,
                "truncated": false,
                "max_results": max_results,
                "max_output_bytes": max_output_bytes,
                "stdout": stdout,
                "stderr": stderr,
                "output": body.clone(),
            });
            Ok(Box::new(FunctionToolOutput::with_payload(
                body, false, payload,
            )))
        }
    }
}

fn prepend_note(note: Option<&str>, body: String) -> String {
    match note {
        Some(note) if !note.is_empty() => format!("{note}\n{body}"),
        _ => body,
    }
}

fn truncate_lines_and_bytes(text: &str, max_lines: usize, max_bytes: usize) -> (String, bool) {
    if max_lines == 0 || max_bytes == 0 {
        return (String::new(), !text.is_empty());
    }

    let mut rendered = String::new();
    let mut truncated = false;
    for (index, line) in text.lines().enumerate() {
        if index >= max_lines {
            truncated = true;
            break;
        }
        let next_line = if rendered.is_empty() {
            line.to_owned()
        } else {
            format!("\n{line}")
        };
        if rendered.len().saturating_add(next_line.len()) > max_bytes {
            truncated = true;
            break;
        }
        rendered.push_str(next_line.as_str());
    }

    if truncated {
        let suffix = format!(
            "\n... [truncated to {} lines / {} bytes]",
            max_lines, max_bytes
        );
        if rendered.len().saturating_add(suffix.len()) <= max_bytes {
            rendered.push_str(suffix.as_str());
        }
    }

    (rendered, truncated)
}

async fn run_rg_search(
    pattern: &str,
    glob: Option<&str>,
    case_sensitive: bool,
    max_results: usize,
    search_path: &Path,
    workdir: &Path,
    environment: &BTreeMap<String, String>,
    timeout_ms: u64,
) -> Result<Option<Output>, ToolError> {
    let mut command = Command::new("rg");
    command.arg("--line-number");
    command.arg("--no-heading");
    command.arg("--color").arg("never");
    append_default_rg_excludes(&mut command);
    if !case_sensitive {
        command.arg("-i");
    }
    if let Some(glob) = glob {
        command.arg("-g").arg(glob);
    }
    command.arg("--max-count").arg(max_results.to_string());
    command.arg(pattern);
    command.arg(search_path.as_os_str());
    command.current_dir(workdir);
    command.envs(environment.iter());

    let output = timeout(Duration::from_millis(timeout_ms), command.output())
        .await
        .map_err(|_| ToolError::execution_failed(format!("rg timed out after {timeout_ms}ms")))?;

    match output {
        Ok(output) => Ok(Some(output)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ToolError::execution_failed(format!(
            "failed to execute rg: {error}"
        ))),
    }
}

async fn run_grep_fallback(
    pattern: &str,
    case_sensitive: bool,
    search_path: &Path,
    workdir: &Path,
    environment: &BTreeMap<String, String>,
    timeout_ms: u64,
) -> Result<Output, ToolError> {
    let mut command = Command::new("grep");
    command.arg("-R");
    command.arg("-n");
    for excluded in DEFAULT_GREP_EXCLUDED_DIRS {
        command.arg(format!("--exclude-dir={excluded}"));
    }
    if !case_sensitive {
        command.arg("-i");
    }
    command.arg("--");
    command.arg(pattern);
    command.arg(search_path.as_os_str());
    command.current_dir(workdir);
    command.envs(environment.iter());

    timeout(Duration::from_millis(timeout_ms), command.output())
        .await
        .map_err(|_| ToolError::execution_failed(format!("grep timed out after {timeout_ms}ms")))?
        .map_err(|error| ToolError::execution_failed(format!("failed to execute grep: {error}")))
}

async fn count_rg_search_files(
    search_path: &Path,
    workdir: &Path,
    environment: &BTreeMap<String, String>,
    timeout_ms: u64,
) -> Result<Option<usize>, ToolError> {
    let mut command = Command::new("rg");
    command.arg("--files");
    append_default_rg_excludes(&mut command);
    command.arg(search_path.as_os_str());
    command.current_dir(workdir);
    command.envs(environment.iter());

    let output = timeout(Duration::from_millis(timeout_ms), command.output())
        .await
        .map_err(|_| {
            ToolError::execution_failed(format!("rg --files timed out after {timeout_ms}ms"))
        })?;
    let output = match output {
        Ok(output) => output,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ToolError::execution_failed(format!(
                "failed to execute rg --files: {error}"
            )));
        }
    };
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(Some(
        stdout
            .lines()
            .take(BROAD_GREP_FILE_LIMIT.saturating_add(1))
            .count(),
    ))
}

fn append_default_rg_excludes(command: &mut Command) {
    for excluded in DEFAULT_GREP_EXCLUDED_DIRS {
        command.arg("-g").arg(format!("!{excluded}/**"));
    }
}

fn needs_narrowing_output(
    message: &str,
    search_path: &Path,
    scanned_file_count: Option<usize>,
    max_results: usize,
    max_output_bytes: usize,
    reason: &str,
) -> Box<dyn ToolOutput> {
    let suggestions = serde_json::json!([
        {"path": "crates/tasks/src", "glob": "*.rs"},
        {"path": "crates/gateway/src", "glob": "*.rs"},
        {"path": "crates/desktop/src", "glob": "*.rs"}
    ]);
    let payload = serde_json::json!({
        "ok": false,
        "status": "needs_narrowing",
        "errorClass": "needs_narrowing",
        "message": message,
        "reason": reason,
        "path": search_path.display().to_string(),
        "scannedFileCount": scanned_file_count,
        "maxResults": max_results,
        "maxOutputBytes": max_output_bytes,
        "suggestions": suggestions,
        "retryableByModel": true,
        "retrySameArguments": false,
    });
    let body = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| message.to_owned());
    Box::new(FunctionToolOutput::with_payload(body, false, payload))
}

fn parse_json_args<T>(payload: ToolPayload) -> Result<T, ToolError>
where
    T: for<'de> Deserialize<'de>,
{
    let value = match payload {
        ToolPayload::Function { arguments } => arguments,
        ToolPayload::Custom { input } => serde_json::from_str::<JsonValue>(input.as_str())
            .map_err(|error| {
                ToolError::invalid_arguments(format!("failed to parse JSON input: {error}"))
            })?,
        _ => {
            return Err(ToolError::invalid_arguments(
                "expected function-style JSON arguments",
            ));
        }
    };

    serde_json::from_value(value)
        .map_err(|error| ToolError::invalid_arguments(format!("invalid arguments: {error}")))
}

fn parse_write_file_args(payload: ToolPayload) -> Result<WriteFileArgs, ToolError> {
    let args = parse_json_args::<WriteFileArgs>(payload)?;
    if args.path.trim().is_empty() {
        return Err(ToolError::invalid_arguments(
            "write_file `path` must not be empty",
        ));
    }
    if let Some(expected_sha256) = args.expected_sha256.as_deref() {
        validate_expected_sha256("write_file", expected_sha256)?;
    }
    Ok(args)
}

fn parse_edit_file_args(payload: ToolPayload) -> Result<EditFileArgs, ToolError> {
    let args = parse_json_args::<EditFileArgs>(payload)?;
    if args.path.trim().is_empty() {
        return Err(ToolError::invalid_arguments(
            "edit_file `path` must not be empty",
        ));
    }
    if args.old_string.is_empty() {
        return Err(ToolError::invalid_arguments(
            "edit_file `old_string` must not be empty",
        ));
    }
    if let Some(expected_sha256) = args.expected_sha256.as_deref() {
        validate_expected_sha256("edit_file", expected_sha256)?;
    }
    Ok(args)
}

fn validate_expected_sha256(tool_name: &str, value: &str) -> Result<(), ToolError> {
    let is_valid = value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    if is_valid {
        Ok(())
    } else {
        Err(ToolError::invalid_arguments(format!(
            "{tool_name} `expected_sha256` must be a 64-character hex SHA-256 digest"
        )))
    }
}

fn write_max_bytes() -> usize {
    DEFAULT_WRITE_MAX_BYTES.min(HARD_WRITE_MAX_BYTES)
}

async fn prepare_write_file_target(
    workdir: &Path,
    args: &WriteFileArgs,
) -> Result<WriteFileTarget, ToolError> {
    let resolved_path = normalize_path_lexically(resolve_path(workdir, args.path.as_str()));
    let parent = resolved_path.parent().ok_or_else(|| {
        ToolError::invalid_arguments(format!(
            "write_file target `{}` does not have a parent directory",
            resolved_path.display()
        ))
    })?;
    let create_dirs = args.create_dirs.unwrap_or(true);
    let overwrite = args.overwrite.unwrap_or(true);
    let mut created_dirs = Vec::new();

    match tokio::fs::metadata(parent).await {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(ToolError::invalid_arguments(format!(
                    "write_file parent `{}` is not a directory",
                    parent.display()
                )));
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            if !create_dirs {
                return Err(ToolError::invalid_arguments(format!(
                    "write_file parent directory `{}` does not exist and create_dirs=false",
                    parent.display()
                )));
            }
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to create parent directory `{}`: {error}",
                    parent.display()
                ))
            })?;
            created_dirs.push(parent.to_path_buf());
        }
        Err(error) => {
            return Err(ToolError::execution_failed(format!(
                "failed to stat parent directory `{}`: {error}",
                parent.display()
            )));
        }
    }

    let operation = match tokio::fs::metadata(resolved_path.as_path()).await {
        Ok(metadata) if metadata.is_dir() => {
            return Err(ToolError::invalid_arguments(format!(
                "write_file target `{}` is a directory",
                resolved_path.display()
            )));
        }
        Ok(_) if !overwrite => {
            return Err(ToolError::invalid_arguments(format!(
                "write_file target `{}` exists and overwrite=false",
                resolved_path.display()
            )));
        }
        Ok(_) => WriteFileOperation::Overwritten,
        Err(error) if error.kind() == ErrorKind::NotFound => WriteFileOperation::Created,
        Err(error) => {
            return Err(ToolError::execution_failed(format!(
                "failed to stat write_file target `{}`: {error}",
                resolved_path.display()
            )));
        }
    };

    Ok(WriteFileTarget {
        original_path: args.path.clone(),
        resolved_path,
        operation,
        created_dirs,
    })
}

async fn prepare_edit_file_target(
    workdir: &Path,
    args: &EditFileArgs,
) -> Result<EditFileTargetValidation, ToolError> {
    let resolved_path = normalize_path_lexically(resolve_path(workdir, args.path.as_str()));

    match tokio::fs::metadata(resolved_path.as_path()).await {
        Ok(metadata) if metadata.is_file() => Ok(EditFileTargetValidation::Ready(EditFileTarget {
            original_path: args.path.clone(),
            resolved_path,
        })),
        Ok(metadata) if metadata.is_dir() => Err(ToolError::invalid_arguments(format!(
            "edit_file target `{}` is a directory",
            resolved_path.display()
        ))),
        Ok(_) => Err(ToolError::invalid_arguments(format!(
            "edit_file target `{}` is not a regular file",
            resolved_path.display()
        ))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(EditFileTargetValidation::Failed(
            edit_file_target_not_found_output(args.path.as_str(), resolved_path.as_path()),
        )),
        Err(error) => Err(ToolError::execution_failed(format!(
            "failed to stat edit_file target `{}`: {error}",
            resolved_path.display()
        ))),
    }
}

fn edit_file_target_not_found_output(
    original_path: &str,
    resolved_path: &Path,
) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "target_not_found",
        "errorClass": "invalid_arguments",
        "message": "edit_file target does not exist; use write_file if the goal is to create a new file",
        "path": original_path,
        "resolved_path": resolved_path.display().to_string(),
        "retryableByModel": true,
        "retrySameArguments": false,
        "suggestedTool": "write_file",
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| {
            "edit_file target does not exist; use write_file if the goal is to create a new file"
                .to_owned()
        }),
        false,
        payload,
    ))
}

fn edit_max_file_bytes() -> usize {
    DEFAULT_EDIT_MAX_FILE_BYTES.min(HARD_EDIT_MAX_FILE_BYTES)
}

async fn load_edit_file_text(target: EditFileTarget) -> Result<EditFileTextLoad, ToolError> {
    let metadata = tokio::fs::metadata(target.resolved_path.as_path())
        .await
        .map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to stat edit_file target `{}` before edit: {error}",
                target.resolved_path.display()
            ))
        })?;
    if !metadata.is_file() {
        return Err(ToolError::invalid_arguments(format!(
            "edit_file target `{}` is not a regular file",
            target.resolved_path.display()
        )));
    }

    let max_bytes = edit_max_file_bytes();
    let metadata_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if metadata_len > max_bytes {
        return Err(ToolError::invalid_arguments(format!(
            "edit_file target `{}` is larger than edit_file limit ({max_bytes} bytes)",
            target.resolved_path.display()
        )));
    }

    let bytes = tokio::fs::read(target.resolved_path.as_path())
        .await
        .map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to read edit_file target `{}` before edit: {error}",
                target.resolved_path.display()
            ))
        })?;
    if bytes.len() > max_bytes {
        return Err(ToolError::invalid_arguments(format!(
            "edit_file target `{}` is larger than edit_file limit ({max_bytes} bytes)",
            target.resolved_path.display()
        )));
    }

    let current = CurrentFileState {
        bytes: bytes.len() as u64,
        sha256: sha256_hex(bytes.as_slice()),
        mtime_ms: metadata_mtime_ms(&metadata).unwrap_or_default(),
    };
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => {
            return Ok(EditFileTextLoad::Failed(edit_file_not_utf8_output(
                target.original_path.as_str(),
                target.resolved_path.as_path(),
            )));
        }
    };

    Ok(EditFileTextLoad::Loaded(EditFileLoadedTarget {
        target,
        text,
        current,
    }))
}

fn edit_file_not_utf8_output(original_path: &str, resolved_path: &Path) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "not_utf8",
        "errorClass": "invalid_arguments",
        "message": "edit_file target is not valid UTF-8 text",
        "path": original_path,
        "resolved_path": resolved_path.display().to_string(),
        "retryableByModel": false,
        "retrySameArguments": false,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload)
            .unwrap_or_else(|_| "edit_file target is not valid UTF-8 text".to_owned()),
        false,
        payload,
    ))
}

async fn verify_existing_file_preconditions(
    observation_store: &FileObservationStore,
    target: &WriteFileTarget,
    args: &WriteFileArgs,
) -> Result<Option<Box<dyn ToolOutput>>, ToolError> {
    if target.operation == WriteFileOperation::Created {
        return Ok(None);
    }

    verify_file_preconditions(
        observation_store,
        FilePreconditionInput {
            tool: FileMutationTool::WriteFile,
            original_path: args.path.as_str(),
            resolved_path: target.resolved_path.as_path(),
            read_observation_id: args.read_observation_id.as_deref(),
            expected_sha256: args.expected_sha256.as_deref(),
            expected_mtime_ms: args.expected_mtime_ms,
        },
    )
    .await
}

async fn verify_file_preconditions(
    observation_store: &FileObservationStore,
    input: FilePreconditionInput<'_>,
) -> Result<Option<Box<dyn ToolOutput>>, ToolError> {
    let observation = match input.read_observation_id {
        Some(observation_id) => {
            observation_store.complete_by_id_for_path(observation_id, input.resolved_path)
        }
        None => observation_store.latest_complete_for_path(input.resolved_path),
    };
    let has_explicit_precondition = input.has_explicit_precondition();

    if observation.is_none() && !has_explicit_precondition {
        return Ok(Some(read_required_output(input)));
    }

    let current = read_current_file_state_for_tool(
        input.tool.name(),
        input.resolved_path,
        input.tool.current_state_action_context(),
    )
    .await?;
    if has_explicit_precondition {
        if let Some(expected_sha256) = input.expected_sha256
            && current.sha256 != expected_sha256
        {
            return Ok(Some(precondition_failed_output_for_expected(
                input,
                &current,
                Some(expected_sha256),
                input.expected_mtime_ms,
            )));
        }

        if let Some(expected_mtime_ms) = input.expected_mtime_ms
            && current.mtime_ms != expected_mtime_ms
        {
            return Ok(Some(precondition_failed_output_for_expected(
                input,
                &current,
                input.expected_sha256,
                Some(expected_mtime_ms),
            )));
        }

        return Ok(None);
    }

    if let Some(observation) = observation.as_ref()
        && (current.sha256 != observation.sha256 || current.mtime_ms != observation.mtime_ms)
    {
        return Ok(Some(precondition_failed_output(
            input,
            &current,
            observation,
        )));
    }

    Ok(None)
}

async fn verify_edit_file_observation_preconditions(
    observation_store: &FileObservationStore,
    loaded: &EditFileLoadedTarget,
    args: &EditFileArgs,
) -> Result<Option<Box<dyn ToolOutput>>, ToolError> {
    verify_file_preconditions(
        observation_store,
        FilePreconditionInput {
            tool: FileMutationTool::EditFile,
            original_path: loaded.target.original_path.as_str(),
            resolved_path: loaded.target.resolved_path.as_path(),
            read_observation_id: args.read_observation_id.as_deref(),
            expected_sha256: args.expected_sha256.as_deref(),
            expected_mtime_ms: args.expected_mtime_ms,
        },
    )
    .await
}

fn compute_edit_file_raw_match(
    loaded: &EditFileLoadedTarget,
    args: &EditFileArgs,
) -> EditFileRawMatchResult {
    let line_ending_mode = detect_line_ending_mode(loaded.text.as_str());
    let occurrences = loaded.text.matches(args.old_string.as_str()).count();
    if occurrences > 0 {
        return validate_edit_file_match_count(
            loaded,
            args,
            EditFileRawMatch {
                occurrences,
                source: EditFileMatchSource::Raw { line_ending_mode },
            },
        );
    }

    if line_ending_mode == LineEndingMode::Crlf {
        let normalized_text = loaded.text.replace("\r\n", "\n");
        let normalized_old_string = args.old_string.replace("\r\n", "\n");
        let normalized_new_string = args.new_string.replace("\r\n", "\n");
        let normalized_occurrences = normalized_text
            .matches(normalized_old_string.as_str())
            .count();
        if normalized_occurrences > 0 {
            return validate_edit_file_match_count(
                loaded,
                args,
                EditFileRawMatch {
                    occurrences: normalized_occurrences,
                    source: EditFileMatchSource::CrlfFallback {
                        normalized_text,
                        normalized_old_string,
                        normalized_new_string,
                    },
                },
            );
        }
    }

    EditFileRawMatchResult::Failed(edit_file_not_found_output(
        loaded.target.original_path.as_str(),
        loaded.target.resolved_path.as_path(),
    ))
}

fn validate_edit_file_match_count(
    loaded: &EditFileLoadedTarget,
    args: &EditFileArgs,
    raw_match: EditFileRawMatch,
) -> EditFileRawMatchResult {
    if raw_match.occurrences > 1 && !args.replace_all.unwrap_or(false) {
        return EditFileRawMatchResult::Failed(edit_file_ambiguous_match_output(
            loaded.target.original_path.as_str(),
            loaded.target.resolved_path.as_path(),
            raw_match.occurrences,
        ));
    }

    EditFileRawMatchResult::Matched(raw_match)
}

fn compute_edit_file_replacement(
    loaded: &EditFileLoadedTarget,
    args: &EditFileArgs,
    raw_match: &EditFileRawMatch,
) -> EditFileComputedEdit {
    match &raw_match.source {
        EditFileMatchSource::Raw { .. } => {
            if args.replace_all.unwrap_or(false) {
                compute_edit_file_replace_all(loaded, args, raw_match.occurrences)
            } else if raw_match.occurrences == 1 {
                compute_edit_file_single_replacement(loaded, args)
            } else {
                EditFileComputedEdit {
                    final_text: loaded.text.clone(),
                    matches_replaced: 0,
                }
            }
        }
        EditFileMatchSource::CrlfFallback {
            normalized_text,
            normalized_old_string,
            normalized_new_string,
        } => {
            let normalized_final_text = if args.replace_all.unwrap_or(false) {
                normalized_text.replace(
                    normalized_old_string.as_str(),
                    normalized_new_string.as_str(),
                )
            } else {
                normalized_text.replacen(
                    normalized_old_string.as_str(),
                    normalized_new_string.as_str(),
                    1,
                )
            };
            EditFileComputedEdit {
                final_text: normalized_final_text.replace('\n', "\r\n"),
                matches_replaced: if args.replace_all.unwrap_or(false) {
                    raw_match.occurrences
                } else {
                    1
                },
            }
        }
    }
}

fn compute_edit_file_single_replacement(
    loaded: &EditFileLoadedTarget,
    args: &EditFileArgs,
) -> EditFileComputedEdit {
    EditFileComputedEdit {
        final_text: loaded
            .text
            .replacen(args.old_string.as_str(), args.new_string.as_str(), 1),
        matches_replaced: 1,
    }
}

fn compute_edit_file_replace_all(
    loaded: &EditFileLoadedTarget,
    args: &EditFileArgs,
    occurrences: usize,
) -> EditFileComputedEdit {
    EditFileComputedEdit {
        final_text: loaded
            .text
            .replace(args.old_string.as_str(), args.new_string.as_str()),
        matches_replaced: occurrences,
    }
}

async fn edit_file_atomically(
    loaded: &EditFileLoadedTarget,
    computed: &EditFileComputedEdit,
) -> Result<AtomicWriteResult, ToolError> {
    write_text_atomically_for_tool(
        FileMutationTool::EditFile,
        loaded.target.resolved_path.as_path(),
        computed.final_text.as_str(),
        edit_max_file_bytes(),
    )
    .await
}

fn detect_line_ending_mode(text: &str) -> LineEndingMode {
    let bytes = text.as_bytes();
    let mut crlf = 0usize;
    let mut lone_lf = 0usize;
    let mut lone_cr = 0usize;
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'\r' if bytes.get(index + 1) == Some(&b'\n') => {
                crlf += 1;
                index += 2;
            }
            b'\r' => {
                lone_cr += 1;
                index += 1;
            }
            b'\n' => {
                lone_lf += 1;
                index += 1;
            }
            _ => {
                index += 1;
            }
        }
    }

    match (crlf > 0, lone_lf > 0, lone_cr > 0) {
        (false, false, false) => LineEndingMode::None,
        (false, true, false) => LineEndingMode::Lf,
        (true, false, false) => LineEndingMode::Crlf,
        _ => LineEndingMode::Mixed,
    }
}

fn edit_file_no_change_if_unchanged(
    loaded: &EditFileLoadedTarget,
    computed: &EditFileComputedEdit,
) -> Option<Box<dyn ToolOutput>> {
    (loaded.text.as_bytes() == computed.final_text.as_bytes()).then(|| {
        edit_file_no_change_output(
            loaded.target.original_path.as_str(),
            loaded.target.resolved_path.as_path(),
        )
    })
}

fn edit_file_no_change_output(original_path: &str, resolved_path: &Path) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "no_change",
        "errorClass": "invalid_arguments",
        "message": "computed edit did not change file contents",
        "path": original_path,
        "resolved_path": resolved_path.display().to_string(),
        "retryableByModel": false,
        "retrySameArguments": false,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload)
            .unwrap_or_else(|_| "computed edit did not change file contents".to_owned()),
        false,
        payload,
    ))
}

fn edit_file_identical_no_change_output(
    original_path: &str,
    resolved_path: &Path,
) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "no_change",
        "errorClass": "invalid_arguments",
        "message": "old_string and new_string are identical",
        "path": original_path,
        "resolved_path": resolved_path.display().to_string(),
        "retryableByModel": false,
        "retrySameArguments": false,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload)
            .unwrap_or_else(|_| "old_string and new_string are identical".to_owned()),
        false,
        payload,
    ))
}

fn edit_file_not_found_output(original_path: &str, resolved_path: &Path) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "not_found",
        "errorClass": "invalid_arguments",
        "message": "old_string was not found in the current file",
        "path": original_path,
        "resolved_path": resolved_path.display().to_string(),
        "retryableByModel": true,
        "retrySameArguments": false,
        "suggestedTool": "read_file",
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload)
            .unwrap_or_else(|_| "old_string was not found in the current file".to_owned()),
        false,
        payload,
    ))
}

fn edit_file_ambiguous_match_output(
    original_path: &str,
    resolved_path: &Path,
    occurrences: usize,
) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "ambiguous_match",
        "errorClass": "invalid_arguments",
        "message": "old_string matched more than once; provide more surrounding context or set replace_all=true",
        "path": original_path,
        "resolved_path": resolved_path.display().to_string(),
        "matches": occurrences,
        "retryableByModel": true,
        "retrySameArguments": false,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| {
            "old_string matched more than once; provide more surrounding context or set replace_all=true"
                .to_owned()
        }),
        false,
        payload,
    ))
}

async fn read_current_file_state_for_tool(
    tool_name: &str,
    path: &Path,
    action_context: &str,
) -> Result<CurrentFileState, ToolError> {
    let metadata = tokio::fs::metadata(path).await.map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to stat {tool_name} target `{}` {action_context}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(ToolError::invalid_arguments(format!(
            "{tool_name} target `{}` is not a regular file",
            path.display()
        )));
    }

    let bytes = tokio::fs::read(path).await.map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to read {tool_name} target `{}` {action_context}: {error}",
            path.display()
        ))
    })?;

    Ok(CurrentFileState {
        bytes: bytes.len() as u64,
        sha256: sha256_hex(bytes.as_slice()),
        mtime_ms: metadata_mtime_ms(&metadata).unwrap_or_default(),
    })
}

fn read_required_output(input: FilePreconditionInput<'_>) -> Box<dyn ToolOutput> {
    let message = input.tool.read_required_message();
    let payload = serde_json::json!({
        "ok": false,
        "status": "read_required",
        "errorClass": "invalid_arguments",
        "message": message,
        "path": input.original_path,
        "resolved_path": input.resolved_path.display().to_string(),
        "retryableByModel": true,
        "retrySameArguments": false,
        "suggestedTool": "read_file",
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| message.to_owned()),
        false,
        payload,
    ))
}

fn precondition_failed_output(
    input: FilePreconditionInput<'_>,
    current: &CurrentFileState,
    observation: &FileObservation,
) -> Box<dyn ToolOutput> {
    let message = input.tool.stale_message();
    let payload = serde_json::json!({
        "ok": false,
        "status": "precondition_failed",
        "errorClass": "precondition_failed",
        "message": message,
        "path": input.original_path,
        "resolved_path": input.resolved_path.display().to_string(),
        "retryableByModel": true,
        "retrySameArguments": false,
        "observed_sha256": observation.sha256,
        "current_sha256": current.sha256,
        "observed_mtime_ms": observation.mtime_ms,
        "current_mtime_ms": current.mtime_ms,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| message.to_owned()),
        false,
        payload,
    ))
}

fn precondition_failed_output_for_expected(
    input: FilePreconditionInput<'_>,
    current: &CurrentFileState,
    expected_sha256: Option<&str>,
    expected_mtime_ms: Option<i64>,
) -> Box<dyn ToolOutput> {
    let message = input.tool.stale_message();
    let payload = serde_json::json!({
        "ok": false,
        "status": "precondition_failed",
        "errorClass": "precondition_failed",
        "message": message,
        "path": input.original_path,
        "resolved_path": input.resolved_path.display().to_string(),
        "retryableByModel": true,
        "retrySameArguments": false,
        "expected_sha256": expected_sha256,
        "current_sha256": current.sha256,
        "expected_mtime_ms": expected_mtime_ms,
        "current_mtime_ms": current.mtime_ms,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| message.to_owned()),
        false,
        payload,
    ))
}

async fn write_file_atomically(
    target: &WriteFileTarget,
    content: &str,
) -> Result<AtomicWriteResult, ToolError> {
    write_text_atomically_for_tool(
        FileMutationTool::WriteFile,
        target.resolved_path.as_path(),
        content,
        write_max_bytes(),
    )
    .await
}

async fn write_text_atomically_for_tool(
    tool: FileMutationTool,
    resolved_path: &Path,
    content: &str,
    max_bytes: usize,
) -> Result<AtomicWriteResult, ToolError> {
    let bytes = content.as_bytes();
    if bytes.len() > max_bytes {
        return Err(ToolError::invalid_arguments(format!(
            "{} is larger than {} limit ({max_bytes} bytes)",
            tool.text_payload_label(),
            tool.name()
        )));
    }

    let parent = resolved_path.parent().ok_or_else(|| {
        ToolError::invalid_arguments(format!(
            "{} target `{}` does not have a parent directory",
            tool.name(),
            resolved_path.display()
        ))
    })?;
    let (temp_path, mut temp_file) = create_write_temp_file(parent, resolved_path).await?;

    if let Err(error) = temp_file.write_all(bytes).await {
        cleanup_write_temp_file(temp_path.as_path()).await;
        return Err(ToolError::execution_failed(format!(
            "failed to write temporary file `{}`: {error}",
            temp_path.display()
        )));
    }

    if let Err(error) = temp_file.flush().await {
        cleanup_write_temp_file(temp_path.as_path()).await;
        return Err(ToolError::execution_failed(format!(
            "failed to flush temporary file `{}`: {error}",
            temp_path.display()
        )));
    }

    let _ = temp_file.sync_all().await;
    drop(temp_file);

    if let Err(error) = tokio::fs::rename(temp_path.as_path(), resolved_path).await {
        cleanup_write_temp_file(temp_path.as_path()).await;
        return Err(ToolError::execution_failed(format!(
            "failed to move temporary file `{}` to `{}`: {error}",
            temp_path.display(),
            resolved_path.display()
        )));
    }

    Ok(AtomicWriteResult {
        bytes_written: bytes.len() as u64,
        sha256: sha256_hex(bytes),
    })
}

async fn verify_written_file(
    target: &WriteFileTarget,
    expected: &AtomicWriteResult,
) -> Result<WriteVerification, ToolError> {
    verify_text_write_for_tool(
        FileMutationTool::WriteFile,
        target.original_path.as_str(),
        target.resolved_path.as_path(),
        expected,
    )
    .await
}

async fn verify_edited_file(
    loaded: &EditFileLoadedTarget,
    expected: &AtomicWriteResult,
) -> Result<WriteVerification, ToolError> {
    verify_text_write_for_tool(
        FileMutationTool::EditFile,
        loaded.target.original_path.as_str(),
        loaded.target.resolved_path.as_path(),
        expected,
    )
    .await
}

async fn verify_text_write_for_tool(
    tool: FileMutationTool,
    original_path: &str,
    resolved_path: &Path,
    expected: &AtomicWriteResult,
) -> Result<WriteVerification, ToolError> {
    let current = read_current_file_state_for_tool(
        tool.name(),
        resolved_path,
        tool.current_state_action_context(),
    )
    .await?;
    if current.bytes != expected.bytes_written || current.sha256 != expected.sha256 {
        return Ok(WriteVerification::Failed(verification_failed_output(
            tool,
            original_path,
            resolved_path,
            expected,
            &current,
        )));
    }

    Ok(WriteVerification::Verified(current))
}

fn verification_failed_output(
    tool: FileMutationTool,
    original_path: &str,
    resolved_path: &Path,
    expected: &AtomicWriteResult,
    current: &CurrentFileState,
) -> Box<dyn ToolOutput> {
    let message = tool.verification_failed_message();
    let payload = serde_json::json!({
        "ok": false,
        "status": "verification_failed",
        "errorClass": "execution_failed",
        "message": message,
        "path": original_path,
        "resolved_path": resolved_path.display().to_string(),
        "expected_bytes": expected.bytes_written,
        "actual_bytes": current.bytes,
        "expected_sha256": expected.sha256,
        "actual_sha256": current.sha256,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| message.to_owned()),
        false,
        payload,
    ))
}

fn record_file_mutation_observation(
    observation_store: &FileObservationStore,
    tool: FileMutationTool,
    call_id: &str,
    resolved_path: &Path,
    current: &CurrentFileState,
) -> FileObservation {
    let observation = FileObservation {
        id: format!("{}:{call_id}", tool.name()),
        resolved_path: resolved_path.to_path_buf(),
        bytes: current.bytes,
        sha256: current.sha256.clone(),
        mtime_ms: current.mtime_ms,
        complete: true,
        source_tool_call_id: call_id.to_owned(),
    };
    observation_store.record(observation.clone());
    observation
}

fn write_file_success_output(
    target: &WriteFileTarget,
    current: &CurrentFileState,
    observation: FileObservation,
) -> Box<dyn ToolOutput> {
    let body = format!(
        "write_file completed: {} {}, {} bytes.",
        target.operation.as_str(),
        target.resolved_path.display(),
        current.bytes
    );
    let created_dirs = target
        .created_dirs
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    let changed_files = vec![target.resolved_path.display().to_string()];
    let payload = serde_json::json!({
        "ok": true,
        "status": "completed",
        "operation": target.operation.as_str(),
        "path": target.original_path,
        "resolved_path": target.resolved_path.display().to_string(),
        "bytes_written": current.bytes,
        "sha256": current.sha256,
        "file_observation": observation,
        "created_dirs": created_dirs,
        "changed_files": changed_files,
    });

    Box::new(FunctionToolOutput::with_payload(body, true, payload))
}

fn edit_file_success_output(
    loaded: &EditFileLoadedTarget,
    args: &EditFileArgs,
    raw_match: &EditFileRawMatch,
    computed: &EditFileComputedEdit,
    current: &CurrentFileState,
    observation: FileObservation,
) -> Box<dyn ToolOutput> {
    let body = format!(
        "edit_file completed: edited {}, replaced {} occurrence{}, {} bytes.",
        loaded.target.resolved_path.display(),
        computed.matches_replaced,
        if computed.matches_replaced == 1 {
            ""
        } else {
            "s"
        },
        current.bytes
    );
    let changed_files = vec![loaded.target.resolved_path.display().to_string()];
    let payload = serde_json::json!({
        "ok": true,
        "status": "completed",
        "operation": "edited",
        "path": loaded.target.original_path,
        "resolved_path": loaded.target.resolved_path.display().to_string(),
        "matches": raw_match.occurrences,
        "matches_replaced": computed.matches_replaced,
        "replace_all": args.replace_all.unwrap_or(false),
        "bytes_before": loaded.current.bytes,
        "bytes_after": current.bytes,
        "bytes_written": current.bytes,
        "sha256_before": loaded.current.sha256,
        "sha256": current.sha256,
        "old_string_bytes": args.old_string.as_bytes().len(),
        "new_string_bytes": args.new_string.as_bytes().len(),
        "old_string_sha256": sha256_hex(args.old_string.as_bytes()),
        "new_string_sha256": sha256_hex(args.new_string.as_bytes()),
        "line_ending_mode": raw_match.source.line_ending_mode(),
        "file_observation": observation,
        "changed_files": changed_files,
    });

    Box::new(FunctionToolOutput::with_payload(body, true, payload))
}

async fn create_write_temp_file(
    parent: &Path,
    target_path: &Path,
) -> Result<(PathBuf, tokio::fs::File), ToolError> {
    for attempt in 0..16 {
        let temp_path = write_temp_path(parent, target_path, attempt);
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        match options.open(temp_path.as_path()).await {
            Ok(file) => return Ok((temp_path, file)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(ToolError::execution_failed(format!(
                    "failed to create temporary file `{}`: {error}",
                    temp_path.display()
                )));
            }
        }
    }

    Err(ToolError::execution_failed(format!(
        "failed to create unique temporary file for `{}`",
        target_path.display()
    )))
}

fn write_temp_path(parent: &Path, target_path: &Path, attempt: u8) -> PathBuf {
    let target_name = target_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("file");
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    parent.join(format!(
        ".{target_name}.pioneer-write-{}-{now_nanos}-{attempt}.tmp",
        std::process::id()
    ))
}

async fn cleanup_write_temp_file(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

fn resolve_path(base: &Path, requested: &str) -> PathBuf {
    let path = Path::new(requested);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn normalize_path_lexically(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(_) | Component::RootDir | Component::Prefix(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }

    normalized
}

fn resolve_path_within_workdir(base: &Path, requested: &str) -> Result<PathBuf, ToolError> {
    let base = base.canonicalize().unwrap_or_else(|_| base.to_path_buf());
    let requested_path = Path::new(requested);
    let mut candidate = if requested_path.is_absolute() {
        requested_path.to_path_buf()
    } else {
        base.clone()
    };

    if requested_path.is_absolute() {
        if !requested_path.starts_with(base.as_path()) {
            return Err(ToolError::invalid_arguments(format!(
                "`path` must stay inside workspace `{}`",
                base.display()
            )));
        }
    } else {
        for component in requested_path.components() {
            match component {
                Component::CurDir => {}
                Component::Normal(part) => candidate.push(part),
                Component::ParentDir => {
                    if candidate == base
                        || !candidate.pop()
                        || !candidate.starts_with(base.as_path())
                    {
                        return Err(ToolError::invalid_arguments(
                            "`path` must not traverse outside the workspace",
                        ));
                    }
                }
                Component::RootDir | Component::Prefix(_) => {
                    return Err(ToolError::invalid_arguments(
                        "`path` must be relative to the workspace or inside it",
                    ));
                }
            }
        }
    }

    if !candidate.starts_with(base.as_path()) {
        return Err(ToolError::invalid_arguments(format!(
            "`path` must stay inside workspace `{}`",
            base.display()
        )));
    }
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{ToolCallSource, ToolPayload};
    use crate::events::ToolEventBus;
    use crate::spec::ToolRecoveryMetadata;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::fs;

    fn grep_invocation(workdir: PathBuf, arguments: JsonValue) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_1".to_owned(),
            tool_name: "grep_files".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function { arguments },
            workdir,
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn read_invocation(call_id: &str, workdir: PathBuf, arguments: JsonValue) -> ToolInvocation {
        ToolInvocation {
            call_id: call_id.to_owned(),
            tool_name: "read_file".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function { arguments },
            workdir,
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn write_invocation(call_id: &str, workdir: PathBuf, arguments: JsonValue) -> ToolInvocation {
        ToolInvocation {
            call_id: call_id.to_owned(),
            tool_name: "write_file".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function { arguments },
            workdir,
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn edit_invocation(call_id: &str, workdir: PathBuf, arguments: JsonValue) -> ToolInvocation {
        ToolInvocation {
            call_id: call_id.to_owned(),
            tool_name: "edit_file".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function { arguments },
            workdir,
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn trace() -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", "call_1", "grep_files")
    }

    fn read_trace(call_id: &str) -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", call_id, "read_file")
    }

    fn write_trace(call_id: &str) -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", call_id, "write_file")
    }

    fn edit_trace(call_id: &str) -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", call_id, "edit_file")
    }

    fn temp_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "pioneer-tools-{name}-{}-{suffix}",
            std::process::id()
        ))
    }

    #[test]
    fn file_observation_sha256_helper_is_stable() {
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn file_observation_mtime_helper_converts_epoch_millis() {
        let time = UNIX_EPOCH + std::time::Duration::from_millis(1_234);

        assert_eq!(system_time_mtime_ms(time), Some(1_234));
    }

    #[test]
    fn file_observation_serializes_payload_shape() {
        let observation = FileObservation {
            id: "read_file:call_1".to_owned(),
            resolved_path: PathBuf::from("/tmp/example.txt"),
            bytes: 5,
            sha256: sha256_hex(b"hello"),
            mtime_ms: 1_234,
            complete: true,
            source_tool_call_id: "call_1".to_owned(),
        };

        let json = serde_json::to_value(&observation).expect("observation serializes");

        assert_eq!(json["id"], "read_file:call_1");
        assert_eq!(json["path"], "/tmp/example.txt");
        assert_eq!(json["bytes"], 5);
        assert_eq!(json["mtime_ms"], 1_234);
        assert_eq!(json["complete"], true);
        assert_eq!(json["source_tool_call_id"], "call_1");
    }

    #[test]
    fn file_observation_metadata_mtime_helper_reads_file_metadata() {
        let temp = temp_path("mtime");
        std::fs::write(temp.as_path(), "hello").expect("temp file should write");
        let metadata = std::fs::metadata(temp.as_path()).expect("metadata should load");
        let mtime_ms = metadata_mtime_ms(&metadata).expect("mtime should convert");

        assert!(mtime_ms > 0);
        let _ = std::fs::remove_file(temp);
    }

    #[tokio::test]
    async fn current_file_state_helper_reads_regular_file_state() {
        let temp = temp_path("current-state");
        fs::write(temp.as_path(), b"hello")
            .await
            .expect("temp file should write");

        let state = read_current_file_state_for_tool("edit_file", temp.as_path(), "before edit")
            .await
            .expect("current state should read");

        assert_eq!(state.bytes, 5);
        assert_eq!(state.sha256, sha256_hex(b"hello"));
        assert!(state.mtime_ms > 0);
        let _ = fs::remove_file(temp).await;
    }

    #[tokio::test]
    async fn current_file_state_helper_reports_missing_with_tool_context() {
        let missing = temp_path("current-state-missing");

        let error = read_current_file_state_for_tool("edit_file", missing.as_path(), "before edit")
            .await
            .expect_err("missing file should fail");

        assert!(
            error
                .to_string()
                .contains("failed to stat edit_file target")
        );
        assert!(error.to_string().contains("before edit"));
    }

    #[tokio::test]
    async fn current_file_state_helper_rejects_directory_with_tool_context() {
        let temp = temp_path("current-state-directory");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp directory should create");

        let error = read_current_file_state_for_tool("edit_file", temp.as_path(), "before edit")
            .await
            .expect_err("directory should fail");

        assert!(error.to_string().contains("edit_file target"));
        assert!(error.to_string().contains("is not a regular file"));
        let _ = fs::remove_dir_all(temp).await;
    }

    #[test]
    fn file_mutation_tool_model_provides_tool_specific_precondition_messages() {
        assert_eq!(FileMutationTool::WriteFile.name(), "write_file");
        assert_eq!(FileMutationTool::EditFile.name(), "edit_file");
        assert!(
            FileMutationTool::WriteFile
                .read_required_message()
                .contains("overwrite")
        );
        assert!(
            FileMutationTool::EditFile
                .read_required_message()
                .contains("modify")
        );
        assert!(
            FileMutationTool::WriteFile
                .stale_message()
                .contains("write_file")
        );
        assert!(
            FileMutationTool::EditFile
                .stale_message()
                .contains("edit_file")
        );
    }

    #[test]
    fn file_precondition_input_tracks_explicit_preconditions() {
        let path = Path::new("/tmp/example.txt");
        let without_explicit = FilePreconditionInput {
            tool: FileMutationTool::EditFile,
            original_path: "example.txt",
            resolved_path: path,
            read_observation_id: None,
            expected_sha256: None,
            expected_mtime_ms: None,
        };
        let with_sha = FilePreconditionInput {
            expected_sha256: Some(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            ..without_explicit
        };
        let with_mtime = FilePreconditionInput {
            expected_mtime_ms: Some(1_234),
            ..without_explicit
        };

        assert!(!without_explicit.has_explicit_precondition());
        assert!(with_sha.has_explicit_precondition());
        assert!(with_mtime.has_explicit_precondition());
        assert_eq!(without_explicit.tool, FileMutationTool::EditFile);
        assert_eq!(without_explicit.original_path, "example.txt");
        assert_eq!(without_explicit.resolved_path, path);
    }

    fn test_observation(id: &str, path: &str, complete: bool, content: &[u8]) -> FileObservation {
        FileObservation {
            id: id.to_owned(),
            resolved_path: PathBuf::from(path),
            bytes: content.len() as u64,
            sha256: sha256_hex(content),
            mtime_ms: 1_234,
            complete,
            source_tool_call_id: id
                .split_once(':')
                .map(|(_, suffix)| suffix)
                .unwrap_or(id)
                .to_owned(),
        }
    }

    fn write_args(path: &str, content: &str) -> WriteFileArgs {
        WriteFileArgs {
            path: path.to_owned(),
            content: content.to_owned(),
            create_dirs: None,
            overwrite: None,
            read_observation_id: None,
            expected_sha256: None,
            expected_mtime_ms: None,
        }
    }

    async fn observation_for_file(id: &str, path: &Path) -> FileObservation {
        let bytes = fs::read(path).await.expect("file should read");
        let metadata = fs::metadata(path).await.expect("metadata should load");

        FileObservation {
            id: id.to_owned(),
            resolved_path: path.to_path_buf(),
            bytes: bytes.len() as u64,
            sha256: sha256_hex(bytes.as_slice()),
            mtime_ms: metadata_mtime_ms(&metadata).unwrap_or_default(),
            complete: true,
            source_tool_call_id: id
                .split_once(':')
                .map(|(_, suffix)| suffix)
                .unwrap_or(id)
                .to_owned(),
        }
    }

    async fn write_temp_file_count(parent: &Path, target_name: &str) -> usize {
        let prefix = format!(".{target_name}.pioneer-write-");
        let mut count = 0usize;
        let mut entries = fs::read_dir(parent).await.expect("temp parent should list");
        while let Some(entry) = entries
            .next_entry()
            .await
            .expect("temp parent entry should read")
        {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(prefix.as_str())
            {
                count = count.saturating_add(1);
            }
        }
        count
    }

    #[test]
    fn file_observation_store_returns_latest_complete_for_path() {
        let store = FileObservationStore::default();
        let path = PathBuf::from("/tmp/example.txt");

        store.record(test_observation(
            "read_file:call_1",
            "/tmp/example.txt",
            true,
            b"old",
        ));
        store.record(test_observation(
            "read_file:call_2",
            "/tmp/example.txt",
            true,
            b"new",
        ));

        let observation = store
            .latest_complete_for_path(path.as_path())
            .expect("latest complete observation should exist");

        assert_eq!(observation.id, "read_file:call_2");
        assert_eq!(observation.sha256, sha256_hex(b"new"));
    }

    #[test]
    fn file_observation_store_partial_observations_do_not_authorize_overwrite() {
        let store = FileObservationStore::default();
        let path = PathBuf::from("/tmp/example.txt");

        store.record(test_observation(
            "read_file:partial",
            "/tmp/example.txt",
            false,
            b"partial",
        ));

        assert!(store.latest_complete_for_path(path.as_path()).is_none());
        assert!(
            store
                .complete_by_id_for_path("read_file:partial", path.as_path())
                .is_none()
        );
    }

    #[test]
    fn file_observation_store_id_lookup_requires_same_resolved_path() {
        let store = FileObservationStore::default();
        store.record(test_observation(
            "read_file:call_1",
            "/tmp/example.txt",
            true,
            b"hello",
        ));

        assert!(
            store
                .complete_by_id_for_path("read_file:call_1", Path::new("/tmp/example.txt"))
                .is_some()
        );
        assert!(
            store
                .complete_by_id_for_path("read_file:call_1", Path::new("/tmp/other.txt"))
                .is_none()
        );
    }

    #[test]
    fn read_file_handler_accepts_shared_observation_store() {
        let store = Arc::new(FileObservationStore::default());
        let handler = ReadFileHandler::new(store.clone());

        assert!(Arc::ptr_eq(&handler.observation_store, &store));
    }

    #[test]
    fn write_file_args_parse_valid_payload() {
        let args = parse_write_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "file.txt",
                "content": "hello",
                "create_dirs": true,
                "overwrite": true,
                "read_observation_id": "read_file:call_1",
                "expected_sha256": sha256_hex(b"old"),
                "expected_mtime_ms": 1234
            }),
        })
        .expect("valid args should parse");

        assert_eq!(args.path, "file.txt");
        assert_eq!(args.content, "hello");
        assert_eq!(args.create_dirs, Some(true));
        assert_eq!(args.overwrite, Some(true));
        assert_eq!(
            args.read_observation_id.as_deref(),
            Some("read_file:call_1")
        );
        assert_eq!(
            args.expected_sha256.as_deref(),
            Some(sha256_hex(b"old")).as_deref()
        );
        assert_eq!(args.expected_mtime_ms, Some(1234));
    }

    #[tokio::test]
    async fn write_file_handler_rejects_missing_path() {
        let error = match WriteFileHandler::default()
            .handle(
                write_invocation(
                    "write_missing_path",
                    PathBuf::from("/tmp"),
                    serde_json::json!({ "content": "hello" }),
                ),
                write_trace("write_missing_path"),
            )
            .await
        {
            Ok(_) => panic!("missing path should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("missing field `path`"));
    }

    #[tokio::test]
    async fn write_file_handler_rejects_empty_path() {
        let error = match WriteFileHandler::default()
            .handle(
                write_invocation(
                    "write_empty_path",
                    PathBuf::from("/tmp"),
                    serde_json::json!({ "path": "   ", "content": "hello" }),
                ),
                write_trace("write_empty_path"),
            )
            .await
        {
            Ok(_) => panic!("empty path should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("`path` must not be empty"));
    }

    #[tokio::test]
    async fn write_file_handler_rejects_missing_content() {
        let error = match WriteFileHandler::default()
            .handle(
                write_invocation(
                    "write_missing_content",
                    PathBuf::from("/tmp"),
                    serde_json::json!({ "path": "file.txt" }),
                ),
                write_trace("write_missing_content"),
            )
            .await
        {
            Ok(_) => panic!("missing content should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("missing field `content`"));
    }

    #[tokio::test]
    async fn write_file_target_validation_resolves_relative_create_path() {
        let temp = temp_path("write-relative");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let args = write_args("./nested/../file.txt", "hello");

        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        assert_eq!(target.original_path, "./nested/../file.txt");
        assert_eq!(target.resolved_path, temp.join("file.txt"));
        assert_eq!(target.operation, WriteFileOperation::Created);
        assert!(target.created_dirs.is_empty());
        assert!(!target.resolved_path.exists());

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_target_validation_creates_missing_parent_by_default() {
        let temp = temp_path("write-parent");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let args = write_args("nested/file.txt", "hello");

        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        assert_eq!(target.resolved_path, temp.join("nested/file.txt"));
        assert_eq!(target.operation, WriteFileOperation::Created);
        assert_eq!(target.created_dirs, vec![temp.join("nested")]);
        assert!(temp.join("nested").is_dir());
        assert!(!target.resolved_path.exists());

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_target_validation_rejects_missing_parent_when_create_dirs_false() {
        let temp = temp_path("write-missing-parent");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let mut args = write_args("nested/file.txt", "hello");
        args.create_dirs = Some(false);

        let error = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect_err("missing parent should fail");

        assert!(error.to_string().contains("create_dirs=false"));
        assert!(!temp.join("nested").exists());

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_target_validation_rejects_directory_target() {
        let temp = temp_path("write-dir-target");
        fs::create_dir_all(temp.join("target").as_path())
            .await
            .expect("target dir should create");
        let args = write_args("target", "hello");

        let error = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect_err("directory target should fail");

        assert!(error.to_string().contains("is a directory"));

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_target_validation_rejects_existing_file_when_overwrite_false() {
        let temp = temp_path("write-overwrite-false");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "old")
            .await
            .expect("file should write");
        let mut args = write_args("file.txt", "new");
        args.overwrite = Some(false);

        let error = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect_err("overwrite=false should fail");

        assert!(error.to_string().contains("overwrite=false"));

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_existing_requires_read_observation() {
        let temp = temp_path("write-read-required");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "old")
            .await
            .expect("file should write");

        let output = WriteFileHandler::default()
            .handle(
                write_invocation(
                    "write_read_required",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "content": "new" }),
                ),
                write_trace("write_read_required"),
            )
            .await
            .expect("read_required should be a structured tool output");
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("read_required")
        );
        assert_eq!(
            json.get("suggestedTool").and_then(JsonValue::as_str),
            Some("read_file")
        );
        assert_eq!(
            fs::read_to_string(temp.join("file.txt"))
                .await
                .expect("file should read"),
            "old"
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_existing_allows_complete_observation() {
        let temp = temp_path("write-complete-observation");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = FileObservationStore::default();
        store.record(observation_for_file("read_file:complete", target_path.as_path()).await);
        let args = write_args("file.txt", "new");
        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        let output = verify_existing_file_preconditions(&store, &target, &args)
            .await
            .expect("preconditions should validate");

        assert!(output.is_none());

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_existing_rejects_partial_observation() {
        let temp = temp_path("write-partial-observation");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = FileObservationStore::default();
        store.record(FileObservation {
            complete: false,
            ..observation_for_file("read_file:partial", target_path.as_path()).await
        });
        let mut args = write_args("file.txt", "new");
        args.read_observation_id = Some("read_file:partial".to_owned());
        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        let output = verify_existing_file_preconditions(&store, &target, &args)
            .await
            .expect("partial observation should produce structured output")
            .expect("partial observation should not authorize overwrite");
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("read_required")
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_existing_rejects_stale_file_after_read() {
        let temp = temp_path("write-stale-observation");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = FileObservationStore::default();
        store.record(observation_for_file("read_file:stale", target_path.as_path()).await);
        fs::write(target_path.as_path(), "changed")
            .await
            .expect("file should change");
        let args = write_args("file.txt", "new");
        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        let output = verify_existing_file_preconditions(&store, &target, &args)
            .await
            .expect("stale observation should produce structured output")
            .expect("stale observation should not authorize overwrite");
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("precondition_failed")
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("file should read"),
            "changed"
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_existing_expected_sha256_mismatch() {
        let temp = temp_path("write-sha-mismatch");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "old")
            .await
            .expect("file should write");
        let mut args = write_args("file.txt", "new");
        args.expected_sha256 = Some(sha256_hex(b"different"));
        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        let output =
            verify_existing_file_preconditions(&FileObservationStore::default(), &target, &args)
                .await
                .expect("sha mismatch should produce structured output")
                .expect("sha mismatch should not authorize overwrite");
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("precondition_failed")
        );
        assert_eq!(
            json.get("expected_sha256").and_then(JsonValue::as_str),
            Some(sha256_hex(b"different")).as_deref()
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_existing_allows_matching_expected_sha256() {
        let temp = temp_path("write-sha-match");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "old")
            .await
            .expect("file should write");
        let mut args = write_args("file.txt", "new");
        args.expected_sha256 = Some(sha256_hex(b"old"));
        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        let output =
            verify_existing_file_preconditions(&FileObservationStore::default(), &target, &args)
                .await
                .expect("matching sha should validate");

        assert!(output.is_none());

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_existing_expected_mtime_mismatch() {
        let temp = temp_path("write-mtime-mismatch");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "old")
            .await
            .expect("file should write");
        let mut args = write_args("file.txt", "new");
        args.expected_mtime_ms = Some(-1);
        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        let output =
            verify_existing_file_preconditions(&FileObservationStore::default(), &target, &args)
                .await
                .expect("mtime mismatch should produce structured output")
                .expect("mtime mismatch should not authorize overwrite");
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("precondition_failed")
        );
        assert_eq!(
            json.get("expected_mtime_ms").and_then(JsonValue::as_i64),
            Some(-1)
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_existing_allows_matching_expected_mtime() {
        let temp = temp_path("write-mtime-match");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let current = read_current_file_state_for_tool(
            FileMutationTool::WriteFile.name(),
            target_path.as_path(),
            FileMutationTool::WriteFile.current_state_action_context(),
        )
        .await
        .expect("current state should read");
        let mut args = write_args("file.txt", "new");
        args.expected_mtime_ms = Some(current.mtime_ms);
        let target = prepare_write_file_target(temp.as_path(), &args)
            .await
            .expect("target should validate");

        let output =
            verify_existing_file_preconditions(&FileObservationStore::default(), &target, &args)
                .await
                .expect("matching mtime should validate");

        assert!(output.is_none());

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn current_file_state_helper_preserves_write_file_error_wording() {
        let missing = temp_path("write-current-state-missing");

        let error = read_current_file_state_for_tool(
            FileMutationTool::WriteFile.name(),
            missing.as_path(),
            FileMutationTool::WriteFile.current_state_action_context(),
        )
        .await
        .expect_err("missing file should fail");

        assert!(
            error
                .to_string()
                .contains("failed to stat write_file target")
        );
        assert!(error.to_string().contains("before overwrite"));
    }

    #[test]
    fn write_file_rejects_invalid_expected_sha256() {
        let error = parse_write_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "file.txt",
                "content": "new",
                "expected_sha256": "not-a-sha"
            }),
        })
        .expect_err("invalid sha should fail");

        assert!(error.to_string().contains("expected_sha256"));
    }

    #[test]
    fn edit_file_args_parse_valid_payload() {
        let args = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "file.txt",
                "old_string": "old",
                "new_string": "new",
                "replace_all": true,
                "read_observation_id": "read_file:call_1",
                "expected_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "expected_mtime_ms": 1_234
            }),
        })
        .expect("valid edit_file args should parse");

        assert_eq!(args.path, "file.txt");
        assert_eq!(args.old_string, "old");
        assert_eq!(args.new_string, "new");
        assert_eq!(args.replace_all, Some(true));
        assert_eq!(
            args.read_observation_id.as_deref(),
            Some("read_file:call_1")
        );
        assert_eq!(
            args.expected_sha256.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(args.expected_mtime_ms, Some(1_234));
    }

    #[test]
    fn edit_file_args_reject_missing_path() {
        let error = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "old_string": "old",
                "new_string": "new"
            }),
        })
        .expect_err("missing path should fail");

        assert!(error.to_string().contains("path"));
    }

    #[test]
    fn edit_file_args_reject_empty_path() {
        let error = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "   ",
                "old_string": "old",
                "new_string": "new"
            }),
        })
        .expect_err("empty path should fail");

        assert!(
            error
                .to_string()
                .contains("edit_file `path` must not be empty")
        );
    }

    #[test]
    fn edit_file_args_reject_missing_old_string() {
        let error = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "file.txt",
                "new_string": "new"
            }),
        })
        .expect_err("missing old_string should fail");

        assert!(error.to_string().contains("old_string"));
    }

    #[test]
    fn edit_file_args_reject_empty_old_string() {
        let error = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "file.txt",
                "old_string": "",
                "new_string": "new"
            }),
        })
        .expect_err("empty old_string should fail");

        assert!(
            error
                .to_string()
                .contains("edit_file `old_string` must not be empty")
        );
    }

    #[test]
    fn edit_file_args_reject_missing_new_string() {
        let error = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "file.txt",
                "old_string": "old"
            }),
        })
        .expect_err("missing new_string should fail");

        assert!(error.to_string().contains("new_string"));
    }

    #[test]
    fn edit_file_args_allow_identical_old_and_new_for_structured_no_change() {
        let args = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "file.txt",
                "old_string": "same",
                "new_string": "same"
            }),
        })
        .expect("identical strings should parse so handler can return structured no_change");

        assert_eq!(args.old_string, "same");
        assert_eq!(args.new_string, "same");
    }

    #[test]
    fn edit_file_args_reject_invalid_expected_sha256() {
        let error = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "file.txt",
                "old_string": "old",
                "new_string": "new",
                "expected_sha256": "bad"
            }),
        })
        .expect_err("invalid expected_sha256 should fail");

        assert!(error.to_string().contains("edit_file `expected_sha256`"));
        assert!(error.to_string().contains("64-character hex"));
    }

    #[tokio::test]
    async fn edit_file_atomic_write_persists_exact_final_contents() {
        let temp = temp_path("edit-atomic-write");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:edit_atomic", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_atomic_write",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_atomic_write"),
            )
            .await
            .expect("edit_file should write through atomic helper");
        let json = output.raw_json();

        assert!(output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("completed")
        );
        assert_eq!(
            json.get("operation").and_then(JsonValue::as_str),
            Some("edited")
        );
        assert_eq!(
            json.get("path").and_then(JsonValue::as_str),
            Some("file.txt")
        );
        assert_eq!(
            json.get("matches_replaced").and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            json.get("replace_all").and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            json.get("bytes_before").and_then(JsonValue::as_u64),
            Some("old".len() as u64)
        );
        assert_eq!(
            json.get("bytes_after").and_then(JsonValue::as_u64),
            Some("new".len() as u64)
        );
        assert_eq!(
            json.get("bytes_written").and_then(JsonValue::as_u64),
            Some("new".len() as u64)
        );
        assert_eq!(
            json.get("sha256_before").and_then(JsonValue::as_str),
            Some(sha256_hex(b"old").as_str())
        );
        assert_eq!(
            json.get("sha256").and_then(JsonValue::as_str),
            Some(sha256_hex(b"new").as_str())
        );
        assert_eq!(
            json.get("old_string_bytes").and_then(JsonValue::as_u64),
            Some("old".len() as u64)
        );
        assert_eq!(
            json.get("new_string_bytes").and_then(JsonValue::as_u64),
            Some("new".len() as u64)
        );
        assert_eq!(
            json.get("old_string_sha256").and_then(JsonValue::as_str),
            Some(sha256_hex(b"old").as_str())
        );
        assert_eq!(
            json.get("new_string_sha256").and_then(JsonValue::as_str),
            Some(sha256_hex(b"new").as_str())
        );
        assert_eq!(
            json.get("line_ending_mode").and_then(JsonValue::as_str),
            Some("none")
        );
        let changed_files = json
            .get("changed_files")
            .and_then(JsonValue::as_array)
            .expect("changed_files should be array");
        let expected_changed_file = target_path.display().to_string();
        assert_eq!(changed_files.len(), 1);
        assert_eq!(
            changed_files[0].as_str(),
            Some(expected_changed_file.as_str())
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "new"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_atomic_write_rejects_result_over_limit() {
        let temp = temp_path("edit-result-over-limit");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store
            .record(observation_for_file("read_file:edit_over_limit", target_path.as_path()).await);
        let oversized_content = "x".repeat(edit_max_file_bytes().saturating_add(1));

        let error = match EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_over_limit",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": oversized_content
                    }),
                ),
                edit_trace("edit_over_limit"),
            )
            .await
        {
            Ok(_) => panic!("oversized edit result should fail"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("edit_file result is larger than edit_file limit")
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "old"
        );
        assert_eq!(write_temp_file_count(temp.as_path(), "file.txt").await, 0);
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_identical_old_new_returns_structured_no_change_without_read_or_write() {
        let temp = temp_path("edit-identical-no-change");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "same")
            .await
            .expect("file should write");

        let output = EditFileHandler::default()
            .handle(
                edit_invocation(
                    "edit_identical",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "same",
                        "new_string": "same"
                    }),
                ),
                edit_trace("edit_identical"),
            )
            .await
            .expect("identical strings should return structured output");
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("no_change")
        );
        assert_eq!(
            json.get("message").and_then(JsonValue::as_str),
            Some("old_string and new_string are identical")
        );
        assert_eq!(
            json.get("retryableByModel").and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            json.get("retrySameArguments").and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "same"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_success_output_does_not_echo_old_new_or_final_content() {
        let temp = temp_path("edit-no-echo");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        let old_secret = "OLD_SECRET_EDIT_FILE_SENTINEL_1974";
        let new_secret = "NEW_SECRET_EDIT_FILE_SENTINEL_9832";
        let initial_content = format!("prefix {old_secret} suffix");
        let final_content = format!("prefix {new_secret} suffix");
        fs::write(target_path.as_path(), initial_content.as_str())
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:edit_no_echo", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_no_echo",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": old_secret,
                        "new_string": new_secret
                    }),
                ),
                edit_trace("edit_no_echo"),
            )
            .await
            .expect("edit_file should succeed");
        let raw_json = output.raw_json().to_string();

        assert!(output.success());
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            final_content
        );
        assert!(!output.raw_text().contains(old_secret));
        assert!(!output.raw_text().contains(new_secret));
        assert!(!output.raw_text().contains(final_content.as_str()));
        assert!(!raw_json.contains(old_secret));
        assert!(!raw_json.contains(new_secret));
        assert!(!raw_json.contains(final_content.as_str()));
        assert_eq!(
            output
                .raw_json()
                .get("old_string_bytes")
                .and_then(JsonValue::as_u64),
            Some(old_secret.len() as u64)
        );
        assert_eq!(
            output
                .raw_json()
                .get("new_string_bytes")
                .and_then(JsonValue::as_u64),
            Some(new_secret.len() as u64)
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_observation_store_updates_and_authorizes_followup_edit() {
        let temp = temp_path("edit-store-update");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "first")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(
            observation_for_file("read_file:edit_store_initial", target_path.as_path()).await,
        );
        let handler = EditFileHandler::new(Arc::clone(&store));

        let first_output = handler
            .handle(
                edit_invocation(
                    "edit_store_first",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "first",
                        "new_string": "second"
                    }),
                ),
                edit_trace("edit_store_first"),
            )
            .await
            .expect("first edit should succeed");
        let first_json = first_output.raw_json();

        assert!(first_output.success());
        assert_eq!(
            first_json
                .get("file_observation")
                .and_then(|value| value.get("id"))
                .and_then(JsonValue::as_str),
            Some("edit_file:edit_store_first")
        );
        let latest = store
            .latest_complete_for_path(target_path.as_path())
            .expect("edit observation should be latest");
        assert_eq!(latest.id, "edit_file:edit_store_first");
        assert_eq!(latest.bytes, "second".len() as u64);
        assert_eq!(latest.sha256, sha256_hex(b"second"));

        let second_output = handler
            .handle(
                edit_invocation(
                    "edit_store_second",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "second",
                        "new_string": "third"
                    }),
                ),
                edit_trace("edit_store_second"),
            )
            .await
            .expect("second edit should use first edit observation");

        assert!(second_output.success());
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "third"
        );
        assert_eq!(
            store
                .latest_complete_for_path(target_path.as_path())
                .expect("second edit observation should be latest")
                .id,
            "edit_file:edit_store_second"
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_requires_latest_complete_observation() {
        let temp = temp_path("edit-read-required");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");

        let output = EditFileHandler::default()
            .handle(
                edit_invocation(
                    "edit_read_required",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_read_required"),
            )
            .await
            .expect("read_required should be a structured output");
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("read_required")
        );
        assert_eq!(
            json.get("message").and_then(JsonValue::as_str),
            Some(
                "edit_file cannot modify an existing file until read_file has observed the complete current file"
            )
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "old"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_rejects_partial_observation() {
        let temp = temp_path("edit-partial-observation");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(FileObservation {
            complete: false,
            ..observation_for_file("read_file:partial", target_path.as_path()).await
        });

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_partial",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_partial"),
            )
            .await
            .expect("partial observation should return structured output");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("read_required")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_rejects_stale_file_after_read() {
        let temp = temp_path("edit-stale-observation");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:stale", target_path.as_path()).await);
        fs::write(target_path.as_path(), "changed")
            .await
            .expect("file should change");

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_stale",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "changed",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_stale"),
            )
            .await
            .expect("stale observation should return structured output");
        let json = output.raw_json();

        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("precondition_failed")
        );
        assert_eq!(
            json.get("message").and_then(JsonValue::as_str),
            Some("file changed before edit_file could modify it")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_allows_matching_read_observation_id() {
        let temp = temp_path("edit-observation-id-match");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:explicit", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_observation_id",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "read_observation_id": "read_file:explicit"
                    }),
                ),
                edit_trace("edit_observation_id"),
            )
            .await
            .expect("matching observation id should succeed");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("completed")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_rejects_read_observation_id_for_wrong_path() {
        let temp = temp_path("edit-observation-id-wrong-path");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        let other_path = temp.join("other.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        fs::write(other_path.as_path(), "other")
            .await
            .expect("other file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:other", other_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_observation_wrong_path",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "read_observation_id": "read_file:other"
                    }),
                ),
                edit_trace("edit_observation_wrong_path"),
            )
            .await
            .expect("wrong-path observation id should return structured output");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("read_required")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_rejects_partial_read_observation_id() {
        let temp = temp_path("edit-observation-id-partial");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(FileObservation {
            complete: false,
            ..observation_for_file("read_file:partial_id", target_path.as_path()).await
        });

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_observation_partial",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "read_observation_id": "read_file:partial_id"
                    }),
                ),
                edit_trace("edit_observation_partial"),
            )
            .await
            .expect("partial observation id should return structured output");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("read_required")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_allows_matching_expected_sha256() {
        let temp = temp_path("edit-sha-match");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");

        let output = EditFileHandler::default()
            .handle(
                edit_invocation(
                    "edit_sha_match",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "expected_sha256": sha256_hex(b"old")
                    }),
                ),
                edit_trace("edit_sha_match"),
            )
            .await
            .expect("matching sha should succeed");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("completed")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_rejects_mismatching_expected_sha256() {
        let temp = temp_path("edit-sha-mismatch");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");

        let output = EditFileHandler::default()
            .handle(
                edit_invocation(
                    "edit_sha_mismatch",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "expected_sha256": sha256_hex(b"different")
                    }),
                ),
                edit_trace("edit_sha_mismatch"),
            )
            .await
            .expect("mismatching sha should return structured output");
        let json = output.raw_json();

        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("precondition_failed")
        );
        assert_eq!(
            json.get("message").and_then(JsonValue::as_str),
            Some("file changed before edit_file could modify it")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_allows_matching_expected_mtime() {
        let temp = temp_path("edit-mtime-match");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let current = read_current_file_state_for_tool(
            FileMutationTool::EditFile.name(),
            target_path.as_path(),
            FileMutationTool::EditFile.current_state_action_context(),
        )
        .await
        .expect("current state should read");

        let output = EditFileHandler::default()
            .handle(
                edit_invocation(
                    "edit_mtime_match",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "expected_mtime_ms": current.mtime_ms
                    }),
                ),
                edit_trace("edit_mtime_match"),
            )
            .await
            .expect("matching mtime should succeed");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("completed")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_existing_rejects_mismatching_expected_mtime() {
        let temp = temp_path("edit-mtime-mismatch");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");

        let output = EditFileHandler::default()
            .handle(
                edit_invocation(
                    "edit_mtime_mismatch",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "expected_mtime_ms": -1
                    }),
                ),
                edit_trace("edit_mtime_mismatch"),
            )
            .await
            .expect("mismatching mtime should return structured output");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("precondition_failed")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_raw_match_counting_allows_one_match_to_reach_next_stage() {
        let temp = temp_path("edit-raw-one-match");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "hello old world")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:raw_one", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_raw_one",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_raw_one"),
            )
            .await
            .expect("one raw match should succeed");
        let json = output.raw_json();

        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("completed")
        );
        assert_eq!(json.get("matches").and_then(JsonValue::as_u64), Some(1));
        assert_eq!(
            json.get("matches_replaced").and_then(JsonValue::as_u64),
            Some(1)
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_single_replacement_writes_final_text() {
        let temp = temp_path("edit-single-replacement");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "hello old world")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:single", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_single",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_single"),
            )
            .await
            .expect("single replacement should compute");
        let json = output.raw_json();

        assert_eq!(
            json.get("matches_replaced").and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            json.get("bytes_after").and_then(JsonValue::as_u64),
            Some("hello new world".len() as u64)
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "hello new world"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_deletion_replacement_writes_final_text() {
        let temp = temp_path("edit-delete-replacement");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "keep remove keep")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:delete", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_delete",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": " remove",
                        "new_string": ""
                    }),
                ),
                edit_trace("edit_delete"),
            )
            .await
            .expect("deletion replacement should compute");
        let json = output.raw_json();

        assert_eq!(
            json.get("matches_replaced").and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            json.get("bytes_after").and_then(JsonValue::as_u64),
            Some("keep keep".len() as u64)
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "keep keep"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[test]
    fn edit_file_computed_no_change_returns_no_change_output() {
        let loaded = EditFileLoadedTarget {
            target: EditFileTarget {
                original_path: "file.txt".to_owned(),
                resolved_path: PathBuf::from("/tmp/file.txt"),
            },
            text: "unchanged".to_owned(),
            current: CurrentFileState {
                bytes: "unchanged".len() as u64,
                sha256: sha256_hex(b"unchanged"),
                mtime_ms: 1_234,
            },
        };
        let computed = EditFileComputedEdit {
            final_text: "unchanged".to_owned(),
            matches_replaced: 1,
        };

        let output = edit_file_no_change_if_unchanged(&loaded, &computed)
            .expect("unchanged computation should return output");
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("no_change")
        );
        assert_eq!(
            json.get("retryableByModel").and_then(JsonValue::as_bool),
            Some(false)
        );
    }

    #[test]
    fn edit_file_line_ending_detection_classifies_common_modes() {
        assert_eq!(detect_line_ending_mode("plain text"), LineEndingMode::None);
        assert_eq!(detect_line_ending_mode("a\nb\n"), LineEndingMode::Lf);
        assert_eq!(detect_line_ending_mode("a\r\nb\r\n"), LineEndingMode::Crlf);
        assert_eq!(detect_line_ending_mode("a\r\nb\n"), LineEndingMode::Mixed);
        assert_eq!(detect_line_ending_mode("a\rb"), LineEndingMode::Mixed);
    }

    #[tokio::test]
    async fn edit_file_line_ending_metadata_is_reported_in_handler_output() {
        let temp = temp_path("edit-line-ending-metadata");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old\r\nline\r\n")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:line_endings", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_line_endings",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_line_endings"),
            )
            .await
            .expect("handler should return metadata");

        assert_eq!(
            output
                .raw_json()
                .get("line_ending_mode")
                .and_then(JsonValue::as_str),
            Some("crlf")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[test]
    fn edit_file_crlf_fallback_computes_crlf_final_text() {
        let loaded = EditFileLoadedTarget {
            target: EditFileTarget {
                original_path: "file.txt".to_owned(),
                resolved_path: PathBuf::from("/tmp/file.txt"),
            },
            text: "old\r\nline\r\n".to_owned(),
            current: CurrentFileState {
                bytes: "old\r\nline\r\n".len() as u64,
                sha256: sha256_hex(b"old\r\nline\r\n"),
                mtime_ms: 1_234,
            },
        };
        let args = EditFileArgs {
            path: "file.txt".to_owned(),
            old_string: "old\nline".to_owned(),
            new_string: "new\nline".to_owned(),
            replace_all: None,
            read_observation_id: None,
            expected_sha256: None,
            expected_mtime_ms: None,
        };

        let raw_match = match compute_edit_file_raw_match(&loaded, &args) {
            EditFileRawMatchResult::Matched(raw_match) => raw_match,
            EditFileRawMatchResult::Failed(_) => panic!("CRLF fallback should match"),
        };
        let computed = compute_edit_file_replacement(&loaded, &args, &raw_match);

        assert_eq!(raw_match.occurrences, 1);
        assert_eq!(raw_match.source.line_ending_mode(), "crlf_fallback");
        assert_eq!(computed.final_text, "new\r\nline\r\n");
        assert_eq!(computed.matches_replaced, 1);
    }

    #[tokio::test]
    async fn edit_file_crlf_fallback_writes_crlf_final_text() {
        let temp = temp_path("edit-crlf-fallback");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old\r\nline\r\n")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:crlf_fallback", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_crlf_fallback",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old\nline",
                        "new_string": "new\nline"
                    }),
                ),
                edit_trace("edit_crlf_fallback"),
            )
            .await
            .expect("CRLF fallback should compute");
        let json = output.raw_json();

        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("completed")
        );
        assert_eq!(
            json.get("line_ending_mode").and_then(JsonValue::as_str),
            Some("crlf_fallback")
        );
        assert_eq!(
            json.get("bytes_after").and_then(JsonValue::as_u64),
            Some("new\r\nline\r\n".len() as u64)
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "new\r\nline\r\n"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_raw_crlf_match_uses_raw_mode() {
        let temp = temp_path("edit-raw-crlf-match");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old\r\nline\r\n")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:raw_crlf", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_raw_crlf",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old\r\nline",
                        "new_string": "new\r\nline"
                    }),
                ),
                edit_trace("edit_raw_crlf"),
            )
            .await
            .expect("raw CRLF match should compute");
        let json = output.raw_json();

        assert_eq!(
            json.get("line_ending_mode").and_then(JsonValue::as_str),
            Some("crlf")
        );
        assert_eq!(json.get("matches").and_then(JsonValue::as_u64), Some(1));
        assert_eq!(
            json.get("matches_replaced").and_then(JsonValue::as_u64),
            Some(1)
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_mixed_line_endings_do_not_use_crlf_fallback() {
        let temp = temp_path("edit-mixed-no-fallback");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old\r\nline\n")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(
            observation_for_file("read_file:mixed_no_fallback", target_path.as_path()).await,
        );

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_mixed_no_fallback",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old\nline",
                        "new_string": "new\nline"
                    }),
                ),
                edit_trace("edit_mixed_no_fallback"),
            )
            .await
            .expect("mixed line endings should return structured output");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("not_found")
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "old\r\nline\n"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_raw_match_counting_returns_not_found_for_zero_matches() {
        let temp = temp_path("edit-raw-not-found");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "hello current world")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:raw_missing", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_raw_missing",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_raw_missing"),
            )
            .await
            .expect("zero raw matches should return structured output");
        let json = output.raw_json();

        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("not_found")
        );
        assert_eq!(
            json.get("suggestedTool").and_then(JsonValue::as_str),
            Some("read_file")
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "hello current world"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_ambiguous_match_rejects_multiple_matches_by_default() {
        let temp = temp_path("edit-ambiguous-match");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old one old two")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:ambiguous", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_ambiguous",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new"
                    }),
                ),
                edit_trace("edit_ambiguous"),
            )
            .await
            .expect("ambiguous match should return structured output");
        let json = output.raw_json();

        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("ambiguous_match")
        );
        assert_eq!(json.get("matches").and_then(JsonValue::as_u64), Some(2));
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "old one old two"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_replace_all_writes_all_replacements() {
        let temp = temp_path("edit-replace-all");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old one old two")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:replace_all", target_path.as_path()).await);

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_replace_all",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "replace_all": true
                    }),
                ),
                edit_trace("edit_replace_all"),
            )
            .await
            .expect("replace_all should compute");
        let json = output.raw_json();

        assert_eq!(
            json.get("matches_replaced").and_then(JsonValue::as_u64),
            Some(2)
        );
        assert_eq!(
            json.get("bytes_after").and_then(JsonValue::as_u64),
            Some("new one new two".len() as u64)
        );
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "new one new two"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_replace_all_still_returns_not_found_for_zero_matches() {
        let temp = temp_path("edit-replace-all-not-found");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "current")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(
            observation_for_file("read_file:replace_all_missing", target_path.as_path()).await,
        );

        let output = EditFileHandler::new(store)
            .handle(
                edit_invocation(
                    "edit_replace_all_missing",
                    temp.clone(),
                    serde_json::json!({
                        "path": "file.txt",
                        "old_string": "old",
                        "new_string": "new",
                        "replace_all": true
                    }),
                ),
                edit_trace("edit_replace_all_missing"),
            )
            .await
            .expect("replace_all missing should return structured output");

        assert_eq!(
            output.raw_json().get("status").and_then(JsonValue::as_str),
            Some("not_found")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_target_validation_resolves_relative_existing_file() {
        let temp = temp_path("edit-target-relative");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "old")
            .await
            .expect("file should write");
        let args = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "./file.txt",
                "old_string": "old",
                "new_string": "new"
            }),
        })
        .expect("args should parse");

        let target = match prepare_edit_file_target(temp.as_path(), &args)
            .await
            .expect("target validation should run")
        {
            EditFileTargetValidation::Ready(target) => target,
            EditFileTargetValidation::Failed(_) => panic!("existing file should be ready"),
        };

        assert_eq!(target.original_path, "./file.txt");
        assert_eq!(target.resolved_path, target_path);
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_target_validation_returns_target_not_found_for_missing_file() {
        let temp = temp_path("edit-target-missing");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let args = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "missing.txt",
                "old_string": "old",
                "new_string": "new"
            }),
        })
        .expect("args should parse");

        let output = match prepare_edit_file_target(temp.as_path(), &args)
            .await
            .expect("target validation should run")
        {
            EditFileTargetValidation::Ready(_) => panic!("missing file should not be ready"),
            EditFileTargetValidation::Failed(output) => output,
        };
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("target_not_found")
        );
        assert_eq!(
            json.get("suggestedTool").and_then(JsonValue::as_str),
            Some("write_file")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_target_validation_rejects_directory_target() {
        let temp = temp_path("edit-target-directory");
        fs::create_dir_all(temp.join("dir").as_path())
            .await
            .expect("temp dir should create");
        let args = parse_edit_file_args(ToolPayload::Function {
            arguments: serde_json::json!({
                "path": "dir",
                "old_string": "old",
                "new_string": "new"
            }),
        })
        .expect("args should parse");

        let error = match prepare_edit_file_target(temp.as_path(), &args).await {
            Ok(_) => panic!("directory target should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("edit_file target"));
        assert!(error.to_string().contains("is a directory"));
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_utf8_load_reads_text_and_current_state() {
        let temp = temp_path("edit-utf8-load");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "hello")
            .await
            .expect("file should write");
        let target = EditFileTarget {
            original_path: "file.txt".to_owned(),
            resolved_path: target_path.clone(),
        };

        let loaded = match load_edit_file_text(target).await.expect("load should run") {
            EditFileTextLoad::Loaded(loaded) => loaded,
            EditFileTextLoad::Failed(_) => panic!("valid UTF-8 should load"),
        };

        assert_eq!(loaded.text, "hello");
        assert_eq!(loaded.current.bytes, 5);
        assert_eq!(loaded.current.sha256, sha256_hex(b"hello"));
        assert!(loaded.current.mtime_ms > 0);
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_utf8_load_returns_not_utf8_for_invalid_utf8() {
        let temp = temp_path("edit-not-utf8");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), [0xff, 0xfe])
            .await
            .expect("file should write");
        let target = EditFileTarget {
            original_path: "file.txt".to_owned(),
            resolved_path: target_path,
        };

        let output = match load_edit_file_text(target).await.expect("load should run") {
            EditFileTextLoad::Loaded(_) => panic!("invalid UTF-8 should fail"),
            EditFileTextLoad::Failed(output) => output,
        };
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("not_utf8")
        );
        assert_eq!(
            json.get("retryableByModel").and_then(JsonValue::as_bool),
            Some(false)
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_utf8_load_rejects_file_over_size_limit() {
        let temp = temp_path("edit-size-limit");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), vec![b'x'; edit_max_file_bytes() + 1])
            .await
            .expect("file should write");
        let target = EditFileTarget {
            original_path: "file.txt".to_owned(),
            resolved_path: target_path,
        };

        let error = match load_edit_file_text(target).await {
            Ok(_) => panic!("oversized file should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("larger than edit_file limit"));
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_atomic_create_writes_exact_content() {
        let temp = temp_path("write-atomic-create");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");

        let output = WriteFileHandler::default()
            .handle(
                write_invocation(
                    "write_atomic_create",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "content": "hello\nworld\n" }),
                ),
                write_trace("write_atomic_create"),
            )
            .await
            .expect("write_file should succeed");
        let json = output.raw_json();

        assert!(output.success());
        assert_eq!(
            json.get("operation").and_then(JsonValue::as_str),
            Some("created")
        );
        assert_eq!(
            fs::read_to_string(temp.join("file.txt"))
                .await
                .expect("file should read"),
            "hello\nworld\n"
        );
        assert_eq!(write_temp_file_count(temp.as_path(), "file.txt").await, 0);

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_atomic_overwrite_after_complete_read_writes_exact_content() {
        let temp = temp_path("write-atomic-overwrite");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "old")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        let read_handler = ReadFileHandler::new(store.clone());
        let write_handler = WriteFileHandler::new(store);

        read_handler
            .handle(
                read_invocation(
                    "read_before_overwrite",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt" }),
                ),
                read_trace("read_before_overwrite"),
            )
            .await
            .expect("read_file should succeed");
        let output = write_handler
            .handle(
                write_invocation(
                    "write_atomic_overwrite",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "content": "new\ncontent\n" }),
                ),
                write_trace("write_atomic_overwrite"),
            )
            .await
            .expect("write_file should succeed");
        let json = output.raw_json();

        assert!(output.success());
        assert_eq!(
            json.get("operation").and_then(JsonValue::as_str),
            Some("overwritten")
        );
        assert_eq!(
            fs::read_to_string(temp.join("file.txt"))
                .await
                .expect("file should read"),
            "new\ncontent\n"
        );
        assert_eq!(write_temp_file_count(temp.as_path(), "file.txt").await, 0);

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_atomic_rejects_content_over_limit() {
        let temp = temp_path("write-over-limit");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let content = "x".repeat(write_max_bytes().saturating_add(1));

        let error = match WriteFileHandler::default()
            .handle(
                write_invocation(
                    "write_over_limit",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "content": content }),
                ),
                write_trace("write_over_limit"),
            )
            .await
        {
            Ok(_) => panic!("oversized content should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("larger than write_file limit"));
        assert!(!temp.join("file.txt").exists());
        assert_eq!(write_temp_file_count(temp.as_path(), "file.txt").await, 0);

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_atomic_cleanup_removes_temp_file() {
        let temp = temp_path("write-cleanup");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        let temp_path = write_temp_path(temp.as_path(), target_path.as_path(), 0);
        fs::write(temp_path.as_path(), "leftover")
            .await
            .expect("temp file should write");

        cleanup_write_temp_file(temp_path.as_path()).await;

        assert!(!temp_path.exists());

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn atomic_text_write_helper_writes_exact_content_for_edit_file_context() {
        let temp = temp_path("atomic-text-edit");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");

        let result = write_text_atomically_for_tool(
            FileMutationTool::EditFile,
            target_path.as_path(),
            "edited",
            write_max_bytes(),
        )
        .await
        .expect("atomic helper should write");

        assert_eq!(result.bytes_written, 6);
        assert_eq!(result.sha256, sha256_hex(b"edited"));
        assert_eq!(
            fs::read_to_string(target_path.as_path())
                .await
                .expect("target should read"),
            "edited"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn atomic_text_write_helper_cleans_temp_file_for_edit_file_context() {
        let temp = temp_path("atomic-text-cleanup");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");

        write_text_atomically_for_tool(
            FileMutationTool::EditFile,
            target_path.as_path(),
            "edited",
            write_max_bytes(),
        )
        .await
        .expect("atomic helper should write");

        assert_eq!(
            write_temp_file_count(temp.as_path(), "file.txt").await,
            0,
            "shared atomic helper should not leave temp files after success"
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_post_write_created_output_shape() {
        let temp = temp_path("write-created-output");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");

        let output = WriteFileHandler::default()
            .handle(
                write_invocation(
                    "write_created_output",
                    temp.clone(),
                    serde_json::json!({ "path": "nested/file.txt", "content": "hello" }),
                ),
                write_trace("write_created_output"),
            )
            .await
            .expect("write_file should succeed");
        let json = output.raw_json();
        let resolved_path = temp.join("nested/file.txt");

        assert!(output.success());
        assert_eq!(json.get("ok").and_then(JsonValue::as_bool), Some(true));
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("completed")
        );
        assert_eq!(
            json.get("operation").and_then(JsonValue::as_str),
            Some("created")
        );
        assert_eq!(
            json.get("path").and_then(JsonValue::as_str),
            Some("nested/file.txt")
        );
        assert_eq!(
            json.get("resolved_path").and_then(JsonValue::as_str),
            Some(resolved_path.display().to_string()).as_deref()
        );
        assert_eq!(
            json.get("bytes_written").and_then(JsonValue::as_u64),
            Some(5)
        );
        assert_eq!(
            json.get("sha256").and_then(JsonValue::as_str),
            Some(sha256_hex(b"hello")).as_deref()
        );
        assert_eq!(
            json["file_observation"]["id"].as_str(),
            Some("write_file:write_created_output")
        );
        assert_eq!(
            json["file_observation"]["path"].as_str(),
            Some(resolved_path.display().to_string()).as_deref()
        );
        assert_eq!(json["file_observation"]["complete"].as_bool(), Some(true));
        assert_eq!(
            json["created_dirs"][0].as_str(),
            Some(temp.join("nested").display().to_string()).as_deref()
        );
        assert_eq!(
            json["changed_files"][0].as_str(),
            Some(resolved_path.display().to_string()).as_deref()
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_post_write_overwritten_output_shape() {
        let temp = temp_path("write-overwritten-output");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "old")
            .await
            .expect("file should write");
        let store = Arc::new(FileObservationStore::default());
        store.record(observation_for_file("read_file:old", temp.join("file.txt").as_path()).await);
        let output = WriteFileHandler::new(store)
            .handle(
                write_invocation(
                    "write_overwritten_output",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "content": "new" }),
                ),
                write_trace("write_overwritten_output"),
            )
            .await
            .expect("write_file should succeed");
        let json = output.raw_json();

        assert!(output.success());
        assert_eq!(
            json.get("operation").and_then(JsonValue::as_str),
            Some("overwritten")
        );
        assert_eq!(
            json.get("bytes_written").and_then(JsonValue::as_u64),
            Some(3)
        );
        assert_eq!(
            json["file_observation"]["source_tool_call_id"].as_str(),
            Some("write_overwritten_output")
        );
        assert_eq!(
            fs::read_to_string(temp.join("file.txt"))
                .await
                .expect("file should read"),
            "new"
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_post_write_updates_observation_store() {
        let temp = temp_path("write-store-update");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let store = Arc::new(FileObservationStore::default());
        let handler = WriteFileHandler::new(store.clone());
        let resolved_path = temp.join("file.txt");

        handler
            .handle(
                write_invocation(
                    "write_store_update",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "content": "first" }),
                ),
                write_trace("write_store_update"),
            )
            .await
            .expect("write_file should succeed");
        let observation = store
            .latest_complete_for_path(resolved_path.as_path())
            .expect("write observation should be stored");
        assert_eq!(observation.id, "write_file:write_store_update");
        assert_eq!(observation.sha256, sha256_hex(b"first"));

        handler
            .handle(
                write_invocation(
                    "write_store_update_second",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "content": "second" }),
                ),
                write_trace("write_store_update_second"),
            )
            .await
            .expect("second write should use first write observation");
        assert_eq!(
            fs::read_to_string(resolved_path.as_path())
                .await
                .expect("file should read"),
            "second"
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_post_write_success_output_does_not_echo_content() {
        let temp = temp_path("write-no-echo");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let content = "unique-output-content-that-should-not-be-echoed";

        let output = WriteFileHandler::default()
            .handle(
                write_invocation(
                    "write_no_echo",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "content": content }),
                ),
                write_trace("write_no_echo"),
            )
            .await
            .expect("write_file should succeed");

        assert!(!output.raw_text().contains(content));
        assert!(!output.raw_json().to_string().contains(content));

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn write_file_post_write_verification_failed_output_shape() {
        let temp = temp_path("write-verification-failed");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let resolved_path = temp.join("file.txt");
        fs::write(resolved_path.as_path(), "actual")
            .await
            .expect("file should write");
        let target = WriteFileTarget {
            original_path: "file.txt".to_owned(),
            resolved_path,
            operation: WriteFileOperation::Overwritten,
            created_dirs: Vec::new(),
        };
        let expected = AtomicWriteResult {
            bytes_written: 99,
            sha256: sha256_hex(b"expected"),
        };

        let output = match verify_written_file(&target, &expected)
            .await
            .expect("verification should run")
        {
            WriteVerification::Verified(_) => panic!("mismatched file should fail verification"),
            WriteVerification::Failed(output) => output,
        };
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("verification_failed")
        );
        assert_eq!(
            json.get("errorClass").and_then(JsonValue::as_str),
            Some("execution_failed")
        );
        assert_eq!(
            json.get("expected_bytes").and_then(JsonValue::as_u64),
            Some(99)
        );
        assert_eq!(
            json.get("actual_bytes").and_then(JsonValue::as_u64),
            Some("actual".len() as u64)
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn text_write_verification_helper_uses_edit_file_metadata_shape() {
        let temp = temp_path("edit-verification-failed");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "actual")
            .await
            .expect("file should write");
        let expected = AtomicWriteResult {
            bytes_written: 999,
            sha256: sha256_hex(b"expected"),
        };

        let verification = verify_text_write_for_tool(
            FileMutationTool::EditFile,
            "file.txt",
            target_path.as_path(),
            &expected,
        )
        .await
        .expect("verification helper should run");

        let WriteVerification::Failed(output) = verification else {
            panic!("verification should fail");
        };
        let json = output.raw_json();
        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("verification_failed")
        );
        assert_eq!(
            json.get("message").and_then(JsonValue::as_str),
            Some("edit_file wrote bytes but post-write verification failed")
        );
        assert_eq!(
            json.get("path").and_then(JsonValue::as_str),
            Some("file.txt")
        );
        let expected_resolved_path = target_path.display().to_string();
        assert_eq!(
            json.get("resolved_path").and_then(JsonValue::as_str),
            Some(expected_resolved_path.as_str())
        );
        assert_eq!(
            json.get("expected_bytes").and_then(JsonValue::as_u64),
            Some(999)
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn edit_file_post_write_verification_failed_output_shape() {
        let temp = temp_path("edit-wrapper-verification-failed");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        let target_path = temp.join("file.txt");
        fs::write(target_path.as_path(), "actual")
            .await
            .expect("file should write");
        let loaded = EditFileLoadedTarget {
            target: EditFileTarget {
                original_path: "file.txt".to_owned(),
                resolved_path: target_path.clone(),
            },
            text: "before".to_owned(),
            current: CurrentFileState {
                bytes: "before".len() as u64,
                sha256: sha256_hex(b"before"),
                mtime_ms: 1_234,
            },
        };
        let expected = AtomicWriteResult {
            bytes_written: 999,
            sha256: sha256_hex(b"expected"),
        };

        let output = match verify_edited_file(&loaded, &expected)
            .await
            .expect("verification should run")
        {
            WriteVerification::Verified(_) => panic!("mismatched file should fail verification"),
            WriteVerification::Failed(output) => output,
        };
        let json = output.raw_json();

        assert!(!output.success());
        assert_eq!(
            json.get("status").and_then(JsonValue::as_str),
            Some("verification_failed")
        );
        assert_eq!(
            json.get("message").and_then(JsonValue::as_str),
            Some("edit_file wrote bytes but post-write verification failed")
        );
        assert_eq!(
            json.get("path").and_then(JsonValue::as_str),
            Some("file.txt")
        );
        assert_eq!(
            json.get("expected_bytes").and_then(JsonValue::as_u64),
            Some(999)
        );
        assert_eq!(
            json.get("actual_bytes").and_then(JsonValue::as_u64),
            Some("actual".len() as u64)
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[test]
    fn file_mutation_observation_helper_records_edit_file_observation() {
        let store = FileObservationStore::default();
        let path = Path::new("/tmp/edited.txt");
        let current = CurrentFileState {
            bytes: 6,
            sha256: sha256_hex(b"edited"),
            mtime_ms: 1_234,
        };

        let observation = record_file_mutation_observation(
            &store,
            FileMutationTool::EditFile,
            "call_edit",
            path,
            &current,
        );

        assert_eq!(observation.id, "edit_file:call_edit");
        assert_eq!(observation.resolved_path, path);
        assert_eq!(observation.bytes, 6);
        assert_eq!(observation.sha256, sha256_hex(b"edited"));
        assert!(observation.complete);
        let latest = store
            .latest_complete_for_path(path)
            .expect("recorded observation should be latest");
        assert_eq!(latest.id, "edit_file:call_edit");
    }

    #[tokio::test]
    async fn read_file_complete_read_emits_and_records_file_observation() {
        let temp = temp_path("read-complete");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "hello\nworld\n")
            .await
            .expect("file should write");
        let handler = ReadFileHandler::default();

        let output = handler
            .handle(
                read_invocation(
                    "call_complete",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt" }),
                ),
                read_trace("call_complete"),
            )
            .await
            .expect("read_file should succeed");
        let json = output.raw_json();
        let resolved_path = temp.join("file.txt");
        let observation = json
            .get("file_observation")
            .and_then(JsonValue::as_object)
            .expect("file_observation should be object");

        assert_eq!(
            json.get("resolved_path").and_then(JsonValue::as_str),
            Some(resolved_path.display().to_string()).as_deref()
        );
        assert_eq!(
            observation.get("id").and_then(JsonValue::as_str),
            Some("read_file:call_complete")
        );
        assert_eq!(
            observation.get("path").and_then(JsonValue::as_str),
            Some(resolved_path.display().to_string()).as_deref()
        );
        assert_eq!(
            observation.get("bytes").and_then(JsonValue::as_u64),
            Some("hello\nworld\n".len() as u64)
        );
        assert_eq!(
            observation.get("sha256").and_then(JsonValue::as_str),
            Some(sha256_hex(b"hello\nworld\n")).as_deref()
        );
        assert_eq!(
            observation.get("complete").and_then(JsonValue::as_bool),
            Some(true)
        );
        assert!(
            handler
                .observation_store
                .latest_complete_for_path(resolved_path.as_path())
                .is_some()
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn read_file_line_limited_read_emits_partial_observation() {
        let temp = temp_path("read-partial");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "hello\nworld\n")
            .await
            .expect("file should write");
        let handler = ReadFileHandler::default();

        let output = handler
            .handle(
                read_invocation(
                    "call_partial",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "start_line": 1, "end_line": 1 }),
                ),
                read_trace("call_partial"),
            )
            .await
            .expect("read_file should succeed");
        let json = output.raw_json();
        let resolved_path = temp.join("file.txt");

        assert_eq!(json["file_observation"]["complete"].as_bool(), Some(false));
        assert!(
            handler
                .observation_store
                .latest_complete_for_path(resolved_path.as_path())
                .is_none()
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn read_file_truncated_read_emits_partial_observation() {
        let temp = temp_path("read-truncated");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("temp dir should create");
        fs::write(temp.join("file.txt"), "hello\nworld\n")
            .await
            .expect("file should write");
        let handler = ReadFileHandler::default();

        let output = handler
            .handle(
                read_invocation(
                    "call_truncated",
                    temp.clone(),
                    serde_json::json!({ "path": "file.txt", "max_bytes": 2 }),
                ),
                read_trace("call_truncated"),
            )
            .await
            .expect("read_file should succeed");
        let json = output.raw_json();
        let resolved_path = temp.join("file.txt");

        assert_eq!(
            json.get("truncated").and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(json["file_observation"]["complete"].as_bool(), Some(false));
        assert_eq!(
            json["file_observation"]["bytes"].as_u64(),
            Some("hello\nworld\n".len() as u64)
        );
        assert!(
            handler
                .observation_store
                .latest_complete_for_path(resolved_path.as_path())
                .is_none()
        );

        let _ = fs::remove_dir_all(temp).await;
    }

    #[test]
    fn grep_path_normalization_rejects_workspace_traversal() {
        let temp = temp_path("traversal");
        std::fs::create_dir_all(temp.as_path()).expect("tempdir should create");
        let error = resolve_path_within_workdir(temp.as_path(), "../outside")
            .expect_err("traversal should be rejected");
        assert!(error.to_string().contains("outside the workspace"));
        let _ = std::fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn grep_caps_result_count_and_output_bytes() {
        let temp = temp_path("caps");
        let src = temp.join("src");
        fs::create_dir_all(src.as_path())
            .await
            .expect("src dir should create");
        let mut body = String::new();
        for index in 0..20 {
            body.push_str(
                format!("match line number {index} with long trailing content\n").as_str(),
            );
        }
        fs::write(src.join("lib.rs"), body)
            .await
            .expect("file should write");

        let output = GrepHandler
            .handle(
                grep_invocation(
                    temp.clone(),
                    serde_json::json!({
                        "pattern": "match",
                        "path": "src",
                        "glob": "*.rs",
                        "max_results": 2,
                        "max_output_bytes": 120,
                    }),
                ),
                trace(),
            )
            .await
            .expect("grep should run");
        let json = output.raw_json();
        assert_eq!(json.get("status").and_then(JsonValue::as_str), Some("ok"));
        assert_eq!(
            json.get("truncated").and_then(JsonValue::as_bool),
            Some(true)
        );
        assert!(
            json.get("output")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .contains("truncated")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn broad_workspace_grep_requires_narrowing() {
        if std::process::Command::new("rg")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }

        let temp = temp_path("broad");
        fs::create_dir_all(temp.as_path())
            .await
            .expect("tempdir should create");
        for index in 0..=BROAD_GREP_FILE_LIMIT {
            fs::write(temp.join(format!("file_{index}.txt")), "")
                .await
                .expect("file should write");
        }

        let output = GrepHandler
            .handle(
                grep_invocation(
                    temp.clone(),
                    serde_json::json!({
                        "pattern": "anything",
                    }),
                ),
                trace(),
            )
            .await
            .expect("grep should return structured output");
        let json = output.raw_json();
        assert!(!output.success());
        assert_eq!(
            json.get("errorClass").and_then(JsonValue::as_str),
            Some("needs_narrowing")
        );
        assert_eq!(
            json.get("reason").and_then(JsonValue::as_str),
            Some("broad_workspace_search")
        );
        let _ = fs::remove_dir_all(temp).await;
    }

    #[test]
    fn grep_default_excludes_cover_heavy_directories() {
        for expected in [".git", "target", "node_modules", ".next", "dist", "build"] {
            assert!(DEFAULT_GREP_EXCLUDED_DIRS.contains(&expected));
        }
    }
}
