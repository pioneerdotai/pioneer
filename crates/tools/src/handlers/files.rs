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
        let observation = FileObservation {
            id: format!("write_file:{}", invocation.call_id),
            resolved_path: target.resolved_path.clone(),
            bytes: current.bytes,
            sha256: current.sha256.clone(),
            mtime_ms: current.mtime_ms,
            complete: true,
            source_tool_call_id: invocation.call_id,
        };
        self.observation_store.record(observation.clone());

        Ok(write_file_success_output(&target, &current, observation))
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
        validate_expected_sha256(expected_sha256)?;
    }
    Ok(args)
}

fn validate_expected_sha256(value: &str) -> Result<(), ToolError> {
    let is_valid = value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    if is_valid {
        Ok(())
    } else {
        Err(ToolError::invalid_arguments(
            "write_file `expected_sha256` must be a 64-character hex SHA-256 digest",
        ))
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

async fn verify_existing_file_preconditions(
    observation_store: &FileObservationStore,
    target: &WriteFileTarget,
    args: &WriteFileArgs,
) -> Result<Option<Box<dyn ToolOutput>>, ToolError> {
    if target.operation == WriteFileOperation::Created {
        return Ok(None);
    }

    let observation = match args.read_observation_id.as_deref() {
        Some(observation_id) => observation_store
            .complete_by_id_for_path(observation_id, target.resolved_path.as_path()),
        None => observation_store.latest_complete_for_path(target.resolved_path.as_path()),
    };
    let has_explicit_precondition =
        args.expected_sha256.is_some() || args.expected_mtime_ms.is_some();

    if observation.is_none() && !has_explicit_precondition {
        return Ok(Some(read_required_output(target)));
    }

    let current = read_current_file_state(target.resolved_path.as_path()).await?;
    if has_explicit_precondition {
        if let Some(expected_sha256) = args.expected_sha256.as_deref()
            && current.sha256 != expected_sha256
        {
            return Ok(Some(precondition_failed_output_for_expected(
                target,
                &current,
                Some(expected_sha256),
                args.expected_mtime_ms,
            )));
        }

        if let Some(expected_mtime_ms) = args.expected_mtime_ms
            && current.mtime_ms != expected_mtime_ms
        {
            return Ok(Some(precondition_failed_output_for_expected(
                target,
                &current,
                args.expected_sha256.as_deref(),
                Some(expected_mtime_ms),
            )));
        }

        return Ok(None);
    }

    if let Some(observation) = observation.as_ref()
        && (current.sha256 != observation.sha256 || current.mtime_ms != observation.mtime_ms)
    {
        return Ok(Some(precondition_failed_output(
            target,
            &current,
            observation,
        )));
    }

    Ok(None)
}

async fn read_current_file_state(path: &Path) -> Result<CurrentFileState, ToolError> {
    let metadata = tokio::fs::metadata(path).await.map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to stat write_file target `{}` before overwrite: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(ToolError::invalid_arguments(format!(
            "write_file target `{}` is not a regular file",
            path.display()
        )));
    }

    let bytes = tokio::fs::read(path).await.map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to read write_file target `{}` before overwrite: {error}",
            path.display()
        ))
    })?;

    Ok(CurrentFileState {
        bytes: bytes.len() as u64,
        sha256: sha256_hex(bytes.as_slice()),
        mtime_ms: metadata_mtime_ms(&metadata).unwrap_or_default(),
    })
}

fn read_required_output(target: &WriteFileTarget) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "read_required",
        "errorClass": "invalid_arguments",
        "message": "write_file cannot overwrite an existing file until read_file has observed the complete current file",
        "path": target.original_path,
        "resolved_path": target.resolved_path.display().to_string(),
        "retryableByModel": true,
        "retrySameArguments": false,
        "suggestedTool": "read_file",
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| {
            "write_file cannot overwrite an existing file until read_file has observed it"
                .to_owned()
        }),
        false,
        payload,
    ))
}

fn precondition_failed_output(
    target: &WriteFileTarget,
    current: &CurrentFileState,
    observation: &FileObservation,
) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "precondition_failed",
        "errorClass": "precondition_failed",
        "message": "file changed before write_file could overwrite it",
        "path": target.original_path,
        "resolved_path": target.resolved_path.display().to_string(),
        "retryableByModel": true,
        "retrySameArguments": false,
        "observed_sha256": observation.sha256,
        "current_sha256": current.sha256,
        "observed_mtime_ms": observation.mtime_ms,
        "current_mtime_ms": current.mtime_ms,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload)
            .unwrap_or_else(|_| "file changed before write_file could overwrite it".to_owned()),
        false,
        payload,
    ))
}

fn precondition_failed_output_for_expected(
    target: &WriteFileTarget,
    current: &CurrentFileState,
    expected_sha256: Option<&str>,
    expected_mtime_ms: Option<i64>,
) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "precondition_failed",
        "errorClass": "precondition_failed",
        "message": "file changed before write_file could overwrite it",
        "path": target.original_path,
        "resolved_path": target.resolved_path.display().to_string(),
        "retryableByModel": true,
        "retrySameArguments": false,
        "expected_sha256": expected_sha256,
        "current_sha256": current.sha256,
        "expected_mtime_ms": expected_mtime_ms,
        "current_mtime_ms": current.mtime_ms,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload)
            .unwrap_or_else(|_| "file changed before write_file could overwrite it".to_owned()),
        false,
        payload,
    ))
}

async fn write_file_atomically(
    target: &WriteFileTarget,
    content: &str,
) -> Result<AtomicWriteResult, ToolError> {
    let bytes = content.as_bytes();
    let max_bytes = write_max_bytes();
    if bytes.len() > max_bytes {
        return Err(ToolError::invalid_arguments(format!(
            "write_file content is larger than write_file limit ({max_bytes} bytes)"
        )));
    }

    let parent = target.resolved_path.parent().ok_or_else(|| {
        ToolError::invalid_arguments(format!(
            "write_file target `{}` does not have a parent directory",
            target.resolved_path.display()
        ))
    })?;
    let (temp_path, mut temp_file) = create_write_temp_file(parent, &target.resolved_path).await?;

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

    if let Err(error) = tokio::fs::rename(temp_path.as_path(), target.resolved_path.as_path()).await
    {
        cleanup_write_temp_file(temp_path.as_path()).await;
        return Err(ToolError::execution_failed(format!(
            "failed to move temporary file `{}` to `{}`: {error}",
            temp_path.display(),
            target.resolved_path.display()
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
    let current = read_current_file_state(target.resolved_path.as_path()).await?;
    if current.bytes != expected.bytes_written || current.sha256 != expected.sha256 {
        return Ok(WriteVerification::Failed(verification_failed_output(
            target, expected, &current,
        )));
    }

    Ok(WriteVerification::Verified(current))
}

fn verification_failed_output(
    target: &WriteFileTarget,
    expected: &AtomicWriteResult,
    current: &CurrentFileState,
) -> Box<dyn ToolOutput> {
    let payload = serde_json::json!({
        "ok": false,
        "status": "verification_failed",
        "errorClass": "execution_failed",
        "message": "write_file wrote bytes but post-write verification failed",
        "path": target.original_path,
        "resolved_path": target.resolved_path.display().to_string(),
        "expected_bytes": expected.bytes_written,
        "actual_bytes": current.bytes,
        "expected_sha256": expected.sha256,
        "actual_sha256": current.sha256,
    });
    Box::new(FunctionToolOutput::with_payload(
        serde_json::to_string_pretty(&payload)
            .unwrap_or_else(|_| "write_file post-write verification failed".to_owned()),
        false,
        payload,
    ))
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

    fn trace() -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", "call_1", "grep_files")
    }

    fn read_trace(call_id: &str) -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", call_id, "read_file")
    }

    fn write_trace(call_id: &str) -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", call_id, "write_file")
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
        let current = read_current_file_state(target_path.as_path())
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
