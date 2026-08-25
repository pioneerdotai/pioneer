use crate::apply_patch::file_mutation::{
    AllowAllReadAccess, PaginatedReader, ReadError, ReadErrorCode, ReadRequest, SnapshotLimits,
    TargetResolver,
};
use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use crate::{FilePolicyChecker, FilePolicyDecision, FilePolicyOperation};
use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, VecDeque};
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::process::ExitStatus;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

const DEFAULT_READ_MAX_BYTES: usize = 256 * 1024;
const HARD_MAX_READ_PAGE_BYTES: usize = 1024 * 1024;
const DEFAULT_READ_MAX_LINES: usize = 2000;
const HARD_MAX_READ_LINES: usize = 20_000;
const HARD_MAX_READ_FILE_BYTES: u64 = 16 * 1024 * 1024;
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

#[derive(Clone, Default)]
pub struct ReadFileHandler;

pub struct ListDirHandler;
pub struct GrepHandler;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFileArgs {
    path: String,
    #[serde(default)]
    start_line: Option<usize>,
    #[serde(default)]
    start_byte: Option<u64>,
    #[serde(default)]
    max_lines: Option<usize>,
    #[serde(default)]
    max_bytes: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
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
        let workspace_root = invocation
            .workdir
            .canonicalize()
            .unwrap_or_else(|_| invocation.workdir.clone());
        let mut file_path =
            resolve_path_within_workdir(workspace_root.as_path(), args.path.as_str())?;
        if let Some(allowed_path) = enforce_file_policy_for_tool(
            invocation.execution_security_snapshot.as_ref(),
            FilePolicyOperation::Read,
            file_path.as_path(),
            invocation.workdir.as_path(),
        )? {
            file_path = allowed_path;
        }

        let name = file_path
            .strip_prefix(workspace_root.as_path())
            .map_err(|_| ToolError::invalid_arguments("read_file path is outside the workspace"))?
            .to_string_lossy()
            .into_owned();
        if name.is_empty() {
            return Err(ToolError::invalid_arguments(
                "read_file path has no file name",
            ));
        }
        let root = workspace_root;
        let max_bytes = args
            .max_bytes
            .unwrap_or(DEFAULT_READ_MAX_BYTES)
            .clamp(1, HARD_MAX_READ_PAGE_BYTES) as u64;
        let max_lines = args
            .max_lines
            .unwrap_or(DEFAULT_READ_MAX_LINES)
            .clamp(1, HARD_MAX_READ_LINES) as u32;
        if args.start_byte.is_some() && args.start_line.is_some() {
            return Err(ToolError::invalid_arguments(
                "read_file accepts either start_line or start_byte, not both",
            ));
        }
        let start_line = args.start_line.unwrap_or(1).max(1).saturating_sub(1) as u64;
        let start_byte = args.start_byte;
        let cursor = args.cursor;
        let requested_path = file_path.clone();
        let page = tokio::task::spawn_blocking(move || {
            let resolver =
                TargetResolver::new(root).map_err(|_| ReadError::new(ReadErrorCode::PathDenied))?;
            let reader = PaginatedReader::new(
                SnapshotLimits {
                    max_file_bytes: HARD_MAX_READ_FILE_BYTES,
                    inline_threshold: 64 * 1024,
                },
                AllowAllReadAccess,
            );
            reader
                .read_path(
                    &resolver,
                    name.as_str(),
                    ReadRequest {
                        start_line,
                        start_byte,
                        max_lines,
                        max_bytes,
                    },
                    cursor.as_deref(),
                )
                .map_err(|error| error)
        })
        .await
        .map_err(|error| ToolError::execution_failed(format!("read worker failed: {error}")))?
        .map_err(|error| {
            map_read_error(
                invocation.workdir.as_path(),
                requested_path.as_path(),
                error,
            )
        })?;

        let display_path = page.path.clone();
        let mut rendered = String::new();
        rendered.push_str(format!("File: {display_path}\n---\n").as_str());
        for (index, line) in split_lines_inclusive(page.content.as_str())
            .iter()
            .enumerate()
        {
            let line_no = page
                .start_line
                .saturating_add(index as u64)
                .saturating_add(1);
            rendered.push_str(format!("{:>6}\t{}", line_no, line).as_str());
        }
        if !page.content.is_empty() && !page.content.ends_with('\n') {
            rendered.push('\n');
        }
        if rendered.ends_with("---\n") {
            rendered.push_str("<empty selection>\n");
        }
        let version_token = page.token.to_string();
        let full_byte_count = page.token.byte_len();
        let selected_line_count = split_lines_inclusive(page.content.as_str()).len();
        let payload = serde_json::json!({
            "path": display_path,
            "resolved_path": page.path,
            "start_line": page.start_line.saturating_add(1),
            "start_byte": page.start_byte,
            "end_line": if page.content.is_empty() { JsonValue::Null } else { serde_json::json!(page.start_line.saturating_add(selected_line_count as u64)) },
            "max_lines": max_lines,
            "max_bytes": max_bytes,
            "next_line": page.next_line.map(|line| line.saturating_add(1)),
            "cursor": page.cursor.clone(),
            "continuation": page.cursor,
            "truncated": page.truncated,
            "range": {
                "start": page.start_byte,
                "end": page.end_byte,
                "unit": "bytes"
            },
            "bytes": full_byte_count,
            "version": version_token,
            "text": page.content,
            "output": rendered.clone(),
            "line_endings": page.line_endings,
        });
        Ok(Box::new(FunctionToolOutput::with_payload(
            rendered, true, payload,
        )))
    }
}

fn split_lines_inclusive(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    let bytes = text.as_bytes();
    let mut lines = Vec::new();
    let mut start = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        let ending_len = match bytes[index] {
            b'\n' => 1,
            b'\r' if bytes.get(index + 1) == Some(&b'\n') => 2,
            b'\r' => 1,
            _ => {
                index += 1;
                continue;
            }
        };
        let end = index + ending_len;
        lines.push(&text[start..end]);
        start = end;
        index = end;
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

fn map_read_error(workdir: &Path, path: &Path, error: ReadError) -> ToolError {
    let message = format!(
        "failed to read file `{}`: {error}",
        safe_display_path(workdir, path)
    );
    match error.code {
        ReadErrorCode::CursorInvalid
        | ReadErrorCode::CursorPathMismatch
        | ReadErrorCode::CursorOffsetMismatch
        | ReadErrorCode::StaleCursor
        | ReadErrorCode::OffsetOutOfRange
        | ReadErrorCode::InvalidRequest
        | ReadErrorCode::PathDenied
        | ReadErrorCode::BinaryContent
        | ReadErrorCode::InvalidUtf8
        | ReadErrorCode::TooLarge => ToolError::invalid_arguments(message),
        ReadErrorCode::AccessDenied | ReadErrorCode::Io => ToolError::execution_failed(message),
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
        let mut root = resolve_path_within_workdir(invocation.workdir.as_path(), base.as_str())?;
        if let Some(allowed_path) = enforce_file_policy_for_tool(
            invocation.execution_security_snapshot.as_ref(),
            FilePolicyOperation::Read,
            root.as_path(),
            invocation.workdir.as_path(),
        )? {
            root = allowed_path;
        }
        reject_symlink_components(invocation.workdir.as_path(), root.as_path())?;
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
        let mut truncated = false;
        while let Some((path, depth)) = queue.pop_front() {
            if items.len() >= limit {
                truncated = true;
                break;
            }

            let mut read_dir = tokio::fs::read_dir(path.as_path()).await.map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to list `{}`: {error}",
                    display_workspace_path(invocation.workdir.as_path(), path.as_path())
                ))
            })?;
            // Keep only the lexicographically smallest entries that could
            // still fit in the bounded result.  A directory may contain
            // millions of entries; collecting all of them before sorting
            // would turn a model-visible `limit` into an unbounded memory
            // allocation.  The extra slot proves whether more entries were
            // present without exposing them.
            let remaining = limit.saturating_sub(items.len());
            let candidate_limit = remaining.saturating_add(1);
            let mut entries = BTreeMap::new();
            let mut directory_truncated = false;
            while let Some(entry) = read_dir.next_entry().await.map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to read dir entry under `{}`: {error}",
                    display_workspace_path(invocation.workdir.as_path(), path.as_path())
                ))
            })? {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if !include_hidden && file_name.starts_with('.') {
                    continue;
                }
                if candidate_limit == 0 {
                    directory_truncated = true;
                    continue;
                }
                let entry_path = entry.path();
                let key = entry_path.clone();
                entries.insert(key, entry);
                if entries.len() > candidate_limit {
                    if let Some(last) = entries.keys().next_back().cloned() {
                        entries.remove(&last);
                    }
                    directory_truncated = true;
                }
            }

            if directory_truncated {
                truncated = true;
            }

            for (_, entry) in entries {
                if items.len() >= limit {
                    truncated = true;
                    break;
                }

                let entry_path = entry.path();
                let file_type = entry.file_type().await.map_err(|error| {
                    ToolError::execution_failed(format!(
                        "failed to inspect file type for `{}`: {error}",
                        display_workspace_path(invocation.workdir.as_path(), entry_path.as_path())
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
                                    display_workspace_path(
                                        invocation.workdir.as_path(),
                                        entry_path.as_path(),
                                    )
                                ))
                            })?
                            .len(),
                    )
                } else {
                    None
                };

                items.push(DirEntryView {
                    path: display_workspace_path(invocation.workdir.as_path(), &entry_path),
                    kind: kind.clone(),
                    size,
                });

                if file_type.is_dir() && !file_type.is_symlink() && depth < depth_limit {
                    queue.push_back((entry_path, depth.saturating_add(1)));
                }
            }
        }

        // Reaching the limit is only truncation when there is actually more
        // work that could have produced an entry.  This avoids reporting a
        // false positive for a directory whose result happens to contain
        // exactly `limit` entries.
        if !queue.is_empty() {
            truncated = true;
        }

        let payload = serde_json::json!({
            "root": display_workspace_path(invocation.workdir.as_path(), &root),
            "truncated": truncated,
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
        let requested_path = resolve_path_within_workdir(invocation.workdir.as_path(), base)?;
        let search_path = enforce_file_policy_for_tool(
            invocation.execution_security_snapshot.as_ref(),
            FilePolicyOperation::Read,
            requested_path.as_path(),
            invocation.workdir.as_path(),
        )?
        .unwrap_or(requested_path);
        reject_symlink_components(invocation.workdir.as_path(), search_path.as_path())?;

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
        let workspace_root = invocation
            .workdir
            .canonicalize()
            .unwrap_or_else(|_| invocation.workdir.to_path_buf());
        let is_broad_workspace_search = args.glob.is_none()
            && (args.path.is_none()
                || search_path
                    .canonicalize()
                    .unwrap_or_else(|_| search_path.to_path_buf())
                    == workspace_root);
        if is_broad_workspace_search {
            match count_rg_search_files(
                search_path.as_path(),
                invocation.workdir.as_path(),
                &invocation.environment,
                timeout_ms.min(3_000),
            )
            .await?
            {
                Some(file_count) if file_count > BROAD_GREP_FILE_LIMIT => {
                    return Ok(needs_narrowing_output(
                        "grep_files is too broad for this workspace. Narrow path or glob.",
                        search_path.as_path(),
                        invocation.workdir.as_path(),
                        Some(file_count),
                        max_results,
                        max_output_bytes,
                        "broad_workspace_search",
                    ));
                }
                None => {
                    // A broad search must never silently fall back to an
                    // unbounded recursive shell scan.  Without the scoped
                    // file enumerator we cannot prove the workspace stays
                    // within the resource budget, so require the model to
                    // narrow the request explicitly.
                    return Ok(needs_narrowing_output(
                        "grep_files cannot establish the workspace file-count limit; narrow path or glob.",
                        search_path.as_path(),
                        invocation.workdir.as_path(),
                        None,
                        max_results,
                        max_output_bytes,
                        "search_backend_unavailable",
                    ));
                }
                Some(_) => {}
            }
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
                    invocation.workdir.as_path(),
                    None,
                    max_results,
                    max_output_bytes,
                    "timeout",
                ));
            }
            Err(error) => return Err(error),
            Ok(None) => {
                if args.glob.is_some() {
                    // The portable grep fallback cannot reproduce ripgrep's
                    // path-aware glob semantics. Do not silently return
                    // matches outside the requested filter; fail closed and
                    // let the model choose a truthful retry.
                    return Ok(needs_narrowing_output(
                        "grep_files cannot honor `glob` because the scoped search backend is unavailable",
                        search_path.as_path(),
                        invocation.workdir.as_path(),
                        None,
                        max_results,
                        max_output_bytes,
                        "glob_backend_unavailable",
                    ));
                }
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
                            invocation.workdir.as_path(),
                            None,
                            max_results,
                            max_output_bytes,
                            "timeout",
                        ));
                    }
                    Err(error) => return Err(error),
                };
                let note = "note: rg is unavailable; used grep fallback".to_owned();
                (output, Some(note), true)
            }
        };

        let process_exit_code = output.status.code().unwrap_or(-1);
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let (truncated_stdout, output_truncated) =
            truncate_lines_and_bytes(stdout.as_str(), max_results, max_output_bytes);
        let output_truncated = output_truncated || output.output_limit_exceeded;
        let exit_code = if output.output_limit_exceeded && !stdout.is_empty() {
            // The process was deliberately stopped after producing bounded
            // match output.  Treat that as a successful, truncated search;
            // the original process exit status is retained for diagnostics.
            0
        } else {
            process_exit_code
        };

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
                "path": display_workspace_path(invocation.workdir.as_path(), &search_path),
                "exit_code": exit_code,
                "process_exit_code": process_exit_code,
                "truncated": output_truncated,
                "output_limit_exceeded": output.output_limit_exceeded,
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
                "path": display_workspace_path(invocation.workdir.as_path(), &search_path),
                "exit_code": exit_code,
                "process_exit_code": process_exit_code,
                "truncated": output.output_limit_exceeded,
                "output_limit_exceeded": output.output_limit_exceeded,
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
                    display_workspace_path(invocation.workdir.as_path(), &search_path)
                ),
            );
            let payload = serde_json::json!({
                "status": "failed",
                "engine": engine,
                "path": display_workspace_path(invocation.workdir.as_path(), &search_path),
                "exit_code": exit_code,
                "process_exit_code": process_exit_code,
                "truncated": output.output_limit_exceeded,
                "output_limit_exceeded": output.output_limit_exceeded,
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
) -> Result<Option<BoundedCommandOutput>, ToolError> {
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
    // Keep model-controlled patterns in the positional pattern slot.  Without
    // the separator, a pattern beginning with `-` can be parsed as an rg
    // option rather than searched literally.
    command.arg("--");
    command.arg(pattern);
    command.arg(command_path(workdir, search_path));
    command.current_dir(workdir);
    command.envs(environment.iter());

    run_bounded_command(command, timeout_ms, HARD_MAX_GREP_OUTPUT_BYTES, "rg").await
}

async fn run_grep_fallback(
    pattern: &str,
    case_sensitive: bool,
    search_path: &Path,
    workdir: &Path,
    environment: &BTreeMap<String, String>,
    timeout_ms: u64,
) -> Result<BoundedCommandOutput, ToolError> {
    let mut command = Command::new("grep");
    // Lower-case `-r` deliberately does not follow descendant symlinks.  The
    // fallback must preserve the same workspace boundary as ripgrep; `-R`
    // could follow a symlinked directory out of the workspace.
    command.arg("-r");
    command.arg("-n");
    for excluded in DEFAULT_GREP_EXCLUDED_DIRS {
        command.arg(format!("--exclude-dir={excluded}"));
    }
    if !case_sensitive {
        command.arg("-i");
    }
    command.arg("--");
    command.arg(pattern);
    command.arg(command_path(workdir, search_path));
    command.current_dir(workdir);
    command.envs(environment.iter());

    run_bounded_command(command, timeout_ms, HARD_MAX_GREP_OUTPUT_BYTES, "grep")
        .await?
        .ok_or_else(|| ToolError::execution_failed("grep executable is unavailable"))
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
    command.arg("--");
    command.arg(command_path(workdir, search_path));
    command.current_dir(workdir);
    command.envs(environment.iter());

    let Some(output) = run_bounded_command(
        command,
        timeout_ms,
        HARD_MAX_GREP_OUTPUT_BYTES,
        "rg --files",
    )
    .await?
    else {
        return Ok(None);
    };
    if output.output_limit_exceeded {
        return Ok(Some(BROAD_GREP_FILE_LIMIT.saturating_add(1)));
    }
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

#[derive(Debug)]
struct BoundedCommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    output_limit_exceeded: bool,
}

async fn run_bounded_command(
    mut command: Command,
    timeout_ms: u64,
    max_output_bytes: usize,
    executable_name: &str,
) -> Result<Option<BoundedCommandOutput>, ToolError> {
    command.kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ToolError::execution_failed(format!(
                "failed to execute {executable_name}: {error}"
            )));
        }
    };
    let stdout = child.stdout.take().ok_or_else(|| {
        ToolError::execution_failed(format!("{executable_name} did not provide stdout"))
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        ToolError::execution_failed(format!("{executable_name} did not provide stderr"))
    })?;
    let (limit_tx, mut limit_rx) = mpsc::unbounded_channel();
    let stdout_task = tokio::spawn(read_bounded_stream(
        stdout,
        max_output_bytes,
        limit_tx.clone(),
    ));
    let stderr_task = tokio::spawn(read_bounded_stream(stderr, max_output_bytes, limit_tx));

    let result = timeout(Duration::from_millis(timeout_ms), async {
        let (status, killed_for_limit) = tokio::select! {
            status = child.wait() => {
                (status.map_err(|error| ToolError::execution_failed(format!("failed waiting for {executable_name}: {error}")))?, false)
            }
            _ = limit_rx.recv() => {
                let _ = child.kill().await;
                let status = child.wait().await.map_err(|error| ToolError::execution_failed(format!("failed waiting for capped {executable_name}: {error}")))?;
                (status, true)
            }
        };
        let stdout = stdout_task
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("{executable_name} stdout worker failed: {error}"))
            })?
            .map_err(|error| {
                ToolError::execution_failed(format!("{executable_name} stdout read failed: {error}"))
            })?;
        let stderr = stderr_task
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("{executable_name} stderr worker failed: {error}"))
            })?
            .map_err(|error| {
                ToolError::execution_failed(format!("{executable_name} stderr read failed: {error}"))
            })?;
        Ok(BoundedCommandOutput {
            status,
            output_limit_exceeded: killed_for_limit || stdout.1 || stderr.1,
            stdout: stdout.0,
            stderr: stderr.0,
        })
    })
    .await;

    match result {
        Ok(result) => result.map(Some),
        Err(_) => {
            // `kill_on_drop(true)` terminates the child when this future is
            // cancelled by timeout.  The bounded readers retain at most the
            // configured capture limit and finish once the pipes close.
            Err(ToolError::execution_failed(format!(
                "{executable_name} timed out after {timeout_ms}ms"
            )))
        }
    }
}

async fn read_bounded_stream<R: AsyncRead + Unpin>(
    mut reader: R,
    max_output_bytes: usize,
    limit_tx: mpsc::UnboundedSender<()>,
) -> Result<(Vec<u8>, bool), std::io::Error> {
    let mut output = Vec::with_capacity(max_output_bytes.min(8192));
    let mut buffer = [0u8; 8192];
    let mut total = 0usize;
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let next_total = total.saturating_add(read);
        if output.len() < max_output_bytes {
            let take = (max_output_bytes - output.len()).min(read);
            output.extend_from_slice(&buffer[..take]);
        }
        total = next_total;
        if total > max_output_bytes && !truncated {
            truncated = true;
            let _ = limit_tx.send(());
        }
    }
    Ok((output, truncated))
}

fn append_default_rg_excludes(command: &mut Command) {
    for excluded in DEFAULT_GREP_EXCLUDED_DIRS {
        command.arg("-g").arg(format!("!{excluded}/**"));
    }
}

fn needs_narrowing_output(
    message: &str,
    search_path: &Path,
    workdir: &Path,
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
        "path": display_workspace_path(workdir, search_path),
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

fn command_path(workdir: &Path, path: &Path) -> PathBuf {
    workspace_relative_path(workdir, path)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn display_workspace_path(workdir: &Path, path: &Path) -> String {
    workspace_relative_path(workdir, path).unwrap_or_else(|| ".".to_owned())
}

fn workspace_relative_path(workdir: &Path, path: &Path) -> Option<String> {
    let root = workdir
        .canonicalize()
        .unwrap_or_else(|_| workdir.to_path_buf());
    let candidate = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let relative = candidate
        .strip_prefix(root.as_path())
        .ok()
        .filter(|relative| !relative.as_os_str().is_empty())?;
    Some(relative.to_string_lossy().replace('\\', "/"))
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

fn enforce_file_policy_for_tool(
    snapshot: Option<&pioneer_protocol::TurnExecutionSecuritySnapshot>,
    operation: FilePolicyOperation,
    requested_path: &Path,
    workdir: &Path,
) -> Result<Option<PathBuf>, ToolError> {
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };

    match FilePolicyChecker::check(snapshot, operation, requested_path) {
        FilePolicyDecision::Allowed(grant) => Ok(Some(grant.resolved_path)),
        FilePolicyDecision::Denied(deny) => Err(ToolError::Rejected(format!(
            "filesystem sandbox denied {operation:?} for `{}`: {}",
            safe_display_path(workdir, deny.requested_path.as_path()),
            deny.message
        ))),
    }
}

fn resolve_path_within_workdir(base: &Path, requested: &str) -> Result<PathBuf, ToolError> {
    let base = base.canonicalize().unwrap_or_else(|_| base.to_path_buf());
    let requested_path = Path::new(requested);
    let mut candidate = if requested_path.is_absolute() {
        normalize_absolute_path(requested_path).ok_or_else(|| {
            ToolError::invalid_arguments("`path` must be a valid absolute workspace path")
        })?
    } else {
        base.clone()
    };

    if !requested_path.is_absolute() {
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
        return Err(ToolError::invalid_arguments(
            "`path` must stay inside workspace",
        ));
    }
    Ok(candidate)
}

fn normalize_absolute_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
        }
    }
    normalized.is_absolute().then_some(normalized)
}

fn safe_display_path(workdir: &Path, path: &Path) -> String {
    let root = workdir
        .canonicalize()
        .unwrap_or_else(|_| workdir.to_path_buf());
    let candidate = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    candidate
        .strip_prefix(root.as_path())
        .ok()
        .map(|relative| {
            if relative.as_os_str().is_empty() {
                ".".to_owned()
            } else {
                relative.to_string_lossy().replace('\\', "/")
            }
        })
        .unwrap_or_else(|| "<outside-workspace>".to_owned())
}

fn reject_symlink_components(workdir: &Path, candidate: &Path) -> Result<(), ToolError> {
    let workdir = workdir
        .canonicalize()
        .unwrap_or_else(|_| workdir.to_path_buf());
    if !candidate.starts_with(&workdir) {
        return Err(ToolError::invalid_arguments(
            "filesystem path is outside the workspace",
        ));
    }
    let relative = candidate
        .strip_prefix(&workdir)
        .map_err(|_| ToolError::invalid_arguments("filesystem path is outside the workspace"))?;
    let mut current = workdir.clone();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            continue;
        };
        current.push(part);
        if let Ok(metadata) = std::fs::symlink_metadata(&current)
            && metadata.file_type().is_symlink()
        {
            return Err(ToolError::invalid_arguments(format!(
                "filesystem path contains a symlink component: `{}`",
                display_workspace_path(workdir.as_path(), current.as_path())
            )));
        }
    }
    Ok(())
}
