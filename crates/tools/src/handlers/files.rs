use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::process::Output;
use tokio::process::Command;
use tokio::time::{Duration, timeout};

const DEFAULT_READ_MAX_BYTES: usize = 256 * 1024;
const HARD_MAX_READ_BYTES: usize = 1024 * 1024;
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

pub struct ReadFileHandler;
pub struct ListDirHandler;
pub struct GrepHandler;

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
        let file_path = resolve_path(invocation.workdir.as_path(), args.path.as_str());

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

        let payload = serde_json::json!({
            "path": file_path.display().to_string(),
            "start_line": start,
            "end_line": if end == usize::MAX { JsonValue::Null } else { serde_json::json!(end) },
            "truncated": was_truncated,
            "max_bytes": max_bytes,
            "output": rendered.clone(),
        });
        Ok(Box::new(FunctionToolOutput::with_payload(
            rendered, true, payload,
        )))
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

    timeout(Duration::from_millis(timeout_ms), command.output())
        .await
        .map_err(|_| ToolError::execution_failed(format!("grep timed out after {timeout_ms}ms")))?
        .map_err(|error| ToolError::execution_failed(format!("failed to execute grep: {error}")))
}

async fn count_rg_search_files(
    search_path: &Path,
    workdir: &Path,
    timeout_ms: u64,
) -> Result<Option<usize>, ToolError> {
    let mut command = Command::new("rg");
    command.arg("--files");
    append_default_rg_excludes(&mut command);
    command.arg(search_path.as_os_str());
    command.current_dir(workdir);

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

fn resolve_path(base: &Path, requested: &str) -> PathBuf {
    let path = Path::new(requested);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
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
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
        }
    }

    fn trace() -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", "call_1", "grep_files")
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
