use crate::apply_patch::file_mutation::{
    AllowAllReadAccess, PaginatedReader, ReadError, ReadErrorCode, ReadRequest, SnapshotLimits,
};
use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::file_policy::FilePolicyCapability;
use crate::registry::ToolHandler;
use crate::{FilePolicyChecker, FilePolicyDecision, FilePolicyDenyReason, FilePolicyOperation};
use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::{BTreeSet, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[cfg(test)]
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

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
const NATIVE_FILESYSTEM_MAX_CONCURRENCY: usize = 8;
static NATIVE_FILESYSTEM_CONCURRENCY: OnceLock<Arc<Semaphore>> = OnceLock::new();

fn native_filesystem_concurrency() -> Arc<Semaphore> {
    NATIVE_FILESYSTEM_CONCURRENCY
        .get_or_init(|| Arc::new(Semaphore::new(NATIVE_FILESYSTEM_MAX_CONCURRENCY)))
        .clone()
}

async fn acquire_native_filesystem_slot(
    cancellation: &CancellationToken,
) -> Result<OwnedSemaphorePermit, ToolError> {
    acquire_native_filesystem_slot_from(native_filesystem_concurrency(), cancellation).await
}

async fn acquire_native_filesystem_slot_from(
    concurrency: Arc<Semaphore>,
    cancellation: &CancellationToken,
) -> Result<OwnedSemaphorePermit, ToolError> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(ToolError::cancelled(
                "cancelled while waiting for native filesystem capacity",
            ))
        }
        permit = concurrency.acquire_owned() => {
            permit.map_err(|_| ToolError::internal("native filesystem concurrency gate is closed"))
        }
    }
}

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
        let filesystem_slot = acquire_native_filesystem_slot(&invocation.cancellation).await?;
        let resolved = resolve_authorized_tool_path(
            invocation.execution_security_snapshot.as_ref(),
            invocation.workdir.as_path(),
            FilePolicyOperation::Read,
            args.path.as_str(),
        )?;
        let file_path = resolved.absolute.clone();
        let capability = resolved.capability.clone();
        let target = capability.canonical_target().cloned().ok_or_else(|| {
            ToolError::Rejected("read_file capability has no target object".to_owned())
        })?;
        let max_bytes = args
            .max_bytes
            .unwrap_or(DEFAULT_READ_MAX_BYTES)
            .clamp(1, HARD_MAX_READ_PAGE_BYTES) as u64;
        let max_lines = args
            .max_lines
            .unwrap_or(DEFAULT_READ_MAX_LINES)
            .clamp(1, HARD_MAX_READ_LINES) as u32;
        let (start_line, start_byte, cursor) = match (args.start_line, args.start_byte) {
            (Some(line), _) => (line.max(1).saturating_sub(1) as u64, None, None),
            (None, Some(byte)) => (0, Some(byte), None),
            (None, None) => (0, None, args.cursor),
        };
        let requested_path = file_path.clone();
        let page = tokio::task::spawn_blocking(move || {
            let _filesystem_slot = filesystem_slot;
            let file = capability
                .open_regular_file()
                .map_err(|_| ReadError::new(ReadErrorCode::PathDenied))?;
            let reader = PaginatedReader::new(
                SnapshotLimits {
                    max_file_bytes: HARD_MAX_READ_FILE_BYTES,
                    inline_threshold: 64 * 1024,
                },
                AllowAllReadAccess,
            );
            reader
                .read_target_with_file(
                    &target,
                    file,
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
        .map_err(|error| map_read_error(requested_path.as_path(), error))?;

        let display_path = display_absolute_path(file_path.as_path());
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
            "resolved_path": display_absolute_path(file_path.as_path()),
            "relative_path": relative_path(resolved.cwd.as_path(), file_path.as_path()),
            "cwd": display_absolute_path(resolved.cwd.as_path()),
            "authorized_root": display_absolute_path(resolved.authorized_root.as_path()),
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

fn map_read_error(path: &Path, error: ReadError) -> ToolError {
    let message = format!(
        "failed to read file `{}`: {error}",
        display_absolute_path(path)
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
        let filesystem_slot = acquire_native_filesystem_slot(&invocation.cancellation).await?;
        let base = args.path.unwrap_or_else(|| ".".to_owned());
        let resolved = resolve_authorized_tool_path(
            invocation.execution_security_snapshot.as_ref(),
            invocation.workdir.as_path(),
            FilePolicyOperation::Read,
            base.as_str(),
        )?;
        let root = resolved.absolute.clone();
        let depth_limit = args
            .depth
            .unwrap_or(DEFAULT_LIST_DEPTH)
            .min(HARD_MAX_LIST_DEPTH);
        let limit = args
            .limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, HARD_MAX_LIST_LIMIT);
        let include_hidden = args.include_hidden.unwrap_or(false);
        let scan_root = root.clone();
        let capability = resolved.capability.clone();
        let (items, truncated) = tokio::task::spawn_blocking(move || {
            let _filesystem_slot = filesystem_slot;
            list_directory_tree_secure_with_capability(
                scan_root.as_path(),
                &capability,
                depth_limit,
                limit,
                include_hidden,
            )
        })
        .await
        .map_err(|error| ToolError::internal(format!("directory listing task failed: {error}")))?
        .map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to securely list `{}`: {error}",
                display_workspace_path(invocation.workdir.as_path(), root.as_path())
            ))
        })?;

        let payload = serde_json::json!({
            "root": display_absolute_path(&root),
            "relative_root": relative_path(resolved.cwd.as_path(), root.as_path()),
            "cwd": display_absolute_path(resolved.cwd.as_path()),
            "authorized_root": display_absolute_path(resolved.authorized_root.as_path()),
            "truncated": truncated,
            "has_more": truncated,
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

#[cfg(unix)]
#[allow(dead_code)]
fn list_directory_tree_secure(
    root: &Path,
    depth_limit: usize,
    limit: usize,
    include_hidden: bool,
) -> std::io::Result<(Vec<DirEntryView>, bool)> {
    let root_descriptor = crate::apply_patch::file_mutation::open_directory(root)?;
    list_directory_tree_secure_from_descriptor(
        root,
        root_descriptor,
        depth_limit,
        limit,
        include_hidden,
    )
}

#[cfg(unix)]
fn list_directory_tree_secure_from_descriptor(
    root: &Path,
    root_descriptor: std::fs::File,
    depth_limit: usize,
    limit: usize,
    include_hidden: bool,
) -> std::io::Result<(Vec<DirEntryView>, bool)> {
    use std::ffi::{CStr, CString, OsString};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    struct PendingDirectory {
        descriptor: std::fs::File,
        display_path: PathBuf,
        depth: usize,
    }

    struct DirectoryStream(*mut libc::DIR);

    impl Drop for DirectoryStream {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }

    fn directory_names(
        directory: &std::fs::File,
        include_hidden: bool,
        candidate_limit: usize,
        truncated: &mut bool,
    ) -> std::io::Result<BTreeSet<OsString>> {
        let duplicate = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if duplicate < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let stream = unsafe { libc::fdopendir(duplicate) };
        if stream.is_null() {
            let error = std::io::Error::last_os_error();
            unsafe {
                libc::close(duplicate);
            }
            return Err(error);
        }
        let stream = DirectoryStream(stream);
        let mut names = BTreeSet::new();
        loop {
            let mut entry = std::mem::MaybeUninit::<libc::dirent>::zeroed();
            let mut result = std::ptr::null_mut();
            let status = unsafe { libc::readdir_r(stream.0, entry.as_mut_ptr(), &mut result) };
            if status != 0 {
                return Err(std::io::Error::from_raw_os_error(status));
            }
            if result.is_null() {
                break;
            }
            let name = unsafe { CStr::from_ptr((*result).d_name.as_ptr()) }.to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            // A directory is untrusted input. Once the bounded candidate
            // window is full, one additional eligible entry is enough to
            // prove that the result is incomplete; do not scan the rest of
            // a high-cardinality directory merely to improve lexical order.
            if names.len() >= candidate_limit {
                *truncated = true;
                break;
            }
            if !include_hidden && name.first() == Some(&b'.') {
                // Hidden entries still consume the bounded scan window.
                // Otherwise a directory containing only hidden names can
                // force an unbounded traversal.
                if names.len().saturating_add(1) >= candidate_limit {
                    *truncated = true;
                    break;
                }
                names.insert(OsString::from_vec(name.to_vec()));
                continue;
            }
            names.insert(OsString::from_vec(name.to_vec()));
        }
        Ok(names)
    }

    fn entry_metadata(
        parent: &std::fs::File,
        name: &std::ffi::OsStr,
    ) -> std::io::Result<libc::stat> {
        let name = CString::new(name.as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "directory entry contains NUL",
            )
        })?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::zeroed();
        let status = unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                metadata.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if status != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { metadata.assume_init() })
    }

    fn open_child_directory(
        parent: &std::fs::File,
        name: &std::ffi::OsStr,
    ) -> std::io::Result<std::fs::File> {
        let name = CString::new(name.as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "directory entry contains NUL",
            )
        })?;
        let descriptor = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0,
            )
        };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { std::fs::File::from_raw_fd(descriptor) })
    }

    let mut queue = VecDeque::from([PendingDirectory {
        descriptor: root_descriptor,
        display_path: root.to_path_buf(),
        depth: 0,
    }]);
    let mut items = Vec::new();
    let mut truncated = false;

    while let Some(directory) = queue.pop_front() {
        if items.len() >= limit {
            truncated = true;
            break;
        }

        let remaining = limit.saturating_sub(items.len());
        let candidate_limit = remaining.saturating_add(1);
        let names = directory_names(
            &directory.descriptor,
            include_hidden,
            candidate_limit,
            &mut truncated,
        )?;

        for name in names {
            if items.len() >= limit {
                truncated = true;
                break;
            }
            let metadata = match entry_metadata(&directory.descriptor, &name) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if !include_hidden && name.as_bytes().first() == Some(&b'.') {
                continue;
            }
            let file_type = metadata.st_mode & libc::S_IFMT;
            let entry_path = directory.display_path.join(&name);
            let kind = if file_type == libc::S_IFLNK {
                "symlink"
            } else if file_type == libc::S_IFDIR {
                "dir"
            } else if file_type == libc::S_IFREG {
                "file"
            } else {
                "other"
            };
            items.push(DirEntryView {
                path: display_lexical_absolute_path(entry_path.as_path()),
                kind: kind.to_owned(),
                size: (file_type == libc::S_IFREG).then_some(metadata.st_size.max(0) as u64),
            });

            if file_type == libc::S_IFDIR && directory.depth < depth_limit {
                match open_child_directory(&directory.descriptor, &name) {
                    Ok(descriptor) => queue.push_back(PendingDirectory {
                        descriptor,
                        display_path: entry_path,
                        depth: directory.depth.saturating_add(1),
                    }),
                    // The entry changed or disappeared after enumeration. It
                    // remains safe to report the observed item, but recursion
                    // is incomplete and must be marked as truncated.
                    Err(_) => truncated = true,
                }
            }
        }
    }

    if !queue.is_empty() {
        truncated = true;
    }
    Ok((items, truncated))
}

fn list_directory_tree_secure_with_capability(
    root: &Path,
    capability: &FilePolicyCapability,
    depth_limit: usize,
    limit: usize,
    include_hidden: bool,
) -> std::io::Result<(Vec<DirEntryView>, bool)> {
    #[cfg(unix)]
    {
        return list_directory_tree_secure_from_descriptor(
            root,
            capability.open_directory()?,
            depth_limit,
            limit,
            include_hidden,
        );
    }
    #[cfg(not(unix))]
    {
        // The capability retains the checked directory object. On Windows
        // that handle denies rename/delete sharing, so the verified pathname
        // cannot be redirected while enumeration is in progress.
        let _directory_guard = capability.open_directory()?;
        list_directory_tree_secure(root, depth_limit, limit, include_hidden)
    }
}

#[cfg(not(unix))]
fn list_directory_tree_secure(
    root: &Path,
    depth_limit: usize,
    limit: usize,
    include_hidden: bool,
) -> std::io::Result<(Vec<DirEntryView>, bool)> {
    let mut queue = VecDeque::from([(root.to_path_buf(), 0usize)]);
    let mut items = Vec::new();
    let mut truncated = false;
    while let Some((directory, depth)) = queue.pop_front() {
        if items.len() >= limit {
            truncated = true;
            break;
        }
        let remaining = limit.saturating_sub(items.len());
        let candidate_limit = remaining.saturating_add(1);
        let mut paths = BTreeSet::new();
        for entry in std::fs::read_dir(directory.as_path())? {
            let entry = entry?;
            if paths.len() >= candidate_limit {
                truncated = true;
                break;
            }
            paths.insert(entry.path());
        }
        for path in paths {
            if items.len() >= limit {
                truncated = true;
                break;
            }
            let metadata = std::fs::symlink_metadata(path.as_path())?;
            if !include_hidden
                && path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with('.'))
            {
                continue;
            }
            let file_type = metadata.file_type();
            let kind = if file_type.is_symlink() {
                "symlink"
            } else if file_type.is_dir() {
                "dir"
            } else if file_type.is_file() {
                "file"
            } else {
                "other"
            };
            items.push(DirEntryView {
                path: display_lexical_absolute_path(path.as_path()),
                kind: kind.to_owned(),
                size: file_type.is_file().then_some(metadata.len()),
            });
            if file_type.is_dir() && depth < depth_limit {
                queue.push_back((path, depth.saturating_add(1)));
            }
        }
    }
    if !queue.is_empty() {
        truncated = true;
    }
    Ok((items, truncated))
}

#[async_trait]
impl ToolHandler for GrepHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<GrepArgs>(invocation.payload)?;
        let filesystem_slot = acquire_native_filesystem_slot(&invocation.cancellation).await?;
        let resolved = resolve_authorized_tool_path(
            invocation.execution_security_snapshot.as_ref(),
            invocation.workdir.as_path(),
            FilePolicyOperation::Read,
            args.path.as_deref().unwrap_or("."),
        )?;
        let search_path = resolved.absolute.clone();
        let max_results = args
            .max_results
            .unwrap_or(DEFAULT_GREP_RESULTS)
            .clamp(1, HARD_MAX_GREP_RESULTS);
        let max_output_bytes = args
            .max_output_bytes
            .unwrap_or(DEFAULT_GREP_MAX_OUTPUT_BYTES)
            .clamp(1, HARD_MAX_GREP_OUTPUT_BYTES);
        let timeout_ms = args
            .timeout_ms
            .unwrap_or(DEFAULT_GREP_TIMEOUT_MS)
            .clamp(1, HARD_MAX_GREP_TIMEOUT_MS);
        let outcome = super::fff_grep::search(
            super::fff_grep::SearchRequest {
                path: search_path.clone(),
                workdir: resolved.cwd,
                turn_id: trace.turn_id().to_owned(),
                pattern: args.pattern,
                glob: args.glob,
                case_sensitive: args.case_sensitive.unwrap_or(true),
                max_results,
                max_output_bytes,
                timeout_ms,
                broad_file_limit: BROAD_GREP_FILE_LIMIT,
                cancellation: invocation.cancellation,
            },
            filesystem_slot,
        )
        .await?;
        match outcome {
            super::fff_grep::SearchOutcome::NeedsNarrowing {
                reason,
                message,
                scanned_file_count,
            } => Ok(needs_narrowing_output(
                message,
                search_path.as_path(),
                invocation.workdir.as_path(),
                scanned_file_count,
                max_results,
                max_output_bytes,
                reason,
            )),
            super::fff_grep::SearchOutcome::Complete {
                output,
                match_count,
                truncated,
                skipped_large_files,
            } => {
                let incomplete = truncated || skipped_large_files > 0;
                let status = if incomplete {
                    "partial"
                } else if match_count == 0 {
                    "no_matches"
                } else {
                    "ok"
                };
                let body = if incomplete && output.is_empty() {
                    match (truncated, skipped_large_files) {
                        (true, 0) => "search incomplete: output limit reached".to_owned(),
                        (true, count) => format!(
                            "search incomplete: output limit reached; {count} oversized file(s) skipped"
                        ),
                        (false, count) => {
                            format!("search incomplete: {count} oversized file(s) skipped")
                        }
                    }
                } else if output.is_empty() {
                    "no matches".to_owned()
                } else {
                    output.clone()
                };
                let payload = serde_json::json!({
                    "status": status,
                    "engine": "fff",
                    "path": display_workspace_path(invocation.workdir.as_path(), &search_path),
                    "truncated": incomplete,
                    "max_results": max_results,
                    "max_output_bytes": max_output_bytes,
                    "match_count": match_count,
                    "skipped_large_files": skipped_large_files,
                    "stdout": output,
                    "stderr": "",
                    "output": body.clone(),
                });
                Ok(Box::new(FunctionToolOutput::with_payload(
                    body, true, payload,
                )))
            }
        }
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
    let absolute_path = display_workspace_path(workdir, search_path);
    let next_action = format!(
        "Call list_dir for `{absolute_path}`, choose the smallest relevant returned directory, then call grep_files again with that absolute path and an appropriate glob. Do not repeat the same broad search."
    );
    let suggestions = serde_json::json!([{
        "tool": "list_dir",
        "arguments": {
            "path": absolute_path,
            "depth": 1,
            "limit": 200
        }
    }]);
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
        "next_action": next_action,
        "retryableByModel": true,
        "retrySameArguments": false,
    });
    let body = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| message.to_owned());
    Box::new(FunctionToolOutput::with_payload(body, false, payload))
}

fn display_workspace_path(_workdir: &Path, path: &Path) -> String {
    display_absolute_path(path)
}

fn relative_path(root: &Path, path: &Path) -> Option<String> {
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let candidate = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let relative = candidate.strip_prefix(root.as_path()).ok()?;
    Some(if relative.as_os_str().is_empty() {
        ".".to_owned()
    } else {
        relative.to_string_lossy().replace('\\', "/")
    })
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

#[derive(Debug)]
struct ResolvedToolPath {
    absolute: PathBuf,
    cwd: PathBuf,
    authorized_root: PathBuf,
    capability: FilePolicyCapability,
}

fn resolve_authorized_tool_path(
    snapshot: Option<&pioneer_protocol::TurnExecutionSecuritySnapshot>,
    fallback_workdir: &Path,
    operation: FilePolicyOperation,
    requested_path: &str,
) -> Result<ResolvedToolPath, ToolError> {
    let Some(snapshot) = snapshot else {
        let cwd = fallback_workdir
            .canonicalize()
            .unwrap_or_else(|_| fallback_workdir.to_path_buf());
        let absolute = resolve_path_within_workdir(cwd.as_path(), requested_path)?;
        let capability = FilePolicyCapability::capture_unchecked(operation, absolute.as_path())
            .map_err(|reason| {
                ToolError::Rejected(format!(
                    "filesystem capability could not be captured for `{}`: {reason:?}",
                    absolute.display()
                ))
            })?;
        return Ok(ResolvedToolPath {
            absolute,
            cwd: cwd.clone(),
            authorized_root: cwd,
            capability,
        });
    };

    let cwd = PathBuf::from(snapshot.sandbox.cwd.as_str())
        .canonicalize()
        .unwrap_or_else(|_| normalize_path_lexically(PathBuf::from(&snapshot.sandbox.cwd)));
    match FilePolicyChecker::check(snapshot, operation, Path::new(requested_path)) {
        FilePolicyDecision::Allowed(grant) => {
            let authorized_root = grant.matched_root.unwrap_or_else(|| {
                if grant.resolved_path.starts_with(cwd.as_path()) {
                    cwd.clone()
                } else {
                    grant
                        .resolved_path
                        .parent()
                        .map(Path::to_path_buf)
                        .unwrap_or_else(|| cwd.clone())
                }
            });
            Ok(ResolvedToolPath {
                absolute: grant.resolved_path,
                cwd,
                authorized_root,
                capability: grant.capability,
            })
        }
        FilePolicyDecision::Denied(deny) => {
            let roots = FilePolicyChecker::allowed_roots(snapshot, operation)
                .into_iter()
                .map(|root| display_absolute_path(root.as_path()))
                .collect::<Vec<_>>();
            let resolved = deny
                .resolved_path
                .as_deref()
                .map(display_absolute_path)
                .unwrap_or_else(|| display_absolute_path(deny.requested_path.as_path()));
            let message = format!(
                "filesystem {operation:?} denied for input `{requested_path}` (resolved `{resolved}`): {}. Current working directory: `{}`. Authorized roots for this operation: {}",
                deny.message,
                display_absolute_path(cwd.as_path()),
                if roots.is_empty() {
                    "none".to_owned()
                } else {
                    roots
                        .iter()
                        .map(|root| format!("`{root}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            );
            match deny.reason {
                FilePolicyDenyReason::EmptyPath => Err(ToolError::invalid_arguments(format!(
                    "{message}. Pass a non-empty relative path from the current working directory or an authorized absolute path."
                ))),
                FilePolicyDenyReason::MissingPath => Err(ToolError::invalid_arguments(format!(
                    "{message}. Use list_dir on the nearest existing parent, then retry with the exact returned absolute path."
                ))),
                FilePolicyDenyReason::OutsideAllowedRoots
                | FilePolicyDenyReason::SymlinkEscape
                | FilePolicyDenyReason::WriteRequiresWritableRoot
                | FilePolicyDenyReason::NoUsableRoots
                | FilePolicyDenyReason::InvalidRoot => Err(ToolError::Rejected(format!(
                    "{message}. Choose a path under an authorized root; do not repeat the same denied call."
                ))),
            }
        }
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

fn display_absolute_path(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| normalize_path_lexically(path.to_path_buf()))
        .to_string_lossy()
        .replace('\\', "/")
}

fn display_lexical_absolute_path(path: &Path) -> String {
    normalize_path_lexically(path.to_path_buf())
        .to_string_lossy()
        .replace('\\', "/")
}

fn normalize_path_lexically(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !matches!(
                    normalized.components().next_back(),
                    Some(Component::RootDir | Component::Prefix(_))
                ) {
                    normalized.pop();
                }
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ToolCallSource;
    use pioneer_protocol::{
        TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
        TurnFilesystemSandboxPath, TurnPermissionMode, TurnPermissionProfileSnapshot,
        TurnPermissionProfileSource, TurnSecurityRuleProvenance,
    };
    use std::collections::BTreeMap;

    fn invocation(
        tool_name: &str,
        payload: ToolPayload,
        cwd: &Path,
        snapshot: TurnExecutionSecuritySnapshot,
    ) -> ToolInvocation {
        ToolInvocation {
            call_id: format!("call_{tool_name}"),
            tool_name: tool_name.to_owned(),
            source: ToolCallSource::Model,
            payload,
            workdir: cwd.to_path_buf(),
            environment: BTreeMap::new(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: Some(snapshot),
            apply_patch_preflight: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn native_filesystem_concurrency_waits_and_reuses_capacity() {
        let concurrency = Arc::new(Semaphore::new(1));
        let holder =
            acquire_native_filesystem_slot_from(concurrency.clone(), &CancellationToken::new())
                .await
                .expect("first filesystem operation should acquire the only slot");

        let waiting_cancellation = CancellationToken::new();
        let waiting =
            acquire_native_filesystem_slot_from(concurrency.clone(), &waiting_cancellation);
        tokio::pin!(waiting);
        assert!(
            timeout(Duration::from_millis(25), &mut waiting)
                .await
                .is_err(),
            "a second operation must wait instead of exceeding concurrency"
        );

        drop(holder);
        let next = timeout(Duration::from_secs(1), &mut waiting)
            .await
            .expect("waiting operation should resume when capacity is released")
            .expect("released filesystem capacity should remain reusable");
        drop(next);

        for _ in 0..32 {
            let permit =
                acquire_native_filesystem_slot_from(concurrency.clone(), &CancellationToken::new())
                    .await
                    .expect("completed operations must not consume a lifetime quota");
            drop(permit);
        }
    }

    #[tokio::test]
    async fn native_filesystem_concurrency_wait_is_cancellation_safe() {
        let concurrency = Arc::new(Semaphore::new(1));
        let holder =
            acquire_native_filesystem_slot_from(concurrency.clone(), &CancellationToken::new())
                .await
                .expect("first filesystem operation should acquire the only slot");

        let cancellation = CancellationToken::new();
        let waiter = tokio::spawn({
            let concurrency = concurrency.clone();
            let cancellation = cancellation.clone();
            async move { acquire_native_filesystem_slot_from(concurrency, &cancellation).await }
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the queued operation must still be waiting for capacity"
        );

        cancellation.cancel();
        let error = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancelled capacity wait should finish promptly")
            .expect("capacity waiter should not panic")
            .expect_err("cancelled capacity wait must not execute");
        assert!(matches!(error, ToolError::Cancelled(_)));

        drop(holder);
        let permit = timeout(
            Duration::from_secs(1),
            acquire_native_filesystem_slot_from(concurrency, &CancellationToken::new()),
        )
        .await
        .expect("capacity should not leak after cancellation")
        .expect("capacity should be available after the holder finishes");
        drop(permit);
    }

    fn snapshot(cwd: &Path, additional: &Path) -> TurnExecutionSecuritySnapshot {
        TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            cwd.to_string_lossy(),
            vec![
                TurnFilesystemSandboxEntry::workspace_root(
                    TurnFilesystemAccess::Write,
                    cwd.to_string_lossy(),
                ),
                TurnFilesystemSandboxEntry {
                    path: TurnFilesystemSandboxPath::ExplicitPath {
                        path: additional.to_string_lossy().into_owned(),
                    },
                    access: TurnFilesystemAccess::Read,
                    provenance: TurnSecurityRuleProvenance::Project,
                    resolved_path: Some(additional.to_string_lossy().into_owned()),
                },
            ],
            1,
        )
    }

    #[tokio::test]
    async fn file_tools_use_dynamic_cwd_and_additional_roots_without_path_ambiguity() {
        let cwd = tempfile::tempdir().unwrap();
        let additional = tempfile::tempdir().unwrap();
        let relative_file = cwd.path().join("relative.txt");
        let absolute_file = additional.path().join("absolute.txt");
        std::fs::write(&relative_file, "relative\n").unwrap();
        std::fs::write(&absolute_file, "absolute\n").unwrap();
        let security = snapshot(cwd.path(), additional.path());

        let relative_read = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({"path": "relative.txt"}),
                    },
                    cwd.path(),
                    security.clone(),
                ),
                crate::events::ToolEventBus::default().start_trace(
                    "turn_files",
                    "call_read_relative",
                    "read_file",
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            relative_read.raw_json()["path"],
            relative_file
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );

        let absolute_read = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({"path": absolute_file}),
                    },
                    cwd.path(),
                    security.clone(),
                ),
                crate::events::ToolEventBus::default().start_trace(
                    "turn_files",
                    "call_read_absolute",
                    "read_file",
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            absolute_read.raw_json()["path"],
            absolute_file
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );
        assert_eq!(
            absolute_read.raw_json()["cwd"],
            cwd.path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );
        assert_eq!(
            absolute_read.raw_json()["authorized_root"],
            additional
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );

        let listing = ListDirHandler
            .handle(
                invocation(
                    "list_dir",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": additional.path(),
                            "depth": 0,
                            "limit": 10
                        }),
                    },
                    cwd.path(),
                    security,
                ),
                crate::events::ToolEventBus::default().start_trace(
                    "turn_files",
                    "call_list_absolute",
                    "list_dir",
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            listing.raw_json()["root"],
            additional
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );
        assert_eq!(
            listing.raw_json()["entries"][0]["path"],
            absolute_file
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );
    }

    #[tokio::test]
    async fn read_file_prefers_start_line_when_both_offsets_are_supplied() {
        let root = tempfile::tempdir().expect("workspace root");
        std::fs::write(root.path().join("lines.txt"), "alpha\nbeta\ngamma\n")
            .expect("read fixture");
        let security = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            root.path().to_string_lossy(),
            1,
        );
        let bus = crate::events::ToolEventBus::default();
        let by_line = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": "lines.txt",
                            "start_line": 2,
                            "start_byte": 0,
                            "cursor": "0",
                            "max_lines": 1
                        }),
                    },
                    root.path(),
                    security.clone(),
                ),
                bus.start_trace("turn_read_offsets", "call_both_offsets", "read_file"),
            )
            .await
            .expect("start_line should take precedence over byte and cursor");
        assert_eq!(by_line.raw_json()["text"], "beta\n");

        let by_byte = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": "lines.txt",
                            "start_byte": 11,
                            "cursor": "",
                            "max_lines": 1
                        }),
                    },
                    root.path(),
                    security.clone(),
                ),
                bus.start_trace("turn_read_offsets", "call_byte_only", "read_file"),
            )
            .await
            .expect("start_byte should take precedence over cursor");
        assert_eq!(by_byte.raw_json()["text"], "gamma\n");

        let first_page = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": "lines.txt",
                            "max_lines": 1
                        }),
                    },
                    root.path(),
                    security.clone(),
                ),
                bus.start_trace("turn_read_offsets", "call_first_page", "read_file"),
            )
            .await
            .expect("first page should be readable");
        assert_eq!(first_page.raw_json()["text"], "alpha\n");
        let continuation = first_page.raw_json()["continuation"]
            .as_str()
            .expect("continuation")
            .to_owned();
        let next_page = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": "lines.txt",
                            "cursor": continuation,
                            "max_lines": 1
                        }),
                    },
                    root.path(),
                    security.clone(),
                ),
                bus.start_trace("turn_read_offsets", "call_next_page", "read_file"),
            )
            .await
            .expect("valid cursor should be used without explicit offsets");
        assert_eq!(next_page.raw_json()["text"], "beta\n");

        let invalid_cursor = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": "lines.txt",
                            "cursor": "0"
                        }),
                    },
                    root.path(),
                    security,
                ),
                bus.start_trace("turn_read_offsets", "call_invalid_cursor", "read_file"),
            )
            .await;
        assert!(invalid_cursor.is_err());
    }

    #[tokio::test]
    async fn repeated_small_file_operations_do_not_exhaust_the_turn() {
        let root = tempfile::tempdir().expect("workspace root");
        let file = root.path().join("small.txt");
        std::fs::write(file.as_path(), "small-file\n").expect("small file fixture");
        let security = TurnExecutionSecuritySnapshot::read_only(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            root.path().to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                root.path().to_string_lossy(),
            )],
            1,
        );
        let event_bus = crate::events::ToolEventBus::default();
        let turn_id = "turn_long_running_file_work";

        let listing = ListDirHandler
            .handle(
                invocation(
                    "list_dir",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": root.path(),
                            "depth": 0,
                            "limit": 10
                        }),
                    },
                    root.path(),
                    security.clone(),
                ),
                event_bus.start_trace(turn_id, "call_list", "list_dir"),
            )
            .await
            .expect("bounded listing should succeed");
        assert_eq!(
            listing.raw_json()["entries"].as_array().map(Vec::len),
            Some(1)
        );

        // The removed lifetime budget charged every read as 48 MiB regardless
        // of the actual file size. Its sixth read in this sequence failed even
        // though every individual operation was well within the existing
        // per-operation limits.
        for index in 0..8 {
            let output = ReadFileHandler
                .handle(
                    invocation(
                        "read_file",
                        ToolPayload::Function {
                            arguments: serde_json::json!({"path": "small.txt"}),
                        },
                        root.path(),
                        security.clone(),
                    ),
                    event_bus.start_trace(turn_id, format!("call_read_{index}"), "read_file"),
                )
                .await
                .expect("completed file operations must not consume a lifetime Turn quota");
            assert_eq!(output.raw_json()["text"], "small-file\n");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn slash_cwd_keeps_leading_slashes_in_reusable_file_tool_paths() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("file.txt");
        std::fs::write(&file, "absolute\n").unwrap();
        let security = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "/",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "/",
            )],
            1,
        );

        let listing = ListDirHandler
            .handle(
                invocation(
                    "list_dir",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": directory.path(),
                            "depth": 0,
                            "limit": 10
                        }),
                    },
                    Path::new("/"),
                    security.clone(),
                ),
                crate::events::ToolEventBus::default().start_trace(
                    "turn_slash_files",
                    "call_list_slash",
                    "list_dir",
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            listing.raw_json()["root"],
            directory
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );
        let listing_payload = listing.raw_json();
        let listed_path = listing_payload["entries"][0]["path"].as_str().unwrap();
        assert!(listed_path.starts_with('/'));

        let read = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({"path": listed_path}),
                    },
                    Path::new("/"),
                    security,
                ),
                crate::events::ToolEventBus::default().start_trace(
                    "turn_slash_files",
                    "call_read_slash",
                    "read_file",
                ),
            )
            .await
            .unwrap();
        assert_eq!(read.raw_json()["path"], listed_path);
        assert_eq!(read.raw_json()["text"], "absolute\n");
    }

    #[cfg(unix)]
    #[test]
    fn secure_directory_listing_never_descends_through_symlinks() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("listing root");
        let outside = tempfile::tempdir().expect("outside root");
        std::fs::write(outside.path().join("secret.txt"), "outside-secret")
            .expect("outside secret");
        symlink(outside.path(), root.path().join("escape")).expect("directory symlink");

        let (entries, _) = list_directory_tree_secure(root.path(), HARD_MAX_LIST_DEPTH, 100, true)
            .expect("secure listing");
        assert!(
            entries.iter().any(|entry| {
                entry.path.ends_with("/escape") && entry.kind.as_str() == "symlink"
            })
        );
        assert!(
            !entries
                .iter()
                .any(|entry| entry.path.ends_with("/escape/secret.txt"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn directory_listing_stops_after_bounded_candidate_window() {
        let root = tempfile::tempdir().expect("listing root");
        for index in 0..1_000 {
            std::fs::write(root.path().join(format!("entry-{index:04}.txt")), b"x")
                .expect("high-cardinality entry");
        }

        let (entries, truncated) =
            list_directory_tree_secure(root.path(), HARD_MAX_LIST_DEPTH, 2, true)
                .expect("bounded listing");
        assert_eq!(entries.len(), 2);
        assert!(
            truncated,
            "the candidate window must report omitted entries"
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn grep_files_uses_fff_for_restricted_turn() {
        let root = tempfile::tempdir().expect("workspace root");
        let search_dir = root.path().join("src");
        std::fs::create_dir_all(search_dir.as_path()).expect("search dir");
        std::fs::write(
            search_dir.join("sample.txt"),
            "alpha\npioneer-permission-marker\nomega\n",
        )
        .expect("search fixture");
        let security = TurnExecutionSecuritySnapshot::read_only(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            root.path().to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                root.path().to_string_lossy(),
            )],
            1,
        );

        let event_bus = crate::events::ToolEventBus::default();
        let turn_id = "turn_grep_files";
        let output = GrepHandler
            .handle(
                invocation(
                    "grep_files",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "pattern": "pioneer-permission-marker",
                            "path": search_dir.clone(),
                            "max_results": 10,
                            "timeout_ms": 5_000
                        }),
                    },
                    root.path(),
                    security.clone(),
                ),
                event_bus.start_trace(turn_id, "call_grep_files", "grep_files"),
            )
            .await
            .expect("grep_files should search with FFF");

        assert_eq!(output.raw_json()["status"], "ok");
        assert_eq!(output.raw_json()["engine"], "fff");
        assert!(
            output.raw_json()["stdout"]
                .as_str()
                .is_some_and(|stdout| stdout.contains("pioneer-permission-marker"))
        );

        // A successful grep previously reserved the complete 256 MiB lifetime
        // allowance and made every subsequent native file operation fail for
        // this Turn. Per-operation safeguards must not poison later calls.
        let read = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "path": search_dir.join("sample.txt")
                        }),
                    },
                    root.path(),
                    security,
                ),
                event_bus.start_trace(turn_id, "call_read_after_grep", "read_file"),
            )
            .await
            .expect("grep_files must not disable subsequent file operations");
        assert_eq!(
            read.raw_json()["text"],
            "alpha\npioneer-permission-marker\nomega\n"
        );
    }

    #[tokio::test]
    async fn grep_files_fff_backend_honors_glob_and_exact_no_matches() {
        let root = tempfile::tempdir().expect("workspace root");
        let source = root.path().join("main.rs");
        std::fs::write(&source, "fn exact_marker() {}\n").expect("source fixture");
        std::fs::write(
            root.path().join("notes.txt"),
            "exact_marker\nnotes_only_marker\n",
        )
        .expect("excluded fixture");
        let security = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            root.path().to_string_lossy(),
            1,
        );
        let bus = crate::events::ToolEventBus::default();
        let found = GrepHandler
            .handle(
                invocation(
                    "grep_files",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "pattern": "exact_marker",
                            "path": root.path(),
                            "glob": "*.rs",
                        }),
                    },
                    root.path(),
                    security.clone(),
                ),
                bus.start_trace("turn_fff", "call_match", "grep_files"),
            )
            .await
            .expect("FFF search succeeds");
        assert_eq!(found.raw_json()["status"], "ok");
        assert_eq!(found.raw_json()["engine"], "fff");
        assert!(
            found.raw_json()["stdout"]
                .as_str()
                .unwrap()
                .contains("main.rs")
        );
        assert!(
            !found.raw_json()["stdout"]
                .as_str()
                .unwrap()
                .contains("notes.txt")
        );

        let absent = GrepHandler
            .handle(
                invocation(
                    "grep_files",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "pattern": "notes_only_marker",
                            "path": root.path(),
                            "glob": "*.rs",
                        }),
                    },
                    root.path(),
                    security,
                ),
                bus.start_trace("turn_fff", "call_no_match", "grep_files"),
            )
            .await
            .expect("zero matches is a successful search");
        assert_eq!(absent.raw_json()["status"], "no_matches");
        assert_eq!(absent.raw_json()["stdout"], "");
    }

    #[tokio::test]
    async fn grep_files_indexes_only_the_requested_file() {
        let root = tempfile::tempdir().expect("workspace root");
        let selected = root.path().join("selected.rs");
        std::fs::write(&selected, "selected_marker\n").expect("selected fixture");
        std::fs::write(root.path().join("sibling.rs"), "sibling_marker\n")
            .expect("sibling fixture");
        let security = TurnExecutionSecuritySnapshot::read_only(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            root.path().to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                root.path().to_string_lossy(),
            )],
            1,
        );
        let output = GrepHandler
            .handle(
                invocation(
                    "grep_files",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "pattern": "selected_marker|sibling_marker",
                            "path": selected,
                        }),
                    },
                    root.path(),
                    security,
                ),
                crate::events::ToolEventBus::default().start_trace(
                    "turn_single_file",
                    "call_single_file",
                    "grep_files",
                ),
            )
            .await
            .expect("FFF file search succeeds");
        assert_eq!(output.raw_json()["status"], "ok");
        assert_eq!(output.raw_json()["engine"], "fff");
        let raw = output.raw_json();
        let stdout = raw["stdout"].as_str().expect("stdout");
        assert!(stdout.contains("selected_marker"));
        assert!(!stdout.contains("sibling_marker"));
    }

    #[tokio::test]
    async fn grep_files_fff_keeps_match_after_clipped_line_prefix() {
        let root = tempfile::tempdir().expect("workspace root");
        std::fs::write(
            root.path().join("long.rs"),
            format!("{}needle_at_tail\n", "x".repeat(600)),
        )
        .expect("long line fixture");
        let security = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            root.path().to_string_lossy(),
            1,
        );
        let output = GrepHandler
            .handle(
                invocation(
                    "grep_files",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "pattern": "needle_at_tail",
                            "path": root.path(),
                            "glob": "*.rs",
                        }),
                    },
                    root.path(),
                    security,
                ),
                crate::events::ToolEventBus::default().start_trace(
                    "turn_long_grep",
                    "call_long_grep",
                    "grep_files",
                ),
            )
            .await
            .expect("FFF long-line search succeeds");
        assert_eq!(output.raw_json()["status"], "ok");
        assert!(
            output.raw_json()["stdout"]
                .as_str()
                .unwrap()
                .contains("needle_at_tail")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn grep_files_scoped_glob_does_not_read_symlink_escape() {
        let root = tempfile::tempdir().expect("authorized root");
        let outside = tempfile::tempdir().expect("outside root");
        std::fs::write(root.path().join("inside.rs"), "inside_marker\n").expect("inside fixture");
        std::fs::write(outside.path().join("secret.rs"), "outside_marker\n")
            .expect("outside fixture");
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape"))
            .expect("escape symlink");
        std::os::unix::fs::symlink(
            outside.path().join("secret.rs"),
            root.path().join("escape_file.rs"),
        )
        .expect("file escape symlink");
        let security = TurnExecutionSecuritySnapshot::read_only(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            root.path().to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                root.path().to_string_lossy(),
            )],
            1,
        );
        let bus = crate::events::ToolEventBus::default();
        let output = GrepHandler
            .handle(
                invocation(
                    "grep_files",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "pattern": "inside_marker|outside_marker",
                            "path": root.path(),
                            "glob": "*.rs",
                        }),
                    },
                    root.path(),
                    security,
                ),
                bus.start_trace("turn_scoped", "call_glob", "grep_files"),
            )
            .await
            .expect("scoped search succeeds");
        assert_eq!(output.raw_json()["status"], "ok");
        let raw = output.raw_json();
        let stdout = raw["stdout"].as_str().unwrap();
        assert!(stdout.contains("inside_marker"));
        assert!(!stdout.contains("outside_marker"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn broad_grep_needs_narrowing_does_not_poison_later_file_calls() {
        let root = tempfile::tempdir().expect("workspace root");
        let marker = root.path().join("marker.txt");
        std::fs::write(marker.as_path(), "still-readable\n").expect("read marker");
        for index in 0..BROAD_GREP_FILE_LIMIT {
            std::fs::write(root.path().join(format!("candidate-{index:04}.txt")), b"")
                .expect("broad grep fixture");
        }
        let security = TurnExecutionSecuritySnapshot::read_only(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            root.path().to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                root.path().to_string_lossy(),
            )],
            1,
        );
        let event_bus = crate::events::ToolEventBus::default();
        let turn_id = "turn_broad_grep";

        let grep = GrepHandler
            .handle(
                invocation(
                    "grep_files",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "pattern": "absent-pattern",
                            "timeout_ms": 5_000
                        }),
                    },
                    root.path(),
                    security.clone(),
                ),
                event_bus.start_trace(turn_id, "call_broad_grep", "grep_files"),
            )
            .await
            .expect("broad grep should return a structured narrowing result");
        assert_eq!(grep.raw_json()["status"], "needs_narrowing");

        let read = ReadFileHandler
            .handle(
                invocation(
                    "read_file",
                    ToolPayload::Function {
                        arguments: serde_json::json!({"path": "marker.txt"}),
                    },
                    root.path(),
                    security,
                ),
                event_bus.start_trace(turn_id, "call_read_after_narrowing", "read_file"),
            )
            .await
            .expect("a narrowing result must not disable later file operations");
        assert_eq!(read.raw_json()["text"], "still-readable\n");
    }
}
