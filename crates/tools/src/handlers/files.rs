use crate::apply_patch::file_mutation::{
    AllowAllReadAccess, PaginatedReader, ReadError, ReadErrorCode, ReadRequest, SnapshotLimits,
};
use crate::context::{
    ExecCommandArgs, FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload,
};
use crate::error::ToolError;
use crate::file_policy::FilePolicyCapability;
use crate::registry::ToolHandler;
use crate::{
    FilePolicyChecker, FilePolicyDecision, FilePolicyDenyReason, FilePolicyOperation,
    NativeSandboxPrepareOutcome, NativeSandboxRequest, NonoSandboxBackend, ProcessSpawnPlan,
    WindowsRestrictedTokenBackend, build_process_spawn_plan, configure_nono_command,
    configure_windows_restricted_token_command, prepare_native_sandbox_backend,
};
use async_trait::async_trait;
use pioneer_protocol::{SandboxBackendKind, TurnExecutionSecuritySnapshot};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::io::{self, ErrorKind};
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
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
        trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<ReadFileArgs>(invocation.payload)?;
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
        if args.start_byte.is_some() && args.start_line.is_some() {
            return Err(ToolError::invalid_arguments(
                "read_file accepts either start_line or start_byte, not both",
            ));
        }
        let start_line = args.start_line.unwrap_or(1).max(1).saturating_sub(1) as u64;
        let start_byte = args.start_byte;
        let cursor = args.cursor;
        let requested_path = file_path.clone();
        let filesystem_permit = trace
            // Snapshot validation may read the object before and after the
            // selected page. Account for all three bounded passes.
            .acquire_filesystem_io_budget("read_file", HARD_MAX_READ_FILE_BYTES.saturating_mul(3))
            .await
            .map_err(ToolError::Rejected)?;
        let page = tokio::task::spawn_blocking(move || {
            let _filesystem_permit = filesystem_permit;
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
        trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<ListDirArgs>(invocation.payload)?;
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
        let filesystem_permit = trace
            .acquire_filesystem_io_budget("list_dir", (limit as u64).saturating_mul(4 * 1024))
            .await
            .map_err(ToolError::Rejected)?;

        let scan_root = root.clone();
        let capability = resolved.capability.clone();
        let (items, truncated) = tokio::task::spawn_blocking(move || {
            let _filesystem_permit = filesystem_permit;
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
        let base = args.path.as_deref().unwrap_or(".");
        let resolved = resolve_authorized_tool_path(
            invocation.execution_security_snapshot.as_ref(),
            invocation.workdir.as_path(),
            FilePolicyOperation::Read,
            base,
        )?;
        let search_path = resolved.absolute;
        #[cfg(unix)]
        let (search_descriptor, command_search_path) = {
            let target = resolved.capability.open_target().map_err(|error| {
                ToolError::Rejected(format!(
                    "grep_files could not retain the authorized search object: {error}"
                ))
            })?;
            let target_is_directory = target
                .metadata()
                .map(|metadata| metadata.is_dir())
                .map_err(|error| {
                    ToolError::Rejected(format!(
                        "grep_files could not inspect the authorized search object: {error}"
                    ))
                })?;
            let (descriptor, path) = inherited_descriptor_path(target)?;
            let command_path = if target_is_directory {
                PathBuf::from(".")
            } else {
                path
            };
            (Some(descriptor), command_path)
        };
        #[cfg(not(unix))]
        let (search_descriptor, command_search_path): (Option<File>, PathBuf) = (
            Some(resolved.capability.open_target().map_err(|error| {
                ToolError::Rejected(format!(
                    "grep_files could not retain the authorized search object: {error}"
                ))
            })?),
            search_path.clone(),
        );

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
        let _filesystem_permit = trace
            // `rg` performs filesystem reads outside this process, so reserve
            // the entire Turn I/O allowance up front. This makes the external
            // scan a single bounded operation instead of pretending its small
            // output size measures the bytes searched.
            .acquire_filesystem_io_budget(
                "grep_files",
                crate::resource_budget::TURN_FILESYSTEM_MAX_BYTES,
            )
            .await
            .map_err(ToolError::Rejected)?;
        let workspace_root = resolved.cwd;
        let is_broad_workspace_search = args.glob.is_none()
            && (args.path.is_none()
                || search_path
                    .canonicalize()
                    .unwrap_or_else(|_| search_path.to_path_buf())
                    == workspace_root);
        if is_broad_workspace_search {
            match count_rg_search_files(
                command_search_path.as_path(),
                invocation.workdir.as_path(),
                invocation.execution_security_snapshot.as_ref(),
                timeout_ms.min(3_000),
                search_descriptor.as_ref(),
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
            command_search_path.as_path(),
            invocation.workdir.as_path(),
            invocation.execution_security_snapshot.as_ref(),
            timeout_ms,
            search_descriptor.as_ref(),
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
                    command_search_path.as_path(),
                    invocation.workdir.as_path(),
                    invocation.execution_security_snapshot.as_ref(),
                    timeout_ms,
                    search_descriptor.as_ref(),
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
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    timeout_ms: u64,
    search_descriptor: Option<&File>,
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
    let process_plan = prepare_scoped_search_command(&mut command, snapshot, workdir, timeout_ms)?;
    configure_descriptor_search(&mut command, search_path, search_descriptor);

    run_bounded_command(
        command,
        timeout_ms,
        HARD_MAX_GREP_OUTPUT_BYTES,
        "rg",
        process_plan,
    )
    .await
}

async fn run_grep_fallback(
    pattern: &str,
    case_sensitive: bool,
    search_path: &Path,
    workdir: &Path,
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    timeout_ms: u64,
    search_descriptor: Option<&File>,
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
    let process_plan = prepare_scoped_search_command(&mut command, snapshot, workdir, timeout_ms)?;
    configure_descriptor_search(&mut command, search_path, search_descriptor);

    run_bounded_command(
        command,
        timeout_ms,
        HARD_MAX_GREP_OUTPUT_BYTES,
        "grep",
        process_plan,
    )
    .await?
    .ok_or_else(|| ToolError::execution_failed("grep executable is unavailable"))
}

async fn count_rg_search_files(
    search_path: &Path,
    workdir: &Path,
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    timeout_ms: u64,
    search_descriptor: Option<&File>,
) -> Result<Option<usize>, ToolError> {
    let mut command = Command::new("rg");
    command.arg("--files");
    append_default_rg_excludes(&mut command);
    command.arg("--");
    command.arg(command_path(workdir, search_path));
    let process_plan = prepare_scoped_search_command(&mut command, snapshot, workdir, timeout_ms)?;
    configure_descriptor_search(&mut command, search_path, search_descriptor);

    let Some(output) = run_bounded_command(
        command,
        timeout_ms,
        HARD_MAX_GREP_OUTPUT_BYTES,
        "rg --files",
        process_plan,
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

fn prepare_scoped_search_command(
    command: &mut Command,
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    workdir: &Path,
    timeout_ms: u64,
) -> Result<Option<ProcessSpawnPlan>, ToolError> {
    let Some(snapshot) = snapshot else {
        // Legacy direct handler tests may not carry a turn snapshot. Product
        // agent execution rejects a missing snapshot before tool dispatch.
        command.current_dir(workdir);
        return Ok(None);
    };

    let std_command = command.as_std();
    let program = std_command.get_program().to_string_lossy().into_owned();
    let argv = std::iter::once(program)
        .chain(
            std_command
                .get_args()
                .map(|argument| argument.to_string_lossy().into_owned()),
        )
        .collect::<Vec<_>>();
    let args = ExecCommandArgs {
        command: Some(argv),
        workdir: None,
        timeout_ms: Some(timeout_ms),
        max_output_tokens: None,
        yield_time_ms: None,
        tty: Some(false),
    };
    // Search helpers need no turn-projected variables (including the artifact
    // output directory). Build their environment from the immutable process
    // policy so host secrets and rg configuration cannot leak into an
    // unapproved internal subprocess.
    let process_plan =
        build_process_spawn_plan(Some(snapshot), workdir, &args, &BTreeMap::new(), timeout_ms)?;
    command.current_dir(process_plan.cwd.as_path());
    if !process_plan.inherit_environment {
        command.env_clear();
    }
    for key in &process_plan.removed_environment {
        command.env_remove(key);
    }
    command.envs(process_plan.environment.iter());

    match snapshot.backend.sandbox_backend {
        None => {}
        Some(SandboxBackendKind::Nono) => {
            let backend = NonoSandboxBackend::new();
            let request = NativeSandboxRequest {
                snapshot,
                process_plan: &process_plan,
                workspace_roots: &[],
                execution_label: "grep_files",
            };
            match prepare_native_sandbox_backend(&backend, &request)? {
                NativeSandboxPrepareOutcome::Ready(_) => {
                    configure_nono_command(command, snapshot, &process_plan)?;
                }
                NativeSandboxPrepareOutcome::Degraded { reason, .. }
                | NativeSandboxPrepareOutcome::Unavailable { reason, .. } => {
                    return Err(ToolError::Rejected(format!(
                        "grep_files sandbox is unavailable: {reason}"
                    )));
                }
            }
        }
        Some(SandboxBackendKind::WindowsRestrictedToken) => {
            let backend = WindowsRestrictedTokenBackend::new();
            let request = NativeSandboxRequest {
                snapshot,
                process_plan: &process_plan,
                workspace_roots: &[],
                execution_label: "grep_files",
            };
            match prepare_native_sandbox_backend(&backend, &request)? {
                NativeSandboxPrepareOutcome::Ready(_) => {
                    configure_windows_restricted_token_command(command, snapshot, &process_plan)?;
                }
                NativeSandboxPrepareOutcome::Degraded { reason, .. }
                | NativeSandboxPrepareOutcome::Unavailable { reason, .. } => {
                    return Err(ToolError::Rejected(format!(
                        "grep_files sandbox is unavailable: {reason}"
                    )));
                }
            }
        }
        Some(SandboxBackendKind::ProviderNative) => {
            return Err(ToolError::Rejected(
                "provider-native sandbox cannot protect Pioneer grep_files execution".to_owned(),
            ));
        }
    }

    Ok(Some(process_plan))
}

async fn run_bounded_command(
    mut command: Command,
    timeout_ms: u64,
    max_output_bytes: usize,
    executable_name: &str,
    _process_plan: Option<ProcessSpawnPlan>,
) -> Result<Option<BoundedCommandOutput>, ToolError> {
    command.kill_on_drop(true);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
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

fn command_path(_workdir: &Path, path: &Path) -> PathBuf {
    path.to_path_buf()
}

#[cfg(unix)]
fn inherited_descriptor_path(file: File) -> Result<(File, PathBuf), ToolError> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let duplicate = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(ToolError::Rejected(format!(
            "failed to duplicate authorized filesystem descriptor: {}",
            io::Error::last_os_error()
        )));
    }
    let flags = unsafe { libc::fcntl(duplicate, libc::F_GETFD) };
    if flags < 0 {
        let error = io::Error::last_os_error();
        unsafe {
            libc::close(duplicate);
        }
        return Err(ToolError::Rejected(format!(
            "failed to inspect authorized filesystem descriptor: {error}"
        )));
    }
    if unsafe { libc::fcntl(duplicate, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } != 0 {
        let error = io::Error::last_os_error();
        unsafe {
            libc::close(duplicate);
        }
        return Err(ToolError::Rejected(format!(
            "failed to retain authorized filesystem descriptor for search: {error}"
        )));
    }
    let path = PathBuf::from(format!("/dev/fd/{duplicate}"));
    Ok((unsafe { File::from_raw_fd(duplicate) }, path))
}

#[cfg(unix)]
fn configure_descriptor_search(
    command: &mut Command,
    search_path: &Path,
    descriptor: Option<&File>,
) {
    use std::os::fd::AsRawFd;

    if search_path != Path::new(".") {
        return;
    }
    let Some(descriptor) = descriptor else {
        return;
    };
    let fd = descriptor.as_raw_fd();
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(fd) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_descriptor_search(
    _command: &mut Command,
    _search_path: &Path,
    _descriptor: Option<&File>,
) {
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
    async fn grep_files_returns_matches_through_native_sandbox_backend() {
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

        let output = GrepHandler
            .handle(
                invocation(
                    "grep_files",
                    ToolPayload::Function {
                        arguments: serde_json::json!({
                            "pattern": "pioneer-permission-marker",
                            "path": search_dir,
                            "max_results": 10,
                            "timeout_ms": 5_000
                        }),
                    },
                    root.path(),
                    security,
                ),
                crate::events::ToolEventBus::default().start_trace(
                    "turn_grep_files",
                    "call_grep_files",
                    "grep_files",
                ),
            )
            .await
            .expect("grep_files should execute inside the native sandbox");

        assert_eq!(output.raw_json()["status"], "ok");
        assert!(
            output.raw_json()["stdout"]
                .as_str()
                .is_some_and(|stdout| stdout.contains("pioneer-permission-marker"))
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn grep_helper_process_cannot_read_outside_native_sandbox() {
        let root = tempfile::tempdir().expect("authorized root");
        let outside = tempfile::tempdir().expect("outside root");
        let secret = outside.path().join("secret.txt");
        std::fs::write(secret.as_path(), "outside-secret").expect("outside secret");
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
        let mut command = Command::new("/bin/cat");
        command.arg(secret.as_path());
        let process_plan =
            prepare_scoped_search_command(&mut command, Some(&security), root.path(), 2_000)
                .expect("native sandbox should prepare");

        let output = run_bounded_command(command, 2_000, 16 * 1024, "sandbox probe", process_plan)
            .await
            .expect("sandbox probe should spawn")
            .expect("shell is available");

        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stdout).contains("outside-secret"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("outside-secret"));
    }
}
