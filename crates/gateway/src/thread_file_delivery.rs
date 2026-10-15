use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pioneer_protocol::THREAD_FILE_VIEW_MAX_BYTES;
use pioneer_tools::apply_patch::file_mutation::open_regular_file;
use url::Url;

use crate::authorization::{
    AuthorizationResolver, AuthorizationService, ProofResolution, ResourceAction,
};
use crate::message::MessageProcessor;
use crate::request_context::AuthenticatedRequestContext;
use crate::view_grants::ThreadFileViewGrantScope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedThreadFile {
    pub(crate) canonical_root: PathBuf,
    pub(crate) canonical_path: PathBuf,
    pub(crate) file_name: String,
    pub(crate) content_type: String,
    pub(crate) size_bytes: u64,
    pub(crate) line: Option<u32>,
    pub(crate) column: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct ThreadFileContent {
    pub(crate) bytes: Vec<u8>,
    pub(crate) file_name: String,
    pub(crate) content_type: String,
}

#[derive(Debug)]
pub(crate) enum ThreadFileDeliveryError {
    Denied,
    AuthorizationUnavailable,
    InvalidReference,
    OutsideWorkspace,
    NotFound,
    NotText,
    TooLarge,
    Unavailable,
}

#[derive(Clone)]
pub(crate) struct ThreadFileDeliveryService {
    processor: Arc<MessageProcessor>,
}

impl ThreadFileDeliveryService {
    pub(crate) fn new(processor: Arc<MessageProcessor>) -> Self {
        Self { processor }
    }

    pub(crate) async fn authorize_and_read(
        &self,
        request: &AuthenticatedRequestContext,
        scope: &ThreadFileViewGrantScope,
    ) -> Result<ThreadFileContent, ThreadFileDeliveryError> {
        self.authorize_thread(request, scope).await?;
        let path = scope.canonical_path.clone();
        let root = scope.canonical_root.clone();
        let file_name = scope.file_name.clone();
        let content_type = scope.content_type.clone();
        let bytes = tokio::task::spawn_blocking(move || read_text_file(&root, &path))
            .await
            .map_err(|_| ThreadFileDeliveryError::Unavailable)??;
        self.authorize_thread(request, scope).await?;
        Ok(ThreadFileContent {
            bytes,
            file_name,
            content_type,
        })
    }

    async fn authorize_thread(
        &self,
        request: &AuthenticatedRequestContext,
        scope: &ThreadFileViewGrantScope,
    ) -> Result<(), ThreadFileDeliveryError> {
        let action = ResourceAction::ThreadRead;
        let action_gate = AuthorizationService::new().authorize_action(
            request.principal().kind,
            request.role_key(),
            action,
        );
        let resolution = AuthorizationResolver::new((*self.processor.crud_store).clone())
            .authorize_thread(
                request.principal(),
                &action_gate,
                action,
                scope.thread_id.as_str(),
                Some(scope.workspace_id.as_str()),
            )
            .await
            .map_err(|_| ThreadFileDeliveryError::AuthorizationUnavailable)?;
        match resolution {
            ProofResolution::Authorized(proof)
                if proof.workspace_id() == scope.workspace_id
                    && proof.thread_id() == scope.thread_id =>
            {
                Ok(())
            }
            ProofResolution::Authorized(_) => Err(ThreadFileDeliveryError::NotFound),
            ProofResolution::Denied(_) => Err(ThreadFileDeliveryError::Denied),
        }
    }
}

pub(crate) async fn prepare_thread_file(
    workspace_root: String,
    href: String,
) -> Result<PreparedThreadFile, ThreadFileDeliveryError> {
    tokio::task::spawn_blocking(move || prepare_thread_file_blocking(&workspace_root, &href))
        .await
        .map_err(|_| ThreadFileDeliveryError::Unavailable)?
}

fn prepare_thread_file_blocking(
    workspace_root: &str,
    href: &str,
) -> Result<PreparedThreadFile, ThreadFileDeliveryError> {
    let target = parse_local_file_reference(href)?;
    let canonical_root = std::fs::canonicalize(workspace_root).map_err(map_file_io_error)?;
    if !canonical_root.is_dir() {
        return Err(ThreadFileDeliveryError::NotFound);
    }
    let canonical_path = std::fs::canonicalize(&target.path).map_err(map_file_io_error)?;
    if canonical_path == canonical_root || !canonical_path.starts_with(&canonical_root) {
        return Err(ThreadFileDeliveryError::OutsideWorkspace);
    }
    let bytes = read_text_file(&canonical_root, &canonical_path)?;
    let file_name = canonical_path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty() && value.len() <= 255)
        .filter(|value| !value.chars().any(char::is_control))
        .ok_or(ThreadFileDeliveryError::InvalidReference)?
        .to_owned();
    Ok(PreparedThreadFile {
        content_type: text_content_type(&canonical_path),
        canonical_root,
        canonical_path,
        file_name,
        size_bytes: bytes.len() as u64,
        line: target.line,
        column: target.column,
    })
}

fn read_text_file(root: &Path, path: &Path) -> Result<Vec<u8>, ThreadFileDeliveryError> {
    if path == root || !path.starts_with(root) {
        return Err(ThreadFileDeliveryError::OutsideWorkspace);
    }
    let mut file = open_regular_file(path).map_err(map_file_io_error)?;
    let metadata = file.metadata().map_err(map_file_io_error)?;
    if metadata.len() > THREAD_FILE_VIEW_MAX_BYTES {
        return Err(ThreadFileDeliveryError::TooLarge);
    }
    let mut bytes = Vec::with_capacity(metadata.len().min(THREAD_FILE_VIEW_MAX_BYTES) as usize);
    file.by_ref()
        .take(THREAD_FILE_VIEW_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(map_file_io_error)?;
    if bytes.len() as u64 > THREAD_FILE_VIEW_MAX_BYTES {
        return Err(ThreadFileDeliveryError::TooLarge);
    }
    if std::str::from_utf8(&bytes).is_err() || bytes.contains(&0) {
        return Err(ThreadFileDeliveryError::NotText);
    }
    Ok(bytes)
}

#[derive(Debug, PartialEq, Eq)]
struct LocalFileReference {
    path: PathBuf,
    line: Option<u32>,
    column: Option<u32>,
}

fn parse_local_file_reference(raw: &str) -> Result<LocalFileReference, ThreadFileDeliveryError> {
    if raw.is_empty()
        || raw.len() > 4096
        || raw.trim() != raw
        || raw.chars().any(|character| character.is_control())
    {
        return Err(ThreadFileDeliveryError::InvalidReference);
    }
    if raw.starts_with("file:") {
        return parse_file_url(raw);
    }
    if raw.contains("\0") {
        return Err(ThreadFileDeliveryError::InvalidReference);
    }
    let (path, line, column) = split_path_position(raw);
    let path = decode_absolute_path(path).ok_or(ThreadFileDeliveryError::InvalidReference)?;
    Ok(LocalFileReference { path, line, column })
}

fn parse_file_url(raw: &str) -> Result<LocalFileReference, ThreadFileDeliveryError> {
    let url = Url::parse(raw).map_err(|_| ThreadFileDeliveryError::InvalidReference)?;
    if url.scheme() != "file" || (!url.username().is_empty()) || url.password().is_some() {
        return Err(ThreadFileDeliveryError::InvalidReference);
    }
    let fragment_position = url.fragment().and_then(parse_line_fragment);
    let path = url
        .to_file_path()
        .map_err(|_| ThreadFileDeliveryError::InvalidReference)?;
    let path_text = path.to_string_lossy();
    let (path_text, suffix_line, suffix_column) = split_path_position(path_text.as_ref());
    let (line, column) = fragment_position.unwrap_or((suffix_line, suffix_column));
    Ok(LocalFileReference {
        path: PathBuf::from(path_text),
        line,
        column,
    })
}

fn decode_absolute_path(raw: &str) -> Option<PathBuf> {
    let raw_path = Path::new(raw);
    if raw_path.is_absolute() && !raw.contains('%') {
        return Some(raw_path.to_path_buf());
    }

    #[cfg(windows)]
    if is_windows_absolute_path(raw) {
        if !raw.contains('%') {
            return Some(PathBuf::from(raw));
        }
        let normalized = raw.replace('\\', "/");
        return Url::parse(format!("file:///{normalized}").as_str())
            .ok()?
            .to_file_path()
            .ok();
    }

    Url::parse(format!("file://{raw}").as_str())
        .ok()?
        .to_file_path()
        .ok()
        .filter(|path| path.is_absolute())
}

#[cfg(windows)]
fn is_windows_absolute_path(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\'))
        || raw.starts_with("\\\\")
}

fn split_path_position(raw: &str) -> (&str, Option<u32>, Option<u32>) {
    let Some((before_last, last)) = raw.rsplit_once(':') else {
        return (raw, None, None);
    };
    let Ok(last_number) = last.parse::<u32>() else {
        return (raw, None, None);
    };
    if last_number == 0 {
        return (raw, None, None);
    }
    if let Some((path, possible_line)) = before_last.rsplit_once(':')
        && let Ok(line) = possible_line.parse::<u32>()
        && line > 0
    {
        return (path, Some(line), Some(last_number));
    }
    (before_last, Some(last_number), None)
}

fn parse_line_fragment(fragment: &str) -> Option<(Option<u32>, Option<u32>)> {
    let fragment = fragment
        .strip_prefix('L')
        .or_else(|| fragment.strip_prefix('l'))?;
    let (line, column) = fragment
        .split_once('C')
        .or_else(|| fragment.split_once('c'))
        .map_or((fragment, None), |(line, column)| (line, Some(column)));
    let line = line.parse::<u32>().ok().filter(|line| *line > 0)?;
    let column = column
        .and_then(|column| column.parse::<u32>().ok())
        .filter(|column| *column > 0);
    Some((Some(line), column))
}

fn text_content_type(path: &Path) -> String {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let mime = match extension.as_str() {
        "md" | "markdown" | "mdx" => "text/markdown",
        "json" | "jsonc" | "map" => "application/json",
        "js" | "jsx" | "mjs" | "cjs" => "text/javascript",
        "ts" | "tsx" | "mts" | "cts" => "text/typescript",
        "html" | "htm" => "text/html",
        "css" | "scss" | "sass" | "less" => "text/css",
        "xml" | "svg" => "application/xml",
        "yaml" | "yml" => "application/yaml",
        "toml" => "application/toml",
        "csv" => "text/csv",
        _ => "text/plain",
    };
    format!("{mime}; charset=utf-8")
}

fn map_file_io_error(error: std::io::Error) -> ThreadFileDeliveryError {
    match error.kind() {
        std::io::ErrorKind::NotFound
        | std::io::ErrorKind::PermissionDenied
        | std::io::ErrorKind::InvalidInput => ThreadFileDeliveryError::NotFound,
        _ => ThreadFileDeliveryError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_absolute_paths_file_urls_and_positions() {
        #[cfg(unix)]
        {
            assert_eq!(
                parse_local_file_reference("/tmp/example.rs:42:7").unwrap(),
                LocalFileReference {
                    path: PathBuf::from("/tmp/example.rs"),
                    line: Some(42),
                    column: Some(7),
                }
            );
            assert_eq!(
                parse_local_file_reference("file:///tmp/example%20file.rs#L9C3").unwrap(),
                LocalFileReference {
                    path: PathBuf::from("/tmp/example file.rs"),
                    line: Some(9),
                    column: Some(3),
                }
            );
        }
        assert!(parse_local_file_reference("https://example.com/main.rs").is_err());
        assert!(parse_local_file_reference("relative/main.rs").is_err());
    }

    #[test]
    fn reads_only_bounded_utf8_files_inside_the_workspace() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("main.rs");
        std::fs::write(&source, "fn main() {}\n").unwrap();
        assert_eq!(
            read_text_file(root.path(), &source).unwrap(),
            b"fn main() {}\n"
        );

        let outside = tempfile::NamedTempFile::new().unwrap();
        assert!(matches!(
            read_text_file(root.path(), outside.path()),
            Err(ThreadFileDeliveryError::OutsideWorkspace)
        ));
        let binary = root.path().join("binary.dat");
        std::fs::write(&binary, [0, 1, 2]).unwrap();
        assert!(matches!(
            read_text_file(root.path(), &binary),
            Err(ThreadFileDeliveryError::NotText)
        ));

        let oversized = root.path().join("oversized.txt");
        let oversized_file = std::fs::File::create(&oversized).unwrap();
        oversized_file
            .set_len(THREAD_FILE_VIEW_MAX_BYTES + 1)
            .unwrap();
        assert!(matches!(
            read_text_file(root.path(), &oversized),
            Err(ThreadFileDeliveryError::TooLarge)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks_that_escape_the_workspace() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let link = root.path().join("outside.txt");
        symlink(outside.path(), &link).unwrap();

        assert!(matches!(
            prepare_thread_file_blocking(root.path().to_str().unwrap(), link.to_str().unwrap()),
            Err(ThreadFileDeliveryError::OutsideWorkspace)
        ));
    }

    #[test]
    fn content_types_are_textual_and_utf8_explicit() {
        assert_eq!(
            text_content_type(Path::new("AGENTS.md")),
            "text/markdown; charset=utf-8"
        );
        assert_eq!(
            text_content_type(Path::new("main.rs")),
            "text/plain; charset=utf-8"
        );
    }
}
