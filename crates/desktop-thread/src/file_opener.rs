use std::path::{Path, PathBuf};
use url::Url;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalFileTarget {
    path: PathBuf,
    line: Option<u32>,
    column: Option<u32>,
}

impl LocalFileTarget {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn line(&self) -> Option<u32> {
        self.line
    }
    pub(crate) fn column(&self) -> Option<u32> {
        self.column
    }
}
pub(crate) fn local_file_target(raw: &str) -> Option<LocalFileTarget> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    if raw.to_ascii_lowercase().starts_with("file:") {
        return local_file_url_target(raw);
    }
    if raw.contains("://") {
        return None;
    }

    let (path, line, column) = split_path_position(raw);
    let path = decode_absolute_path(path)?;
    Some(LocalFileTarget { path, line, column })
}

fn local_file_url_target(raw: &str) -> Option<LocalFileTarget> {
    let url = Url::parse(raw).ok()?;
    if url.scheme() != "file" {
        return None;
    }
    let fragment_position = url.fragment().and_then(parse_line_fragment);
    let path = url.to_file_path().ok()?;
    let path_text = path.to_string_lossy();
    let (path_text, suffix_line, suffix_column) = split_path_position(path_text.as_ref());
    let (line, column) = fragment_position.unwrap_or((suffix_line, suffix_column));
    Some(LocalFileTarget {
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

    let url = Url::parse(format!("file://{raw}").as_str()).ok()?;
    url.to_file_path().ok().filter(|path| path.is_absolute())
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
    if let Some((path, possible_line)) = before_last.rsplit_once(':')
        && let Ok(line) = possible_line.parse::<u32>()
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
    let line = line.parse::<u32>().ok()?;
    let column = column.and_then(|column| column.parse::<u32>().ok());
    Some((Some(line), column))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn local_file_links_support_positions_and_file_urls() {
        assert_eq!(
            local_file_target("/tmp/example.rs:42:7"),
            Some(LocalFileTarget {
                path: PathBuf::from("/tmp/example.rs"),
                line: Some(42),
                column: Some(7),
            })
        );
        assert_eq!(
            local_file_target("file:///tmp/my%20file.rs#L9C3"),
            Some(LocalFileTarget {
                path: PathBuf::from("/tmp/my file.rs"),
                line: Some(9),
                column: Some(3),
            })
        );
        assert!(local_file_target("https://example.com/file.rs").is_none());
        assert!(local_file_target("relative/file.rs").is_none());
    }
}
