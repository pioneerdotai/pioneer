use crate::apply_patch::file_mutation::{
    CanonicalTarget, FileVersionToken, SnapshotError, SnapshotErrorCode, SnapshotLimits,
    SnapshotLineEndings, TargetExpectation, TargetResolver, TargetRole, TextSnapshot,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{BufRead, BufReader};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadRequest {
    pub start_line: u64,
    /// Optional raw UTF-8 byte offset.  A BOM, when present, occupies its
    /// normal first three raw bytes and is omitted only from model-visible
    /// text.  When set, the reader starts at that exact byte and returns the
    /// containing logical line number in the page metadata.
    #[serde(default)]
    pub start_byte: Option<u64>,
    pub max_lines: u32,
    pub max_bytes: u64,
}

impl Default for ReadRequest {
    fn default() -> Self {
        Self {
            start_line: 0,
            start_byte: None,
            max_lines: 2000,
            max_bytes: 256 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ReadCursorBody {
    identity: String,
    token: FileVersionToken,
    next_line: u64,
    #[serde(default)]
    next_byte: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadCursor {
    body: ReadCursorBody,
}

impl ReadCursor {
    pub fn encode(&self) -> Result<String, ReadError> {
        let json = serde_json::to_vec(&self.body)
            .map_err(|_| ReadError::new(ReadErrorCode::CursorInvalid))?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json))
    }

    pub fn decode(value: &str) -> Result<Self, ReadError> {
        if value.len() > 16 * 1024 {
            return Err(ReadError::new(ReadErrorCode::CursorInvalid));
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| ReadError::new(ReadErrorCode::CursorInvalid))?;
        let body = serde_json::from_slice(&bytes)
            .map_err(|_| ReadError::new(ReadErrorCode::CursorInvalid))?;
        Ok(Self { body })
    }

    fn new(
        target: &CanonicalTarget,
        token: FileVersionToken,
        next_line: u64,
        next_byte: Option<u64>,
    ) -> Self {
        Self {
            body: ReadCursorBody {
                identity: target.identity().to_owned(),
                token,
                next_line,
                next_byte,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadPage {
    pub path: String,
    pub content: String,
    pub start_line: u64,
    pub next_line: Option<u64>,
    pub truncated: bool,
    pub cursor: Option<String>,
    pub token: FileVersionToken,
    pub line_endings: SnapshotLineEndings,
    /// Byte offsets in the returned UTF-8 text (the optional UTF-8 BOM is not
    /// part of the model-visible text range, while the whole-file token still
    /// covers the original raw bytes).
    pub start_byte: u64,
    pub end_byte: u64,
}

pub trait ReadAccess: Send + Sync {
    fn authorize(&self, target: &CanonicalTarget) -> Result<(), ReadError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllReadAccess;

impl ReadAccess for AllowAllReadAccess {
    fn authorize(&self, _target: &CanonicalTarget) -> Result<(), ReadError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PaginatedReader<A = AllowAllReadAccess> {
    limits: SnapshotLimits,
    access: A,
}

impl Default for PaginatedReader<AllowAllReadAccess> {
    fn default() -> Self {
        Self::new(SnapshotLimits::default(), AllowAllReadAccess)
    }
}

impl<A: ReadAccess> PaginatedReader<A> {
    pub fn new(limits: SnapshotLimits, access: A) -> Self {
        Self { limits, access }
    }

    pub fn read_path(
        &self,
        resolver: &TargetResolver,
        path: &str,
        request: ReadRequest,
        cursor: Option<&str>,
    ) -> Result<ReadPage, ReadError> {
        let target = resolver
            .resolve(path, TargetRole::Source, TargetExpectation::ExistingRegular)
            .map_err(|_| ReadError::new(ReadErrorCode::PathDenied))?;
        self.read_target(&target, request, cursor)
    }

    pub fn read_target(
        &self,
        target: &CanonicalTarget,
        request: ReadRequest,
        cursor: Option<&str>,
    ) -> Result<ReadPage, ReadError> {
        self.access.authorize(target)?;
        if request.max_lines == 0 {
            return Err(ReadError::new(ReadErrorCode::InvalidRequest));
        }
        if request.max_bytes == 0 {
            return Err(ReadError::new(ReadErrorCode::InvalidRequest));
        }
        let snapshot =
            TextSnapshot::from_file(target.absolute(), self.limits).map_err(ReadError::snapshot)?;
        let token = snapshot.version.token;
        if request.start_byte.is_some() && request.start_line != 0 {
            return Err(ReadError::new(ReadErrorCode::InvalidRequest));
        }
        let (start_line, start_byte) = if let Some(encoded) = cursor {
            let cursor = ReadCursor::decode(encoded)?;
            if cursor.body.identity != target.identity() {
                return Err(ReadError::new(ReadErrorCode::CursorPathMismatch));
            }
            if cursor.body.token != token {
                return Err(ReadError::new(ReadErrorCode::StaleCursor));
            }
            if request.start_line != 0 && request.start_line != cursor.body.next_line {
                return Err(ReadError::new(ReadErrorCode::CursorOffsetMismatch));
            }
            if request.start_byte.is_some() && request.start_byte != cursor.body.next_byte {
                return Err(ReadError::new(ReadErrorCode::CursorOffsetMismatch));
            }
            (cursor.body.next_line, cursor.body.next_byte)
        } else {
            (request.start_line, request.start_byte)
        };
        let file = crate::apply_patch::file_mutation::open_regular_file(target.absolute())
            .map_err(|error| ReadError {
                code: ReadErrorCode::Io,
                source: Some(error),
            })?;
        let selection = match start_byte {
            Some(start_byte) => read_page_streaming_from_byte(
                BufReader::new(file),
                start_byte,
                request.max_lines as u64,
                request.max_bytes,
            )?,
            None => read_page_streaming(
                BufReader::new(file),
                start_line,
                request.max_lines as u64,
                request.max_bytes,
                snapshot.encoding == crate::apply_patch::file_mutation::SnapshotEncoding::Utf8Bom,
            )?,
        };
        // The token must describe the bytes that produced this page.  A
        // concurrent writer between the metadata pass and the streaming page
        // pass is rejected rather than returning a page paired with a stale
        // whole-file token.
        let after_token = crate::apply_patch::file_mutation::version_on_disk(target, self.limits)
            .map_err(|_| ReadError::new(ReadErrorCode::Io))?;
        if after_token != Some(token) {
            return Err(ReadError::new(ReadErrorCode::StaleCursor));
        }
        let cursor = selection
            .next_line
            .map(|next| ReadCursor::new(target, token, next, selection.next_byte).encode())
            .transpose()?;
        Ok(ReadPage {
            path: target.relative().to_string_lossy().into_owned(),
            content: selection.content,
            start_line: selection.start_line,
            next_line: selection.next_line,
            truncated: selection.truncated,
            cursor,
            token,
            line_endings: snapshot.line_endings,
            start_byte: selection.start_byte,
            end_byte: selection.end_byte,
        })
    }
}

struct PageSelection {
    content: String,
    start_line: u64,
    next_line: Option<u64>,
    next_byte: Option<u64>,
    truncated: bool,
    start_byte: u64,
    end_byte: u64,
}

/// Read only the requested page.  The first snapshot pass computes the
/// complete-file token and metadata; this second pass never materializes the
/// rest of a near-limit file in memory.
fn read_page_streaming<R: BufRead>(
    mut reader: R,
    start_line: u64,
    max_lines: u64,
    max_bytes: u64,
    has_bom: bool,
) -> Result<PageSelection, ReadError> {
    let selected_end = start_line.saturating_add(max_lines);
    let mut line_index = 0u64;
    let mut text_offset = 0u64;
    let mut page_bytes = 0u64;
    let mut start_byte = None;
    let mut end_byte = 0u64;
    let mut selected = Vec::new();
    let mut line = Vec::new();
    let mut line_len = 0u64;
    let mut pending_cr = false;
    if has_bom {
        consume_prefix(&mut reader, 3)?;
        // Byte ranges are raw-file offsets, so model-visible text starts
        // after the three-byte UTF-8 BOM.
        text_offset = 3;
    }

    loop {
        let buffer = reader.fill_buf().map_err(|error| ReadError {
            code: ReadErrorCode::Io,
            source: Some(error),
        })?;
        if buffer.is_empty() {
            if pending_cr {
                if finish_stream_line(
                    &mut line_index,
                    &mut text_offset,
                    &mut page_bytes,
                    &mut start_byte,
                    &mut end_byte,
                    &mut selected,
                    &mut line,
                    &mut line_len,
                    start_line,
                    selected_end,
                    max_bytes,
                )? {
                    return page_selection(
                        selected,
                        start_line,
                        start_byte,
                        end_byte,
                        Some(line_index),
                    );
                }
            } else if line_len > 0 {
                if finish_stream_line(
                    &mut line_index,
                    &mut text_offset,
                    &mut page_bytes,
                    &mut start_byte,
                    &mut end_byte,
                    &mut selected,
                    &mut line,
                    &mut line_len,
                    start_line,
                    selected_end,
                    max_bytes,
                )? {
                    return page_selection(
                        selected,
                        start_line,
                        start_byte,
                        end_byte,
                        Some(line_index),
                    );
                }
            }
            break;
        }

        let mut consumed = 0usize;
        let mut stop = false;
        for &byte in buffer {
            // Do not consume the first byte of the line after the requested
            // page.  `fill_buf` may expose several lines at once; advancing
            // the reader before noticing `selected_end` would make the
            // continuation cursor silently skip that byte.
            if line_index >= selected_end {
                stop = true;
                break;
            }
            consumed += 1;
            if pending_cr {
                pending_cr = false;
                if byte == b'\n' {
                    if line_index >= start_line && line_index < selected_end {
                        line.push(byte);
                    }
                    line_len = line_len.saturating_add(1);
                    if finish_stream_line(
                        &mut line_index,
                        &mut text_offset,
                        &mut page_bytes,
                        &mut start_byte,
                        &mut end_byte,
                        &mut selected,
                        &mut line,
                        &mut line_len,
                        start_line,
                        selected_end,
                        max_bytes,
                    )? {
                        return page_selection(
                            selected,
                            start_line,
                            start_byte,
                            end_byte,
                            Some(line_index),
                        );
                    }
                    continue;
                }
                if finish_stream_line(
                    &mut line_index,
                    &mut text_offset,
                    &mut page_bytes,
                    &mut start_byte,
                    &mut end_byte,
                    &mut selected,
                    &mut line,
                    &mut line_len,
                    start_line,
                    selected_end,
                    max_bytes,
                )? {
                    return page_selection(
                        selected,
                        start_line,
                        start_byte,
                        end_byte,
                        Some(line_index),
                    );
                }
            }
            if line_index >= start_line && line_index < selected_end {
                line.push(byte);
            }
            line_len = line_len.saturating_add(1);
            if line_index >= start_line
                && line_index < selected_end
                && line_len > max_bytes.saturating_sub(page_bytes)
            {
                if line_index == start_line {
                    return Err(ReadError::new(ReadErrorCode::TooLarge));
                }
                return page_selection(
                    selected,
                    start_line,
                    start_byte,
                    end_byte,
                    Some(line_index),
                );
            }
            if byte == b'\r' {
                pending_cr = true;
            } else if byte == b'\n' {
                if finish_stream_line(
                    &mut line_index,
                    &mut text_offset,
                    &mut page_bytes,
                    &mut start_byte,
                    &mut end_byte,
                    &mut selected,
                    &mut line,
                    &mut line_len,
                    start_line,
                    selected_end,
                    max_bytes,
                )? {
                    return page_selection(
                        selected,
                        start_line,
                        start_byte,
                        end_byte,
                        Some(line_index),
                    );
                }
            }
        }
        reader.consume(consumed);
        if stop {
            return page_selection(
                selected,
                start_line,
                start_byte,
                end_byte,
                Some(selected_end),
            );
        }
    }

    if line_index < start_line {
        return Err(ReadError::new(ReadErrorCode::OffsetOutOfRange));
    }
    let truncated = line_index > selected_end;
    let next_line = truncated.then_some(selected_end);
    let end_byte = if start_byte.is_none() {
        // A BOM-only file has no model-visible line, but its returned byte
        // range still starts after the three-byte marker and must not invert
        // into `start_byte > end_byte`.
        end_byte.max(text_offset)
    } else {
        end_byte
    };
    Ok(PageSelection {
        content: String::from_utf8(selected)
            .map_err(|_| ReadError::new(ReadErrorCode::InvalidUtf8))?,
        start_line,
        next_line,
        next_byte: None,
        truncated,
        start_byte: start_byte.unwrap_or(text_offset),
        end_byte,
    })
}

fn consume_prefix<R: BufRead>(reader: &mut R, length: usize) -> Result<(), ReadError> {
    let mut remaining = length;
    while remaining > 0 {
        let available = reader.fill_buf().map_err(|error| ReadError {
            code: ReadErrorCode::Io,
            source: Some(error),
        })?;
        if available.is_empty() {
            return Err(ReadError::new(ReadErrorCode::Io));
        }
        let consumed = remaining.min(available.len());
        reader.consume(consumed);
        remaining -= consumed;
    }
    Ok(())
}

/// Byte-offset pagination is deliberately implemented as a second bounded
/// streaming path.  It keeps at most one logical line plus the requested page
/// in memory, supports LF/CRLF/lone-CR files, and never turns a page request
/// into a full-file allocation.
fn read_page_streaming_from_byte<R: BufRead>(
    mut reader: R,
    requested_start_byte: u64,
    max_lines: u64,
    max_bytes: u64,
) -> Result<PageSelection, ReadError> {
    let mut line_index = 0u64;
    let mut line_start_byte = 0u64;
    let mut selected_start_line = None;
    let mut selected = Vec::new();
    let mut selected_lines = 0u64;
    let mut page_bytes = 0u64;
    let mut first_selected_byte = None;
    let mut end_byte = requested_start_byte;
    let mut first_line = true;
    let mut effective_start_byte = requested_start_byte;

    loop {
        let Some(mut line) = read_logical_line(&mut reader)? else {
            if first_line && requested_start_byte == 0 {
                // An empty file has no logical line, and a BOM-only file is
                // represented as an empty model-visible range after its BOM.
                return Ok(PageSelection {
                    content: String::new(),
                    start_line: line_index,
                    next_line: None,
                    next_byte: None,
                    truncated: false,
                    start_byte: line_start_byte,
                    end_byte: line_start_byte,
                });
            }
            if effective_start_byte > line_start_byte {
                return Err(ReadError::new(ReadErrorCode::OffsetOutOfRange));
            }
            let selected_empty = selected.is_empty();
            return Ok(PageSelection {
                content: String::from_utf8(selected)
                    .map_err(|_| ReadError::new(ReadErrorCode::InvalidUtf8))?,
                start_line: selected_start_line.unwrap_or(line_index),
                next_line: None,
                next_byte: None,
                truncated: false,
                start_byte: first_selected_byte.unwrap_or(effective_start_byte),
                end_byte: if selected_empty {
                    effective_start_byte
                } else {
                    end_byte
                },
            });
        };

        if first_line {
            first_line = false;
            if line.starts_with(&[0xef, 0xbb, 0xbf]) {
                line.drain(..3);
                line_start_byte = 3;
                if requested_start_byte == 0 {
                    effective_start_byte = 3;
                } else if requested_start_byte < 3 {
                    return Err(ReadError::new(ReadErrorCode::InvalidRequest));
                }
            }
        }

        let line_end_byte = line_start_byte.saturating_add(line.len() as u64);
        if effective_start_byte >= line_end_byte {
            if effective_start_byte == line_end_byte && line.is_empty() {
                // A BOM-only file reaches this branch; it has no model line.
                return Ok(PageSelection {
                    content: String::new(),
                    start_line: line_index,
                    next_line: None,
                    next_byte: None,
                    truncated: false,
                    start_byte: line_end_byte,
                    end_byte: line_end_byte,
                });
            }
            line_start_byte = line_end_byte;
            line_index = line_index.saturating_add(1);
            continue;
        }

        if selected_lines >= max_lines {
            return page_selection_from_byte(
                selected,
                selected_start_line.unwrap_or(line_index),
                first_selected_byte,
                end_byte,
                Some(line_index),
                Some(line_start_byte),
            );
        }

        let skip = usize::try_from(effective_start_byte.saturating_sub(line_start_byte))
            .map_err(|_| ReadError::new(ReadErrorCode::OffsetOutOfRange))?;
        let visible = line
            .get(skip..)
            .ok_or_else(|| ReadError::new(ReadErrorCode::InvalidRequest))?;
        if std::str::from_utf8(visible).is_err() {
            return Err(ReadError::new(ReadErrorCode::InvalidRequest));
        }
        if selected_start_line.is_none() {
            selected_start_line = Some(line_index);
            first_selected_byte = Some(effective_start_byte.max(line_start_byte));
        }
        let visible_len = visible.len() as u64;
        if visible_len > max_bytes.saturating_sub(page_bytes) {
            if selected.is_empty() {
                return Err(ReadError::new(ReadErrorCode::TooLarge));
            }
            return page_selection_from_byte(
                selected,
                selected_start_line.unwrap_or(line_index),
                first_selected_byte,
                end_byte,
                Some(line_index),
                Some(line_start_byte),
            );
        }
        selected.extend_from_slice(visible);
        page_bytes = page_bytes.saturating_add(visible_len);
        end_byte = line_end_byte;
        selected_lines = selected_lines.saturating_add(1);
        line_start_byte = line_end_byte;
        line_index = line_index.saturating_add(1);
        // Only the first line may be started in the middle by an explicit
        // byte offset.  Subsequent continuation pages begin at line starts.
        effective_start_byte = line_start_byte;
    }
}

fn read_logical_line<R: BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>, ReadError> {
    let mut line = Vec::new();
    loop {
        let buffer = reader.fill_buf().map_err(|error| ReadError {
            code: ReadErrorCode::Io,
            source: Some(error),
        })?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(line))
            };
        }
        let boundary = buffer
            .iter()
            .position(|byte| *byte == b'\r' || *byte == b'\n');
        let Some(index) = boundary else {
            line.extend_from_slice(buffer);
            let length = buffer.len();
            reader.consume(length);
            continue;
        };
        let terminator = buffer[index];
        line.extend_from_slice(&buffer[..=index]);
        reader.consume(index + 1);
        if terminator == b'\r' {
            let next = reader.fill_buf().map_err(|error| ReadError {
                code: ReadErrorCode::Io,
                source: Some(error),
            })?;
            if next.first() == Some(&b'\n') {
                line.push(b'\n');
                reader.consume(1);
            }
        }
        return Ok(Some(line));
    }
}

fn page_selection_from_byte(
    selected: Vec<u8>,
    start_line: u64,
    start_byte: Option<u64>,
    end_byte: u64,
    next_line: Option<u64>,
    next_byte: Option<u64>,
) -> Result<PageSelection, ReadError> {
    Ok(PageSelection {
        content: String::from_utf8(selected)
            .map_err(|_| ReadError::new(ReadErrorCode::InvalidUtf8))?,
        start_line,
        next_line,
        next_byte,
        truncated: next_line.is_some(),
        start_byte: start_byte.unwrap_or(end_byte),
        end_byte,
    })
}

#[allow(clippy::too_many_arguments)]
fn finish_stream_line(
    line_index: &mut u64,
    text_offset: &mut u64,
    page_bytes: &mut u64,
    start_byte: &mut Option<u64>,
    end_byte: &mut u64,
    selected: &mut Vec<u8>,
    line: &mut Vec<u8>,
    line_len: &mut u64,
    start_line: u64,
    selected_end: u64,
    max_bytes: u64,
) -> Result<bool, ReadError> {
    if *line_index >= start_line && *line_index < selected_end {
        if *line_len > max_bytes.saturating_sub(*page_bytes) {
            if *line_index == start_line {
                return Err(ReadError::new(ReadErrorCode::TooLarge));
            }
            return Ok(true);
        }
        if start_byte.is_none() {
            *start_byte = Some(*text_offset);
        }
        *page_bytes = page_bytes.saturating_add(*line_len);
        *end_byte = text_offset.saturating_add(*line_len);
        selected.extend_from_slice(line);
    }
    *text_offset = text_offset.saturating_add(*line_len);
    *line_index = line_index.saturating_add(1);
    line.clear();
    *line_len = 0;
    Ok(false)
}

fn page_selection(
    selected: Vec<u8>,
    start_line: u64,
    start_byte: Option<u64>,
    end_byte: u64,
    next_line: Option<u64>,
) -> Result<PageSelection, ReadError> {
    Ok(PageSelection {
        content: String::from_utf8(selected)
            .map_err(|_| ReadError::new(ReadErrorCode::InvalidUtf8))?,
        start_line,
        next_line,
        next_byte: None,
        truncated: next_line.is_some(),
        start_byte: start_byte.unwrap_or(end_byte),
        end_byte,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadErrorCode {
    PathDenied,
    InvalidRequest,
    CursorInvalid,
    CursorPathMismatch,
    CursorOffsetMismatch,
    StaleCursor,
    OffsetOutOfRange,
    BinaryContent,
    InvalidUtf8,
    TooLarge,
    Io,
    AccessDenied,
}

#[derive(Debug)]
pub struct ReadError {
    pub code: ReadErrorCode,
    pub source: Option<std::io::Error>,
}

impl ReadError {
    pub const fn new(code: ReadErrorCode) -> Self {
        Self { code, source: None }
    }

    fn snapshot(error: SnapshotError) -> Self {
        Self {
            code: match error.code {
                SnapshotErrorCode::BinaryContent => ReadErrorCode::BinaryContent,
                SnapshotErrorCode::InvalidUtf8 => ReadErrorCode::InvalidUtf8,
                SnapshotErrorCode::TooLarge => ReadErrorCode::TooLarge,
                SnapshotErrorCode::InvalidLimits
                | SnapshotErrorCode::Io
                | SnapshotErrorCode::SpoolUnavailable
                | SnapshotErrorCode::SpoolCorrupt => ReadErrorCode::Io,
            },
            source: error.source,
        }
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "read failed: {:?}", self.code)
    }
}

impl std::error::Error for ReadError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::TargetResolver;
    use std::fs;

    #[test]
    fn pages_share_token_and_cursor_continuation_is_bounded() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), b"one\ntwo\nthree\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let reader = PaginatedReader::new(
            SnapshotLimits {
                max_file_bytes: 1024,
                inline_threshold: 1024,
            },
            AllowAllReadAccess,
        );
        let first = reader
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest {
                    start_line: 0,
                    start_byte: None,
                    max_lines: 1,
                    max_bytes: 1024,
                },
                None,
            )
            .unwrap();
        assert_eq!(first.content, "one\n");
        let second = reader
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest {
                    start_line: 0,
                    start_byte: None,
                    max_lines: 1,
                    max_bytes: 1024,
                },
                first.cursor.as_deref(),
            )
            .unwrap();
        assert_eq!(second.content, "two\n");
        assert_eq!(first.token, second.token);
    }

    #[test]
    fn changed_file_rejects_old_cursor() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        fs::write(&path, b"one\ntwo\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let reader = PaginatedReader::default();
        let first = reader
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest {
                    start_line: 0,
                    start_byte: None,
                    max_lines: 1,
                    max_bytes: 1024,
                },
                None,
            )
            .unwrap();
        fs::write(&path, b"ONE\ntwo\n").unwrap();
        let error = reader
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest {
                    start_line: 0,
                    start_byte: None,
                    max_lines: 1,
                    max_bytes: 1024,
                },
                first.cursor.as_deref(),
            )
            .unwrap_err();
        assert_eq!(error.code, ReadErrorCode::StaleCursor);
    }

    #[test]
    fn cursor_tampering_and_page_offset_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), b"one\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let reader = PaginatedReader::default();
        let page = reader
            .read_path(&resolver, "file.txt", ReadRequest::default(), None)
            .unwrap();
        let bad = page.cursor.unwrap_or_else(|| "bad".into());
        assert_eq!(
            reader
                .read_path(&resolver, "file.txt", ReadRequest::default(), Some(&bad))
                .unwrap_err()
                .code,
            ReadErrorCode::CursorInvalid
        );
    }

    #[test]
    fn every_page_has_a_complete_file_token() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), b"a\nb\nc\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let reader = PaginatedReader::new(
            SnapshotLimits {
                max_file_bytes: 1024,
                inline_threshold: 1024,
            },
            AllowAllReadAccess,
        );
        let page = reader
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest {
                    start_line: 1,
                    start_byte: None,
                    max_lines: 1,
                    max_bytes: 1024,
                },
                None,
            )
            .unwrap();
        assert_eq!(page.token.byte_len(), 6);
        assert_eq!(page.start_line, 1);
    }

    #[test]
    fn byte_bound_stops_before_the_next_complete_line() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), b"1234\n5678\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let page = PaginatedReader::default()
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest {
                    start_line: 0,
                    start_byte: None,
                    max_lines: 10,
                    max_bytes: 5,
                },
                None,
            )
            .unwrap();
        assert_eq!(page.content, "1234\n");
        assert!(page.truncated);
    }

    #[test]
    fn crlf_ranges_and_byte_budget_count_each_raw_byte_once() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), b"one\r\ntwo\r\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let page = PaginatedReader::default()
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest {
                    start_line: 0,
                    start_byte: None,
                    max_lines: 1,
                    max_bytes: 5,
                },
                None,
            )
            .unwrap();
        assert_eq!(page.content, "one\r\n");
        assert_eq!(page.start_byte, 0);
        assert_eq!(page.end_byte, 5);
        assert!(page.truncated);
    }

    #[test]
    fn byte_offset_can_start_inside_utf8_line_and_cursor_continues_by_byte() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), "zero\nαβ\nend\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let reader = PaginatedReader::default();
        let first = reader
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest {
                    start_line: 0,
                    start_byte: Some(7),
                    max_lines: 1,
                    max_bytes: 1024,
                },
                None,
            )
            .unwrap();
        assert_eq!(first.content, "β\n");
        assert_eq!(first.start_line, 1);
        assert_eq!(first.start_byte, 7);
        assert_eq!(first.end_byte, 10);
        let second = reader
            .read_path(
                &resolver,
                "file.txt",
                ReadRequest::default(),
                first.cursor.as_deref(),
            )
            .unwrap();
        assert_eq!(second.content, "end\n");
        assert_eq!(second.start_byte, 10);
    }

    #[test]
    fn byte_offset_rejects_utf8_continuation_and_past_end() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), "α\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let reader = PaginatedReader::default();
        assert_eq!(
            reader
                .read_path(
                    &resolver,
                    "file.txt",
                    ReadRequest {
                        start_line: 0,
                        start_byte: Some(1),
                        max_lines: 1,
                        max_bytes: 1024,
                    },
                    None,
                )
                .unwrap_err()
                .code,
            ReadErrorCode::InvalidRequest
        );
        assert_eq!(
            reader
                .read_path(
                    &resolver,
                    "file.txt",
                    ReadRequest {
                        start_line: 0,
                        start_byte: Some(4),
                        max_lines: 1,
                        max_bytes: 1024,
                    },
                    None,
                )
                .unwrap_err()
                .code,
            ReadErrorCode::OffsetOutOfRange
        );
    }

    #[test]
    fn invalid_utf8_is_not_lossily_replaced() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), [0xff, b'\n']).unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let error = PaginatedReader::default()
            .read_path(&resolver, "file.txt", ReadRequest::default(), None)
            .unwrap_err();
        assert_eq!(error.code, ReadErrorCode::InvalidUtf8);
    }

    #[test]
    fn bom_only_file_reports_a_non_inverted_text_range() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file.txt"), [0xef, 0xbb, 0xbf]).unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let page = PaginatedReader::default()
            .read_path(&resolver, "file.txt", ReadRequest::default(), None)
            .unwrap();
        assert!(page.content.is_empty());
        assert_eq!(page.start_byte, 3);
        assert_eq!(page.end_byte, 3);
    }

    #[test]
    fn non_bom_utf8_prefix_is_not_lost_by_bom_detection() {
        let root = tempfile::tempdir().unwrap();
        let content = "\u{fec0}\nrest\n";
        fs::write(root.path().join("file.txt"), content.as_bytes()).unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let page = PaginatedReader::default()
            .read_path(&resolver, "file.txt", ReadRequest::default(), None)
            .unwrap();
        assert_eq!(page.content, content);
        assert_eq!(page.start_byte, 0);
    }
}
