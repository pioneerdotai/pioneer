use crate::apply_patch::file_mutation::{PatchLimits, PatchRequest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

pub const PARSER_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatchDocument {
    pub schema_version: u16,
    pub input_bytes: u64,
    pub operations: Vec<Operation>,
    pub payload_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Add,
    Replace,
    Update,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    pub kind: OperationKind,
    pub path: String,
    pub source_guard: Option<GuardSyntax>,
    pub destination_guard: Option<GuardSyntax>,
    pub move_to: Option<String>,
    pub body: OperationBody,
    pub header_line: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "body")]
pub enum OperationBody {
    Add(AddFile),
    Replace(ReplaceFile),
    Update(UpdateFile),
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AddFile {
    pub lines: Vec<String>,
}

impl AddFile {
    pub fn content(&self) -> String {
        self.lines.join("\n")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplaceFile {
    pub lines: Vec<String>,
}

impl ReplaceFile {
    pub fn content(&self) -> String {
        self.lines.join("\n")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpdateFile {
    pub hunks: Vec<Hunk>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Hunk {
    pub context: Option<String>,
    pub lines: Vec<HunkLine>,
    pub end_of_file: bool,
    pub header_line: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "line")]
pub enum HunkLine {
    Context(String),
    Remove(String),
    Add(String),
}

impl Hunk {
    pub fn old_lines(&self) -> Vec<String> {
        self.lines
            .iter()
            .filter_map(|line| match line {
                HunkLine::Context(value) | HunkLine::Remove(value) => Some(value.clone()),
                HunkLine::Add(_) => None,
            })
            .collect()
    }

    pub fn new_lines(&self) -> Vec<String> {
        self.lines
            .iter()
            .filter_map(|line| match line {
                HunkLine::Context(value) | HunkLine::Add(value) => Some(value.clone()),
                HunkLine::Remove(_) => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "token")]
pub enum GuardSyntax {
    IfMatch(String),
    IfDestinationAbsent,
    IfDestinationVersion(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseErrorCode {
    EmptyInput,
    MissingBegin,
    MissingEnd,
    TrailingContent,
    UnknownDirective,
    MissingPath,
    InvalidPath,
    InvalidOperationBody,
    MissingHunk,
    InvalidHunkLine,
    EmptyAdd,
    EmptyReplace,
    TooManyOperations,
    TooManyChunks,
    TooManyHunks,
    PathTooLong,
    InputTooLarge,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParseError {
    pub code: ParseErrorCode,
    pub line: usize,
    pub column: usize,
    pub message: String,
}

impl ParseError {
    fn new(code: ParseErrorCode, line: usize, message: impl Into<String>) -> Self {
        Self {
            code,
            line,
            column: 1,
            message: message.into(),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "patch parse error at {}:{}: {}",
            self.line, self.column, self.message
        )
    }
}

impl std::error::Error for ParseError {}

pub fn parse(request: &PatchRequest, limits: PatchLimits) -> Result<PatchDocument, ParseError> {
    if request.patch.len() as u64 > limits.max_patch_bytes {
        return Err(ParseError::new(
            ParseErrorCode::InputTooLarge,
            1,
            "patch exceeds configured input limit",
        ));
    }
    let raw = request.patch.as_str();
    if raw.trim().is_empty() {
        return Err(ParseError::new(
            ParseErrorCode::EmptyInput,
            1,
            "patch is empty",
        ));
    }
    let lines = raw.split('\n').map(strip_cr).collect::<Vec<_>>();
    if lines.first().copied() != Some("*** Begin Patch") {
        return Err(ParseError::new(
            ParseErrorCode::MissingBegin,
            1,
            "first line must be *** Begin Patch",
        ));
    }
    let end = lines
        .iter()
        .position(|line| *line == "*** End Patch")
        .ok_or_else(|| {
            ParseError::new(
                ParseErrorCode::MissingEnd,
                lines.len(),
                "missing *** End Patch",
            )
        })?;
    if lines[end + 1..].iter().any(|line| !line.trim().is_empty()) {
        return Err(ParseError::new(
            ParseErrorCode::TrailingContent,
            end + 2,
            "non-whitespace content follows *** End Patch",
        ));
    }
    if end == 1 {
        return Err(ParseError::new(
            ParseErrorCode::EmptyInput,
            end,
            "patch contains no operations",
        ));
    }

    let mut operations = Vec::new();
    let mut total_hunks = 0usize;
    let mut index = 1usize;
    while index < end {
        if operations.len() >= limits.max_operations as usize {
            return Err(ParseError::new(
                ParseErrorCode::TooManyOperations,
                index + 1,
                "operation limit exceeded",
            ));
        }
        let header_line = index + 1;
        let line = lines[index];
        let (kind, path) = if let Some(path) = line.strip_prefix("*** Add File:") {
            (
                OperationKind::Add,
                parse_path(path, header_line, limits.max_path_bytes)?,
            )
        } else if let Some(path) = line.strip_prefix("*** Replace File:") {
            (
                OperationKind::Replace,
                parse_path(path, header_line, limits.max_path_bytes)?,
            )
        } else if let Some(path) = line.strip_prefix("*** Update File:") {
            (
                OperationKind::Update,
                parse_path(path, header_line, limits.max_path_bytes)?,
            )
        } else if let Some(path) = line.strip_prefix("*** Delete File:") {
            (
                OperationKind::Delete,
                parse_path(path, header_line, limits.max_path_bytes)?,
            )
        } else {
            return Err(ParseError::new(
                ParseErrorCode::UnknownDirective,
                header_line,
                "expected a file operation directive",
            ));
        };
        index += 1;
        let mut source_guard = None;
        let mut destination_guard = None;
        let mut move_to = None;
        while index < end {
            let directive = lines[index];
            if let Some(token) = directive.strip_prefix("*** If-Match:") {
                if source_guard.is_some() {
                    return Err(ParseError::new(
                        ParseErrorCode::InvalidOperationBody,
                        index + 1,
                        "duplicate *** If-Match directive",
                    ));
                }
                source_guard = Some(GuardSyntax::IfMatch(token.trim().to_owned()));
                index += 1;
            } else if let Some(value) = directive.strip_prefix("*** If-Destination:") {
                if destination_guard.is_some() {
                    return Err(ParseError::new(
                        ParseErrorCode::InvalidOperationBody,
                        index + 1,
                        "duplicate *** If-Destination directive",
                    ));
                }
                let value = value.trim();
                destination_guard = Some(if value == "absent" {
                    GuardSyntax::IfDestinationAbsent
                } else {
                    GuardSyntax::IfDestinationVersion(value.to_owned())
                });
                index += 1;
            } else if let Some(value) = directive.strip_prefix("*** Move to:") {
                if move_to.is_some() || kind != OperationKind::Update {
                    return Err(ParseError::new(
                        ParseErrorCode::InvalidOperationBody,
                        index + 1,
                        "*** Move to is valid only once on Update File",
                    ));
                }
                move_to = Some(parse_path(value, index + 1, limits.max_path_bytes)?);
                index += 1;
            } else {
                break;
            }
        }

        let (body, next_index) = match kind {
            OperationKind::Add => parse_add(&lines, index, end)?,
            OperationKind::Replace => parse_replace(&lines, index, end)?,
            OperationKind::Update => parse_update(&lines, index, end, limits, move_to.is_some())?,
            OperationKind::Delete => (OperationBody::Delete, index),
        };
        if let OperationBody::Update(update) = &body {
            total_hunks = total_hunks.saturating_add(update.hunks.len());
            if total_hunks > limits.max_total_hunks as usize {
                return Err(ParseError::new(
                    ParseErrorCode::TooManyHunks,
                    header_line,
                    "total hunk limit exceeded",
                ));
            }
        }
        if kind == OperationKind::Delete
            && next_index < end
            && !lines[next_index].starts_with("***")
        {
            return Err(ParseError::new(
                ParseErrorCode::InvalidOperationBody,
                next_index + 1,
                "Delete File cannot have a body",
            ));
        }
        operations.push(Operation {
            kind,
            path,
            source_guard,
            destination_guard,
            move_to,
            body,
            header_line,
        });
        index = next_index;
    }
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let mut payload_hash = [0; 32];
    payload_hash.copy_from_slice(&hasher.finalize());
    Ok(PatchDocument {
        schema_version: PARSER_SCHEMA_VERSION,
        input_bytes: raw.len() as u64,
        operations,
        payload_hash,
    })
}

fn parse_add(
    lines: &[&str],
    mut index: usize,
    end: usize,
) -> Result<(OperationBody, usize), ParseError> {
    let start = index;
    let mut values = Vec::new();
    while index < end && !lines[index].starts_with("*** ") {
        let line = lines[index];
        if !line.starts_with('+') {
            return Err(ParseError::new(
                ParseErrorCode::InvalidOperationBody,
                index + 1,
                "Add File lines must start with +",
            ));
        }
        values.push(line[1..].to_owned());
        index += 1;
    }
    if index == start {
        return Err(ParseError::new(
            ParseErrorCode::EmptyAdd,
            start + 1,
            "Add File has no content",
        ));
    }
    Ok((OperationBody::Add(AddFile { lines: values }), index))
}

fn parse_replace(
    lines: &[&str],
    mut index: usize,
    end: usize,
) -> Result<(OperationBody, usize), ParseError> {
    let start = index;
    let mut values = Vec::new();
    while index < end && !lines[index].starts_with("*** ") {
        let line = lines[index];
        if !line.starts_with('+') {
            return Err(ParseError::new(
                ParseErrorCode::InvalidOperationBody,
                index + 1,
                "Replace File lines must start with +",
            ));
        }
        values.push(line[1..].to_owned());
        index += 1;
    }
    if index == start {
        return Err(ParseError::new(
            ParseErrorCode::EmptyReplace,
            start + 1,
            "Replace File has no content",
        ));
    }
    Ok((OperationBody::Replace(ReplaceFile { lines: values }), index))
}

fn parse_update(
    lines: &[&str],
    mut index: usize,
    end: usize,
    limits: PatchLimits,
    allow_empty_for_move: bool,
) -> Result<(OperationBody, usize), ParseError> {
    let mut hunks = Vec::new();
    while index < end && !lines[index].starts_with("*** ") {
        if !lines[index].starts_with("@@") {
            return Err(ParseError::new(
                ParseErrorCode::MissingHunk,
                index + 1,
                "Update File expects an @@ hunk header",
            ));
        }
        if hunks.len() >= limits.max_chunks_per_update as usize {
            return Err(ParseError::new(
                ParseErrorCode::TooManyChunks,
                index + 1,
                "update hunk limit exceeded",
            ));
        }
        let header_line = index + 1;
        let context = lines[index][2..].trim();
        let context = (!context.is_empty()).then(|| context.to_owned());
        index += 1;
        let mut hunk_lines = Vec::new();
        let mut end_of_file = false;
        while index < end && !lines[index].starts_with("@@") && !lines[index].starts_with("*** ") {
            let line = lines[index];
            let value = line.get(1..).unwrap_or_default().to_owned();
            match line.as_bytes().first().copied() {
                Some(b' ') => hunk_lines.push(HunkLine::Context(value)),
                Some(b'-') => hunk_lines.push(HunkLine::Remove(value)),
                Some(b'+') => hunk_lines.push(HunkLine::Add(value)),
                _ => {
                    return Err(ParseError::new(
                        ParseErrorCode::InvalidHunkLine,
                        index + 1,
                        "hunk lines must start with space, - or +",
                    ));
                }
            }
            index += 1;
        }
        if index < end && lines[index] == "*** End of File" {
            end_of_file = true;
            index += 1;
        }
        if hunk_lines.is_empty() && !end_of_file {
            return Err(ParseError::new(
                ParseErrorCode::InvalidHunkLine,
                header_line,
                "empty hunk",
            ));
        }
        hunks.push(Hunk {
            context,
            lines: hunk_lines,
            end_of_file,
            header_line,
        });
    }
    if hunks.is_empty() && !allow_empty_for_move {
        return Err(ParseError::new(
            ParseErrorCode::MissingHunk,
            index + 1,
            "Update File needs an @@ hunk unless it is a pure Move to operation",
        ));
    }
    Ok((OperationBody::Update(UpdateFile { hunks }), index))
}

fn parse_path(value: &str, line: usize, max_path_bytes: u64) -> Result<String, ParseError> {
    let path = value.trim();
    if path.is_empty() || path.contains('\0') {
        return Err(ParseError::new(
            ParseErrorCode::InvalidPath,
            line,
            "path is empty or contains NUL",
        ));
    }
    if path.len() as u64 > max_path_bytes {
        return Err(ParseError::new(
            ParseErrorCode::PathTooLong,
            line,
            "path exceeds configured byte limit",
        ));
    }
    Ok(path.to_owned())
}

fn strip_cr(line: &str) -> &str {
    line.strip_suffix('\r').unwrap_or(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{PatchRequest, PatchRequestSource};

    fn request(text: &str) -> PatchRequest {
        PatchRequest::from_provider_text(
            text,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn parses_all_common_operations_and_guards() {
        let document = parse(
            &request(
                "*** Begin Patch\n*** Add File: add.txt\n+new\n*** Replace File: replace.txt\n*** If-Match: token\n+complete\n*** Update File: old.txt\n*** If-Destination: absent\n*** Move to: new.txt\n@@ section\n-old\n+new\n*** Delete File: gone.txt\n*** End Patch",
            ),
            PatchLimits::default(),
        )
        .unwrap();
        assert_eq!(document.operations.len(), 4);
        assert_eq!(
            document.operations[1].source_guard,
            Some(GuardSyntax::IfMatch("token".into()))
        );
        assert_eq!(document.operations[2].move_to.as_deref(), Some("new.txt"));
        assert_eq!(
            document.operations[2].destination_guard,
            Some(GuardSyntax::IfDestinationAbsent)
        );
    }

    #[test]
    fn rejects_missing_envelope_trailing_body_and_bad_hunk_line() {
        let limits = PatchLimits::default();
        assert_eq!(
            parse(&request("bad"), limits).unwrap_err().code,
            ParseErrorCode::MissingBegin
        );
        assert_eq!(
            parse(
                &request("*** Begin Patch\n*** Add File: a\n+x\n*** End Patch\ntrailing"),
                limits
            )
            .unwrap_err()
            .code,
            ParseErrorCode::TrailingContent
        );
        assert_eq!(
            parse(
                &request("*** Begin Patch\n*** Update File: a\n@@\nbad\n*** End Patch"),
                limits
            )
            .unwrap_err()
            .code,
            ParseErrorCode::InvalidHunkLine
        );
    }

    #[test]
    fn enforces_operation_and_hunk_limits() {
        let limits = PatchLimits {
            max_operations: 1,
            ..PatchLimits::default()
        };
        let error = parse(
            &request("*** Begin Patch\n*** Add File: a\n+x\n*** Add File: b\n+y\n*** End Patch"),
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code, ParseErrorCode::TooManyOperations);

        let limits = PatchLimits {
            max_total_hunks: 1,
            ..PatchLimits::default()
        };
        let error = parse(
            &request("*** Begin Patch\n*** Update File: a\n@@\n-a\n+b\n@@\n+b\n+c\n*** End Patch"),
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code, ParseErrorCode::TooManyHunks);
    }

    #[test]
    fn rejects_overlong_paths_during_parsing() {
        let limits = PatchLimits {
            max_path_bytes: 3,
            ..PatchLimits::default()
        };
        let error = parse(
            &request("*** Begin Patch\n*** Add File: long.txt\n+x\n*** End Patch"),
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code, ParseErrorCode::PathTooLong);
    }

    #[test]
    fn parser_is_json_round_trippable() {
        let document = parse(
            &request("*** Begin Patch\n*** Add File: a\n+x\n*** End Patch"),
            PatchLimits::default(),
        )
        .unwrap();
        let json = serde_json::to_string(&document).unwrap();
        assert_eq!(
            serde_json::from_str::<PatchDocument>(&json).unwrap(),
            document
        );
    }

    #[test]
    fn bounded_deterministic_fuzz_corpus_never_panics() {
        // Keep the corpus deterministic so a failure is reproducible without
        // an external fuzzer, while still exercising arbitrary envelope,
        // directive, control-character, CRLF and Unicode combinations.
        const ALPHABET: &[char] = &[
            '*', '+', '-', ' ', ':', '/', '.', '@', '\n', '\r', '\t', '\0', 'a', 'Z', '0', 'é',
            'Ж', '中', '🦀',
        ];
        let limits = PatchLimits {
            max_patch_bytes: 2_048,
            max_operations: 16,
            max_chunks_per_update: 16,
            max_total_hunks: 32,
            max_path_bytes: 128,
            ..PatchLimits::default()
        };
        let mut state = 0x67_c0_de_5e_ed_u64;

        for case in 0..4_096_usize {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let char_count = (state as usize) % 512;
            let mut patch = String::with_capacity(char_count.saturating_mul(4));
            for _ in 0..char_count {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                patch.push(ALPHABET[(state as usize) % ALPHABET.len()]);
            }
            if patch.is_empty() {
                patch.push('x');
            }
            let request = PatchRequest {
                schema_version: 1,
                patch,
                source: PatchRequestSource::NativeFreeform,
            };
            let _ = parse(&request, limits);

            // Mix valid structural fragments into a subset of cases so the
            // corpus reaches operation and hunk parsing, not only the first
            // envelope check.
            if case % 8 == 0 {
                let request = PatchRequest {
                    schema_version: 1,
                    patch: format!(
                        "*** Begin Patch\n*** Update File: fuzz-{case}.txt\n@@\n-old\n+{}\n*** End Patch",
                        request.patch
                    ),
                    source: PatchRequestSource::NativeFreeform,
                };
                let _ = parse(&request, limits);
            }
        }

        let oversized = PatchRequest {
            schema_version: 1,
            patch: "x".repeat(limits.max_patch_bytes as usize + 1),
            source: PatchRequestSource::NativeFreeform,
        };
        assert_eq!(
            parse(&oversized, limits).unwrap_err().code,
            ParseErrorCode::InputTooLarge
        );
    }
}
