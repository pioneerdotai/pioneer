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
    #[serde(default)]
    pub source_guard_line: Option<usize>,
    #[serde(default)]
    pub destination_guard_line: Option<usize>,
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
    MissingAddPrefix,
    MissingReplacePrefix,
    DeleteHasBody,
    DuplicateDirective,
    InvalidMoveDirective,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseStopReason {
    AmbiguousStructure,
    ResourceLimit,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParseError {
    pub code: ParseErrorCode,
    pub line: usize,
    pub column: usize,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<OperationKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl ParseError {
    fn new(code: ParseErrorCode, line: usize, message: impl Into<String>) -> Self {
        Self {
            code,
            line,
            column: 1,
            message: message.into(),
            operation_index: None,
            operation: None,
            path: None,
        }
    }

    fn in_operation(mut self, index: usize, kind: OperationKind, path: Option<&str>) -> Self {
        self.operation_index = Some(index.try_into().unwrap_or(u32::MAX));
        self.operation = Some(kind);
        self.path = path.map(str::to_owned);
        self
    }
}

/// Syntax diagnostics are ordered by patch position. `unchecked_from_line` is
/// present when ambiguity or a resource limit stopped the scan. The kind of
/// operation determines its body grammar even when its path is invalid.
/// Syntax completeness says nothing about later pipeline stages.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParseFailure {
    pub diagnostics: Vec<ParseError>,
    pub unchecked_from_line: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unverified_ranges: Vec<UnverifiedRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<ParseStopReason>,
    /// Reliably delimited directives remain checkable after an unrelated
    /// syntax error. This is internal scan state, never part of the response.
    #[serde(skip)]
    pub(crate) guard_candidates: Vec<GuardCandidate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GuardCandidate {
    pub operation_index: usize,
    pub kind: OperationKind,
    pub path: Option<String>,
    pub header_line: usize,
    pub source_guard: Option<GuardSyntax>,
    pub source_guard_line: Option<usize>,
    pub destination_guard: Option<GuardSyntax>,
    pub destination_guard_line: Option<usize>,
    pub move_present: bool,
    pub move_presence_known: bool,
    pub repeated_guards: Vec<GuardDirectiveCandidate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GuardDirectiveCandidate {
    pub syntax: GuardSyntax,
    pub line: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UnverifiedRange {
    pub start: usize,
    pub end: usize,
}

impl ParseFailure {
    pub fn first(&self) -> &ParseError {
        &self.diagnostics[0]
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

/// Compatibility view for callers that only need the first syntax error.
pub fn parse(request: &PatchRequest, limits: PatchLimits) -> Result<PatchDocument, ParseError> {
    parse_validated(request, limits).map_err(|failure| failure.first().clone())
}

pub fn parse_validated(
    request: &PatchRequest,
    limits: PatchLimits,
) -> Result<PatchDocument, ParseFailure> {
    let mut scan = SyntaxScan::new(limits);
    if request.patch.len() as u64 > limits.max_patch_bytes {
        scan.stop(
            ParseError::new(
                ParseErrorCode::InputTooLarge,
                1,
                "patch exceeds configured input limit",
            ),
            1,
        );
        return Err(scan.failure());
    }
    let raw = request.patch.as_str();
    if raw.trim().is_empty() {
        scan.add(ParseError::new(
            ParseErrorCode::EmptyInput,
            1,
            "patch is empty",
        ));
        return Err(scan.failure());
    }
    let lines = raw.split('\n').map(strip_cr).collect::<Vec<_>>();
    if lines.first().copied() != Some("*** Begin Patch") {
        scan.stop(
            ParseError::new(
                ParseErrorCode::MissingBegin,
                1,
                "first line must be *** Begin Patch",
            ),
            1,
        );
        return Err(scan.failure());
    }
    let end = lines.iter().position(|line| *line == "*** End Patch");
    let body_end = end.unwrap_or_else(|| {
        if raw.ends_with('\n') {
            lines.len() - 1
        } else {
            lines.len()
        }
    });
    if let Some(end) = end {
        if let Some(offset) = lines[end + 1..]
            .iter()
            .position(|line| !line.trim().is_empty())
        {
            scan.add(ParseError::new(
                ParseErrorCode::TrailingContent,
                end + 2 + offset,
                "non-whitespace content follows *** End Patch",
            ));
        }
    } else {
        scan.add(ParseError::new(
            ParseErrorCode::MissingEnd,
            lines.len(),
            "missing *** End Patch",
        ));
    }
    if body_end == 1 {
        scan.add(ParseError::new(
            ParseErrorCode::EmptyInput,
            2,
            "patch contains no operations",
        ));
    }

    let mut operations = Vec::new();
    let mut guard_candidates = Vec::new();
    let mut operation_index = 0usize;
    let mut total_hunks = 0usize;
    let mut index = 1usize;
    while index < body_end && scan.unchecked_from_line.is_none() {
        if operation_index >= limits.max_operations as usize {
            scan.stop(
                ParseError::new(
                    ParseErrorCode::TooManyOperations,
                    index + 1,
                    "operation limit exceeded",
                ),
                index + 1,
            );
            break;
        }
        let header_line = index + 1;
        let line = lines[index];
        let (kind, raw_path) =
            if let Some(path) = line.strip_prefix("*** Add File:") {
                (OperationKind::Add, path)
            } else if let Some(path) = line.strip_prefix("*** Replace File:") {
                (OperationKind::Replace, path)
            } else if let Some(path) = line.strip_prefix("*** Update File:") {
                (OperationKind::Update, path)
            } else if let Some(path) = line.strip_prefix("*** Delete File:") {
                (OperationKind::Delete, path)
            } else {
                scan.stop(ParseError::new(
                ParseErrorCode::UnknownDirective, header_line,
                "expected a file operation directive; later operation boundaries are unknown",
            ), header_line);
                break;
            };
        let this_operation = operation_index;
        operation_index += 1;
        index += 1;
        let path = match parse_path(raw_path, header_line, limits.max_path_bytes) {
            Ok(path) => Some(path),
            Err(error) => {
                scan.add(error.in_operation(this_operation, kind, None));
                None
            }
        };
        let mut source_guard = None;
        let mut destination_guard = None;
        let mut move_to = None;
        let mut source_guard_line = None;
        let mut destination_guard_line = None;
        let mut move_present = false;
        let mut repeated_guards = Vec::new();
        while index < body_end && scan.unchecked_from_line.is_none() {
            let directive = lines[index];
            if let Some(token) = directive.strip_prefix("*** If-Match:") {
                let syntax = GuardSyntax::IfMatch(token.trim().to_owned());
                if source_guard.is_some() {
                    scan.add(
                        ParseError::new(
                            ParseErrorCode::DuplicateDirective,
                            index + 1,
                            "duplicate *** If-Match directive",
                        )
                        .in_operation(
                            this_operation,
                            kind,
                            path.as_deref(),
                        ),
                    );
                    if scan.unchecked_from_line.is_none() {
                        repeated_guards.push(GuardDirectiveCandidate {
                            syntax,
                            line: index + 1,
                        });
                    }
                } else {
                    source_guard = Some(syntax);
                    source_guard_line = Some(index + 1);
                }
            } else if let Some(value) = directive.strip_prefix("*** If-Destination:") {
                let value = value.trim();
                let syntax = if value == "absent" {
                    GuardSyntax::IfDestinationAbsent
                } else {
                    GuardSyntax::IfDestinationVersion(value.to_owned())
                };
                if destination_guard.is_some() {
                    scan.add(
                        ParseError::new(
                            ParseErrorCode::DuplicateDirective,
                            index + 1,
                            "duplicate *** If-Destination directive",
                        )
                        .in_operation(
                            this_operation,
                            kind,
                            path.as_deref(),
                        ),
                    );
                    if scan.unchecked_from_line.is_none() {
                        repeated_guards.push(GuardDirectiveCandidate {
                            syntax,
                            line: index + 1,
                        });
                    }
                } else {
                    destination_guard = Some(syntax);
                    destination_guard_line = Some(index + 1);
                }
            } else if let Some(value) = directive.strip_prefix("*** Move to:") {
                let usable = !move_present && kind == OperationKind::Update;
                if !usable {
                    scan.add(
                        ParseError::new(
                            ParseErrorCode::InvalidMoveDirective,
                            index + 1,
                            "*** Move to is valid only once on Update File",
                        )
                        .in_operation(
                            this_operation,
                            kind,
                            path.as_deref(),
                        ),
                    );
                } else {
                    move_present = true;
                }
                if scan.unchecked_from_line.is_none() {
                    match parse_path(value, index + 1, limits.max_path_bytes) {
                        Ok(destination) if usable => move_to = Some(destination),
                        Ok(_) => {}
                        Err(error) => {
                            scan.add(error.in_operation(this_operation, kind, path.as_deref()))
                        }
                    }
                }
            } else {
                break;
            }
            index += 1;
        }
        if scan.unchecked_from_line.is_some() {
            break;
        }
        // An unknown *** directive can still be a missing or malformed
        // directive for this operation. Only a known boundary or body start
        // establishes that no more Move to directive can follow.
        let move_presence_known =
            known_file_boundary_or_end(&lines, index, body_end, end.is_some())
                || (index < body_end && !lines[index].starts_with("*** "));
        guard_candidates.push(GuardCandidate {
            operation_index: this_operation,
            kind,
            path: path.clone(),
            header_line,
            source_guard: source_guard.clone(),
            source_guard_line,
            destination_guard: destination_guard.clone(),
            destination_guard_line,
            move_present,
            move_presence_known,
            repeated_guards,
        });
        let context = (this_operation, kind, path.as_deref());
        let body =
            match kind {
                OperationKind::Add | OperationKind::Replace => {
                    let start = index;
                    let mut values = Vec::new();
                    while index < body_end && !lines[index].starts_with("*** ") {
                        let line = lines[index];
                        if let Some(value) = line.strip_prefix('+') {
                            values.push(value.to_owned());
                        } else {
                            let message = if kind == OperationKind::Add {
                                "Add File lines must start with +"
                            } else {
                                "Replace File lines must start with +"
                            };
                            scan.add(
                                ParseError::new(
                                    if kind == OperationKind::Add {
                                        ParseErrorCode::MissingAddPrefix
                                    } else {
                                        ParseErrorCode::MissingReplacePrefix
                                    },
                                    index + 1,
                                    message,
                                )
                                .in_operation(context.0, context.1, context.2),
                            );
                        }
                        index += 1;
                        if scan.unchecked_from_line.is_some() {
                            break;
                        }
                    }
                    if index == start
                        && known_file_boundary_or_end(&lines, index, body_end, end.is_some())
                        && scan.unchecked_from_line.is_none()
                    {
                        let (code, message) = if kind == OperationKind::Add {
                            (ParseErrorCode::EmptyAdd, "Add File has no content")
                        } else {
                            (ParseErrorCode::EmptyReplace, "Replace File has no content")
                        };
                        scan.add(
                            ParseError::new(code, start + 1, message)
                                .in_operation(context.0, context.1, context.2),
                        );
                    }
                    if kind == OperationKind::Add {
                        OperationBody::Add(AddFile { lines: values })
                    } else {
                        OperationBody::Replace(ReplaceFile { lines: values })
                    }
                }
                OperationKind::Delete => {
                    if index < body_end && !lines[index].starts_with("*** ") {
                        scan.add(
                            ParseError::new(
                                ParseErrorCode::DeleteHasBody,
                                index + 1,
                                "Delete File cannot have a body",
                            )
                            .in_operation(context.0, context.1, context.2),
                        );
                        while index < body_end && !lines[index].starts_with("*** ") {
                            index += 1;
                        }
                    }
                    OperationBody::Delete
                }
                OperationKind::Update => {
                    let mut hunks = Vec::new();
                    let mut had_missing_header = false;
                    while index < body_end
                        && !lines[index].starts_with("*** ")
                        && scan.unchecked_from_line.is_none()
                    {
                        if !lines[index].starts_with("@@") {
                            had_missing_header = true;
                            scan.add(
                                ParseError::new(
                                    ParseErrorCode::MissingHunk,
                                    index + 1,
                                    "Update File expects an @@ hunk header",
                                )
                                .in_operation(context.0, context.1, context.2),
                            );
                            // Content before a hunk header has no reliable hunk
                            // structure; resume only at a known header.
                            while index < body_end
                                && !lines[index].starts_with("@@")
                                && !lines[index].starts_with("*** ")
                            {
                                index += 1;
                            }
                            continue;
                        }
                        if hunks.len() >= limits.max_chunks_per_update as usize
                            || total_hunks >= limits.max_total_hunks as usize
                        {
                            let (code, message) =
                                if hunks.len() >= limits.max_chunks_per_update as usize {
                                    (ParseErrorCode::TooManyChunks, "update hunk limit exceeded")
                                } else {
                                    (ParseErrorCode::TooManyHunks, "total hunk limit exceeded")
                                };
                            scan.stop(
                                ParseError::new(code, index + 1, message)
                                    .in_operation(context.0, context.1, context.2),
                                index + 1,
                            );
                            break;
                        }
                        total_hunks += 1;
                        let hunk_header = index + 1;
                        let context_text = lines[index][2..].trim();
                        let hunk_context =
                            (!context_text.is_empty()).then(|| context_text.to_owned());
                        index += 1;
                        let mut hunk_lines = Vec::new();
                        let mut had_invalid_line = false;
                        while index < body_end
                            && !lines[index].starts_with("@@")
                            && !lines[index].starts_with("*** ")
                        {
                            let line = lines[index];
                            let value = line.get(1..).unwrap_or_default().to_owned();
                            match line.as_bytes().first().copied() {
                                Some(b' ') => hunk_lines.push(HunkLine::Context(value)),
                                Some(b'-') => hunk_lines.push(HunkLine::Remove(value)),
                                Some(b'+') => hunk_lines.push(HunkLine::Add(value)),
                                _ => {
                                    had_invalid_line = true;
                                    scan.add(
                                        ParseError::new(
                                            ParseErrorCode::InvalidHunkLine,
                                            index + 1,
                                            "hunk lines must start with space, - or +",
                                        )
                                        .in_operation(context.0, context.1, context.2),
                                    );
                                }
                            }
                            index += 1;
                            if scan.unchecked_from_line.is_some() {
                                break;
                            }
                        }
                        let mut end_of_file = false;
                        if index < body_end && lines[index] == "*** End of File" {
                            end_of_file = true;
                            index += 1;
                        }
                        if hunk_lines.is_empty()
                            && !end_of_file
                            && !had_invalid_line
                            && scan.unchecked_from_line.is_none()
                            && (known_file_boundary_or_end(&lines, index, body_end, end.is_some())
                                || (index < body_end && lines[index].starts_with("@@")))
                        {
                            scan.add(
                                ParseError::new(
                                    ParseErrorCode::InvalidHunkLine,
                                    hunk_header,
                                    "empty hunk",
                                )
                                .in_operation(context.0, context.1, context.2),
                            );
                        }
                        hunks.push(Hunk {
                            context: hunk_context,
                            lines: hunk_lines,
                            end_of_file,
                            header_line: hunk_header,
                        });
                    }
                    if hunks.is_empty()
                        && !move_present
                        && move_presence_known
                        && !had_missing_header
                        && scan.unchecked_from_line.is_none()
                    {
                        scan.add(ParseError::new(
                        ParseErrorCode::MissingHunk, index + 1,
                        "Update File needs an @@ hunk unless it is a pure Move to operation",
                    ).in_operation(context.0, context.1, context.2));
                    }
                    OperationBody::Update(UpdateFile { hunks })
                }
            };
        if let Some(path) = path {
            operations.push(Operation {
                kind,
                path,
                source_guard,
                destination_guard,
                source_guard_line,
                destination_guard_line,
                move_to,
                body,
                header_line,
            });
        }
    }
    if end.is_none() && scan.unchecked_from_line.is_none() {
        scan.unchecked_from_line = Some(lines.len() + 1);
        scan.stop_reason = Some(ParseStopReason::AmbiguousStructure);
    }
    if !scan.diagnostics.is_empty() {
        scan.guard_candidates = guard_candidates;
        return Err(scan.failure());
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

fn is_file_directive(line: &str) -> bool {
    line.starts_with("*** Add File:")
        || line.starts_with("*** Replace File:")
        || line.starts_with("*** Update File:")
        || line.starts_with("*** Delete File:")
}

fn known_file_boundary_or_end(
    lines: &[&str],
    index: usize,
    body_end: usize,
    end_marker_present: bool,
) -> bool {
    // Neither an unknown *** line nor an unclosed input establishes emptiness.
    if index < body_end {
        is_file_directive(lines[index])
    } else {
        end_marker_present
    }
}

struct SyntaxScan {
    diagnostics: Vec<ParseError>,
    unchecked_from_line: Option<usize>,
    unverified_ranges: Vec<UnverifiedRange>,
    stop_reason: Option<ParseStopReason>,
    guard_candidates: Vec<GuardCandidate>,
    max_diagnostics: usize,
}

impl SyntaxScan {
    fn new(limits: PatchLimits) -> Self {
        // The existing total-hunk budget also bounds diagnostic storage. A
        // malformed line can otherwise produce far more objects than hunks.
        Self {
            diagnostics: Vec::new(),
            unchecked_from_line: None,
            unverified_ranges: Vec::new(),
            stop_reason: None,
            guard_candidates: Vec::new(),
            max_diagnostics: limits.max_total_hunks.max(1) as usize,
        }
    }

    fn add(&mut self, error: ParseError) {
        if self.unchecked_from_line.is_some() {
            return;
        }
        if self.diagnostics.len() >= self.max_diagnostics {
            self.unchecked_from_line = Some(error.line);
            self.stop_reason = Some(ParseStopReason::ResourceLimit);
        } else {
            self.diagnostics.push(error);
        }
    }

    fn stop(&mut self, error: ParseError, from_line: usize) {
        let reason = match error.code {
            ParseErrorCode::InputTooLarge
            | ParseErrorCode::TooManyOperations
            | ParseErrorCode::TooManyChunks
            | ParseErrorCode::TooManyHunks => ParseStopReason::ResourceLimit,
            _ => ParseStopReason::AmbiguousStructure,
        };
        let at_diagnostic_limit = self.diagnostics.len() >= self.max_diagnostics;
        self.add(error);
        self.unchecked_from_line = Some(from_line);
        self.stop_reason = Some(if at_diagnostic_limit {
            ParseStopReason::ResourceLimit
        } else {
            reason
        });
    }

    fn failure(self) -> ParseFailure {
        let mut diagnostics = self.diagnostics;
        diagnostics.sort_by_key(|error| error.line);
        ParseFailure {
            diagnostics,
            unchecked_from_line: self.unchecked_from_line,
            unverified_ranges: self.unverified_ranges,
            stop_reason: self.stop_reason,
            guard_candidates: self.guard_candidates,
        }
    }
}

fn parse_path(value: &str, line: usize, max_path_bytes: u64) -> Result<String, ParseError> {
    let path = value.trim();
    if path.is_empty() {
        return Err(ParseError::new(
            ParseErrorCode::MissingPath,
            line,
            "file operation path is missing",
        ));
    }
    if path.contains('\0') {
        return Err(ParseError::new(
            ParseErrorCode::InvalidPath,
            line,
            "path contains NUL",
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
    fn collects_add_lines_across_operations_without_repairing_them() {
        let patch = "*** Begin Patch\n*** Add File: first.txt\nwrong\n+\nalso wrong\n*** Add File: second.txt\nmissing\n+valid\n*** End Patch";
        let failure = parse_validated(&request(patch), PatchLimits::default()).unwrap_err();
        assert_eq!(failure.unchecked_from_line, None);
        assert_eq!(failure.diagnostics.len(), 3);
        assert_eq!(
            failure
                .diagnostics
                .iter()
                .map(|d| d.line)
                .collect::<Vec<_>>(),
            vec![3, 5, 7]
        );
        assert_eq!(failure.diagnostics[0].operation_index, Some(0));
        assert_eq!(failure.diagnostics[2].operation_index, Some(1));
        assert_eq!(failure.diagnostics[2].path.as_deref(), Some("second.txt"));
        assert!(
            failure
                .diagnostics
                .iter()
                .all(|d| d.code == ParseErrorCode::MissingAddPrefix)
        );
        let valid = parse_validated(
            &request("*** Begin Patch\n*** Add File: empty.txt\n+\n*** End Patch"),
            PatchLimits::default(),
        )
        .unwrap();
        assert_eq!(
            valid.operations[0].body,
            OperationBody::Add(AddFile {
                lines: vec![String::new()]
            })
        );
    }

    #[test]
    fn valid_add_update_move_and_delete_still_parse() {
        let patch = "*** Begin Patch\n*** Add File: add.txt\n+new\n*** Update File: update.txt\n@@\n-old\n+new\n*** Update File: from.txt\n*** Move to: to.txt\n*** Delete File: gone.txt\n*** End Patch";
        let document = parse_validated(&request(patch), PatchLimits::default()).unwrap();
        assert_eq!(
            document
                .operations
                .iter()
                .map(|op| op.kind)
                .collect::<Vec<_>>(),
            vec![
                OperationKind::Add,
                OperationKind::Update,
                OperationKind::Update,
                OperationKind::Delete,
            ]
        );
        assert_eq!(document.operations[2].move_to.as_deref(), Some("to.txt"));
    }

    #[test]
    fn distinct_operation_bodies_report_independent_codes() {
        let patch = "*** Begin Patch\n*** Add File: a.txt\nwrong\n*** Update File: b.txt\n@@\nwrong\n*** Delete File: c.txt\nstray\n*** End Patch";
        let failure = parse_validated(&request(patch), PatchLimits::default()).unwrap_err();
        assert_eq!(failure.unchecked_from_line, None);
        assert_eq!(
            failure
                .diagnostics
                .iter()
                .map(|d| d.code)
                .collect::<Vec<_>>(),
            vec![
                ParseErrorCode::MissingAddPrefix,
                ParseErrorCode::InvalidHunkLine,
                ParseErrorCode::DeleteHasBody
            ],
        );
        assert_eq!(
            failure
                .diagnostics
                .iter()
                .map(|d| d.operation_index)
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2)]
        );
    }

    #[test]
    fn unknown_boundary_marks_unchecked_tail_without_cascade() {
        let patch = "*** Begin Patch\n*** Add File: a.txt\nwrong\n*** Unknown: x\nnoise\n*** Add File: b.txt\nwrong\n*** End Patch";
        let failure = parse_validated(&request(patch), PatchLimits::default()).unwrap_err();
        assert_eq!(failure.diagnostics.len(), 2);
        assert_eq!(failure.diagnostics[0].line, 3);
        assert_eq!(
            failure.diagnostics[1].code,
            ParseErrorCode::UnknownDirective
        );
        assert_eq!(failure.unchecked_from_line, Some(4));
        assert_eq!(
            failure.stop_reason,
            Some(ParseStopReason::AmbiguousStructure)
        );

        let hunk = "*** Begin Patch\n*** Update File: a.txt\n@@\nwrong\n*** End Patch";
        let failure = parse_validated(&request(hunk), PatchLimits::default()).unwrap_err();
        assert_eq!(failure.diagnostics.len(), 1);
        assert_eq!(failure.diagnostics[0].code, ParseErrorCode::InvalidHunkLine);
    }

    #[test]
    fn unknown_boundary_does_not_prove_empty_add_replace_or_hunk() {
        let cases = [
            ("*** Add File: a.txt\n*** Unknown: x\n+content", 3),
            ("*** Replace File: a.txt\n*** Unknown: x\n+content", 3),
            ("*** Update File: a.txt\n@@\n*** Unknown: x\n-old\n+new", 4),
        ];
        for (body, unknown_line) in cases {
            let patch = format!("*** Begin Patch\n{body}\n*** End Patch");
            let failure = parse_validated(&request(&patch), PatchLimits::default()).unwrap_err();
            assert_eq!(
                failure
                    .diagnostics
                    .iter()
                    .map(|error| (error.code, error.line))
                    .collect::<Vec<_>>(),
                vec![(ParseErrorCode::UnknownDirective, unknown_line)]
            );
            assert_eq!(failure.unchecked_from_line, Some(unknown_line));
            assert_eq!(
                failure.stop_reason,
                Some(ParseStopReason::AmbiguousStructure)
            );
        }
    }

    #[test]
    fn empty_content_is_reported_at_known_boundaries() {
        let cases = [
            ("*** Add File: a.txt", ParseErrorCode::EmptyAdd),
            ("*** Replace File: a.txt", ParseErrorCode::EmptyReplace),
        ];
        for (operation, code) in cases {
            for suffix in ["", "\n*** Delete File: b.txt"] {
                let patch = format!("*** Begin Patch\n{operation}{suffix}\n*** End Patch");
                let failure =
                    parse_validated(&request(&patch), PatchLimits::default()).unwrap_err();
                assert_eq!(
                    failure
                        .diagnostics
                        .iter()
                        .map(|error| error.code)
                        .collect::<Vec<_>>(),
                    vec![code]
                );
                assert!(failure.unchecked_from_line.is_none());
            }
        }
    }

    #[test]
    fn empty_hunk_is_reported_only_at_known_boundaries() {
        for suffix in ["", "\n*** Delete File: b.txt", "\n@@\n-old\n+new"] {
            let patch =
                format!("*** Begin Patch\n*** Update File: a.txt\n@@{suffix}\n*** End Patch");
            let failure = parse_validated(&request(&patch), PatchLimits::default()).unwrap_err();
            assert_eq!(
                failure
                    .diagnostics
                    .iter()
                    .map(|error| (error.code, error.line))
                    .collect::<Vec<_>>(),
                vec![(ParseErrorCode::InvalidHunkLine, 3)]
            );
            assert_eq!(failure.diagnostics[0].message, "empty hunk");
            assert!(failure.unchecked_from_line.is_none());
        }
        let end_of_file = parse_validated(
            &request("*** Begin Patch\n*** Update File: a.txt\n@@\n*** End of File\n*** End Patch"),
            PatchLimits::default(),
        )
        .unwrap();
        assert!(matches!(
            end_of_file.operations[0].body,
            OperationBody::Update(_)
        ));
    }

    #[test]
    fn errors_read_before_an_unknown_boundary_are_preserved() {
        let patch =
            "*** Begin Patch\n*** Add File: a.txt\nwrong\n*** Unknown: x\n+content\n*** End Patch";
        let failure = parse_validated(&request(patch), PatchLimits::default()).unwrap_err();
        assert_eq!(
            failure
                .diagnostics
                .iter()
                .map(|error| (error.code, error.line))
                .collect::<Vec<_>>(),
            vec![
                (ParseErrorCode::MissingAddPrefix, 3),
                (ParseErrorCode::UnknownDirective, 4)
            ]
        );
        assert_eq!(failure.unchecked_from_line, Some(4));
    }

    #[test]
    fn missing_end_marker_does_not_prove_empty_content() {
        for patch in [
            "*** Begin Patch\n*** Add File: a.txt",
            "*** Begin Patch\n*** Replace File: a.txt",
            "*** Begin Patch\n*** Update File: a.txt\n@@",
        ] {
            let failure = parse_validated(&request(patch), PatchLimits::default()).unwrap_err();
            assert_eq!(
                failure
                    .diagnostics
                    .iter()
                    .map(|error| error.code)
                    .collect::<Vec<_>>(),
                vec![ParseErrorCode::MissingEnd]
            );
            assert_eq!(
                failure.stop_reason,
                Some(ParseStopReason::AmbiguousStructure)
            );
            assert!(failure.unchecked_from_line.is_some());
        }
        let known_boundary = parse_validated(
            &request("*** Begin Patch\n*** Add File: a.txt\n*** Delete File: b.txt"),
            PatchLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            known_boundary
                .diagnostics
                .iter()
                .map(|error| error.code)
                .collect::<Vec<_>>(),
            vec![ParseErrorCode::MissingEnd, ParseErrorCode::EmptyAdd]
        );
    }

    #[test]
    fn invalid_path_still_checks_its_body_then_resumes_at_known_file_directive() {
        let patch =
            "*** Begin Patch\n*** Add File:\nwrong\n*** Add File: next.txt\nwrong\n*** End Patch";
        let failure = parse_validated(&request(patch), PatchLimits::default()).unwrap_err();
        assert_eq!(failure.unchecked_from_line, None);
        assert_eq!(failure.diagnostics.len(), 3);
        assert_eq!(failure.diagnostics[0].code, ParseErrorCode::MissingPath);
        assert!(failure.unverified_ranges.is_empty());
        assert_eq!(
            failure.diagnostics[1].code,
            ParseErrorCode::MissingAddPrefix
        );
        assert_eq!(failure.diagnostics[1].path, None);
        assert_eq!(failure.diagnostics[2].path.as_deref(), Some("next.txt"));
    }

    #[test]
    fn malformed_move_value_is_present_and_duplicate_remains_visible() {
        let pure = parse_validated(
            &request("*** Begin Patch\n*** Update File: a.txt\n*** Move to:\n*** End Patch"),
            PatchLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            pure.diagnostics.iter().map(|d| d.code).collect::<Vec<_>>(),
            vec![ParseErrorCode::MissingPath]
        );
        let repeated = parse_validated(&request("*** Begin Patch\n*** Update File: a.txt\n*** Move to:\n*** Move to: b.txt\n*** End Patch"), PatchLimits::default()).unwrap_err();
        assert_eq!(
            repeated
                .diagnostics
                .iter()
                .map(|d| d.code)
                .collect::<Vec<_>>(),
            vec![
                ParseErrorCode::MissingPath,
                ParseErrorCode::InvalidMoveDirective
            ]
        );
    }

    #[test]
    fn unknown_directive_does_not_prove_an_update_has_no_move_or_hunk() {
        let failure = parse_validated(
            &request("*** Begin Patch\n*** Update File: a.txt\n*** If-Destination: absent\n*** Unknown: x\n*** Move to: b.txt\n*** End Patch"),
            PatchLimits::default(),
        ).unwrap_err();
        assert_eq!(
            failure
                .diagnostics
                .iter()
                .map(|error| error.code)
                .collect::<Vec<_>>(),
            vec![ParseErrorCode::UnknownDirective]
        );
        assert_eq!(failure.unchecked_from_line, Some(4));
        assert_eq!(
            failure.stop_reason,
            Some(ParseStopReason::AmbiguousStructure)
        );
        assert_eq!(failure.guard_candidates.len(), 1);
        assert!(!failure.guard_candidates[0].move_presence_known);

        let missing = parse_validated(
            &request("*** Begin Patch\n*** Update File: a.txt\n*** End Patch"),
            PatchLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            missing
                .diagnostics
                .iter()
                .map(|error| error.code)
                .collect::<Vec<_>>(),
            vec![ParseErrorCode::MissingHunk]
        );
    }

    #[test]
    fn repeated_move_checks_its_own_path_without_replacing_the_first() {
        let bad = parse_validated(
            &request("*** Begin Patch\n*** Update File: a.txt\n*** Move to: b.txt\n*** Move to:\n*** End Patch"),
            PatchLimits::default(),
        ).unwrap_err();
        assert_eq!(
            bad.diagnostics
                .iter()
                .map(|error| (error.code, error.line))
                .collect::<Vec<_>>(),
            vec![
                (ParseErrorCode::InvalidMoveDirective, 4),
                (ParseErrorCode::MissingPath, 4)
            ]
        );
        assert_eq!(
            bad.diagnostics
                .iter()
                .map(|error| error.operation_index)
                .collect::<Vec<_>>(),
            vec![Some(0), Some(0)]
        );

        let valid = parse_validated(
            &request("*** Begin Patch\n*** Update File: a.txt\n*** Move to: b.txt\n*** Move to: c.txt\n*** End Patch"),
            PatchLimits::default(),
        ).unwrap_err();
        assert_eq!(
            valid
                .diagnostics
                .iter()
                .map(|error| error.code)
                .collect::<Vec<_>>(),
            vec![ParseErrorCode::InvalidMoveDirective]
        );
    }

    #[test]
    fn body_grammar_is_checked_without_a_valid_target_path() {
        let cases = [
            (
                "*** Replace File:\nwrong",
                ParseErrorCode::MissingReplacePrefix,
            ),
            ("*** Delete File:\nstray", ParseErrorCode::DeleteHasBody),
            (
                "*** Update File:\n@@\nwrong",
                ParseErrorCode::InvalidHunkLine,
            ),
        ];
        for (body, body_error) in cases {
            let patch = format!("*** Begin Patch\n{body}\n*** End Patch");
            let failure = parse_validated(&request(&patch), PatchLimits::default()).unwrap_err();
            assert_eq!(
                failure
                    .diagnostics
                    .iter()
                    .map(|error| error.code)
                    .collect::<Vec<_>>(),
                vec![ParseErrorCode::MissingPath, body_error]
            );
            assert!(failure.unverified_ranges.is_empty());
            assert!(failure.unchecked_from_line.is_none());
            assert!(failure.diagnostics[1].path.is_none());
        }
    }

    #[test]
    fn diagnostic_budget_stops_validation_without_claiming_a_total() {
        let limits = PatchLimits {
            max_total_hunks: 2,
            ..PatchLimits::default()
        };
        let failure = parse_validated(
            &request("*** Begin Patch\n*** Add File: a\none\ntwo\nthree\n*** End Patch"),
            limits,
        )
        .unwrap_err();
        assert_eq!(failure.diagnostics.len(), 2);
        assert_eq!(failure.unchecked_from_line, Some(5));
        assert_eq!(failure.stop_reason, Some(ParseStopReason::ResourceLimit));
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
