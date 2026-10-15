use crate::apply_patch::file_mutation::FileVersionToken;
use crate::apply_patch::{GuardCandidate, GuardSyntax, Operation, OperationKind, PatchDocument};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "token")]
pub enum DestinationGuard {
    MustNotExist,
    Exact(FileVersionToken),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidatedOperation {
    pub operation: Operation,
    pub source_guard: Option<FileVersionToken>,
    pub destination_guard: Option<DestinationGuard>,
}

impl ValidatedOperation {
    pub fn kind(&self) -> OperationKind {
        self.operation.kind
    }

    pub fn path(&self) -> &str {
        &self.operation.path
    }

    pub fn is_move(&self) -> bool {
        self.operation.move_to.is_some()
    }

    /// Version guards are an internal/advanced compatibility feature, not a
    /// requirement of the model-facing patch syntax. The executor snapshots
    /// and revalidates every source under the target locks before commit.
    pub const fn requires_real_source_guard(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidatedPatchDocument {
    pub schema_version: u16,
    pub input_bytes: u64,
    pub payload_hash: [u8; 32],
    pub operations: Vec<ValidatedOperation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardErrorCode {
    MissingRequiredSourceGuard,
    InvalidSourceGuard,
    InvalidDestinationGuard,
    InapplicableGuard,
    DuplicateGuard,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GuardError {
    pub code: GuardErrorCode,
    pub operation_index: usize,
    pub line: usize,
    pub operation: OperationKind,
    pub path: Option<String>,
    pub message: String,
}

impl GuardError {
    fn new(
        code: GuardErrorCode,
        candidate: &GuardCandidate,
        line: usize,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            operation_index: candidate.operation_index,
            line,
            operation: candidate.kind,
            path: candidate.path.clone(),
            message: message.into(),
        }
    }
}

impl fmt::Display for GuardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "guard error at operation {}: {}",
            self.operation_index, self.message
        )
    }
}

impl std::error::Error for GuardError {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GuardFailure {
    pub diagnostics: Vec<GuardError>,
}

pub fn validate_guards(document: PatchDocument) -> Result<ValidatedPatchDocument, GuardError> {
    validate_guards_all(document).map_err(|failure| failure.diagnostics[0].clone())
}

pub fn validate_guards_all(
    document: PatchDocument,
) -> Result<ValidatedPatchDocument, GuardFailure> {
    let mut operations = Vec::with_capacity(document.operations.len());
    let mut diagnostics = Vec::new();
    for (operation_index, operation) in document.operations.into_iter().enumerate() {
        let candidate = GuardCandidate {
            operation_index,
            kind: operation.kind,
            path: Some(operation.path.clone()),
            header_line: operation.header_line,
            source_guard: operation.source_guard.clone(),
            source_guard_line: operation.source_guard_line,
            destination_guard: operation.destination_guard.clone(),
            destination_guard_line: operation.destination_guard_line,
            move_present: operation.move_to.is_some(),
            move_presence_known: true,
            repeated_guards: Vec::new(),
        };
        let (source_guard, destination_guard) = validate_candidate(&candidate, &mut diagnostics);
        operations.push(ValidatedOperation {
            operation,
            source_guard,
            destination_guard,
        });
    }
    diagnostics.sort_by_key(|error| error.line);
    if !diagnostics.is_empty() {
        return Err(GuardFailure { diagnostics });
    }
    Ok(ValidatedPatchDocument {
        schema_version: document.schema_version,
        input_bytes: document.input_bytes,
        payload_hash: document.payload_hash,
        operations,
    })
}

/// Validate directives recognized before the syntax scan stopped. Guard token
/// format needs neither a valid file path nor a complete operation body.
pub(crate) fn validate_guard_candidates(candidates: &[GuardCandidate]) -> GuardFailure {
    let mut diagnostics = Vec::new();
    for candidate in candidates {
        validate_candidate(candidate, &mut diagnostics);
    }
    diagnostics.sort_by_key(|error| error.line);
    GuardFailure { diagnostics }
}

fn validate_candidate(
    candidate: &GuardCandidate,
    diagnostics: &mut Vec<GuardError>,
) -> (Option<FileVersionToken>, Option<DestinationGuard>) {
    let source_line = candidate.source_guard_line.unwrap_or(candidate.header_line);
    let destination_line = candidate
        .destination_guard_line
        .unwrap_or(candidate.header_line);
    let source_guard = candidate
        .source_guard
        .as_ref()
        .and_then(|syntax| check_source_guard(syntax, candidate, source_line, diagnostics));
    let destination_guard = candidate.destination_guard.as_ref().and_then(|syntax| {
        check_destination_guard(syntax, candidate, destination_line, diagnostics)
    });
    for repeated in &candidate.repeated_guards {
        match &repeated.syntax {
            GuardSyntax::IfMatch(_) => {
                check_source_guard(&repeated.syntax, candidate, repeated.line, diagnostics);
            }
            GuardSyntax::IfDestinationAbsent | GuardSyntax::IfDestinationVersion(_) => {
                check_destination_guard(&repeated.syntax, candidate, repeated.line, diagnostics);
            }
        }
    }
    // Directive presence and token validity are independent facts.
    let inapplicable = match candidate.kind {
        OperationKind::Add => {
            candidate.source_guard.is_some()
                || candidate.destination_guard.is_some()
                || candidate.move_present
        }
        OperationKind::Replace | OperationKind::Delete => {
            candidate.destination_guard.is_some() || candidate.move_present
        }
        OperationKind::Update => {
            candidate.move_presence_known
                && !candidate.move_present
                && candidate.destination_guard.is_some()
        }
    };
    if inapplicable {
        let message = match candidate.kind {
            OperationKind::Add => "Add File accepts no guards or move destination",
            OperationKind::Replace | OperationKind::Delete => {
                "operation cannot carry a destination guard"
            }
            OperationKind::Update => "If-Destination requires Move to",
        };
        diagnostics.push(GuardError::new(
            GuardErrorCode::InapplicableGuard,
            candidate,
            if candidate.kind == OperationKind::Add && candidate.source_guard.is_some() {
                source_line
            } else {
                destination_line
            },
            message,
        ));
    }
    (source_guard, destination_guard)
}

fn check_source_guard(
    syntax: &GuardSyntax,
    candidate: &GuardCandidate,
    line: usize,
    diagnostics: &mut Vec<GuardError>,
) -> Option<FileVersionToken> {
    match syntax {
        GuardSyntax::IfMatch(token) => match FileVersionToken::parse(token) {
            Ok(token) => Some(token),
            Err(_) => {
                diagnostics.push(GuardError::new(
                    GuardErrorCode::InvalidSourceGuard,
                    candidate,
                    line,
                    "If-Match is not a canonical version token",
                ));
                None
            }
        },
        GuardSyntax::IfDestinationAbsent | GuardSyntax::IfDestinationVersion(_) => {
            diagnostics.push(GuardError::new(
                GuardErrorCode::InvalidSourceGuard,
                candidate,
                line,
                "destination guard cannot occupy If-Match",
            ));
            None
        }
    }
}

fn check_destination_guard(
    syntax: &GuardSyntax,
    candidate: &GuardCandidate,
    line: usize,
    diagnostics: &mut Vec<GuardError>,
) -> Option<DestinationGuard> {
    match syntax {
        GuardSyntax::IfDestinationAbsent => Some(DestinationGuard::MustNotExist),
        GuardSyntax::IfDestinationVersion(token) => match FileVersionToken::parse(token) {
            Ok(token) => Some(DestinationGuard::Exact(token)),
            Err(_) => {
                diagnostics.push(GuardError::new(
                    GuardErrorCode::InvalidDestinationGuard,
                    candidate,
                    line,
                    "If-Destination is not absent or a canonical version token",
                ));
                None
            }
        },
        GuardSyntax::IfMatch(_) => {
            diagnostics.push(GuardError::new(
                GuardErrorCode::InvalidDestinationGuard,
                candidate,
                line,
                "If-Match cannot occupy If-Destination",
            ));
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{PatchLimits, PatchRequest, PatchRequestSource};
    use crate::apply_patch::parse;

    fn validate(text: &str) -> Result<ValidatedPatchDocument, GuardError> {
        let request = PatchRequest::from_provider_text(
            text,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        validate_guards(parse(&request, PatchLimits::default()).unwrap())
    }

    #[test]
    fn destructive_operations_do_not_require_a_model_source_token() {
        let missing =
            validate("*** Begin Patch\n*** Delete File: file.txt\n*** End Patch").unwrap();
        assert!(!missing.operations[0].requires_real_source_guard());
        let malformed = validate(
            "*** Begin Patch\n*** Delete File: file.txt\n*** If-Match: token\n*** End Patch",
        )
        .unwrap_err();
        assert_eq!(malformed.code, GuardErrorCode::InvalidSourceGuard);
    }

    #[test]
    fn update_and_move_use_automatic_internal_preconditions() {
        let ordinary =
            validate("*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch")
                .unwrap();
        assert!(ordinary.operations[0].source_guard.is_none());
        let move_without_source = validate(
            "*** Begin Patch\n*** Update File: old.txt\n*** Move to: new.txt\n*** End Patch",
        )
        .unwrap();
        assert!(!move_without_source.operations[0].requires_real_source_guard());
        assert!(move_without_source.operations[0].source_guard.is_none());
        assert!(
            move_without_source.operations[0]
                .destination_guard
                .is_none()
        );
        let move_with_source = validate("*** Begin Patch\n*** Update File: old.txt\n*** If-Match: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:3\n*** Move to: new.txt\n*** If-Destination: absent\n@@\n-old\n+new\n*** End Patch").unwrap();
        assert_eq!(
            move_with_source.operations[0].destination_guard,
            Some(DestinationGuard::MustNotExist)
        );
    }

    #[test]
    fn add_rejects_guards_and_destination_guard_needs_move() {
        let add = validate("*** Begin Patch\n*** Add File: file.txt\n*** If-Match: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0\n+x\n*** End Patch").unwrap_err();
        assert_eq!(add.code, GuardErrorCode::InapplicableGuard);
        let update = validate("*** Begin Patch\n*** Update File: file.txt\n*** If-Destination: absent\n@@\n-old\n+new\n*** End Patch").unwrap_err();
        assert_eq!(update.code, GuardErrorCode::InapplicableGuard);
    }

    #[test]
    fn guard_validation_does_not_echo_stale_replacement_token() {
        let error = validate(
            "*** Begin Patch\n*** Delete File: file.txt\n*** If-Match: bad\n*** End Patch",
        )
        .unwrap_err();
        assert!(!error.message.contains("bad"));
    }

    #[test]
    fn independent_guard_errors_are_reported_across_operations() {
        let request = PatchRequest::from_provider_text(
            "*** Begin Patch\n*** Delete File: first.txt\n*** If-Match: bad\n*** Update File: second.txt\n*** If-Destination: invalid\n@@\n-old\n+new\n*** End Patch",
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        ).unwrap();
        let document = parse(&request, PatchLimits::default()).unwrap();
        let failure = validate_guards_all(document).unwrap_err();
        assert_eq!(failure.diagnostics.len(), 3);
        assert_eq!(
            failure.diagnostics[0].code,
            GuardErrorCode::InvalidSourceGuard
        );
        assert_eq!(
            failure.diagnostics[1].code,
            GuardErrorCode::InvalidDestinationGuard
        );
        assert_eq!(
            failure.diagnostics[2].code,
            GuardErrorCode::InapplicableGuard
        );
        assert_eq!(failure.diagnostics[1].line, 5);
        assert_eq!(failure.diagnostics[1].path.as_deref(), Some("second.txt"));
    }

    #[test]
    fn applicability_and_token_format_are_independent() {
        let cases = [
            (
                "*** Add File: a.txt\n*** If-Match: bad\n+x",
                GuardErrorCode::InvalidSourceGuard,
            ),
            (
                "*** Delete File: a.txt\n*** If-Destination: bad",
                GuardErrorCode::InvalidDestinationGuard,
            ),
            (
                "*** Update File: a.txt\n*** If-Destination: bad\n@@\n-old\n+new",
                GuardErrorCode::InvalidDestinationGuard,
            ),
        ];
        for (body, token_error) in cases {
            let request = PatchRequest::from_provider_text(
                &format!("*** Begin Patch\n{body}\n*** End Patch"),
                PatchRequestSource::NativeFreeform,
                PatchLimits::default(),
            )
            .unwrap();
            let failure =
                validate_guards_all(parse(&request, PatchLimits::default()).unwrap()).unwrap_err();
            assert_eq!(
                failure
                    .diagnostics
                    .iter()
                    .map(|error| error.code)
                    .collect::<Vec<_>>(),
                vec![token_error, GuardErrorCode::InapplicableGuard]
            );
            assert_eq!(failure.diagnostics[0].line, 3);
            assert_eq!(failure.diagnostics[1].line, 3);
        }
    }

    #[test]
    fn invalid_move_value_does_not_make_destination_guard_inapplicable() {
        let request = PatchRequest::from_provider_text(
            "*** Begin Patch\n*** Update File: a.txt\n*** If-Destination: bad\n*** Move to:\n*** End Patch",
            PatchRequestSource::NativeFreeform, PatchLimits::default(),
        ).unwrap();
        let failure =
            crate::apply_patch::parse_validated(&request, PatchLimits::default()).unwrap_err();
        let guards = validate_guard_candidates(&failure.guard_candidates);
        assert_eq!(guards.diagnostics.len(), 1);
        assert_eq!(
            guards.diagnostics[0].code,
            GuardErrorCode::InvalidDestinationGuard
        );
        assert_eq!(failure.diagnostics.len(), 1);
    }

    #[test]
    fn unknown_move_presence_suppresses_only_dependent_applicability() {
        for (token, expected) in [
            ("absent", Vec::new()),
            ("bad", vec![GuardErrorCode::InvalidDestinationGuard]),
        ] {
            let patch = format!(
                "*** Begin Patch\n*** Update File: a.txt\n*** If-Destination: {token}\n*** Unknown: x\n*** Move to: b.txt\n*** End Patch"
            );
            let failure = crate::apply_patch::parse_validated(
                &PatchRequest::from_provider_text(
                    &patch,
                    PatchRequestSource::NativeFreeform,
                    PatchLimits::default(),
                )
                .unwrap(),
                PatchLimits::default(),
            )
            .unwrap_err();
            let guards = validate_guard_candidates(&failure.guard_candidates);
            assert_eq!(
                guards
                    .diagnostics
                    .iter()
                    .map(|error| error.code)
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(guards.diagnostics.iter().all(|error| error.line == 3));
        }

        let complete = PatchRequest::from_provider_text(
            "*** Begin Patch\n*** Update File: a.txt\n*** If-Destination: absent\n@@\n-old\n+new\n*** End Patch",
            PatchRequestSource::NativeFreeform, PatchLimits::default(),
        ).unwrap();
        let guards =
            validate_guards_all(parse(&complete, PatchLimits::default()).unwrap()).unwrap_err();
        assert_eq!(
            guards
                .diagnostics
                .iter()
                .map(|error| error.code)
                .collect::<Vec<_>>(),
            vec![GuardErrorCode::InapplicableGuard]
        );
    }

    #[test]
    fn repeated_guard_values_are_checked_at_their_own_lines() {
        let canonical = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:3";
        let cases = [
            (format!("*** Delete File: a.txt\n*** If-Match: {canonical}\n*** If-Match: bad"), GuardErrorCode::InvalidSourceGuard),
            ("*** Update File: a.txt\n*** If-Destination: absent\n*** If-Destination: bad\n*** Move to: b.txt".to_owned(), GuardErrorCode::InvalidDestinationGuard),
        ];
        for (body, expected) in cases {
            let patch = format!("*** Begin Patch\n{body}\n*** End Patch");
            let failure = crate::apply_patch::parse_validated(
                &PatchRequest::from_provider_text(
                    &patch,
                    PatchRequestSource::NativeFreeform,
                    PatchLimits::default(),
                )
                .unwrap(),
                PatchLimits::default(),
            )
            .unwrap_err();
            assert_eq!(
                failure
                    .diagnostics
                    .iter()
                    .map(|error| error.code)
                    .collect::<Vec<_>>(),
                vec![crate::apply_patch::ParseErrorCode::DuplicateDirective]
            );
            let guards = validate_guard_candidates(&failure.guard_candidates);
            assert_eq!(
                guards
                    .diagnostics
                    .iter()
                    .map(|error| (error.code, error.line, error.operation_index))
                    .collect::<Vec<_>>(),
                vec![(expected, 4, 0)]
            );
        }

        for body in [
            format!("*** Delete File: a.txt\n*** If-Match: {canonical}\n*** If-Match: {canonical}"),
            "*** Update File: a.txt\n*** If-Destination: absent\n*** If-Destination: absent\n*** Move to: b.txt".to_owned(),
        ] {
            let patch = format!("*** Begin Patch\n{body}\n*** End Patch");
            let failure = crate::apply_patch::parse_validated(
                &PatchRequest::from_provider_text(&patch, PatchRequestSource::NativeFreeform,
                    PatchLimits::default()).unwrap(), PatchLimits::default(),
            ).unwrap_err();
            assert_eq!(failure.diagnostics.len(), 1);
            assert_eq!(failure.diagnostics[0].code, crate::apply_patch::ParseErrorCode::DuplicateDirective);
            assert!(validate_guard_candidates(&failure.guard_candidates).diagnostics.is_empty());
        }
    }
}
