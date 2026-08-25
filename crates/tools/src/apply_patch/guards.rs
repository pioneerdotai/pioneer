use crate::apply_patch::file_mutation::FileVersionToken;
use crate::apply_patch::{GuardSyntax, Operation, OperationKind, PatchDocument};
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
    pub message: String,
}

impl GuardError {
    fn new(code: GuardErrorCode, operation_index: usize, message: impl Into<String>) -> Self {
        Self {
            code,
            operation_index,
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

pub fn validate_guards(document: PatchDocument) -> Result<ValidatedPatchDocument, GuardError> {
    let mut operations = Vec::with_capacity(document.operations.len());
    for (operation_index, operation) in document.operations.into_iter().enumerate() {
        let source_guard = match operation.source_guard.as_ref() {
            Some(GuardSyntax::IfMatch(token)) => {
                Some(FileVersionToken::parse(token).map_err(|_| {
                    GuardError::new(
                        GuardErrorCode::InvalidSourceGuard,
                        operation_index,
                        "If-Match is not a canonical version token",
                    )
                })?)
            }
            Some(GuardSyntax::IfDestinationAbsent | GuardSyntax::IfDestinationVersion(_)) => {
                return Err(GuardError::new(
                    GuardErrorCode::InvalidSourceGuard,
                    operation_index,
                    "destination guard cannot occupy If-Match",
                ));
            }
            None => None,
        };
        let destination_guard = match operation.destination_guard.as_ref() {
            Some(GuardSyntax::IfDestinationAbsent) => Some(DestinationGuard::MustNotExist),
            Some(GuardSyntax::IfDestinationVersion(token)) => Some(DestinationGuard::Exact(
                FileVersionToken::parse(token).map_err(|_| {
                    GuardError::new(
                        GuardErrorCode::InvalidDestinationGuard,
                        operation_index,
                        "If-Destination is not absent or a canonical version token",
                    )
                })?,
            )),
            Some(GuardSyntax::IfMatch(_)) => {
                return Err(GuardError::new(
                    GuardErrorCode::InvalidDestinationGuard,
                    operation_index,
                    "If-Match cannot occupy If-Destination",
                ));
            }
            None => None,
        };
        let is_move = operation.move_to.is_some();
        match operation.kind {
            OperationKind::Add => {
                if source_guard.is_some() || destination_guard.is_some() || is_move {
                    return Err(GuardError::new(
                        GuardErrorCode::InapplicableGuard,
                        operation_index,
                        "Add File accepts no guards or move destination",
                    ));
                }
            }
            OperationKind::Replace | OperationKind::Delete => {
                if destination_guard.is_some() || is_move {
                    return Err(GuardError::new(
                        GuardErrorCode::InapplicableGuard,
                        operation_index,
                        "operation cannot carry a destination guard",
                    ));
                }
            }
            OperationKind::Update => {
                if !is_move && destination_guard.is_some() {
                    return Err(GuardError::new(
                        GuardErrorCode::InapplicableGuard,
                        operation_index,
                        "If-Destination requires Move to",
                    ));
                }
            }
        }
        operations.push(ValidatedOperation {
            operation,
            source_guard,
            destination_guard,
        });
    }
    Ok(ValidatedPatchDocument {
        schema_version: document.schema_version,
        input_bytes: document.input_bytes,
        payload_hash: document.payload_hash,
        operations,
    })
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
}
