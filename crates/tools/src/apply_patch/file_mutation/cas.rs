use crate::apply_patch::file_mutation::{
    CanonicalTarget, FileVersionToken, SnapshotErrorCode, SnapshotLimits, TextSnapshot,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CasRole {
    Source,
    Destination,
    MoveDestination,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CasExpectation {
    Exact(FileVersionToken),
    MustNotExist,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CasState {
    Missing,
    Existing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CasErrorCode {
    StaleSource,
    StaleDestination,
    DestinationExists,
    UnsupportedContent,
    ReadFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CasError {
    pub code: CasErrorCode,
    pub role: CasRole,
    pub state: CasState,
}

impl CasError {
    pub const fn new(code: CasErrorCode, role: CasRole, state: CasState) -> Self {
        Self { code, role, state }
    }
}

impl std::fmt::Display for CasError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAS failed: {:?} for {:?}", self.code, self.role)
    }
}

impl std::error::Error for CasError {}

/// Checks an already-read state against an explicit expectation. It never
/// includes the current token in an error, so stale failures cannot become a
/// blind model retry instruction.
pub fn validate_cas(
    expectation: CasExpectation,
    current: Option<FileVersionToken>,
    role: CasRole,
) -> Result<(), CasError> {
    match (expectation, current) {
        (CasExpectation::MustNotExist, None) => Ok(()),
        (CasExpectation::MustNotExist, Some(_)) => Err(CasError::new(
            CasErrorCode::DestinationExists,
            role,
            CasState::Existing,
        )),
        (CasExpectation::Exact(expected), Some(actual)) if expected == actual => Ok(()),
        (CasExpectation::Exact(_), Some(_)) => Err(CasError::new(
            match role {
                CasRole::Source => CasErrorCode::StaleSource,
                CasRole::Destination | CasRole::MoveDestination => CasErrorCode::StaleDestination,
            },
            role,
            CasState::Existing,
        )),
        (CasExpectation::Exact(_), None) => Err(CasError::new(
            match role {
                CasRole::Source => CasErrorCode::StaleSource,
                CasRole::Destination | CasRole::MoveDestination => CasErrorCode::StaleDestination,
            },
            role,
            CasState::Missing,
        )),
    }
}

/// Reads and hashes one regular text target while its caller holds the target
/// lock. The portable guarantee ends at this read/CAS boundary: an
/// uncooperative external process can still write immediately afterwards.
pub fn version_on_disk(
    target: &CanonicalTarget,
    limits: SnapshotLimits,
) -> Result<Option<FileVersionToken>, CasError> {
    let path = target.absolute();
    let metadata = match std::fs::symlink_metadata(target.absolute()) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(CasError::new(
                CasErrorCode::ReadFailed,
                CasRole::Source,
                CasState::Existing,
            ));
        }
    };
    // Never follow a path component that changed into a link or another
    // non-regular target after resolution.  In particular, treating a broken
    // symlink as "missing" would let an Add race an external path swap.
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CasError::new(
            CasErrorCode::UnsupportedContent,
            CasRole::Source,
            CasState::Existing,
        ));
    }
    match TextSnapshot::from_file(path, limits) {
        Ok(snapshot) => Ok(Some(snapshot.version.token)),
        Err(error) => Err(CasError::new(
            match error.code {
                SnapshotErrorCode::BinaryContent
                | SnapshotErrorCode::InvalidUtf8
                | SnapshotErrorCode::TooLarge => CasErrorCode::UnsupportedContent,
                SnapshotErrorCode::InvalidLimits
                | SnapshotErrorCode::Io
                | SnapshotErrorCode::SpoolUnavailable
                | SnapshotErrorCode::SpoolCorrupt => CasErrorCode::ReadFailed,
            },
            CasRole::Source,
            CasState::Existing,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{TargetExpectation, TargetResolver, TargetRole};
    use std::fs;

    #[test]
    fn cas_distinguishes_source_and_destination_failures_without_token_leak() {
        let expected = FileVersionToken::from_bytes(b"old");
        let actual = FileVersionToken::from_bytes(b"new");
        let source = validate_cas(
            CasExpectation::Exact(expected),
            Some(actual),
            CasRole::Source,
        )
        .unwrap_err();
        assert_eq!(source.code, CasErrorCode::StaleSource);
        let destination = validate_cas(
            CasExpectation::MustNotExist,
            Some(actual),
            CasRole::Destination,
        )
        .unwrap_err();
        assert_eq!(destination.code, CasErrorCode::DestinationExists);
        let encoded = serde_json::to_string(&source).unwrap();
        assert!(!encoded.contains(&actual.to_string()));
    }

    #[test]
    fn version_on_disk_detects_change_with_equal_shape() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        fs::write(&path, b"old\n").unwrap();
        let resolver = TargetResolver::new(root.path()).unwrap();
        let target = resolver
            .resolve(
                "file.txt",
                TargetRole::Source,
                TargetExpectation::ExistingRegular,
            )
            .unwrap();
        let first = version_on_disk(&target, SnapshotLimits::default()).unwrap();
        fs::write(&path, b"new\n").unwrap();
        let second = version_on_disk(&target, SnapshotLimits::default()).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn missing_path_satisfies_must_not_exist() {
        let role = CasRole::MoveDestination;
        assert!(validate_cas(CasExpectation::MustNotExist, None, role).is_ok());
    }
}
