use crate::apply_patch::PreparedPatch;
use crate::apply_patch::file_mutation::{CanonicalTarget, TargetManifest, TargetRole};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionEffect {
    Read,
    Mutate,
    CreateParent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PermissionTarget {
    pub path: String,
    pub role: TargetRole,
    pub effect: PermissionEffect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PermissionIntent {
    pub parser_schema_version: u16,
    pub payload_hash: [u8; 32],
    pub plan_fingerprint: [u8; 32],
    pub targets: Vec<PermissionTarget>,
    pub fingerprint: [u8; 32],
}

impl PermissionIntent {
    pub fn from_prepared(prepared: &PreparedPatch) -> Self {
        let targets = permission_targets(&prepared.target_manifest);
        let fingerprint = intent_fingerprint(prepared, &targets);
        Self {
            parser_schema_version: prepared.parser_schema_version,
            payload_hash: prepared.payload_hash,
            plan_fingerprint: prepared.fingerprint,
            targets,
            fingerprint,
        }
    }

    pub fn has_write_effect(&self) -> bool {
        self.targets
            .iter()
            .any(|target| !matches!(target.effect, PermissionEffect::Read))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    FullAccess,
    Supervised,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApprovalReceipt {
    pub mode: PermissionMode,
    pub intent_fingerprint: [u8; 32],
    pub approval_fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorizedPatch {
    pub prepared: PreparedPatch,
    pub intent: PermissionIntent,
    pub receipt: ApprovalReceipt,
}

impl AuthorizedPatch {
    pub fn prepared(&self) -> &PreparedPatch {
        &self.prepared
    }

    pub fn into_prepared(self) -> PreparedPatch {
        self.prepared
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionErrorCode {
    SandboxDenied,
    ApprovalDenied,
    ApprovalBinding,
    InvalidIntent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PermissionError {
    pub code: PermissionErrorCode,
    pub path: Option<String>,
    pub message: String,
}

impl PermissionError {
    pub fn new(
        code: PermissionErrorCode,
        path: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            path,
            message: message.into(),
        }
    }
}

impl fmt::Display for PermissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(path) = &self.path {
            write!(f, "permission error for {path}: {}", self.message)
        } else {
            f.write_str(&self.message)
        }
    }
}

impl std::error::Error for PermissionError {}

pub trait SandboxPolicy: Send + Sync {
    fn check(&self, target: &CanonicalTarget) -> Result<(), PermissionError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllSandbox;

impl SandboxPolicy for AllowAllSandbox {
    fn check(&self, _target: &CanonicalTarget) -> Result<(), PermissionError> {
        Ok(())
    }
}

pub trait PermissionAuthorizer: Send + Sync {
    fn approve(&self, intent: &PermissionIntent) -> Result<ApprovalReceipt, PermissionError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FullAccessAuthorizer;

impl PermissionAuthorizer for FullAccessAuthorizer {
    fn approve(&self, intent: &PermissionIntent) -> Result<ApprovalReceipt, PermissionError> {
        Ok(ApprovalReceipt {
            mode: PermissionMode::FullAccess,
            intent_fingerprint: intent.fingerprint,
            approval_fingerprint: intent.fingerprint,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAuthorizer;

impl PermissionAuthorizer for DenyAuthorizer {
    fn approve(&self, _intent: &PermissionIntent) -> Result<ApprovalReceipt, PermissionError> {
        Err(PermissionError::new(
            PermissionErrorCode::ApprovalDenied,
            None,
            "permission approval denied",
        ))
    }
}

/// Bind one complete prepared plan to one approval decision. The authorizer
/// sees the complete target manifest but has no API to alter operations,
/// guards, payload or versions after preparation.
pub fn authorize<S: SandboxPolicy, A: PermissionAuthorizer>(
    prepared: PreparedPatch,
    sandbox: &S,
    authorizer: &A,
) -> Result<AuthorizedPatch, PermissionError> {
    if prepared.fingerprint == [0; 32] {
        return Err(PermissionError::new(
            PermissionErrorCode::InvalidIntent,
            None,
            "prepared patch has no plan fingerprint",
        ));
    }
    for target in prepared.target_manifest.targets() {
        sandbox.check(target)?;
    }
    let intent = PermissionIntent::from_prepared(&prepared);
    let receipt = authorizer.approve(&intent)?;
    if receipt.intent_fingerprint != intent.fingerprint {
        return Err(PermissionError::new(
            PermissionErrorCode::ApprovalBinding,
            None,
            "approval is bound to a different immutable intent",
        ));
    }
    if receipt.approval_fingerprint != intent.fingerprint {
        return Err(PermissionError::new(
            PermissionErrorCode::ApprovalBinding,
            None,
            "approval fingerprint does not match the immutable intent",
        ));
    }
    Ok(AuthorizedPatch {
        prepared,
        intent,
        receipt,
    })
}

fn permission_targets(manifest: &TargetManifest) -> Vec<PermissionTarget> {
    let mut targets = manifest
        .targets()
        .iter()
        .map(|target| PermissionTarget {
            path: target.relative().to_string_lossy().replace('\\', "/"),
            role: target.role,
            effect: match target.role {
                TargetRole::Parent => PermissionEffect::CreateParent,
                TargetRole::Source | TargetRole::Destination => PermissionEffect::Mutate,
            },
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| (left.role as u8).cmp(&(right.role as u8)))
    });
    targets.dedup();
    targets
}

fn intent_fingerprint(prepared: &PreparedPatch, targets: &[PermissionTarget]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(prepared.parser_schema_version.to_le_bytes());
    hasher.update(prepared.payload_hash);
    hasher.update(prepared.fingerprint);
    for target in targets {
        hasher.update(target.path.as_bytes());
        hasher.update([0]);
        hasher.update([target.role as u8, target.effect as u8]);
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{
        PatchLimits, PatchRequest, PatchRequestSource, TargetResolver,
    };
    use crate::apply_patch::{PrepareOptions, parse, prepare, validate_guards};

    fn prepared(root: &std::path::Path) -> PreparedPatch {
        let request = PatchRequest::from_provider_text(
            "*** Begin Patch\n*** Add File: nested/new.txt\n+new\n*** End Patch",
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        let document = validate_guards(parse(&request, PatchLimits::default()).unwrap()).unwrap();
        prepare(
            &document,
            &TargetResolver::new(root).unwrap(),
            PrepareOptions::default(),
        )
        .unwrap()
    }

    #[test]
    fn full_access_and_supervised_authorization_share_exact_plan() {
        let root = tempfile::tempdir().unwrap();
        let first = prepared(root.path());
        let second = prepared(root.path());
        let left = authorize(first, &AllowAllSandbox, &FullAccessAuthorizer).unwrap();
        let right = authorize(second, &AllowAllSandbox, &FullAccessAuthorizer).unwrap();
        assert_eq!(left.prepared.fingerprint, right.prepared.fingerprint);
        assert_eq!(left.intent.fingerprint, right.intent.fingerprint);
        assert_eq!(left.receipt.mode, PermissionMode::FullAccess);
    }

    #[test]
    fn denial_occurs_after_preflight_without_mutating_disk() {
        let root = tempfile::tempdir().unwrap();
        let error =
            authorize(prepared(root.path()), &AllowAllSandbox, &DenyAuthorizer).unwrap_err();
        assert_eq!(error.code, PermissionErrorCode::ApprovalDenied);
        assert!(!root.path().join("nested/new.txt").exists());
    }

    #[test]
    fn malformed_receipt_cannot_rebind_approval_to_another_plan() {
        struct WrongReceipt;
        impl PermissionAuthorizer for WrongReceipt {
            fn approve(
                &self,
                _intent: &PermissionIntent,
            ) -> Result<ApprovalReceipt, PermissionError> {
                Ok(ApprovalReceipt {
                    mode: PermissionMode::Supervised,
                    intent_fingerprint: [1; 32],
                    approval_fingerprint: [1; 32],
                })
            }
        }
        let root = tempfile::tempdir().unwrap();
        let error = authorize(prepared(root.path()), &AllowAllSandbox, &WrongReceipt).unwrap_err();
        assert_eq!(error.code, PermissionErrorCode::ApprovalBinding);
    }

    #[test]
    fn sandbox_is_checked_for_every_manifest_target() {
        struct RejectNested;
        impl SandboxPolicy for RejectNested {
            fn check(&self, target: &CanonicalTarget) -> Result<(), PermissionError> {
                if target.relative().to_string_lossy().starts_with("nested") {
                    return Err(PermissionError::new(
                        PermissionErrorCode::SandboxDenied,
                        Some(target.relative().to_string_lossy().into_owned()),
                        "sandbox denied target",
                    ));
                }
                Ok(())
            }
        }
        let root = tempfile::tempdir().unwrap();
        let error =
            authorize(prepared(root.path()), &RejectNested, &FullAccessAuthorizer).unwrap_err();
        assert_eq!(error.code, PermissionErrorCode::SandboxDenied);
    }
}
