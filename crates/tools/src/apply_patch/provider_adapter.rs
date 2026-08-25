//! Provider-wire normalization and stable Apply Patch result projection.

use crate::apply_patch::file_mutation::{
    PatchError, PatchLimits, PatchRequest, PatchRequestSource,
};
use crate::apply_patch::history::{ApplyPatchOutcome, ChangeKind, PatchSideEffects};
use pioneer_provider::{NATIVE_FILE_TOOL_SCHEMA_VERSION, NativePatchPayload, NativePatchWireShape};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativePatchAdapterError {
    ShapeMismatch,
    UnsupportedShape,
    JsonMustBeObject,
    ExactlyOnePatchProperty,
    UnknownPatchProperty(String),
    PatchPropertyMustBeString,
    Patch(PatchError),
}

impl fmt::Display for NativePatchAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShapeMismatch => {
                f.write_str("native patch payload does not match the selected wire shape")
            }
            Self::UnsupportedShape => {
                f.write_str("native provider has no supported patch wire shape")
            }
            Self::JsonMustBeObject => f.write_str("JSON patch payload must be an object"),
            Self::ExactlyOnePatchProperty => {
                f.write_str("JSON patch payload must contain exactly one `patch` property")
            }
            Self::UnknownPatchProperty(property) => {
                write!(f, "unknown JSON patch property `{property}`")
            }
            Self::PatchPropertyMustBeString => f.write_str("patch property must be a string"),
            Self::Patch(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for NativePatchAdapterError {}

impl From<PatchError> for NativePatchAdapterError {
    fn from(error: PatchError) -> Self {
        Self::Patch(error)
    }
}

/// Normalize a supported native provider shape without shell interpolation,
/// trusted-field injection, or an unbounded intermediate copy.
pub fn normalize_native_patch_payload(
    payload: NativePatchPayload<'_>,
    shape: NativePatchWireShape,
    limits: PatchLimits,
) -> Result<PatchRequest, NativePatchAdapterError> {
    match (shape, payload) {
        (NativePatchWireShape::Unavailable, _) => Err(NativePatchAdapterError::UnsupportedShape),
        (NativePatchWireShape::Freeform, NativePatchPayload::Freeform(patch)) => {
            PatchRequest::from_provider_text(patch, PatchRequestSource::NativeFreeform, limits)
                .map_err(Into::into)
        }
        (NativePatchWireShape::JsonFunction, NativePatchPayload::Json(value)) => {
            let object = value
                .as_object()
                .ok_or(NativePatchAdapterError::JsonMustBeObject)?;
            let patch = strict_patch_property(object)?;
            PatchRequest::from_provider_text(patch, PatchRequestSource::NativeFunction, limits)
                .map_err(Into::into)
        }
        _ => Err(NativePatchAdapterError::ShapeMismatch),
    }
}

fn strict_patch_property(object: &Map<String, JsonValue>) -> Result<&str, NativePatchAdapterError> {
    if object.len() != 1 {
        return Err(NativePatchAdapterError::ExactlyOnePatchProperty);
    }
    let (property, value) = object.iter().next().expect("length checked");
    if property != "patch" {
        return Err(NativePatchAdapterError::UnknownPatchProperty(
            property.clone(),
        ));
    }
    value
        .as_str()
        .ok_or(NativePatchAdapterError::PatchPropertyMustBeString)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativePatchOutcome {
    pub schema_version: u16,
    pub status: String,
    pub success: bool,
    pub exact: bool,
    pub history_bearing: bool,
    pub changed_files: Vec<String>,
    /// Safe per-operation metadata. Snapshot bytes never cross this result
    /// boundary; only paths, kinds, hashes and bounded sizes are exposed.
    pub changes: Vec<NativePatchChange>,
    pub side_effects: PatchSideEffects,
    pub failed_stage: Option<String>,
    pub error: Option<NativePatchError>,
    pub tracking: NativePatchTracking,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativePatchChange {
    pub operation_index: u32,
    pub commit_step: u16,
    pub sequence: u32,
    pub kind: ChangeKind,
    pub source_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overwritten_destination_hash: Option<String>,
    pub before_bytes: Option<u64>,
    pub after_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativePatchError {
    pub code: String,
    pub stage: String,
    pub message: String,
    pub operation_index: Option<u32>,
    pub path: Option<String>,
    pub guard_horizon: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativePatchTrackingStatus {
    RecordedAndProjected,
    RecordedProjectionPending,
    Pending,
    Incomplete,
    NotApplicable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativePatchTracking {
    pub status: NativePatchTrackingStatus,
    pub authority: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_ordinal: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_revision: Option<u64>,
}

impl Default for NativePatchTracking {
    fn default() -> Self {
        Self {
            status: NativePatchTrackingStatus::NotApplicable,
            authority: "untracked".to_owned(),
            record_id: None,
            commit_ordinal: None,
            aggregate_revision: None,
        }
    }
}

pub fn project_apply_patch_outcome(outcome: &ApplyPatchOutcome) -> NativePatchOutcome {
    let delta = outcome.delta();
    let mut changed_files = delta
        .into_iter()
        .flat_map(|delta| delta.changes.iter())
        .flat_map(|change| {
            std::iter::once(change.source_path.clone()).chain(change.destination_path.clone())
        })
        .collect::<Vec<_>>();
    changed_files.sort();
    changed_files.dedup();
    let diagnostic_ref = match outcome {
        ApplyPatchOutcome::Partial { failure, .. }
        | ApplyPatchOutcome::Rejected { failure }
        | ApplyPatchOutcome::Failed { failure, .. } => Some(failure),
        ApplyPatchOutcome::CommitStateUncertain { reason, .. } => Some(reason),
        ApplyPatchOutcome::Applied { .. } => None,
    };
    let changes = delta
        .map(|delta| {
            delta
                .changes
                .iter()
                .map(|change| NativePatchChange {
                    operation_index: change.operation_index,
                    commit_step: change.commit_step,
                    sequence: change.sequence,
                    kind: change.kind,
                    source_path: change.source_path.clone(),
                    destination_path: change.destination_path.clone(),
                    before_hash: change
                        .before
                        .as_ref()
                        .map(|snapshot| snapshot.version.token.to_string()),
                    after_hash: change
                        .after
                        .as_ref()
                        .map(|snapshot| snapshot.version.token.to_string()),
                    overwritten_destination_hash: change
                        .overwritten_destination
                        .as_ref()
                        .map(|snapshot| snapshot.version.token.to_string()),
                    before_bytes: change
                        .before
                        .as_ref()
                        .map(|snapshot| snapshot.bytes.len() as u64),
                    after_bytes: change
                        .after
                        .as_ref()
                        .map(|snapshot| snapshot.bytes.len() as u64),
                })
                .collect()
        })
        .unwrap_or_default();
    NativePatchOutcome {
        schema_version: NATIVE_FILE_TOOL_SCHEMA_VERSION,
        status: outcome.status().to_owned(),
        success: matches!(outcome, ApplyPatchOutcome::Applied { .. }),
        exact: delta.is_none_or(|delta| delta.exact),
        history_bearing: outcome.is_history_bearing(),
        changed_files,
        changes,
        side_effects: delta
            .map(|delta| delta.side_effects.clone())
            .unwrap_or_default(),
        failed_stage: diagnostic_ref.map(|diagnostic| enum_name(diagnostic.stage)),
        error: diagnostic_ref.map(|diagnostic| NativePatchError {
            code: enum_name(diagnostic.code),
            stage: enum_name(diagnostic.stage),
            message: diagnostic.message.clone(),
            operation_index: diagnostic.operation_index,
            path: diagnostic.path.clone(),
            guard_horizon: diagnostic.guard_horizon.map(enum_name),
        }),
        tracking: NativePatchTracking::default(),
    }
}

fn enum_name<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{
        GuardHorizon, PatchDiagnostic, PatchErrorCode, PatchStage, Retryability,
    };

    #[test]
    fn freeform_and_json_normalize_to_identical_requests() {
        let patch = "*** Begin Patch\n*** Add File: a.txt\n+hello\n*** End Patch";
        let limits = PatchLimits::default();
        let freeform = normalize_native_patch_payload(
            NativePatchPayload::Freeform(patch),
            NativePatchWireShape::Freeform,
            limits,
        )
        .unwrap();
        let json = normalize_native_patch_payload(
            NativePatchPayload::Json(&serde_json::json!({"patch": patch})),
            NativePatchWireShape::JsonFunction,
            limits,
        )
        .unwrap();
        assert_eq!(freeform.patch, json.patch);
        assert_ne!(freeform.source, json.source);
    }

    #[test]
    fn strict_json_rejects_injection_fields_and_wrong_types() {
        let limits = PatchLimits::default();
        for value in [
            serde_json::json!({"patch": "patch", "thread_id": "spoof"}),
            serde_json::json!({"patch": 42}),
            serde_json::json!({"command": "cat secret"}),
        ] {
            assert!(
                normalize_native_patch_payload(
                    NativePatchPayload::Json(&value),
                    NativePatchWireShape::JsonFunction,
                    limits,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn strict_json_rejects_removed_input_alias_and_string_wrapper() {
        let limits = PatchLimits::default();
        for value in [
            serde_json::json!({"input": "patch"}),
            serde_json::json!("patch"),
        ] {
            assert!(
                normalize_native_patch_payload(
                    NativePatchPayload::Json(&value),
                    NativePatchWireShape::JsonFunction,
                    limits,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn outcome_has_one_required_shape_without_legacy_or_subprocess_fields() {
        let outcome = ApplyPatchOutcome::Rejected {
            failure: PatchDiagnostic {
                code: PatchErrorCode::PermissionDenied,
                stage: PatchStage::Authorize,
                message: "denied".to_owned(),
                retryability: Retryability::Never,
                operation_index: None,
                path: Some("file.txt".to_owned()),
                guard_horizon: None::<GuardHorizon>,
            },
        };
        let projected = project_apply_patch_outcome(&outcome);
        let value = serde_json::to_value(&projected).unwrap();
        let object = value.as_object().unwrap();

        for required in [
            "schema_version",
            "status",
            "success",
            "exact",
            "history_bearing",
            "changed_files",
            "changes",
            "side_effects",
            "failed_stage",
            "error",
            "tracking",
        ] {
            assert!(object.contains_key(required), "missing field `{required}`");
        }
        for forbidden in ["failure", "stdout", "stderr", "exit_code", "operation"] {
            assert!(
                !object.contains_key(forbidden),
                "legacy/subprocess field `{forbidden}` must not be serialized"
            );
        }

        let mut missing_tracking = value;
        missing_tracking.as_object_mut().unwrap().remove("tracking");
        assert!(serde_json::from_value::<NativePatchOutcome>(missing_tracking).is_err());
    }
}
