//! Provider-wire normalization and stable Apply Patch result projection.

use crate::apply_patch::file_mutation::{
    PatchDiagnostic, PatchError, PatchErrorCode, PatchLimits, PatchRequest, PatchRequestSource,
};
use crate::apply_patch::history::{ApplyPatchOutcome, ChangeKind, PatchSideEffects};
use crate::apply_patch::{ExecutionReport, ValidationFailure};
use pioneer_provider::{NATIVE_FILE_TOOL_SCHEMA_VERSION, NativePatchPayload, NativePatchWireShape};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use std::fmt;
use std::path::{Component, Path, PathBuf};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<NativePatchValidation>,
    pub tracking: NativePatchTracking,
}

impl NativePatchOutcome {
    /// Present paths to the model without requiring it to know the private
    /// execution root selected for a single- or multi-root patch. Internal
    /// history remains paired with that root; the tool result is deliberately
    /// unambiguous and directly reusable by read/list/apply_patch.
    pub fn make_paths_absolute(&mut self, execution_root: &Path) {
        for path in &mut self.changed_files {
            *path = absolute_display_path(execution_root, path);
        }
        self.changed_files.sort();
        self.changed_files.dedup();

        for change in &mut self.changes {
            change.source_path = absolute_display_path(execution_root, &change.source_path);
            if let Some(destination) = &mut change.destination_path {
                *destination = absolute_display_path(execution_root, destination);
            }
        }

        if let Some(error) = self.error.as_mut()
            && let Some(old_path) = error.path.clone()
        {
            let absolute_path = absolute_display_path(execution_root, old_path.as_str());
            if old_path != absolute_path {
                error.message = error
                    .message
                    .replace(old_path.as_str(), absolute_path.as_str());
                error.next_action = error
                    .next_action
                    .replace(old_path.as_str(), absolute_path.as_str());
            }
            error.path = Some(absolute_path);
            if error.path.as_ref().is_some_and(|path| {
                serde_json::to_vec(path)
                    .map_or(true, |bytes| bytes.len() > MAX_VALIDATION_DISPLAY_BYTES / 4)
            }) {
                error.path = None;
                error.path_truncated = true;
            }
        }
        if let Some(validation) = self.validation.as_mut() {
            for diagnostic in &mut validation.diagnostics {
                if let Some(path) = diagnostic.path.as_mut() {
                    *path = absolute_display_path(execution_root, path);
                }
            }
            bound_validation_paths(validation);
        }
    }
}

fn absolute_display_path(execution_root: &Path, value: &str) -> String {
    let path = Path::new(value);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        execution_root.join(path)
    };
    lexical_normalize(&absolute)
        .to_string_lossy()
        .replace('\\', "/")
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
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
    #[serde(default, skip_serializing_if = "is_false")]
    pub path_truncated: bool,
    pub guard_horizon: Option<String>,
    pub retryability: String,
    pub next_action: String,
    pub retry_same_patch: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativePatchValidation {
    pub stage: String,
    pub syntax_complete: bool,
    pub later_stages_checked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unchecked_from_line: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unverified_ranges: Vec<NativePatchLineRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    /// Known only when the syntax scan completed. Later pipeline stages have
    /// not run for a syntax rejection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_violations: Option<usize>,
    pub observed_violations: usize,
    pub shown_violations: usize,
    pub display_truncated: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub details_truncated: bool,
    pub diagnostics: Vec<NativePatchDiagnosticGroup>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativePatchDiagnosticGroup {
    pub code: String,
    pub column: usize,
    pub operation_index: Option<u32>,
    pub operation: Option<String>,
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub path_truncated: bool,
    pub message: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub message_truncated: bool,
    pub lines: Vec<NativePatchLineRange>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativePatchLineRange {
    pub start: usize,
    pub end: usize,
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
            path_truncated: false,
            guard_horizon: diagnostic.guard_horizon.map(enum_name),
            retryability: enum_name(diagnostic.retryability),
            next_action: next_action_for(diagnostic),
            retry_same_patch: false,
        }),
        validation: None,
        tracking: NativePatchTracking::default(),
    }
}

/// Project the canonical report before its legacy single-failure outcome is
/// consumed. The validation collection is authoritative; `error` is its
/// first-item compatibility view.
pub fn project_execution_report(mut report: ExecutionReport) -> NativePatchOutcome {
    let validation_failure = report.validation.take();
    let mut projected = project_apply_patch_outcome(&report.into_outcome());
    if let Some(failure) = &validation_failure {
        let validation = render_validation(failure);
        if let Some(error) = projected.error.as_mut() {
            let (operation_index, path) = match failure {
                ValidationFailure::Syntax(failure) => (
                    failure.first().operation_index,
                    failure.first().path.clone(),
                ),
                ValidationFailure::Guards(failure) => {
                    let first = &failure.diagnostics[0];
                    (
                        Some(first.operation_index.try_into().unwrap_or(u32::MAX)),
                        first.path.clone(),
                    )
                }
                ValidationFailure::SyntaxAndGuards { syntax, guards } => {
                    if guards.diagnostics[0].line < syntax.first().line {
                        let first = &guards.diagnostics[0];
                        (
                            Some(first.operation_index.try_into().unwrap_or(u32::MAX)),
                            first.path.clone(),
                        )
                    } else {
                        (syntax.first().operation_index, syntax.first().path.clone())
                    }
                }
            };
            error.operation_index = operation_index;
            error.path_truncated = path.as_ref().is_some_and(|path| {
                serde_json::to_vec(path)
                    .map_or(true, |bytes| bytes.len() > MAX_VALIDATION_DISPLAY_BYTES / 4)
            });
            error.path = if error.path_truncated { None } else { path };
            error.next_action = if !validation.syntax_complete {
                "Correct the listed syntax issues and inspect the unverified patch lines or reported limit. Later stages were not checked; submit a new patch.".to_owned()
            } else if validation.display_truncated {
                "Correct the listed issues; the display is truncated, so inspect the full patch for additional violations. Submit a new patch. Later stages were not checked.".to_owned()
            } else {
                "Correct every listed issue and submit a new patch. Later stages were not checked."
                    .to_owned()
            };
        }
        projected.validation = Some(validation);
    }
    projected
}

const MAX_VALIDATION_DISPLAY_BYTES: usize = 8 * 1024;

fn bound_validation_paths(validation: &mut NativePatchValidation) {
    while serde_json::to_vec(&validation.diagnostics).map_or(usize::MAX, |bytes| bytes.len())
        > MAX_VALIDATION_DISPLAY_BYTES
    {
        if let Some(group) = validation
            .diagnostics
            .iter_mut()
            .filter(|group| group.path.is_some())
            .max_by_key(|group| group.path.as_ref().map_or(0, |path| path.len()))
        {
            group.path = None;
            group.path_truncated = true;
            validation.details_truncated = true;
            continue;
        }
        let Some(last) = validation.diagnostics.last_mut() else {
            break;
        };
        if last.lines.len() > 1 {
            let range = last.lines.pop().expect("nonempty lines");
            validation.shown_violations -= range.end - range.start + 1;
        } else if validation.diagnostics.len() > 1 {
            let group = validation.diagnostics.pop().expect("nonempty diagnostics");
            validation.shown_violations -= group
                .lines
                .iter()
                .map(|range| range.end - range.start + 1)
                .sum::<usize>();
        } else {
            break;
        }
        validation.display_truncated = true;
    }
}

fn render_validation(failure: &ValidationFailure) -> NativePatchValidation {
    let mut grouped: Vec<NativePatchDiagnosticGroup> = Vec::new();
    let (syntax, guards, stage) = match failure {
        ValidationFailure::Syntax(syntax) => (Some(syntax), None, "syntax"),
        ValidationFailure::Guards(guards) => (None, Some(guards), "guards"),
        ValidationFailure::SyntaxAndGuards { syntax, guards } => {
            (Some(syntax), Some(guards), "syntax_and_guards")
        }
    };
    if let Some(syntax) = syntax {
        for diagnostic in &syntax.diagnostics {
            append_group(
                &mut grouped,
                enum_name(diagnostic.code),
                diagnostic.operation_index,
                diagnostic.operation.map(enum_name),
                diagnostic.path.clone(),
                diagnostic.message.clone(),
                diagnostic.line,
                diagnostic.column,
            );
        }
    }
    if let Some(guards) = guards {
        for diagnostic in &guards.diagnostics {
            append_group(
                &mut grouped,
                enum_name(diagnostic.code),
                Some(diagnostic.operation_index.try_into().unwrap_or(u32::MAX)),
                Some(enum_name(diagnostic.operation)),
                diagnostic.path.clone(),
                diagnostic.message.clone(),
                diagnostic.line,
                1,
            );
        }
    }
    grouped.sort_by_key(|group| group.lines[0].start);
    let syntax_complete = syntax.is_none_or(|syntax| {
        syntax.unchecked_from_line.is_none() && syntax.unverified_ranges.is_empty()
    });
    let unchecked_from_line = syntax.and_then(|syntax| syntax.unchecked_from_line);
    let unverified_ranges = syntax.map_or_else(Vec::new, |syntax| {
        syntax
            .unverified_ranges
            .iter()
            .map(|range| NativePatchLineRange {
                start: range.start,
                end: range.end,
            })
            .collect()
    });
    let stop_reason = syntax.and_then(|syntax| syntax.stop_reason.map(enum_name));
    let observed_violations = syntax.map_or(0, |syntax| syntax.diagnostics.len())
        + guards.map_or(0, |guards| guards.diagnostics.len());

    let mut diagnostics: Vec<NativePatchDiagnosticGroup> = Vec::new();
    let mut used_bytes = 0usize;
    let mut shown_violations = 0usize;
    let mut details_truncated = false;
    'groups: for group in grouped {
        let mut visible = NativePatchDiagnosticGroup {
            lines: Vec::new(),
            ..group.clone()
        };
        for range in group.lines {
            let before = serde_json::to_vec(&visible).map_or(0, |json| json.len());
            visible.lines.push(range);
            let mut after = serde_json::to_vec(&visible).map_or(usize::MAX, |json| json.len());
            if used_bytes.saturating_add(if visible.lines.len() == 1 {
                after
            } else {
                after.saturating_sub(before)
            }) > MAX_VALIDATION_DISPLAY_BYTES
                && visible.lines.len() == 1
            {
                // Keep the code, position and operation identity even when an
                // escaped path or description would consume the whole budget.
                if visible.path.take().is_some() {
                    visible.path_truncated = true;
                    details_truncated = true;
                    after = serde_json::to_vec(&visible).map_or(usize::MAX, |json| json.len());
                }
                if used_bytes.saturating_add(after) > MAX_VALIDATION_DISPLAY_BYTES {
                    visible.message = "Description omitted to fit response".to_owned();
                    visible.message_truncated = true;
                    details_truncated = true;
                    after = serde_json::to_vec(&visible).map_or(usize::MAX, |json| json.len());
                }
            }
            let extra = if visible.lines.len() == 1 {
                after
            } else {
                after.saturating_sub(before)
            };
            if used_bytes.saturating_add(extra) > MAX_VALIDATION_DISPLAY_BYTES {
                visible.lines.pop();
                if !visible.lines.is_empty() {
                    diagnostics.push(visible);
                }
                break 'groups;
            }
            used_bytes += extra;
            shown_violations += range.end - range.start + 1;
        }
        diagnostics.push(visible);
    }
    let mut validation = NativePatchValidation {
        stage: stage.to_owned(),
        syntax_complete,
        later_stages_checked: false,
        unchecked_from_line,
        unverified_ranges,
        stop_reason,
        total_violations: syntax_complete.then_some(observed_violations),
        observed_violations,
        shown_violations,
        display_truncated: shown_violations < observed_violations,
        details_truncated,
        diagnostics,
    };
    bound_validation_paths(&mut validation);
    validation
}

fn append_group(
    grouped: &mut Vec<NativePatchDiagnosticGroup>,
    code: String,
    operation_index: Option<u32>,
    operation: Option<String>,
    path: Option<String>,
    message: String,
    line: usize,
    column: usize,
) {
    if let Some(group) = grouped.iter_mut().find(|group| {
        group.code == code
            && group.column == column
            && group.operation_index == operation_index
            && group.operation == operation
            && group.path == path
            && group.message == message
    }) {
        if let Some(last) = group.lines.last_mut()
            && last.end.checked_add(1) == Some(line)
        {
            last.end = line;
        } else {
            group.lines.push(NativePatchLineRange {
                start: line,
                end: line,
            });
        }
    } else {
        grouped.push(NativePatchDiagnosticGroup {
            code,
            column,
            operation_index,
            operation,
            path,
            path_truncated: false,
            message,
            message_truncated: false,
            lines: vec![NativePatchLineRange {
                start: line,
                end: line,
            }],
        });
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn next_action_for(diagnostic: &PatchDiagnostic) -> String {
    let path = diagnostic
        .path
        .as_deref()
        .map(|path| format!("`{path}`"))
        .unwrap_or_else(|| "the reported path".to_owned());
    let parent = diagnostic
        .path
        .as_deref()
        .and_then(|path| Path::new(path).parent())
        .map(|path| format!("`{}`", path.display()))
        .unwrap_or_else(|| "its nearest existing parent".to_owned());
    match diagnostic.code {
        PatchErrorCode::PatchSyntaxError
        | PatchErrorCode::PatchEmpty
        | PatchErrorCode::InvalidPayload
        | PatchErrorCode::InvalidRequest => {
            "Submit a new patch from Begin Patch through End Patch. Use Add File with + lines, Update File with @@ context hunks, Update File followed by Move to with no hunk for a pure rename, or Delete File. Do not repeat the invalid patch.".to_owned()
        }
        PatchErrorCode::InvalidVersionToken | PatchErrorCode::PreconditionRequired => {
            "Remove If-Match and If-Destination directives and use the ordinary Add/Update/Move/Delete syntax; concurrency checks are automatic.".to_owned()
        }
        PatchErrorCode::InvalidPath
        | PatchErrorCode::PathOutsideAllowedRoot
        | PatchErrorCode::UnauthorizedPath
        | PatchErrorCode::PermissionDenied => {
            format!("The input resolved to {path}. Use the current working directory and writable roots shown in the error: pass a path relative to that cwd or an authorized absolute path.")
        }
        PatchErrorCode::ContextNotFound | PatchErrorCode::AmbiguousContext => {
            format!("Read {path} again, then build a new Update hunk with enough unchanged context to match exactly one location.")
        }
        PatchErrorCode::SourceMissing => {
            format!("Call list_dir for {parent}, select the exact returned absolute source path for {path}, and submit a new patch.")
        }
        PatchErrorCode::DestinationExists => {
            format!("The move destination {path} already exists. Choose a different destination, or explicitly delete that file before moving; Move never overwrites implicitly.")
        }
        PatchErrorCode::DestinationMissing => {
            format!("Call list_dir for {parent}, correct the destination {path}, and submit a new patch.")
        }
        PatchErrorCode::StaleFile => {
            format!("Read {path} again and construct a new patch from its current contents.")
        }
        PatchErrorCode::IoCreateFailed
        | PatchErrorCode::IoWriteFailed
        | PatchErrorCode::IoSyncFailed
        | PatchErrorCode::IoRenameFailed
        | PatchErrorCode::IoDeleteFailed
        | PatchErrorCode::Io => {
            format!("Inspect the operating-system cause reported for {path}. Correct that exact path, ancestor permissions, disk state, or conflicting file, then submit a new patch.")
        }
        PatchErrorCode::UnsupportedFileType
        | PatchErrorCode::InvalidUtf8
        | PatchErrorCode::UnsupportedContent
        | PatchErrorCode::FileTooLarge => {
            format!("{path} is not a supported bounded regular UTF-8 text target. Use an appropriate non-text or large-file workflow instead of retrying this patch.")
        }
        PatchErrorCode::LockTimeout | PatchErrorCode::HistoryCapacity => {
            "Wait briefly and retry only after the transient contention or history-capacity condition has cleared.".to_owned()
        }
        PatchErrorCode::PartialCommit
        | PatchErrorCode::CommitStateUncertain
        | PatchErrorCode::TrackerPublishFailed => {
            "Do not retry automatically. Inspect changed_files and the workspace first, then make only the remaining changes in a new patch.".to_owned()
        }
        PatchErrorCode::CrossDeviceMove => {
            format!("The move involving {path} crosses filesystems. Move within one filesystem, or create the destination and delete the source as separate explicit operations.")
        }
        PatchErrorCode::InvalidLimits
        | PatchErrorCode::InputTooLarge
        | PatchErrorCode::TooManyOperations
        | PatchErrorCode::TooManyFiles
        | PatchErrorCode::TooManyHunks => {
            "Split the work into smaller valid patches and submit the first one.".to_owned()
        }
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
    use crate::apply_patch::{
        ExecutionReport, parse_validated, validate_guard_candidates, validate_guards_all,
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
        let mut projected = project_apply_patch_outcome(&outcome);
        assert!(
            projected
                .error
                .as_ref()
                .unwrap()
                .next_action
                .contains("`file.txt`")
        );
        projected.make_paths_absolute(Path::new("/workspace"));
        let projected_error = projected.error.as_ref().unwrap();
        assert_eq!(projected_error.path.as_deref(), Some("/workspace/file.txt"));
        assert!(
            projected_error
                .next_action
                .contains("`/workspace/file.txt`")
        );
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

    fn rejected_text(text: &str) -> NativePatchOutcome {
        let request = PatchRequest::from_provider_text(
            text,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        let failure = parse_validated(&request, PatchLimits::default()).unwrap_err();
        project_execution_report(ExecutionReport::rejected_parse_failure(&failure))
    }

    #[test]
    fn projection_groups_adjacent_lines_and_keeps_all_small_diagnostics() {
        let projected = rejected_text(
            "*** Begin Patch\n*** Add File: a.txt\nwrong\nalso wrong\n+valid\nwrong again\n*** Add File: b.txt\nwrong\n*** End Patch",
        );
        let validation = projected.validation.unwrap();
        assert_eq!(validation.stage, "syntax");
        assert!(validation.syntax_complete);
        assert!(!validation.later_stages_checked);
        assert_eq!(validation.total_violations, Some(4));
        assert_eq!(validation.shown_violations, 4);
        assert!(!validation.display_truncated);
        assert_eq!(validation.diagnostics.len(), 2);
        assert_eq!(
            validation.diagnostics[0].lines,
            vec![
                NativePatchLineRange { start: 3, end: 4 },
                NativePatchLineRange { start: 6, end: 6 },
            ]
        );
        assert_eq!(validation.diagnostics[1].path.as_deref(), Some("b.txt"));
    }

    #[test]
    fn completed_scan_with_short_display_differs_from_stopped_scan() {
        let mut patch = String::from("*** Begin Patch\n*** Add File: a.txt\n");
        for _ in 0..600 {
            patch.push_str("wrong\n+valid\n");
        }
        patch.push_str("*** End Patch");
        let projected = rejected_text(&patch);
        let validation = projected.validation.unwrap();
        assert!(validation.syntax_complete);
        assert_eq!(validation.total_violations, Some(600));
        assert!(validation.display_truncated);
        assert!(validation.shown_violations < 600);
        assert_eq!(validation.stop_reason, None);

        let stopped = rejected_text(
            "*** Begin Patch\n*** Add File: a.txt\nwrong\n*** Unknown: x\n*** End Patch",
        );
        let validation = stopped.validation.unwrap();
        assert!(!validation.syntax_complete);
        assert_eq!(validation.total_violations, None);
        assert_eq!(
            validation.stop_reason.as_deref(),
            Some("ambiguous_structure")
        );
        assert_eq!(validation.unchecked_from_line, Some(4));
        assert!(!validation.display_truncated);
    }

    #[test]
    fn guard_diagnostics_reach_the_same_model_projection() {
        let request = PatchRequest::from_provider_text(
            "*** Begin Patch\n*** Delete File: a.txt\n*** If-Match: bad\n*** Delete File: b.txt\n*** If-Match: bad\n*** End Patch",
            PatchRequestSource::NativeFreeform, PatchLimits::default(),
        ).unwrap();
        let document = parse_validated(&request, PatchLimits::default()).unwrap();
        let failure = validate_guards_all(document).unwrap_err();
        let projected = project_execution_report(ExecutionReport::rejected_guard_failure(&failure));
        let value = serde_json::to_value(&projected).unwrap();
        assert_eq!(value["validation"]["stage"], "guards");
        assert_eq!(value["validation"]["total_violations"], 2);
        assert_eq!(
            value["validation"]["diagnostics"][0]["code"],
            "invalid_source_guard"
        );
        assert_eq!(value["validation"]["diagnostics"][1]["path"], "b.txt");
        assert_eq!(value["error"]["code"], "invalid_version_token");
    }

    #[test]
    fn mixed_validation_uses_earliest_position_for_compatibility_error() {
        let request = PatchRequest::from_provider_text(
            "*** Begin Patch\n*** Delete File: a.txt\n*** If-Match: bad\n*** Add File: b.txt\nwrong\n*** End Patch",
            PatchRequestSource::NativeFreeform, PatchLimits::default(),
        ).unwrap();
        let syntax = parse_validated(&request, PatchLimits::default()).unwrap_err();
        let guards = validate_guard_candidates(&syntax.guard_candidates);
        let projected = project_execution_report(ExecutionReport::rejected_syntax_and_guards(
            &syntax, &guards,
        ));
        assert_eq!(
            projected.error.as_ref().unwrap().code,
            "invalid_version_token"
        );
        let validation = projected.validation.unwrap();
        assert_eq!(validation.stage, "syntax_and_guards");
        assert_eq!(validation.total_violations, Some(2));
        assert_eq!(validation.diagnostics[0].code, "invalid_source_guard");
        assert_eq!(validation.diagnostics[1].code, "missing_add_prefix");
    }

    #[test]
    fn resource_stop_never_claims_observed_examples_are_the_total() {
        let limits = PatchLimits {
            max_total_hunks: 1,
            ..PatchLimits::default()
        };
        let request = PatchRequest::from_provider_text(
            "*** Begin Patch\n*** Add File: a.txt\nwrong\nalso wrong\n*** End Patch",
            PatchRequestSource::NativeFreeform,
            limits,
        )
        .unwrap();
        let failure = parse_validated(&request, limits).unwrap_err();
        let value = serde_json::to_value(project_execution_report(
            ExecutionReport::rejected_parse_failure(&failure),
        ))
        .unwrap();
        assert_eq!(value["validation"]["stop_reason"], "resource_limit");
        assert!(value["validation"].get("total_violations").is_none());
        assert_eq!(value["validation"]["observed_violations"], 1);
    }

    #[test]
    fn invalid_path_body_is_checked_without_claiming_later_stages() {
        let projected = rejected_text(
            "*** Begin Patch\n*** Add File:\nwrong\n*** Add File: next.txt\nwrong\n*** End Patch",
        );
        let validation = projected.validation.unwrap();
        assert!(validation.syntax_complete);
        assert_eq!(validation.unchecked_from_line, None);
        assert!(validation.unverified_ranges.is_empty());
        assert_eq!(validation.total_violations, Some(3));
        assert_eq!(validation.observed_violations, 3);
        assert!(!validation.later_stages_checked);
    }

    #[test]
    fn escaped_path_cannot_hide_the_only_concrete_diagnostic() {
        let path = "\"".repeat(PatchLimits::default().max_path_bytes as usize);
        let patch = format!("*** Begin Patch\n*** Add File: {path}\nwrong\n*** End Patch");
        let projected = rejected_text(&patch);
        let validation = projected.validation.as_ref().unwrap();
        assert_eq!(validation.total_violations, Some(1));
        assert_eq!(validation.shown_violations, 1);
        assert!(!validation.display_truncated);
        assert!(validation.details_truncated);
        assert_eq!(validation.diagnostics[0].code, "missing_add_prefix");
        assert_eq!(validation.diagnostics[0].operation_index, Some(0));
        assert_eq!(validation.diagnostics[0].operation.as_deref(), Some("add"));
        assert_eq!(
            validation.diagnostics[0].lines[0],
            NativePatchLineRange { start: 3, end: 3 }
        );
        assert!(validation.diagnostics[0].path.is_none());
        assert!(validation.diagnostics[0].path_truncated);
        assert!(projected.error.as_ref().unwrap().path_truncated);
        assert!(projected.error.as_ref().unwrap().path.is_none());
        assert!(
            serde_json::to_vec(&validation.diagnostics).unwrap().len()
                <= MAX_VALIDATION_DISPLAY_BYTES
        );
    }
}
