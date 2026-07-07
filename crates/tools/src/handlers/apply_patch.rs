use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use crate::{FilePolicyChecker, FilePolicyDecision, FilePolicyOperation};
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use tokio::process::Command;

const BEGIN_PATCH: &str = "*** Begin Patch";
const END_PATCH: &str = "*** End Patch";
const ADD_FILE: &str = "*** Add File: ";
const DELETE_FILE: &str = "*** Delete File: ";
const UPDATE_FILE: &str = "*** Update File: ";
const MOVE_TO: &str = "*** Move to: ";
const END_OF_FILE: &str = "*** End of File";

pub struct ApplyPatchHandler;

#[async_trait]
impl ToolHandler for ApplyPatchHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let patch = extract_patch(invocation.payload)?;
        let validation = validate_patch_document_with_targets(patch.as_str())?;
        enforce_patch_targets(
            invocation.execution_security_snapshot.as_ref(),
            invocation.workdir.as_path(),
            validation.targets.as_slice(),
        )?;
        let changed_files = validation.changed_files;

        let output = Command::new("apply_patch")
            .arg(patch)
            .current_dir(invocation.workdir.as_path())
            .output()
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to run apply_patch binary: {error}"))
            })?;

        let exit_code = output.status.code().unwrap_or_default();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();

        let payload = serde_json::json!({
            "status": if output.status.success() { "applied" } else { "failed" },
            "changed_files": changed_files,
            "exit_code": exit_code,
            "stdout": (!stdout.is_empty()).then_some(stdout),
            "stderr": (!stderr.is_empty()).then_some(stderr),
        });
        let body = serde_json::to_string_pretty(&payload).map_err(|error| {
            ToolError::internal(format!("failed to serialize apply_patch result: {error}"))
        })?;

        Ok(Box::new(FunctionToolOutput::with_payload(
            body,
            output.status.success(),
            payload,
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PatchDocumentValidation {
    changed_files: Vec<String>,
    targets: Vec<PatchTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PatchTarget {
    operation: FilePolicyOperation,
    path: String,
}

fn extract_patch(payload: ToolPayload) -> Result<String, ToolError> {
    extract_patch_input(&payload).map(str::to_owned)
}

pub(crate) fn extract_patch_input(payload: &ToolPayload) -> Result<&str, ToolError> {
    match payload {
        ToolPayload::Custom { input } => Ok(input.as_str()),
        ToolPayload::Function { arguments } => extract_patch_from_json(arguments),
        other => Err(ToolError::invalid_arguments(format!(
            "unsupported apply_patch payload: {}",
            other.log_payload()
        ))),
    }
}

fn extract_patch_from_json(value: &JsonValue) -> Result<&str, ToolError> {
    if let Some(input) = value.get("input").and_then(JsonValue::as_str) {
        return Ok(input);
    }
    if let Some(input) = value.get("patch").and_then(JsonValue::as_str) {
        return Ok(input);
    }
    if let Some(input) = value.as_str() {
        return Ok(input);
    }

    Err(ToolError::invalid_arguments(
        "apply_patch expects `input` or `patch` string",
    ))
}

pub(crate) fn validate_patch_document(patch: &str) -> Result<Vec<String>, ToolError> {
    validate_patch_document_with_targets(patch).map(|validation| validation.changed_files)
}

fn validate_patch_document_with_targets(patch: &str) -> Result<PatchDocumentValidation, ToolError> {
    let lines = patch.lines().collect::<Vec<_>>();
    if lines.first().copied() != Some(BEGIN_PATCH) {
        return Err(ToolError::invalid_arguments(
            "patch must start with `*** Begin Patch`",
        ));
    }

    let mut idx = 1usize;
    let mut saw_hunk = false;
    let mut saw_end = false;
    let mut changed = BTreeSet::new();
    let mut targets = Vec::new();

    while idx < lines.len() {
        let line = lines[idx];

        if line == END_PATCH {
            saw_end = true;
            idx = idx.saturating_add(1);
            break;
        }

        if let Some(path) = line.strip_prefix(ADD_FILE) {
            let path = path.trim();
            validate_patch_path(path)?;
            changed.insert(path.to_owned());
            targets.push(PatchTarget {
                operation: FilePolicyOperation::Write,
                path: path.to_owned(),
            });
            saw_hunk = true;
            idx = idx.saturating_add(1);

            let mut saw_add_line = false;
            while idx < lines.len() {
                let current = lines[idx];
                if current == END_PATCH || is_hunk_header(current) {
                    break;
                }
                if !current.starts_with('+') {
                    return Err(ToolError::invalid_arguments(
                        "add-file hunk must contain only `+` lines",
                    ));
                }
                saw_add_line = true;
                idx = idx.saturating_add(1);
            }

            if !saw_add_line {
                return Err(ToolError::invalid_arguments(
                    "add-file hunk must include at least one `+` line",
                ));
            }

            continue;
        }

        if let Some(path) = line.strip_prefix(DELETE_FILE) {
            let path = path.trim();
            validate_patch_path(path)?;
            changed.insert(path.to_owned());
            targets.push(PatchTarget {
                operation: FilePolicyOperation::Write,
                path: path.to_owned(),
            });
            saw_hunk = true;
            idx = idx.saturating_add(1);
            continue;
        }

        if let Some(path) = line.strip_prefix(UPDATE_FILE) {
            let path = path.trim();
            validate_patch_path(path)?;
            changed.insert(path.to_owned());
            targets.push(PatchTarget {
                operation: FilePolicyOperation::Write,
                path: path.to_owned(),
            });
            saw_hunk = true;
            idx = idx.saturating_add(1);

            if idx < lines.len()
                && let Some(moved_path) = lines[idx].strip_prefix(MOVE_TO)
            {
                let moved_path = moved_path.trim();
                validate_patch_path(moved_path)?;
                changed.insert(moved_path.to_owned());
                targets.push(PatchTarget {
                    operation: FilePolicyOperation::Write,
                    path: moved_path.to_owned(),
                });
                idx = idx.saturating_add(1);
            }

            let mut saw_change = false;
            while idx < lines.len() {
                let current = lines[idx];
                if current == END_PATCH || is_hunk_header(current) {
                    break;
                }
                if current == END_OF_FILE
                    || current.starts_with("@@")
                    || current.starts_with('+')
                    || current.starts_with('-')
                    || current.starts_with(' ')
                {
                    saw_change = true;
                    idx = idx.saturating_add(1);
                    continue;
                }

                return Err(ToolError::invalid_arguments(format!(
                    "invalid update hunk line: `{current}`"
                )));
            }

            if !saw_change {
                return Err(ToolError::invalid_arguments(
                    "update-file hunk must include at least one change line",
                ));
            }

            continue;
        }

        return Err(ToolError::invalid_arguments(format!(
            "unexpected patch line: `{line}`"
        )));
    }

    if !saw_hunk {
        return Err(ToolError::invalid_arguments(
            "patch must include at least one hunk",
        ));
    }

    if !saw_end {
        return Err(ToolError::invalid_arguments(
            "patch must end with `*** End Patch`",
        ));
    }

    if idx < lines.len() && lines[idx..].iter().any(|line| !line.trim().is_empty()) {
        return Err(ToolError::invalid_arguments(
            "patch contains non-empty trailing lines after `*** End Patch`",
        ));
    }

    Ok(PatchDocumentValidation {
        changed_files: changed.into_iter().collect(),
        targets,
    })
}

fn enforce_patch_targets(
    snapshot: Option<&pioneer_protocol::TurnExecutionSecuritySnapshot>,
    workdir: &Path,
    targets: &[PatchTarget],
) -> Result<(), ToolError> {
    let Some(snapshot) = snapshot else {
        return Ok(());
    };

    for target in targets {
        let requested_path = resolve_patch_target_path(workdir, target.path.as_str());
        match FilePolicyChecker::check(snapshot, target.operation, requested_path.as_path()) {
            FilePolicyDecision::Allowed(_) => {}
            FilePolicyDecision::Denied(deny) => {
                return Err(ToolError::Rejected(format!(
                    "filesystem sandbox denied {:?} for patch target `{}`: {}",
                    deny.operation,
                    deny.requested_path.display(),
                    deny.message
                )));
            }
        }
    }
    Ok(())
}

fn resolve_patch_target_path(workdir: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    let path = if path.is_absolute() {
        path
    } else {
        workdir.join(path)
    };
    normalize_path_lexically(path)
}

fn normalize_path_lexically(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn is_hunk_header(line: &str) -> bool {
    line.starts_with(ADD_FILE) || line.starts_with(DELETE_FILE) || line.starts_with(UPDATE_FILE)
}

fn validate_patch_path(path: &str) -> Result<(), ToolError> {
    if path.is_empty() {
        return Err(ToolError::invalid_arguments("patch path must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ToolCallSource;
    use crate::events::ToolEventBus;
    use crate::spec::{ToolPermissionMetadata, ToolRecoveryMetadata};
    use pioneer_protocol::{
        TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
        TurnPermissionMode, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
    };
    use tokio_util::sync::CancellationToken;

    fn workspace_write_security_snapshot(root: &Path) -> TurnExecutionSecuritySnapshot {
        TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            root.to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                root.to_string_lossy(),
            )],
            1,
        )
    }

    fn apply_patch_invocation(
        workdir: PathBuf,
        patch: &str,
        snapshot: Option<TurnExecutionSecuritySnapshot>,
    ) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_apply_patch".to_owned(),
            tool_name: "apply_patch".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Custom {
                input: patch.to_owned(),
            },
            workdir,
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            permission_metadata: ToolPermissionMetadata::default(),
            execution_security_snapshot: snapshot,
            cancellation: CancellationToken::new(),
        }
    }

    fn trace() -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn", "call_apply_patch", "apply_patch")
    }

    #[test]
    fn validate_patch_document_accepts_valid_hunks() {
        let patch = "\
*** Begin Patch
*** Add File: a.txt
+hello
*** Update File: b.txt
@@
-old
+new
*** Delete File: c.txt
*** End Patch";

        let changed = validate_patch_document(patch).expect("patch should be valid");
        assert_eq!(
            changed,
            vec!["a.txt".to_owned(), "b.txt".to_owned(), "c.txt".to_owned()]
        );
    }

    #[test]
    fn validate_patch_document_collects_move_target() {
        let patch = "\
*** Begin Patch
*** Update File: old.txt
*** Move to: new.txt
@@
-old
+new
*** End Patch";

        let validation =
            validate_patch_document_with_targets(patch).expect("patch should be valid");

        assert_eq!(
            validation.changed_files,
            vec!["new.txt".to_owned(), "old.txt".to_owned()]
        );
        assert_eq!(
            validation
                .targets
                .iter()
                .map(|target| target.path.as_str())
                .collect::<Vec<_>>(),
            vec!["old.txt", "new.txt"]
        );
    }

    #[test]
    fn validate_patch_document_rejects_invalid_header() {
        let patch = "*** Add File: a.txt\n+hello\n*** End Patch";
        let error = validate_patch_document(patch).expect_err("invalid header must fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn validate_patch_document_rejects_non_empty_trailing_text() {
        let patch = "\
*** Begin Patch
*** Add File: a.txt
+hello
*** End Patch
unexpected";
        let error = validate_patch_document(patch).expect_err("trailing text should fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn extract_patch_supports_json_aliases() {
        let payload = ToolPayload::Function {
            arguments: serde_json::json!({ "patch": "PATCH_TEXT" }),
        };
        let extracted = extract_patch(payload).expect("json patch alias should work");
        assert_eq!(extracted, "PATCH_TEXT");
    }

    #[test]
    fn extract_patch_rejects_unsupported_payload() {
        let payload = ToolPayload::ToolSearch {
            query: "q".to_owned(),
            limit: None,
            include_hidden: None,
        };
        let error = extract_patch(payload).expect_err("unsupported payload must fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn apply_patch_policy_denies_mixed_patch_before_any_mutation() {
        let root = tempfile::tempdir().expect("root tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let denied_path = outside.path().join("blocked.txt");
        let patch = format!(
            "\
*** Begin Patch
*** Add File: allowed.txt
+allowed
*** Add File: {}
+blocked
*** End Patch",
            denied_path.display()
        );
        let snapshot = workspace_write_security_snapshot(root.path());

        let result = ApplyPatchHandler
            .handle(
                apply_patch_invocation(root.path().to_path_buf(), patch.as_str(), Some(snapshot)),
                trace(),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("mixed allowed/denied patch should be rejected before mutation"),
            Err(error) => error,
        };

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("patch target") && message.contains("outside the allowed sandbox roots"))
        );
        assert!(
            !root.path().join("allowed.txt").exists(),
            "allowed target must not be partially applied after denied target"
        );
        assert!(!denied_path.exists());
    }

    #[tokio::test]
    async fn apply_patch_path_escape_denies_move_target_outside_before_mutation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.as_path()).expect("create root");
        std::fs::create_dir_all(outside.as_path()).expect("create outside");
        std::fs::write(root.join("old.txt"), "old\n").expect("write old file");
        let moved_path = outside.join("moved.txt");
        let patch = "\
*** Begin Patch
*** Update File: old.txt
*** Move to: ../outside/moved.txt
@@
-old
+new
*** End Patch";
        let snapshot = workspace_write_security_snapshot(root.as_path());

        let result = ApplyPatchHandler
            .handle(
                apply_patch_invocation(root.clone(), patch, Some(snapshot)),
                trace(),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("move target outside root should be denied before mutation"),
            Err(error) => error,
        };

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("patch target") && message.contains("outside the allowed sandbox roots"))
        );
        assert_eq!(
            std::fs::read_to_string(root.join("old.txt")).expect("old file should read"),
            "old\n"
        );
        assert!(!moved_path.exists());
    }

    #[tokio::test]
    async fn apply_patch_path_escape_denies_delete_outside_root_before_mutation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.as_path()).expect("create root");
        std::fs::create_dir_all(outside.as_path()).expect("create outside");
        let outside_file = outside.join("secret.txt");
        std::fs::write(outside_file.as_path(), "secret\n").expect("write outside file");
        let patch = "\
*** Begin Patch
*** Delete File: ../outside/secret.txt
*** End Patch";
        let snapshot = workspace_write_security_snapshot(root.as_path());

        let result = ApplyPatchHandler
            .handle(
                apply_patch_invocation(root.clone(), patch, Some(snapshot)),
                trace(),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("delete outside root should be denied before mutation"),
            Err(error) => error,
        };

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("patch target") && message.contains("outside the allowed sandbox roots"))
        );
        assert_eq!(
            std::fs::read_to_string(outside_file.as_path()).expect("outside file should read"),
            "secret\n"
        );
    }

    #[test]
    fn apply_patch_path_escape_fullaccess_allows_relative_move_target_policy() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.as_path()).expect("create root");
        std::fs::create_dir_all(outside.as_path()).expect("create outside");
        std::fs::write(root.join("old.txt"), "old\n").expect("write old file");
        let patch = "\
*** Begin Patch
*** Update File: old.txt
*** Move to: ../outside/moved.txt
@@
-old
+new
*** End Patch";
        let validation =
            validate_patch_document_with_targets(patch).expect("patch should be valid");
        let snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access(root.to_string_lossy(), 1);

        enforce_patch_targets(
            Some(&snapshot),
            root.as_path(),
            validation.targets.as_slice(),
        )
        .expect("full access should allow relative move outside root");
        assert!(!outside.join("moved.txt").exists());
    }
}
