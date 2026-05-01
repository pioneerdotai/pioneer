use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;
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
        let changed_files = validate_patch_document(patch.as_str())?;

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

fn extract_patch(payload: ToolPayload) -> Result<String, ToolError> {
    match payload {
        ToolPayload::Custom { input } => Ok(input),
        ToolPayload::Function { arguments } => extract_patch_from_json(arguments),
        other => Err(ToolError::invalid_arguments(format!(
            "unsupported apply_patch payload: {}",
            other.log_payload()
        ))),
    }
}

fn extract_patch_from_json(value: JsonValue) -> Result<String, ToolError> {
    if let Some(input) = value.get("input").and_then(JsonValue::as_str) {
        return Ok(input.to_owned());
    }
    if let Some(input) = value.get("patch").and_then(JsonValue::as_str) {
        return Ok(input.to_owned());
    }
    if let Some(input) = value.as_str() {
        return Ok(input.to_owned());
    }

    Err(ToolError::invalid_arguments(
        "apply_patch expects `input` or `patch` string",
    ))
}

fn validate_patch_document(patch: &str) -> Result<Vec<String>, ToolError> {
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
            saw_hunk = true;
            idx = idx.saturating_add(1);
            continue;
        }

        if let Some(path) = line.strip_prefix(UPDATE_FILE) {
            let path = path.trim();
            validate_patch_path(path)?;
            changed.insert(path.to_owned());
            saw_hunk = true;
            idx = idx.saturating_add(1);

            if idx < lines.len()
                && let Some(moved_path) = lines[idx].strip_prefix(MOVE_TO)
            {
                let moved_path = moved_path.trim();
                validate_patch_path(moved_path)?;
                changed.insert(moved_path.to_owned());
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

    Ok(changed.into_iter().collect())
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
}
