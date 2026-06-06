//! RPC request and response validation helpers.

use anyhow::{Result, anyhow};

pub fn require_non_empty_field(value: &str, field: &str, method: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} is required for {method}"));
    }
    Ok(())
}

pub fn require_optional_non_empty_field(
    value: Option<&str>,
    field: &str,
    method: &str,
) -> Result<()> {
    if value.is_some_and(|value| value.trim().is_empty()) {
        return Err(anyhow!("{field} must not be empty for {method}"));
    }
    Ok(())
}

pub fn require_condition(condition: bool, message: &str) -> Result<()> {
    if !condition {
        return Err(anyhow!("{message}"));
    }
    Ok(())
}

pub fn validate_task_review_target(
    task_id: &str,
    run_id: &str,
    candidate_id: &str,
    method: &str,
) -> Result<()> {
    require_non_empty_field(task_id, "task_id", method)?;
    require_non_empty_field(run_id, "run_id", method)?;
    require_non_empty_field(candidate_id, "candidate_id", method)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_validation_requires_non_empty_fields() {
        assert_eq!(
            format!(
                "{:#}",
                require_non_empty_field(" ", "workspace_id", "workspace/list")
                    .expect_err("field should be rejected")
            ),
            "workspace_id is required for workspace/list"
        );
        require_non_empty_field("ws_1", "workspace_id", "workspace/list").expect("field");
    }

    #[test]
    fn rpc_validation_rejects_empty_optional_fields() {
        assert_eq!(
            format!(
                "{:#}",
                require_optional_non_empty_field(Some(" "), "name", "workspace/create")
                    .expect_err("field should be rejected")
            ),
            "name must not be empty for workspace/create"
        );
        require_optional_non_empty_field(None, "name", "workspace/create").expect("none");
        require_optional_non_empty_field(Some("Main"), "name", "workspace/create").expect("value");
    }
}
