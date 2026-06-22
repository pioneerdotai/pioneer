#![allow(dead_code)]
// Persists Pioneer turn to native CLI runtime turn bindings.

use anyhow::{Context, Result, bail};
use pioneer_crud::{
    CliRuntimeTurnBindingRecord, CrudStore, NewCliRuntimeTurnBinding, serialize_cli_runtime_json,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde::Serialize;
use serde_json::Value as JsonValue;

pub(crate) const CLI_RUNTIME_TURN_STATUS_STARTING: &str = "starting";
pub(crate) const CLI_RUNTIME_TURN_STATUS_RUNNING: &str = "running";
pub(crate) const CLI_RUNTIME_TURN_STATUS_COMPLETED: &str = "completed";
pub(crate) const CLI_RUNTIME_TURN_STATUS_FAILED: &str = "failed";
pub(crate) const CLI_RUNTIME_TURN_STATUS_INTERRUPTED: &str = "interrupted";
pub(crate) const CLI_RUNTIME_TURN_STATUS_BLOCKED: &str = "blocked";

#[derive(Debug, Clone)]
pub(crate) struct CLIAgentRuntimeTurnBindingStartRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub request_id: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub sandbox_json: Option<String>,
    pub approval_policy: Option<String>,
    pub input_mapping_json: String,
    pub created_at: DateTimeWithTimeZone,
}

impl CLIAgentRuntimeTurnBindingStartRequest {
    pub(crate) fn with_input_mapping<T: Serialize>(
        workspace_id: impl Into<String>,
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        runtime_id: impl Into<String>,
        runtime_kind: impl Into<String>,
        native_thread_id: impl Into<String>,
        input_mapping: &T,
        created_at: DateTimeWithTimeZone,
    ) -> Result<Self> {
        Ok(Self {
            workspace_id: workspace_id.into(),
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            runtime_id: runtime_id.into(),
            runtime_kind: runtime_kind.into(),
            native_thread_id: native_thread_id.into(),
            request_id: None,
            model: None,
            cwd: None,
            sandbox_json: None,
            approval_policy: None,
            input_mapping_json: serialize_cli_runtime_json(input_mapping)?,
            created_at,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CLIAgentRuntimeNativeTurnStarted {
    pub turn_id: String,
    pub native_turn_id: String,
    pub request_id: Option<String>,
    pub started_at: DateTimeWithTimeZone,
}

pub(crate) async fn persist_cli_runtime_turn_binding_before_native_start(
    store: &CrudStore,
    request: CLIAgentRuntimeTurnBindingStartRequest,
) -> Result<CliRuntimeTurnBindingRecord> {
    validate_start_request(&request)?;
    let existing = store
        .get_cli_runtime_turn_binding(request.turn_id.as_str())
        .await
        .with_context(|| {
            format!(
                "failed to read CLI runtime turn binding `{}`",
                request.turn_id
            )
        })?;
    if let Some(existing) = existing.as_ref() {
        validate_existing_turn_binding(existing, &request)?;
    }

    store
        .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
            turn_id: request.turn_id,
            thread_id: request.thread_id,
            workspace_id: request.workspace_id,
            runtime_id: request.runtime_id,
            runtime_kind: request.runtime_kind,
            native_thread_id: request.native_thread_id,
            native_turn_id: existing
                .as_ref()
                .and_then(|binding| binding.native_turn_id.clone()),
            request_id: request.request_id,
            status: CLI_RUNTIME_TURN_STATUS_STARTING.to_owned(),
            model: request.model,
            cwd: request.cwd,
            sandbox_json: request.sandbox_json,
            approval_policy: request.approval_policy,
            input_mapping_json: request.input_mapping_json,
            created_at: existing
                .as_ref()
                .map(|binding| binding.created_at)
                .unwrap_or(request.created_at),
            updated_at: request.created_at,
        })
        .await
        .context("failed to persist CLI runtime turn binding before native start")
}

pub(crate) async fn persist_cli_runtime_turn_binding_after_native_start(
    store: &CrudStore,
    native_turn: CLIAgentRuntimeNativeTurnStarted,
) -> Result<CliRuntimeTurnBindingRecord> {
    validate_native_turn_started(&native_turn)?;
    let Some(existing) = store
        .get_cli_runtime_turn_binding(native_turn.turn_id.as_str())
        .await
        .with_context(|| {
            format!(
                "failed to read CLI runtime turn binding `{}`",
                native_turn.turn_id
            )
        })?
    else {
        bail!(
            "cannot persist native turn id for CLI runtime turn `{}` before pre-start binding exists",
            native_turn.turn_id
        );
    };

    store
        .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
            turn_id: existing.turn_id,
            thread_id: existing.thread_id,
            workspace_id: existing.workspace_id,
            runtime_id: existing.runtime_id,
            runtime_kind: existing.runtime_kind,
            native_thread_id: existing.native_thread_id,
            native_turn_id: Some(native_turn.native_turn_id),
            request_id: native_turn.request_id.or(existing.request_id),
            status: CLI_RUNTIME_TURN_STATUS_RUNNING.to_owned(),
            model: existing.model,
            cwd: existing.cwd,
            sandbox_json: existing.sandbox_json,
            approval_policy: existing.approval_policy,
            input_mapping_json: existing.input_mapping_json,
            created_at: existing.created_at,
            updated_at: native_turn.started_at,
        })
        .await
        .context("failed to persist CLI runtime turn binding after native start")
}

pub(crate) async fn update_cli_runtime_turn_binding_status(
    store: &CrudStore,
    turn_id: &str,
    status: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<CliRuntimeTurnBindingRecord>> {
    validate_terminal_status(status)?;
    let Some(existing) = store
        .get_cli_runtime_turn_binding(turn_id)
        .await
        .with_context(|| format!("failed to read CLI runtime turn binding `{turn_id}`"))?
    else {
        return Ok(None);
    };

    store
        .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
            turn_id: existing.turn_id,
            thread_id: existing.thread_id,
            workspace_id: existing.workspace_id,
            runtime_id: existing.runtime_id,
            runtime_kind: existing.runtime_kind,
            native_thread_id: existing.native_thread_id,
            native_turn_id: existing.native_turn_id,
            request_id: existing.request_id,
            status: status.to_owned(),
            model: existing.model,
            cwd: existing.cwd,
            sandbox_json: existing.sandbox_json,
            approval_policy: existing.approval_policy,
            input_mapping_json: existing.input_mapping_json,
            created_at: existing.created_at,
            updated_at,
        })
        .await
        .map(Some)
        .context("failed to update CLI runtime turn binding status")
}

pub(crate) fn cli_runtime_turn_input_mapping_json<T: Serialize>(value: &T) -> Result<String> {
    serialize_cli_runtime_json(value)
}

pub(crate) fn cli_runtime_turn_sandbox_json(value: Option<&JsonValue>) -> Result<Option<String>> {
    value.map(serialize_cli_runtime_json).transpose()
}

fn validate_start_request(request: &CLIAgentRuntimeTurnBindingStartRequest) -> Result<()> {
    for (label, value) in [
        ("workspace_id", request.workspace_id.as_str()),
        ("thread_id", request.thread_id.as_str()),
        ("turn_id", request.turn_id.as_str()),
        ("runtime_id", request.runtime_id.as_str()),
        ("runtime_kind", request.runtime_kind.as_str()),
        ("native_thread_id", request.native_thread_id.as_str()),
        ("input_mapping_json", request.input_mapping_json.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("CLI runtime turn binding request `{label}` cannot be empty");
        }
    }
    Ok(())
}

fn validate_native_turn_started(native_turn: &CLIAgentRuntimeNativeTurnStarted) -> Result<()> {
    if native_turn.turn_id.trim().is_empty() {
        bail!("CLI runtime native turn start `turn_id` cannot be empty");
    }
    if native_turn.native_turn_id.trim().is_empty() {
        bail!("CLI runtime native turn id cannot be empty");
    }
    Ok(())
}

fn validate_terminal_status(status: &str) -> Result<()> {
    match status {
        CLI_RUNTIME_TURN_STATUS_COMPLETED
        | CLI_RUNTIME_TURN_STATUS_FAILED
        | CLI_RUNTIME_TURN_STATUS_INTERRUPTED
        | CLI_RUNTIME_TURN_STATUS_BLOCKED => Ok(()),
        _ => bail!("unsupported CLI runtime terminal status `{status}`"),
    }
}

fn validate_existing_turn_binding(
    existing: &CliRuntimeTurnBindingRecord,
    request: &CLIAgentRuntimeTurnBindingStartRequest,
) -> Result<()> {
    if existing.workspace_id != request.workspace_id || existing.thread_id != request.thread_id {
        bail!(
            "turn `{}` is bound to workspace/thread `{}/{}` not `{}/{}`",
            request.turn_id,
            existing.workspace_id,
            existing.thread_id,
            request.workspace_id,
            request.thread_id
        );
    }
    if existing.runtime_id != request.runtime_id || existing.runtime_kind != request.runtime_kind {
        bail!(
            "turn `{}` is bound to CLI runtime `{}`/`{}` not `{}`/`{}`",
            request.turn_id,
            existing.runtime_kind,
            existing.runtime_id,
            request.runtime_kind,
            request.runtime_id
        );
    }
    if existing.native_thread_id != request.native_thread_id {
        bail!(
            "turn `{}` is bound to native thread `{}` not `{}`",
            request.turn_id,
            existing.native_thread_id,
            request.native_thread_id
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CLI_RUNTIME_TURN_STATUS_RUNNING, CLI_RUNTIME_TURN_STATUS_STARTING,
        CLIAgentRuntimeNativeTurnStarted, CLIAgentRuntimeTurnBindingStartRequest,
        cli_runtime_turn_sandbox_json, persist_cli_runtime_turn_binding_after_native_start,
        persist_cli_runtime_turn_binding_before_native_start,
    };
    use chrono::{FixedOffset, TimeZone};
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::CrudStore;
    use sea_orm::Database;
    use sea_orm::entity::prelude::DateTimeWithTimeZone;
    use serde_json::json;

    async fn test_store() -> CrudStore {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        CrudStore::new(connection)
    }

    fn timestamp(secs: i64) -> DateTimeWithTimeZone {
        FixedOffset::east_opt(0)
            .expect("UTC offset should exist")
            .timestamp_opt(secs, 0)
            .single()
            .expect("valid timestamp")
    }

    fn start_request(created_at: DateTimeWithTimeZone) -> CLIAgentRuntimeTurnBindingStartRequest {
        let mut request = CLIAgentRuntimeTurnBindingStartRequest::with_input_mapping(
            "ws_cli_turn",
            "thread_cli_turn",
            "turn_cli_turn",
            "codex",
            "codex",
            "codex-thread-1",
            &json!({ "source": "turn/start" }),
            created_at,
        )
        .expect("input mapping should serialize");
        request.request_id = Some("rpc-turn-start-1".to_owned());
        request.model = Some("gpt-5".to_owned());
        request.cwd = Some("/tmp/project".to_owned());
        request.sandbox_json =
            cli_runtime_turn_sandbox_json(Some(&json!({ "mode": "workspace-write" })))
                .expect("sandbox should serialize");
        request.approval_policy = Some("on-request".to_owned());
        request
    }

    #[tokio::test]
    async fn cli_runtime_recovery_turn_binding_persists_pre_and_post_native_start() {
        let store = test_store().await;
        let created_at = timestamp(1_700_020_000);

        let starting =
            persist_cli_runtime_turn_binding_before_native_start(&store, start_request(created_at))
                .await
                .expect("pre-start binding should persist");

        assert_eq!(starting.status, CLI_RUNTIME_TURN_STATUS_STARTING);
        assert_eq!(starting.native_turn_id, None);
        assert_eq!(starting.request_id.as_deref(), Some("rpc-turn-start-1"));

        let running = persist_cli_runtime_turn_binding_after_native_start(
            &store,
            CLIAgentRuntimeNativeTurnStarted {
                turn_id: "turn_cli_turn".to_owned(),
                native_turn_id: "codex-turn-1".to_owned(),
                request_id: None,
                started_at: timestamp(1_700_020_010),
            },
        )
        .await
        .expect("post-start binding should persist");

        assert_eq!(running.status, CLI_RUNTIME_TURN_STATUS_RUNNING);
        assert_eq!(running.native_turn_id.as_deref(), Some("codex-turn-1"));
        assert_eq!(running.request_id.as_deref(), Some("rpc-turn-start-1"));
        assert_eq!(running.created_at, created_at);
        assert_eq!(running.updated_at, timestamp(1_700_020_010));
    }

    #[tokio::test]
    async fn cli_runtime_recovery_turn_binding_rejects_native_start_without_prestart() {
        let store = test_store().await;

        let error = persist_cli_runtime_turn_binding_after_native_start(
            &store,
            CLIAgentRuntimeNativeTurnStarted {
                turn_id: "turn_missing_prestart".to_owned(),
                native_turn_id: "codex-turn-missing".to_owned(),
                request_id: None,
                started_at: timestamp(1_700_020_010),
            },
        )
        .await
        .expect_err("post-start binding should require pre-start row");

        assert!(
            format!("{error:#}").contains("before pre-start binding exists"),
            "error should explain missing pre-start row"
        );
    }
}
