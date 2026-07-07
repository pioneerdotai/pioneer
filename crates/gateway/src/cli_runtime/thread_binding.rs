#![allow(dead_code)]
// Persists Pioneer thread to native CLI runtime thread bindings.

use crate::cli_runtime::manager::{
    CLIAgentRuntimeSession, CLIAgentRuntimeThreadOpenParams, CLIAgentRuntimeThreadOpenSnapshot,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use pioneer_crud::{
    CliRuntimeThreadBindingRecord, CrudStore, NewCliRuntimeThreadBinding,
    serialize_cli_runtime_json,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct CLIAgentRuntimeThreadBindingOpenRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub cwd: String,
    pub model: Option<String>,
    pub approval_policy: Option<String>,
    pub sandbox: Option<serde_json::Value>,
    pub permissions: Option<String>,
    pub service_tier: Option<String>,
    pub resume_existing: bool,
    pub request_timeout: Duration,
    pub opened_at: DateTimeWithTimeZone,
}

impl CLIAgentRuntimeThreadBindingOpenRequest {
    fn start_params(&self) -> CLIAgentRuntimeThreadOpenParams {
        CLIAgentRuntimeThreadOpenParams {
            cwd: self.cwd.clone(),
            model: self.model.clone(),
            approval_policy: self.approval_policy.clone(),
            sandbox: self.sandbox.clone(),
            permissions: self.permissions.clone(),
            service_tier: self.service_tier.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CLIAgentRuntimeThreadBindingOpenMode {
    Started,
    Resumed,
}

#[derive(Debug, Clone)]
pub(crate) struct CLIAgentRuntimeThreadBindingOpenResult {
    pub binding: CliRuntimeThreadBindingRecord,
    pub mode: CLIAgentRuntimeThreadBindingOpenMode,
}

#[async_trait]
pub(crate) trait CLIAgentRuntimeThreadOpenClient: Send + Sync {
    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot>;

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot>;
}

#[async_trait]
impl CLIAgentRuntimeThreadOpenClient for std::sync::Arc<dyn CLIAgentRuntimeSession> {
    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        CLIAgentRuntimeSession::start_thread(self.as_ref(), params, timeout).await
    }

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        CLIAgentRuntimeSession::resume_thread(self.as_ref(), native_thread_id, params, timeout)
            .await
    }
}

pub(crate) async fn open_cli_runtime_thread_binding<C>(
    store: &CrudStore,
    client: &C,
    request: CLIAgentRuntimeThreadBindingOpenRequest,
) -> Result<CLIAgentRuntimeThreadBindingOpenResult>
where
    C: CLIAgentRuntimeThreadOpenClient + ?Sized,
{
    validate_generic_open_request(&request)?;

    let existing = store
        .get_cli_runtime_thread_binding(request.thread_id.as_str())
        .await
        .with_context(|| {
            format!(
                "failed to read CLI runtime binding for thread `{}`",
                request.thread_id
            )
        })?;
    if let Some(existing) = existing.as_ref() {
        validate_existing_generic_binding(existing, &request)?;
    }

    let start_params = request.start_params();
    let (mode, opened, preserve_native_metadata) = match existing.as_ref() {
        Some(binding) if request.resume_existing => {
            let opened = client
                .resume_thread(
                    binding.native_thread_id.as_str(),
                    start_params,
                    request.request_timeout,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to resume native CLI runtime thread `{}` for Pioneer thread `{}`",
                        binding.native_thread_id, request.thread_id
                    )
                })?;
            if opened.native_thread_id != binding.native_thread_id {
                bail!(
                    "CLI runtime thread resume returned native thread `{}` for stored native thread `{}`",
                    opened.native_thread_id,
                    binding.native_thread_id
                );
            }
            (CLIAgentRuntimeThreadBindingOpenMode::Resumed, opened, true)
        }
        Some(_) | None => {
            let opened = client
                .start_thread(start_params, request.request_timeout)
                .await
                .with_context(|| {
                    format!(
                        "failed to start native CLI runtime thread for Pioneer thread `{}`",
                        request.thread_id
                    )
                })?;
            (CLIAgentRuntimeThreadBindingOpenMode::Started, opened, false)
        }
    };

    let binding = store
        .upsert_cli_runtime_thread_binding(generic_thread_binding_from_opened(
            &request,
            existing.as_ref(),
            &opened,
            preserve_native_metadata,
        )?)
        .await
        .with_context(|| {
            format!(
                "failed to persist CLI runtime binding for thread `{}`",
                request.thread_id
            )
        })?;

    Ok(CLIAgentRuntimeThreadBindingOpenResult { binding, mode })
}

fn generic_thread_binding_from_opened(
    request: &CLIAgentRuntimeThreadBindingOpenRequest,
    existing: Option<&CliRuntimeThreadBindingRecord>,
    opened: &CLIAgentRuntimeThreadOpenSnapshot,
    preserve_native_metadata: bool,
) -> Result<NewCliRuntimeThreadBinding> {
    Ok(NewCliRuntimeThreadBinding {
        thread_id: request.thread_id.clone(),
        workspace_id: request.workspace_id.clone(),
        runtime_id: request.runtime_id.clone(),
        runtime_kind: request.runtime_kind.clone(),
        native_thread_id: opened.native_thread_id.clone(),
        native_session_id: preserve_native_metadata
            .then(|| existing.and_then(|binding| binding.native_session_id.clone()))
            .flatten(),
        native_root_thread_id: preserve_native_metadata
            .then(|| existing.and_then(|binding| binding.native_root_thread_id.clone()))
            .flatten(),
        native_cwd: opened.cwd.clone().or_else(|| Some(request.cwd.clone())),
        native_model: opened.model.clone().or_else(|| request.model.clone()),
        resume_cursor_json: serialize_cli_runtime_json(&serde_json::json!({
            "threadId": opened.native_thread_id
        }))?,
        status: "active".to_owned(),
        created_at: existing
            .map(|binding| binding.created_at)
            .unwrap_or(request.opened_at),
        updated_at: request.opened_at,
    })
}

fn validate_generic_open_request(request: &CLIAgentRuntimeThreadBindingOpenRequest) -> Result<()> {
    for (label, value) in [
        ("workspace_id", request.workspace_id.as_str()),
        ("thread_id", request.thread_id.as_str()),
        ("runtime_id", request.runtime_id.as_str()),
        ("runtime_kind", request.runtime_kind.as_str()),
        ("cwd", request.cwd.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("CLI runtime thread binding request `{label}` cannot be empty");
        }
    }
    Ok(())
}

fn validate_existing_generic_binding(
    existing: &CliRuntimeThreadBindingRecord,
    request: &CLIAgentRuntimeThreadBindingOpenRequest,
) -> Result<()> {
    if existing.workspace_id != request.workspace_id {
        bail!(
            "CLI runtime binding for thread `{}` belongs to workspace `{}` not `{}`",
            request.thread_id,
            existing.workspace_id,
            request.workspace_id
        );
    }
    if existing.runtime_id != request.runtime_id || existing.runtime_kind != request.runtime_kind {
        bail!(
            "CLI runtime binding for thread `{}` belongs to runtime `{}`/`{}` not `{}`/`{}`",
            request.thread_id,
            existing.runtime_id,
            existing.runtime_kind,
            request.runtime_id,
            request.runtime_kind
        );
    }
    if existing.status != "active" {
        bail!(
            "CLI runtime binding for thread `{}` is `{}`",
            request.thread_id,
            existing.status
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CLIAgentRuntimeThreadBindingOpenMode, CLIAgentRuntimeThreadBindingOpenRequest,
        CLIAgentRuntimeThreadOpenClient, open_cli_runtime_thread_binding,
    };
    use crate::cli_runtime::manager::{
        CLIAgentRuntimeThreadOpenParams, CLIAgentRuntimeThreadOpenSnapshot,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{CrudStore, NewCliRuntimeThreadBinding};
    use sea_orm::entity::prelude::DateTimeWithTimeZone;
    use sea_orm::{Database, DatabaseConnection};
    use serde_json::json;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Debug)]
    struct FakeCliRuntimeThreadClient {
        starts: Mutex<Vec<CLIAgentRuntimeThreadOpenParams>>,
        resumes: Mutex<Vec<(String, CLIAgentRuntimeThreadOpenParams)>>,
        start_result: Mutex<std::result::Result<CLIAgentRuntimeThreadOpenSnapshot, String>>,
        resume_result: Mutex<std::result::Result<CLIAgentRuntimeThreadOpenSnapshot, String>>,
    }

    #[async_trait]
    impl CLIAgentRuntimeThreadOpenClient for FakeCliRuntimeThreadClient {
        async fn start_thread(
            &self,
            params: CLIAgentRuntimeThreadOpenParams,
            _timeout: Duration,
        ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
            self.starts.lock().expect("starts lock").push(params);
            self.start_result
                .lock()
                .expect("start result lock")
                .clone()
                .map_err(anyhow::Error::msg)
        }

        async fn resume_thread(
            &self,
            native_thread_id: &str,
            params: CLIAgentRuntimeThreadOpenParams,
            _timeout: Duration,
        ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
            self.resumes
                .lock()
                .expect("resumes lock")
                .push((native_thread_id.to_owned(), params));
            self.resume_result
                .lock()
                .expect("resume result lock")
                .clone()
                .map_err(anyhow::Error::msg)
        }
    }

    impl FakeCliRuntimeThreadClient {
        fn new() -> Self {
            Self {
                starts: Mutex::new(Vec::new()),
                resumes: Mutex::new(Vec::new()),
                start_result: Mutex::new(Ok(open_snapshot("cli-thread-started"))),
                resume_result: Mutex::new(Ok(open_snapshot("cli-thread-existing"))),
            }
        }

        fn set_resume_error(&self, message: &str) {
            *self.resume_result.lock().expect("resume result lock") = Err(message.to_owned());
        }
    }

    async fn setup_store() -> (DatabaseConnection, CrudStore) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory connection");
        Migrator::up(&connection, None)
            .await
            .expect("migrations should apply");
        let store = CrudStore::new(connection.clone());
        (connection, store)
    }

    fn open_request(thread_id: &str, opened_at: i64) -> CLIAgentRuntimeThreadBindingOpenRequest {
        CLIAgentRuntimeThreadBindingOpenRequest {
            workspace_id: "ws_cli_binding".to_owned(),
            thread_id: thread_id.to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            cwd: "/tmp/project".to_owned(),
            model: Some("gpt-5".to_owned()),
            approval_policy: Some("on-request".to_owned()),
            sandbox: Some(json!("workspace-write")),
            permissions: None,
            service_tier: None,
            resume_existing: true,
            request_timeout: Duration::from_secs(5),
            opened_at: unix_to_datetime(opened_at),
        }
    }

    fn open_snapshot(native_thread_id: &str) -> CLIAgentRuntimeThreadOpenSnapshot {
        CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: native_thread_id.to_owned(),
            cwd: Some("/tmp/project".to_owned()),
            model: Some("gpt-5".to_owned()),
            raw: json!({ "thread": { "id": native_thread_id } }),
        }
    }

    fn unix_to_datetime(timestamp: i64) -> DateTimeWithTimeZone {
        chrono::DateTime::from_timestamp(timestamp, 0)
            .expect("valid timestamp")
            .fixed_offset()
    }

    #[tokio::test]
    async fn cli_runtime_binding_first_start_persists_thread_binding() {
        let (_connection, store) = setup_store().await;
        let client = FakeCliRuntimeThreadClient::new();

        let result =
            open_cli_runtime_thread_binding(&store, &client, open_request("thread_cli_a", 100))
                .await
                .expect("first open should succeed");

        assert_eq!(result.mode, CLIAgentRuntimeThreadBindingOpenMode::Started);
        assert_eq!(result.binding.native_thread_id, "cli-thread-started");
        assert_eq!(result.binding.native_cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(result.binding.native_model.as_deref(), Some("gpt-5"));
        assert_eq!(
            result.binding.resume_cursor_json,
            r#"{"threadId":"cli-thread-started"}"#
        );
        assert_eq!(client.starts.lock().expect("starts lock").len(), 1);
        assert!(client.resumes.lock().expect("resumes lock").is_empty());
    }

    #[tokio::test]
    async fn cli_runtime_binding_resume_uses_stored_native_thread() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_resume".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "cli-thread-existing".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("gpt-5".to_owned()),
                resume_cursor_json: r#"{"threadId":"cli-thread-existing"}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("seed binding should persist");
        let client = FakeCliRuntimeThreadClient::new();

        let result = open_cli_runtime_thread_binding(
            &store,
            &client,
            open_request("thread_cli_resume", 200),
        )
        .await
        .expect("resume should succeed");

        assert_eq!(result.mode, CLIAgentRuntimeThreadBindingOpenMode::Resumed);
        assert_eq!(result.binding.native_thread_id, "cli-thread-existing");
        assert_eq!(result.binding.created_at, opened_at);
        assert_eq!(result.binding.updated_at, unix_to_datetime(200));
        assert!(client.starts.lock().expect("starts lock").is_empty());
        let resumes = client.resumes.lock().expect("resumes lock");
        assert_eq!(resumes.len(), 1);
        assert_eq!(resumes[0].0, "cli-thread-existing");
    }

    #[tokio::test]
    async fn cli_runtime_binding_resume_error_keeps_existing_binding() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_error".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "cli-thread-existing".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("gpt-5".to_owned()),
                resume_cursor_json: r#"{"threadId":"cli-thread-existing"}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("seed binding should persist");
        let client = FakeCliRuntimeThreadClient::new();
        client.set_resume_error("native thread missing");

        let error =
            open_cli_runtime_thread_binding(&store, &client, open_request("thread_cli_error", 200))
                .await
                .expect_err("resume failure should surface");
        assert!(
            format!("{error:#}").contains("failed to resume native CLI runtime thread"),
            "resume error should be actionable"
        );

        let binding = store
            .get_cli_runtime_thread_binding("thread_cli_error")
            .await
            .expect("binding read should succeed")
            .expect("binding should remain");
        assert_eq!(binding.native_thread_id, "cli-thread-existing");
        assert_eq!(binding.updated_at, opened_at);
        assert!(client.starts.lock().expect("starts lock").is_empty());
        assert_eq!(client.resumes.lock().expect("resumes lock").len(), 1);
    }

    #[tokio::test]
    async fn cli_runtime_binding_non_resumable_runtime_starts_new_native_thread() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_non_resumable".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "claude".to_owned(),
                runtime_kind: "claude".to_owned(),
                native_thread_id: "cli-thread-existing".to_owned(),
                native_session_id: Some("native-session-existing".to_owned()),
                native_root_thread_id: Some("native-root-existing".to_owned()),
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("sonnet".to_owned()),
                resume_cursor_json: r#"{"threadId":"cli-thread-existing"}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("seed binding should persist");
        let client = FakeCliRuntimeThreadClient::new();
        let mut request = open_request("thread_cli_non_resumable", 200);
        request.runtime_id = "claude".to_owned();
        request.runtime_kind = "claude".to_owned();
        request.model = Some("sonnet".to_owned());
        request.resume_existing = false;

        let result = open_cli_runtime_thread_binding(&store, &client, request)
            .await
            .expect("non-resumable runtime should start a fresh native thread");

        assert_eq!(result.mode, CLIAgentRuntimeThreadBindingOpenMode::Started);
        assert_eq!(result.binding.native_thread_id, "cli-thread-started");
        assert_eq!(result.binding.native_session_id, None);
        assert_eq!(result.binding.native_root_thread_id, None);
        assert_eq!(result.binding.created_at, opened_at);
        assert_eq!(result.binding.updated_at, unix_to_datetime(200));
        assert_eq!(client.starts.lock().expect("starts lock").len(), 1);
        assert!(client.resumes.lock().expect("resumes lock").is_empty());
    }
}
