#![allow(dead_code)]
// Codex thread binding is wired into turn routing in the following WP steps.

use crate::cli_runtime::manager::CLIAgentRuntimeSession;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use pioneer_cli_agent_runtime::codex::{
    CodexAppServerClient, CodexThreadOpenSnapshot, CodexThreadStartParams,
};
use pioneer_crud::{
    CliRuntimeThreadBindingRecord, CrudStore, NewCliRuntimeThreadBinding,
    serialize_cli_runtime_json,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde_json::json;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct CodexThreadBindingOpenRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub cwd: String,
    pub model: Option<String>,
    pub approval_policy: String,
    pub sandbox: String,
    pub service_tier: Option<String>,
    pub request_timeout: Duration,
    pub opened_at: DateTimeWithTimeZone,
}

impl CodexThreadBindingOpenRequest {
    fn start_params(&self) -> CodexThreadStartParams {
        CodexThreadStartParams {
            cwd: self.cwd.clone(),
            approval_policy: self.approval_policy.clone(),
            sandbox: self.sandbox.clone(),
            model: self.model.clone(),
            service_tier: self.service_tier.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexThreadBindingOpenMode {
    Started,
    Resumed,
}

#[derive(Debug, Clone)]
pub(crate) struct CodexThreadBindingOpenResult {
    pub binding: CliRuntimeThreadBindingRecord,
    pub mode: CodexThreadBindingOpenMode,
}

#[async_trait]
pub(crate) trait CodexThreadOpenClient: Send + Sync {
    async fn start_codex_thread(
        &self,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot>;

    async fn resume_codex_thread(
        &self,
        native_thread_id: &str,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot>;
}

#[async_trait]
impl CodexThreadOpenClient for CodexAppServerClient {
    async fn start_codex_thread(
        &self,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot> {
        self.thread_start(params, timeout)
            .await
            .context("Codex thread/start failed")
    }

    async fn resume_codex_thread(
        &self,
        native_thread_id: &str,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot> {
        self.thread_resume(native_thread_id, params, timeout)
            .await
            .context("Codex thread/resume failed")
    }
}

#[async_trait]
impl CodexThreadOpenClient for std::sync::Arc<dyn CLIAgentRuntimeSession> {
    async fn start_codex_thread(
        &self,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot> {
        CLIAgentRuntimeSession::start_codex_thread(self.as_ref(), params, timeout).await
    }

    async fn resume_codex_thread(
        &self,
        native_thread_id: &str,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot> {
        CLIAgentRuntimeSession::resume_codex_thread(
            self.as_ref(),
            native_thread_id,
            params,
            timeout,
        )
        .await
    }
}

pub(crate) async fn open_codex_thread_binding<C>(
    store: &CrudStore,
    client: &C,
    request: CodexThreadBindingOpenRequest,
) -> Result<CodexThreadBindingOpenResult>
where
    C: CodexThreadOpenClient + ?Sized,
{
    validate_open_request(&request)?;

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
        validate_existing_binding(existing, &request)?;
    }

    let start_params = request.start_params();
    let (mode, opened) = match existing.as_ref() {
        Some(binding) => {
            let opened = client
                .resume_codex_thread(
                    binding.native_thread_id.as_str(),
                    start_params,
                    request.request_timeout,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to resume Codex native thread `{}` for Pioneer thread `{}`",
                        binding.native_thread_id, request.thread_id
                    )
                })?;
            if opened.native_thread_id != binding.native_thread_id {
                bail!(
                    "Codex thread/resume returned native thread `{}` for stored native thread `{}`",
                    opened.native_thread_id,
                    binding.native_thread_id
                );
            }
            (CodexThreadBindingOpenMode::Resumed, opened)
        }
        None => {
            let opened = client
                .start_codex_thread(start_params, request.request_timeout)
                .await
                .with_context(|| {
                    format!(
                        "failed to start Codex native thread for Pioneer thread `{}`",
                        request.thread_id
                    )
                })?;
            (CodexThreadBindingOpenMode::Started, opened)
        }
    };

    let binding = store
        .upsert_cli_runtime_thread_binding(thread_binding_from_opened(
            &request,
            existing.as_ref(),
            &opened,
        )?)
        .await
        .with_context(|| {
            format!(
                "failed to persist Codex CLI runtime binding for thread `{}`",
                request.thread_id
            )
        })?;

    Ok(CodexThreadBindingOpenResult { binding, mode })
}

fn thread_binding_from_opened(
    request: &CodexThreadBindingOpenRequest,
    existing: Option<&CliRuntimeThreadBindingRecord>,
    opened: &CodexThreadOpenSnapshot,
) -> Result<NewCliRuntimeThreadBinding> {
    Ok(NewCliRuntimeThreadBinding {
        thread_id: request.thread_id.clone(),
        workspace_id: request.workspace_id.clone(),
        runtime_id: request.runtime_id.clone(),
        runtime_kind: request.runtime_kind.clone(),
        native_thread_id: opened.native_thread_id.clone(),
        native_session_id: existing.and_then(|binding| binding.native_session_id.clone()),
        native_root_thread_id: existing.and_then(|binding| binding.native_root_thread_id.clone()),
        native_cwd: opened.cwd.clone().or_else(|| Some(request.cwd.clone())),
        native_model: opened.model.clone().or_else(|| request.model.clone()),
        resume_cursor_json: serialize_cli_runtime_json(&json!({
            "threadId": opened.native_thread_id
        }))?,
        status: "active".to_owned(),
        created_at: existing
            .map(|binding| binding.created_at)
            .unwrap_or(request.opened_at),
        updated_at: request.opened_at,
    })
}

fn validate_open_request(request: &CodexThreadBindingOpenRequest) -> Result<()> {
    for (label, value) in [
        ("workspace_id", request.workspace_id.as_str()),
        ("thread_id", request.thread_id.as_str()),
        ("runtime_id", request.runtime_id.as_str()),
        ("runtime_kind", request.runtime_kind.as_str()),
        ("cwd", request.cwd.as_str()),
        ("approval_policy", request.approval_policy.as_str()),
        ("sandbox", request.sandbox.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("Codex thread binding request `{label}` cannot be empty");
        }
    }
    Ok(())
}

fn validate_existing_binding(
    existing: &CliRuntimeThreadBindingRecord,
    request: &CodexThreadBindingOpenRequest,
) -> Result<()> {
    if existing.workspace_id != request.workspace_id {
        bail!(
            "thread `{}` is bound to workspace `{}` not `{}`",
            request.thread_id,
            existing.workspace_id,
            request.workspace_id
        );
    }
    if existing.runtime_id != request.runtime_id || existing.runtime_kind != request.runtime_kind {
        bail!(
            "thread `{}` is bound to CLI runtime `{}`/`{}` not `{}`/`{}`",
            request.thread_id,
            existing.runtime_kind,
            existing.runtime_id,
            request.runtime_kind,
            request.runtime_id
        );
    }
    if existing.native_thread_id.trim().is_empty() {
        bail!(
            "thread `{}` has an empty Codex native thread binding",
            request.thread_id
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CodexThreadBindingOpenMode, CodexThreadBindingOpenRequest, CodexThreadOpenClient,
        open_codex_thread_binding,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use migration::{Migrator, MigratorTrait};
    use pioneer_cli_agent_runtime::codex::{CodexThreadOpenSnapshot, CodexThreadStartParams};
    use pioneer_crud::{CrudStore, NewCliRuntimeThreadBinding};
    use sea_orm::entity::prelude::DateTimeWithTimeZone;
    use sea_orm::{Database, DatabaseConnection};
    use serde_json::json;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Debug)]
    struct FakeCodexThreadClient {
        starts: Mutex<Vec<CodexThreadStartParams>>,
        resumes: Mutex<Vec<(String, CodexThreadStartParams)>>,
        start_result: Mutex<std::result::Result<CodexThreadOpenSnapshot, String>>,
        resume_result: Mutex<std::result::Result<CodexThreadOpenSnapshot, String>>,
    }

    #[async_trait]
    impl CodexThreadOpenClient for FakeCodexThreadClient {
        async fn start_codex_thread(
            &self,
            params: CodexThreadStartParams,
            _timeout: Duration,
        ) -> Result<CodexThreadOpenSnapshot> {
            self.starts.lock().expect("starts lock").push(params);
            self.start_result
                .lock()
                .expect("start result lock")
                .clone()
                .map_err(anyhow::Error::msg)
        }

        async fn resume_codex_thread(
            &self,
            native_thread_id: &str,
            params: CodexThreadStartParams,
            _timeout: Duration,
        ) -> Result<CodexThreadOpenSnapshot> {
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

    impl FakeCodexThreadClient {
        fn new() -> Self {
            Self {
                starts: Mutex::new(Vec::new()),
                resumes: Mutex::new(Vec::new()),
                start_result: Mutex::new(Ok(open_snapshot("codex-thread-started"))),
                resume_result: Mutex::new(Ok(open_snapshot("codex-thread-existing"))),
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

    fn open_request(thread_id: &str, opened_at: i64) -> CodexThreadBindingOpenRequest {
        CodexThreadBindingOpenRequest {
            workspace_id: "ws_cli_binding".to_owned(),
            thread_id: thread_id.to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            cwd: "/tmp/project".to_owned(),
            model: Some("gpt-5".to_owned()),
            approval_policy: "on-request".to_owned(),
            sandbox: "workspace-write".to_owned(),
            service_tier: None,
            request_timeout: Duration::from_secs(5),
            opened_at: unix_to_datetime(opened_at),
        }
    }

    fn open_snapshot(native_thread_id: &str) -> CodexThreadOpenSnapshot {
        CodexThreadOpenSnapshot {
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
    async fn cli_runtime_binding_codex_first_start_persists_thread_binding() {
        let (_connection, store) = setup_store().await;
        let client = FakeCodexThreadClient::new();

        let result = open_codex_thread_binding(&store, &client, open_request("thread_cli_a", 100))
            .await
            .expect("first open should succeed");

        assert_eq!(result.mode, CodexThreadBindingOpenMode::Started);
        assert_eq!(result.binding.native_thread_id, "codex-thread-started");
        assert_eq!(result.binding.native_cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(result.binding.native_model.as_deref(), Some("gpt-5"));
        assert_eq!(
            result.binding.resume_cursor_json,
            r#"{"threadId":"codex-thread-started"}"#
        );
        assert_eq!(client.starts.lock().expect("starts lock").len(), 1);
        assert!(client.resumes.lock().expect("resumes lock").is_empty());
    }

    #[tokio::test]
    async fn cli_runtime_binding_codex_resume_uses_stored_native_thread() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_resume".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "codex-thread-existing".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("gpt-5".to_owned()),
                resume_cursor_json: r#"{"threadId":"codex-thread-existing"}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("seed binding should persist");
        let client = FakeCodexThreadClient::new();

        let result =
            open_codex_thread_binding(&store, &client, open_request("thread_cli_resume", 200))
                .await
                .expect("resume should succeed");

        assert_eq!(result.mode, CodexThreadBindingOpenMode::Resumed);
        assert_eq!(result.binding.native_thread_id, "codex-thread-existing");
        assert_eq!(result.binding.created_at, opened_at);
        assert_eq!(result.binding.updated_at, unix_to_datetime(200));
        assert!(client.starts.lock().expect("starts lock").is_empty());
        let resumes = client.resumes.lock().expect("resumes lock");
        assert_eq!(resumes.len(), 1);
        assert_eq!(resumes[0].0, "codex-thread-existing");
    }

    #[tokio::test]
    async fn cli_runtime_binding_codex_resume_error_keeps_existing_binding() {
        let (_connection, store) = setup_store().await;
        let opened_at = unix_to_datetime(100);
        store
            .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                thread_id: "thread_cli_error".to_owned(),
                workspace_id: "ws_cli_binding".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "codex-thread-existing".to_owned(),
                native_session_id: None,
                native_root_thread_id: None,
                native_cwd: Some("/tmp/project".to_owned()),
                native_model: Some("gpt-5".to_owned()),
                resume_cursor_json: r#"{"threadId":"codex-thread-existing"}"#.to_owned(),
                status: "active".to_owned(),
                created_at: opened_at,
                updated_at: opened_at,
            })
            .await
            .expect("seed binding should persist");
        let client = FakeCodexThreadClient::new();
        client.set_resume_error("native thread missing");

        let error =
            open_codex_thread_binding(&store, &client, open_request("thread_cli_error", 200))
                .await
                .expect_err("resume failure should surface");
        assert!(
            format!("{error:#}").contains("failed to resume Codex native thread"),
            "resume error should be actionable"
        );

        let binding = store
            .get_cli_runtime_thread_binding("thread_cli_error")
            .await
            .expect("binding read should succeed")
            .expect("binding should remain");
        assert_eq!(binding.native_thread_id, "codex-thread-existing");
        assert_eq!(binding.updated_at, opened_at);
        assert!(client.starts.lock().expect("starts lock").is_empty());
        assert_eq!(client.resumes.lock().expect("resumes lock").len(), 1);
    }
}
