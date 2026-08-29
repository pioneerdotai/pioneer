use crate::context::{
    ExecCommandArgs, LocalShellPayload, ToolInvocation, ToolOutput, ToolPayload, WriteStdinArgs,
};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use crate::shell_format::{
    ExecModelPayload, ExecOutputStats, ExecPayloadInput, ExecStreamOutputStats,
    build_exec_model_payload, build_exec_model_payload_with_stats, render_exec_ui_text,
};
use crate::{
    NativeSandboxPrepareOutcome, NativeSandboxRequest, NonoSandboxBackend, ProcessSpawnPlan,
    WindowsRestrictedTokenBackend, build_process_spawn_plan, configure_nono_command,
    configure_windows_restricted_token_command, prepare_native_sandbox_backend,
};
use async_trait::async_trait;
use pioneer_protocol::{SandboxBackendKind, TurnExecutionSecuritySnapshot};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, sleep};

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_YIELD_MS: u64 = 120;
const MAX_ACTIVE_SESSIONS: usize = 64;
const SESSION_MAX_BUFFER_BYTES: usize = 512 * 1024;
const ONE_SHOT_MAX_STREAM_BYTES: usize = 512 * 1024;
const ONE_SHOT_MAX_TOTAL_BYTES: usize = 1024 * 1024;
const ONE_SHOT_ABSOLUTE_MAX_BYTES: usize = 64 * 1024 * 1024;
const ONE_SHOT_CHUNK_CHANNEL_CAPACITY: usize = 64;
const SESSION_TTL_MS: u64 = if cfg!(test) { 500 } else { 10 * 60 * 1000 };
const PROCESS_KILL_GRACE_MS: u64 = 300;
const ONE_SHOT_STREAM_LIMIT_ENV: &str = "PIONEER_EXEC_ONE_SHOT_MAX_STREAM_BYTES";
const ONE_SHOT_TOTAL_LIMIT_ENV: &str = "PIONEER_EXEC_ONE_SHOT_MAX_TOTAL_BYTES";

#[derive(Debug, Clone, Copy)]
struct OneShotOutputLimits {
    stream_bytes: usize,
    total_bytes: usize,
}

impl Default for OneShotOutputLimits {
    fn default() -> Self {
        let configured = |name: &str, fallback: usize| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .map(|value| value.min(ONE_SHOT_ABSOLUTE_MAX_BYTES))
                .unwrap_or(fallback)
        };
        let total_bytes = configured(ONE_SHOT_TOTAL_LIMIT_ENV, ONE_SHOT_MAX_TOTAL_BYTES);
        Self {
            stream_bytes: configured(ONE_SHOT_STREAM_LIMIT_ENV, ONE_SHOT_MAX_STREAM_BYTES)
                .min(total_bytes),
            total_bytes,
        }
    }
}

pub struct UnifiedExecHandler {
    sessions: Arc<Mutex<HashMap<u64, Arc<Mutex<ExecSession>>>>>,
    next_session_id: AtomicU64,
    one_shot_limits: OneShotOutputLimits,
}

struct ExecSession {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Arc<Mutex<SessionBuffer>>,
    stderr: Arc<Mutex<SessionBuffer>>,
    stdout_cursor: usize,
    stderr_cursor: usize,
    command: Vec<String>,
    started_at: Instant,
    last_touched: Instant,
    _runtime_temp_dir: Option<Arc<crate::process_policy::ProcessRuntimeTempDir>>,
}

impl Default for UnifiedExecHandler {
    fn default() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: AtomicU64::new(0),
            one_shot_limits: OneShotOutputLimits::default(),
        }
    }
}

impl Drop for UnifiedExecHandler {
    fn drop(&mut self) {
        if let Ok(sessions) = self.sessions.try_lock() {
            for session in sessions.values() {
                if let Ok(mut guard) = session.try_lock() {
                    terminate_child_process_now(&mut guard.child);
                }
            }
        }
    }
}

#[derive(Default)]
struct SessionBuffer {
    bytes: Vec<u8>,
    base_offset: usize,
}

impl SessionBuffer {
    fn append(&mut self, chunk: &[u8]) {
        self.bytes.extend_from_slice(chunk);
        let overflow = self.bytes.len().saturating_sub(SESSION_MAX_BUFFER_BYTES);
        if overflow > 0 {
            self.bytes.drain(0..overflow);
            self.base_offset = self.base_offset.saturating_add(overflow);
        }
    }

    fn read_from(&self, cursor: usize) -> (Vec<u8>, usize, bool) {
        let current_end = self.base_offset.saturating_add(self.bytes.len());
        let start_offset = cursor.max(self.base_offset);
        let start_index = start_offset.saturating_sub(self.base_offset);
        let lost_output = cursor < self.base_offset;
        (self.bytes[start_index..].to_vec(), current_end, lost_output)
    }
}

struct ExecToolOutput {
    success: bool,
    ui_text: String,
    payload: ExecModelPayload,
}

impl ToolOutput for ExecToolOutput {
    fn success(&self) -> bool {
        self.success
    }

    fn raw_text(&self) -> String {
        self.ui_text.clone()
    }

    fn raw_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.payload).unwrap_or_else(|_| serde_json::json!({}))
    }

    fn to_model_input_item(
        &self,
        call_id: &str,
        tool_name: &str,
    ) -> pioneer_provider::ModelInputItem {
        pioneer_provider::ModelInputItem::tool_result(
            call_id.to_owned(),
            tool_name.to_owned(),
            self.ui_text.clone(),
            Some(self.raw_json()),
        )
    }
}

#[async_trait]
impl ToolHandler for UnifiedExecHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let parsed_payload =
            parse_local_shell_payload(invocation.payload.clone(), invocation.tool_name.as_str())?;
        match parsed_payload {
            LocalShellPayload::ExecCommand(args) => {
                self.handle_exec_command(args, invocation, trace).await
            }
            LocalShellPayload::WriteStdin(args) => {
                self.handle_write_stdin(args, invocation.cancellation, trace)
                    .await
            }
        }
    }
}

impl UnifiedExecHandler {
    async fn handle_exec_command(
        &self,
        args: ExecCommandArgs,
        invocation: ToolInvocation,
        trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        self.cleanup_stale_sessions().await;
        let cancellation = invocation.cancellation.clone();
        let use_tty = args.tty.unwrap_or(false);
        if !use_tty {
            let result = run_one_shot(
                args,
                invocation.workdir.as_path(),
                &invocation.environment,
                invocation.execution_security_snapshot.as_ref(),
                &trace,
                cancellation,
                self.one_shot_limits,
            )
            .await?;
            return Ok(Box::new(result.into_tool_output()));
        }

        if self.sessions.lock().await.len() >= MAX_ACTIVE_SESSIONS {
            return Err(ToolError::execution_failed(format!(
                "too many active exec sessions (limit={MAX_ACTIVE_SESSIONS}); close existing sessions or wait for cleanup"
            )));
        }

        let process_plan = build_process_spawn_plan(
            invocation.execution_security_snapshot.as_ref(),
            invocation.workdir.as_path(),
            &args,
            &invocation.environment,
            DEFAULT_TIMEOUT_MS,
        )?;
        let (command_preview, mut command) = build_command(&args, process_plan.cwd.as_path())?;
        apply_process_spawn_plan_environment(&mut command, &process_plan);
        apply_shell_sandbox_backend(
            &mut command,
            invocation.execution_security_snapshot.as_ref(),
            &process_plan,
        )?;
        let session_started_at = Instant::now();

        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        let mut child = command.spawn().map_err(|error| {
            ToolError::execution_failed(format!("failed to spawn command: {error}"))
        })?;

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdin = child.stdin.take();
        let session_id = self
            .next_session_id
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let stdout_buffer = Arc::new(Mutex::new(SessionBuffer::default()));
        let stderr_buffer = Arc::new(Mutex::new(SessionBuffer::default()));
        if let Some(stdout) = stdout {
            spawn_reader(stdout, stdout_buffer.clone());
        }
        if let Some(stderr) = stderr {
            spawn_reader(stderr, stderr_buffer.clone());
        }

        let session = Arc::new(Mutex::new(ExecSession {
            child,
            stdin,
            stdout: stdout_buffer,
            stderr: stderr_buffer,
            stdout_cursor: 0,
            stderr_cursor: 0,
            command: command_preview.clone(),
            started_at: session_started_at,
            last_touched: Instant::now(),
            _runtime_temp_dir: process_plan.runtime_temp_dir.clone(),
        }));

        self.sessions
            .lock()
            .await
            .insert(session_id, session.clone());

        let session_cancelled = tokio::select! {
            _ = cancellation.cancelled() => true,
            _ = sleep(Duration::from_millis(args.yield_time_ms.unwrap_or(DEFAULT_YIELD_MS))) => false,
        };

        if session_cancelled {
            {
                let mut guard = session.lock().await;
                if let Err(error) = terminate_child_process(&mut guard.child).await {
                    tracing::error!(
                        session_id,
                        error = %format!("{error:#}"),
                        "failed to terminate cancelled shell session"
                    );
                }
            }
            self.sessions.lock().await.remove(&session_id);
            return Err(ToolError::cancelled("command cancelled"));
        }

        let initial = read_session_chunk(session.clone()).await?;
        emit_shell_chunk_delta(&trace, &initial.stdout, "stdout");
        emit_shell_chunk_delta(&trace, &initial.stderr, "stderr");
        let state = {
            let mut guard = session.lock().await;
            let status = guard.child.try_wait().map_err(|error| {
                ToolError::execution_failed(format!("failed to poll process state: {error}"))
            })?;
            ProcessState::from_status(status)
        };

        if state.finished {
            self.sessions.lock().await.remove(&session_id);
        }

        let payload = build_exec_model_payload(ExecPayloadInput {
            exit_code: state.exit_code,
            timed_out: false,
            duration_ms: session_started_at.elapsed().as_millis() as u64,
            stdout: initial.stdout,
            stderr: initial.stderr,
            session_id: Some(session_id),
            command: command_preview,
            max_output_tokens: args.max_output_tokens,
            force_truncated_stdout: initial.stdout_lost_output,
            force_truncated_stderr: initial.stderr_lost_output,
        });

        Ok(Box::new(ExecToolOutput {
            success: state.success,
            ui_text: render_exec_ui_text(&payload),
            payload,
        }))
    }

    async fn handle_write_stdin(
        &self,
        args: WriteStdinArgs,
        cancellation: tokio_util::sync::CancellationToken,
        trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        self.cleanup_stale_sessions().await;
        let session = self
            .sessions
            .lock()
            .await
            .get(&args.session_id)
            .cloned()
            .ok_or_else(|| {
                ToolError::NotFound(format!("session {} was not found", args.session_id))
            })?;

        if let Some(chars) = args.chars.as_deref() {
            let mut guard = session.lock().await;
            guard.last_touched = Instant::now();
            match guard.stdin.as_mut() {
                Some(stdin) => {
                    stdin.write_all(chars.as_bytes()).await.map_err(|error| {
                        ToolError::execution_failed(format!("failed to write stdin: {error}"))
                    })?;
                    stdin.flush().await.map_err(|error| {
                        ToolError::execution_failed(format!("failed to flush stdin: {error}"))
                    })?;
                }
                None => {
                    return Err(ToolError::execution_failed(
                        "stdin is closed for this session".to_owned(),
                    ));
                }
            }
        }

        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(ToolError::cancelled("write_stdin cancelled"));
            }
            _ = sleep(Duration::from_millis(args.yield_time_ms.unwrap_or(DEFAULT_YIELD_MS))) => {}
        }

        let chunk = read_session_chunk(session.clone()).await?;
        emit_shell_chunk_delta(&trace, &chunk.stdout, "stdout");
        emit_shell_chunk_delta(&trace, &chunk.stderr, "stderr");

        let (state, command_preview, started_at) = {
            let mut guard = session.lock().await;
            guard.last_touched = Instant::now();
            let status = guard.child.try_wait().map_err(|error| {
                ToolError::execution_failed(format!("failed to poll process state: {error}"))
            })?;
            (
                ProcessState::from_status(status),
                guard.command.clone(),
                guard.started_at,
            )
        };

        if state.finished {
            self.sessions.lock().await.remove(&args.session_id);
        }

        let payload = build_exec_model_payload(ExecPayloadInput {
            exit_code: state.exit_code,
            timed_out: false,
            duration_ms: started_at.elapsed().as_millis() as u64,
            stdout: chunk.stdout,
            stderr: chunk.stderr,
            session_id: Some(args.session_id),
            command: command_preview,
            max_output_tokens: args.max_output_tokens,
            force_truncated_stdout: chunk.stdout_lost_output,
            force_truncated_stderr: chunk.stderr_lost_output,
        });

        Ok(Box::new(ExecToolOutput {
            success: state.success,
            ui_text: render_exec_ui_text(&payload),
            payload,
        }))
    }

    async fn cleanup_stale_sessions(&self) {
        let snapshot = {
            let sessions = self.sessions.lock().await;
            sessions
                .iter()
                .map(|(id, session)| (*id, session.clone()))
                .collect::<Vec<_>>()
        };

        let mut to_remove = Vec::new();
        for (session_id, session) in snapshot {
            let mut guard = session.lock().await;
            let expired = guard.last_touched.elapsed() >= Duration::from_millis(SESSION_TTL_MS);
            let finished = guard.child.try_wait().ok().flatten().is_some();

            if finished || expired {
                if !finished {
                    if let Err(error) = terminate_child_process(&mut guard.child).await {
                        tracing::error!(
                            session_id,
                            error = %format!("{error:#}"),
                            "failed to terminate stale shell session"
                        );
                    }
                }
                to_remove.push(session_id);
            }
        }

        if !to_remove.is_empty() {
            let mut sessions = self.sessions.lock().await;
            for session_id in to_remove {
                sessions.remove(&session_id);
            }
        }
    }
}

fn parse_local_shell_payload(
    payload: ToolPayload,
    tool_name: &str,
) -> Result<LocalShellPayload, ToolError> {
    match payload {
        ToolPayload::LocalShell(payload) => Ok(payload),
        ToolPayload::Function { arguments } => {
            if tool_name == "write_stdin" {
                let args =
                    serde_json::from_value::<WriteStdinArgs>(arguments).map_err(|error| {
                        ToolError::invalid_arguments(format!(
                            "failed to parse write_stdin arguments: {error}"
                        ))
                    })?;
                Ok(LocalShellPayload::WriteStdin(args))
            } else {
                let args =
                    serde_json::from_value::<ExecCommandArgs>(arguments).map_err(|error| {
                        ToolError::invalid_arguments(format!(
                            "failed to parse exec arguments: {error}"
                        ))
                    })?;
                Ok(LocalShellPayload::ExecCommand(args))
            }
        }
        other => Err(ToolError::invalid_arguments(format!(
            "unsupported payload for unified exec handler: {:?}",
            other.log_payload()
        ))),
    }
}

struct OneShotCommandResult {
    success: bool,
    ui_text: String,
    payload: ExecModelPayload,
}

impl OneShotCommandResult {
    fn into_tool_output(self) -> ExecToolOutput {
        ExecToolOutput {
            success: self.success,
            ui_text: self.ui_text,
            payload: self.payload,
        }
    }
}

impl From<OneShotCommandResult> for ExecToolOutput {
    fn from(value: OneShotCommandResult) -> Self {
        value.into_tool_output()
    }
}

struct SessionChunk {
    stdout: String,
    stderr: String,
    stdout_lost_output: bool,
    stderr_lost_output: bool,
}

#[derive(Debug)]
struct ProcessState {
    finished: bool,
    success: bool,
    exit_code: Option<i32>,
}

impl ProcessState {
    fn from_status(status: Option<ExitStatus>) -> Self {
        let Some(status) = status else {
            return Self {
                finished: false,
                success: true,
                exit_code: None,
            };
        };

        Self {
            finished: true,
            success: status.success(),
            exit_code: status.code(),
        }
    }
}

async fn run_one_shot(
    args: ExecCommandArgs,
    base_dir: &Path,
    environment: &BTreeMap<String, String>,
    execution_security_snapshot: Option<&pioneer_protocol::TurnExecutionSecuritySnapshot>,
    trace: &crate::events::ToolEventTrace,
    cancellation: tokio_util::sync::CancellationToken,
    configured_limits: OneShotOutputLimits,
) -> Result<OneShotCommandResult, ToolError> {
    let started_at = Instant::now();
    let process_plan = build_process_spawn_plan(
        execution_security_snapshot,
        base_dir,
        &args,
        environment,
        DEFAULT_TIMEOUT_MS,
    )?;
    let (command_preview, mut command) = build_command(&args, process_plan.cwd.as_path())?;
    apply_process_spawn_plan_environment(&mut command, &process_plan);
    apply_shell_sandbox_backend(&mut command, execution_security_snapshot, &process_plan)?;

    let timeout_ms = process_plan.timeout_ms;
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| {
        ToolError::execution_failed(format!("failed to spawn command: {error}"))
    })?;
    let process_id = child.id();

    let stdout_reader = child.stdout.take().ok_or_else(|| {
        ToolError::internal("stdout pipe was unexpectedly unavailable".to_owned())
    })?;
    let stderr_reader = child.stderr.take().ok_or_else(|| {
        ToolError::internal("stderr pipe was unexpectedly unavailable".to_owned())
    })?;

    let (stream_limit, total_limit) =
        one_shot_output_limits(args.max_output_tokens, configured_limits);
    let output_budget = Arc::new(OneShotOutputBudget::new(stream_limit, total_limit));
    let (chunk_tx, mut chunk_rx) =
        mpsc::channel::<ShellChunkEvent>(ONE_SHOT_CHUNK_CHANNEL_CAPACITY);
    let stdout_task = tokio::spawn(read_stream_with_chunks(
        stdout_reader,
        "stdout",
        chunk_tx.clone(),
        output_budget.clone(),
    ));
    let stderr_task = tokio::spawn(read_stream_with_chunks(
        stderr_reader,
        "stderr",
        chunk_tx,
        output_budget,
    ));

    let mut stdout_buf = String::new();
    let mut stderr_buf = String::new();
    let wait_deadline = tokio::time::sleep(Duration::from_millis(timeout_ms));
    tokio::pin!(wait_deadline);

    let mut status_opt: Option<ExitStatus> = None;
    let mut timed_out = false;
    let mut cancelled = false;
    let mut limit_termination_started = false;
    let mut stdout_truncated = false;
    let mut stderr_truncated = false;

    loop {
        if status_opt.is_some()
            && stdout_task.is_finished()
            && stderr_task.is_finished()
            && chunk_rx.is_empty()
        {
            break;
        }

        tokio::select! {
            _ = cancellation.cancelled() => {
                cancelled = true;
                if status_opt.is_none() {
                    status_opt = Some(terminate_child_process(&mut child).await?);
                } else {
                    kill_process_tree(process_id, true);
                }
                stdout_task.abort();
                stderr_task.abort();
                break;
            }
            _ = &mut wait_deadline, if status_opt.is_none() => {
                timed_out = true;
                status_opt = Some(terminate_child_process(&mut child).await?);
            }
            wait_result = child.wait(), if status_opt.is_none() => {
                let status = wait_result.map_err(|error| {
                    ToolError::execution_failed(format!("failed to wait for command: {error}"))
                })?;
                status_opt = Some(status);
                // A one-shot invocation owns its whole process group. Do not
                // let a detached descendant retain stdout/stderr indefinitely
                // after the direct child exits.
                kill_process_tree(process_id, true);
            }
            chunk = chunk_rx.recv() => {
                match chunk {
                    Some(ShellChunkEvent::Data { stream, text }) => {
                        let text = String::from_utf8_lossy(text.as_slice()).into_owned();
                        if stream == "stdout" {
                            stdout_buf.push_str(text.as_str());
                        } else {
                            stderr_buf.push_str(text.as_str());
                        }
                        trace.emit_output_chunk_delta(
                            1,
                            shell_delta_stream(stream),
                            text,
                            false,
                        );
                    }
                    Some(ShellChunkEvent::LimitExceeded { stream }) => {
                        if stream == "stdout" {
                            stdout_truncated = true;
                        } else {
                            stderr_truncated = true;
                        }
                        if status_opt.is_none() && !limit_termination_started {
                            limit_termination_started = true;
                            status_opt = Some(terminate_child_process(&mut child).await?);
                        }
                    }
                    Some(ShellChunkEvent::Closed { stream }) => {
                        let _ = stream;
                    }
                    None => {
                        if status_opt.is_some() && stdout_task.is_finished() && stderr_task.is_finished() {
                            break;
                        }
                    }
                }
            }
        }
    }

    if cancelled {
        return Err(ToolError::cancelled("command cancelled"));
    }

    let stdout_summary = stdout_task
        .await
        .map_err(|error| ToolError::internal(format!("stdout task failed: {error}")))?
        .map_err(|error| ToolError::execution_failed(format!("failed to read stdout: {error}")))?;
    let stderr_summary = stderr_task
        .await
        .map_err(|error| ToolError::internal(format!("stderr task failed: {error}")))?
        .map_err(|error| ToolError::execution_failed(format!("failed to read stderr: {error}")))?;
    stdout_truncated |= stdout_summary.truncated;
    stderr_truncated |= stderr_summary.truncated;

    let status = status_opt.ok_or_else(|| {
        ToolError::execution_failed("command finished without exit status".to_owned())
    })?;

    let output_stats = ExecOutputStats {
        stdout: ExecStreamOutputStats {
            bytes_seen: stdout_summary.bytes_seen,
            bytes_retained: stdout_summary.bytes_retained,
            bytes_dropped: stdout_summary
                .bytes_seen
                .saturating_sub(stdout_summary.bytes_retained),
        },
        stderr: ExecStreamOutputStats {
            bytes_seen: stderr_summary.bytes_seen,
            bytes_retained: stderr_summary.bytes_retained,
            bytes_dropped: stderr_summary
                .bytes_seen
                .saturating_sub(stderr_summary.bytes_retained),
        },
        truncation_method: "bounded_head_then_drain".to_owned(),
        full_output_available: false,
    };
    let payload = build_exec_model_payload_with_stats(
        ExecPayloadInput {
            exit_code: status.code(),
            timed_out,
            duration_ms: started_at.elapsed().as_millis() as u64,
            stdout: stdout_buf,
            stderr: stderr_buf,
            session_id: None,
            command: command_preview,
            max_output_tokens: args.max_output_tokens,
            force_truncated_stdout: stdout_truncated,
            force_truncated_stderr: stderr_truncated,
        },
        Some(output_stats),
    );

    let success = status.success() && !timed_out;
    let ui_text = render_exec_ui_text(&payload);

    Ok(OneShotCommandResult {
        success,
        ui_text,
        payload,
    })
}

enum ShellChunkEvent {
    Data { stream: &'static str, text: Vec<u8> },
    LimitExceeded { stream: &'static str },
    Closed { stream: &'static str },
}

#[derive(Debug, Clone, Copy, Default)]
struct StreamReadSummary {
    bytes_seen: usize,
    bytes_retained: usize,
    truncated: bool,
}

struct OneShotOutputBudget {
    stream_limit: usize,
    total_limit: usize,
    total_seen: AtomicU64,
}

impl OneShotOutputBudget {
    fn new(stream_limit: usize, total_limit: usize) -> Self {
        Self {
            stream_limit,
            total_limit,
            total_seen: AtomicU64::new(0),
        }
    }

    fn claim(&self, stream_seen: usize, requested: usize) -> (usize, bool) {
        let total_before = self
            .total_seen
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_add(requested as u64))
            })
            .unwrap_or(u64::MAX) as usize;
        let stream_remaining = self.stream_limit.saturating_sub(stream_seen);
        let total_remaining = self.total_limit.saturating_sub(total_before);
        let retained = requested.min(stream_remaining).min(total_remaining);
        (retained, retained < requested)
    }
}

fn one_shot_output_limits(
    max_output_tokens: Option<usize>,
    configured: OneShotOutputLimits,
) -> (usize, usize) {
    let token_limit = max_output_tokens
        .map(|tokens| tokens.saturating_mul(4))
        .unwrap_or(usize::MAX);
    let total_limit = configured.total_bytes.min(token_limit);
    (configured.stream_bytes.min(total_limit), total_limit)
}

async fn read_stream_with_chunks<R>(
    mut reader: R,
    stream: &'static str,
    tx: mpsc::Sender<ShellChunkEvent>,
    budget: Arc<OneShotOutputBudget>,
) -> std::io::Result<StreamReadSummary>
where
    R: AsyncRead + Unpin,
{
    let mut summary = StreamReadSummary::default();
    let mut chunk = [0u8; 4096];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            let _ = tx.send(ShellChunkEvent::Closed { stream }).await;
            break;
        }
        summary.bytes_seen = summary.bytes_seen.saturating_add(read);
        let (retained, exceeded) = budget.claim(summary.bytes_seen.saturating_sub(read), read);
        if retained > 0 {
            summary.bytes_retained = summary.bytes_retained.saturating_add(retained);
            let _ = tx
                .send(ShellChunkEvent::Data {
                    stream,
                    text: chunk[..retained].to_vec(),
                })
                .await;
        }
        if exceeded && !summary.truncated {
            summary.truncated = true;
            let _ = tx.send(ShellChunkEvent::LimitExceeded { stream }).await;
        }
    }
    Ok(summary)
}

fn apply_shell_sandbox_backend(
    command: &mut Command,
    execution_security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
    process_plan: &ProcessSpawnPlan,
) -> Result<(), ToolError> {
    let Some(snapshot) = execution_security_snapshot else {
        return Err(ToolError::Rejected(
            "missing turn execution security snapshot; refusing to spawn shell command without resolved sandbox policy".to_owned(),
        ));
    };
    let Some(backend_kind) = snapshot.backend.sandbox_backend else {
        return Ok(());
    };

    match backend_kind {
        SandboxBackendKind::Nono => {
            let backend = NonoSandboxBackend::new();
            let request = NativeSandboxRequest {
                snapshot,
                process_plan,
                workspace_roots: &[],
                execution_label: "shell",
            };
            match prepare_native_sandbox_backend(&backend, &request)? {
                NativeSandboxPrepareOutcome::Ready(_) => {
                    configure_nono_command(command, snapshot, process_plan)
                }
                NativeSandboxPrepareOutcome::Degraded { reason, .. }
                | NativeSandboxPrepareOutcome::Unavailable { reason, .. } => {
                    tracing::warn!(
                        reason = %reason,
                        "optional nono sandbox backend is not active for shell command"
                    );
                    Ok(())
                }
            }
        }
        SandboxBackendKind::WindowsRestrictedToken => {
            let backend = WindowsRestrictedTokenBackend::new();
            let request = NativeSandboxRequest {
                snapshot,
                process_plan,
                workspace_roots: &[],
                execution_label: "shell",
            };
            match prepare_native_sandbox_backend(&backend, &request)? {
                NativeSandboxPrepareOutcome::Ready(_) => {
                    configure_windows_restricted_token_command(command, snapshot, process_plan)
                }
                NativeSandboxPrepareOutcome::Degraded { reason, .. }
                | NativeSandboxPrepareOutcome::Unavailable { reason, .. } => {
                    tracing::warn!(
                        reason = %reason,
                        "optional windows restricted-token sandbox backend is not active for shell command"
                    );
                    Ok(())
                }
            }
        }
        SandboxBackendKind::ProviderNative => Err(ToolError::Rejected(
            "provider-native sandbox cannot protect Pioneer native shell execution".to_owned(),
        )),
    }
}

fn build_command(args: &ExecCommandArgs, cwd: &Path) -> Result<(Vec<String>, Command), ToolError> {
    if let Some(command) = args.command.as_ref()
        && !command.is_empty()
    {
        let command = maybe_prefix_pipefail_for_argv(command.as_slice());
        let mut cmd = Command::new(command[0].as_str());
        cmd.args(&command[1..]);
        cmd.current_dir(cwd);
        configure_process_group(&mut cmd)?;
        // The async owner is the cancellation fence for one-shot commands.
        // If that owner is dropped while cleanup is being unwound, Tokio must
        // still terminate the child instead of detaching it.
        cmd.kill_on_drop(true);
        return Ok((command, cmd));
    }

    Err(ToolError::invalid_arguments("`command` argv is required"))
}

fn maybe_prefix_pipefail_for_argv(command: &[String]) -> Vec<String> {
    if command.is_empty() || !supports_pipefail(command[0].as_str()) {
        return command.to_vec();
    }

    let mut rewritten = command.to_vec();
    let script_idx = rewritten.iter().enumerate().skip(1).find_map(|(idx, arg)| {
        if arg == "-c" || (arg.starts_with('-') && arg.len() > 2 && arg.contains('c')) {
            (idx + 1 < rewritten.len()).then_some(idx + 1)
        } else {
            None
        }
    });

    let Some(script_idx) = script_idx else {
        return rewritten;
    };

    rewritten[script_idx] =
        maybe_prefix_pipefail(rewritten[script_idx].as_str(), rewritten[0].as_str());
    rewritten
}

fn maybe_prefix_pipefail(script: &str, shell: &str) -> String {
    if !supports_pipefail(shell) {
        return script.to_owned();
    }

    let trimmed = script.trim_start();
    if trimmed.starts_with("set -o pipefail") {
        return script.to_owned();
    }

    format!("set -o pipefail; {script}")
}

fn supports_pipefail(shell: &str) -> bool {
    let shell_name = shell_basename(shell);
    matches!(shell_name, "zsh" | "bash" | "ksh" | "mksh")
}

fn shell_basename(shell: &str) -> &str {
    Path::new(shell)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(shell)
}

fn apply_process_spawn_plan_environment(command: &mut Command, plan: &ProcessSpawnPlan) {
    if !plan.inherit_environment {
        command.env_clear();
    }
    for key in &plan.removed_environment {
        command.env_remove(key);
    }
    command.envs(plan.environment.iter());
}

fn configure_process_group(command: &mut Command) -> Result<(), ToolError> {
    #[cfg(unix)]
    {
        // Spawn each command in its own process group so we can terminate descendants safely.
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(CREATE_NEW_PROCESS_GROUP);
    }

    let _ = command;
    Ok(())
}

async fn terminate_child_process(child: &mut Child) -> Result<ExitStatus, ToolError> {
    let process_id = child.id();
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let pgid = -(pid as i32);
        unsafe {
            let _ = libc::kill(pgid, libc::SIGTERM);
        }
    }
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        let _ = terminate_windows_process_tree(pid, false);
    }

    let waited =
        tokio::time::timeout(Duration::from_millis(PROCESS_KILL_GRACE_MS), child.wait()).await;
    match waited {
        Ok(Ok(status)) => {
            // The parent can exit on SIGTERM while a descendant ignores it.
            // Always fence the original process group before returning.
            kill_process_tree(process_id, true);
            Ok(status)
        }
        Ok(Err(error)) => Err(ToolError::execution_failed(format!(
            "failed to wait for process termination: {error}"
        ))),
        Err(_) => {
            kill_process_tree(process_id, true);
            let _ = child.start_kill();
            child.wait().await.map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to wait after SIGKILL termination: {error}"
                ))
            })
        }
    }
}

fn terminate_child_process_now(child: &mut Child) {
    terminate_process_group_now(child);
    let _ = child.start_kill();
}

fn terminate_process_group_now(child: &mut Child) {
    kill_process_tree(child.id(), true);
}

fn kill_process_tree(process_id: Option<u32>, force: bool) {
    #[cfg(unix)]
    if let Some(pid) = process_id {
        let pgid = -(pid as i32);
        unsafe {
            let _ = libc::kill(pgid, if force { libc::SIGKILL } else { libc::SIGTERM });
        }
    }
    #[cfg(windows)]
    if let Some(pid) = process_id {
        let _ = terminate_windows_process_tree(pid, force);
    }
}

#[cfg(windows)]
fn terminate_windows_process_tree(pid: u32, force: bool) -> Result<(), ToolError> {
    let mut command = std::process::Command::new("taskkill");
    command.arg("/PID").arg(pid.to_string()).arg("/T");
    if force {
        command.arg("/F");
    }

    let output = command.output().map_err(|error| {
        ToolError::execution_failed(format!("failed to execute taskkill: {error}"))
    })?;
    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_lowercase();
    let stderr = String::from_utf8_lossy(&output.stderr).to_lowercase();
    let gone_patterns = ["not found", "no running instance", "process does not exist"];
    if gone_patterns
        .iter()
        .any(|pattern| stdout.contains(pattern) || stderr.contains(pattern))
    {
        return Ok(());
    }

    Err(ToolError::execution_failed(format!(
        "taskkill failed (exit={}): {}",
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

fn spawn_reader<R>(mut reader: R, target: Arc<Mutex<SessionBuffer>>)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut chunk = vec![0u8; 4096];
        loop {
            let read = match reader.read(&mut chunk).await {
                Ok(read) => read,
                Err(_) => return,
            };
            if read == 0 {
                return;
            }
            target.lock().await.append(&chunk[..read]);
        }
    });
}

fn emit_shell_chunk_delta(trace: &crate::events::ToolEventTrace, text: &str, stream: &str) {
    if text.is_empty() {
        return;
    }
    trace.emit_output_chunk_delta(1, shell_delta_stream(stream), text.to_owned(), false);
}

fn shell_delta_stream(stream: &str) -> pioneer_protocol::ItemDeltaStream {
    match stream {
        "stderr" => pioneer_protocol::ItemDeltaStream::Stderr,
        _ => pioneer_protocol::ItemDeltaStream::Stdout,
    }
}

async fn read_session_chunk(session: Arc<Mutex<ExecSession>>) -> Result<SessionChunk, ToolError> {
    let (
        stdout_slice,
        stderr_slice,
        stdout_cursor,
        stderr_cursor,
        stdout_lost_output,
        stderr_lost_output,
    ) = {
        let guard = session.lock().await;
        let stdout = guard.stdout.lock().await;
        let stderr = guard.stderr.lock().await;

        let (stdout_slice, stdout_cursor, stdout_lost_output) =
            stdout.read_from(guard.stdout_cursor);
        let (stderr_slice, stderr_cursor, stderr_lost_output) =
            stderr.read_from(guard.stderr_cursor);

        (
            stdout_slice,
            stderr_slice,
            stdout_cursor,
            stderr_cursor,
            stdout_lost_output,
            stderr_lost_output,
        )
    };

    {
        let mut guard = session.lock().await;
        guard.stdout_cursor = stdout_cursor;
        guard.stderr_cursor = stderr_cursor;
        guard.last_touched = Instant::now();
    }

    Ok(SessionChunk {
        stdout: String::from_utf8_lossy(stdout_slice.as_slice()).to_string(),
        stderr: String::from_utf8_lossy(stderr_slice.as_slice()).to_string(),
        stdout_lost_output,
        stderr_lost_output,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{ToolCallSource, ToolInvocation};
    use crate::events::ToolEventBus;
    use pioneer_protocol::{
        SandboxBackendKind, TurnExecutionSecuritySnapshot, TurnPermissionMode,
        TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
    };

    fn invocation(tool_name: &str, payload: ToolPayload) -> ToolInvocation {
        let workdir = std::env::current_dir().expect("cwd must be available");
        ToolInvocation {
            call_id: "call_1".to_owned(),
            tool_name: tool_name.to_owned(),
            source: ToolCallSource::Model,
            payload,
            workdir: workdir.clone(),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: Some(shell_security_snapshot(
                TurnPermissionMode::FullAccess,
                workdir.as_path(),
            )),
            apply_patch_preflight: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn parse_session_id(output: &str) -> u64 {
        output
            .lines()
            .find_map(|line| line.strip_prefix("Session ID: "))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .expect("session id must be present in output")
    }

    fn trace(tool_name: &str) -> crate::events::ToolEventTrace {
        ToolEventBus::default().start_trace("turn_test", "call_1", tool_name)
    }

    fn shell_security_snapshot(
        mode: TurnPermissionMode,
        cwd: &Path,
    ) -> TurnExecutionSecuritySnapshot {
        match mode {
            TurnPermissionMode::FullAccess => {
                TurnExecutionSecuritySnapshot::unrestricted_full_access(cwd.to_string_lossy(), 1)
            }
            TurnPermissionMode::AutoAcceptEdits => TurnExecutionSecuritySnapshot::workspace_write(
                TurnPermissionProfileSnapshot::from_mode(
                    mode,
                    TurnPermissionProfileSource::Composer,
                ),
                cwd.to_string_lossy(),
                vec![
                    pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                        pioneer_protocol::TurnFilesystemAccess::Write,
                        cwd.to_string_lossy(),
                    ),
                ],
                1,
            ),
            TurnPermissionMode::Supervised => TurnExecutionSecuritySnapshot::read_only(
                TurnPermissionProfileSnapshot::from_mode(
                    mode,
                    TurnPermissionProfileSource::Composer,
                ),
                cwd.to_string_lossy(),
                vec![
                    pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                        pioneer_protocol::TurnFilesystemAccess::Read,
                        cwd.to_string_lossy(),
                    ),
                ],
                1,
            ),
        }
    }

    #[tokio::test]
    async fn exec_command_receives_invocation_environment() {
        let handler = UnifiedExecHandler::default();
        let mut tool_invocation = invocation(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "printf %s \"$PIONEER_TEST_OUTPUT_DIR\"".to_owned(),
                ]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: Some(false),
            })),
        );
        tool_invocation.environment.insert(
            "PIONEER_TEST_OUTPUT_DIR".to_owned(),
            "/tmp/pioneer-output-test".to_owned(),
        );

        let output = handler
            .handle(tool_invocation, trace("exec_command"))
            .await
            .expect("exec command should execute");

        assert!(
            output.raw_text().contains("/tmp/pioneer-output-test"),
            "expected invocation env in command output: {}",
            output.raw_text()
        );
    }

    #[tokio::test]
    async fn shell_security_full_access_runs_with_unrestricted_backend() {
        let handler = UnifiedExecHandler::default();
        let mut tool_invocation = invocation(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "printf full-access-ok".to_owned(),
                ]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: Some(false),
            })),
        );
        tool_invocation.execution_security_snapshot = Some(shell_security_snapshot(
            TurnPermissionMode::FullAccess,
            tool_invocation.workdir.as_path(),
        ));

        let output = handler
            .handle(tool_invocation, trace("exec_command"))
            .await
            .expect("full access command should run");

        assert!(output.success(), "full access output should succeed");
        assert!(
            output.raw_text().contains("full-access-ok"),
            "unexpected full access output: {}",
            output.raw_text()
        );
    }

    #[tokio::test]
    async fn shell_security_required_windows_backend_fails_before_spawn() {
        let handler = UnifiedExecHandler::default();
        let mut tool_invocation = invocation(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec![
                    "definitely-not-a-real-pioneer-command".to_owned(),
                    "would-have-spawned".to_owned(),
                ]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: Some(false),
            })),
        );
        let mut snapshot = shell_security_snapshot(
            TurnPermissionMode::AutoAcceptEdits,
            tool_invocation.workdir.as_path(),
        );
        snapshot.backend.sandbox_backend = Some(SandboxBackendKind::WindowsRestrictedToken);
        snapshot.sandbox.backend_preference = vec![SandboxBackendKind::WindowsRestrictedToken];
        tool_invocation.execution_security_snapshot = Some(snapshot);

        let result = handler.handle(tool_invocation, trace("exec_command")).await;
        let error = match result {
            Ok(output) => panic!(
                "required unavailable sandbox should fail before spawn, got output: {}",
                output.raw_text()
            ),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("required sandbox backend")
                || error
                    .to_string()
                    .contains("windows restricted-token backend"),
            "unexpected error: {error}"
        );
        assert!(
            !error.to_string().contains("No such file")
                && !error.to_string().contains("os error 2"),
            "error should come from sandbox before command spawn: {error}"
        );
    }

    #[tokio::test]
    async fn shell_security_provider_native_snapshot_cannot_use_native_spawn_path() {
        let handler = UnifiedExecHandler::default();
        let mut tool_invocation = invocation(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec!["sh".to_owned(), "-c".to_owned(), "echo no".to_owned()]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: Some(false),
            })),
        );
        let mut snapshot = shell_security_snapshot(
            TurnPermissionMode::FullAccess,
            tool_invocation.workdir.as_path(),
        );
        snapshot.backend.sandbox_backend = Some(SandboxBackendKind::ProviderNative);
        snapshot.sandbox.backend_preference = vec![SandboxBackendKind::ProviderNative];
        tool_invocation.execution_security_snapshot = Some(snapshot);

        let result = handler.handle(tool_invocation, trace("exec_command")).await;
        let error = match result {
            Ok(output) => panic!(
                "provider-native snapshot should not run through native shell, got output: {}",
                output.raw_text()
            ),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("provider-native sandbox cannot protect"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn unified_exec_session_roundtrip_with_write_stdin() {
        let handler = UnifiedExecHandler::default();

        let exec_args = ExecCommandArgs {
            command: Some(vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "read line; echo ACK:$line".to_owned(),
            ]),
            workdir: None,
            timeout_ms: None,
            max_output_tokens: None,
            yield_time_ms: Some(50),
            tty: Some(true),
        };
        let exec_output = handler
            .handle(
                invocation(
                    "exec_command",
                    ToolPayload::LocalShell(LocalShellPayload::ExecCommand(exec_args)),
                ),
                trace("exec_command"),
            )
            .await
            .expect("exec command should start a session");
        let session_id = parse_session_id(exec_output.raw_text().as_str());

        let write_output = handler
            .handle(
                invocation(
                    "write_stdin",
                    ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                        session_id,
                        chars: Some("ping\n".to_owned()),
                        yield_time_ms: Some(150),
                        max_output_tokens: None,
                    })),
                ),
                trace("write_stdin"),
            )
            .await
            .expect("write_stdin should return command output");
        let body = write_output.raw_text();
        assert!(
            body.contains("ACK:ping"),
            "expected child process response in output: {body}"
        );
        assert!(
            body.contains("Exit Code: 0"),
            "expected process to exit cleanly: {body}"
        );

        let missing_session = handler
            .handle(
                invocation(
                    "write_stdin",
                    ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                        session_id,
                        chars: None,
                        yield_time_ms: Some(10),
                        max_output_tokens: None,
                    })),
                ),
                trace("write_stdin"),
            )
            .await;
        match missing_session {
            Ok(_) => panic!("session should be removed after process exit"),
            Err(error) => assert!(matches!(error, ToolError::NotFound(_))),
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn restricted_tty_keeps_spawn_sandbox_across_write_stdin() {
        if !nono::Sandbox::is_supported() {
            return;
        }

        let current = std::env::current_dir().expect("test cwd");
        let fixture = tempfile::tempdir_in(current).expect("tty sandbox fixture");
        let workspace = fixture.path().join("workspace");
        let outside = fixture.path().join("outside");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(outside.as_path()).expect("outside");
        let outside_secret = outside.join("secret.txt");
        std::fs::write(outside_secret.as_path(), "OUTSIDE_SECRET\n").expect("outside fixture");

        let handler = UnifiedExecHandler::default();
        let mut exec_invocation = invocation(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "read target; if cat \"$target\" 2>/dev/null; then echo LEAKED; else echo DENIED; fi; printf WORKSPACE_OK > allowed.txt; cat allowed.txt".to_owned(),
                ]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: Some(50),
                tty: Some(true),
            })),
        );
        exec_invocation.workdir = workspace.clone();
        exec_invocation.execution_security_snapshot = Some(shell_security_snapshot(
            TurnPermissionMode::AutoAcceptEdits,
            workspace.as_path(),
        ));

        let exec_output = handler
            .handle(exec_invocation, trace("exec_command"))
            .await
            .expect("restricted tty should start");
        let session_id = parse_session_id(exec_output.raw_text().as_str());

        let mut write_invocation = invocation(
            "write_stdin",
            ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                session_id,
                chars: Some(format!("{}\n", outside_secret.display())),
                yield_time_ms: Some(250),
                max_output_tokens: None,
            })),
        );
        write_invocation.workdir = workspace.clone();
        write_invocation.execution_security_snapshot = Some(shell_security_snapshot(
            TurnPermissionMode::AutoAcceptEdits,
            workspace.as_path(),
        ));

        let output = handler
            .handle(write_invocation, trace("write_stdin"))
            .await
            .expect("write_stdin should interact with the live restricted process");
        let text = output.raw_text();
        let stdout = output
            .raw_json()
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        assert!(text.contains("DENIED"), "outside read must fail: {text}");
        assert!(
            text.contains("WORKSPACE_OK"),
            "workspace write/read should remain available: {text}"
        );
        assert!(
            !stdout.contains("OUTSIDE_SECRET") && !stdout.contains("LEAKED"),
            "stdin must not widen the spawn sandbox: {stdout}"
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join("allowed.txt"))
                .expect("workspace output should exist"),
            "WORKSPACE_OK"
        );
    }

    #[tokio::test]
    async fn exec_command_marks_non_zero_exit_as_failed() {
        let handler = UnifiedExecHandler::default();
        let output = handler
            .handle(
                invocation(
                    "exec_command",
                    ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                        command: Some(vec![
                            "sh".to_owned(),
                            "-c".to_owned(),
                            "echo boom >&2; exit 2".to_owned(),
                        ]),
                        workdir: None,
                        timeout_ms: None,
                        max_output_tokens: None,
                        yield_time_ms: None,
                        tty: Some(false),
                    })),
                ),
                trace("exec_command"),
            )
            .await
            .expect("exec command should execute");

        assert!(
            !output.success(),
            "expected tool output to be marked failed"
        );
        assert!(
            output.raw_text().contains("Exit Code: 2"),
            "expected rendered output to include exit code: {}",
            output.raw_text()
        );
    }

    #[tokio::test]
    async fn tty_exec_command_marks_immediate_non_zero_exit_as_failed() {
        let handler = UnifiedExecHandler::default();
        let output = handler
            .handle(
                invocation(
                    "exec_command",
                    ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                        command: Some(vec![
                            "sh".to_owned(),
                            "-c".to_owned(),
                            "echo fail >&2; exit 7".to_owned(),
                        ]),
                        workdir: None,
                        timeout_ms: None,
                        max_output_tokens: None,
                        yield_time_ms: Some(120),
                        tty: Some(true),
                    })),
                ),
                trace("exec_command"),
            )
            .await
            .expect("tty exec command should execute");

        assert!(
            !output.success(),
            "expected failed output for immediate non-zero tty exit"
        );
        assert!(
            output.raw_text().contains("Exit Code: 7"),
            "expected exit code in tty output: {}",
            output.raw_text()
        );

        let session_id = parse_session_id(output.raw_text().as_str());
        let missing_session = handler
            .handle(
                invocation(
                    "write_stdin",
                    ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                        session_id,
                        chars: None,
                        yield_time_ms: Some(10),
                        max_output_tokens: None,
                    })),
                ),
                trace("write_stdin"),
            )
            .await;
        match missing_session {
            Ok(_) => panic!("session should be removed after immediate tty exit"),
            Err(error) => assert!(matches!(error, ToolError::NotFound(_))),
        }
    }

    #[tokio::test]
    async fn exec_command_timeout_returns_structured_payload() {
        let handler = UnifiedExecHandler::default();
        let output = handler
            .handle(
                invocation(
                    "exec_command",
                    ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                        command: Some(vec![
                            "sh".to_owned(),
                            "-c".to_owned(),
                            "sleep 1; echo done".to_owned(),
                        ]),
                        workdir: None,
                        timeout_ms: Some(20),
                        max_output_tokens: None,
                        yield_time_ms: None,
                        tty: Some(false),
                    })),
                ),
                trace("exec_command"),
            )
            .await
            .expect("timeout should be represented as tool output");

        assert!(!output.success(), "timeout must be marked as failure");
        let json = output.raw_json();
        assert_eq!(
            json.get("timed_out").and_then(serde_json::Value::as_bool),
            Some(true),
            "payload should include timed_out=true: {json}"
        );
    }

    #[tokio::test]
    async fn one_shot_output_is_bounded_before_reader_completion() {
        let handler = UnifiedExecHandler::default();
        let output = handler
            .handle(
                invocation(
                    "exec_command",
                    ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                        command: Some(vec![
                            "sh".to_owned(),
                            "-c".to_owned(),
                            "head -c 2000000 /dev/zero".to_owned(),
                        ]),
                        workdir: None,
                        timeout_ms: Some(5_000),
                        max_output_tokens: None,
                        yield_time_ms: None,
                        tty: Some(false),
                    })),
                ),
                trace("exec_command"),
            )
            .await
            .expect("bounded output should return a structured result");

        let payload = output.raw_json();
        let stdout = payload
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .expect("stdout should be present");
        assert!(stdout.len() <= ONE_SHOT_MAX_STREAM_BYTES);
        assert_eq!(
            payload
                .pointer("/truncated/stdout")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "oversized one-shot stdout must carry truncation metadata: {payload}"
        );
        assert_eq!(
            payload
                .pointer("/output_stats/stdout/bytes_retained")
                .and_then(serde_json::Value::as_u64),
            Some(ONE_SHOT_MAX_STREAM_BYTES as u64),
            "the retained head must stay at the per-stream hard limit: {payload}"
        );
        assert!(
            payload
                .pointer("/output_stats/stdout/bytes_dropped")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|dropped| dropped > 0)
        );
        assert_eq!(
            payload.pointer("/output_stats/full_output_available"),
            Some(&serde_json::Value::Bool(false))
        );
    }

    #[test]
    fn model_output_hint_can_only_narrow_deployment_limits() {
        let configured = OneShotOutputLimits {
            stream_bytes: 2_000,
            total_bytes: 3_000,
        };
        assert_eq!(one_shot_output_limits(None, configured), (2_000, 3_000));
        assert_eq!(one_shot_output_limits(Some(100), configured), (400, 400));
        assert_eq!(
            one_shot_output_limits(Some(100_000), configured),
            (2_000, 3_000)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn one_shot_reaps_descendant_that_keeps_pipes_open_after_parent_exit() {
        let handler = UnifiedExecHandler::default();
        let run = handler.handle(
            invocation(
                "exec_command",
                ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                    command: Some(vec![
                        "sh".to_owned(),
                        "-c".to_owned(),
                        "(trap '' TERM; sleep 30) & exit 0".to_owned(),
                    ]),
                    workdir: None,
                    timeout_ms: Some(5_000),
                    max_output_tokens: None,
                    yield_time_ms: None,
                    tty: Some(false),
                })),
            ),
            trace("exec_command"),
        );
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("descendant pipe ownership must have a bounded terminal path")
            .expect("process tree cleanup should produce a result");
    }

    #[tokio::test]
    async fn exec_command_rejects_missing_command() {
        let handler = UnifiedExecHandler::default();
        let result = handler
            .handle(
                invocation(
                    "exec_command",
                    ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                        command: None,
                        workdir: None,
                        timeout_ms: None,
                        max_output_tokens: None,
                        yield_time_ms: None,
                        tty: Some(false),
                    })),
                ),
                trace("exec_command"),
            )
            .await;

        let error = match result {
            Ok(output) => panic!(
                "missing command should be rejected, got output: {}",
                output.raw_text()
            ),
            Err(error) => error,
        };
        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error.to_string().contains("`command` argv is required"));
    }

    #[tokio::test]
    async fn exec_command_cancellation_terminates_one_shot_process() {
        let handler = UnifiedExecHandler::default();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let mut tool_invocation = invocation(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "sleep 5; echo should-not-run".to_owned(),
                ]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: None,
                tty: Some(false),
            })),
        );
        tool_invocation.cancellation = cancellation.clone();

        let handle =
            tokio::spawn(
                async move { handler.handle(tool_invocation, trace("exec_command")).await },
            );

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("cancelled command should finish promptly")
            .expect("command task should not panic");

        match result {
            Ok(output) => panic!(
                "cancelled command should not succeed: {}",
                output.raw_text()
            ),
            Err(error) => assert!(matches!(error, ToolError::Cancelled(_))),
        }
    }

    #[tokio::test]
    async fn tty_exec_command_cancellation_removes_live_session() {
        let handler = std::sync::Arc::new(UnifiedExecHandler::default());
        let cancellation = tokio_util::sync::CancellationToken::new();
        let mut tool_invocation = invocation(
            "exec_command",
            ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                command: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "sleep 5; echo should-not-run".to_owned(),
                ]),
                workdir: None,
                timeout_ms: None,
                max_output_tokens: None,
                yield_time_ms: Some(1_000),
                tty: Some(true),
            })),
        );
        tool_invocation.cancellation = cancellation.clone();

        let task_handler = handler.clone();
        let handle = tokio::spawn(async move {
            task_handler
                .handle(tool_invocation, trace("exec_command"))
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("cancelled tty command should finish promptly")
            .expect("command task should not panic");

        match result {
            Ok(output) => panic!(
                "cancelled tty command should not succeed: {}",
                output.raw_text()
            ),
            Err(error) => assert!(matches!(error, ToolError::Cancelled(_))),
        }
        assert!(
            handler.sessions.lock().await.is_empty(),
            "cancelled tty command should not leave a live session"
        );
    }

    #[tokio::test]
    async fn write_stdin_cleans_up_expired_sessions() {
        let handler = UnifiedExecHandler::default();
        let output = handler
            .handle(
                invocation(
                    "exec_command",
                    ToolPayload::LocalShell(LocalShellPayload::ExecCommand(ExecCommandArgs {
                        command: Some(vec!["sh".to_owned(), "-c".to_owned(), "sleep 2".to_owned()]),
                        workdir: None,
                        timeout_ms: None,
                        max_output_tokens: None,
                        yield_time_ms: Some(10),
                        tty: Some(true),
                    })),
                ),
                trace("exec_command"),
            )
            .await
            .expect("exec command should create session");
        let session_id = parse_session_id(output.raw_text().as_str());

        tokio::time::sleep(Duration::from_millis(SESSION_TTL_MS + 150)).await;

        let result = handler
            .handle(
                invocation(
                    "write_stdin",
                    ToolPayload::LocalShell(LocalShellPayload::WriteStdin(WriteStdinArgs {
                        session_id,
                        chars: Some("ping\n".to_owned()),
                        yield_time_ms: Some(10),
                        max_output_tokens: None,
                    })),
                ),
                trace("write_stdin"),
            )
            .await;

        match result {
            Ok(_) => panic!("expired session must be cleaned before write_stdin"),
            Err(error) => assert!(matches!(error, ToolError::NotFound(_))),
        }
    }

    #[test]
    fn pipefail_prefix_is_applied_for_zsh_like_shells() {
        assert_eq!(
            maybe_prefix_pipefail("echo ok", "zsh"),
            "set -o pipefail; echo ok"
        );
        assert_eq!(
            maybe_prefix_pipefail("echo ok", "/bin/bash"),
            "set -o pipefail; echo ok"
        );
        assert_eq!(maybe_prefix_pipefail("echo ok", "sh"), "echo ok");
    }

    #[test]
    fn pipefail_prefix_is_applied_for_command_argv_shell_invocations() {
        let rewritten = maybe_prefix_pipefail_for_argv(&[
            "zsh".to_owned(),
            "-lc".to_owned(),
            "echo ok | cat".to_owned(),
        ]);
        assert_eq!(
            rewritten,
            vec![
                "zsh".to_owned(),
                "-lc".to_owned(),
                "set -o pipefail; echo ok | cat".to_owned(),
            ]
        );
    }
}
