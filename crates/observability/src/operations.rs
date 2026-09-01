use crate::telemetry::TelemetryTarget;
use opentelemetry::trace::{
    Span as _, SpanBuilder, SpanKind, Status, TraceContextExt, Tracer as _,
};
use opentelemetry::{Context, KeyValue};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayCliRuntimeKind {
    Codex,
    Claude,
}

impl GatewayCliRuntimeKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayProviderWarmupScope {
    Api,
    Cli,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayProviderType {
    Api,
    Cli,
}

impl GatewayProviderType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Cli => "cli",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayProviderReadinessState {
    Ready,
    Unverified,
    Disabled,
    MissingBinary,
    SpawnFailed,
    Initializing,
    NeedsAuth,
    Degraded,
    UnsupportedVersion,
    Error,
}

impl GatewayProviderReadinessState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Unverified => "unverified",
            Self::Disabled => "disabled",
            Self::MissingBinary => "missing_binary",
            Self::SpawnFailed => "spawn_failed",
            Self::Initializing => "initializing",
            Self::NeedsAuth => "needs_auth",
            Self::Degraded => "degraded",
            Self::UnsupportedVersion => "unsupported_version",
            Self::Error => "error",
        }
    }

    const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }

    const fn is_applicable(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    const fn is_unverified(self) -> bool {
        matches!(self, Self::Unverified)
    }
}

impl GatewayProviderWarmupScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Cli => "cli",
            Self::All => "all",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayProviderWarmupStage {
    SchedulerQueueWait,
    WorkspaceLoad,
    ApiCatalogLoad,
    ApiInstancesWarmup,
    ApiInstanceWarmup,
    CliCatalogLoad,
    CliSnapshotPublish,
    RuntimeProxyLoad,
    RuntimeAccountProbe,
    RuntimeMcpReadiness,
    RuntimeModelsLoad,
}

impl GatewayProviderWarmupStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SchedulerQueueWait => "scheduler.queue_wait",
            Self::WorkspaceLoad => "workspace.load",
            Self::ApiCatalogLoad => "api.catalog.load",
            Self::ApiInstancesWarmup => "api.instances.warmup",
            Self::ApiInstanceWarmup => "api.instance.warmup",
            Self::CliCatalogLoad => "cli.catalog.load",
            Self::CliSnapshotPublish => "cli.snapshot.publish",
            Self::RuntimeProxyLoad => "runtime.proxy.load",
            Self::RuntimeAccountProbe => "runtime.account.probe",
            Self::RuntimeMcpReadiness => "runtime.mcp.readiness",
            Self::RuntimeModelsLoad => "runtime.models.load",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperationStageOutcome {
    Ok,
    Error,
    Cancelled,
}

impl OperationStageOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }

    const fn status(self) -> Status {
        match self {
            Self::Ok => Status::Ok,
            Self::Error => Status::Error {
                description: std::borrow::Cow::Borrowed("operation stage failed"),
            },
            Self::Cancelled => Status::Unset,
        }
    }
}

#[derive(Clone, Debug)]
struct OperationStageRecord {
    name: &'static str,
    runtime_kind: Option<GatewayCliRuntimeKind>,
    provider_kind: Option<String>,
    started_at: SystemTime,
    elapsed: Duration,
    outcome: OperationStageOutcome,
}

#[derive(Clone, Debug)]
struct ProviderReadinessRecord {
    provider_type: GatewayProviderType,
    runtime_kind: Option<GatewayCliRuntimeKind>,
    provider_kind: Option<String>,
    state: GatewayProviderReadinessState,
}

#[derive(Debug)]
struct GatewayProviderWarmupState {
    scope: GatewayProviderWarmupScope,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    started_instant: Instant,
    stages: Vec<OperationStageRecord>,
    readiness: Vec<ProviderReadinessRecord>,
    finalized: bool,
}

#[derive(Clone, Debug)]
struct GatewayProviderWarmupTimeline {
    inner: Arc<Mutex<GatewayProviderWarmupState>>,
}

impl GatewayProviderWarmupTimeline {
    fn start(scope: GatewayProviderWarmupScope) -> Self {
        let (telemetry_enabled, consent_generation) = super::telemetry_consent_snapshot();
        Self {
            inner: Arc::new(Mutex::new(GatewayProviderWarmupState {
                scope,
                consent_generation: telemetry_enabled.then_some(consent_generation),
                started_at: SystemTime::now(),
                started_instant: Instant::now(),
                stages: Vec::new(),
                readiness: Vec::new(),
                finalized: false,
            })),
        }
    }

    fn stage(
        &self,
        stage: GatewayProviderWarmupStage,
        runtime_kind: Option<GatewayCliRuntimeKind>,
        provider_kind: Option<String>,
    ) -> GatewayProviderWarmupStageGuard {
        GatewayProviderWarmupStageGuard {
            timeline: self.clone(),
            name: stage.as_str(),
            runtime_kind,
            provider_kind,
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
            finished: false,
        }
    }

    fn record_stage(
        &self,
        name: &'static str,
        runtime_kind: Option<GatewayCliRuntimeKind>,
        provider_kind: Option<String>,
        started_at: SystemTime,
        elapsed: Duration,
        outcome: OperationStageOutcome,
    ) {
        let mut state = self.lock();
        if !state.finalized {
            state.stages.push(OperationStageRecord {
                name,
                runtime_kind,
                provider_kind,
                started_at,
                elapsed,
                outcome,
            });
        }
    }

    fn record_readiness(
        &self,
        provider_type: GatewayProviderType,
        runtime_kind: Option<GatewayCliRuntimeKind>,
        provider_kind: Option<String>,
        readiness_state: GatewayProviderReadinessState,
    ) {
        let mut state = self.lock();
        if !state.finalized {
            state.readiness.push(ProviderReadinessRecord {
                provider_type,
                runtime_kind,
                provider_kind,
                state: readiness_state,
            });
        }
    }

    fn finish(&self, outcome: &'static str, failed: bool) {
        let snapshot = {
            let mut state = self.lock();
            if state.finalized {
                return;
            }
            state.finalized = true;
            GatewayProviderWarmupSnapshot {
                scope: state.scope,
                consent_generation: state.consent_generation,
                started_at: state.started_at,
                elapsed: state.started_instant.elapsed(),
                stages: state.stages.clone(),
                readiness: state.readiness.clone(),
                outcome,
                failed,
            }
        };
        emit_gateway_provider_warmup(&snapshot);
    }

    fn lock(&self) -> MutexGuard<'_, GatewayProviderWarmupState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Records one complete Gateway-owned provider warm-up.
///
/// The trace intentionally contains only bounded attributes. Runtime and
/// workspace identifiers are never exported; `runtime.kind` is limited to
/// `codex` and `claude`, while `provider.kind` is the canonical adapter name
/// produced by the fixed provider factory (or the bounded `unknown` fallback).
#[must_use = "the provider warm-up trace must be completed"]
pub struct GatewayProviderWarmupTrace {
    timeline: GatewayProviderWarmupTimeline,
    finished: bool,
}

impl GatewayProviderWarmupTrace {
    pub fn start(scope: GatewayProviderWarmupScope) -> Self {
        Self {
            timeline: GatewayProviderWarmupTimeline::start(scope),
            finished: false,
        }
    }

    pub fn stage(&self, stage: GatewayProviderWarmupStage) -> GatewayProviderWarmupStageGuard {
        self.timeline.stage(stage, None, None)
    }

    pub fn runtime_stage(
        &self,
        stage: GatewayProviderWarmupStage,
        runtime_kind: GatewayCliRuntimeKind,
    ) -> GatewayProviderWarmupStageGuard {
        self.timeline.stage(stage, Some(runtime_kind), None)
    }

    pub fn api_provider_stage(
        &self,
        stage: GatewayProviderWarmupStage,
        provider_kind: impl Into<String>,
    ) -> GatewayProviderWarmupStageGuard {
        self.timeline.stage(stage, None, Some(provider_kind.into()))
    }

    pub fn record_api_readiness(
        &self,
        provider_kind: impl Into<String>,
        state: GatewayProviderReadinessState,
    ) {
        self.timeline.record_readiness(
            GatewayProviderType::Api,
            None,
            Some(provider_kind.into()),
            state,
        );
    }

    pub fn record_cli_readiness(
        &self,
        runtime_kind: GatewayCliRuntimeKind,
        state: GatewayProviderReadinessState,
    ) {
        self.timeline
            .record_readiness(GatewayProviderType::Cli, Some(runtime_kind), None, state);
    }

    pub fn finish_success(mut self) {
        self.timeline.finish("ok", false);
        self.finished = true;
    }

    /// Finishes a probe whose result was fenced because provider
    /// configuration changed while it was running. A newer queued probe owns
    /// the replacement result, so this is neither success nor failure.
    pub fn finish_superseded(mut self) {
        self.timeline.finish("superseded", false);
        self.finished = true;
    }

    pub fn finish_failure(mut self) {
        self.timeline.finish("error", true);
        self.finished = true;
    }
}

impl Drop for GatewayProviderWarmupTrace {
    fn drop(&mut self) {
        if !self.finished {
            // The supervisor aborts in-flight probes during normal Gateway
            // shutdown. Actual probe errors and panics are finalized
            // explicitly by the task owner through `finish_failure`.
            self.timeline.finish("cancelled", false);
            self.finished = true;
        }
    }
}

#[must_use = "an operation stage must be marked successful; dropping it records a failure"]
pub struct GatewayProviderWarmupStageGuard {
    timeline: GatewayProviderWarmupTimeline,
    name: &'static str,
    runtime_kind: Option<GatewayCliRuntimeKind>,
    provider_kind: Option<String>,
    started_at: SystemTime,
    started_instant: Instant,
    finished: bool,
}

impl GatewayProviderWarmupStageGuard {
    pub fn succeed(mut self) {
        self.finish(OperationStageOutcome::Ok);
    }

    pub fn cancel(mut self) {
        self.finish(OperationStageOutcome::Cancelled);
    }

    fn finish(&mut self, outcome: OperationStageOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.timeline.record_stage(
            self.name,
            self.runtime_kind,
            self.provider_kind.clone(),
            self.started_at,
            self.started_instant.elapsed(),
            outcome,
        );
    }
}

impl Drop for GatewayProviderWarmupStageGuard {
    fn drop(&mut self) {
        self.finish(OperationStageOutcome::Error);
    }
}

struct GatewayProviderWarmupSnapshot {
    scope: GatewayProviderWarmupScope,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    elapsed: Duration,
    stages: Vec<OperationStageRecord>,
    readiness: Vec<ProviderReadinessRecord>,
    outcome: &'static str,
    failed: bool,
}

impl GatewayProviderWarmupSnapshot {
    fn failed_stage(&self) -> &'static str {
        self.stages
            .iter()
            .rev()
            .find(|stage| stage.outcome == OperationStageOutcome::Error)
            .map(|stage| stage.name)
            .unwrap_or("unknown")
    }

    fn readiness_outcome(&self) -> &'static str {
        if matches!(self.outcome, "superseded" | "cancelled") {
            return self.outcome;
        }
        let applicable = self
            .readiness
            .iter()
            .filter(|record| record.state.is_applicable())
            .collect::<Vec<_>>();
        let ready = applicable
            .iter()
            .filter(|record| record.state.is_ready())
            .count();
        let unverified = applicable
            .iter()
            .filter(|record| record.state.is_unverified())
            .count();
        let unavailable = applicable.len().saturating_sub(ready + unverified);
        match (ready, unverified, unavailable) {
            (_, _, _) if applicable.is_empty() => "not_applicable",
            (_, 0, 0) => "ready",
            (0, _, 0) => "unverified",
            (0, 0, _) => "unavailable",
            _ => "partial",
        }
    }
}

fn emit_gateway_provider_warmup(snapshot: &GatewayProviderWarmupSnapshot) {
    if !super::telemetry_sample_allowed(snapshot.consent_generation) {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    if state.target != TelemetryTarget::Gateway {
        return;
    }
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };

    let failed_stage = if snapshot.failed {
        snapshot.failed_stage()
    } else {
        "none"
    };
    let ready_count = snapshot
        .readiness
        .iter()
        .filter(|record| record.state.is_ready())
        .count();
    let disabled_count = snapshot
        .readiness
        .iter()
        .filter(|record| !record.state.is_applicable())
        .count();
    let unverified_count = snapshot
        .readiness
        .iter()
        .filter(|record| record.state.is_unverified())
        .count();
    let unready_count = snapshot
        .readiness
        .len()
        .saturating_sub(ready_count + disabled_count + unverified_count);
    let metric_attributes = vec![
        KeyValue::new("operation.name", "providers.warmup"),
        KeyValue::new("provider.scope", snapshot.scope.as_str()),
        KeyValue::new("outcome", snapshot.outcome),
        KeyValue::new("operation.failed_stage", failed_stage),
        KeyValue::new("provider.readiness.outcome", snapshot.readiness_outcome()),
    ];
    let mut trace_attributes = metric_attributes.clone();
    trace_attributes.extend([
        KeyValue::new("provider.ready_count", ready_count as i64),
        KeyValue::new("provider.unready_count", unready_count as i64),
        KeyValue::new("provider.unverified_count", unverified_count as i64),
        KeyValue::new("provider.disabled_count", disabled_count as i64),
    ]);
    metrics.provider_warmup_duration.record(
        snapshot.elapsed.as_secs_f64() * 1_000.0,
        metric_attributes.as_slice(),
    );
    if snapshot.failed {
        metrics
            .provider_warmup_failures
            .add(1, metric_attributes.as_slice());
    }
    for readiness in &snapshot.readiness {
        metrics
            .provider_readiness_checks
            .add(1, readiness_attributes(readiness).as_slice());
    }

    for stage in &snapshot.stages {
        let attributes = stage_attributes(stage, snapshot.scope, snapshot.outcome);
        metrics
            .provider_warmup_stage_duration
            .record(stage.elapsed.as_secs_f64() * 1_000.0, attributes.as_slice());
    }

    let root_builder = SpanBuilder::from_name("gateway.providers.warmup")
        .with_kind(SpanKind::Internal)
        .with_start_time(snapshot.started_at)
        .with_attributes(trace_attributes);
    let root_span = state.tracer.build(root_builder);
    let root_context = Context::new().with_span(root_span);

    for stage in &snapshot.stages {
        let builder = SpanBuilder::from_name(stage.name)
            .with_kind(SpanKind::Internal)
            .with_start_time(stage.started_at)
            .with_attributes(stage_attributes(stage, snapshot.scope, snapshot.outcome));
        let mut span = state.tracer.build_with_context(builder, &root_context);
        span.set_status(if matches!(snapshot.outcome, "superseded" | "cancelled") {
            Status::Unset
        } else {
            stage.outcome.status()
        });
        span.end_with_timestamp(end_timestamp(stage.started_at, stage.elapsed));
    }

    root_context.span().set_status(if snapshot.failed {
        Status::Error {
            description: std::borrow::Cow::Borrowed("provider warm-up failed"),
        }
    } else if matches!(snapshot.outcome, "superseded" | "cancelled") {
        Status::Unset
    } else {
        Status::Ok
    });
    root_context
        .span()
        .end_with_timestamp(end_timestamp(snapshot.started_at, snapshot.elapsed));
}

fn readiness_attributes(record: &ProviderReadinessRecord) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("provider.type", record.provider_type.as_str()),
        KeyValue::new("provider.readiness.state", record.state.as_str()),
    ];
    if let Some(runtime_kind) = record.runtime_kind {
        attributes.push(KeyValue::new("runtime.kind", runtime_kind.as_str()));
    }
    if let Some(provider_kind) = record.provider_kind.as_deref() {
        attributes.push(KeyValue::new("provider.kind", provider_kind.to_owned()));
    }
    attributes
}

fn stage_attributes(
    stage: &OperationStageRecord,
    scope: GatewayProviderWarmupScope,
    operation_outcome: &'static str,
) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("operation.name", "providers.warmup"),
        KeyValue::new("provider.scope", scope.as_str()),
        KeyValue::new("operation.stage", stage.name),
        KeyValue::new(
            "outcome",
            if matches!(operation_outcome, "superseded" | "cancelled") {
                operation_outcome
            } else {
                stage.outcome.as_str()
            },
        ),
    ];
    if let Some(runtime_kind) = stage.runtime_kind {
        attributes.push(KeyValue::new("runtime.kind", runtime_kind.as_str()));
    }
    if let Some(provider_kind) = stage.provider_kind.as_deref() {
        attributes.push(KeyValue::new("provider.kind", provider_kind.to_owned()));
    }
    attributes
}

fn end_timestamp(started_at: SystemTime, elapsed: Duration) -> SystemTime {
    started_at
        .checked_add(elapsed)
        .unwrap_or_else(SystemTime::now)
}

/// A bounded Gateway operation that is useful outside the process-startup
/// critical path. The enum deliberately prevents request data, workspace IDs,
/// SQL, or other unbounded values from entering telemetry attributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayOperation {
    AuthRefresh,
    AuthMe,
    ResilienceInitialize,
    SelfImprovementInitialize,
    McpWorkspaceInitialize,
    SkillsWatcherInitialize,
    DatabaseStartupMaintenance,
    ThreadTreeLoad,
}

impl GatewayOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AuthRefresh => "auth.refresh",
            Self::AuthMe => "auth.me",
            Self::ResilienceInitialize => "services.resilience.initialize",
            Self::SelfImprovementInitialize => "services.self_improvement.initialize",
            Self::McpWorkspaceInitialize => "services.mcp.initialize",
            Self::SkillsWatcherInitialize => "services.skills_watcher.initialize",
            Self::DatabaseStartupMaintenance => "database.startup_maintenance",
            Self::ThreadTreeLoad => "thread_tree.load",
        }
    }

    const fn span_name(self) -> &'static str {
        match self {
            Self::AuthRefresh => "gateway.auth.refresh",
            Self::AuthMe => "gateway.auth.me",
            Self::ResilienceInitialize => "gateway.services.resilience.initialize",
            Self::SelfImprovementInitialize => "gateway.services.self_improvement.initialize",
            Self::McpWorkspaceInitialize => "gateway.services.mcp.initialize",
            Self::SkillsWatcherInitialize => "gateway.services.skills_watcher.initialize",
            Self::DatabaseStartupMaintenance => "gateway.database.startup_maintenance",
            Self::ThreadTreeLoad => "gateway.thread_tree.load",
        }
    }
}

/// Stable execution variants for bounded Gateway operations. Variants are
/// deliberately modeled as an enum so request values and authorization
/// identifiers cannot become telemetry dimensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayOperationVariant {
    ThreadTreeWorkspaceWide,
    ThreadTreePrincipalScoped,
}

impl GatewayOperationVariant {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ThreadTreeWorkspaceWide => "thread_tree.workspace_wide",
            Self::ThreadTreePrincipalScoped => "thread_tree.principal_scoped",
        }
    }
}

/// Stable stages used by [`GatewayOperationTrace`]. Repeated stages are
/// allowed (for example, one MCP reload per workspace) and remain bounded
/// because identifiers are intentionally excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayOperationStage {
    AuthRefreshDatabaseTransactionAcquire,
    AuthMeSessionLoad,
    AuthMeDeviceLoad,
    AuthMePrincipalLoad,
    AuthMeAvatarLoad,
    ResilienceReadModelRepair,
    ResilienceDeadlineBackfill,
    ResilienceAdmissionLeaseReconcile,
    ResilienceTaskRuntimeStart,
    ResilienceTaskEventListenerStart,
    ResilienceHookRecoveryStart,
    SelfImprovementInitialWake,
    McpWorkspacesLoad,
    McpWorkspaceReload,
    SkillsWorkspacesLoad,
    SkillsCatalogSnapshot,
    DatabaseTurnEventProjectionBackfill,
    DatabaseChildLaunchGrantBackfill,
    DatabaseAuthorizationIntegrityAudit,
    DatabaseTurnPermissionProfileBackfill,
    DatabasePayloadCompression,
    DatabaseThreadEpisodicRefill,
    ThreadTreeConnectionWorkspaceSet,
    ThreadTreePersistedThreadsLoad,
    ThreadTreeRuntimeThreadsLoad,
    ThreadTreeMerge,
    ThreadTreeFoldersLoad,
    ThreadTreePlacementsLoad,
    ThreadTreeUnreadLoad,
    ThreadTreeAgentsDocsLoad,
    ThreadTreeResponseEncode,
    ThreadTreeResponseSend,
}

impl GatewayOperationStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthRefreshDatabaseTransactionAcquire => "db.transaction.acquire",
            Self::AuthMeSessionLoad => "session.load",
            Self::AuthMeDeviceLoad => "device.load",
            Self::AuthMePrincipalLoad => "principal.load",
            Self::AuthMeAvatarLoad => "avatar.load",
            Self::ResilienceReadModelRepair => "read_model.repair",
            Self::ResilienceDeadlineBackfill => "deadlines.backfill",
            Self::ResilienceAdmissionLeaseReconcile => "admission_leases.reconcile",
            Self::ResilienceTaskRuntimeStart => "task_runtime.start",
            Self::ResilienceTaskEventListenerStart => "task_events.start",
            Self::ResilienceHookRecoveryStart => "hook_recovery.start",
            Self::SelfImprovementInitialWake => "self_improvement.initial_wake",
            Self::McpWorkspacesLoad => "workspaces.load",
            Self::McpWorkspaceReload => "workspace.reload",
            Self::SkillsWorkspacesLoad => "workspaces.load",
            Self::SkillsCatalogSnapshot => "catalog.snapshot",
            Self::DatabaseTurnEventProjectionBackfill => "turn_event_projection.backfill",
            Self::DatabaseChildLaunchGrantBackfill => "child_launch_grant.backfill",
            Self::DatabaseAuthorizationIntegrityAudit => "authorization.integrity_audit",
            Self::DatabaseTurnPermissionProfileBackfill => "turn_permission_profile.backfill",
            Self::DatabasePayloadCompression => "payload.compress",
            Self::DatabaseThreadEpisodicRefill => "thread_episodic.refill",
            Self::ThreadTreeConnectionWorkspaceSet => "connection_workspace.set",
            Self::ThreadTreePersistedThreadsLoad => "threads.persisted.load",
            Self::ThreadTreeRuntimeThreadsLoad => "threads.runtime.load",
            Self::ThreadTreeMerge => "threads.merge",
            Self::ThreadTreeFoldersLoad => "folders.load",
            Self::ThreadTreePlacementsLoad => "placements.load",
            Self::ThreadTreeUnreadLoad => "unread.load",
            Self::ThreadTreeAgentsDocsLoad => "agents_docs.load",
            Self::ThreadTreeResponseEncode => "response.encode",
            Self::ThreadTreeResponseSend => "response.send",
        }
    }
}

/// Bounded item categories attached to Gateway operation traces and metrics.
/// Counts are useful for separating query complexity from database contention;
/// identifiers and request data are deliberately excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayOperationItemKind {
    ThreadTreePersistedThreads,
    ThreadTreeRuntimeThreads,
    ThreadTreeReturnedThreads,
    ThreadTreeFolders,
    ThreadTreePlacements,
    ThreadTreeUnreadCandidateThreads,
    ThreadTreeUnreadThreads,
    ThreadTreeUnreadMessages,
    ThreadTreeAgentsDocs,
}

impl GatewayOperationItemKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ThreadTreePersistedThreads => "thread_tree.threads.persisted",
            Self::ThreadTreeRuntimeThreads => "thread_tree.threads.runtime",
            Self::ThreadTreeReturnedThreads => "thread_tree.threads.returned",
            Self::ThreadTreeFolders => "thread_tree.folders",
            Self::ThreadTreePlacements => "thread_tree.placements",
            Self::ThreadTreeUnreadCandidateThreads => "thread_tree.unread.candidate_threads",
            Self::ThreadTreeUnreadThreads => "thread_tree.unread.nonzero_threads",
            Self::ThreadTreeUnreadMessages => "thread_tree.unread.messages",
            Self::ThreadTreeAgentsDocs => "thread_tree.agents_docs",
        }
    }

    const fn span_attribute(self) -> &'static str {
        match self {
            Self::ThreadTreePersistedThreads => "thread_tree.threads.persisted.count",
            Self::ThreadTreeRuntimeThreads => "thread_tree.threads.runtime.count",
            Self::ThreadTreeReturnedThreads => "thread_tree.threads.returned.count",
            Self::ThreadTreeFolders => "thread_tree.folders.count",
            Self::ThreadTreePlacements => "thread_tree.placements.count",
            Self::ThreadTreeUnreadCandidateThreads => "thread_tree.unread.candidate_threads.count",
            Self::ThreadTreeUnreadThreads => "thread_tree.unread.nonzero_threads.count",
            Self::ThreadTreeUnreadMessages => "thread_tree.unread.messages.count",
            Self::ThreadTreeAgentsDocs => "thread_tree.agents_docs.count",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct GatewayOperationItemRecord {
    kind: GatewayOperationItemKind,
    count: u64,
}

#[derive(Clone, Debug)]
struct GatewayOperationStageRecord {
    name: &'static str,
    started_at: SystemTime,
    elapsed: Duration,
    outcome: OperationStageOutcome,
}

#[derive(Debug)]
struct GatewayOperationState {
    operation: GatewayOperation,
    variant: Option<GatewayOperationVariant>,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    started_instant: Instant,
    stages: Vec<GatewayOperationStageRecord>,
    items: Vec<GatewayOperationItemRecord>,
    finalized: bool,
}

#[derive(Clone, Debug)]
struct GatewayOperationTimeline {
    inner: Arc<Mutex<GatewayOperationState>>,
}

impl GatewayOperationTimeline {
    fn start(operation: GatewayOperation) -> Self {
        let (telemetry_enabled, consent_generation) = super::telemetry_consent_snapshot();
        Self {
            inner: Arc::new(Mutex::new(GatewayOperationState {
                operation,
                variant: None,
                consent_generation: telemetry_enabled.then_some(consent_generation),
                started_at: SystemTime::now(),
                started_instant: Instant::now(),
                stages: Vec::new(),
                items: Vec::new(),
                finalized: false,
            })),
        }
    }

    fn stage(&self, stage: GatewayOperationStage) -> GatewayOperationStageGuard {
        GatewayOperationStageGuard {
            timeline: self.clone(),
            name: stage.as_str(),
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
            finished: false,
        }
    }

    fn record_stage(
        &self,
        name: &'static str,
        started_at: SystemTime,
        elapsed: Duration,
        outcome: OperationStageOutcome,
    ) {
        let mut state = self.lock();
        if !state.finalized {
            state.stages.push(GatewayOperationStageRecord {
                name,
                started_at,
                elapsed,
                outcome,
            });
        }
    }

    fn record_items(&self, kind: GatewayOperationItemKind, count: u64) {
        let mut state = self.lock();
        if state.finalized {
            return;
        }
        if let Some(existing) = state.items.iter_mut().find(|item| item.kind == kind) {
            existing.count = count;
        } else {
            state.items.push(GatewayOperationItemRecord { kind, count });
        }
    }

    fn set_variant(&self, variant: GatewayOperationVariant) {
        let mut state = self.lock();
        if !state.finalized {
            state.variant = Some(variant);
        }
    }

    fn finish(&self, outcome: &'static str, failed: bool) {
        let snapshot = {
            let mut state = self.lock();
            if state.finalized {
                return;
            }
            state.finalized = true;
            GatewayOperationSnapshot {
                operation: state.operation,
                variant: state.variant,
                consent_generation: state.consent_generation,
                started_at: state.started_at,
                elapsed: state.started_instant.elapsed(),
                stages: state.stages.clone(),
                items: state.items.clone(),
                outcome,
                failed,
            }
        };
        emit_gateway_operation(&snapshot);
    }

    fn lock(&self) -> MutexGuard<'_, GatewayOperationState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Records one bounded Gateway operation and all of its diagnostic stages.
#[must_use = "the Gateway operation trace must be completed"]
pub struct GatewayOperationTrace {
    timeline: GatewayOperationTimeline,
    finished: bool,
}

impl GatewayOperationTrace {
    pub fn start(operation: GatewayOperation) -> Self {
        Self {
            timeline: GatewayOperationTimeline::start(operation),
            finished: false,
        }
    }

    pub fn stage(&self, stage: GatewayOperationStage) -> GatewayOperationStageGuard {
        self.timeline.stage(stage)
    }

    pub fn record_items(&self, kind: GatewayOperationItemKind, count: u64) {
        self.timeline.record_items(kind, count);
    }

    pub fn set_variant(&self, variant: GatewayOperationVariant) {
        self.timeline.set_variant(variant);
    }

    pub fn finish_success(mut self) {
        self.timeline.finish("ok", false);
        self.finished = true;
    }

    pub fn finish_failure(mut self) {
        self.timeline.finish("error", true);
        self.finished = true;
    }

    pub fn finish_cancelled(mut self) {
        self.timeline.finish("cancelled", false);
        self.finished = true;
    }
}

impl Drop for GatewayOperationTrace {
    fn drop(&mut self) {
        if !self.finished {
            self.timeline.finish("cancelled", false);
            self.finished = true;
        }
    }
}

#[must_use = "an operation stage must be marked successful; dropping it records a failure"]
pub struct GatewayOperationStageGuard {
    timeline: GatewayOperationTimeline,
    name: &'static str,
    started_at: SystemTime,
    started_instant: Instant,
    finished: bool,
}

impl GatewayOperationStageGuard {
    pub fn succeed(mut self) {
        self.finish(OperationStageOutcome::Ok);
    }

    pub fn cancel(mut self) {
        self.finish(OperationStageOutcome::Cancelled);
    }

    fn finish(&mut self, outcome: OperationStageOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.timeline.record_stage(
            self.name,
            self.started_at,
            self.started_instant.elapsed(),
            outcome,
        );
    }
}

impl Drop for GatewayOperationStageGuard {
    fn drop(&mut self) {
        self.finish(OperationStageOutcome::Error);
    }
}

struct GatewayOperationSnapshot {
    operation: GatewayOperation,
    variant: Option<GatewayOperationVariant>,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    elapsed: Duration,
    stages: Vec<GatewayOperationStageRecord>,
    items: Vec<GatewayOperationItemRecord>,
    outcome: &'static str,
    failed: bool,
}

impl GatewayOperationSnapshot {
    fn failed_stage(&self) -> &'static str {
        self.stages
            .iter()
            .rev()
            .find(|stage| stage.outcome == OperationStageOutcome::Error)
            .map(|stage| stage.name)
            .unwrap_or("unknown")
    }
}

fn emit_gateway_operation(snapshot: &GatewayOperationSnapshot) {
    if !super::telemetry_sample_allowed(snapshot.consent_generation) {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    if state.target != TelemetryTarget::Gateway {
        return;
    }
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };

    let failed_stage = if snapshot.failed {
        snapshot.failed_stage()
    } else {
        "none"
    };
    let mut attributes = vec![
        KeyValue::new("operation.name", snapshot.operation.as_str()),
        KeyValue::new("outcome", snapshot.outcome),
        KeyValue::new("operation.failed_stage", failed_stage),
    ];
    if let Some(variant) = snapshot.variant {
        attributes.push(KeyValue::new("operation.variant", variant.as_str()));
    }
    metrics
        .gateway_operation_duration
        .record(snapshot.elapsed.as_secs_f64() * 1_000.0, &attributes);
    if snapshot.failed {
        metrics.gateway_operation_failures.add(1, &attributes);
    }
    for stage in &snapshot.stages {
        let mut stage_attributes = vec![
            KeyValue::new("operation.name", snapshot.operation.as_str()),
            KeyValue::new("operation.stage", stage.name),
            KeyValue::new("outcome", stage.outcome.as_str()),
        ];
        if let Some(variant) = snapshot.variant {
            stage_attributes.push(KeyValue::new("operation.variant", variant.as_str()));
        }
        metrics
            .gateway_operation_stage_duration
            .record(stage.elapsed.as_secs_f64() * 1_000.0, &stage_attributes);
    }
    for item in &snapshot.items {
        let mut item_attributes = vec![
            KeyValue::new("operation.name", snapshot.operation.as_str()),
            KeyValue::new("operation.item.kind", item.kind.as_str()),
        ];
        if let Some(variant) = snapshot.variant {
            item_attributes.push(KeyValue::new("operation.variant", variant.as_str()));
        }
        metrics
            .gateway_operation_items
            .record(item.count, &item_attributes);
    }

    let mut root_attributes = attributes;
    for item in &snapshot.items {
        root_attributes.push(KeyValue::new(
            item.kind.span_attribute(),
            i64::try_from(item.count).unwrap_or(i64::MAX),
        ));
    }
    let root_builder = SpanBuilder::from_name(snapshot.operation.span_name())
        .with_kind(SpanKind::Internal)
        .with_start_time(snapshot.started_at)
        .with_attributes(root_attributes);
    let root_span = state.tracer.build(root_builder);
    let root_context = Context::new().with_span(root_span);
    for stage in &snapshot.stages {
        let mut stage_attributes = vec![
            KeyValue::new("operation.name", snapshot.operation.as_str()),
            KeyValue::new("operation.stage", stage.name),
            KeyValue::new("outcome", stage.outcome.as_str()),
        ];
        if let Some(variant) = snapshot.variant {
            stage_attributes.push(KeyValue::new("operation.variant", variant.as_str()));
        }
        let builder = SpanBuilder::from_name(stage.name)
            .with_kind(SpanKind::Internal)
            .with_start_time(stage.started_at)
            .with_attributes(stage_attributes);
        let mut span = state.tracer.build_with_context(builder, &root_context);
        span.set_status(stage.outcome.status());
        span.end_with_timestamp(end_timestamp(stage.started_at, stage.elapsed));
    }
    root_context.span().set_status(if snapshot.failed {
        Status::Error {
            description: std::borrow::Cow::Borrowed("Gateway operation failed"),
        }
    } else if snapshot.outcome == "cancelled" {
        Status::Unset
    } else {
        Status::Ok
    });
    root_context
        .span()
        .end_with_timestamp(end_timestamp(snapshot.started_at, snapshot.elapsed));
}

#[cfg(test)]
mod tests {
    use super::{
        GatewayCliRuntimeKind, GatewayOperation, GatewayOperationItemKind, GatewayOperationStage,
        GatewayOperationTrace, GatewayOperationVariant, GatewayProviderReadinessState,
        GatewayProviderWarmupScope, GatewayProviderWarmupStage, GatewayProviderWarmupTrace,
        OperationStageOutcome,
    };

    #[test]
    fn gateway_operations_use_stable_bounded_names() {
        assert_eq!(
            GatewayOperation::ResilienceInitialize.as_str(),
            "services.resilience.initialize"
        );
        assert_eq!(
            GatewayOperation::ThreadTreeLoad.span_name(),
            "gateway.thread_tree.load"
        );
        assert_eq!(
            GatewayOperationStage::ThreadTreePersistedThreadsLoad.as_str(),
            "threads.persisted.load"
        );
        assert_eq!(
            GatewayOperationStage::DatabasePayloadCompression.as_str(),
            "payload.compress"
        );
        assert_eq!(GatewayOperation::AuthMe.span_name(), "gateway.auth.me");
        assert_eq!(
            GatewayOperationStage::AuthMePrincipalLoad.as_str(),
            "principal.load"
        );
    }

    #[test]
    fn gateway_operation_supports_repeated_stages_and_explicit_failure() {
        let trace = GatewayOperationTrace::start(GatewayOperation::McpWorkspaceInitialize);
        trace
            .stage(GatewayOperationStage::McpWorkspaceReload)
            .succeed();
        drop(trace.stage(GatewayOperationStage::McpWorkspaceReload));
        let timeline = trace.timeline.clone();

        trace.finish_failure();

        let state = timeline.lock();
        assert!(state.finalized);
        assert_eq!(state.stages.len(), 2);
        assert_eq!(state.stages[0].outcome, OperationStageOutcome::Ok);
        assert_eq!(state.stages[1].outcome, OperationStageOutcome::Error);
    }

    #[test]
    fn gateway_operation_item_counts_are_bounded_and_replaceable() {
        let trace = GatewayOperationTrace::start(GatewayOperation::ThreadTreeLoad);
        trace.set_variant(GatewayOperationVariant::ThreadTreePrincipalScoped);
        trace.record_items(GatewayOperationItemKind::ThreadTreeReturnedThreads, 10);
        trace.record_items(GatewayOperationItemKind::ThreadTreeReturnedThreads, 12);
        let timeline = trace.timeline.clone();

        trace.finish_success();

        let state = timeline.lock();
        assert_eq!(
            state.variant,
            Some(GatewayOperationVariant::ThreadTreePrincipalScoped)
        );
        assert_eq!(state.items.len(), 1);
        assert_eq!(state.items[0].count, 12);
        assert_eq!(
            state.items[0].kind.span_attribute(),
            "thread_tree.threads.returned.count"
        );
    }

    #[test]
    fn stages_use_stable_names_and_bounded_runtime_kinds() {
        let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::All);
        trace
            .stage(GatewayProviderWarmupStage::WorkspaceLoad)
            .succeed();
        trace
            .runtime_stage(
                GatewayProviderWarmupStage::RuntimeAccountProbe,
                GatewayCliRuntimeKind::Codex,
            )
            .succeed();
        trace
            .runtime_stage(
                GatewayProviderWarmupStage::RuntimeModelsLoad,
                GatewayCliRuntimeKind::Codex,
            )
            .succeed();
        trace
            .api_provider_stage(GatewayProviderWarmupStage::ApiInstanceWarmup, "openai")
            .succeed();

        let state = trace.timeline.lock();
        assert_eq!(state.stages[0].name, "workspace.load");
        assert_eq!(state.stages[0].runtime_kind, None);
        assert_eq!(state.stages[1].name, "runtime.account.probe");
        assert_eq!(
            state.stages[1].runtime_kind,
            Some(GatewayCliRuntimeKind::Codex)
        );
        assert_eq!(state.stages[2].name, "runtime.models.load");
        assert_eq!(
            state.stages[2].runtime_kind,
            Some(GatewayCliRuntimeKind::Codex)
        );
        assert_eq!(state.stages[3].name, "api.instance.warmup");
        assert_eq!(state.stages[3].provider_kind.as_deref(), Some("openai"));
    }

    #[test]
    fn a_provider_sample_cannot_cross_a_consent_generation() {
        assert!(super::super::telemetry_sample_allowed_for_state(
            Some(7),
            true,
            7
        ));
        assert!(!super::super::telemetry_sample_allowed_for_state(
            None, true, 7
        ));
        assert!(!super::super::telemetry_sample_allowed_for_state(
            Some(7),
            false,
            7
        ));
        assert!(!super::super::telemetry_sample_allowed_for_state(
            Some(7),
            true,
            8
        ));
    }

    #[test]
    fn dropped_trace_is_a_cancellation_and_interrupted_stage_is_recorded() {
        let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::Cli);
        drop(trace.stage(GatewayProviderWarmupStage::CliCatalogLoad));
        let timeline = trace.timeline.clone();
        drop(trace);

        let state = timeline.lock();
        assert!(state.finalized);
        assert_eq!(state.stages[0].outcome, OperationStageOutcome::Error);
    }

    #[test]
    fn cancelled_probe_has_an_explicit_non_failure_readiness_outcome() {
        let snapshot = super::GatewayProviderWarmupSnapshot {
            scope: GatewayProviderWarmupScope::Cli,
            consent_generation: Some(0),
            started_at: std::time::SystemTime::now(),
            elapsed: std::time::Duration::ZERO,
            stages: Vec::new(),
            readiness: Vec::new(),
            outcome: "cancelled",
            failed: false,
        };

        assert_eq!(snapshot.readiness_outcome(), "cancelled");
        assert!(!snapshot.failed);
    }

    #[test]
    fn explicitly_cancelled_stage_is_not_recorded_as_an_error() {
        let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::Cli);
        trace
            .stage(GatewayProviderWarmupStage::WorkspaceLoad)
            .cancel();

        let state = trace.timeline.lock();
        assert_eq!(state.stages.len(), 1);
        assert_eq!(state.stages[0].outcome, OperationStageOutcome::Cancelled);
    }

    #[test]
    fn explicit_probe_failure_finalizes_the_trace() {
        let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::Cli);
        let timeline = trace.timeline.clone();

        trace.finish_failure();

        assert!(timeline.lock().finalized);
    }

    #[test]
    fn readiness_distinguishes_probe_completion_from_provider_availability() {
        let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::All);
        trace.record_api_readiness("openai", GatewayProviderReadinessState::Ready);
        trace.record_cli_readiness(
            GatewayCliRuntimeKind::Codex,
            GatewayProviderReadinessState::NeedsAuth,
        );

        let state = trace.timeline.lock();
        assert_eq!(state.readiness.len(), 2);
        assert_eq!(
            state.readiness[0].state,
            GatewayProviderReadinessState::Ready
        );
        assert_eq!(state.readiness[0].provider_kind.as_deref(), Some("openai"));
        assert_eq!(
            state.readiness[1].state,
            GatewayProviderReadinessState::NeedsAuth
        );
    }

    #[test]
    fn disabled_providers_are_not_counted_as_unavailable() {
        let snapshot = super::GatewayProviderWarmupSnapshot {
            scope: GatewayProviderWarmupScope::Cli,
            consent_generation: Some(0),
            started_at: std::time::SystemTime::now(),
            elapsed: std::time::Duration::ZERO,
            stages: Vec::new(),
            readiness: vec![super::ProviderReadinessRecord {
                provider_type: super::GatewayProviderType::Cli,
                runtime_kind: Some(GatewayCliRuntimeKind::Claude),
                provider_kind: None,
                state: GatewayProviderReadinessState::Disabled,
            }],
            outcome: "ok",
            failed: false,
        };

        assert_eq!(snapshot.readiness_outcome(), "not_applicable");
    }

    #[test]
    fn unsupported_safe_probe_is_reported_as_unverified_without_retry_failure() {
        let snapshot = super::GatewayProviderWarmupSnapshot {
            scope: GatewayProviderWarmupScope::Api,
            consent_generation: Some(0),
            started_at: std::time::SystemTime::now(),
            elapsed: std::time::Duration::ZERO,
            stages: Vec::new(),
            readiness: vec![super::ProviderReadinessRecord {
                provider_type: super::GatewayProviderType::Api,
                runtime_kind: None,
                provider_kind: Some("local".to_owned()),
                state: GatewayProviderReadinessState::Unverified,
            }],
            outcome: "ok",
            failed: false,
        };

        assert_eq!(snapshot.readiness_outcome(), "unverified");
        assert!(!snapshot.failed);
    }

    #[test]
    fn superseded_probe_has_an_explicit_non_failure_outcome() {
        let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::Cli);
        let timeline = trace.timeline.clone();

        trace.finish_superseded();

        let state = timeline.lock();
        assert!(state.finalized);
        drop(state);

        let snapshot = super::GatewayProviderWarmupSnapshot {
            scope: GatewayProviderWarmupScope::Cli,
            consent_generation: Some(0),
            started_at: std::time::SystemTime::now(),
            elapsed: std::time::Duration::ZERO,
            stages: Vec::new(),
            readiness: Vec::new(),
            outcome: "superseded",
            failed: false,
        };
        assert_eq!(snapshot.readiness_outcome(), "superseded");
    }
}
