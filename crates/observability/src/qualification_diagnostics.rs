//! Qualification instrumentation contracts.
//!
//! This module is intentionally shell-neutral, bounded, and free of
//! user-provided identifiers or payload content. Recording is a no-op unless the
//! `qualification-diagnostics` feature is compiled and an isolated qualification run
//! explicitly starts a capture. It is not coupled to telemetry consent and it
//! never performs I/O or network export by itself.

use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fmt,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub const QUALIFICATION_DIAGNOSTIC_SCHEMA_VERSION: u16 = 1;
pub const MAX_CAPTURE_RECORDS: usize = 200_000;
pub const MAX_CAPTURE_DURATION_MS: u64 = 120_000;

macro_rules! bounded_enum {
    ($name:ident { $($variant:ident),+ $(,)? }) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($variant),+ }
    };
}

/// Application-owned source lifecycle observations. `Scheduled` and `Woke`
/// bracket an owned delay, `Requested` means that the source invoked its
/// immediate downstream handoff, and `Executed` marks an owned animation
/// callback. `Completed` means that the observed local source or handoff ended;
/// it does not claim that later domain work changed state or succeeded.
/// `Observed` records selection of an opaque stock component, not its paint or
/// framework callback lifecycle. The source registry defines the meaningful
/// subset and reconciliation rules for each source.
bounded_enum!(AnimationAction {
    Scheduled,
    Executed,
    Woke,
    Cancelled,
    Requested,
    Completed,
    Observed,
});

/// Call-site token accepted by the recorder APIs. Each recorder validates and
/// converts it to the narrower action enum stored in the serialized series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticAction {
    Scheduled,
    Executed,
    Woke,
    Cancelled,
    Attempted,
    Requested,
    Delivered,
    Completed,
    StaleDiscard,
    Received,
    Applied,
    Dropped,
    Hit,
    Miss,
}

bounded_enum!(PresentationAction { Executed });

bounded_enum!(DeliveryAction {
    Attempted,
    Received,
    Delivered,
    Applied,
    Completed,
    StaleDiscard,
    Dropped,
});

bounded_enum!(TimelineAction {
    Executed,
    Requested,
    Applied,
    Completed,
    StaleDiscard,
    Hit,
    Miss,
});

bounded_enum!(Visibility {
    Visible,
    Offscreen,
    Global,
    NotApplicable,
});

bounded_enum!(RenderRegion {
    DesktopShell,
    Titlebar,
    SidebarHost,
    Sidebar,
    ScreenHost,
    ThreadScreen,
    ThreadHeader,
    Timeline,
    TimelineRowSlot,
    RowBody,
    Markdown,
    RunningActivity,
    RunningDino,
    RunningElapsed,
    Composer,
    SidePanel,
    AgentsDoc,
    AgentsDocEditor,
    Providers,
    Administration,
    Mcp,
    Skills,
    Settings,
    GatewaySetup,
    Invitation,
    BottomBar,
});

bounded_enum!(ClientHostApp { Desktop, Mobile });
bounded_enum!(ClientScope {
    Thread,
    Workspace,
    Navigation,
    Composer,
    Provider,
    Administration,
    Mcp,
    Skills,
    Settings,
    Auth,
    PendingRequest,
    TaskNotification,
    Artifact,
    Avatar,
    Startup,
    Other,
});
bounded_enum!(Shell { Desktop, Mobile });
bounded_enum!(DeliveryLayer {
    DesktopEventPump,
    DesktopRootReducer,
    MobileFfiGatewayEvents,
    MobileFfiActiveThreadReducer,
    MobileBinding,
});
bounded_enum!(DeliveryMeasurement {
    PayloadBytes,
    BatchItems,
});

bounded_enum!(PresentationOwner { DesktopShell, Client });

bounded_enum!(PresentationStage {
    SemanticFlatten,
    SemanticProjection,
    RunningHydration,
    SemanticRowPendingMerge,
    TimelinePendingRequestMerge,
    TimelineFingerprint,
});

bounded_enum!(TimelineStage {
    RowReconcile,
    RowBuild,
    ItemSizesCacheLookup,
    ItemSizesBuild,
    RowLayoutCacheLookup,
    RowLayoutInvoke,
    RowLayoutResult,
    VisibleRowTraversal,
    VisibleRowElementBuild,
    MarkdownDocumentProjection,
    MarkdownCodeBlockProjection,
    MarkdownHighlightPlan,
    MarkdownHighlightResultApply,
    MarkdownElementBuild,
    InlineElapsedFormat,
});

/// Stable IDs for application-owned animation selections and timer-driven
/// sources that can request work or produce UI updates.
/// Existing discriminants are immutable; additions must append a new value.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[repr(u16)]
#[serde(rename_all = "snake_case")]
pub enum AnimationSourceId {
    TimelineRunningCommand = 1,
    TimelineRunningDownload = 2,
    TimelineRunningDynamicTool = 3,
    TimelineRunningFileChange = 4,
    TimelineRunningReasoning = 5,
    TimelineRunningWebFetch = 6,
    TimelineRunningWebSearch = 7,
    TimelineRunningDinoClock = 8,
    TimelineRunningElapsedClock = 9,
    ProgressCircleTransition = 10,
    AdministrationInvitationList = 20,
    AdministrationInvitationCreateOverlay = 21,
    AdministrationMemberList = 22,
    GatewayPopover = 23,
    GatewaySetupRemoteButton = 24,
    GatewaySetupDeleteButton = 25,
    InvitationJoin = 26,
    McpInstallDialogButton = 27,
    McpPoller = 28,
    ProviderRefreshButton = 29,
    RemoteAccessPoller = 30,
    DesktopUpdateDownload = 31,
    SkillsUpdateButton = 32,
    SkillsPoller = 33,
    ThreadArtifactAction = 34,
    ComposerModelSelector = 35,
    ThreadMemberList = 36,
    WorkspaceSelector = 37,
    DeviceActivation = 38,
    SharedModelSelector = 39,
    ProfileEditor = 40,
    AdministrationInvitationCreateButton = 41,
    McpSidebarInstallButton = 42,
    McpSidebarRefreshButton = 43,
    McpSidebarRestartButton = 44,
    GatewaySetupLocalButton = 45,
    ComposerCancelTurnButton = 46,
    ComposerPrimaryActionButton = 47,
    ComposerAttachmentUpload = 48,
    ThreadMemberAddButton = 49,
    ArtifactDownloadProgressClock = 50,
    DesktopVoiceStatusPoller = 51,
    ComposerSubmissionSessionWait = 52,
    VoiceCaptureSessionWait = 53,
    WorkspaceSwitchSessionWait = 54,
    GatewaySessionRefreshClock = 55,
    GatewaySessionRefreshDeferral = 56,
    ThreadStartRetryClock = 57,
    TurnResumeScheduleClock = 58,
    CurrentPrincipalRefreshRetry = 59,
    ThreadCapabilityRefreshRetry = 60,
    ThreadArtifactRefreshRetry = 61,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "series", rename_all = "snake_case", deny_unknown_fields)]
pub enum DiagnosticEventKey {
    Animation {
        source_id: AnimationSourceId,
        action: AnimationAction,
        visibility: Visibility,
    },
    Render {
        region: RenderRegion,
    },
    Presentation {
        owner: PresentationOwner,
        host_app: ClientHostApp,
        stage: PresentationStage,
        visibility: Visibility,
        action: PresentationAction,
    },
    ClientDelivery {
        shell: Shell,
        layer: DeliveryLayer,
        scope: ClientScope,
        action: DeliveryAction,
        visibility: Visibility,
    },
    ClientDeliveryMeasurement {
        shell: Shell,
        layer: DeliveryLayer,
        scope: ClientScope,
        measurement: DeliveryMeasurement,
    },
    Timeline {
        stage: TimelineStage,
        action: TimelineAction,
    },
    ConsistencyError {
        kind: ConsistencyErrorKind,
    },
}

bounded_enum!(ConsistencyErrorKind {
    InvalidShellLayerPair,
    InvalidPresentationOwnerHostPair,
    InvalidActionForSeries,
    InvalidAnimationActionForSource,
    InvalidAnimationVisibilityForSourceAction,
    InvalidTimelineActionForStage,
    InvalidDeliveryActionForLayer,
    InvalidDeliveryMeasurementForLayer,
    InvalidDeliveryVisibilityForLayer,
});

/// Immutable output record. Records can only be created by the bounded
/// recorder; accessors expose their safe numeric and enum fields read-only.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DiagnosticRecord {
    elapsed_micros: u64,
    event: DiagnosticEventKey,
    /// Exact count or bounded numeric observation. Never an identifier.
    value: u64,
}

impl DiagnosticRecord {
    pub fn elapsed_micros(&self) -> u64 {
        self.elapsed_micros
    }

    pub fn event(&self) -> DiagnosticEventKey {
        self.event
    }

    pub fn value(&self) -> u64 {
        self.value
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationScenario {
    Idle,
    VisibleAnimation,
    VisibleStream,
    OffscreenStream,
    OneRowChange,
    FailureLoop,
    IdenticalReplay,
    CallbackAttribution,
    CounterReconciliation,
    ProfilerOverhead,
    UiOracleCapture,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureLimits {
    pub max_records: usize,
    pub max_duration_ms: u64,
}

impl CaptureLimits {
    pub fn validate(self) -> Result<Self, DiagnosticGuardError> {
        if self.max_records == 0 || self.max_records > MAX_CAPTURE_RECORDS {
            return Err(DiagnosticGuardError::InvalidRecordLimit);
        }
        if self.max_duration_ms == 0 || self.max_duration_ms > MAX_CAPTURE_DURATION_MS {
            return Err(DiagnosticGuardError::InvalidDurationLimit);
        }
        Ok(self)
    }
}

/// Explicit, non-serializable caller attestation required before local
/// capture or export. This validates claims; it cannot inspect the host.
pub struct QualificationIsolationAttestation {
    _private: (),
}

impl QualificationIsolationAttestation {
    pub fn attest(
        runner_is_disposable: bool,
        process_owned_by_run: bool,
        credentials_absent: bool,
        product_state_absent: bool,
        network_denied: bool,
    ) -> Result<Self, DiagnosticGuardError> {
        if !(runner_is_disposable
            && process_owned_by_run
            && credentials_absent
            && product_state_absent
            && network_denied)
        {
            return Err(DiagnosticGuardError::IsolationNotAttested);
        }
        Ok(Self { _private: () })
    }
}

/// Immutable, serialization-only capture returned by
/// [`stop_qualification_capture`]. Internal limits travel with the value so
/// export can revalidate the exact capture bounds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DiagnosticCaptureSnapshot {
    schema_version: u16,
    scenario: QualificationScenario,
    records: Vec<DiagnosticRecord>,
    dropped_records: u64,
    stopped_after_micros: u64,
    #[serde(skip_serializing)]
    record_limit: usize,
    #[serde(skip_serializing)]
    duration_limit_micros: u64,
}

impl DiagnosticCaptureSnapshot {
    pub fn schema_version(&self) -> u16 {
        self.schema_version
    }

    pub fn scenario(&self) -> QualificationScenario {
        self.scenario
    }

    pub fn records(&self) -> &[DiagnosticRecord] {
        self.records.as_slice()
    }

    pub fn dropped_records(&self) -> u64 {
        self.dropped_records
    }

    pub fn stopped_after_micros(&self) -> u64 {
        self.stopped_after_micros
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticGuardError {
    IsolationNotAttested,
    InvalidRecordLimit,
    InvalidDurationLimit,
    CaptureAlreadyActive,
    CaptureAlreadyConsumed,
    CaptureNotActive,
    InvalidSnapshot,
    SerializationFailed,
}

impl fmt::Display for DiagnosticGuardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", match self {
            Self::IsolationNotAttested => "qualification isolation was not fully attested",
            Self::InvalidRecordLimit => "diagnostic record limit is outside the bounded range",
            Self::InvalidDurationLimit => "diagnostic duration is outside the bounded range",
            Self::CaptureAlreadyActive => "a diagnostic capture is already active",
            Self::CaptureAlreadyConsumed => {
                "this process has already consumed its single diagnostic capture"
            }
            Self::CaptureNotActive => "no diagnostic capture is active",
            Self::InvalidSnapshot => "diagnostic snapshot violates its frozen capture bounds",
            Self::SerializationFailed => "diagnostic snapshot serialization failed",
        })
    }
}

impl std::error::Error for DiagnosticGuardError {}

struct CaptureState {
    generation: u64,
    scenario: QualificationScenario,
    limits: CaptureLimits,
    started_at: Instant,
    records: VecDeque<DiagnosticRecord>,
    dropped_records: u64,
    recording_stopped_after_micros: Option<u64>,
}

static CAPTURE: LazyLock<Mutex<Option<CaptureState>>> = LazyLock::new(|| Mutex::new(None));
static CAPTURE_CONSUMED: AtomicBool = AtomicBool::new(false);
static ACTIVE_CAPTURE_GENERATION: AtomicU64 = AtomicU64::new(0);
static NEXT_CAPTURE_GENERATION: AtomicU64 = AtomicU64::new(1);

/// Starts the process's single capture. Qualification uses one disposable
/// process per scenario so late asynchronous work cannot bleed into a later
/// scenario's records.
pub fn start_qualification_capture(
    _isolation: &QualificationIsolationAttestation,
    scenario: QualificationScenario,
    limits: CaptureLimits,
) -> Result<(), DiagnosticGuardError> {
    let limits = limits.validate()?;
    let mut capture = CAPTURE.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if capture.is_some() {
        return Err(DiagnosticGuardError::CaptureAlreadyActive);
    }
    if CAPTURE_CONSUMED.swap(true, Ordering::AcqRel) {
        return Err(DiagnosticGuardError::CaptureAlreadyConsumed);
    }
    let generation = loop {
        let candidate = NEXT_CAPTURE_GENERATION.fetch_add(1, Ordering::Relaxed);
        if candidate != 0 {
            break candidate;
        }
    };
    let initial_capacity = limits.max_records.min(4_096);
    *capture = Some(CaptureState {
        generation,
        scenario,
        limits,
        started_at: Instant::now(),
        records: VecDeque::with_capacity(initial_capacity),
        dropped_records: 0,
        recording_stopped_after_micros: None,
    });
    ACTIVE_CAPTURE_GENERATION.store(generation, Ordering::Release);
    Ok(())
}

pub fn stop_qualification_capture(
    _isolation: &QualificationIsolationAttestation,
) -> Result<DiagnosticCaptureSnapshot, DiagnosticGuardError> {
    let mut capture = CAPTURE.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    ACTIVE_CAPTURE_GENERATION.store(0, Ordering::Release);
    let state = capture.take().ok_or(DiagnosticGuardError::CaptureNotActive)?;
    let max_duration_micros = state.limits.max_duration_ms.saturating_mul(1_000);
    let stopped_after_micros = state
        .recording_stopped_after_micros
        .unwrap_or_else(|| elapsed_micros(state.started_at.elapsed()))
        .min(max_duration_micros);
    Ok(DiagnosticCaptureSnapshot {
        schema_version: QUALIFICATION_DIAGNOSTIC_SCHEMA_VERSION,
        scenario: state.scenario,
        records: state.records.into(),
        dropped_records: state.dropped_records,
        stopped_after_micros,
        record_limit: state.limits.max_records,
        duration_limit_micros: max_duration_micros,
    })
}

/// Serialization is explicit and still performs no filesystem or network I/O.
pub fn export_qualification_capture_json(
    _isolation: &QualificationIsolationAttestation,
    snapshot: &DiagnosticCaptureSnapshot,
) -> Result<Vec<u8>, DiagnosticGuardError> {
    validate_snapshot_for_export(snapshot)?;
    serde_json::to_vec(snapshot).map_err(|_| DiagnosticGuardError::SerializationFailed)
}

fn validate_snapshot_for_export(
    snapshot: &DiagnosticCaptureSnapshot,
) -> Result<(), DiagnosticGuardError> {
    let global_duration_limit_micros = MAX_CAPTURE_DURATION_MS.saturating_mul(1_000);
    if snapshot.schema_version != QUALIFICATION_DIAGNOSTIC_SCHEMA_VERSION
        || snapshot.record_limit == 0
        || snapshot.record_limit > MAX_CAPTURE_RECORDS
        || snapshot.duration_limit_micros < 1_000
        || snapshot.duration_limit_micros % 1_000 != 0
        || snapshot.duration_limit_micros > global_duration_limit_micros
        || snapshot.records.len() > snapshot.record_limit
        || snapshot.stopped_after_micros > snapshot.duration_limit_micros
    {
        return Err(DiagnosticGuardError::InvalidSnapshot);
    }

    let mut previous_elapsed_micros = 0;
    for record in &snapshot.records {
        if record.elapsed_micros < previous_elapsed_micros
            || record.elapsed_micros > snapshot.stopped_after_micros
            || !diagnostic_event_is_valid(record.event)
        {
            return Err(DiagnosticGuardError::InvalidSnapshot);
        }
        previous_elapsed_micros = record.elapsed_micros;
    }
    Ok(())
}

fn diagnostic_event_is_valid(event: DiagnosticEventKey) -> bool {
    match event {
        DiagnosticEventKey::Animation { source_id, action, visibility } => {
            animation_action_matches_source(source_id, action)
                && animation_visibility_matches_source_action(source_id, action, visibility)
        }
        DiagnosticEventKey::Render { .. } | DiagnosticEventKey::ConsistencyError { .. } => true,
        DiagnosticEventKey::Presentation { owner, host_app, .. } => {
            owner != PresentationOwner::DesktopShell || host_app == ClientHostApp::Desktop
        }
        DiagnosticEventKey::ClientDelivery { shell, layer, action, visibility, .. } => {
            delivery_layer_matches_shell(shell, layer)
                && delivery_wire_action_matches_layer(layer, action)
                && delivery_visibility_matches_layer(layer, visibility)
        }
        DiagnosticEventKey::ClientDeliveryMeasurement {
            shell,
            layer,
            measurement,
            ..
        } => {
            delivery_layer_matches_shell(shell, layer)
                && delivery_measurement_matches_layer(layer, measurement)
        }
        DiagnosticEventKey::Timeline { stage, action } => {
            timeline_action_matches_stage(stage, action)
        }
    }
}

#[inline(always)]
fn record_diagnostic_event(event: DiagnosticEventKey, value: u64) {
    let generation = ACTIVE_CAPTURE_GENERATION.load(Ordering::Acquire);
    if generation == 0 {
        return;
    }
    let mut capture = CAPTURE.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(state) = capture.as_mut() else { return };
    if state.generation != generation {
        return;
    }
    let elapsed = state.started_at.elapsed();
    if elapsed >= Duration::from_millis(state.limits.max_duration_ms) {
        state.dropped_records = state.dropped_records.saturating_add(1);
        state.recording_stopped_after_micros = Some(elapsed_micros(elapsed));
        ACTIVE_CAPTURE_GENERATION.store(0, Ordering::Release);
        return;
    }
    if state.records.len() >= state.limits.max_records {
        state.records.pop_front();
        state.dropped_records = state.dropped_records.saturating_add(1);
    }
    state.records.push_back(DiagnosticRecord {
        elapsed_micros: elapsed_micros(elapsed),
        event,
        value,
    });
}

/// Emits a call-site observation for an application-owned animation selection.
/// This records source presence, not paint or a GPUI animation-frame callback.
#[inline(always)]
pub fn record_animation_source_observed(
    source_id: AnimationSourceId,
    visibility: Visibility,
) {
    let action = AnimationAction::Observed;
    if !animation_action_matches_source(source_id, action) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidAnimationActionForSource,
            },
            1,
        );
        return;
    }
    if !animation_visibility_matches_source_action(source_id, action, visibility) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidAnimationVisibilityForSourceAction,
            },
            1,
        );
        return;
    }
    record_diagnostic_event(
        DiagnosticEventKey::Animation {
            source_id,
            action,
            visibility,
        },
        1,
    );
}

/// Records a stock loading-indicator selection only when the application's
/// existing loading state is active.
#[inline(always)]
pub fn record_loading_animation_source(
    source_id: AnimationSourceId,
    is_loading: bool,
) {
    if is_loading {
        record_animation_source_observed(source_id, Visibility::NotApplicable);
    }
}

#[inline(always)]
pub fn record_animation_activity(
    source_id: AnimationSourceId,
    action: DiagnosticAction,
    visibility: Visibility,
) {
    let Some(action) = animation_action(action) else {
        record_invalid_action_for_series();
        return;
    };
    if !animation_action_matches_source(source_id, action) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidAnimationActionForSource,
            },
            1,
        );
        return;
    }
    if !animation_visibility_matches_source_action(source_id, action, visibility) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidAnimationVisibilityForSourceAction,
            },
            1,
        );
        return;
    }
    record_diagnostic_event(DiagnosticEventKey::Animation { source_id, action, visibility }, 1);
}

#[inline(always)]
pub fn record_render(region: RenderRegion) {
    record_diagnostic_event(DiagnosticEventKey::Render { region }, 1);
}

#[inline(always)]
pub fn record_presentation(
    owner: PresentationOwner,
    host_app: ClientHostApp,
    stage: PresentationStage,
    visibility: Visibility,
    action: DiagnosticAction,
) {
    if owner == PresentationOwner::DesktopShell && host_app != ClientHostApp::Desktop {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidPresentationOwnerHostPair,
            },
            1,
        );
        return;
    }
    let Some(action) = presentation_action(action) else {
        record_invalid_action_for_series();
        return;
    };
    record_diagnostic_event(
        DiagnosticEventKey::Presentation {
            owner,
            host_app,
            stage,
            visibility,
            action,
        },
        1,
    );
}

#[inline(always)]
pub fn record_client_delivery(
    shell: Shell,
    layer: DeliveryLayer,
    scope: ClientScope,
    action: DiagnosticAction,
    visibility: Visibility,
) {
    if !delivery_layer_matches_shell(shell, layer) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidShellLayerPair,
            },
            1,
        );
        return;
    }
    if !delivery_action_matches_layer(layer, action) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidDeliveryActionForLayer,
            },
            1,
        );
        return;
    }
    if !delivery_visibility_matches_layer(layer, visibility) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidDeliveryVisibilityForLayer,
            },
            1,
        );
        return;
    }
    let Some(action) = delivery_action(action) else {
        record_invalid_action_for_series();
        return;
    };
    record_diagnostic_event(
        DiagnosticEventKey::ClientDelivery { shell, layer, scope, action, visibility },
        1,
    );
}

#[inline(always)]
pub fn record_client_delivery_measurement(
    shell: Shell,
    layer: DeliveryLayer,
    scope: ClientScope,
    measurement: DeliveryMeasurement,
    value: u64,
) {
    if !delivery_layer_matches_shell(shell, layer) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidShellLayerPair,
            },
            1,
        );
        return;
    }
    if !delivery_measurement_matches_layer(layer, measurement) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidDeliveryMeasurementForLayer,
            },
            1,
        );
        return;
    }
    record_diagnostic_event(
        DiagnosticEventKey::ClientDeliveryMeasurement {
            shell,
            layer,
            scope,
            measurement,
        },
        value,
    );
}

fn delivery_layer_matches_shell(shell: Shell, layer: DeliveryLayer) -> bool {
    matches!(
        (shell, layer),
        (Shell::Desktop, DeliveryLayer::DesktopEventPump | DeliveryLayer::DesktopRootReducer)
            | (
                Shell::Mobile,
                DeliveryLayer::MobileFfiGatewayEvents
                    | DeliveryLayer::MobileFfiActiveThreadReducer
                    | DeliveryLayer::MobileBinding
            )
    )
}

fn delivery_action_matches_layer(layer: DeliveryLayer, action: DiagnosticAction) -> bool {
    delivery_action(action)
        .is_some_and(|action| delivery_wire_action_matches_layer(layer, action))
}

fn delivery_wire_action_matches_layer(layer: DeliveryLayer, action: DeliveryAction) -> bool {
    matches!(
        (layer, action),
        (DeliveryLayer::DesktopEventPump, DeliveryAction::Delivered)
            | (DeliveryLayer::DesktopRootReducer, DeliveryAction::Attempted)
            | (DeliveryLayer::DesktopRootReducer, DeliveryAction::Completed)
            | (
                DeliveryLayer::MobileFfiGatewayEvents,
                DeliveryAction::Completed | DeliveryAction::Dropped
            )
            | (DeliveryLayer::MobileFfiActiveThreadReducer, DeliveryAction::Received)
            | (DeliveryLayer::MobileFfiActiveThreadReducer, DeliveryAction::Completed)
            | (DeliveryLayer::MobileFfiActiveThreadReducer, DeliveryAction::Dropped)
            | (DeliveryLayer::MobileBinding, DeliveryAction::Received)
            | (DeliveryLayer::MobileBinding, DeliveryAction::Delivered)
            | (DeliveryLayer::MobileBinding, DeliveryAction::Applied)
            | (DeliveryLayer::MobileBinding, DeliveryAction::StaleDiscard)
            | (DeliveryLayer::MobileBinding, DeliveryAction::Dropped)
    )
}

fn delivery_measurement_matches_layer(
    layer: DeliveryLayer,
    measurement: DeliveryMeasurement,
) -> bool {
    matches!(
        (layer, measurement),
        (DeliveryLayer::MobileFfiGatewayEvents, DeliveryMeasurement::BatchItems)
            | (
                DeliveryLayer::MobileFfiActiveThreadReducer,
                DeliveryMeasurement::PayloadBytes
            )
            | (DeliveryLayer::MobileBinding, DeliveryMeasurement::BatchItems)
    )
}

fn delivery_visibility_matches_layer(layer: DeliveryLayer, visibility: Visibility) -> bool {
    match layer {
        DeliveryLayer::DesktopEventPump
        | DeliveryLayer::DesktopRootReducer
        | DeliveryLayer::MobileFfiGatewayEvents
        | DeliveryLayer::MobileFfiActiveThreadReducer => visibility == Visibility::NotApplicable,
        DeliveryLayer::MobileBinding => matches!(
            visibility,
            Visibility::Visible | Visibility::Offscreen | Visibility::NotApplicable
        ),
    }
}

#[inline(always)]
pub fn record_timeline(stage: TimelineStage, action: DiagnosticAction, value: u64) {
    let Some(action) = timeline_action(action) else {
        record_invalid_action_for_series();
        return;
    };
    if !timeline_action_matches_stage(stage, action) {
        record_diagnostic_event(
            DiagnosticEventKey::ConsistencyError {
                kind: ConsistencyErrorKind::InvalidTimelineActionForStage,
            },
            1,
        );
        return;
    }
    record_diagnostic_event(DiagnosticEventKey::Timeline { stage, action }, value);
}

fn animation_action_matches_source(
    source_id: AnimationSourceId,
    action: AnimationAction,
) -> bool {
    use AnimationAction::{Cancelled, Completed, Executed, Observed, Requested, Scheduled, Woke};
    use AnimationSourceId::*;

    match source_id {
        TimelineRunningCommand
        | TimelineRunningDownload
        | TimelineRunningDynamicTool
        | TimelineRunningFileChange
        | TimelineRunningReasoning
        | TimelineRunningWebFetch
        | TimelineRunningWebSearch
        | AdministrationInvitationList
        | AdministrationInvitationCreateOverlay
        | AdministrationMemberList
        | GatewayPopover
        | GatewaySetupRemoteButton
        | GatewaySetupDeleteButton
        | InvitationJoin
        | McpInstallDialogButton
        | ProviderRefreshButton
        | DesktopUpdateDownload
        | SkillsUpdateButton
        | ThreadArtifactAction
        | ComposerModelSelector
        | ThreadMemberList
        | WorkspaceSelector
        | DeviceActivation
        | SharedModelSelector
        | ProfileEditor
        | AdministrationInvitationCreateButton
        | McpSidebarInstallButton
        | McpSidebarRefreshButton
        | McpSidebarRestartButton
        | GatewaySetupLocalButton
        | ComposerCancelTurnButton
        | ComposerPrimaryActionButton
        | ComposerAttachmentUpload
        | ThreadMemberAddButton => action == Observed,
        TimelineRunningDinoClock | TimelineRunningElapsedClock | McpPoller | SkillsPoller => {
            matches!(action, Scheduled | Woke | Requested | Cancelled)
        }
        ProgressCircleTransition => matches!(
            action,
            Scheduled | Woke | Requested | Executed | Completed
        ),
        RemoteAccessPoller | ArtifactDownloadProgressClock | DesktopVoiceStatusPoller => {
            matches!(action, Scheduled | Woke | Requested | Cancelled | Completed)
        }
        ComposerSubmissionSessionWait | VoiceCaptureSessionWait => {
            matches!(action, Scheduled | Woke | Requested)
        }
        WorkspaceSwitchSessionWait
        | GatewaySessionRefreshClock
        | GatewaySessionRefreshDeferral
        | ThreadStartRetryClock
        | TurnResumeScheduleClock
        | CurrentPrincipalRefreshRetry
        | ThreadCapabilityRefreshRetry
        | ThreadArtifactRefreshRetry => {
            matches!(action, Scheduled | Woke | Requested | Cancelled | Completed)
        }
    }
}

fn animation_visibility_matches_source_action(
    source_id: AnimationSourceId,
    action: AnimationAction,
    visibility: Visibility,
) -> bool {
    use AnimationAction::{Cancelled, Completed, Requested, Scheduled, Woke};
    use AnimationSourceId::*;

    match source_id {
        McpPoller | SkillsPoller | RemoteAccessPoller | DesktopVoiceStatusPoller => match action {
            Scheduled | Woke | Cancelled | Completed => visibility == Visibility::Global,
            Requested => visibility == Visibility::NotApplicable,
            _ => false,
        },
        _ => visibility == Visibility::NotApplicable,
    }
}

fn timeline_action_matches_stage(stage: TimelineStage, action: TimelineAction) -> bool {
    use TimelineAction::{Applied, Completed, Executed, Hit, Miss, Requested, StaleDiscard};
    use TimelineStage::*;

    match stage {
        RowReconcile | ItemSizesBuild | RowLayoutResult | VisibleRowElementBuild => {
            action == Completed
        }
        RowBuild
        | RowLayoutInvoke
        | VisibleRowTraversal
        | MarkdownDocumentProjection
        | MarkdownCodeBlockProjection
        | MarkdownElementBuild
        | InlineElapsedFormat => action == Executed,
        ItemSizesCacheLookup | RowLayoutCacheLookup => matches!(action, Hit | Miss),
        MarkdownHighlightPlan => action == Requested,
        MarkdownHighlightResultApply => matches!(action, Applied | StaleDiscard),
    }
}

fn animation_action(action: DiagnosticAction) -> Option<AnimationAction> {
    Some(match action {
        DiagnosticAction::Scheduled => AnimationAction::Scheduled,
        DiagnosticAction::Executed => AnimationAction::Executed,
        DiagnosticAction::Woke => AnimationAction::Woke,
        DiagnosticAction::Cancelled => AnimationAction::Cancelled,
        DiagnosticAction::Requested => AnimationAction::Requested,
        DiagnosticAction::Completed => AnimationAction::Completed,
        _ => return None,
    })
}

fn presentation_action(action: DiagnosticAction) -> Option<PresentationAction> {
    match action {
        DiagnosticAction::Executed => Some(PresentationAction::Executed),
        _ => None,
    }
}

fn delivery_action(action: DiagnosticAction) -> Option<DeliveryAction> {
    Some(match action {
        DiagnosticAction::Attempted => DeliveryAction::Attempted,
        DiagnosticAction::Received => DeliveryAction::Received,
        DiagnosticAction::Delivered => DeliveryAction::Delivered,
        DiagnosticAction::Applied => DeliveryAction::Applied,
        DiagnosticAction::Completed => DeliveryAction::Completed,
        DiagnosticAction::StaleDiscard => DeliveryAction::StaleDiscard,
        DiagnosticAction::Dropped => DeliveryAction::Dropped,
        _ => return None,
    })
}

fn timeline_action(action: DiagnosticAction) -> Option<TimelineAction> {
    Some(match action {
        DiagnosticAction::Executed => TimelineAction::Executed,
        DiagnosticAction::Requested => TimelineAction::Requested,
        DiagnosticAction::Applied => TimelineAction::Applied,
        DiagnosticAction::Completed => TimelineAction::Completed,
        DiagnosticAction::StaleDiscard => TimelineAction::StaleDiscard,
        DiagnosticAction::Hit => TimelineAction::Hit,
        DiagnosticAction::Miss => TimelineAction::Miss,
        _ => return None,
    })
}

fn record_invalid_action_for_series() {
    record_diagnostic_event(
        DiagnosticEventKey::ConsistencyError {
            kind: ConsistencyErrorKind::InvalidActionForSeries,
        },
        1,
    );
}

fn elapsed_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolation_and_capture_limits_reject_incomplete_guards() {
        assert_eq!(
            QualificationIsolationAttestation::attest(true, true, true, true, false).err(),
            Some(DiagnosticGuardError::IsolationNotAttested)
        );
        assert_eq!(
            CaptureLimits {
                max_records: 0,
                max_duration_ms: 1,
            }
            .validate(),
            Err(DiagnosticGuardError::InvalidRecordLimit)
        );
        assert_eq!(
            CaptureLimits {
                max_records: 1,
                max_duration_ms: MAX_CAPTURE_DURATION_MS + 1,
            }
            .validate(),
            Err(DiagnosticGuardError::InvalidDurationLimit)
        );
    }

    #[test]
    fn serialized_records_use_the_canonical_event_field() {
        let record = DiagnosticRecord {
            elapsed_micros: 7,
            event: DiagnosticEventKey::Render {
                region: RenderRegion::DesktopShell,
            },
            value: 1,
        };
        let value = serde_json::to_value(record).expect("diagnostic record JSON");
        assert!(value.get("event").is_some());
        assert!(value.get("key").is_none());
    }

    #[test]
    fn serialized_series_reject_actions_from_another_series() {
        let value = serde_json::json!({
            "series": "timeline",
            "stage": "row_build",
            "action": "scheduled"
        });
        assert!(serde_json::from_value::<DiagnosticEventKey>(value).is_err());
    }

    #[test]
    fn recurring_timer_capabilities_are_closed_over_their_exact_action_sets() {
        use AnimationAction::{
            Cancelled, Completed, Executed, Observed, Requested, Scheduled, Woke,
        };
        use AnimationSourceId::*;

        let timer_sources = [
            WorkspaceSwitchSessionWait,
            GatewaySessionRefreshClock,
            GatewaySessionRefreshDeferral,
            ThreadStartRetryClock,
            TurnResumeScheduleClock,
            CurrentPrincipalRefreshRetry,
            ThreadCapabilityRefreshRetry,
            ThreadArtifactRefreshRetry,
        ];
        for source in timer_sources {
            for action in [Scheduled, Woke, Requested, Cancelled, Completed] {
                assert!(animation_action_matches_source(source, action));
            }
            assert!(!animation_action_matches_source(source, Executed));
            assert!(!animation_action_matches_source(source, Observed));
        }

        for source in [ComposerSubmissionSessionWait, VoiceCaptureSessionWait] {
            for action in [Scheduled, Woke, Requested] {
                assert!(animation_action_matches_source(source, action));
            }
            assert!(!animation_action_matches_source(source, Cancelled));
            assert!(!animation_action_matches_source(source, Completed));
            assert!(!animation_action_matches_source(source, Executed));
            assert!(!animation_action_matches_source(source, Observed));
        }

        for action in [Scheduled, Woke, Requested, Executed, Completed] {
            assert!(animation_action_matches_source(ProgressCircleTransition, action));
        }
        assert!(!animation_action_matches_source(
            ProgressCircleTransition,
            Cancelled
        ));
        assert!(!animation_action_matches_source(
            ProgressCircleTransition,
            Observed
        ));
    }

    #[test]
    fn native_delivery_capabilities_are_closed_over_exact_terminal_actions() {
        assert!(delivery_wire_action_matches_layer(
            DeliveryLayer::MobileFfiGatewayEvents,
            DeliveryAction::Completed,
        ));
        assert!(delivery_wire_action_matches_layer(
            DeliveryLayer::MobileFfiGatewayEvents,
            DeliveryAction::Dropped,
        ));
        assert!(!delivery_wire_action_matches_layer(
            DeliveryLayer::MobileFfiGatewayEvents,
            DeliveryAction::Delivered,
        ));
        assert!(delivery_measurement_matches_layer(
            DeliveryLayer::MobileFfiGatewayEvents,
            DeliveryMeasurement::BatchItems,
        ));
        assert!(!delivery_measurement_matches_layer(
            DeliveryLayer::MobileFfiGatewayEvents,
            DeliveryMeasurement::PayloadBytes,
        ));
    }

    #[cfg(feature = "qualification-diagnostics")]
    mod enabled {
        use super::*;

        static TEST_CAPTURE: Mutex<()> = Mutex::new(());

        fn isolation() -> QualificationIsolationAttestation {
            QualificationIsolationAttestation::attest(true, true, true, true, true)
                .expect("complete isolation attestation")
        }

        fn reset_capture() {
            *CAPTURE.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            CAPTURE_CONSUMED.store(false, Ordering::Release);
            ACTIVE_CAPTURE_GENERATION.store(0, Ordering::Release);
        }

        #[test]
        fn capture_lifecycle_rejects_duplicate_start_and_stop() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            let limits = CaptureLimits {
                max_records: 1,
                max_duration_ms: MAX_CAPTURE_DURATION_MS,
            };
            start_qualification_capture(&isolation, QualificationScenario::Idle, limits)
                .expect("start capture");
            assert_eq!(
                start_qualification_capture(&isolation, QualificationScenario::Idle, limits),
                Err(DiagnosticGuardError::CaptureAlreadyActive)
            );
            stop_qualification_capture(&isolation).expect("stop capture");
            assert_eq!(
                stop_qualification_capture(&isolation),
                Err(DiagnosticGuardError::CaptureNotActive)
            );
            assert_eq!(
                start_qualification_capture(&isolation, QualificationScenario::Idle, limits),
                Err(DiagnosticGuardError::CaptureAlreadyConsumed)
            );
        }

        #[test]
        fn capture_retains_only_the_configured_number_of_records() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            start_qualification_capture(
                &isolation,
                QualificationScenario::CounterReconciliation,
                CaptureLimits {
                    max_records: 2,
                    max_duration_ms: MAX_CAPTURE_DURATION_MS,
                },
            )
            .expect("start capture");

            for _ in 0..4 {
                record_render(RenderRegion::DesktopShell);
            }

            let snapshot = stop_qualification_capture(&isolation).expect("stop capture");
            assert_eq!(snapshot.records.len(), 2);
            assert_eq!(snapshot.dropped_records, 2);
            assert!(snapshot.records.iter().all(|record| {
                record.event
                    == (DiagnosticEventKey::Render {
                        region: RenderRegion::DesktopShell,
                    })
            }));
        }

        #[test]
        fn invalid_shell_layer_pairs_are_replaced_by_a_consistency_event() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            start_qualification_capture(
                &isolation,
                QualificationScenario::CounterReconciliation,
                CaptureLimits {
                    max_records: 2,
                    max_duration_ms: MAX_CAPTURE_DURATION_MS,
                },
            )
            .expect("start capture");

            record_client_delivery(
                Shell::Desktop,
                DeliveryLayer::MobileFfiGatewayEvents,
                ClientScope::Thread,
                DiagnosticAction::Received,
                Visibility::NotApplicable,
            );

            let snapshot = stop_qualification_capture(&isolation).expect("stop capture");
            assert_eq!(snapshot.records.len(), 1);
            assert_eq!(
                snapshot.records[0].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidShellLayerPair,
                }
            );
        }

        #[test]
        fn invalid_delivery_capabilities_are_replaced_by_consistency_events() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            start_qualification_capture(
                &isolation,
                QualificationScenario::CounterReconciliation,
                CaptureLimits {
                    max_records: 3,
                    max_duration_ms: MAX_CAPTURE_DURATION_MS,
                },
            )
            .expect("start capture");

            record_client_delivery(
                Shell::Desktop,
                DeliveryLayer::DesktopEventPump,
                ClientScope::Other,
                DiagnosticAction::Applied,
                Visibility::NotApplicable,
            );
            record_client_delivery_measurement(
                Shell::Desktop,
                DeliveryLayer::DesktopRootReducer,
                ClientScope::Other,
                DeliveryMeasurement::PayloadBytes,
                1,
            );
            record_client_delivery(
                Shell::Mobile,
                DeliveryLayer::MobileFfiGatewayEvents,
                ClientScope::Other,
                DiagnosticAction::Completed,
                Visibility::Global,
            );

            let snapshot = stop_qualification_capture(&isolation).expect("stop capture");
            assert_eq!(snapshot.records.len(), 3);
            assert_eq!(
                snapshot.records[0].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidDeliveryActionForLayer,
                }
            );
            assert_eq!(
                snapshot.records[1].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidDeliveryMeasurementForLayer,
                }
            );
            assert_eq!(
                snapshot.records[2].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidDeliveryVisibilityForLayer,
                }
            );
        }

        #[test]
        fn invalid_animation_capabilities_are_replaced_by_consistency_events() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            start_qualification_capture(
                &isolation,
                QualificationScenario::CounterReconciliation,
                CaptureLimits {
                    max_records: 3,
                    max_duration_ms: MAX_CAPTURE_DURATION_MS,
                },
            )
            .expect("start capture");

            record_animation_activity(
                AnimationSourceId::TimelineRunningCommand,
                DiagnosticAction::Scheduled,
                Visibility::NotApplicable,
            );
            record_animation_source_observed(
                AnimationSourceId::McpPoller,
                Visibility::NotApplicable,
            );
            record_animation_activity(
                AnimationSourceId::McpPoller,
                DiagnosticAction::Scheduled,
                Visibility::NotApplicable,
            );

            let snapshot = stop_qualification_capture(&isolation).expect("stop capture");
            assert_eq!(snapshot.records.len(), 3);
            assert_eq!(
                snapshot.records[0].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidAnimationActionForSource,
                }
            );
            assert_eq!(
                snapshot.records[1].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidAnimationActionForSource,
                }
            );
            assert_eq!(
                snapshot.records[2].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidAnimationVisibilityForSourceAction,
                }
            );
        }

        #[test]
        fn invalid_timeline_stage_action_pair_is_replaced_by_a_consistency_event() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            start_qualification_capture(
                &isolation,
                QualificationScenario::CounterReconciliation,
                CaptureLimits {
                    max_records: 1,
                    max_duration_ms: MAX_CAPTURE_DURATION_MS,
                },
            )
            .expect("start capture");

            record_timeline(TimelineStage::RowBuild, DiagnosticAction::Hit, 1);

            let snapshot = stop_qualification_capture(&isolation).expect("stop capture");
            assert_eq!(snapshot.records.len(), 1);
            assert_eq!(
                snapshot.records[0].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidTimelineActionForStage,
                }
            );
        }

        #[test]
        fn invalid_presentation_owner_host_pairs_are_replaced_by_a_consistency_event() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            start_qualification_capture(
                &isolation,
                QualificationScenario::CounterReconciliation,
                CaptureLimits {
                    max_records: 2,
                    max_duration_ms: MAX_CAPTURE_DURATION_MS,
                },
            )
            .expect("start capture");

            record_presentation(
                PresentationOwner::DesktopShell,
                ClientHostApp::Mobile,
                PresentationStage::SemanticProjection,
                Visibility::NotApplicable,
                DiagnosticAction::Executed,
            );

            let snapshot = stop_qualification_capture(&isolation).expect("stop capture");
            assert_eq!(snapshot.records.len(), 1);
            assert_eq!(
                snapshot.records[0].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidPresentationOwnerHostPair,
                }
            );
        }

        #[test]
        fn invalid_actions_are_replaced_by_a_consistency_event() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            start_qualification_capture(
                &isolation,
                QualificationScenario::CounterReconciliation,
                CaptureLimits {
                    max_records: 2,
                    max_duration_ms: MAX_CAPTURE_DURATION_MS,
                },
            )
            .expect("start capture");

            record_timeline(
                TimelineStage::RowBuild,
                DiagnosticAction::Scheduled,
                1,
            );

            let snapshot = stop_qualification_capture(&isolation).expect("stop capture");
            assert_eq!(snapshot.records.len(), 1);
            assert_eq!(
                snapshot.records[0].event,
                DiagnosticEventKey::ConsistencyError {
                    kind: ConsistencyErrorKind::InvalidActionForSeries,
                }
            );
        }

        #[test]
        fn duration_limit_stops_recording_without_accepting_later_events() {
            let _serial = TEST_CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reset_capture();
            let isolation = isolation();
            start_qualification_capture(
                &isolation,
                QualificationScenario::CounterReconciliation,
                CaptureLimits {
                    max_records: 2,
                    max_duration_ms: 1,
                },
            )
            .expect("start capture");
            CAPTURE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
                .expect("active capture")
                .started_at = Instant::now()
                .checked_sub(Duration::from_millis(2))
                .expect("representable test instant");

            record_render(RenderRegion::DesktopShell);
            record_render(RenderRegion::DesktopShell);

            let snapshot = stop_qualification_capture(&isolation).expect("stop capture");
            assert!(snapshot.records.is_empty());
            assert_eq!(snapshot.dropped_records, 1);
            assert_eq!(snapshot.stopped_after_micros, 1_000);
        }

        #[test]
        fn export_rejects_snapshots_outside_frozen_capture_invariants() {
            let isolation = isolation();
            let valid_record = DiagnosticRecord {
                elapsed_micros: 1,
                event: DiagnosticEventKey::Render {
                    region: RenderRegion::DesktopShell,
                },
                value: 1,
            };
            let mut snapshot = DiagnosticCaptureSnapshot {
                schema_version: QUALIFICATION_DIAGNOSTIC_SCHEMA_VERSION,
                scenario: QualificationScenario::CounterReconciliation,
                records: vec![valid_record],
                dropped_records: 0,
                stopped_after_micros: 1,
                record_limit: 1,
                duration_limit_micros: 1_000,
            };

            let encoded = export_qualification_capture_json(&isolation, &snapshot)
                .expect("valid bounded snapshot");
            let encoded: serde_json::Value =
                serde_json::from_slice(&encoded).expect("diagnostic snapshot JSON");
            assert!(encoded.get("record_limit").is_none());
            assert!(encoded.get("duration_limit_micros").is_none());

            snapshot.schema_version = QUALIFICATION_DIAGNOSTIC_SCHEMA_VERSION + 1;
            assert_eq!(
                export_qualification_capture_json(&isolation, &snapshot),
                Err(DiagnosticGuardError::InvalidSnapshot)
            );
            snapshot.schema_version = QUALIFICATION_DIAGNOSTIC_SCHEMA_VERSION;

            snapshot.records.push(valid_record);
            assert_eq!(
                export_qualification_capture_json(&isolation, &snapshot),
                Err(DiagnosticGuardError::InvalidSnapshot)
            );
            snapshot.records.pop();

            snapshot.records[0].event = DiagnosticEventKey::Animation {
                source_id: AnimationSourceId::TimelineRunningCommand,
                action: AnimationAction::Scheduled,
                visibility: Visibility::NotApplicable,
            };
            assert_eq!(
                export_qualification_capture_json(&isolation, &snapshot),
                Err(DiagnosticGuardError::InvalidSnapshot)
            );

            snapshot.records[0] = valid_record;
            snapshot.stopped_after_micros = 1_001;
            assert_eq!(
                export_qualification_capture_json(&isolation, &snapshot),
                Err(DiagnosticGuardError::InvalidSnapshot)
            );
            snapshot.stopped_after_micros = 1;
            snapshot.records[0].elapsed_micros = 2;
            assert_eq!(
                export_qualification_capture_json(&isolation, &snapshot),
                Err(DiagnosticGuardError::InvalidSnapshot)
            );
        }
    }
}
