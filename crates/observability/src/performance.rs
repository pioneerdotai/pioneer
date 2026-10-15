//! Consent-gated, low-cardinality performance observations for hot runtime paths.
//!
//! The public records in this module intentionally accept only bounded enums and
//! numeric measurements. Markdown source, rendered text, URLs, workspace/thread/item
//! identifiers, and unrestricted diagnostics must never be added as metric attributes.

use crate::telemetry::TelemetryTarget;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayMarkdownStage {
    BufferLockWait,
    BufferUpdate,
    Parse,
    NotificationEncode,
    NotificationSerialize,
}

impl GatewayMarkdownStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BufferLockWait => "buffer.lock_wait",
            Self::BufferUpdate => "buffer.update",
            Self::Parse => "parse",
            Self::NotificationEncode => "notification.encode",
            Self::NotificationSerialize => "notification.serialize",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayMarkdownStreamKind {
    AgentMessage,
    Generic,
    CommandOutput,
    FileChange,
    ToolProgress,
    Snapshot,
}

impl GatewayMarkdownStreamKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AgentMessage => "agent_message",
            Self::Generic => "generic",
            Self::CommandOutput => "command_output",
            Self::FileChange => "file_change",
            Self::ToolProgress => "tool_progress",
            Self::Snapshot => "snapshot",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayMarkdownOutcome {
    Ok,
    Empty,
    Fallback,
    PanicFallback,
    Error,
}

impl GatewayMarkdownOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Empty => "empty",
            Self::Fallback => "fallback",
            Self::PanicFallback => "panic_fallback",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayMarkdownStageMetric {
    pub stage: GatewayMarkdownStage,
    pub stream: GatewayMarkdownStreamKind,
    pub outcome: GatewayMarkdownOutcome,
    pub elapsed: Duration,
    pub input_bytes: Option<usize>,
    pub output_bytes: Option<usize>,
    pub block_count: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayMarkdownMessageOutcome {
    Completed,
    Abandoned,
    Replaced,
}

impl GatewayMarkdownMessageOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Abandoned => "abandoned",
            Self::Replaced => "replaced",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayMarkdownMessageMetric {
    pub outcome: GatewayMarkdownMessageOutcome,
    pub delta_count: u64,
    pub final_source_bytes: usize,
    pub cumulative_parse_input_bytes: u64,
    pub cumulative_parse_duration: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopTimelineStage {
    NotificationReduce,
    NotificationApply,
    SemanticModelBuild,
    RenderFingerprint,
    ItemSizes,
    RowCacheLookup,
    RowElementBuild,
    RowLayout,
    MarkdownElementBuild,
}

impl DesktopTimelineStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotificationReduce => "notification.reduce",
            Self::NotificationApply => "notification.apply",
            Self::SemanticModelBuild => "semantic_model.build",
            Self::RenderFingerprint => "render_fingerprint",
            Self::ItemSizes => "item_sizes",
            Self::RowCacheLookup => "row.cache_lookup",
            Self::RowElementBuild => "row.element_build",
            Self::RowLayout => "row.layout",
            Self::MarkdownElementBuild => "markdown.element_build",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopTimelineCacheStatus {
    Hit,
    Miss,
    NotApplicable,
}

impl DesktopTimelineCacheStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::NotApplicable => "not_applicable",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopTimelineContentKind {
    Markdown,
    PlainText,
    Mixed,
    NotApplicable,
}

impl DesktopTimelineContentKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Markdown => "markdown",
            Self::PlainText => "plain_text",
            Self::Mixed => "mixed",
            Self::NotApplicable => "not_applicable",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopTimelineOutcome {
    Ok,
    Skipped,
    Error,
}

impl DesktopTimelineOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Skipped => "skipped",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DesktopTimelineStageMetric {
    pub stage: DesktopTimelineStage,
    pub cache: DesktopTimelineCacheStatus,
    pub content: DesktopTimelineContentKind,
    pub outcome: DesktopTimelineOutcome,
    pub elapsed: Duration,
    pub input_bytes: Option<usize>,
    pub block_count: Option<usize>,
    pub row_count: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopCodeHighlightCacheStatus {
    Hit,
    Miss,
}

impl DesktopCodeHighlightCacheStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopCodeHighlightOutcome {
    Highlighted,
    Fallback,
    Error,
    Stale,
}

impl DesktopCodeHighlightOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Highlighted => "highlighted",
            Self::Fallback => "fallback",
            Self::Error => "error",
            Self::Stale => "stale",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopCodeHighlightFallbackReason {
    None,
    Empty,
    Plaintext,
    UnknownLanguage,
    SourceTooLarge,
    SpanLimit,
    ParserError,
    CacheCapacity,
    UnexpectedState,
}

impl DesktopCodeHighlightFallbackReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Empty => "empty",
            Self::Plaintext => "plaintext",
            Self::UnknownLanguage => "unknown_language",
            Self::SourceTooLarge => "source_too_large",
            Self::SpanLimit => "span_limit",
            Self::ParserError => "parser_error",
            Self::CacheCapacity => "cache_capacity",
            Self::UnexpectedState => "unexpected_state",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopCodeHighlightTheme {
    Light,
    Dark,
}

impl DesktopCodeHighlightTheme {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DesktopCodeHighlightMetric {
    pub cache: DesktopCodeHighlightCacheStatus,
    pub outcome: DesktopCodeHighlightOutcome,
    pub fallback_reason: DesktopCodeHighlightFallbackReason,
    pub theme: DesktopCodeHighlightTheme,
    pub source_bytes: usize,
    pub span_count: usize,
    /// Present only when a highlighting job actually ran. Cache hits and
    /// immediate fallbacks record operation and size distributions without a
    /// synthetic zero-duration sample.
    pub elapsed: Option<Duration>,
}

pub(crate) struct GatewayMarkdownMetrics {
    operations: Counter<u64>,
    stage_duration: Histogram<f64>,
    input_bytes: Histogram<u64>,
    output_bytes: Histogram<u64>,
    ast_blocks: Histogram<u64>,
    messages: Counter<u64>,
    message_delta_count: Histogram<u64>,
    message_final_bytes: Histogram<u64>,
    message_cumulative_parse_bytes: Histogram<u64>,
    message_cumulative_parse_duration: Histogram<f64>,
    message_parse_amplification: Histogram<f64>,
}

impl GatewayMarkdownMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            operations: meter
                .u64_counter("pioneer.gateway.markdown.operations")
                .with_description(
                    "Gateway Markdown pipeline operations by bounded stage and stream",
                )
                .with_unit("{operation}")
                .build(),
            stage_duration: meter
                .f64_histogram("pioneer.gateway.markdown.stage.duration")
                .with_description("Gateway Markdown pipeline stage duration")
                .with_unit("ms")
                .with_boundaries(performance_duration_boundaries())
                .build(),
            input_bytes: meter
                .u64_histogram("pioneer.gateway.markdown.input.bytes")
                .with_description("Input bytes processed by a Gateway Markdown pipeline stage")
                .with_unit("By")
                .with_boundaries(byte_boundaries())
                .build(),
            output_bytes: meter
                .u64_histogram("pioneer.gateway.markdown.output.bytes")
                .with_description(
                    "Serialized output bytes produced by a Gateway Markdown pipeline stage",
                )
                .with_unit("By")
                .with_boundaries(byte_boundaries())
                .build(),
            ast_blocks: meter
                .u64_histogram("pioneer.gateway.markdown.ast.blocks")
                .with_description("Top-level blocks produced by Gateway Markdown parsing")
                .with_unit("{block}")
                .with_boundaries(count_boundaries())
                .build(),
            messages: meter
                .u64_counter("pioneer.gateway.markdown.messages")
                .with_description(
                    "Completed, abandoned, or replaced Gateway Markdown message buffers",
                )
                .with_unit("{message}")
                .build(),
            message_delta_count: meter
                .u64_histogram("pioneer.gateway.markdown.message.delta.count")
                .with_description("Streaming deltas accumulated per Gateway Markdown message")
                .with_unit("{delta}")
                .with_boundaries(count_boundaries())
                .build(),
            message_final_bytes: meter
                .u64_histogram("pioneer.gateway.markdown.message.final.bytes")
                .with_description("Final source bytes in a Gateway Markdown message")
                .with_unit("By")
                .with_boundaries(byte_boundaries())
                .build(),
            message_cumulative_parse_bytes: meter
                .u64_histogram("pioneer.gateway.markdown.message.parse.input.bytes")
                .with_description(
                    "Cumulative source bytes reparsed across all deltas of one message",
                )
                .with_unit("By")
                .with_boundaries(cumulative_byte_boundaries())
                .build(),
            message_cumulative_parse_duration: meter
                .f64_histogram("pioneer.gateway.markdown.message.parse.duration")
                .with_description(
                    "Cumulative Markdown parsing duration across all deltas of one message",
                )
                .with_unit("ms")
                .with_boundaries(performance_duration_boundaries())
                .build(),
            message_parse_amplification: meter
                .f64_histogram("pioneer.gateway.markdown.message.parse.amplification")
                .with_description("Ratio of cumulative reparsed bytes to final message bytes")
                .with_unit("1")
                .with_boundaries(vec![
                    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1_024.0,
                ])
                .build(),
        }
    }
}

pub(crate) struct DesktopTimelineMetrics {
    operations: Counter<u64>,
    stage_duration: Histogram<f64>,
    input_bytes: Histogram<u64>,
    ast_blocks: Histogram<u64>,
    rows: Histogram<u64>,
    code_highlight_operations: Counter<u64>,
    code_highlight_duration: Histogram<f64>,
    code_highlight_source_bytes: Histogram<u64>,
    code_highlight_spans: Histogram<u64>,
}

impl DesktopTimelineMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            operations: meter
                .u64_counter("pioneer.desktop.timeline.operations")
                .with_description("Desktop timeline operations by bounded stage and cache result")
                .with_unit("{operation}")
                .build(),
            stage_duration: meter
                .f64_histogram("pioneer.desktop.timeline.stage.duration")
                .with_description(
                    "Desktop timeline projection, rendering, and layout stage duration",
                )
                .with_unit("ms")
                .with_boundaries(performance_duration_boundaries())
                .build(),
            input_bytes: meter
                .u64_histogram("pioneer.desktop.timeline.input.bytes")
                .with_description("Text bytes handled by a Desktop timeline stage")
                .with_unit("By")
                .with_boundaries(byte_boundaries())
                .build(),
            ast_blocks: meter
                .u64_histogram("pioneer.desktop.timeline.ast.blocks")
                .with_description("Top-level Markdown blocks handled by a Desktop timeline stage")
                .with_unit("{block}")
                .with_boundaries(count_boundaries())
                .build(),
            rows: meter
                .u64_histogram("pioneer.desktop.timeline.rows")
                .with_description("Timeline rows handled by a Desktop timeline stage")
                .with_unit("{row}")
                .with_boundaries(count_boundaries())
                .build(),
            code_highlight_operations: meter
                .u64_counter("pioneer.desktop.timeline.code_highlight.operations")
                .with_description("Desktop timeline code highlighting outcomes and cache results")
                .with_unit("{operation}")
                .build(),
            code_highlight_duration: meter
                .f64_histogram("pioneer.desktop.timeline.code_highlight.duration")
                .with_description("End-to-end latency of a background code highlighting job")
                .with_unit("ms")
                .with_boundaries(performance_duration_boundaries())
                .build(),
            code_highlight_source_bytes: meter
                .u64_histogram("pioneer.desktop.timeline.code_highlight.source.bytes")
                .with_description("Source bytes requested from Desktop timeline code highlighting")
                .with_unit("By")
                .with_boundaries(byte_boundaries())
                .build(),
            code_highlight_spans: meter
                .u64_histogram("pioneer.desktop.timeline.code_highlight.spans")
                .with_description("Highlight spans returned to the Desktop timeline")
                .with_unit("{span}")
                .with_boundaries(count_boundaries())
                .build(),
        }
    }
}

pub fn record_gateway_markdown_stage(metric: GatewayMarkdownStageMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_markdown_metrics.as_ref() else {
        return;
    };
    let attributes = [
        KeyValue::new("markdown.stage", metric.stage.as_str()),
        KeyValue::new("markdown.stream", metric.stream.as_str()),
        KeyValue::new("outcome", metric.outcome.as_str()),
    ];
    metrics.operations.add(1, &attributes);
    metrics
        .stage_duration
        .record(duration_ms(metric.elapsed), &attributes);
    if let Some(input_bytes) = metric.input_bytes {
        metrics
            .input_bytes
            .record(usize_u64(input_bytes), &attributes);
    }
    if let Some(output_bytes) = metric.output_bytes {
        metrics
            .output_bytes
            .record(usize_u64(output_bytes), &attributes);
    }
    if let Some(block_count) = metric.block_count {
        metrics
            .ast_blocks
            .record(usize_u64(block_count), &attributes);
    }
}

pub fn record_gateway_markdown_message(metric: GatewayMarkdownMessageMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_markdown_metrics.as_ref() else {
        return;
    };
    let attributes = [KeyValue::new("outcome", metric.outcome.as_str())];
    metrics.messages.add(1, &attributes);
    metrics
        .message_delta_count
        .record(metric.delta_count, &attributes);
    metrics
        .message_final_bytes
        .record(usize_u64(metric.final_source_bytes), &attributes);
    metrics
        .message_cumulative_parse_bytes
        .record(metric.cumulative_parse_input_bytes, &attributes);
    metrics
        .message_cumulative_parse_duration
        .record(duration_ms(metric.cumulative_parse_duration), &attributes);
    metrics.message_parse_amplification.record(
        parse_amplification(
            metric.cumulative_parse_input_bytes,
            usize_u64(metric.final_source_bytes),
        ),
        &attributes,
    );
}

pub fn record_desktop_timeline_stage(metric: DesktopTimelineStageMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.desktop_timeline_metrics.as_ref() else {
        return;
    };
    let attributes = [
        KeyValue::new("timeline.stage", metric.stage.as_str()),
        KeyValue::new("cache.result", metric.cache.as_str()),
        KeyValue::new("content.kind", metric.content.as_str()),
        KeyValue::new("outcome", metric.outcome.as_str()),
    ];
    metrics.operations.add(1, &attributes);
    metrics
        .stage_duration
        .record(duration_ms(metric.elapsed), &attributes);
    if let Some(input_bytes) = metric.input_bytes {
        metrics
            .input_bytes
            .record(usize_u64(input_bytes), &attributes);
    }
    if let Some(block_count) = metric.block_count {
        metrics
            .ast_blocks
            .record(usize_u64(block_count), &attributes);
    }
    if let Some(row_count) = metric.row_count {
        metrics.rows.record(usize_u64(row_count), &attributes);
    }
}

pub fn record_desktop_code_highlight(metric: DesktopCodeHighlightMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.desktop_timeline_metrics.as_ref() else {
        return;
    };
    let attributes = [
        KeyValue::new("cache.result", metric.cache.as_str()),
        KeyValue::new("outcome", metric.outcome.as_str()),
        KeyValue::new("fallback.reason", metric.fallback_reason.as_str()),
        KeyValue::new("ui.theme", metric.theme.as_str()),
    ];
    metrics.code_highlight_operations.add(1, &attributes);
    if let Some(elapsed) = metric.elapsed {
        metrics
            .code_highlight_duration
            .record(duration_ms(elapsed), &attributes);
    }
    metrics
        .code_highlight_source_bytes
        .record(usize_u64(metric.source_bytes), &attributes);
    metrics
        .code_highlight_spans
        .record(usize_u64(metric.span_count), &attributes);
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn usize_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn parse_amplification(cumulative_parse_bytes: u64, final_source_bytes: u64) -> f64 {
    if final_source_bytes == 0 {
        0.0
    } else {
        cumulative_parse_bytes as f64 / final_source_bytes as f64
    }
}

fn performance_duration_boundaries() -> Vec<f64> {
    vec![
        0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
        1_000.0, 2_500.0, 5_000.0, 10_000.0, 30_000.0,
    ]
}

fn byte_boundaries() -> Vec<f64> {
    vec![
        64.0,
        256.0,
        1_024.0,
        4_096.0,
        16_384.0,
        65_536.0,
        262_144.0,
        1_048_576.0,
        4_194_304.0,
        16_777_216.0,
    ]
}

fn cumulative_byte_boundaries() -> Vec<f64> {
    vec![
        1_024.0,
        4_096.0,
        16_384.0,
        65_536.0,
        262_144.0,
        1_048_576.0,
        4_194_304.0,
        16_777_216.0,
        67_108_864.0,
        268_435_456.0,
        1_073_741_824.0,
    ]
}

fn count_boundaries() -> Vec<f64> {
    vec![
        1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1_024.0, 4_096.0, 16_384.0,
    ]
}

pub(crate) fn target_supports_gateway_markdown(target: TelemetryTarget) -> bool {
    target == TelemetryTarget::Gateway
}

pub(crate) fn target_supports_desktop_timeline(target: TelemetryTarget) -> bool {
    target == TelemetryTarget::Desktop
}

#[cfg(test)]
mod tests {
    use super::{
        DesktopTimelineStage, GatewayMarkdownStage, GatewayMarkdownStreamKind, parse_amplification,
    };

    #[test]
    fn performance_attribute_values_are_bounded_static_names() {
        assert_eq!(GatewayMarkdownStage::Parse.as_str(), "parse");
        assert_eq!(
            GatewayMarkdownStreamKind::CommandOutput.as_str(),
            "command_output"
        );
        assert_eq!(
            DesktopTimelineStage::MarkdownElementBuild.as_str(),
            "markdown.element_build"
        );
    }

    #[test]
    fn parse_amplification_handles_empty_and_reparsed_messages() {
        assert_eq!(parse_amplification(0, 0), 0.0);
        assert_eq!(parse_amplification(4_096, 1_024), 4.0);
    }
}
