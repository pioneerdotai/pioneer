use crate::attachments::types::{AttachmentBudgetReport, PreparedAttachment};
use std::time::Duration;
use tracing::{debug, warn};

pub fn emit_preflight_start(provider: &str, message_count: usize, attachment_count_hint: usize) {
    debug!(
        target: "pioneer_provider::attachments",
        event = "attachment.preflight.start",
        provider,
        message_count,
        attachment_count_hint,
        "attachment preflight start"
    );
}

pub fn emit_preflight_ok(provider: &str, budget: AttachmentBudgetReport) {
    debug!(
        target: "pioneer_provider::attachments",
        event = "attachment.preflight.ok",
        provider,
        attachment_count = budget.attachment_count,
        total_bytes = budget.total_bytes,
        max_attachments = budget.max_attachments,
        max_total_bytes = budget.max_total_bytes,
        max_bytes_per_attachment = budget.max_bytes_per_attachment,
        "attachment preflight completed"
    );
}

pub fn emit_preflight_fail(provider: &str, error_code: &str, error_message: &str) {
    warn!(
        target: "pioneer_provider::attachments",
        event = "attachment.preflight.fail",
        provider,
        error_code,
        error_message,
        "attachment preflight failed"
    );
}

pub fn emit_security_blocked(provider: &str, source: &str, reason: &str, dry_run: bool) {
    let level = if dry_run { "dry_run" } else { "enforced" };
    warn!(
        target: "pioneer_provider::attachments",
        event = "attachment.security.blocked",
        provider,
        source,
        level,
        reason,
        "attachment security policy blocked source"
    );
}

pub fn emit_transport_selected(
    provider: &str,
    attachment: &PreparedAttachment,
    transport: &str,
    reason: &str,
) {
    debug!(
        target: "pioneer_provider::attachments",
        event = "attachment.transport.selected",
        provider,
        attachment_name = attachment.name,
        attachment_kind = ?attachment.kind,
        attachment_mime = attachment.mime_type,
        attachment_size_bytes = attachment.size_bytes,
        transport,
        selection_reason = reason,
        "attachment transport selected"
    );
}

pub fn emit_upload_retry(provider: &str, operation: &str, attempt: usize, delay: Duration) {
    debug!(
        target: "pioneer_provider::attachments",
        event = "attachment.upload.retry",
        provider,
        operation,
        retry_attempt = attempt,
        retry_delay_ms = delay.as_millis() as u64,
        "attachment operation scheduled for retry"
    );
}

pub fn emit_upload_fail(
    provider: &str,
    operation: &str,
    attempt: usize,
    error_code: &str,
    error_message: &str,
) {
    warn!(
        target: "pioneer_provider::attachments",
        event = "attachment.upload.fail",
        provider,
        operation,
        attempt,
        error_code,
        error_message,
        "attachment operation failed"
    );
}

pub fn emit_request_materialized(provider: &str, attachment_count: usize, total_bytes: usize) {
    debug!(
        target: "pioneer_provider::attachments",
        event = "attachment.request.materialized",
        provider,
        attachment_count,
        total_bytes,
        "provider request materialized with attachment payload"
    );
}

pub fn emit_upload_registry_hit(provider: &str, key: &str) {
    debug!(
        target: "pioneer_provider::attachments",
        event = "attachment.upload_registry.hit",
        provider,
        key,
        "attachment upload registry hit"
    );
}

pub fn emit_upload_registry_miss(provider: &str, key: &str) {
    debug!(
        target: "pioneer_provider::attachments",
        event = "attachment.upload_registry.miss",
        provider,
        key,
        "attachment upload registry miss"
    );
}

pub fn emit_upload_registry_write(provider: &str, key: &str) {
    debug!(
        target: "pioneer_provider::attachments",
        event = "attachment.upload_registry.write",
        provider,
        key,
        "attachment upload registry write"
    );
}
