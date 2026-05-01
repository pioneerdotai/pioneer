use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::types::{
    AttachmentBudgetReport, AttachmentPipelineConfig, PreparedAttachment,
};
use anyhow::Result;

pub fn validate_budget(
    config: &AttachmentPipelineConfig,
    attachments: &[PreparedAttachment],
) -> Result<AttachmentBudgetReport> {
    if attachments.len() > config.max_attachments_per_request {
        return Err(AttachmentPipelineError::attachment_count_exceeded(
            attachments.len(),
            config.max_attachments_per_request,
        )
        .into());
    }

    let mut total_bytes = 0usize;
    for attachment in attachments {
        if attachment.size_bytes > config.max_bytes_per_attachment {
            return Err(AttachmentPipelineError::attachment_too_large(
                attachment.size_bytes,
                config.max_bytes_per_attachment,
                attachment.name.as_str(),
            )
            .into());
        }

        total_bytes = total_bytes.saturating_add(attachment.size_bytes);
        if total_bytes > config.max_total_bytes_per_request {
            return Err(AttachmentPipelineError::attachment_total_budget_exceeded(
                total_bytes,
                config.max_total_bytes_per_request,
            )
            .into());
        }
    }

    Ok(AttachmentBudgetReport {
        attachment_count: attachments.len(),
        total_bytes,
        max_attachments: config.max_attachments_per_request,
        max_total_bytes: config.max_total_bytes_per_request,
        max_bytes_per_attachment: config.max_bytes_per_attachment,
    })
}
