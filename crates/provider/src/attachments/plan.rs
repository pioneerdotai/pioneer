use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::observability;
use crate::attachments::types::{
    AttachmentPipelineConfig, AttachmentTransportKind, AttachmentTransportPlan, PreparedAttachment,
};
use crate::types::ProviderCapabilities;
use anyhow::Result;

pub fn assign_transport_plans(
    provider_name: &str,
    capabilities: &ProviderCapabilities,
    config: &AttachmentPipelineConfig,
    attachments: &mut [PreparedAttachment],
) -> Result<()> {
    for attachment in attachments {
        let support = capabilities.input_types.support_for(attachment.kind);

        let prefer_upload =
            support.file_upload && attachment.size_bytes >= config.upload_preferred_min_bytes;

        let plan = if prefer_upload {
            AttachmentTransportPlan {
                kind: AttachmentTransportKind::Upload,
                reason: "provider_declares_native_upload_support_and_attachment_exceeds_inline_threshold".to_owned(),
            }
        } else if support.native {
            AttachmentTransportPlan {
                kind: AttachmentTransportKind::Inline,
                reason: "provider_declares_native_inline_support".to_owned(),
            }
        } else if support.file_upload {
            AttachmentTransportPlan {
                kind: AttachmentTransportKind::Upload,
                reason: "provider_declares_native_upload_support".to_owned(),
            }
        } else if support.data_url_inline {
            if attachment.bytes.is_none() {
                return Err(AttachmentPipelineError::unsupported_attachment_source(
                    "reference_without_materialized_bytes_for_data_url_inline",
                )
                .into());
            }
            AttachmentTransportPlan {
                kind: AttachmentTransportKind::DataUrlPart,
                reason: "provider_declares_data_url_inline_support".to_owned(),
            }
        } else {
            AttachmentTransportPlan {
                kind: AttachmentTransportKind::Unsupported,
                reason: format!(
                    "provider `{provider_name}` does not support {:?} attachments",
                    attachment.kind
                ),
            }
        };

        if matches!(plan.kind, AttachmentTransportKind::Unsupported) {
            return Err(AttachmentPipelineError::contract_violation(plan.reason.clone()).into());
        }

        attachment.transport_plan = plan;

        observability::emit_transport_selected(
            provider_name,
            attachment,
            match attachment.transport_plan.kind {
                AttachmentTransportKind::Inline => "inline",
                AttachmentTransportKind::Upload => "upload",
                AttachmentTransportKind::DataUrlPart => "data_url_part",
                AttachmentTransportKind::Unsupported => "unsupported",
            },
            attachment.transport_plan.reason.as_str(),
        );
    }

    Ok(())
}
