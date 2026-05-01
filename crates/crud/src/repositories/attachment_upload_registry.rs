use anyhow::{Context, Result};
use pioneer_entity::attachment_upload_registry;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};

pub async fn prune_expired_entries<C: ConnectionTrait>(db: &C, now_unix_ms: i64) -> Result<u64> {
    let result = attachment_upload_registry::Entity::delete_many()
        .filter(attachment_upload_registry::Column::ExpiresAtUnixMs.lte(now_unix_ms))
        .exec(db)
        .await
        .context("failed to prune expired attachment upload registry entries")?;
    Ok(result.rows_affected)
}

pub async fn find_active_file_id_by_key<C: ConnectionTrait>(
    db: &C,
    registry_key: &str,
    now_unix_ms: i64,
) -> Result<Option<String>> {
    let row = attachment_upload_registry::Entity::find()
        .filter(attachment_upload_registry::Column::RegistryKey.eq(registry_key.to_owned()))
        .filter(attachment_upload_registry::Column::ExpiresAtUnixMs.gt(now_unix_ms))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to query attachment upload registry key `{registry_key}`")
        })?;

    Ok(row.and_then(|model| {
        let trimmed = model.provider_file_id.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    }))
}

pub async fn upsert_entry<C: ConnectionTrait>(
    db: &C,
    record: &crate::AttachmentUploadRegistryRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    attachment_upload_registry::Entity::insert(attachment_upload_registry::ActiveModel {
        registry_key: Set(record.registry_key.clone()),
        provider: Set(record.provider.clone()),
        model_family: Set(record.model_family.clone()),
        transport_kind: Set(record.transport_kind.clone()),
        sha256: Set(record.sha256.clone()),
        provider_file_id: Set(record.provider_file_id.clone()),
        uploaded_at_unix_ms: Set(record.uploaded_at_unix_ms),
        ttl_secs: Set(i64::try_from(record.ttl_secs).unwrap_or(i64::MAX)),
        expires_at_unix_ms: Set(record.expires_at_unix_ms),
        mime_type: Set(record.mime_type.clone()),
        size_bytes: Set(i64::try_from(record.size_bytes).unwrap_or(i64::MAX)),
        file_name: Set(record.file_name.clone()),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::column(attachment_upload_registry::Column::RegistryKey)
            .update_columns([
                attachment_upload_registry::Column::Provider,
                attachment_upload_registry::Column::ModelFamily,
                attachment_upload_registry::Column::TransportKind,
                attachment_upload_registry::Column::Sha256,
                attachment_upload_registry::Column::ProviderFileId,
                attachment_upload_registry::Column::UploadedAtUnixMs,
                attachment_upload_registry::Column::TtlSecs,
                attachment_upload_registry::Column::ExpiresAtUnixMs,
                attachment_upload_registry::Column::MimeType,
                attachment_upload_registry::Column::SizeBytes,
                attachment_upload_registry::Column::FileName,
                attachment_upload_registry::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to upsert attachment upload registry key `{}`",
            record.registry_key
        )
    })?;

    Ok(())
}
