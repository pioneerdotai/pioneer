use anyhow::{Context, Result};
use pioneer_entity::skill_installation;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

pub async fn upsert_skill_installation<C: ConnectionTrait>(
    db: &C,
    record: &crate::SkillInstallationRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    skill_installation::Entity::insert(skill_installation::ActiveModel {
        id: Set(pioneer_protocol::generate_id(21)),
        slug: Set(record.slug.clone()),
        version: Set(record.version.clone()),
        source_kind: Set(record.source_kind.clone()),
        source_ref: Set(record.source_ref.clone()),
        install_path: Set(record.install_path.clone()),
        trust_level: Set(record.trust_level.clone()),
        fingerprint: Set(record.fingerprint.clone()),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::columns([
            skill_installation::Column::Slug,
            skill_installation::Column::SourceKind,
        ])
        .update_columns([
            skill_installation::Column::Version,
            skill_installation::Column::SourceRef,
            skill_installation::Column::InstallPath,
            skill_installation::Column::TrustLevel,
            skill_installation::Column::Fingerprint,
            skill_installation::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to upsert skill installation `{}` ({})",
            record.slug, record.source_kind
        )
    })?;

    Ok(())
}

pub async fn list_skill_installations<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<skill_installation::Model>> {
    skill_installation::Entity::find()
        .order_by_asc(skill_installation::Column::Slug)
        .order_by_asc(skill_installation::Column::SourceKind)
        .all(db)
        .await
        .context("failed to query skill installations")
}

pub async fn find_skill_installation<C: ConnectionTrait>(
    db: &C,
    slug: &str,
    source_kind: &str,
) -> Result<Option<skill_installation::Model>> {
    skill_installation::Entity::find()
        .filter(skill_installation::Column::Slug.eq(slug.to_owned()))
        .filter(skill_installation::Column::SourceKind.eq(source_kind.to_owned()))
        .one(db)
        .await
        .with_context(|| format!("failed to query skill installation `{slug}` ({source_kind})"))
}

pub async fn delete_skill_installation<C: ConnectionTrait>(
    db: &C,
    slug: &str,
    source_kind: &str,
) -> Result<()> {
    skill_installation::Entity::delete_many()
        .filter(skill_installation::Column::Slug.eq(slug.to_owned()))
        .filter(skill_installation::Column::SourceKind.eq(source_kind.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to delete skill installation `{slug}` ({source_kind})"))?;
    Ok(())
}
