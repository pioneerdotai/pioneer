use anyhow::{Context, Result, bail};
use pioneer_entity::skill_installation;
use pioneer_protocol::{SkillId, SkillPackId};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

pub async fn insert_skill_installation<C: ConnectionTrait>(
    db: &C,
    record: &crate::SkillInstallationRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    skill_installation::Entity::insert(skill_installation::ActiveModel {
        id: Set(record.skill_id.to_string()),
        owner: Set(record.owner.clone()),
        slug: Set(record.slug.clone()),
        version: Set(record.version.clone()),
        source_kind: Set(record.source_kind.clone()),
        scope_key: Set(record.scope_key.clone()),
        source_ref: Set(record.source_ref.clone()),
        install_path: Set(record.install_path.clone()),
        trust_level: Set(record.trust_level.clone()),
        fingerprint: Set(record.fingerprint.clone()),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
        pack_id: Set(record.pack_id.as_ref().map(ToString::to_string)),
        pack_member_key: Set(record.pack_member_key.clone()),
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert skill installation `{}` ({}/{})",
            record.skill_id, record.source_kind, record.scope_key
        )
    })?;

    Ok(())
}

pub async fn update_skill_installation<C: ConnectionTrait>(
    db: &C,
    skill_id: &SkillId,
    patch: &crate::SkillInstallationPatch,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if patch.pack_id.is_some() || patch.pack_member_key.is_some() {
        bail!("generic skill installation updates cannot change pack membership for `{skill_id}`");
    }
    let mut model = skill_installation::ActiveModel {
        updated_at: Set(updated_at),
        ..Default::default()
    };
    if let Some(owner) = &patch.owner {
        model.owner = Set(owner.clone());
    }
    if let Some(slug) = &patch.slug {
        model.slug = Set(slug.clone());
    }
    if let Some(version) = &patch.version {
        model.version = Set(version.clone());
    }
    if let Some(source_kind) = &patch.source_kind {
        model.source_kind = Set(source_kind.clone());
    }
    if let Some(scope_key) = &patch.scope_key {
        model.scope_key = Set(scope_key.clone());
    }
    if let Some(source_ref) = &patch.source_ref {
        model.source_ref = Set(source_ref.clone());
    }
    if let Some(install_path) = &patch.install_path {
        model.install_path = Set(install_path.clone());
    }
    if let Some(trust_level) = &patch.trust_level {
        model.trust_level = Set(trust_level.clone());
    }
    if let Some(fingerprint) = &patch.fingerprint {
        model.fingerprint = Set(fingerprint.clone());
    }

    let result = skill_installation::Entity::update_many()
        .set(model)
        .filter(skill_installation::Column::Id.eq(skill_id.to_string()))
        .exec(db)
        .await
        .with_context(|| format!("failed to update skill installation `{skill_id}`"))?;
    Ok(result.rows_affected == 1)
}

pub async fn update_pack_skill_installation<C: ConnectionTrait>(
    db: &C,
    record: &crate::SkillInstallationRecord,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let pack_id = record
        .pack_id
        .as_ref()
        .context("pack child update requires pack_id")?;
    let member_key = record
        .pack_member_key
        .as_ref()
        .context("pack child update requires pack_member_key")?;
    let result = skill_installation::Entity::update_many()
        .filter(skill_installation::Column::Id.eq(record.skill_id.to_string()))
        .filter(skill_installation::Column::ScopeKey.eq(record.scope_key.clone()))
        .filter(skill_installation::Column::PackId.eq(pack_id.to_string()))
        .filter(skill_installation::Column::PackMemberKey.eq(member_key.clone()))
        .col_expr(
            skill_installation::Column::Owner,
            sea_orm::sea_query::Expr::value(record.owner.clone()),
        )
        .col_expr(
            skill_installation::Column::Slug,
            sea_orm::sea_query::Expr::value(record.slug.clone()),
        )
        .col_expr(
            skill_installation::Column::Version,
            sea_orm::sea_query::Expr::value(record.version.clone()),
        )
        .col_expr(
            skill_installation::Column::SourceKind,
            sea_orm::sea_query::Expr::value(record.source_kind.clone()),
        )
        .col_expr(
            skill_installation::Column::SourceRef,
            sea_orm::sea_query::Expr::value(record.source_ref.clone()),
        )
        .col_expr(
            skill_installation::Column::InstallPath,
            sea_orm::sea_query::Expr::value(record.install_path.clone()),
        )
        .col_expr(
            skill_installation::Column::TrustLevel,
            sea_orm::sea_query::Expr::value(record.trust_level.clone()),
        )
        .col_expr(
            skill_installation::Column::Fingerprint,
            sea_orm::sea_query::Expr::value(record.fingerprint.clone()),
        )
        .col_expr(
            skill_installation::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(updated_at),
        )
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to update pack skill installation `{}`",
                record.skill_id
            )
        })?;
    Ok(result.rows_affected == 1)
}

pub async fn list_skill_installations<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<skill_installation::Model>> {
    skill_installation::Entity::find()
        .order_by_asc(skill_installation::Column::ScopeKey)
        .order_by_asc(skill_installation::Column::SourceKind)
        .order_by_asc(skill_installation::Column::Owner)
        .order_by_asc(skill_installation::Column::Slug)
        .order_by_asc(skill_installation::Column::Id)
        .all(db)
        .await
        .context("failed to query skill installations")
}

pub async fn list_skill_installations_for_pack<C: ConnectionTrait>(
    db: &C,
    scope_key: &str,
    pack_id: &SkillPackId,
) -> Result<Vec<skill_installation::Model>> {
    skill_installation::Entity::find()
        .filter(skill_installation::Column::ScopeKey.eq(scope_key.to_owned()))
        .filter(skill_installation::Column::PackId.eq(pack_id.to_string()))
        .order_by_asc(skill_installation::Column::PackMemberKey)
        .order_by_asc(skill_installation::Column::Id)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to query children for pack `{pack_id}` in scope `{scope_key}`")
        })
}

pub async fn list_skill_installations_by_pack_id<C: ConnectionTrait>(
    db: &C,
    pack_id: &SkillPackId,
) -> Result<Vec<skill_installation::Model>> {
    skill_installation::Entity::find()
        .filter(skill_installation::Column::PackId.eq(pack_id.to_string()))
        .order_by_asc(skill_installation::Column::PackMemberKey)
        .order_by_asc(skill_installation::Column::Id)
        .all(db)
        .await
        .with_context(|| format!("failed to query children for pack `{pack_id}`"))
}

pub async fn find_skill_installation<C: ConnectionTrait>(
    db: &C,
    skill_id: &SkillId,
) -> Result<Option<skill_installation::Model>> {
    skill_installation::Entity::find_by_id(skill_id.to_string())
        .one(db)
        .await
        .with_context(|| format!("failed to query skill installation `{skill_id}`"))
}

pub async fn delete_skill_installation<C: ConnectionTrait>(
    db: &C,
    skill_id: &SkillId,
) -> Result<bool> {
    let result = skill_installation::Entity::delete_many()
        .filter(skill_installation::Column::Id.eq(skill_id.to_string()))
        .exec(db)
        .await
        .with_context(|| format!("failed to delete skill installation `{skill_id}`"))?;
    Ok(result.rows_affected == 1)
}
