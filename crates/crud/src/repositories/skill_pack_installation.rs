use crate::util::unix_to_datetime;
use anyhow::{Context, Result};
use pioneer_entity::skill_pack_installation;
use pioneer_protocol::SkillPackId;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

#[derive(Clone, Debug)]
pub(crate) struct PreparedSkillPackInstallation {
    row: skill_pack_installation::ActiveModel,
}

pub(crate) fn prepare_skill_pack_installation(
    record: &crate::SkillPackInstallationRecord,
) -> PreparedSkillPackInstallation {
    PreparedSkillPackInstallation {
        row: skill_pack_installation::ActiveModel {
            id: Set(record.pack_id.to_string()),
            name: Set(record.name.clone()),
            scope_key: Set(record.scope_key.clone()),
            source_kind: Set(record.source_kind.clone()),
            created_at: Set(unix_to_datetime(record.created_at_unix)),
            updated_at: Set(unix_to_datetime(record.updated_at_unix)),
        },
    }
}

pub(crate) async fn insert_prepared_skill_pack_installation<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedSkillPackInstallation,
) -> Result<()> {
    skill_pack_installation::Entity::insert(prepared.row)
        .exec(db)
        .await
        .context("failed to insert prepared skill pack installation")?;
    Ok(())
}

pub async fn insert_skill_pack_installation<C: ConnectionTrait>(
    db: &C,
    record: &crate::SkillPackInstallationRecord,
) -> Result<()> {
    insert_prepared_skill_pack_installation(db, prepare_skill_pack_installation(record))
        .await
        .with_context(|| {
            format!(
                "failed to insert skill pack installation `{}` ({})",
                record.pack_id, record.scope_key
            )
        })
}

pub async fn find_skill_pack_installation<C: ConnectionTrait>(
    db: &C,
    scope_key: &str,
    pack_id: &SkillPackId,
) -> Result<Option<skill_pack_installation::Model>> {
    skill_pack_installation::Entity::find_by_id(pack_id.to_string())
        .filter(skill_pack_installation::Column::ScopeKey.eq(scope_key.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to query skill pack installation `{pack_id}` in scope `{scope_key}`")
        })
}

pub async fn find_skill_pack_installation_by_id<C: ConnectionTrait>(
    db: &C,
    pack_id: &SkillPackId,
) -> Result<Option<skill_pack_installation::Model>> {
    skill_pack_installation::Entity::find_by_id(pack_id.to_string())
        .one(db)
        .await
        .with_context(|| format!("failed to query skill pack installation `{pack_id}`"))
}

pub async fn list_skill_pack_installations<C: ConnectionTrait>(
    db: &C,
    scope_key: &str,
) -> Result<Vec<skill_pack_installation::Model>> {
    skill_pack_installation::Entity::find()
        .filter(skill_pack_installation::Column::ScopeKey.eq(scope_key.to_owned()))
        .order_by_asc(skill_pack_installation::Column::Name)
        .order_by_asc(skill_pack_installation::Column::Id)
        .all(db)
        .await
        .with_context(|| format!("failed to query skill packs in scope `{scope_key}`"))
}

pub async fn update_skill_pack_installation_name<C: ConnectionTrait>(
    db: &C,
    scope_key: &str,
    pack_id: &SkillPackId,
    name: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = skill_pack_installation::Entity::update_many()
        .filter(skill_pack_installation::Column::Id.eq(pack_id.to_string()))
        .filter(skill_pack_installation::Column::ScopeKey.eq(scope_key.to_owned()))
        .col_expr(
            skill_pack_installation::Column::Name,
            sea_orm::sea_query::Expr::value(name.to_owned()),
        )
        .col_expr(
            skill_pack_installation::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(updated_at),
        )
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to update skill pack installation `{pack_id}` in scope `{scope_key}`")
        })?;
    Ok(result.rows_affected == 1)
}

pub async fn delete_skill_pack_installation<C: ConnectionTrait>(
    db: &C,
    scope_key: &str,
    pack_id: &SkillPackId,
) -> Result<bool> {
    let result = skill_pack_installation::Entity::delete_many()
        .filter(skill_pack_installation::Column::Id.eq(pack_id.to_string()))
        .filter(skill_pack_installation::Column::ScopeKey.eq(scope_key.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to delete skill pack installation `{pack_id}` in scope `{scope_key}`")
        })?;
    Ok(result.rows_affected == 1)
}
