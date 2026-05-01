use anyhow::{Context, Result};
use pioneer_entity::skill_workspace_policy;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

pub async fn list_workspace_skill_policies<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<skill_workspace_policy::Model>> {
    skill_workspace_policy::Entity::find()
        .filter(skill_workspace_policy::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_asc(skill_workspace_policy::Column::SkillSlug)
        .order_by_asc(skill_workspace_policy::Column::SourceKind)
        .all(db)
        .await
        .context("failed to query workspace skill policies")
}

pub async fn upsert_workspace_skill_policy<C: ConnectionTrait>(
    db: &C,
    record: &crate::WorkspaceSkillPolicyRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    skill_workspace_policy::Entity::insert(skill_workspace_policy::ActiveModel {
        id: Set(pioneer_protocol::generate_id(21)),
        workspace_id: Set(record.workspace_id.clone()),
        skill_slug: Set(record.skill_slug.clone()),
        source_kind: Set(record.source_kind.clone()),
        enabled: Set(record.enabled),
        allow_implicit_invocation: Set(record.allow_implicit_invocation),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::columns([
            skill_workspace_policy::Column::WorkspaceId,
            skill_workspace_policy::Column::SkillSlug,
            skill_workspace_policy::Column::SourceKind,
        ])
        .update_columns([
            skill_workspace_policy::Column::Enabled,
            skill_workspace_policy::Column::AllowImplicitInvocation,
            skill_workspace_policy::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to upsert workspace skill policy `{}` for workspace `{}` ({})",
            record.skill_slug, record.workspace_id, record.source_kind
        )
    })?;

    Ok(())
}

pub async fn delete_workspace_skill_policy<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    skill_slug: &str,
    source_kind: &str,
) -> Result<()> {
    skill_workspace_policy::Entity::delete_many()
        .filter(skill_workspace_policy::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(skill_workspace_policy::Column::SkillSlug.eq(skill_slug.to_owned()))
        .filter(skill_workspace_policy::Column::SourceKind.eq(source_kind.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to delete workspace skill policy `{skill_slug}` for workspace `{workspace_id}` ({source_kind})"
            )
        })?;

    Ok(())
}
