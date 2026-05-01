use anyhow::{Context, Result};
use pioneer_entity::turn_skill_binding;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

pub async fn replace_turn_skill_bindings<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    bindings: &[crate::TurnSkillBindingRecord],
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    turn_skill_binding::Entity::delete_many()
        .filter(turn_skill_binding::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to clear turn skill bindings")?;

    for binding in bindings {
        turn_skill_binding::Entity::insert(turn_skill_binding::ActiveModel {
            id: Set(pioneer_protocol::generate_id(21)),
            turn_id: Set(turn_id.to_owned()),
            skill_slug: Set(binding.skill_slug.clone()),
            skill_version: Set(binding.skill_version.clone()),
            fingerprint: Set(binding.fingerprint.clone()),
            source_kind: Set(binding.source_kind.clone()),
            resolved_reason: Set(binding.resolved_reason.clone()),
            created_at: Set(created_at),
        })
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to insert turn skill binding `{}` for turn `{turn_id}`",
                binding.skill_slug
            )
        })?;
    }

    Ok(())
}

pub async fn list_turn_skill_bindings<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<turn_skill_binding::Model>> {
    turn_skill_binding::Entity::find()
        .filter(turn_skill_binding::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(turn_skill_binding::Column::SkillSlug)
        .all(db)
        .await
        .context("failed to query turn skill bindings")
}

pub async fn find_turn_skill_bindings<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<turn_skill_binding::Model>> {
    list_turn_skill_bindings(db, turn_id).await
}
