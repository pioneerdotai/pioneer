use anyhow::{Context, Result};
use pioneer_entity::turn_skill_binding;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

#[derive(Clone, Debug)]
pub(crate) struct PreparedTurnSkillBindings {
    turn_id: String,
    models: Vec<turn_skill_binding::ActiveModel>,
}

pub(crate) fn prepare_turn_skill_bindings(
    turn_id: &str,
    bindings: &[crate::TurnSkillBindingRecord],
    created_at: DateTimeWithTimeZone,
) -> PreparedTurnSkillBindings {
    PreparedTurnSkillBindings {
        turn_id: turn_id.to_owned(),
        models: bindings
            .iter()
            .map(|binding| turn_skill_binding::ActiveModel {
                id: Set(pioneer_protocol::generate_id(21)),
                turn_id: Set(turn_id.to_owned()),
                skill_id: Set(binding.skill_id.to_string()),
                skill_owner: Set(binding.skill_owner.clone()),
                skill_slug: Set(binding.skill_slug.clone()),
                skill_version: Set(binding.skill_version.clone()),
                fingerprint: Set(binding.fingerprint.clone()),
                source_kind: Set(binding.source_kind.clone()),
                resolved_reason: Set(binding.resolved_reason.clone()),
                created_at: Set(created_at),
            })
            .collect(),
    }
}

pub(crate) async fn replace_prepared_turn_skill_bindings<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnSkillBindings,
) -> Result<()> {
    let PreparedTurnSkillBindings { turn_id, models } = prepared;
    turn_skill_binding::Entity::delete_many()
        .filter(turn_skill_binding::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to clear turn skill bindings")?;

    if !models.is_empty() {
        turn_skill_binding::Entity::insert_many(models)
            .exec(db)
            .await
            .with_context(|| {
                format!("failed to insert prepared turn skill bindings for turn `{turn_id}`")
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
        .order_by_asc(turn_skill_binding::Column::SkillId)
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
