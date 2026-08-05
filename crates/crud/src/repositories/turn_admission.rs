use anyhow::{Context, Result};
use pioneer_entity::turn_admission;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewTurnAdmission {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub request_digest: String,
}

pub async fn insert<C: ConnectionTrait>(
    db: &C,
    admission: NewTurnAdmission,
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    turn_admission::ActiveModel {
        turn_id: Set(admission.turn_id),
        thread_id: Set(admission.thread_id),
        workspace_id: Set(admission.workspace_id),
        request_digest: Set(admission.request_digest),
        created_at: Set(created_at),
    }
    .insert(db)
    .await
    .context("failed to insert native Turn admission")?;
    Ok(())
}

pub async fn find<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn_admission::Model>> {
    turn_admission::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query native Turn admission")
}
