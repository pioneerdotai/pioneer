use anyhow::{Context, Result};
use pioneer_entity::thread_sandox_policy;
use pioneer_protocol::SandboxMode;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ConnectionTrait, EntityTrait, Set};

use crate::convention::sandbox_mode_to_db;

pub async fn upsert_thread_sandbox_policy<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    sandbox_mode: SandboxMode,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    thread_sandox_policy::Entity::insert(thread_sandox_policy::ActiveModel {
        thread_id: Set(thread_id.to_owned()),
        mode: Set(sandbox_mode_to_db(sandbox_mode).to_owned()),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::column(thread_sandox_policy::Column::ThreadId)
            .update_columns([
                thread_sandox_policy::Column::Mode,
                thread_sandox_policy::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert thread sandbox policy")?;

    Ok(())
}

pub async fn find_thread_sandbox_mode<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<SandboxMode>> {
    let model = thread_sandox_policy::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread sandbox policy")?;

    let mode = model.and_then(|model| match model.mode.as_str() {
        "full_access" => Some(SandboxMode::FullAccess),
        _ => None,
    });

    Ok(mode)
}
