//! Nickname ownership shared by principals and runtime agents.

use anyhow::{Context, Result, bail};
use pioneer_entity::actor_nickname_index;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Set,
};

pub const NICKNAME_OWNER_PRINCIPAL: &str = "principal";
pub const NICKNAME_OWNER_AGENT: &str = "agent";
pub const NICKNAME_OWNER_RESERVED: &str = "reserved";
pub const NICKNAME_ACTIVE: &str = "active";
pub const NICKNAME_TOMBSTONED: &str = "tombstoned";

pub async fn claim_actor_nickname<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    nickname_key: &str,
    owner_kind: &str,
    owner_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<actor_nickname_index::Model> {
    if !matches!(
        owner_kind,
        NICKNAME_OWNER_PRINCIPAL | NICKNAME_OWNER_AGENT | NICKNAME_OWNER_RESERVED
    ) {
        bail!("unsupported actor nickname owner kind `{owner_kind}`");
    }
    let normalized = nickname_key.trim().to_ascii_lowercase();
    if normalized.len() < 2
        || normalized.len() > 32
        || normalized.chars().any(|c| {
            !(c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
        })
    {
        bail!("invalid actor nickname key");
    }
    if let Some(existing) = actor_nickname_index::Entity::find()
        .filter(actor_nickname_index::Column::WorkspaceId.eq(workspace_id))
        .filter(actor_nickname_index::Column::NicknameKey.eq(&normalized))
        .one(db)
        .await
        .context("failed to inspect actor nickname claim")?
    {
        if existing.status != NICKNAME_ACTIVE
            || existing.owner_kind != owner_kind
            || existing.owner_id != owner_id
        {
            bail!("actor nickname is already owned or tombstoned");
        }
        return Ok(existing);
    }

    // A source rename keeps the owner identity but retires the previous
    // handle.  Tombstoning the old row before claiming the new key preserves
    // historical lookups while ensuring only one active nickname belongs to
    // an actor.  Callers performing a multi-row mutation pass a transaction,
    // so a failed new claim rolls the tombstone back with the source update.
    let previous = actor_nickname_index::Entity::find()
        .filter(actor_nickname_index::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(actor_nickname_index::Column::OwnerKind.eq(owner_kind.to_owned()))
        .filter(actor_nickname_index::Column::OwnerId.eq(owner_id.to_owned()))
        .filter(actor_nickname_index::Column::Status.eq(NICKNAME_ACTIVE))
        .limit(2)
        .all(db)
        .await
        .context("failed to inspect previous actor nickname claim")?;
    if previous.len() > 1 {
        bail!("actor has multiple active nickname claims");
    }
    if let Some(previous) = previous.into_iter().next() {
        if previous.nickname_key != normalized {
            let mut retired: actor_nickname_index::ActiveModel = previous.into();
            retired.status = Set(NICKNAME_TOMBSTONED.to_owned());
            retired.tombstoned_at = Set(Some(now.clone()));
            retired
                .update(db)
                .await
                .context("failed to tombstone previous actor nickname")?;
        }
    }

    let insert_result = actor_nickname_index::Entity::insert(actor_nickname_index::ActiveModel {
        workspace_id: Set(workspace_id.to_owned()),
        nickname_key: Set(normalized.clone()),
        owner_kind: Set(owner_kind.to_owned()),
        owner_id: Set(owner_id.to_owned()),
        status: Set(NICKNAME_ACTIVE.to_owned()),
        claimed_at: Set(now),
        tombstoned_at: Set(None),
    })
    .on_conflict(OnConflict::new().do_nothing().to_owned())
    .exec(db)
    .await;
    let row = actor_nickname_index::Entity::find()
        .filter(actor_nickname_index::Column::WorkspaceId.eq(workspace_id))
        .filter(actor_nickname_index::Column::NicknameKey.eq(normalized))
        .one(db)
        .await
        .context("failed to reload actor nickname claim")?
        .context("actor nickname claim disappeared after insert")?;
    match insert_result {
        Ok(_) => {}
        Err(_error)
            if row.status == NICKNAME_ACTIVE
                && row.owner_kind == owner_kind
                && row.owner_id == owner_id =>
        {
            return Ok(row);
        }
        Err(error) => return Err(error).context("failed to claim actor nickname"),
    }
    if row.status != NICKNAME_ACTIVE || row.owner_kind != owner_kind || row.owner_id != owner_id {
        bail!("actor nickname is already owned or tombstoned");
    }
    Ok(row)
}
