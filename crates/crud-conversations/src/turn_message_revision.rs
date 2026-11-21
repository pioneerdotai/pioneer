//! Immutable message edits, delete receipts and revision paging.
use anyhow::{Context, Result};
use pioneer_crud_access::identity::{actor_ref_from_db, actor_ref_to_db};
use pioneer_entity::turn_message_revision;
use pioneer_protocol::{
    PersistedActorRef, TurnMention, TurnMessageRevision, TurnMessageRevisionChangeKind, UserInput,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::collaboration::{canonical_turn_mentions_json, turn_mentions_from_db};

const TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq)]
pub struct NewTurnMessageRevision<'a> {
    pub turn_id: &'a str,
    pub revision: u64,
    pub input: &'a [UserInput],
    pub mentions: &'a [TurnMention],
    pub changed_by: &'a PersistedActorRef,
    pub change_kind: TurnMessageRevisionChangeKind,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone)]
pub struct PreparedTurnMessageRevision {
    row: turn_message_revision::ActiveModel,
}

pub async fn insert_turn_message_revision<C: ConnectionTrait>(
    db: &C,
    revision: NewTurnMessageRevision<'_>,
) -> Result<()> {
    let prepared = prepare_turn_message_revision(revision)?;
    insert_prepared_turn_message_revision(db, prepared, false).await
}

pub fn prepare_turn_message_revision(
    revision: NewTurnMessageRevision<'_>,
) -> Result<PreparedTurnMessageRevision> {
    let revision_number = i64::try_from(revision.revision)
        .context("Turn message revision exceeds database integer range")?;
    let input_json = serde_json::to_string(revision.input)
        .context("failed to encode Turn message revision input")?;
    if input_json.len() > TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES {
        anyhow::bail!(
            "Turn message revision input exceeds {TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES} bytes"
        );
    }
    let mentions_json = canonical_turn_mentions_json(revision.mentions)?;
    let (changed_by_actor_kind, changed_by_actor_id) = actor_ref_to_db(revision.changed_by);
    let changed_by_actor_kind =
        changed_by_actor_kind.context("Turn message revision actor kind must be persisted")?;
    let change_kind = match revision.change_kind {
        TurnMessageRevisionChangeKind::Edit => "edit",
        TurnMessageRevisionChangeKind::Delete => "delete",
    };
    Ok(PreparedTurnMessageRevision {
        row: turn_message_revision::ActiveModel {
            turn_id: Set(revision.turn_id.to_owned()),
            revision: Set(revision_number),
            input_json: Set(input_json),
            mentions_json: Set(mentions_json),
            changed_by_actor_kind: Set(changed_by_actor_kind),
            changed_by_actor_id: Set(changed_by_actor_id),
            change_kind: Set(change_kind.to_owned()),
            created_at: Set(revision.created_at),
        },
    })
}

pub async fn insert_prepared_turn_message_revision<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnMessageRevision,
    if_absent: bool,
) -> Result<()> {
    let mut insert = turn_message_revision::Entity::insert(prepared.row);
    if if_absent {
        insert = insert.on_conflict(
            OnConflict::columns([
                turn_message_revision::Column::TurnId,
                turn_message_revision::Column::Revision,
            ])
            .do_nothing()
            .to_owned(),
        );
    }
    insert
        .exec_without_returning(db)
        .await
        .context(if if_absent {
            "failed to idempotently insert Turn message revision"
        } else {
            "failed to insert Turn message revision"
        })?;
    Ok(())
}

pub async fn list_turn_message_revisions<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    before_revision: Option<u64>,
    limit: u64,
) -> Result<Vec<turn_message_revision::Model>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut query = turn_message_revision::Entity::find()
        .filter(turn_message_revision::Column::TurnId.eq(turn_id.to_owned()));
    if let Some(before_revision) = before_revision {
        let before_revision = i64::try_from(before_revision)
            .context("Turn message revision cursor exceeds database integer range")?;
        query = query.filter(turn_message_revision::Column::Revision.lt(before_revision));
    }
    query
        .order_by_desc(turn_message_revision::Column::Revision)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list Turn message revisions")
}

pub fn turn_message_revision_from_model(
    model: turn_message_revision::Model,
) -> Result<TurnMessageRevision> {
    let revision = u64::try_from(model.revision)
        .context("Turn message revision has a negative persisted revision")?;
    let changed_by = actor_ref_from_db(
        Some(model.changed_by_actor_kind.as_str()),
        model.changed_by_actor_id.as_deref(),
    )?
    .context("Turn message revision is missing its persisted actor")?;
    let change_kind = match model.change_kind.as_str() {
        "edit" => TurnMessageRevisionChangeKind::Edit,
        "delete" => TurnMessageRevisionChangeKind::Delete,
        unknown => anyhow::bail!("unknown Turn message revision change kind `{unknown}`"),
    };
    if model.input_json.len() > TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES {
        anyhow::bail!(
            "persisted Turn message revision input exceeds {TURN_MESSAGE_REVISION_INPUT_JSON_MAX_BYTES} bytes"
        );
    }
    let input = serde_json::from_str::<Vec<UserInput>>(model.input_json.as_str())
        .context("failed to decode Turn message revision input")?;
    Ok(TurnMessageRevision {
        turn_id: model.turn_id,
        revision,
        change_kind,
        changed_by,
        created_at: model.created_at.timestamp(),
        input: Some(input),
        mentions: turn_mentions_from_db(model.mentions_json.as_str())?,
    })
}
