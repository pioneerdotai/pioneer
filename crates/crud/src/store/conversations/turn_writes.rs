use anyhow::Result;
use pioneer_crud_artifacts::html_preview;
use pioneer_crud_conversations::turn_writes::PreparedTurnUpsert;
#[cfg(test)]
use pioneer_crud_conversations::turn_writes::TurnPromptManifestColumns;
#[cfg(test)]
use pioneer_protocol::Turn;
use pioneer_protocol::UserInput;
use sea_orm::ConnectionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

#[cfg(test)]
pub async fn upsert_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    thread_id: &str,
    turn_model: &Turn,
    prompt_manifest: Option<&TurnPromptManifestColumns>,
    reasoning_effort: Option<&str>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    upsert_turn_with_actor_columns(
        db,
        turn_id,
        thread_id,
        turn_model,
        prompt_manifest,
        reasoning_effort,
        None,
        None,
        false,
        created_at,
        updated_at,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn upsert_turn_with_actor_columns<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    thread_id: &str,
    turn_model: &Turn,
    prompt_manifest: Option<&TurnPromptManifestColumns>,
    reasoning_effort: Option<&str>,
    initiated_by_actor_kind: Option<String>,
    initiated_by_actor_id: Option<String>,
    update_initiator: bool,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let prepared = pioneer_crud_conversations::turn_writes::prepare_turn_upsert_with_actor_columns(
        db,
        turn_id,
        thread_id,
        turn_model,
        prompt_manifest,
        reasoning_effort,
        None,
        initiated_by_actor_kind,
        initiated_by_actor_id,
        update_initiator,
        created_at,
        updated_at,
    )
    .await?;
    apply_prepared_turn_upsert(db, prepared).await
}

pub(crate) async fn apply_prepared_turn_upsert<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnUpsert,
) -> Result<()> {
    let change =
        pioneer_crud_conversations::turn_writes::apply_prepared_turn_upsert(db, prepared).await?;
    pioneer_crud_tasks::turn_sources::publish_task_turn_change(db, &change).await
}

#[cfg(any(test, feature = "test-support"))]
pub async fn replace_turn_input<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    input: &[UserInput],
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    let prepared = prepare_turn_input_projection(turn_id, input, created_at)?;
    replace_prepared_turn_input(db, prepared).await
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedTurnInputProjection {
    input: pioneer_crud_conversations::turn_input::PreparedTurnInput,
    preview_adoptions: Vec<html_preview::PreparedPreviewInputAdoption>,
}

pub(crate) fn prepare_turn_input_projection(
    turn_id: &str,
    input: &[UserInput],
    created_at: DateTimeWithTimeZone,
) -> Result<PreparedTurnInputProjection> {
    let projection = pioneer_crud_conversations::turn_input::prepare(turn_id, input, created_at)?;
    let preview_adoptions = input
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match item {
            UserInput::Artifact {
                artifact_id,
                version_id,
            } => Some(html_preview::PreparedPreviewInputAdoption::new(
                turn_id,
                artifact_id,
                version_id.as_deref(),
                i64::try_from(index).unwrap_or(i64::MAX),
                created_at,
            )),
            _ => None,
        })
        .collect();
    Ok(PreparedTurnInputProjection {
        input: projection,
        preview_adoptions,
    })
}

pub(crate) async fn replace_prepared_turn_input<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnInputProjection,
) -> Result<()> {
    for adoption in &prepared.preview_adoptions {
        html_preview::retain_preview_input(db, adoption).await?;
    }
    pioneer_crud_conversations::turn_input::replace(db, prepared.input).await
}
