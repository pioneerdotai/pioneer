use anyhow::{Context, Result};
#[cfg(test)]
use pioneer_protocol::Turn;
use pioneer_protocol::{TurnStatus, UserInput};
use sea_orm::ConnectionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

pub use pioneer_crud_conversations::turn_reads::{
    ExactAgentMessageRow, TURN_MESSAGE_GUARD_PAGE_BYTES, TURN_MESSAGE_GUARD_PAGE_ROWS,
    TimelineAssistantItemRow, TurnMessageGuard, count_completed_turns_for_thread,
    exact_agent_messages, find_completed_turn_items, find_completed_turns_for_thread,
    find_latest_conversation_turn_for_thread, find_latest_turn_for_thread,
    find_oldest_user_conversation_turn, find_terminal_turns_for_thread, find_turn_by_id,
    find_turn_by_thread_and_id, find_turn_inputs, find_turn_inputs_for_turns, find_turn_item,
    find_turn_item_metadata, find_turn_item_type, find_turn_message_guards_by_thread_and_ids,
    find_turn_work_owner, find_turns_by_thread_and_ids, has_in_progress_conversation_turn,
    is_completed_agent_message, list_turn_items_by_ids, list_turn_items_by_type,
    list_turn_items_by_type_for_turns, timeline_assistant_items,
};

pub use pioneer_crud_conversations::tool_summary::{
    PostTurnToolSummary, list as post_turn_tool_summaries,
};

pub use pioneer_crud_conversations::turn_input::post_turn_user_text;

pub use pioneer_crud_conversations::collaboration::{
    PersistedTurnCollaboration, collaboration_from_model,
};

pub use pioneer_crud_access::turn_execution::{
    TurnExecutionBinding, find_turn_execution_authorization_context, find_turn_execution_binding,
    find_turn_execution_status, find_turn_initiator,
};

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

pub use pioneer_crud_conversations::turn_writes::{
    PreparedMessageTurnMutationState, PreparedTurnStatusHistory, PreparedTurnUpsert,
    TurnPromptManifestColumns, append_prepared_turn_status_history, append_turn_status_history,
    find_turn_collaboration, prepare_message_turn_mutation_state, prepare_projected_turn_upsert,
    prepare_turn_status_history, project_prepared_message_turn_mutation_state,
    update_turn_prompt_manifest,
};

pub use pioneer_crud_conversations::turn_message_revision::{
    NewTurnMessageRevision, PreparedTurnMessageRevision, insert_prepared_turn_message_revision,
    insert_turn_message_revision, list_turn_message_revisions, prepare_turn_message_revision,
    turn_message_revision_from_model,
};

pub use pioneer_crud_execution::turn_envelope::{
    TurnExecutionSecuritySnapshotRecord, find_turn_execution_security_snapshot,
    set_turn_execution_authorization_context, set_turn_execution_envelope,
    set_turn_execution_security_snapshot,
};

pub async fn update_turn_status<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    status: TurnStatus,
    error: Option<&str>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(change) = pioneer_crud_conversations::turn_writes::update_turn_status(
        db, thread_id, turn_id, status, error, updated_at,
    )
    .await?
    else {
        return Ok(false);
    };
    publish_task_turn_change(db, &change).await?;
    Ok(true)
}

pub(crate) async fn apply_prepared_turn_upsert<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnUpsert,
) -> Result<()> {
    let change =
        pioneer_crud_conversations::turn_writes::apply_prepared_turn_upsert(db, prepared).await?;
    publish_task_turn_change(db, &change).await
}

async fn publish_task_turn_change<C: ConnectionTrait>(
    db: &C,
    change: &pioneer_crud_conversations::turn_writes::TurnChange,
) -> Result<()> {
    let previous = change.previous.as_ref();
    let current = Some(&change.current);
    super::task_child_producers::turn_change(db, previous, current).await?;
    super::task_occurrence_producers::turn_change(db, previous, current).await
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
    preview_adoptions: Vec<super::html_preview_retention::PreparedPreviewInputAdoption>,
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
            } => Some(
                super::html_preview_retention::PreparedPreviewInputAdoption::new(
                    turn_id,
                    artifact_id,
                    version_id.as_deref(),
                    i64::try_from(index).unwrap_or(i64::MAX),
                    created_at,
                ),
            ),
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
        super::html_preview_retention::retain_preview_input(db, adoption).await?;
    }
    pioneer_crud_conversations::turn_input::replace(db, prepared.input).await
}

pub(crate) use pioneer_crud_conversations::turn_item::{
    PreparedTurnItemProjection, prepare_turn_item_projection, upsert_prepared_turn_item,
};

pub(crate) use pioneer_crud_conversations::turn_order::{
    THREAD_TURN_SNAPSHOT_BATCH_SIZE, find_latest_turns_for_threads,
    latest_turn_id_by_creation_order, turn_before_launch_and_intervening_by_creation_order,
    turn_ids_after_by_creation_order,
};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[pioneer_postgres_test::postgres_test]
    async fn compaction_history_and_cli_share_the_current_turn_order() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection().clone();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE); \
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) \
                 VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
             INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES \
                 ('z','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
                 ('a','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
                 ('launch','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
                 ('backdated','thread','completed','conversation','user','2026-01-01T09:00:00Z',CURRENT_TIMESTAMP), \
                 ('occurrence','thread','completed','task_run','system','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
                 ('future','thread','completed','conversation','user','2026-01-01T11:00:00Z',CURRENT_TIMESTAMP)",
        ).await.unwrap();
        let store = crate::CrudStore::new(db.clone());
        let history_fence = store.compaction_history_read_fence().await.unwrap();
        let mut history_turns = store
            .compaction_history_turn_page("ws", "thread", "", &history_fence)
            .await
            .unwrap();
        history_turns.sort_by(|left, right| {
            (
                &left.created_at,
                left.creation_order,
                left.legacy_creation_order,
                &left.id,
            )
                .cmp(&(
                    &right.created_at,
                    right.creation_order,
                    right.legacy_creation_order,
                    &right.id,
                ))
        });
        let history_order = history_turns
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            history_order,
            vec!["backdated", "z", "a", "launch", "occurrence", "future"]
        );
        let launch_index = history_order
            .iter()
            .position(|turn| *turn == "launch")
            .unwrap();
        assert_eq!(history_order[launch_index - 1], "a");
        let boundary = turn_before_launch_and_intervening_by_creation_order(
            &db,
            "thread",
            "launch",
            "occurrence",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            boundary,
            (Some(history_order[launch_index - 1].into()), true)
        );
    }
}
