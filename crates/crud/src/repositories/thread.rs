use anyhow::Result;
use pioneer_crud_access::membership::PersistedThreadAccessClass;
use pioneer_crud_conversations::thread_mutations;
use pioneer_protocol::{PersistedActorRef, PrincipalId, Thread};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

pub use pioneer_crud_conversations::thread_metadata::{
    find_thread_by_id, prepare_preview_author_json, preview_author_from_json,
    replace_thread_preview_if_matches, touch_thread_for_completed_message_prepared,
    update_thread_name, update_thread_name_if_changed, update_thread_status, update_thread_summary,
};
use pioneer_crud_conversations::thread_state::{self, NativeThreadNameFence};
pub use pioneer_crud_conversations::thread_state::{
    ThreadReadPosition, advance_thread_read_cursor, find_thread_read_cursor, is_list_conversation,
    list_read_candidates_page, list_threads_by_workspace, thread_read_cursor_from_model,
};

#[cfg(test)]
pub async fn upsert_thread<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    upsert_projected_thread(
        db,
        thread_model,
        None,
        prepare_preview_author_json(thread_model.preview_author.as_ref())?,
        created_at,
        updated_at,
    )
    .await
}

pub async fn upsert_thread_with_creator<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: &PersistedActorRef,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    if thread_mutations::upsert_thread_with_creator(
        db,
        thread_model,
        creator,
        created_at,
        updated_at,
    )
    .await?
    {
        super::task_child_producers::thread(db, &thread_model.id).await?;
    }
    Ok(())
}

pub async fn upsert_agent_thread_with_creator<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: &PersistedActorRef,
    access_class: PersistedThreadAccessClass,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    if thread_mutations::upsert_agent_thread_with_creator(
        db,
        thread_model,
        creator,
        access_class,
        created_at,
        updated_at,
    )
    .await?
    {
        super::task_child_producers::thread(db, &thread_model.id).await?;
    }
    Ok(())
}

pub async fn insert_user_thread_with_creator<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: &PersistedActorRef,
    access_class: PersistedThreadAccessClass,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let public_start = if access_class == PersistedThreadAccessClass::Workspace {
        Some(super::task_completion_inbox::current_completion_sequence(db).await?)
    } else {
        None
    };
    thread_mutations::insert_user_thread_with_creator(
        db,
        thread_model,
        creator,
        access_class,
        created_at,
        updated_at,
        public_start,
    )
    .await?;
    super::task_child_producers::thread(db, &thread_model.id).await
}

pub(crate) async fn upsert_projected_thread<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: Option<&PersistedActorRef>,
    preview_author_json: Option<String>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    if thread_mutations::upsert_projected_thread(
        db,
        thread_model,
        creator,
        preview_author_json,
        created_at,
        updated_at,
    )
    .await?
    {
        super::task_child_producers::thread(db, &thread_model.id).await?;
    }
    Ok(())
}

pub use pioneer_crud_conversations::thread_mutations::ThreadManagementChange;

pub async fn update_user_thread_management(
    db: &DatabaseTransaction,
    workspace_id: &str,
    thread_id: &str,
    expected_creator: Option<&PrincipalId>,
    name: Option<&str>,
    access_class: Option<PersistedThreadAccessClass>,
    archived: Option<bool>,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<ThreadManagementChange>> {
    thread_mutations::update_user_thread_management(
        db,
        workspace_id,
        thread_id,
        expected_creator,
        name,
        access_class,
        archived,
        updated_at,
        super::task_completion_inbox::current_completion_sequence(db),
    )
    .await
}

/// A single writer statement revalidates the title and, for native names, its
/// exact durable owner at commit. No provider work happens under DB capacity.
pub async fn update_automatic_thread_name<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    name: &str,
    fallback: Option<&str>,
    binding: Option<&crate::CliRuntimeThreadBindingRecord>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let binding = binding.map(|binding| NativeThreadNameFence {
        workspace_id: &binding.workspace_id,
        runtime_id: &binding.runtime_id,
        native_thread_id: &binding.native_thread_id,
    });
    thread_state::update_automatic_thread_name(db, thread_id, name, fallback, binding, updated_at)
        .await
}
