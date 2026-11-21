use anyhow::Result;
use pioneer_crud_access::membership::PersistedThreadAccessClass;
use pioneer_crud_conversations::thread_metadata::prepare_preview_author_json;
use pioneer_crud_conversations::thread_mutations::ThreadManagementChange;
use pioneer_crud_conversations::thread_state::NativeThreadNameFence;
use pioneer_crud_conversations::{thread_mutations, thread_state};
use pioneer_crud_tasks::task_child_producers;
use pioneer_protocol::{PersistedActorRef, PrincipalId, TaskThreadLineage, Thread};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

pub async fn upsert_lineage<C: ConnectionTrait>(db: &C, lineage: &TaskThreadLineage) -> Result<()> {
    if pioneer_crud_conversations::thread_lineage::upsert_lineage(db, lineage).await? {
        task_child_producers::lineage(db, &lineage.child_thread_id).await?;
    }
    Ok(())
}

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
        task_child_producers::thread(db, &thread_model.id).await?;
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
        task_child_producers::thread(db, &thread_model.id).await?;
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
        Some(pioneer_crud_tasks::completion_inbox::current_completion_sequence(db).await?)
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
    task_child_producers::thread(db, &thread_model.id).await
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
        task_child_producers::thread(db, &thread_model.id).await?;
    }
    Ok(())
}

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
        pioneer_crud_tasks::completion_inbox::current_completion_sequence(db),
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
