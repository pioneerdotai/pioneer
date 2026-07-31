use anyhow::{Context, Result};
use pioneer_entity::{
    thread, thread_episodic_capsules, thread_episodic_embedding_artifacts,
    thread_episodic_exclusions, thread_episodic_index_jobs, thread_episodic_items,
    thread_episodic_recall_events, thread_episodic_thread_directory,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict, Query};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, IntoActiveModel,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};
use std::collections::HashSet;
use std::mem::size_of;

use crate::convention::DB_ID_LEN;
use crate::thread_episodic::{
    NewThreadEpisodicCapsuleRecord, NewThreadEpisodicEmbeddingArtifactRecord,
    NewThreadEpisodicExclusionRecord, NewThreadEpisodicIndexJobRecord, NewThreadEpisodicItemRecord,
    NewThreadEpisodicRecallEventRecord, NewThreadEpisodicThreadDirectoryRecord,
    THREAD_EPISODIC_WORKSPACE_CAPSULE_THREAD_ID, ThreadEpisodicCapsuleCapacityUpdate,
    ThreadEpisodicCapsuleStatus, ThreadEpisodicCapsuleWriteState,
    ThreadEpisodicIndexJobCompletionUpdate, ThreadEpisodicIndexJobFailureUpdate,
    ThreadEpisodicIndexJobStatus, ThreadEpisodicItemIndexedUpdate, ThreadEpisodicItemStatus,
    ThreadEpisodicItemVisibility, ThreadEpisodicRefillSourceCounts, ThreadEpisodicRefillThread,
    ThreadEpisodicThreadDirectorySelection, ThreadEpisodicThreadDirectoryStatus,
    ThreadEpisodicThreadDirectoryVisibility, capsule_status_to_db, capsule_write_state_to_db,
    exclusion_reason_to_db, graph_enrichment_state_to_db, index_job_status_from_db,
    index_job_status_to_db, item_status_to_db, item_visibility_to_db, repair_status_to_db,
    source_actor_role_to_db, source_runtime_kind_to_db, thread_directory_status_to_db,
    thread_directory_visibility_to_db,
};

pub async fn find_active_write_capsule<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<Option<thread_episodic_capsules::Model>> {
    thread_episodic_capsules::Entity::find()
        .filter(thread_episodic_capsules::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_capsules::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(
            thread_episodic_capsules::Column::WriteState.eq(capsule_write_state_to_db(
                ThreadEpisodicCapsuleWriteState::ActiveWrite,
            )),
        )
        .filter(
            thread_episodic_capsules::Column::Status
                .ne(capsule_status_to_db(ThreadEpisodicCapsuleStatus::Deleted)),
        )
        .order_by_desc(thread_episodic_capsules::Column::SegmentIndex)
        .one(db)
        .await
        .with_context(|| {
            format!("failed to find active thread episodic capsule for thread `{thread_id}`")
        })
}

pub async fn find_active_write_workspace_capsule<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Option<thread_episodic_capsules::Model>> {
    find_active_write_capsule(
        db,
        workspace_id,
        THREAD_EPISODIC_WORKSPACE_CAPSULE_THREAD_ID,
    )
    .await
    .with_context(|| {
        format!(
            "failed to find active workspace thread episodic capsule for workspace `{workspace_id}`"
        )
    })
}

pub async fn max_segment_index_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<Option<i64>> {
    let row = thread_episodic_capsules::Entity::find()
        .filter(thread_episodic_capsules::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_capsules::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(thread_episodic_capsules::Column::SegmentIndex)
        .one(db)
        .await
        .with_context(|| {
            format!("failed to find latest thread episodic segment for thread `{thread_id}`")
        })?;
    Ok(row.map(|row| row.segment_index))
}

pub async fn insert_capsule_if_absent<C: ConnectionTrait>(
    db: &C,
    capsule: NewThreadEpisodicCapsuleRecord,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    thread_episodic_capsules::Entity::insert(thread_episodic_capsules::ActiveModel {
        id: Set(capsule.id),
        workspace_id: Set(capsule.workspace_id),
        workspace_key_hash: Set(capsule.workspace_key_hash),
        thread_id: Set(capsule.thread_id),
        thread_key_hash: Set(capsule.thread_key_hash),
        segment_index: Set(capsule.segment_index),
        write_state: Set(capsule_write_state_to_db(capsule.write_state).to_owned()),
        capsule_ref: Set(capsule.capsule_ref),
        storage_uri: Set(capsule.storage_uri),
        backend: Set(capsule.backend),
        format: Set(capsule.format),
        encrypted: Set(capsule.encrypted),
        status: Set(capsule_status_to_db(capsule.status).to_owned()),
        repair_status: Set(repair_status_to_db(capsule.repair_status).to_owned()),
        active_frame_count: Set(capsule.active_frame_count),
        capacity_bytes: Set(capsule.capacity_bytes),
        size_bytes: Set(capsule.size_bytes),
        utilization_percent: Set(capsule.utilization_percent),
        last_capacity_check_at: Set(None),
        near_capacity_at: Set(None),
        capacity_exceeded_at: Set(None),
        last_vacuumed_at: Set(None),
        last_compacted_at: Set(None),
        content_hash: Set(capsule.content_hash),
        metadata_json: Set(capsule.metadata_json),
        last_error: Set(capsule.last_error),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::column(thread_episodic_capsules::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to insert thread episodic capsule")?;
    Ok(())
}

pub async fn upsert_item_by_source_identity<C: ConnectionTrait>(
    db: &C,
    item: NewThreadEpisodicItemRecord,
    now: DateTimeWithTimeZone,
) -> Result<thread_episodic_items::Model> {
    if let Some(existing) = find_item_by_source_identity(
        db,
        item.workspace_id.as_str(),
        item.thread_id.as_str(),
        item.turn_id.as_str(),
        item.item_id.as_str(),
        item.text_hash.as_str(),
    )
    .await?
    {
        return Ok(existing);
    }

    let id = item
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    let source_context_json = serde_json::to_string(&item.source_context)
        .context("failed to serialize thread episodic item source context")?;

    thread_episodic_items::Entity::insert(thread_episodic_items::ActiveModel {
        id: Set(id),
        workspace_id: Set(item.workspace_id.clone()),
        thread_id: Set(item.thread_id.clone()),
        turn_id: Set(item.turn_id.clone()),
        item_id: Set(item.item_id.clone()),
        source_actor_role: Set(source_actor_role_to_db(item.source_actor_role).to_owned()),
        source_runtime_kind: Set(source_runtime_kind_to_db(item.source_runtime_kind).to_owned()),
        source_context_json: Set(source_context_json),
        visibility: Set(item_visibility_to_db(item.visibility).to_owned()),
        status: Set(item_status_to_db(item.status).to_owned()),
        text_hash: Set(item.text_hash.clone()),
        source_text_hash: Set(item.source_text_hash),
        projection_group_id: Set(Some(item.projection_group_id)),
        embedding_artifact_id: Set(None),
        language_hint: Set(item.language_hint),
        token_estimate: Set(item.token_estimate),
        capsule_id: Set(item.capsule_id),
        capsule_ref: Set(item.capsule_ref),
        segment_index: Set(item.segment_index),
        frame_id: Set(item.frame_id),
        frame_uri: Set(item.frame_uri),
        indexed_at: Set(item.indexed_at),
        created_at: Set(now),
        updated_at: Set(now),
        deleted_at: Set(item.deleted_at),
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to upsert thread episodic item `{}/{}/{}/{}`",
            item.workspace_id, item.thread_id, item.turn_id, item.item_id
        )
    })?;

    find_item_by_source_identity(
        db,
        item.workspace_id.as_str(),
        item.thread_id.as_str(),
        item.turn_id.as_str(),
        item.item_id.as_str(),
        item.text_hash.as_str(),
    )
    .await?
    .context("upserted thread episodic item missing")
}

pub async fn find_embedding_artifact_by_id<C: ConnectionTrait>(
    db: &C,
    artifact_id: &str,
) -> Result<Option<thread_episodic_embedding_artifacts::Model>> {
    thread_episodic_embedding_artifacts::Entity::find_by_id(artifact_id.to_owned())
        .one(db)
        .await
        .with_context(|| {
            format!("failed to find thread episodic embedding artifact `{artifact_id}`")
        })
}

pub async fn insert_embedding_artifact_if_absent<C: ConnectionTrait>(
    db: &C,
    artifact: NewThreadEpisodicEmbeddingArtifactRecord,
    now: DateTimeWithTimeZone,
) -> Result<thread_episodic_embedding_artifacts::Model> {
    let dimension = i64::try_from(artifact.dimension)
        .context("thread episodic embedding dimension exceeds i64")?;
    let vector_bytes = encode_embedding_vector(artifact.vector.as_slice());
    let artifact_id = artifact.id.clone();
    thread_episodic_embedding_artifacts::Entity::insert(
        thread_episodic_embedding_artifacts::ActiveModel {
            id: Set(artifact.id),
            workspace_id: Set(artifact.workspace_id),
            pipeline_identity_hash: Set(artifact.pipeline_identity_hash),
            input_hash: Set(artifact.input_hash),
            provider_id: Set(artifact.provider_id),
            model: Set(artifact.model),
            dimension: Set(dimension),
            normalized: Set(artifact.normalized),
            vector_bytes: Set(vector_bytes),
            created_at: Set(now),
            last_used_at: Set(now),
        },
    )
    .on_conflict(
        OnConflict::column(thread_episodic_embedding_artifacts::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!("failed to insert thread episodic embedding artifact `{artifact_id}`")
    })?;

    find_embedding_artifact_by_id(db, artifact_id.as_str())
        .await?
        .context("thread episodic embedding artifact missing after insert")
}

pub async fn touch_embedding_artifact<C: ConnectionTrait>(
    db: &C,
    artifact_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_embedding_artifacts::Model>> {
    let Some(row) = find_embedding_artifact_by_id(db, artifact_id).await? else {
        return Ok(None);
    };
    let mut active = row.into_active_model();
    active.last_used_at = Set(now);
    active
        .update(db)
        .await
        .with_context(|| {
            format!("failed to touch thread episodic embedding artifact `{artifact_id}`")
        })
        .map(Some)
}

fn encode_embedding_vector(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len().saturating_mul(size_of::<f32>()));
    for value in vector {
        bytes.extend_from_slice(value.to_le_bytes().as_slice());
    }
    bytes
}

pub async fn find_item_by_source_identity<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    text_hash: &str,
) -> Result<Option<thread_episodic_items::Model>> {
    thread_episodic_items::Entity::find()
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_items::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(thread_episodic_items::Column::TurnId.eq(turn_id.to_owned()))
        .filter(thread_episodic_items::Column::ItemId.eq(item_id.to_owned()))
        .filter(thread_episodic_items::Column::TextHash.eq(text_hash.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to find thread episodic item `{workspace_id}/{thread_id}/{turn_id}/{item_id}`"
            )
        })
}

pub async fn find_capsule_by_id<C: ConnectionTrait>(
    db: &C,
    capsule_id: &str,
) -> Result<Option<thread_episodic_capsules::Model>> {
    thread_episodic_capsules::Entity::find_by_id(capsule_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to find thread episodic capsule `{capsule_id}`"))
}

pub async fn list_capsules_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_capsules::Model>> {
    thread_episodic_capsules::Entity::find()
        .filter(thread_episodic_capsules::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_capsules::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(thread_episodic_capsules::Column::SegmentIndex)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list thread episodic capsules for thread `{thread_id}`")
        })
}

pub async fn list_workspace_capsules<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_capsules::Model>> {
    thread_episodic_capsules::Entity::find()
        .filter(thread_episodic_capsules::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread_episodic_capsules::Column::ThreadId
                .eq(THREAD_EPISODIC_WORKSPACE_CAPSULE_THREAD_ID.to_owned()),
        )
        .order_by_asc(thread_episodic_capsules::Column::SegmentIndex)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list workspace thread episodic capsules for workspace `{workspace_id}`"
            )
        })
}

pub async fn list_all_capsules<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<thread_episodic_capsules::Model>> {
    thread_episodic_capsules::Entity::find()
        .order_by_asc(thread_episodic_capsules::Column::WorkspaceId)
        .order_by_asc(thread_episodic_capsules::Column::ThreadId)
        .order_by_asc(thread_episodic_capsules::Column::SegmentIndex)
        .all(db)
        .await
        .context("failed to list thread episodic capsules")
}

pub async fn list_all_capsules_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<thread_episodic_capsules::Model>> {
    thread_episodic_capsules::Entity::find()
        .filter(thread_episodic_capsules::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_asc(thread_episodic_capsules::Column::ThreadId)
        .order_by_asc(thread_episodic_capsules::Column::SegmentIndex)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list thread episodic capsules for workspace `{workspace_id}`")
        })
}

pub async fn delete_all_capsules<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let result = thread_episodic_capsules::Entity::delete_many()
        .exec(db)
        .await
        .context("failed to delete thread episodic capsule rows")?;
    Ok(result.rows_affected)
}

pub async fn delete_capsules_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<u64> {
    let result = thread_episodic_capsules::Entity::delete_many()
        .filter(thread_episodic_capsules::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to delete thread episodic capsule rows for workspace `{workspace_id}`")
        })?;
    Ok(result.rows_affected)
}

pub async fn delete_all_items<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let result = thread_episodic_items::Entity::delete_many()
        .exec(db)
        .await
        .context("failed to delete thread episodic item rows")?;
    Ok(result.rows_affected)
}

pub async fn delete_items_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<u64> {
    let result = thread_episodic_items::Entity::delete_many()
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to delete thread episodic item rows for workspace `{workspace_id}`")
        })?;
    Ok(result.rows_affected)
}

pub async fn delete_all_exclusions<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let result = thread_episodic_exclusions::Entity::delete_many()
        .exec(db)
        .await
        .context("failed to delete thread episodic exclusion rows")?;
    Ok(result.rows_affected)
}

pub async fn delete_exclusions_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<u64> {
    let result = thread_episodic_exclusions::Entity::delete_many()
        .filter(thread_episodic_exclusions::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to delete thread episodic exclusion rows for workspace `{workspace_id}`"
            )
        })?;
    Ok(result.rows_affected)
}

pub async fn delete_all_index_jobs<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let result = thread_episodic_index_jobs::Entity::delete_many()
        .exec(db)
        .await
        .context("failed to delete thread episodic index jobs")?;
    Ok(result.rows_affected)
}

pub async fn delete_index_jobs_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<u64> {
    let result = thread_episodic_index_jobs::Entity::delete_many()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to delete thread episodic index jobs for workspace `{workspace_id}`")
        })?;
    Ok(result.rows_affected)
}

pub async fn delete_all_thread_directory_entries<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let result = thread_episodic_thread_directory::Entity::delete_many()
        .exec(db)
        .await
        .context("failed to delete thread episodic thread directory rows")?;
    Ok(result.rows_affected)
}

pub async fn delete_thread_directory_entries_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<u64> {
    let result = thread_episodic_thread_directory::Entity::delete_many()
        .filter(thread_episodic_thread_directory::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to delete thread episodic thread directory rows for workspace `{workspace_id}`"
            )
        })?;
    Ok(result.rows_affected)
}

pub async fn list_refill_workspace_ids<C: ConnectionTrait>(db: &C) -> Result<Vec<String>> {
    thread::Entity::find()
        .select_only()
        .column(thread::Column::WorkspaceId)
        .distinct()
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to list thread episodic refill workspace ids")
}

pub async fn list_refill_threads<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<ThreadEpisodicRefillThread>> {
    let rows = thread::Entity::find()
        .select_only()
        .columns([thread::Column::WorkspaceId, thread::Column::Id])
        .order_by_asc(thread::Column::WorkspaceId)
        .order_by_asc(thread::Column::Id)
        .into_tuple::<(String, String)>()
        .all(db)
        .await
        .context("failed to list thread episodic refill source threads")?;

    Ok(rows
        .into_iter()
        .map(|(workspace_id, thread_id)| ThreadEpisodicRefillThread {
            workspace_id,
            thread_id,
        })
        .collect())
}

pub async fn list_refill_threads_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<ThreadEpisodicRefillThread>> {
    let rows = thread::Entity::find()
        .select_only()
        .columns([thread::Column::WorkspaceId, thread::Column::Id])
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_asc(thread::Column::Id)
        .into_tuple::<(String, String)>()
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list thread episodic refill source threads for workspace `{workspace_id}`")
        })?;

    Ok(rows
        .into_iter()
        .map(|(workspace_id, thread_id)| ThreadEpisodicRefillThread {
            workspace_id,
            thread_id,
        })
        .collect())
}

pub async fn count_refill_sources<C: ConnectionTrait>(
    db: &C,
) -> Result<ThreadEpisodicRefillSourceCounts> {
    let rows = thread_episodic_items::Entity::find()
        .select_only()
        .columns([
            thread_episodic_items::Column::WorkspaceId,
            thread_episodic_items::Column::ThreadId,
            thread_episodic_items::Column::TurnId,
            thread_episodic_items::Column::ItemId,
        ])
        .filter(thread_episodic_items::Column::Status.is_in([
            item_status_to_db(ThreadEpisodicItemStatus::PendingIndex).to_owned(),
            item_status_to_db(ThreadEpisodicItemStatus::Active).to_owned(),
            item_status_to_db(ThreadEpisodicItemStatus::Failed).to_owned(),
        ]))
        .filter(thread_episodic_items::Column::DeletedAt.is_null())
        .filter(
            thread_episodic_items::Column::Visibility.ne(item_visibility_to_db(
                ThreadEpisodicItemVisibility::InternalHidden,
            )),
        )
        .into_tuple::<(String, String, String, String)>()
        .all(db)
        .await
        .context("failed to count thread episodic refill source rows")?;

    let mut source_threads = HashSet::new();
    let mut source_turns = HashSet::new();
    for (workspace_id, thread_id, turn_id, _item_id) in rows.iter() {
        source_threads.insert((workspace_id.clone(), thread_id.clone()));
        source_turns.insert((workspace_id.clone(), thread_id.clone(), turn_id.clone()));
    }

    Ok(ThreadEpisodicRefillSourceCounts {
        source_thread_count: saturating_i64_from_usize(source_threads.len()),
        source_turn_count: saturating_i64_from_usize(source_turns.len()),
        source_turn_item_count: saturating_i64_from_usize(rows.len()),
    })
}

pub async fn count_refill_sources_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<ThreadEpisodicRefillSourceCounts> {
    let rows = thread_episodic_items::Entity::find()
        .select_only()
        .columns([
            thread_episodic_items::Column::ThreadId,
            thread_episodic_items::Column::TurnId,
            thread_episodic_items::Column::ItemId,
        ])
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_items::Column::Status.is_in([
            item_status_to_db(ThreadEpisodicItemStatus::PendingIndex).to_owned(),
            item_status_to_db(ThreadEpisodicItemStatus::Active).to_owned(),
            item_status_to_db(ThreadEpisodicItemStatus::Failed).to_owned(),
        ]))
        .filter(thread_episodic_items::Column::DeletedAt.is_null())
        .filter(
            thread_episodic_items::Column::Visibility.ne(item_visibility_to_db(
                ThreadEpisodicItemVisibility::InternalHidden,
            )),
        )
        .into_tuple::<(String, String, String)>()
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to count thread episodic refill source rows for workspace `{workspace_id}`"
            )
        })?;

    let mut source_threads = HashSet::new();
    let mut source_turns = HashSet::new();
    for (thread_id, turn_id, _item_id) in rows.iter() {
        source_threads.insert(thread_id.clone());
        source_turns.insert((thread_id.clone(), turn_id.clone()));
    }

    Ok(ThreadEpisodicRefillSourceCounts {
        source_thread_count: saturating_i64_from_usize(source_threads.len()),
        source_turn_count: saturating_i64_from_usize(source_turns.len()),
        source_turn_item_count: saturating_i64_from_usize(rows.len()),
    })
}

pub async fn list_refill_indexable_items<C: ConnectionTrait>(
    db: &C,
    limit: u64,
) -> Result<Vec<thread_episodic_items::Model>> {
    let items_with_jobs = Query::select()
        .column(thread_episodic_index_jobs::Column::IndexItemId)
        .from(thread_episodic_index_jobs::Entity)
        .to_owned();

    thread_episodic_items::Entity::find()
        .filter(
            thread_episodic_items::Column::Status
                .eq(item_status_to_db(ThreadEpisodicItemStatus::PendingIndex)),
        )
        .filter(thread_episodic_items::Column::DeletedAt.is_null())
        .filter(
            thread_episodic_items::Column::Visibility.ne(item_visibility_to_db(
                ThreadEpisodicItemVisibility::InternalHidden,
            )),
        )
        .filter(thread_episodic_items::Column::Id.not_in_subquery(items_with_jobs))
        .order_by_asc(thread_episodic_items::Column::WorkspaceId)
        .order_by_asc(thread_episodic_items::Column::ThreadId)
        .order_by_asc(thread_episodic_items::Column::TurnId)
        .order_by_asc(thread_episodic_items::Column::ItemId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list thread episodic refill indexable items")
}

pub async fn list_refill_indexable_items_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_items::Model>> {
    let items_with_jobs = Query::select()
        .column(thread_episodic_index_jobs::Column::IndexItemId)
        .from(thread_episodic_index_jobs::Entity)
        .to_owned();

    thread_episodic_items::Entity::find()
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread_episodic_items::Column::Status
                .eq(item_status_to_db(ThreadEpisodicItemStatus::PendingIndex)),
        )
        .filter(thread_episodic_items::Column::DeletedAt.is_null())
        .filter(
            thread_episodic_items::Column::Visibility.ne(item_visibility_to_db(
                ThreadEpisodicItemVisibility::InternalHidden,
            )),
        )
        .filter(thread_episodic_items::Column::Id.not_in_subquery(items_with_jobs))
        .order_by_asc(thread_episodic_items::Column::ThreadId)
        .order_by_asc(thread_episodic_items::Column::TurnId)
        .order_by_asc(thread_episodic_items::Column::ItemId)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list thread episodic refill indexable items for workspace `{workspace_id}`"
            )
        })
}

fn saturating_i64_from_usize(value: usize) -> i64 {
    value.min(i64::MAX as usize) as i64
}

pub async fn count_incomplete_index_jobs<C: ConnectionTrait>(db: &C) -> Result<u64> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::Status.is_in([
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Queued).to_owned(),
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Running).to_owned(),
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Failed).to_owned(),
        ]))
        .count(db)
        .await
        .context("failed to count incomplete thread episodic index jobs")
}

pub async fn count_incomplete_index_jobs_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<u64> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_index_jobs::Column::Status.is_in([
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Queued).to_owned(),
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Running).to_owned(),
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Failed).to_owned(),
        ]))
        .count(db)
        .await
        .with_context(|| {
            format!(
                "failed to count incomplete thread episodic index jobs for workspace `{workspace_id}`"
            )
        })
}

pub async fn count_canceled_index_jobs_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<u64> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread_episodic_index_jobs::Column::Status.eq(index_job_status_to_db(
                ThreadEpisodicIndexJobStatus::Canceled,
            )),
        )
        .count(db)
        .await
        .with_context(|| {
            format!(
                "failed to count canceled thread episodic index jobs for workspace `{workspace_id}`"
            )
        })
}

pub async fn transition_capsule_write_state<C: ConnectionTrait>(
    db: &C,
    capsule_id: &str,
    from_state: ThreadEpisodicCapsuleWriteState,
    to_state: ThreadEpisodicCapsuleWriteState,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_capsules::Model>> {
    thread_episodic_capsules::Entity::update_many()
        .col_expr(
            thread_episodic_capsules::Column::WriteState,
            Expr::value(capsule_write_state_to_db(to_state)),
        )
        .col_expr(
            thread_episodic_capsules::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(thread_episodic_capsules::Column::Id.eq(capsule_id.to_owned()))
        .filter(
            thread_episodic_capsules::Column::WriteState.eq(capsule_write_state_to_db(from_state)),
        )
        .exec(db)
        .await
        .with_context(|| format!("failed to transition thread episodic capsule `{capsule_id}`"))?;

    thread_episodic_capsules::Entity::find_by_id(capsule_id.to_owned())
        .one(db)
        .await
        .with_context(|| {
            format!("failed to reload transitioned thread episodic capsule `{capsule_id}`")
        })
}

pub async fn update_capsule_capacity_metadata<C: ConnectionTrait>(
    db: &C,
    capsule_id: &str,
    update: ThreadEpisodicCapsuleCapacityUpdate,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_capsules::Model>> {
    let mut query = thread_episodic_capsules::Entity::update_many()
        .col_expr(
            thread_episodic_capsules::Column::CapacityBytes,
            Expr::value(update.capacity_bytes),
        )
        .col_expr(
            thread_episodic_capsules::Column::SizeBytes,
            Expr::value(update.size_bytes),
        )
        .col_expr(
            thread_episodic_capsules::Column::UtilizationPercent,
            Expr::value(update.utilization_percent),
        )
        .col_expr(
            thread_episodic_capsules::Column::LastCapacityCheckAt,
            Expr::value(now),
        )
        .col_expr(
            thread_episodic_capsules::Column::NearCapacityAt,
            Expr::value(update.near_capacity_at),
        )
        .col_expr(
            thread_episodic_capsules::Column::CapacityExceededAt,
            Expr::value(update.capacity_exceeded_at),
        )
        .col_expr(
            thread_episodic_capsules::Column::LastError,
            Expr::value(update.last_error),
        )
        .col_expr(
            thread_episodic_capsules::Column::UpdatedAt,
            Expr::value(now),
        );
    if let Some(active_frame_count) = update.active_frame_count {
        query = query.col_expr(
            thread_episodic_capsules::Column::ActiveFrameCount,
            Expr::value(active_frame_count),
        );
    }
    query
        .filter(thread_episodic_capsules::Column::Id.eq(capsule_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to update thread episodic capsule capacity `{capsule_id}`")
        })?;

    thread_episodic_capsules::Entity::find_by_id(capsule_id.to_owned())
        .one(db)
        .await
        .with_context(|| {
            format!("failed to reload capacity-updated thread episodic capsule `{capsule_id}`")
        })
}

pub async fn update_capsule_metadata_json<C: ConnectionTrait>(
    db: &C,
    capsule_id: &str,
    metadata_json: String,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_capsules::Model>> {
    thread_episodic_capsules::Entity::update_many()
        .col_expr(
            thread_episodic_capsules::Column::MetadataJson,
            Expr::value(metadata_json),
        )
        .col_expr(
            thread_episodic_capsules::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(thread_episodic_capsules::Column::Id.eq(capsule_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to update thread episodic capsule metadata `{capsule_id}`")
        })?;

    thread_episodic_capsules::Entity::find_by_id(capsule_id.to_owned())
        .one(db)
        .await
        .with_context(|| {
            format!("failed to reload metadata-updated thread episodic capsule `{capsule_id}`")
        })
}

pub async fn find_item_by_id<C: ConnectionTrait>(
    db: &C,
    index_item_id: &str,
) -> Result<Option<thread_episodic_items::Model>> {
    thread_episodic_items::Entity::find_by_id(index_item_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to find thread episodic item `{index_item_id}`"))
}

pub async fn list_items_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_items::Model>> {
    thread_episodic_items::Entity::find()
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_items::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(thread_episodic_items::Column::TurnId)
        .order_by_asc(thread_episodic_items::Column::ItemId)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list thread episodic items for thread `{thread_id}`"))
}

pub async fn list_recallable_items_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_items::Model>> {
    thread_episodic_items::Entity::find()
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_items::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(thread_episodic_items::Column::Status.eq(item_status_to_db(
            crate::thread_episodic::ThreadEpisodicItemStatus::Active,
        )))
        .filter(
            thread_episodic_items::Column::Visibility.ne(item_visibility_to_db(
                crate::thread_episodic::ThreadEpisodicItemVisibility::InternalHidden,
            )),
        )
        .order_by_asc(thread_episodic_items::Column::TurnId)
        .order_by_asc(thread_episodic_items::Column::ItemId)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list recallable thread episodic items for thread `{thread_id}`")
        })
}

pub async fn find_index_job_by_id<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    thread_episodic_index_jobs::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to find thread episodic index job `{job_id}`"))
}

pub async fn find_index_job_by_index_item<C: ConnectionTrait>(
    db: &C,
    index_item_id: &str,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::IndexItemId.eq(index_item_id.to_owned()))
        .order_by_asc(thread_episodic_index_jobs::Column::CreatedAt)
        .one(db)
        .await
        .with_context(|| {
            format!("failed to find thread episodic index job for item `{index_item_id}`")
        })
}

pub async fn insert_index_job_if_absent<C: ConnectionTrait>(
    db: &C,
    job: NewThreadEpisodicIndexJobRecord,
    now: DateTimeWithTimeZone,
) -> Result<thread_episodic_index_jobs::Model> {
    if let Some(existing) = find_index_job_by_index_item(db, job.index_item_id.as_str()).await? {
        return Ok(existing);
    }

    let id = job
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    thread_episodic_index_jobs::Entity::insert(thread_episodic_index_jobs::ActiveModel {
        id: Set(id),
        workspace_id: Set(job.workspace_id),
        thread_id: Set(job.thread_id),
        index_item_id: Set(job.index_item_id.clone()),
        capsule_id: Set(job.capsule_id),
        capsule_ref: Set(job.capsule_ref),
        segment_index: Set(job.segment_index),
        frame_uri: Set(job.frame_uri),
        status: Set(index_job_status_to_db(job.status).to_owned()),
        graph_enrichment_state: Set(
            graph_enrichment_state_to_db(job.graph_enrichment_state).to_owned()
        ),
        attempt_count: Set(0),
        capacity_error_count: Set(0),
        last_attempt_latency_ms: Set(None),
        next_run_at: Set(job.next_run_at),
        last_error: Set(job.last_error),
        created_at: Set(now),
        updated_at: Set(now),
        completed_at: Set(None),
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert thread episodic index job for item `{}`",
            job.index_item_id
        )
    })?;

    find_index_job_by_index_item(db, job.index_item_id.as_str())
        .await?
        .context("inserted thread episodic index job missing")
}

pub async fn list_index_jobs_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_index_jobs::Model>> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_index_jobs::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(thread_episodic_index_jobs::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list thread episodic index jobs for thread `{thread_id}`")
        })
}

pub async fn find_thread_directory_entry<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<Option<thread_episodic_thread_directory::Model>> {
    thread_episodic_thread_directory::Entity::find()
        .filter(thread_episodic_thread_directory::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_thread_directory::Column::ThreadId.eq(thread_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to find thread episodic directory entry for thread `{thread_id}`")
        })
}

pub async fn upsert_thread_directory_entry<C: ConnectionTrait>(
    db: &C,
    record: NewThreadEpisodicThreadDirectoryRecord,
    now: DateTimeWithTimeZone,
) -> Result<thread_episodic_thread_directory::Model> {
    let id = record
        .id
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    let workspace_id = record.workspace_id;
    let thread_id = record.thread_id;
    thread_episodic_thread_directory::Entity::insert(
        thread_episodic_thread_directory::ActiveModel {
            id: Set(id),
            workspace_id: Set(workspace_id.clone()),
            thread_id: Set(thread_id.clone()),
            title: Set(record.title),
            summary_hash: Set(record.summary_hash),
            summary_ref: Set(record.summary_ref),
            thread_created_at: Set(record.thread_created_at),
            thread_updated_at: Set(record.thread_updated_at),
            last_indexed_at: Set(record.last_indexed_at),
            indexed_item_count: Set(record.indexed_item_count),
            task_affinity_json: Set(record.task_affinity_json),
            project_affinity_json: Set(record.project_affinity_json),
            visibility: Set(thread_directory_visibility_to_db(record.visibility).to_owned()),
            status: Set(thread_directory_status_to_db(record.status).to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
        },
    )
    .on_conflict(
        OnConflict::columns([
            thread_episodic_thread_directory::Column::WorkspaceId,
            thread_episodic_thread_directory::Column::ThreadId,
        ])
        .update_columns([
            thread_episodic_thread_directory::Column::Title,
            thread_episodic_thread_directory::Column::SummaryHash,
            thread_episodic_thread_directory::Column::SummaryRef,
            thread_episodic_thread_directory::Column::ThreadCreatedAt,
            thread_episodic_thread_directory::Column::ThreadUpdatedAt,
            thread_episodic_thread_directory::Column::LastIndexedAt,
            thread_episodic_thread_directory::Column::IndexedItemCount,
            thread_episodic_thread_directory::Column::TaskAffinityJson,
            thread_episodic_thread_directory::Column::ProjectAffinityJson,
            thread_episodic_thread_directory::Column::Visibility,
            thread_episodic_thread_directory::Column::Status,
            thread_episodic_thread_directory::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!("failed to upsert thread episodic directory entry for thread `{thread_id}`")
    })?;
    find_thread_directory_entry(db, workspace_id.as_str(), thread_id.as_str())
        .await?
        .with_context(|| {
            format!("thread episodic directory entry missing after upsert for thread `{thread_id}`")
        })
}

pub async fn list_selectable_thread_directory_entries<C: ConnectionTrait>(
    db: &C,
    selection: ThreadEpisodicThreadDirectorySelection,
) -> Result<Vec<thread_episodic_thread_directory::Model>> {
    let mut query =
        thread_episodic_thread_directory::Entity::find()
            .filter(
                thread_episodic_thread_directory::Column::WorkspaceId
                    .eq(selection.workspace_id.clone()),
            )
            .filter(thread_episodic_thread_directory::Column::Status.eq(
                thread_directory_status_to_db(ThreadEpisodicThreadDirectoryStatus::Active),
            ))
            .filter(thread_episodic_thread_directory::Column::Visibility.eq(
                thread_directory_visibility_to_db(ThreadEpisodicThreadDirectoryVisibility::Visible),
            ))
            .filter(thread_episodic_thread_directory::Column::IndexedItemCount.gt(0));
    if !selection.exclude_thread_ids.is_empty() {
        query = query.filter(
            thread_episodic_thread_directory::Column::ThreadId
                .is_not_in(selection.exclude_thread_ids),
        );
    }
    query
        .order_by_desc(thread_episodic_thread_directory::Column::LastIndexedAt)
        .order_by_desc(thread_episodic_thread_directory::Column::ThreadUpdatedAt)
        .limit(selection.limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list selectable thread episodic directory entries for workspace `{}`",
                selection.workspace_id
            )
        })
}

pub async fn list_thread_directory_entries_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_thread_directory::Model>> {
    thread_episodic_thread_directory::Entity::find()
        .filter(thread_episodic_thread_directory::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_desc(thread_episodic_thread_directory::Column::LastIndexedAt)
        .order_by_desc(thread_episodic_thread_directory::Column::ThreadUpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list thread episodic directory entries for workspace `{workspace_id}`"
            )
        })
}

/// Lists the exact authorized cross-thread recall candidates with the
/// allow-list applied before ordering and pagination.
///
/// The caller derives `allowed_thread_ids` from current control-plane ACL.
/// Keeping the predicate in SQL prevents inaccessible directory rows from
/// crowding accessible candidates out of the bounded recall page.
pub async fn list_thread_directory_entries_for_workspace_scoped<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    allowed_thread_ids: &[String],
    limit: u64,
) -> Result<Vec<thread_episodic_thread_directory::Model>> {
    if allowed_thread_ids.is_empty() {
        return Ok(Vec::new());
    }
    thread_episodic_thread_directory::Entity::find()
        .filter(thread_episodic_thread_directory::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread_episodic_thread_directory::Column::ThreadId.is_in(allowed_thread_ids.to_vec()),
        )
        .order_by_desc(thread_episodic_thread_directory::Column::LastIndexedAt)
        .order_by_desc(thread_episodic_thread_directory::Column::ThreadUpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list authorized thread episodic directory entries for workspace \
                 `{workspace_id}`"
            )
        })
}

pub async fn count_active_items_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<i64> {
    let count = thread_episodic_items::Entity::find()
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_items::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(
            thread_episodic_items::Column::Status
                .eq(item_status_to_db(ThreadEpisodicItemStatus::Active)),
        )
        .count(db)
        .await
        .with_context(|| {
            format!("failed to count active thread episodic items for thread `{thread_id}`")
        })?;
    Ok(count.min(i64::MAX as u64) as i64)
}

pub async fn list_failed_or_stale_index_jobs_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    stale_before: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<thread_episodic_index_jobs::Model>> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_index_jobs::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(
            Condition::any()
                .add(
                    thread_episodic_index_jobs::Column::Status
                        .eq(index_job_status_to_db(ThreadEpisodicIndexJobStatus::Failed)),
                )
                .add(
                    Condition::all()
                        .add(
                            thread_episodic_index_jobs::Column::Status.eq(index_job_status_to_db(
                                ThreadEpisodicIndexJobStatus::Running,
                            )),
                        )
                        .add(thread_episodic_index_jobs::Column::UpdatedAt.lte(stale_before)),
                ),
        )
        .order_by_asc(thread_episodic_index_jobs::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list failed or stale thread episodic index jobs for thread `{thread_id}`"
            )
        })
}

pub async fn list_due_index_jobs<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<thread_episodic_index_jobs::Model>> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::Status.is_in([
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Queued).to_owned(),
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Failed).to_owned(),
        ]))
        .filter(thread_episodic_index_jobs::Column::NextRunAt.lte(now))
        .order_by_asc(thread_episodic_index_jobs::Column::NextRunAt)
        .order_by_asc(thread_episodic_index_jobs::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list due thread episodic index jobs")
}

pub async fn list_due_index_jobs_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<thread_episodic_index_jobs::Model>> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_index_jobs::Column::Status.is_in([
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Queued).to_owned(),
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Failed).to_owned(),
        ]))
        .filter(thread_episodic_index_jobs::Column::NextRunAt.lte(now))
        .order_by_asc(thread_episodic_index_jobs::Column::NextRunAt)
        .order_by_asc(thread_episodic_index_jobs::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list due thread episodic index jobs for workspace `{workspace_id}`")
        })
}

pub async fn find_next_scheduled_index_job_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_index_jobs::Column::Status.is_in([
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Queued).to_owned(),
            index_job_status_to_db(ThreadEpisodicIndexJobStatus::Failed).to_owned(),
        ]))
        .order_by_asc(thread_episodic_index_jobs::Column::NextRunAt)
        .order_by_asc(thread_episodic_index_jobs::Column::CreatedAt)
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to find next scheduled thread episodic index job for workspace `{workspace_id}`"
            )
        })
}

pub async fn list_canceled_index_jobs_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_index_jobs::Model>> {
    thread_episodic_index_jobs::Entity::find()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread_episodic_index_jobs::Column::Status.eq(index_job_status_to_db(
                ThreadEpisodicIndexJobStatus::Canceled,
            )),
        )
        .order_by_asc(thread_episodic_index_jobs::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list canceled thread episodic index jobs for workspace `{workspace_id}`"
            )
        })
}

pub async fn mark_index_job_running<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    let Some(row) = find_index_job_by_id(db, job_id).await? else {
        return Ok(None);
    };
    let status = index_job_status_from_db(row.status.as_str())?;
    if !matches!(
        status,
        ThreadEpisodicIndexJobStatus::Queued | ThreadEpisodicIndexJobStatus::Failed
    ) {
        return Ok(None);
    }

    let mut active = row.into_active_model();
    active.status = Set(index_job_status_to_db(ThreadEpisodicIndexJobStatus::Running).to_owned());
    active.attempt_count = Set(active_attempt_count(&active).saturating_add(1));
    active.last_attempt_latency_ms = Set(None);
    active.updated_at = Set(now);
    let row = active
        .update(db)
        .await
        .with_context(|| format!("failed to mark thread episodic index job `{job_id}` running"))?;
    Ok(Some(row))
}

pub async fn mark_index_job_completed<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    update: ThreadEpisodicIndexJobCompletionUpdate,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    let Some(row) = find_index_job_by_id(db, job_id).await? else {
        return Ok(None);
    };
    let mut active = row.into_active_model();
    active.capsule_id = Set(Some(update.capsule_id));
    active.capsule_ref = Set(Some(update.capsule_ref));
    active.segment_index = Set(Some(update.segment_index));
    active.frame_uri = Set(Some(update.frame_uri));
    active.status = Set(index_job_status_to_db(ThreadEpisodicIndexJobStatus::Completed).to_owned());
    active.last_error = Set(None);
    active.last_attempt_latency_ms = Set(update.last_attempt_latency_ms);
    active.updated_at = Set(now);
    active.completed_at = Set(Some(now));
    let row = active.update(db).await.with_context(|| {
        format!("failed to mark thread episodic index job `{job_id}` completed")
    })?;
    Ok(Some(row))
}

pub async fn mark_index_job_failed<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    update: ThreadEpisodicIndexJobFailureUpdate,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    let Some(row) = find_index_job_by_id(db, job_id).await? else {
        return Ok(None);
    };
    let mut active = row.into_active_model();
    active.status = Set(if update.retryable {
        index_job_status_to_db(ThreadEpisodicIndexJobStatus::Failed).to_owned()
    } else {
        index_job_status_to_db(ThreadEpisodicIndexJobStatus::Canceled).to_owned()
    });
    active.next_run_at = Set(update
        .next_run_at_unix
        .map(crate::util::unix_to_datetime)
        .unwrap_or(now));
    active.last_error = Set(update.last_error);
    active.last_attempt_latency_ms = Set(update.last_attempt_latency_ms);
    if update.capacity_error {
        active.capacity_error_count = Set(active_capacity_error_count(&active).saturating_add(1));
    }
    active.updated_at = Set(now);
    active.completed_at = Set(if update.retryable { None } else { Some(now) });
    let row = active
        .update(db)
        .await
        .with_context(|| format!("failed to mark thread episodic index job `{job_id}` failed"))?;
    Ok(Some(row))
}

pub async fn retry_failed_or_stale_index_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    stale_before: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    let Some(row) = find_index_job_by_id(db, job_id).await? else {
        return Ok(None);
    };
    let status = index_job_status_from_db(row.status.as_str())?;
    let retryable = matches!(status, ThreadEpisodicIndexJobStatus::Failed)
        || (matches!(status, ThreadEpisodicIndexJobStatus::Running)
            && row.updated_at <= stale_before);
    if matches!(status, ThreadEpisodicIndexJobStatus::Queued) {
        return Ok(Some(row));
    }
    if !retryable {
        return Ok(Some(row));
    }

    let mut active = row.into_active_model();
    active.status = Set(index_job_status_to_db(ThreadEpisodicIndexJobStatus::Queued).to_owned());
    active.next_run_at = Set(now);
    active.last_error = Set(None);
    active.last_attempt_latency_ms = Set(None);
    active.updated_at = Set(now);
    active.completed_at = Set(None);
    let row = active
        .update(db)
        .await
        .with_context(|| format!("failed to retry thread episodic index job `{job_id}`"))?;
    Ok(Some(row))
}

pub async fn requeue_canceled_index_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    let Some(row) = find_index_job_by_id(db, job_id).await? else {
        return Ok(None);
    };
    if index_job_status_from_db(row.status.as_str())? != ThreadEpisodicIndexJobStatus::Canceled {
        return Ok(None);
    }

    let mut active = row.into_active_model();
    active.status = Set(index_job_status_to_db(ThreadEpisodicIndexJobStatus::Failed).to_owned());
    active.next_run_at = Set(now);
    active.updated_at = Set(now);
    active.completed_at = Set(None);
    let row = active.update(db).await.with_context(|| {
        format!("failed to requeue canceled thread episodic index job `{job_id}`")
    })?;
    Ok(Some(row))
}

pub async fn requeue_running_index_jobs_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    interrupted_before: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let result = thread_episodic_index_jobs::Entity::update_many()
        .filter(thread_episodic_index_jobs::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread_episodic_index_jobs::Column::Status
                .eq(index_job_status_to_db(ThreadEpisodicIndexJobStatus::Running)),
        )
        .filter(thread_episodic_index_jobs::Column::UpdatedAt.lte(interrupted_before))
        .col_expr(
            thread_episodic_index_jobs::Column::Status,
            Expr::value(index_job_status_to_db(ThreadEpisodicIndexJobStatus::Queued).to_owned()),
        )
        .col_expr(
            thread_episodic_index_jobs::Column::NextRunAt,
            Expr::value(now),
        )
        .col_expr(
            thread_episodic_index_jobs::Column::UpdatedAt,
            Expr::value(now),
        )
        .col_expr(
            thread_episodic_index_jobs::Column::CompletedAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to requeue interrupted thread episodic index jobs for workspace `{workspace_id}`"
            )
        })?;
    Ok(result.rows_affected)
}

pub async fn mark_index_job_canceled<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_index_jobs::Model>> {
    let Some(row) = find_index_job_by_id(db, job_id).await? else {
        return Ok(None);
    };
    let mut active = row.into_active_model();
    active.status = Set(index_job_status_to_db(ThreadEpisodicIndexJobStatus::Canceled).to_owned());
    active.last_error = Set(last_error);
    active.updated_at = Set(now);
    active.completed_at = Set(Some(now));
    let row = active
        .update(db)
        .await
        .with_context(|| format!("failed to mark thread episodic index job `{job_id}` canceled"))?;
    Ok(Some(row))
}

pub async fn mark_item_indexed<C: ConnectionTrait>(
    db: &C,
    index_item_id: &str,
    update: ThreadEpisodicItemIndexedUpdate,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_items::Model>> {
    let Some(row) = find_item_by_id(db, index_item_id).await? else {
        return Ok(None);
    };
    let mut active = row.into_active_model();
    active.status = Set(item_status_to_db(ThreadEpisodicItemStatus::Active).to_owned());
    active.capsule_id = Set(Some(update.capsule_id));
    active.capsule_ref = Set(Some(update.capsule_ref));
    active.segment_index = Set(Some(update.segment_index));
    active.frame_id = Set(Some(update.frame_id));
    active.frame_uri = Set(Some(update.frame_uri));
    active.embedding_artifact_id = Set(update.embedding_artifact_id);
    active.indexed_at = Set(Some(now));
    active.updated_at = Set(now);
    let row = active.update(db).await.with_context(|| {
        format!("failed to mark thread episodic item `{index_item_id}` indexed")
    })?;
    Ok(Some(row))
}

pub async fn mark_item_failed<C: ConnectionTrait>(
    db: &C,
    index_item_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_items::Model>> {
    let Some(row) = find_item_by_id(db, index_item_id).await? else {
        return Ok(None);
    };
    let mut active = row.into_active_model();
    active.status = Set(item_status_to_db(ThreadEpisodicItemStatus::Failed).to_owned());
    active.updated_at = Set(now);
    let row = active
        .update(db)
        .await
        .with_context(|| format!("failed to mark thread episodic item `{index_item_id}` failed"))?;
    Ok(Some(row))
}

pub async fn mark_items_deleted_by_source_item<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Vec<thread_episodic_items::Model>> {
    let rows = thread_episodic_items::Entity::find()
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_items::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(thread_episodic_items::Column::TurnId.eq(turn_id.to_owned()))
        .filter(thread_episodic_items::Column::ItemId.eq(item_id.to_owned()))
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list thread episodic items for deleted item `{workspace_id}/{thread_id}/{turn_id}/{item_id}`"
            )
        })?;
    mark_item_rows_deleted(db, rows, now).await
}

pub async fn mark_items_deleted_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Vec<thread_episodic_items::Model>> {
    let rows = thread_episodic_items::Entity::find()
        .filter(thread_episodic_items::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_items::Column::ThreadId.eq(thread_id.to_owned()))
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list thread episodic items for deleted thread `{workspace_id}/{thread_id}`"
            )
        })?;
    mark_item_rows_deleted(db, rows, now).await
}

async fn mark_item_rows_deleted<C: ConnectionTrait>(
    db: &C,
    rows: Vec<thread_episodic_items::Model>,
    now: DateTimeWithTimeZone,
) -> Result<Vec<thread_episodic_items::Model>> {
    let mut updated = Vec::with_capacity(rows.len());
    for row in rows {
        if row.status == item_status_to_db(ThreadEpisodicItemStatus::Deleted) {
            updated.push(row);
            continue;
        }
        let index_item_id = row.id.clone();
        let mut active = row.into_active_model();
        active.status = Set(item_status_to_db(ThreadEpisodicItemStatus::Deleted).to_owned());
        active.deleted_at = Set(Some(now));
        active.updated_at = Set(now);
        let row = active.update(db).await.with_context(|| {
            format!("failed to tombstone thread episodic item `{index_item_id}`")
        })?;
        updated.push(row);
    }
    Ok(updated)
}

fn active_attempt_count(active: &thread_episodic_index_jobs::ActiveModel) -> i64 {
    match &active.attempt_count {
        sea_orm::ActiveValue::Set(value) | sea_orm::ActiveValue::Unchanged(value) => *value,
        sea_orm::ActiveValue::NotSet => 0,
    }
}

fn active_capacity_error_count(active: &thread_episodic_index_jobs::ActiveModel) -> i64 {
    match &active.capacity_error_count {
        sea_orm::ActiveValue::Set(value) | sea_orm::ActiveValue::Unchanged(value) => *value,
        sea_orm::ActiveValue::NotSet => 0,
    }
}

pub async fn find_exclusion_by_index_item<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    index_item_id: &str,
) -> Result<Option<thread_episodic_exclusions::Model>> {
    thread_episodic_exclusions::Entity::find()
        .filter(thread_episodic_exclusions::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_exclusions::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(thread_episodic_exclusions::Column::IndexItemId.eq(index_item_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to find thread episodic exclusion for item `{index_item_id}`")
        })
}

pub async fn insert_exclusion_if_absent<C: ConnectionTrait>(
    db: &C,
    exclusion: NewThreadEpisodicExclusionRecord,
    now: DateTimeWithTimeZone,
) -> Result<thread_episodic_exclusions::Model> {
    if let Some(existing) = find_exclusion_by_index_item(
        db,
        exclusion.workspace_id.as_str(),
        exclusion.thread_id.as_str(),
        exclusion.index_item_id.as_str(),
    )
    .await?
    {
        return Ok(existing);
    }

    let id = exclusion
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    thread_episodic_exclusions::Entity::insert(thread_episodic_exclusions::ActiveModel {
        id: Set(id),
        workspace_id: Set(exclusion.workspace_id.clone()),
        thread_id: Set(exclusion.thread_id.clone()),
        index_item_id: Set(exclusion.index_item_id.clone()),
        reason: Set(exclusion_reason_to_db(exclusion.reason).to_owned()),
        created_by: Set(exclusion.created_by),
        created_at: Set(now),
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert thread episodic exclusion for item `{}`",
            exclusion.index_item_id
        )
    })?;

    find_exclusion_by_index_item(
        db,
        exclusion.workspace_id.as_str(),
        exclusion.thread_id.as_str(),
        exclusion.index_item_id.as_str(),
    )
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "inserted thread episodic exclusion for item `{}` was not found",
            exclusion.index_item_id
        )
    })
}

pub async fn list_exclusions_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_exclusions::Model>> {
    thread_episodic_exclusions::Entity::find()
        .filter(thread_episodic_exclusions::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_exclusions::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(thread_episodic_exclusions::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list thread episodic exclusions for thread `{thread_id}`")
        })
}

pub async fn list_recall_events_for_thread<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<thread_episodic_recall_events::Model>> {
    thread_episodic_recall_events::Entity::find()
        .filter(thread_episodic_recall_events::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_episodic_recall_events::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(thread_episodic_recall_events::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list thread episodic recall events for thread `{thread_id}`")
        })
}

pub async fn insert_recall_event<C: ConnectionTrait>(
    db: &C,
    event: NewThreadEpisodicRecallEventRecord,
    now: DateTimeWithTimeZone,
) -> Result<thread_episodic_recall_events::Model> {
    let id = event
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    let thread_id_for_error = event.thread_id.clone();
    thread_episodic_recall_events::Entity::insert(thread_episodic_recall_events::ActiveModel {
        id: Set(id.clone()),
        workspace_id: Set(event.workspace_id),
        thread_id: Set(event.thread_id),
        turn_id: Set(event.turn_id),
        query_hash: Set(event.query_hash),
        search_profile_json: Set(event.search_profile_json),
        search_mode: Set(event.search_mode),
        adaptive_strategy: Set(event.adaptive_strategy),
        cutoff_json: Set(event.cutoff_json),
        candidate_count: Set(event.candidate_count),
        returned_count: Set(event.returned_count),
        latency_ms: Set(event.latency_ms),
        fallback_used: Set(event.fallback_used),
        error: Set(event.error),
        created_at: Set(now),
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert thread episodic recall event for thread `{}`",
            thread_id_for_error
        )
    })?;

    thread_episodic_recall_events::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to read inserted thread episodic recall event")?
        .context("inserted thread episodic recall event missing")
}
