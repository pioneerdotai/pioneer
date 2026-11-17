//! Page-scoped HTML publication reads. Never load generic artifact lists or
//! current-version summaries before resolving immutable publication bindings.
use super::*;
use sea_orm::{Condition, FromQueryResult};

const SOURCE_BATCH: usize = 64;
const ROW_BATCH: u64 = 256;

#[derive(FromQueryResult)]
struct PreviewRow {
    id: String,
    artifact_id: String,
    artifact_version_id: String,
    projection_kind: String,
    blob_id: Option<String>,
    updated_at: DateTimeWithTimeZone,
}

impl ArtifactRepository {
    pub async fn html_publication_summaries<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace: &str,
        sources: &[(String, String)],
    ) -> ArtifactCrudResult<HashMap<(String, String), Vec<ArtifactSummary>>> {
        let sources = sources
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut result = HashMap::new();
        for sources in sources.chunks(SOURCE_BATCH) {
            let source_set = sources.iter().cloned().collect::<HashSet<_>>();
            let mut source_filter = Condition::any();
            for (thread, turn) in sources {
                source_filter = source_filter.add(
                    Condition::all()
                        .add(artifact_binding::Column::ThreadId.eq(thread.clone()))
                        .add(artifact_binding::Column::TurnId.eq(turn.clone())),
                );
            }
            // Start with exact binding identities so the artifact lookup uses
            // their primary keys rather than scanning workspace summaries.
            let eligible = artifact_binding::Entity::find()
                .select_only()
                .column(artifact_binding::Column::ArtifactId)
                .filter(artifact_binding::Column::WorkspaceId.eq(workspace))
                .filter(source_filter.clone())
                .into_query();
            let mut after = String::new();
            let mut artifacts = HashMap::new();
            let mut owners = HashMap::new();
            let mut counts = HashMap::<(String, String), usize>::new();
            loop {
                let rows = artifact::Entity::find()
                    .filter(artifact::Column::WorkspaceId.eq(workspace))
                    .filter(artifact::Column::CreatedByActorId.eq("html_render"))
                    .filter(
                        artifact::Column::PrimaryThreadId.is_in(
                            sources
                                .iter()
                                .map(|(thread, _)| thread.clone())
                                .collect::<HashSet<_>>(),
                        ),
                    )
                    .filter(artifact::Column::Id.in_subquery(eligible.clone()))
                    .filter(artifact::Column::Id.gt(after.clone()))
                    .filter(sea_orm::sea_query::ExprTrait::not(
                        super::super::html_preview_retention::temporary_preview_predicate(
                            "artifact",
                        ),
                    ))
                    .order_by_asc(artifact::Column::Id)
                    .limit(ROW_BATCH)
                    .all(db)
                    .await
                    .map_err(database_error)?;
                let full = rows.len() == ROW_BATCH as usize;
                if let Some(row) = rows.last() {
                    after = row.id.clone();
                }
                let mut candidates = Vec::new();
                for row in rows {
                    // Keep JSON ownership checks backend-independent. A copy
                    // bound in another Turn is never a second publication.
                    let metadata = metadata_from_db(&row.metadata_json)?;
                    let Some(marker) = metadata.get("htmlTool") else {
                        continue;
                    };
                    let Some(turn) = marker.get("turnId").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let Some(thread) = row.primary_thread_id.as_ref() else {
                        continue;
                    };
                    let key = (thread.clone(), turn.to_owned());
                    if !source_set.contains(&key) {
                        continue;
                    }
                    let call = marker
                        .get("callId")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned);
                    candidates.push((row, key, call));
                }
                // A page can contain several Turns of the same thread. Match
                // the marker's exact Turn, not just any requested binding.
                for candidates in candidates.chunks(SOURCE_BATCH) {
                    let mut exact = Condition::any();
                    for (row, (thread, turn), _) in candidates {
                        exact = exact.add(
                            Condition::all()
                                .add(artifact_binding::Column::ArtifactId.eq(row.id.clone()))
                                .add(artifact_binding::Column::ThreadId.eq(thread.clone()))
                                .add(artifact_binding::Column::TurnId.eq(turn.clone())),
                        );
                    }
                    let bound = artifact_binding::Entity::find()
                        .select_only()
                        .column(artifact_binding::Column::ArtifactId)
                        .distinct()
                        .filter(artifact_binding::Column::WorkspaceId.eq(workspace))
                        .filter(exact)
                        .into_tuple::<String>()
                        .all(db)
                        .await
                        .map_err(database_error)?
                        .into_iter()
                        .collect::<HashSet<_>>();
                    for (row, key, call) in candidates {
                        if !bound.contains(&row.id) {
                            continue;
                        }
                        let count = counts.entry(key.clone()).or_default();
                        *count += 1;
                        if *count > MAX_LIST_LIMIT as usize {
                            return Err(ArtifactCrudError::InvalidRequest {
                                message: "turn artifact publication page exceeds timeline delivery budget".into(),
                            });
                        }
                        owners.insert(row.id.clone(), (key.clone(), call.clone()));
                        artifacts.insert(row.id.clone(), row.clone());
                    }
                }
                if !full {
                    break;
                }
            }
            let ids = artifacts.keys().cloned().collect::<Vec<_>>();
            for ids in ids.chunks(SOURCE_BATCH) {
                // Keep only the newest version choice and the original minimum
                // item position for that version/call, not every echo/binding.
                let mut bindings = HashMap::<String, Vec<artifact_binding::Model>>::new();
                let mut cursor: Option<(DateTimeWithTimeZone, String)> = None;
                loop {
                    let mut query = artifact_binding::Entity::find()
                        .filter(artifact_binding::Column::WorkspaceId.eq(workspace))
                        .filter(artifact_binding::Column::ArtifactId.is_in(ids.to_vec()))
                        .filter(source_filter.clone())
                        .filter(artifact_binding::Column::BindingKind.eq("tool_output"))
                        .filter(artifact_binding::Column::Direction.eq("output"))
                        .filter(artifact_binding::Column::Role.eq("tool"));
                    if let Some((at, id)) = &cursor {
                        query = query.filter(
                            Condition::any()
                                .add(artifact_binding::Column::CreatedAt.lt(*at))
                                .add(
                                    Condition::all()
                                        .add(artifact_binding::Column::CreatedAt.eq(*at))
                                        .add(artifact_binding::Column::Id.lt(id.clone())),
                                ),
                        );
                    }
                    let rows = query
                        .order_by_desc(artifact_binding::Column::CreatedAt)
                        .order_by_desc(artifact_binding::Column::Id)
                        .limit(ROW_BATCH)
                        .all(db)
                        .await
                        .map_err(database_error)?;
                    let full = rows.len() == ROW_BATCH as usize;
                    for row in rows {
                        cursor = Some((row.created_at, row.id.clone()));
                        let ((thread, turn), call) = &owners[&row.artifact_id];
                        if row.thread_id.as_deref() != Some(thread.as_str())
                            || row.turn_id.as_deref() != Some(turn.as_str())
                        {
                            continue;
                        }
                        let selected = bindings.entry(row.artifact_id.clone()).or_default();
                        if selected.is_empty() {
                            selected.push(row.clone());
                        }
                        if row.artifact_version_id == selected[0].artifact_version_id
                            && row.tool_call_id.as_ref() == call.as_ref()
                        {
                            if selected.len() == 1 {
                                selected.push(row);
                            } else if row.item_index.unwrap_or(i64::MAX)
                                < selected[1].item_index.unwrap_or(i64::MAX)
                            {
                                selected[1] = row;
                            }
                        }
                    }
                    if !full {
                        break;
                    }
                }
                let mut exact = Condition::any();
                let mut version_ids = Vec::new();
                for id in ids {
                    let Some(binding) = bindings.get(id).and_then(|v| v.first()) else {
                        continue;
                    };
                    let version = binding
                        .artifact_version_id
                        .as_ref()
                        .or(artifacts[id].current_version_id.as_ref())
                        .ok_or_else(|| missing("publication has no version"))?;
                    version_ids.push(version.clone());
                    exact = exact.add(
                        Condition::all()
                            .add(artifact_version::Column::ArtifactId.eq(id.clone()))
                            .add(artifact_version::Column::Id.eq(version.clone())),
                    );
                }
                if version_ids.is_empty() {
                    continue;
                }
                let versions = artifact_version::Entity::find()
                    .filter(artifact_version::Column::WorkspaceId.eq(workspace))
                    .filter(exact)
                    .all(db)
                    .await
                    .map_err(database_error)?
                    .into_iter()
                    .map(|v| (v.artifact_id.clone(), v))
                    .collect::<HashMap<_, _>>();
                // Projection text/metadata are intentionally not materialized.
                let mut previews = HashMap::new();
                let mut cursor: Option<(DateTimeWithTimeZone, String)> = None;
                loop {
                    let mut query = artifact_projection::Entity::find()
                        .select_only()
                        .columns([
                            artifact_projection::Column::Id,
                            artifact_projection::Column::ArtifactId,
                            artifact_projection::Column::ArtifactVersionId,
                            artifact_projection::Column::ProjectionKind,
                            artifact_projection::Column::BlobId,
                            artifact_projection::Column::UpdatedAt,
                        ])
                        .filter(artifact_projection::Column::WorkspaceId.eq(workspace))
                        .filter(artifact_projection::Column::ArtifactId.is_in(ids.to_vec()))
                        .filter(
                            artifact_projection::Column::ArtifactVersionId
                                .is_in(version_ids.clone()),
                        )
                        .filter(artifact_projection::Column::Status.eq("ready"));
                    if let Some((at, id)) = &cursor {
                        query = query.filter(
                            Condition::any()
                                .add(artifact_projection::Column::UpdatedAt.lt(*at))
                                .add(
                                    Condition::all()
                                        .add(artifact_projection::Column::UpdatedAt.eq(*at))
                                        .add(artifact_projection::Column::Id.lt(id.clone())),
                                ),
                        );
                    }
                    let rows = query
                        .order_by_desc(artifact_projection::Column::UpdatedAt)
                        .order_by_desc(artifact_projection::Column::Id)
                        .limit(ROW_BATCH)
                        .into_model::<PreviewRow>()
                        .all(db)
                        .await
                        .map_err(database_error)?;
                    let full = rows.len() == ROW_BATCH as usize;
                    for row in rows {
                        cursor = Some((row.updated_at, row.id.clone()));
                        if versions
                            .get(&row.artifact_id)
                            .is_some_and(|v| v.id == row.artifact_version_id)
                        {
                            previews.entry(row.artifact_id.clone()).or_insert(row);
                        }
                    }
                    if !full {
                        break;
                    }
                }
                let blob_ids = versions
                    .values()
                    .map(|v| v.blob_id.clone())
                    .chain(previews.values().filter_map(|v| v.blob_id.clone()))
                    .collect::<HashSet<_>>();
                let blobs = artifact_blob::Entity::find()
                    .filter(artifact_blob::Column::WorkspaceId.eq(workspace))
                    .filter(artifact_blob::Column::Id.is_in(blob_ids))
                    .all(db)
                    .await
                    .map_err(database_error)?
                    .into_iter()
                    .map(|v| (v.id.clone(), v))
                    .collect::<HashMap<_, _>>();
                for id in ids {
                    let Some(selected) = bindings.remove(id) else {
                        continue;
                    };
                    let version = versions
                        .get(id)
                        .ok_or_else(|| missing("publication version"))?;
                    let blob = blobs
                        .get(&version.blob_id)
                        .ok_or_else(|| missing("publication blob"))?;
                    let preview = previews.remove(id).map(|p| {
                        let preview_blob = p.blob_id.as_ref().and_then(|id| blobs.get(id));
                        ArtifactPreviewRef {
                            projection_kind: projection_kind_from_db(&p.projection_kind),
                            status: ArtifactProjectionStatus::Ready,
                            artifact_id: id.clone(),
                            version_id: version.id.clone(),
                            blob_id: p.blob_id.clone(),
                            mime_type: preview_blob
                                .and_then(|b| b.mime_type.clone())
                                .or(blob.mime_type.clone()),
                            size_bytes: preview_blob
                                .map(|b| b.size_bytes.max(0) as u64)
                                .or(u64::try_from(blob.size_bytes).ok()),
                            sha256: preview_blob.map(|b| b.sha256.clone()),
                        }
                    });
                    let artifact = artifacts.remove(id).expect("selected publication artifact");
                    let (key, _) = owners.remove(id).expect("selected publication owner");
                    let summary = summary_from_models(
                        artifact,
                        version.clone(),
                        blob.clone(),
                        selected,
                        preview,
                    )?;
                    result.entry(key).or_insert_with(Vec::new).push(summary);
                }
            }
        }
        Ok(result)
    }
}

fn database_error(source: DbErr) -> ArtifactCrudError {
    ArtifactCrudError::Database {
        message: "failed to read HTML publication batch".into(),
        source,
    }
}
fn missing(message: &str) -> ArtifactCrudError {
    ArtifactCrudError::NotFound {
        message: message.to_owned(),
    }
}
