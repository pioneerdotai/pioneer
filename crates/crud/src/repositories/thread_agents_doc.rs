use std::collections::HashSet;
use std::fmt;

use chrono::Utc;
use pioneer_entity::{
    thread_agents_doc, thread_agents_doc_revision, thread_folder, thread_placement,
};
use pioneer_protocol::generate_id;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DbErr, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, Set, entity::prelude::DateTimeWithTimeZone,
};
use sha2::{Digest, Sha256};

pub type ThreadAgentsDocResult<T> = Result<T, ThreadAgentsDocError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadAgentsDocStatus {
    Draft,
    Active,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadAgentsDocSaveReason {
    Autosave,
    Manual,
    Archive,
    Restore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocRecord {
    pub id: String,
    pub workspace_id: String,
    pub folder_id: Option<String>,
    pub status: ThreadAgentsDocStatus,
    pub title: String,
    pub content: String,
    pub content_sha256: String,
    pub version: i64,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
    pub archived_at_unix: Option<i64>,
    pub created_by: Option<String>,
    pub updated_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocRevisionRecord {
    pub id: String,
    pub doc_id: String,
    pub version: i64,
    pub content_sha256: String,
    pub content: String,
    pub saved_at_unix: i64,
    pub saved_by: Option<String>,
    pub save_reason: ThreadAgentsDocSaveReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocSummaryRecord {
    pub id: String,
    pub workspace_id: String,
    pub folder_id: Option<String>,
    pub status: ThreadAgentsDocStatus,
    pub content_sha256: String,
    pub version: i64,
    pub char_count: usize,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocScope {
    pub workspace_id: String,
    pub folder_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedThreadAgentsDocRecord {
    pub doc: ThreadAgentsDocRecord,
    pub resolved_for_workspace_id: String,
    pub resolved_for_folder_id: Option<String>,
    pub source_folder_id: Option<String>,
    pub source_path: Vec<String>,
    pub inherited: bool,
    pub resolved_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocScopeContext {
    pub explicit: Option<ThreadAgentsDocRecord>,
    pub effective: Option<ResolvedThreadAgentsDocRecord>,
}

#[derive(Debug)]
pub enum ThreadAgentsDocError {
    NotFound { message: String },
    WorkspaceMismatch { message: String },
    VersionConflict { expected: i64, actual: i64 },
    Database { message: String, source: DbErr },
    InvalidData { message: String },
}

impl fmt::Display for ThreadAgentsDocError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { message } => {
                write!(formatter, "thread agents doc not found: {message}")
            }
            Self::WorkspaceMismatch { message } => {
                write!(formatter, "thread agents doc workspace mismatch: {message}")
            }
            Self::VersionConflict { expected, actual } => {
                write!(
                    formatter,
                    "thread agents doc version conflict: expected {expected}, actual {actual}"
                )
            }
            Self::Database { message, source } => write!(formatter, "{message}: {source}"),
            Self::InvalidData { message } => {
                write!(formatter, "invalid thread agents doc data: {message}")
            }
        }
    }
}

impl std::error::Error for ThreadAgentsDocError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ThreadAgentsDocRepository;

impl ThreadAgentsDocRepository {
    pub(crate) fn new() -> Self {
        Self
    }

    pub async fn find_explicit<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> ThreadAgentsDocResult<Option<ThreadAgentsDocRecord>> {
        find_explicit_model(db, workspace_id, folder_id)
            .await
            .map(|model| model.map(record_from_model))
    }

    pub async fn create_draft<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        folder_id: Option<&str>,
        now: DateTimeWithTimeZone,
        actor_id: Option<&str>,
    ) -> ThreadAgentsDocResult<ThreadAgentsDocRecord> {
        if let Some(existing) = self.find_explicit(db, workspace_id, folder_id).await? {
            return Ok(existing);
        }

        insert_draft_model(db, workspace_id, folder_id, now, actor_id)
            .await
            .map(record_from_model)
    }

    pub async fn save_content<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        folder_id: Option<&str>,
        content: &str,
        expected_version: Option<i64>,
        now: DateTimeWithTimeZone,
        actor_id: Option<&str>,
        save_reason: ThreadAgentsDocSaveReason,
    ) -> ThreadAgentsDocResult<ThreadAgentsDocRecord> {
        let normalized_content = normalize_content(content);
        let next_status = if normalized_content.trim().is_empty() {
            ThreadAgentsDocStatus::Draft
        } else {
            ThreadAgentsDocStatus::Active
        };
        let next_hash = sha256_hex(normalized_content.as_bytes());

        let model = match find_explicit_model(db, workspace_id, folder_id).await? {
            Some(model) => model,
            None => insert_draft_model(db, workspace_id, folder_id, now, actor_id).await?,
        };

        ensure_version(&model, expected_version)?;

        if model.content_sha256 == next_hash
            && status_from_db(model.status.as_str())? == next_status
        {
            return Ok(record_from_model(model));
        }

        let next_version = model.version.saturating_add(1);
        let doc_id = model.id.clone();
        let mut active = model.into_active_model();
        active.status = Set(status_to_db(next_status).to_owned());
        active.content = Set(normalized_content.clone());
        active.content_sha256 = Set(next_hash.clone());
        active.version = Set(next_version);
        active.updated_at = Set(now);
        active.archived_at = Set(None);
        active.updated_by = Set(actor_id.map(str::to_owned));

        let saved = active
            .update(db)
            .await
            .map_err(|source| ThreadAgentsDocError::Database {
                message: "failed to update thread agents doc content".to_owned(),
                source,
            })?;

        insert_revision(
            db,
            doc_id.as_str(),
            next_version,
            next_hash.as_str(),
            normalized_content.as_str(),
            now,
            actor_id,
            save_reason,
        )
        .await?;

        Ok(record_from_model(saved))
    }

    pub async fn archive<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        folder_id: Option<&str>,
        expected_version: Option<i64>,
        now: DateTimeWithTimeZone,
        actor_id: Option<&str>,
    ) -> ThreadAgentsDocResult<Option<ThreadAgentsDocRecord>> {
        let Some(model) = find_explicit_model(db, workspace_id, folder_id).await? else {
            return Ok(None);
        };

        ensure_version(&model, expected_version)?;

        let next_version = model.version.saturating_add(1);
        let doc_id = model.id.clone();
        let content = model.content.clone();
        let content_sha256 = model.content_sha256.clone();

        let mut active = model.into_active_model();
        active.status = Set(status_to_db(ThreadAgentsDocStatus::Archived).to_owned());
        active.version = Set(next_version);
        active.updated_at = Set(now);
        active.archived_at = Set(Some(now));
        active.updated_by = Set(actor_id.map(str::to_owned));

        let saved = active
            .update(db)
            .await
            .map_err(|source| ThreadAgentsDocError::Database {
                message: "failed to archive thread agents doc".to_owned(),
                source,
            })?;

        insert_revision(
            db,
            doc_id.as_str(),
            next_version,
            content_sha256.as_str(),
            content.as_str(),
            now,
            actor_id,
            ThreadAgentsDocSaveReason::Archive,
        )
        .await?;

        Ok(Some(record_from_model(saved)))
    }

    pub async fn list_revisions<C: ConnectionTrait>(
        &self,
        db: &C,
        doc_id: &str,
    ) -> ThreadAgentsDocResult<Vec<ThreadAgentsDocRevisionRecord>> {
        thread_agents_doc_revision::Entity::find()
            .filter(thread_agents_doc_revision::Column::DocId.eq(doc_id.to_owned()))
            .order_by_asc(thread_agents_doc_revision::Column::Version)
            .all(db)
            .await
            .map_err(|source| ThreadAgentsDocError::Database {
                message: "failed to list thread agents doc revisions".to_owned(),
                source,
            })?
            .into_iter()
            .map(revision_record_from_model)
            .collect()
    }

    pub async fn list_summaries<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
    ) -> ThreadAgentsDocResult<Vec<ThreadAgentsDocSummaryRecord>> {
        thread_agents_doc::Entity::find()
            .filter(thread_agents_doc::Column::WorkspaceId.eq(workspace_id))
            .filter(
                thread_agents_doc::Column::Status.ne(status_to_db(ThreadAgentsDocStatus::Archived)),
            )
            .order_by_asc(thread_agents_doc::Column::FolderId)
            .order_by_asc(thread_agents_doc::Column::UpdatedAt)
            .all(db)
            .await
            .map_err(|source| ThreadAgentsDocError::Database {
                message: "failed to list thread agents doc summaries".to_owned(),
                source,
            })?
            .into_iter()
            .map(summary_record_from_model)
            .collect()
    }

    pub async fn resolve_for_folder<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> ThreadAgentsDocResult<Option<ResolvedThreadAgentsDocRecord>> {
        let (ancestors, candidates) =
            folder_resolution_candidates(db, workspace_id, folder_id).await?;

        for candidate in &candidates {
            let candidate_folder_id = candidate.as_deref();
            let Some(model) = find_active_model(db, workspace_id, candidate_folder_id).await?
            else {
                continue;
            };

            let source_folder_id = model.folder_id.clone();
            let source_path = source_path_for_folder(source_folder_id.as_deref(), &ancestors);
            let inherited = source_folder_id.as_deref() != folder_id;
            return Ok(Some(ResolvedThreadAgentsDocRecord {
                doc: record_from_model(model),
                resolved_for_workspace_id: workspace_id.to_owned(),
                resolved_for_folder_id: folder_id.map(str::to_owned),
                source_folder_id,
                source_path,
                inherited,
                resolved_at_unix: now().timestamp(),
            }));
        }

        Ok(None)
    }

    pub async fn resolve_for_thread<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        thread_id: &str,
    ) -> ThreadAgentsDocResult<Option<ResolvedThreadAgentsDocRecord>> {
        let placement = thread_placement::Entity::find_by_id(thread_id.to_owned())
            .one(db)
            .await
            .map_err(|source| ThreadAgentsDocError::Database {
                message: "failed to query thread placement for agents doc resolution".to_owned(),
                source,
            })?;

        if let Some(placement) = placement {
            if placement.workspace_id != workspace_id {
                return Err(ThreadAgentsDocError::WorkspaceMismatch {
                    message: format!(
                        "thread `{thread_id}` placement belongs to workspace `{}`",
                        placement.workspace_id
                    ),
                });
            }
            return self
                .resolve_for_folder(db, workspace_id, placement.folder_id.as_deref())
                .await;
        }

        self.resolve_for_folder(db, workspace_id, None).await
    }

    pub async fn scope_context<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> ThreadAgentsDocResult<ThreadAgentsDocScopeContext> {
        Ok(ThreadAgentsDocScopeContext {
            explicit: self.find_explicit(db, workspace_id, folder_id).await?,
            effective: self.resolve_for_folder(db, workspace_id, folder_id).await?,
        })
    }
}

async fn folder_resolution_candidates<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    folder_id: Option<&str>,
) -> ThreadAgentsDocResult<(Vec<thread_folder::Model>, Vec<Option<String>>)> {
    let mut ancestors = Vec::new();
    let mut candidates = Vec::new();
    let mut visited = HashSet::<String>::new();
    let mut cursor = folder_id.map(str::to_owned);

    while let Some(current_id) = cursor {
        if !visited.insert(current_id.clone()) {
            return Err(ThreadAgentsDocError::InvalidData {
                message: format!("cycle detected while resolving folder `{current_id}`"),
            });
        }

        let Some(folder) = thread_folder::Entity::find_by_id(current_id.clone())
            .one(db)
            .await
            .map_err(|source| ThreadAgentsDocError::Database {
                message: "failed to query folder for agents doc resolution".to_owned(),
                source,
            })?
        else {
            return Err(ThreadAgentsDocError::NotFound {
                message: format!("folder `{current_id}` was not found"),
            });
        };

        if folder.workspace_id != workspace_id {
            return Err(ThreadAgentsDocError::WorkspaceMismatch {
                message: format!(
                    "folder `{}` belongs to workspace `{}`",
                    folder.id, folder.workspace_id
                ),
            });
        }

        candidates.push(Some(folder.id.clone()));
        cursor = folder.parent_folder_id.clone();
        ancestors.push(folder);
    }

    candidates.push(None);
    Ok((ancestors, candidates))
}

async fn find_active_model<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    folder_id: Option<&str>,
) -> ThreadAgentsDocResult<Option<thread_agents_doc::Model>> {
    let mut query = thread_agents_doc::Entity::find()
        .filter(thread_agents_doc::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread_agents_doc::Column::Status.eq(status_to_db(ThreadAgentsDocStatus::Active)));

    query = match folder_id {
        Some(folder_id) => {
            query.filter(thread_agents_doc::Column::FolderId.eq(folder_id.to_owned()))
        }
        None => query.filter(thread_agents_doc::Column::FolderId.is_null()),
    };

    query
        .one(db)
        .await
        .map_err(|source| ThreadAgentsDocError::Database {
            message: "failed to query active thread agents doc".to_owned(),
            source,
        })
}

fn source_path_for_folder(
    source_folder_id: Option<&str>,
    ancestors_current_to_root: &[thread_folder::Model],
) -> Vec<String> {
    let Some(source_folder_id) = source_folder_id else {
        return Vec::new();
    };

    let Some(source_index) = ancestors_current_to_root
        .iter()
        .position(|folder| folder.id == source_folder_id)
    else {
        return Vec::new();
    };

    let mut path = ancestors_current_to_root[source_index..]
        .iter()
        .map(|folder| folder.name.clone())
        .collect::<Vec<_>>();
    path.reverse();
    path
}

async fn insert_draft_model<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    folder_id: Option<&str>,
    now: DateTimeWithTimeZone,
    actor_id: Option<&str>,
) -> ThreadAgentsDocResult<thread_agents_doc::Model> {
    let content = String::new();
    let content_sha256 = sha256_hex(content.as_bytes());
    thread_agents_doc::ActiveModel {
        id: Set(generate_id(21)),
        workspace_id: Set(workspace_id.to_owned()),
        folder_id: Set(folder_id.map(str::to_owned)),
        status: Set(status_to_db(ThreadAgentsDocStatus::Draft).to_owned()),
        title: Set("AGENTS.md".to_owned()),
        content: Set(content),
        content_sha256: Set(content_sha256),
        version: Set(1),
        created_at: Set(now),
        updated_at: Set(now),
        archived_at: Set(None),
        created_by: Set(actor_id.map(str::to_owned)),
        updated_by: Set(actor_id.map(str::to_owned)),
    }
    .insert(db)
    .await
    .map_err(|source| ThreadAgentsDocError::Database {
        message: "failed to insert thread agents doc draft".to_owned(),
        source,
    })
}

async fn find_explicit_model<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    folder_id: Option<&str>,
) -> ThreadAgentsDocResult<Option<thread_agents_doc::Model>> {
    let mut query = thread_agents_doc::Entity::find()
        .filter(thread_agents_doc::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread_agents_doc::Column::Status.ne(status_to_db(ThreadAgentsDocStatus::Archived)),
        );

    query = match folder_id {
        Some(folder_id) => {
            query.filter(thread_agents_doc::Column::FolderId.eq(folder_id.to_owned()))
        }
        None => query.filter(thread_agents_doc::Column::FolderId.is_null()),
    };

    query
        .one(db)
        .await
        .map_err(|source| ThreadAgentsDocError::Database {
            message: "failed to query explicit thread agents doc".to_owned(),
            source,
        })
}

async fn insert_revision<C: ConnectionTrait>(
    db: &C,
    doc_id: &str,
    version: i64,
    content_sha256: &str,
    content: &str,
    saved_at: DateTimeWithTimeZone,
    saved_by: Option<&str>,
    save_reason: ThreadAgentsDocSaveReason,
) -> ThreadAgentsDocResult<ThreadAgentsDocRevisionRecord> {
    thread_agents_doc_revision::ActiveModel {
        id: Set(generate_id(21)),
        doc_id: Set(doc_id.to_owned()),
        version: Set(version),
        content_sha256: Set(content_sha256.to_owned()),
        content: Set(content.to_owned()),
        saved_at: Set(saved_at),
        saved_by: Set(saved_by.map(str::to_owned)),
        save_reason: Set(save_reason_to_db(save_reason).to_owned()),
    }
    .insert(db)
    .await
    .map_err(|source| ThreadAgentsDocError::Database {
        message: "failed to insert thread agents doc revision".to_owned(),
        source,
    })
    .and_then(revision_record_from_model)
}

fn ensure_version(
    model: &thread_agents_doc::Model,
    expected_version: Option<i64>,
) -> ThreadAgentsDocResult<()> {
    let Some(expected) = expected_version else {
        return Ok(());
    };
    let actual = model.version;
    if expected == actual {
        Ok(())
    } else {
        Err(ThreadAgentsDocError::VersionConflict { expected, actual })
    }
}

pub fn normalize_content(content: &str) -> String {
    content.replace("\r\n", "\n").replace('\r', "\n")
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub fn now() -> DateTimeWithTimeZone {
    Utc::now().fixed_offset()
}

pub fn status_to_db(status: ThreadAgentsDocStatus) -> &'static str {
    match status {
        ThreadAgentsDocStatus::Draft => "draft",
        ThreadAgentsDocStatus::Active => "active",
        ThreadAgentsDocStatus::Archived => "archived",
    }
}

pub fn status_from_db(value: &str) -> ThreadAgentsDocResult<ThreadAgentsDocStatus> {
    match value {
        "draft" => Ok(ThreadAgentsDocStatus::Draft),
        "active" => Ok(ThreadAgentsDocStatus::Active),
        "archived" => Ok(ThreadAgentsDocStatus::Archived),
        value => Err(ThreadAgentsDocError::InvalidData {
            message: format!("unknown status `{value}`"),
        }),
    }
}

pub fn save_reason_to_db(reason: ThreadAgentsDocSaveReason) -> &'static str {
    match reason {
        ThreadAgentsDocSaveReason::Autosave => "autosave",
        ThreadAgentsDocSaveReason::Manual => "manual",
        ThreadAgentsDocSaveReason::Archive => "archive",
        ThreadAgentsDocSaveReason::Restore => "restore",
    }
}

pub fn save_reason_from_db(value: &str) -> ThreadAgentsDocResult<ThreadAgentsDocSaveReason> {
    match value {
        "autosave" => Ok(ThreadAgentsDocSaveReason::Autosave),
        "manual" => Ok(ThreadAgentsDocSaveReason::Manual),
        "archive" => Ok(ThreadAgentsDocSaveReason::Archive),
        "restore" => Ok(ThreadAgentsDocSaveReason::Restore),
        value => Err(ThreadAgentsDocError::InvalidData {
            message: format!("unknown save reason `{value}`"),
        }),
    }
}

pub fn record_from_model(model: thread_agents_doc::Model) -> ThreadAgentsDocRecord {
    ThreadAgentsDocRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        folder_id: model.folder_id,
        status: status_from_db(model.status.as_str()).unwrap_or(ThreadAgentsDocStatus::Draft),
        title: model.title,
        content: model.content,
        content_sha256: model.content_sha256,
        version: i64::from(model.version),
        created_at_unix: model.created_at.timestamp(),
        updated_at_unix: model.updated_at.timestamp(),
        archived_at_unix: model.archived_at.map(|value| value.timestamp()),
        created_by: model.created_by,
        updated_by: model.updated_by,
    }
}

pub fn summary_record_from_model(
    model: thread_agents_doc::Model,
) -> ThreadAgentsDocResult<ThreadAgentsDocSummaryRecord> {
    Ok(ThreadAgentsDocSummaryRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        folder_id: model.folder_id,
        status: status_from_db(model.status.as_str())?,
        content_sha256: model.content_sha256,
        version: i64::from(model.version),
        char_count: model.content.chars().count(),
        updated_at_unix: model.updated_at.timestamp(),
    })
}

pub fn revision_record_from_model(
    model: thread_agents_doc_revision::Model,
) -> ThreadAgentsDocResult<ThreadAgentsDocRevisionRecord> {
    Ok(ThreadAgentsDocRevisionRecord {
        id: model.id,
        doc_id: model.doc_id,
        version: i64::from(model.version),
        content_sha256: model.content_sha256,
        content: model.content,
        saved_at_unix: model.saved_at.timestamp(),
        saved_by: model.saved_by,
        save_reason: save_reason_from_db(model.save_reason.as_str())?,
    })
}
