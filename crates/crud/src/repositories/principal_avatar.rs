use anyhow::{Context, Result, bail};
use pioneer_entity::{principal_avatar, principal_avatar_revision};
use pioneer_protocol::{PrincipalId, ProfileAvatarMediaType};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    Set,
};
use std::collections::HashMap;

pub struct NewPrincipalAvatarRow {
    pub principal_id: PrincipalId,
    pub media_type: ProfileAvatarMediaType,
    pub content: Vec<u8>,
    pub content_hash: [u8; 32],
    pub width: u32,
    pub height: u32,
    pub now: DateTimeWithTimeZone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrincipalAvatarRow {
    pub principal_id: String,
    pub media_type: String,
    pub content: Vec<u8>,
    pub content_hash: Vec<u8>,
    pub width: i64,
    pub height: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalAvatarRevisionRow {
    pub principal_id: String,
    pub content_hash: Vec<u8>,
}

impl std::fmt::Debug for NewPrincipalAvatarRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NewPrincipalAvatarRow")
            .field("principal_id", &self.principal_id)
            .field("media_type", &self.media_type)
            .field("content", &"[redacted]")
            .field("content_hash", &"[redacted]")
            .field("width", &self.width)
            .field("height", &self.height)
            .field("now", &self.now)
            .finish()
    }
}

pub async fn insert_principal_avatar(
    transaction: &DatabaseTransaction,
    row: NewPrincipalAvatarRow,
) -> Result<PrincipalAvatarRow> {
    let revision = persist_immutable_revision(transaction, &row).await?;
    let current = principal_avatar::ActiveModel {
        principal_id: Set(row.principal_id.to_string()),
        revision_id: Set(revision.id.clone()),
        updated_at: Set(row.now),
    }
    .insert(transaction)
    .await
    .context("failed to insert current principal avatar revision")?;
    combine_current_and_revision(current, revision)
}

pub async fn replace_principal_avatar(
    transaction: &DatabaseTransaction,
    row: NewPrincipalAvatarRow,
) -> Result<PrincipalAvatarRow> {
    let revision = persist_immutable_revision(transaction, &row).await?;
    let current = if let Some(existing) =
        principal_avatar::Entity::find_by_id(row.principal_id.to_string())
            .one(transaction)
            .await
            .context("failed to load current principal avatar revision")?
    {
        let mut active: principal_avatar::ActiveModel = existing.into();
        active.revision_id = Set(revision.id.clone());
        active.updated_at = Set(row.now);
        active
            .update(transaction)
            .await
            .context("failed to replace current principal avatar revision")?
    } else {
        principal_avatar::ActiveModel {
            principal_id: Set(row.principal_id.to_string()),
            revision_id: Set(revision.id.clone()),
            updated_at: Set(row.now),
        }
        .insert(transaction)
        .await
        .context("failed to insert current principal avatar revision")?
    };
    combine_current_and_revision(current, revision)
}

pub async fn delete_principal_avatar(
    transaction: &DatabaseTransaction,
    principal_id: &PrincipalId,
) -> Result<bool> {
    let result = principal_avatar::Entity::delete_by_id(principal_id.to_string())
        .exec(transaction)
        .await
        .context("failed to delete principal avatar")?;
    Ok(result.rows_affected == 1)
}

pub async fn load_principal_avatar<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
) -> Result<Option<PrincipalAvatarRow>> {
    let Some(current) = principal_avatar::Entity::find_by_id(principal_id.to_string())
        .one(db)
        .await
        .context("failed to load current principal avatar revision")?
    else {
        return Ok(None);
    };
    let revision = principal_avatar_revision::Entity::find_by_id(current.revision_id.clone())
        .one(db)
        .await
        .context("failed to load current immutable principal avatar revision")?
        .ok_or_else(|| anyhow::anyhow!("current principal avatar revision is missing"))?;
    combine_current_and_revision(current, revision).map(Some)
}

pub async fn load_principal_avatar_revision<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    content_hash: &[u8],
) -> Result<Option<PrincipalAvatarRow>> {
    let Some(revision) = load_revision_model(db, principal_id, content_hash).await? else {
        return Ok(None);
    };
    Ok(Some(PrincipalAvatarRow {
        principal_id: revision.principal_id,
        media_type: revision.media_type,
        content: revision.content,
        content_hash: revision.content_hash,
        width: revision.width,
        height: revision.height,
    }))
}

pub async fn list_principal_avatar_revisions<C: ConnectionTrait>(
    db: &C,
    principal_ids: &[PrincipalId],
) -> Result<Vec<PrincipalAvatarRevisionRow>> {
    if principal_ids.is_empty() {
        return Ok(Vec::new());
    }
    let current = principal_avatar::Entity::find()
        .filter(
            principal_avatar::Column::PrincipalId
                .is_in(principal_ids.iter().map(ToString::to_string)),
        )
        .all(db)
        .await
        .context("failed to load bounded current member avatar revisions")?;
    let revision_ids = current
        .iter()
        .map(|avatar| avatar.revision_id.clone())
        .collect::<Vec<_>>();
    let revisions = principal_avatar_revision::Entity::find()
        .filter(principal_avatar_revision::Column::Id.is_in(revision_ids))
        .all(db)
        .await
        .context("failed to load bounded member avatar revisions")?
        .into_iter()
        .map(|revision| (revision.id.clone(), revision))
        .collect::<HashMap<_, _>>();

    current
        .into_iter()
        .map(|avatar| {
            let revision = revisions
                .get(&avatar.revision_id)
                .ok_or_else(|| anyhow::anyhow!("current principal avatar revision is missing"))?;
            if avatar.principal_id != revision.principal_id {
                bail!("current principal avatar points to a different principal");
            }
            Ok(PrincipalAvatarRevisionRow {
                principal_id: revision.principal_id.clone(),
                content_hash: revision.content_hash.clone(),
            })
        })
        .collect()
}

async fn persist_immutable_revision(
    transaction: &DatabaseTransaction,
    row: &NewPrincipalAvatarRow,
) -> Result<principal_avatar_revision::Model> {
    if let Some(existing) =
        load_revision_model(transaction, &row.principal_id, row.content_hash.as_slice()).await?
    {
        if existing.media_type != row.media_type.as_str()
            || existing.content != row.content
            || existing.width != i64::from(row.width)
            || existing.height != i64::from(row.height)
        {
            bail!("immutable principal avatar revision conflicts with persisted content");
        }
        return Ok(existing);
    }

    principal_avatar_revision::ActiveModel {
        id: Set(revision_id(&row.principal_id, row.content_hash.as_slice())
            .expect("a SHA-256 avatar content hash is always 32 bytes")),
        principal_id: Set(row.principal_id.to_string()),
        content_hash: Set(row.content_hash.to_vec()),
        media_type: Set(row.media_type.as_str().to_owned()),
        content: Set(row.content.clone()),
        width: Set(i64::from(row.width)),
        height: Set(i64::from(row.height)),
        created_at: Set(row.now),
    }
    .insert(transaction)
    .await
    .context("failed to insert immutable principal avatar revision")
}

async fn load_revision_model<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    content_hash: &[u8],
) -> Result<Option<principal_avatar_revision::Model>> {
    let Some(revision_id) = revision_id(principal_id, content_hash) else {
        return Ok(None);
    };
    principal_avatar_revision::Entity::find_by_id(revision_id)
        .one(db)
        .await
        .context("failed to load immutable principal avatar revision")
}

fn combine_current_and_revision(
    current: principal_avatar::Model,
    revision: principal_avatar_revision::Model,
) -> Result<PrincipalAvatarRow> {
    if current.principal_id != revision.principal_id || current.revision_id != revision.id {
        bail!("current principal avatar points to a different immutable revision");
    }
    Ok(PrincipalAvatarRow {
        principal_id: revision.principal_id,
        media_type: revision.media_type,
        content: revision.content,
        content_hash: revision.content_hash,
        width: revision.width,
        height: revision.height,
    })
}

fn revision_id(principal_id: &PrincipalId, content_hash: &[u8]) -> Option<String> {
    (content_hash.len() == 32).then(|| format!("{principal_id}:{}", hex::encode(content_hash)))
}
