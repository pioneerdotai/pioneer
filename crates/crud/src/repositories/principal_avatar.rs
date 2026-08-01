use anyhow::{Context, Result};
use pioneer_entity::principal_avatar;
use pioneer_protocol::{PrincipalId, ProfileAvatarMediaType};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QuerySelect, Set,
};

pub struct NewPrincipalAvatarRow {
    pub principal_id: PrincipalId,
    pub media_type: ProfileAvatarMediaType,
    pub content: Vec<u8>,
    pub content_hash: [u8; 32],
    pub width: u32,
    pub height: u32,
    pub now: DateTimeWithTimeZone,
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
) -> Result<principal_avatar::Model> {
    principal_avatar::ActiveModel {
        principal_id: Set(row.principal_id.to_string()),
        media_type: Set(row.media_type.as_str().to_owned()),
        content: Set(row.content),
        content_hash: Set(row.content_hash.to_vec()),
        width: Set(i64::from(row.width)),
        height: Set(i64::from(row.height)),
        created_at: Set(row.now),
        updated_at: Set(row.now),
    }
    .insert(transaction)
    .await
    .context("failed to insert principal avatar")
}

pub async fn load_principal_avatar<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
) -> Result<Option<principal_avatar::Model>> {
    principal_avatar::Entity::find_by_id(principal_id.to_string())
        .one(db)
        .await
        .context("failed to load principal avatar")
}

pub async fn list_principal_avatar_revisions<C: ConnectionTrait>(
    db: &C,
    principal_ids: &[PrincipalId],
) -> Result<Vec<PrincipalAvatarRevisionRow>> {
    if principal_ids.is_empty() {
        return Ok(Vec::new());
    }
    principal_avatar::Entity::find()
        .select_only()
        .column(principal_avatar::Column::PrincipalId)
        .column(principal_avatar::Column::ContentHash)
        .filter(
            principal_avatar::Column::PrincipalId
                .is_in(principal_ids.iter().map(ToString::to_string)),
        )
        .into_tuple::<(String, Vec<u8>)>()
        .all(db)
        .await
        .context("failed to load bounded member avatar revisions")
        .map(|rows| {
            rows.into_iter()
                .map(|(principal_id, content_hash)| PrincipalAvatarRevisionRow {
                    principal_id,
                    content_hash,
                })
                .collect()
        })
}
