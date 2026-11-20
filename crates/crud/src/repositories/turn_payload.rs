//! Read payload frames in a PostgreSQL snapshot and decode them after release.
use crate::util::PreflightRace;
use anyhow::Result;
use pioneer_crud_payloads::storage::{self, CapturedPayload, Selection};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::EntityTrait;

macro_rules! reader {
    ($load:ident, $hydrate:ident, $capture:ident, $decode:ident, $module:ident) => {
        pub(crate) async fn $load(
            db: &SqliteDatabase,
            query: sea_orm::Select<pioneer_entity::$module::Entity>,
        ) -> Result<Vec<pioneer_entity::$module::Model>> {
            let reader = db.begin_read().await?;
            let rows = query.all(&reader).await?;
            let captured = $capture(&reader, rows).await?;
            reader.commit().await?;
            $decode(captured)
        }

        pub(crate) async fn $hydrate(
            db: &SqliteDatabase,
            rows: Vec<pioneer_entity::$module::Model>,
        ) -> Result<Vec<pioneer_entity::$module::Model>> {
            let reader = db.begin_read().await?;
            let captured = $capture(&reader, rows).await?;
            reader.commit().await?;
            $decode(captured)
        }

        async fn $capture<C: sea_orm::ConnectionTrait>(
            db: &C,
            rows: Vec<pioneer_entity::$module::Model>,
        ) -> Result<Vec<(pioneer_entity::$module::Model, Option<CapturedPayload>)>> {
            let mut captured = Vec::with_capacity(rows.len());
            for row in rows {
                let body = if let Some(id) = &row.payload_payload_id {
                    Some(
                        storage::capture(db, id, Selection::Full)
                            .await?
                            .ok_or(PreflightRace)?,
                    )
                } else {
                    None
                };
                captured.push((row, body));
            }
            Ok(captured)
        }

        fn $decode(
            captured: Vec<(pioneer_entity::$module::Model, Option<CapturedPayload>)>,
        ) -> Result<Vec<pioneer_entity::$module::Model>> {
            captured
                .into_iter()
                .map(|(mut row, body)| {
                    if let Some(body) = body {
                        row.payload = Some(serde_json::from_slice(&body.into_bytes()?)?);
                    }
                    Ok(row)
                })
                .collect()
        }
    };
}

reader!(
    events,
    hydrate_events,
    capture_events,
    decode_events,
    turn_event
);
reader!(items, hydrate_items, capture_items, decode_items, turn_item);

/// Capture exact bytes for readers that publish a hash or compare a source CAS.
pub(crate) async fn exact_items(
    db: &SqliteDatabase,
    query: sea_orm::Select<pioneer_entity::turn_item::Entity>,
) -> Result<Vec<(pioneer_entity::turn_item::Model, String)>> {
    use anyhow::Context;
    use pioneer_entity::turn_item as item;
    use sea_orm::QuerySelect;
    use sea_orm::sea_query::{Expr, ExprTrait};

    let reader = db.begin_read().await?;
    let rows = query.all(&reader).await?;
    let encoded = capture_items(&reader, rows).await?;
    let mut captured = Vec::with_capacity(encoded.len());
    for (row, body) in encoded {
        let inline = if body.is_none() {
            item::Entity::find_by_id(&row.id)
                .select_only()
                .expr(Expr::col(item::Column::Payload).cast_as("text"))
                .into_tuple::<String>()
                .one(&reader)
                .await?
        } else {
            None
        };
        captured.push((row, body, inline));
    }
    reader.commit().await?;
    captured
        .into_iter()
        .map(|(row, body, inline)| {
            let text = match body {
                Some(body) => body.into_text()?,
                None => inline.context("missing inline turn item payload")?,
            };
            Ok((row, text))
        })
        .collect()
}

pub(crate) fn decode_item(
    row: &pioneer_entity::turn_item::Model,
) -> Result<pioneer_protocol::TurnItem> {
    use anyhow::Context;
    let value = row
        .payload
        .as_ref()
        .context("turn item payload was not loaded")?;
    Ok(serde::Deserialize::deserialize(value)?)
}

/// The comparison uses metadata, so it never decompresses data in a writer.
#[derive(Clone, Debug)]
pub(crate) struct PreparedItemPayload {
    text: String,
    sha256: [u8; 32],
}

impl PreparedItemPayload {
    pub(crate) fn new(text: String) -> Self {
        use sha2::{Digest, Sha256};
        let sha256 = Sha256::digest(text.as_bytes()).into();
        Self { text, sha256 }
    }

    pub(crate) async fn matches<C: sea_orm::ConnectionTrait>(
        &self,
        db: &C,
        row: &pioneer_entity::turn_item::Model,
    ) -> Result<bool> {
        use pioneer_entity::turn_item as item;
        use sea_orm::sea_query::{Expr, ExprTrait};
        use sea_orm::{ColumnTrait, QueryFilter, QuerySelect};
        if let Some(id) = &row.payload_payload_id {
            return Ok(storage::metadata(db, id).await?.is_some_and(|meta| {
                meta.original_bytes == self.text.len() && meta.sha256 == self.sha256
            }));
        }
        Ok(item::Entity::find_by_id(&row.id)
            .select_only()
            .column(item::Column::Id)
            .filter(item::Column::PortableIdentity.eq(&row.portable_identity))
            .filter(item::Column::PortableRevision.eq(row.portable_revision))
            .filter(
                Expr::col(item::Column::Payload)
                    .cast_as("text")
                    .eq(&self.text),
            )
            .into_tuple::<String>()
            .one(db)
            .await?
            .is_some())
    }
}
