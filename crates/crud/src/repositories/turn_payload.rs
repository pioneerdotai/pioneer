//! Logical turn payload reads. Holds own no database resources; retry a source
//! capture lost to reclamation rather than decode a partially retired value.
use super::portable_payload::{self, Family, PayloadReadHold};
use anyhow::Result;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::EntityTrait;

macro_rules! reader {
    ($load:ident, $hydrate:ident, $module:ident, $family:ident) => {
        pub(crate) async fn $hydrate(
            db: &SqliteDatabase,
            rows: Vec<pioneer_entity::$module::Model>,
        ) -> Result<Vec<pioneer_entity::$module::Model>> {
            let captured = rows
                .into_iter()
                .map(|row| {
                    let hold = row
                        .payload_payload_id
                        .as_deref()
                        .map(|id| PayloadReadHold::capture(id, Family::$family))
                        .transpose()?;
                    Ok((row, hold))
                })
                .collect::<Result<Vec<_>>>()?;
            let mut rows = Vec::with_capacity(captured.len());
            for (mut row, hold) in captured {
                tracing::trace!(
                    family = Family::$family.name(),
                    storage = if row.payload_payload_id.is_some() {
                        "portable"
                    } else if row.portable_identity.is_empty() {
                        "legacy"
                    } else {
                        "pending"
                    },
                    "turn payload read"
                );
                row.payload = portable_payload::hydrate_with_hold(
                    db,
                    Family::$family,
                    row.payload_payload_id.as_deref(),
                    row.payload,
                    hold.as_ref(),
                )
                .await?;
                rows.push(row);
            }
            Ok(rows)
        }
        pub(crate) async fn $load(
            db: &SqliteDatabase,
            query: sea_orm::Select<pioneer_entity::$module::Entity>,
        ) -> Result<Vec<pioneer_entity::$module::Model>> {
            crate::CrudStore::new(db.clone())
                .run_scoped_database_quantum(|| async {
                    $hydrate(db, query.clone().all(db).await?).await
                })
                .await
        }
    };
}
reader!(events, hydrate_events, turn_event, TurnEvent);
reader!(items, hydrate_items, turn_item, TurnItem);

/// Exact logical equality prepared before writer admission. Portable metadata
/// supplies the already verified digest; no block decoding or hashing occurs
/// in the atomic mutation. Inline comparisons retain their existing semantics.
#[derive(Clone, Debug)]
pub(crate) struct PreparedItemPayload {
    text: String,
    sha256: Vec<u8>,
}
impl PreparedItemPayload {
    pub(crate) fn new(text: String) -> Self {
        use sha2::{Digest, Sha256};
        let sha256 = Sha256::digest(text.as_bytes()).to_vec();
        Self { text, sha256 }
    }
    pub(crate) async fn matches<C: sea_orm::ConnectionTrait>(
        &self,
        db: &C,
        row: &pioneer_entity::turn_item::Model,
    ) -> Result<bool> {
        use pioneer_entity::portable_payload as p;
        use sea_orm::{ColumnTrait, QueryFilter, QuerySelect};
        let Some(id) = &row.payload_payload_id else {
            return Ok(row.payload == self.text);
        };
        Ok(p::Entity::find_by_id(id)
            .select_only()
            .column(p::Column::Id)
            .filter(p::Column::Family.eq("turn.item"))
            .filter(p::Column::OriginalBytes.eq(self.text.len() as i64))
            .filter(p::Column::Sha256.eq(self.sha256.clone()))
            .into_tuple::<String>()
            .one(db)
            .await?
            .is_some())
    }
}

/// A domain projection may read its canonical item again inside the writer.
/// Prepare logical bytes outside capacity, then restore inline representation
/// only if that writer actually needs a published source. Publication itself
/// does not change its logical revision, so this also covers a raw-to-PPAY race.
#[derive(Clone, Debug)]
pub(crate) struct PreparedInlineItem {
    turn_id: String,
    item_id: String,
    source: Option<pioneer_entity::turn_item::Model>,
}
impl PreparedInlineItem {
    pub(crate) async fn prepare(
        db: &SqliteDatabase,
        event: &crate::CanonicalTurnEventPayload,
    ) -> Result<Option<Self>> {
        let Some(item_id) = crate::timeline_live_projection::payload_item_id(event) else {
            return Ok(None);
        };
        let source = super::turn::find_turn_item(db, event.turn_id(), item_id).await?;
        Ok(Some(Self {
            turn_id: event.turn_id().to_owned(),
            item_id: item_id.to_owned(),
            source,
        }))
    }
    pub(crate) async fn apply<C: sea_orm::ConnectionTrait>(&self, db: &C) -> Result<()> {
        use pioneer_entity::turn_item as item;
        use sea_orm::sea_query::Expr;
        use sea_orm::{ColumnTrait, QueryFilter, QuerySelect};
        let Some(current) =
            super::turn::find_turn_item_metadata(db, &self.turn_id, &self.item_id).await?
        else {
            return Ok(());
        };
        if current.payload_payload_id.is_none() {
            return Ok(());
        }
        let source = self
            .source
            .as_ref()
            .filter(|old| {
                old.id == current.id
                    && old.portable_identity == current.portable_identity
                    && old.portable_revision == current.portable_revision
            })
            .ok_or(super::portable_payload::PreflightRace)?;
        item::Entity::update_many()
            .col_expr(item::Column::Payload, Expr::value(source.payload.clone()))
            .col_expr(
                item::Column::PayloadPayloadId,
                Expr::value(Option::<String>::None),
            )
            .filter(item::Column::Id.eq(&current.id))
            .filter(item::Column::PortableIdentity.eq(&source.portable_identity))
            .filter(item::Column::PortableRevision.eq(source.portable_revision))
            .filter(item::Column::PayloadPayloadId.eq(current.payload_payload_id))
            .exec(db)
            .await?;
        // Writable legacy views return zero changes even on successful UPDATE.
        anyhow::ensure!(
            item::Entity::find_by_id(&current.id)
                .select_only()
                .column(item::Column::Id)
                .filter(item::Column::PayloadPayloadId.is_null())
                .filter(item::Column::PortableIdentity.eq(&source.portable_identity))
                .filter(item::Column::PortableRevision.eq(source.portable_revision))
                .into_tuple::<String>()
                .one(db)
                .await?
                .is_some(),
            "portable item inline restoration lost its source fence"
        );
        Ok(())
    }
}

/// JSON1 uses the first matching object label. serde_json::Value ordinarily
/// keeps the last duplicate, which would change existing SQL predicates when
/// their portable inputs are decoded outside database capacity.
pub(crate) fn json1_value(text: &str) -> Result<serde_json::Value> {
    use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
    use serde_json::Value;
    struct First(Value);
    impl<'de> Deserialize<'de> for First {
        fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
            struct FirstVisitor;
            impl<'de> Visitor<'de> for FirstVisitor {
                type Value = First;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("JSON value")
                }
                fn visit_unit<E: de::Error>(self) -> std::result::Result<First, E> {
                    Ok(First(Value::Null))
                }
                fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<First, E> {
                    Ok(First(v.into()))
                }
                fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<First, E> {
                    Ok(First(v.into()))
                }
                fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<First, E> {
                    Ok(First(v.into()))
                }
                fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<First, E> {
                    Ok(First(Value::Number(
                        serde_json::Number::from_f64(v)
                            .ok_or_else(|| E::custom("invalid JSON number"))?,
                    )))
                }
                fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<First, E> {
                    Ok(First(v.into()))
                }
                fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<First, E> {
                    Ok(First(v.into()))
                }
                fn visit_seq<A: SeqAccess<'de>>(
                    self,
                    mut seq: A,
                ) -> std::result::Result<First, A::Error> {
                    let mut out = Vec::new();
                    while let Some(First(v)) = seq.next_element()? {
                        out.push(v);
                    }
                    Ok(First(out.into()))
                }
                fn visit_map<A: MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> std::result::Result<First, A::Error> {
                    let mut out = serde_json::Map::new();
                    while let Some(key) = map.next_key::<String>()? {
                        let First(v) = map.next_value()?;
                        out.entry(key).or_insert(v);
                    }
                    Ok(First(Value::Object(out)))
                }
            }
            d.deserialize_any(FirstVisitor)
        }
    }
    Ok(serde_json::from_str::<First>(text)?.0)
}
