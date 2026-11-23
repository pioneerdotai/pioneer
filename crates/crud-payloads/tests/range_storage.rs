use anyhow::{Context, Result};
use pioneer_crud_payloads::BLOCK_BYTES;
use pioneer_crud_payloads::storage::{PreparedPayload, Selection, capture, delete};
use pioneer_entity::{portable_payload_part as part, portable_payload_value as value};
use pioneer_postgres_test::with_database;
use sea_orm::{
    AccessMode, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, IsolationLevel,
    PaginatorTrait, QueryFilter, Statement, TransactionTrait,
};

#[tokio::test]
async fn ranged_storage_checks_utf8_boundaries_and_loads_only_intersecting_blocks() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        let at_boundary = "x".repeat(BLOCK_BYTES);
        let split = format!("{}界🙂{}", "x".repeat(BLOCK_BYTES - 1), "ё".repeat(BLOCK_BYTES));
        let empty = PreparedPayload::new("")?;
        let one = PreparedPayload::new(&at_boundary)?;
        let several = PreparedPayload::new(&split)?;
        let tx = db.begin().await?;
        for prepared in [&empty, &one, &several] {
            prepared.insert(&tx).await?;
        }
        tx.commit().await?;
        assert_eq!(part::Entity::find().filter(part::Column::PayloadId.eq(one.id())).count(db).await?, 0);
        assert!(part::Entity::find().filter(part::Column::PayloadId.eq(several.id())).count(db).await? > 1);

        let tx = db.begin_with_config(Some(IsolationLevel::RepeatableRead), Some(AccessMode::ReadOnly)).await?;
        let empty_capture = capture(&tx, empty.id(), Selection::Full).await?.context("missing empty value")?;
        let single_capture = capture(&tx, one.id(), Selection::Full).await?.context("missing single value")?;
        let full_capture = capture(&tx, several.id(), Selection::Full).await?.context("missing split value")?;
        let bytes = BLOCK_BYTES - 3..BLOCK_BYTES + 3;
        let crossing = capture(&tx, several.id(), Selection::Bytes(bytes.clone())).await?.context("missing range")?;
        let characters = capture(&tx, several.id(), Selection::Characters(BLOCK_BYTES - 2..BLOCK_BYTES + 2)).await?.context("missing character range")?;
        let empty_range = capture(&tx, several.id(), Selection::Bytes(12..12)).await?.context("missing empty range")?;
        tx.commit().await?;
        assert_eq!(empty_capture.into_text()?, "");
        assert_eq!(single_capture.into_text()?, at_boundary);
        assert_eq!(full_capture.into_text()?, split);
        assert_eq!(crossing.into_bytes()?, split.as_bytes()[bytes]);
        assert_eq!(characters.into_text()?, "x界🙂ё");
        assert!(empty_range.into_bytes()?.is_empty());

        // Changing the checksum of the first block must affect reads of that
        // block only, until a full verification is requested.
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "UPDATE portable_payload_part SET sha256 = $1 WHERE payload_id = $2 AND byte_offset = 0",
            [vec![0_u8; 32].into(), several.id().into()],
        )).await?;
        let tx = db.begin_with_config(Some(IsolationLevel::RepeatableRead), Some(AccessMode::ReadOnly)).await?;
        let tail = capture(&tx, several.id(), Selection::Bytes(split.len() - 6..split.len())).await?.context("missing tail")?;
        let damaged = capture(&tx, several.id(), Selection::Bytes(0..1)).await?.context("missing damaged block")?;
        tx.commit().await?;
        assert_eq!(tail.into_text()?, "ёёё");
        assert!(damaged.into_bytes().is_err());

        let tx = db.begin().await?;
        assert!(delete(&tx, several.id()).await?);
        tx.commit().await?;
        assert!(value::Entity::find_by_id(several.id()).one(db).await?.is_none());
        assert_eq!(part::Entity::find().filter(part::Column::PayloadId.eq(several.id())).count(db).await?, 0);
        Ok(())
    }).await
}
