//! Application envelopes and bounded UTF-8 blocks. No extension or data scan.
use sea_orm_migration::sea_orm::{DbBackend, Statement};
use sea_orm_migration::{prelude::*, schema::*};
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        m.create_table(
            Table::create()
                .table(Alias::new("portable_payload_dictionary"))
                .col(text("id").primary_key())
                .col(text("family"))
                .col(integer("version"))
                .col(binary("data"))
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("portable_payload_dictionary_family_version")
                .table(Alias::new("portable_payload_dictionary"))
                .col("family")
                .col("version")
                .unique()
                .to_owned(),
        )
        .await?;
        m.create_table(
            Table::create()
                .table(Alias::new("portable_payload"))
                .col(text("id").primary_key())
                .col(text("family"))
                .col(big_integer("original_bytes"))
                .col(big_integer("original_chars"))
                .col(text("sha256"))
                .col(integer("raw"))
                .col(integer("retired").default(1))
                .col(text_null("dictionary_id"))
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("portable_payload_retired")
                .table(Alias::new("portable_payload"))
                .col("retired")
                .col("id")
                .to_owned(),
        )
        .await?;
        m.create_table(
            Table::create()
                .table(Alias::new("portable_payload_block"))
                .col(text("payload_id"))
                .col(big_integer("ordinal"))
                .col(big_integer("byte_offset"))
                .col(big_integer("char_offset"))
                .col(big_integer("original_bytes"))
                .col(big_integer("original_chars"))
                .col(binary("data"))
                .primary_key(Index::create().col("payload_id").col("ordinal"))
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("portable_payload_block_bytes")
                .table(Alias::new("portable_payload_block"))
                .col("payload_id")
                .col("byte_offset")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("portable_payload_block_chars")
                .table(Alias::new("portable_payload_block"))
                .col("payload_id")
                .col("char_offset")
                .to_owned(),
        )
        .await?;
        Ok(())
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        // Never silently discard the only surviving copy of a logical value.
        // Rollback requires an explicit reverse-data migration after review.
        if m.get_connection()
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 1 FROM portable_payload WHERE retired=0 AND raw=0 LIMIT 1".to_owned(),
            ))
            .await?
            .is_some()
        {
            return Err(DbErr::Custom(
                "portable payload rollback requires restoring logical TEXT first".into(),
            ));
        }
        for table in [
            "portable_payload_block",
            "portable_payload",
            "portable_payload_dictionary",
        ] {
            m.drop_table(Table::drop().table(Alias::new(table)).to_owned())
                .await?;
        }
        Ok(())
    }
}
