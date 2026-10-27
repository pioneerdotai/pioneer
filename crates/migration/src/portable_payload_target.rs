//! Fixed target DDL shared by the four portable-storage migrations.
use sea_orm_migration::sea_orm::{DbBackend, Statement};
use sea_orm_migration::{prelude::*, schema::*};

pub(super) async fn up(
    m: &SchemaManager<'_>,
    table: &str,
    key: &str,
    fields: &[&str],
    ownership: &[&str],
) -> Result<(), DbErr> {
    m.alter_table(
        Table::alter()
            .table(Alias::new(table))
            .add_column(big_integer("portable_revision").default(0))
            .to_owned(),
    )
    .await?;
    for field in fields {
        let reference = format!("{field}_payload_id");
        m.alter_table(
            Table::alter()
                .table(Alias::new(table))
                .add_column(text_null(Alias::new(&reference)))
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name(format!("{table}_{field}_portable_pending"))
                .table(Alias::new(table))
                .col(Alias::new(&reference))
                .col(Alias::new(key))
                .to_owned(),
        )
        .await?;
        // Trigger DDL and OLD/NEW are absent from the installed builder API.
        // Physical publication changes the explicit reference; ordinary writes
        // leave it unchanged and must invalidate the old storage/revision.
        m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_{field}_portable_logical AFTER UPDATE OF {field} ON {table} WHEN NEW.{reference} IS OLD.{reference} BEGIN UPDATE {table} SET {reference}=NULL,portable_revision=portable_revision+1 WHERE {key}=NEW.{key}; END")).await?;
        m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_{field}_portable_replace AFTER UPDATE OF {reference} ON {table} WHEN OLD.{reference} IS NOT NULL AND OLD.{reference} IS NOT NEW.{reference} BEGIN UPDATE portable_payload SET retired=1 WHERE id=OLD.{reference}; END")).await?;
        m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_{field}_portable_delete AFTER DELETE ON {table} WHEN OLD.{reference} IS NOT NULL BEGIN UPDATE portable_payload SET retired=1 WHERE id=OLD.{reference}; END")).await?;
    }
    let clear = fields
        .iter()
        .map(|f| format!("{f}_payload_id=NULL"))
        .collect::<Vec<_>>()
        .join(",");
    m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_portable_owner AFTER UPDATE OF {} ON {table} BEGIN UPDATE {table} SET {clear},portable_revision=portable_revision+1 WHERE {key}=NEW.{key}; END",ownership.join(","))).await?;
    Ok(())
}
pub(super) async fn down(m: &SchemaManager<'_>, table: &str, fields: &[&str]) -> Result<(), DbErr> {
    let refs = fields
        .iter()
        .map(|f| format!("SELECT {f}_payload_id AS id FROM {table}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    if m.get_connection()
        .query_one(Statement::from_string(
            DbBackend::Sqlite,
            format!("SELECT 1 FROM portable_payload p WHERE p.raw=0 AND p.id IN ({refs}) LIMIT 1"),
        ))
        .await?
        .is_some()
    {
        return Err(DbErr::Custom(
            "portable rollback requires restoring original TEXT first".into(),
        ));
    }
    m.get_connection()
        .execute_unprepared(&format!("DROP TRIGGER {table}_portable_owner"))
        .await?;
    for field in fields {
        for suffix in ["logical", "replace", "delete"] {
            m.get_connection()
                .execute_unprepared(&format!("DROP TRIGGER {table}_{field}_portable_{suffix}"))
                .await?;
        }
        m.drop_index(
            Index::drop()
                .name(format!("{table}_{field}_portable_pending"))
                .table(Alias::new(table))
                .to_owned(),
        )
        .await?;
        m.alter_table(
            Table::alter()
                .table(Alias::new(table))
                .drop_column(Alias::new(format!("{field}_payload_id")))
                .to_owned(),
        )
        .await?;
    }
    m.alter_table(
        Table::alter()
            .table(Alias::new(table))
            .drop_column(Alias::new("portable_revision"))
            .to_owned(),
    )
    .await?;
    Ok(())
}
