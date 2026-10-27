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
    m.alter_table(
        Table::alter()
            .table(Alias::new(table))
            .add_column(text("portable_identity").default(""))
            .to_owned(),
    )
    .await?;
    m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_portable_insert AFTER INSERT ON {table} BEGIN UPDATE {table} SET portable_identity=lower(hex(randomblob(16))) WHERE {key}=NEW.{key}; END")).await?;
    for field in fields {
        let reference = format!("{field}_payload_id");
        m.alter_table(
            Table::alter()
                .table(Alias::new(table))
                .add_column(text_null(Alias::new(&reference)))
                .to_owned(),
        )
        .await?;
        let mut pending = Expr::col(Alias::new(&reference))
            .is_null()
            .and(Expr::col(Alias::new(*field)).is_not_null());
        if matches!(
            table,
            "turn_cli_runtime_binding" | "task_run_conversation_snapshot"
        ) {
            pending = pending.and(Expr::cust(
                "(frozen_manifest_id IS NULL OR frozen_manifest_id<>'')",
            ));
        }
        m.create_index(
            Index::create()
                .name(format!("{table}_{field}_portable_pending"))
                .table(Alias::new(table))
                .col(Alias::new(&reference))
                .col(Alias::new(key))
                .and_where(pending)
                .to_owned(),
        )
        .await?;
        // Trigger DDL and OLD/NEW are absent from the installed builder API.
        // Physical publication changes the explicit reference; ordinary writes
        // leave it unchanged and must invalidate the old storage/revision.
        m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_{field}_portable_logical AFTER UPDATE OF {field} ON {table} WHEN NEW.{reference} IS OLD.{reference} BEGIN UPDATE {table} SET {reference}=NULL,portable_revision=CASE WHEN portable_revision>=9223372036854775807 THEN RAISE(ABORT,'portable revision overflow') ELSE portable_revision+1 END WHERE {key}=NEW.{key}; END")).await?;
        m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_{field}_portable_replace AFTER UPDATE OF {reference} ON {table} WHEN OLD.{reference} IS NOT NULL AND OLD.{reference} IS NOT NEW.{reference} BEGIN UPDATE portable_payload SET retired=1 WHERE id=OLD.{reference}; END")).await?;
        m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_{field}_portable_delete AFTER DELETE ON {table} WHEN OLD.{reference} IS NOT NULL BEGIN UPDATE portable_payload SET retired=1 WHERE id=OLD.{reference}; END")).await?;
    }
    // Ownership/control changes fence a prepared source, but do not discard
    // an already published payload when the logical bytes are unchanged.
    m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_portable_owner AFTER UPDATE OF {} ON {table} BEGIN UPDATE {table} SET portable_revision=CASE WHEN portable_revision>=9223372036854775807 THEN RAISE(ABORT,'portable revision overflow') ELSE portable_revision+1 END WHERE {key}=NEW.{key}; END",ownership.join(","))).await?;
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
    // Raw values retain original TEXT. Retire their auxiliary metadata before
    // dropping references so rollback/reapply cannot strand live headers.
    m.get_connection()
        .execute_unprepared(&format!(
            "UPDATE portable_payload SET retired=1 WHERE raw=1 AND id IN ({refs})"
        ))
        .await?;
    m.get_connection()
        .execute_unprepared(&format!("DROP TRIGGER {table}_portable_owner"))
        .await?;
    m.get_connection()
        .execute_unprepared(&format!("DROP TRIGGER {table}_portable_insert"))
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
            .drop_column(Alias::new("portable_identity"))
            .to_owned(),
    )
    .await?;
    m.alter_table(
        Table::alter()
            .table(Alias::new(table))
            .drop_column(Alias::new("portable_revision"))
            .to_owned(),
    )
    .await?;
    Ok(())
}
