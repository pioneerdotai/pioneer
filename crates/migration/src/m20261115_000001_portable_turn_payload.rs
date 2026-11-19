//! Stage one admits INSERTs only. Existing payload bytes and dictionaries are
//! untouched. Partial index creation scans metadata once, but never decodes a
//! payload and stores no historical keys (their identity remains empty).
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
        for logical in ["turn_event", "turn_item"] {
            install(m, logical).await?;
        }
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        // Removing references cannot restore compressed TEXT for an old binary.
        // Keep the migration marker and all data instead of claiming downgrade.
        Err(DbErr::Migration(
            "portable turn payload downgrade requires a pre-switch backup and matching binary"
                .into(),
        ))
    }
}

async fn install(m: &SchemaManager<'_>, logical: &str) -> Result<(), DbErr> {
    let backing = format!("_{logical}_zstd");
    let compressed = m.has_table(&backing).await?;
    let storage = if compressed {
        backing.as_str()
    } else {
        logical
    };
    for column in [
        big_integer("portable_revision").default(0).to_owned(),
        text("portable_identity").default("").to_owned(),
        text_null("payload_payload_id").to_owned(),
    ] {
        m.alter_table(
            Table::alter()
                .table(Alias::new(storage))
                .add_column(column)
                .to_owned(),
        )
        .await?;
    }
    m.create_index(
        Index::create()
            .name(format!("{logical}_payload_portable_pending"))
            .table(Alias::new(storage))
            .col(Alias::new("payload_payload_id"))
            .col(Alias::new("id"))
            .and_where(Expr::col(Alias::new("portable_identity")).ne(""))
            .and_where(Expr::col(Alias::new("payload_payload_id")).is_null())
            .to_owned(),
    )
    .await?;
    let db = m.get_connection();
    // Trigger DDL/OLD/NEW and writable-view replacement have no SeaQuery API.
    db.execute_unprepared(&format!("CREATE TRIGGER {logical}_portable_insert AFTER INSERT ON {storage} BEGIN UPDATE {storage} SET portable_identity=lower(hex(randomblob(16))),portable_revision=0,payload_payload_id=NULL WHERE id=NEW.id; END")).await?;
    db.execute_unprepared(&format!("CREATE TRIGGER {logical}_payload_portable_logical AFTER UPDATE OF payload ON {storage} WHEN NEW.payload_payload_id IS OLD.payload_payload_id AND typeof(NEW.payload)<>'blob' AND NEW.payload IS NOT OLD.payload BEGIN UPDATE {storage} SET payload_payload_id=NULL,portable_revision=CASE WHEN portable_revision=9223372036854775807 THEN RAISE(ABORT,'portable revision overflow') ELSE portable_revision+1 END WHERE id=NEW.id; END")).await?;
    db.execute_unprepared(&format!("CREATE TRIGGER {logical}_payload_portable_replace AFTER UPDATE OF payload_payload_id ON {storage} WHEN OLD.payload_payload_id IS NOT NULL AND OLD.payload_payload_id IS NOT NEW.payload_payload_id BEGIN UPDATE portable_payload SET retired=1 WHERE id=OLD.payload_payload_id; END")).await?;
    db.execute_unprepared(&format!("CREATE TRIGGER {logical}_payload_portable_delete AFTER DELETE ON {storage} WHEN OLD.payload_payload_id IS NOT NULL BEGIN UPDATE portable_payload SET retired=1 WHERE id=OLD.payload_payload_id; END")).await?;
    let fields = if logical == "turn_event" {
        "id,thread_id,turn_id,sequence,event_type,idempotency_key,created_at"
    } else {
        "id,turn_id,item_id,item_type,status,active_attempt_id,active_attempt_status,active_attempt_number,created_at"
    };
    let changed = fields
        .split(',')
        .map(|f| format!("NEW.{f} IS NOT OLD.{f}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    db.execute_unprepared(&format!("CREATE TRIGGER {logical}_portable_owner AFTER UPDATE OF {fields} ON {storage} WHEN {changed} BEGIN UPDATE {storage} SET portable_revision=CASE WHEN portable_revision=9223372036854775807 THEN RAISE(ABORT,'portable revision overflow') ELSE portable_revision+1 END WHERE id=NEW.id; END")).await?;
    // Adapt only the payload-change term of existing domain triggers. Their
    // original names, bodies, field-change predicates and root fences survive.
    let triggers = db.query_all_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND (name LIKE 'compaction_%' OR name='read_model_repair_turn_item_update') AND sql LIKE '%typeof(NEW.payload)%'",
        [storage.into()],
    )).await?;
    for trigger in triggers {
        let name: String = trigger.try_get("", "name")?;
        let sql: String = trigger.try_get("", "sql")?;
        let adjusted = sql.replace(
            "typeof(NEW.payload) <> 'blob'",
            "typeof(NEW.payload) <> 'blob' AND NEW.payload_payload_id IS OLD.payload_payload_id",
        ).replace("typeof(NEW.payload) = 'text'", "typeof(NEW.payload) = 'text' AND NEW.payload_payload_id IS OLD.payload_payload_id");
        if adjusted != sql {
            db.execute_unprepared(&format!("DROP TRIGGER {}", quoted(&name)))
                .await?;
            db.execute_unprepared(&adjusted).await?;
        }
    }
    if compressed {
        // A second metadata-only installation scan bounds legacy discovery:
        // admitted raw rows cannot become an ever-growing EOF scan for Zstd.
        m.create_index(
            Index::create()
                .name(format!("{logical}_payload_legacy_pending"))
                .table(Alias::new(storage))
                .col(Alias::new("_payload_dict"))
                .and_where(Expr::col(Alias::new("portable_identity")).eq(""))
                .and_where(Expr::col(Alias::new("_payload_dict")).is_null())
                .to_owned(),
        )
        .await?;
        rebuild_view(m, logical, storage).await?;
    }
    Ok(())
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn rebuild_view(m: &SchemaManager<'_>, logical: &str, storage: &str) -> Result<(), DbErr> {
    let db = m.get_connection();
    // PRAGMA/schema SQL is metadata introspection, unavailable in SchemaManager.
    let columns = db
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            format!("PRAGMA table_info({})", quoted(storage)),
        ))
        .await?;
    let names = columns
        .iter()
        .map(|r| r.try_get::<String>("", "name"))
        .collect::<Result<Vec<_>, _>>()?;
    let visible = names
        .iter()
        .filter(|n| n.as_str() != "_payload_dict")
        .collect::<Vec<_>>();
    let saved = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
            [logical.into()],
        ))
        .await?;
    db.execute_unprepared(&format!("DROP VIEW {}", quoted(logical)))
        .await?;
    let projection = visible
        .iter()
        .map(|n| {
            if n.as_str() == "payload" {
                "zstd_decompress_col(payload,1,_payload_dict,1) AS payload".to_owned()
            } else {
                quoted(n)
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    db.execute_unprepared(&format!(
        "CREATE VIEW {} AS SELECT {projection} FROM {}",
        quoted(logical),
        quoted(storage)
    ))
    .await?;
    let columns = visible
        .iter()
        .map(|n| quoted(n))
        .collect::<Vec<_>>()
        .join(",");
    let values = visible
        .iter()
        .map(|n| match n.as_str() {
            "portable_identity" => "''".to_owned(),
            "portable_revision" => "0".to_owned(),
            "payload_payload_id" => "NULL".to_owned(),
            _ => format!("NEW.{}", quoted(n)),
        })
        .collect::<Vec<_>>()
        .join(",");
    db.execute_unprepared(&format!("CREATE TRIGGER {logical}_insert_trigger INSTEAD OF INSERT ON {logical} BEGIN INSERT INTO {storage}({columns},_payload_dict) VALUES({values},NULL); END")).await?;
    // One backing UPDATE preserves the reference/payload publication boundary;
    // per-column triggers would turn it into unrelated logical replacements.
    let assignments = visible
        .iter()
        .map(|n| {
            if n.as_str() == "payload" {
                "payload=CASE WHEN NEW.payload IS OLD.payload THEN payload ELSE NEW.payload END"
                    .to_owned()
            } else {
                format!("{0}=NEW.{0}", quoted(n))
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    db.execute_unprepared(&format!("CREATE TRIGGER {logical}_portable_update_trigger INSTEAD OF UPDATE ON {logical} BEGIN UPDATE {storage} SET {assignments},_payload_dict=CASE WHEN NEW.payload IS NOT OLD.payload THEN NULL ELSE _payload_dict END WHERE id=OLD.id; END")).await?;
    db.execute_unprepared(&format!("CREATE TRIGGER {logical}_delete_trigger INSTEAD OF DELETE ON {logical} BEGIN DELETE FROM {storage} WHERE id=OLD.id; END")).await?;
    for row in saved {
        let name: String = row.try_get("", "name")?;
        if name == format!("{logical}_insert_trigger")
            || name == format!("{logical}_delete_trigger")
            || name.starts_with(&format!("{logical}_update_"))
        {
            continue;
        }
        let sql: String = row.try_get("", "sql")?;
        db.execute_unprepared(&sql).await?;
    }
    Ok(())
}
