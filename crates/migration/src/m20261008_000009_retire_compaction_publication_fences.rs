use sea_orm::{ConnectionTrait, DbBackend, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        // Finished summaries retain their consumed source versions. Global
        // publication counters are no longer used; retire their write overhead
        // on both plain and sqlite-zstd physical tables. Keep the historical
        // tables and rows to avoid rewriting historical migration state.
        let triggers = db.query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name FROM sqlite_schema WHERE type='trigger' AND name GLOB 'compaction_publication_*' ORDER BY name LIMIT 257".to_owned(),
        )).await?;
        if triggers.len() > 256 {
            return Err(DbErr::Migration(
                "publication trigger retirement exceeds its bounded quantum".into(),
            ));
        }
        for row in triggers {
            let name: String = row.try_get("", "name")?;
            db.execute_unprepared(&format!(
                "DROP TRIGGER IF EXISTS \"{}\"",
                name.replace('"', "\"\"")
            ))
            .await?;
        }
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        // Once retired, these generations no longer describe intervening
        // writes and cannot safely be reused as publication proofs.
        Err(DbErr::Migration(
            "retired compaction publication generations cannot be restored safely".into(),
        ))
    }
}
