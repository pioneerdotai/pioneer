use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        // Cleanup must not observe the new column before every valid legacy
        // frozen reference has been retained.
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("compaction_source_revision"))
                    .add_column(
                        ColumnDef::new(Alias::new("frozen_revision"))
                            .integer()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        // Read the logical view so manifests that already use shared physical
        // ranges receive exactly the same retention as unshared manifests.
        // Invalid or stale references are deliberately not repaired here: only
        // an exact, still-present revision can be made durable by retention.
        manager
            .get_connection()
            .execute_unprepared(
                r#"
WITH retained(source_id, turn_id, source_version) AS (
    SELECT json_extract(source.value, '$.id'),
           substr(json_extract(source.value, '$.scope'), 9),
           json_extract(source.value, '$.version')
      FROM compaction_frozen_message AS message,
           json_each(
               CASE WHEN json_valid(message.reference_json)
                    THEN message.reference_json ELSE '{}' END,
               '$.sources'
           ) AS source
     WHERE json_extract(source.value, '$.scope') LIKE 'context:%'
    UNION
    SELECT json_extract(message.reference_json, '$.replay_source.id'),
           substr(json_extract(message.reference_json, '$.replay_source.scope'), 9),
           json_extract(message.reference_json, '$.replay_source.version')
      FROM compaction_frozen_message AS message
     WHERE json_valid(message.reference_json)
       AND json_extract(message.reference_json, '$.replay_source.scope') LIKE 'context:%'
)
UPDATE compaction_source_revision AS revision
   SET frozen_revision = revision.revision
 WHERE revision.present = 1
   AND EXISTS (
       SELECT 1
         FROM retained
        WHERE retained.source_id = revision.source_id
          AND retained.turn_id = revision.turn_id
          AND retained.source_version = 'revision:' || revision.revision
   )
"#,
            )
            .await?;
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "frozen context retention cannot be removed safely".into(),
        ))
    }
}
