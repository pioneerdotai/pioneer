use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Derived lifetime metadata only: no media, payload, new history format
        // or independent lease. An absent/incomplete index retains its scope.
        manager
            .create_table(
                Table::create()
                    .table("html_preview_frozen_index")
                    .col(text("manifest_id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("identity_sha256"))
                    .col(integer("message_count"))
                    .col(integer("ready"))
                    .col(integer("expired"))
                    .col(
                        integer("next_ordinal")
                            .default(0)
                            .check(Expr::col("next_ordinal").gte(0)),
                    )
                    .col(
                        integer("complete")
                            .default(0)
                            .check(Expr::col("complete").is_in([0, 1])),
                    )
                    .col(
                        integer("unknown")
                            .default(0)
                            .check(Expr::col("unknown").is_in([0, 1])),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table("html_preview_index_backfill")
                    .col(
                        integer("singleton")
                            .primary_key()
                            .check(Expr::col("singleton").eq(1)),
                    )
                    .col(integer("complete").check(Expr::col("complete").is_in([0, 1])))
                    .col(
                        integer("inputs_complete")
                            .default(0)
                            .check(Expr::col("inputs_complete").is_in([0, 1])),
                    )
                    .to_owned(),
            )
            .await?;
        let db = manager.get_connection();
        db.execute(
            &Query::insert()
                .into_table("html_preview_index_backfill")
                .columns(["singleton", "complete"])
                .values_panic([1.into(), 0.into()])
                .to_owned(),
        )
        .await?;
        manager
            .create_index(
                Index::create()
                    .name("html_preview_frozen_unready")
                    .table("html_preview_frozen_index")
                    .col("workspace_id")
                    .and_where(Expr::col("expired").eq(0))
                    .and_where(
                        Expr::col("ready")
                            .ne(1)
                            .or(Expr::col("complete").ne(1))
                            .or(Expr::col("unknown").ne(0)),
                    )
                    .to_owned(),
            )
            .await?;
        // SQLite triggers have no SeaQuery builder. They maintain unsafe
        // markers with foreign_keys=OFF; no cascade or FK enforcement is assumed.
        // New and changing headers publish an unsafe marker before the writer
        // is released. Legacy headers are covered by the bounded backfill gate.
        db.execute_unprepared(r#"CREATE TRIGGER html_preview_header_insert AFTER INSERT ON compaction_frozen_history
BEGIN
    INSERT INTO html_preview_frozen_index(manifest_id,workspace_id,identity_sha256,message_count,ready,expired) VALUES(NEW.id,NEW.workspace_id,NEW.identity_sha256,NEW.message_count,NEW.ready,NEW.expired)
    ON CONFLICT(manifest_id)
    DO UPDATE SET
        workspace_id=excluded.workspace_id,
        identity_sha256=excluded.identity_sha256,
        message_count=excluded.message_count,
        ready=excluded.ready,
        expired=excluded.expired,
        next_ordinal=0,
        complete=0,
        unknown=0;
END"#).await?;
        db.execute_unprepared(r#"CREATE TRIGGER html_preview_header_update AFTER UPDATE OF id,workspace_id,identity_sha256,message_count,ready,expired ON compaction_frozen_history
BEGIN
    INSERT INTO html_preview_frozen_index(manifest_id,workspace_id,identity_sha256,message_count,ready,expired) VALUES(NEW.id,NEW.workspace_id,NEW.identity_sha256,NEW.message_count,NEW.ready,NEW.expired)
    ON CONFLICT(manifest_id)
    DO UPDATE SET
        workspace_id=excluded.workspace_id,
        identity_sha256=excluded.identity_sha256,
        message_count=excluded.message_count,
        ready=excluded.ready,
        expired=excluded.expired,
        next_ordinal=CASE WHEN OLD.id=NEW.id AND OLD.identity_sha256=NEW.identity_sha256 AND OLD.message_count=NEW.message_count THEN next_ordinal ELSE 0 END,
        complete=CASE WHEN OLD.id=NEW.id AND OLD.identity_sha256=NEW.identity_sha256 AND OLD.message_count=NEW.message_count THEN complete ELSE 0 END;
    UPDATE html_preview_frozen_index SET expired=1 WHERE manifest_id=OLD.id AND OLD.id<>NEW.id;
END"#).await?;
        db.execute_unprepared(
            r#"CREATE TRIGGER html_preview_header_delete AFTER DELETE ON compaction_frozen_history
BEGIN
    UPDATE html_preview_frozen_index SET expired=1 WHERE manifest_id=OLD.id;
END"#,
        )
        .await?;
        db.execute_unprepared(
            r#"CREATE TRIGGER html_preview_index_retain BEFORE DELETE ON html_preview_frozen_index
BEGIN
    SELECT RAISE(ABORT,'HTML preview lifetime marker retained');
END"#,
        )
        .await?;
        manager
            .create_table(
                Table::create()
                    .table("html_preview_frozen_turn")
                    .col(text("manifest_id"))
                    .col(text("turn_id"))
                    .primary_key(Index::create().col("manifest_id").col("turn_id"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("html_preview_frozen_turn_lookup")
                    .table("html_preview_frozen_turn")
                    .col("turn_id")
                    .col("manifest_id")
                    .to_owned(),
            )
            .await?;
        // Compact receipts only. Frozen references and identity stay immutable.
        manager
            .create_table(
                Table::create()
                    .table("html_preview_frozen_result")
                    .col(text("manifest_id"))
                    .col(integer("ordinal"))
                    .col(text("reference_json"))
                    .col(text("turn_id"))
                    .col(text("source_id"))
                    .col(text("source_version"))
                    .col(text_null("archive_wire_sha256"))
                    .primary_key(Index::create().col("manifest_id").col("ordinal"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("html_preview_frozen_result_replay")
                    .table("html_preview_frozen_result")
                    .col("source_id")
                    .col("source_version")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("artifact_html_preview_discovery")
                    .table("artifact")
                    .col("id")
                    .and_where(Expr::col("created_by_kind").eq("tool"))
                    .and_where(Expr::col("created_by_actor_id").eq("html_preview"))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        for trigger in [
            "html_preview_header_insert",
            "html_preview_header_update",
            "html_preview_header_delete",
            "html_preview_index_retain",
        ] {
            db.execute_unprepared(&format!("DROP TRIGGER {trigger}"))
                .await?;
        }
        manager
            .drop_index(
                Index::drop()
                    .name("artifact_html_preview_discovery")
                    .to_owned(),
            )
            .await?;
        for table in [
            "html_preview_index_backfill",
            "html_preview_frozen_result",
            "html_preview_frozen_turn",
            "html_preview_frozen_index",
        ] {
            manager
                .drop_table(Table::drop().table(table).to_owned())
                .await?;
        }
        Ok(())
    }
}
