//! Schema only: no legacy scan, backfill, expiry or physical cleanup.
use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_query::{Expr, ExprTrait};
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        m.alter_table(
            Table::alter()
                .table(Alias::new("compaction_checkpoint"))
                .add_column(
                    ColumnDef::new(Alias::new("proof_version"))
                        .integer()
                        .not_null()
                        .default(0)
                        .check(Expr::col(Alias::new("proof_version")).is_in([0, 1])),
                )
                .to_owned(),
        )
        .await?;
        m.alter_table(
            Table::alter()
                .table(Alias::new("compaction_frozen_history"))
                .add_column(
                    ColumnDef::new(Alias::new("expired"))
                        .integer()
                        .not_null()
                        .default(0)
                        .check(Expr::col(Alias::new("expired")).is_in([0, 1])),
                )
                .to_owned(),
        )
        .await?;
        // SQLite row triggers have no SeaQuery builder. They enforce local
        // guards even on the production FK OFF writer. Views below encode the
        // SQLite direct/shared UNION layout and logical bounds.
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER frozen_expired_monotonic BEFORE UPDATE OF expired ON compaction_frozen_history WHEN OLD.expired=1 AND NEW.expired<>1 BEGIN SELECT RAISE(ABORT, 'frozen expiry is irreversible'); END"#).await?;
        m.alter_table(
            Table::alter()
                .table(Alias::new("task_run_conversation_snapshot"))
                .add_column(
                    ColumnDef::new(Alias::new("frozen_manifest_id"))
                        .text()
                        .null()
                        .default(""),
                )
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("task_run_conversation_snapshot_frozen_root")
                .table(Alias::new("task_run_conversation_snapshot"))
                .col("frozen_manifest_id")
                .col("workspace_id")
                .col("task_id")
                .to_owned(),
        )
        .await?;
        m.alter_table(
            Table::alter()
                .table(Alias::new("turn_runtime_snapshot"))
                .add_column(
                    ColumnDef::new(Alias::new("frozen_manifest_id"))
                        .text()
                        .null()
                        .default(""),
                )
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("turn_runtime_snapshot_frozen_root")
                .table(Alias::new("turn_runtime_snapshot"))
                .col("frozen_manifest_id")
                .col("workspace_id")
                .to_owned(),
        )
        .await?;
        m.alter_table(
            Table::alter()
                .table(Alias::new("thread_cli_runtime_binding"))
                .add_column(
                    ColumnDef::new(Alias::new("frozen_manifest_id"))
                        .text()
                        .null()
                        .default(""),
                )
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("thread_cli_runtime_binding_frozen_root")
                .table(Alias::new("thread_cli_runtime_binding"))
                .col("frozen_manifest_id")
                .col("workspace_id")
                .to_owned(),
        )
        .await?;
        m.alter_table(
            Table::alter()
                .table(Alias::new("turn_cli_runtime_binding"))
                .add_column(
                    ColumnDef::new(Alias::new("frozen_manifest_id"))
                        .text()
                        .null()
                        .default(""),
                )
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("turn_cli_runtime_binding_frozen_root")
                .table(Alias::new("turn_cli_runtime_binding"))
                .col("frozen_manifest_id")
                .col("workspace_id")
                .to_owned(),
        )
        .await?;
        m.create_table(
            Table::create()
                .table(Alias::new("compaction_checkpoint_replay_alias"))
                .col(
                    ColumnDef::new(Alias::new("checkpoint_id"))
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(Alias::new("covered_thread"))
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(Alias::new("covered_scope"))
                        .text()
                        .not_null(),
                )
                .col(ColumnDef::new(Alias::new("covered_id")).text().not_null())
                .col(
                    ColumnDef::new(Alias::new("covered_version"))
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(Alias::new("replay_thread"))
                        .text()
                        .not_null(),
                )
                .col(ColumnDef::new(Alias::new("replay_scope")).text().not_null())
                .col(ColumnDef::new(Alias::new("replay_id")).text().not_null())
                .col(
                    ColumnDef::new(Alias::new("replay_version"))
                        .text()
                        .not_null(),
                )
                .col(ColumnDef::new(Alias::new("tool_item_id")).text().null())
                .foreign_key(
                    ForeignKey::create()
                        .from(
                            Alias::new("compaction_checkpoint_replay_alias"),
                            Alias::new("checkpoint_id"),
                        )
                        .to(Alias::new("compaction_checkpoint"), Alias::new("id")),
                )
                .to_owned(),
        )
        .await?;
        // SeaQuery 1.0's SQLite index builder panics on expression columns.
        // Both expressions are needed to distinguish NULL from an empty ID
        // while rejecting duplicate aliases with either nullable identity.
        m.get_connection().execute_unprepared(r#"CREATE UNIQUE INDEX checkpoint_replay_alias_identity ON compaction_checkpoint_replay_alias(checkpoint_id,covered_thread,covered_scope,covered_id,covered_version,replay_thread,replay_scope,replay_id,replay_version, (tool_item_id IS NULL), COALESCE(tool_item_id,''))"#).await?;
        m.create_table(
            Table::create()
                .table(Alias::new("compaction_checkpoint_event_input"))
                .col(
                    ColumnDef::new(Alias::new("checkpoint_id"))
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(Alias::new("source_thread"))
                        .text()
                        .not_null(),
                )
                .col(ColumnDef::new(Alias::new("source_scope")).text().not_null())
                .col(ColumnDef::new(Alias::new("source_id")).text().not_null())
                .col(
                    ColumnDef::new(Alias::new("source_version"))
                        .text()
                        .not_null(),
                )
                .col(ColumnDef::new(Alias::new("role")).text().not_null().check(
                    Expr::col(Alias::new("role")).is_in(["authoritative", "deleted", "input_copy"]),
                ))
                .primary_key(
                    Index::create()
                        .col("checkpoint_id")
                        .col("source_thread")
                        .col("source_scope")
                        .col("source_id")
                        .col("source_version"),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(
                            Alias::new("compaction_checkpoint_event_input"),
                            Alias::new("checkpoint_id"),
                        )
                        .to(Alias::new("compaction_checkpoint"), Alias::new("id")),
                )
                .to_owned(),
        )
        .await?;
        m.create_table(
            Table::create()
                .table(Alias::new("compaction_checkpoint_import"))
                .col(
                    ColumnDef::new(Alias::new("checkpoint_id"))
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(Alias::new("import_ordinal"))
                        .integer()
                        .not_null()
                        .check(Expr::col(Alias::new("import_ordinal")).gte(0)),
                )
                .col(
                    ColumnDef::new(Alias::new("message_ordinal"))
                        .integer()
                        .not_null()
                        .check(Expr::col(Alias::new("message_ordinal")).gte(0)),
                )
                .col(
                    ColumnDef::new(Alias::new("target_source_thread"))
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(Alias::new("context_thread"))
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(Alias::new("target_checkpoint_json"))
                        .text()
                        .null(),
                )
                .col(ColumnDef::new(Alias::new("proof_json")).text().not_null())
                .col(
                    ColumnDef::new(Alias::new("bytes"))
                        .integer()
                        .not_null()
                        .check(Expr::col(Alias::new("bytes")).gte(0)),
                )
                .primary_key(Index::create().col("checkpoint_id").col("import_ordinal"))
                .foreign_key(
                    ForeignKey::create()
                        .from(
                            Alias::new("compaction_checkpoint_import"),
                            Alias::new("checkpoint_id"),
                        )
                        .to(Alias::new("compaction_checkpoint"), Alias::new("id")),
                )
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("checkpoint_proof_operation")
                .table(Alias::new("compaction_checkpoint"))
                .col("operation_id")
                .col("proof_version")
                .col("status")
                .to_owned(),
        )
        .await?;
        // A shared input can bind arbitrarily many terminal operations. This
        // derived generation avoids parsing every saved runner or holding N
        // writer reservations in the final indexed expiry guard. Not authority:
        // every operation/runner mutation invalidates it atomically.
        m.alter_table(
            Table::alter()
                .table(Alias::new("compaction_operation_projection"))
                .add_column(
                    ColumnDef::new(Alias::new("terminal_generation"))
                        .integer()
                        .null()
                        .check(
                            Expr::col(Alias::new("terminal_generation"))
                                .is_null()
                                .or(Expr::col(Alias::new("terminal_generation")).gte(0)),
                        ),
                )
                .to_owned(),
        )
        .await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER projection_terminal_reset BEFORE UPDATE OF operation_id,manifest_id,identity_sha256,imports_sha256,import_count ON compaction_operation_projection BEGIN UPDATE compaction_operation_projection SET terminal_generation=NULL WHERE operation_id=OLD.operation_id; END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER operation_terminal_reset BEFORE UPDATE OF id,owner,status ON compaction_operation BEGIN UPDATE compaction_operation_projection SET terminal_generation=NULL WHERE operation_id=OLD.id; END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER runner_terminal_reset_update BEFORE UPDATE OF operation_id,generation,state ON compaction_runner_state BEGIN UPDATE compaction_operation_projection SET terminal_generation=NULL WHERE operation_id=OLD.operation_id; END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER runner_terminal_reset_insert BEFORE INSERT ON compaction_runner_state BEGIN UPDATE compaction_operation_projection SET terminal_generation=NULL WHERE operation_id=NEW.operation_id; END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER runner_terminal_reset_delete BEFORE DELETE ON compaction_runner_state BEGIN UPDATE compaction_operation_projection SET terminal_generation=NULL WHERE operation_id=OLD.operation_id; END"#).await?;
        m.create_index(
            Index::create()
                .name("projection_frozen_origin")
                .table(Alias::new("compaction_operation_projection"))
                .col("manifest_id")
                .col("operation_id")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("task_output_frozen_root")
                .table(Alias::new("compaction_task_output"))
                .col("manifest_id")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("checkpoint_incoming_previous")
                .table(Alias::new("compaction_checkpoint"))
                .col("previous")
                .col("id")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("coverage_incoming_checkpoint")
                .table(Alias::new("compaction_coverage"))
                .col("source_id")
                .col("source_scope")
                .col("source_version")
                .col("checkpoint_id")
                .to_owned(),
        )
        .await?;
        // Existing pending(kind,manifest) index cannot seek reverse candidates.
        // Scoped missing-origin lookup must not scan unrelated contexts or
        // sealed checkpoints. Existing owner/id and operation/proof indexes
        // do not serve workspace -> owner -> unproved checkpoint discovery.
        m.create_index(
            Index::create()
                .name("context_frozen_scope")
                .table(Alias::new("compaction_context"))
                .col("workspace_id")
                .col("owner")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("checkpoint_unproved_owner")
                .table(Alias::new("compaction_checkpoint"))
                .col("owner")
                .col("proof_version")
                .col("operation_id")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("frozen_layout_candidate")
                .table(Alias::new("compaction_frozen_layout"))
                .col("candidate")
                .col("kind")
                .col("manifest_id")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("frozen_expiry_discovery")
                .table(Alias::new("compaction_frozen_history"))
                .col("expired")
                .col("ready")
                .col("id")
                .to_owned(),
        )
        .await?;
        m.get_connection()
            .execute_unprepared("DROP VIEW compaction_frozen_message")
            .await?;
        m.get_connection().execute_unprepared(r#"CREATE VIEW compaction_frozen_message AS
          SELECT d.manifest_id, d.ordinal, d.reference_json, d.bytes FROM compaction_frozen_message_data d
          JOIN compaction_frozen_history h ON h.id=d.manifest_id
          WHERE h.expired=0 AND h.ready IN (0,1) AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal BETWEEN 0 AND h.message_count AND h.next_import BETWEEN 0 AND h.import_count AND (h.ready=0 OR (h.next_ordinal=h.message_count AND h.next_import=h.import_count)) AND d.ordinal>=0 AND d.ordinal<CASE h.ready WHEN 1 THEN h.message_count ELSE h.next_ordinal END
          AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=0 AND l.active=1)
          UNION ALL
          SELECT s.manifest_id, d.ordinal, d.reference_json, d.bytes FROM compaction_frozen_span s
          JOIN compaction_frozen_message_data d ON d.manifest_id=s.source_manifest AND d.ordinal>=s.start AND d.ordinal<s.end
          JOIN compaction_frozen_layout l ON l.manifest_id=s.manifest_id AND l.kind=s.kind
          JOIN compaction_frozen_history h ON h.id=s.manifest_id
          WHERE s.kind=0 AND l.active=1 AND h.expired=0 AND h.ready IN (0,1) AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal BETWEEN 0 AND h.message_count AND h.next_import BETWEEN 0 AND h.import_count AND (h.ready=0 OR (h.next_ordinal=h.message_count AND h.next_import=h.import_count)) AND d.ordinal>=0 AND d.ordinal<CASE h.ready WHEN 1 THEN h.message_count ELSE h.next_ordinal END"#).await?;
        m.get_connection()
            .execute_unprepared("DROP VIEW compaction_frozen_import")
            .await?;
        m.get_connection().execute_unprepared(r#"CREATE VIEW compaction_frozen_import AS
          SELECT d.manifest_id, d.ordinal, d.message_ordinal, d.source_scope, d.source_id, d.source_version, d.source_thread, d.proof_json, d.bytes FROM compaction_frozen_import_data d
          JOIN compaction_frozen_history h ON h.id=d.manifest_id
          WHERE h.expired=0 AND h.ready IN (0,1) AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal BETWEEN 0 AND h.message_count AND h.next_import BETWEEN 0 AND h.import_count AND (h.ready=0 OR (h.next_ordinal=h.message_count AND h.next_import=h.import_count)) AND d.ordinal>=0 AND d.ordinal<CASE h.ready WHEN 1 THEN h.import_count ELSE h.next_import END AND d.message_ordinal>=0 AND d.message_ordinal<CASE h.ready WHEN 1 THEN h.message_count ELSE h.next_ordinal END
          AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=1 AND l.active=1)
          UNION ALL
          SELECT s.manifest_id, d.ordinal, d.message_ordinal, d.source_scope, d.source_id, d.source_version, d.source_thread, d.proof_json, d.bytes FROM compaction_frozen_span s
          JOIN compaction_frozen_import_data d ON d.manifest_id=s.source_manifest AND d.ordinal>=s.start AND d.ordinal<s.end
          JOIN compaction_frozen_layout l ON l.manifest_id=s.manifest_id AND l.kind=s.kind
          JOIN compaction_frozen_history h ON h.id=s.manifest_id
          WHERE s.kind=1 AND l.active=1 AND h.expired=0 AND h.ready IN (0,1) AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal BETWEEN 0 AND h.message_count AND h.next_import BETWEEN 0 AND h.import_count AND (h.ready=0 OR (h.next_ordinal=h.message_count AND h.next_import=h.import_count)) AND d.ordinal>=0 AND d.ordinal<CASE h.ready WHEN 1 THEN h.import_count ELSE h.next_import END AND d.message_ordinal>=0 AND d.message_ordinal<CASE h.ready WHEN 1 THEN h.message_count ELSE h.next_ordinal END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER compaction_task_output_available_insert BEFORE INSERT ON compaction_task_output WHEN NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count) BEGIN SELECT RAISE(ABORT,'frozen root unavailable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER compaction_task_output_available_update BEFORE UPDATE OF manifest_id ON compaction_task_output WHEN NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count) BEGIN SELECT RAISE(ABORT,'frozen root unavailable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER compaction_operation_projection_available_insert BEFORE INSERT ON compaction_operation_projection WHEN NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count) BEGIN SELECT RAISE(ABORT,'frozen root unavailable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER compaction_operation_projection_available_update BEFORE UPDATE OF manifest_id ON compaction_operation_projection WHEN NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count) BEGIN SELECT RAISE(ABORT,'frozen root unavailable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER task_run_conversation_snapshot_verified_insert BEFORE INSERT ON task_run_conversation_snapshot WHEN NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)) BEGIN SELECT RAISE(ABORT,'unverified or unavailable frozen root'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER task_run_conversation_snapshot_verified_update BEFORE UPDATE OF history_json,run_id,task_id,workspace_id,conversation_thread_id,frozen_manifest_id ON task_run_conversation_snapshot WHEN NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)) BEGIN SELECT RAISE(ABORT,'unverified or unavailable frozen root'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER turn_runtime_snapshot_verified_insert BEFORE INSERT ON turn_runtime_snapshot WHEN NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)) BEGIN SELECT RAISE(ABORT,'unverified or unavailable frozen root'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER turn_runtime_snapshot_verified_update BEFORE UPDATE OF history_json,turn_id,thread_id,workspace_id,frozen_manifest_id ON turn_runtime_snapshot WHEN NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)) BEGIN SELECT RAISE(ABORT,'unverified or unavailable frozen root'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER thread_cli_runtime_binding_verified_insert BEFORE INSERT ON thread_cli_runtime_binding WHEN NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)) BEGIN SELECT RAISE(ABORT,'unverified or unavailable frozen root'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER thread_cli_runtime_binding_verified_update BEFORE UPDATE OF resume_cursor_json,thread_id,workspace_id,frozen_manifest_id ON thread_cli_runtime_binding WHEN NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)) BEGIN SELECT RAISE(ABORT,'unverified or unavailable frozen root'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER turn_cli_runtime_binding_verified_insert BEFORE INSERT ON turn_cli_runtime_binding WHEN NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)) BEGIN SELECT RAISE(ABORT,'unverified or unavailable frozen root'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER turn_cli_runtime_binding_verified_update BEFORE UPDATE OF input_mapping_json,turn_id,thread_id,workspace_id,continuation_thread_id,frozen_manifest_id ON turn_cli_runtime_binding WHEN NEW.frozen_manifest_id='' OR (NEW.frozen_manifest_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.frozen_manifest_id AND h.workspace_id=NEW.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)) BEGIN SELECT RAISE(ABORT,'unverified or unavailable frozen root'); END"#).await?;
        // Task projection upserts can change status without replacing snapshots.
        // Completed -> retaining must never make expired old inputs roots again.
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER task_frozen_roots_insert BEFORE INSERT ON task WHEN NEW.status<>'completed' AND NOT EXISTS(SELECT 1 FROM task t WHERE t.id=NEW.id AND t.workspace_id=NEW.workspace_id AND t.status<>'completed') AND EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN compaction_frozen_history h ON h.id=s.frozen_manifest_id WHERE s.task_id=NEW.id AND (s.workspace_id<>NEW.workspace_id OR s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NOT NULL AND (h.id IS NULL OR h.expired<>0 OR h.ready<>1)))) BEGIN SELECT RAISE(ABORT,'frozen_input_expired or unverified Task roots'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER task_frozen_roots_update BEFORE UPDATE OF status,workspace_id ON task WHEN NEW.status<>'completed' AND (OLD.status='completed' OR OLD.workspace_id<>NEW.workspace_id) AND EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN compaction_frozen_history h ON h.id=s.frozen_manifest_id WHERE s.task_id=NEW.id AND (s.workspace_id<>NEW.workspace_id OR s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NOT NULL AND (h.id IS NULL OR h.expired<>0 OR h.ready<>1)))) BEGIN SELECT RAISE(ABORT,'frozen_input_expired or unverified Task roots'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER layout_available_insert BEFORE INSERT ON compaction_frozen_layout WHEN NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0) OR (NEW.candidate IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h JOIN compaction_frozen_history c ON c.id=NEW.manifest_id WHERE h.id=NEW.candidate AND h.expired=0 AND h.ready=1 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count AND h.workspace_id=c.workspace_id AND h.owner_thread=c.owner_thread)) BEGIN SELECT RAISE(ABORT,'layout logical dependency unavailable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER layout_available_dependency BEFORE UPDATE OF manifest_id,kind,candidate,active ON compaction_frozen_layout WHEN (NEW.manifest_id<>OLD.manifest_id OR NEW.kind<>OLD.kind OR NEW.candidate IS NOT OLD.candidate OR (NEW.active=1 AND OLD.active<>1)) AND (NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0) OR (NEW.candidate IS NOT NULL AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h JOIN compaction_frozen_history c ON c.id=NEW.manifest_id WHERE h.id=NEW.candidate AND h.expired=0 AND h.ready=1 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count AND h.workspace_id=c.workspace_id AND h.owner_thread=c.owner_thread))) BEGIN SELECT RAISE(ABORT,'layout logical dependency unavailable'); END"#).await?;
        // Physical backing can be expired. Availability belongs to logical H.
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER span_available_insert BEFORE INSERT ON compaction_frozen_span WHEN NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=NEW.manifest_id AND h.expired=0) OR (EXISTS(SELECT 1 FROM compaction_frozen_layout l JOIN compaction_frozen_history h ON h.id=l.manifest_id WHERE l.manifest_id=NEW.manifest_id AND l.kind=NEW.kind AND l.active=1 AND h.ready=1) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s WHERE s.manifest_id=NEW.manifest_id AND s.kind=NEW.kind AND s.start=NEW.start AND s.end=NEW.end AND s.source_manifest=NEW.source_manifest)) BEGIN SELECT RAISE(ABORT,'published span immutable or logical input expired'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER span_published_update BEFORE UPDATE ON compaction_frozen_span WHEN EXISTS(SELECT 1 FROM compaction_frozen_layout l JOIN compaction_frozen_history h ON h.id=l.manifest_id WHERE l.manifest_id IN (OLD.manifest_id,NEW.manifest_id) AND l.kind IN (OLD.kind,NEW.kind) AND (h.expired=1 OR (l.active=1 AND h.ready=1))) BEGIN SELECT RAISE(ABORT,'published span immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER span_published_delete BEFORE DELETE ON compaction_frozen_span WHEN EXISTS(SELECT 1 FROM compaction_frozen_layout l JOIN compaction_frozen_history h ON h.id=l.manifest_id WHERE l.manifest_id=OLD.manifest_id AND l.kind=OLD.kind AND l.active=1 AND h.ready=1) BEGIN SELECT RAISE(ABORT,'published span retained'); END"#).await?;
        // Exact historical owner lookup needs indexed first/last thread seeks
        // for one source, even with dense duplicate ordinals. Extend the
        // existing source lookup index rather than add a redundant prefix.
        m.drop_index(
            Index::drop()
                .name("compaction_manifest_source")
                .if_exists()
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("compaction_manifest_source")
                .table(Alias::new("compaction_manifest"))
                .col("operation_id")
                .col("reference_only")
                .col("source_scope")
                .col("source_id")
                .col("source_version")
                .col("source_thread")
                .to_owned(),
        )
        .await?;
        // next_portion is the existing candidate write-set boundary. Closing
        // a portion freezes its identity/coverage/selected manifest ownership
        // before paged preparation; exact candidate retries remain allowed.
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER operation_portion_monotonic BEFORE UPDATE OF next_portion ON compaction_operation WHEN NEW.next_portion<OLD.next_portion BEGIN SELECT RAISE(ABORT,'candidate portion boundary is monotonic'); END"#).await?;
        // The only snapshot rewrite is deadline resume: its repository changes
        // only admission.deadline_ms, and atomically advances runner generation.
        // Paged readers/seal fence that control transition, including ABA.
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER operation_frozen_admission BEFORE UPDATE OF snapshot,fingerprint ON compaction_operation WHEN OLD.next_portion>0 AND NOT (OLD.status='failed' AND OLD.outcome IS 'deadline' AND NEW.status='running' AND NEW.outcome IS NULL AND NEW.deadline_ms>OLD.deadline_ms AND NEW.fingerprint=OLD.fingerprint AND EXISTS(SELECT 1 FROM compaction_runner_state WHERE operation_id=OLD.id)) BEGIN SELECT RAISE(ABORT,'prepared admission immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER checkpoint_closed_portion_insert BEFORE INSERT ON compaction_checkpoint WHEN NEW.portion<(SELECT next_portion FROM compaction_operation WHERE id=NEW.operation_id) AND NOT EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id=NEW.id AND operation_id=NEW.operation_id AND owner=NEW.owner AND portion=NEW.portion AND identity_sha256=NEW.identity_sha256) BEGIN SELECT RAISE(ABORT,'candidate portion already closed'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER operation_sealed_identity BEFORE UPDATE OF id,owner ON compaction_operation WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=OLD.id AND proof_version=1) BEGIN SELECT RAISE(ABORT,'historical operation identity immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER plan_sealed_readiness BEFORE UPDATE OF source_count,reference_count,ready ON compaction_runner_plan WHEN (NEW.source_count<>OLD.source_count OR NEW.reference_count<>OLD.reference_count OR NEW.ready<>OLD.ready) AND EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=OLD.operation_id AND proof_version=1) BEGIN SELECT RAISE(ABORT,'sealed plan readiness immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER plan_sealed_delete BEFORE DELETE ON compaction_runner_plan WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=OLD.operation_id AND proof_version=1) BEGIN SELECT RAISE(ABORT,'sealed plan readiness retained'); END"#).await?;
        // Local immutable guards protect bounded readback and every historical
        // reader; they introduce no global publication revision fence.
        for table in [
            "compaction_checkpoint_replay_alias",
            "compaction_checkpoint_event_input",
            "compaction_checkpoint_import",
        ] {
            m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_sealed_insert BEFORE INSERT ON {table} WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id=NEW.checkpoint_id AND proof_version=1) BEGIN SELECT RAISE(ABORT,'checkpoint proofs sealed'); END")).await?;
            m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_immutable_update BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT,'checkpoint evidence immutable'); END")).await?;
            m.get_connection().execute_unprepared(&format!("CREATE TRIGGER {table}_sealed_delete BEFORE DELETE ON {table} WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id=OLD.checkpoint_id AND proof_version=1) BEGIN SELECT RAISE(ABORT,'checkpoint proofs sealed'); END")).await?;
        }
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER checkpoint_sealed_identity BEFORE UPDATE OF id,operation_id,owner,previous,portion,summary,selection,projection_version,format_version,identity_sha256 ON compaction_checkpoint WHEN (OLD.proof_version=1 OR OLD.portion<(SELECT next_portion FROM compaction_operation WHERE id=OLD.operation_id)) BEGIN SELECT RAISE(ABORT,'sealed checkpoint identity immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER checkpoint_sealed_delete BEFORE DELETE ON compaction_checkpoint WHEN (OLD.proof_version=1 OR OLD.portion<(SELECT next_portion FROM compaction_operation WHERE id=OLD.operation_id)) BEGIN SELECT RAISE(ABORT,'sealed checkpoint retained'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER checkpoint_published_proof_reset BEFORE UPDATE OF proof_version ON compaction_checkpoint WHEN OLD.proof_version=1 AND NEW.proof_version=0 AND (OLD.status='applied' OR EXISTS(SELECT 1 FROM compaction_operation o WHERE o.id=OLD.operation_id AND o.status='completed') OR EXISTS(SELECT 1 FROM compaction_checkpoint p WHERE p.previous=OLD.id) OR EXISTS(SELECT 1 FROM compaction_coverage v JOIN compaction_checkpoint p ON p.id=v.checkpoint_id WHERE v.source_scope='checkpoint:'||OLD.owner AND v.source_id=OLD.id AND v.source_version=OLD.identity_sha256)) BEGIN SELECT RAISE(ABORT,'reachable checkpoint proofs retained'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER checkpoint_coverage_sealed_insert BEFORE INSERT ON compaction_coverage WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id=NEW.checkpoint_id AND (proof_version=1 OR portion<(SELECT next_portion FROM compaction_operation WHERE id=operation_id))) AND NOT EXISTS(SELECT 1 FROM compaction_coverage WHERE checkpoint_id=NEW.checkpoint_id AND source_scope=NEW.source_scope AND source_id=NEW.source_id AND source_version=NEW.source_version) BEGIN SELECT RAISE(ABORT,'sealed coverage immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER checkpoint_coverage_sealed_update BEFORE UPDATE ON compaction_coverage WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id IN (OLD.checkpoint_id,NEW.checkpoint_id) AND (proof_version=1 OR portion<(SELECT next_portion FROM compaction_operation WHERE id=operation_id))) BEGIN SELECT RAISE(ABORT,'sealed coverage immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER checkpoint_coverage_sealed_delete BEFORE DELETE ON compaction_coverage WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id=OLD.checkpoint_id AND (proof_version=1 OR portion<(SELECT next_portion FROM compaction_operation WHERE id=operation_id))) BEGIN SELECT RAISE(ABORT,'sealed coverage immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER manifest_sealed_ownership_insert BEFORE INSERT ON compaction_manifest WHEN EXISTS(SELECT 1 FROM compaction_checkpoint p JOIN compaction_coverage v ON v.checkpoint_id=p.id WHERE p.operation_id=NEW.operation_id AND (p.proof_version=1 OR p.portion<(SELECT next_portion FROM compaction_operation WHERE id=p.operation_id)) AND v.source_scope=NEW.source_scope AND v.source_id=NEW.source_id AND v.source_version=NEW.source_version) AND NOT EXISTS(SELECT 1 FROM compaction_manifest m WHERE m.operation_id=NEW.operation_id AND m.ordinal=NEW.ordinal AND m.unit_ordinal=NEW.unit_ordinal AND m.reference_only=NEW.reference_only AND m.source_thread=NEW.source_thread AND m.source_scope=NEW.source_scope AND m.source_id=NEW.source_id AND m.source_version=NEW.source_version) BEGIN SELECT RAISE(ABORT,'sealed historical ownership immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER manifest_sealed_ownership_update BEFORE UPDATE ON compaction_manifest WHEN EXISTS(SELECT 1 FROM compaction_checkpoint p JOIN compaction_coverage v ON v.checkpoint_id=p.id WHERE (p.proof_version=1 OR p.portion<(SELECT next_portion FROM compaction_operation WHERE id=p.operation_id)) AND ((p.operation_id=OLD.operation_id AND v.source_scope=OLD.source_scope AND v.source_id=OLD.source_id AND v.source_version=OLD.source_version) OR (p.operation_id=NEW.operation_id AND v.source_scope=NEW.source_scope AND v.source_id=NEW.source_id AND v.source_version=NEW.source_version))) BEGIN SELECT RAISE(ABORT,'sealed historical ownership immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER manifest_sealed_ownership_delete BEFORE DELETE ON compaction_manifest WHEN EXISTS(SELECT 1 FROM compaction_checkpoint p JOIN compaction_coverage v ON v.checkpoint_id=p.id WHERE p.operation_id=OLD.operation_id AND (p.proof_version=1 OR p.portion<(SELECT next_portion FROM compaction_operation WHERE id=p.operation_id)) AND v.source_scope=OLD.source_scope AND v.source_id=OLD.source_id AND v.source_version=OLD.source_version) BEGIN SELECT RAISE(ABORT,'sealed historical ownership immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER projection_sealed_insert BEFORE INSERT ON compaction_operation_projection WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=NEW.operation_id AND proof_version=1) AND NOT EXISTS(SELECT 1 FROM compaction_operation_projection b WHERE b.operation_id=NEW.operation_id AND b.manifest_id=NEW.manifest_id AND b.identity_sha256=NEW.identity_sha256 AND b.imports_sha256=NEW.imports_sha256 AND b.import_count=NEW.import_count) BEGIN SELECT RAISE(ABORT,'sealed projection binding immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER projection_sealed_update BEFORE UPDATE OF operation_id,manifest_id,identity_sha256,imports_sha256,import_count ON compaction_operation_projection WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id IN (OLD.operation_id,NEW.operation_id) AND proof_version=1) BEGIN SELECT RAISE(ABORT,'sealed projection binding immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER projection_sealed_delete BEFORE DELETE ON compaction_operation_projection WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE operation_id=OLD.operation_id AND proof_version=1) BEGIN SELECT RAISE(ABORT,'sealed projection binding retained'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER frozen_complete_identity BEFORE UPDATE OF id,workspace_id,owner_thread,identity_sha256,message_count,imports_sha256,import_count,next_ordinal,next_import,ready ON compaction_frozen_history WHEN (OLD.ready=1 OR OLD.expired=1) AND (NEW.id<>OLD.id OR NEW.workspace_id<>OLD.workspace_id OR NEW.owner_thread<>OLD.owner_thread OR NEW.identity_sha256<>OLD.identity_sha256 OR NEW.message_count<>OLD.message_count OR NEW.imports_sha256<>OLD.imports_sha256 OR NEW.import_count<>OLD.import_count OR NEW.next_ordinal<>OLD.next_ordinal OR NEW.next_import<>OLD.next_import OR NEW.ready<>OLD.ready) BEGIN SELECT RAISE(ABORT,'complete frozen identity immutable'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER frozen_sealed_header_delete BEFORE DELETE ON compaction_frozen_history WHEN EXISTS(SELECT 1 FROM compaction_operation_projection b JOIN compaction_checkpoint p ON p.operation_id=b.operation_id WHERE b.manifest_id=OLD.id AND p.proof_version=1) BEGIN SELECT RAISE(ABORT,'historical origin identity retained'); END"#).await?;
        m.get_connection().execute_unprepared(r#"CREATE TRIGGER context_sealed_identity BEFORE UPDATE OF owner,workspace_id,thread_id,format_version ON compaction_context WHEN EXISTS(SELECT 1 FROM compaction_checkpoint WHERE owner=OLD.owner AND proof_version=1) BEGIN SELECT RAISE(ABORT,'historical checkpoint scope immutable'); END"#).await?;
        Ok(())
    }
    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom("frozen lifetime is irreversible after expiry; downgrade requires a forward compatible reader".into()))
    }
}
