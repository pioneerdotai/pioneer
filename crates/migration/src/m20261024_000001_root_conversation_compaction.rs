//! Durable user activity and publication-triggered optional root preparation.
use sea_orm::ConnectionTrait;
use sea_orm_migration::{
    prelude::*,
    sea_query::{ExprTrait, IndexOrder},
};

// Frozen migration identifiers: never import runtime entities or policy helpers.
#[derive(Clone, Copy, DeriveIden)]
enum Schema {
    CompactionHistoryCheck,
    CompactionRootActivity,
    CompactionEventRevision,
    Thread,
    Turn,
    Task,
    TurnEvent,
    TurnItem,
    #[sea_orm(iden = "_task_zstd")]
    TaskZstd,
    #[sea_orm(iden = "_turn_event_zstd")]
    TurnEventZstd,
    #[sea_orm(iden = "_turn_item_zstd")]
    TurnItemZstd,
    Id,
    ThreadId,
    UserTurnId,
    LastUserActivityMs,
    Purpose,
    NextAttemptMs,
    TurnId,
    State,
    Managed,
    WorkspaceId,
    CreatedByThreadId,
    Status,
    MetadataJson,
    CreatedAt,
    EventType,
    ItemType,
    ItemId,
    SourceId,
    Present,
    ProjectionRevision,
    Revision,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

// SQLite strftime reads the durable original timestamp, including its offset.
fn millis(column: &str) -> String {
    format!(
        "(CAST(strftime('%s',{column}) AS INTEGER)*1000 + CAST(substr(strftime('%f',{column}),4,3) AS INTEGER))"
    )
}

fn enqueue(selection: &str) -> String {
    format!(
        r#"INSERT INTO compaction_history_check(turn_id,state,managed,purpose)
        {selection}
        ON CONFLICT(turn_id) DO UPDATE SET state='pending',outcome=NULL,
          revision=compaction_history_check.revision+1,failures=0,next_attempt_ms=0,
          attempt_deadline_ms=NULL,diagnostic=NULL,config_hash=NULL,managed=1,purpose='root_conversation';"#
    )
}

// These historical trigger predicates intentionally freeze the publication
// invariant. The receipt/status transition and debt must commit together, and
// only an exact accepted immutable output may enter this root's future context.
// Runtime independently validates current authorization when consuming debt;
// it cannot replace these checks with a fallible post-commit callback.
// A delivery locator is not authority: require the exact immutable binding,
// accepted candidate and the result's already materialized acknowledgement.
const DELIVERIES: &str = r#"
 FROM task_delivery d
 JOIN thread h ON h.id=d.target_thread_id AND h.workspace_id=d.workspace_id
 JOIN compaction_delivery_output b ON b.delivery_id=d.id
 JOIN compaction_task_output s ON s.task_run_turn_id=b.task_run_turn_id
   AND s.task_id=d.task_id AND s.run_id=d.run_id AND s.workspace_id=d.workspace_id
 JOIN task_result_candidate c ON c.id=b.candidate_id AND c.status='accepted'
   AND c.task_run_turn_id=s.task_run_turn_id AND c.task_id=s.task_id AND c.run_id=s.run_id
   AND c.thread_id=s.source_thread AND c.turn_id=s.source_turn
 JOIN task k ON k.id=d.task_id AND k.workspace_id=d.workspace_id
 JOIN turn_event acknowledgement ON acknowledgement.turn_id=d.delivered_turn_id
   AND acknowledgement.thread_id=h.id AND acknowledgement.event_type='item/completed'
 JOIN compaction_event_revision receipt ON receipt.source_id=acknowledgement.id
   AND receipt.turn_id=acknowledgement.turn_id AND receipt.present=1
   AND receipt.projection_revision=receipt.revision
   AND receipt.item_id='task_delivery_result_'||d.id
 JOIN turn published ON published.id=d.delivered_turn_id
   AND published.thread_id=h.id
 WHERE d.mode='thread' AND d.status='delivered' AND d.delivered_at IS NOT NULL
   AND d.error_snapshot_json IS NULL
   AND h.origin_kind IN ('user','collaborative','direct_message')
   AND NOT EXISTS(SELECT 1 FROM thread_lineage l WHERE l.child_thread_id=h.id)
   AND EXISTS(SELECT 1 FROM turn_item i WHERE i.turn_id=published.id
       AND i.item_id='task_delivery_result_'||d.id AND i.status='completed')
"#;

// Only a response bound to an original human request extends activity. This
// short exact-delivery check stays atomic with receipt publication; frozen JSON
// paths describe the released wire format, not a runtime policy import.
const COMPOSER: &str = r#"
 json_valid(k.metadata_json)
 AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.version')=1
 AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.launch.thread_id')=h.id
 AND EXISTS(SELECT 1 FROM turn human
   WHERE human.id=json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.launch.turn_id')
     AND human.thread_id=h.id AND human.origin='user'
     AND human.turn_kind='conversation' AND human.initiated_by_actor_kind='principal'
     AND human.work_owner='detached_task'
     AND k.created_by_turn_id=human.id AND k.created_by_thread_id=h.id)
"#;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        // Schema and completion marker share one transaction. There is no
        // history inventory/backfill under the bootstrap writer reservation.
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        if !manager
            .has_column("compaction_history_check", "purpose")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Schema::CompactionHistoryCheck)
                        .add_column(
                            ColumnDef::new(Schema::Purpose)
                                .text()
                                .not_null()
                                .default("execution"),
                        )
                        .to_owned(),
                )
                .await?;
        }
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("compaction_root_history_due")
                    .table(Schema::CompactionHistoryCheck)
                    .col(Schema::NextAttemptMs)
                    .col(Schema::TurnId)
                    .and_where(Expr::col(Schema::Purpose).eq("root_conversation"))
                    .and_where(Expr::col(Schema::State).eq("pending"))
                    .and_where(Expr::col(Schema::Managed).eq(1))
                    .to_owned(),
            )
            .await?;
        let task_storage = if manager.has_table("_task_zstd").await? {
            "_task_zstd"
        } else {
            "task"
        };
        // The metadata guard is a cheap superset of Composer work. The worker
        // verifies composerWork after reading only live rows in this root.
        let task_table = if task_storage == "_task_zstd" {
            Schema::TaskZstd
        } else {
            Schema::Task
        };
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("compaction_root_live_composer")
                    .table(task_table)
                    .col(Schema::WorkspaceId)
                    .col(Schema::CreatedByThreadId)
                    .col(Schema::Status)
                    .and_where(Expr::col(Schema::Status).is_not_in([
                        "completed",
                        "failed",
                        "blocked",
                        "cancelled",
                    ]))
                    .and_where(Expr::col(Schema::MetadataJson).is_not_null())
                    .to_owned(),
            )
            .await?;
        for name in [
            "compaction_root_human_input",
            "compaction_root_answer_completed",
            "compaction_root_task_delivered",
        ] {
            db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {name}"))
                .await?;
        }
        manager
            .create_table(
                Table::create()
                    .if_not_exists()
                    .table(Schema::CompactionRootActivity)
                    .col(
                        ColumnDef::new(Schema::ThreadId)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Schema::UserTurnId).text().not_null())
                    .col(
                        ColumnDef::new(Schema::LastUserActivityMs)
                            .integer()
                            .not_null(),
                    )
                    .check(Expr::col(Schema::LastUserActivityMs).gte(0))
                    .foreign_key(
                        ForeignKey::create()
                            .from(Schema::CompactionRootActivity, Schema::ThreadId)
                            .to(Schema::Thread, Schema::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(Schema::CompactionRootActivity, Schema::UserTurnId)
                            .to(Schema::Turn, Schema::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .unique()
                    .name("compaction_root_activity_turn")
                    .table(Schema::CompactionRootActivity)
                    .col(Schema::UserTurnId)
                    .to_owned(),
            )
            .await?;
        db.execute_unprepared("DROP TRIGGER IF EXISTS compaction_history_cli_completed")
            .await?;
        let turn_storage = if manager.has_table("_turn_zstd").await? {
            "_turn_zstd"
        } else {
            "turn"
        };
        let delivery_storage = if manager.has_table("_task_delivery_zstd").await? {
            "_task_delivery_zstd"
        } else {
            "task_delivery"
        };
        // Current publication seeks only this turn's completion events. Keep
        // index keys as columns: sqlite-zstd cannot introspect expression keys
        // when subsequently enabling payload compression. The trigger still
        // orders this exact subset by original epoch milliseconds, so timezone
        // offsets never make lexical created_at order authoritative.
        let event_storage = if manager.has_table("_turn_event_zstd").await? {
            "_turn_event_zstd"
        } else {
            "turn_event"
        };
        let item_storage = if manager.has_table("_turn_item_zstd").await? {
            "_turn_item_zstd"
        } else {
            "turn_item"
        };
        let event_table = if event_storage == "_turn_event_zstd" {
            Schema::TurnEventZstd
        } else {
            Schema::TurnEvent
        };
        let item_table = if item_storage == "_turn_item_zstd" {
            Schema::TurnItemZstd
        } else {
            Schema::TurnItem
        };
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("compaction_root_completion_event")
                    .table(event_table)
                    .col(Schema::TurnId)
                    .col(Schema::EventType)
                    .col((Schema::CreatedAt, IndexOrder::Desc))
                    .col((Schema::Id, IndexOrder::Desc))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("compaction_root_ready_item")
                    .table(item_table)
                    .col(Schema::TurnId)
                    .col(Schema::ItemType)
                    .col(Schema::Status)
                    .to_owned(),
            )
            .await?;
        // The current delivery joins only its exact materialized receipt.
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("compaction_root_delivery_receipt")
                    .table(Schema::CompactionEventRevision)
                    .col(Schema::TurnId)
                    .col(Schema::ItemId)
                    .col(Schema::SourceId)
                    .and_where(Expr::col(Schema::Present).eq(1))
                    .and_where(Expr::col(Schema::ProjectionRevision).equals(Schema::Revision))
                    .to_owned(),
            )
            .await?;
        let input_ms = millis("NEW.created_at");
        db.execute_unprepared(&format!(r#"CREATE TRIGGER IF NOT EXISTS compaction_root_human_input
          AFTER INSERT ON {turn_storage}
          WHEN NEW.origin='user' AND NEW.turn_kind='conversation'
            AND NEW.initiated_by_actor_kind='principal'
            AND EXISTS(SELECT 1 FROM thread h WHERE h.id=NEW.thread_id AND h.origin_kind IN ('user','collaborative','direct_message'))
            AND NOT EXISTS(SELECT 1 FROM thread_lineage WHERE child_thread_id=NEW.thread_id)
          BEGIN
            -- Establish the owner even if the older worker has not admitted
            -- its first operation yet. The following fence must beat admission.
            INSERT INTO compaction_context(owner,workspace_id,thread_id)
              SELECT 'native:'||length(CAST(h.workspace_id AS BLOB))||':'||h.workspace_id||':'||length(CAST(h.id AS BLOB))||':'||h.id,
                     h.workspace_id,h.id
              FROM thread h JOIN compaction_root_activity a ON a.thread_id=h.id
              WHERE h.id=NEW.thread_id
              ON CONFLICT(owner) DO NOTHING;
            INSERT INTO compaction_execution_stop(owner,turn_id)
              SELECT c.owner,a.user_turn_id FROM compaction_root_activity a
              JOIN compaction_context c ON c.thread_id=a.thread_id
              JOIN thread h ON h.id=a.thread_id AND c.workspace_id=h.workspace_id
              WHERE a.thread_id=NEW.thread_id AND a.user_turn_id<>NEW.id
                AND c.owner='native:'||length(CAST(h.workspace_id AS BLOB))||':'||h.workspace_id||':'||length(CAST(h.id AS BLOB))||':'||h.id
              ON CONFLICT(owner,turn_id) DO NOTHING;
            UPDATE compaction_history_check SET state='finished',outcome='cancelled',
                revision=revision+1,attempt_deadline_ms=NULL
              WHERE turn_id=(SELECT user_turn_id FROM compaction_root_activity WHERE thread_id=NEW.thread_id)
                AND turn_id<>NEW.id AND purpose='root_conversation' AND state='pending';
            INSERT INTO compaction_root_activity(thread_id,user_turn_id,last_user_activity_ms)
              VALUES(NEW.thread_id,NEW.id,{input_ms})
              ON CONFLICT(thread_id) DO UPDATE SET
                user_turn_id=NEW.id,
                last_user_activity_ms=MAX(compaction_root_activity.last_user_activity_ms,{input_ms});
          END"#)).await?;
        let completion_ms = millis("e.created_at");
        let direct_enqueue = enqueue(
            "SELECT a.user_turn_id,'pending',1,'root_conversation' FROM compaction_root_activity a WHERE a.thread_id=NEW.thread_id",
        );
        db.execute_unprepared(&format!(r#"CREATE TRIGGER IF NOT EXISTS compaction_root_answer_completed
          AFTER UPDATE OF status ON {turn_storage}
          WHEN NEW.status='completed' AND OLD.status<>'completed'
            AND NEW.origin='user' AND NEW.turn_kind='conversation'
            AND NEW.initiated_by_actor_kind='principal' AND NEW.work_owner='turn'
            AND EXISTS(SELECT 1 FROM turn_item i WHERE i.turn_id=NEW.id
                AND i.item_type='agent_message' AND i.status='completed')
            AND EXISTS(SELECT 1 FROM turn_event e WHERE e.turn_id=NEW.id AND e.event_type='turn/completed')
          BEGIN
            -- A newly published reply can belong to an input from before
            -- upgrade. Initialize from this exact reply, not old history.
            INSERT INTO compaction_root_activity(thread_id,user_turn_id,last_user_activity_ms)
              SELECT h.id,NEW.id,{completion_ms}
              FROM thread h JOIN turn_event e ON e.turn_id=NEW.id
                AND e.thread_id=h.id AND e.event_type='turn/completed'
              WHERE h.id=NEW.thread_id
                AND h.origin_kind IN ('user','collaborative','direct_message')
                AND NOT EXISTS(SELECT 1 FROM thread_lineage l WHERE l.child_thread_id=h.id)
              ORDER BY {completion_ms} DESC,e.id DESC LIMIT 1
              ON CONFLICT(thread_id) DO UPDATE SET last_user_activity_ms=
                MAX(compaction_root_activity.last_user_activity_ms,excluded.last_user_activity_ms);
            {direct_enqueue}
          END"#)).await?;
        let delivery_ms = millis("acknowledgement.created_at");
        let delivery_enqueue = enqueue(&format!(
            "SELECT a.user_turn_id,'pending',1,'root_conversation' FROM compaction_root_activity a WHERE a.thread_id=NEW.target_thread_id AND EXISTS(SELECT 1 {DELIVERIES} AND d.id=NEW.id)"
        ));
        db.execute_unprepared(&format!(
            r#"CREATE TRIGGER IF NOT EXISTS compaction_root_task_delivered
          AFTER UPDATE OF status ON {delivery_storage}
          WHEN NEW.status='delivered' AND OLD.status<>'delivered'
          BEGIN
            -- Only an actual Composer reply may initialize absent activity.
            -- Existing live input owns the anchor, including a newer question.
            INSERT INTO compaction_root_activity(thread_id,user_turn_id,last_user_activity_ms)
              SELECT h.id,k.created_by_turn_id,MAX({delivery_ms})
              {DELIVERIES} AND d.id=NEW.id AND {COMPOSER}
              GROUP BY h.id,k.created_by_turn_id
              ON CONFLICT(thread_id) DO UPDATE SET last_user_activity_ms=
                MAX(compaction_root_activity.last_user_activity_ms,excluded.last_user_activity_ms);
            {delivery_enqueue}
          END"#
        ))
        .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        for name in [
            "compaction_root_human_input",
            "compaction_root_answer_completed",
            "compaction_root_task_delivered",
        ] {
            db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {name}"))
                .await?;
        }
        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table(Schema::CompactionRootActivity)
                    .to_owned(),
            )
            .await?;
        for name in [
            "compaction_root_completion_event",
            "compaction_root_ready_item",
            "compaction_root_delivery_receipt",
        ] {
            manager
                .drop_index(Index::drop().if_exists().name(name).to_owned())
                .await?;
        }
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("compaction_root_history_due")
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("compaction_root_live_composer")
                    .to_owned(),
            )
            .await?;
        if manager
            .has_column("compaction_history_check", "purpose")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Schema::CompactionHistoryCheck)
                        .drop_column(Schema::Purpose)
                        .to_owned(),
                )
                .await?;
        }
        // Restore the previous policy only on explicit schema rollback.
        db.execute_unprepared("CREATE TRIGGER IF NOT EXISTS compaction_history_cli_completed AFTER UPDATE OF status ON turn WHEN NEW.status='completed' AND OLD.status!='completed' AND EXISTS (SELECT 1 FROM turn_cli_runtime_binding b WHERE b.turn_id=NEW.id) BEGIN INSERT INTO compaction_history_check(turn_id) VALUES(NEW.id) ON CONFLICT(turn_id) DO NOTHING; END").await?;
        Ok(())
    }
}
