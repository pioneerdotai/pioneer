use sea_orm::{ConnectionTrait, Statement};
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;
const PENDING: &str = "compaction_lifecycle_pending";
const SCOPE: &str = "compaction_lifecycle_scope";
const SEQUENCE: &str = "compaction_lifecycle_sequence";
const PREFIX: &str = "compaction_lifecycle";
// All deadlines, eligibility and retry times are Unix MILLISECONDS. Source
// deadlines already use milliseconds; never compare them to strftime seconds.

fn bump() -> String {
    format!(
        "SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM {SEQUENCE} WHERE singleton=1 AND generation<9223372036854775807) THEN RAISE(ABORT,'compaction lifecycle generation exhausted') END; UPDATE {SEQUENCE} SET generation=generation+1 WHERE singleton=1;"
    )
}
fn point(id: &str) -> String {
    // Only exact PK lookups; no fanout and no payload decoding in triggers.
    let eligible = format!(
        "CASE WHEN o.status='running' AND NOT EXISTS(SELECT 1 FROM compaction_execution_stop s WHERE s.owner=o.owner AND s.turn_id=o.execution_turn) AND NOT EXISTS(SELECT 1 FROM turn t WHERE t.id=o.execution_turn AND t.status IN ('interrupted','cancelled')) THEN o.deadline_ms ELSE 0 END"
    );
    format!(
        "{} INSERT INTO {PENDING}(operation_id,generation,eligible_at,retry_not_before,due_at,attempts,claim_token) SELECT o.id,q.generation,{eligible},0,{eligible},0,NULL FROM compaction_operation o,{SEQUENCE} q WHERE o.id={id} AND q.singleton=1 ON CONFLICT(operation_id) DO UPDATE SET generation=excluded.generation,eligible_at=excluded.eligible_at,due_at=max(excluded.eligible_at,{PENDING}.retry_not_before),claim_token=NULL;",
        bump()
    )
}
fn scope(kind: &str, owner: &str, turn: &str) -> String {
    format!(
        "{} INSERT INTO {SCOPE}(kind,owner,turn_id,generation,due_at,attempts,claim_token,cursor_id,upper_id) SELECT '{kind}',{owner},{turn},generation,0,0,NULL,NULL,NULL FROM {SEQUENCE} WHERE singleton=1 ON CONFLICT(kind,owner,turn_id) DO UPDATE SET generation=excluded.generation,claim_token=NULL,cursor_id=NULL,upper_id=NULL;",
        bump()
    )
}
async fn trigger(
    db: &SchemaManagerConnection<'_>,
    table: &str,
    name: &str,
    event: &str,
    when: &str,
    body: String,
) -> Result<(), DbErr> {
    db.execute_unprepared(&format!(
        "CREATE TRIGGER {PREFIX}_{name} AFTER {event} ON \"{table}\" {when} BEGIN {body} END"
    ))
    .await?;
    Ok(())
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(SEQUENCE)
                    .col(
                        integer("singleton")
                            .primary_key()
                            .check(Expr::col("singleton").eq(1)),
                    )
                    .col(
                        big_integer("generation")
                            .check(Expr::cust("typeof(generation)='integer' AND generation>=0")),
                    )
                    .col(
                        integer("seed_complete")
                            .default(0)
                            .check(Expr::col("seed_complete").is_in([0, 1])),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(PENDING)
                    .col(text("operation_id").primary_key())
                    .col(
                        big_integer("generation")
                            .check(Expr::cust("typeof(generation)='integer' AND generation>0")),
                    )
                    .col(big_integer("eligible_at"))
                    .col(big_integer("retry_not_before").default(0))
                    .col(big_integer("due_at").check(Expr::cust(
                        "typeof(due_at)='integer' AND due_at=max(eligible_at,retry_not_before)",
                    )))
                    .col(
                        integer("attempts")
                            .default(0)
                            .check(Expr::col("attempts").between(0, 16)),
                    )
                    .col(text("claim_token").null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(SCOPE)
                    .col(text("kind").check(Expr::col("kind").is_in([
                        "seed",
                        "turn",
                        "owner_turn",
                        "owner",
                        "thread",
                    ])))
                    .col(text("owner"))
                    .col(text("turn_id"))
                    .primary_key(Index::create().col("kind").col("owner").col("turn_id"))
                    .col(
                        big_integer("generation")
                            .check(Expr::cust("typeof(generation)='integer' AND generation>0")),
                    )
                    .col(big_integer("due_at"))
                    .col(
                        integer("attempts")
                            .default(0)
                            .check(Expr::col("attempts").between(0, 16)),
                    )
                    .col(text("claim_token").null())
                    .col(text("cursor_id").null())
                    .col(text("upper_id").null())
                    .to_owned(),
            )
            .await?;
        let db = manager.get_connection();
        db.execute_unprepared(&format!("INSERT INTO {SEQUENCE}(singleton,generation) VALUES(1,1); INSERT INTO {SCOPE}(kind,owner,turn_id,generation,due_at) VALUES('seed','','',1,0);")).await?;
        // One-time O(history) index builds. There is no terminal-history seed.
        for (name, table, columns) in [
            (
                "idx_compaction_lifecycle_due",
                PENDING,
                vec!["due_at", "generation", "operation_id"],
            ),
            (
                "idx_compaction_lifecycle_scope_due",
                SCOPE,
                vec!["due_at", "generation", "kind"],
            ),
            (
                "idx_compaction_lifecycle_execution",
                "compaction_operation",
                vec!["execution_turn", "id"],
            ),
            (
                "idx_compaction_lifecycle_owner_execution",
                "compaction_operation",
                vec!["owner", "execution_turn", "id"],
            ),
            (
                "idx_compaction_lifecycle_owner",
                "compaction_operation",
                vec!["owner", "id"],
            ),
            (
                "idx_compaction_lifecycle_running",
                "compaction_operation",
                vec!["status", "id"],
            ),
            (
                "idx_compaction_lifecycle_context_thread",
                "compaction_context",
                vec!["thread_id", "owner"],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(table);
            for col in columns {
                index.col(col);
            }
            manager.create_index(index.to_owned()).await?;
        }
        for (table, key, columns) in [
            (
                "compaction_operation",
                "id",
                "id,owner,status,outcome,deadline_ms,execution_turn",
            ),
            (
                "compaction_runner_state",
                "operation_id",
                "operation_id,generation,state",
            ),
        ] {
            trigger(
                db,
                table,
                &format!("{table}_insert"),
                "INSERT",
                "",
                point(&format!("NEW.{key}")),
            )
            .await?;
            let body = if table == "compaction_operation" {
                format!("DELETE FROM {PENDING} WHERE operation_id=OLD.id;")
            } else {
                point(&format!("OLD.{key}"))
            };
            trigger(db, table, &format!("{table}_delete"), "DELETE", "", body).await?;
            let when = format!(
                "WHEN {}",
                columns
                    .split(',')
                    .map(|c| format!("OLD.{c} IS NOT NEW.{c}"))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            );
            trigger(
                db,
                table,
                &format!("{table}_update"),
                &format!("UPDATE OF {columns}"),
                &when,
                format!(
                    "{}{}",
                    point(&format!("OLD.{key}")),
                    point(&format!("NEW.{key}"))
                ),
            )
            .await?;
        }
        for (table, columns, kind, owner, key) in [
            ("turn", "id,status,thread_id", "turn", "''", "id"),
            (
                "compaction_context",
                "owner,workspace_id,thread_id",
                "owner",
                "ROW.owner",
                "''",
            ),
            (
                "compaction_execution_stop",
                "owner,turn_id",
                "owner_turn",
                "ROW.owner",
                "turn_id",
            ),
            ("thread", "id,workspace_id", "thread", "''", "id"),
        ] {
            let mark = |row: &str| {
                scope(
                    kind,
                    &owner.replace("ROW", row),
                    &if key == "''" {
                        key.to_owned()
                    } else {
                        format!("{row}.{key}")
                    },
                )
            };
            trigger(
                db,
                table,
                &format!("{table}_insert"),
                "INSERT",
                "",
                mark("NEW"),
            )
            .await?;
            trigger(
                db,
                table,
                &format!("{table}_delete"),
                "DELETE",
                "",
                mark("OLD"),
            )
            .await?;
            let when = format!(
                "WHEN {}",
                columns
                    .split(',')
                    .map(|c| format!("OLD.{c} IS NOT NEW.{c}"))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            );
            trigger(
                db,
                table,
                &format!("{table}_update"),
                &format!("UPDATE OF {columns}"),
                &when,
                format!("{}{}", mark("OLD"), mark("NEW")),
            )
            .await?;
        }
        let storage = db.query_one_raw(Statement::from_string(db.get_database_backend(),"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('turn_item','_turn_item_zstd') ORDER BY name LIMIT 1".to_owned())).await?
            .ok_or_else(||DbErr::Migration("compaction lifecycle turn_item physical storage missing".into()))?.try_get::<String>("","name")?;
        for (name, event, row) in [
            ("item_insert", "INSERT", "NEW"),
            ("item_delete", "DELETE", "OLD"),
        ] {
            trigger(
                db,
                &storage,
                name,
                event,
                &format!("WHEN substr({row}.item_id,1,11)='compaction:'"),
                point(&format!("substr({row}.item_id,12)")),
            )
            .await?;
        }
        let metadata = "id,turn_id,item_id,item_type,status";
        let changed = metadata
            .split(',')
            .map(|c| format!("OLD.{c} IS NOT NEW.{c}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let prefix =
            "(substr(OLD.item_id,1,11)='compaction:' OR substr(NEW.item_id,1,11)='compaction:')";
        let body = format!("{}{}", conditional_point("OLD"), conditional_point("NEW"));
        trigger(
            db,
            &storage,
            "item_update",
            &format!("UPDATE OF {metadata}"),
            &format!("WHEN {prefix} AND ({changed})"),
            body.clone(),
        )
        .await?;
        // Separate payload coverage avoids reading a possibly compressed BLOB
        // and avoids heartbeat/unchanged metadata invalidating poison claims.
        // SQLite permits UPDATE OF columns that do not exist yet: dictionary
        // coverage therefore survives the subsequent compression ALTER TABLE.
        // https://www.sqlite.org/lang_createtrigger.html (UPDATE OF behavior)
        trigger(
            db,
            &storage,
            "item_payload_update",
            "UPDATE OF payload,_payload_dict",
            &format!("WHEN {prefix}"),
            body,
        )
        .await?;
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        for table in [
            "compaction_operation",
            "compaction_runner_state",
            "turn",
            "compaction_context",
            "compaction_execution_stop",
            "thread",
        ] {
            for event in ["insert", "update", "delete"] {
                db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {PREFIX}_{table}_{event}"))
                    .await?;
            }
        }
        for event in ["insert", "update", "delete"] {
            db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {PREFIX}_item_{event}"))
                .await?;
        }
        db.execute_unprepared(&format!(
            "DROP TRIGGER IF EXISTS {PREFIX}_item_payload_update"
        ))
        .await?;
        for name in [
            "execution",
            "owner_execution",
            "owner",
            "running",
            "context_thread",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .name(format!("idx_compaction_lifecycle_{name}"))
                        .to_owned(),
                )
                .await?;
        }
        for table in [PENDING, SCOPE, SEQUENCE] {
            manager
                .drop_table(Table::drop().table(table).to_owned())
                .await?;
        }
        Ok(())
    }
}
fn conditional_point(row: &str) -> String {
    // SELECT inside a trigger is constrained to one operation PK; metadata is
    // sufficient even when the physical payload is compressed or malformed.
    let sql = point(&format!("substr({row}.item_id,12)"));
    // SQL WHEN cannot be attached to individual statements in a trigger body.
    // Gate the point lookup by NULL, which can never match the NOT NULL PK.
    sql.replace(&format!("o.id=substr({row}.item_id,12)"),&format!("o.id=CASE WHEN substr({row}.item_id,1,11)='compaction:' THEN substr({row}.item_id,12) END"))
}
