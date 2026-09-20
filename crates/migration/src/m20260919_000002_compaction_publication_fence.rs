use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

const MAX_GENERATION: i64 = i64::MAX;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        // The fence is useful only if every dependency starts invalidating it
        // in the same schema transition in which the fence becomes visible.
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_publication_fence"))
                    .col(integer("singleton").primary_key())
                    .col(text("database_id"))
                    .col(big_integer("structural_generation").default(0))
                    .check(Expr::col("singleton").eq(1))
                    .check(Expr::col("structural_generation").gte(0))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_publication_source_fence"))
                    .col(text("workspace_id").primary_key())
                    .col(big_integer("mutation_generation").default(0))
                    .col(big_integer("insert_generation").default(0))
                    .check(Expr::col("mutation_generation").gte(0))
                    .check(Expr::col("insert_generation").gte(0))
                    // Deliberately no workspace FK: retaining the row across
                    // delete/recreate prevents an old proof matching by ABA.
                    .to_owned(),
            )
            .await?;

        let db = manager.get_connection();
        db.execute_unprepared(
            "INSERT INTO compaction_publication_fence(singleton,database_id,structural_generation) \
             VALUES (1,lower(hex(randomblob(16))),0)",
        )
        .await?;
        db.execute_unprepared(
            "INSERT INTO compaction_publication_source_fence(\
                 workspace_id,mutation_generation,insert_generation) \
             SELECT id,0,0 FROM workspace",
        )
        .await?;

        install_fence_integrity_triggers(db).await?;
        install_structural_triggers(db).await?;
        install_workspace_source_fence_trigger(db).await?;
        install_topology_insert_triggers(db).await?;
        install_canonical_source_triggers(manager).await?;
        install_task_basis_triggers(db).await?;
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "compaction publication generations cannot be reset safely".into(),
        ))
    }
}

async fn install_fence_integrity_triggers(db: &SchemaManagerConnection<'_>) -> Result<(), DbErr> {
    // Internal generations are append-only durable state. Failing a mutation
    // is safer than deleting or lowering one and allowing an older proof to
    // compare equal again.
    for sql in [
        "CREATE TRIGGER compaction_publication_fence_no_delete \
         BEFORE DELETE ON compaction_publication_fence \
         BEGIN SELECT RAISE(ABORT,'compaction publication fence cannot be deleted'); END",
        "CREATE TRIGGER compaction_publication_fence_monotonic \
         BEFORE UPDATE OF database_id,structural_generation ON compaction_publication_fence \
         WHEN NEW.database_id IS NOT OLD.database_id \
          OR NEW.structural_generation<OLD.structural_generation \
         BEGIN SELECT RAISE(ABORT,'compaction publication fence cannot move backwards'); END",
        "CREATE TRIGGER compaction_publication_source_fence_no_delete \
         BEFORE DELETE ON compaction_publication_source_fence \
         BEGIN SELECT RAISE(ABORT,'compaction publication source fence cannot be deleted'); END",
        "CREATE TRIGGER compaction_publication_source_fence_monotonic \
         BEFORE UPDATE OF workspace_id,mutation_generation,insert_generation \
         ON compaction_publication_source_fence \
         WHEN NEW.workspace_id IS NOT OLD.workspace_id \
          OR NEW.mutation_generation<OLD.mutation_generation \
          OR NEW.insert_generation<OLD.insert_generation \
         BEGIN SELECT RAISE(ABORT,'compaction publication source fence cannot move backwards'); END",
    ] {
        db.execute_unprepared(sql).await?;
    }
    Ok(())
}

async fn install_structural_triggers(db: &SchemaManagerConnection<'_>) -> Result<(), DbErr> {
    // This is a domain generation, not a database-wide write counter. The
    // listed rows are precisely the topology, compaction and logical frozen
    // storage inputs of the final manifest predicate. Point publication guards
    // (head, runner generation and execution stop) remain in the writer.
    for (table, update_columns, structural_insert) in [
        ("workspace", Some("id"), false),
        ("thread", Some("id,workspace_id"), false),
        ("turn", Some("id,thread_id"), false),
        (
            "compaction_context",
            Some("workspace_id,thread_id,owner"),
            true,
        ),
        (
            "compaction_operation",
            Some("id,owner,status,snapshot"),
            true,
        ),
        ("compaction_operation_projection", None, true),
        (
            "compaction_checkpoint",
            Some("id,operation_id,owner,previous,identity_sha256,format_version,status"),
            true,
        ),
        ("compaction_coverage", None, true),
        ("compaction_manifest", None, true),
        (
            "compaction_frozen_history",
            Some(
                "id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,\
                 import_count,imports_sha256,next_import,ready",
            ),
            true,
        ),
        ("compaction_frozen_message_data", None, true),
        ("compaction_frozen_import_data", None, true),
        ("compaction_frozen_layout", None, true),
        ("compaction_frozen_span", None, true),
    ] {
        let mut row_events = vec![("delete", "AFTER DELETE")];
        if structural_insert {
            row_events.push(("insert", "AFTER INSERT"));
        }
        for (suffix, event) in row_events {
            db.execute_unprepared(&format!(
                "CREATE TRIGGER compaction_publication_{table}_{suffix} {event} ON \"{table}\" \
                 BEGIN {bump} END",
                bump = structural_bump_sql(),
            ))
            .await?;
        }
        let event = update_columns
            .map(|columns| format!("AFTER UPDATE OF {columns}"))
            .unwrap_or_else(|| "AFTER UPDATE".to_owned());
        db.execute_unprepared(&format!(
            "CREATE TRIGGER compaction_publication_{table}_update {event} ON \"{table}\" \
             BEGIN {bump} END",
            bump = structural_bump_sql(),
        ))
        .await?;
    }
    Ok(())
}

async fn install_topology_insert_triggers(db: &SchemaManagerConnection<'_>) -> Result<(), DbErr> {
    // A new topology row can repair a negative exact-ID result, but cannot
    // invalidate an already-positive result because IDs are unique. Route it
    // through the insert component of the affected workspace instead of the
    // global structural generation so ordinary new turns do not fence an
    // otherwise publishable runner.
    db.execute_unprepared(&format!(
        "CREATE TRIGGER compaction_publication_thread_source_insert AFTER INSERT ON thread \
         BEGIN {bump} END",
        bump = source_bump_select_sql(
            "insert_generation",
            "SELECT NEW.workspace_id AS workspace_id",
            "source insert"
        ),
    ))
    .await?;
    db.execute_unprepared(&format!(
        "CREATE TRIGGER compaction_publication_turn_source_insert AFTER INSERT ON turn \
         BEGIN {bump} END",
        bump = source_bump_select_sql(
            "insert_generation",
            "SELECT workspace_id AS workspace_id FROM thread WHERE id=NEW.thread_id",
            "source insert"
        ),
    ))
    .await?;
    Ok(())
}

async fn install_workspace_source_fence_trigger(
    db: &SchemaManagerConnection<'_>,
) -> Result<(), DbErr> {
    // New workspaces start at zero. Recreating a previously deleted workspace
    // advances both components, so a proof from its earlier lifetime cannot
    // become current again.
    db.execute_unprepared(&format!(
        "CREATE TRIGGER compaction_publication_workspace_source_insert AFTER INSERT ON workspace \
         BEGIN \
          INSERT INTO compaction_publication_source_fence(\
           workspace_id,mutation_generation,insert_generation) VALUES (NEW.id,0,0) \
          ON CONFLICT(workspace_id) DO UPDATE SET \
           mutation_generation={mutation},insert_generation={insert}; \
         END",
        mutation = checked_increment("mutation_generation", "source mutation"),
        insert = checked_increment("insert_generation", "source insert"),
    ))
    .await?;
    db.execute_unprepared(&format!(
        "CREATE TRIGGER compaction_publication_workspace_source_update \
         AFTER UPDATE OF id ON workspace WHEN NEW.id IS NOT OLD.id \
         BEGIN \
          INSERT INTO compaction_publication_source_fence(\
           workspace_id,mutation_generation,insert_generation) VALUES (NEW.id,1,1) \
          ON CONFLICT(workspace_id) DO UPDATE SET \
           mutation_generation={mutation},insert_generation={insert}; \
         END",
        mutation = checked_increment("mutation_generation", "source mutation"),
        insert = checked_increment("insert_generation", "source insert"),
    ))
    .await?;
    Ok(())
}

async fn install_canonical_source_triggers(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let db = manager.get_connection();
    for (logical, revision, metadata) in [
        (
            "turn_llm_context",
            "compaction_source_revision",
            "turn_id,source,item_id,sequence",
        ),
        (
            "turn_item",
            "compaction_item_revision",
            "turn_id,item_id,item_type,status",
        ),
        (
            "turn_event",
            "compaction_event_revision",
            "turn_id,thread_id,event_type,sequence",
        ),
        (
            "turn_input",
            "compaction_input_revision",
            "text,turn_id,input_type,input_index",
        ),
    ] {
        let compressed = format!("_{logical}_zstd");
        let storage = if manager.has_table(&compressed).await? {
            compressed
        } else if manager.has_table(logical).await? {
            logical.to_owned()
        } else {
            return Err(DbErr::Migration(format!(
                "{logical} storage is missing for publication fence"
            )));
        };
        let workspace_for_turns =
            "SELECT DISTINCT th.workspace_id FROM turn t JOIN thread th ON th.id=t.thread_id";
        let insert_select = format!("{workspace_for_turns} WHERE t.id=NEW.turn_id");
        let old_select = format!("{workspace_for_turns} WHERE t.id=OLD.turn_id");
        let update_select =
            format!("{workspace_for_turns} WHERE t.id IN (OLD.turn_id,NEW.turn_id)");

        db.execute_unprepared(&format!(
            "CREATE TRIGGER compaction_publication_{logical}_insert AFTER INSERT ON \"{storage}\" \
             BEGIN {bump} END",
            bump = source_bump_select_sql("insert_generation", &insert_select, "source insert"),
        ))
        .await?;
        db.execute_unprepared(&format!(
            "CREATE TRIGGER compaction_publication_{logical}_delete BEFORE DELETE ON \"{storage}\" \
             BEGIN {bump} END",
            bump = source_bump_select_sql("mutation_generation", &old_select, "source mutation"),
        ))
        .await?;
        let changed = std::iter::once("NEW.id IS NOT OLD.id".to_owned())
            .chain(std::iter::once(
                "(typeof(NEW.payload) <> 'blob' AND NEW.payload IS NOT OLD.payload)".to_owned(),
            ))
            .chain(
                metadata
                    .split(',')
                    .map(|field| format!("NEW.{field} IS NOT OLD.{field}")),
            )
            .collect::<Vec<_>>()
            .join(" OR ");
        db.execute_unprepared(&format!(
            "CREATE TRIGGER compaction_publication_{logical}_update \
             AFTER UPDATE OF id,payload,{metadata} ON \"{storage}\" WHEN {changed} \
             BEGIN {bump} END",
            bump = source_bump_select_sql("mutation_generation", &update_select, "source mutation"),
        ))
        .await?;

        // Revision rows are part of the predicate themselves. These triggers
        // also cover repair/admin mutations that do not pass through canonical
        // payload storage. Normal canonical writes may advance a component
        // twice; equality, not the delta, is the publication invariant.
        let revision_name = revision.trim_start_matches("compaction_");
        db.execute_unprepared(&format!(
            "CREATE TRIGGER compaction_publication_{revision_name}_insert AFTER INSERT ON {revision} \
             BEGIN {bump} END",
            bump = source_bump_select_sql("insert_generation", &insert_select, "source insert"),
        ))
        .await?;
        db.execute_unprepared(&format!(
            "CREATE TRIGGER compaction_publication_{revision_name}_update \
             AFTER UPDATE OF source_id,turn_id,revision,present ON {revision} \
             BEGIN {bump} END",
            bump = source_bump_select_sql("mutation_generation", &update_select, "source mutation"),
        ))
        .await?;
        db.execute_unprepared(&format!(
            "CREATE TRIGGER compaction_publication_{revision_name}_delete BEFORE DELETE ON {revision} \
             BEGIN {bump} END",
            bump = source_bump_select_sql(
                "mutation_generation",
                &old_select,
                "source mutation"
            ),
        ))
        .await?;
    }
    Ok(())
}

async fn install_task_basis_triggers(db: &SchemaManagerConnection<'_>) -> Result<(), DbErr> {
    let snapshot_insert = "SELECT NEW.workspace_id AS workspace_id";
    let snapshot_old = "SELECT OLD.workspace_id AS workspace_id";
    let snapshot_update = "SELECT OLD.workspace_id AS workspace_id UNION SELECT NEW.workspace_id AS workspace_id \
         WHERE NEW.workspace_id<>OLD.workspace_id";
    for (suffix, event, column, select, error) in [
        (
            "insert",
            "AFTER INSERT",
            "insert_generation",
            snapshot_insert,
            "source insert",
        ),
        (
            "update",
            "AFTER UPDATE",
            "mutation_generation",
            snapshot_update,
            "source mutation",
        ),
        (
            "delete",
            "BEFORE DELETE",
            "mutation_generation",
            snapshot_old,
            "source mutation",
        ),
    ] {
        db.execute_unprepared(&format!(
            "CREATE TRIGGER compaction_publication_task_basis_{suffix} {event} \
             ON task_run_conversation_snapshot BEGIN {bump} END",
            bump = source_bump_select_sql(column, select, error),
        ))
        .await?;
    }

    let current_workspace = "SELECT workspace_id AS workspace_id \
         FROM task_run_conversation_snapshot WHERE run_id=NEW.run_id";
    let changed_workspaces = "SELECT workspace_id AS workspace_id \
         FROM task_run_conversation_snapshot WHERE run_id=OLD.run_id \
         UNION SELECT workspace_id AS workspace_id \
         FROM task_run_conversation_snapshot WHERE run_id=NEW.run_id";
    let old_workspace = "SELECT workspace_id AS workspace_id \
         FROM task_run_conversation_snapshot WHERE run_id=OLD.run_id";
    db.execute_unprepared(&format!(
        "CREATE TRIGGER compaction_publication_task_basis_revision_insert AFTER INSERT \
         ON compaction_task_basis_revision WHEN NEW.revision<>1 BEGIN {bump} END",
        // Unlike the other source insertions, this can invalidate a positive
        // proof when a revision other than 1 replaces the fallback
        // COALESCE(revision,1). The ordinary auto-inserted revision 1 is
        // logically identical to that fallback; the snapshot insertion has
        // already advanced insert_generation for negative proofs.
        bump = source_bump_select_sql("mutation_generation", current_workspace, "source mutation"),
    ))
    .await?;
    db.execute_unprepared(&format!(
        "CREATE TRIGGER compaction_publication_task_basis_revision_update \
         AFTER UPDATE OF run_id,revision \
         ON compaction_task_basis_revision BEGIN {bump} END",
        bump = source_bump_select_sql("mutation_generation", changed_workspaces, "source mutation"),
    ))
    .await?;
    db.execute_unprepared(&format!(
        "CREATE TRIGGER compaction_publication_task_basis_revision_delete BEFORE DELETE \
         ON compaction_task_basis_revision BEGIN {bump} END",
        bump = source_bump_select_sql("mutation_generation", old_workspace, "source mutation"),
    ))
    .await?;
    Ok(())
}

fn structural_bump_sql() -> String {
    format!(
        "UPDATE compaction_publication_fence SET structural_generation={} WHERE singleton=1;",
        checked_increment("structural_generation", "structural")
    )
}

fn source_bump_select_sql(column: &str, select: &str, error: &str) -> String {
    let other = if column == "mutation_generation" {
        "insert_generation"
    } else {
        "mutation_generation"
    };
    format!(
        "INSERT INTO compaction_publication_source_fence(workspace_id,{column},{other}) \
         SELECT workspace_id,1,0 FROM ({select}) AS changed_workspaces WHERE true \
         ON CONFLICT(workspace_id) DO UPDATE SET \
         {column}={increment};",
        increment = checked_increment(column, error),
    )
}

fn checked_increment(column: &str, name: &str) -> String {
    format!(
        "CASE WHEN {column}>={MAX_GENERATION} THEN RAISE(ABORT,'compaction publication {name} generation overflow') ELSE {column}+1 END"
    )
}
