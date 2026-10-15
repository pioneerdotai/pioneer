use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const INDEX: &str = "idx_cli_turn_binding_status_created_turn";

// Index-only additions on tables already present in 0.56.0. Builds read the
// existing tables once; no rows, history backfill or new queues are created.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Active CLI binding discovery.
        manager
            .create_index(
                Index::create()
                    .name(INDEX)
                    .table(Alias::new("turn_cli_runtime_binding"))
                    .col(Alias::new("status"))
                    .col(Alias::new("created_at"))
                    .col(Alias::new("turn_id"))
                    .if_not_exists()
                    .to_owned(),
            )
            .await?;

        // Immediate and timed action outbox ranges; predicates match the selectors.
        manager
            .get_connection()
            .execute_unprepared(
                "CREATE INDEX IF NOT EXISTS idx_agent_action_outbox_immediate \
             ON agent_action_outbox(created_at,id) \
             WHERE next_attempt_at IS NULL AND status IN ('pending','failed') AND attempts<8",
            )
            .await?;
        manager.get_connection().execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_agent_action_outbox_timed \
             ON agent_action_outbox(next_attempt_at,created_at,id) \
             WHERE next_attempt_at IS NOT NULL AND (status='pending' OR (status='failed' AND attempts<8))",
        ).await?;

        // Scoped skill discovery, provenance and active workspace pages.
        // One-time index builds read existing tables. No data/entity shape changes.
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_skill_installation_source_scope_id")
                    .table(Alias::new("skill_installation"))
                    .col(Alias::new("source_kind"))
                    .col(Alias::new("scope_key"))
                    .col(Alias::new("id"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_skill_installation_import_provenance")
                    .table(Alias::new("skill_installation"))
                    .col(Alias::new("source_kind"))
                    .col(Alias::new("scope_key"))
                    .col(Alias::new("source_ref"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_workspace_active_id")
                    .table(Alias::new("workspace"))
                    .col(Alias::new("is_active"))
                    .col(Alias::new("id"))
                    .to_owned(),
            )
            .await?;

        // Workspace job recovery, readiness and runnable episodic jobs.
        for (name, columns) in [
            (
                "idx_thread_episodic_jobs_workspace_due",
                &["workspace_id", "status", "next_run_at", "created_at"][..],
            ),
            (
                "idx_thread_episodic_jobs_workspace_recovery",
                &["workspace_id", "status", "updated_at", "id"][..],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table("thread_episodic_index_jobs");
            for column in columns {
                index.col(*column);
            }
            manager.create_index(index.to_owned()).await?;
        }
        // The readiness guard must not scan canceled superseded/deleted history.
        // Keep this predicate identical to the repository existence probe.
        manager.create_index(Index::create()
            .name("idx_thread_episodic_jobs_workspace_terminal")
            .table("thread_episodic_index_jobs").col("workspace_id")
            .cond_where(Expr::cust("status = 'canceled' AND (last_error IS NULL OR last_error NOT IN ('thread episodic source version superseded during reconciliation', 'thread episodic source deleted by user', 'thread episodic source excluded by user'))"))
            .to_owned()).await?;
        // Row-value seek, abandoned Running settlement and the next-run timer
        // use this same partial index.
        // ID bounds timestamp ties; ready/canceled history is outside its range.
        // Each index build reads existing jobs once; no table/data rewrite.
        manager
            .create_index(
                Index::create()
                    .name("idx_thread_episodic_jobs_runnable_seek")
                    .table("thread_episodic_index_jobs")
                    .col("next_run_at")
                    .col("created_at")
                    .col("id")
                    .cond_where(Expr::cust("status IN ('queued','failed','running')"))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "idx_thread_episodic_jobs_workspace_due",
            "idx_thread_episodic_jobs_workspace_recovery",
            "idx_thread_episodic_jobs_workspace_terminal",
            "idx_thread_episodic_jobs_runnable_seek",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .name(name)
                        .table("thread_episodic_index_jobs")
                        .to_owned(),
                )
                .await?;
        }

        for (name, table) in [
            (
                "idx_skill_installation_source_scope_id",
                "skill_installation",
            ),
            (
                "idx_skill_installation_import_provenance",
                "skill_installation",
            ),
            ("idx_workspace_active_id", "workspace"),
        ] {
            manager
                .drop_index(Index::drop().name(name).table(Alias::new(table)).to_owned())
                .await?;
        }

        for name in [
            "idx_agent_action_outbox_immediate",
            "idx_agent_action_outbox_timed",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .name(name)
                        .table(Alias::new("agent_action_outbox"))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }

        manager
            .drop_index(
                Index::drop()
                    .name(INDEX)
                    .table(Alias::new("turn_cli_runtime_binding"))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
