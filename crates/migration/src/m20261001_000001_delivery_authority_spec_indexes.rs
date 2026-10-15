use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

// Lifecycle writer preflight must reselect the same review policy. These
// indexes make the existing ORDER BY updated_at LIMIT 1 selectors bounded
// index lookups rather than sorting a Task's accumulated Agent spec history.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, owner) in [
            ("idx_task_agent_spec_task_updated", "task_id"),
            ("idx_task_agent_spec_run_updated", "run_id"),
        ] {
            manager
                .create_index(
                    Index::create()
                        .name(name)
                        .table(Alias::new("task_agent_spec"))
                        .col(Alias::new(owner))
                        .col(Alias::new("updated_at"))
                        .if_not_exists()
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "idx_task_agent_spec_task_updated",
            "idx_task_agent_spec_run_updated",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .name(name)
                        .table(Alias::new("task_agent_spec"))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}
