use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // One index build scans existing threads. Workspace equality followed
        // by a reverse scan supports created_at DESC, id DESC without sorting
        // the workspace for every bounded observation page.
        manager
            .create_index(
                Index::create()
                    .name("idx_thread_workspace_created_id")
                    .table(Alias::new("thread"))
                    .col(Alias::new("workspace_id"))
                    .col(Alias::new("created_at"))
                    .col(Alias::new("id"))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_thread_workspace_created_id")
                    .to_owned(),
            )
            .await
    }
}
