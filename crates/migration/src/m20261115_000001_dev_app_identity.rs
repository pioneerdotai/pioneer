use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // SeaQuery has no trigger DDL builder. Dev source identities occupy a
        // distinct namespace; normal release slug uniqueness stays unchanged.
        manager.get_connection().execute_unprepared(
            "DROP TRIGGER app_workspace_slug_insert;
             CREATE TRIGGER app_workspace_slug_insert BEFORE INSERT ON app_installation
             WHEN EXISTS (
               SELECT 1 FROM app_installation a
               JOIN package_installation existing ON existing.id=a.id
               JOIN package_installation incoming ON incoming.id=NEW.id
               WHERE a.slug=NEW.slug AND existing.workspace_id=incoming.workspace_id
               AND ((existing.source_upload_id NOT LIKE 'dev:%' AND incoming.source_upload_id NOT LIKE 'dev:%')
                 OR existing.source_upload_id=incoming.source_upload_id)
             ) BEGIN SELECT RAISE(ABORT,'app slug already installed in workspace'); END;"
        ).await?;
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Refuse a downgrade that would leave identities forbidden by the old
        // schema. Remove dev connections before downgrading.
        let connection = manager.get_connection();
        let query = Query::select()
            .column(Alias::new("id"))
            .from(Alias::new("package_installation"))
            .and_where(Expr::col(Alias::new("source_upload_id")).like("dev:%"))
            .limit(1)
            .to_owned();
        if connection.query_one(&query).await?.is_some() {
            return Err(DbErr::Custom(
                "Remove dev App connections before downgrading".into(),
            ));
        }
        connection
            .execute_unprepared(
                "DROP TRIGGER app_workspace_slug_insert;
             CREATE TRIGGER app_workspace_slug_insert BEFORE INSERT ON app_installation
             WHEN EXISTS (
               SELECT 1 FROM app_installation a
               JOIN package_installation existing ON existing.id=a.id
               JOIN package_installation incoming ON incoming.id=NEW.id
               WHERE a.slug=NEW.slug AND existing.workspace_id=incoming.workspace_id
             ) BEGIN SELECT RAISE(ABORT,'app slug already installed in workspace'); END;",
            )
            .await?;
        Ok(())
    }
}
