use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

const INSTALLATION: &str = "plugin_installation";
const COMPONENT: &str = "plugin_component";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(INSTALLATION)
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("name"))
                    .col(text("version").null())
                    .col(text("source_upload_id").unique_key())
                    .col(text("package_path"))
                    .col(text("data_path"))
                    .col(text("package_fingerprint"))
                    .col(boolean("enabled"))
                    .col(text("state").check(Expr::col("state").is_in([
                        "installing",
                        "installed",
                        "updating",
                        "removing",
                        "interrupted",
                    ])))
                    .col(big_integer("revision").check(Expr::col("revision").gt(0)))
                    .col(text("pending_json").null().check(Expr::cust(
                        "pending_json IS NULL OR length(pending_json) <= 65536",
                    )))
                    .col(text("last_error").null())
                    .col(timestamp_with_time_zone("created_at"))
                    .col(timestamp_with_time_zone("updated_at"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(INSTALLATION, "workspace_id")
                            .to("workspace", "id")
                            .on_delete(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("ix_plugin_workspace")
                    .table(INSTALLATION)
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(COMPONENT)
                    .col(text("plugin_id"))
                    .col(text("kind").check(Expr::col("kind").is_in(["skill", "mcp"])))
                    .col(text("member_key"))
                    .col(text("member_path").null())
                    .col(text("skill_id").null().unique_key())
                    .col(text("mcp_installation_id").null().unique_key())
                    .col(text("package_fingerprint").null())
                    .col(text("status").check(Expr::col("status").is_in([
                        "installed",
                        "invalid",
                        "failed",
                        "removed_by_user",
                    ])))
                    .col(text("diagnostic").null())
                    .col(text("override_fields_json").default("[]"))
                    .primary_key(
                        Index::create()
                            .col("plugin_id")
                            .col("kind")
                            .col("member_key"),
                    )
                    .check(
                        Expr::col("kind")
                            .eq("skill")
                            .and(Expr::col("mcp_installation_id").is_null())
                            .or(Expr::col("kind")
                                .eq("mcp")
                                .and(Expr::col("skill_id").is_null())),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(COMPONENT, "plugin_id")
                            .to(INSTALLATION, "id")
                            .on_delete(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(COMPONENT, "skill_id")
                            .to("skill_installation", "id")
                            .on_delete(ForeignKeyAction::SetNull),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(COMPONENT, "mcp_installation_id")
                            .to("mcp_server_installation", "id")
                            .on_delete(ForeignKeyAction::SetNull),
                    )
                    .to_owned(),
            )
            .await?;
        // Gateway deliberately has foreign_keys=OFF. Local triggers implement
        // these new relationships without changing legacy database semantics.
        manager.get_connection().execute_unprepared(r#"
CREATE TRIGGER plugin_workspace_insert BEFORE INSERT ON plugin_installation
WHEN NOT EXISTS (SELECT 1 FROM workspace WHERE id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'plugin workspace missing'); END;
CREATE TRIGGER plugin_workspace_immutable BEFORE UPDATE OF workspace_id ON plugin_installation
WHEN NEW.workspace_id != OLD.workspace_id
BEGIN SELECT RAISE(ABORT,'plugin workspace is immutable'); END;
CREATE TRIGGER plugin_workspace_restrict BEFORE DELETE ON workspace
WHEN EXISTS (SELECT 1 FROM plugin_installation WHERE workspace_id=OLD.id)
BEGIN SELECT RAISE(ABORT,'workspace still owns plugins'); END;
CREATE TRIGGER plugin_parent_restrict BEFORE DELETE ON plugin_installation
WHEN EXISTS (SELECT 1 FROM plugin_component WHERE plugin_id=OLD.id)
BEGIN SELECT RAISE(ABORT,'plugin still owns component links'); END;
CREATE TRIGGER plugin_skill_unlink AFTER DELETE ON skill_installation
BEGIN UPDATE plugin_component SET skill_id=NULL WHERE skill_id=OLD.id; END;
CREATE TRIGGER plugin_mcp_unlink AFTER DELETE ON mcp_server_installation
BEGIN UPDATE plugin_component SET mcp_installation_id=NULL WHERE mcp_installation_id=OLD.id; END;
CREATE TRIGGER plugin_skill_scope BEFORE UPDATE OF scope_key,pack_id ON skill_installation
WHEN EXISTS (SELECT 1 FROM plugin_component c JOIN plugin_installation p ON p.id=c.plugin_id
    WHERE c.skill_id=OLD.id AND (p.workspace_id!=NEW.scope_key OR NEW.pack_id IS NOT NULL))
BEGIN SELECT RAISE(ABORT,'plugin skill scope mismatch'); END;
CREATE TRIGGER plugin_mcp_scope BEFORE UPDATE OF scope_key,scope_kind ON mcp_server_installation
WHEN EXISTS (SELECT 1 FROM plugin_component c JOIN plugin_installation p ON p.id=c.plugin_id
    WHERE c.mcp_installation_id=OLD.id AND (p.workspace_id!=NEW.scope_key OR NEW.scope_kind!='workspace'))
BEGIN SELECT RAISE(ABORT,'plugin MCP scope mismatch'); END;
"#).await?;
        for (name, event) in [
            ("plugin_component_insert", "INSERT"),
            ("plugin_component_update", "UPDATE"),
        ] {
            manager.get_connection().execute_unprepared(&format!(r#"
CREATE TRIGGER {name} BEFORE {event} ON plugin_component
WHEN NOT EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.plugin_id)
    OR (NEW.skill_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM skill_installation s JOIN plugin_installation p ON p.id=NEW.plugin_id
        WHERE s.id=NEW.skill_id AND s.scope_key=p.workspace_id AND s.pack_id IS NULL))
    OR (NEW.mcp_installation_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM mcp_server_installation m JOIN plugin_installation p ON p.id=NEW.plugin_id
        WHERE m.id=NEW.mcp_installation_id AND m.scope_key=p.workspace_id AND m.scope_kind='workspace'))
BEGIN SELECT RAISE(ABORT,'plugin component relationship invalid'); END;
"#)).await?;
        }
        manager.get_connection().execute_unprepared(r#"
CREATE TRIGGER plugin_component_bound BEFORE INSERT ON plugin_component
WHEN NOT EXISTS (SELECT 1 FROM plugin_component WHERE plugin_id=NEW.plugin_id AND kind=NEW.kind AND member_key=NEW.member_key)
    AND (SELECT count(*) FROM plugin_component WHERE plugin_id=NEW.plugin_id) >= 256
BEGIN SELECT RAISE(ABORT,'plugin component limit'); END;
"#).await?;

        if !manager
            .has_column("skill_upload_session", "purpose")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table("skill_upload_session")
                        .add_column(
                            text("purpose")
                                .default("skill")
                                .check(Expr::col("purpose").is_in(["skill", "plugin"])),
                        )
                        .to_owned(),
                )
                .await?;
        }
        if !manager.has_column("turn", "plugin_selection_json").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table("turn")
                        .add_column(text("plugin_selection_json").null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Remove all triggers, including those attached to existing child tables,
        // before dropping the parent/links they reference.
        for name in [
            "plugin_workspace_insert",
            "plugin_workspace_immutable",
            "plugin_workspace_restrict",
            "plugin_parent_restrict",
            "plugin_skill_unlink",
            "plugin_mcp_unlink",
            "plugin_skill_scope",
            "plugin_mcp_scope",
            "plugin_component_insert",
            "plugin_component_update",
            "plugin_component_bound",
        ] {
            manager
                .get_connection()
                .execute_unprepared(&format!("DROP TRIGGER IF EXISTS {name}"))
                .await?;
        }
        for (table, column) in [
            ("turn", "plugin_selection_json"),
            ("skill_upload_session", "purpose"),
        ] {
            if manager.has_column(table, column).await? {
                manager
                    .alter_table(Table::alter().table(table).drop_column(column).to_owned())
                    .await?;
            }
        }
        for table in [COMPONENT, INSTALLATION] {
            manager
                .drop_table(Table::drop().table(table).to_owned())
                .await?;
        }
        Ok(())
    }
}
