//! Split package operations from domain metadata without rewriting turn history.
//! Installation cost is O(existing Plugin installations) for metadata relocation;
//! native rows and history are not scanned. IDs, paths, pending bytes and links survive.
use sea_orm_migration::{
    prelude::*,
    schema::*,
    sea_query::{Alias, ExprTrait, Func},
};

#[derive(DeriveMigrationName)]
pub struct Migration;
const METADATA: &[&str] = &[
    "name",
    "version",
    "display_name",
    "short_description",
    "description",
    "long_description",
    "developer_name",
    "author_name",
    "author_email",
    "author_url",
    "homepage",
    "repository",
    "documentation_url",
    "support_url",
    "privacy_policy_url",
    "terms_of_service_url",
    "license",
    "category",
    "brand_color",
    "logo_path",
    "logo_revision",
    "composer_icon_path",
    "composer_icon_revision",
];
const LEGACY_TRIGGERS: &[&str] = &[
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
];
async fn rename(m: &SchemaManager<'_>, from: &str, to: &str) -> Result<(), DbErr> {
    m.rename_table(
        Table::rename()
            .table(Alias::new(from), Alias::new(to))
            .to_owned(),
    )
    .await
}
async fn rename_column(
    m: &SchemaManager<'_>,
    table: &str,
    from: &str,
    to: &str,
) -> Result<(), DbErr> {
    m.alter_table(
        Table::alter()
            .table(Alias::new(table))
            .rename_column(Alias::new(from), Alias::new(to))
            .to_owned(),
    )
    .await
}
async fn drop_column(m: &SchemaManager<'_>, table: &str, column: &str) -> Result<(), DbErr> {
    m.alter_table(
        Table::alter()
            .table(Alias::new(table))
            .drop_column(Alias::new(column))
            .to_owned(),
    )
    .await
}
async fn purpose(m: &SchemaManager<'_>, apps: bool) -> Result<(), DbErr> {
    rename_column(m, "skill_upload_session", "purpose", "previous_purpose").await?;
    let allowed = if apps {
        vec!["skill", "plugin", "app"]
    } else {
        vec!["skill", "plugin"]
    };
    m.alter_table(
        Table::alter()
            .table("skill_upload_session")
            .add_column(
                text("purpose")
                    .default("skill")
                    .check(Expr::col("purpose").is_in(allowed)),
            )
            .to_owned(),
    )
    .await?;
    m.get_connection()
        .execute(
            Query::update()
                .table("skill_upload_session")
                .value("purpose", Expr::col("previous_purpose")),
        )
        .await?;
    drop_column(m, "skill_upload_session", "previous_purpose").await
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        let db = m.get_connection();
        // SeaQuery does not provide trigger DDL; only trigger creation/removal uses SQL.
        for name in LEGACY_TRIGGERS {
            db.execute_unprepared(&format!("DROP TRIGGER {name}"))
                .await?;
        }
        rename(m, "plugin_installation", "package_installation").await?;
        let mut extension = Table::create();
        extension
            .table("plugin_installation")
            .col(text("id").primary_key());
        for field in METADATA {
            let mut column = text(*field);
            if *field != "name" {
                column.null();
            }
            extension.col(column);
        }
        extension.foreign_key(
            ForeignKey::create()
                .from("plugin_installation", "id")
                .to("package_installation", "id")
                .on_delete(ForeignKeyAction::Restrict),
        );
        m.create_table(extension.to_owned()).await?;
        let columns: Vec<_> = std::iter::once("id")
            .chain(METADATA.iter().copied())
            .map(Alias::new)
            .collect();
        let select = Query::select()
            .columns(columns.clone())
            .from("package_installation")
            .to_owned();
        db.execute(
            Query::insert()
                .into_table("plugin_installation")
                .columns(columns)
                .select_from(select)
                .map_err(|e| DbErr::Custom(e.to_string()))?,
        )
        .await?;
        for field in METADATA {
            drop_column(m, "package_installation", field).await?;
        }
        m.alter_table(
            Table::alter()
                .table("package_installation")
                .add_column(
                    text("kind")
                        .default("plugin")
                        .check(Expr::col("kind").is_in(["plugin", "app"])),
                )
                .to_owned(),
        )
        .await?;
        rename_column(m, "package_installation", "last_error", "last_error_json").await?;
        // Preserve every legacy diagnostic as a string inside a structured document,
        // including semantic codes used for retained-data projection and recovery.
        db.execute(
            Query::update()
                .table("package_installation")
                .value(
                    "last_error_json",
                    Func::cust(Alias::new("json_object"))
                        .arg(Expr::val("legacy"))
                        .arg(Expr::col("last_error_json")),
                )
                .and_where(Expr::col("last_error_json").is_not_null()),
        )
        .await?;
        m.drop_index(
            Index::drop()
                .name("ix_plugin_workspace")
                .table("package_installation")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("ix_package_workspace_kind")
                .table("package_installation")
                .col("workspace_id")
                .col("kind")
                .to_owned(),
        )
        .await?;
        m.create_table(Table::create().table("app_installation")
            .col(text("id").primary_key()).col(text("slug"))
            .col(text("manifest_json").check(Expr::cust("json_valid(manifest_json) AND json_type(manifest_json)='object' AND json_type(manifest_json,'$.slug')='text' AND json_extract(manifest_json,'$.slug')=slug")))
            .col(text("localizations_json").default("{}").check(Expr::cust("json_valid(localizations_json) AND json_type(localizations_json)='object'")))
            .foreign_key(ForeignKey::create().from("app_installation", "id").to("package_installation", "id").on_delete(ForeignKeyAction::Restrict)).to_owned()).await?;
        // Apps reserve a slug within the parent's workspace. The index serves
        // that lookup; the joined uniqueness guard is installed below. This
        // table starts empty, so no existing App rows are scanned or rebuilt.
        m.create_index(
            Index::create()
                .name("ix_app_slug")
                .table("app_installation")
                .col("slug")
                .to_owned(),
        )
        .await?;
        rename(m, "plugin_component", "package_component").await?;
        rename_column(m, "package_component", "plugin_id", "package_id").await?;
        // A single persisted selection. Old snapshots decode kind=plugin by default;
        // prepared/ready phases and children are retained byte-for-byte.
        rename_column(m, "turn", "plugin_selection_json", "package_selection_json").await?;
        purpose(m, true).await?;
        install_triggers(m).await
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        let db = m.get_connection();
        // Downgrade cannot silently destroy an App or reinterpret its upload as Plugin.
        let apps = db
            .query_one(
                Query::select()
                    .expr(Expr::val(1))
                    .from("package_installation")
                    .and_where(Expr::col("kind").eq("app"))
                    .limit(1),
            )
            .await?;
        let uploads = db
            .query_one(
                Query::select()
                    .expr(Expr::val(1))
                    .from("skill_upload_session")
                    .and_where(Expr::col("purpose").eq("app"))
                    .limit(1),
            )
            .await?;
        if apps.is_some() || uploads.is_some() {
            return Err(DbErr::Custom(
                "remove Apps and expire App uploads before package schema downgrade".into(),
            ));
        }
        drop_triggers(m).await?;
        purpose(m, false).await?;
        rename_column(m, "turn", "package_selection_json", "plugin_selection_json").await?;
        for field in METADATA {
            let mut c = text(*field);
            if *field == "name" {
                c.default("");
            } else {
                c.null();
            }
            m.alter_table(
                Table::alter()
                    .table("package_installation")
                    .add_column(c)
                    .to_owned(),
            )
            .await?;
            let sub = Query::select()
                .column(Alias::new(*field))
                .from("plugin_installation")
                .and_where(
                    Expr::col((Alias::new("plugin_installation"), Alias::new("id"))).eq(Expr::col(
                        (Alias::new("package_installation"), Alias::new("id")),
                    )),
                )
                .to_owned();
            db.execute(Query::update().table("package_installation").value(
                Alias::new(*field),
                SimpleExpr::SubQuery(None, Box::new(sub.into())),
            ))
            .await?;
        }
        // New structured operation errors expose a bounded message/code; historical
        // diagnostics recover exactly their original legacy string.
        db.execute(
            Query::update()
                .table("package_installation")
                .value(
                    "last_error_json",
                    Func::coalesce([
                        Func::cust(Alias::new("json_extract"))
                            .arg(Expr::col("last_error_json"))
                            .arg(Expr::val("$.legacy")),
                        Func::cust(Alias::new("json_extract"))
                            .arg(Expr::col("last_error_json"))
                            .arg(Expr::val("$.code")),
                    ]),
                )
                .and_where(Expr::col("last_error_json").is_not_null()),
        )
        .await?;
        rename_column(m, "package_installation", "last_error_json", "last_error").await?;
        m.drop_index(
            Index::drop()
                .name("ix_package_workspace_kind")
                .table("package_installation")
                .to_owned(),
        )
        .await?;
        drop_column(m, "package_installation", "kind").await?;
        m.drop_table(Table::drop().table("app_installation").to_owned())
            .await?;
        m.drop_table(Table::drop().table("plugin_installation").to_owned())
            .await?;
        rename(m, "package_installation", "plugin_installation").await?;
        m.create_index(
            Index::create()
                .name("ix_plugin_workspace")
                .table("plugin_installation")
                .col("workspace_id")
                .to_owned(),
        )
        .await?;
        rename_column(m, "package_component", "package_id", "plugin_id").await?;
        rename(m, "package_component", "plugin_component").await?;
        restore_legacy_triggers(m).await
    }
}
async fn drop_triggers(m: &SchemaManager<'_>) -> Result<(), DbErr> {
    for name in PACKAGE_TRIGGERS {
        m.get_connection()
            .execute_unprepared(&format!("DROP TRIGGER {name}"))
            .await?;
    }
    Ok(())
}
async fn restore_legacy_triggers(m: &SchemaManager<'_>) -> Result<(), DbErr> {
    m.get_connection().execute_unprepared(r#"CREATE TRIGGER plugin_workspace_insert BEFORE INSERT ON plugin_installation
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
CREATE TRIGGER plugin_component_insert BEFORE INSERT ON plugin_component
WHEN NOT EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.plugin_id)
    OR (NEW.skill_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM skill_installation s JOIN plugin_installation p ON p.id=NEW.plugin_id
        WHERE s.id=NEW.skill_id AND s.scope_key=p.workspace_id AND s.pack_id IS NULL))
    OR (NEW.mcp_installation_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM mcp_server_installation m JOIN plugin_installation p ON p.id=NEW.plugin_id
        WHERE m.id=NEW.mcp_installation_id AND m.scope_key=p.workspace_id AND m.scope_kind='workspace'))
BEGIN SELECT RAISE(ABORT,'plugin component relationship invalid'); END;
CREATE TRIGGER plugin_component_update BEFORE UPDATE ON plugin_component
WHEN NOT EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.plugin_id)
    OR (NEW.skill_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM skill_installation s JOIN plugin_installation p ON p.id=NEW.plugin_id
        WHERE s.id=NEW.skill_id AND s.scope_key=p.workspace_id AND s.pack_id IS NULL))
    OR (NEW.mcp_installation_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM mcp_server_installation m JOIN plugin_installation p ON p.id=NEW.plugin_id
        WHERE m.id=NEW.mcp_installation_id AND m.scope_key=p.workspace_id AND m.scope_kind='workspace'))
BEGIN SELECT RAISE(ABORT,'plugin component relationship invalid'); END;
CREATE TRIGGER plugin_component_bound BEFORE INSERT ON plugin_component
WHEN NOT EXISTS (SELECT 1 FROM plugin_component WHERE plugin_id=NEW.plugin_id AND kind=NEW.kind AND member_key=NEW.member_key)
    AND (SELECT count(*) FROM plugin_component WHERE plugin_id=NEW.plugin_id) >= 256
BEGIN SELECT RAISE(ABORT,'plugin component limit'); END;
"#).await?;
    Ok(())
}

const PACKAGE_TRIGGERS: &[&str] = &[
    "package_workspace_insert",
    "package_identity_immutable",
    "package_workspace_restrict",
    "package_parent_restrict",
    "package_domain_cleanup",
    "package_skill_unlink",
    "package_mcp_unlink",
    "package_skill_scope",
    "package_mcp_scope",
    "package_component_insert",
    "package_component_update",
    "package_component_bound",
    "package_component_identity",
    "plugin_extension_insert",
    "app_extension_insert",
    "app_workspace_slug_insert",
    "plugin_extension_identity",
    "app_extension_identity",
    "plugin_extension_delete",
    "app_extension_delete",
    "plugin_keyword_parent_insert",
    "plugin_keyword_parent_update",
    "plugin_prompt_parent_insert",
    "plugin_prompt_parent_update",
];
async fn install_triggers(m: &SchemaManager<'_>) -> Result<(), DbErr> {
    // SQLite FK checks are intentionally OFF at runtime. These triggers protect
    // cross-row relationships; reservation CRUD creates parent+extension atomically.
    let db = m.get_connection();
    db.execute_unprepared(r#"
CREATE TRIGGER package_workspace_insert BEFORE INSERT ON package_installation
WHEN NOT EXISTS (SELECT 1 FROM workspace WHERE id=NEW.workspace_id)
BEGIN SELECT RAISE(ABORT,'package workspace missing'); END;
CREATE TRIGGER package_identity_immutable BEFORE UPDATE OF id,kind,workspace_id ON package_installation
WHEN NEW.id!=OLD.id OR NEW.kind!=OLD.kind OR NEW.workspace_id!=OLD.workspace_id
BEGIN SELECT RAISE(ABORT,'package identity immutable'); END;
CREATE TRIGGER package_workspace_restrict BEFORE DELETE ON workspace
WHEN EXISTS (SELECT 1 FROM package_installation WHERE workspace_id=OLD.id)
BEGIN SELECT RAISE(ABORT,'workspace still owns packages'); END;
CREATE TRIGGER package_parent_restrict BEFORE DELETE ON package_installation
WHEN EXISTS (SELECT 1 FROM package_component WHERE package_id=OLD.id)
BEGIN SELECT RAISE(ABORT,'package still owns component links'); END;
CREATE TRIGGER package_domain_cleanup AFTER DELETE ON package_installation
BEGIN
 DELETE FROM plugin_keyword WHERE plugin_id=OLD.id;
 DELETE FROM plugin_default_prompt WHERE plugin_id=OLD.id;
 DELETE FROM plugin_installation WHERE id=OLD.id;
 DELETE FROM app_installation WHERE id=OLD.id;
END;
CREATE TRIGGER package_skill_unlink AFTER DELETE ON skill_installation
BEGIN UPDATE package_component SET skill_id=NULL WHERE skill_id=OLD.id; END;
CREATE TRIGGER package_mcp_unlink AFTER DELETE ON mcp_server_installation
BEGIN UPDATE package_component SET mcp_installation_id=NULL WHERE mcp_installation_id=OLD.id; END;
CREATE TRIGGER package_skill_scope BEFORE UPDATE OF scope_key,pack_id ON skill_installation
WHEN EXISTS (SELECT 1 FROM package_component c JOIN package_installation p ON p.id=c.package_id
 WHERE c.skill_id=OLD.id AND (p.workspace_id!=NEW.scope_key OR NEW.pack_id IS NOT NULL))
BEGIN SELECT RAISE(ABORT,'package skill scope mismatch'); END;
CREATE TRIGGER package_mcp_scope BEFORE UPDATE OF scope_key,scope_kind ON mcp_server_installation
WHEN EXISTS (SELECT 1 FROM package_component c JOIN package_installation p ON p.id=c.package_id
 WHERE c.mcp_installation_id=OLD.id AND (p.workspace_id!=NEW.scope_key OR NEW.scope_kind!='workspace'))
BEGIN SELECT RAISE(ABORT,'package MCP scope mismatch'); END;
CREATE TRIGGER package_component_identity BEFORE UPDATE OF package_id,kind,member_key ON package_component
WHEN NEW.package_id!=OLD.package_id OR NEW.kind!=OLD.kind OR NEW.member_key!=OLD.member_key
BEGIN SELECT RAISE(ABORT,'package component identity immutable'); END;
CREATE TRIGGER package_component_bound BEFORE INSERT ON package_component
WHEN NOT EXISTS (SELECT 1 FROM package_component WHERE package_id=NEW.package_id AND kind=NEW.kind AND member_key=NEW.member_key)
 AND (SELECT count(*) FROM package_component WHERE package_id=NEW.package_id)>=256
BEGIN SELECT RAISE(ABORT,'package component limit'); END;
CREATE TRIGGER plugin_extension_insert BEFORE INSERT ON plugin_installation
WHEN NOT EXISTS (SELECT 1 FROM package_installation WHERE id=NEW.id AND kind='plugin')
 OR EXISTS (SELECT 1 FROM app_installation WHERE id=NEW.id)
BEGIN SELECT RAISE(ABORT,'plugin extension parent mismatch'); END;
CREATE TRIGGER app_extension_insert BEFORE INSERT ON app_installation
WHEN NOT EXISTS (SELECT 1 FROM package_installation WHERE id=NEW.id AND kind='app')
 OR EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.id)
BEGIN SELECT RAISE(ABORT,'app extension parent mismatch'); END;
-- A UNIQUE index cannot span metadata and the operational parent table.
-- Workspace, App id and slug are immutable under the identity guards.
CREATE TRIGGER app_workspace_slug_insert BEFORE INSERT ON app_installation
WHEN EXISTS (
 SELECT 1 FROM app_installation a
 JOIN package_installation existing ON existing.id=a.id
 JOIN package_installation incoming ON incoming.id=NEW.id
 WHERE a.slug=NEW.slug AND existing.workspace_id=incoming.workspace_id
)
BEGIN SELECT RAISE(ABORT,'app slug already installed in workspace'); END;
CREATE TRIGGER plugin_extension_identity BEFORE UPDATE OF id ON plugin_installation
WHEN NEW.id!=OLD.id
BEGIN SELECT RAISE(ABORT,'plugin extension identity immutable'); END;
CREATE TRIGGER app_extension_identity BEFORE UPDATE OF id,slug ON app_installation
WHEN NEW.id!=OLD.id OR NEW.slug!=OLD.slug
BEGIN SELECT RAISE(ABORT,'app identity immutable'); END;
CREATE TRIGGER plugin_extension_delete BEFORE DELETE ON plugin_installation
WHEN EXISTS (SELECT 1 FROM package_installation WHERE id=OLD.id)
BEGIN SELECT RAISE(ABORT,'delete package parent instead'); END;
CREATE TRIGGER app_extension_delete BEFORE DELETE ON app_installation
WHEN EXISTS (SELECT 1 FROM package_installation WHERE id=OLD.id)
BEGIN SELECT RAISE(ABORT,'delete package parent instead'); END;
"#).await?;
    for (name, event) in [
        ("package_component_insert", "INSERT"),
        ("package_component_update", "UPDATE"),
    ] {
        db.execute_unprepared(&format!(
            r#"
CREATE TRIGGER {name} BEFORE {event} ON package_component
WHEN NOT EXISTS (SELECT 1 FROM package_installation p WHERE p.id=NEW.package_id
 AND ((p.kind='plugin' AND EXISTS (SELECT 1 FROM plugin_installation WHERE id=p.id))
 OR (p.kind='app' AND EXISTS (SELECT 1 FROM app_installation WHERE id=p.id))))
 OR (NEW.skill_id IS NOT NULL AND NOT EXISTS (
 SELECT 1 FROM skill_installation s JOIN package_installation p ON p.id=NEW.package_id
 WHERE s.id=NEW.skill_id AND s.scope_key=p.workspace_id AND s.pack_id IS NULL))
 OR (NEW.mcp_installation_id IS NOT NULL AND NOT EXISTS (
 SELECT 1 FROM mcp_server_installation c JOIN package_installation p ON p.id=NEW.package_id
 WHERE c.id=NEW.mcp_installation_id AND c.scope_key=p.workspace_id AND c.scope_kind='workspace'))
BEGIN SELECT RAISE(ABORT,'package component relationship invalid'); END;
"#
        ))
        .await?;
    }
    for (table, prefix) in [
        ("plugin_keyword", "plugin_keyword"),
        ("plugin_default_prompt", "plugin_prompt"),
    ] {
        for (event, suffix) in [("INSERT", "insert"), ("UPDATE", "update")] {
            db.execute_unprepared(&format!(
                r#"
CREATE TRIGGER {prefix}_parent_{suffix} BEFORE {event} ON {table}
WHEN NOT EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.plugin_id)
BEGIN SELECT RAISE(ABORT,'plugin metadata parent missing'); END;
"#
            ))
            .await?;
        }
    }
    Ok(())
}
