use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.get_connection().execute_unprepared(r#"
CREATE TABLE plugin_installation (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    version TEXT,
    source_upload_id TEXT NOT NULL UNIQUE,
    package_path TEXT NOT NULL,
    data_path TEXT NOT NULL,
    package_fingerprint TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('installing','installed','updating','removing','interrupted')),
    revision BIGINT NOT NULL CHECK(revision > 0),
    pending_json TEXT CHECK(pending_json IS NULL OR length(pending_json) <= 65536),
    last_error TEXT,
    created_at TIMESTAMP_WITH_TIME_ZONE NOT NULL,
    updated_at TIMESTAMP_WITH_TIME_ZONE NOT NULL
);
CREATE INDEX ix_plugin_workspace ON plugin_installation(workspace_id);
CREATE TABLE plugin_component (
    plugin_id TEXT NOT NULL REFERENCES plugin_installation(id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK(kind IN ('skill','mcp')),
    member_key TEXT NOT NULL,
    member_path TEXT,
    skill_id TEXT UNIQUE REFERENCES skill_installation(id) ON DELETE SET NULL,
    mcp_installation_id TEXT UNIQUE REFERENCES mcp_server_installation(id) ON DELETE SET NULL,
    package_fingerprint TEXT,
    status TEXT NOT NULL CHECK(status IN ('installed','invalid','failed','removed_by_user')),
    diagnostic TEXT,
    override_fields_json TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (plugin_id,kind,member_key),
    CHECK((kind = 'skill' AND mcp_installation_id IS NULL) OR
          (kind = 'mcp' AND skill_id IS NULL))
);
"#).await?;
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

        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TRIGGER plugin_workspace_restrict; DROP TRIGGER plugin_skill_unlink; DROP TRIGGER plugin_mcp_unlink; DROP TRIGGER plugin_skill_scope; DROP TRIGGER plugin_mcp_scope; DROP TABLE plugin_component; DROP TABLE plugin_installation;")
            .await?;
        Ok(())
    }
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}
