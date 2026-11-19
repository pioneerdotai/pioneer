-- Package ownership, component limits, subtype integrity, and unlink on deletion.

CREATE OR REPLACE FUNCTION "package_workspace_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS (SELECT 1 FROM workspace WHERE id=NEW.workspace_id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package workspace missing';
END;
$trigger$;

CREATE TRIGGER "package_workspace_insert" BEFORE INSERT
ON "package_installation" FOR EACH ROW EXECUTE FUNCTION "package_workspace_insert"();

CREATE OR REPLACE FUNCTION "package_identity_immutable"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.id!=OLD.id OR NEW.kind!=OLD.kind OR NEW.workspace_id!=OLD.workspace_id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package identity immutable';
END;
$trigger$;

CREATE TRIGGER "package_identity_immutable" BEFORE UPDATE OF id,kind,workspace_id
ON "package_installation" FOR EACH ROW EXECUTE FUNCTION "package_identity_immutable"();

CREATE OR REPLACE FUNCTION "package_workspace_restrict"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS (SELECT 1 FROM package_installation WHERE workspace_id=OLD.id)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'workspace still owns packages';
END;
$trigger$;

CREATE TRIGGER "package_workspace_restrict" BEFORE DELETE
ON "workspace" FOR EACH ROW EXECUTE FUNCTION "package_workspace_restrict"();

CREATE OR REPLACE FUNCTION "package_parent_restrict"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS (SELECT 1 FROM package_component WHERE package_id=OLD.id)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package still owns component links';
END;
$trigger$;

CREATE TRIGGER "package_parent_restrict" BEFORE DELETE
ON "package_installation" FOR EACH ROW EXECUTE FUNCTION "package_parent_restrict"();

CREATE OR REPLACE FUNCTION "package_skill_unlink"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE package_component
    SET skill_id = NULL
    WHERE skill_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "package_skill_unlink" AFTER DELETE
ON "skill_installation" FOR EACH ROW EXECUTE FUNCTION "package_skill_unlink"();

CREATE OR REPLACE FUNCTION "package_mcp_unlink"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    UPDATE package_component
    SET mcp_installation_id = NULL
    WHERE mcp_installation_id = old.id;
    RETURN OLD;
END;
$trigger$;

CREATE TRIGGER "package_mcp_unlink" AFTER DELETE
ON "mcp_server_installation" FOR EACH ROW EXECUTE FUNCTION "package_mcp_unlink"();

CREATE OR REPLACE FUNCTION "package_skill_scope"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    -- UPDATE already holds the installation row lock. This query takes
    -- a fresh READ COMMITTED snapshot after any wait for a component link.
    IF (EXISTS (SELECT 1 FROM package_component c JOIN package_installation p ON p.id=c.package_id
 WHERE c.skill_id=OLD.id AND (p.workspace_id!=NEW.scope_key OR NEW.pack_id IS NOT NULL))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package skill scope mismatch';
END;
$trigger$;

CREATE TRIGGER "package_skill_scope" BEFORE UPDATE OF scope_key,pack_id
ON "skill_installation" FOR EACH ROW EXECUTE FUNCTION "package_skill_scope"();

CREATE OR REPLACE FUNCTION "package_mcp_scope"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    -- UPDATE already holds the installation row lock. This query takes
    -- a fresh READ COMMITTED snapshot after any wait for a component link.
    IF (EXISTS (SELECT 1 FROM package_component c JOIN package_installation p ON p.id=c.package_id
 WHERE c.mcp_installation_id=OLD.id AND (p.workspace_id!=NEW.scope_key OR NEW.scope_kind!='workspace'))) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package MCP scope mismatch';
END;
$trigger$;

CREATE TRIGGER "package_mcp_scope" BEFORE UPDATE OF scope_key,scope_kind
ON "mcp_server_installation" FOR EACH ROW EXECUTE FUNCTION "package_mcp_scope"();

CREATE OR REPLACE FUNCTION "package_component_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.package_id!=OLD.package_id OR NEW.kind!=OLD.kind OR NEW.member_key!=OLD.member_key) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package component identity immutable';
END;
$trigger$;

CREATE TRIGGER "package_component_identity" BEFORE UPDATE OF package_id,kind,member_key
ON "package_component" FOR EACH ROW EXECUTE FUNCTION "package_component_identity"();

CREATE OR REPLACE FUNCTION "package_component_bound"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM package_installation WHERE id = NEW.package_id FOR NO KEY UPDATE;

    IF (NOT EXISTS (SELECT 1 FROM package_component WHERE package_id=NEW.package_id AND kind=NEW.kind AND member_key=NEW.member_key)
 AND (SELECT count(*) FROM package_component WHERE package_id=NEW.package_id)>=256) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package component limit';
END;
$trigger$;

CREATE TRIGGER "package_component_bound" BEFORE INSERT
ON "package_component" FOR EACH ROW EXECUTE FUNCTION "package_component_bound"();

CREATE OR REPLACE FUNCTION "plugin_extension_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS (SELECT 1 FROM package_installation WHERE id=NEW.id AND kind='plugin')
 OR EXISTS (SELECT 1 FROM app_installation WHERE id=NEW.id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'plugin extension parent mismatch';
END;
$trigger$;

CREATE TRIGGER "plugin_extension_insert" BEFORE INSERT
ON "plugin_installation" FOR EACH ROW EXECUTE FUNCTION "plugin_extension_insert"();

CREATE OR REPLACE FUNCTION "app_extension_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS (SELECT 1 FROM package_installation WHERE id=NEW.id AND kind='app')
 OR EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app extension parent mismatch';
END;
$trigger$;

CREATE TRIGGER "app_extension_insert" BEFORE INSERT
ON "app_installation" FOR EACH ROW EXECUTE FUNCTION "app_extension_insert"();

CREATE OR REPLACE FUNCTION "app_workspace_slug_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    PERFORM 1 FROM workspace w
    JOIN package_installation p ON p.workspace_id = w.id
    WHERE p.id = NEW.id FOR NO KEY UPDATE OF w;

    IF (EXISTS (
 SELECT 1 FROM app_installation a
 JOIN package_installation existing ON existing.id=a.id
 JOIN package_installation incoming ON incoming.id=NEW.id
 WHERE a.slug=NEW.slug AND existing.workspace_id=incoming.workspace_id
)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app slug already installed in workspace';
END;
$trigger$;

CREATE TRIGGER "app_workspace_slug_insert" BEFORE INSERT
ON "app_installation" FOR EACH ROW EXECUTE FUNCTION "app_workspace_slug_insert"();

CREATE OR REPLACE FUNCTION "plugin_extension_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.id!=OLD.id) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'plugin extension identity immutable';
END;
$trigger$;

CREATE TRIGGER "plugin_extension_identity" BEFORE UPDATE OF id
ON "plugin_installation" FOR EACH ROW EXECUTE FUNCTION "plugin_extension_identity"();

CREATE OR REPLACE FUNCTION "app_extension_identity"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NEW.id!=OLD.id OR NEW.slug!=OLD.slug) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app identity immutable';
END;
$trigger$;

CREATE TRIGGER "app_extension_identity" BEFORE UPDATE OF id,slug
ON "app_installation" FOR EACH ROW EXECUTE FUNCTION "app_extension_identity"();

CREATE OR REPLACE FUNCTION "plugin_extension_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS (SELECT 1 FROM package_installation WHERE id=OLD.id)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'delete package parent instead';
END;
$trigger$;

CREATE TRIGGER "plugin_extension_delete" BEFORE DELETE
ON "plugin_installation" FOR EACH ROW EXECUTE FUNCTION "plugin_extension_delete"();

CREATE OR REPLACE FUNCTION "app_extension_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (EXISTS (SELECT 1 FROM package_installation WHERE id=OLD.id)) IS NOT TRUE THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'delete package parent instead';
END;
$trigger$;

CREATE TRIGGER "app_extension_delete" BEFORE DELETE
ON "app_installation" FOR EACH ROW EXECUTE FUNCTION "app_extension_delete"();

CREATE OR REPLACE FUNCTION "package_component_validate"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
DECLARE
    package_workspace text;
BEGIN
    SELECT p.workspace_id INTO package_workspace
    FROM package_installation p
    WHERE p.id = NEW.package_id
      AND ((p.kind = 'plugin' AND EXISTS (SELECT 1 FROM plugin_installation WHERE id = p.id))
        OR (p.kind = 'app' AND EXISTS (SELECT 1 FROM app_installation WHERE id = p.id)));

    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package component relationship invalid';
    END IF;

    -- The component CHECK permits at most one installation. FOR SHARE
    -- conflicts with UPDATE of its scope fields. At READ COMMITTED PostgreSQL
    -- rechecks these predicates on the current row after a conflicting update.
    IF NEW.skill_id IS NOT NULL THEN
        PERFORM 1 FROM skill_installation s
        WHERE s.id = NEW.skill_id
          AND s.scope_key = package_workspace
          AND s.pack_id IS NULL
        FOR SHARE OF s;
    ELSIF NEW.mcp_installation_id IS NOT NULL THEN
        PERFORM 1 FROM mcp_server_installation m
        WHERE m.id = NEW.mcp_installation_id
          AND m.scope_key = package_workspace
          AND m.scope_kind = 'workspace'
        FOR SHARE OF m;
    ELSE
        RETURN NEW;
    END IF;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'package component relationship invalid';
    END IF;
    RETURN NEW;
END;
$trigger$;

CREATE TRIGGER "package_component_insert" BEFORE INSERT
ON "package_component" FOR EACH ROW EXECUTE FUNCTION "package_component_validate"();

CREATE TRIGGER "package_component_update" BEFORE UPDATE
ON "package_component" FOR EACH ROW EXECUTE FUNCTION "package_component_validate"();

CREATE OR REPLACE FUNCTION "plugin_keyword_parent_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.plugin_id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'plugin metadata parent missing';
END;
$trigger$;

CREATE TRIGGER "plugin_keyword_parent_insert" BEFORE INSERT
ON "plugin_keyword" FOR EACH ROW EXECUTE FUNCTION "plugin_keyword_parent_insert"();

CREATE OR REPLACE FUNCTION "plugin_keyword_parent_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.plugin_id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'plugin metadata parent missing';
END;
$trigger$;

CREATE TRIGGER "plugin_keyword_parent_update" BEFORE UPDATE
ON "plugin_keyword" FOR EACH ROW EXECUTE FUNCTION "plugin_keyword_parent_update"();

CREATE OR REPLACE FUNCTION "plugin_prompt_parent_insert"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.plugin_id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'plugin metadata parent missing';
END;
$trigger$;

CREATE TRIGGER "plugin_prompt_parent_insert" BEFORE INSERT
ON "plugin_default_prompt" FOR EACH ROW EXECUTE FUNCTION "plugin_prompt_parent_insert"();

CREATE OR REPLACE FUNCTION "plugin_prompt_parent_update"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    IF (NOT EXISTS (SELECT 1 FROM plugin_installation WHERE id=NEW.plugin_id)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'plugin metadata parent missing';
END;
$trigger$;

CREATE TRIGGER "plugin_prompt_parent_update" BEFORE UPDATE
ON "plugin_default_prompt" FOR EACH ROW EXECUTE FUNCTION "plugin_prompt_parent_update"();
