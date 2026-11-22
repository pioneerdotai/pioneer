DROP TRIGGER package_component_update ON package_component;

-- Status and override changes leave the validated relationship intact.
-- Rechecking it under the component row lock conflicts with native writes
-- that already hold the Skill or MCP installation row and need the component.
CREATE TRIGGER package_component_update
BEFORE UPDATE ON package_component
FOR EACH ROW
WHEN (OLD.package_id IS DISTINCT FROM NEW.package_id
   OR OLD.skill_id IS DISTINCT FROM NEW.skill_id
   OR OLD.mcp_installation_id IS DISTINCT FROM NEW.mcp_installation_id)
EXECUTE FUNCTION package_component_validate();
