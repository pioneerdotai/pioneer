DROP TRIGGER package_component_update ON package_component;

CREATE TRIGGER package_component_update
BEFORE UPDATE ON package_component
FOR EACH ROW EXECUTE FUNCTION package_component_validate();
