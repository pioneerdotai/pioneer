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
   AND ((existing.source_upload_id NOT LIKE 'dev:%' AND incoming.source_upload_id NOT LIKE 'dev:%')
     OR existing.source_upload_id=incoming.source_upload_id)
)) IS NOT TRUE THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app slug already installed in workspace';
END;
$trigger$;
