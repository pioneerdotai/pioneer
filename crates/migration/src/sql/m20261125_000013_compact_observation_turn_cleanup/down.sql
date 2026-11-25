CREATE OR REPLACE FUNCTION "execution_observation_turn_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM provider_usage_observation
    WHERE id IN (SELECT CASE
                          WHEN operation_id LIKE 'provider:journal:%'
                            THEN substr(operation_id, 18)
                          ELSE substr(operation_id
                                    , length('provider:' || old.id || ':') + 1)
                        END
                 FROM execution_observation
                 WHERE turn_id = old.id
                   AND kind IN ('provider_attempt', 'external_usage_snapshot')
                   AND operation_id LIKE 'provider:%');

    DELETE FROM execution_observation
    WHERE turn_id = old.id;
    RETURN OLD;
END;
$trigger$;
