CREATE OR REPLACE FUNCTION "execution_observation_turn_delete"()
RETURNS trigger LANGUAGE plpgsql
AS $trigger$
BEGIN
    DELETE FROM provider_usage_observation AS ledger
    USING execution_observation AS source
    WHERE source.turn_id = OLD.id
      AND source.thread_id = OLD.thread_id
      AND source.workspace_id = ledger.workspace_id
      AND source.kind IN ('provider_attempt', 'external_usage_snapshot')
      AND ledger.id = CASE
            WHEN starts_with(source.operation_id, 'provider:journal:')
              THEN substr(source.operation_id, 18)
            WHEN starts_with(source.operation_id, 'provider:' || OLD.id || ':')
              THEN substr(source.operation_id, length('provider:' || OLD.id || ':') + 1)
            WHEN starts_with(source.operation_id, 'obs1_')
              THEN source.facts_json::json ->> 'provider_source_item_id'
          END
      AND CASE
            WHEN starts_with(source.operation_id, 'provider:journal:')
              THEN TRUE
            WHEN starts_with(source.operation_id, 'obs1_')
              THEN ledger.owner_id = OLD.thread_id
               AND (ledger.usage_json::json -> '_pioneer_observation' ->> 'live_only') = 'true'
               AND (ledger.usage_json::json -> '_pioneer_observation' ->> 'native_turn_id') = OLD.id
               AND (ledger.usage_json::json -> '_pioneer_observation' ->> 'native_source_sequence')
                   = source.sequence::text
            ELSE ledger.owner_id = OLD.thread_id
          END;

    DELETE FROM execution_observation
    WHERE turn_id = old.id;
    RETURN OLD;
END;
$trigger$;
