-- The action row is authoritative. This predicate only narrows the exhausted
-- outbox range, retaining legacy candidates when kind is absent or JSON null
-- or the payload is malformed. Valid JSON is lexed without decoding strings.
CREATE FUNCTION agent_action_outbox_start_candidate(payload text)
RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN CASE WHEN payload IS JSON OBJECT THEN COALESCE((
    SELECT value_token = 'null'
        OR value_token COLLATE "C" ~ $start$^"(s|\\u0073)(t|\\u0074)(a|\\u0061)(r|\\u0072)(t|\\u0074)(_|\\u005[fF])(a|\\u0061)(g|\\u0067)(e|\\u0065)(n|\\u006[eE])(t|\\u0074)"$$start$
    FROM (
        SELECT token[1] AS key_token, ordinal,
               lead(token[1]) OVER tokens AS separator,
               lead(token[1], 2) OVER tokens AS value_token,
               sum(CASE token[1]
                   WHEN '{' THEN 1 WHEN '[' THEN 1
                   WHEN '}' THEN -1 WHEN ']' THEN -1
                   ELSE 0 END) OVER tokens AS depth
        FROM regexp_matches(payload COLLATE "C",
            $lex$"(?:[^"\\]|\\.)*"|[][{}:,]|[a-z]+|[-0-9.eE+]+$lex$, 'g')
            WITH ORDINALITY AS t(token, ordinal)
        WINDOW tokens AS (ORDER BY ordinal)
    ) AS fields
    WHERE depth = 1 AND separator = ':'
      AND key_token COLLATE "C" ~ $kind$^"(k|\\u006[bB])(i|\\u0069)(n|\\u006[eE])(d|\\u0064)"$$kind$
    -- Match the first top-level kind, as in SQLite json_extract.
    ORDER BY ordinal
    LIMIT 1
), true) ELSE true END;
