-- Bind a Unicode collation at install time so tokenization and case mapping
-- are independent of the database's default locale. PostgreSQL 16 can use
-- the ICU root collation or an explicitly named UTF-8 libc locale.
DO $install$
DECLARE
    preferred text[] := ARRAY['pg_c_utf8', 'und-x-icu',
                              'C.utf8', 'C.UTF-8', 'en_US.utf8', 'en_US.UTF-8'];
    -- Combining Diacritical Marks and their extended/supplement blocks.
    accent_range constant text := U&'\0300-\036F\1AB0-\1AFF\1DC0-\1DFF\FE20-\FE2F';
    token_collation text;
BEGIN
    SELECT c.collname INTO token_collation
    FROM pg_catalog.pg_collation c
    WHERE c.collnamespace = 'pg_catalog'::regnamespace
      AND c.collencoding IN (-1, pg_char_to_encoding(getdatabaseencoding()))
      AND c.collname = ANY (preferred)
    ORDER BY array_position(preferred, c.collname::text)
    LIMIT 1;

    IF token_collation IS NULL THEN
        RAISE EXCEPTION 'episodic search requires an available Unicode collation'
            USING ERRCODE = '0A000';
    END IF;

    -- Strip accents from Latin letters with an ASCII base before splitting
    -- words. Recompose the other letters so, for example, ё stays distinct
    -- from е. Fold final sigma after lowercasing to handle both Greek forms.
    EXECUTE format($function$
        CREATE FUNCTION thread_episodic_search_terms(source text)
        RETURNS text[]
        LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
        RETURN ARRAY(
            SELECT DISTINCT normalize(
                translate(lower(t.token[1] COLLATE pg_catalog.%1$I), 'ς', 'σ'),
                NFC
            ) COLLATE "C"
            FROM pg_catalog.regexp_matches(
                normalize(
                    pg_catalog.regexp_replace(
                        normalize(source, NFD) COLLATE "C", %2$L, E'\\1', 'g'
                    ),
                    NFC
                ) COLLATE pg_catalog.%1$I,
                %3$L, 'g'
            ) AS t(token)
        );
    $function$, token_collation,
        '([A-Za-z])[' || accent_range || ']+',
        '[[:alnum:]][[:alnum:]' || accent_range || ']*'
    );
END;
$install$;

-- Fixed-size GIN keys also admit terms larger than a GIN index entry.
-- The search predicate rechecks exact terms after the indexed overlap.
CREATE FUNCTION thread_episodic_search_keys(source text)
RETURNS bytea[]
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN ARRAY(
    SELECT sha256(convert_to(l.term, 'UTF8'))
    FROM unnest(thread_episodic_search_terms(source)) AS l(term)
);
