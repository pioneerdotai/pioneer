# Generated Entity

- Generate this crate from a database produced by the normal Migrator in an
  isolated temporary database. Do not substitute hand-written or simplified DDL,
  or use a user's database for codegen.

Use SeaORM CLI 2.0.4 with PostgreSQL and `postgres-vector` codegen support.

SeaORM 2.0.4 reads `information_schema.columns` without an ordering clause.
Give that catalog view a stable column order in the isolated codegen database
and refresh the catalog statistics. Run the following only in the disposable
database with the codegen administrator role after applying all migrations.
These statements leave the Pioneer tables, constraints and indexes as deployed.
The planner settings apply only to the generator process.

```sh
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 <<'SQL'
DO $codegen$
DECLARE
    catalog_view text := pg_get_viewdef('information_schema.columns'::regclass);
BEGIN
    EXECUTE 'CREATE OR REPLACE VIEW information_schema.columns AS ' ||
        rtrim(catalog_view, E';\n\r ') ||
        ' ORDER BY table_schema, table_name, ordinal_position';
END
$codegen$;
ANALYZE pg_catalog.pg_class;
ANALYZE pg_catalog.pg_attribute;
ANALYZE pg_catalog.pg_constraint;
ANALYZE pg_catalog.pg_index;
SQL
export PGOPTIONS='-c join_collapse_limit=1 -c from_collapse_limit=1 -c enable_nestloop=off -c max_parallel_workers_per_gather=0'
```

Run from the repository root with `DATABASE_URL` explicitly set to that temporary
database, using exactly these generation options:

```sh
sea-orm-cli generate entity \
  --lib \
  --with-serde both \
  --output-dir crates/entity/src \
  --entity-format dense \
  --ignore-tables seaql_migrations
```

- Do not hand-patch generated models or add extra codegen attributes. Fix schema
  defects under [migration rules](../migration/AGENTS.md), or adapt consumers to
  the generated types. Verify generation from two freshly migrated databases.
- Check `lib.rs`/`prelude.rs` exports and remove only genuinely obsolete generated
  files. Avoid unrelated generated-file churn. Ordinary consumers follow
  [CRUD rules](../crud/AGENTS.md).
- For composite key lookups, filter by the generated column names. SeaORM's
  positional key tuples follow Rust field order, which can change on generation.
