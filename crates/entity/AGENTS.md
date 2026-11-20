# Generated Entity

- Generate this crate from a database produced by the normal Migrator in an
  isolated temporary database. Do not substitute hand-written or simplified DDL,
  or use a user's database for codegen.

Use SeaORM CLI 2.0.4 with PostgreSQL and `postgres-vector` codegen support.

On PostgreSQL 18, refresh catalog statistics in the temporary database before
generation. SeaORM's catalog discovery can otherwise choose a slow plan or
return columns in a different order. The planner settings below apply only to
the generator process.

```sh
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -c '
  ANALYZE pg_catalog.pg_class;
  ANALYZE pg_catalog.pg_attribute;
  ANALYZE pg_catalog.pg_constraint;
  ANALYZE pg_catalog.pg_index;
'
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
  the generated types. Repeated generation must produce compiling models.
- Check `lib.rs`/`prelude.rs` exports and remove only genuinely obsolete generated
  files. Avoid unrelated generated-file churn. Ordinary consumers follow
  [CRUD rules](../crud/AGENTS.md).
