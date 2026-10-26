# Generated Entity

- Generate this crate from a database produced by the normal Migrator in an
  isolated temporary database. Do not substitute hand-written or simplified DDL,
  or use a user's database for codegen.

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
