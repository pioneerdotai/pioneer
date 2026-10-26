# Schema Migrations

- Use `SchemaManager`/SeaQuery builders and migration-local `DeriveIden`/`Alias`.
  Never depend on `pioneer-entity`, including Entity for existing tables.
- Raw SQL requires a specific operation missing from the installed builders,
  such as trigger DDL. Document that gap; historical SQL is not blanket precedent.
- Change applied or released schema through a new migration registered in
  [Migrator](src/lib.rs). Preserve historical names, order and semantics.
  An unpublished, unapplied draft may be amended instead of adding another file.
- `up` installs the new schema; `down` restores the previous schema, removing
  dependent objects in the appropriate order. Do not promise recovery of deleted data.
- Justify each new table/index and its installation cost on existing databases,
  including scans and index construction over large tables.
- Keep schema installation limited to schema changes and necessary seed rows.
  Heavy historical backfill belongs to the existing
  [Gateway data-migration mechanism](../gateway/src/database/AGENTS.md).
- After schema changes, follow [Entity generation](../entity/AGENTS.md).
  The schema must support repeatable generation without manual model repairs.
- Put regressions in the existing integration-fixture suites, outside migration
  source files. Cover fresh install, upgrade and schema rollback/reapply.
