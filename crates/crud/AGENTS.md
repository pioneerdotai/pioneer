# Queries and Repositories

These rules govern application implementation. For direct SQL inspection during
debugging, see the root [diagnostics scope](../../AGENTS.md#diagnostics).

- Put ordinary CRUD in the existing repositories and expose it through
  `CrudStore`, using Entity/Model/ActiveModel and typed projections. Follow a
  relevant existing method's structure, not a separate access path in a caller.
- Use SeaQuery expressions within the ORM path where needed. Building a typed
  `Query` and executing a `Statement` through `query_*_raw` does not by itself
  satisfy the Entity/Model pattern for ordinary CRUD.
- Low-level/raw SQL is a local exception: document the specific gap in the
  installed API and bind values as parameters. JOINs, transactions or query
  complexity alone do not justify it; existing SQL is not blanket precedent.
- Gateway SQLite uses `foreign_keys=OFF`; declarative FK/relations may describe
  schema but do not enforce integrity or cascade deletion. Validate existence,
  ownership and access explicitly, and perform necessary dependent cleanup in
  the same atomic operation. PK/UNIQUE constraints remain effective.
- Regressions must use project access paths and the runtime connection settings.
  A cascade test with FK enabled does not establish behavior with FK disabled.
- Pass the operation's scoped database handle through helpers and repositories.
  Follow the root runtime, scheduling and transaction rules; do not create
  alternate connections or classify individual SQL calls here.
- For schema changes read [migration rules](../migration/AGENTS.md); new tables
  require [generated Entity](../entity/AGENTS.md) before their ordinary CRUD.
