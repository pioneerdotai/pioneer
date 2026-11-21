# Queries and Repositories

These rules govern application implementation. For direct SQL inspection during
debugging, see the root [diagnostics scope](../../AGENTS.md#diagnostics).

- Put ordinary CRUD in the owning domain repository and expose it through
  `CrudStore`, using Entity/Model/ActiveModel and typed projections. Pass the
  facade's transaction to domain operations that commit together.
- Keep repository tests and their fixtures in the domain crate that owns the
  operation. Use the facade's test targets for public delegation and commits
  spanning domains. Run database suites through the PostgreSQL fixture runner
  described in the [root test guidance](../../AGENTS.md).
- Prepare portable payload bytes before reserving writer capacity. Capture
  encoded frames in one PostgreSQL snapshot and release the snapshot before
  decompression or hashing. Use the persisted offsets for range reads.
- Use SeaQuery expressions within the ORM path where needed. Building a typed
  `Query` and executing a `Statement` through `query_*_raw` does not by itself
  satisfy the Entity/Model pattern for ordinary CRUD.
- Low-level/raw SQL is a local exception: document the specific gap in the
  installed API and bind values as parameters. JOINs, transactions or query
  complexity alone do not justify it; existing SQL is not blanket precedent.
- PostgreSQL enforces the foreign keys, checks, and unique constraints in its
  schema. Use the declared cascade rules, validate domain ownership and access,
  and perform the remaining dependent cleanup in the same transaction.
- Regressions must use the project access paths and the runtime connection
  settings. Use separate connections to the same test database for concurrency
  and synchronize the connections where the operation locks or publishes.
- Pass the operation's scoped database handle through helpers and repositories.
  Follow the root runtime, scheduling and transaction rules; do not create
  alternate connections or classify individual SQL calls here.
- For schema changes read [migration rules](../migration/AGENTS.md); new tables
  require [generated Entity](../entity/AGENTS.md) before their ordinary CRUD.

## CRUD package boundaries

- Keep `CrudStore` entry points and their transaction helpers together in
  `src/store`. Import domain repositories from the crates that own them.
  Keep compatibility exports in domain modules under `src/api/`.
  Use `src/api.rs` to assemble the facade and preserve existing public paths.
- Domain repositories and their test targets use the scoped pool or passed
  transaction directly. Keep the facade outside their dependency graphs.
  Shared commit notifications live in `pioneer-crud-signals`.

## Background Invalidation

Follow the root [background contract](../../AGENTS.md#background-work-and-idle).
For each mutation affecting background work, preserve the relevant domain
signal at its semantic commit boundary, including custom atomic publishers,
trigger side effects, earlier deadlines and release of protection on old rows.
Reuse existing `WorkChange` guards before dispatch; complete known no-ops
quietly and preserve unknown outcomes on error/cancellation. Do not broadcast
every write to every domain or replace missing invalidation with polling.
