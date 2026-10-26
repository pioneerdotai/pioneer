# Gateway Data Migrations

- This module owns data-migration orchestration. Register startup work through
  [startup/mod.rs](startup/mod.rs) and reuse the existing lifecycle; do not move
  database backfills into feature modules or invent a parallel migration runner.
- Keep schema changes in the [migration crate](../../../migration/AGENTS.md).
  Delegate database operations to [CRUD repositories](../../../crud/AGENTS.md).
- Run heavy historical backfill through existing background maintenance, using
  bounded batches, durable progress and restart-safe, idempotent operations.
  Reuse the project's database quantum and cancellation mechanisms.
- A blocking startup gate needs a concrete correctness invariant and explicit
  ordering before affected runtime work. Background data preparation should not
  become an unconditional startup scan of the database.
- Completion markers must describe committed durable results. Retry, failure,
  cancellation or restart must not mark unfinished work complete or skip it.
- Keep database migration ownership separate from runtime watchers/services.
  Reuse [maintenance](maintenance/mod.rs) for ongoing database maintenance;
  preserve the root rules for scheduling, resource lifetimes and transactions.
