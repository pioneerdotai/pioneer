# Database Architecture

These rules apply to the whole repository. More specific `AGENTS.md` files may
add crate-local rules, but must not weaken the database invariants below.

## Runtime Model

The Gateway SQLite runtime has two independent contours:

- a read-only reader pool, protected with `PRAGMA query_only=ON`;
- one serialized writer executor, because SQLite still has only one effective
  writer.

All database access must go through `SqliteDatabase`, `CrudStore`, and the
existing repositories. Do not open ad hoc SQLite connections, use the writer
pool directly, acquire scheduling permits manually, or bypass the writer
executor.

Row-returning SQL is routed from the actual statement. Ordinary reads use the
reader pool; writes, write transactions, and mutating `... RETURNING`
statements use the writer. A `SELECT` that invokes a mutating SQLite extension
must use the explicit writer query API.

## Scheduling Classes

Database handles carry their scheduling class. Queries and repositories must
inherit the class from the scoped handle instead of classifying individual
call sites.

- `Interactive` is the default for request work whose result a client is
  waiting for.
- `Maintenance` must be selected explicitly for background scanning,
  reconciliation, compression, migration/backfill, cleanup, indexing, and
  periodic work. Use `CrudStore::with_maintenance_access()` so both reads and
  writes are scoped correctly.
- `Critical` writes are reserved for narrowly defined control-plane or
  correctness work that must outrank ordinary writes. Background discovery
  that needs critical commits must use
  `with_maintenance_reads_and_critical_writes()`; it must not turn its reads
  interactive.

Do not classify work from actor kind, repository method, SQL text at the call
site, or an arbitrary timeout. Do not promote background work to interactive
or critical to make a test or latency symptom disappear.

## Never Hold Database Capacity During Other Work

A writer reservation, write transaction, read transaction, query stream, or
maintenance-read permit may be held only while performing the immediate,
necessary database operation.

While any of those resources is held, do not perform:

- CPU-heavy parsing, serialization, compression, hashing, diffing, ranking,
  rendering, or large collection transformations;
- filesystem access or process execution;
- network, provider, MCP, or other service calls;
- channel sends, notification fanout, task joins, sleeps, retry backoff, or
  unrelated `await` points;
- unbounded iteration or work whose cost grows with the full database.

Prepare and validate inputs before acquiring database capacity. Commit or drop
the transaction/stream/permit before doing CPU work, I/O, notifications, or
backoff. An `await` is acceptable while a transaction is open only when it is
the database call required by that transaction.

The same rule applies to readers: an open read snapshot or stream occupies a
reader connection, and a maintenance stream also retains its limiter permit.
Fetch a bounded amount of data, release the database resource, then process the
rows.

## Transactions and Batches

Use a transaction only for a write set whose atomicity is a correctness
invariant. Inside it, perform only the reads and writes required to validate and
commit that write set. Never split an atomic domain transition merely to reduce
writer hold time.

For maintenance or bulk work that does not require whole-job atomicity:

1. discover or prepare a bounded batch;
2. open a short maintenance transaction;
3. revalidate any state that may have changed;
4. apply the bounded write set and commit;
5. release the writer before preparing, sleeping, yielding, or processing the
   next batch.

Every batch must be bounded by an explicit row, byte, or time-oriented quantum
and must be idempotent and restart-safe. A poison row must not create an
infinite retry loop or permanently block later rows; use an explicit terminal
state such as quarantine when the domain contract permits it.

`run_background_database_quantum` supplies maintenance scope and lock-race
retry behavior. It does not reserve the writer for the entire operation and
must not be changed to do so.

## Deadlines, Cancellation, and Backpressure

Use the operation's end-to-end request deadline. Do not add a shorter nested
timeout solely for reader-pool or writer-queue acquisition. Queue waiting is
part of the same operation budget.

Cancellation must drop queued reservations, transactions, streams, and read
permits promptly. Never detach database work that can outlive its owning
request unless it is an explicitly owned background worker with durable,
idempotent recovery.

Keep queues and batches bounded. Surface overload honestly; do not hide it with
longer timeouts, unbounded retries, priority promotion, or extra SQLite writer
connections.

## Correctness Requirements

- Preserve event order and read-your-own-write behavior inside atomic batches.
  If event B depends on event A in the same transaction, prepare/apply them in
  an order where B observes A; do not precompute B from stale pre-transaction
  state.
- Make migrations and reconciliation idempotent across retry and restart.
  Migration completion markers must reflect the actual durable outcome.
- Keep reader connections physically read-only. Do not weaken `query_only`,
  silently route writes through readers, or use a read transaction as a write
  boundary.
- Keep database observability low-cardinality and free of SQL text, payloads,
  identifiers, paths, credentials, and raw errors.

## Change and Test Discipline

Before changing a transaction boundary or moving preparation outside it,
document which state the preparation reads and prove that it cannot become
stale before commit. If it can, revalidate inside the transaction.

Database changes require focused regression coverage for:

- the intended scheduling class and physical reader/writer route;
- atomicity, event ordering, and rollback;
- cancellation without leaked queue entries or permits;
- concurrent interactive and maintenance work;
- retry/restart idempotency and poison-row progress for background jobs.

Run the narrow affected tests first. Before declaring a repository-wide
database change complete, run formatting checks and the relevant workspace
tests. Never weaken a production invariant merely to make an invalid fixture
pass; repair the fixture instead.
