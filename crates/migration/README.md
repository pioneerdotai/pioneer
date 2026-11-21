# Pioneer 1.0 database migrations

The Migrator installs a fresh Pioneer schema on PostgreSQL. The installed Gateway
starts its bundled PostgreSQL 18 and pgvector and applies these migrations at
startup. Use PostgreSQL 18 with pgvector for development and tests. The migration
role must be able to create objects in the target schema and install `vector`, or
the extension must already be visible in the target search path. The default
schema is `public`.

Pass the target database URL explicitly.

```sh
cargo run --locked -p pioneer-migration -- --database-url "$PIONEER_POSTGRES_URL" up
cargo run --locked -p pioneer-migration -- --database-url "$PIONEER_POSTGRES_URL" status
```

SeaORM applies PostgreSQL migrations and their ledger entries in the same
transaction. On a disposable database, `down -n 1` rolls back the last migration
and `reset` rolls back the whole chain. The shared `vector` extension survives
rollback. Use the 0.x release history to inspect the SQLite migration chain.
