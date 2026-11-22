# Gateway Database Lifecycle

- Gateway uses the shipped PostgreSQL runtime by default. Keep the cluster
  under the private runtime home, outside the installation. Start the bundled
  postmaster directly, and close application connections before waiting for it
  to exit.
- Gateway and database operator commands use `DATABASE_URL` from the process
  environment or the runtime home's `.env` to select an external application
  database. An explicit dev-only `gateway.database.development_url` takes
  precedence. Keep URL values out of logs and inherited agent environments.
  The test administrative URL belongs to the test runner.
- Treat external PostgreSQL as an independently managed service. Gateway owns
  the pools and applies its schema migrations. Use one active Gateway per
  database and connect to the cluster's writable endpoint.
- Gateway uses the managed `public` schema for Pioneer and pgvector. Configure
  connection search paths as `public, pg_temp`, which searches `pg_catalog`
  implicitly first. The routing policy relies on the managed functions,
  operators and types in these schemas.
- Apply the registered migrations to an empty PostgreSQL database at initial
  setup. Keep application reads and ongoing maintenance on PostgreSQL under
  their scoped handles.
- Schema changes belong in the [migration crate](../../../migration/AGENTS.md).
  Delegate ordinary database operations to [CRUD repositories](../../../crud/AGENTS.md).
- Keep work needed for current PostgreSQL data bounded, restart-safe and
  cancellation-aware. Publish signals after the atomic transition and after
  releasing database capacity. Follow the [root rules](../../../../AGENTS.md).
