# Gateway Database Lifecycle

- Production uses the PostgreSQL runtime shipped with Gateway. Keep the cluster
  under the private runtime home, outside the installation. Start the bundled
  postmaster directly, and close application connections before waiting for it
  to exit.
- Development can explicitly select a private application database through
  `gateway.database.development_url`. Keep the URL out of logs. The test
  administrative URL belongs to the test runner.
- New clusters start from the PostgreSQL schema. Do not introduce SQLite
  compatibility reads or historical data migrations. Keep ongoing maintenance
  in the existing domain workers under their scoped handles.
- Schema changes belong in the [migration crate](../../../migration/AGENTS.md).
  Delegate ordinary database operations to [CRUD repositories](../../../crud/AGENTS.md).
- Keep work needed for current PostgreSQL data bounded, restart-safe and
  cancellation-aware. Publish signals after the atomic transition and after
  releasing database capacity. Follow the [root rules](../../../../AGENTS.md).
