# Running Migrator CLI

- Set `DATABASE_URL` via environment or `.env` file before running migrations.
- You can also pass `-u/--database-url` explicitly.
- Generate a new migration file
    ```sh
    cargo run -p pioneer-migration -- generate MIGRATION_NAME
    ```
- Apply all pending migrations
    ```sh
    cargo run -p pioneer-migration -- up
    ```
- Apply first 10 pending migrations
    ```sh
    cargo run -p pioneer-migration -- up -n 10
    ```
- Rollback last applied migrations
    ```sh
    cargo run -p pioneer-migration -- down
    ```
- Rollback last 10 applied migrations
    ```sh
    cargo run -p pioneer-migration -- down -n 10
    ```
- Drop all tables from the database, then reapply all migrations
    ```sh
    cargo run -p pioneer-migration -- fresh
    ```
- Rollback all applied migrations, then reapply all migrations
    ```sh
    cargo run -p pioneer-migration -- refresh
    ```
- Rollback all applied migrations
    ```sh
    cargo run -p pioneer-migration -- reset
    ```
- Check the status of all migrations
    ```sh
    cargo run -p pioneer-migration -- status
    ```
