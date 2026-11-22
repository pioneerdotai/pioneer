use pioneer_crud::CrudStore;
use pioneer_postgres_test::TestDatabase;
use std::sync::Arc;

pub(crate) async fn postgres_store(database: &TestDatabase) -> Arc<CrudStore> {
    let writer = database.connect().await.expect("PostgreSQL test writer");
    let reader = database
        .connect_read_only()
        .await
        .expect("PostgreSQL test reader");
    Arc::new(CrudStore::new(
        pioneer_postgres_pools::PostgresDatabase::new(reader, writer),
    ))
}
