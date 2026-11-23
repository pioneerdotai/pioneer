use anyhow::Result;
use sea_orm::{ConnectionTrait, DbErr, IsolationLevel, RuntimeErr, SqlxError, TransactionTrait};

pub(crate) fn sqlstate(error: &DbErr) -> String {
    let (DbErr::Exec(RuntimeErr::SqlxError(cause)) | DbErr::Query(RuntimeErr::SqlxError(cause))) =
        error
    else {
        panic!("fixture must preserve the database driver's typed error");
    };
    let SqlxError::Database(database) = cause.as_ref() else {
        panic!("fixture must produce a database error");
    };
    database.code().expect("PostgreSQL SQLSTATE").into_owned()
}

pub(crate) async fn rejected_statement() -> Result<DbErr> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let db = fixture.connect_with_limit(1).await?;
    db.execute_unprepared(
        "CREATE TABLE fault_fixture(id integer PRIMARY KEY); INSERT INTO fault_fixture VALUES (1)",
    )
    .await?;
    let error = db
        .execute_unprepared("INSERT INTO fault_fixture VALUES (1)")
        .await
        .unwrap_err();
    db.close().await?;
    Ok(error)
}

pub(crate) async fn lock_unavailable(table_lock: bool) -> Result<DbErr> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let owner = fixture.connect_with_limit(1).await?;
    let peer = fixture.connect_with_limit(1).await?;
    owner.execute_unprepared("CREATE TABLE fault_fixture(id integer PRIMARY KEY); INSERT INTO fault_fixture VALUES (1)").await?;
    let held = owner.begin().await?;
    let contender = peer.begin().await?;
    let (lock, conflict) = if table_lock {
        (
            "LOCK TABLE fault_fixture IN ACCESS EXCLUSIVE MODE",
            "LOCK TABLE fault_fixture IN ACCESS SHARE MODE NOWAIT",
        )
    } else {
        (
            "SELECT id FROM fault_fixture WHERE id=1 FOR UPDATE",
            "SELECT id FROM fault_fixture WHERE id=1 FOR UPDATE NOWAIT",
        )
    };
    held.execute_unprepared(lock).await?;
    let error = contender.execute_unprepared(conflict).await.unwrap_err();
    contender.rollback().await?;
    held.rollback().await?;
    peer.close().await?;
    owner.close().await?;
    Ok(error)
}

pub(crate) async fn rejected_snapshot() -> Result<DbErr> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let owner = fixture.connect_with_limit(1).await?;
    let peer = fixture.connect_with_limit(1).await?;
    owner.execute_unprepared("CREATE TABLE fault_fixture(id integer PRIMARY KEY, value integer); INSERT INTO fault_fixture VALUES (1, 0)").await?;
    let stale = owner
        .begin_with_config(Some(IsolationLevel::RepeatableRead), None)
        .await?;
    stale
        .execute_unprepared("SELECT value FROM fault_fixture WHERE id=1")
        .await?;
    peer.execute_unprepared("UPDATE fault_fixture SET value=1 WHERE id=1")
        .await?;
    let error = stale
        .execute_unprepared("UPDATE fault_fixture SET value=2 WHERE id=1")
        .await
        .unwrap_err();
    stale.rollback().await?;
    peer.close().await?;
    owner.close().await?;
    Ok(error)
}
