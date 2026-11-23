use anyhow::{Context, Result};
use pioneer_crud_memory::identity::{self, IdentityKey};
use pioneer_crud_memory::payloads::{get_payload, put_payload, search};
use pioneer_entity::agent_memory;
use pioneer_postgres_test::with_database;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, QueryTrait,
    TransactionTrait,
};

async fn seed(db: &impl ConnectionTrait) -> Result<()> {
    db.execute_unprepared(
        r#"
        INSERT INTO workspace (id, name) VALUES ('w','Scoped'), ('other','Other');
        INSERT INTO agent_memory
            (id, workspace_id, scope_kind, scope_key, scope_key_hash, category, status)
            VALUES ('a','w','workspace','w','scope-hash','fact','active'),
                   ('b','w','workspace','w','scope-hash','fact','active'),
                   ('unbound','w','workspace','w','scope-hash','fact','active'),
                   ('foreign','other','workspace','other','other-hash','fact','active');
    "#,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn memory_search_combines_literal_terms_and_checks_payload_scope_before_ranking() -> Result<()>
{
    with_database(async |database| {
        let db = database.connection();
        seed(db).await?;
        for (id, hash, body) in [
            ("a", "scope-hash", "orchid orchid orchid sprouts"),
            ("b", "scope-hash", "a river nearby"),
            (
                "unbound",
                "other-hash",
                "orchid orchid orchid orchid orchid",
            ),
            (
                "foreign",
                "other-hash",
                "orchid orchid orchid orchid orchid",
            ),
        ] {
            assert_eq!(put_payload(db, id, "workspace", hash, body).await?, 1);
        }
        let eligible = agent_memory::Entity::find()
            .select_only()
            .column(agent_memory::Column::Id)
            .filter(agent_memory::Column::WorkspaceId.eq("w"))
            .into_query();
        let hits = search(db, "orchid river", eligible.clone(), 4).await?;
        assert_eq!(
            hits.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert!(hits[0].1.contains("orchid"));
        assert!(hits[1].1.contains("river"));
        assert_eq!(
            search(db, "orchid river", eligible.clone(), 1).await?.len(),
            1
        );
        assert!(search(db, "---", eligible, 4).await?.is_empty());

        assert!(
            put_payload(db, "b", "workspace", "other-hash", "replacement")
                .await
                .is_err()
        );
        let actual = get_payload(db, "b", "workspace", "scope-hash")
            .await?
            .context("memory payload missing")?;
        assert_eq!(actual.content, "a river nearby");
        assert_eq!(
            put_payload(db, "b", "workspace", "scope-hash", "exact text").await?,
            2
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn identity_rejects_an_active_rebind_and_publishes_replacement_after_commit() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        seed(db).await?;
        let key = IdentityKey {
            scope_kind: "workspace",
            scope_hash: "scope-hash",
            namespace: "main",
            key: "same",
        };
        let writer = database.connect().await?;
        let tx = writer.begin().await?;
        identity::bind(&tx, &key, "a").await?;
        assert_eq!(identity::lookup(&tx, &key).await?.as_deref(), Some("a"));
        assert_eq!(identity::lookup(db, &key).await?, None);
        tx.commit().await?;
        assert_eq!(identity::lookup(db, &key).await?.as_deref(), Some("a"));

        let tx = writer.begin().await?;
        assert!(identity::bind(&tx, &key, "b").await.is_err());
        tx.rollback().await?;
        assert_eq!(identity::lookup(db, &key).await?.as_deref(), Some("a"));

        let tx = writer.begin().await?;
        tx.execute_unprepared("UPDATE agent_memory SET status='superseded' WHERE id='a'")
            .await?;
        identity::bind(&tx, &key, "b").await?;
        assert_eq!(identity::lookup(&tx, &key).await?.as_deref(), Some("b"));
        assert_eq!(identity::lookup(db, &key).await?.as_deref(), Some("a"));
        tx.commit().await?;
        assert_eq!(identity::lookup(db, &key).await?.as_deref(), Some("b"));
        writer.close().await?;
        Ok(())
    })
    .await
}
