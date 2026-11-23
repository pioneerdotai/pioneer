use anyhow::Result;
use pioneer_entity::agent_delegation_route as route;
use pioneer_postgres_test::with_database;
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};

#[tokio::test]
async fn fresh_install_accepts_capsule_kind_and_rejects_unknown_kind() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('workspace','Routes');
             INSERT INTO agent_identity(id,workspace_id,source_kind,source_id,source_fingerprint)
                VALUES ('Identity0000000000000','workspace','ephemeral','identity','fingerprint');
             INSERT INTO agent_execution(
                id,workspace_id,agent_identity_id,identity_source_revision,identity_source_fingerprint,
                home_root_thread_id,work_graph_root_execution_id,requested_identity_selection_json,
                requested_profile_selection_json,authorization_context_fingerprint
             ) VALUES (
                'Execution000000000000','workspace','Identity0000000000000',1,'fingerprint',
                'thread','Execution000000000000','{}','{}','authorization'
             );",
        )
        .await?;
        route::ActiveModel {
            id: Set("capsule-route".into()),
            source_execution_id: Set("Execution000000000000".into()),
            destination_thread_id: Set("thread".into()),
            route_kind: Set("capsule_bound".into()),
            grant_fingerprint: Set("route fingerprint".into()),
            ..Default::default()
        }
        .insert(db)
        .await?;
        let error = route::Entity::update_many()
            .col_expr(route::Column::RouteKind, Expr::value("unknown"))
            .exec(db)
            .await
            .expect_err("the installed schema must reject unrecognized routes");
        assert!(error.to_string().contains("check constraint"));
        assert_eq!(
            route::Entity::find_by_id("capsule-route")
                .one(db)
                .await?
                .expect("the failed update keeps the capsule route")
                .route_kind,
            "capsule_bound"
        );
        Ok(())
    })
    .await
}
