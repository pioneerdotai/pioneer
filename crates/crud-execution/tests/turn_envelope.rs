use anyhow::{Context, Result};
use pioneer_crud_execution::turn_envelope::{
    authorization_context_as_text, authorization_context_value, pin_native_context,
};
use pioneer_entity::turn;
use pioneer_postgres_test::with_database;
use pioneer_protocol::{NativeContextBudget, NativeServiceModePlan, ServiceModeParameters};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};
use serde_json::Value;

#[tokio::test]
async fn native_context_cas_keeps_existing_cap_and_authorization_json_keeps_its_bytes() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'agent', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status, execution_security_snapshot_json)
                VALUES ('turn', 'thread', 'in_progress',
                    '{ "backend": { "execution_backend": "native" }, "unknown": "\u0000" }');
            "#,
        )
        .await?;
        let authorization = r#"{ "z": 1, "a": "\u0000" }"#;
        turn::Entity::update_many()
            .filter(turn::Column::Id.eq("turn"))
            .col_expr(
                turn::Column::ExecutionAuthorizationContextJson,
                authorization_context_value(authorization)?,
            )
            .exec(db)
            .await?;
        let saved = turn::Entity::find_by_id("turn")
            .select_only()
            .column_as(authorization_context_as_text(), "authorization")
            .into_tuple::<String>()
            .one(db)
            .await?
            .context("missing authorization envelope")?;
        assert_eq!(saved.as_bytes(), authorization.as_bytes());

        let budget = NativeContextBudget {
            service_mode: None,
            instance: "provider".into(),
            authority: Some("account".into()),
            model: "model".into(),
            manual_window: None,
            context_window: 272_000,
            max_input: None,
            max_output: Some(16_000),
        };
        assert!(!pin_native_context(db, "foreign", "turn", &budget, None).await?);
        let other = fixture.connect().await?;
        let (first, second) = tokio::try_join!(
            pin_native_context(db, "w", "turn", &budget, None),
            pin_native_context(&other, "w", "turn", &budget, None),
        )?;
        assert_ne!(first, second);
        let mut plan = budget.clone();
        plan.context_window = 1_050_000;
        plan.service_mode = Some(NativeServiceModePlan {
            id: "standard".into(),
            parameters: ServiceModeParameters::OpenAiStandard,
        });
        assert!(pin_native_context(db, "w", "turn", &plan, None).await?);
        assert!(!pin_native_context(db, "w", "turn", &plan, None).await?);
        let snapshot = turn::Entity::find_by_id("turn")
            .one(db)
            .await?
            .context("missing turn")?
            .execution_security_snapshot_json
            .context("missing security envelope")?;
        let value: Value = serde_json::from_str(&snapshot)?;
        assert_eq!(value["native_context"]["context_window"], 272_000);
        assert_eq!(value["native_context"]["service_mode"]["id"], "standard");
        assert_eq!(value["unknown"], "\0");
        other.close().await?;
        Ok(())
    })
    .await
}
