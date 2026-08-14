use anyhow::{Context, Result, anyhow, bail};
use pioneer_protocol::{
    AuthorizationChangeKind, AuthorizationChangeScope, AuthorizationProjectionChangedNotification,
    PolicyGeneration,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, TransactionTrait};

const STATE_ROW: &str = "SELECT generation, code_policy_fingerprint FROM authorization_policy_state WHERE singleton_id = 1";

pub async fn current_policy_generation(db: &DatabaseConnection) -> Result<PolicyGeneration> {
    current_policy_generation_on(db).await
}

pub async fn current_policy_generation_on<C: ConnectionTrait>(db: &C) -> Result<PolicyGeneration> {
    let row = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            STATE_ROW.to_owned(),
        ))
        .await
        .context("failed to load durable authorization policy generation")?
        .context("authorization policy state singleton is missing")?;
    generation_from_i64(row.try_get("", "generation")?)
}

pub async fn ensure_code_policy_generation(
    db: &DatabaseConnection,
    fingerprint: &str,
) -> Result<AuthorizationProjectionChangedNotification> {
    if fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("authorization policy fingerprint must be a SHA-256 hex digest");
    }
    let transaction = db
        .begin()
        .await
        .context("failed to begin code-policy generation transaction")?;
    // The update obtains SQLite's write lock before reading the old
    // fingerprint. Concurrent Gateway startup/current-generation calls can
    // therefore never lose or duplicate an actual code-policy transition.
    let row = transaction
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE authorization_policy_state
             SET generation = CASE
                     WHEN code_policy_fingerprint = '' OR code_policy_fingerprint = ?
                         THEN generation
                     ELSE generation + 1
                 END,
                 code_policy_fingerprint = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE singleton_id = 1 AND generation < 9223372036854775807
             RETURNING generation",
            [fingerprint.to_owned().into(), fingerprint.to_owned().into()],
        ))
        .await
        .context("failed to advance changed code policy")?
        .context("authorization policy state is missing or exhausted")?;
    let generation = generation_from_i64(row.try_get("", "generation")?)?;
    let notification = AuthorizationProjectionChangedNotification {
        policy_generation: generation,
        change: AuthorizationChangeKind::CodePolicy,
        affected: AuthorizationChangeScope::Global,
    };
    insert_change_if_absent(&transaction, &notification).await?;
    transaction
        .commit()
        .await
        .context("failed to commit code-policy generation")?;
    Ok(notification)
}

pub async fn append_authorization_change(
    db: &DatabaseConnection,
    change: AuthorizationChangeKind,
    affected: AuthorizationChangeScope,
) -> Result<AuthorizationProjectionChangedNotification> {
    let transaction = db
        .begin()
        .await
        .context("failed to begin authorization change transaction")?;
    let row = transaction
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "UPDATE authorization_policy_state
             SET generation = generation + 1, updated_at = CURRENT_TIMESTAMP
             WHERE singleton_id = 1 AND generation < 9223372036854775807
             RETURNING generation"
                .to_owned(),
        ))
        .await
        .context("failed to advance authorization generation")?;
    let row = row.context("authorization policy state is missing or exhausted")?;
    let generation = generation_from_i64(row.try_get("", "generation")?)?;
    let notification = AuthorizationProjectionChangedNotification {
        policy_generation: generation,
        change,
        affected,
    };
    insert_change_if_absent(&transaction, &notification).await?;
    transaction
        .commit()
        .await
        .context("failed to commit authorization change")?;
    Ok(notification)
}

/// Reads the durable, ordered change feed for missed-event reconciliation.
/// The feed carries only typed, payload-safe invalidation scopes.
pub async fn list_authorization_changes_after(
    db: &DatabaseConnection,
    after: Option<PolicyGeneration>,
    limit: usize,
) -> Result<Vec<AuthorizationProjectionChangedNotification>> {
    if !(1..=1024).contains(&limit) {
        bail!("authorization change feed limit must be between 1 and 1024");
    }
    let after = after.map_or(0, generation_i64);
    let limit = i64::try_from(limit).expect("bounded authorization feed limit fits i64");
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT generation, change_kind, affected_scope_json
             FROM authorization_change_feed
             WHERE generation > ?
             ORDER BY generation ASC
             LIMIT ?",
            [after.into(), limit.into()],
        ))
        .await
        .context("failed to list durable authorization changes")?;
    rows.into_iter()
        .map(|row| {
            let generation = generation_from_i64(row.try_get("", "generation")?)?;
            let change = parse_change_kind(row.try_get::<String>("", "change_kind")?.as_str())?;
            let scope_json: String = row.try_get("", "affected_scope_json")?;
            let affected = serde_json::from_str(&scope_json)
                .context("invalid durable authorization change scope")?;
            Ok(AuthorizationProjectionChangedNotification {
                policy_generation: generation,
                change,
                affected,
            })
        })
        .collect()
}

async fn insert_change_if_absent<C: ConnectionTrait>(
    db: &C,
    notification: &AuthorizationProjectionChangedNotification,
) -> Result<()> {
    let scope_json = serde_json::to_string(&notification.affected)
        .context("failed to serialize authorization change scope")?;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT OR IGNORE INTO authorization_change_feed(generation, change_kind, affected_scope_json, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
        [
            generation_i64(notification.policy_generation).into(),
            change_kind_name(notification.change).into(),
            scope_json.into(),
        ],
    ))
    .await
    .context("failed to append durable authorization change")?;
    Ok(())
}

fn generation_from_i64(value: i64) -> Result<PolicyGeneration> {
    let value = u64::try_from(value).context("authorization generation is negative")?;
    PolicyGeneration::new(value).ok_or_else(|| anyhow!("authorization generation is zero"))
}

fn generation_i64(generation: PolicyGeneration) -> i64 {
    i64::try_from(generation.get()).expect("persisted policy generation exceeds SQLite INTEGER")
}

fn parse_change_kind(value: &str) -> Result<AuthorizationChangeKind> {
    match value {
        "code_policy" => Ok(AuthorizationChangeKind::CodePolicy),
        "role_policy" => Ok(AuthorizationChangeKind::RolePolicy),
        "role_assignment" => Ok(AuthorizationChangeKind::RoleAssignment),
        "workspace_acl" => Ok(AuthorizationChangeKind::WorkspaceAcl),
        "thread_acl" => Ok(AuthorizationChangeKind::ThreadAcl),
        "resource_selector" => Ok(AuthorizationChangeKind::ResourceSelector),
        _ => bail!("unknown durable authorization change kind `{value}`"),
    }
}

const fn change_kind_name(kind: AuthorizationChangeKind) -> &'static str {
    match kind {
        AuthorizationChangeKind::CodePolicy => "code_policy",
        AuthorizationChangeKind::RolePolicy => "role_policy",
        AuthorizationChangeKind::RoleAssignment => "role_assignment",
        AuthorizationChangeKind::WorkspaceAcl => "workspace_acl",
        AuthorizationChangeKind::ThreadAcl => "thread_acl",
        AuthorizationChangeKind::ResourceSelector => "resource_selector",
    }
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use sea_orm::Database;

    use super::*;

    #[tokio::test]
    async fn generation_survives_reconnect_and_code_policy_change() {
        let path = std::env::temp_dir().join(format!(
            "pioneer-policy-generation-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let database = Database::connect(url.as_str()).await.unwrap();
        Migrator::up(&database, None).await.unwrap();

        let first = ensure_code_policy_generation(&database, &"a".repeat(64))
            .await
            .unwrap();
        assert_eq!(first.policy_generation, PolicyGeneration::INITIAL);
        let changed = append_authorization_change(
            &database,
            AuthorizationChangeKind::WorkspaceAcl,
            AuthorizationChangeScope::Workspace {
                workspace_id: "workspace-red".to_owned(),
            },
        )
        .await
        .unwrap();
        assert_eq!(changed.policy_generation.get(), 2);
        let changes =
            list_authorization_changes_after(&database, Some(PolicyGeneration::INITIAL), 10)
                .await
                .unwrap();
        assert_eq!(changes, vec![changed.clone()]);
        database.close().await.unwrap();

        let restarted = Database::connect(url.as_str()).await.unwrap();
        assert_eq!(
            current_policy_generation(&restarted).await.unwrap().get(),
            2
        );
        let redeployed = ensure_code_policy_generation(&restarted, &"b".repeat(64))
            .await
            .unwrap();
        assert_eq!(redeployed.policy_generation.get(), 3);
        assert_eq!(
            list_authorization_changes_after(&restarted, None, 10)
                .await
                .unwrap(),
            vec![first, changed, redeployed]
        );
        restarted.close().await.unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn concurrent_committed_changes_receive_unique_monotonic_generations() {
        let path = std::env::temp_dir().join(format!(
            "pioneer-policy-generation-concurrent-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let database = Database::connect(url.as_str()).await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        ensure_code_policy_generation(&database, &"c".repeat(64))
            .await
            .unwrap();

        let append = |workspace_id: &'static str| {
            append_authorization_change(
                &database,
                AuthorizationChangeKind::WorkspaceAcl,
                AuthorizationChangeScope::Workspace {
                    workspace_id: workspace_id.to_owned(),
                },
            )
        };
        let (first, second, third, fourth) = tokio::join!(
            append("workspace-a"),
            append("workspace-b"),
            append("workspace-c"),
            append("workspace-d"),
        );
        let mut generations = [first, second, third, fourth]
            .into_iter()
            .map(|result| result.unwrap().policy_generation.get())
            .collect::<Vec<_>>();
        generations.sort_unstable();
        assert_eq!(generations, vec![2, 3, 4, 5]);
        assert_eq!(current_policy_generation(&database).await.unwrap().get(), 5);

        database.close().await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
