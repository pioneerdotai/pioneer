//! Bounded auxiliary-call usage journal. No provider payload or replay history.
use crate::CrudStore;
use anyhow::{Result, ensure};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderUsageObservation {
    pub id: String,
    pub workspace_id: String,
    pub operation_kind: String,
    pub owner_id: String,
    pub status: String,
    pub usage_json: String,
    pub started_at: i64,
    pub updated_at: i64,
}

impl ProviderUsageObservation {
    fn validate(&self) -> Result<()> {
        for value in [&self.id, &self.workspace_id, &self.owner_id] {
            ensure!(
                !value.is_empty() && value.len() <= 256,
                "invalid usage journal identity"
            );
        }
        ensure!(
            matches!(
                self.operation_kind.as_str(),
                "title" | "summary" | "self_improvement" | "memory_extraction"
            ),
            "invalid usage operation"
        );
        ensure!(
            matches!(self.status.as_str(), "started" | "completed" | "failed"),
            "invalid usage status"
        );
        ensure!(
            self.usage_json.len() <= 32 * 1024,
            "usage journal exceeds byte bound"
        );
        ensure!(
            serde_json::from_str::<serde_json::Value>(&self.usage_json)?.is_object(),
            "usage must be an object"
        );
        Ok(())
    }
}

impl CrudStore {
    pub async fn record_provider_usage(&self, record: ProviderUsageObservation) -> Result<()> {
        // All validation/serialization happens before the writer reservation.
        record.validate()?;
        self.run_serialized_write(|| async {
            let result = self.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO provider_usage_observation (id,workspace_id,operation_kind,owner_id,status,usage_json,started_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(id) DO UPDATE SET status=excluded.status,usage_json=excluded.usage_json,updated_at=excluded.updated_at WHERE provider_usage_observation.workspace_id=excluded.workspace_id AND provider_usage_observation.owner_id=excluded.owner_id AND provider_usage_observation.operation_kind=excluded.operation_kind AND (provider_usage_observation.status='started' OR provider_usage_observation.status=excluded.status) AND excluded.updated_at>=provider_usage_observation.updated_at",
                vec![record.id.clone().into(),record.workspace_id.clone().into(),record.operation_kind.clone().into(),record.owner_id.clone().into(),record.status.clone().into(),record.usage_json.clone().into(),record.started_at.into(),record.updated_at.into()])).await?;
            ensure!(result.rows_affected() == 1, "usage journal identity/state conflict");
            Ok(())
        }).await
    }

    /// Stable bounded pagination. Inherits the caller's reader scheduling class.
    pub async fn provider_usage_page(
        &self,
        workspace: &str,
        owner: &str,
        after: &str,
        limit: u64,
    ) -> Result<Vec<ProviderUsageObservation>> {
        ensure!((1..=100).contains(&limit), "usage page limit out of bounds");
        let rows = self.connection.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT id,workspace_id,operation_kind,owner_id,status,usage_json,started_at,updated_at FROM provider_usage_observation WHERE workspace_id=?1 AND owner_id=?2 AND id>?3 ORDER BY id LIMIT ?4",
            vec![workspace.into(),owner.into(),after.into(),(limit as i64).into()])).await?;
        // Reader resources have been released before mapping/JSON consumers.
        rows.into_iter()
            .map(|row| {
                Ok(ProviderUsageObservation {
                    id: row.try_get("", "id")?,
                    workspace_id: row.try_get("", "workspace_id")?,
                    operation_kind: row.try_get("", "operation_kind")?,
                    owner_id: row.try_get("", "owner_id")?,
                    status: row.try_get("", "status")?,
                    usage_json: row.try_get("", "usage_json")?,
                    started_at: row.try_get("", "started_at")?,
                    updated_at: row.try_get("", "updated_at")?,
                })
            })
            .collect()
    }
}
