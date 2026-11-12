//! Payload operations for the existing agent-memory backend.
use crate::{CrudStore, convention::memory_scope_kind_to_db, repositories::agent_memory};
use anyhow::Result;
use pioneer_protocol::MemoryScope;
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn put_memory_payload(
        &self,
        id: &str,
        scope: MemoryScope,
        content: &str,
    ) -> Result<i64> {
        let resolved = self.resolve_memory_scope(scope).await?;
        self.run_serialized_write(|| {
            let resolved = resolved.clone();
            async move {
                let tx = self.connection.begin().await?;
                let revision = agent_memory::put_payload(
                    &tx,
                    id,
                    memory_scope_kind_to_db(resolved.scope.kind),
                    &resolved.scope_key_hash,
                    content,
                )
                .await?;
                tx.commit().await?;
                Ok(revision)
            }
        })
        .await
    }

    pub async fn get_memory_payload(&self, id: &str, scope: MemoryScope) -> Result<Option<String>> {
        Ok(self
            .get_memory_payload_versioned(id, scope)
            .await?
            .map(|(content, _)| content))
    }

    pub async fn get_memory_payload_versioned(
        &self,
        id: &str,
        scope: MemoryScope,
    ) -> Result<Option<(String, i64)>> {
        let key = crate::memory::normalized_scope_key(&scope.key)?;
        let hash = crate::memory_scope_key_hash(scope.kind, &key)?;
        Ok(agent_memory::get_payload(
            &self.connection,
            id,
            memory_scope_kind_to_db(scope.kind),
            &hash,
        )
        .await?
        .map(|row| (row.content, row.revision)))
    }

    pub async fn delete_memory_payload(
        &self,
        id: &str,
        scope: MemoryScope,
        repair_scope_hash: Option<&str>,
    ) -> Result<bool> {
        // A tombstoned/deleted source owner must not prevent physical cleanup.
        // Durable orphan-repair jobs carry a scope hash but no original key.
        let hash = if scope.key.trim().is_empty() {
            repair_scope_hash
                .ok_or_else(|| anyhow::anyhow!("payload cleanup scope missing"))?
                .to_owned()
        } else {
            let key = crate::memory::normalized_scope_key(&scope.key)?;
            crate::memory_scope_key_hash(scope.kind, &key)?
        };
        anyhow::ensure!(!hash.trim().is_empty(), "payload cleanup scope hash empty");
        let repair = scope.key.trim().is_empty();
        self.run_serialized_write(|| async {
            use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
            let tx = self.connection.begin().await?;
            let kind = memory_scope_kind_to_db(scope.kind);
            if let Some(payload) = pioneer_entity::agent_memory_payloads::Entity::find_by_id(id)
                .one(&tx)
                .await?
            {
                if payload.scope_kind != kind || payload.scope_key_hash != hash {
                    anyhow::ensure!(!repair, "payload cleanup ownership mismatch");
                    tx.commit().await?;
                    return Ok(false);
                }
            }
            if let Some(owner) = agent_memory::find_memory_by_id(&tx, id, true).await? {
                if owner.scope_kind != kind || owner.scope_key_hash != hash {
                    anyhow::ensure!(!repair, "payload owner scope changed");
                    tx.commit().await?;
                    return Ok(false);
                }
                let quarantined = pioneer_entity::agent_memory_quarantine::Entity::find()
                    .filter(pioneer_entity::agent_memory_quarantine::Column::MemoryId.eq(id))
                    .filter(pioneer_entity::agent_memory_quarantine::Column::ResolvedAt.is_null())
                    .one(&tx)
                    .await?
                    .is_some();
                // Re-read under the same serialized transaction as DELETE.
                if repair || (owner.status == "active" && !quarantined) {
                    tx.commit().await?;
                    return Ok(false);
                }
            }
            let deleted = agent_memory::delete_payload(&tx, id, kind, &hash).await?;
            tx.commit().await?;
            Ok(deleted)
        })
        .await
    }

    pub async fn search_memory_payloads(
        &self,
        query: &str,
        filter: crate::AgentMemoryListFilter,
        sensitivities: &[pioneer_protocol::MemorySensitivity],
        now_unix: i64,
        limit: u32,
    ) -> Result<Vec<(String, String)>> {
        let resolved = self.resolve_memory_scopes(filter.scopes.clone()).await?;
        agent_memory::search_payloads(
            &self.connection,
            query,
            filter,
            resolved,
            sensitivities,
            now_unix,
            limit,
        )
        .await
    }
}

pub const MEMORY_SQLITE_RESET_KEY: &str = "memory_sqlite_payload_reset_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPayloadResetOutcome {
    Progress,
    FilesPending,
    Complete,
}

impl CrudStore {
    /// One bounded quantum of the existing startup migration lifecycle. Inputs
    /// and checkpoint JSON are prepared outside writer capacity. Commit rechecks
    /// both checkpoint and absence of a new payload before retiring legacy rows.
    pub async fn reset_legacy_memory_payload_quantum(
        &self,
        now_unix: i64,
    ) -> Result<MemoryPayloadResetOutcome> {
        use anyhow::{Context, ensure};
        use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
        let marker = crate::find_projection_meta(&self.connection, MEMORY_SQLITE_RESET_KEY)
            .await?
            .context("SQLite memory reset marker missing")?;
        if marker.status == crate::PROJECTION_META_STATUS_COMPLETE {
            return Ok(MemoryPayloadResetOutcome::Complete);
        }
        if marker.status == crate::PROJECTION_META_STATUS_BACKFILLING {
            return Ok(MemoryPayloadResetOutcome::FilesPending);
        }
        ensure!(
            marker.status == crate::PROJECTION_META_STATUS_PENDING,
            "invalid SQLite memory reset state"
        );
        let after: Option<String> = marker
            .projection_config_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .flatten();
        let ids = agent_memory::legacy_payloadless_ids(
            &self.connection,
            marker.created_at,
            after.as_deref(),
        )
        .await?;
        let legacy_jobs =
            crate::repositories::agent_memory_repair_job::legacy_storage_job_ids(&self.connection)
                .await?;
        let eof = ids.is_empty() && legacy_jobs.is_empty();
        let next_json = serde_json::to_string(&ids.last().cloned().or(after))?;
        self.run_serialized_write(|| {
            let marker = marker.clone();
            let ids = ids.clone();
            let legacy_jobs = legacy_jobs.clone();
            let next_json = next_json.clone();
            async move {
                let tx = self.connection.begin().await?;
                let current = crate::find_projection_meta(&tx, MEMORY_SQLITE_RESET_KEY)
                    .await?
                    .context("SQLite memory reset marker disappeared")?;
                ensure!(
                    current.status == marker.status
                        && current.projection_config_json == marker.projection_config_json,
                    "SQLite memory reset checkpoint changed"
                );
                for id in &ids {
                    agent_memory::retire_legacy_payloadless_record(
                        &tx,
                        id,
                        marker.created_at,
                        crate::util::unix_to_datetime(now_unix),
                    )
                    .await?;
                }
                for id in &legacy_jobs {
                    crate::repositories::agent_memory_repair_job::retire_legacy_storage_job(
                        &tx,
                        id,
                        crate::util::unix_to_datetime(now_unix),
                    )
                    .await?;
                }
                let mut current = current.into_active_model();
                current.projection_config_json = Set(Some(next_json));
                current.updated_at = Set(crate::util::unix_to_datetime(now_unix));
                if eof {
                    current.status = Set(crate::PROJECTION_META_STATUS_BACKFILLING.to_owned());
                    current.backfilled_at = Set(Some(crate::util::unix_to_datetime(now_unix)));
                }
                current.update(&tx).await?;
                tx.commit().await?;
                Ok(if eof {
                    MemoryPayloadResetOutcome::FilesPending
                } else {
                    MemoryPayloadResetOutcome::Progress
                })
            }
        })
        .await
    }
}

impl CrudStore {
    pub async fn complete_legacy_memory_reset(&self, now_unix: i64) -> Result<()> {
        use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let marker = crate::find_projection_meta(&tx, MEMORY_SQLITE_RESET_KEY)
                .await?
                .ok_or_else(|| anyhow::anyhow!("SQLite memory reset marker missing"))?;
            anyhow::ensure!(
                marker.status == crate::PROJECTION_META_STATUS_BACKFILLING
                    || marker.status == crate::PROJECTION_META_STATUS_COMPLETE,
                "memory reset database phase incomplete"
            );
            let mut marker = marker.into_active_model();
            marker.projection_version = Set(2);
            marker.status = Set(crate::PROJECTION_META_STATUS_COMPLETE.to_owned());
            marker.backfilled_at = Set(Some(crate::util::unix_to_datetime(now_unix)));
            marker.updated_at = Set(crate::util::unix_to_datetime(now_unix));
            marker.update(&tx).await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}

impl CrudStore {
    /// The full payload is read outside capacity by the existing backend. This
    /// short transaction revalidates owner, lifecycle, quarantine and (for a
    /// durable backend) payload revision before clearing repair-needed state.
    pub async fn confirm_agent_memory_payload_repair(
        &self,
        expected: &crate::AgentMemoryControlRecord,
        revision: Option<i64>,
        now_unix: i64,
    ) -> Result<()> {
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
        let source = agent_memory::find_memory_by_id(&self.connection, &expected.id, true)
            .await?
            .ok_or_else(|| anyhow::anyhow!("repair owner missing"))?;
        let details = serde_json::json!({"repair_status":"ok"}).to_string();
        let event = crate::prepare_memory_event_for_stored_memory(
            pioneer_protocol::generate_id(21),
            &source,
            crate::MEMORY_EVENT_REPAIR_STATUS_CHANGED,
            None,
            Some(details),
            now_unix,
        );
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let current = agent_memory::find_memory_by_id(&tx, &expected.id, true)
                .await?
                .ok_or_else(|| anyhow::anyhow!("repair owner disappeared"))?;
            anyhow::ensure!(
                current.status == "active"
                    && current.scope_kind == memory_scope_kind_to_db(expected.scope.kind)
                    && current.scope_key_hash == expected.scope_key_hash
                    && current.workspace_id == expected.workspace_id
                    && current.source_thread_id == expected.source_thread_id
                    && current.source_turn_id == expected.source_turn_id
                    && current.source_item_id == expected.source_item_id
                    && current.payload_version == expected.payload_version
                    && current.updated_at.timestamp() == expected.updated_at_unix
                    && current.repair_status == expected.repair_status
                    && crate::stored_memory_event_identity_matches(&source, &current),
                "repair owner changed during payload verification"
            );
            anyhow::ensure!(
                pioneer_entity::agent_memory_quarantine::Entity::find()
                    .filter(
                        pioneer_entity::agent_memory_quarantine::Column::MemoryId.eq(&expected.id)
                    )
                    .filter(pioneer_entity::agent_memory_quarantine::Column::ResolvedAt.is_null())
                    .one(&tx)
                    .await?
                    .is_none(),
                "repair owner quarantined"
            );
            if let Some(revision) = revision {
                let payload = agent_memory::get_payload(
                    &tx,
                    &expected.id,
                    memory_scope_kind_to_db(expected.scope.kind),
                    &expected.scope_key_hash,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("full repair payload disappeared"))?;
                anyhow::ensure!(
                    payload.revision == revision && revision == current.payload_version,
                    "full repair payload revision changed"
                );
            }
            if current.repair_status != "ok" {
                agent_memory::mark_memory_repair_status(
                    &tx,
                    &expected.id,
                    "ok",
                    crate::util::unix_to_datetime(now_unix),
                )
                .await?;
                crate::repositories::agent_memory_event::append_prepared_memory_event(
                    &tx,
                    event.clone(),
                )
                .await?;
            }
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}

impl CrudStore {
    /// Version 1 only removed .mv2 files. Version 2 proves removal of the entire
    /// dedicated legacy tree; retain the existing checkpoint, not another queue.
    pub async fn legacy_memory_tree_cleanup_complete(&self) -> Result<bool> {
        let marker = crate::find_projection_meta(&self.connection, MEMORY_SQLITE_RESET_KEY)
            .await?
            .ok_or_else(|| anyhow::anyhow!("SQLite memory reset marker missing"))?;
        Ok(marker.status == crate::PROJECTION_META_STATUS_COMPLETE
            && marker.projection_version >= 2)
    }
}
