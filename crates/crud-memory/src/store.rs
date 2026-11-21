use anyhow::{Context, Result, bail};
use pioneer_protocol::{
    MemoryCandidateDecision, MemoryCandidateStatus, MemoryScope, MemoryScopeKind, generate_id,
};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::{EntityTrait, TransactionTrait};
use std::{future::Future, time::Duration};

use crate::{
    agent_memory, agent_memory_candidate, agent_memory_event, agent_memory_identity,
    agent_memory_policy_decision, agent_memory_quality_decision, agent_memory_quarantine,
    agent_memory_repair_job, convention::*, memory::*, util::unix_to_datetime,
};

pub struct MemoryStore {
    pub(crate) connection: SqliteDatabase,
}

impl MemoryStore {
    pub fn new(connection: impl Into<SqliteDatabase>) -> Self {
        Self {
            connection: connection.into(),
        }
    }

    pub(crate) async fn retry_write<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            is_retryable_database_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn resolve_memory_scope(&self, scope: MemoryScope) -> Result<MemoryScopeResolution> {
        let key = crate::memory::normalized_scope_key(scope.key.as_str())?;
        let normalized_scope = MemoryScope {
            kind: scope.kind,
            key: key.clone(),
        };
        let scope_key_hash = memory_scope_key_hash(scope.kind, key.as_str())?;
        let workspace_id = match scope.kind {
            MemoryScopeKind::User => crate::memory::parse_workspace_user_scope_key(&key),
            MemoryScopeKind::Workspace => Some(key),
            MemoryScopeKind::Thread => {
                let thread = pioneer_entity::thread::Entity::find_by_id(key.clone())
                    .one(&self.connection)
                    .await
                    .with_context(|| format!("failed to resolve thread memory scope `{key}`"))?
                    .with_context(|| format!("thread memory scope `{key}` does not exist"))?;
                Some(thread.workspace_id)
            }
            MemoryScopeKind::Task => {
                let task = pioneer_entity::task::Entity::find_by_id(key.clone())
                    .one(&self.connection)
                    .await
                    .with_context(|| format!("failed to resolve task memory scope `{key}`"))?
                    .with_context(|| format!("task memory scope `{key}` does not exist"))?;
                Some(task.workspace_id)
            }
            MemoryScopeKind::Agent => {
                if let Some(workspace_id) = crate::memory::parse_workspace_agent_scope_key(&key) {
                    Some(workspace_id)
                } else if crate::memory::is_global_agent_scope_key(&key) {
                    None
                } else {
                    anyhow::bail!(
                        "agent memory scope `{key}` must be `workspace:{{workspace_id}}:agent:{{agent_id}}` or `global:agent:{{agent_id}}`"
                    );
                }
            }
        };

        Ok(MemoryScopeResolution {
            scope: normalized_scope,
            scope_key_hash,
            workspace_id,
        })
    }

    pub async fn resolve_memory_scopes(
        &self,
        scopes: impl IntoIterator<Item = MemoryScope>,
    ) -> Result<Vec<MemoryScopeResolution>> {
        let mut resolved = Vec::new();
        for scope in scopes {
            resolved.push(self.resolve_memory_scope(scope).await?);
        }
        Ok(resolved)
    }

    pub async fn insert_agent_memory_record(
        &self,
        mut record: NewAgentMemoryControlRecord,
        event: Option<NewAgentMemoryEvent>,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryControlRecord> {
        let memory_id = record
            .id
            .get_or_insert_with(|| generate_id(DB_ID_LEN))
            .clone();
        self.retry_write(|| {
            let record = record.clone();
            let event = event.clone();
            let memory_id = memory_id.clone();
            async move {
                let resolved = self.resolve_memory_scope(record.scope.clone()).await?;
                let prepared_event =
                    agent_memory_event::prepare_memory_event(memory_event_for_record(
                        event,
                        memory_id,
                        resolved.workspace_id.clone(),
                        MEMORY_EVENT_CREATED,
                        event_timestamp_secs,
                    ));
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin agent memory insert transaction")?;
                let now = unix_to_datetime(event_timestamp_secs);
                let row =
                    agent_memory::insert_memory_record(&transaction, record, resolved, now).await?;
                agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                    .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit agent memory insert transaction")?;
                crate::memory::agent_memory_control_record_from_model(row)
            }
        })
        .await
    }

    pub async fn upsert_active_agent_memory_record(
        &self,
        mut record: NewAgentMemoryControlRecord,
        event: Option<NewAgentMemoryEvent>,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryControlRecord> {
        let memory_id = record
            .id
            .get_or_insert_with(|| generate_id(DB_ID_LEN))
            .clone();
        self.retry_write(|| {
            let record = record.clone();
            let event = event.clone();
            let memory_id = memory_id.clone();
            async move {
                let resolved = self.resolve_memory_scope(record.scope.clone()).await?;
                let created_event =
                    agent_memory_event::prepare_memory_event(memory_event_for_record(
                        event.clone(),
                        memory_id.clone(),
                        resolved.workspace_id.clone(),
                        MEMORY_EVENT_CREATED,
                        event_timestamp_secs,
                    ));
                let updated_event =
                    agent_memory_event::prepare_memory_event(memory_event_for_record(
                        event,
                        memory_id,
                        resolved.workspace_id.clone(),
                        MEMORY_EVENT_UPDATED,
                        event_timestamp_secs,
                    ));
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin agent memory upsert transaction")?;
                let now = unix_to_datetime(event_timestamp_secs);
                let row =
                    agent_memory::upsert_active_memory_record(&transaction, record, resolved, now)
                        .await?;
                let prepared_event = if row.created_at == now {
                    created_event
                } else {
                    updated_event
                };
                agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                    .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit agent memory upsert transaction")?;
                crate::memory::agent_memory_control_record_from_model(row)
            }
        })
        .await
    }

    pub async fn publish_agent_memory_record(
        &self,
        record: NewAgentMemoryControlRecord,
        expected_active_id: Option<String>,
        supersedes: Option<String>,
        canonical_identity: Option<pioneer_protocol::MemoryCanonicalKey>,
        payload_revision: Option<i64>,
        publication_content: Option<String>,
        now_unix: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let memory_id = record
            .id
            .clone()
            .context("memory publication requires an id")?;
        let resolved = self.resolve_memory_scope(record.scope.clone()).await?;
        let namespace = crate::memory::normalized_memory_namespace(record.namespace.as_deref())?;
        let key = crate::memory::normalized_optional_memory_key(record.key.clone())?;
        let event_kind = if expected_active_id.is_some() && supersedes.is_none() {
            MEMORY_EVENT_UPDATED
        } else {
            MEMORY_EVENT_CREATED
        };
        let publication_event = agent_memory_event::prepare_memory_event(memory_event_for_record(
            None,
            memory_id.clone(),
            resolved.workspace_id.clone(),
            event_kind,
            now_unix,
        ));
        let superseded_event = supersedes.as_ref().map(|id| {
            agent_memory_event::prepare_memory_event(memory_event_for_record(
                None,
                id.clone(),
                resolved.workspace_id.clone(),
                MEMORY_EVENT_SUPERSEDED,
                now_unix,
            ))
        });
        self.retry_write(|| {
            let record = record.clone();
            let resolved = resolved.clone();
            let namespace = namespace.clone();
            let key = key.clone();
            let memory_id = memory_id.clone();
            let expected_active_id = expected_active_id.clone();
            let supersedes = supersedes.clone();
            let publication_event = publication_event.clone();
            let superseded_event = superseded_event.clone();
            let canonical_identity = canonical_identity.clone();
            let publication_content = publication_content.clone();
            async move {
                let tx = self
                    .connection
                    .begin_with_config(Some(sea_orm::IsolationLevel::Serializable), None)
                    .await
                    .context("begin memory publication")?;
                // Only thread/task ownership is mutable state used by scope
                // preparation. Revalidate it inside the publication boundary.
                let current_workspace = match resolved.scope.kind {
                    MemoryScopeKind::Thread => Some(
                        pioneer_entity::thread::Entity::find_by_id(resolved.scope.key.clone())
                            .one(&tx)
                            .await?
                            .context("memory thread disappeared")?
                            .workspace_id,
                    ),
                    MemoryScopeKind::Task => Some(
                        pioneer_entity::task::Entity::find_by_id(resolved.scope.key.clone())
                            .one(&tx)
                            .await?
                            .context("memory task disappeared")?
                            .workspace_id,
                    ),
                    _ => resolved.workspace_id.clone(),
                };
                if current_workspace != resolved.workspace_id {
                    bail!("memory scope ownership changed during publication");
                }
                let current = if let Some(id) = supersedes.as_deref() {
                    agent_memory::find_memory_by_id(&tx, id, false).await?
                } else if let Some(key) = key.as_deref() {
                    agent_memory::find_active_memory_by_scoped_key(&tx, &resolved, &namespace, key)
                        .await?
                } else if let Some(id) = expected_active_id.as_deref() {
                    agent_memory::find_memory_by_id(&tx, id, false).await?
                } else {
                    None
                };
                if current.as_ref().map(|row| row.id.as_str()) != expected_active_id.as_deref() {
                    tx.rollback().await?;
                    return Ok(None);
                }
                // Scope/hash/content were prepared without DB capacity. Owner
                // CAS and the exact previous payload revision are revalidated
                // here; payload + FTS triggers + control publish share one commit.
                if let Some(revision) = payload_revision {
                    let payload = agent_memory::get_payload(
                        &tx,
                        &memory_id,
                        crate::convention::memory_scope_kind_to_db(resolved.scope.kind),
                        &resolved.scope_key_hash,
                    )
                    .await?;
                    if publication_content.is_some() {
                        anyhow::ensure!(
                            payload.as_ref().map_or(0, |p| p.revision) == revision,
                            "prepared memory payload revision changed"
                        );
                    } else {
                        anyhow::ensure!(
                            payload.as_ref().is_some_and(|p| p.revision == revision),
                            "prepared memory payload disappeared before publication"
                        );
                    }
                }
                let now = unix_to_datetime(now_unix);
                if let Some(old) = current.as_ref() {
                    if old.scope_kind
                        != crate::convention::memory_scope_kind_to_db(resolved.scope.kind)
                        || old.scope_key_hash != resolved.scope_key_hash
                        || old.workspace_id != resolved.workspace_id
                    {
                        bail!("memory replacement must retain its owning scope");
                    }
                }
                if let Some(id) = supersedes.as_deref() {
                    if expected_active_id.as_deref() != Some(id) {
                        bail!("supersession must replace the expected active record");
                    }
                    agent_memory::mark_memory_superseded(&tx, id, &memory_id, now)
                        .await?
                        .context("superseded memory disappeared")?;
                    agent_memory_event::append_prepared_memory_event(
                        &tx,
                        superseded_event.expect("supersession event"),
                    )
                    .await?;
                }
                if let Some(content) = publication_content.as_deref() {
                    anyhow::ensure!(
                        payload_revision.is_some(),
                        "deferred payload requires a revision guard"
                    );
                    let revision = agent_memory::put_payload(
                        &tx,
                        &memory_id,
                        crate::convention::memory_scope_kind_to_db(resolved.scope.kind),
                        &resolved.scope_key_hash,
                        content,
                    )
                    .await?;
                    anyhow::ensure!(
                        revision == record.payload_version,
                        "published payload version mismatch"
                    );
                }
                let row = if supersedes.is_some() || key.is_none() {
                    agent_memory::insert_memory_record(&tx, record, resolved.clone(), now).await?
                } else {
                    agent_memory::upsert_active_memory_record(&tx, record, resolved.clone(), now)
                        .await?
                };
                if row.id != memory_id {
                    bail!("memory publication identity mismatch");
                }
                if let Some(old_id) = supersedes.as_deref() {
                    agent_memory_identity::transfer(&tx, old_id, &memory_id).await?;
                }
                if let Some(identity) = canonical_identity.as_ref() {
                    agent_memory_identity::bind(
                        &tx,
                        &resolved,
                        &identity.namespace,
                        &identity.key,
                        &memory_id,
                    )
                    .await?;
                }
                agent_memory_event::append_prepared_memory_event(&tx, publication_event).await?;
                tx.commit().await.context("commit memory publication")?;
                Ok(Some(crate::memory::agent_memory_control_record_from_model(
                    row,
                )?))
            }
        })
        .await
    }

    pub async fn get_agent_memory_by_identity(
        &self,
        scope: MemoryScope,
        namespace: &str,
        key: &str,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let resolved = self.resolve_memory_scope(scope).await?;
        let namespace = crate::memory::normalized_memory_namespace(Some(namespace))?;
        let key = crate::memory::normalized_memory_key(key)?;
        let id =
            agent_memory_identity::lookup(&self.connection, &resolved, &namespace, &key).await?;
        match id {
            Some(id) => self.get_agent_memory_record(&id, false).await,
            None => Ok(None),
        }
    }

    pub async fn bind_agent_memory_identity(
        &self,
        memory_id: &str,
        identity: pioneer_protocol::MemoryCanonicalKey,
    ) -> Result<()> {
        let resolved = self.resolve_memory_scope(identity.scope.clone()).await?;
        self.retry_write(|| {
            let resolved = resolved.clone();
            let identity = identity.clone();
            async move {
                let tx = self.connection.begin().await?;
                let row = agent_memory::find_memory_by_id(&tx, memory_id, false)
                    .await?
                    .context("identity target is no longer active")?;
                if row.scope_kind != crate::convention::memory_scope_kind_to_db(resolved.scope.kind)
                    || row.scope_key_hash != resolved.scope_key_hash
                    || row.workspace_id != resolved.workspace_id
                {
                    bail!("identity target scope mismatch");
                }
                agent_memory_identity::bind(
                    &tx,
                    &resolved,
                    &identity.namespace,
                    &identity.key,
                    memory_id,
                )
                .await?;
                tx.commit().await?;
                Ok(())
            }
        })
        .await
    }

    pub async fn get_agent_memory_record(
        &self,
        memory_id: &str,
        include_non_active: bool,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        Ok(
            agent_memory::find_memory_by_id(&self.connection, memory_id, include_non_active)
                .await?
                .map(crate::memory::agent_memory_control_record_from_model)
                .transpose()?,
        )
    }

    pub async fn get_active_agent_memory_by_key(
        &self,
        scope: MemoryScope,
        namespace: Option<&str>,
        key: &str,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let resolved = self.resolve_memory_scope(scope).await?;
        let namespace = crate::memory::normalized_memory_namespace(namespace)?;
        let Some(row) = agent_memory::find_active_memory_by_scoped_key(
            &self.connection,
            &resolved,
            namespace.as_str(),
            key,
        )
        .await?
        else {
            return Ok(None);
        };
        let record = crate::memory::agent_memory_control_record_from_model(row)?;
        if let Some(guard) = workspace_guard
            && !crate::memory::workspace_allowed_by_guard(
                record.scope.kind,
                &record.workspace_id,
                &guard,
            )
        {
            return Ok(None);
        }
        Ok(Some(record))
    }

    pub async fn update_agent_memory_metadata(
        &self,
        memory_id: &str,
        metadata_json: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let Some(event_source) =
            agent_memory::find_memory_by_id(&self.connection, memory_id, true).await?
        else {
            return Ok(None);
        };
        let event_details_json =
            serde_json::json!({ "reason": "semantic_evidence_merge" }).to_string();
        let prepared_event = prepare_memory_event_for_stored_memory(
            generate_id(DB_ID_LEN),
            &event_source,
            MEMORY_EVENT_UPDATED,
            None,
            Some(event_details_json),
            event_timestamp_secs,
        );
        let now = unix_to_datetime(event_timestamp_secs);
        let row = self
            .retry_write(|| {
                let metadata_json = metadata_json.clone();
                let event_source = event_source.clone();
                let prepared_event = prepared_event.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin agent memory metadata update transaction")?;
                    let Some(row) = agent_memory::update_memory_metadata(
                        &transaction,
                        memory_id,
                        metadata_json,
                        now.clone(),
                    )
                    .await?
                    else {
                        transaction.commit().await.context(
                            "failed to commit empty agent memory metadata update transaction",
                        )?;
                        return Ok(None);
                    };
                    if !stored_memory_event_identity_matches(&event_source, &row) {
                        bail!("agent memory identity changed during metadata update");
                    }
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit agent memory metadata update transaction")?;
                    Ok(Some(row))
                }
            })
            .await?;
        row.map(crate::memory::agent_memory_control_record_from_model)
            .transpose()
    }

    pub async fn list_agent_memory_records(
        &self,
        filter: AgentMemoryListFilter,
    ) -> Result<Vec<AgentMemoryControlRecord>> {
        let resolved = self.resolve_memory_scopes(filter.scopes.clone()).await?;
        let rows = agent_memory::list_memory_records(
            &self.connection,
            filter,
            resolved,
            chrono::Utc::now().fixed_offset(),
        )
        .await?;
        rows.into_iter()
            .map(crate::memory::agent_memory_control_record_from_model)
            .collect()
    }

    pub async fn list_agent_memory_record_page(
        &self,
        filter: AgentMemoryListFilter,
        before: Option<(i64, String)>,
        now_unix: i64,
    ) -> Result<Vec<AgentMemoryControlRecord>> {
        let resolved = self.resolve_memory_scopes(filter.scopes.clone()).await?;
        agent_memory::list_memory_record_page(
            &self.connection,
            filter,
            resolved,
            unix_to_datetime(now_unix),
            before,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_control_record_from_model)
        .collect()
    }

    pub async fn mark_agent_memory_deleted(
        &self,
        memory_id: &str,
        actor: Option<MemoryActorRecord>,
        reason: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let Some(event_source) =
            agent_memory::find_memory_by_id(&self.connection, memory_id, true).await?
        else {
            return Ok(None);
        };
        let event_details_json = reason
            .as_ref()
            .map(|reason| serde_json::json!({ "reason": reason }).to_string());
        let prepared_event = prepare_memory_event_for_stored_memory(
            generate_id(DB_ID_LEN),
            &event_source,
            MEMORY_EVENT_FORGOTTEN,
            actor.clone(),
            event_details_json,
            event_timestamp_secs,
        );
        let now = unix_to_datetime(event_timestamp_secs);
        let row = self
            .retry_write(|| {
                let actor = actor.clone();
                let reason = reason.clone();
                let event_source = event_source.clone();
                let prepared_event = prepared_event.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin agent memory delete transaction")?;
                    let Some(row) = agent_memory::mark_memory_deleted(
                        &transaction,
                        memory_id,
                        actor.clone(),
                        reason.clone(),
                        now.clone(),
                    )
                    .await?
                    else {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit empty agent memory delete transaction")?;
                        return Ok(None);
                    };
                    if !stored_memory_event_identity_matches(&event_source, &row) {
                        bail!("agent memory identity changed during delete");
                    }
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit agent memory delete transaction")?;
                    Ok(Some(row))
                }
            })
            .await?;
        row.map(crate::memory::agent_memory_control_record_from_model)
            .transpose()
    }

    pub async fn mark_agent_memory_superseded(
        &self,
        memory_id: &str,
        superseded_by: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let Some(event_source) =
            agent_memory::find_memory_by_id(&self.connection, memory_id, true).await?
        else {
            return Ok(None);
        };
        let event_details_json = serde_json::json!({ "superseded_by": superseded_by }).to_string();
        let prepared_event = prepare_memory_event_for_stored_memory(
            generate_id(DB_ID_LEN),
            &event_source,
            MEMORY_EVENT_SUPERSEDED,
            None,
            Some(event_details_json),
            event_timestamp_secs,
        );
        let now = unix_to_datetime(event_timestamp_secs);
        let row = self
            .retry_write(|| {
                let event_source = event_source.clone();
                let prepared_event = prepared_event.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin agent memory supersede transaction")?;
                    let Some(row) = agent_memory::mark_memory_superseded(
                        &transaction,
                        memory_id,
                        superseded_by,
                        now.clone(),
                    )
                    .await?
                    else {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit empty agent memory supersede transaction")?;
                        return Ok(None);
                    };
                    if !stored_memory_event_identity_matches(&event_source, &row) {
                        bail!("agent memory identity changed during supersede");
                    }
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit agent memory supersede transaction")?;
                    Ok(Some(row))
                }
            })
            .await?;
        row.map(crate::memory::agent_memory_control_record_from_model)
            .transpose()
    }

    pub async fn mark_agent_memory_expired(
        &self,
        memory_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let Some(event_source) =
            agent_memory::find_memory_by_id(&self.connection, memory_id, true).await?
        else {
            return Ok(None);
        };
        let prepared_event = prepare_memory_event_for_stored_memory(
            generate_id(DB_ID_LEN),
            &event_source,
            MEMORY_EVENT_EXPIRED,
            None,
            None,
            event_timestamp_secs,
        );
        let now = unix_to_datetime(event_timestamp_secs);
        let row = self
            .retry_write(|| {
                let event_source = event_source.clone();
                let prepared_event = prepared_event.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin agent memory expire transaction")?;
                    let Some(row) =
                        agent_memory::mark_memory_expired(&transaction, memory_id, now.clone())
                            .await?
                    else {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit empty agent memory expire transaction")?;
                        return Ok(None);
                    };
                    if !stored_memory_event_identity_matches(&event_source, &row) {
                        bail!("agent memory identity changed during expiration");
                    }
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit agent memory expire transaction")?;
                    Ok(Some(row))
                }
            })
            .await?;
        row.map(crate::memory::agent_memory_control_record_from_model)
            .transpose()
    }

    pub async fn mark_agent_memory_repair_status(
        &self,
        memory_id: &str,
        repair_status: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryControlRecord>> {
        let Some(event_source) =
            agent_memory::find_memory_by_id(&self.connection, memory_id, true).await?
        else {
            return Ok(None);
        };
        let event_details_json = serde_json::json!({ "repair_status": repair_status }).to_string();
        let prepared_event = prepare_memory_event_for_stored_memory(
            generate_id(DB_ID_LEN),
            &event_source,
            MEMORY_EVENT_REPAIR_STATUS_CHANGED,
            None,
            Some(event_details_json),
            event_timestamp_secs,
        );
        let now = unix_to_datetime(event_timestamp_secs);
        let row = self
            .retry_write(|| {
                let event_source = event_source.clone();
                let prepared_event = prepared_event.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin agent memory repair status transaction")?;
                    let row = agent_memory::mark_memory_repair_status(
                        &transaction,
                        memory_id,
                        repair_status,
                        now.clone(),
                    )
                    .await?;
                    let Some(row) = row else {
                        transaction.commit().await.context(
                            "failed to commit empty agent memory repair status transaction",
                        )?;
                        return Ok(None);
                    };
                    if !stored_memory_event_identity_matches(&event_source, &row) {
                        bail!("agent memory identity changed during repair-status update");
                    }
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit agent memory repair status transaction")?;
                    Ok(Some(row))
                }
            })
            .await?;
        row.map(crate::memory::agent_memory_control_record_from_model)
            .transpose()
    }

    pub async fn record_agent_memory_access(
        &self,
        memory_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        let Some(event_source) =
            agent_memory::find_memory_by_id(&self.connection, memory_id, true).await?
        else {
            return Ok(false);
        };
        let prepared_event = agent_memory_event::prepare_memory_event_with_id(
            generate_id(DB_ID_LEN),
            NewAgentMemoryEvent {
                memory_id: Some(event_source.id.clone()),
                candidate_id: None,
                workspace_id: event_source.workspace_id.clone(),
                event_kind: MEMORY_EVENT_ACCESSED.to_owned(),
                actor: None,
                thread_id: None,
                turn_id: None,
                item_id: None,
                details_json: None,
                created_at_unix: event_timestamp_secs,
            },
        );
        let now = unix_to_datetime(event_timestamp_secs);
        self.retry_write(|| {
            let event_source = event_source.clone();
            let prepared_event = prepared_event.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin agent memory access transaction")?;
                let updated =
                    agent_memory::increment_memory_access(&transaction, memory_id, now.clone())
                        .await?;
                if updated {
                    let row = agent_memory::find_memory_by_id(&transaction, memory_id, true)
                        .await?
                        .context("accessed memory row missing after update")?;
                    if !stored_memory_event_identity_matches(&event_source, &row) {
                        bail!("agent memory identity changed during access update");
                    }
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit agent memory access transaction")?;
                Ok(updated)
            }
        })
        .await
    }

    pub async fn list_agent_memory_events(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        agent_memory_event::list_memory_events(&self.connection, memory_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_event_record_from_model)
            .collect()
    }

    pub async fn list_agent_memory_candidate_events(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        agent_memory_event::list_candidate_events(&self.connection, candidate_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_event_record_from_model)
            .collect()
    }

    pub async fn list_workspace_agent_memory_events(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryEventRecord>> {
        agent_memory_event::list_workspace_memory_events(&self.connection, workspace_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_event_record_from_model)
            .collect()
    }

    pub async fn create_agent_memory_quarantine_marker(
        &self,
        quarantine: NewAgentMemoryQuarantine,
    ) -> Result<AgentMemoryQuarantineRecord> {
        let prepared_quarantine = agent_memory_quarantine::prepare_active_quarantine(quarantine);
        let prepared_event = agent_memory_event::prepare_memory_event(prepared_quarantine.event());
        self.retry_write(|| {
            let prepared_quarantine = prepared_quarantine.clone();
            let prepared_event = prepared_event.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory quarantine transaction")?;
                let row = agent_memory_quarantine::create_active_quarantine(
                    &transaction,
                    prepared_quarantine,
                )
                .await?;
                agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                    .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory quarantine transaction")?;
                crate::memory::agent_memory_quarantine_record_from_model(row)
            }
        })
        .await
    }

    pub async fn get_active_agent_memory_quarantine(
        &self,
        memory_id: &str,
    ) -> Result<Option<AgentMemoryQuarantineRecord>> {
        agent_memory_quarantine::find_active_quarantine_by_memory(&self.connection, memory_id)
            .await?
            .map(crate::memory::agent_memory_quarantine_record_from_model)
            .transpose()
    }

    pub async fn list_active_agent_memory_quarantines(
        &self,
        memory_ids: &[String],
    ) -> Result<Vec<AgentMemoryQuarantineRecord>> {
        agent_memory_quarantine::list_active_quarantines_by_memory_ids(&self.connection, memory_ids)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_quarantine_record_from_model)
            .collect()
    }

    pub async fn resolve_agent_memory_quarantine(
        &self,
        resolution: ResolveAgentMemoryQuarantine,
    ) -> Result<Option<AgentMemoryQuarantineRecord>> {
        let (prepared_resolution, prepared_event) =
            agent_memory_quarantine::prepare_quarantine_resolution(&self.connection, resolution)
                .await?;
        let prepared_event = prepared_event.map(agent_memory_event::prepare_memory_event);
        self.retry_write(|| {
            let prepared_resolution = prepared_resolution.clone();
            let prepared_event = prepared_event.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory quarantine restore transaction")?;
                let row = agent_memory_quarantine::resolve_active_quarantine(
                    &transaction,
                    prepared_resolution,
                )
                .await?;
                let Some(row) = row else {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit empty memory quarantine restore transaction")?;
                    return Ok(None);
                };
                let prepared_event = prepared_event.context(
                    "resolved memory quarantine is missing its prepared lifecycle event",
                )?;
                agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                    .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory quarantine restore transaction")?;
                Ok(Some(
                    crate::memory::agent_memory_quarantine_record_from_model(row)?,
                ))
            }
        })
        .await
    }

    pub async fn list_agent_memory_quarantine_history(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQuarantineRecord>> {
        agent_memory_quarantine::list_quarantine_history_for_memory(
            &self.connection,
            memory_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_quarantine_record_from_model)
        .collect()
    }

    pub async fn insert_agent_memory_candidate(
        &self,
        candidate: NewAgentMemoryCandidate,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryCandidateRecord> {
        self.insert_agent_memory_candidate_with_outcome(candidate, event_timestamp_secs)
            .await
            .map(|(candidate, _created)| candidate)
    }

    pub async fn insert_agent_memory_candidate_with_outcome(
        &self,
        mut candidate: NewAgentMemoryCandidate,
        event_timestamp_secs: i64,
    ) -> Result<(AgentMemoryCandidateRecord, bool)> {
        let candidate_id = candidate
            .id
            .get_or_insert_with(|| generate_id(DB_ID_LEN))
            .clone();
        self.retry_write(|| {
            let candidate = candidate.clone();
            let candidate_id = candidate_id.clone();
            async move {
                let resolved = self.resolve_memory_scope(candidate.scope.clone()).await?;
                let prepared_event =
                    agent_memory_event::prepare_memory_event(NewAgentMemoryEvent {
                        memory_id: None,
                        candidate_id: Some(candidate_id),
                        workspace_id: resolved.workspace_id.clone(),
                        event_kind: MEMORY_EVENT_CANDIDATE_CREATED.to_owned(),
                        actor: None,
                        thread_id: candidate.source_thread_id.clone(),
                        turn_id: candidate.source_turn_id.clone(),
                        item_id: candidate.source_item_id.clone(),
                        details_json: None,
                        created_at_unix: event_timestamp_secs,
                    });
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin memory candidate insert transaction")?;
                let (row, created) = agent_memory_candidate::insert_candidate(
                    &transaction,
                    candidate,
                    resolved,
                    unix_to_datetime(event_timestamp_secs),
                )
                .await?;
                if created {
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit memory candidate insert transaction")?;
                Ok((
                    crate::memory::agent_memory_candidate_record_from_model(row)?,
                    created,
                ))
            }
        })
        .await
    }

    pub async fn get_agent_memory_candidate_by_dedupe(
        &self,
        scope: MemoryScope,
        namespace: Option<&str>,
        dedupe_key: &str,
        statuses: Vec<MemoryCandidateStatus>,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        let resolved = self.resolve_memory_scope(scope).await?;
        let namespace = crate::memory::normalized_memory_namespace(namespace)?;
        let Some(row) = agent_memory_candidate::find_candidate_by_dedupe(
            &self.connection,
            &resolved,
            namespace.as_str(),
            dedupe_key,
            statuses.as_slice(),
        )
        .await?
        else {
            return Ok(None);
        };
        let record = crate::memory::agent_memory_candidate_record_from_model(row)?;
        if let Some(guard) = workspace_guard
            && !crate::memory::workspace_allowed_by_guard(
                record.scope.kind,
                &record.workspace_id,
                &guard,
            )
        {
            return Ok(None);
        }
        Ok(Some(record))
    }

    pub async fn get_agent_memory_candidate(
        &self,
        candidate_id: &str,
        workspace_guard: Option<MemoryWorkspaceGuard>,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        let Some(row) =
            agent_memory_candidate::find_candidate_by_id(&self.connection, candidate_id).await?
        else {
            return Ok(None);
        };
        let record = crate::memory::agent_memory_candidate_record_from_model(row)?;
        if let Some(guard) = workspace_guard
            && !crate::memory::workspace_allowed_by_guard(
                record.scope.kind,
                &record.workspace_id,
                &guard,
            )
        {
            return Ok(None);
        }
        Ok(Some(record))
    }

    pub async fn update_agent_memory_candidate_metadata_if_unchanged(
        &self,
        candidate_id: &str,
        expected_metadata_json: Option<String>,
        metadata_json: Option<String>,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        let row = self
            .retry_write(|| {
                let expected_metadata_json = expected_metadata_json.clone();
                let metadata_json = metadata_json.clone();
                async move {
                    let transaction =
                        self.connection.begin().await.context(
                            "failed to begin memory candidate metadata update transaction",
                        )?;
                    let Some(row) = agent_memory_candidate::update_candidate_metadata_if_unchanged(
                        &transaction,
                        candidate_id,
                        expected_metadata_json,
                        metadata_json,
                    )
                    .await?
                    else {
                        transaction.commit().await.context(
                            "failed to commit empty memory candidate metadata update transaction",
                        )?;
                        return Ok(None);
                    };
                    transaction
                        .commit()
                        .await
                        .context("failed to commit memory candidate metadata update transaction")?;
                    Ok(Some(row))
                }
            })
            .await?;
        row.map(crate::memory::agent_memory_candidate_record_from_model)
            .transpose()
    }

    pub async fn update_agent_memory_candidate_status(
        &self,
        update: AgentMemoryCandidateStatusUpdateRecord,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        let Some(event_source) = agent_memory_candidate::find_candidate_by_id(
            &self.connection,
            update.candidate_id.as_str(),
        )
        .await?
        else {
            return Ok(None);
        };
        let event_details_json = update
            .decision_reason
            .as_ref()
            .map(|reason| serde_json::json!({ "reason": reason }).to_string());
        let prepared_event = prepare_memory_event_for_stored_candidate(
            generate_id(DB_ID_LEN),
            &event_source,
            update.promoted_memory_id.clone(),
            memory_candidate_status_event_kind(update.status),
            update.decided_by.clone(),
            event_details_json,
            update.decided_at_unix,
        );
        let row = self
            .retry_write(|| {
                let update = update.clone();
                let event_source = event_source.clone();
                let prepared_event = prepared_event.clone();
                async move {
                    let transaction =
                        self.connection.begin().await.context(
                            "failed to begin memory candidate status update transaction",
                        )?;
                    let Some(row) = agent_memory_candidate::update_candidate_status(
                        &transaction,
                        update.clone(),
                    )
                    .await?
                    else {
                        transaction.commit().await.context(
                            "failed to commit empty memory candidate status update transaction",
                        )?;
                        return Ok(None);
                    };
                    if !stored_candidate_event_identity_matches(&event_source, &row) {
                        bail!("agent memory candidate identity changed during status update");
                    }
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit memory candidate status update transaction")?;
                    Ok(Some(row))
                }
            })
            .await?;
        row.map(crate::memory::agent_memory_candidate_record_from_model)
            .transpose()
    }

    pub async fn list_agent_memory_candidates(
        &self,
        filter: AgentMemoryCandidateListFilter,
    ) -> Result<Vec<AgentMemoryCandidateRecord>> {
        let resolved = self.resolve_memory_scopes(filter.scopes.clone()).await?;
        agent_memory_candidate::list_candidates(&self.connection, filter, resolved)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_candidate_record_from_model)
            .collect()
    }

    pub async fn decide_agent_memory_candidate(
        &self,
        decision: AgentMemoryCandidateDecisionRecord,
    ) -> Result<Option<AgentMemoryCandidateRecord>> {
        let Some(event_source) = agent_memory_candidate::find_candidate_by_id(
            &self.connection,
            decision.candidate_id.as_str(),
        )
        .await?
        else {
            return Ok(None);
        };
        let event_kind = match decision.decision {
            MemoryCandidateDecision::Approve => MEMORY_EVENT_CANDIDATE_APPROVED,
            MemoryCandidateDecision::Reject => MEMORY_EVENT_CANDIDATE_REJECTED,
            MemoryCandidateDecision::Expire => MEMORY_EVENT_CANDIDATE_EXPIRED,
        };
        let event_details_json = decision
            .decision_reason
            .as_ref()
            .map(|reason| serde_json::json!({ "reason": reason }).to_string());
        let prepared_event = prepare_memory_event_for_stored_candidate(
            generate_id(DB_ID_LEN),
            &event_source,
            decision.promoted_memory_id.clone(),
            event_kind,
            decision.decided_by.clone(),
            event_details_json,
            decision.decided_at_unix,
        );
        let row = self
            .retry_write(|| {
                let decision = decision.clone();
                let event_source = event_source.clone();
                let prepared_event = prepared_event.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin memory candidate decision transaction")?;
                    let Some(row) =
                        agent_memory_candidate::decide_candidate(&transaction, decision.clone())
                            .await?
                    else {
                        transaction.commit().await.context(
                            "failed to commit empty memory candidate decision transaction",
                        )?;
                        return Ok(None);
                    };
                    if !stored_candidate_event_identity_matches(&event_source, &row) {
                        bail!("agent memory candidate identity changed during decision");
                    }
                    agent_memory_event::append_prepared_memory_event(&transaction, prepared_event)
                        .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit memory candidate decision transaction")?;
                    Ok(Some(row))
                }
            })
            .await?;
        row.map(crate::memory::agent_memory_candidate_record_from_model)
            .transpose()
    }

    pub async fn insert_agent_memory_policy_decision(
        &self,
        decision: NewAgentMemoryPolicyDecision,
    ) -> Result<AgentMemoryPolicyDecisionRecord> {
        self.retry_write(|| {
            let decision = decision.clone();
            async move {
                let row = agent_memory_policy_decision::insert_policy_decision(
                    &self.connection,
                    decision,
                )
                .await?;
                crate::memory::agent_memory_policy_decision_record_from_model(row)
            }
        })
        .await
    }

    pub async fn list_agent_memory_policy_decisions_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        agent_memory_policy_decision::list_policy_decisions_for_memory(
            &self.connection,
            memory_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_policy_decision_record_from_model)
        .collect()
    }

    pub async fn list_agent_memory_policy_decisions_for_candidate(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        agent_memory_policy_decision::list_policy_decisions_for_candidate(
            &self.connection,
            candidate_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_policy_decision_record_from_model)
        .collect()
    }

    pub async fn list_agent_memory_policy_decisions_for_thread(
        &self,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryPolicyDecisionRecord>> {
        agent_memory_policy_decision::list_policy_decisions_for_thread(
            &self.connection,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_policy_decision_record_from_model)
        .collect()
    }

    pub async fn insert_agent_memory_quality_decision(
        &self,
        decision: NewAgentMemoryQualityDecision,
    ) -> Result<AgentMemoryQualityDecisionRecord> {
        self.retry_write(|| {
            let decision = decision.clone();
            async move {
                let row = agent_memory_quality_decision::insert_quality_decision(
                    &self.connection,
                    decision,
                )
                .await?;
                crate::memory::agent_memory_quality_decision_record_from_model(row)
            }
        })
        .await
    }

    pub async fn list_agent_memory_quality_decisions_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        agent_memory_quality_decision::list_quality_decisions_for_memory(
            &self.connection,
            memory_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_quality_decision_record_from_model)
        .collect()
    }

    pub async fn list_agent_memory_quality_decisions_for_candidate(
        &self,
        candidate_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        agent_memory_quality_decision::list_quality_decisions_for_candidate(
            &self.connection,
            candidate_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_quality_decision_record_from_model)
        .collect()
    }

    pub async fn list_agent_memory_quality_decisions_for_thread(
        &self,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryQualityDecisionRecord>> {
        agent_memory_quality_decision::list_quality_decisions_for_thread(
            &self.connection,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::memory::agent_memory_quality_decision_record_from_model)
        .collect()
    }

    pub async fn enqueue_agent_memory_repair_job(
        &self,
        job: NewAgentMemoryRepairJob,
        event_timestamp_secs: i64,
    ) -> Result<AgentMemoryRepairJobRecord> {
        self.retry_write(|| {
            let job = job.clone();
            async move {
                let write = self.connection.begin().await?;
                let row = agent_memory_repair_job::enqueue_repair_job(
                    &write,
                    job,
                    unix_to_datetime(event_timestamp_secs),
                )
                .await?;
                write.commit().await?;
                crate::memory::agent_memory_repair_job_record_from_model(row)
            }
        })
        .await
    }

    pub async fn get_agent_memory_repair_job(
        &self,
        job_id: &str,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        agent_memory_repair_job::find_repair_job_by_id(&self.connection, job_id)
            .await?
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .transpose()
    }

    pub async fn list_agent_memory_repair_jobs_for_memory(
        &self,
        memory_id: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryRepairJobRecord>> {
        agent_memory_repair_job::list_repair_jobs_for_memory(&self.connection, memory_id, limit)
            .await?
            .into_iter()
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .collect()
    }

    pub async fn claim_due_agent_memory_repair_jobs(
        &self,
        now_unix: i64,
        lock_ttl_secs: i64,
        locked_by: &str,
        limit: u64,
    ) -> Result<Vec<AgentMemoryRepairJobRecord>> {
        self.retry_write(|| async {
            let now = unix_to_datetime(now_unix);
            let lock_expires_at = unix_to_datetime(now_unix.saturating_add(lock_ttl_secs));
            let write = self.connection.begin().await?;
            let rows = agent_memory_repair_job::claim_due_repair_jobs(
                &write,
                now,
                lock_expires_at,
                locked_by,
                limit,
            )
            .await?;
            write.commit().await?;
            rows.into_iter()
                .map(crate::memory::agent_memory_repair_job_record_from_model)
                .collect()
        })
        .await
    }

    pub async fn mark_agent_memory_repair_job_running(
        &self,
        job_id: &str,
        locked_by: &str,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        self.retry_write(|| async {
            agent_memory_repair_job::mark_repair_job_running(
                &self.connection,
                job_id,
                locked_by,
                unix_to_datetime(event_timestamp_secs),
            )
            .await?
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .transpose()
        })
        .await
    }

    pub async fn mark_agent_memory_repair_job_completed(
        &self,
        job_id: &str,
        locked_by: &str,
        result_json: Option<String>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        let result_json_value = result_json.clone();
        self.retry_write(|| async {
            agent_memory_repair_job::mark_repair_job_completed(
                &self.connection,
                job_id,
                locked_by,
                result_json_value.clone(),
                unix_to_datetime(event_timestamp_secs),
            )
            .await?
            .map(crate::memory::agent_memory_repair_job_record_from_model)
            .transpose()
        })
        .await
    }

    pub async fn mark_agent_memory_repair_job_failed(
        &self,
        job_id: &str,
        locked_by: &str,
        last_error: String,
        retry_at_unix: Option<i64>,
        event_timestamp_secs: i64,
    ) -> Result<Option<AgentMemoryRepairJobRecord>> {
        let last_error_value = last_error.clone();
        self.retry_write(|| async {
            let write = self.connection.begin().await?;
            let row = agent_memory_repair_job::mark_repair_job_failed(
                &write,
                job_id,
                locked_by,
                last_error_value.clone(),
                retry_at_unix.map(unix_to_datetime),
                unix_to_datetime(event_timestamp_secs),
            )
            .await?;
            write.commit().await?;
            row.map(crate::memory::agent_memory_repair_job_record_from_model)
                .transpose()
        })
        .await
    }

    pub async fn put_memory_payload(
        &self,
        id: &str,
        scope: MemoryScope,
        content: &str,
    ) -> Result<i64> {
        let resolved = self.resolve_memory_scope(scope).await?;
        self.retry_write(|| {
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
        let hash = memory_scope_key_hash(scope.kind, &key)?;
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
            memory_scope_key_hash(scope.kind, &key)?
        };
        anyhow::ensure!(!hash.trim().is_empty(), "payload cleanup scope hash empty");
        let repair = scope.key.trim().is_empty();
        self.retry_write(|| async {
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
        filter: AgentMemoryListFilter,
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

    /// The full payload is read outside capacity by the existing backend. This
    /// short transaction revalidates owner, lifecycle, quarantine and (for a
    /// durable backend) payload revision before clearing repair-needed state.
    pub async fn confirm_agent_memory_payload_repair(
        &self,
        expected: &AgentMemoryControlRecord,
        revision: Option<i64>,
        now_unix: i64,
    ) -> Result<()> {
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
        let source = agent_memory::find_memory_by_id(&self.connection, &expected.id, true)
            .await?
            .ok_or_else(|| anyhow::anyhow!("repair owner missing"))?;
        let details = serde_json::json!({"repair_status":"ok"}).to_string();
        let event = prepare_memory_event_for_stored_memory(
            pioneer_protocol::generate_id(21),
            &source,
            MEMORY_EVENT_REPAIR_STATUS_CHANGED,
            None,
            Some(details),
            now_unix,
        );
        self.retry_write(|| async {
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
                    && stored_memory_event_identity_matches(&source, &current),
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
                agent_memory_event::append_prepared_memory_event(&tx, event.clone()).await?;
            }
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}

fn memory_candidate_status_event_kind(status: MemoryCandidateStatus) -> &'static str {
    match status {
        MemoryCandidateStatus::Approved => MEMORY_EVENT_CANDIDATE_APPROVED,
        MemoryCandidateStatus::Rejected
        | MemoryCandidateStatus::AutoRejected
        | MemoryCandidateStatus::ReviewDisabledRejected => MEMORY_EVENT_CANDIDATE_REJECTED,
        MemoryCandidateStatus::Expired => MEMORY_EVENT_CANDIDATE_EXPIRED,
        MemoryCandidateStatus::Pending
        | MemoryCandidateStatus::PendingSilent
        | MemoryCandidateStatus::AskOnUse
        | MemoryCandidateStatus::NeedsReview => MEMORY_EVENT_CANDIDATE_CREATED,
        MemoryCandidateStatus::Superseded => "candidate_superseded",
        MemoryCandidateStatus::MergedDuplicate => "candidate_merged_duplicate",
    }
}

fn memory_event_for_record(
    event: Option<NewAgentMemoryEvent>,
    memory_id: String,
    workspace_id: Option<String>,
    default_event_kind: &str,
    created_at_unix: i64,
) -> NewAgentMemoryEvent {
    match event {
        Some(mut event) => {
            if event.memory_id.is_none() {
                event.memory_id = Some(memory_id);
            }
            if event.workspace_id.is_none() {
                event.workspace_id = workspace_id;
            }
            event
        }
        None => NewAgentMemoryEvent {
            memory_id: Some(memory_id),
            candidate_id: None,
            workspace_id,
            event_kind: default_event_kind.to_owned(),
            actor: None,
            thread_id: None,
            turn_id: None,
            item_id: None,
            details_json: None,
            created_at_unix,
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_memory_event_for_stored_memory(
    event_id: String,
    row: &pioneer_entity::agent_memory::Model,
    event_kind: &str,
    actor: Option<MemoryActorRecord>,
    details_json: Option<String>,
    created_at_unix: i64,
) -> agent_memory_event::PreparedAgentMemoryEvent {
    agent_memory_event::prepare_memory_event_with_id(
        event_id,
        NewAgentMemoryEvent {
            memory_id: Some(row.id.clone()),
            candidate_id: None,
            workspace_id: row.workspace_id.clone(),
            event_kind: event_kind.to_owned(),
            actor,
            thread_id: row.source_thread_id.clone(),
            turn_id: row.source_turn_id.clone(),
            item_id: row.source_item_id.clone(),
            details_json,
            created_at_unix,
        },
    )
}

fn stored_memory_event_identity_matches(
    expected: &pioneer_entity::agent_memory::Model,
    actual: &pioneer_entity::agent_memory::Model,
) -> bool {
    expected.id == actual.id
        && expected.workspace_id == actual.workspace_id
        && expected.source_thread_id == actual.source_thread_id
        && expected.source_turn_id == actual.source_turn_id
        && expected.source_item_id == actual.source_item_id
}

#[allow(clippy::too_many_arguments)]
fn prepare_memory_event_for_stored_candidate(
    event_id: String,
    row: &pioneer_entity::agent_memory_candidate::Model,
    memory_id: Option<String>,
    event_kind: &str,
    actor: Option<MemoryActorRecord>,
    details_json: Option<String>,
    created_at_unix: i64,
) -> agent_memory_event::PreparedAgentMemoryEvent {
    agent_memory_event::prepare_memory_event_with_id(
        event_id,
        NewAgentMemoryEvent {
            memory_id,
            candidate_id: Some(row.id.clone()),
            workspace_id: row.workspace_id.clone(),
            event_kind: event_kind.to_owned(),
            actor,
            thread_id: row.source_thread_id.clone(),
            turn_id: row.source_turn_id.clone(),
            item_id: row.source_item_id.clone(),
            details_json,
            created_at_unix,
        },
    )
}

fn stored_candidate_event_identity_matches(
    expected: &pioneer_entity::agent_memory_candidate::Model,
    actual: &pioneer_entity::agent_memory_candidate::Model,
) -> bool {
    expected.id == actual.id
        && expected.workspace_id == actual.workspace_id
        && expected.source_thread_id == actual.source_thread_id
        && expected.source_turn_id == actual.source_turn_id
        && expected.source_item_id == actual.source_item_id
}
