//! Bounded auxiliary-call usage journal. No provider payload or replay history.
use super::execution_observability::ExecutionObservationInput;
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::provider_usage_observation as journal;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionTrait,
};

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
                "title"
                    | "summary"
                    | "self_improvement"
                    | "memory_extraction"
                    | "embedding"
                    | "main"
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
    pub async fn record_provider_usage(&self, mut record: ProviderUsageObservation) -> Result<()> {
        // All validation/serialization happens before the writer reservation.

        let mut value: serde_json::Value = serde_json::from_str(&record.usage_json)?;
        let modern = value["_pioneer_observation"]["revision"].as_i64().is_some();
        let evidence = if modern {
            value
                .get_mut("accounting")
                .and_then(serde_json::Value::as_object_mut)
                .and_then(|a| a.remove("pricing"))
                .filter(serde_json::Value::is_object)
        } else {
            None
        };
        // Diagnostics yield byte capacity to the numeric lane. The complete
        // tariff is prepared separately and never trimmed by this budget.
        if modern {
            fit_journal_diagnostics(&mut value)?;
        }
        let observation = if let Some(revision) = value["_pioneer_observation"]["revision"].as_i64()
        {
            // Legacy owners are not inferred from names or model identifiers.
            // Only a verified thread relation gives thread-scoped attribution.
            let owner = pioneer_entity::thread::Entity::find_by_id(&record.owner_id)
                .one(&self.connection)
                .await?
                .filter(|row| row.workspace_id == record.workspace_id);
            ensure!(
                value["_pioneer_observation"]["bound_thread"] != true || owner.is_some(),
                "prepared usage thread removed"
            );
            let causal_parent = value["_pioneer_observation"]["causal_parent"].as_str();
            let causal_turn = if let Some(id) = causal_parent.and_then(|p| p.strip_prefix("turn:"))
            {
                let turn = pioneer_entity::turn::Entity::find_by_id(id)
                    .one(&self.connection)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("usage causal turn unavailable"))?;
                let thread = pioneer_entity::thread::Entity::find_by_id(&turn.thread_id)
                    .one(&self.connection)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("usage causal thread unavailable"))?;
                ensure!(
                    thread.workspace_id == record.workspace_id,
                    "usage causal workspace conflict"
                );
                Some(turn)
            } else {
                None
            };
            let parent = if let Some(parent) = causal_parent.filter(|_| causal_turn.is_none()) {
                let parent =
                    find_usage_causal_parent(&self.connection, &record.workspace_id, parent)
                        .await?;
                // Keep the durable parent identity in both the journal and its
                // projection. Preparation reads only scoped observation metadata;
                // commit_revision rechecks parent existence, ownership and scope
                // inside the same transaction as the journal publication.
                value["_pioneer_observation"]["causal_parent"] = parent.operation_id.clone().into();
                Some(parent)
            } else {
                None
            };
            let thread_id = parent
                .as_ref()
                .and_then(|row| row.thread_id.clone())
                .or_else(|| causal_turn.as_ref().map(|row| row.thread_id.clone()))
                .or_else(|| owner.map(|row| row.id));
            let turn_id = parent
                .as_ref()
                .and_then(|row| row.turn_id.clone())
                .or_else(|| causal_turn.as_ref().map(|row| row.id.clone()));
            let self_improvement =
                if record.operation_kind == "self_improvement" && thread_id.is_none() {
                    let run =
                        pioneer_entity::self_improvement_run::Entity::find_by_id(&record.owner_id)
                            .one(&self.connection)
                            .await?
                            .ok_or_else(|| {
                                anyhow::anyhow!("self improvement usage owner unavailable")
                            })?;
                    ensure!(
                        run.workspace_id == record.workspace_id,
                        "self improvement usage workspace conflict"
                    );
                    true
                } else {
                    false
                };

            let mut financial = value.clone();
            if let Some(metadata) = financial
                .get_mut("_pioneer_observation")
                .and_then(serde_json::Value::as_object_mut)
            {
                metadata.remove("context_manifest"); // stored once at facts.context_manifest
            }

            if let Some(a) = financial
                .get_mut("accounting")
                .and_then(serde_json::Value::as_object_mut)
            {
                a.remove("pricing");
            }
            Some(ExecutionObservationInput {
                workspace_id:record.workspace_id.clone(),operation_id:format!("provider:journal:{}",record.id),revision,
                thread_id:thread_id.clone(),turn_id:turn_id.clone(),
                owner_kind:if turn_id.is_some(){"turn"}else if thread_id.is_some(){"thread"}else if self_improvement{"self_improvement_run"}else{"workspace"}.into(),
                owner_id:turn_id.or_else(||thread_id.clone()).unwrap_or_else(||if self_improvement{record.owner_id.clone()}else{record.workspace_id.clone()}),
                parent_operation_id:value["_pioneer_observation"]["causal_parent"].as_str().map(str::to_owned),kind:if matches!(value["provider"].as_str(),Some("codex-cli"|"claude-cli"|"opencode-cli")){"external_usage_snapshot"}else{"provider_attempt"}.into(),purpose:record.operation_kind.clone(),
                provider:value["provider"].as_str().map(str::to_owned),model:value["model"].as_str().map(str::to_owned),
                status:record.status.clone(),recorded_at_ms:record.updated_at.saturating_mul(1000),
                facts:serde_json::json!({"usage":financial,"timing":value["_pioneer_observation"]["timing"],
                    "context_manifest":value["_pioneer_observation"]["context_manifest"],
                    "legacy_owner_id":record.owner_id,"ownership_coverage":if thread_id.is_some(){"verified_thread"}else if self_improvement{"verified_self_improvement_run"}else{"workspace_only"},
                    "executor_coverage":if matches!(value["provider"].as_str(),Some("codex-cli"|"claude-cli"|"opencode-cli")){"external_service_operation_internal_physical_attempts_unobserved"}else{"gateway_owned_physical_attempt"}}),
                tariff_evidence:evidence.clone(),
            }.prepare()?)
        } else {
            None
        };
        // The journal keeps its legacy public payload. The separate projection
        // is committed atomically and never becomes a second charge source.
        if let Some(hash) = observation.as_ref().and_then(|v| v.tariff_hash()) {
            value["accounting"]["pricing"] =
                serde_json::json!({"retained_evidence_hash":hash,"storage":"execution_tariff"});
        }
        if modern {
            record.usage_json = serde_json::to_string(&value)?;
        }
        record.validate()?;
        // Decode revision evidence outside database capacity. Revalidate the exact
        // predecessor in the writer, so same-second cumulative redelivery cannot
        // replace newer usage. A conflict never re-executes the provider call.
        let predecessor = journal::Entity::find_by_id(&record.id)
            .one(&self.connection)
            .await?;
        let previous_revision = predecessor
            .as_ref()
            .map(|row| -> Result<Option<i64>> {
                let previous: serde_json::Value = serde_json::from_str(&row.usage_json)?;
                Ok(previous["_pioneer_observation"]["revision"].as_i64())
            })
            .transpose()?
            .flatten();
        let incoming_revision = value["_pioneer_observation"]["revision"].as_i64();
        let stale =
            matches!((previous_revision, incoming_revision), (Some(old), Some(new)) if old > new);
        let duplicate =
            matches!((previous_revision, incoming_revision), (Some(old), Some(new)) if old == new);
        let live_only = value["_pioneer_observation"]["live_only"].as_bool() == Some(true);
        let native_turn = value["_pioneer_observation"]["native_turn_id"]
            .as_str()
            .map(str::to_owned);
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            ensure!(
                pioneer_entity::workspace::Entity::find_by_id(&record.workspace_id)
                    .one(&tx)
                    .await?
                    .is_some(),
                "usage workspace missing"
            );
            if let Some(owner) = pioneer_entity::thread::Entity::find_by_id(&record.owner_id)
                .one(&tx)
                .await?
            {
                ensure!(
                    owner.workspace_id == record.workspace_id,
                    "usage owner workspace conflict"
                );
            }
            if let Some(turn) = &native_turn {
                let source_sequence = value["_pioneer_observation"]["native_source_sequence"]
                    .as_i64()
                    .ok_or_else(|| anyhow::anyhow!("native usage source missing"))?;
                let source =
                    pioneer_entity::execution_observation::Entity::find_by_id(source_sequence)
                        .one(&tx)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("native usage source removed"))?;
                ensure!(
                    source.workspace_id == record.workspace_id
                        && source.thread_id.as_deref() == Some(record.owner_id.as_str())
                        && source.turn_id.as_deref() == Some(turn.as_str()),
                    "native usage source owner conflict"
                );
                let owner = pioneer_entity::turn::Entity::find_by_id(turn)
                    .one(&tx)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("native usage turn missing"))?;
                ensure!(
                    owner.thread_id == record.owner_id,
                    "native usage owner conflict"
                );
                let thread = pioneer_entity::thread::Entity::find_by_id(&owner.thread_id)
                    .one(&tx)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("native usage thread missing"))?;
                ensure!(
                    thread.workspace_id == record.workspace_id,
                    "native usage workspace conflict"
                );
            }
            if let Some(existing) = journal::Entity::find_by_id(&record.id).one(&tx).await? {
                ensure!(
                    existing.workspace_id == record.workspace_id
                        && existing.owner_id == record.owner_id
                        && existing.operation_kind == record.operation_kind,
                    "usage journal identity conflict"
                );
                ensure!(
                    predecessor
                        .as_ref()
                        .is_some_and(|before| before.usage_json == existing.usage_json
                            && before.status == existing.status
                            && before.updated_at == existing.updated_at),
                    "usage journal predecessor changed"
                );
                if stale {
                    tx.commit().await?;
                    return Ok(());
                }
                if duplicate {
                    ensure!(
                        existing.usage_json == record.usage_json
                            && existing.status == record.status,
                        "usage journal revision conflict"
                    );
                    tx.commit().await?;
                    return Ok(());
                }
                ensure!(
                    (existing.status == "started" || existing.status == record.status)
                        && (incoming_revision.is_some()
                            || record.updated_at >= existing.updated_at),
                    "usage journal state conflict"
                );
                let mut model: journal::ActiveModel = existing.into();
                model.status = Set(record.status.clone());
                model.usage_json = Set(record.usage_json.clone());
                model.updated_at = Set(record.updated_at);
                model.update(&tx).await?;
            } else {
                ensure!(predecessor.is_none(), "usage journal predecessor removed");
                journal::ActiveModel {
                    id: Set(record.id.clone()),
                    workspace_id: Set(record.workspace_id.clone()),
                    operation_kind: Set(record.operation_kind.clone()),
                    owner_id: Set(record.owner_id.clone()),
                    status: Set(record.status.clone()),
                    usage_json: Set(record.usage_json.clone()),
                    started_at: Set(record.started_at),
                    updated_at: Set(record.updated_at),
                }
                .insert(&tx)
                .await?;
            }
            if let Some(observation) = &observation {
                if live_only {
                    super::execution_observability::validate_prepared_owner(&tx, observation)
                        .await?;
                    super::execution_observability::retain_prepared_tariff(&tx, observation)
                        .await?;
                } else {
                    super::execution_observability::commit(&tx, observation).await?;
                }
            }
            tx.commit().await?;
            Ok(())
        })
        .await
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
        let rows = journal::Entity::find()
            .filter(journal::Column::WorkspaceId.eq(workspace))
            .filter(journal::Column::OwnerId.eq(owner))
            .filter(journal::Column::Id.gt(after))
            .order_by_asc(journal::Column::Id)
            .limit(limit)
            .all(&self.connection)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| ProviderUsageObservation {
                id: row.id,
                workspace_id: row.workspace_id,
                operation_kind: row.operation_kind,
                owner_id: row.owner_id,
                status: row.status,
                usage_json: row.usage_json,
                started_at: row.started_at,
                updated_at: row.updated_at,
            })
            .collect())
    }
}

impl CrudStore {
    pub async fn record_native_provider_usage(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
        revision: i64,
        mut usage: serde_json::Value,
    ) -> Result<()> {
        use sea_orm::{ColumnTrait, QueryFilter, QueryOrder};
        let source = pioneer_entity::execution_observation::Entity::find()
            .filter(pioneer_entity::execution_observation::Column::WorkspaceId.eq(workspace))
            .filter(pioneer_entity::execution_observation::Column::ThreadId.eq(thread))
            .filter(pioneer_entity::execution_observation::Column::TurnId.eq(turn))
            .filter(
                pioneer_entity::execution_observation::Column::OperationId
                    .eq(format!("provider:{turn}:{id}")),
            )
            .order_by_asc(pioneer_entity::execution_observation::Column::Sequence)
            .one(&self.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("native usage attempt was not prepared durably"))?;
        let prepared: serde_json::Value = serde_json::from_str(&source.facts_json)?;
        ensure!(
            prepared["usage"]["physical_attempt_id"].is_string()
                && prepared["usage"]["physical_attempt_id"] == usage["physical_attempt_id"],
            "native usage attempt identity conflict"
        );
        usage["_pioneer_observation"] = serde_json::json!({"revision":revision,"live_only":true,"native_turn_id":turn,"native_source_sequence":source.sequence});
        let now = chrono::Utc::now().timestamp();
        self.record_provider_usage(ProviderUsageObservation {
            id: id.into(),
            workspace_id: workspace.into(),
            operation_kind: "main".into(),
            owner_id: thread.into(),
            status: "started".into(),
            usage_json: serde_json::to_string(&usage)?,
            started_at: source
                .start_wall_ms
                .map(|n| n.div_euclid(1000))
                .unwrap_or(now),
            updated_at: now,
        })
        .await
    }
    pub async fn execution_live_usage(
        &self,
        workspace: &str,
        thread: &str,
        ids: Vec<String>,
    ) -> Result<Vec<journal::Model>> {
        self.execution_scoped_live_usage(workspace, Some(thread), ids)
            .await
    }
    pub async fn execution_scoped_live_usage(
        &self,
        workspace: &str,
        owner: Option<&str>,
        ids: Vec<String>,
    ) -> Result<Vec<journal::Model>> {
        use sea_orm::{ColumnTrait, QueryFilter};
        ensure!(ids.len() <= 100, "live usage page exceeds bound");
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = journal::Entity::find()
            .filter(journal::Column::WorkspaceId.eq(workspace))
            .filter(journal::Column::Id.is_in(ids));
        if let Some(owner) = owner {
            query = query.filter(journal::Column::OwnerId.eq(owner));
        }
        Ok(query.all(&self.connection).await?)
    }
}

fn fit_journal_diagnostics(value: &mut serde_json::Value) -> Result<()> {
    let mut dropped = 0u64;
    // Reserve space for the retained tariff reference and explicit loss marker.
    while serde_json::to_vec(value)?.len() > 30 * 1024 {
        let manifest = &mut value["_pioneer_observation"]["context_manifest"];
        let mut removed = false;
        for key in ["tool_entries", "entries"] {
            if manifest
                .get_mut(key)
                .and_then(serde_json::Value::as_array_mut)
                .is_some_and(|entries| entries.pop().is_some())
            {
                let omitted = if key == "entries" {
                    "omitted_message_entries"
                } else {
                    "omitted_tool_entries"
                };
                manifest[omitted] = (manifest[omitted].as_u64().unwrap_or(0) + 1).into();
                removed = true;
                break;
            }
        }
        if !removed
            && manifest
                .get_mut("compiled_sections")
                .and_then(|v| v.get_mut("sections"))
                .and_then(serde_json::Value::as_array_mut)
                .is_some_and(|entries| entries.pop().is_some())
        {
            manifest["compiled_sections"]["detail_truncated"] = true.into();
            removed = true;
        }
        if !removed {
            break;
        }
        dropped += 1;
    }
    if dropped > 0 {
        value["_pioneer_observation"]["diagnostic_loss"] = serde_json::json!({"context_entries_dropped_for_numeric_capacity":dropped,"financial_facts_dropped":false});
    }
    Ok(())
}

/// Native CLI tools publish a canonical item without ToolObservation, whereas
/// the shared orchestrator scopes nested provider work to its first attempt.
/// Resolve that one known identity difference against persisted evidence, never
/// by dropping a missing parent or guessing a turn/workspace owner.
async fn find_usage_causal_parent<C: sea_orm::ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    requested: &str,
) -> Result<pioneer_entity::execution_observation::Model> {
    use pioneer_entity::execution_observation as fact;
    let exact = fact::Entity::find()
        .filter(fact::Column::WorkspaceId.eq(workspace_id))
        .filter(fact::Column::OperationId.eq(requested))
        .order_by_desc(fact::Column::Revision)
        .one(db)
        .await?;
    if let Some(parent) = exact {
        return Ok(parent);
    }
    let logical = requested
        .strip_prefix("tool:")
        .and_then(|identity| identity.strip_suffix(":1"))
        .and_then(|identity| identity.split_once(':'));
    if let Some((turn_id, item_id)) = logical {
        let item = fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace_id))
            .filter(fact::Column::OperationId.eq(format!("item:{turn_id}:{item_id}")))
            .order_by_desc(fact::Column::Revision)
            .one(db)
            .await?;
        if let Some(item) = item {
            let facts: serde_json::Value = serde_json::from_str(&item.facts_json)?;
            ensure!(
                item.kind == "tool_invocation"
                    && item.turn_id.as_deref() == Some(turn_id)
                    && facts
                        .get("observation")
                        .is_some_and(serde_json::Value::is_null),
                "usage causal parent is not an unobserved CLI tool"
            );
            return Ok(item);
        }
    }
    anyhow::bail!("usage causal parent unavailable")
}
